package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ResilientPollLoopTest {

  private static final Duration IDLE = Duration.ofMillis(5);
  private static final Duration INITIAL_BACKOFF = Duration.ofMillis(10);
  private static final Duration MAX_BACKOFF = Duration.ofMillis(50);

  private ResilientPollLoop loop(AtomicBoolean running) {
    return new ResilientPollLoop(
        "test-loop", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF);
  }

  // ==================== Constructor validation ====================

  @Test
  void constructorRejectsInvalidArguments() {
    AtomicBoolean running = new AtomicBoolean(true);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop(null, running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop("  ", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop("n", null, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop("n", running::get, null, INITIAL_BACKOFF, MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop("n", running::get, () -> IDLE, null, MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ResilientPollLoop(
                "n", running::get, () -> IDLE, Duration.ofMillis(-1), MAX_BACKOFF));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ResilientPollLoop("n", running::get, () -> IDLE, INITIAL_BACKOFF, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ResilientPollLoop(
                "n", running::get, () -> IDLE, Duration.ofSeconds(10), Duration.ofSeconds(1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ResilientPollLoop(
                "n", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF, null));
  }

  // ==================== Loop behavior ====================

  @Test
  void runExitsWhenConditionBecomesFalse() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop = loop(running);

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          if (polls.incrementAndGet() >= 3) {
                            running.set(false);
                          }
                        }));

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
    assertEquals(3, polls.get());
  }

  @Test
  void runSurvivesFailuresAndTracksConsecutiveFailures() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop = loop(running);

    // Fail the first two polls, then succeed and stop.
    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          int n = polls.incrementAndGet();
                          if (n <= 2) {
                            throw new RuntimeException("transient-" + n);
                          }
                          running.set(false);
                        }));

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "loop must not die on poll failures");
    assertEquals(3, polls.get(), "loop must retry after failures");
    assertEquals(0, loop.consecutiveFailures(), "counter resets after successful poll");
    assertFalse(loop.isDegraded());
  }

  @Test
  void consecutiveFailuresGrowsWhileFailing() throws Exception {
    var running = new AtomicBoolean(true);
    var loop = loop(running);

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          throw new RuntimeException("always fails");
                        }));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (loop.consecutiveFailures() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertTrue(
        loop.consecutiveFailures() >= 2, "failures must accumulate while polls keep failing");
    assertTrue(loop.isDegraded());

    running.set(false);
    t.interrupt();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
  }

  @Test
  void runExitsOnInterruptDuringIdleSleep() throws Exception {
    var running = new AtomicBoolean(true);
    var loop =
        new ResilientPollLoop(
            "interruptible",
            running::get,
            () -> Duration.ofSeconds(60),
            INITIAL_BACKOFF,
            MAX_BACKOFF);

    Thread t = Thread.ofVirtual().start(() -> loop.run(() -> {}));
    Thread.sleep(50); // let it enter the long idle sleep
    t.interrupt();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
  }

  @Test
  void runExitsOnInterruptDuringBackoffSleep() throws Exception {
    var running = new AtomicBoolean(true);
    var loop =
        new ResilientPollLoop(
            "interruptible-backoff",
            running::get,
            () -> IDLE,
            Duration.ofSeconds(60),
            Duration.ofSeconds(120));

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          throw new RuntimeException("fail into long backoff");
                        }));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (loop.consecutiveFailures() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    t.interrupt();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
  }

  @Test
  void progressInsideAPoll_clearsTheFailureCount_andLogsTheRecoveryOnce() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop = loop(running);
    var seenAfterProgress = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    var cleared = new java.util.concurrent.CopyOnWriteArrayList<Boolean>();
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ResilientPollLoop.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    ch.qos.logback.classic.Level level = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    try {
      Thread t =
          Thread.ofVirtual()
              .start(
                  () ->
                      loop.run(
                          () -> {
                            if (polls.incrementAndGet() == 1) {
                              throw new RuntimeException("transient");
                            }
                            // A long poll that made progress part of the way through.
                            cleared.add(loop.recordProgress());
                            seenAfterProgress.add(loop.consecutiveFailures());
                            cleared.add(loop.recordProgress());
                            running.set(false);
                          }));
      t.join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(t.isAlive());
    } finally {
      logger.setLevel(level);
      logger.detachAppender(appender);
      appender.stop();
    }

    assertEquals(java.util.List.of(0), seenAfterProgress, "cleared inside the poll");
    assertEquals(java.util.List.of(true, false), cleared, "only the first progress clears it");
    long progress =
        appender.list.stream()
            .filter(
                e ->
                    e.getFormattedMessage()
                        .startsWith("Poll loop 'test-loop' made progress after 1 failed poll(s)"))
            .count();
    assertEquals(1, progress, "the progress is logged once");
    long recoveries =
        appender.list.stream()
            .filter(
                e ->
                    e.getFormattedMessage()
                        .startsWith("Poll loop 'test-loop' recovered after 1 consecutive"))
            .count();
    assertEquals(1, recoveries, "the recovery is logged once, when the poll returns");
  }

  /** Runs {@code task} in {@code loop} on a virtual thread and returns the loop's log lines. */
  private static java.util.List<ch.qos.logback.classic.spi.ILoggingEvent> runLogged(
      ResilientPollLoop loop, ResilientPollLoop.PollTask task) throws InterruptedException {
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ResilientPollLoop.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    ch.qos.logback.classic.Level level = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    try {
      Thread t = Thread.ofVirtual().start(() -> loop.run(task));
      t.join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(t.isAlive(), "the loop must end");
      return java.util.List.copyOf(appender.list);
    } finally {
      logger.setLevel(level);
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  @Test
  void aPollThatReturnsBecauseOfTheStop_isNoRecovery() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop = loop(running);

    // Two failed polls; the third finds the owner's stop before it does any work and returns
    // with the interrupt handed back, as a relay stopped before its claim does.
    var events =
        runLogged(
            loop,
            () -> {
              if (polls.incrementAndGet() <= 2) {
                throw new RuntimeException("database unreachable");
              }
              Thread.currentThread().interrupt();
            });

    assertEquals(3, polls.get());
    assertEquals(2, loop.consecutiveFailures(), "a stopped poll does not clear the count");
    assertTrue(
        events.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains("'test-loop'"))
            .noneMatch(m -> m.contains("recovered") || m.contains("made progress")),
        () -> "a stopped poll is not logged as a recovery: " + events);
  }

  @Test
  void pollsThatMakeProgressAndThenFail_escalateTheCount_andReportEachChange() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var reported = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    var loop =
        new ResilientPollLoop(
            "test-loop", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF, reported::add);

    // Each of the first three polls stores something and then fails; the fourth returns.
    var events =
        runLogged(
            loop,
            () -> {
              if (polls.incrementAndGet() <= 3) {
                loop.recordProgress();
                throw new RuntimeException("failed after progress");
              }
              running.set(false);
            });

    // The backoff follows the failed polls in a row, whatever progress each made before failing.
    var lines =
        events.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .filter(m -> m.startsWith("Poll loop 'test-loop' "))
            .toList();
    var failureLines = lines.stream().filter(m -> m.contains("failed (")).toList();
    assertEquals(3, failureLines.size(), failureLines::toString);
    assertTrue(failureLines.get(0).contains("(1 consecutive failure(s))"), failureLines::toString);
    assertTrue(failureLines.get(1).contains("(2 consecutive failure(s))"), failureLines::toString);
    assertTrue(failureLines.get(2).contains("(3 consecutive failure(s))"), failureLines::toString);
    // Every change of the count reaches the listener: each failure, and each clearing.
    assertEquals(java.util.List.of(1, 0, 2, 0, 3, 0), reported);
    assertEquals(0, loop.consecutiveFailures());
    // The progress inside a failing poll is not called a recovery: the run of failures goes on.
    var progressLines = lines.stream().filter(m -> m.contains("made progress after")).toList();
    // The first poll had no failure before it to clear.
    assertEquals(2, progressLines.size(), lines::toString);
    assertTrue(progressLines.get(0).contains("after 1 failed poll(s) in a row"), lines::toString);
    assertTrue(progressLines.get(1).contains("after 2 failed poll(s) in a row"), lines::toString);
    var recoveries = lines.stream().filter(m -> m.contains("recovered after")).toList();
    assertEquals(
        java.util.List.of("Poll loop 'test-loop' recovered after 3 consecutive failure(s)"),
        recoveries);
  }

  @Test
  void aListenerThatThrows_isLogged_andTheLoopKeepsCountingAndRetrying() throws Exception {
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop =
        new ResilientPollLoop(
            "test-loop",
            running::get,
            () -> IDLE,
            INITIAL_BACKOFF,
            MAX_BACKOFF,
            count -> {
              throw new IllegalStateException("gauge backend down");
            });

    var events =
        runLogged(
            loop,
            () -> {
              if (polls.incrementAndGet() <= 2) {
                throw new RuntimeException("database unreachable");
              }
              running.set(false);
            });

    assertEquals(3, polls.get(), "the loop survives the listener and retries");
    assertEquals(0, loop.consecutiveFailures());
    var lines =
        events.stream()
            .filter(e -> e.getFormattedMessage().startsWith("Poll loop 'test-loop' "))
            .toList();
    // The polls' own failures are still logged, with the poll's exception.
    var failures =
        lines.stream().filter(e -> e.getLevel() == ch.qos.logback.classic.Level.ERROR).toList();
    assertEquals(2, failures.size(), lines::toString);
    assertTrue(failures.get(1).getFormattedMessage().contains("(2 consecutive failure(s))"));
    assertEquals("database unreachable", failures.get(1).getThrowableProxy().getMessage());
    // Each report the listener refused: 1, 2, and the clearing to 0.
    var refused =
        lines.stream()
            .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .toList();
    assertEquals(3, refused.size(), lines::toString);
    assertTrue(refused.get(0).contains("could not report its failure count 1"), refused::toString);
    assertTrue(refused.get(2).contains("could not report its failure count 0"), refused::toString);
  }

  @Test
  void aNewRun_clearsTheCountItInherits_andReportsIt() throws Exception {
    var running = new AtomicBoolean(true);
    var reported = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
    var loop =
        new ResilientPollLoop(
            "test-loop", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF, reported::add);
    // The first run ends on a failure that the stop cut short: its count stays.
    runLogged(
        loop,
        () -> {
          running.set(false);
          throw new RuntimeException("cut short");
        });
    assertEquals(1, loop.consecutiveFailures());

    loop.run(() -> {}); // condition still false: the run only clears what it inherited

    assertEquals(0, loop.consecutiveFailures());
    assertEquals(java.util.List.of(1, 0), reported);
  }

  @Test
  void resetsFailureCounterAtStartOfRun() {
    var running = new AtomicBoolean(false); // condition false — run() returns immediately
    var loop = loop(running);
    loop.run(() -> {});
    assertEquals(0, loop.consecutiveFailures());
  }

  @Test
  void benignSignalIsLoggedQuietlyButDoesNotAlterLoopControlFlow() throws Exception {
    // A BenignSignal (e.g. leadership loss) is logged at INFO without a stacktrace instead of
    // ERROR,
    // but its control flow is deliberately identical to any other cycle failure: the loop survives
    // it, counts it, backs off, then continues. This guards the log-level branch from accidentally
    // changing the loop's behavior.
    var running = new AtomicBoolean(true);
    var polls = new AtomicInteger();
    var loop = loop(running);

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          int n = polls.incrementAndGet();
                          if (n <= 2) {
                            throw new BenignSignalException("standing by " + n);
                          }
                          running.set(false);
                        }));

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "loop must not die on a benign signal");
    assertEquals(
        3, polls.get(), "loop must keep polling after a benign signal, just like a failure");
    assertEquals(0, loop.consecutiveFailures(), "counter still resets after a successful poll");
  }

  /** A minimal {@link ResilientPollLoop.BenignSignal} for the log-branch control-flow test. */
  private static final class BenignSignalException extends RuntimeException
      implements ResilientPollLoop.BenignSignal {
    BenignSignalException(String message) {
      super(message);
    }
  }

  // ==================== keepRunning already false must skip the backoff sleep =========

  @Test
  void skipsTheBackoffSleepWhenKeepRunningIsAlreadyFalse() throws Exception {
    // StreamEventSubscription.onReadFailure sets running=false THEN throws the terminal
    // read-poison exception (running is this loop's keepRunning). Pre-fix, the catch block below
    // slept the FULL computed backoff (initial x 2^(failures-1), capped at maxBackoff — up to
    // max(pollingInterval, 60s) in production) before the while loop ever rechecked keepRunning, so
    // the owning thread stayed alive for that whole window. start()'s "Previous polling thread is
    // still running" guard then refused a restart for the same window, contradicting the
    // documented remedy ("repair the event, then start() the subscription again"). The loop must
    // notice keepRunning is already false and skip the sleep instead of waiting it out — this
    // helps every ResilientPollLoop consumer whose task both stops the loop and throws, not only
    // the read-poison bound.
    var running = new AtomicBoolean(true);
    var loop =
        new ResilientPollLoop(
            "stop-and-throw",
            running::get,
            () -> IDLE,
            Duration.ofSeconds(60), // a huge backoff: the test fails loudly if it is not skipped
            Duration.ofSeconds(120));

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    loop.run(
                        () -> {
                          running.set(false); // e.g. onReadFailure's terminal stop
                          throw new RuntimeException("terminal poison");
                        }));

    t.join(TimeUnit.SECONDS.toMillis(1));
    assertFalse(
        t.isAlive(),
        "the loop must exit promptly once keepRunning is false, not sleep out a 60s backoff first");
  }

  // ==================== Backoff computation ====================

  @Test
  void backoffDelayGrowsExponentiallyAndIsCapped() {
    Duration initial = Duration.ofMillis(100);
    Duration max = Duration.ofMillis(400);

    // Jitter is uniform in [computed/2, computed]; assert bounds per failure count.
    for (int i = 0; i < 20; i++) {
      Duration first = ResilientPollLoop.backoffDelay(initial, max, 1);
      assertTrue(first.toMillis() >= 50 && first.toMillis() <= 100, "failure 1: " + first);

      Duration second = ResilientPollLoop.backoffDelay(initial, max, 2);
      assertTrue(second.toMillis() >= 100 && second.toMillis() <= 200, "failure 2: " + second);

      Duration big = ResilientPollLoop.backoffDelay(initial, max, 10);
      assertTrue(big.toMillis() >= 200 && big.toMillis() <= 400, "capped: " + big);

      // Very large failure counts must not overflow and stay capped.
      Duration huge = ResilientPollLoop.backoffDelay(initial, max, Integer.MAX_VALUE);
      assertTrue(huge.toMillis() >= 200 && huge.toMillis() <= 400, "huge: " + huge);
    }
  }

  @Test
  void backoffDelayFloorsTinyInitialBackoff() {
    Duration tiny = ResilientPollLoop.backoffDelay(Duration.ZERO, Duration.ofSeconds(1), 1);
    assertTrue(tiny.toMillis() >= 5, "floored initial backoff must not be zero: " + tiny);
  }

  @Test
  void backoffDelayRejectsNonPositiveFailureCount() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ResilientPollLoop.backoffDelay(INITIAL_BACKOFF, MAX_BACKOFF, 0));
  }

  // ==================== An Error must not kill the loop silently ====================

  @Test
  void errorTerminatesTheLoopWithANamedErrorLogAndIsRethrown() {
    // The loop caught only RuntimeException, so a java.lang.Error (AssertionError,
    // LinkageError from a missing optional dependency, StackOverflowError, OutOfMemoryError) killed
    // the owning virtual thread with no log line naming the loop — an owner without its own
    // liveness accessor (the four retention sweepers) froze silently, every gauge it fed holding
    // its last value. The loop must log the death BY NAME and rethrow, so the owner's own handling
    // (PollingEventSubscription's own catch) still runs and a dead thread stays observable.
    var running = new AtomicBoolean(true);
    var loop = loop(running);
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ResilientPollLoop.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      assertThrows(
          AssertionError.class,
          () ->
              loop.run(
                  () -> {
                    throw new AssertionError("boom — not retryable");
                  }),
          "an Error is not retryable: it must terminate the loop by propagating to the owner");
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.ERROR
                          && e.getFormattedMessage().contains("test-loop")
                          && e.getFormattedMessage().contains("unrecoverable error")),
          "the loop must log its own death by name before rethrowing");
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  // ==================== The ERROR log must not promise a retry it will not do ============

  @Test
  void errorLogSaysStoppingNotRetrying_whenKeepRunningIsAlreadyFalse() {
    // keepRunning may already be false by the time a poll task's RuntimeException lands in
    // the loop's catch block — e.g. a terminal read-poison signal sets it before throwing (
    // see StreamEventSubscription.onReadFailure). The ERROR line used to unconditionally say
    // "retrying in N ms" on exactly this path, immediately before the loop returned WITHOUT
    // retrying.
    var running = new AtomicBoolean(true);
    var loop = loop(running);
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ResilientPollLoop.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      loop.run(
          () -> {
            running.set(false); // simulates e.g. onReadFailure setting keepRunning false first
            throw new RuntimeException("boom — the loop is about to stop, not retry");
          });
      assertTrue(
          appender.list.stream()
              .anyMatch(
                  e ->
                      e.getLevel() == ch.qos.logback.classic.Level.ERROR
                          && e.getFormattedMessage().contains("test-loop")
                          && e.getFormattedMessage().contains("stopping")
                          && !e.getFormattedMessage().contains("retrying in")),
          "the loop must not claim it will retry when keepRunning is already false");
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  // ==================== A call the stop's own interrupt cut short is no failure ============

  /** Runs the loop on a thread whose task blocks until the owner's stop interrupts it. */
  private static java.util.List<ch.qos.logback.classic.spi.ILoggingEvent> stopDuringABlockedCall(
      java.util.function.Function<InterruptedException, RuntimeException> cutShort,
      boolean flagStaysSet)
      throws InterruptedException {
    var running = new AtomicBoolean(true);
    var loop =
        new ResilientPollLoop("test-loop", running::get, () -> IDLE, INITIAL_BACKOFF, MAX_BACKOFF);
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ResilientPollLoop.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    // The test configuration passes WARN and above; the line under test is an INFO.
    ch.qos.logback.classic.Level level = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    var inTheCall = new java.util.concurrent.CountDownLatch(1);
    try {
      Thread thread =
          Thread.ofVirtual()
              .start(
                  () ->
                      loop.run(
                          () -> {
                            inTheCall.countDown();
                            try {
                              Thread.sleep(Duration.ofSeconds(30));
                            } catch (InterruptedException interrupted) {
                              if (flagStaysSet) {
                                Thread.currentThread().interrupt();
                              }
                              throw cutShort.apply(interrupted);
                            }
                          }));
      assertTrue(inTheCall.await(5, TimeUnit.SECONDS), "the call must start");
      running.set(false);
      thread.interrupt();
      thread.join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(thread.isAlive(), "the loop must stop");
      return java.util.List.copyOf(appender.list);
    } finally {
      logger.setLevel(level);
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  private static void assertOneInfoWithoutAStack(
      java.util.List<ch.qos.logback.classic.spi.ILoggingEvent> events) {
    assertTrue(
        events.stream().noneMatch(e -> e.getLevel() == ch.qos.logback.classic.Level.ERROR),
        () -> "a call the stop cut short is no failure: " + events);
    var stopped =
        events.stream()
            .filter(
                e ->
                    e.getLevel() == ch.qos.logback.classic.Level.INFO
                        && e.getFormattedMessage().contains("test-loop"))
            .toList();
    assertEquals(1, stopped.size(), () -> "one INFO line names the loop: " + events);
    assertNull(stopped.getFirst().getThrowableProxy(), "the INFO line carries no stack trace");
    assertTrue(
        stopped.getFirst().getFormattedMessage().contains("socket closed by the interrupt")
            || stopped.getFirst().getFormattedMessage().contains("cut short"),
        () -> "the INFO line says what the stop cut short: " + stopped);
    assertTrue(
        stopped
            .getFirst()
            .getFormattedMessage()
            .contains("ended: the owner's interrupt cut a call in flight short"),
        () -> "the INFO line fits a stop and a pause alike: " + stopped);
  }

  @Test
  void aCallTheStopsInterruptCutShort_isLoggedAtInfoWithoutAStack() throws Exception {
    // A JDBC read on a virtual thread: the interrupt closes the socket, the driver throws, and the
    // thread's interrupt status stays set.
    var events =
        stopDuringABlockedCall(
            interrupted ->
                new IllegalStateException(
                    "I/O error", new java.net.SocketException("socket closed by the interrupt")),
            true);

    assertOneInfoWithoutAStack(events);
  }

  @Test
  void aCallTheStopCutShort_whoseFlagWasConsumed_isKnownByItsCause() throws Exception {
    // A driver that consumed the flag but kept the cause: an interruption in the cause chain.
    var events =
        stopDuringABlockedCall(
            interrupted -> new IllegalStateException("read cut short", interrupted), false);

    assertOneInfoWithoutAStack(events);
  }
}

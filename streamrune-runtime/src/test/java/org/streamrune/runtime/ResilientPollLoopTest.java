package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

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
}

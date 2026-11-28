package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The lifecycle every retention sweeper delegates to: the started guard, the named sweep thread,
 * liveness, and a close that ends the thread even when the sweep swallows its interrupt or calls
 * close from the sweep itself.
 */
class RetentionSweepLoopTest {

  private static final Duration SHORT = Duration.ofMillis(10);
  private static final Duration LONG = Duration.ofHours(1);
  private static final Duration WAIT = Duration.ofSeconds(5);

  /** Every loop a test created; closed after each test so no sweep thread outlives its test. */
  private final List<RetentionSweepLoop> loops = new ArrayList<>();

  private RetentionSweepLoop loop(Duration interval, ResilientPollLoop.PollTask sweep) {
    var loop = new RetentionSweepLoop("TestSweeper", "test-retention-sweeper", interval, sweep);
    loops.add(loop);
    return loop;
  }

  @AfterEach
  void closeEveryLoop() {
    loops.forEach(RetentionSweepLoop::close);
  }

  /** Polls every few milliseconds; a sweep thread's own uncaught error must not fail the wait. */
  private static void awaitTrue(String what, BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).as(what).isTrue();
  }

  @Test
  void aDisabledStartLaunchesNoThreadButHoldsTheGuardUntilClose() {
    var sweeps = new AtomicInteger();
    var loop = loop(LONG, sweeps::incrementAndGet);

    assertThat(loop.start(false)).isFalse();

    assertThat(loop.isStarted()).isFalse();
    assertThat(loop.isAlive()).isFalse();
    assertThatThrownBy(() -> loop.start(false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("TestSweeper already started — call start() exactly once");
    loop.close();
    assertThat(loop.start(false)).as("restartable after close").isFalse();
    loop.close();
    assertThat(sweeps).hasValue(0);
  }

  @Test
  void anEnabledStartRunsSweepsOnTheNamedThreadAndCloseEndsIt() throws Exception {
    var threadName = new AtomicReference<String>();
    var swept = new CountDownLatch(2);
    var loop =
        loop(
            SHORT,
            () -> {
              threadName.set(Thread.currentThread().getName());
              swept.countDown();
            });

    assertThat(loop.start(true)).isTrue();

    assertThat(swept.await(5, TimeUnit.SECONDS)).as("swept every interval").isTrue();
    assertThat(threadName).hasValue("test-retention-sweeper");
    assertThat(loop.isStarted()).isTrue();
    assertThat(loop.isAlive()).isTrue();
    assertThatThrownBy(() -> loop.start(true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("TestSweeper already started — call start() exactly once");

    loop.close();

    assertThat(loop.isStarted()).isFalse();
    assertThat(loop.isAlive()).isFalse();
    assertThat(loop.start(true)).as("restartable after close").isTrue();
    loop.close();
  }

  @Test
  void failedSweepsAreCountedAndAReturnedSweepClearsTheCount() throws Exception {
    var failing = new AtomicBoolean(true);
    var calls = new AtomicInteger();
    var loop =
        loop(
            SHORT,
            () -> {
              calls.incrementAndGet();
              if (failing.get()) {
                throw new IllegalStateException("simulated store outage");
              }
            });
    loop.start(true);

    awaitTrue("a failed sweep is counted", () -> loop.consecutiveFailures() > 0);
    assertThat(loop.isAlive()).as("a RuntimeException never ends the thread").isTrue();
    failing.set(false);
    int callsBeforeRecovery = calls.get();
    awaitTrue(
        "a returned sweep clears the count",
        () -> calls.get() > callsBeforeRecovery && loop.consecutiveFailures() == 0);
  }

  @Test
  void anErrorEndsTheThreadWhichThenReadsStartedButNotAlive() throws Exception {
    var loop =
        loop(
            SHORT,
            () -> {
              throw new AssertionError("simulated unrecoverable error in the sweep");
            });
    loop.start(true);

    awaitTrue("the thread ended", () -> !loop.isAlive());

    assertThat(loop.isStarted()).isTrue();
    loop.close();
    assertThat(loop.isStarted()).isFalse();
  }

  @Test
  void closeEndsTheLoopWhenTheSweepSwallowsTheInterrupt() throws Exception {
    var entered = new CountDownLatch(1);
    var sweeps = new AtomicInteger();
    var loop =
        loop(
            LONG,
            () -> {
              sweeps.incrementAndGet();
              entered.countDown();
              try {
                Thread.sleep(WAIT);
              } catch (InterruptedException _) {
                // A store that catches the interruption and returns normally.
              }
            });
    loop.closeJoinTimeoutMs(WAIT.toMillis());
    loop.start(true);
    assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

    long before = System.nanoTime();
    loop.close();

    assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(WAIT);
    assertThat(loop.isAlive()).isFalse();
    assertThat(sweeps).as("no second sweep after the close").hasValue(1);
  }

  @Test
  void closeFromTheSweepItselfReturnsAtOnceAndEndsTheLoop() throws Exception {
    var loopRef = new AtomicReference<RetentionSweepLoop>();
    var closedFromSweep = new CountDownLatch(1);
    var sweeps = new AtomicInteger();
    var loop =
        loop(
            SHORT,
            () -> {
              sweeps.incrementAndGet();
              loopRef.get().close();
              closedFromSweep.countDown();
            });
    loopRef.set(loop);
    loop.start(true);

    assertThat(closedFromSweep.await(5, TimeUnit.SECONDS)).isTrue();
    awaitTrue("the loop closed itself", () -> !loop.isStarted());
    assertThat(sweeps).hasValue(1);
  }

  @Test
  void aSecondCloseWhileTheFirstWaitsReturnsAtOnce() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var loop =
        loop(
            LONG,
            () -> {
              entered.countDown();
              boolean interrupted = false;
              while (true) {
                try {
                  if (release.await(10, TimeUnit.SECONDS)) {
                    break;
                  }
                } catch (InterruptedException _) {
                  interrupted = true;
                }
              }
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            });
    loop.start(true);
    try {
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      var firstReturned = new AtomicBoolean();
      Thread first =
          Thread.ofVirtual()
              .start(
                  () -> {
                    loop.close();
                    firstReturned.set(true);
                  });
      awaitTrue(
          "the first close waits for the sweep",
          () ->
              first.getState() == Thread.State.TIMED_WAITING
                  || first.getState() == Thread.State.WAITING);

      loop.close();

      assertThat(firstReturned).as("the first close still waits for the sweep").isFalse();
      release.countDown();
      first.join(WAIT.toMillis());
      assertThat(firstReturned).isTrue();
    } finally {
      // The sweep ignores interrupts until released: release it so the close after this test ends.
      release.countDown();
    }
  }

  @Test
  void anIntervalLongerThanTheBackoffCapStillConstructsAndStarts() {
    var loop = loop(RetentionSweepLoop.MAX_FAILURE_BACKOFF.multipliedBy(2), () -> {});

    assertThat(loop.start(true)).isTrue();
    assertThat(loop.consecutiveFailures()).isZero();
    loop.close();
    assertThat(loop.isAlive()).isFalse();
  }
}

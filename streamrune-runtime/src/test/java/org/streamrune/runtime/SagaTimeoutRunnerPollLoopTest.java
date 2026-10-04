package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaTimeoutRunnerTest.CapturingCommandBus;
import org.streamrune.runtime.SagaTimeoutRunnerTest.OrderState;
import org.streamrune.runtime.SagaTimeoutRunnerTest.TimeoutDecider;
import org.streamrune.test.InMemorySagaStore;

/**
 * The timeout runner's poll loop: a poll that keeps failing is retried with capped exponential
 * backoff like every other background loop, never in a tight loop, and a decider that can never be
 * polled is refused when the runner is built.
 */
class SagaTimeoutRunnerPollLoopTest {

  private static final SagaType ORDER = SagaType.fromClass(OrderState.class);

  /** A decider that declares no timeout: the runner has nothing to compare a saga's age against. */
  static class NoTimeoutDecider extends TimeoutDecider {
    @Override
    public Optional<Duration> timeout() {
      return Optional.empty();
    }
  }

  private static SagaTimeoutRunner.Builder<OrderState> builder(SagaStore store, Duration interval) {
    return SagaTimeoutRunner.<OrderState>builder()
        .decider(new TimeoutDecider())
        .sagaStore(store)
        .commandBus(new CapturingCommandBus())
        .sagaType(ORDER)
        .pollInterval(interval);
  }

  private static void awaitTrue(java.util.function.BooleanSupplier condition, String what)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(condition.getAsBoolean(), "timed out waiting for: " + what);
  }

  @Test
  void aFailingPollIsRetriedAfterABackoff_notImmediately() throws Exception {
    List<Long> attemptNanos = new CopyOnWriteArrayList<>();
    SagaStore failingStore = mock(SagaStore.class);
    when(failingStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              attemptNanos.add(System.nanoTime());
              throw new IllegalStateException("saga store unreachable");
            });
    // Initial backoff = poll interval (400 ms), jittered into [200 ms, 400 ms], then doubled.
    var runner = builder(failingStore, Duration.ofMillis(400)).build();

    runner.start();
    try {
      awaitTrue(() -> attemptNanos.size() >= 3, "three poll attempts");
      long firstGapMs = TimeUnit.NANOSECONDS.toMillis(attemptNanos.get(1) - attemptNanos.get(0));
      long secondGapMs = TimeUnit.NANOSECONDS.toMillis(attemptNanos.get(2) - attemptNanos.get(1));
      assertTrue(
          firstGapMs >= 150,
          "the retry after the first failure must wait out a backoff, but came after "
              + firstGapMs
              + " ms");
      assertTrue(
          secondGapMs >= 300,
          "the backoff must grow with consecutive failures; the second retry came after "
              + secondGapMs
              + " ms");
      assertTrue(runner.isAlive(), "a failing poll must not kill the poll thread");
      assertTrue(runner.consecutiveFailures() >= 2, "the failures are counted for health");
    } finally {
      runner.stop();
    }
  }

  @Test
  void aStoreThatKeepsFailingIsNotQueriedInATightLoop() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    SagaStore failingStore = mock(SagaStore.class);
    when(failingStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("permission denied for table saga_state");
            });
    var runner = builder(failingStore, Duration.ofMillis(50)).build();

    runner.start();
    try {
      Thread.sleep(1_000);
    } finally {
      runner.stop();
    }

    // Backoff 50, 100, 200, 400, 800 ms (jittered down to half): at most ~8 attempts in a second.
    // Without a backoff the loop spins thousands of times.
    assertTrue(
        attempts.get() < 25, "a failing poll must back off, but the store was queried " + attempts);
  }

  @Test
  void theRunnerRecoversAndResetsItsFailureCountOnceThePollSucceeds() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    SagaStore flakyStore = mock(SagaStore.class);
    when(flakyStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("transient");
              }
              return List.of();
            });
    var runner = builder(flakyStore, Duration.ofMillis(20)).build();

    runner.start();
    try {
      awaitTrue(() -> calls.get() >= 4 && runner.consecutiveFailures() == 0, "recovery");
      assertTrue(runner.isAlive());
    } finally {
      runner.stop();
    }
  }

  @Test
  void stopReturnsPromptlyWhileThePollLoopIsSleepingOffABackoff() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    SagaStore failingStore = mock(SagaStore.class);
    when(failingStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("down");
            });
    // A 30 s poll interval means a 15-30 s backoff sleep after the first failure.
    var runner = builder(failingStore, Duration.ofSeconds(30)).build();

    runner.start();
    awaitTrue(() -> attempts.get() >= 1 && runner.consecutiveFailures() >= 1, "first failure");

    long startNanos = System.nanoTime();
    runner.stop();
    long stopMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

    assertTrue(stopMs < 5_000, "stop() must interrupt the backoff sleep, took " + stopMs + " ms");
    assertFalse(runner.isStarted(), "stop() resets the started guard");
    assertFalse(runner.isAlive());
  }

  @Test
  void anErrorOutOfThePollEndsTheThreadSoTheHealthCheckReportsItDead() throws Exception {
    SagaStore brokenStore = mock(SagaStore.class);
    when(brokenStore.findTimedOut(any(), any(), anyInt()))
        .thenThrow(new NoClassDefFoundError("optional dependency missing"));
    var runner = builder(brokenStore, Duration.ofMillis(20)).build();

    runner.start();
    try {
      awaitTrue(() -> !runner.isAlive(), "the poll thread to die");
      assertTrue(runner.isStarted(), "started && !alive is the dead-driver signal");
    } finally {
      runner.stop();
    }
  }

  @Test
  void aRestartedRunnerStartsWithAFreshFailureCount() throws Exception {
    SagaStore failingStore = mock(SagaStore.class);
    AtomicInteger calls = new AtomicInteger();
    when(failingStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("down");
              }
              return List.of();
            });
    var runner = builder(failingStore, Duration.ofMillis(20)).build();

    runner.start();
    awaitTrue(() -> calls.get() >= 4 && runner.consecutiveFailures() == 0, "recovery");
    runner.stop();

    assertEquals(0, runner.consecutiveFailures());
  }

  @Test
  void buildRefusesADeciderWithoutATimeout() {
    var builder =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new NoTimeoutDecider())
            .sagaStore(new InMemorySagaStore())
            .commandBus(new CapturingCommandBus())
            .sagaType(ORDER);

    var e = assertThrows(IllegalArgumentException.class, builder::build);

    assertTrue(
        e.getMessage().contains("timeout()"),
        "the message must name the decider's timeout(): " + e.getMessage());
    assertTrue(
        e.getMessage().contains("SagaCompensationRetrySweeper"),
        "the message must say what recovers such a saga instead: " + e.getMessage());
  }

  @Test
  void buildRefusesAPollIntervalThatIsNullZeroOrNegative() {
    for (Duration bad : new Duration[] {null, Duration.ZERO, Duration.ofSeconds(-1)}) {
      var builder = builder(new InMemorySagaStore(), bad);

      var e = assertThrows(IllegalArgumentException.class, builder::build, String.valueOf(bad));

      assertTrue(e.getMessage().contains("pollInterval"), e.getMessage());
    }
  }
}

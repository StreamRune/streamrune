package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;

/**
 * The four retention sweepers expose liveness ({@link RetentionSweeper}) and register with {@link
 * BackgroundRelayHealthContributor}, so a sweeper whose thread died on an {@link Error} is reported
 * DOWN instead of silently freezing — with every gauge it feeds (the saga faulted backlog is
 * sampled ONLY by the saga dead-letter sweeper) holding its last value forever.
 */
class RetentionSweeperLivenessTest {

  private static final Duration TICK = Duration.ofMillis(10);
  private static final Duration ENABLED = Duration.ofDays(7);
  private static final Clock CLOCK = Clock.systemUTC();

  private static Error kill() {
    return new AssertionError("simulated unrecoverable error in the sweep");
  }

  private static RuntimeException blip() {
    return new IllegalStateException("simulated store blip");
  }

  private static boolean await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    return condition.getAsBoolean();
  }

  /**
   * The contract every sweeper must honour: retention disabled → never "started" for health (UP); a
   * sweep that keeps failing with a RuntimeException → alive but DEGRADED; a sweep that throws an
   * Error → thread dead, {@code isStarted() && !isAlive()} → DOWN; closed → no longer reported
   * dead.
   */
  private static void assertLivenessContract(
      String expectedName,
      RetentionSweeper disabled,
      RetentionSweeper degraded,
      RetentionSweeper dying)
      throws InterruptedException {
    var health = new BackgroundRelayHealthContributor();
    health.registerRetentionSweeper(disabled);
    health.registerRetentionSweeper(degraded);
    health.registerRetentionSweeper(dying);
    assertEquals(3, health.components().size());
    assertTrue(
        health.components().stream().allMatch(c -> c.name().equals(expectedName)),
        "components carry the sweeper's own name");
    assertEquals(Status.UP, health.overallStatus(), "nothing started yet");
    try {
      disabled.start();
      assertFalse(
          disabled.isStarted(),
          "retention disabled launches no thread — such a sweeper is not 'started' for health");
      assertFalse(disabled.isAlive());
      assertEquals(Status.UP, health.overallStatus(), "a disabled sweeper is not a dead one");

      degraded.start();
      assertTrue(await(() -> degraded.consecutiveFailures() >= 1), "a failing sweep is counted");
      assertTrue(degraded.isStarted());
      assertTrue(
          degraded.isAlive(), "a RuntimeException is retried with backoff — the thread lives");
      assertEquals(Status.DEGRADED, health.overallStatus());

      dying.start();
      assertTrue(
          await(() -> dying.isStarted() && !dying.isAlive()),
          "an Error kills the sweep thread, and the liveness accessors must say so");
      assertEquals(
          Status.DOWN,
          health.overallStatus(),
          "a started sweeper whose thread died is DOWN — it used to be invisible");

      dying.close();
      assertFalse(dying.isStarted(), "a closed sweeper is not reported dead");
      assertEquals(
          Status.DEGRADED, health.overallStatus(), "only the still-failing sweeper remains");
    } finally {
      disabled.close();
      degraded.close();
      dying.close();
    }
  }

  @Test
  void deadLetterRetentionSweeper_exposesLiveness() throws InterruptedException {
    var disabledQueue = mock(DeadLetterQueue.class);
    var degradedQueue = mock(DeadLetterQueue.class);
    when(degradedQueue.deleteOlderThan(any())).thenThrow(blip());
    var dyingQueue = mock(DeadLetterQueue.class);
    when(dyingQueue.deleteOlderThan(any())).thenThrow(kill());
    assertLivenessContract(
        "dead-letter-retention-sweeper",
        new DeadLetterRetentionSweeper(
            disabledQueue, Duration.ZERO, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new DeadLetterRetentionSweeper(degradedQueue, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new DeadLetterRetentionSweeper(dyingQueue, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP));
  }

  @Test
  void inboxRetentionSweeper_exposesLiveness() throws InterruptedException {
    var disabledInbox = mock(CommandInbox.class);
    var degradedInbox = mock(CommandInbox.class);
    when(degradedInbox.deleteProcessedBefore(any())).thenThrow(blip());
    var dyingInbox = mock(CommandInbox.class);
    when(dyingInbox.deleteProcessedBefore(any())).thenThrow(kill());
    assertLivenessContract(
        "inbox-retention-sweeper",
        new InboxRetentionSweeper(
            disabledInbox, Duration.ZERO, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new InboxRetentionSweeper(degradedInbox, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new InboxRetentionSweeper(dyingInbox, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP));
  }

  @Test
  void outboxRetentionSweeper_exposesLiveness() throws InterruptedException {
    var disabledStore = mock(OutboxStore.class);
    var degradedStore = mock(OutboxStore.class);
    when(degradedStore.deleteDelivered(any())).thenThrow(blip());
    var dyingStore = mock(OutboxStore.class);
    when(dyingStore.deleteDelivered(any())).thenThrow(kill());
    assertLivenessContract(
        "outbox-retention-sweeper",
        new OutboxRetentionSweeper(
            disabledStore, Duration.ZERO, Duration.ZERO, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new OutboxRetentionSweeper(
            degradedStore, ENABLED, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new OutboxRetentionSweeper(
            dyingStore, ENABLED, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP));
  }

  @Test
  void sagaDeadLetterRetentionSweeper_exposesLiveness() throws InterruptedException {
    var disabledStore = mock(SagaDeadLetterStore.class);
    var degradedStore = mock(SagaDeadLetterStore.class);
    when(degradedStore.deleteOlderThan(any())).thenThrow(blip());
    var dyingStore = mock(SagaDeadLetterStore.class);
    when(dyingStore.deleteOlderThan(any())).thenThrow(kill());
    assertLivenessContract(
        "saga-dead-letter-retention-sweeper",
        new SagaDeadLetterRetentionSweeper(
            disabledStore, Duration.ZERO, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new SagaDeadLetterRetentionSweeper(
            degradedStore, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        new SagaDeadLetterRetentionSweeper(
            dyingStore, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP));
  }
}

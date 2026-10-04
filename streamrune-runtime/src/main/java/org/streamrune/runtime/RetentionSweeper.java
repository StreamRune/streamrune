package org.streamrune.runtime;

/**
 * Lifecycle and liveness view of a background retention sweeper, implemented by {@link
 * DeadLetterRetentionSweeper}, {@link InboxRetentionSweeper}, {@link OutboxRetentionSweeper} and
 * {@link SagaDeadLetterRetentionSweeper}, and consumed by {@link
 * BackgroundRelayHealthContributor#registerRetentionSweeper(RetentionSweeper)}.
 *
 * <p>Each sweeper runs one virtual thread through a {@link ResilientPollLoop}, which retries a
 * {@link RuntimeException} with backoff but cannot retry an {@link Error}: the thread dies. Until
 * this view existed the sweepers exposed no liveness at all, so a dead sweeper was invisible — its
 * table grew unbounded and any gauge it fed ({@code streamrune.saga.faulted_backlog} is sampled
 * only by the saga dead-letter sweeper) froze at its last value while {@code /health} stayed UP.
 * The three accessors are the same trio the outbox relay, the DLQ retry runner and the saga drivers
 * already expose, mapped the same way: started-but-not-alive is DOWN, alive with consecutive
 * failures is DEGRADED, otherwise UP.
 */
public interface RetentionSweeper extends AutoCloseable {

  /** Stable component name, e.g. {@code outbox-retention-sweeper}; also the sweep thread's name. */
  String name();

  /** Starts the sweep thread; a no-op (no thread) when the sweeper's retention is disabled. */
  void start();

  /** Stops the sweep thread. Idempotent. */
  @Override
  void close();

  /**
   * Whether {@link #start()} launched a sweep thread that {@link #close()} has not yet stopped.
   * {@code false} when retention is disabled — such a sweeper never had a thread and must not read
   * as a dead one.
   */
  boolean isStarted();

  /** Whether the sweep thread is currently alive; {@code false} once it died or was closed. */
  boolean isAlive();

  /** Consecutive failed sweep cycles; {@code 0} when the last sweep succeeded. */
  int consecutiveFailures();
}

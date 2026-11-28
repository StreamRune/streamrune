package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.runtime.health.RelayComponentNames;

/**
 * Background sweeper that prunes {@code dead_letter_queue} rows published before a configured
 * retention window.
 *
 * <p>Runs on a virtual thread ({@link Thread#ofVirtual()}). Call {@link #start()} to begin sweeping
 * and {@link #close()} to stop the thread.
 *
 * <p>If {@code maxAge} is zero or negative, retention is disabled: {@link #start()} is a no-op and
 * {@link DeadLetterQueue#deleteOlderThan} is never called.
 *
 * <p>A command dead-letter entry stores the full command payload — which is encrypted when it
 * carries {@code @Encrypted} fields, but still represents bounded PII exposure while it sits
 * queryable in the table. Its retention window is a GDPR storage-limitation concern: choose a
 * window comfortably long enough for operators to notice, investigate, and replay or discard
 * quarantined entries, mirroring the saga dead-letter retention window (see {@link
 * SagaDeadLetterRetentionSweeper}).
 *
 * <p><b>Resilience:</b> a failure of the sweep cycle is logged and retried with capped exponential
 * backoff — it never silently kills the sweeper thread. An {@link Error} does end the thread; it is
 * logged by the poll loop, and this sweeper's {@link RetentionSweeper} liveness reports it DOWN
 * through {@link BackgroundRelayHealthContributor#registerRetentionSweeper}.
 *
 * <p><b>Testing:</b> {@link #sweepOnce()} is package-private to allow tests to invoke one sweep
 * cycle synchronously without starting a background thread.
 */
public final class DeadLetterRetentionSweeper implements RetentionSweeper {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterRetentionSweeper.class);

  /** Component and thread name; also what {@link #name()} reports. */
  private static final String NAME = RelayComponentNames.DEAD_LETTER_RETENTION_SWEEPER;

  private final DeadLetterQueue deadLetterQueue;
  private final Duration maxAge;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final RetentionSweepLoop loop;

  /**
   * Creates a retention sweeper.
   *
   * @param deadLetterQueue the command dead-letter queue to prune (required)
   * @param maxAge how far back to keep dead-letter entries; zero or negative disables retention
   * @param interval how often to sweep; must be positive
   * @param clock the clock the cutoff instant is computed from (required)
   * @param metrics metrics collector (required; pass {@link StreamRuneMetrics#NOOP} to disable)
   */
  public DeadLetterRetentionSweeper(
      DeadLetterQueue deadLetterQueue,
      Duration maxAge,
      Duration interval,
      Clock clock,
      StreamRuneMetrics metrics) {
    Objects.requireNonNull(deadLetterQueue, "deadLetterQueue is required");
    Objects.requireNonNull(maxAge, "maxAge is required");
    Objects.requireNonNull(interval, "interval is required");
    Objects.requireNonNull(clock, "clock is required");
    Objects.requireNonNull(metrics, "metrics is required");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException(
          "interval must be positive (Duration.ZERO causes a tight spin loop)");
    }
    this.deadLetterQueue = deadLetterQueue;
    this.maxAge = maxAge;
    this.clock = clock;
    this.metrics = metrics;
    this.loop =
        new RetentionSweepLoop(
            DeadLetterRetentionSweeper.class.getSimpleName(), NAME, interval, this::sweepOnce);
  }

  /**
   * Starts the background virtual thread. start()/close() pairs are repeatable; may be restarted
   * after close().
   *
   * <p>If retention is disabled ({@code maxAge} is zero or negative) this method logs a message and
   * returns immediately without starting a thread.
   *
   * @throws IllegalStateException if already started
   */
  @Override
  public void start() {
    boolean retentionEnabled = !maxAge.isZero() && !maxAge.isNegative();
    if (!loop.start(retentionEnabled)) {
      LOG.info(
          "Dead-letter queue retention is disabled (maxAge={}); deleteOlderThan will never be"
              + " called",
          maxAge);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code false} when retention is disabled: {@link #start()} then launches no thread, and a
   * sweeper that never had a thread must not read as a dead one.
   */
  @Override
  public boolean isStarted() {
    return loop.isStarted();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAlive() {
    return loop.isAlive();
  }

  /** {@inheritDoc} */
  @Override
  public int consecutiveFailures() {
    return loop.consecutiveFailures();
  }

  /**
   * Executes one sweep cycle synchronously. Package-private for testing; production code uses
   * {@link #start()}.
   */
  void sweepOnce() {
    Instant cutoff = clock.instant().minus(maxAge);
    int deleted = deadLetterQueue.deleteOlderThan(cutoff);
    if (deleted > 0) {
      LOG.info("Dead-letter queue retention swept {} rows", deleted);
      // Guarded — was bare; the delete already committed, but an unguarded throw here
      // would still escape sweepOnce() and could kill whatever periodic driver calls it.
      try {
        metrics.recordDeadLetterSwept(deleted);
      } catch (RuntimeException e) {
        LOG.warn("Metrics recordDeadLetterSwept failed", e);
      }
    }
  }

  /**
   * Interrupts the background thread, waits for a sweep in flight to return, and resets the started
   * guard so start() may be called again. It returns once the sweep thread has ended, so nothing of
   * this sweeper touches the store any more, with two exceptions: the wait timed out after 30 s or
   * the closing thread was interrupted while it waited (both log a WARN, and the sweep may still be
   * running), or close() ran on the sweep thread itself, from a store calling back, which returns
   * at once without a WARN while that sweep finishes. A second close() that runs while the first
   * one waits returns at once as well, without waiting for the sweep.
   */
  @Override
  public void close() {
    loop.close();
  }

  /** Lowers the bound on the wait for a sweep in flight; for tests. */
  void closeJoinTimeoutMs(long millis) {
    loop.closeJoinTimeoutMs(millis);
  }
}

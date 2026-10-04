package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxStore;

/**
 * Background sweeper that prunes delivered and, independently, operator-skipped ({@code SKIPPED})
 * outbox entries older than configured retention windows. There is no FAILED retention: the
 * framework never deletes an unresolved FAILED row; an operator resolves it with {@link
 * OutboxFailedReplayer#replay} or {@link OutboxFailedReplayer#skip}, and the skipped row is then
 * pruned by this window.
 *
 * <p>Runs on a virtual thread ({@link Thread#ofVirtual()}). Call {@link #start()} to begin sweeping
 * and {@link #close()} to stop the thread.
 *
 * <p>If {@code maxAge} is zero or negative, DELIVERED retention is disabled and {@link
 * OutboxStore#deleteDelivered} is never called. Independently, if {@code skippedMaxAge} is zero or
 * negative, SKIPPED retention is disabled and {@link OutboxStore#deleteSkipped} is never called.
 * {@link #start()} is a no-op only when <em>both</em> windows are disabled.
 *
 * <p><b>Resilience:</b> a failure of the sweep cycle is logged and retried with capped exponential
 * backoff — it never silently kills the sweeper thread. An {@link Error} does end the thread; it is
 * logged by the poll loop, and this sweeper's {@link RetentionSweeper} liveness reports it DOWN
 * through {@link BackgroundRelayHealthContributor#registerRetentionSweeper}.
 *
 * <p><b>Testing:</b> {@link #sweepOnce()} is package-private to allow tests to invoke one sweep
 * cycle synchronously without starting a background thread.
 */
public final class OutboxRetentionSweeper implements RetentionSweeper {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxRetentionSweeper.class);

  /** Component and thread name; also what {@link #name()} reports. */
  private static final String NAME = "outbox-retention-sweeper";

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  private final OutboxStore store;
  private final Duration maxAge;
  private final Duration skippedMaxAge;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  private volatile Thread thread;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Creates a retention sweeper with independent DELIVERED and SKIPPED retention windows.
   *
   * @param store the outbox store to prune (required)
   * @param maxAge how far back to keep DELIVERED entries; zero or negative disables DELIVERED
   *     retention
   * @param skippedMaxAge how far back to keep SKIPPED entries (the audit of an operator decision);
   *     zero or negative disables the SKIPPED sweep. Defaults to 30d in the integrations — keep it
   *     at least as long as your audit requires
   * @param interval how often to sweep; must be positive
   * @param clock clock used to compute the cutoff instant (required)
   * @param metrics metrics collector (required; pass {@link StreamRuneMetrics#NOOP} to disable)
   */
  public OutboxRetentionSweeper(
      OutboxStore store,
      Duration maxAge,
      Duration skippedMaxAge,
      Duration interval,
      Clock clock,
      StreamRuneMetrics metrics) {
    Objects.requireNonNull(store, "store is required");
    Objects.requireNonNull(maxAge, "maxAge is required");
    Objects.requireNonNull(skippedMaxAge, "skippedMaxAge is required");
    Objects.requireNonNull(interval, "interval is required");
    Objects.requireNonNull(clock, "clock is required");
    Objects.requireNonNull(metrics, "metrics is required");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException(
          "interval must be positive (Duration.ZERO causes a tight spin loop)");
    }
    this.store = store;
    this.maxAge = maxAge;
    this.skippedMaxAge = skippedMaxAge;
    this.clock = clock;
    this.metrics = metrics;
    Duration maxBackoff =
        interval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? interval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            NAME,
            () -> !Thread.currentThread().isInterrupted(),
            () -> interval,
            interval,
            maxBackoff);
  }

  private boolean deliveredRetentionEnabled() {
    return !(maxAge.isZero() || maxAge.isNegative());
  }

  private boolean skippedRetentionEnabled() {
    return !(skippedMaxAge.isZero() || skippedMaxAge.isNegative());
  }

  /**
   * Starts the background virtual thread. start()/close() pairs are repeatable; may be restarted
   * after close().
   *
   * <p>If both retention windows are disabled ({@code maxAge} and {@code skippedMaxAge} are each
   * zero or negative) this method logs a message and returns immediately without starting a thread.
   *
   * @throws IllegalStateException if already started
   */
  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "OutboxRetentionSweeper already started — call start() exactly once");
    }
    if (!deliveredRetentionEnabled() && !skippedRetentionEnabled()) {
      LOG.info(
          "Outbox retention is disabled (maxAge={}, skippedMaxAge={}); neither deleteDelivered nor"
              + " deleteSkipped will be called",
          maxAge,
          skippedMaxAge);
      return;
    }
    thread = Thread.ofVirtual().name(NAME).start(this::loop);
  }

  private void loop() {
    pollLoop.run(this::sweepOnce);
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
    return started.get() && thread != null;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isAlive() {
    Thread t = thread;
    return t != null && t.isAlive();
  }

  /** {@inheritDoc} */
  @Override
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  /**
   * Executes one sweep cycle synchronously. Package-private for testing; production code uses
   * {@link #start()}.
   */
  void sweepOnce() {
    if (deliveredRetentionEnabled()) {
      Instant cutoff = clock.instant().minus(maxAge);
      int deleted = store.deleteDelivered(cutoff);
      if (deleted > 0) {
        LOG.info("Outbox retention swept {} delivered entries", deleted);
        // Guarded — was bare; the delete already committed, but an unguarded throw here
        // would still escape sweepOnce(), skipping the SKIPPED sweep below entirely.
        try {
          metrics.recordOutboxSwept(deleted);
        } catch (RuntimeException e) {
          LOG.warn("Metrics recordOutboxSwept failed", e);
        }
      }
    }
    if (skippedRetentionEnabled()) {
      Instant cutoff = clock.instant().minus(skippedMaxAge);
      int deleted = store.deleteSkipped(cutoff);
      if (deleted > 0) {
        LOG.info("Outbox retention swept {} SKIPPED entries", deleted);
        try {
          metrics.recordOutboxSkippedSwept(deleted);
        } catch (RuntimeException e) {
          LOG.warn("Metrics recordOutboxSkippedSwept failed", e);
        }
      }
    }
  }

  /**
   * Interrupts the background thread and resets the started guard so start() may be called again.
   */
  @Override
  public void close() {
    Thread t = thread;
    if (t != null) {
      t.interrupt();
    }
    thread = null;
    started.set(false);
  }
}

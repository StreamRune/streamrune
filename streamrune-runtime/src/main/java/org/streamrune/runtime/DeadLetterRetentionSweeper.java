package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;

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
  private static final String NAME = "dead-letter-retention-sweeper";

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  private final DeadLetterQueue deadLetterQueue;
  private final Duration maxAge;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  private volatile Thread thread;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Creates a retention sweeper.
   *
   * @param deadLetterQueue the command dead-letter queue to prune (required)
   * @param maxAge how far back to keep dead-letter entries; zero or negative disables retention
   * @param interval how often to sweep; must be positive
   * @param clock clock used to compute the cutoff instant (required)
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
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "DeadLetterRetentionSweeper already started — call start() exactly once");
    }
    if (maxAge.isZero() || maxAge.isNegative()) {
      LOG.info(
          "Dead-letter queue retention is disabled (maxAge={}); deleteOlderThan will never be"
              + " called",
          maxAge);
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

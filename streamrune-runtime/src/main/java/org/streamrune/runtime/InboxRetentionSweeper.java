package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.StreamRuneMetrics;

/**
 * Background sweeper that prunes command-inbox rows processed before a configured retention window.
 *
 * <p>Runs on a virtual thread ({@link Thread#ofVirtual()}). Call {@link #start()} to begin sweeping
 * and {@link #close()} to stop the thread.
 *
 * <p>If {@code maxAge} is zero or negative, retention is disabled: {@link #start()} is a no-op and
 * {@link CommandInbox#deleteProcessedBefore} is never called.
 *
 * <p>The retention window must exceed the maximum at-least-once redelivery horizon so a pruned key
 * cannot re-admit a duplicate. The recommended default is 7 days — comfortably beyond typical
 * broker redelivery and subscription replay windows.
 *
 * <p><b>Resilience:</b> a failure of the sweep cycle is logged and retried with capped exponential
 * backoff — it never silently kills the sweeper thread. An {@link Error} does end the thread; it is
 * logged by the poll loop, and this sweeper's {@link RetentionSweeper} liveness reports it DOWN
 * through {@link BackgroundRelayHealthContributor#registerRetentionSweeper}.
 *
 * <p><b>Testing:</b> {@link #sweepOnce()} is package-private to allow tests to invoke one sweep
 * cycle synchronously without starting a background thread.
 */
public final class InboxRetentionSweeper implements RetentionSweeper {

  private static final Logger LOG = LoggerFactory.getLogger(InboxRetentionSweeper.class);

  /** Component and thread name; also what {@link #name()} reports. */
  private static final String NAME = "inbox-retention-sweeper";

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  private final CommandInbox inbox;
  private final Duration maxAge;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  private volatile Thread thread;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Creates a retention sweeper.
   *
   * @param inbox the command inbox to prune (required)
   * @param maxAge how far back to keep processed entries; zero or negative disables retention
   * @param interval how often to sweep; must be positive
   * @param clock clock used to compute the cutoff instant (required)
   * @param metrics metrics collector (required; pass {@link StreamRuneMetrics#NOOP} to disable)
   */
  public InboxRetentionSweeper(
      CommandInbox inbox,
      Duration maxAge,
      Duration interval,
      Clock clock,
      StreamRuneMetrics metrics) {
    Objects.requireNonNull(inbox, "inbox is required");
    Objects.requireNonNull(maxAge, "maxAge is required");
    Objects.requireNonNull(interval, "interval is required");
    Objects.requireNonNull(clock, "clock is required");
    Objects.requireNonNull(metrics, "metrics is required");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException(
          "interval must be positive (Duration.ZERO causes a tight spin loop)");
    }
    this.inbox = inbox;
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
          "InboxRetentionSweeper already started — call start() exactly once");
    }
    if (maxAge.isZero() || maxAge.isNegative()) {
      LOG.info(
          "Command inbox retention is disabled (maxAge={}); deleteProcessedBefore will never be"
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
    int deleted = inbox.deleteProcessedBefore(cutoff);
    if (deleted > 0) {
      LOG.info("Command inbox retention swept {} processed rows", deleted);
      // Guarded — was bare; the delete already committed, but an unguarded throw here
      // would still escape sweepOnce() and could kill whatever periodic driver calls it.
      try {
        metrics.recordInboxSwept(deleted);
      } catch (RuntimeException e) {
        LOG.warn("Metrics recordInboxSwept failed", e);
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

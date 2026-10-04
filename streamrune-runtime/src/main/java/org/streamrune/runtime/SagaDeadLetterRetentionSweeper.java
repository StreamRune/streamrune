package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaDeadLetterStore;

/**
 * Background sweeper that prunes {@code saga_dead_letters} rows faulted before a configured
 * retention window.
 *
 * <p>Runs on a virtual thread ({@link Thread#ofVirtual()}). Call {@link #start()} to begin sweeping
 * and {@link #close()} to stop the thread.
 *
 * <p>If {@code maxAge} is zero or negative, retention is disabled: {@link #start()} is a no-op and
 * {@link SagaDeadLetterStore#deleteOlderThan} is never called.
 *
 * <p>A saga dead-letter entry carries metadata for operator triage, but it is <b>not</b> merely
 * metadata: it is the framework's sole handle for recovering its saga — the replayer drains a
 * saga's entries, re-reading each quarantined event at the entry's offset, and a {@code FAULTED}
 * row is cleared only by the successful CAS of such a replayed step. Retention is therefore both a
 * GDPR storage-limitation concern <em>and</em> a correctness one: {@link
 * SagaDeadLetterStore#deleteOlderThan} excludes every entry whose owning saga row is still {@code
 * FAULTED} or carries the {@code dead_letter_pending} shield, so a fix that ships after the window
 * can still replay the saga — the entry becomes prunable again once its row is neither. A NAMED
 * entry whose saga has no row at all (the poison handler's row-less residual) is never pruned
 * either — an operator who removes a saga row by hand discards its entries explicitly — and neither
 * is a null-saga entry the replayer has RESOLVED to a target. Every protected entry is counted into
 * the {@code faulted_backlog} gauge below. Note the interaction with the stale-compensation guard:
 * an entry retained past the inbox-retention window this way is exactly the case {@code
 * SagaDeadLetterReplayer} refuses with {@code STALE_COMPENSATION_BLOCKED} for a saga that faulted
 * mid-compensation (reconcile, then {@code force}). Choose a window comfortably long enough for
 * operators to notice, investigate, and replay or discard quarantined entries.
 *
 * <p><b>The prune cutoff compares each entry's immutable {@code firstFaultedAt}</b>, not the {@code
 * faultedAt} a failed-replay re-quarantine refreshes — so an entry's life is bounded from its FIRST
 * fault and cannot be extended past the window the boot validator (dead-letter retention &le; inbox
 * retention) reasons about. The comparison lives in {@link SagaDeadLetterStore#deleteOlderThan};
 * this sweeper only supplies the cutoff.
 *
 * <p><b>Observability.</b> Because the sweep protects the entries above, that population
 * accumulates silently — {@code SAGA_FAULTED} is a counter fired once at fault time, not a standing
 * depth, and every runner skips {@code FAULTED} sagas. Each cycle therefore samples {@link
 * SagaDeadLetterStore#countFaultedBacklog} and reports it as the {@code
 * streamrune.saga.faulted_backlog} gauge, so a halted saga awaiting an operator replay is visible
 * on the metrics endpoint. One sample per {@code interval}; not sampled at all while retention is
 * disabled (this sweeper does not run), and disabled permanently against a store that does not
 * support the count.
 *
 * <p><b>Resilience:</b> a failure of the sweep cycle is logged and retried with capped exponential
 * backoff — it never silently kills the sweeper thread. An {@link Error} does end the thread; it is
 * logged by the poll loop, and this sweeper's {@link RetentionSweeper} liveness reports it DOWN
 * through {@link BackgroundRelayHealthContributor#registerRetentionSweeper}.
 *
 * <p><b>Testing:</b> {@link #sweepOnce()} is package-private to allow tests to invoke one sweep
 * cycle synchronously without starting a background thread.
 */
public final class SagaDeadLetterRetentionSweeper implements RetentionSweeper {

  private static final Logger LOG = LoggerFactory.getLogger(SagaDeadLetterRetentionSweeper.class);

  /** Component and thread name; also what {@link #name()} reports. */
  private static final String NAME = "saga-dead-letter-retention-sweeper";

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  private final SagaDeadLetterStore store;
  private final Duration maxAge;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final ResilientPollLoop pollLoop;
  private volatile Thread thread;
  private final AtomicBoolean started = new AtomicBoolean(false);

  // Set false the first time countFaultedBacklog throws UnsupportedOperationException
  // (a third-party store that cannot see saga_state), so the gauge degrades gracefully instead of
  // retrying every cycle. Pruning is never affected. Mirrors OutboxPoller.backlogSamplingSupported.
  private volatile boolean faultedBacklogSamplingSupported = true;

  /**
   * Creates a retention sweeper.
   *
   * @param store the saga dead-letter store to prune (required)
   * @param maxAge how far back to keep quarantined entries; zero or negative disables retention
   * @param interval how often to sweep; must be positive
   * @param clock clock used to compute the cutoff instant (required)
   * @param metrics metrics collector (required; pass {@link StreamRuneMetrics#NOOP} to disable)
   */
  public SagaDeadLetterRetentionSweeper(
      SagaDeadLetterStore store,
      Duration maxAge,
      Duration interval,
      Clock clock,
      StreamRuneMetrics metrics) {
    Objects.requireNonNull(store, "store is required");
    Objects.requireNonNull(maxAge, "maxAge is required");
    Objects.requireNonNull(interval, "interval is required");
    Objects.requireNonNull(clock, "clock is required");
    Objects.requireNonNull(metrics, "metrics is required");
    if (interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException(
          "interval must be positive (Duration.ZERO causes a tight spin loop)");
    }
    this.store = store;
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
          "SagaDeadLetterRetentionSweeper already started — call start() exactly once");
    }
    if (maxAge.isZero() || maxAge.isNegative()) {
      LOG.info(
          "Saga dead-letter retention is disabled (maxAge={}); deleteOlderThan will never be"
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
    sampleFaultedBacklog();
    Instant cutoff = clock.instant().minus(maxAge);
    int deleted = store.deleteOlderThan(cutoff);
    if (deleted > 0) {
      LOG.info("Saga dead-letter retention swept {} rows", deleted);
      // Guarded — was bare; the delete already committed, but an unguarded throw here
      // would still escape sweepOnce() and could kill whatever periodic driver calls it.
      try {
        metrics.recordSagaDeadLetterSwept(deleted);
      } catch (RuntimeException e) {
        LOG.warn("Metrics recordSagaDeadLetterSwept failed", e);
      }
    }
  }

  /**
   * Reports the {@code streamrune.saga.faulted_backlog} gauge — the count of dead-letter entries
   * whose owning saga row is still {@code FAULTED}, i.e. exactly the entries {@link
   * SagaDeadLetterStore#deleteOlderThan} now refuses to prune.
   *
   * <p>Protecting those entries is what keeps a stranded saga replayable, but it also means the
   * population accumulates <em>silently</em>: {@code SAGA_FAULTED} fires once at fault time and is
   * not a standing depth, and every runner skips {@code FAULTED} sagas. Sampling here — at the top
   * of the sweep, before the prune, so a prune failure cannot skip the sample — is what puts a
   * halted business process on the metrics endpoint. Cadence is one sample per sweep {@code
   * interval}; the gauge holds its last value in between (and while retention is disabled, {@code
   * maxAge <= 0}, this sweeper never runs at all, so the gauge is never sampled).
   *
   * <p>Runs in its own guard so a dead-letter-store outage cannot abort the prune: a transient
   * count failure is logged and swallowed (the gauge holds its last value), and a store that does
   * not support {@link SagaDeadLetterStore#countFaultedBacklog} disables the sample permanently
   * instead of retrying every cycle — the same discipline as the outbox backlog gauge.
   */
  private void sampleFaultedBacklog() {
    if (metrics == StreamRuneMetrics.NOOP || !faultedBacklogSamplingSupported) {
      return;
    }
    try {
      metrics.recordSagaFaultedBacklog(store.countFaultedBacklog());
    } catch (UnsupportedOperationException _) {
      faultedBacklogSamplingSupported = false;
      LOG.debug(
          "SagaDeadLetterStore {} does not support countFaultedBacklog; saga faulted-backlog gauge"
              + " disabled",
          store.getClass().getSimpleName());
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to sample the FAULTED saga dead-letter backlog this cycle: {}", e.getMessage());
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

package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Durable, automatic recovery driver for stuck {@code COMPENSATING} sagas. Runs on a virtual
 * thread; call {@link #start()} to begin sweeping and {@link #close()} to stop it.
 *
 * <p><b>The gap it closes.</b> When a saga's compensation command fails <em>transiently</em>,
 * {@link SagaRunner} correctly leaves it {@link org.streamrune.core.saga.SagaStatus#COMPENSATING}
 * (never force-{@code FAILED}) so the undo is not abandoned on a momentary blip — the episode is
 * meant to be re-driven "until it durably succeeds". But the two existing re-drive paths do not
 * fire for a saga with an empty {@link org.streamrune.core.saga.SagaDecider#timeout()}: there is no
 * {@link SagaTimeoutRunner} for it, and the correlated-event compensation resume (now driven by
 * {@link SagaStepExecutor}) only runs on a <em>redelivered</em> correlated event — which never
 * arrives, because the triggering event's offset already advanced when the transient failure
 * returned normally. Such a saga would otherwise stay {@code COMPENSATING} forever with the refund
 * never issued / the stock never released, and nothing alerts. This sweeper is the missing driver:
 * it periodically finds those sagas and re-drives their compensation until it completes.
 *
 * <p><b>How it re-drives (no double-dispatch, and no re-drive of the same episode missed across
 * concurrent drivers).</b> This is a race-coordination guarantee only — it does not make a
 * compensation list that omits an already-executed-but-unrecorded forward command complete; that is
 * the orchestrator's cancel/void responsibility (see {@code SagaOrchestrator.compensate}). Each
 * cycle it queries {@link SagaStore#findCompensating} for {@code COMPENSATING} sagas of its type
 * last written before {@code now - retryInterval} (the age cutoff excludes an episode the event
 * path just claimed and is still compensating), then calls {@link SagaRunner#sweepCompensation} for
 * each — which re-drives at the saga's <em>current</em> episode version under the shared {@link
 * SagaCommandDispatch#episodeCompensationKey}. So an already-succeeded compensation dedups in the
 * command inbox (only the failed one re-runs), and the terminal write is a version-CAS: if a
 * concurrent event-path resume or {@link SagaTimeoutRunner} re-pick drives the same episode,
 * exactly one terminal write wins and the losers skip. A transient failure leaves the saga {@code
 * COMPENSATING} for the next cycle; a success terminalizes it.
 *
 * <p><b>Bounded (poison protection).</b> A compensation that keeps failing "transiently" forever
 * (e.g. a permanently broken downstream) must not loop indefinitely. The sweeper measures the
 * episode's total stuck-{@code COMPENSATING} time from the durable claim instant ({@code
 * episode_claimed_at} — fault-cycle-immutable, surfaced by {@link SagaStore#findCompensating};
 * every write that enters {@code COMPENSATING} stamps it, and a row surfaced without one is refused
 * — {@link SagaUnstampedCompensationEpisodeException}, logged, nothing dispatched — rather than
 * measured from {@code updated_at}); once that exceeds {@code giveUpAfter}, for a saga this sweeper
 * re-drove in the preceding cycle, it terminalizes the saga {@code FAULTED} (halted, excluded from
 * future sweeps) with a WARN + fault metric. The anchor is deliberately NOT {@code updated_at}:
 * while a transient-failure re-drive leaves the row untouched, a fault→replay cycle refreshes
 * {@code updated_at} ({@code markFaulted} and the replay feed's own CAS that clears the fault are
 * both writes), which would reset the give-up clock and let the episode dwell past the
 * command-inbox retention window — after which a re-drive misses the pruned dedup key of an
 * already-succeeded compensation and re-executes it (double refund). A deterministic (business)
 * compensation failure or a throwing {@code compensate()} is bounded even sooner — the former
 * terminalizes {@code FAILED} on the first re-drive, the latter {@code FAULTED}.
 *
 * <p><b>A refused re-drive is not bounded by {@code giveUpAfter}.</b> A re-drive whose every
 * compensation command is refused admission ({@code COMPENSATING_REFUSED}: an open circuit breaker,
 * a closing bus, an interceptor veto) dispatched nothing, so it is not an attempt and the give-up
 * horizon never fires for it. A saga refused on every cycle — a policy veto, a breaker that never
 * closes — stays {@code COMPENSATING} indefinitely unless {@link Builder#inboxRetentionMaxAge} is
 * set (the key-age guard below then faults it once the episode outlives the inbox window) or a
 * {@link SagaTimeoutRunner} for the saga type has {@code maxCompensatingDwell} set (a time-based
 * bound that also applies to a refused resume).
 *
 * <p><b>Key-age guard.</b> The give-up horizon bounds dwell only while this sweeper actually runs.
 * An episode that dwelled past the <em>command-inbox retention window</em> anyway — a
 * deployment-wide outage longer than the window, or {@code giveUpAfter} misconfigured at or above
 * it (the boot-time {@code SagaRetentionValidator} deliberately only WARNs on that config) — must
 * NOT be re-driven: its succeeded-compensation dedup keys may already be pruned, so the re-dispatch
 * would re-execute an already-succeeded compensation (double refund). With {@link
 * Builder#inboxRetentionMaxAge} configured (the auto-configs wire {@code
 * streamrune.inbox.retention-max-age}), such an episode is CAS-FAULTed via the give-up path
 * <em>without any dispatch</em> — the same quarantine-for-operator-reconciliation remedy as the
 * replayer's {@code STALE_COMPENSATION_BLOCKED}, measured from the same {@code episode_claimed_at}
 * anchor — and this deliberately overrides the first-re-drive grant.
 *
 * <p><b>Sampling-only mode.</b> The {@code streamrune.saga.compensating} and {@code
 * streamrune.saga.faulted_rows} gauges are emitted by this sweeper's per-cycle sample and by
 * nothing else, so the integrations no longer withhold the sweeper when {@code
 * streamrune.saga.compensation-retry-enabled=false}: they build it with {@link
 * Builder#compensationRetryEnabled(boolean) compensationRetryEnabled(false)}, and it then samples
 * every cycle but never takes the lease or re-drives. Switching the re-drive off no longer blinds
 * the saga backlog observability. The one exception: when no {@code StreamRuneMetrics} is wired
 * either, such a sweeper would neither re-drive nor sample anything, so the integrations do not
 * build it at all.
 *
 * <p><b>Single-active-consumer.</b> Like {@link DeadLetterRetryRunner}, the sweep is leadership
 * -gated: only the instance holding leadership for {@code "saga-compensation-retry:<sagaType>"}
 * polls; a non-leader returns before reading a single row. The default is {@link
 * SubscriptionLeadership#NOOP} (always leader), so single-instance behavior is unchanged. This is
 * defense-in-depth on top of the shared-episode-key + terminal-CAS invariant, which already makes
 * even a concurrent double-drive safe.
 *
 * @param <S> the saga state type
 */
public final class SagaCompensationRetrySweeper<S extends SagaState> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SagaCompensationRetrySweeper.class);

  /**
   * Single-active-consumer name prefix; the saga type ({@code SagaType.fromClass}: the state
   * class's fully-qualified name) is appended.
   */
  static final String CONSUMER_NAME_PREFIX = "saga-compensation-retry:";

  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /**
   * Upper bound {@link #close()} waits to join the sweep thread before proceeding. The interrupted
   * thread exits promptly in practice; this only caps a pathologically stuck sweep.
   */
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

  private final SagaRunner<S> sagaRunner;
  private final SagaStore sagaStore;
  private final SagaType sagaType;
  private final String sagaTypeName;
  private final String consumerName;
  private final SubscriptionLeadership leadership;
  private final Duration retryInterval;
  private final Duration giveUpAfter;
  private final Duration inboxRetentionMaxAge;
  private final int batchSize;
  private final Clock clock;
  private final StreamRuneMetrics metrics;

  /**
   * {@code false} puts the sweeper in sampling-only mode — it still reports the saga backlog gauges
   * every cycle but never takes the lease or re-drives compensation.
   */
  private final boolean compensationRetryEnabled;

  private final ResilientPollLoop pollLoop;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private volatile Thread thread;
  private volatile boolean running;

  // One-shot latch PER METRIC CALL SITE for a persistently throwing metrics sink, same
  // shape as SubscriptionHealthContributor's lagMetricFailedWarned — warn once when a call
  // site starts failing, stay quiet while it keeps failing (sampleBacklog() alone calls
  // recordMetric twice EVERY cycle, forever, once compensation-retry-enabled=false makes sampling
  // the whole sweep), and re-arm the moment that SAME call succeeds again so a flapping sink is
  // still visible as repeated warnings rather than either one lost line or an unbounded flood.
  // Deliberately one flag PER call site, not one shared flag for the whole sweeper: a real sink
  // failure (a single meter-id collision, e.g.) is often specific to ONE metric name, and a
  // shared flag would have the healthy sibling call's success reset it every cycle — re-arming
  // the failing call's warning before its NEXT cycle, defeating the one-shot latch entirely.
  // Caught empirically: an early version with one shared flag still warned every cycle in a test
  // where only recordSagaCompensatingBacklog threw and recordSagaFaultedRows kept succeeding.
  private final AtomicBoolean compensatingBacklogMetricFailedWarned = new AtomicBoolean(false);
  private final AtomicBoolean faultedRowsMetricFailedWarned = new AtomicBoolean(false);
  private final AtomicBoolean compensationRetryMetricFailedWarned = new AtomicBoolean(false);

  // Set false the first time countByStatus throws UnsupportedOperationException (a custom
  // store that does not support counting), so the backlog gauge degrades gracefully without probing
  // every cycle. Mirrors OutboxPoller.backlogSamplingSupported.
  private volatile boolean backlogSamplingSupported = true;

  /**
   * Saga ids this sweeper actually re-drove in the immediately-preceding cycle. The give-up horizon
   * is only honored for a saga that appears here — i.e. one this sweeper has already made at least
   * one real re-drive attempt for. Without this, process downtime longer than {@code giveUpAfter}
   * would make the episode's age exceed the horizon on the very first post- restart cycle and
   * CAS-FAULT a recoverable COMPENSATING saga with zero attempts. A fresh sweeper instance (new
   * process) starts empty, so every saga is guaranteed one attempt after a restart; self-pruning
   * because only ids re-driven last cycle carry over (bounded by {@code batchSize}). Only ever
   * touched by the single sweep thread (or a test thread), never concurrently.
   */
  private volatile java.util.Set<org.streamrune.core.saga.SagaId> previouslyReDriven =
      java.util.Set.of();

  private SagaCompensationRetrySweeper(Builder<S> b) {
    this.compensationRetryEnabled = b.compensationRetryEnabled;
    this.sagaRunner = b.sagaRunner;
    this.sagaStore = b.sagaStore;
    this.sagaType = SagaType.fromClass(b.sagaRunner.stateType());
    // The health component and metric tag key is the saga type itself, never the simple class
    // name — two saga-state classes may share a simple name across packages.
    this.sagaTypeName = sagaType.value();
    this.consumerName = CONSUMER_NAME_PREFIX + sagaType.value();
    this.leadership = b.leadership != null ? b.leadership : SubscriptionLeadership.NOOP;
    this.retryInterval = b.retryInterval;
    this.giveUpAfter = b.giveUpAfter;
    // zero/negative means the inbox retention sweeper is DISABLED (keys are never
    // pruned — see SagaRetentionValidator's convention), so the key-age guard must be inert, same
    // as when the window is simply unknown (null).
    this.inboxRetentionMaxAge =
        b.inboxRetentionMaxAge == null
                || b.inboxRetentionMaxAge.isZero()
                || b.inboxRetentionMaxAge.isNegative()
            ? null
            : b.inboxRetentionMaxAge;
    this.batchSize = b.batchSize;
    this.clock = b.clock;
    this.metrics = b.metrics != null ? b.metrics : StreamRuneMetrics.NOOP;
    Duration maxBackoff =
        retryInterval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? retryInterval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "saga-compensation-retry-" + sagaType.value(),
            () -> running,
            () -> retryInterval,
            retryInterval,
            maxBackoff);
  }

  /**
   * Starts the sweep loop on a virtual thread. Not idempotent — call exactly once, or after a
   * preceding {@link #close()}.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "SagaCompensationRetrySweeper already started — call start() exactly once, or close()"
              + " first");
    }
    running = true;
    if (!compensationRetryEnabled) {
      // Worded by mode — with no metrics wired this loop is a pure idle no-op (still
      // health-registered, so a dead thread would still surface DOWN), so the log must not claim
      // it "reports the saga backlog gauges" when sampleBacklog() has nothing to report through.
      if (metrics == StreamRuneMetrics.NOOP) {
        LOG.info(
            "SagaCompensationRetrySweeper for {} runs in sampling-only mode (compensation retry is"
                + " disabled) with no metrics wired — a pure idle loop, present only so /health can"
                + " report on it; it never re-drives compensation",
            sagaTypeName);
      } else {
        LOG.info(
            "SagaCompensationRetrySweeper for {} runs in sampling-only mode (compensation retry is"
                + " disabled): it reports the saga backlog gauges each cycle and never re-drives"
                + " compensation",
            sagaTypeName);
      }
    }
    thread =
        Thread.ofVirtual().name("saga-compensation-retry-" + sagaType.value()).start(this::runLoop);
  }

  private void runLoop() {
    try {
      pollLoop.run(this::sweepOnce);
    } finally {
      running = false;
    }
  }

  /**
   * Stops the sweep loop, resigns leadership so a standby can take over immediately, and resets the
   * started guard so {@link #start()} can be called again (restartable lifecycle).
   */
  @Override
  public void close() {
    running = false;
    Thread t = thread;
    thread = null;
    boolean stuck = false;
    if (t != null) {
      t.interrupt();
      // Join the sweep thread before resetting `started`, so its
      // finally { running = false } cannot run AFTER a subsequent start() set running = true —
      // which would silently kill the restarted sweeper. Self-join guard: a thread cannot join
      // itself (close() invoked from within the sweep thread).
      if (t != Thread.currentThread()) {
        try {
          t.join(CLOSE_JOIN_TIMEOUT_MS);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
        // The join timed out and the sweep thread is still alive — a restart would run
        // a SECOND sweeper whose stale finally { running = false } then silently kills the
        // restarted one. Stay not-restartable and log loudly instead of resetting the guard.
        stuck = t.isAlive();
      }
    }
    // The SubscriptionLeadership contract requires resign to be no-throw for transient
    // failures, but close() must complete its lifecycle bookkeeping (re-drive evidence reset,
    // stuck warning, restartability reset below) even against a contract-breaking implementation —
    // a resign throw used to escape close() into the shutting-down lifecycle adapter and leave the
    // sweeper permanently not-restartable.
    // Sampling-only mode (compensationRetryEnabled=false) returns out of sweepOnce()
    // before the leadership gate (see below), so it never calls tryAcquire and never holds a
    // lease to give back. Resigning anyway was a harmless no-op UPDATE per saga type per replica
    // on every shutdown — or, if the DB is already gone by then, a misleading "resign(...) failed;
    // lease will expire naturally" WARN for a lease this instance never took.
    if (compensationRetryEnabled) {
      try {
        leadership.resign(consumerName);
      } catch (RuntimeException resignFailure) {
        LOG.warn(
            "SagaCompensationRetrySweeper could not resign leadership on close; the lease will"
                + " expire naturally at its TTL",
            resignFailure);
      }
    }
    // Drop the re-drive evidence so a restart (start() after close()) treats the next cycle as a
    // fresh attempt window — a resume after a stop, like a fresh process, gives each saga ≥1 re-
    // drive before the give-up horizon is honored.
    previouslyReDriven = java.util.Set.of();
    if (stuck) {
      LOG.warn(
          "SagaCompensationRetrySweeper thread did not stop within {}ms; leaving the sweeper"
              + " not-restartable so a restart cannot duplicate or silently kill it. Investigate"
              + " the stuck sweep.",
          CLOSE_JOIN_TIMEOUT_MS);
    } else {
      started.set(false);
    }
  }

  /** Whether the sweep thread is currently active. */
  public boolean isRunning() {
    return running;
  }

  /**
   * Whether {@link #start()} has been called and {@link #close()} has not. Combined with {@link
   * #isAlive()} this distinguishes a sweeper that was never started (or was cleanly closed) from
   * one whose sweep thread <em>died</em> — {@code isStarted() && !isAlive()} is the dead-driver
   * signal the saga health check reports DOWN. Mirrors the outbox/DLQ relay liveness surface.
   */
  public boolean isStarted() {
    return started.get();
  }

  /**
   * Whether the sweeper's thread is currently alive. A started sweeper whose thread is no longer
   * alive died unexpectedly (e.g. an {@link Error} escaped the sweep loop) — see {@link
   * #isStarted()}.
   */
  public boolean isAlive() {
    Thread t = thread;
    return t != null && t.isAlive();
  }

  /**
   * Consecutive sweep-cycle failures; {@code 0} when the last cycle succeeded, rising while the
   * {@link ResilientPollLoop} is in backoff-retry (saga store unreachable). A nonzero value means
   * the sweeper is DEGRADED but still alive. Mirrors {@code OutboxPoller.consecutiveFailures()}.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  /**
   * Whether this sweeper re-drives stuck compensation ({@code true}, the default) or only samples
   * the saga backlog gauges (sampling-only mode, {@code
   * streamrune.saga.compensation-retry-enabled=false} in the integrations).
   */
  public boolean isCompensationRetryEnabled() {
    return compensationRetryEnabled;
  }

  /**
   * The saga type this sweeper drives, as {@link SagaType#value()} — the saga state's
   * fully-qualified class name, the value the saga store persists in {@code saga_type}. It keys the
   * sweeper's health component ({@code saga-compensation-retry:<saga type>}) and is the {@code
   * saga.type} tag of its metrics: unlike the simple class name it is unique, so two saga-state
   * classes sharing a simple name in different packages report two components and two series
   * instead of hiding one behind the other.
   */
  public String sagaTypeName() {
    return sagaTypeName;
  }

  /** Package-private for testing — the single-active-consumer coordinator the sweep gates on. */
  SubscriptionLeadership leadership() {
    return leadership;
  }

  /**
   * Executes one sweep cycle: leadership-gated, then for each due {@code COMPENSATING} saga, decide
   * give-up vs re-drive and delegate to {@link SagaRunner#sweepCompensation}. Package-private so
   * tests can drive a single cycle synchronously with a fixed clock, without a background thread.
   */
  void sweepOnce() {
    // Sample the COMPENSATING backlog gauge each cycle BEFORE the leadership gate, so every
    // replica (leader or standby) reports the same backlog — a stalled/dead leader is then visible
    // both via the driver-liveness health check AND a climbing streamrune.saga.compensating gauge.
    sampleBacklog();
    // With compensation retry switched off this sweeper is sampling-only. The two saga
    // backlog gauges above are emitted nowhere else, so the knob must stop the RE-DRIVE (below),
    // not the observability. No lease is taken either — sampling is per-replica by design.
    if (!compensationRetryEnabled) {
      return;
    }
    // Single-active-consumer gate: only the leader for this saga type polls and re-drives. A
    // non-leader returns before reading a single row (no findCompensating, no re-drive), so N
    // replicas never all re-drive the same episode. NOOP (single-instance default) always hands out
    // a lease. Re-drives are command-inbox idempotent, so no fencing epoch is threaded — the lease
    // gate alone suffices.
    if (leadership.tryAcquire(consumerName).isEmpty()) {
      return;
    }
    Instant now = clock.instant();
    Instant dueCutoff = now.minus(retryInterval);
    List<SagaStore.CompensatingSaga> due =
        sagaStore.findCompensating(sagaType, dueCutoff, batchSize);
    // Collect the ids we re-drive THIS cycle; it becomes next cycle's "already attempted" evidence.
    java.util.Set<org.streamrune.core.saga.SagaId> reDrivenThisCycle =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    for (SagaStore.CompensatingSaga cs : due) {
      reDrive(cs, now, reDrivenThisCycle);
    }
    previouslyReDriven = reDrivenThisCycle;
  }

  /**
   * Reports the {@code streamrune.saga.compensating} backlog gauge — the current count of {@code
   * COMPENSATING} sagas of this type (refunds not yet issued / stock not yet released). Runs in its
   * own guard so a saga-store outage does not abort the sweep: a transient count failure is logged
   * and swallowed (the gauge holds its last value), a store that does not support {@link
   * SagaStore#countByStatus} disables the sample permanently, and the outage still surfaces via the
   * driver-liveness health check.
   *
   * <p><b>The {@code streamrune.saga.faulted_rows} gauge is sampled from the SAME cycle, off the
   * SAME {@code countByStatus} capability.</b> Every automatic quarantine this framework performs
   * writes {@code FAULTED} with NO dead-letter entry — this sweeper's own stale-key / give-up route
   * ({@code SagaRunner.faultStuckCompensation}), {@code SagaTimeoutRunner}'s dwell and
   * stale-episode faults, and {@code SagaStepExecutor.faultEpisode} when a {@code compensate()}
   * throws on a resume. Such a saga is HALTED (every runner, sweeper and timeout poller skips
   * {@code FAULTED}), frequently with money half-moved, and it was invisible to every standing
   * series: {@code streamrune.saga.faulted_backlog} is computed purely from {@code
   * saga_dead_letters} rows and reports 0 because no row exists, {@code
   * streamrune.saga.compensating} reports 0 because the status moved off {@code COMPENSATING}, and
   * {@code streamrune.saga.faulted} is a one-shot counter, not a depth. The operator's alert sat
   * green over a stuck refund. Counting the saga rows themselves is the only signal that covers the
   * whole halted population; {@code faulted_backlog} remains correct for what it measures, the
   * replayable subset.
   *
   * <p>Both samples share one degradation switch on purpose: they need the identical store
   * capability, and a store lacking it must report NO series rather than a false 0 for either.
   */
  private void sampleBacklog() {
    if (metrics == StreamRuneMetrics.NOOP || !backlogSamplingSupported) {
      return;
    }
    // The STORE READS and the METRICS WRITES are deliberately two separate guarded steps,
    // not one try. Store-read failures keep the behavior below (propagate in sampling-only
    // mode so a real outage surfaces as DEGRADED; swallow-and-WARN in re-drive mode, where the
    // leader's own findCompensating failure already surfaces the same outage). Metric-write
    // failures ALWAYS go through recordMetric()'s swallow-and-WARN guard regardless of mode — a
    // throwing sink (a Prometheus meter-id collision, a closed registry: the same hazard class
    // that is guarded against at every other metrics call site) is not a saga-store outage and must
    // not read as
    // one: before this split, the sampling-only propagation rule rethrew a sink failure too,
    // turning a healthy store + a broken sink into ERROR-with-trace-per-cycle and DEGRADED health.
    long compensatingCount;
    long faultedCount;
    try {
      compensatingCount =
          sagaStore.countByStatus(sagaType, org.streamrune.core.saga.SagaStatus.COMPENSATING);
      faultedCount = sagaStore.countByStatus(sagaType, org.streamrune.core.saga.SagaStatus.FAULTED);
    } catch (UnsupportedOperationException _) {
      backlogSamplingSupported = false;
      LOG.debug(
          "SagaStore {} does not support countByStatus; saga compensating/faulted backlog gauges"
              + " disabled",
          sagaStore.getClass().getSimpleName());
      return;
    } catch (RuntimeException e) {
      if (!compensationRetryEnabled) {
        // In sampling-only mode this IS the whole sweep cycle — sweepOnce() returns right
        // after this call, before the leadership gate, so there is no OTHER store interaction
        // (like findCompensating in re-drive mode) whose failure could surface the outage.
        // Swallowing here left consecutiveFailures() stuck at 0 (health UP) through a sustained
        // saga-store outage, under a component name that implies an active driver. Let it
        // propagate so the poll loop's backoff + consecutiveFailures (and thus DEGRADED health)
        // reflect the outage. re-drive mode is unaffected ONLY for the LEADER — its own
        // findCompensating failure already propagates the same way; a STANDBY in re-drive mode
        // never reaches findCompensating (tryAcquire returns empty first), so for a standby this
        // swallow-and-WARN is the only per-replica signal, by design (sampling is per-replica —
        // the durable, fleet-wide signal is streamrune.saga.faulted_backlog / the dead-letter
        // store, not this gauge).
        throw e;
      }
      LOG.warn("Failed to sample saga backlog gauges this cycle: {}", e.getMessage());
      return;
    }
    recordMetric(
        compensatingBacklogMetricFailedWarned,
        () -> metrics.recordSagaCompensatingBacklog(sagaTypeName, compensatingCount));
    recordMetric(
        faultedRowsMetricFailedWarned,
        () -> metrics.recordSagaFaultedRows(sagaTypeName, faultedCount));
  }

  private void reDrive(
      SagaStore.CompensatingSaga cs,
      Instant now,
      java.util.Set<org.streamrune.core.saga.SagaId> reDrivenThisCycle) {
    // The give-up horizon anchors on the episode's durable, fault-cycle-immutable
    // claim instant (episode_claimed_at — the same anchor discipline as the replayer's
    // stale guard) and on nothing else. Anchoring on updatedAt was sound only while it equaled the
    // claim instant: every CAS write refreshes it, so a fault->replay cycle (markFaulted + the
    // replay feed's fault-clearing CAS) would RESET the give-up clock — the episode could dwell
    // past the command-inbox retention window, after which a re-drive misses the pruned dedup key
    // of an already-succeeded compensation and re-executes it (double refund). The due cutoff
    // (findCompensating's updated_at predicate) deliberately stays on updatedAt: spacing re-drives
    // around fresh writes is exactly what it is for.
    // Every write that enters COMPENSATING stamps the claim
    // instant, so a COMPENSATING row surfaced without one violates the SagaStore contract. It is
    // REFUSED — logged, nothing dispatched, the row left for the next cycle to refuse again until
    // the store is fixed — never re-driven on a guessed anchor (the deleted updatedAt fallback).
    Instant episodeAnchor = cs.episodeClaimedAt();
    if (episodeAnchor == null) {
      var unstamped = SagaUnstampedCompensationEpisodeException.ofEnumeration(cs.sagaId());
      LOG.warn(
          "Refusing to re-drive compensation for saga {} — the row is COMPENSATING without a durable"
              + " episode stamp, a SagaStore contract violation; nothing was dispatched and the row"
              + " is unchanged. Fix the store (or repair the row), then it is re-driven normally",
          LogSanitizer.sanitizeForLog(cs.sagaId().value()),
          unstamped);
      return;
    }
    // Key-age guard — the automatic-path companion of the replayer's
    // STALE_COMPENSATION_BLOCKED, anchored on the SAME durable claim instant (episodeAnchor
    // above; no third clock). Once the episode's age exceeds the command-inbox retention window,
    // its succeeded-compensation dedup keys are unprovably fresh: the InboxRetentionSweeper may
    // already have pruned them — after an outage longer than the window, or under a giveUpAfter
    // misconfigured >= the window (the boot validator deliberately only WARNs there; this guard is
    // the runtime compensating control) — and a re-dispatch would MISS the inbox and RE-EXECUTE an
    // already-succeeded compensation (double refund). Route to the existing give-up handling
    // (CAS-FAULT via sweepCompensation(giveUp=true), NO dispatch), deliberately overriding
    // The one-free-re-drive-after-restart grant: that grant exists for recoverable downtime,
    // not for an episode whose dedup keys may be gone.
    boolean staleKeys =
        inboxRetentionMaxAge != null
            && Duration.between(episodeAnchor, now).compareTo(inboxRetentionMaxAge) > 0;
    // Only honor the give-up horizon for a saga this sweeper already re-drove at least
    // once (in the immediately-preceding cycle). A saga past the horizon on its first sight — e.g.
    // after process downtime longer than giveUpAfter — is re-driven, not faulted, so a saga stuck
    // solely because of downtime gets a real recovery attempt before it can be quarantined.
    boolean pastHorizon = Duration.between(episodeAnchor, now).compareTo(giveUpAfter) > 0;
    boolean giveUp = staleKeys || (pastHorizon && previouslyReDriven.contains(cs.sagaId()));
    StepOutcome outcome;
    try {
      outcome = sagaRunner.sweepCompensation(cs.sagaId(), giveUp);
    } catch (RuntimeException infra) {
      // SagaStore I/O (load/update) failed for THIS saga — log and move on to the next; the whole
      // sweep cycle is not aborted, and the row is retried next cycle. (A cycle-wide failure is
      // handled by ResilientPollLoop's backoff.)
      LOG.warn(
          "Failed to re-drive compensation for saga {} — will retry next sweep",
          LogSanitizer.sanitizeForLog(cs.sagaId().value()),
          infra);
      return;
    }
    switch (outcome) {
      // A real re-drive attempt happened — the episode reached a terminal status (COMPENSATED /
      // FAILED) or was left COMPENSATING for the next cycle. Record it so a still-stuck saga can be
      // given up on the NEXT cycle (a terminalized COMPENSATED/FAILED saga simply won't be swept
      // again).
      case COMPENSATED, FAILED, COMPENSATING_LEFT -> {
        reDrivenThisCycle.add(cs.sagaId());
        recordMetric(
            compensationRetryMetricFailedWarned,
            () -> metrics.recordSagaCompensationRetry(sagaTypeName));
        LOG.info(
            "Re-drove stuck compensation for {} saga {} (no timeout runner / no redelivery needed);"
                + " already-succeeded compensations dedup, only the failed one re-runs",
            sagaTypeName,
            LogSanitizer.sanitizeForLog(cs.sagaId().value()));
      }
      case COMPENSATING_REFUSED -> {
        // Every compensation command was REFUSED ADMISSION (open breaker, closing bus,
        // interceptor veto) — nothing was attempted, so this is NOT recorded as a re-drive: the
        // pre-fix sweeper counted it and, one cycle past giveUpAfter, FAULTED the saga with zero
        // actual dispatches (the class DeadLetterRetryRunner already guards against). The saga
        // stays
        // COMPENSATING and is re-driven next cycle once the gate lifts. The staleKeys route above
        // is untouched: an episode whose dedup keys may already be pruned still gives up on the
        // key-age bound regardless of how its re-drives fared.
        // NOT counted in streamrune.saga.compensation_retries either — that series is
        // defined as re-driven episodes (MetricNames), and a refused admission re-drove nothing;
        // counting it made a breaker-open window read as a climbing retry rate with zero
        // dispatches behind it. The INFO line below is the refusal's only signal.
        LOG.info(
            "Re-drive of stuck compensation for {} saga {} was refused admission (circuit breaker"
                + " open, bus closing, or interceptor veto) — not counted as an attempt; retried"
                + " next cycle",
            sagaTypeName,
            LogSanitizer.sanitizeForLog(cs.sagaId().value()));
      }
      case FAULTED -> {
        if (staleKeys) {
          LOG.warn(
              "{} saga {} dwelled COMPENSATING for {} — past the command-inbox retention window"
                  + " ({}) — so its episode's succeeded-compensation dedup keys may already be"
                  + " pruned; a re-dispatch could RE-EXECUTE an already-succeeded compensation"
                  + " (double refund). Marked FAULTED WITHOUT re-dispatching —"
                  + " reconcile the partial compensation manually, treating the episode's dedup"
                  + " keys as expired, then resume it with"
                  + " SagaDeadLetterReplayer.resumeFaulted(sagaId, true)",
              sagaTypeName,
              LogSanitizer.sanitizeForLog(cs.sagaId().value()),
              Duration.between(episodeAnchor, now),
              inboxRetentionMaxAge);
        } else if (giveUp) {
          LOG.warn(
              "{} saga {} was stuck COMPENSATING past the give-up horizon ({}); marked FAULTED —"
                  + " its compensation never durably completed. Once the cause is fixed, resume"
                  + " it with SagaDeadLetterReplayer.resumeFaulted(sagaId)",
              sagaTypeName,
              LogSanitizer.sanitizeForLog(cs.sagaId().value()),
              giveUpAfter);
        }
        // else: a throwing compensate() during the sweep re-drive — the poison FAULT is already
        // WARN-logged + fault-metric'd inside SagaStepExecutor; nothing to do here.
      }
      case NOT_COMPENSATING -> {
        // a concurrent path terminalized it first — nothing to do
      }
      default -> {
        // CLAIM_LOST and the forward outcomes cannot occur on a ResumeCompensation trigger; no-op.
      }
    }
  }

  private void recordMetric(AtomicBoolean failedWarned, Runnable metricCall) {
    try {
      metricCall.run();
      failedWarned.set(false);
    } catch (RuntimeException e) {
      if (failedWarned.compareAndSet(false, true)) {
        LOG.warn("Metrics recording failed for saga compensation-retry sweeper", e);
      }
    }
  }

  /** Returns a new builder. */
  public static <S extends SagaState> Builder<S> builder() {
    return new Builder<>();
  }

  /** Builder for {@link SagaCompensationRetrySweeper}. */
  public static final class Builder<S extends SagaState> {
    private SagaRunner<S> sagaRunner;
    private SagaStore sagaStore;
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private Duration retryInterval = Duration.ofSeconds(60);
    private Duration giveUpAfter = Duration.ofHours(1);
    private Duration inboxRetentionMaxAge;
    private int batchSize = 50;
    private Clock clock = Clock.systemUTC();
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private boolean compensationRetryEnabled = true;

    /** The saga runner whose compensation logic (and saga type) this sweeper re-drives. */
    public Builder<S> sagaRunner(SagaRunner<S> sagaRunner) {
      this.sagaRunner = sagaRunner;
      return this;
    }

    /** The saga store to poll for stuck {@code COMPENSATING} sagas (must be the runner's store). */
    public Builder<S> sagaStore(SagaStore sagaStore) {
      this.sagaStore = sagaStore;
      return this;
    }

    /**
     * Single-active-consumer coordinator. Optional; defaults to {@link SubscriptionLeadership#NOOP}
     * (always leader), which makes single- and multi-instance behavior identical.
     */
    public Builder<S> leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
      return this;
    }

    /**
     * How often to sweep, and the age cutoff below which a freshly-{@code COMPENSATING} saga is not
     * yet re-driven (so the sweeper does not race an in-flight event-path compensation). Defaults
     * to 60 seconds. Must be positive.
     */
    public Builder<S> retryInterval(Duration retryInterval) {
      this.retryInterval = retryInterval;
      return this;
    }

    /**
     * How long a saga may stay {@code COMPENSATING} before the sweeper gives up and terminalizes it
     * {@code FAULTED} (poison bound). Measured as {@code now - episode_claimed_at} — the durable,
     * fault-cycle-immutable claim instant of the episode (a row surfaced without one is refused,
     * never measured from {@code updated_at}). Never the refreshable {@code updated_at}: a
     * fault→replay cycle rewrites the row and would reset the clock, letting the episode dwell past
     * the command-inbox retention window (double refund on the next re-drive). Defaults to 1 hour.
     * Must be positive, and must stay strictly below {@code streamrune.inbox.retention-max-age}
     * (validated by {@code SagaRetentionValidator}).
     *
     * <p>The horizon is honoured only for a saga this sweeper actually re-drove in the preceding
     * cycle. A re-drive refused admission ({@code COMPENSATING_REFUSED}: an open circuit breaker, a
     * closing bus, an interceptor veto) is not an attempt, so a saga refused on every cycle never
     * reaches this bound and stays {@code COMPENSATING} indefinitely — unless {@link
     * #inboxRetentionMaxAge} is set, or a {@code SagaTimeoutRunner} for the saga type has {@code
     * maxCompensatingDwell} set. Configure one of them wherever a permanent refusal is possible.
     */
    public Builder<S> giveUpAfter(Duration giveUpAfter) {
      this.giveUpAfter = giveUpAfter;
      return this;
    }

    /**
     * Sets the command-inbox retention window (i.e. {@code streamrune.inbox.retention-max-age}) so
     * the sweeper can refuse to re-drive an episode whose dedup keys may already be pruned.
     * Effectively-once re-drive relies on a succeeded compensation's command-inbox row surviving
     * until the episode terminalizes; an episode that dwelled past this window — through an outage
     * longer than the window, or a {@code giveUpAfter} misconfigured above it (the boot-time {@code
     * SagaRetentionValidator} deliberately only WARNs there) — may have lost those rows to the
     * {@code InboxRetentionSweeper}, so a re-dispatch would MISS the inbox and RE-EXECUTE an
     * already-succeeded compensation (double refund). When the episode's age — measured from the
     * same durable anchor as the give-up horizon and the replayer's stale guard: {@code
     * episode_claimed_at}, which every row in a compensation episode carries — exceeds this window,
     * the sweeper CAS-FAULTs the saga via the existing give-up path <em>without dispatching
     * anything</em>, overriding the one-free-re-drive-after-restart grant: that grant exists for
     * recoverable downtime, not for an episode whose dedup keys are unprovably fresh. This is the
     * automatic-path companion of {@code SagaDeadLetterReplayer.Builder#inboxRetentionMaxAge}
     * ({@code STALE_COMPENSATION_BLOCKED}).
     *
     * <p>Optional; {@code null} (the default) disables the guard (the sweeper cannot reason about
     * key staleness), and zero/negative also disables it — that value means inbox pruning itself is
     * disabled, so keys are never swept and can never go stale. The framework auto-configs wire
     * {@code streamrune.inbox.retention-max-age} in.
     */
    public Builder<S> inboxRetentionMaxAge(Duration inboxRetentionMaxAge) {
      this.inboxRetentionMaxAge = inboxRetentionMaxAge;
      return this;
    }

    /** Maximum sagas re-driven per sweep cycle. Defaults to 50. Must be positive. */
    public Builder<S> batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Clock used to compute the due cutoff and give-up age. Defaults to {@link Clock#systemUTC()}.
     */
    public Builder<S> clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Whether the sweeper re-drives stuck compensation. Defaults to {@code true}. {@code false}
     * keeps the sweeper alive in sampling-only mode: it reports {@code
     * streamrune.saga.compensating} and {@code streamrune.saga.faulted_rows} every cycle — those
     * gauges are emitted nowhere else — but never takes the lease or re-drives. The integrations
     * wire {@code streamrune.saga.compensation-retry-enabled} here instead of withholding the
     * sweeper, so switching the re-drive off no longer blinds the saga backlog observability; they
     * skip the sweeper only when no {@code StreamRuneMetrics} is wired either, since it would then
     * have nothing to re-drive or sample. {@link #build()} validates {@code retryInterval} and
     * {@code giveUpAfter} in both modes.
     */
    public Builder<S> compensationRetryEnabled(boolean enabled) {
      this.compensationRetryEnabled = enabled;
      return this;
    }

    /** Metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. */
    public Builder<S> metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /** Builds the sweeper. All required fields must be set and durations must be positive. */
    public SagaCompensationRetrySweeper<S> build() {
      Objects.requireNonNull(sagaRunner, "sagaRunner is required");
      Objects.requireNonNull(sagaStore, "sagaStore is required");
      Objects.requireNonNull(clock, "clock is required");
      requirePositive(retryInterval, "retryInterval");
      requirePositive(giveUpAfter, "giveUpAfter");
      if (batchSize <= 0) {
        throw new IllegalArgumentException("batchSize must be positive");
      }
      return new SagaCompensationRetrySweeper<>(this);
    }

    private static void requirePositive(Duration d, String name) {
      if (d == null || d.isZero() || d.isNegative()) {
        throw new IllegalArgumentException(
            name + " must be non-null and positive (Duration.ZERO causes a tight spin loop)");
      }
    }
  }
}

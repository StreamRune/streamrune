package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCheckpointSaveException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionErrorClass;
import org.streamrune.core.projection.ProjectionErrorClassifier;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Cron-triggered projection runner. At each cron fire, drains the projection (loops batch
 * fetch+process until caught up to tail), updates the checkpoint, then sleeps until the next fire.
 * Mirrors {@link MultiProjectionRunner} shape.
 *
 * <p>Each registered projection runs on its own virtual thread.
 */
public final class ScheduledProjectionRunner implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ScheduledProjectionRunner.class);
  private static final long STOP_TIMEOUT_MS = 30_000;
  private static final String RUNNER_NAME = "ScheduledProjectionRunner";
  static final Duration DEFAULT_DRAIN_RETRY_INITIAL_BACKOFF = Duration.ofSeconds(1);
  static final Duration DEFAULT_DRAIN_RETRY_MAX_BACKOFF = Duration.ofSeconds(60);
  // Max consecutive TRANSIENT retries under the DLQ strategy before a batch is dead-lettered and
  // advanced past — bounds a genuinely stuck read-model so it does not retry the tick forever.
  static final int DEFAULT_MAX_TRANSIENT_RETRIES = 50;
  // Max consecutive deterministic read-poison failures at the same checkpoint before the projection
  // HALTs terminally instead of retrying an unreadable event every tick forever. A
  // small bound still tolerates a brief startup race (e.g. the event type not yet registered).
  static final int DEFAULT_MAX_READ_POISON_RETRIES = 5;
  // The no-op updater for a fenced empty-batch checkpoint advance. An empty-batch
  // executeAtomically takes the checkpoint row lock, runs the epoch fence + monotonic guard, and
  // stamps the epoch WITHOUT touching the read model, so the updater writes nothing.
  private static final AtomicBatchProcessor.ProjectionUpdater NO_OP_UPDATER = repository -> {};

  private final List<ScheduledProjectionRegistration> registrations;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final int fetchSize;
  private final int batchSize;
  private final AtomicBatchProcessor atomicProcessor;
  private final ProjectionDeadLetterStore deadLetterStore;
  private final StreamRuneMetrics metrics;
  private final SubscriptionLeadership leadership;
  private final ProjectionErrorClassifier classifier;
  private final SubscriptionHealthContributor healthContributor;
  // Tunable retry knobs — production defaults above; a package-private builder override keeps tests
  // that must exhaust the transient bound fast without slowing production behavior.
  private final Duration drainRetryInitialBackoff;
  private final Duration drainRetryMaxBackoff;
  private final int maxTransientRetries;
  private final int maxReadPoisonRetries;
  private final long stopTimeoutMs;

  private final ConcurrentHashMap<ProjectionName, RuntimeStatus> statuses =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Thread> threads = new ConcurrentHashMap<>();
  // One per projection thread: stop() interrupts through it, so the interrupt never lands in a
  // store write (see runHeldFromStop).
  private final ConcurrentHashMap<ProjectionName, InterruptDeferral> interrupts =
      new ConcurrentHashMap<>();
  // Per-CHUNK transient-retry budget, keyed by projection and the chunk's START offset. It is
  // fully DECOUPLED from the tick-level drainWithRetry counter: a transient DLQ
  // failure rethrows out of drain() and drainWithRetry re-drains, which re-reads the SAME chunk
  // first (its offset did not advance). This state — living on the runner, not on the discarded
  // drain() stack frame — carries the chunk's own accumulated transient count across those
  // re-drains and resets when the offset advances (a new chunk). A whole-tick read/offset failure
  // never touches it, so a read outage can no longer inflate a chunk's budget.
  private final ConcurrentHashMap<ProjectionName, ChunkTransientState> chunkTransientStates =
      new ConcurrentHashMap<>();
  // Per-projection count of consecutive deterministic read-poison failures at the current (stuck)
  // checkpoint. A successful read removes the entry, so the count only ever
  // accumulates across the tick-retries that a poison read triggers (the read fails first, so no
  // successful read resets it between attempts). Once it reaches maxReadPoisonRetries the
  // projection HALTs.
  private final ConcurrentHashMap<ProjectionName, Integer> readPoisonFailures =
      new ConcurrentHashMap<>();
  // Per projection: tells a commit rejection the next tick's re-read resolves from one that repeats
  // at an unchanged checkpoint (see CommitRejectionTracker).
  private final ConcurrentHashMap<ProjectionName, CommitRejectionTracker> commitRejections =
      new ConcurrentHashMap<>();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean stopping = new AtomicBoolean(false);
  // Set false the first time countPending() throws UnsupportedOperationException, so the
  // projection dead-letter depth gauge degrades gracefully without retrying every tick — mirrors
  // DeadLetterRetryRunner.backlogSamplingSupported.
  private volatile boolean deadLetterBacklogSamplingSupported = true;

  private ScheduledProjectionRunner(Builder b) {
    this.registrations = List.copyOf(b.registrations);
    this.eventStore = b.eventStore;
    this.offsetStore = b.offsetStore;
    this.fetchSize = b.fetchSize;
    this.batchSize = b.batchSize;
    this.atomicProcessor = b.atomicProcessor;
    this.deadLetterStore = b.deadLetterStore;
    this.metrics = b.metrics != null ? b.metrics : StreamRuneMetrics.NOOP;
    this.leadership = b.leadership != null ? b.leadership : SubscriptionLeadership.NOOP;
    this.classifier = b.classifier != null ? b.classifier : ProjectionErrorClassifier.DEFAULT;
    this.healthContributor = b.healthContributor;
    this.drainRetryInitialBackoff = b.drainRetryInitialBackoff;
    this.drainRetryMaxBackoff = b.drainRetryMaxBackoff;
    this.maxTransientRetries = b.maxTransientRetries;
    this.maxReadPoisonRetries = b.maxReadPoisonRetries;
    this.stopTimeoutMs = b.stopTimeoutMs;

    for (var reg : registrations) {
      statuses.put(
          reg.name(),
          new RuntimeStatus(
              ScheduledProjectionState.PENDING,
              null,
              reg.cron().nextFire(Instant.now()),
              0L,
              null));
    }
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Starts the runner — spawns a virtual thread per registered projection. */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("ScheduledProjectionRunner already started");
    }

    for (var reg : registrations) {
      // Register a lifecycle view of this projection with the health contributor (when wired) so
      // health() lists it and overallStatus() reflects its state: a scheduled projection that has
      // stopped or errored surfaces as DOWN instead of the endpoint reporting a silent UP. No-op
      // when no contributor is present.
      registerHealth(reg.name());
      // The one INFO line per registration — build() already accepted the pairing, so this
      // re-evaluation of the pure policy only names the verdict — and the delivery-mode gauge.
      var verdict =
          ProjectionDeliveryPolicy.require(
              reg.projection(),
              reg.deliveryMode(),
              atomicProcessor,
              RUNNER_NAME,
              reg.name().value());
      logger.info(
          "Projection '{}' delivery mode {} on {} — {}",
          LogSanitizer.sanitizeForLog(reg.name().value()),
          reg.deliveryMode(),
          ProjectionDeliveryPolicy.processorLabel(atomicProcessor),
          verdict.describe());
      recordDeliveryMode(reg.name(), reg.deliveryMode());
      interrupts.put(reg.name(), new InterruptDeferral());
      // Published before it starts, so the thread finds itself in the map (runHeldFromStop).
      Thread t =
          Thread.ofVirtual()
              .name("scheduled-projection-" + reg.name().value())
              .unstarted(() -> runLoop(reg));
      threads.put(reg.name(), t);
      t.start();
    }
  }

  /**
   * Registers a {@link SubscriptionLifecycle} view of the scheduled projection {@code name} with
   * the health contributor (when one is wired). The view derives its lifecycle state from the
   * runner's live {@link ScheduledProjectionState}, so a STOPPED/ERROR projection reports DOWN.
   * Pause/resume are unsupported (scheduled projections have no pausable subscription). No-op when
   * no contributor is present.
   */
  private void registerHealth(ProjectionName name) {
    if (healthContributor == null) {
      return;
    }
    healthContributor.register(name.value(), new ScheduledSubscriptionView(name));
  }

  /**
   * Read-only {@link SubscriptionLifecycle} adapter exposing a scheduled projection's state to the
   * health contributor. Only {@link #state()}, {@link #isRunning()}, and {@link #close()} are
   * meaningful; {@link #start()}, {@link #pause()}, and {@link #resume()} throw since the runner —
   * not the health contributor — owns the lifecycle.
   */
  private final class ScheduledSubscriptionView implements SubscriptionLifecycle {
    private final ProjectionName name;

    ScheduledSubscriptionView(ProjectionName name) {
      this.name = name;
    }

    @Override
    public SubscriptionLifecycleState state() {
      var rs = statuses.get(name);
      if (rs == null || rs.state == ScheduledProjectionState.STOPPED) {
        return SubscriptionLifecycleState.STOPPED;
      }
      // ERROR, RUNNING, SLEEPING, and PENDING all map to RUNNING for lifecycle purposes; the
      // contributor's consecutive-error count (fed by recordError) reports DEGRADED separately.
      return SubscriptionLifecycleState.RUNNING;
    }

    @Override
    public boolean isRunning() {
      return state() == SubscriptionLifecycleState.RUNNING;
    }

    @Override
    public void start() {
      throw new UnsupportedOperationException("lifecycle owned by ScheduledProjectionRunner");
    }

    @Override
    public void pause() {
      throw new UnsupportedOperationException("scheduled projections cannot be paused");
    }

    @Override
    public void resume() {
      throw new UnsupportedOperationException("scheduled projections cannot be resumed");
    }

    @Override
    public void close() {
      // No-op: the runner owns the lifecycle; unregistration happens in stop().
    }
  }

  private void runLoop(ScheduledProjectionRegistration reg) {
    setState(reg.name(), ScheduledProjectionState.SLEEPING);
    try {
      while (!stopping.get()) {
        Instant nextFire = reg.cron().nextFire(Instant.now());

        // Guard: Instant.MAX means the cron will never fire again.
        if (nextFire.equals(Instant.MAX)) {
          logger.warn(
              "Projection '{}' cron '{}' has no future fire — stopping loop.",
              reg.name().value(),
              reg.cron());
          break;
        }

        updateNextFire(reg.name(), nextFire);

        long sleepMs = Duration.between(Instant.now(), nextFire).toMillis();
        if (sleepMs > 0) {
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        if (stopping.get()) {
          break;
        }

        // Sample the fleet-wide projection dead-letter backlog once per tick, BEFORE the
        // leadership gate so any replica reports it (the backlog is a shared-store depth, and a
        // non-leader replica is exactly where an operator may look). Mirrors the saga sweeper,
        // which samples its faulted backlog ahead of its own leadership gate.
        sampleDeadLetterBacklog();

        // Single-active-consumer gate: only the leader for this name drains this tick. A non-leader
        // skips entirely (no read/process/advance) and re-checks on the next fire — takes over on
        // the leader's death. NOOP is always leader, so single-instance behavior is unchanged.
        Optional<SubscriptionLeadership.Lease> tickLease =
            leadership.tryAcquire(reg.name().value());
        if (tickLease.isEmpty()) {
          logger.info(
              "Projection '{}' is STANDBY — another instance holds leadership; skipping this tick",
              reg.name().value());
          setState(reg.name(), ScheduledProjectionState.SLEEPING);
          continue;
        }

        // Durably stamp the held epoch on the checkpoint row BEFORE this tick reads or
        // processes anything. The epoch fence compares against the epoch STORED on the row, which
        // only a committed advance used to stamp — for a cron-cadence runner the takeover window
        // (acquire -> first committed advance) could span a whole idle cron period, during which
        // the superseded leader's writes (including advanceCheckpointFenced's empty-batch
        // forward-jumps) passed the fence. Stamping every leader tick is an idempotent 0-row
        // no-op once stamped (the store's WHERE only fires on a strictly newer epoch). FAIL-CLOSED
        // on a stamp failure: skip the tick rather than process with the fence inert — the next
        // fire retries; with the DB down, the drain could not have proceeded anyway.
        StampOutcome stamp = stampTakeoverEpoch(reg.name(), tickLease.get().epoch());
        if (stamp == StampOutcome.FATAL) {
          // The acquired epoch is below the stored one AND we still hold the lease
          // (the superseded case routes to RETRY/sleep), so the lease table and the
          // checkpoint have diverged and no tick can ever commit past the fence.
          // stampTakeoverEpoch already recorded the ERROR state and lastError; leave
          // the loop so the finally takes the markTerminalError branch, exactly as for an
          // unrecoverable drain error, instead of skipping ticks forever while /health reports the
          // projection healthy.
          break;
        }
        if (stamp == StampOutcome.RETRY) {
          setState(reg.name(), ScheduledProjectionState.SLEEPING);
          continue;
        }

        setState(reg.name(), ScheduledProjectionState.RUNNING);
        long batchesProcessed;
        try {
          batchesProcessed = drainWithRetry(reg);
        } catch (Throwable t) {
          // drainWithRetry retries RuntimeExceptions internally, so only an unrecoverable
          // Error (AssertionError under -ea, NoClassDefFoundError, StackOverflowError, OOME) from
          // the drain/process path escapes here. Terminate as ERROR — populate lastError and let
          // the finally take the markTerminalError branch (health DOWN) — instead of unwinding to
          // the finally's clean STOPPED branch, which would be indistinguishable from a deliberate
          // stop. Mirrors ContinuousProjectionRunner's catch-Throwable discipline.
          setError(reg.name(), t);
          logger.error(
              "Projection '{}' terminated on an unrecoverable error", reg.name().value(), t);
          break;
        }
        recordFire(reg.name(), Instant.now(), batchesProcessed);

        if (statuses.get(reg.name()).state == ScheduledProjectionState.ERROR) {
          break;
        }

        setState(reg.name(), ScheduledProjectionState.SLEEPING);
      }
    } finally {
      // The loop terminated (stop, cron-exhausted, or HALT error) — resign leadership so a healthy
      // standby replica can take over immediately instead of waiting for process exit / context
      // shutdown to release the lease. NOOP.resign is a no-op, so single-instance
      // behavior is unchanged.
      //
      // The SubscriptionLeadership contract requires resign to be no-throw for transient
      // failures — but the bookkeeping below must survive even a contract-breaking implementation.
      // A resign throw used to truncate this finally: the STOPPED transition (or the terminal-DOWN
      // health marking for an ERROR exit) was skipped, so the status view kept showing a dead
      // projection as SLEEPING/RUNNING — and the throw replaced any in-flight failure, mislabeling
      // the death in logs. A RuntimeException from resign is logged and swallowed; the
      // state/health bookkeeping runs in a nested finally so even an Error unwinding out of resign
      // completes it before propagating.
      try {
        // Resign is a store write: on a thread interrupted by stop() it would fail at the socket
        // and leave the lease to expire at its TTL. Clear the flag around it and restore it.
        runHeldFromStop(
            reg.name(),
            () -> {
              leadership.resign(reg.name().value());
              return null;
            });
      } catch (RuntimeException resignFailure) {
        logger.warn(
            "Projection '{}' could not resign leadership on termination; the lease will expire"
                + " naturally at its TTL",
            reg.name().value(),
            resignFailure);
      } finally {
        // Only mark STOPPED if not already in ERROR
        var current = statuses.get(reg.name());
        if (current == null || current.state != ScheduledProjectionState.ERROR) {
          setState(reg.name(), ScheduledProjectionState.STOPPED);
        } else if (healthContributor != null) {
          // Permanent ERROR/halt exit (HALT strategy set state=ERROR and the loop broke): keep the
          // projection registered and force its health terminally DOWN so overallStatus() — and
          // the /health endpoint — stays DOWN, not merely DEGRADED via lag/error-count. A clean
          // stop() unregisters instead; a STANDBY tick stays SLEEPING and never reaches this ERROR
          // branch.
          healthContributor.markTerminalError(reg.name().value());
        }
      }
    }
  }

  /**
   * Durably stamps the held leadership epoch on the projection's checkpoint row (via {@link
   * AtomicBatchProcessor#stampFencingEpoch}) so the epoch fence rejects the superseded leader's
   * writes from the moment of takeover instead of only after this leader's first committed advance.
   * Called on every leader tick before the drain — idempotent and monotonic in the store, so a
   * re-stamp of an already-stamped epoch is a 0-row no-op. Epoch {@code 0} ({@link
   * SubscriptionLeadership#NOOP} — unfenced single-node) skips the write entirely.
   *
   * <p>A transient DB blip is {@link StampOutcome#RETRY} — skip the tick, the next fire retries. A
   * {@link ProjectionEpochRegressionException} is {@link StampOutcome#FATAL}: leadership is handing
   * out an epoch BELOW the one stored on the checkpoint row, so every commit this runner could make
   * is guaranteed to be rejected by the fence. Skipping the tick forever would freeze the read
   * model silently at cron cadence, so the registration terminates as ERROR and its health goes
   * terminally DOWN.
   *
   * <p><b>An epoch regression has TWO shapes, and only one of them is a bug.</b> The stored epoch
   * is higher than ours either because the lease table diverged from the checkpoint ({@code
   * subscription_leases} cleared or restored on its own) or because a NEWER leader legitimately
   * took over and stamped its higher epoch while this tick stalled between {@code tryAcquire} and
   * this call: a GC pause, a VM freeze, or a {@code dataSource.getConnection()} wait longer than
   * the lease TTL. The second is an ordinary failover — the exact race the fence exists to handle
   * gracefully — and terminating on it removes a healthy replica from the failover pool for good,
   * with health DOWN, so nothing takes over when the new leader later dies. Leadership is the
   * discriminator, and it is the same question this loop already asks one instruction earlier:
   * superseded ⇒ SLEEP this tick, exactly as if {@code tryAcquire} had returned empty (a later fire
   * re-acquires at a fresh higher epoch once the current leader goes away, and the stamp then
   * succeeds); still holding the lease ⇒ the lease table can hand this name nothing higher, so it
   * is the divergence and no retry can help. A leadership probe that cannot answer keeps the loud
   * verdict — see {@link #stillLeader}.
   */
  private StampOutcome stampTakeoverEpoch(ProjectionName projectionName, long epoch) {
    if (epoch == 0L) {
      return StampOutcome.STAMPED;
    }
    try {
      runHeldFromStop(
          projectionName,
          () -> {
            atomicProcessor.stampFencingEpoch(projectionName, epoch);
            return null;
          });
      return StampOutcome.STAMPED;
    } catch (ProjectionEpochRegressionException regression) {
      if (!stillLeader(projectionName)) {
        logger.info(
            "Projection '{}' was superseded before its takeover stamp landed: it was handed epoch"
                + " {} but the checkpoint row already stores epoch {}, written by the newer leader"
                + " that took over while this tick was stalled. This is an ordinary failover —"
                + " sleeping this tick, exactly as if the acquire itself had lost. (A regression"
                + " observed while this replica STILL held its lease is the lease/checkpoint"
                + " divergence, and terminates the registration instead.)",
            projectionName.value(),
            regression.acquiredEpoch(),
            regression.storedEpoch());
        return StampOutcome.RETRY;
      }
      logger.error(
          "Projection '{}' cannot arm its epoch fence: leadership handed out epoch {} but the"
              + " checkpoint row already stores epoch {}. Every commit would be rejected by the"
              + " fence, so every tick would drain nothing and the read model would freeze"
              + " silently — terminating this projection as ERROR and reporting health DOWN"
              + " instead. {}",
          projectionName.value(),
          regression.acquiredEpoch(),
          regression.storedEpoch(),
          regression.getMessage());
      setError(projectionName, regression);
      return StampOutcome.FATAL;
    } catch (RuntimeException e) {
      logger.warn(
          "Projection '{}' could not stamp takeover epoch {} on the checkpoint row — skipping"
              + " this tick rather than draining with the epoch fence inert; the next fire"
              + " retries",
          projectionName.value(),
          epoch,
          e);
      return StampOutcome.RETRY;
    }
  }

  /**
   * Whether this runner still holds the lease for {@code projectionName}. Used only to DIAGNOSE an
   * epoch regression, never as a safety gate — nothing is drained on either branch. A {@link
   * SubscriptionLeadership#current} that throws therefore answers "assume still leader", which
   * keeps the loud, terminal verdict as the default so a broken leadership probe cannot silence a
   * genuine lease/checkpoint divergence into an INFO-level skipped tick.
   */
  private boolean stillLeader(ProjectionName projectionName) {
    try {
      return leadership.current(projectionName.value()).isPresent();
    } catch (RuntimeException probeFailed) {
      logger.warn(
          "Projection '{}' could not re-check leadership while classifying an epoch regression;"
              + " treating the regression as the lease/checkpoint divergence (the loud verdict)"
              + " rather than assuming a benign supersession",
          projectionName.value(),
          probeFailed);
      return true;
    }
  }

  /** Outcome of {@link #stampTakeoverEpoch}. */
  private enum StampOutcome {
    /** The fence is durably armed at this leader's epoch; the tick may drain. */
    STAMPED,
    /**
     * Skip this tick; the next fire retries. Two causes, one response: a transient stamp failure
     * (DB blip), and an epoch regression observed after this replica was legitimately superseded —
     * for which sleeping IS the correct answer, since a later fire re-acquires at a fresh higher
     * epoch once the current leader goes away.
     */
    RETRY,
    /**
     * The acquired epoch is below the stored one AND this runner still holds its lease, so the
     * lease table and the checkpoint have diverged and no retry can help.
     */
    FATAL
  }

  /**
   * Runs {@link #drain(ScheduledProjectionRegistration)} and retries transient failures (event
   * store reads, offset store access, dead-letter writes) with capped exponential backoff instead
   * of letting the projection thread die. Processing failures inside the drain are handled by the
   * registration's error strategy and do not reach this method — only infrastructure errors do.
   * While retrying, the failure is recorded in the status as {@code lastError} and the state stays
   * RUNNING; on success the transient error is cleared (unless the strategy set ERROR).
   */
  private long drainWithRetry(ScheduledProjectionRegistration reg) {
    // Tick-level counter — drives whole-drain (tick) backoff for genuine infrastructure failures
    // (event-store read, offset-store access, or a DLQ-write blip that rethrows out of drain). It
    // is NO LONGER used to seed any chunk's transient-retry budget: the per-chunk
    // budget lives in chunkTransientStates and is position-local, so an infra outage that inflates
    // this counter cannot cause a chunk to be dead-lettered on its first transient failure.
    int consecutiveFailures = 0;
    while (!stopping.get()) {
      try {
        long batches = drain(reg);
        if (consecutiveFailures > 0) {
          logger.info(
              "Projection '{}' drain recovered after {} failed attempt(s)",
              reg.name().value(),
              consecutiveFailures);
        }
        clearTransientError(reg.name());
        return batches;
      } catch (RuntimeException e) {
        consecutiveFailures++;
        Duration backoff =
            ResilientPollLoop.backoffDelay(
                drainRetryInitialBackoff, drainRetryMaxBackoff, consecutiveFailures);
        logger.error(
            "Projection '{}' drain failed ({} consecutive failure(s)); retrying in {} ms",
            reg.name().value(),
            consecutiveFailures,
            backoff.toMillis(),
            e);
        recordTransientError(reg.name(), e);
        try {
          Thread.sleep(backoff.toMillis());
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
          return 0;
        }
      }
    }
    return 0;
  }

  private long drain(ScheduledProjectionRegistration reg) {
    long batches = 0;
    while (!stopping.get()) {
      // Re-check leadership before reading the next page: leadership can be lost mid-drain (leader
      // connection blip / failover). If we are no longer the leader, stop draining this tick so a
      // stale leader cannot advance offsets — the drain gates every offset-advancing write on live
      // leadership, matching ContinuousProjectionRunner's per-advance re-check. NOOP is always
      // leader, so this is a no-op for single-instance deployments.
      if (leadership.current(reg.name().value()).isEmpty()) {
        logger.info(
            "Projection '{}' lost leadership before reading the next page; stopping this drain",
            reg.name().value());
        return batches;
      }
      GlobalOffset currentOffset = offsetStore.getLastOffset(reg.name());
      List<EventEnvelope> events;
      try {
        events = eventStore.readGlobalStream(currentOffset, fetchSize);
        // A successful read clears any accumulated read-poison streak.
        readPoisonFailures.remove(reg.name());
      } catch (RuntimeException readError) {
        // Distinguish a deterministic read/deserialization POISON (unregistered event type,
        // corrupt/schema-incompatible payload) from a genuine TRANSIENT infra blip. A
        // poison read fails identically every tick; left as "transient" it retries forever and the
        // read model never advances, never reaching the error strategy/DLQ (the failure is before
        // process() runs). Bound it: once exhausted, HALT terminally with a distinct signal.
        if (ReadPoisonClassifier.isDeterministicReadPoison(readError)) {
          int failures = readPoisonFailures.merge(reg.name(), 1, Integer::sum);
          if (failures >= maxReadPoisonRetries) {
            readPoisonFailures.remove(reg.name());
            var poison = new ProjectionReadPoisonException(reg.name().value(), failures, readError);
            logger.error(
                "Projection '{}' halting on a read/deserialization poison at checkpoint {} after {}"
                    + " attempt(s) — the offending event can never be read on retry; not retrying"
                    + " the tick forever",
                reg.name().value(),
                currentOffset.value(),
                failures,
                readError);
            setError(reg.name(), poison);
            return batches; // runLoop observes ERROR and breaks the loop
          }
        }
        // Transient, or poison still under the bound: rethrow so drainWithRetry backs off and
        // retries the tick. A transient read never touches readPoisonFailures, so a DB outage can
        // never push a chunk toward the poison bound.
        throw readError;
      }

      if (events.isEmpty()) {
        break;
      }

      // Chunk by batchSize
      for (int i = 0; i < events.size(); i += batchSize) {
        var chunk = events.subList(i, Math.min(i + batchSize, events.size()));
        long start = System.nanoTime();
        try {
          // Re-check leadership BEFORE the offset-advancing commit: the
          // atomic commit below advances the offset in the SAME transaction as the read-model
          // write, a forward checkpoint move the monotonic guard cannot reject, so a stale leader
          // must NOT commit. Abandon and stop draining without recording success; the next leader
          // re-reads from the last committed offset. NOOP is always leader (epoch 0, unfenced), so
          // this is a no-op for single-instance deployments.
          Optional<SubscriptionLeadership.Lease> lease = leadership.current(reg.name().value());
          if (lease.isEmpty()) {
            logger.info(
                "Projection '{}' lost leadership before a checkpoint advance; abandoning batch and"
                    + " stopping this drain",
                reg.name().value());
            return batches;
          }
          // Route chunk processing through the AtomicBatchProcessor so
          // the read-model write and the offset checkpoint commit — or roll back — together,
          // exactly like ContinuousProjectionRunner/PollingProjectionRunner. A
          // crash between processing and the checkpoint can no longer redeliver-and-double-apply
          // the chunk, and the in-transaction monotonic/overlap guard rejects a superseded
          // split-brain leader. What the updater receives is decided per registration by its
          // delivery mode: a TRANSACTIONAL_LOCAL / EXTERNAL_EFFECT registration gets the
          // transaction-scoped repository, an AT_LEAST_ONCE_IDEMPOTENT one gets null and writes
          // outside this transaction.
          GlobalOffset newOffset = chunk.getLast().globalOffset();
          // Thread the held lease's fencing epoch into the atomic commit so a transactional
          // processor rejects a superseded leader's write (epoch below the stored one). NOOP's
          // epoch 0 is unfenced.
          // Held from stop()'s interrupt: one landing in the commit would fail it at the socket and
          // hand an intact chunk to the error strategy. stop() waits for it.
          runHeldFromStop(
              reg.name(),
              () -> {
                // Only a registration that writes through the handed repository has a read model
                // in the processor's store to prepare.
                if (reg.deliveryMode().writesInCheckpointTransaction()) {
                  atomicProcessor.prepareReadModel(reg.name());
                }
                atomicProcessor.executeAtomically(
                    reg.name(),
                    chunk,
                    newOffset,
                    lease.get().epoch(),
                    txRepository ->
                        reg.projection()
                            .process(
                                chunk,
                                reg.deliveryMode().writesInCheckpointTransaction()
                                    ? txRepository
                                    : null),
                    offsetStore);
                return null;
              });
          commitRejectionsFor(reg.name()).reset();
          recordProcessed(reg.name(), chunk.size());
          recordHealthSuccess(reg.name());
          batches++;
          // A chunk advanced cleanly — clear the per-chunk transient budget so the next chunk
          // starts fresh. Keyed by start offset, so this is a no-op if no failure
          // was recorded for it.
          chunkTransientStates.remove(reg.name());
        } catch (ProjectionCommitFencedException fenced) {
          // The atomic commit was rejected by executeAtomically — the epoch fence (a newer
          // leader took over the lease), the first-offset overlap guard, or the monotonic guard.
          // This is a LEADERSHIP/CAS signal, NOT a projection error: it must NEVER reach
          // handleError, because SKIP (and DLQ) would convert the REJECTION into a forward
          // checkpoint advance by the very stale leader the fence just rejected — silently,
          // permanently skipping the un-applied events. Mirror the leadership-lost handling above:
          // abandon this drain WITHOUT advancing and WITHOUT recording a processing failure; the
          // next tick re-reads from the fresh committed checkpoint (and a genuine monotonic/overlap
          // rejection resolves the same way — reload the checkpoint, retry from fresh state). A
          // rejection that repeats at an unchanged checkpoint halts the projection instead:
          // haltIfRejectionRepeats records the ERROR and runLoop breaks on it.
          if (!haltIfRejectionRepeats(reg.name(), fenced)) {
            logger.info(
                "Projection '{}' atomic commit rejected ({} guard) — abandoning drain without"
                    + " advancing, will re-read from the checkpoint next tick: {}",
                LogSanitizer.sanitizeForLog(reg.name().value()),
                fenced.guard(),
                fenced.getMessage());
          }
          return batches;
        } catch (ProjectionCheckpointSaveException saveFailed) {
          // Applied, not checkpointed, store unavailable. End this drain without advancing and
          // without the error strategy; the next tick re-reads from the checkpoint.
          recordFailed(reg.name());
          recordHealthError(reg.name());
          logger.warn(
              "Projection '{}' applied the batch up to {} but the checkpoint save failed; the next"
                  + " tick re-reads from the checkpoint: {}",
              LogSanitizer.sanitizeForLog(reg.name().value()),
              saveFailed.offset().value(),
              String.valueOf(saveFailed.getCause()));
          return batches;
        } catch (RuntimeException e) {
          recordFailed(reg.name());
          recordHealthError(reg.name());
          // Per-CHUNK transient budget: the count is scoped to THIS chunk's start
          // offset, NOT to the tick-level counter. Read the accumulated count for this chunk (0 for
          // a fresh chunk), and persist the incremented count BEFORE calling handleError so it
          // survives handleError's rethrow of a TRANSIENT-under-bound failure (which unwinds this
          // whole stack frame; drainWithRetry then re-drains and re-reads this same chunk).
          GlobalOffset chunkStart = chunk.getFirst().globalOffset();
          // This failure's 1-based attempt count. ContinuousProjectionRunner increments
          // BEFORE its bound check (first failure = 1), so pass the POST-increment count here too —
          // handleError's `< maxTransientRetries` bounds and the dead-letter attempts field are
          // then off-by-one-free (halt on the Nth failure where N == the bound, not N+1).
          int chunkFailures = transientFailuresFor(reg.name(), chunkStart) + 1;
          chunkTransientStates.put(reg.name(), new ChunkTransientState(chunkStart, chunkFailures));
          // handleError rethrows a TRANSIENT DLQ error (while chunkFailures < the bound) so
          // drainWithRetry retries the whole tick with backoff — the offset is NOT advanced and no
          // dead-letter is written; on the next drain this same chunk is re-read with its carried
          // count. It also gates every SKIP/DLQ offset advance on live leadership; a
          // lost lease returns false so we stop this drain without advancing.
          if (!handleError(reg, chunk, e, chunkFailures)) {
            // Non-throwing false: HALT (terminal) or leadership lost (chunk abandoned, offset not
            // advanced). Clear the budget — neither is a transient processing failure that should
            // consume this chunk's budget; a leadership churn must not burn it, and a HALT is dead.
            chunkTransientStates.remove(reg.name());
            return batches;
          }
          batches++;
          // A SKIP/dead-letter advanced past this chunk — clear the per-chunk budget so the next
          // chunk gets its OWN full budget instead of inheriting this chunk's.
          chunkTransientStates.remove(reg.name());
        } finally {
          recordDuration(reg.name(), System.nanoTime() - start);
        }
      }

      // A short page is not the end of the stream: a store may return fewer events than asked
      // for while more are committed, and readGlobalStream withholds everything from the append
      // still in flight at the tail onwards. The offset advanced past this page, so the loop reads
      // again from there within THIS drain and ends only when a read returns nothing (the
      // isEmpty() break above).
    }
    return batches;
  }

  /**
   * Returns the transient-failure count already accumulated for the chunk starting at {@code
   * chunkStart}, or {@code 0} if this is a fresh chunk (no recorded state, or the recorded state
   * belongs to a different — already-advanced — chunk). Position-local by design: a stale entry for
   * a different start offset does not leak into a new chunk's budget.
   */
  private int transientFailuresFor(ProjectionName name, GlobalOffset chunkStart) {
    ChunkTransientState state = chunkTransientStates.get(name);
    return state != null && state.chunkStart().equals(chunkStart) ? state.failures() : 0;
  }

  /**
   * Per-chunk transient-retry budget: the START offset of the chunk it counts for and the number of
   * transient failures that chunk has accumulated. Keyed by projection in {@link
   * #chunkTransientStates}; carried across re-drains for the SAME chunk (its offset does not
   * advance) and cleared when the offset advances.
   */
  private record ChunkTransientState(GlobalOffset chunkStart, int failures) {}

  /**
   * Returns {@code true} if drain should continue; {@code false} if it should halt (HALT strategy).
   *
   * <p><b>Note:</b> Unlike {@code ContinuousProjectionRunner} where DLQ halts after writing, this
   * runner's DLQ strategy advances past the bad batch and continues draining. Scheduled projections
   * fire periodically so resilience (skip bad batch, keep processing) is preferred over halting.
   * Advancing leaves a hole in the read model — once the cause is fixed, replay the dead-lettered
   * range with {@link ProjectionDeadLetterReplayer} to restore completeness.
   *
   * <p><b>Poison-vs-transient.</b> Under DLQ the error is classified: a POISON batch is
   * dead-lettered and advanced past (the current behavior); a TRANSIENT infra blip is
   * <em>rethrown</em> (never dead-lettered, offset not advanced) so {@link #drainWithRetry} backs
   * off and retries the whole tick — a blip no longer punches a permanent hole. Only after {@link
   * #DEFAULT_MAX_TRANSIENT_RETRIES} transient failures OF THE SAME CHUNK is the batch dead-lettered
   * and advanced past, so a genuinely stuck read-model does not spin forever. The transient budget
   * is per-CHUNK and fully DECOUPLED from the tick-level {@code consecutiveFailures} counter : it
   * is tracked in {@link #chunkTransientStates} keyed by the chunk's start offset, carries across
   * the re-drains a rethrow triggers (the offset does not advance, so the same chunk is re-read),
   * and resets once the offset advances (cleanly, on SKIP, or after a dead-letter). So a chunk that
   * exhausts the bound never robs the next chunk of its own full budget, and a whole-tick
   * read/offset outage — which drives only the tick counter — never consumes any chunk's transient
   * budget.
   *
   * <p><b>Failed dead-letter write.</b> If the dead-letter STORE WRITE itself throws (the DLQ store
   * is down), that is a TRANSIENT infra failure: the offset is <em>not</em> advanced and the
   * exception is rethrown so {@link #drainWithRetry} backs off and retries the whole tick — the
   * poison range is re-read and re-attempted once the store recovers rather than being silently
   * lost from the read model.
   */
  private boolean handleError(
      ScheduledProjectionRegistration reg,
      List<EventEnvelope> chunk,
      RuntimeException error,
      int consecutiveTransientFailures) {
    switch (reg.errorStrategy()) {
      case HALT -> {
        // Bounded backoff-retry parity with ContinuousProjectionRunner, which retries
        // a HALT-strategy failure from the checkpoint up to MAX_CONSECUTIVE_ERRORS before halting.
        // A single transient read-model blip must self-heal, not permanently halt the projection
        // (the OLD branch halted on the FIRST failure). Retry the tick from the checkpoint —
        // rethrow so drainWithRetry backs off and re-drains; the offset did not advance, so the
        // SAME chunk is re-read with its carried per-chunk count — until maxTransientRetries
        // consecutive failures OF THE SAME CHUNK, then HALT terminally. Unlike DLQ, HALT does not
        // classify: every failure counts toward the bound, matching the CONTINUOUS HALT path.
        if (consecutiveTransientFailures < maxTransientRetries) {
          logger.warn(
              "Projection '{}' HALT error (attempt {}/{}) — retrying the tick from the checkpoint,"
                  + " not halting yet: {}",
              reg.name().value(),
              consecutiveTransientFailures,
              maxTransientRetries,
              error.getMessage(),
              error);
          throw error;
        }
        logger.error(
            "Projection '{}' halted after {} consecutive failure(s) of the same chunk",
            reg.name().value(),
            consecutiveTransientFailures);
        setError(reg.name(), error);
        return false;
      }
      case SKIP -> {
        // Re-check leadership before the SKIP checkpoint advance: a forward move the
        // monotonic guard cannot reject, so a stale leader must NOT advance. Return false to stop
        // draining without advancing; the next leader re-reads (and re-skips) from the checkpoint.
        Optional<SubscriptionLeadership.Lease> lease = leadership.current(reg.name().value());
        if (lease.isEmpty()) {
          logger.info(
              "Projection '{}' lost leadership before a SKIP checkpoint advance; abandoning batch",
              reg.name().value());
          return false;
        }
        if (stopping.get()) {
          return abandonWhileStopping(reg.name()); // the failure may be the stop itself
        }
        logger.warn(
            "Projection '{}' SKIP on batch error: {}",
            reg.name().value(),
            error.getMessage(),
            error);
        // Route the checkpoint-only advance through the EPOCH-FENCED empty-batch commit
        // rather than a raw saveOffset — see advanceCheckpointFenced. A fence rejection means a
        // newer leader has taken over: abandon this drain WITHOUT advancing (return false).
        return advanceCheckpointFenced(
            reg.name(), chunk.getLast().globalOffset(), lease.get().epoch());
      }
      case DLQ -> {
        ProjectionErrorClass errorClass = classifier.classify(error);
        if (errorClass == ProjectionErrorClass.TRANSIENT
            && consecutiveTransientFailures < maxTransientRetries) {
          // TRANSIENT: do NOT dead-letter and do NOT advance the offset — rethrow so drainWithRetry
          // backs off and retries this tick. The offset stays put, so the batch is re-read next
          // attempt.
          logger.warn(
              "Projection '{}' DLQ TRANSIENT error (attempt {}/{}) — retrying the tick, not"
                  + " dead-lettering: {}",
              reg.name().value(),
              consecutiveTransientFailures,
              maxTransientRetries,
              error.getMessage(),
              error);
          throw error;
        }
        // POISON, or TRANSIENT bound exhausted: dead-letter the range and advance past it.
        if (errorClass == ProjectionErrorClass.TRANSIENT) {
          logger.warn(
              "Projection '{}' DLQ TRANSIENT error exhausted the retry bound ({}); dead-lettering"
                  + " and continuing",
              reg.name().value(),
              maxTransientRetries);
        } else {
          logger.warn(
              "Projection '{}' DLQ POISON error — dead-lettering after first failure and"
                  + " continuing: {}",
              reg.name().value(),
              error.getMessage(),
              error);
        }
        // Re-check leadership before dead-lettering + advancing past the range:
        // advancing is a forward checkpoint move the monotonic guard cannot reject, so a stale
        // leader must NOT advance (and must not write a spurious dead-letter). Return false to stop
        // draining; the next leader re-reads from the last committed checkpoint.
        Optional<SubscriptionLeadership.Lease> lease = leadership.current(reg.name().value());
        if (lease.isEmpty()) {
          logger.info(
              "Projection '{}' lost leadership before a DLQ checkpoint advance; abandoning batch",
              reg.name().value());
          return false;
        }
        if (stopping.get()) {
          return abandonWhileStopping(reg.name()); // the failure may be the stop itself
        }
        GlobalOffset fromOffset = chunk.getFirst().globalOffset();
        GlobalOffset toOffset = chunk.getLast().globalOffset();
        var entry =
            new ProjectionDeadLetterEntry(
                reg.name(),
                fromOffset,
                toOffset,
                chunk.size(),
                error.getClass().getName(),
                // Persisted free text, sanitized at the projection_dead_letters sink.
                LogSanitizer.sanitizeFreeText(error.getMessage()),
                consecutiveTransientFailures > 0 ? consecutiveTransientFailures : 1,
                Instant.now());
        try {
          runHeldFromStop(
              reg.name(),
              () -> {
                deadLetterStore.save(entry);
                return null;
              });
        } catch (RuntimeException dlqError) {
          // The dead-letter STORE WRITE itself failed (DLQ store down) — a TRANSIENT infra failure,
          // NOT a reason to advance. Do NOT saveOffset; rethrow so drainWithRetry backs off and
          // retries the whole tick (identical to a transient read/offset-store failure). The offset
          // stays put, so the poison range is re-read and re-attempted once the store recovers and
          // is NEVER silently lost from the read model. The WARN carries the context only, not the
          // throwable: drainWithRetry ERROR-logs this same exception with its stack trace on every
          // retry, so attaching it here printed two stack traces per attempt.
          logger.warn(
              "Projection '{}' DLQ dead-letter WRITE failed — transient infra failure; not"
                  + " advancing, retrying the tick",
              reg.name().value());
          throw dlqError;
        }
        recordDeadLettered(reg.name());
        // Fence the checkpoint-only advance (empty-batch executeAtomically) so a stale
        // leader cannot forward-jump the checkpoint past events a newer leader has not applied. If
        // fenced out, the dead-letter stays (idempotent upsert on (projection_name, from_offset),
        // so the next leader re-dead-letters as a no-op) but the checkpoint is NOT advanced by the
        // stale leader — abandon this drain (return false).
        return advanceCheckpointFenced(reg.name(), toOffset, lease.get().epoch());
      }
      default -> {
        return false;
      }
    }
  }

  /**
   * Advances the checkpoint past a SKIP'd or dead-lettered chunk through the EPOCH-FENCED
   * empty-batch commit ({@code atomicProcessor.executeAtomically(name, List.of(), toOffset,
   * fencingEpoch, ...)}) rather than a raw {@link OffsetStore#saveOffset}.
   *
   * <p>A raw forward {@code saveOffset} of the skip/DLQ endpoint cannot be rejected by the
   * monotonic guard, so a stale leader still inside its local {@code
   * leadership.current()}-staleness window could advance the checkpoint past events a newer leader
   * has not yet applied — silently skipping them from the read model (no dead-letter under SKIP).
   * The empty-batch commit takes the checkpoint row lock, runs the epoch fence ({@code
   * rejectStaleEpoch}) and monotonic guard, and stamps the epoch WITHOUT any read-model write.
   * Under {@code nonAtomicAtLeastOnce()} (single-node, {@code fencingEpoch == 0}) this is the plain
   * {@code saveOffset}.
   *
   * <p>Returns {@code true} if the advance committed (drain continues past the chunk); {@code
   * false} if the commit was fenced out ({@link OptimisticLockException} — a newer leader took over
   * the lease, or the monotonic guard rejected a non-advancing save), in which case the caller
   * abandons this drain WITHOUT advancing and the next leader re-reads from the fresh committed
   * checkpoint. A fence rejection is a LEADERSHIP/CAS signal, never a transient blip, so it is not
   * retried. Any OTHER commit failure (a transient store blip) propagates so {@link
   * #drainWithRetry} backs off and retries the whole tick from the un-advanced checkpoint — the
   * pre-existing at-least-once behavior.
   */
  private boolean advanceCheckpointFenced(
      ProjectionName name, GlobalOffset toOffset, long fencingEpoch) {
    try {
      runHeldFromStop(
          name,
          () -> {
            atomicProcessor.executeAtomically(
                name, List.of(), toOffset, fencingEpoch, NO_OP_UPDATER, offsetStore);
            return null;
          });
      commitRejectionsFor(name).reset();
      return true;
    } catch (ProjectionCommitFencedException fenced) {
      if (!haltIfRejectionRepeats(name, fenced)) {
        logger.info(
            "Projection '{}' SKIP/DLQ checkpoint advance to {} rejected ({} guard) — abandoning"
                + " without advancing; the next tick re-reads from the fresh checkpoint: {}",
            LogSanitizer.sanitizeForLog(name.value()),
            toOffset.value(),
            fenced.guard(),
            fenced.getMessage());
      }
      return false;
    }
  }

  private CommitRejectionTracker commitRejectionsFor(ProjectionName name) {
    return commitRejections.computeIfAbsent(name, n -> new CommitRejectionTracker());
  }

  /**
   * Decides whether a commit rejection is one the next tick's re-read resolves, or one that repeats
   * on every tick. A rejection by the epoch fence names a newer leader and is never counted. An
   * overlap or monotonic rejection means the processor's checkpoint is ahead of the one this runner
   * read: tolerated once (a commit whose acknowledgement was lost is rejected on its retry, and the
   * re-read starts after the moved checkpoint), and on a second consecutive one with the runner's
   * checkpoint unchanged the projection is put in {@code ERROR} with a {@link
   * ProjectionCheckpointDivergedException} as its last error, so {@code runLoop} leaves the loop
   * and marks it terminally DOWN.
   *
   * @return {@code true} when the projection was halted
   */
  private boolean haltIfRejectionRepeats(
      ProjectionName name, ProjectionCommitFencedException fenced) {
    if (fenced.guard() == ProjectionCommitFencedException.Guard.EPOCH_FENCE) {
      commitRejectionsFor(name).reset();
      return false;
    }
    long checkpoint;
    try {
      checkpoint = offsetStore.getLastOffset(name).value();
    } catch (RuntimeException readFailed) {
      // Whether the checkpoint moved is unknown; the next rejection decides.
      return false;
    }
    if (!commitRejectionsFor(name).repeatsAt(checkpoint)) {
      return false;
    }
    var diverged = new ProjectionCheckpointDivergedException(name.value(), checkpoint, fenced);
    logger.error(
        "Projection '{}' halting on a configuration error: {}",
        LogSanitizer.sanitizeForLog(name.value()),
        diverged.getMessage());
    setError(name, diverged);
    return true;
  }

  /**
   * Runs {@code write} with {@link #stop()}'s interrupt held back when called on the projection's
   * own thread: the flag is cleared for the call and a stop requested meanwhile (or already
   * flagged) is delivered afterwards. On a virtual thread an interrupt fails every blocking socket
   * call made while the flag is set, so a chunk commit, a dead-letter write, a checkpoint advance,
   * the epoch stamp or the resign would otherwise fail and leave its work unrecorded. Sleeps stay
   * outside, so stop() still cuts them short.
   */
  private <T> T runHeldFromStop(ProjectionName name, Supplier<T> write) {
    InterruptDeferral deferral = interrupts.get(name);
    if (deferral == null || threads.get(name) != Thread.currentThread()) {
      return write.get();
    }
    deferral.defer();
    try {
      return write.get();
    } finally {
      deferral.resume();
    }
  }

  /**
   * A chunk failed after stop() was called: the failure may be the stop's interrupt cutting the
   * chunk short, so the error strategy writes nothing and the drain ends. The chunk is read again
   * on the next start.
   */
  private boolean abandonWhileStopping(ProjectionName name) {
    logger.info(
        "Projection '{}' is stopping; the failed chunk is neither skipped nor dead-lettered and is"
            + " read again on the next start",
        LogSanitizer.sanitizeForLog(name.value()));
    return false;
  }

  /**
   * Records {@code count} processed events; metric failures never disrupt projection processing.
   */
  private void recordProcessed(ProjectionName projectionName, int count) {
    try {
      for (int i = 0; i < count; i++) {
        metrics.recordProjectionProcessed(projectionName);
      }
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionProcessed failed for projection '{}'",
          projectionName.value(),
          e);
    }
  }

  private void recordFailed(ProjectionName projectionName) {
    try {
      metrics.recordProjectionFailed(projectionName);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionFailed failed for projection '{}'", projectionName.value(), e);
    }
  }

  /** Records the delivery-mode gauge; metric failures never disrupt projection processing. */
  private void recordDeliveryMode(ProjectionName projectionName, ProjectionDeliveryMode mode) {
    try {
      metrics.recordProjectionDeliveryMode(projectionName, mode);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDeliveryMode failed for projection '{}'",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }

  /** Records a dead-letter; metric failures never disrupt projection processing. */
  private void recordDeadLettered(ProjectionName projectionName) {
    try {
      metrics.recordProjectionDeadLettered(projectionName);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDeadLettered failed for projection '{}'",
          projectionName.value(),
          e);
    }
  }

  /**
   * Samples the fleet-wide projection dead-letter backlog gauge ({@code
   * streamrune.projections.dead_letter_backlog}) once per tick — the projection analog of {@code
   * DeadLetterRetryRunner.sampleObservability}. Skipped when no metrics or no dead-letter store is
   * wired, and the sample is disabled after the first {@link UnsupportedOperationException} from a
   * store that does not support {@code countPending()}. A transient count failure is logged and
   * swallowed so it never disturbs the tick; the gauge simply holds its last value.
   */
  private void sampleDeadLetterBacklog() {
    if (metrics == StreamRuneMetrics.NOOP
        || deadLetterStore == null
        || !deadLetterBacklogSamplingSupported) {
      return;
    }
    try {
      metrics.recordProjectionDeadLetterBacklog(deadLetterStore.countPending());
    } catch (UnsupportedOperationException _) {
      deadLetterBacklogSamplingSupported = false;
      logger.debug(
          "ProjectionDeadLetterStore {} does not support countPending(); projection dead-letter"
              + " backlog gauge disabled",
          deadLetterStore.getClass().getSimpleName());
    } catch (RuntimeException e) {
      logger.warn("Failed to sample projection dead-letter backlog this cycle: {}", e.getMessage());
    }
  }

  private void recordDuration(ProjectionName projectionName, long durationNanos) {
    try {
      metrics.recordProjectionDuration(projectionName, durationNanos);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDuration failed for projection '{}'", projectionName.value(), e);
    }
  }

  /**
   * Feeds a clean chunk advance to the health contributor (resets the error count). No-op when no
   * contributor is wired.
   */
  private void recordHealthSuccess(ProjectionName projectionName) {
    if (healthContributor != null) {
      healthContributor.recordSuccess(projectionName.value());
    }
  }

  /** Feeds a chunk processing failure to the health contributor. No-op when none is wired. */
  private void recordHealthError(ProjectionName projectionName) {
    if (healthContributor != null) {
      healthContributor.recordError(projectionName.value());
    }
  }

  /**
   * Signals all projection threads to stop and waits for them to finish.
   *
   * <p>The threads are interrupted to cut a cron or backoff sleep short, but never a store write: a
   * chunk commit, a dead-letter write, a checkpoint advance, the epoch stamp and the resign run
   * with the interrupt held back until they finish (on a virtual thread an interrupt fails every
   * blocking socket call made while the flag is set), so a chunk in flight commits and the lease is
   * released.
   *
   * <p>If a thread does not exit within the join timeout (e.g. stuck inside {@code process()} or a
   * black-holed read), this runner stays in the started/stopping state and {@link #start()} keeps
   * throwing {@link IllegalStateException} — resetting the guards would let a restart spawn a
   * SECOND drain thread against the same projection alongside the still-live stuck one (duplicate
   * offset writes), and would let the stuck thread itself, once it eventually unblocks, re-read
   * {@code !stopping.get()} as true again and resume its own loop.
   */
  public void stop() {
    stopping.set(true);
    for (var entry : threads.entrySet()) {
      InterruptDeferral deferral = interrupts.get(entry.getKey());
      if (deferral != null) {
        deferral.interrupt(entry.getValue());
      } else {
        entry.getValue().interrupt();
      }
    }
    boolean anyStuck = false;
    for (var entry : threads.entrySet()) {
      Thread t = entry.getValue();
      try {
        t.join(stopTimeoutMs);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        logger.warn(
            "Interrupted while waiting for projection '{}' to stop", entry.getKey().value());
      }
      if (t.isAlive()) {
        anyStuck = true;
        logger.warn(
            "Projection '{}' thread did not stop within {}ms; leaving the runner not-restartable"
                + " so a restart cannot spawn a duplicate drain against the same projection."
                + " Investigate the stuck thread.",
            entry.getKey().value(),
            stopTimeoutMs);
      }
    }
    if (anyStuck) {
      // Do NOT reset `stopping` or `started` while any joined thread is still alive —
      // see the class-level contract in the javadoc above.
      return;
    }
    // Drop each projection from the health view — a stopped runner should not linger as a
    // registered subscription. No-op when no contributor is wired.
    if (healthContributor != null) {
      for (var reg : registrations) {
        healthContributor.unregister(reg.name().value());
      }
    }
    // Clear thread references and reset both guards so start() can be called again on the same
    // instance — matches the restartable lifecycle contract (actuator restart, context refresh).
    threads.clear();
    interrupts.clear();
    // Drop any in-flight per-chunk transient budgets so a restart begins each chunk fresh — a
    // restart re-reads from the committed offset, so a stale in-memory retry count must not carry
    // over.
    chunkTransientStates.clear();
    // Same rationale for the read-poison streak: a restart re-reads from the committed
    // offset, so a stale in-memory poison count must not carry into the fresh run.
    readPoisonFailures.clear();
    // A restart re-reads from the committed offset, so a rejection seen by the previous run says
    // nothing about this one.
    commitRejections.clear();
    stopping.set(false);
    started.set(false);
  }

  /**
   * Returns an immutable snapshot of the current status for each registered projection, keyed by
   * projection name.
   */
  public Map<String, ScheduledProjectionStatus> status() {
    var result = new LinkedHashMap<String, ScheduledProjectionStatus>();
    for (var reg : registrations) {
      var rs = statuses.get(reg.name());
      result.put(
          reg.name().value(),
          new ScheduledProjectionStatus(
              reg.name(),
              rs.state,
              offsetStore.getLastOffset(reg.name()),
              rs.lastFireAt,
              rs.nextFireAt,
              rs.batchesProcessedLastFire,
              rs.lastError));
    }
    return result;
  }

  @Override
  public void close() {
    stop();
  }

  // --- internal status mutators ---

  private void setState(ProjectionName name, ScheduledProjectionState newState) {
    statuses.compute(
        name,
        (k, v) ->
            v == null
                ? new RuntimeStatus(newState, null, null, 0L, null)
                : new RuntimeStatus(
                    newState, v.lastFireAt, v.nextFireAt, v.batchesProcessedLastFire, v.lastError));
  }

  private void updateNextFire(ProjectionName name, Instant nextFire) {
    statuses.compute(
        name,
        (k, v) ->
            new RuntimeStatus(
                v.state, v.lastFireAt, nextFire, v.batchesProcessedLastFire, v.lastError));
  }

  private void recordFire(ProjectionName name, Instant when, long batches) {
    statuses.compute(
        name, (k, v) -> new RuntimeStatus(v.state, when, v.nextFireAt, batches, v.lastError));
  }

  private void setError(ProjectionName name, Throwable e) {
    statuses.compute(
        name,
        (k, v) ->
            new RuntimeStatus(
                ScheduledProjectionState.ERROR,
                v.lastFireAt,
                v.nextFireAt,
                v.batchesProcessedLastFire,
                e.getMessage()));
  }

  /** Records a transient drain failure in {@code lastError} without changing the state. */
  private void recordTransientError(ProjectionName name, Throwable e) {
    statuses.compute(
        name,
        (k, v) ->
            new RuntimeStatus(
                v.state, v.lastFireAt, v.nextFireAt, v.batchesProcessedLastFire, e.getMessage()));
  }

  /** Clears a previously recorded transient error unless the projection is in ERROR state. */
  private void clearTransientError(ProjectionName name) {
    statuses.compute(
        name,
        (k, v) ->
            v.state == ScheduledProjectionState.ERROR
                ? v
                : new RuntimeStatus(
                    v.state, v.lastFireAt, v.nextFireAt, v.batchesProcessedLastFire, null));
  }

  private record RuntimeStatus(
      ScheduledProjectionState state,
      Instant lastFireAt,
      Instant nextFireAt,
      long batchesProcessedLastFire,
      String lastError) {}

  // --- Builder ---

  /** Builder for {@link ScheduledProjectionRunner}. */
  public static final class Builder {
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private int fetchSize = 100;
    private int batchSize = 50;
    private AtomicBatchProcessor atomicProcessor;
    private ProjectionDeadLetterStore deadLetterStore;
    private StreamRuneMetrics metrics;
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private ProjectionErrorClassifier classifier = ProjectionErrorClassifier.DEFAULT;
    private SubscriptionHealthContributor healthContributor;
    private Duration drainRetryInitialBackoff = DEFAULT_DRAIN_RETRY_INITIAL_BACKOFF;
    private Duration drainRetryMaxBackoff = DEFAULT_DRAIN_RETRY_MAX_BACKOFF;
    private int maxTransientRetries = DEFAULT_MAX_TRANSIENT_RETRIES;
    private int maxReadPoisonRetries = DEFAULT_MAX_READ_POISON_RETRIES;
    private long stopTimeoutMs = STOP_TIMEOUT_MS;
    private final List<ScheduledProjectionRegistration> registrations = new ArrayList<>();

    /**
     * Test-only: overrides the whole-tick drain-retry backoff bounds. Production uses {@link
     * #DEFAULT_DRAIN_RETRY_INITIAL_BACKOFF}/{@link #DEFAULT_DRAIN_RETRY_MAX_BACKOFF}.
     * Package-private so the transient-retry-budget test can exhaust the bound without minutes of
     * real backoff.
     */
    Builder drainRetryBackoff(Duration initial, Duration max) {
      this.drainRetryInitialBackoff = initial;
      this.drainRetryMaxBackoff = max;
      return this;
    }

    /**
     * Test-only: overrides the max consecutive TRANSIENT retries before a DLQ batch is
     * dead-lettered. Production uses {@link #DEFAULT_MAX_TRANSIENT_RETRIES}. Package-private.
     */
    Builder maxTransientRetries(int n) {
      this.maxTransientRetries = n;
      return this;
    }

    /**
     * Test-only: overrides the max consecutive deterministic read-poison failures before the
     * projection HALTs terminally. Production uses {@link #DEFAULT_MAX_READ_POISON_RETRIES}.
     * Package-private so the read-poison test halts fast.
     */
    Builder maxReadPoisonRetries(int n) {
      this.maxReadPoisonRetries = n;
      return this;
    }

    /**
     * Test-only: overrides the {@link #stop()} per-thread join timeout. Production uses {@link
     * #STOP_TIMEOUT_MS} (30s). Package-private so the stuck-thread test can shrink it and make the
     * join provably time out without a 30s wait, mirroring OutboxPoller's closeJoinTimeoutMs seam.
     */
    Builder stopTimeoutMs(long stopTimeoutMs) {
      this.stopTimeoutMs = stopTimeoutMs;
      return this;
    }

    /** Sets the event store from which events are read. */
    public Builder eventStore(EventStore es) {
      this.eventStore = es;
      return this;
    }

    /** Sets the offset store that tracks the last processed offset per projection. */
    public Builder offsetStore(OffsetStore os) {
      this.offsetStore = os;
      return this;
    }

    /** Maximum number of events fetched per read call (default: 100). */
    public Builder fetchSize(int n) {
      this.fetchSize = n;
      return this;
    }

    /**
     * Maximum number of events passed to the projection per {@code process()} call (default: 50).
     */
    public Builder batchSize(int n) {
      this.batchSize = n;
      return this;
    }

    /**
     * Sets the atomic batch processor. Required: pass the {@code JdbcProjectionRepository} your
     * {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} projections write to, so each chunk's
     * read-model write and its offset checkpoint commit — or roll back — in one transaction for
     * those registrations, giving SCHEDULED projections the same split-brain guard the continuous
     * and polling runners have; or {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()} for a runner
     * of {@code AT_LEAST_ONCE_IDEMPOTENT} projections. A transactional registration's {@code
     * process(batch, repository)} override must write through the supplied transaction-scoped
     * repository (the framework's {@link org.streamrune.core.projection.BaseProjection} does this
     * transparently); {@link #build()} checks each registration's declared mode against the
     * processor.
     *
     * @throws IllegalArgumentException if {@code processor} is {@code null}
     */
    public Builder atomicProcessor(AtomicBatchProcessor processor) {
      if (processor == null) {
        throw new IllegalArgumentException(
            "atomicProcessor cannot be null: pass the JdbcProjectionRepository your"
                + " TRANSACTIONAL_LOCAL projections write to, or"
                + " AtomicBatchProcessor.nonAtomicAtLeastOnce()");
      }
      this.atomicProcessor = processor;
      return this;
    }

    /** Dead-letter store required when any projection uses {@link ProjectionErrorStrategy#DLQ}. */
    public Builder deadLetterStore(ProjectionDeadLetterStore s) {
      this.deadLetterStore = s;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; when absent (or {@code null}) projection metrics are
     * not recorded and behavior is unchanged. Records processed/failed counts and processing
     * duration per chunk, tagged with the projection name.
     */
    public Builder metrics(StreamRuneMetrics m) {
      this.metrics = m;
      return this;
    }

    /**
     * Sets the single-active-consumer leadership coordinator. Optional; defaults to {@link
     * SubscriptionLeadership#NOOP} (always leader), so single-instance behavior is unchanged. When
     * a real leadership is injected, only the leader for a projection's name drains its ticks; a
     * non-leader skips each tick until it takes over on the leader's death.
     */
    public Builder leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
      return this;
    }

    /**
     * Sets the projection error classifier consulted by the {@link ProjectionErrorStrategy#DLQ}
     * strategy to distinguish a TRANSIENT infra blip (retry the tick with backoff, dead-letter only
     * if the bound is exhausted) from a POISON batch (dead-letter after the first failure).
     * Optional; defaults to {@link ProjectionErrorClassifier#DEFAULT} (infrastructure/SQL/IO
     * failures are transient, everything else poison). Not consulted by SKIP or HALT.
     */
    public Builder classifier(ProjectionErrorClassifier classifier) {
      this.classifier = classifier != null ? classifier : ProjectionErrorClassifier.DEFAULT;
      return this;
    }

    /**
     * Sets the subscription health contributor. Optional; when present, each scheduled projection
     * is registered with it (so {@code health()} lists it and {@code overallStatus()} reflects its
     * state) and each chunk's processing outcome feeds the contributor's success/error counters.
     * Absent (or {@code null}) makes health tracking a no-op.
     */
    public Builder healthContributor(SubscriptionHealthContributor healthContributor) {
      this.healthContributor = healthContributor;
      return this;
    }

    /**
     * Registers a projection with a cron expression, the default HALT error strategy and its
     * delivery mode.
     *
     * @param name unique projection name
     * @param p the projection
     * @param cron Spring 6-field cron expression
     * @param mode the delivery guarantee this projection declares
     * @throws IllegalArgumentException if the cron is invalid
     */
    public Builder register(String name, Projection p, String cron, ProjectionDeliveryMode mode) {
      return register(name, p, cron, ProjectionErrorStrategy.HALT, mode);
    }

    /**
     * Registers a projection with a cron expression, an explicit error strategy and its delivery
     * mode.
     *
     * @param name unique projection name
     * @param p the projection
     * @param cron Spring 6-field cron expression
     * @param strategy how to handle processing failures
     * @param mode the delivery guarantee this projection declares
     * @throws IllegalArgumentException if the cron is invalid
     */
    public Builder register(
        String name,
        Projection p,
        String cron,
        ProjectionErrorStrategy strategy,
        ProjectionDeliveryMode mode) {
      var parsed = CronExpression.parse(cron, ZoneOffset.UTC);
      registrations.add(
          new ScheduledProjectionRegistration(ProjectionName.of(name), p, parsed, strategy, mode));
      return this;
    }

    /**
     * Builds and returns a {@link ScheduledProjectionRunner}.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public ScheduledProjectionRunner build() {
      if (eventStore == null) {
        throw new IllegalArgumentException("eventStore is required");
      }
      if (offsetStore == null) {
        throw new IllegalArgumentException("offsetStore is required");
      }
      if (atomicProcessor == null) {
        throw new IllegalArgumentException(
            "atomicProcessor is required: pass the JdbcProjectionRepository your"
                + " TRANSACTIONAL_LOCAL projections write to, or"
                + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
                + " AT_LEAST_ONCE_IDEMPOTENT projections");
      }
      if (registrations.isEmpty()) {
        throw new IllegalArgumentException("at least one projection must be registered");
      }
      var names = new HashSet<ProjectionName>();
      for (var reg : registrations) {
        // A name the processor cannot store read models under fails here, at startup. With
        // the JDBC repository every valid name has a table of its own, so the exact-name duplicate
        // check below is also the check that no two registrations share a read-model table.
        atomicProcessor.checkProjectionName(reg.name());
        if (!names.add(reg.name())) {
          throw new IllegalArgumentException("Duplicate projection name: " + reg.name().value());
        }
      }
      boolean hasDlq =
          registrations.stream().anyMatch(r -> r.errorStrategy() == ProjectionErrorStrategy.DLQ);
      if (hasDlq && deadLetterStore == null) {
        throw new IllegalArgumentException(
            "deadLetterStore is required when any registration uses DLQ strategy");
      }
      // The scheduled drain runs the same acquire → stamp → chunked-commit protocol as
      // the continuous runner, so it needs the same fencing-capable processor.
      ProjectionFencingPolicy.requireFencingCapable(leadership, atomicProcessor, RUNNER_NAME);
      // Every registration shares this runner's single atomicProcessor, so each is checked
      // against its OWN declared mode — one at-least-once projection never loosens the check for a
      // sibling that declares a transactional mode.
      for (var reg : registrations) {
        ProjectionDeliveryPolicy.require(
            reg.projection(), reg.deliveryMode(), atomicProcessor, RUNNER_NAME, reg.name().value());
      }
      return new ScheduledProjectionRunner(this);
    }
  }
}

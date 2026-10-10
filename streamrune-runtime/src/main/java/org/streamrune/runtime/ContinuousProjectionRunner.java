package org.streamrune.runtime;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.EventSubscription;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * A projection runner that first catches up from the last stored offset, then seamlessly
 * transitions to live mode via an EventSubscription.
 *
 * <p>Phase 1: Catch-up - polls from last known offset until no more events
 *
 * <p>Phase 2: Live - subscribes to new events and processes them in real-time
 *
 * <p><b>Important:</b> Each instance supports only one concurrent projection. Calling {@link
 * #run(ProjectionName, Projection, ProjectionDeliveryMode)} while another projection is running
 * throws {@link IllegalStateException}. Create one runner per projection when running multiple
 * projections.
 */
public final class ContinuousProjectionRunner implements ProjectionRunner, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ContinuousProjectionRunner.class);

  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final int fetchSize;
  private final int batchSize;
  private final SubscriptionConfig subscriptionConfig;
  private final SubscriptionFactory subscriptionFactory;
  private final Consumer<Throwable> errorHandler;
  private final AtomicBatchProcessor atomicProcessor;
  private final ProjectionErrorStrategy errorStrategy;
  private final ProjectionDeadLetterStore deadLetterStore;
  private final StreamRuneMetrics metrics;
  private final SubscriptionLeadership leadership;
  private final ProjectionErrorClassifier classifier;
  private final SubscriptionHealthContributor healthContributor;
  // Read-path retry tunables — production defaults below; package-private builder overrides keep
  // the read-poison test fast without slowing production backoff.
  private final Duration readRetryInitialBackoff;
  private final Duration readRetryMaxBackoff;
  private final int maxReadPoisonRetries;
  // The whole run()/stop lifecycle in ONE atomic slot — see RunSlot. This replaced a bare
  // `running` flag that a stop could only clear: a requestStop()/close() landing before run() won
  // its start CAS was overwritten by that CAS, so the owner's stop was lost and the projection ran
  // on through resources its owner was closing.
  private final AtomicReference<RunSlot> slot = new AtomicReference<>(RunSlot.IDLE);
  private final AtomicReference<ProjectionState> state =
      new AtomicReference<>(ProjectionState.PENDING);
  private final AtomicReference<String> lastError = new AtomicReference<>();
  // Set false the first time countPending() throws UnsupportedOperationException (a
  // store that does not support counting), so the projection dead-letter depth gauge degrades
  // gracefully without retrying every cycle — mirrors
  // DeadLetterRetryRunner.backlogSamplingSupported.
  private volatile boolean deadLetterBacklogSamplingSupported = true;
  // Tells a commit rejection the re-read resolves from one that repeats at an unchanged checkpoint.
  private final CommitRejectionTracker commitRejections = new CommitRejectionTracker();

  /** Where the runner stands in its run()/stop lifecycle. */
  private enum Phase {
    /** No run() holds the runner and no stop is waiting. */
    IDLE,
    /**
     * No run() holds the runner, but a stop arrived: the next run() takes it and returns without
     * reading anything. This is what makes "hand run() to a fresh thread, then stop" safe — the
     * stop may reach the runner before that thread does.
     */
    STOP_HELD,
    /** A run() holds the runner and has not been told to stop. */
    RUNNING,
    /**
     * A run() holds the runner and has been told to stop (or is already on its way out); its loops
     * wind down and its finally releases the runner back to {@link #IDLE}, consuming the stop.
     */
    STOPPING
  }

  /**
   * One atomic snapshot of the lifecycle: the phase plus, while a run() holds the runner, the
   * thread running it and the latch it counts down on exit. Claiming the runner swaps in the whole
   * slot in one CAS, so the run's thread and latch are published together with its claim (a close()
   * never sees a claimed run it cannot interrupt and join), and a stop applies to exactly the phase
   * it observed: it either targets the run holding the runner or is held for the next one, never
   * lost in between.
   */
  private record RunSlot(
      Phase phase, Thread thread, CountDownLatch done, InterruptDeferral interrupts) {
    static final RunSlot IDLE = new RunSlot(Phase.IDLE, null, null, null);
    static final RunSlot STOP_HELD = new RunSlot(Phase.STOP_HELD, null, null, null);

    /** The slot after an owner's stop: a running run is told to stop, an idle runner holds it. */
    RunSlot afterStopRequest() {
      return switch (phase) {
        case IDLE -> STOP_HELD;
        case RUNNING -> new RunSlot(Phase.STOPPING, thread, done, interrupts);
        case STOP_HELD, STOPPING -> this;
      };
    }

    /**
     * The slot after the run stops itself (a halt, or its own exit): only a running run changes — a
     * self-stop is never held, so a late one cannot cancel the next run.
     */
    RunSlot afterRunStops() {
      return phase == Phase.RUNNING ? new RunSlot(Phase.STOPPING, thread, done, interrupts) : this;
    }
  }

  ContinuousProjectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      int fetchSize,
      int batchSize,
      SubscriptionConfig subscriptionConfig,
      SubscriptionFactory subscriptionFactory,
      Consumer<Throwable> errorHandler,
      AtomicBatchProcessor atomicProcessor,
      ProjectionErrorStrategy errorStrategy,
      ProjectionDeadLetterStore deadLetterStore,
      StreamRuneMetrics metrics,
      SubscriptionLeadership leadership,
      ProjectionErrorClassifier classifier,
      SubscriptionHealthContributor healthContributor,
      Duration readRetryInitialBackoff,
      Duration readRetryMaxBackoff,
      int maxReadPoisonRetries) {
    if (eventStore == null) {
      throw new IllegalArgumentException("eventStore is required");
    }
    if (offsetStore == null) {
      throw new IllegalArgumentException("offsetStore is required");
    }
    if (fetchSize <= 0) {
      throw new IllegalArgumentException("fetchSize must be positive");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    if (atomicProcessor == null) {
      throw new IllegalArgumentException("atomicProcessor is required");
    }
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.fetchSize = fetchSize;
    this.batchSize = batchSize;
    this.subscriptionConfig = subscriptionConfig;
    this.subscriptionFactory = subscriptionFactory;
    this.errorHandler = errorHandler;
    this.atomicProcessor = atomicProcessor;
    this.errorStrategy = errorStrategy != null ? errorStrategy : ProjectionErrorStrategy.HALT;
    this.deadLetterStore = deadLetterStore;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
    this.classifier = classifier != null ? classifier : ProjectionErrorClassifier.DEFAULT;
    this.healthContributor = healthContributor;
    this.readRetryInitialBackoff =
        readRetryInitialBackoff != null ? readRetryInitialBackoff : READ_RETRY_INITIAL_BACKOFF;
    this.readRetryMaxBackoff =
        readRetryMaxBackoff != null ? readRetryMaxBackoff : READ_RETRY_MAX_BACKOFF;
    this.maxReadPoisonRetries =
        maxReadPoisonRetries > 0 ? maxReadPoisonRetries : DEFAULT_MAX_READ_POISON_RETRIES;
  }

  @Override
  public void run(
      ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
    // A name the processor cannot store read models under (the JDBC repository's table-name
    // rule) fails here, before a single event is read — not on the first batch.
    atomicProcessor.checkProjectionName(projectionName);
    // Refuse before claiming the runner — a rejected projection never half-starts, and a caller
    // that fixes the wiring can call run() again. The verdict is logged once the runner is claimed.
    ProjectionDeliveryPolicy.Verdict verdict =
        ProjectionDeliveryPolicy.require(
            projection,
            mode,
            atomicProcessor,
            "ContinuousProjectionRunner",
            projectionName.value());
    CountDownLatch done = new CountDownLatch(1);
    // A stop held from before this call (STOP_HELD) is consumed here — the run claims the
    // runner already STOPPING, so it takes exactly the path of a stop that lands just after the
    // claim: the processing loop below is never entered and the shared finally reports STOPPED.
    // Where the scheduler happens to put a racing stop therefore cannot change the outcome.
    // Nothing may run between the claim and the try below, so the claimed phase is only
    // kept here and acted on inside the try.
    Phase claimed = claimRunner(projectionName, done).phase();

    try {
      logDeliveryMode(projectionName, mode, verdict);
      if (claimed == Phase.STOPPING) {
        // The held stop's run processes nothing, so it keeps the previous run's failure: state()
        // still reports that run's ERROR, and lastError() must still say why.
        logger.info(
            "Projection '{}' was told to stop before run() started; returning without processing",
            projectionName.value());
      } else {
        lastError.set(null);
      }
      // Register a read-only view of THIS RUNNER with the health contributor before the
      // leadership loop starts, so the projection is visible in /health from the first tick — even
      // on a replica that never wins the lease. Registration used to happen only inside
      // subscribeToLiveEvents, so a permanent standby registered nothing at all: when NO replica
      // can lead (a terminal standDownTerminally after a heartbeat Error, or a runtime REVOKE on
      // subscription_leases) every replica's /health reported UP with no entry for the projection
      // while the read model was frozen indefinitely. The live-subscription registration below
      // REPLACES this view while LIVE (for the richer lifecycle + delivery-error signal) and the
      // standby view is restored on the way back out. Mirrors ScheduledProjectionRunner, which
      // registers at start() and keeps the entry through SLEEPING/standby ticks.
      //
      // This call MUST sit INSIDE the try. It used to run between claiming the runner
      // and the try, so anything it threw escaped run() without touching the catch(Throwable)/
      // finally: the runner stayed claimed (every later run() failed with "Runner already
      // running"), done was never counted down (a later close() blocked its
      // full CLOSE_JOIN_TIMEOUT), and state/markTerminalError never ran — /health reported UP while
      // the projection processed nothing, on a single-replica deployment forever. The concrete
      // thrower was SubscriptionHealthContributor.register's bare
      // metrics.registerSubscriptionLagGauge (now log-and-continue for RuntimeException); this
      // placement additionally covers any residual Error or future registration side effect.
      registerStandbyHealthView(projectionName);
      // Outer leadership loop: only the single-active-consumer leader for this name reads,
      // processes, and advances the offset. A non-leader sits in STANDBY, retrying acquisition at
      // poll-interval cadence; on the leader's death it takes over. When leadership is lost during
      // catch-up or live mode the phase returns early and we drop back to the standby wait — never
      // committing on a lost lease. With SubscriptionLeadership.NOOP (single-instance default) this
      // loop runs the catch-up/live phases exactly once, byte-identical to the pre-leadership path.
      while (isRunning()) {
        Optional<SubscriptionLeadership.Lease> lease = awaitLeadership(projectionName);
        if (lease.isEmpty()) {
          break; // stopped while standing by
        }
        // Durably stamp the just-acquired epoch on the checkpoint row BEFORE reading
        // or processing anything. The epoch fence compares against the epoch STORED on the row,
        // which only a committed advance used to stamp — so from this takeover until our first
        // committed advance the row still carried the superseded leader's epoch, and ALL of that
        // leader's writes (including the SKIP/DLQ empty-batch forward-jumps the overlap and
        // monotonic guards cannot see) passed the fence while its fail-open local lease view
        // (the lease clock measures from a client-side instant recorded after the server-side
        // renew, so
        // a GC pause stretches it past DB lease expiry) still reported it leader. FAIL-CLOSED on
        // a stamp failure: processing without the stamp would re-open exactly that window, so
        // stand by one poll interval and retry the acquire+stamp — with the DB down, no
        // processing could proceed anyway. Idempotent + monotonic in the store (GREATEST-guarded),
        // so a crash between acquire and stamp just re-stamps on the next pass, and a re-acquire
        // of a lease we already hold re-stamps the same epoch as a no-op.
        StampOutcome stamp = stampTakeoverEpoch(projectionName, lease.get().epoch());
        if (stamp == StampOutcome.FATAL) {
          // The acquired epoch is below the stored one AND we still hold the lease
          // (the superseded case routes to RETRY/standby), so the lease table and the
          // checkpoint have diverged and no amount of retrying can make a commit pass the fence.
          // stampTakeoverEpoch already set state=ERROR and lastError; leave the loop so the shared
          // finally resigns leadership and marks the projection terminally DOWN instead of looping
          // forever on guaranteed rejections.
          break;
        }
        if (stamp == StampOutcome.RETRY) {
          standByBeforeRetry(projectionName);
          continue;
        }
        state.set(ProjectionState.CATCHING_UP);
        // Sample the dead-letter backlog at the phase transition too, so a leader that
        // takes over with a pre-existing backlog reports it before its first page, not only once
        // it eventually goes live.
        sampleDeadLetterBacklog();
        catchUp(projectionName, projection, mode);
        if (!isRunning()) {
          continue; // stopped during catch-up
        }
        if (leadership.current(projectionName.value()).isEmpty()) {
          // Lost leadership during (or at the pre-commit re-check of) catch-up: nothing was
          // committed on the lost lease. Drop to STANDBY and back off one poll interval before
          // retrying acquisition, so a flapping/liveness-failing lease does not busy-spin.
          standByBeforeRetry(projectionName);
          continue;
        }
        state.set(ProjectionState.LIVE);
        subscribeToLiveEvents(projectionName, projection, mode);
        // Live mode returned. If it was leadership loss (not a stop), stand by before reacquiring.
        if (isRunning() && leadership.current(projectionName.value()).isEmpty()) {
          standByBeforeRetry(projectionName);
        }
      }
    } catch (Throwable e) {
      // Catch Throwable, not just Exception. An Error (AssertionError from user
      // mapping code, NoClassDefFoundError from a missing optional dependency, StackOverflowError
      // on pathological data) thrown from process() during CATCH-UP previously escaped this catch —
      // the finally then saw state != ERROR and set STOPPED WITHOUT marking terminal DOWN, so
      // /health stayed UP forever while the read model was frozen (an earlier fix hardened only the
      // LIVE poll thread). Set state=ERROR before rethrow so the finally marks the projection
      // terminally DOWN, exactly like an Exception. compareAndSet keeps the more specific message
      // an inner error site already recorded; a clean stop/close returns normally and never reaches
      // this branch, so this does not clobber a clean pause/close (mirrors the LIVE poll thread's
      // discipline).
      lastError.compareAndSet(null, errorMessage(e));
      state.set(ProjectionState.ERROR);
      throw e;
    } finally {
      stopCurrentRun();
      // The run loop has terminated (stop, HALT exhaustion, or a thrown error) — resign leadership
      // so a healthy standby replica can take over immediately instead of waiting for process exit
      // / context shutdown to release the lease. This is NOT a transient standby
      // wait (those return early inside the loop without exiting run()); we only reach here on
      // actual runner termination. NOOP.resign is a no-op, so single-instance behavior is
      // unchanged.
      //
      // The SubscriptionLeadership contract requires resign to be no-throw for transient
      // failures — but the bookkeeping below must survive even a contract-breaking implementation.
      // A resign throw used to truncate this finally: terminal-DOWN health marking skipped (a
      // catch-up HALT then reported /health UP forever over a frozen read model), done.countDown()
      // skipped (a subsequent close() blocked its full CLOSE_JOIN_TIMEOUT), the runner never
      // released (permanently non-restartable: every later run() refused). So: a
      // RuntimeException from resign is logged and swallowed here, and the state/health/latch
      // bookkeeping runs in a nested finally — even an Error unwinding out of resign completes it
      // before propagating.
      try {
        // Resign is a store write: on a run thread interrupted by close() it would fail at the
        // socket and leave the lease to expire at its TTL. Clear the flag around it and restore it.
        runHeldFromClose(
            () -> {
              leadership.resign(projectionName.value());
              return null;
            });
      } catch (RuntimeException resignFailure) {
        logger.warn(
            "Projection '{}' could not resign leadership on termination; the lease will expire"
                + " naturally at its TTL",
            projectionName.value(),
            resignFailure);
      } finally {
        if (state.get() != ProjectionState.ERROR) {
          state.set(ProjectionState.STOPPED);
          // A deliberate CLEAN stop drops out of the health view, exactly as before —
          // a stopped projection is not a frozen one. This is now the single unregistration point
          // for every clean exit (previously subscribeToLiveEvents unregistered, which missed the
          // stop-while-STANDBY and stop-during-catch-up paths that never registered at all).
          if (healthContributor != null) {
            healthContributor.unregister(projectionName.value());
          }
        } else if (healthContributor != null) {
          // Permanent ERROR exit (HALT exhaustion, a live subscription that died, or any thrown
          // error): keep the subscription registered and force its health terminally DOWN so
          // overallStatus() — and thus the /health endpoint — stays DOWN while the read model is
          // frozen. A CLEAN stop already unregistered inside subscribeToLiveEvents; a STANDBY wait
          // never reaches this ERROR branch. A catch-up HALT that never registered a live
          // subscription gets a fresh terminal-DOWN entry created here.
          healthContributor.markTerminalError(projectionName.value());
        }
        // Release the runner BEFORE counting down, so a close() returning from its join finds
        // it IDLE and an immediate restart claims it rather than being refused as a still-stopping
        // run. Releasing consumes the stop that ended this run: it never lingers to cancel the
        // next one.
        slot.set(RunSlot.IDLE);
        done.countDown();
      }
    }
  }

  private static final int MAX_CONSECUTIVE_ERRORS = 50;
  private static final Duration READ_RETRY_INITIAL_BACKOFF = Duration.ofSeconds(1);
  private static final Duration READ_RETRY_MAX_BACKOFF = Duration.ofSeconds(60);
  // Consecutive deterministic read-poison failures at the same checkpoint before the projection
  // HALTs terminally instead of retrying an unreadable event forever. A small bound
  // still tolerates a brief startup race (e.g. the event type not yet registered) before giving up.
  private static final int DEFAULT_MAX_READ_POISON_RETRIES = 5;
  private static final long CLOSE_JOIN_TIMEOUT_SECONDS = 30;
  private static final Duration DEFAULT_STANDBY_POLL_INTERVAL = Duration.ofSeconds(5);
  // The no-op updater for a fenced empty-batch checkpoint advance. An empty-batch
  // executeAtomically takes the checkpoint row lock, runs the epoch fence + monotonic guard, and
  // stamps the epoch WITHOUT touching the read model, so the updater writes nothing.
  private static final AtomicBatchProcessor.ProjectionUpdater NO_OP_UPDATER = repository -> {};

  /**
   * Blocks until this runner holds leadership for {@code projectionName} or the run is told to
   * stop. Reports {@link ProjectionState#STANDBY} while waiting and retries {@code tryAcquire} at
   * the poll-interval cadence. Returns the acquired lease (whose epoch the caller stamps on the
   * checkpoint row before processing) once leadership is held, or empty if the runner was stopped
   * (or interrupted) before becoming leader. With {@link SubscriptionLeadership#NOOP} the first
   * attempt succeeds immediately, so no standby wait occurs.
   */
  private Optional<SubscriptionLeadership.Lease> awaitLeadership(ProjectionName projectionName) {
    long standbyMillis = standbyPollMillis();
    boolean announcedStandby = false;
    while (isRunning()) {
      Optional<SubscriptionLeadership.Lease> lease = leadership.tryAcquire(projectionName.value());
      if (lease.isPresent()) {
        return lease;
      }
      if (!announcedStandby) {
        logger.info(
            "Projection '{}' is STANDBY — another instance holds leadership; retrying every {} ms",
            projectionName.value(),
            standbyMillis);
        announcedStandby = true;
      }
      state.set(ProjectionState.STANDBY);
      // The backlog is a global read-only count, so every replica samples it — a standby
      // used to hold the gauge at whatever it last read as leader (or never emit it at all), the
      // same "every replica, before the leadership gate" rule DeadLetterRetryRunner applies.
      sampleDeadLetterBacklog();
      if (!sleepQuietly(standbyMillis)) {
        return Optional.empty(); // interrupted during standby wait
      }
    }
    return Optional.empty();
  }

  /**
   * Durably stamps the just-acquired leadership epoch on the projection's checkpoint row (via
   * {@link AtomicBatchProcessor#stampFencingEpoch}) so the epoch fence rejects the superseded
   * leader's writes from the moment of takeover instead of only after this leader's first committed
   * advance. Epoch {@code 0} ({@link SubscriptionLeadership#NOOP} — unfenced single-node) skips the
   * write entirely: there is no second replica to fence. The caller must NOT process without the
   * stamp — that would re-open the takeover window.
   *
   * <p>A stamp failure has two shapes and they need opposite responses. A transient DB blip is
   * {@link StampOutcome#RETRY} — stand by and try again. A {@link
   * ProjectionEpochRegressionException} is {@link StampOutcome#FATAL}: the lease is handing out an
   * epoch BELOW the one on the checkpoint row, so every commit this runner could make is guaranteed
   * to be rejected by the epoch fence. Retrying that forever is the silent freeze this guard is
   * about; it terminates the runner as ERROR instead, which makes the shared {@code finally} resign
   * leadership and {@code markTerminalError} the projection so /health goes DOWN.
   *
   * <p><b>An epoch regression has TWO shapes too, and only one of them is a bug.</b> The stored
   * epoch is higher than ours either because the lease table diverged from the checkpoint (an
   * operator cleared or restored {@code subscription_leases} alone) or simply because a NEWER
   * leader legitimately took over and stamped its own, higher epoch while we stalled between {@link
   * #awaitLeadership} and this call — a GC pause, a VM freeze, or a {@code
   * dataSource.getConnection()} wait longer than the lease TTL. The second is an ordinary failover,
   * the exact race the fence exists to handle gracefully, and it is indistinguishable from the
   * first by the epochs alone. Declaring it FATAL terminated a perfectly healthy replica with
   * health DOWN and removed it from the failover pool permanently: when the new leader later died,
   * nothing took over and the projection stopped fleet-wide until an operator restarted the
   * process.
   *
   * <p>Leadership is the discriminator, and it is the same question the runner already asks one
   * instruction earlier. If this runner no longer holds the lease it was superseded — stand by, as
   * it would have if {@code tryAcquire} had simply returned empty; the standby loop re-acquires at
   * a fresh, higher epoch when the current leader goes away, and the stamp then succeeds. Only a
   * regression observed while this runner STILL holds its lease is a true lease/checkpoint
   * divergence: the lease table can hand this name nothing higher, so no retry can ever help. See
   * {@link #stillLeader}: a leadership probe that cannot answer keeps the loud verdict.
   */
  private StampOutcome stampTakeoverEpoch(ProjectionName projectionName, long epoch) {
    if (epoch == 0L) {
      return StampOutcome.STAMPED;
    }
    try {
      runHeldFromClose(
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
                + " that took over while this replica was stalled. This is an ordinary failover —"
                + " standing by and retrying acquisition, exactly as if the acquire itself had"
                + " lost. (A regression observed while this replica STILL held its lease is the"
                + " lease/checkpoint divergence, and terminates the runner instead.)",
            projectionName.value(),
            regression.acquiredEpoch(),
            regression.storedEpoch());
        return StampOutcome.RETRY;
      }
      logger.error(
          "Projection '{}' cannot arm its epoch fence: leadership handed out epoch {} but the"
              + " checkpoint row already stores epoch {}. Every commit would be rejected by the"
              + " fence, so the projection would freeze silently — terminating it as ERROR and"
              + " reporting health DOWN instead. {}",
          projectionName.value(),
          regression.acquiredEpoch(),
          regression.storedEpoch(),
          regression.getMessage());
      lastError.set(regression.getMessage());
      state.set(ProjectionState.ERROR);
      return StampOutcome.FATAL;
    } catch (RuntimeException e) {
      logger.warn(
          "Projection '{}' could not stamp takeover epoch {} on the checkpoint row — not"
              + " processing without the stamp (the epoch fence would stay inert for the"
              + " superseded leader); standing by and retrying",
          projectionName.value(),
          epoch,
          e);
      return StampOutcome.RETRY;
    }
  }

  /**
   * Whether this runner still holds the lease for {@code projectionName}. Used only to DIAGNOSE an
   * epoch regression, never as a safety gate — nothing is processed on either branch. A {@link
   * SubscriptionLeadership#current} that throws therefore answers "assume still leader": that keeps
   * the loud, terminal verdict as the default, so a broken leadership probe can never silence a
   * genuine lease/checkpoint divergence into an INFO-level standby loop.
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
    /**
     * The fence is durably armed at this leader's epoch; processing may proceed.
     *
     * <p>Also the outcome of a same-epoch re-stamp, which the store treats as an idempotent 0-row
     * no-op.
     */
    STAMPED,
    /**
     * Stand by one poll interval and retry the acquire+stamp. Two causes, one response: a transient
     * stamp failure (DB blip), and an epoch regression observed after this replica was legitimately
     * superseded — for which standing by IS the correct, permanent answer, since the loop
     * re-acquires at a fresh higher epoch once the current leader goes away.
     */
    RETRY,
    /**
     * The acquired epoch is below the stored one AND this runner still holds its lease, so the
     * lease table and the checkpoint have diverged and no retry can help.
     */
    FATAL
  }

  private long standbyPollMillis() {
    Duration interval = subscriptionConfig != null ? subscriptionConfig.pollingInterval() : null;
    if (interval == null || interval.isZero() || interval.isNegative()) {
      interval = DEFAULT_STANDBY_POLL_INTERVAL;
    }
    return interval.toMillis();
  }

  /**
   * Marks the runner STANDBY and sleeps one poll interval before the outer loop retries
   * acquisition. Called when a processing phase returns because leadership was lost, so a
   * liveness-failing or flapping lease settles into STANDBY at poll cadence instead of
   * busy-spinning.
   */
  private void standByBeforeRetry(ProjectionName projectionName) {
    state.set(ProjectionState.STANDBY);
    logger.info(
        "Projection '{}' lost leadership; standing by before retrying acquisition",
        projectionName.value());
    sleepQuietly(standbyPollMillis());
  }

  /** Outcome of {@link #handleBatchError} that tells the catch-up loop how to proceed. */
  private enum ErrorAction {
    /** The failed chunk was skipped (offset advanced); continue with the next chunk. */
    CONTINUE_NEXT_BATCH,
    /** Re-read from the last committed offset and re-process the failed chunk. */
    RETRY_FROM_CHECKPOINT,
    /** The catch-up thread was interrupted during backoff; stop catching up. */
    STOP
  }

  /**
   * Signals that leadership for this projection was lost before a batch could be committed. Thrown
   * from {@link #processBatch} at the pre-commit re-check; {@link #catchUp} catches it, abandons
   * the batch without committing, and returns so the outer loop drops back to STANDBY. Whoever
   * leads next re-reads from the last committed checkpoint (safe under at-least-once).
   */
  private static final class LeadershipLostException extends RuntimeException
      implements ResilientPollLoop.BenignSignal {
    LeadershipLostException(String name) {
      super("Leadership lost for projection '" + name + "' before commit");
    }
  }

  /**
   * Signals that a live commit was rejected by the processor's overlap or monotonic guard: the
   * checkpoint is ahead of the one the batch was read from, and leadership is not in question. The
   * subscription does not advance and reads again from the checkpoint. A second rejection at the
   * same checkpoint halts the runner instead (see {@link #haltIfRejectionRepeats}).
   */
  private static final class CommitRejectedAtCheckpointException extends RuntimeException
      implements ResilientPollLoop.BenignSignal {
    CommitRejectedAtCheckpointException(String name) {
      super(
          "Commit for projection '"
              + LogSanitizer.sanitizeForLog(name)
              + "' was rejected at the checkpoint; reading again from it");
    }
  }

  /**
   * The benign signal for a rejected live commit: leadership lost when the epoch fence named a
   * newer leader, a plain re-read otherwise.
   */
  private static RuntimeException rejectedLiveCommitSignal(
      ProjectionName projectionName, ProjectionCommitFencedException fenced) {
    return fenced.guard() == ProjectionCommitFencedException.Guard.EPOCH_FENCE
        ? new LeadershipLostException(projectionName.value())
        : new CommitRejectedAtCheckpointException(projectionName.value());
  }

  /**
   * Decides whether a commit rejection is one the re-read resolves, or one that repeats for as long
   * as the process runs. A rejection by the epoch fence names a newer leader and is never counted.
   * An overlap or monotonic rejection means the processor's checkpoint is ahead of the one this
   * runner read: tolerated once (a commit whose acknowledgement was lost is rejected on its retry,
   * and the re-read starts after the moved checkpoint), and on a second consecutive one with the
   * runner's checkpoint unchanged the runner halts — state {@code ERROR}, {@code lastError} set,
   * the run told to stop, so {@code run()}'s finally marks the projection terminally DOWN.
   *
   * @return the halt signal to throw, or {@code null} when the rejection is tolerated
   */
  private ProjectionCheckpointDivergedException haltIfRejectionRepeats(
      ProjectionName projectionName, ProjectionCommitFencedException fenced) {
    if (fenced.guard() == ProjectionCommitFencedException.Guard.EPOCH_FENCE) {
      commitRejections.reset();
      return null;
    }
    long checkpoint;
    try {
      checkpoint = offsetStore.getLastOffset(projectionName).value();
    } catch (RuntimeException readFailed) {
      // Whether the checkpoint moved is unknown; the next rejection decides.
      return null;
    }
    if (!commitRejections.repeatsAt(checkpoint)) {
      return null;
    }
    var diverged =
        new ProjectionCheckpointDivergedException(projectionName.value(), checkpoint, fenced);
    logger.error(
        "Projection '{}' halting on a configuration error: {}",
        LogSanitizer.sanitizeForLog(projectionName.value()),
        diverged.getMessage());
    lastError.set(diverged.getMessage());
    state.set(ProjectionState.ERROR);
    stopCurrentRun();
    return diverged;
  }

  private void catchUp(
      ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
    int consecutiveReadFailures = 0;
    int consecutiveReadPoisonFailures = 0;
    int consecutiveProcessingFailures = 0;
    int consecutiveSaveFailures = 0;

    while (isRunning()) {
      // Leadership can be lost between batches (leader connection blip / failover). Re-check before
      // reading the next page; if we are no longer leader, stop catching up so the outer loop drops
      // to STANDBY without advancing the offset. NOOP always returns true, so this is a no-op
      // there.
      if (leadership.current(projectionName.value()).isEmpty()) {
        return;
      }
      List<EventEnvelope> events;
      try {
        GlobalOffset lastOffset = offsetStore.getLastOffset(projectionName);
        events = eventStore.readGlobalStream(lastOffset, fetchSize);
        consecutiveReadFailures = 0;
        consecutiveReadPoisonFailures = 0;
      } catch (RuntimeException e) {
        if (!isRunning()) {
          // A read cut short by close()'s interrupt (reads are not held back from it): not a
          // failure worth an ERROR, and no input to the read-poison bound.
          return;
        }
        // Read failure. Distinguish a deterministic read/deserialization POISON (an unregistered
        // event type or a corrupt/schema-incompatible payload) from a genuine
        // TRANSIENT infra blip. A poison read fails identically forever; the transient backoff loop
        // below would retry it indefinitely and wedge the projection with no progress, never
        // reaching the error strategy/DLQ (the failure is before process() runs). So a poison read
        // is bounded and, once the bound is exhausted, HALTs terminally with a distinct signal.
        if (ReadPoisonClassifier.isDeterministicReadPoison(e)) {
          consecutiveReadPoisonFailures++;
          if (consecutiveReadPoisonFailures >= maxReadPoisonRetries) {
            var poison =
                new ProjectionReadPoisonException(
                    projectionName.value(), consecutiveReadPoisonFailures, e);
            logger.error(
                "Projection '{}' halting on a read/deserialization poison after {} attempt(s) — the"
                    + " offending event can never be read on retry; not spinning forever",
                projectionName.value(),
                consecutiveReadPoisonFailures,
                e);
            // Set state=ERROR BEFORE clearing running so run()'s finally observes ERROR (not a
            // stale state) and marks the projection terminally DOWN, exactly like a HALT
            // exhaustion.
            lastError.set(poison.getMessage());
            state.set(ProjectionState.ERROR);
            stopCurrentRun();
            throw poison;
          }
          // Under the bound: fall through and retry with backoff (tolerates a brief startup race).
        } else {
          // A genuine transient infra failure — reset the poison streak and keep retrying UNBOUNDED
          // with capped backoff, so a DB outage never permanently kills the projection.
          consecutiveReadPoisonFailures = 0;
        }
        consecutiveReadFailures++;
        var backoff =
            ResilientPollLoop.backoffDelay(
                readRetryInitialBackoff, readRetryMaxBackoff, consecutiveReadFailures);
        logger.error(
            "Projection '{}' failed to read the event store during catch-up ({} consecutive"
                + " failure(s)); retrying in {} ms",
            projectionName.value(),
            consecutiveReadFailures,
            backoff.toMillis(),
            e);
        if (!sleepQuietly(backoff.toMillis())) {
          return;
        }
        continue;
      }

      if (events.isEmpty()) {
        break;
      }

      // Process the fetched page chunk by chunk so the error strategy applies to exactly the
      // failed chunk — never to chunks that were not yet attempted.
      for (int i = 0; i < events.size() && isRunning(); i += batchSize) {
        List<EventEnvelope> chunk = events.subList(i, Math.min(i + batchSize, events.size()));
        try {
          // The batch and its checkpoint commit run with close()'s interrupt held back: an
          // interrupt landing in them would fail the commit at the socket and hand an intact batch
          // to the error strategy (SKIP or DLQ would then move past it). close() waits for it.
          runHeldFromClose(
              () -> {
                processBatch(projectionName, projection, mode, chunk);
                return null;
              });
          consecutiveProcessingFailures = 0;
          consecutiveSaveFailures = 0;
          recordHealthSuccess(projectionName);
        } catch (LeadershipLostException _) {
          // Leadership was lost at the pre-commit re-check: abandon this batch WITHOUT committing
          // and return so the outer loop drops to STANDBY. Not a processing error — no DLQ/backoff.
          logger.info(
              "Projection '{}' lost leadership before commit; abandoning batch and standing by",
              projectionName.value());
          return;
        } catch (ProjectionCommitFencedException fenced) {
          // The atomic commit was rejected by executeAtomically — the epoch fence (a newer
          // leader took over the lease), the first-offset overlap guard, or the monotonic guard.
          // This is a LEADERSHIP/CAS signal, NOT a projection error: it must NEVER enter the error
          // strategy, because SKIP/DLQ would convert the REJECTION into a forward checkpoint
          // advance by the very stale leader the fence just rejected — silently, permanently
          // skipping the un-applied events. Mirror LeadershipLostException: abandon the batch
          // WITHOUT advancing and return, so the outer loop re-checks leadership and (if
          // still/again leader) re-reads from the fresh committed checkpoint. A genuine
          // monotonic/overlap rejection resolves the same way — reload the checkpoint, retry from
          // fresh state — unless it repeats at an unchanged checkpoint: the processor then holds a
          // checkpoint this runner never reads, and the runner halts instead of looping.
          ProjectionCheckpointDivergedException diverged =
              haltIfRejectionRepeats(projectionName, fenced);
          if (diverged != null) {
            throw diverged;
          }
          logger.info(
              "Projection '{}' atomic commit rejected ({} guard) — abandoning batch without"
                  + " advancing, reading again from the checkpoint: {}",
              LogSanitizer.sanitizeForLog(projectionName.value()),
              fenced.guard(),
              fenced.getMessage());
          return;
        } catch (ProjectionCheckpointSaveException saveFailed) {
          // The batch IS applied and the checkpoint did NOT move — the offset store is unavailable,
          // the range is not poison. This never reaches the error strategy: SKIP would advance past
          // an applied batch, DLQ would dead-letter a range that was applied, HALT would stop a
          // healthy projection. recordFailed already ran in processBatch.
          consecutiveSaveFailures++;
          recordHealthError(projectionName);
          var backoff =
              ResilientPollLoop.backoffDelay(
                  readRetryInitialBackoff, readRetryMaxBackoff, consecutiveSaveFailures);
          logger.warn(
              "Projection '{}' applied the batch up to {} but the checkpoint save failed; re-reading"
                  + " from the checkpoint after {}ms: {}",
              LogSanitizer.sanitizeForLog(projectionName.value()),
              saveFailed.offset().value(),
              backoff.toMillis(),
              String.valueOf(saveFailed.getCause()));
          if (!sleepQuietly(backoff.toMillis())) {
            return;
          }
          break; // re-read the page from the un-advanced checkpoint
        } catch (Exception e) {
          consecutiveProcessingFailures++;
          if (errorHandler != null) {
            errorHandler.accept(e);
          }
          // Catch-up used to be health-silent — a dead-lettered or skipped chunk left the
          // standby health view at 0 errors (UP) while the checkpoint advanced past it. Record the
          // error BEFORE the strategy, exactly like ScheduledProjectionRunner.
          recordHealthError(projectionName);
          ErrorAction action =
              handleBatchError(projectionName, chunk, e, consecutiveProcessingFailures);
          if (action == ErrorAction.STOP) {
            return;
          }
          if (action == ErrorAction.RETRY_FROM_CHECKPOINT) {
            break; // re-read the page from the last committed offset
          }
          consecutiveProcessingFailures = 0; // SKIP advanced past the failed chunk
        }
      }
    }
  }

  /**
   * Applies the configured {@link ProjectionErrorStrategy} to a failed chunk during catch-up. SKIP
   * advances the offset past the failed chunk only; DLQ classifies the error — a TRANSIENT infra
   * blip under {@link #MAX_CONSECUTIVE_ERRORS} retries from the checkpoint with backoff (no
   * dead-letter if it recovers), while a POISON error (or a TRANSIENT that exhausted the bound)
   * writes the failed range to the dead letter store, advances past it, and CONTINUES (no halt) so
   * a single bad batch never stops the whole projection — replay with {@link
   * ProjectionDeadLetterReplayer}. If that dead-letter WRITE itself fails (DLQ store down) the
   * offset is NOT advanced: the write failure is a TRANSIENT infra failure, retried from the
   * checkpoint with backoff so the range is never silently lost. HALT backs off and retries from
   * the checkpoint, halting after {@link #MAX_CONSECUTIVE_ERRORS} consecutive failures.
   */
  private ErrorAction handleBatchError(
      ProjectionName projectionName,
      List<EventEnvelope> chunk,
      Exception e,
      int consecutiveErrors) {
    switch (errorStrategy) {
      case SKIP -> {
        // A SKIP advances the checkpoint FORWARD past the poison chunk, so the monotonic offset
        // guard cannot catch a stale leader here (a forward move is always accepted). Re-check
        // leadership before saving: if the lease was lost while this batch was processing, abandon
        // WITHOUT saving and STOP the catch-up loop so the outer loop drops to STANDBY. The next
        // leader re-reads (and re-skips) from the last committed checkpoint. NOOP is always leader,
        // so this path is byte-identical to before for single-instance deployments.
        Optional<SubscriptionLeadership.Lease> skipLease =
            leadership.current(projectionName.value());
        if (skipLease.isEmpty()) {
          logger.info(
              "Projection '{}' lost leadership before a SKIP checkpoint advance; abandoning batch"
                  + " and standing by",
              projectionName.value());
          return ErrorAction.STOP;
        }
        if (!isRunning()) {
          return stopBeforeErrorWrite(projectionName); // the failure may be the stop itself
        }
        GlobalOffset skipOffset = chunk.getLast().globalOffset();
        logger.warn(
            "SKIP: Projection '{}' skipping batch ending at offset {} due to error: {}",
            projectionName.value(),
            skipOffset.value(),
            e.getMessage(),
            e);
        return advanceCheckpointPastError(
            projectionName, skipOffset, skipLease.get().epoch(), consecutiveErrors);
      }
      case DLQ -> {
        // Poison-vs-transient. A TRANSIENT infra blip under the retry bound is retried
        // from the last committed checkpoint with the HALT-case capped-exponential backoff — no
        // dead-letter if it recovers. A POISON error (or a TRANSIENT that exhausted the bound) is
        // dead-lettered and the runner CONTINUES past the range (no halt) — this is the behavior
        // change from the old halt-after-DLQ, matching ScheduledProjectionRunner and the enum
        // javadoc ("persist ... and continue").
        ProjectionErrorClass errorClass = classifier.classify(e);
        if (errorClass == ProjectionErrorClass.TRANSIENT
            && consecutiveErrors < MAX_CONSECUTIVE_ERRORS) {
          long backoffMs = Math.min(1000L * (1L << Math.min(consecutiveErrors, 16)), 60_000L);
          logger.warn(
              "DLQ: Projection '{}' TRANSIENT error (attempt {}/{}, backoff {}ms) — retrying from"
                  + " checkpoint, not dead-lettering: {}",
              projectionName.value(),
              consecutiveErrors,
              MAX_CONSECUTIVE_ERRORS,
              backoffMs,
              e.getMessage(),
              e);
          if (!sleepQuietly(backoffMs)) {
            return ErrorAction.STOP;
          }
          return ErrorAction.RETRY_FROM_CHECKPOINT;
        }
        // POISON, or TRANSIENT bound exhausted: dead-letter the range and advance past it.
        if (errorClass == ProjectionErrorClass.TRANSIENT) {
          logger.warn(
              "DLQ: Projection '{}' TRANSIENT error exhausted the retry bound ({}); dead-lettering"
                  + " and continuing",
              projectionName.value(),
              MAX_CONSECUTIVE_ERRORS);
        } else {
          logger.warn(
              "DLQ: Projection '{}' POISON error — dead-lettering after first failure and"
                  + " continuing: {}",
              projectionName.value(),
              e.getMessage(),
              e);
        }
        // Re-check leadership before advancing past the dead-lettered range: advancing is a forward
        // checkpoint move the monotonic guard cannot reject, so a stale leader must NOT advance.
        Optional<SubscriptionLeadership.Lease> dlqLease =
            leadership.current(projectionName.value());
        if (dlqLease.isEmpty()) {
          logger.info(
              "Projection '{}' lost leadership before a DLQ checkpoint advance; abandoning batch"
                  + " and standing by",
              projectionName.value());
          return ErrorAction.STOP;
        }
        if (!isRunning()) {
          return stopBeforeErrorWrite(projectionName); // the failure may be the stop itself
        }
        boolean deadLettered =
            runHeldFromClose(() -> writeDeadLetter(projectionName, chunk, e, consecutiveErrors));
        if (!deadLettered) {
          // The dead-letter STORE WRITE itself failed (DLQ store down) — a TRANSIENT infra failure,
          // not a reason to skip. Do NOT advance the offset; retry from the last committed
          // checkpoint with the same capped-exponential backoff the transient path uses, so the
          // poison range is re-read and re-attempted and is NEVER silently lost from the read
          // model.
          long backoffMs = Math.min(1000L * (1L << Math.min(consecutiveErrors, 16)), 60_000L);
          logger.warn(
              "DLQ: Projection '{}' dead-letter WRITE failed (attempt {}/{}, backoff {}ms) —"
                  + " transient infra failure; not advancing, retrying from checkpoint",
              projectionName.value(),
              consecutiveErrors,
              MAX_CONSECUTIVE_ERRORS,
              backoffMs);
          if (!sleepQuietly(backoffMs)) {
            return ErrorAction.STOP;
          }
          return ErrorAction.RETRY_FROM_CHECKPOINT;
        }
        // The gauge used to be sampled only from the LIVE wait loop, so it was blind
        // through exactly the phase in which a mass dead-lettering happens. Sample right after the
        // write.
        sampleDeadLetterBacklog();
        return advanceCheckpointPastError(
            projectionName,
            chunk.getLast().globalOffset(),
            dlqLease.get().epoch(),
            consecutiveErrors);
      }
      case HALT -> {
        long backoffMs = Math.min(1000L * (1L << Math.min(consecutiveErrors, 16)), 60_000L);
        logger.warn(
            "Error processing batch for projection '{}' (attempt {}/{}, backoff {}ms). Events will be re-processed.",
            projectionName.value(),
            consecutiveErrors,
            MAX_CONSECUTIVE_ERRORS,
            backoffMs,
            e);
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          logger.error(
              "Projection '{}' halted after {} consecutive failures",
              projectionName.value(),
              consecutiveErrors);
          // Set state=ERROR BEFORE clearing running: the run() thread wakes on running==false and
          // its finally reads state to decide unregister (clean stop) vs mark-terminal-DOWN (halt).
          // Ordering the volatile writes this way guarantees it observes ERROR, not a stale LIVE.
          lastError.set(errorMessage(e));
          state.set(ProjectionState.ERROR);
          stopCurrentRun();
          throw new RuntimeException(
              "Projection '"
                  + projectionName.value()
                  + "' halted after "
                  + consecutiveErrors
                  + " consecutive failures",
              e);
        }
        if (!sleepQuietly(backoffMs)) {
          return ErrorAction.STOP;
        }
        return ErrorAction.RETRY_FROM_CHECKPOINT;
      }
      default -> throw new IllegalStateException("Unknown error strategy: " + errorStrategy);
    }
  }

  /**
   * Advances the catch-up checkpoint past a SKIP'd or dead-lettered batch, through the EPOCH-FENCED
   * empty-batch commit rather than a raw {@link OffsetStore#saveOffset}.
   *
   * <p>A raw {@code saveOffset} of the skip/DLQ endpoint is a <em>forward</em> checkpoint move,
   * which the monotonic offset guard cannot reject — so a stale leader still inside its local
   * {@code leadership.current()}-staleness window could advance the checkpoint past events a newer
   * leader has not yet applied, silently and permanently skipping them from the read model (no
   * dead-letter under SKIP). Routing the advance through {@code atomicProcessor.executeAtomically}
   * with an <b>empty batch</b> takes the checkpoint row lock, runs {@code rejectStaleEpoch} (the
   * epoch fence) and the monotonic guard, and stamps the epoch with NO read-model write; a
   * superseded leader's advance is rejected with an {@link OptimisticLockException}, exactly like
   * the normal atomic-batch commit path. Under {@code nonAtomicAtLeastOnce()} (single-node, {@code
   * fencingEpoch == 0}) this is the plain {@code saveOffset}.
   *
   * <p>A fence rejection ({@link OptimisticLockException}) is a LEADERSHIP/CAS signal, not a
   * transient blip: abandon WITHOUT advancing and {@link ErrorAction#STOP} so the outer loop drops
   * to STANDBY (the next leader re-reads from the fresh committed checkpoint). Do NOT retry it
   * (that would re-enter the error strategy and re-attempt the advance).
   *
   * <p>Any OTHER {@link OffsetStore#saveOffset} / commit failure here is a TRANSIENT infrastructure
   * blip (a DB failover), NOT a reason to kill the projection — the old code let the throw
   * propagate out of {@link #handleBatchError} → {@link #catchUp} → {@link #run} which set
   * state=ERROR and marked the projection terminally DOWN. On such a failure the offset is NOT
   * advanced; back off with the same capped exponential backoff and {@link
   * ErrorAction#RETRY_FROM_CHECKPOINT} so the failed range is re-read and re-skipped /
   * re-dead-lettered idempotently (the dead-letter store upserts on {@code (projection_name,
   * from_offset)}, so a re-DLQ is a no-op) once the store recovers, instead of the projection dying
   * on a seconds-long blip. Returns {@link ErrorAction#CONTINUE_NEXT_BATCH} once the advance
   * commits, or {@link ErrorAction#STOP} if fenced out or interrupted during backoff.
   */
  private ErrorAction advanceCheckpointPastError(
      ProjectionName projectionName,
      GlobalOffset newOffset,
      long fencingEpoch,
      int consecutiveErrors) {
    try {
      runHeldFromClose(
          () -> {
            atomicProcessor.executeAtomically(
                projectionName, List.of(), newOffset, fencingEpoch, NO_OP_UPDATER, offsetStore);
            return null;
          });
      commitRejections.reset();
      return ErrorAction.CONTINUE_NEXT_BATCH;
    } catch (ProjectionCommitFencedException fenced) {
      // The fenced empty-batch advance was rejected — a newer leader took over the lease (epoch
      // fence) or the monotonic guard rejected a non-advancing save. A LEADERSHIP/CAS signal, not a
      // transient failure: abandon WITHOUT advancing and drop to STANDBY, exactly like the
      // atomic-batch fenced-commit path. Never retry (that would re-enter the error strategy). A
      // monotonic rejection that repeats at an unchanged checkpoint halts the runner instead.
      ProjectionCheckpointDivergedException diverged =
          haltIfRejectionRepeats(projectionName, fenced);
      if (diverged != null) {
        throw diverged;
      }
      logger.info(
          "Projection '{}' SKIP/DLQ checkpoint advance to {} rejected ({} guard) — abandoning"
              + " without advancing: {}",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          newOffset.value(),
          fenced.guard(),
          fenced.getMessage());
      return ErrorAction.STOP;
    } catch (RuntimeException saveError) {
      long backoffMs = Math.min(1000L * (1L << Math.min(consecutiveErrors, 16)), 60_000L);
      logger.warn(
          "Projection '{}' failed to advance the checkpoint past a SKIP/DLQ'd batch ending at {}"
              + " (backoff {}ms) — transient infra failure; not advancing, retrying from checkpoint",
          projectionName.value(),
          newOffset.value(),
          backoffMs,
          saveError);
      if (!sleepQuietly(backoffMs)) {
        return ErrorAction.STOP;
      }
      return ErrorAction.RETRY_FROM_CHECKPOINT;
    }
  }

  /**
   * Writes the failed batch range to the dead letter store, returning {@code true} on a successful
   * write and {@code false} if the store write itself threw. A failed dead-letter write is a
   * TRANSIENT infrastructure failure (the DLQ store is down): the caller must NOT advance the
   * offset past the range on {@code false} — advancing would silently, permanently lose the poison
   * range from the read model (only a log line). Instead the caller retries with backoff so the
   * range is re-attempted once the store recovers and is never silently skipped.
   */
  private boolean writeDeadLetter(
      ProjectionName projectionName,
      List<EventEnvelope> failedBatch,
      Exception error,
      int attempts) {
    var entry =
        new ProjectionDeadLetterEntry(
            projectionName,
            failedBatch.getFirst().globalOffset(),
            failedBatch.getLast().globalOffset(),
            failedBatch.size(),
            error.getClass().getName(),
            // Persisted free text, sanitized at the projection_dead_letters sink.
            LogSanitizer.sanitizeFreeText(error.getMessage()),
            attempts,
            Instant.now());
    try {
      deadLetterStore.save(entry);
      recordDeadLettered(projectionName);
      logger.error(
          "DLQ: Projection '{}' failed batch [{}-{}] written to dead letter store. Continuing past"
              + " the range.",
          projectionName.value(),
          entry.fromOffset().value(),
          entry.toOffset().value());
      return true;
    } catch (Exception dlqError) {
      logger.error(
          "DLQ: Projection '{}' failed AND dead letter store write failed — treating as a transient"
              + " infra failure; NOT advancing past the range so it is not silently lost.",
          projectionName.value(),
          dlqError);
      return false;
    }
  }

  /**
   * Runs {@code write} with {@link #close()}'s interrupt held back when called on the thread of the
   * run holding the runner: the flag is cleared for the call and a stop requested meanwhile (or
   * already flagged) is delivered afterwards. On any other thread — a live batch runs on its
   * subscription's poll thread, whose own close() holds its interrupt the same way — it just runs.
   * Sleeps and waits stay outside, so close() still cuts them short.
   */
  private <T> T runHeldFromClose(Supplier<T> write) {
    RunSlot current = slot.get();
    if (current.thread() != Thread.currentThread() || current.interrupts() == null) {
      return write.get();
    }
    current.interrupts().defer();
    try {
      return write.get();
    } finally {
      current.interrupts().resume();
    }
  }

  /**
   * A batch failed after the run was told to stop: the failure may be the stop's interrupt cutting
   * the batch short, so the error strategy writes nothing. The range is read again on the next run.
   */
  private ErrorAction stopBeforeErrorWrite(ProjectionName projectionName) {
    logger.info(
        "Projection '{}' is stopping; the failed batch is neither skipped nor dead-lettered and is"
            + " read again on the next start",
        LogSanitizer.sanitizeForLog(projectionName.value()));
    return ErrorAction.STOP;
  }

  /** Returns {@code false} if interrupted (the interrupt flag is restored). */
  private static boolean sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void processBatch(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      List<EventEnvelope> batch) {
    // Pre-commit leadership re-check: the atomic commit below advances the offset, so if leadership
    // was lost since we started the batch we must NOT commit — a stale leader regressing (or
    // racing) the checkpoint is exactly the split-brain this guards against. Abandon without
    // committing; the outer loop drops to STANDBY and the next leader re-reads from the last
    // committed checkpoint. NOOP is always leader (epoch 0, unfenced), so single-instance behavior
    // is unchanged.
    Optional<SubscriptionLeadership.Lease> lease = leadership.current(projectionName.value());
    if (lease.isEmpty()) {
      throw new LeadershipLostException(projectionName.value());
    }
    GlobalOffset newOffset = batch.getLast().globalOffset();
    long start = System.nanoTime();
    try {
      // Only a registration that writes through the handed repository has a read model in the
      // processor's store to prepare; an at-least-once one keeps its read models wherever it
      // writes them.
      if (mode.writesInCheckpointTransaction()) {
        atomicProcessor.prepareReadModel(projectionName);
      }
      // Thread the held lease's fencing epoch into the atomic commit: a transactional processor
      // rejects a commit whose epoch is below the epoch already stamped for this projection,
      // fencing out a superseded (partitioned old) leader. NOOP's epoch 0 is unfenced.
      atomicProcessor.executeAtomically(
          projectionName,
          batch,
          newOffset,
          lease.get().epoch(),
          // Only a TRANSACTIONAL_LOCAL / EXTERNAL_EFFECT registration may write inside the
          // processor's transaction; an AT_LEAST_ONCE_IDEMPOTENT one is handed null and writes
          // wherever it writes, outside it — under every processor.
          txRepository ->
              projection.process(batch, mode.writesInCheckpointTransaction() ? txRepository : null),
          offsetStore);
      commitRejections.reset();
      recordProcessed(projectionName, batch.size());
    } catch (RuntimeException e) {
      recordFailed(projectionName);
      throw e;
    } finally {
      recordDuration(projectionName, System.nanoTime() - start);
    }
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

  /**
   * The one INFO line per registration — the declared mode, the processor it runs on and the
   * policy's verdict — plus the delivery-mode gauge; a metrics failure never disrupts the run.
   */
  private void logDeliveryMode(
      ProjectionName projectionName,
      ProjectionDeliveryMode mode,
      ProjectionDeliveryPolicy.Verdict verdict) {
    logger.info(
        "Projection '{}' delivery mode {} on {} — {}",
        LogSanitizer.sanitizeForLog(projectionName.value()),
        mode,
        ProjectionDeliveryPolicy.processorLabel(atomicProcessor),
        verdict.describe());
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

  private void recordDuration(ProjectionName projectionName, long durationNanos) {
    try {
      metrics.recordProjectionDuration(projectionName, durationNanos);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDuration failed for projection '{}'", projectionName.value(), e);
    }
  }

  private void subscribeToLiveEvents(
      ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
    EventSubscription subscription;

    // Build the FULLY-WRAPPED live EventListener once: processLiveBatch applies the
    // configured error strategy + the atomic checkpoint (via processBatch) + the leadership
    // pre-commit gate, and feeds each batch outcome to the health contributor itself
    // (recordHealthSuccess/recordHealthError). Both the default polling path AND the factory path
    // use THIS listener, so a factory-built HybridEventSubscription gets the identical pipeline.
    // Previously the factory received the RAW projection and silently dropped the error strategy,
    // atomic checkpoint, leadership gate, and health tracking — a poison live batch spun forever
    // (offset frozen, nothing dead-lettered, health stuck UP), a crash double-applied, and a
    // superseded split-brain leader had no pre-commit re-check.
    //
    // Health is deliberately NOT layered on as a HealthTrackingEventListener wrapper any
    // more. The wrapper recorded a SUCCESS on every normal return — and the SKIP and DLQ arms of
    // processLiveBatch return normally by design (so the subscription advances past the failed
    // batch), so a dead-lettered or skipped batch was recorded as a success, resetting the error
    // count the arm had just earned. The projection stayed health-UP while its checkpoint moved
    // past events that were never applied. processLiveBatch now records the error BEFORE the
    // strategy runs and a success only for a genuinely clean batch, exactly like
    // ScheduledProjectionRunner.
    var liveConsecutiveErrors = new AtomicInteger(0);
    EventListener liveListener =
        events -> processLiveBatch(projectionName, projection, mode, events, liveConsecutiveErrors);

    if (subscriptionFactory != null) {
      // Use factory if provided (for HybridEventSubscription). It receives the runner's wrapped
      // listener so processLiveBatch/health/leadership/atomic-checkpoint all apply; thread
      // this runner's read-poison bound so the factory-built live subscription HALTs a
      // deterministic read-poison instead of retrying it forever — the same bound the
      // default-polling path uses.
      subscription = subscriptionFactory.create(projectionName, liveListener, maxReadPoisonRetries);
      warnIfFactorySubscriptionCannotSignalReadPoison(projectionName, subscription);
    } else {
      // Create default polling subscription with the same wrapped listener.
      subscription =
          PollingEventSubscription.builder()
              .subscriptionName(projectionName.value())
              .eventStore(eventStore)
              .offsetStore(offsetStore)
              .listener(liveListener)
              .config(subscriptionConfig)
              .fetchSize(batchSize)
              // Bound deterministic read poison on the live path too, with
              // the SAME bound catch-up uses. Without this a poison event appended AFTER the
              // projection goes LIVE (rolling deploy new type / corrupt payload) is retried forever
              // by the resilient poll loop — offset frozen, nothing dead-lettered, health stuck UP.
              .readPoisonBound(maxReadPoisonRetries)
              .build();
    }

    // Register the live subscription with the health contributor so health() lists it and
    // overallStatus() reflects its state (a STOPPED/dead subscription reports DOWN). Only lifecycle
    // subscriptions expose the state() the contributor reads; the default polling subscription and
    // HybridEventSubscription both qualify. This REPLACES the runner's standby view registered in
    // run(); a factory subscription that is not a SubscriptionLifecycle simply leaves that standby
    // view in place, so the projection stays visible either way. Swapped back to the
    // standby view on a non-error return. No-op when no contributor is wired.
    registerHealth(projectionName, subscription);
    try {
      subscription.start();
      // Wait until closed, watching for the underlying subscription dying. Without this check a
      // dead subscription would leave the runner reporting LIVE forever while no events flow.
      while (isRunning()) {
        // Sample the projection dead-letter backlog once per steady-state cycle (1s),
        // the projection analog of DeadLetterRetryRunner.sampleObservability. The runner advances
        // its checkpoint past dead-lettered ranges and stays health-UP, so this gauge is the only
        // standing signal that N ranges are un-replayed after a failure burst.
        sampleDeadLetterBacklog();
        // Leadership loss in live mode: stop the subscription and return so the outer loop drops to
        // STANDBY (do NOT error — a failover is expected). Returning here closes the subscription
        // via the finally block; the next leader resumes from the last committed checkpoint. NOOP
        // is always leader, so this never fires in single-instance deployments.
        if (leadership.current(projectionName.value()).isEmpty()) {
          logger.info(
              "Projection '{}' lost leadership in live mode; stopping subscription and standing by",
              projectionName.value());
          return;
        }
        if (!subscription.isRunning()) {
          // A poison-bounded live reader stops itself terminally
          // when a deterministic read/deserialization poison persists past the bound (parity with
          // catchUp()). Surface it as a distinct read-poison ERROR — state=ERROR BEFORE clearing
          // running so run()'s finally observes ERROR and marks the projection health terminally
          // DOWN — rather than the generic "stopped unexpectedly" signal. Recognize ANY {@link
          // ReadPoisonAware} subscription (the default PollingEventSubscription AND the documented
          // factory/Hybrid path's HybridEventSubscription), not only a PollingEventSubscription —
          // otherwise a Hybrid live read-poison would be misreported as a generic subscription
          // death.
          ProjectionReadPoisonException poison =
              subscription instanceof ReadPoisonAware rpa ? rpa.readPoisonError() : null;
          if (poison != null) {
            logger.error(
                "Projection '{}' halting on a read/deserialization poison in LIVE mode after {}"
                    + " attempt(s) — the offending event can never be read on retry; not spinning"
                    + " forever",
                projectionName.value(),
                maxReadPoisonRetries,
                poison);
            lastError.set(poison.getMessage());
            state.set(ProjectionState.ERROR);
            stopCurrentRun();
            throw poison;
          }
          state.set(ProjectionState.ERROR);
          throw new IllegalStateException(
              "Live subscription for projection '"
                  + projectionName.value()
                  + "' stopped unexpectedly — the projection is no longer receiving events");
        }
        Thread.sleep(1000);
      }
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    } finally {
      // Held so the subscription's join is not cut short by close()'s interrupt (an interrupted
      // join returns at once and would let this run finish while a live batch is still being
      // delivered); the interrupt is restored afterwards.
      runHeldFromClose(
          () -> {
            subscription.close();
            return null;
          });
      if (state.get() != ProjectionState.ERROR) {
        // On a NON-error return — a deliberate stop (running cleared, state still LIVE)
        // or a leadership-loss return (drops back to STANDBY, which is healthy) — swap the health
        // entry back to the runner's own standby view instead of UNREGISTERING it. Unregistering
        // made a replica vanish from /health the moment it stopped leading, which is exactly the
        // silent window this fix closes; leaving the just-CLOSED live subscription registered would
        // be equally wrong (its state() reads STOPPED, i.e. a spurious DOWN over a healthy
        // standby). run()'s finally still unregisters on the clean stop. A permanent ERROR exit
        // (HALT exhaustion, dead subscription) keeps the live subscription registered so run()'s
        // finally can force it terminally DOWN — /health must stay DOWN while the read model is
        // frozen, not flip UP because the entry was replaced or removed.
        //
        // PRESERVING the accrued health across this swap (not a fresh registration) — see
        // restoreStandbyHealthViewPreservingHealth. A DLQ'd/skipped chunk right before leadership
        // is lost left the LIVE entry DEGRADED (registerHealth already preserves the
        // symmetric catch-up->live direction); wiping it back to 0 here, with no clean batch ever
        // having run since, reported UP over exactly the same hole the departing leader just
        // observed. The standby now reports DEGRADED until it next processes a clean batch —
        // truthful, since the hole is real and this replica has no evidence it closed.
        restoreStandbyHealthViewPreservingHealth(projectionName);
      }
    }
  }

  /**
   * Feeds a genuinely clean batch — catch-up chunk or live batch — to the health contributor
   * (resets the error count). No-op when no contributor is wired. Mirrors {@code
   * ScheduledProjectionRunner.recordHealthSuccess}.
   */
  private void recordHealthSuccess(ProjectionName projectionName) {
    if (healthContributor != null) {
      healthContributor.recordSuccess(projectionName.value());
    }
  }

  /**
   * Feeds a failed batch to the health contributor BEFORE the error strategy runs, so a
   * dead-lettered or skipped batch reads DEGRADED instead of being masked as a success by the
   * strategy's normal return. No-op when no contributor is wired. Mirrors {@code
   * ScheduledProjectionRunner.recordHealthError}. During catch-up the registered entry is the
   * runner's standby view, so the error is visible there too — catch-up used to be health-silent
   * entirely.
   */
  private void recordHealthError(ProjectionName projectionName) {
    if (healthContributor != null) {
      healthContributor.recordError(projectionName.value());
    }
  }

  /**
   * Samples the projection dead-letter backlog gauge ({@code
   * streamrune.projections.dead_letter_backlog}) — the projection analog of {@code
   * DeadLetterRetryRunner.sampleObservability}. Called once per LIVE wait cycle, once per STANDBY
   * poll, on entering catch-up, and right after every successful dead-letter write on both the
   * catch-up and live paths (the live loop used to be the sole call site, so the gauge was blind
   * through the whole catch-up phase). Skipped when no metrics or no dead-letter store is wired,
   * and the sample is disabled after the first {@link UnsupportedOperationException} from a store
   * that does not support {@code countPending()}. A transient count failure is logged and swallowed
   * so it never disturbs the projection loop; the gauge simply holds its last value.
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

  /**
   * Registers the live subscription with the health contributor when one is wired and the
   * subscription exposes lifecycle state. Replaces the runner's standby view for the duration of
   * LIVE mode; when the subscription is not a {@link SubscriptionLifecycle} (a custom factory
   * subscription) the standby view stays registered, so the projection is never invisible.
   *
   * <p>Uses {@link SubscriptionHealthContributor#replaceLifecycle} rather than {@code register} —
   * the standby view being replaced may have just accrued a catch-up error count (see {@link
   * #recordHealthError}), and a plain {@code register} silently reset it to 0 the instant the
   * runner reported itself LIVE, even though no clean batch had run since.
   */
  private void registerHealth(ProjectionName projectionName, EventSubscription subscription) {
    if (healthContributor == null || !(subscription instanceof SubscriptionLifecycle lifecycle)) {
      return;
    }
    healthContributor.replaceLifecycle(projectionName.value(), lifecycle);
  }

  /**
   * Makes the {@link SubscriptionFactory} read-poison contract observable instead of purely
   * advisory. The factory javadoc says a factory-built subscription MUST thread the {@code
   * readPoisonBound} it is handed, but nothing checks it — and a subscription that does not is
   * undetectable at runtime: it keeps {@code isRunning() == true} while the read model is frozen at
   * a poison checkpoint, so the liveness check at the bottom of the LIVE loop never fires, the
   * {@link ReadPoisonAware} branch cannot apply, and (when the subscription is not a {@link
   * SubscriptionLifecycle} either) the runner's own health view keeps mapping LIVE to UP. The
   * projection then sits frozen and healthy until lag crosses the health threshold — which for a
   * low-volume projection never happens.
   *
   * <p>This WARNs rather than refuses: a third-party subscription may legitimately bound poison its
   * own way, or read from a source that cannot poison at all, and refusing would break those. The
   * message names the class and the exact guarantee that is missing, so the gap shows up in the
   * logs at boot rather than as a silent freeze months later.
   */
  private void warnIfFactorySubscriptionCannotSignalReadPoison(
      ProjectionName projectionName, EventSubscription subscription) {
    if (maxReadPoisonRetries <= 0 || subscription instanceof ReadPoisonAware) {
      return;
    }
    logger.warn(
        "Projection '{}' has a SubscriptionFactory returning {}, which does not implement"
            + " ReadPoisonAware. A deterministic read/deserialization poison (an unregistered event"
            + " type after a rolling deploy, a corrupt payload) can then freeze this projection at"
            + " its checkpoint while the subscription still reports running and /health reports UP,"
            + " because this runner has no way to observe the poison. Thread the readPoisonBound"
            + " ({}) the factory is handed into the subscription and implement ReadPoisonAware —"
            + " HybridEventSubscription, PollingEventSubscription and"
            + " PostgresNotificationSubscription all do.",
        projectionName.value(),
        subscription.getClass().getName(),
        maxReadPoisonRetries);
  }

  /**
   * Registers a FRESH runner read-only health view for {@code projectionName} — the always-present
   * entry that keeps a STANDBY / CATCHING_UP replica visible to {@code /health}. No- op when no
   * contributor is wired.
   *
   * <p>Registration resets the entry's error counter and terminal-error flag — the correct
   * semantics ONLY for {@code run()}'s own initial registration, which starts a genuinely fresh
   * episode with nothing yet to preserve. The live-mode return path does NOT use this method any
   * more — see {@link #restoreStandbyHealthViewPreservingHealth}, which swaps to the same {@link
   * RunnerHealthView} without resetting accrued health. The ERROR path never reaches either method
   * — it keeps the live subscription registered so {@code markTerminalError} can force it
   * terminally DOWN.
   */
  private void registerStandbyHealthView(ProjectionName projectionName) {
    if (healthContributor == null) {
      return;
    }
    healthContributor.register(projectionName.value(), new RunnerHealthView());
  }

  /**
   * Restores the runner's standby health view on a live-mode return (leadership loss, or a
   * deliberate non-error stop) WITHOUT resetting the accrued error count — unlike {@link
   * #registerStandbyHealthView}, which {@code run()} uses for its own genuinely fresh initial
   * registration. Mirrors {@link #registerHealth}'s fix for the symmetric catch-up-&gt;live
   * direction (also a {@link SubscriptionHealthContributor#replaceLifecycle} swap, not a fresh
   * {@code register}): a live batch DLQ'd or skipped right before leadership was lost left the
   * entry DEGRADED, and a fresh registration here wiped that back to 0 the instant the replica
   * stood by, with no clean batch ever having run since. A replica that reports DEGRADED while
   * standing by, until it next processes a genuinely clean batch, is the truthful reading — the
   * read-model hole the departing leader observed is still there and this replica has no evidence
   * it closed. No-op when no contributor is wired.
   */
  private void restoreStandbyHealthViewPreservingHealth(ProjectionName projectionName) {
    if (healthContributor == null) {
      return;
    }
    healthContributor.replaceLifecycle(projectionName.value(), new RunnerHealthView());
  }

  /**
   * Read-only {@link SubscriptionLifecycle} adapter exposing THIS RUNNER's state to the health
   * contributor, so a replica that is standing by (or catching up) is present in the health view
   * instead of invisible. Mirrors {@code ScheduledProjectionRunner.ScheduledSubscriptionView}:
   * {@link ProjectionState#STOPPED} maps to {@link SubscriptionLifecycleState#STOPPED} (which the
   * contributor reads as DOWN) and every other state maps to {@link
   * SubscriptionLifecycleState#RUNNING} — STANDBY, CATCHING_UP and LIVE are all "this subscription
   * is alive here".
   *
   * <p>What makes a standby entry useful rather than noise: the contributor computes lag against
   * the SHARED {@code projection_offset} checkpoint, so a registered standby answers "is this
   * subscription making progress ANYWHERE?". A healthy leader keeps the shared lag low and every
   * standby reports UP; a subscription frozen on every replica reads DEGRADED everywhere, never
   * DOWN, with the growing lag in its detail — which is how the all-replicas-standby freeze this
   * view exists to surface shows up, exactly as on the scheduled runner. Alert on {@code
   * streamrune.subscriptions.lag} to catch it.
   *
   * <p>{@link ProjectionState#ERROR} maps to STOPPED because, unlike a scheduled projection's
   * per-tick error, an ERROR here is terminal: {@code run()} is unwinding. The contributor's {@code
   * markTerminalError} independently forces DOWN on that path, so the mapping only keeps the
   * reported lifecycle state honest.
   *
   * <p>The runner — not the health contributor — owns the lifecycle, so the mutating operations
   * throw.
   */
  private final class RunnerHealthView implements SubscriptionLifecycle {

    @Override
    public SubscriptionLifecycleState state() {
      ProjectionState current = state.get();
      return (current == ProjectionState.STOPPED || current == ProjectionState.ERROR)
          ? SubscriptionLifecycleState.STOPPED
          : SubscriptionLifecycleState.RUNNING;
    }

    @Override
    public boolean isRunning() {
      return state() == SubscriptionLifecycleState.RUNNING;
    }

    @Override
    public void start() {
      throw new UnsupportedOperationException("lifecycle owned by ContinuousProjectionRunner");
    }

    @Override
    public void pause() {
      throw new UnsupportedOperationException("continuous projections cannot be paused here");
    }

    @Override
    public void resume() {
      throw new UnsupportedOperationException("continuous projections cannot be resumed here");
    }

    @Override
    public void close() {
      // No-op: the runner owns the lifecycle; unregistration happens in run()'s finally.
    }
  }

  /**
   * Processes one live batch, applying the configured error strategy on failure. A clean batch
   * records a health success; ANY failure records a health error before the strategy runs, so the
   * strategy's normal return never masks a lost batch. SKIP returns normally so the subscription
   * advances its checkpoint past the failed batch; DLQ classifies the error (Task 7) — a TRANSIENT
   * infra blip under the bound is rethrown to retry the batch (no dead-letter if it recovers), a
   * POISON error (or a TRANSIENT that exhausted the bound) is dead-lettered and the runner returns
   * normally so the subscription advances past and CONTINUES (no halt) — unless that dead-letter
   * WRITE itself fails (DLQ store down), which is rethrown so the subscription does NOT advance and
   * the batch is retried (a failed write is a transient infra failure, never a silent skip); HALT
   * rethrows so the subscription retries the batch with backoff, halting the runner after {@link
   * #MAX_CONSECUTIVE_ERRORS} consecutive failures.
   */
  private void processLiveBatch(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      List<EventEnvelope> events,
      AtomicInteger consecutiveErrors) {
    try {
      processBatch(projectionName, projection, mode, events);
      consecutiveErrors.set(0);
      recordHealthSuccess(projectionName);
    } catch (LeadershipLostException lost) {
      // Leadership lost at the pre-commit re-check: rethrow so the subscription does NOT advance
      // its checkpoint. The live-wait loop's lease re-check then returns and the outer loop stands
      // by. Not a processing error — never dead-lettered or backed off.
      throw lost;
    } catch (ProjectionCommitFencedException fenced) {
      // Live path: the atomic commit was rejected (epoch fence / overlap / monotonic
      // guard) — a leadership/CAS signal, NOT a projection error. Do NOT route it to the error
      // strategy: SKIP would return normally and let PollingEventSubscription advance the
      // checkpoint past the un-applied events, and DLQ would dead-letter a benign fence rejection.
      // Convert it to a BenignSignal so the subscription does NOT advance and re-reads from the
      // fresh committed checkpoint: leadership lost for an epoch-fence rejection, a plain re-read
      // for an overlap or monotonic one. A rejection that repeats at an unchanged checkpoint is
      // not benign: the runner halts, and the halt signal keeps the subscription from advancing.
      ProjectionCheckpointDivergedException diverged =
          haltIfRejectionRepeats(projectionName, fenced);
      if (diverged != null) {
        throw diverged;
      }
      logger.info(
          "Projection '{}' atomic commit rejected ({} guard) in live mode — not advancing the"
              + " subscription, reading again from the checkpoint: {}",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          fenced.guard(),
          fenced.getMessage());
      throw rejectedLiveCommitSignal(projectionName, fenced);
    } catch (ProjectionCheckpointSaveException saveFailed) {
      // Live path: rethrow WITHOUT touching consecutiveErrors — the subscription does not save its
      // offset when the listener throws, and its resilient loop backs off and redelivers from the
      // un-advanced checkpoint; the HALT bound is never consumed by a store outage. recordFailed
      // already ran in processBatch.
      recordHealthError(projectionName);
      logger.warn(
          "Projection '{}' applied the live batch up to {} but the checkpoint save failed; not"
              + " advancing the subscription, it re-delivers from the checkpoint: {}",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          saveFailed.offset().value(),
          String.valueOf(saveFailed.getCause()));
      throw saveFailed;
    } catch (RuntimeException e) {
      int errors = consecutiveErrors.incrementAndGet();
      if (errorHandler != null) {
        errorHandler.accept(e);
      }
      // Record the health error BEFORE the strategy runs. SKIP and DLQ return normally so
      // the subscription advances past the batch; without this, nothing told the health contributor
      // that a batch was lost, and the projection stayed UP over a read model with holes. A benign
      // leadership/fence signal (the two arms above) is not a processing failure and records
      // nothing, like ScheduledProjectionRunner.
      recordHealthError(projectionName);
      switch (errorStrategy) {
        case SKIP -> {
          // PollingEventSubscription would otherwise advance its checkpoint
          // FORWARD past this batch via a RAW post-delivery saveOffset — a forward move the
          // monotonic guard cannot reject, so a stale leader could silently skip events a newer
          // leader has not applied. Re-check leadership first: if the lease was lost while this
          // batch was processing, rethrow LeadershipLostException so the subscription does NOT
          // advance. Otherwise advance the checkpoint ourselves through the EPOCH-FENCED
          // empty-batch commit — a fence rejection (newer leader) throws LeadershipLostException
          // (stand by, no advance); on success the checkpoint is at this batch's end, so the
          // subscription's own saveOffset to the same offset is a monotonic no-op. NOOP is always
          // leader at epoch 0 (unfenced), so single-instance behavior is unchanged.
          Optional<SubscriptionLeadership.Lease> skipLease =
              leadership.current(projectionName.value());
          if (skipLease.isEmpty()) {
            logger.info(
                "Projection '{}' lost leadership before a SKIP checkpoint advance in live mode;"
                    + " not advancing the subscription and standing by",
                projectionName.value());
            throw new LeadershipLostException(projectionName.value());
          }
          logger.warn(
              "SKIP: Projection '{}' skipping live batch ending at offset {} due to error: {}",
              projectionName.value(),
              events.getLast().globalOffset().value(),
              e.getMessage(),
              e);
          advanceLiveCheckpointFenced(
              projectionName, events.getLast().globalOffset(), skipLease.get().epoch());
          consecutiveErrors.set(0);
        }
        case DLQ -> {
          // Poison-vs-transient, mirroring the catch-up path. A TRANSIENT infra blip under
          // the retry bound is rethrown so PollingEventSubscription does NOT advance and retries
          // the batch with backoff — no dead-letter if it recovers. A POISON error (or a TRANSIENT
          // that exhausted the bound) is dead-lettered, and we return normally so the subscription
          // advances past the range and CONTINUES (behavior change: live DLQ no longer halts).
          ProjectionErrorClass errorClass = classifier.classify(e);
          if (errorClass == ProjectionErrorClass.TRANSIENT && errors < MAX_CONSECUTIVE_ERRORS) {
            logger.warn(
                "DLQ: Projection '{}' TRANSIENT error in live mode (attempt {}/{}) — not advancing,"
                    + " will retry the batch: {}",
                projectionName.value(),
                errors,
                MAX_CONSECUTIVE_ERRORS,
                e.getMessage(),
                e);
            // Rethrow so the subscription retries with backoff without advancing the checkpoint.
            throw e;
          }
          // POISON, or TRANSIENT bound exhausted: dead-letter and advance past the range.
          // Re-check leadership first: advancing the checkpoint is a forward move the monotonic
          // guard cannot reject, so a stale leader must NOT advance. Capture the held
          // lease so the advance below can be epoch-fenced.
          Optional<SubscriptionLeadership.Lease> dlqLease =
              leadership.current(projectionName.value());
          if (dlqLease.isEmpty()) {
            logger.info(
                "Projection '{}' lost leadership before a DLQ checkpoint advance in live mode; not"
                    + " advancing the subscription and standing by",
                projectionName.value());
            throw new LeadershipLostException(projectionName.value());
          }
          if (errorClass == ProjectionErrorClass.TRANSIENT) {
            logger.warn(
                "DLQ: Projection '{}' TRANSIENT error exhausted the retry bound ({}) in live mode;"
                    + " dead-lettering and continuing",
                projectionName.value(),
                MAX_CONSECUTIVE_ERRORS);
          } else {
            logger.warn(
                "DLQ: Projection '{}' POISON error in live mode — dead-lettering after first"
                    + " failure and continuing: {}",
                projectionName.value(),
                e.getMessage(),
                e);
          }
          if (!writeDeadLetter(projectionName, events, e, errors)) {
            // The dead-letter STORE WRITE itself failed (DLQ store down) — a TRANSIENT infra
            // failure. Rethrow so PollingEventSubscription does NOT advance its checkpoint (exactly
            // like the leadership-lost path): the resilient poll loop re-reads and re-attempts the
            // batch with backoff once the store recovers, so the poison range is never silently
            // lost. consecutiveErrors is left incremented so a persistently-down store still walks
            // toward the retry bound rather than looping forever at attempt 1.
            logger.warn(
                "DLQ: Projection '{}' dead-letter WRITE failed in live mode (attempt {}/{}) —"
                    + " transient infra failure; not advancing, will retry the batch",
                projectionName.value(),
                errors,
                MAX_CONSECUTIVE_ERRORS);
            throw e;
          }
          // Make the backlog gauge reflect this write immediately rather than at the
          // live loop's next 1 s tick.
          sampleDeadLetterBacklog();
          // Advance the checkpoint past the dead-lettered batch through the EPOCH-FENCED
          // empty-batch commit: a fence rejection stands by without advancing; on
          // success the subscription's own saveOffset to the same offset is a monotonic no-op.
          advanceLiveCheckpointFenced(
              projectionName, events.getLast().globalOffset(), dlqLease.get().epoch());
          consecutiveErrors.set(0);
        }
        case HALT -> {
          if (errors >= MAX_CONSECUTIVE_ERRORS) {
            logger.error(
                "Projection '{}' halted after {} consecutive failures",
                projectionName.value(),
                errors);
            // Set state=ERROR BEFORE clearing running so the run() thread — which exits its live
            // wait on running==false — is guaranteed to observe ERROR (not a stale LIVE) in the
            // finally that chooses unregister (clean stop) vs mark-terminal-DOWN (halt).
            lastError.set(errorMessage(e));
            state.set(ProjectionState.ERROR);
            stopCurrentRun();
          } else {
            logger.warn(
                "Error processing live batch for projection '{}' (attempt {}/{}). Events will be re-processed.",
                projectionName.value(),
                errors,
                MAX_CONSECUTIVE_ERRORS,
                e);
          }
          // Rethrow so the subscription does not advance the checkpoint and retries with backoff.
          throw e;
        }
      }
    }
  }

  /**
   * Advances the LIVE subscription's checkpoint past a SKIP'd / dead-lettered batch through the
   * EPOCH-FENCED empty-batch commit, instead of relying on {@link PollingEventSubscription}'s raw
   * post-delivery {@code saveOffset} — a forward move the monotonic guard cannot reject, through
   * which a stale leader (still inside its local {@code leadership.current()}-staleness window)
   * could silently skip events a newer leader has not yet applied. On success the checkpoint is at
   * {@code toOffset}, so the subscription's subsequent {@code saveOffset} to the same offset is a
   * monotonic no-op. On a fence rejection ({@link ProjectionCommitFencedException} — a newer leader
   * took over the lease, or the monotonic guard rejected a non-advancing save) throws a benign
   * signal so the subscription does NOT advance and re-reads from the fresh committed checkpoint; a
   * monotonic rejection that repeats at an unchanged checkpoint halts the runner. A transient
   * (non-fence) commit failure propagates so the resilient poll loop backs off and re-reads the
   * batch from the un-advanced checkpoint. NOOP's epoch 0 is unfenced, so single-node advances
   * always commit.
   */
  private void advanceLiveCheckpointFenced(
      ProjectionName projectionName, GlobalOffset toOffset, long fencingEpoch) {
    try {
      atomicProcessor.executeAtomically(
          projectionName, List.of(), toOffset, fencingEpoch, NO_OP_UPDATER, offsetStore);
      commitRejections.reset();
    } catch (ProjectionCommitFencedException fenced) {
      ProjectionCheckpointDivergedException diverged =
          haltIfRejectionRepeats(projectionName, fenced);
      if (diverged != null) {
        throw diverged;
      }
      logger.info(
          "Projection '{}' SKIP/DLQ checkpoint advance to {} rejected ({} guard) in live mode —"
              + " not advancing the subscription: {}",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          toOffset.value(),
          fenced.guard(),
          fenced.getMessage());
      throw rejectedLiveCommitSignal(projectionName, fenced);
    }
  }

  @Override
  public void reset(ProjectionName projectionName) {
    // Unguarded rewind: saveOffset(initial()) would be no-op'd by the monotonic offset guard, so a
    // reset must use the dedicated OffsetStore.reset() path (see OffsetStore contract).
    offsetStore.reset(projectionName);
    // Defense-in-depth: even a correctly-overridden reset() could have a latent bug (e.g. an
    // unguarded write that targets the wrong row/key) that leaves the checkpoint un-rewound.
    // Verify the rewind actually took before declaring the reset successful, converting a silent
    // partial rewind into a loud failure instead of a rebuild that quietly resumes from the old
    // offset.
    GlobalOffset after = offsetStore.getLastOffset(projectionName);
    if (!GlobalOffset.initial().equals(after)) {
      throw new IllegalStateException(
          "OffsetStore.reset(\""
              + projectionName.value()
              + "\") returned but the checkpoint did not rewind to initial (still at "
              + after.value()
              + "); this OffsetStore's reset() is not performing an unguarded rewind as required by"
              + " the OffsetStore contract.");
    }
  }

  /**
   * Claims the runner for the calling run(), publishing its thread and completion latch in the same
   * atomic swap. A held stop is consumed by claiming the runner already {@link Phase#STOPPING}.
   * Returns the claimed slot.
   */
  private RunSlot claimRunner(ProjectionName projectionName, CountDownLatch done) {
    Thread self = Thread.currentThread();
    var interrupts = new InterruptDeferral();
    return slot.updateAndGet(
        current ->
            switch (current.phase()) {
              case IDLE -> new RunSlot(Phase.RUNNING, self, done, interrupts);
              case STOP_HELD -> new RunSlot(Phase.STOPPING, self, done, interrupts);
              case RUNNING ->
                  throw new IllegalStateException(
                      "Runner already running for: "
                          + LogSanitizer.sanitizeForLog(projectionName.value()));
              // The previous run was told to stop but has not released the runner yet —
              // e.g. a close() timed out with its thread stuck in an uninterruptible JDBC call.
              // Reviving now would put a second processing loop on the same projection beside the
              // stuck one, double-applying under nonAtomicAtLeastOnce(). Refuse; once
              // that thread drains, its finally releases the runner and a restart claims it.
              // Mirrors the ScheduledProjectionRunner.stop() contract.
              case STOPPING ->
                  throw new IllegalStateException(
                      "ContinuousProjectionRunner is not restartable for '"
                          + LogSanitizer.sanitizeForLog(projectionName.value())
                          + "': the previous run() was told to stop but its thread has not exited"
                          + " yet (e.g. a close() timed out with it still processing). It becomes"
                          + " restartable once that thread drains.");
            });
  }

  /**
   * Drops a stop held from before this call, so the next {@link #run(ProjectionName, Projection,
   * ProjectionDeliveryMode)} processes. For an owner that is about to hand run() to a fresh thread
   * and knows that any stop it issued earlier was meant for an earlier run: {@code
   * StreamRune.startProjections()} after a {@code stopProjections()} that found the runner idle
   * (its run had ended on its own, or had never started). A stop issued after this call is still
   * held for the coming run(), so the race that slot closes stays closed. Has no effect unless a
   * stop is held.
   */
  void discardHeldStop() {
    slot.compareAndSet(RunSlot.STOP_HELD, RunSlot.IDLE);
  }

  /** True while the run holding the runner has not been told to stop; every loop checks it. */
  private boolean isRunning() {
    return slot.get().phase() == Phase.RUNNING;
  }

  /**
   * Stops the run holding the runner from inside that run (a halt, or its own exit). Unlike an
   * owner's stop it is never held, so a late one cannot cancel the next run.
   */
  private void stopCurrentRun() {
    slot.updateAndGet(RunSlot::afterRunStops);
  }

  /**
   * Signals the runner to stop without waiting. The {@link #run(ProjectionName, Projection,
   * ProjectionDeliveryMode)} thread observes the signal at its next loop check or sleep wake-up and
   * exits on its own — callers that manage the run thread themselves (e.g. {@link
   * MultiProjectionRunner}) use this to signal all runners before interrupting and joining their
   * threads. Use {@link #close()} to also interrupt the run thread and wait for it to finish.
   *
   * <p>A stop that arrives while no run() holds the runner — typically because the owner handed
   * run() to a fresh thread that has not reached it yet — is held: the next run() returns at once
   * without reading or processing anything and leaves the runner {@link ProjectionState#STOPPED}. A
   * stop is consumed by the run it ends (or, when held, by the next run()), so it never outlives
   * that run to cancel a later restart. The converse also holds: stopping a runner whose run() has
   * already returned holds that stop for whichever run() comes next.
   */
  public void requestStop() {
    slot.updateAndGet(RunSlot::afterStopRequest);
  }

  /**
   * Interrupts the thread of the run holding the runner the way {@link #close()} does — at once, or
   * when the store write it is in ends — without waiting. For {@link MultiProjectionRunner}, which
   * signals every runner with {@link #requestStop()} before joining their threads.
   *
   * @return {@code false} when no run holds the runner (nothing was interrupted)
   */
  boolean interruptRun() {
    RunSlot current = slot.get();
    if (current.thread() == null || current.interrupts() == null) {
      return false;
    }
    current.interrupts().interrupt(current.thread());
    return true;
  }

  /**
   * Stops the runner and waits for an in-flight {@link #run(ProjectionName, Projection,
   * ProjectionDeliveryMode)} to return. The run thread is interrupted so catch-up backoff and
   * live-mode waits (up to 60 s) end promptly; without the join, tearing down shared resources
   * (e.g. a DataSource) right after close() races the still-processing batch.
   *
   * <p>The interrupt never lands in a store write. On a virtual thread an interrupt fails every
   * blocking socket call made while the flag is set, so a catch-up batch and its checkpoint commit,
   * the error strategy's dead-letter write and checkpoint advance, the takeover epoch stamp, the
   * live subscription's close and the leadership resign run with it held back; it is delivered when
   * they finish, and close() waits for them. A batch in flight therefore commits instead of failing
   * and being skipped or dead-lettered, and the lease is released instead of left to expire. Reads
   * stay interruptible: one cut short ends the run without an ERROR.
   *
   * <p>Waits up to {@value #CLOSE_JOIN_TIMEOUT_SECONDS} seconds; if run() has not returned by then
   * (e.g. the projection is stuck), a warning is logged and close() returns anyway. Calling close()
   * from within the projection or error handler (i.e. on the run thread itself) only signals and
   * returns — joining the current thread would deadlock.
   *
   * <p>Like {@link #requestStop()}, a close() that reaches the runner before run() does is held:
   * that run() returns at once without reading or processing anything.
   */
  @Override
  public void close() {
    // The slot this stop was applied to names the run it targets: the thread and latch travel with
    // the claim, so there is no window in which a claimed run is invisible to close().
    RunSlot stopped = slot.getAndUpdate(RunSlot::afterStopRequest);
    Thread t = stopped.thread();
    if (t == null || t == Thread.currentThread()) {
      return;
    }
    stopped.interrupts().interrupt(t);
    CountDownLatch done = stopped.done();
    try {
      if (!done.await(CLOSE_JOIN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        logger.warn(
            "Projection runner did not stop within {} s after close() — the run() thread is still"
                + " processing and may touch shared resources after this call returns",
            CLOSE_JOIN_TIMEOUT_SECONDS);
      }
    } catch (InterruptedException _) {
      // The closing thread is being shut down itself — leave the run thread to exit on its own.
      Thread.currentThread().interrupt();
    }
  }

  /** Returns the current lifecycle state of this runner. */
  public ProjectionState state() {
    return state.get();
  }

  /**
   * Returns the message of the failure that put this runner into {@link ProjectionState#ERROR}, or
   * {@code null} when the runner has not failed. Cleared at the start of each {@link
   * #run(ProjectionName, Projection, ProjectionDeliveryMode)} invocation that processes, and kept
   * by one that returns at once on a held stop.
   */
  public String lastError() {
    return lastError.get();
  }

  /** Never-null failure description for status reporting. */
  private static String errorMessage(Throwable t) {
    return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
  }

  /**
   * Factory interface for creating the live subscription (e.g. a {@code HybridEventSubscription}).
   *
   * <p>The factory receives the runner's FULLY-WRAPPED {@link EventListener} — NOT the raw {@link
   * Projection}. Wire it straight onto the subscription builder (e.g. {@code
   * HybridEventSubscription.builder().listener(listener)}) so that {@code processLiveBatch} — the
   * configured error strategy (SKIP/DLQ/HALT), the atomic offset checkpoint, the leadership
   * pre-commit gate, and health/error tracking — applies to factory-built subscriptions exactly as
   * it does to the default polling subscription. Passing the raw projection would silently
   * downgrade the recommended-for-production Hybrid path in LIVE mode: a poison batch would spin
   * forever, a crash would double-apply, and a split-brain leader would have no pre-commit
   * re-check.
   *
   * <p>{@code readPoisonBound} is the owning runner's configured max consecutive deterministic
   * read-poison retries. A factory building a poison-bounded subscription — {@code
   * HybridEventSubscription} or {@link PollingEventSubscription} — MUST thread it (e.g. {@code
   * HybridEventSubscription.builder().readPoisonBound(readPoisonBound)}) so a live read-poison on
   * the factory path HALTs the projection instead of retrying an unreadable event forever, exactly
   * like the default-polling path. {@code 0} disables the bound (unbounded retry). A subscription
   * that cannot read-poison may ignore the argument.
   */
  @FunctionalInterface
  public interface SubscriptionFactory {
    EventSubscription create(
        ProjectionName projectionName, EventListener listener, int readPoisonBound);
  }

  // Builder
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private int fetchSize = 100;
    private int batchSize = 50;
    private SubscriptionConfig subscriptionConfig = SubscriptionConfig.DEFAULT;
    private SubscriptionFactory subscriptionFactory;
    private Consumer<Throwable> errorHandler;
    private AtomicBatchProcessor atomicProcessor;
    private ProjectionErrorStrategy errorStrategy = ProjectionErrorStrategy.HALT;
    private ProjectionDeadLetterStore deadLetterStore;
    private StreamRuneMetrics metrics;
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private ProjectionErrorClassifier classifier = ProjectionErrorClassifier.DEFAULT;
    private SubscriptionHealthContributor healthContributor;
    private Duration readRetryInitialBackoff = READ_RETRY_INITIAL_BACKOFF;
    private Duration readRetryMaxBackoff = READ_RETRY_MAX_BACKOFF;
    private int maxReadPoisonRetries = DEFAULT_MAX_READ_POISON_RETRIES;

    /**
     * Test-only: overrides the catch-up read-retry backoff bounds. Production uses {@link
     * #READ_RETRY_INITIAL_BACKOFF}/{@link #READ_RETRY_MAX_BACKOFF}. Package-private so the
     * read-poison test can exhaust the poison bound without seconds of real backoff.
     */
    Builder readRetryBackoff(Duration initial, Duration max) {
      this.readRetryInitialBackoff = initial;
      this.readRetryMaxBackoff = max;
      return this;
    }

    /**
     * Test-only: overrides the max consecutive deterministic read-poison failures before the
     * projection HALTs terminally. Production uses {@link #DEFAULT_MAX_READ_POISON_RETRIES}.
     * Package-private.
     */
    Builder maxReadPoisonRetries(int n) {
      this.maxReadPoisonRetries = n;
      return this;
    }

    public Builder eventStore(EventStore store) {
      this.eventStore = store;
      return this;
    }

    public Builder offsetStore(OffsetStore store) {
      this.offsetStore = store;
      return this;
    }

    public Builder fetchSize(int size) {
      this.fetchSize = size;
      return this;
    }

    public Builder batchSize(int size) {
      this.batchSize = size;
      return this;
    }

    public Builder subscriptionConfig(SubscriptionConfig config) {
      this.subscriptionConfig = config;
      return this;
    }

    public Builder subscriptionFactory(SubscriptionFactory factory) {
      this.subscriptionFactory = factory;
      return this;
    }

    public Builder errorHandler(Consumer<Throwable> handler) {
      this.errorHandler = handler;
      return this;
    }

    /**
     * Sets the batch processor this runner commits through. Required: pass the {@code
     * JdbcProjectionRepository} a {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} projection
     * writes to, or {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()} for an {@code
     * AT_LEAST_ONCE_IDEMPOTENT} projection. {@link #run} checks the projection's declared mode
     * against it before any event is read.
     */
    public Builder atomicProcessor(AtomicBatchProcessor processor) {
      this.atomicProcessor = processor;
      return this;
    }

    public Builder errorStrategy(ProjectionErrorStrategy strategy) {
      this.errorStrategy = strategy;
      return this;
    }

    public Builder deadLetterStore(ProjectionDeadLetterStore store) {
      this.deadLetterStore = store;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; when absent (or {@code null}) projection metrics are
     * not recorded and behavior is unchanged. The runner records processed/failed counts and
     * processing duration per batch, tagged with the projection name.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    /**
     * Sets the single-active-consumer leadership coordinator. Optional; defaults to {@link
     * SubscriptionLeadership#NOOP} (always leader), which makes multi-instance and single-instance
     * behavior identical. When a real leadership is injected, only the leader for this projection's
     * name reads/processes/advances; non-leaders run as {@link ProjectionState#STANDBY}.
     */
    public Builder leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
      return this;
    }

    /**
     * Sets the projection error classifier consulted by the {@link ProjectionErrorStrategy#DLQ}
     * strategy to distinguish a TRANSIENT infra blip (retry with backoff, dead-letter only if the
     * bound is exhausted) from a POISON batch (dead-letter after the first failure). Optional;
     * defaults to {@link ProjectionErrorClassifier#DEFAULT} (infrastructure/SQL/IO failures are
     * transient, everything else poison). Not consulted by SKIP or HALT.
     */
    public Builder classifier(ProjectionErrorClassifier classifier) {
      this.classifier = classifier != null ? classifier : ProjectionErrorClassifier.DEFAULT;
      return this;
    }

    /**
     * Sets the subscription health contributor. Optional; when present, the runner is registered
     * with it from the first tick (so {@code health()} lists this projection and {@code
     * overallStatus()} reflects its lifecycle state) and every processed batch — catch-up chunk or
     * live batch — feeds the contributor's success/error counters: a clean batch records a success,
     * and any failed batch records an error BEFORE the error strategy runs, so a dead-lettered or
     * skipped batch reads DEGRADED instead of being masked as a success, mirroring {@link
     * ScheduledProjectionRunner}. When absent (or {@code null}) health tracking is a no-op and
     * behavior is unchanged — single-instance and no-health setups are not affected.
     */
    public Builder healthContributor(SubscriptionHealthContributor healthContributor) {
      this.healthContributor = healthContributor;
      return this;
    }

    public ContinuousProjectionRunner build() {
      if (errorStrategy == ProjectionErrorStrategy.DLQ && deadLetterStore == null) {
        throw new IllegalArgumentException("deadLetterStore is required when errorStrategy is DLQ");
      }
      if (atomicProcessor == null) {
        throw new IllegalArgumentException(
            "atomicProcessor is required: pass the JdbcProjectionRepository your"
                + " TRANSACTIONAL_LOCAL projections write to, or"
                + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
                + " AT_LEAST_ONCE_IDEMPOTENT projections");
      }
      // An unfenceable multi-replica projection is a configuration error, not a
      // degraded mode — refuse to boot rather than run the leadership protocol against a processor
      // that ignores its epoch.
      ProjectionFencingPolicy.requireFencingCapable(
          leadership, atomicProcessor, "ContinuousProjectionRunner");
      return new ContinuousProjectionRunner(
          eventStore,
          offsetStore,
          fetchSize,
          batchSize,
          subscriptionConfig,
          subscriptionFactory,
          errorHandler,
          atomicProcessor,
          errorStrategy,
          deadLetterStore,
          metrics,
          leadership,
          classifier,
          healthContributor,
          readRetryInitialBackoff,
          readRetryMaxBackoff,
          maxReadPoisonRetries);
    }
  }
}

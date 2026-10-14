package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandBus;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTimeoutException;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Polls {@link SagaStore} for sagas that have exceeded their {@link SagaDecider#timeout()} and
 * drives compensation to a terminal status. Runs on a virtual thread.
 *
 * <p><b>Claim-first ownership:</b> for each timed-out saga the runner first claims exclusive
 * ownership of the compensation episode by CAS-writing the saga to {@link SagaStatus#COMPENSATING}
 * <em>before</em> dispatching any compensation command. The order matters: if compensation were
 * dispatched first and the event path won a subsequent race on the terminal write, the
 * already-dispatched compensation would be orphaned (executed against live aggregates but never
 * reconciled in the saga record). With claim-first, losing the claim CAS means no compensation
 * commands were ever dispatched for this attempt — there are no orphaned side effects. Only after
 * the claim succeeds does the runner:
 *
 * <ol>
 *   <li>Pass a {@link org.streamrune.core.saga.SagaTimeoutException} to {@link
 *       SagaDecider#compensate} and classify the outcome via {@link
 *       SagaCommandDispatch#compensateAndClassify} — the same shared utility used by {@link
 *       SagaRunner}, using per-episode idempotency keys (see below).
 *   <li>Persist the terminal status ({@code COMPENSATED} or {@code FAILED}) to the {@link
 *       SagaStore} at the episode's version, so the saga is not re-selected on the next poll cycle.
 * </ol>
 *
 * <p><b>Crash-resume semantics:</b> {@code COMPENSATING} means "a timeout episode has claimed this
 * saga". If the runner crashes after claiming but before the terminal write lands, the saga stays
 * {@code COMPENSATING} and is re-picked by {@code findTimedOut} once another timeout interval
 * elapses (see the class docs on {@link SagaStatus#COMPENSATING}). On resume, {@code
 * processTimedOutSaga} recognizes the saga is already {@code COMPENSATING} and does not re-claim —
 * it reuses the <em>same</em> episode version for its compensation keys. Because the command inbox
 * dedups on that stable key, redispatching the same compensation commands is effectively-once: any
 * command that already ran on the crashed attempt short-circuits, and only truly unexecuted
 * commands run. A genuinely new episode (a fresh claim) gets a new version and therefore fresh
 * keys, so it never collides with a prior episode's dispatches.
 *
 * <p><b>Optimistic concurrency:</b> both the claim write and the terminal-status write use a
 * version CAS via {@link SagaStore#update}. If the event path (or another timeout replica) advances
 * the saga between our load and our claim, the claim CAS fails with an {@link
 * org.streamrune.core.OptimisticLockException}: we skip immediately, before any compensation
 * dispatch — the saga is simply left to whichever writer won. If the claim succeeds but the
 * terminal write later loses a race, the winner can be another timeout-runner replica resuming the
 * same episode, OR the event path resuming the same episode on a redelivered correlated event (it
 * no longer merely defers to a claimed/{@code COMPENSATING} saga — see {@link SagaStepExecutor});
 * either way the runner skips the terminal write silently (DEBUG log), since the per-episode keys
 * mean the winning party's dispatches are the ones that count and ours were harmlessly deduped. A
 * throwing {@code compensate()} on the timeout path has no triggering event to quarantine; {@link
 * SagaStepExecutor} marks the saga {@code FAULTED} (halted — excluded from future polls) using the
 * post-claim episode version and WARN-logs the cause before the CAS-FAULT write.
 *
 * <p><b>The claim waits for a step in flight in this process.</b> A saga's deadline can pass while
 * a delivery is dispatching its forward commands. A claim made then compensates from a state that
 * does not include that step; the step's next forward command is refused at its commit by a runner
 * that fences its forward commands ({@link SagaRunner.Builder#forwardFence}, the default), and is
 * stored after the compensation by one that does not. A runner built with {@link
 * Builder#sagaRunner} shares the runner's {@link SagaStepLock} and takes it around its read of the
 * row and its claim: while a step of the saga holds the lock the claim is not made, the row is left
 * as it is, and the saga — still past its deadline, still selected — is claimed on the first poll
 * that finds the step ended. After {@value #DEFERRALS_BEFORE_WAITING} polls in a row that passed a
 * saga over, the claim waits for the step, for a bounded time and in arrival order, so a delivery
 * that arrives later does not go ahead of it. The claim is never made underneath a step, however
 * long the step lasts: a poll that waited and still found a step in flight says so at WARN, counts
 * {@link StreamRuneMetrics#recordSagaTimeoutClaimDeferred}, and the runner reports {@link
 * #claimsDeferredPastTheWait()} to the health check. The lock is released as soon as the claim is
 * written, before any compensation command. This holds within one process: a timeout runner on
 * another node shares nothing with this node's deliveries, and there the forward fence keeps a
 * forward command from being stored after the claim's compensation. {@link Builder#build()} refuses
 * a builder that names neither {@link Builder#sagaRunner} nor {@link Builder#stepLock}.
 *
 * <p><b>A saga whose claim is deferred takes no slot of the batch.</b> It stays a forward row past
 * its deadline and is selected again on every poll, so counted against {@code batchSize} it would
 * keep later sagas out of the batch for as long as its step lasts. A poll therefore asks the store
 * for two more ids for each saga it passes over — one for each of the store's two populations — and
 * handles everything it reads, so both populations keep the share {@code batchSize} gives them. It
 * reads past at most {@code batchSize} such sagas: no query asks for more than three times {@code
 * batchSize} ids, and a poll repeats its query only while the previous one passed over sagas it had
 * not counted. The sagas a poll has to wait for are tried after all the others, so their waits
 * delay no other saga's claim.
 *
 * <p><b>Fair batches:</b> each poll takes up to {@code batchSize} sagas from {@link
 * SagaStore#findTimedOut}, which serves forward timeouts (oldest start first) and {@code
 * COMPENSATING} re-picks (idle longest first) as two populations interleaved by rank. A backlog of
 * episodes whose compensation keeps failing transiently — each re-drive writes nothing, so they
 * stay the oldest rows by last write — therefore cannot hold the whole batch: forward timeouts keep
 * at least half of every poll and are claimed in deadline order, not after the outage lifts.
 *
 * <p><b>A failing poll backs off.</b> When a poll itself fails — {@code findTimedOut} cannot reach
 * the store, a statement is refused — the loop logs it and retries after a capped exponential
 * backoff with jitter (the poll interval, doubling per consecutive failure, up to one minute), like
 * every other background loop, and {@link #consecutiveFailures()} reports the streak. A decider
 * with no {@link SagaDecider#timeout()} is refused by {@link Builder#build()}: there is nothing to
 * poll against, and such a saga is recovered by {@link SagaCompensationRetrySweeper} instead.
 *
 * <p>Per-saga processing errors are caught and logged (not rethrown), so a single failing saga does
 * not abort the rest of the batch.
 *
 * @param <S> the saga state type
 */
public final class SagaTimeoutRunner<S extends SagaState> {

  private static final Logger LOG = LoggerFactory.getLogger(SagaTimeoutRunner.class);

  /**
   * Upper bound {@link #stop()} waits to join the poll thread before proceeding. The interrupted
   * thread exits promptly in practice; this only caps a pathologically stuck poll.
   */
  private static final long STOP_JOIN_TIMEOUT_MS = 30_000L;

  /** Longest wait between retries of a failing poll, unless the poll interval itself is longer. */
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /**
   * How many polls in a row may pass a saga over because a step of it is in flight before the claim
   * waits for that step instead of asking once.
   */
  static final int DEFERRALS_BEFORE_WAITING = 3;

  /** How long a claim waits for a step in flight once it waits at all. */
  static final Duration DEFAULT_CLAIM_WAIT = Duration.ofSeconds(5);

  private final SagaDecider<S> decider;
  private final SagaStore sagaStore;
  private final SagaType sagaType;
  private final Duration pollInterval;
  private final int batchSize;
  private final Clock clock;
  private final StreamRuneMetrics metrics;
  private final Duration maxCompensatingDwell;
  private final Duration inboxRetentionMaxAge;

  /**
   * The single-step executor to which the timeout path delegates the claim/compensate/terminalize
   * transitions. Built once from a compensation-only adapter over the bare {@link SagaDecider} (the
   * executor needs a {@code SagaOrchestrator}); {@link #processTimedOutSaga} is a thin load+route
   * adapter over {@link SagaStepExecutor#execute}, keeping only the path-specific {@code
   * maxCompensatingDwell} force-FAULT policy out of band.
   */
  private final SagaStepExecutor<S> executor;

  /**
   * The step lock a claim takes around its read of the row and its claim: the {@link SagaRunner}'s
   * when the builder named the runner, so the claim waits for that runner's steps; otherwise the
   * lock the builder named.
   */
  private final SagaStepLock stepLock;

  private final Duration claimWait;

  /**
   * The sagas whose claim the last polls passed over because a step was in flight, with the number
   * of polls in a row. An entry is removed when the saga's claim gets the lock and when the saga
   * leaves the selection (its step wrote a terminal status, or the row is gone), so the map holds
   * only sagas that are past their deadline and in a step at this moment.
   */
  private final Map<SagaId, Integer> deferredClaims = new ConcurrentHashMap<>();

  /** What {@link #claimsDeferredPastTheWait()} answers: written at the end of every poll. */
  private volatile int claimsDeferredPastTheWait;

  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Drives the poll cycle: a failed poll is logged and retried after a capped exponential backoff,
   * never immediately, so a store that is down (or a decider that cannot be polled) costs a handful
   * of attempts a minute instead of a core, a log line per spin and a share of the pool. Its
   * failure count is {@link #consecutiveFailures()}.
   */
  private final ResilientPollLoop pollLoop;

  private volatile boolean running;
  private volatile Thread pollThread;

  private SagaTimeoutRunner(
      SagaDecider<S> decider,
      SagaStore sagaStore,
      CommandBus commandBus,
      SagaType sagaType,
      Duration pollInterval,
      int batchSize,
      Clock clock,
      StreamRuneMetrics metrics,
      Duration maxCompensatingDwell,
      Duration inboxRetentionMaxAge,
      SagaStepLock stepLock,
      Duration claimWait) {
    this.stepLock = stepLock;
    this.claimWait = claimWait;
    this.decider = decider;
    this.sagaStore = sagaStore;
    this.sagaType = sagaType;
    this.pollInterval = pollInterval;
    this.batchSize = batchSize;
    this.clock = clock;
    this.metrics = metrics;
    this.maxCompensatingDwell = maxCompensatingDwell;
    // zero/negative means inbox pruning is disabled (keys never swept), so the key-age
    // guard is inert — same as when the window is unknown (null).
    this.inboxRetentionMaxAge =
        inboxRetentionMaxAge == null
                || inboxRetentionMaxAge.isZero()
                || inboxRetentionMaxAge.isNegative()
            ? null
            : inboxRetentionMaxAge;
    this.executor =
        new SagaStepExecutor<>(
            SagaDeciderAdapter.compensationOnly(decider), sagaStore, commandBus, metrics);
    Duration maxBackoff =
        pollInterval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? pollInterval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "saga-timeout-" + sagaType.value(),
            () -> running,
            () -> this.pollInterval,
            pollInterval,
            maxBackoff);
  }

  /**
   * Starts polling on a virtual thread. Not idempotent — call exactly once, or after a preceding
   * {@link #stop()}. A second call without an intervening stop would spawn a concurrent loop
   * polling the same saga store.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "SagaTimeoutRunner already started — call start() exactly once, or stop() first");
    }
    running = true;
    pollThread =
        Thread.ofVirtual()
            .name("saga-timeout-" + sagaType.value())
            .start(() -> pollLoop.run(this::pollOnce));
  }

  /**
   * Stops polling. Interrupts the poll thread and resets the started guard so {@link #start()} can
   * be called again on the same instance — matches the restartable lifecycle contract (actuator
   * restart, context refresh).
   */
  public void stop() {
    running = false;
    Thread t = pollThread;
    pollThread = null;
    boolean stuck = false;
    if (t != null) {
      t.interrupt();
      // Join the poll thread before resetting `started`, so a stale poll thread cannot
      // still be running when a subsequent start() spawns a new one (a leaked duplicate poller).
      // Self-join guard: a thread cannot join itself (stop() invoked from within the poll thread).
      if (t != Thread.currentThread()) {
        try {
          t.join(STOP_JOIN_TIMEOUT_MS);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
        // The join timed out and the stale poll thread is still alive — a restart would
        // resume a SECOND timeout poller alongside it. Stay not-restartable and log loudly instead
        // of resetting the guard.
        stuck = t.isAlive();
      }
    }
    if (stuck) {
      LOG.warn(
          "SagaTimeoutRunner poll thread did not stop within {}ms; leaving the runner"
              + " not-restartable so a restart cannot spawn a duplicate poller. Investigate the"
              + " stuck poll.",
          STOP_JOIN_TIMEOUT_MS);
    } else {
      // Reset so start() can be called again after stop().
      started.set(false);
    }
  }

  /**
   * Whether {@link #start()} has been called and {@link #stop()} has not. Combined with {@link
   * #isAlive()} this distinguishes a runner that was never started (or was cleanly stopped) from
   * one whose poll thread <em>died</em> — {@code isStarted() && !isAlive()} is the dead-driver
   * signal the saga health check reports DOWN. Mirrors the outbox/DLQ relay liveness surface.
   */
  public boolean isStarted() {
    return started.get();
  }

  /**
   * Whether the runner's poll thread is currently alive. A started runner whose thread is no longer
   * alive died unexpectedly (e.g. an {@link Error} escaped the poll loop) — see {@link
   * #isStarted()}.
   */
  public boolean isAlive() {
    Thread t = pollThread;
    return t != null && t.isAlive();
  }

  /**
   * Consecutive poll-cycle failures; {@code 0} when the last cycle succeeded, rising while {@code
   * findTimedOut}/processing keeps throwing. A nonzero value means the runner is DEGRADED but still
   * alive. Mirrors {@code OutboxPoller.consecutiveFailures()}.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  /**
   * How many sagas past their deadline the latest poll waited for and still did not claim, because
   * a step of each was in flight in this process for the whole wait. {@code 0} when the latest poll
   * claimed every saga it waited for, or waited for none. A nonzero value is a deadline that is not
   * being enforced in this process: a forward command of that saga has not returned (or its
   * attempts follow each other without a gap), the claim is not made underneath it, and it is made
   * by the first poll that finds the step ended — a restart of the process ends such a step and
   * frees its lock. The health check reports the runner {@code DEGRADED} while this is nonzero.
   */
  public int claimsDeferredPastTheWait() {
    return claimsDeferredPastTheWait;
  }

  /**
   * The saga type this runner polls, as {@link SagaType#value()} — the saga state's fully-qualified
   * class name, the value the saga store persists in {@code saga_type}. It keys the runner's health
   * component ({@code saga-timeout:<saga type>}) and is the {@code saga.type} tag of its metrics:
   * Unlike the simple class name it is unique, so two saga-state classes sharing a simple name in
   * different packages report two components and two series instead of hiding one behind the other.
   */
  public String sagaTypeName() {
    return sagaType.value();
  }

  /**
   * Computes the timeout cutoff from the injected {@link Clock} and processes one batch of
   * timed-out sagas. The cutoff is {@code now - timeout}; a {@code STARTED}/{@code RUNNING} saga
   * whose <em>start instant</em> is before the cutoff has exceeded its (absolute) timeout, while a
   * {@code COMPENSATING} crash-resume episode is re-picked on last-write inactivity — see {@link
   * org.streamrune.core.saga.SagaStore#findTimedOut}. Package-private so tests can drive a single
   * poll with a fixed clock and assert the cutoff deterministically.
   */
  void pollOnce() {
    Duration timeout =
        decider.timeout().orElseThrow(() -> new IllegalStateException("No timeout configured"));
    Instant cutoff = clock.instant().minus(timeout);
    processBatch(cutoff);
  }

  /**
   * Processes a single batch of timed-out sagas. Package-private for testing.
   *
   * <p>A saga whose claim is deferred ({@link #claimAtTheDeadline}) takes no slot. The poll asks
   * {@link SagaStore#findTimedOut} for {@code batchSize} ids and two more for each saga it expects
   * to pass over — the sagas the previous poll passed over. A deferred saga is a forward row, and
   * the store gives the forward population half of what is asked for when both populations are
   * backlogged, so two more ids per deferred saga give the forward population the one slot that
   * saga holds and leave both populations the share {@code batchSize} gives them. Every id read is
   * handled. When a query comes back full and the poll has passed over more sagas than it counted,
   * it asks again with the larger number and handles the ids it has not seen; each further query
   * needs sagas newly passed over, and the number counted stops at {@code batchSize}, so no query
   * asks for more than three times {@code batchSize} ids.
   *
   * <p>A saga the poll has to wait for ({@value #DEFERRALS_BEFORE_WAITING} polls in a row passed it
   * over) is counted as passed over when it is read and tried after every other saga of the poll,
   * so its wait delays no other claim. Nothing here starves it: it stays selected, by its start
   * instant, and is tried on every poll.
   */
  void processBatch(Instant cutoff) {
    Set<SagaId> seen = new HashSet<>();
    List<SagaId> toWaitFor = new ArrayList<>();
    int passedOver = 0;
    int readPast = Math.min(deferredClaims.size(), batchSize);
    boolean firstQuery = true;
    while (true) {
      int limit = batchSize + 2 * readPast;
      List<SagaId> timedOut = sagaStore.findTimedOut(sagaType, cutoff, limit);
      if (firstQuery) {
        firstQuery = false;
        // Sample the timed-out backlog gauge each cycle so a stalled-but-alive runner (or a
        // growing timeout backlog) is visible on the metrics endpoint, not only in logs. This is
        // the number of sagas the poll's first query picked up — forward timeouts plus
        // COMPENSATING re-picks. Decoupled from processing: a metric failure never aborts the
        // batch.
        if (metrics != StreamRuneMetrics.NOOP) {
          recordMetric(
              () -> metrics.recordSagaTimedOutBacklog(sagaType.value(), timedOut.size()),
              "recordSagaTimedOutBacklog");
        }
      }
      for (SagaId sagaId : timedOut) {
        if (!seen.add(sagaId)) {
          continue;
        }
        if (deferredClaims.getOrDefault(sagaId, 0) >= DEFERRALS_BEFORE_WAITING) {
          toWaitFor.add(sagaId);
          passedOver++;
        } else if (processTimedOutSaga(sagaId)) {
          passedOver++;
        }
      }
      if (timedOut.size() < limit || passedOver <= readPast || readPast == batchSize) {
        break; // nothing more is selected, or every saga passed over was read past, or the bound
      }
      readPast = Math.min(passedOver, batchSize);
    }
    // A saga this poll did not read is not a forward saga past its deadline within the poll's
    // reach: forget how often its claim was passed over.
    deferredClaims.keySet().retainAll(seen);
    int stillInFlight = 0;
    for (SagaId sagaId : toWaitFor) {
      if (processTimedOutSaga(sagaId)) {
        stillInFlight++;
      }
    }
    claimsDeferredPastTheWait = stillInFlight;
  }

  /**
   * Loads and processes a single timed-out saga as a thin load+route adapter over {@link
   * SagaStepExecutor#execute}. Short-circuits if the saga is already halted (= terminal or {@link
   * SagaStatus#FAULTED}) — a defensive guard the executor's {@code ClaimTimeout} case relies on:
   * that case checks only {@code loaded.isEmpty()}, and {@code SagaStore#update} does NOT
   * terminal-guard {@code FAULTED}, so routing a halted row would CAS it back to {@code
   * COMPENSATING} and dispatch compensation for a quarantined/terminal saga. Otherwise routes by
   * status:
   *
   * <ul>
   *   <li>An already-{@link SagaStatus#COMPENSATING} row → {@link SagaTrigger.ResumeCompensation}:
   *       re-drives the claimed episode at its current version (no re-claim). {@code
   *       catchPoison=true} inside the executor is correct here — the timeout path has no
   *       triggering event, so a throwing {@code compensate()} is CAS-FAULTed, never dead-lettered.
   *       After the resume, this adapter keeps the path-specific {@code maxCompensatingDwell}
   *       force-FAULT via {@link #faultPastDwell}.
   *   <li>A not-yet-{@code COMPENSATING} row → {@link #claimAtTheDeadline}: with the saga's step
   *       lock held, the row is read again and {@link SagaTrigger.ClaimTimeout} CAS-claims it to
   *       {@code COMPENSATING} at that version before any dispatch, then compensates from the claim
   *       version. A lost claim dispatches nothing (no orphaned compensation); while a step of the
   *       saga is in flight in this process the claim is not made at all.
   * </ul>
   *
   * <p>Package-private for testing — tests call this directly to exercise the halted-guard branch
   * without relying on store filtering.
   *
   * @return {@code true} when the saga's claim was deferred because a step of it is in flight in
   *     this process: nothing was written, and the saga holds no slot of the batch
   */
  boolean processTimedOutSaga(SagaId sagaId) {
    try {
      // Type-scoped load so a row owned by a different saga type (a cross-type SagaId collision)
      // reads as "no saga for this type" — never deserialized, routed, or compensated from —
      // rather than relying on the downstream CAS to 0-row after dispatch.
      Optional<LoadedSaga<S>> selected = sagaStore.load(sagaId, sagaType, decider.stateType());
      // HALTED GUARD: skip terminal | FAULTED before routing. The executor's ClaimTimeout case
      // guards only isEmpty(), and update() does not terminal-guard FAULTED, so a halted row
      // routed there would be CAS'd back to COMPENSATING and re-compensated.
      if (selected.isEmpty() || selected.get().status().isHalted()) {
        return false;
      }
      SagaTimeoutException cause =
          new SagaTimeoutException(sagaId, decider.timeout().orElse(Duration.ZERO));
      if (selected.get().status() == SagaStatus.COMPENSATING) {
        resumeClaimedEpisode(sagaId, selected.get(), cause);
        return false;
      }
      return claimAtTheDeadline(sagaId, cause);
    } catch (Exception e) {
      LOG.warn(
          "Failed to process timed-out saga: " + LogSanitizer.sanitizeForLog(sagaId.value()), e);
      return false;
    }
  }

  /**
   * The fresh claim of a saga past its deadline, made with the saga's step lock held around the
   * read of the row and the claim ({@link SagaStepLock}).
   *
   * <p>The lock is asked for once. While a step of the saga is in flight in this process the claim
   * is not made: nothing is read under the lock, nothing is written, and {@link #claimDeferred}
   * counts the poll. Nothing marks the saga as handled — it is selected by its start instant, which
   * no write here touches — so the next poll comes back to it. From the poll after {@value
   * #DEFERRALS_BEFORE_WAITING} such polls in a row the claim waits for the step, for at most the
   * claim wait and in arrival order: a delivery that arrives while the claim waits goes after it. A
   * step that lasts longer than the wait is not waited out: the claim is made by a poll that finds
   * the lock free, or that sees the step end within its wait.
   *
   * <p>With the lock held the row is read again, because the read that routed the saga here is
   * older than the lock: a step may have written since. What the claim then finds:
   *
   * <ul>
   *   <li>a forward row: no step is in flight and none can start before the claim is written, so
   *       the executor's claim at that row's version is refused only by a writer outside this
   *       process. The executor releases the lock right after the claim write and dispatches the
   *       compensation with the lock free;
   *   <li>a row a step has since completed or faulted, or no row: nothing to time out;
   *   <li>a {@code COMPENSATING} row — a step's own claim, or its event write, opened the episode
   *       since the first read: the lock is released and the episode is resumed like a re-pick.
   * </ul>
   *
   * @return {@code true} when the claim was deferred
   */
  private boolean claimAtTheDeadline(SagaId sagaId, SagaTimeoutException cause) {
    int passedOver = deferredClaims.getOrDefault(sagaId, 0);
    Duration wait = passedOver >= DEFERRALS_BEFORE_WAITING ? claimWait : Duration.ZERO;
    SagaStepLock.Hold claimHold;
    try {
      claimHold = stepLock.tryEnterClaim(sagaType, sagaId, wait);
    } catch (SagaStepLockException interrupted) {
      // stop() interrupts the poll thread; its interrupt status is set and the loop ends on it.
      LOG.debug(
          "The timeout claim of saga {} was interrupted while it waited for a step in flight",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      return false;
    }
    if (claimHold == null) {
      claimDeferred(sagaId, wait);
      return true;
    }
    LoadedSaga<S> claimedByAStep;
    try {
      deferredClaims.remove(sagaId);
      Optional<LoadedSaga<S>> current = sagaStore.load(sagaId, sagaType, decider.stateType());
      if (current.isEmpty() || current.get().status().isHalted()) {
        return false;
      }
      if (current.get().status() != SagaStatus.COMPENSATING) {
        executor.execute(sagaId, current, new SagaTrigger.ClaimTimeout(cause, claimHold));
        return false;
      }
      claimedByAStep = current.get();
    } finally {
      claimHold.close(); // every exit on which the executor did not reach its claim
    }
    resumeClaimedEpisode(sagaId, claimedByAStep, cause);
    return false;
  }

  /**
   * A poll passed the saga over: a step of it is in flight in this process, so its claim is not
   * made. Every such poll counts one {@link StreamRuneMetrics#recordSagaTimeoutClaimDeferred}
   * sample. The first polls are expected — a delivery at the deadline, an outage ending in a
   * redelivery — and logged at DEBUG. A poll that waited for the step and still found one in flight
   * is logged at WARN on every occurrence: until a poll finds the saga free, its deadline is not
   * enforced in this process. The poll cannot tell one forward command that does not return from
   * attempts that follow each other without a gap long enough for the claim, and says so.
   */
  private void claimDeferred(SagaId sagaId, Duration waited) {
    int passedOver = deferredClaims.merge(sagaId, 1, Integer::sum);
    recordMetric(
        () -> metrics.recordSagaTimeoutClaimDeferred(sagaType.value()),
        "recordSagaTimeoutClaimDeferred");
    if (waited.isZero()) {
      LOG.debug(
          "Saga {} is past its deadline and a step of it is in flight; its timeout claim waits for"
              + " the next poll ({} in a row)",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          passedOver);
      return;
    }
    LOG.warn(
        "Saga {} is past its deadline, and a step of it has been in flight in this process on {}"
            + " polls in a row; this poll waited {} for it. The timeout claim is not made"
            + " underneath a step: it is made by the first poll that finds the saga free. Either"
            + " one forward command of the saga does not return, or its attempts follow each other"
            + " without a gap — look at the command bus and the aggregate the saga is dispatching"
            + " to. A restart of the process ends a step that does not return and frees its lock.",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        passedOver,
        waited);
  }

  /** How many polls in a row have passed the saga over; for tests of the starvation bound. */
  int deferralsOf(SagaId sagaId) {
    return deferredClaims.getOrDefault(sagaId, 0);
  }

  /**
   * Resumes an already-claimed episode at its current version (no re-claim: routing a COMPENSATING
   * row to ClaimTimeout would re-claim at a NEW version, shifting the episode key → double-dispatch
   * / lost dedup). Then applies the path-specific dwell bound, measured from the DURABLE claim
   * instant (episode_claimed_at, fault-cycle-immutable) and from nothing else. updatedAt is
   * refreshed by EVERY CAS write (markFaulted, the replayed step's write that clears the fault,
   * replay activity), so anchoring on it would let a fault->replay cycle reset the dwell clock —
   * the episode could outlive the command-inbox retention window and a re-drive would re-execute an
   * already-succeeded compensation whose dedup key was pruned (double refund). Every write that
   * enters COMPENSATING stamps the episode, so a COMPENSATING row without the stamp violates the
   * SagaStore contract and is REFUSED — thrown here so the per-saga catch of {@link
   * #processTimedOutSaga} logs it and the row is left for the next cycle; nothing is dispatched and
   * nothing is written.
   *
   * <p>A resume takes no step lock: the row is stamped, and a stamped row is never the row of a
   * forward step.
   */
  private void resumeClaimedEpisode(
      SagaId sagaId, LoadedSaga<S> loaded, SagaTimeoutException cause) {
    if (loaded.episodeVersion() == null || loaded.episodeClaimedAt() == null) {
      throw SagaUnstampedCompensationEpisodeException.ofRow(sagaId, loaded);
    }
    Instant claimInstant = loaded.episodeClaimedAt();
    // Key-age pre-check BEFORE the resume dispatch. The dwell force-FAULT below runs only AFTER
    // the resume (on COMPENSATING_LEFT), so the first post-outage resume of an episode that
    // dwelled past the command-inbox retention window would re-dispatch under possibly-pruned
    // dedup keys — MISSING the inbox and RE-EXECUTING an already-succeeded compensation (double
    // refund) — before any bound applies. Same anchor as the dwell bound and the replayer's stale
    // guard (episode_claimed_at — no third clock, no updatedAt fallback); same remedy as
    // STALE_COMPENSATION_BLOCKED: quarantine FAULTED, dispatch nothing.
    if (inboxRetentionMaxAge != null
        && Duration.between(claimInstant, clock.instant()).compareTo(inboxRetentionMaxAge) > 0) {
      faultStaleEpisode(sagaId, loaded.version(), claimInstant);
      return;
    }
    StepOutcome outcome =
        executor.execute(sagaId, Optional.of(loaded), new SagaTrigger.ResumeCompensation(cause));
    // The dwell bound is TIME-based (from the claim), not attempt-based, so a refused-admission
    // resume keeps the pre-existing dwell semantics here.
    if ((outcome == StepOutcome.COMPENSATING_LEFT || outcome == StepOutcome.COMPENSATING_REFUSED)
        && maxCompensatingDwell != null
        && Duration.between(claimInstant, clock.instant()).compareTo(maxCompensatingDwell) > 0) {
      faultPastDwell(sagaId, loaded.version());
    }
  }

  /**
   * A resumed episode still {@code COMPENSATING} past the dwell bound (measured from the claim
   * instant) is force-FAULTed (halted → not re-driven forever) instead of being re-picked
   * indefinitely. Path-specific to the timeout runner, for sweeper-disabled deployments where this
   * runner is the sole re-drive path — a poison compensation would otherwise outlive the
   * command-inbox retention window and break effectively-once dedup (a pruned
   * succeeded-compensation key would re-execute → double refund). Only applied after a {@link
   * SagaTrigger.ResumeCompensation} that left the episode {@code COMPENSATING} ({@link
   * StepOutcome#COMPENSATING_LEFT}); the resume leaves the row untouched, so the loaded version is
   * still the CAS token.
   */
  private void faultPastDwell(SagaId sagaId, long version) {
    LOG.warn(
        "Timed-out saga {} stuck COMPENSATING past the max dwell bound ({}); marking FAULTED — its"
            + " compensation never durably completed. Once the cause is fixed, resume it with"
            + " SagaDeadLetterReplayer.resumeFaulted(sagaId)",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        maxCompensatingDwell);
    casFault(sagaId, version);
  }

  /**
   * A {@code COMPENSATING} episode whose age (from the durable claim instant) exceeds the
   * command-inbox retention window is quarantined {@code FAULTED} <em>without any dispatch</em> —
   * its succeeded-compensation dedup keys are unprovably fresh (possibly pruned by the {@code
   * InboxRetentionSweeper} during an outage), so a re-dispatch could re-execute an
   * already-succeeded compensation (double refund). Same anchor and remedy as the replayer's {@code
   * STALE_COMPENSATION_BLOCKED}; evaluated BEFORE the resume, unlike {@link #faultPastDwell} which
   * bounds dwell only after a resume attempt.
   */
  private void faultStaleEpisode(SagaId sagaId, long version, Instant claimInstant) {
    LOG.warn(
        "Timed-out saga {} dwelled COMPENSATING for {} — past the command-inbox retention window"
            + " ({}) — so its episode's succeeded-compensation dedup keys may already be pruned; a"
            + " re-dispatch could RE-EXECUTE an already-succeeded compensation (double refund)."
            + " Marked FAULTED WITHOUT re-dispatching — reconcile the partial"
            + " compensation manually, treating the episode's dedup keys as expired, then resume"
            + " it with SagaDeadLetterReplayer.resumeFaulted(sagaId, true)",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        Duration.between(claimInstant, clock.instant()),
        inboxRetentionMaxAge);
    casFault(sagaId, version);
  }

  /**
   * Shared CAS-FAULT tail of {@link #faultPastDwell} and {@link #faultStaleEpisode}: {@code
   * markFaulted}-writes the saga to {@link SagaStatus#FAULTED} at {@code version} — no state write;
   * records {@code pre_fault_status} as the status being replaced. The fault metric is recorded
   * only when the write reports the transition (a still-poison refresh of an already-FAULTED row
   * does not double count).
   */
  private void casFault(SagaId sagaId, long version) {
    try {
      if (sagaStore.markFaulted(sagaId, sagaType, version)) {
        recordMetric(() -> metrics.recordSagaFaulted(sagaType.value()), "recordSagaFaulted");
      }
    } catch (OptimisticLockException _) {
      LOG.debug("Saga {} advanced concurrently; skip", LogSanitizer.sanitizeForLog(sagaId.value()));
    }
  }

  /**
   * Runs a metric-recording action, logging and swallowing any failure. Metric backends must never
   * disrupt saga processing: a throwing recorder must neither corrupt CAS-conflict propagation nor
   * break poison-event isolation.
   */
  private void recordMetric(Runnable metricCall, String metricName) {
    try {
      metricCall.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics recording failed for " + metricName, e);
    }
  }

  /** Returns a new builder for {@link SagaTimeoutRunner}. */
  public static <S extends SagaState> Builder<S> builder() {
    return new Builder<>();
  }

  /** Builder for {@link SagaTimeoutRunner}. */
  public static final class Builder<S extends SagaState> {
    private SagaDecider<S> decider;
    private SagaStore sagaStore;
    private CommandBus commandBus;
    private SagaType sagaType;
    private Duration pollInterval = Duration.ofSeconds(30);
    private int batchSize = 50;
    private Clock clock = Clock.systemUTC();
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private Duration maxCompensatingDwell = null;
    private Duration inboxRetentionMaxAge = null;
    private SagaRunner<?> sagaRunner;
    private SagaStepLock stepLock;
    private Duration claimWait = DEFAULT_CLAIM_WAIT;

    public Builder<S> decider(SagaDecider<S> decider) {
      this.decider = decider;
      return this;
    }

    public Builder<S> sagaStore(SagaStore sagaStore) {
      this.sagaStore = sagaStore;
      return this;
    }

    public Builder<S> commandBus(CommandBus commandBus) {
      this.commandBus = commandBus;
      return this;
    }

    public Builder<S> sagaType(SagaType sagaType) {
      this.sagaType = sagaType;
      return this;
    }

    /**
     * Sets the pause between two polls, and the first wait after a failed one. Optional; defaults
     * to 30 seconds. Must be positive. A poll that fails (the saga store is down, a statement is
     * refused) is retried after this long, then twice as long for each further consecutive failure,
     * jittered and capped at one minute (or at this interval when it is longer), so an outage never
     * turns the runner into a tight loop; {@link SagaTimeoutRunner#consecutiveFailures()} reports
     * how many polls in a row have failed.
     */
    public Builder<S> pollInterval(Duration pollInterval) {
      this.pollInterval = pollInterval;
      return this;
    }

    /**
     * Sets the most sagas one poll handles. Optional; defaults to 50. {@link
     * SagaStore#findTimedOut} splits the batch between forward timeouts and {@code COMPENSATING}
     * re-picks (half each when both are backlogged), so keep it at 2 or more for both to be served
     * on every poll. Must be positive.
     */
    public Builder<S> batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Sets the clock the runner reads "now" from when computing the timeout cutoff in its poll
     * loop. Defaults to {@link Clock#systemUTC()}; inject a fixed clock to assert the cutoff
     * deterministically. Mirrors {@code DeadLetterRetryRunner.Builder.clock}.
     */
    public Builder<S> clock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. Records
     * saga faults, compensation outcomes, and CAS conflicts encountered during timeout processing.
     */
    public Builder<S> metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /**
     * Independent max-COMPENSATING dwell bound. When set, a {@code COMPENSATING} episode this
     * runner resumes whose compensation is still failing transiently is terminalized {@code
     * FAULTED} once it has been stuck compensating for longer than this duration — measured {@code
     * now - episode_claimed_at}, the durable, fault-cycle-immutable claim instant (every write that
     * enters {@code COMPENSATING} stamps it, and a row without it is refused — {@link
     * SagaUnstampedCompensationEpisodeException}, logged per cycle, nothing dispatched), mirroring
     * {@code SagaCompensationRetrySweeper.giveUpAfter} — instead of being re-driven forever. Never
     * the refreshable {@code updatedAt}: a fault→replay cycle rewrites the row and would reset the
     * dwell clock. This bounds dwell even when the compensation-retry sweeper is disabled, so a
     * poison compensation cannot outlive the command-inbox retention window and break
     * effectively-once dedup (a pruned succeeded-compensation key would otherwise re-execute →
     * double refund). Optional; defaults to unset (no independent bound — the sweeper's give-up
     * horizon is the only bound, which is why operators who disable the sweeper should set this).
     * Must be positive when present, and should stay strictly below the inbox retention window.
     */
    public Builder<S> maxCompensatingDwell(Duration maxCompensatingDwell) {
      this.maxCompensatingDwell = maxCompensatingDwell;
      return this;
    }

    /**
     * Sets the command-inbox retention window (i.e. {@code streamrune.inbox.retention-max-age}) so
     * a {@code COMPENSATING} resume can refuse to re-dispatch an episode whose dedup keys may
     * already be pruned. The {@code maxCompensatingDwell} force-FAULT above runs only
     * <em>after</em> a resume, so the first post-outage resume of an episode that dwelled past the
     * inbox window would re-dispatch under possibly-pruned keys — MISSING the inbox and
     * RE-EXECUTING an already-succeeded compensation (double refund) — before any bound applies.
     * With this window set, the resume pre-checks the episode's age — measured from the same
     * durable anchor as {@code maxCompensatingDwell} and the replayer's stale guard: {@code
     * episode_claimed_at}, which every row in a compensation episode carries — and when it exceeds
     * the window, CAS-FAULTs the saga <em>without dispatching anything</em> (the operator-visible
     * quarantine remedy of {@code STALE_COMPENSATION_BLOCKED}). A fresh timeout claim is
     * unaffected: it mints a brand-new episode with fresh keys.
     *
     * <p>Optional; {@code null} (the default) disables the guard, and zero/negative also disables
     * it — that value means inbox pruning itself is disabled, so keys are never swept. Because
     * timeout runners are application-declared (never framework-built), operators must wire their
     * deployment's {@code streamrune.inbox.retention-max-age} here themselves — the same way {@code
     * maxCompensatingDwell} is an application responsibility.
     */
    public Builder<S> inboxRetentionMaxAge(Duration inboxRetentionMaxAge) {
      this.inboxRetentionMaxAge = inboxRetentionMaxAge;
      return this;
    }

    /**
     * Names the {@link SagaRunner} that delivers this saga type's events in this process, so that
     * this runner's claim at a saga's deadline waits for a delivery that is dispatching that saga's
     * forward commands ({@link SagaStepLock}). Without it the claim compensates underneath such a
     * delivery from a state that does not include the step; the delivery's next forward command is
     * then refused at its commit by a runner that fences its forward commands, and stored after the
     * compensation by one that does not.
     *
     * <p>The timeout runner takes the runner's step lock and nothing else from it; the decider, the
     * store, the bus and the saga type are still this builder's. {@link #build()} refuses a runner
     * of another saga type or over a store that keeps other rows: its steps are not the ones this
     * runner's claims race with. The parameter is a {@code SagaRunner<?>} because that check is
     * made on the runner's saga type at {@code build()}, and an application often holds its runner
     * as a {@code SagaRunner<?>} bean.
     *
     * <p>This or {@link #stepLock} is required: {@link #build()} refuses a builder that names
     * neither. This is the wiring to use wherever a {@code SagaRunner} of the saga type runs in the
     * same process. The guarantee is that process's: a timeout runner on another node shares
     * nothing with this node's deliveries. An application that hand-wires two {@code SagaRunner}s
     * of one saga type has two locks; the claims of a timeout runner built from one of them do not
     * wait for the other's steps (a saga type has one runner — {@link SagaTypeOwnership}).
     */
    public Builder<S> sagaRunner(SagaRunner<?> sagaRunner) {
      this.sagaRunner = Objects.requireNonNull(sagaRunner, "sagaRunner is required");
      return this;
    }

    /**
     * Names the step lock this runner's claims take, for an application that arranges the sharing
     * itself: the same instance goes to {@link SagaRunner.Builder#stepLock}. Prefer {@link
     * #sagaRunner}, which cannot end with two instances. Not both this and {@link #sagaRunner}.
     *
     * <p>A lock of the timeout runner's own, {@code stepLock(new SagaStepLock())}, is the explicit
     * way to say that nothing shares it: for a process that runs timeout runners and no {@code
     * SagaRunner} of the saga type, and for an application that wants the deadline to win over a
     * step in flight. Such a runner claims underneath a delivery that is dispatching, as a timeout
     * runner on another node does, and the delivery's forward fence refuses its next forward
     * command.
     */
    public Builder<S> stepLock(SagaStepLock stepLock) {
      this.stepLock = Objects.requireNonNull(stepLock, "stepLock is required");
      return this;
    }

    /** How long a claim waits for a step in flight once it waits; for tests of the bound. */
    Builder<S> claimWait(Duration claimWait) {
      this.claimWait = Objects.requireNonNull(claimWait, "claimWait is required");
      return this;
    }

    /** Builds the {@link SagaTimeoutRunner}. All required fields must be set. */
    public SagaTimeoutRunner<S> build() {
      Objects.requireNonNull(decider, "decider is required");
      Objects.requireNonNull(sagaStore, "sagaStore is required");
      Objects.requireNonNull(commandBus, "commandBus is required");
      Objects.requireNonNull(sagaType, "sagaType is required");
      Objects.requireNonNull(clock, "clock is required");
      // Timeout compensation dispatches through the keyed execute(command, key) with no unkeyed
      // fallback, so a bus without a CommandInbox fails every compensation. Detectable here — fail
      // fast rather than silently at runtime.
      if (!commandBus.supportsIdempotentExecution()) {
        throw new IllegalStateException(
            "saga command dispatch requires idempotent execution — configure a CommandInbox on the"
                + " CommandBus (VirtualThreadCommandBus.builder().commandInbox(...))");
      }
      if (batchSize < 1) {
        throw new IllegalArgumentException("batchSize must be positive, was " + batchSize);
      }
      if (maxCompensatingDwell != null
          && (maxCompensatingDwell.isZero() || maxCompensatingDwell.isNegative())) {
        throw new IllegalArgumentException("maxCompensatingDwell must be positive when set");
      }
      if (pollInterval == null || pollInterval.isZero() || pollInterval.isNegative()) {
        throw new IllegalArgumentException("pollInterval must be positive, was " + pollInterval);
      }
      // A runner measures a saga's age against decider.timeout(); with none, every poll could only
      // fail. Fail at wiring time, naming what recovers a saga that has no deadline.
      if (decider.timeout().isEmpty()) {
        throw new IllegalArgumentException(
            decider.getClass().getName()
                + " declares no timeout() — a SagaTimeoutRunner has no deadline to compare a saga"
                + " against. Do not build one for it: a saga without a timeout is recovered by the"
                + " SagaCompensationRetrySweeper, or give the decider a timeout().");
      }
      // Fail fast on a sagaType that cannot match any saga row this framework creates. SagaRunner
      // (and the executor's CAS writes) persist rows with SagaType.fromClass(stateType), so a
      // mismatched injected sagaType makes findTimedOut() match nothing: the timeout runner would
      // be silently inert — forward timeouts never fire and nothing alerts. Surface the
      // misconfiguration at build time instead.
      SagaType derived = SagaType.fromClass(decider.stateType());
      if (!sagaType.equals(derived)) {
        throw new IllegalArgumentException(
            "sagaType '"
                + sagaType.value()
                + "' does not match SagaType.fromClass("
                + decider.stateType().getName()
                + ".class) = '"
                + derived.value()
                + "' — saga rows are created with the derived type, so this timeout runner could"
                + " never find a saga to time out (silently inert). Pass"
                + " SagaType.fromClass(the decider's state type).");
      }
      if (sagaRunner != null && stepLock != null) {
        throw new IllegalArgumentException(
            "sagaRunner and stepLock are both set — name one: sagaRunner(...) takes that runner's"
                + " step lock, stepLock(...) is for an application that shares one instance"
                + " itself");
      }
      if (sagaRunner != null) {
        // The lock is shared with the runner whose steps this runner's claims race with: the
        // same saga type over the same rows. Any other runner's steps are other sagas' steps.
        if (!sagaRunner.sagaType().equals(sagaType)
            || !SagaTypeOwnership.keepTheSameRows(sagaRunner.sagaStore(), sagaStore)) {
          throw new IllegalArgumentException(
              "sagaRunner drives the saga type '"
                  + sagaRunner.sagaType().value()
                  + "' over its own SagaStore, and this timeout runner claims sagas of type '"
                  + sagaType.value()
                  + "' in the store given to this builder — they must be the same saga type and"
                  + " the same store, or the runner's steps are not the ones these claims wait"
                  + " for. Pass the SagaRunner of this saga type, built over this store; when"
                  + " the two stores are different objects that keep the same rows, share one"
                  + " SagaStepLock through stepLock(...) on both builders instead.");
        }
      }
      if (sagaRunner == null && stepLock == null) {
        throw new IllegalStateException(
            "The SagaTimeoutRunner for '"
                + sagaType.value()
                + "' names neither sagaRunner(...) nor stepLock(...). Pass sagaRunner(theSagaRunner)"
                + " — the SagaRunner that delivers this saga type's events in this process — so that"
                + " a claim at a saga's deadline waits for a delivery that is dispatching that"
                + " saga's forward commands. A timeout runner bound to nothing claims underneath"
                + " such a delivery: the compensation is computed from a state that does not"
                + " include that step, and the step's next forward command is refused at its"
                + " commit when the SagaRunner fences its forward commands, or stored after its"
                + " compensation with nothing to undo it when it does not. A process that runs no"
                + " SagaRunner of"
                + " this saga type, or an application that wants the deadline to win over a step"
                + " in flight, says so with stepLock(new SagaStepLock()).");
      }
      SagaStepLock claimLock = sagaRunner != null ? sagaRunner.stepLock() : stepLock;
      return new SagaTimeoutRunner<>(
          decider,
          sagaStore,
          commandBus,
          sagaType,
          pollInterval,
          batchSize,
          clock,
          metrics,
          maxCompensatingDwell,
          inboxRetentionMaxAge,
          claimLock,
          claimWait);
    }
  }
}

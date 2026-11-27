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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
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
 * does not merely defer to a claimed/{@code COMPENSATING} saga — see {@link SagaStepExecutor});
 * either way the runner skips the terminal write silently (DEBUG log), since the per-episode keys
 * mean the winning party's dispatches are the ones that count and ours were harmlessly deduped. A
 * throwing {@code compensate()} on the timeout path has no triggering event to quarantine; {@link
 * SagaStepExecutor} marks the saga {@code FAULTED} (halted — excluded from future polls) using the
 * post-claim episode version and WARN-logs the cause before the CAS-FAULT write.
 *
 * <p><b>It follows the runner's lease.</b> A runner built with {@link Builder#sagaRunner} from a
 * {@link SagaRunner} that carries a lease ({@link SagaRunner.Builder#leadership}) polls only while
 * that lease is held through a subscription of this process that anchors it ({@link
 * SubscriptionLease#isLeadingHere()}): one replica claims a saga type's deadlines — the one that
 * delivers its events, where its steps run under the step lock. A poll that does not lead reads and
 * writes nothing and forgets the claims it was deferring. The runner never acquires or resigns the
 * lease; {@link #start()} and {@link #close()} leave it alone. In a process where no subscription
 * ever anchored the lease the runner claims nothing: it logs one INFO within the anchor grace after
 * its start and one WARN past it, and {@link #leaseUnanchored()} turns its health {@code DEGRADED}.
 * A runner built with {@link Builder#stepLock}, or from a runner without a lease, polls wherever it
 * runs.
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
 * that arrives later does not go ahead of it. The lock is released as soon as the claim is written,
 * before any compensation command. This holds within one process: a timeout runner on another node
 * shares nothing with this node's deliveries, and there the forward fence keeps a forward command
 * from being stored after the claim's compensation. {@link Builder#build()} refuses a builder that
 * names neither {@link Builder#sagaRunner} nor {@link Builder#stepLock}.
 *
 * <p><b>A step still in flight after the wait.</b> What a poll does when its bounded wait passes
 * with the step still in flight depends on the runner it was built from:
 *
 * <ul>
 *   <li>built with {@link Builder#sagaRunner} from a runner whose forward fence is on (read at that
 *       moment): the claim is made underneath the step, logged at INFO. Each forward command of the
 *       step committed before the claim, and the compensation covers it, or reaches its commit
 *       after the claim and is refused there; the step then resumes the episode. A step that does
 *       not return delays the saga's deadline by the polls that passed it over and one bounded
 *       wait, and no more;
 *   <li>built from a runner with the fence off, or with {@link Builder#stepLock}: the claim is
 *       never made underneath a step, however long the step lasts. The poll says so at WARN, counts
 *       {@link StreamRuneMetrics#recordSagaTimeoutClaimDeferred}, and the runner reports {@link
 *       #claimsDeferredPastTheWait()} to the health check.
 * </ul>
 *
 * <p><b>A saga whose claim is deferred takes no slot of the batch.</b> It stays a forward row past
 * its deadline and is selected again on every poll, so counted against {@code batchSize} it would
 * keep later sagas out of the batch for as long as its step lasts. A poll therefore asks the store
 * for two more ids for each saga it passes over — one for each of the store's two populations — and
 * handles everything it reads, so both populations keep the share {@code batchSize} gives them. It
 * reads past at most {@code batchSize} such sagas: no query asks for more than three times {@code
 * batchSize} ids, and a batch repeats its query only while the previous one passed over sagas it
 * had not counted. The sagas a poll has to wait for are tried after all the others, as one page
 * after the cycle's last batch, so their waits overlap and delay no other saga's claim.
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
 * <p><b>Sagas are claimed in parallel, on the runner's slots.</b> The sagas a query returns run as
 * one page, keyed by saga id: each on a virtual worker thread named {@code saga-timeout-<saga
 * type>-claim-<n>}, which carries the poll thread's request context and trace context, so the span
 * {@code saga/{SagaType}/timeout} of each saga is started on its worker. Each worker takes one of
 * the slots of the {@link SagaRunner} the builder named before it acts on its saga, and keeps it
 * until the saga's compensation has been dispatched and its terminal status written: the runner's
 * steps and these claims share the slots, so together they never exceed the runner's budgeted steps
 * — its {@linkplain SagaRunner#parallelism() parallelism} per bound subscription, read at each
 * acquire — and the connection budget the integrations compute for the runner's steps also holds
 * the claims. A slot is always taken before the saga's step lock, never while one is held; a claim
 * that waits its bound for a step in flight holds its slot, and the step it waits for holds a slot
 * of its own. One slow compensation therefore delays no other saga's claim while a slot is free.
 * The claim of a saga the poll has to wait for waits for its slot and its step lock within one
 * claim wait: when no slot frees within it — every slot held, as by steps that do not return — it
 * goes on without a slot (an INFO line), so a step that does not return delays its saga's deadline
 * by the polls that passed it over and one claim wait, and no more. The budgeted steps can thus be
 * exceeded only while steps hang, by at most the number of sagas claimed past a hung step at once.
 * Every other claim waits for a slot for at most the claim wait as well: while every slot is held
 * by steps, it is postponed to the next poll, with nothing read or written, and the poll goes on to
 * the next saga. A claim whose saga has a step in flight, and that would only ask for the step lock
 * once, is passed over before it takes a slot. A runner built with {@link Builder#stepLock} has no
 * runner to share slots with and acts on one saga at a time, on the poll thread; so does every
 * runner whose limit is 1. Two workers never act on one saga in a poll: a page's keys are distinct,
 * and a saga read once in a cycle is not acted on again in it.
 *
 * <p><b>A cycle drains while batches come back full.</b> After a batch whose last query returned as
 * many ids as it asked for and that acted on at least one saga it had not seen in the cycle, the
 * runner queries again at once instead of waiting {@code pollInterval}. A partial or empty batch
 * ends the cycle, and so does a batch whose sagas were all deferred, failed or seen already, a
 * {@link #close()} or an interrupt, and a poll that has lost the lease. Each batch takes its cutoff
 * from the clock again, since deadlines keep passing while a backlog drains. One cycle drains for
 * at most {@value #DRAIN_BUDGET_POLL_INTERVALS} poll intervals plus the batch in progress, measured
 * on {@link System#nanoTime()}; the first cycle that reaches the budget and every tenth one after
 * it log an INFO line. The timed-out backlog gauge is sampled once per cycle, at its start, so
 * while a backlog drains it can be about 11 poll intervals old.
 *
 * <p>Per-saga processing errors are caught and logged (not rethrown), so a single failing saga does
 * not abort the rest of the batch.
 *
 * @param <S> the saga state type
 */
public final class SagaTimeoutRunner<S extends SagaState> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(SagaTimeoutRunner.class);

  /**
   * Upper bound {@link #close()} waits to join the poll thread before proceeding. The interrupted
   * thread exits promptly in practice; this only caps a pathologically stuck poll.
   */
  private static final long STOP_JOIN_TIMEOUT_MS = 30_000L;

  /**
   * The join bound {@link #close()} uses; {@link #STOP_JOIN_TIMEOUT_MS} unless a test shrinks it.
   */
  private volatile long stopJoinTimeoutMs = STOP_JOIN_TIMEOUT_MS;

  /** Longest wait between retries of a failing poll, unless the poll interval itself is longer. */
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /**
   * How many polls in a row may pass a saga over because a step of it is in flight before the claim
   * waits for that step instead of asking once.
   */
  static final int DEFERRALS_BEFORE_WAITING = 3;

  /** How long a claim waits for a step in flight once it waits at all. */
  static final Duration DEFAULT_CLAIM_WAIT = Duration.ofSeconds(5);

  /**
   * How many poll intervals one poll cycle may keep draining full batches before it yields to the
   * schedule (see {@link #pollCycle(Instant)}). After a cycle the runner waits one {@code
   * pollInterval}, so under a sustained backlog it spends at least 10 of every 11 intervals
   * claiming, whatever the interval, and the backlog gauge is sampled at least every 11 intervals
   * plus one batch. The budget is checked between batches, never inside one, so a cycle can overrun
   * it by the batch in progress. Measured on {@link System#nanoTime()}, whatever clock the builder
   * was given, so a fixed or stepping test clock cannot stretch it.
   */
  static final int DRAIN_BUDGET_POLL_INTERVALS = 10;

  /**
   * How often, in consecutive poll cycles that reach their drain budget, the runner logs the
   * saturation at INFO: on the first such cycle and on every tenth after it; DEBUG otherwise.
   */
  private static final int BUDGET_YIELD_LOG_EVERY = 10;

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

  /**
   * The {@link SagaRunner} the builder named ({@link Builder#sagaRunner}), or {@code null} when it
   * named a lock ({@link Builder#stepLock}). A claim that waited its bound for a step of this
   * runner still in flight reads the runner's forward fence at that moment: on, the claim is made
   * underneath the step; off, it is not.
   */
  private final SagaRunner<?> boundRunner;

  private final Duration claimWait;

  /**
   * The slots of the {@link SagaRunner} the builder named, which this runner's claims share with
   * that runner's steps ({@link SagaSlots}); {@code null} for a runner built with {@link
   * Builder#stepLock}, which acts on one saga at a time.
   */
  private final SagaSlots slots;

  /** Runs the sagas of a query as one page, keyed by saga id. */
  private final KeyedPageExecutor claimPages;

  /** The threads acting on a saga at this moment; {@link #close()} interrupts them. */
  private final Set<Thread> claimThreads = ConcurrentHashMap.newKeySet();

  /** {@link #DRAIN_BUDGET_POLL_INTERVALS} poll intervals in nanoseconds, saturated. */
  private final long drainBudgetNanos;

  /** The monotonic time the drain budget is measured on: System::nanoTime, or a test's ticker. */
  private final LongSupplier drainTicker;

  /** Consecutive poll cycles that ended on the drain budget; reset by one that ended otherwise. */
  private final AtomicInteger budgetYieldStreak = new AtomicInteger();

  /**
   * The lease this runner follows: the {@link SagaRunner}'s when the builder named the runner,
   * otherwise none ({@link SubscriptionLease#ungated()}). The runner claims only while a
   * subscription of this process anchors the lease and leads; it never acquires or resigns it.
   */
  private final SubscriptionLease lease;

  /** Whether this runner may act on a poll, and its lines for a lease no subscription anchored. */
  private final LeaseFollower follower;

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

  /** Serializes {@link #close()}. */
  private final ReentrantLock closeLock = new ReentrantLock();

  private final StreamRuneTracing tracing;

  /** Whether {@link #tracing} records the saga area, read once. */
  private final boolean traced;

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
      SagaRunner<?> boundRunner,
      Duration claimWait,
      Duration anchorGrace,
      StreamRuneTracing tracing,
      LongSupplier drainTicker) {
    this.tracing = GuardedTracing.of(tracing, metrics);
    this.traced = this.tracing.enabled(TracingArea.SAGA);
    this.stepLock = stepLock;
    this.boundRunner = boundRunner;
    this.claimWait = claimWait;
    this.slots = boundRunner != null ? boundRunner.slots() : null;
    this.claimPages =
        new KeyedPageExecutor("saga-timeout-" + sagaType.value() + "-claim-", this.tracing);
    this.drainTicker = drainTicker;
    this.drainBudgetNanos = drainBudgetNanos(pollInterval);
    this.lease = boundRunner != null ? boundRunner.lease() : SubscriptionLease.ungated();
    this.follower =
        new LeaseFollower(
            lease,
            LOG,
            "timeout runner",
            sagaType.value(),
            "deadlines are claimed",
            anchorGrace,
            System::nanoTime);
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
            SagaDeciderAdapter.compensationOnly(decider),
            sagaStore,
            commandBus,
            metrics,
            this.tracing);
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
   * {@link #close()}. A second call without an intervening close would spawn a concurrent loop
   * polling the same saga store.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          "SagaTimeoutRunner already started — call start() exactly once, or close() first");
    }
    running = true;
    follower.started();
    pollThread =
        Thread.ofVirtual()
            .name("saga-timeout-" + sagaType.value())
            .start(() -> pollLoop.run(this::pollOnce));
  }

  /**
   * Stops polling. Interrupts the threads acting on a saga and the poll thread, which starts no
   * further saga, query or batch and waits for those threads, and resets the started guard so
   * {@link #start()} can be called again on the same instance — matches the restartable lifecycle
   * contract (actuator restart, context refresh) of the other background components. Idempotent:
   * closing a runner that is not started does nothing. A DI container's destroy callback, a
   * try-with-resources block or a lifecycle that closes its {@link AutoCloseable} components stops
   * it like them.
   *
   * <p>A poll thread that does not stop within the join bound keeps the runner started and alive,
   * so {@link #start()} is refused; a later {@code close()} interrupts and joins that thread again
   * and resets the guard once it has stopped.
   *
   * <p>A started runner that follows a lease this process holds logs a WARN: while its subscription
   * keeps the lease, no replica claims the deadlines of the saga type.
   */
  @Override
  public void close() {
    closeLock.lock();
    try {
      closeSerially();
    } finally {
      closeLock.unlock();
    }
  }

  /**
   * The body of {@link #close()}, run by one caller at a time: a close() that starts while another
   * waits on a stuck poll thread waits for that verdict, so it cannot find no thread and reset the
   * started guard underneath it.
   */
  private void closeSerially() {
    if (started.get() && lease.isGated() && lease.isLeadingHere()) {
      LOG.warn(
          "The timeout runner of saga type {} stops while this process leads that saga type: no"
              + " replica claims its deadlines until the runner is started again or this process"
              + " stops leading the saga type (its subscription paused or closed)",
          sagaType.value());
    }
    running = false;
    // The workers acting on a saga first, then the poll thread that waits for them: a worker that
    // waits for a slot, for a step in flight or inside a compensation command is told to stop.
    for (Thread worker : claimThreads) {
      worker.interrupt();
    }
    Thread t = pollThread;
    boolean stuck = false;
    if (t != null) {
      t.interrupt();
      // Join the poll thread before resetting `started`, so a stale poll thread cannot
      // still be running when a subsequent start() spawns a new one (a leaked duplicate poller).
      // Self-join guard: a thread cannot join itself (close() invoked from within the poll thread).
      if (t != Thread.currentThread()) {
        try {
          t.join(stopJoinTimeoutMs);
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
      // The stuck thread stays the poll thread: isAlive() keeps reporting it, and a later close()
      // interrupts and joins it again instead of finding no thread and resetting the guard while
      // it still runs.
      LOG.warn(
          "SagaTimeoutRunner poll thread did not stop within {}ms; leaving the runner"
              + " not-restartable so a restart cannot spawn a duplicate poller. Investigate the"
              + " stuck poll.",
          stopJoinTimeoutMs);
    } else {
      // Reset so start() can be called again after close().
      pollThread = null;
      started.set(false);
    }
  }

  /** Shrinks the join bound of {@link #close()}; for the tests of this package. */
  void stopJoinTimeoutMs(long millis) {
    this.stopJoinTimeoutMs = millis;
  }

  /**
   * Whether {@link #start()} has been called and {@link #close()} has not. Combined with {@link
   * #isAlive()} this distinguishes a runner that was never started (or was cleanly stopped) from
   * one whose poll thread <em>died</em> — {@code isStarted() && !isAlive()} is the dead-driver
   * signal the saga health check reports DOWN. Mirrors the outbox/DLQ relay liveness surface.
   */
  public boolean isStarted() {
    return started.get();
  }

  /**
   * Whether the runner's poll thread is currently alive. A started runner whose thread is not alive
   * died unexpectedly (e.g. an {@link Error} escaped the poll loop) — see {@link #isStarted()}.
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
   * claimed every saga it waited for, or waited for none — always, for a runner built from a {@link
   * SagaRunner} whose forward fence is on: once the wait has passed its poll claims underneath the
   * step. A nonzero value is a deadline that is not being enforced in this process: a forward
   * command of that saga has not returned (or its attempts follow each other without a gap), the
   * claim is not made underneath it, and it is made by the first poll that finds the step ended — a
   * restart of the process ends such a step and frees its lock. The health check reports the runner
   * {@code DEGRADED} while this is nonzero.
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
   * Runs one poll cycle on the calling thread — what the poll thread of a started runner runs every
   * poll interval: batch after batch of timed-out sagas while each comes back full and made
   * progress ({@link #pollCycle(Instant)}). Each batch computes its timeout cutoff from the
   * injected {@link Clock}: the cutoff is {@code now - timeout}; a {@code STARTED}/{@code RUNNING}
   * saga whose <em>start instant</em> is before the cutoff has exceeded its (absolute) timeout,
   * while a {@code COMPENSATING} crash-resume episode is re-picked on last-write inactivity — see
   * {@link org.streamrune.core.saga.SagaStore#findTimedOut}. A failure propagates to the caller.
   *
   * <p>A runner built with {@link Builder#sagaRunner} follows that runner's lease: while this
   * process does not lead it — a standby, or a saga subscription that has not taken the lease yet —
   * the call reads nothing and writes nothing, as the poll thread's poll does.
   *
   * <p>For a test that drives the runner on a clock of its own and asserts what one poll claims
   * ({@code SagaFixture.runTimeouts()} in {@code streamrune-test} calls it). A started runner polls
   * on its own thread; a call made beside it is one more poll.
   */
  public void pollOnce() {
    if (!follower.mayAct()) {
      // Not leading here: read nothing, write nothing. A new leadership period counts deferrals
      // from zero.
      deferredClaims.clear();
      claimsDeferredPastTheWait = 0;
      return;
    }
    Duration timeout =
        decider.timeout().orElseThrow(() -> new IllegalStateException("No timeout configured"));
    pollCycle(() -> clock.instant().minus(timeout));
  }

  /**
   * Whether this runner follows a saga runner's lease that no subscription of this process has
   * anchored since its start, past the anchor grace: it claims no deadline here, and the health
   * check reports it {@code DEGRADED}.
   */
  public boolean leaseUnanchored() {
    return follower.unanchoredPastGrace();
  }

  /**
   * Runs one poll cycle with {@code cutoff} for every batch, ignoring the lease. Package-private
   * for testing.
   */
  void pollCycle(Instant cutoff) {
    Objects.requireNonNull(cutoff, "cutoff is required");
    pollCycle(() -> cutoff);
  }

  /**
   * One poll cycle: batch after batch ({@link #processBatch}) while the last one came back full —
   * its last query returned as many ids as it asked for — and made progress: it acted on at least
   * one saga it had not seen earlier in the cycle, neither deferred nor failed. A partial or empty
   * batch ends the cycle, and so does a batch whose sagas were all deferred, failed or seen
   * already, a {@link #close()} or an interrupt, a poll that has lost the lease, and the
   * {@linkplain #DRAIN_BUDGET_POLL_INTERVALS drain budget}, checked between batches.
   *
   * <p>Each batch asks the clock for its cutoff, because deadlines keep passing while a backlog
   * drains: a saga whose deadline passes during the cycle is claimed in the cycle, as the next poll
   * would claim it. The sagas the cycle has read stay seen for the rest of it: a saga deferred,
   * failed or resumed in one batch is read past in the next instead of being tried twice in one
   * cycle. The sagas the cycle has to wait for are tried last, after its last batch, as one page.
   */
  private void pollCycle(Supplier<Instant> cutoffs) {
    long cycleStart = drainTicker.getAsLong();
    Set<SagaId> seen = new HashSet<>();
    List<SagaId> toWaitFor = new ArrayList<>();
    // The sagas the previous poll passed over are read past from the first query on.
    int readPast = Math.min(deferredClaims.size(), batchSize);
    int batches = 1;
    boolean budgetReached = false;
    boolean goesOn = true;
    try {
      while (true) {
        Batch batch = processBatch(cutoffs.get(), seen, toWaitFor, readPast, batches == 1);
        if (!batch.full() || batch.acted() == 0) {
          break;
        }
        if (stopRequested() || !follower.mayAct()) {
          goesOn = false;
          break;
        }
        if (drainTicker.getAsLong() - cycleStart >= drainBudgetNanos) {
          budgetReached = true;
          reportDrainBudgetReached(batches);
          break;
        }
        readPast = Math.min(batch.passedOver(), batchSize);
        batches++;
      }
    } finally {
      if (!budgetReached) {
        budgetYieldStreak.set(0);
      }
    }
    // A saga this cycle did not read is not a forward saga past its deadline within the poll's
    // reach: forget how often its claim was passed over.
    deferredClaims.keySet().retainAll(seen);
    if (!goesOn || toWaitFor.isEmpty() || stopRequested()) {
      claimsDeferredPastTheWait = 0;
      return;
    }
    // The sagas to wait for run as one page: their bounded waits overlap instead of adding up.
    claimsDeferredPastTheWait = claimPage(toWaitFor).deferred();
  }

  /**
   * Logs a cycle that drained full batches until its budget: at INFO on the first of a streak of
   * such cycles and on every {@value #BUDGET_YIELD_LOG_EVERY}th, so a runner that cannot keep up
   * with the deadlines passing is visible in the logs as well as in {@code
   * streamrune.saga.timed_out_backlog}.
   */
  private void reportDrainBudgetReached(int batches) {
    int streak = budgetYieldStreak.incrementAndGet();
    if (streak == 1 || streak % BUDGET_YIELD_LOG_EVERY == 0) {
      LOG.info(
          "The timeout runner of saga type {} is draining a backlog at its full rate: {}"
              + " consecutive poll cycle(s) reached the drain budget of {} poll intervals ({} full"
              + " batches this cycle); the timed-out backlog gauge is sampled once per cycle",
          sagaType.value(),
          streak,
          DRAIN_BUDGET_POLL_INTERVALS,
          batches);
    } else {
      LOG.debug(
          "The timeout runner of saga type {} drained {} full batches and reached its drain budget"
              + " ({} consecutive cycles); yielding to the schedule",
          sagaType.value(),
          batches,
          streak);
    }
  }

  /**
   * What one batch came to.
   *
   * @param full whether the batch's last query returned as many ids as it asked for
   * @param acted the sagas of the batch the runner acted on: not seen earlier in the cycle, and
   *     neither deferred nor failed
   * @param passedOver the sagas the batch read and did not count against {@code batchSize}: those
   *     whose claim was deferred, those to wait for, and those seen earlier in the cycle
   */
  private record Batch(boolean full, int acted, int passedOver) {}

  /**
   * Processes one batch of timed-out sagas.
   *
   * <p>A saga whose claim is deferred ({@link #claimAtTheDeadline}) takes no slot of the batch, and
   * nor does a saga the cycle has seen in an earlier batch. The batch asks {@link
   * SagaStore#findTimedOut} for {@code batchSize} ids and two more for each saga it expects to read
   * past — for the first batch of a cycle the sagas the previous poll passed over, for a later one
   * the sagas the batch before it passed over. Such a saga is a forward row (or a re-pick a resume
   * left as it was), and the store gives each population half of what is asked for when both are
   * backlogged, so two more ids per saga give its population the slot that saga holds and leave
   * both populations the share {@code batchSize} gives them. Every id read is handled. When a query
   * comes back full and the batch has passed over more sagas than it counted, it asks again with
   * the larger number and handles the ids it has not seen; each further query needs sagas newly
   * passed over, and the number counted stops at {@code batchSize}, so no query asks for more than
   * three times {@code batchSize} ids.
   *
   * <p>The new sagas of each query run as one page ({@link #claimPage}): in parallel, up to the
   * {@link SagaRunner}'s budgeted steps, each holding one of its slots. The read-past accounting is
   * made once the page has ended.
   *
   * <p>A saga the poll has to wait for ({@value #DEFERRALS_BEFORE_WAITING} polls in a row passed it
   * over) is counted as passed over when it is read and tried after every other saga of the cycle,
   * so its wait delays no other claim. Nothing here starves it: it stays selected, by its start
   * instant, and is tried on every poll.
   *
   * @param seen the sagas read earlier in the cycle; this batch adds the ones it reads
   * @param toWaitFor the sagas to wait for; this batch adds the ones it finds
   * @param readPast how many sagas the first query expects to read past
   * @param firstOfCycle whether the batch samples the timed-out backlog gauge
   */
  private Batch processBatch(
      Instant cutoff,
      Set<SagaId> seen,
      List<SagaId> toWaitFor,
      int readPast,
      boolean firstOfCycle) {
    Set<SagaId> readThisBatch = new HashSet<>();
    int passedOver = 0;
    int acted = 0;
    boolean sample = firstOfCycle;
    while (true) {
      int limit = batchSize + 2 * readPast;
      List<SagaId> timedOut = sagaStore.findTimedOut(sagaType, cutoff, limit);
      if (sample) {
        sample = false;
        // Sample the timed-out backlog gauge once per cycle so a stalled-but-alive runner (or a
        // growing timeout backlog) is visible on the metrics endpoint, not only in logs. This is
        // the number of sagas the cycle's first query picked up — forward timeouts plus
        // COMPENSATING re-picks. Decoupled from processing: a metric failure never aborts the
        // batch.
        if (metrics != StreamRuneMetrics.NOOP) {
          recordMetric(
              () -> metrics.recordSagaTimedOutBacklog(sagaType.value(), timedOut.size()),
              "recordSagaTimedOutBacklog");
        }
      }
      List<SagaId> toClaim = new ArrayList<>();
      for (SagaId sagaId : timedOut) {
        if (!readThisBatch.add(sagaId)) {
          continue; // read by an earlier query of this batch, and counted there
        }
        if (!seen.add(sagaId)) {
          passedOver++; // read in an earlier batch of the cycle
        } else if (deferredClaims.getOrDefault(sagaId, 0) >= DEFERRALS_BEFORE_WAITING) {
          toWaitFor.add(sagaId);
          passedOver++;
        } else {
          toClaim.add(sagaId);
        }
      }
      PageCounts page = claimPage(toClaim);
      passedOver += page.deferred();
      acted += page.acted();
      if (timedOut.size() < limit || passedOver <= readPast || readPast == batchSize) {
        // nothing more is selected, or every saga passed over was read past, or the bound
        return new Batch(timedOut.size() >= limit, acted, passedOver);
      }
      if (stopRequested()) {
        return new Batch(false, acted, passedOver);
      }
      readPast = Math.min(passedOver, batchSize);
    }
  }

  /** What a page of sagas came to: how many the runner acted on and how many it deferred. */
  private record PageCounts(int acted, int deferred) {
    static final PageCounts NONE = new PageCounts(0, 0);
  }

  /**
   * Runs {@code sagaIds}, which are distinct, as one page: in parallel up to the bound runner's
   * budgeted steps, each saga on a worker that holds one of the runner's slots while it acts on the
   * saga ({@link #actInASlot}); serially on the calling thread for a runner built with {@link
   * Builder#stepLock}, or when the limit is 1. A saga that fails is logged and does not stop the
   * others; an {@link Error} ends the page and is thrown. A page cut short by an interrupt of the
   * calling thread returns what ran and leaves the thread's interrupt status set.
   */
  private PageCounts claimPage(List<SagaId> sagaIds) {
    if (sagaIds.isEmpty()) {
      return PageCounts.NONE;
    }
    Map<SagaId, ClaimOutcome> outcomes = new ConcurrentHashMap<>();
    int parallelism = slots == null ? 1 : slots.currentLimit();
    PageResult result =
        claimPages.run(
            sagaIds,
            Function.identity(),
            parallelism,
            sagaId -> outcomes.put(sagaId, actInASlot(sagaId)),
            NEVER_STOPS,
            PageObserver.NONE);
    if (result.failure() instanceof Error error) {
      throw error;
    }
    if (result.failure() != null && !result.interrupted()) {
      result.rethrow(); // a worker that ended without running its sagas: the poll fails
    }
    int acted = 0;
    int deferred = 0;
    for (ClaimOutcome outcome : outcomes.values()) {
      switch (outcome) {
        case ACTED -> acted++;
        case DEFERRED -> deferred++;
        case FAILED, POSTPONED, NOT_RUN -> {
          // neither progress nor a saga passed over
        }
      }
    }
    return new PageCounts(acted, deferred);
  }

  /** A page never stops on the failure of a saga: each saga's failure is its own. */
  private static final PageStopRule NEVER_STOPS = (failure, failuresInPage) -> false;

  /** What acting on one saga came to. */
  enum ClaimOutcome {
    /** The runner claimed, resumed or skipped the saga (its row was halted or gone). */
    ACTED,
    /** The claim was deferred: a step of the saga is in flight in this process. */
    DEFERRED,
    /** Processing the saga failed; logged. */
    FAILED,
    /** No slot freed within the claim wait: nothing was read or written; the next poll tries. */
    POSTPONED,
    /** The runner stopped before it acted on the saga. */
    NOT_RUN
  }

  /**
   * Acts on one saga while holding one of the bound runner's slots: the slot first, then, inside,
   * the saga's step lock — never the other way round, so a claim never waits for a slot while it
   * holds a step lock. A runner built with {@link Builder#stepLock} has no slots and acts at once.
   * The thread is registered for {@link #close()} to interrupt while it runs.
   *
   * <p>A saga the poll has to wait for ({@value #DEFERRALS_BEFORE_WAITING} polls in a row passed it
   * over) has one claim wait for its slot and its step lock together: it waits for a slot for at
   * most the claim wait, and for the step lock for what is left of it. When no slot frees within
   * the claim wait — every slot held, as by steps that do not return — the claim goes on without a
   * slot, logged at INFO, so a step that does not return still delays the saga's deadline by the
   * polls that passed it over and one claim wait, and no more. Such a claim exceeds the runner's
   * budgeted steps by one while it runs. Every other claim waits for its slot in arrival order for
   * at most the claim wait too; when none frees it is postponed to the next poll, with nothing read
   * and nothing written, and logged at DEBUG — never made without a slot. A claim whose saga has a
   * step in flight at that moment, and that would ask for the step lock once, is passed over at
   * once, without a slot, and nothing is read for it.
   */
  private ClaimOutcome actInASlot(SagaId sagaId) {
    Thread self = Thread.currentThread();
    claimThreads.add(self);
    try {
      // After the registration: a close() that set the stop before it interrupted the registered
      // threads is seen here, and one that set it later interrupts this thread.
      if (stopRequested()) {
        return ClaimOutcome.NOT_RUN;
      }
      long claimDeadline = System.nanoTime() + claimWait.toNanos();
      if (slots == null) {
        return timeOutOne(sagaId, claimDeadline);
      }
      boolean waitsForTheStep = deferredClaims.getOrDefault(sagaId, 0) >= DEFERRALS_BEFORE_WAITING;
      if (!waitsForTheStep && stepLock.heldNow(sagaType, sagaId)) {
        // A step of the saga holds its lock: the claim, which would ask for the lock once, is
        // passed over now, before it takes a slot that a step could use — and without waiting for
        // a slot that the step in flight may hold itself.
        claimDeferred(sagaId, Duration.ZERO);
        return ClaimOutcome.DEFERRED;
      }
      SagaSlots.Slot slot;
      try {
        slot = slots.tryAcquire(claimWait);
      } catch (InterruptedException interrupted) {
        self.interrupt();
        return ClaimOutcome.NOT_RUN;
      }
      if (slot == null && !waitsForTheStep) {
        // Every slot stayed held for the whole claim wait — steps that hold them, maybe steps that
        // do not return. The claim is not made without a slot: the next poll tries again.
        LOG.debug(
            "No slot of the runner freed within the claim wait of {} for the timeout claim of saga"
                + " {}; it is tried again on the next poll",
            claimWait,
            LogSanitizer.sanitizeForLog(sagaId.value()));
        return ClaimOutcome.POSTPONED;
      }
      if (slot == null) {
        int passedOver = deferredClaims.getOrDefault(sagaId, 0);
        LOG.info(
            "Saga {} is past its deadline and a step of it has been in flight in this process for"
                + " {} polls; no slot of its runner freed within the claim wait of {}, so its claim"
                + " goes on without a slot and exceeds the runner's budgeted steps by one while it"
                + " runs",
            LogSanitizer.sanitizeForLog(sagaId.value()),
            passedOver,
            claimWait);
        return timeOutOne(sagaId, claimDeadline);
      }
      try (slot) {
        return timeOutOne(sagaId, claimDeadline);
      }
    } finally {
      claimThreads.remove(self);
    }
  }

  /**
   * Whether a {@link #close()} has asked the runner to stop, or the calling thread is interrupted.
   * A runner that was never started is not stopping: {@link #pollOnce()} driven by a test runs its
   * whole cycle.
   */
  private boolean stopRequested() {
    return (started.get() && !running) || Thread.currentThread().isInterrupted();
  }

  /**
   * Loads and processes a single timed-out saga; {@code true} when its claim was deferred. See
   * {@link #timeOutOne}. Package-private for testing — tests call this directly to exercise the
   * halted-guard branch without relying on store filtering.
   *
   * @return {@code true} when the saga's claim was deferred because a step of it is in flight in
   *     this process: nothing was written, and the saga holds no slot of the batch
   */
  boolean processTimedOutSaga(SagaId sagaId) {
    return timeOutOne(sagaId, System.nanoTime() + claimWait.toNanos()) == ClaimOutcome.DEFERRED;
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
   *       saga is in flight in this process the claim is not made, until a poll's bounded wait has
   *       passed with the forward fence on.
   * </ul>
   *
   * <p>A failure is caught and logged here, so one failing saga never stops the others of its page.
   *
   * @param claimDeadline the {@link System#nanoTime()} at which the claim wait of a saga the poll
   *     has to wait for ends; what is left of it bounds the wait for the step lock
   */
  private ClaimOutcome timeOutOne(SagaId sagaId, long claimDeadline) {
    try {
      // Type-scoped load so a row owned by a different saga type (a cross-type SagaId collision)
      // reads as "no saga for this type" — never deserialized, routed, or compensated from —
      // rather than relying on the downstream CAS to 0-row after dispatch.
      Optional<LoadedSaga<S>> selected = sagaStore.load(sagaId, sagaType, decider.stateType());
      // HALTED GUARD: skip terminal | FAULTED before routing. The executor's ClaimTimeout case
      // guards only isEmpty(), and update() does not terminal-guard FAULTED, so a halted row
      // routed there would be CAS'd back to COMPENSATING and re-compensated.
      if (selected.isEmpty() || selected.get().status().isHalted()) {
        return ClaimOutcome.ACTED;
      }
      return timeOut(sagaId, selected.get(), claimDeadline)
          ? ClaimOutcome.DEFERRED
          : ClaimOutcome.ACTED;
    } catch (Exception e) {
      LOG.warn(
          "Failed to process timed-out saga: {}", LogSanitizer.sanitizeForLog(sagaId.value()), e);
      return ClaimOutcome.FAILED;
    }
  }

  /**
   * Times out {@code sagaId}, which {@code selected} shows to be live: resumes its episode when it
   * is compensating, claims it at its deadline otherwise.
   *
   * @return {@code true} when the claim was deferred
   */
  private boolean timeOut(SagaId sagaId, LoadedSaga<S> selected, long claimDeadline) {
    SagaTimeoutException cause =
        new SagaTimeoutException(sagaId, decider.timeout().orElse(Duration.ZERO));
    if (selected.status() == SagaStatus.COMPENSATING) {
      TraceScope scope = startTimeoutSpan(sagaId);
      try {
        resumeClaimedEpisode(sagaId, selected, cause);
        scope.recordSuccess();
      } catch (Throwable t) {
        scope.recordError(t);
        throw t;
      } finally {
        scope.close();
      }
      return false;
    }
    return claimAtTheDeadline(sagaId, cause, claimDeadline);
  }

  /**
   * Starts the span {@code saga/{SagaType}/timeout} of the saga this runner acts on, when the saga
   * area is traced. It starts only once the runner acts: a claim deferred because a step of the
   * saga is in flight writes nothing and starts no span.
   */
  private TraceScope startTimeoutSpan(SagaId sagaId) {
    if (!traced) {
      return TraceScope.NOOP;
    }
    return tracing.start(
        TracingArea.SAGA,
        TraceAttributes.sagaTimeoutSpanName(sagaType),
        SpanKind.INTERNAL,
        SpanAttributes.builder()
            .put(TraceAttributes.SAGA_TYPE, sagaType.value())
            .put(TraceAttributes.SAGA_ID, LogSanitizer.sanitizeForTrace(sagaId.value()))
            .build());
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
   * the lock free, or that sees the step end within its wait — or, when the runner this one was
   * built from fences its forward commands, by the poll whose wait passed, underneath the step
   * ({@link #claimUnderneathTheStep}).
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
   * <p>The wait for the step is what is left of the claim wait once the claim had its slot: the
   * slot and the step lock share one claim wait ({@link #actInASlot}).
   *
   * @param claimDeadline the {@link System#nanoTime()} at which the claim wait ends
   * @return {@code true} when the claim was deferred
   */
  private boolean claimAtTheDeadline(
      SagaId sagaId, SagaTimeoutException cause, long claimDeadline) {
    int passedOver = deferredClaims.getOrDefault(sagaId, 0);
    boolean waits = passedOver >= DEFERRALS_BEFORE_WAITING;
    Duration wait =
        waits ? Duration.ofNanos(Math.max(0L, claimDeadline - System.nanoTime())) : Duration.ZERO;
    SagaStepLock.Hold claimHold;
    try {
      claimHold = stepLock.tryEnterClaim(sagaType, sagaId, wait);
    } catch (SagaStepLockException interrupted) {
      // close() interrupts the poll thread; its interrupt status is set and the loop ends on it.
      LOG.debug(
          "The timeout claim of saga {} was interrupted while it waited for a step in flight",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      return false;
    }
    if (claimHold == null) {
      if (!waits || boundRunner == null || !boundRunner.forwardFence()) {
        claimDeferred(sagaId, waits ? claimWait : Duration.ZERO);
        return true;
      }
      TraceScope scope = startTimeoutSpan(sagaId);
      try {
        claimUnderneathTheStep(sagaId, passedOver, cause);
        scope.recordSuccess();
      } catch (Throwable t) {
        scope.recordError(t);
        throw t;
      } finally {
        scope.close();
      }
      return false;
    }
    // The claim hold is taken: everything from here to its release runs inside the try whose
    // finally releases it, the start of the span included. A hold that outlived this method would
    // keep the saga's step lock with the poll thread for as long as the runner lives.
    TraceScope scope = TraceScope.NOOP;
    try {
      LoadedSaga<S> claimedByAStep;
      try {
        scope = startTimeoutSpan(sagaId);
        deferredClaims.remove(sagaId);
        Optional<LoadedSaga<S>> current = sagaStore.load(sagaId, sagaType, decider.stateType());
        if (current.isEmpty() || current.get().status().isHalted()) {
          scope.recordSuccess();
          return false;
        }
        if (current.get().status() != SagaStatus.COMPENSATING) {
          executor.execute(sagaId, current, new SagaTrigger.ClaimTimeout(cause, claimHold));
          scope.recordSuccess();
          return false;
        }
        claimedByAStep = current.get();
      } finally {
        claimHold.close(); // every exit on which the executor did not reach its claim
      }
      resumeClaimedEpisode(sagaId, claimedByAStep, cause);
      scope.recordSuccess();
      return false;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * The claim of a saga whose step is still in flight in this process after the poll waited its
   * bound for it, made when the {@link SagaRunner} this runner was built from fences its forward
   * commands. Each forward command of the step either commits before the claim, and the
   * compensation covers it, or reaches its commit after the claim and is refused there; the step
   * then reads the row the claim wrote and resumes the episode (or stops at a saga that has already
   * finished). Nothing waits for the step any more, so a step that does not return delays the
   * saga's deadline by the wait and no more.
   *
   * <p>No step lock is held: the row is read again — a step may have written since the read that
   * routed the saga here — and routed as a re-pick would be: no row, or a halted row, is left
   * alone; a {@code COMPENSATING} row resumes its episode; a forward row is claimed by the
   * executor's compare-and-swap at the version read, which a write in between makes it lose. The
   * INFO line is written only for that claim.
   */
  private void claimUnderneathTheStep(SagaId sagaId, int passedOver, SagaTimeoutException cause) {
    deferredClaims.remove(sagaId);
    Optional<LoadedSaga<S>> current = sagaStore.load(sagaId, sagaType, decider.stateType());
    if (current.isEmpty() || current.get().status().isHalted()) {
      return;
    }
    if (current.get().status() == SagaStatus.COMPENSATING) {
      resumeClaimedEpisode(sagaId, current.get(), cause);
      return;
    }
    LOG.info(
        "Saga {} is past its deadline, and a step of it has been in flight in this process for {}"
            + " polls and the bounded wait of {}; the claim is made underneath it: the forward"
            + " fence refuses the step's later forward commands, and the step resumes the"
            + " episode.",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        passedOver,
        claimWait);
    executor.execute(sagaId, current, new SagaTrigger.ClaimTimeout(cause));
  }

  /**
   * A poll passed the saga over: a step of it is in flight in this process, so its claim is not
   * made. Every such poll counts one {@link StreamRuneMetrics#recordSagaTimeoutClaimDeferred}
   * sample. The first polls are expected — a delivery at the deadline, an outage ending in a
   * redelivery — and logged at DEBUG. A poll that waited for the step and still found one in flight
   * — with the forward fence off, or for a runner built with a step lock alone — is logged at WARN
   * on every occurrence: until a poll finds the saga free, its deadline is not enforced in this
   * process. The poll cannot tell one forward command that does not return from attempts that
   * follow each other without a gap long enough for the claim, and says so.
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
      LOG.warn("Metrics recording failed for {}", metricName, e);
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
    private Duration maxCompensatingDwell;
    private Duration inboxRetentionMaxAge;
    private SagaRunner<?> sagaRunner;
    private SagaStepLock stepLock;
    private StreamRuneTracing tracing = StreamRuneTracing.NOOP;
    private Duration claimWait = DEFAULT_CLAIM_WAIT;
    private Duration anchorGrace = LeaseFollower.DEFAULT_ANCHOR_GRACE;
    private LongSupplier drainTicker = System::nanoTime;

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
     * The tracing the runner starts its spans through when its {@link TracingArea#SAGA} area is on:
     * {@code saga/{SagaType}/timeout} for each saga it acts on past its deadline (a poll that finds
     * none starts no span), and under it {@code saga/{SagaType}/compensate} for each compensation
     * command it dispatches. Optional; defaults to {@link StreamRuneTracing#NOOP}, which traces
     * nothing.
     *
     * @param tracing the tracing; not {@code null}
     * @return this builder
     */
    public Builder<S> tracing(StreamRuneTracing tracing) {
      this.tracing = Objects.requireNonNull(tracing, "tracing is required");
      return this;
    }

    /**
     * Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. Records
     * saga faults, compensation outcomes, and CAS conflicts encountered during timeout processing.
     */
    public Builder<S> metrics(StreamRuneMetrics metrics) {
      this.metrics = Objects.requireNonNull(metrics, "metrics is required");
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
     * <p>When the runner fences its forward commands, a claim that waited its bound for a step
     * still in flight is made underneath it (the step's later forward commands are refused at their
     * commit); when it does not, the claim is never made underneath a step.
     *
     * <p>The timeout runner takes the runner's step lock, its lease and its slots from it; the
     * decider, the store, the bus and the saga type are still this builder's. Its claims share the
     * runner's slots with the runner's steps: steps and claims of the saga type together never
     * exceed the runner's budgeted steps ({@link SagaRunner#parallelism()} per bound subscription),
     * and the claims of one query run in parallel up to that limit. {@link #build()} refuses a
     * runner of another saga type or over a store that keeps other rows: its steps are not the ones
     * this runner's claims race with. The parameter is a {@code SagaRunner<?>} because that check
     * is made on the runner's saga type at {@code build()}, and an application often holds its
     * runner as a {@code SagaRunner<?>} bean.
     *
     * <p>This or {@link #stepLock} is required: {@link #build()} refuses a builder that names
     * neither. This is the wiring to use wherever a {@code SagaRunner} of the saga type runs in the
     * same process. The guarantee is that process's: a timeout runner on another node shares
     * nothing with this node's deliveries. An application that hand-wires two {@code SagaRunner}s
     * of one saga type has two locks; the claims of a timeout runner built from one of them do not
     * wait for the other's steps (a saga type has one runner — {@link SagaTypeOwnership}).
     *
     * <p>The timeout runner also follows the runner's lease ({@link SagaRunner#lease()}): built
     * from a runner that carries one, it claims only while a subscription of this process built
     * with {@code sagaRunner(runner)} leads; in a process that runs no such subscription it claims
     * nothing. A process meant to claim deadlines without the saga's subscription builds the
     * timeout runner with {@code stepLock(new SagaStepLock())} instead.
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
     * step in flight. A runner built this way has no slots to share and acts on one saga at a time,
     * on its poll thread. Such a runner claims underneath a delivery that is dispatching, as a
     * timeout runner on another node does, and the delivery's forward fence refuses its next
     * forward command.
     */
    public Builder<S> stepLock(SagaStepLock stepLock) {
      this.stepLock = Objects.requireNonNull(stepLock, "stepLock is required");
      return this;
    }

    /**
     * How long after {@link SagaTimeoutRunner#start()} a lease no subscription of this process has
     * anchored is still expected to be; for tests.
     */
    Builder<S> anchorGrace(Duration anchorGrace) {
      this.anchorGrace = Objects.requireNonNull(anchorGrace, "anchorGrace is required");
      return this;
    }

    /**
     * Overrides the monotonic time the drain budget is measured on; for tests. Production uses
     * {@link System#nanoTime()}.
     */
    Builder<S> drainTicker(LongSupplier drainTicker) {
      this.drainTicker = Objects.requireNonNull(drainTicker, "drainTicker is required");
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
      SagaRunner.Builder.requireIdempotentDispatch(commandBus);
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
          sagaRunner,
          claimWait,
          anchorGrace,
          tracing,
          drainTicker);
    }
  }

  /**
   * {@link #DRAIN_BUDGET_POLL_INTERVALS} times {@code pollInterval} in nanoseconds, or {@link
   * Long#MAX_VALUE} for an interval so long that the product does not fit.
   */
  private static long drainBudgetNanos(Duration pollInterval) {
    try {
      return pollInterval.multipliedBy(DRAIN_BUDGET_POLL_INTERVALS).toNanos();
    } catch (ArithmeticException _) {
      return Long.MAX_VALUE;
    }
  }
}

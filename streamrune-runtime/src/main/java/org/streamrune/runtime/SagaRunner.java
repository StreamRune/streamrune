package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandBus;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.Internal;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;

/**
 * Bridges a {@link SagaOrchestrator} to an {@link EventListener}.
 *
 * <p>Call {@link #asEventListener()} to obtain an {@link EventListener} that can be registered with
 * any {@link org.streamrune.core.subscription.EventSubscription}. For each event received, the
 * runner routes exactly as {@link #resolveTarget} does — a start event ({@link
 * SagaOrchestrator#isStartEvent}) by {@link SagaOrchestrator#extractSagaId}; any other event that
 * carries a saga origin ({@link org.streamrune.core.EventMetadata#sagaOrigin()}) of this runner's
 * saga type to the saga that origin names, without asking the application; every other event by
 * {@link SagaOrchestrator#correlateByEventData}, which sees the stream id and the payload only —
 * and then:
 *
 * <ul>
 *   <li><b>Start event</b> → {@link SagaStepExecutor} with a live {@code ForwardStep(startPath =
 *       true)}: create-first genesis ({@link SagaStore#createGenesisPending}), dispatch, then
 *       {@link SagaStore#applyEvent}. A redelivery over an applied genesis is dedup-skipped; over a
 *       genesis-pending row it re-drives from {@code initialState} and the committed indices dedup
 *       on their offset-stable forward keys.
 *   <li><b>Correlated event</b> → the row is loaded and the event is either applied by the executor
 *       or <em>held</em>: recorded in the {@link SagaDeadLetterStore} under the saga's id, the
 *       entry and the durable {@code dead_letter_pending} shield on the row in one write — never
 *       processed and never dropped. Which, by the row's recovery state:
 *       <ul>
 *         <li>ABSENT — held with {@link SagaEventHeldException.HoldReason#BEFORE_START} only when
 *             the saga already owns a dead-letter record of this runner's type (the row-less
 *             residual of a start whose {@code initialState} threw); a never-started saga's event
 *             keeps the silent drop, so the store is not flooded with unrelated events.
 *         <li>any row but {@code COMPENSATING}, event at or below the recorded {@code
 *             last_applied_offset} — dropped as a redelivery BEFORE any hold below: it was
 *             delivered before and was applied, held (its entry exists) or dropped, so recording it
 *             as a new entry would make the drain feed an already-applied event a second time. A
 *             {@code COMPENSATING} row is exempt because the executor resumes the episode without
 *             consuming the event.
 *         <li>{@code FAULTED} — held with {@link SagaSkippedWhileFaultedException}.
 *         <li>terminal ({@code COMPLETED}/{@code COMPENSATED}/{@code FAILED}) — dropped silently:
 *             the saga is legitimately done and can never consume it.
 *         <li>{@code COMPENSATING} — routed to the executor, which RESUMES the claimed episode. A
 *             resume never consumes the event, so ordering cannot be violated, whatever the genesis
 *             flag says.
 *         <li>genesis pending ({@code genesis_applied = false}) — held with {@link
 *             SagaEventHeldException.HoldReason#GENESIS_PENDING}: consuming it would apply the
 *             event ahead of the start's own commit, and the start's redelivery (which re-evolves
 *             from {@code initialState}) would then clobber its effect while the recorded {@code
 *             last_applied_offset} dedups its redelivery forever.
 *         <li>shielded ({@code dead_letter_pending = true}) — held with {@link
 *             SagaEventHeldException.HoldReason#BACKLOG_PENDING}: the saga owns older quarantined
 *             entries that must drain first. An event already recorded keeps its entry; when that
 *             entry is the event's own poison, whose fault write did not land, the delivery makes
 *             the fault write ({@link #completeInterruptedFault}).
 *         <li>otherwise — applied by the executor as a live {@code ForwardStep}; the executor's own
 *             redelivery check ({@code SagaStepExecutor.isRedelivery}) stays as defence in depth
 *             for adapters that drive it directly.
 *       </ul>
 *       Held events are drained oldest-first by {@code SagaDeadLetterReplayer.replayAll(sagaId)}
 *       through {@link #feedReplay} — the only replay channel — which never applies these hold
 *       rules: the replay feed IS the recovery channel.
 * </ul>
 *
 * <p><b>Poison-event isolation — the single poison handler.</b> Exceptions thrown by orchestrator
 * pure-logic methods ({@code isStartEvent}, {@code correlateByEventData}, {@code extractSagaId},
 * {@code initialState}, {@code evolve}, {@code handle}, {@code compensate}) and their null returns
 * are poison: {@link #recordPoison} quarantines the offending event in the {@link
 * SagaDeadLetterStore} <em>by metadata only</em> (saga id/type, event offset/type, error details,
 * fault time — the event payload is never serialized or copied, so GDPR crypto-shredding stays
 * authoritative in the event store; inspect the event by reading {@code event_stream} at the
 * entry's {@code eventOffset}) and faults the saga, in one fixed order on every event path: row
 * present → entry and shield as one write ({@link SagaDeadLetterStore#publishShielded}) → {@link
 * SagaStore#markFaulted} (the only fault write; no state write, it records {@code
 * pre_fault_status}); row absent and {@code initialState} succeeds → entry → {@link
 * SagaStore#createFaulted} (so no FAULTED row can ever exist without its entry); row absent and
 * {@code initialState} throws → entry only (the documented row-less residual). A fault write that
 * conflicts is retried once on a reload and otherwise left with a WARNING — the entry and the
 * shield ARE recorded, so live events are held and the next drain re-evaluates the saga. Processing
 * continues with the saga's next event and the rest of the page — no head-of-line blocking. {@link
 * SagaDeadLetterStore} is a <em>required</em> {@link Builder} field; {@link Builder#build()} throws
 * {@link NullPointerException} when it is absent. Infrastructure failures ({@link SagaStore}
 * load/create/update/CAS) are NOT caught; they propagate out of {@code onEvents} so the
 * subscription can retry — every write here is a single durable statement, and every crash point
 * between two of them converges on the rerun. For a correlated poison on a live row: a crash before
 * the entry write leaves nothing, and the redelivery runs the event again, meets the poison again
 * and records it; a crash between the entry write and the fault write leaves the row shielded with
 * the poison's own entry, and the redelivery, held behind the shield, finds that entry and makes
 * the fault write ({@link #completeInterruptedFault}); a crash after the fault write leaves the row
 * {@code FAULTED}, and the redelivery keeps the entry ({@link #recordHold}). Each rerun ends with
 * the row {@code FAULTED} and the poison's own entry, as a run that is not interrupted does. No
 * path writes a shield on its own before its entry: a shield without an entry would turn the
 * redelivery into a hold behind a backlog that does not exist. A hold is decided on the row the
 * delivery read, and the store refuses it when the row has applied the event by then (a drain of
 * another process), so no entry records an applied event. The one such failure that is not
 * propagated at once is a lost compare-and-swap of the step's own saga-row write: the step is run
 * again in place first (see <b>Concurrency</b>).
 *
 * <p><b>State-conversion failures are the one exception.</b> A {@link
 * SagaStateSerializationException} means the store could not convert the state to or from its
 * stored form — a different thing from the storage backend failing. {@link SagaStateConversion}
 * splits it: a provably DETERMINISTIC conversion failure (a Jackson mapping defect, a {@code
 * CryptoMappingException} raised by {@code CryptoShreddingModule}, a crypto-shredded subject) is
 * treated as poison, because retrying it can never succeed and letting it propagate wedges the
 * whole subscription; anything that could be transient (a key-store outage, a JDBC blip inside a
 * crypto engine) keeps propagating as infrastructure so the batch retries. Inside the poison
 * handler an unreadable row leaves the entry and the shield recorded and skips the fault write.
 *
 * <p><b>Effectively-once saga command dispatch:</b> each saga command is dispatched via {@link
 * CommandBus#execute(org.streamrune.core.Command, org.streamrune.core.types.IdempotencyKey)} with a
 * deterministic idempotency key: forward commands by the triggering event's global offset and the
 * command's position (see {@link SagaCommandDispatch#forwardKey}); compensation commands by the
 * episode version the saga persists {@code COMPENSATING} at (see {@link
 * SagaCommandDispatch#episodeCompensationKey}). If the correlated event is redelivered and the
 * runner processes it again, the bus finds the key already in the command inbox and returns the
 * recorded result without re-running the handler or re-persisting events.
 *
 * <p><b>Parallel steps.</b> The listener routes a page on the delivering thread, then steps its
 * sagas, at most {@link #parallelism()} steps at a time. The value is {@link Builder#parallelism},
 * else {@value #DEFAULT_PARALLELISM}. When deployment settings are given to the runner, the
 * precedence is: the setting of a subscription bound to this runner, the builder, the deployment's
 * global setting, then {@value #DEFAULT_PARALLELISM}; the integrations bind these settings to
 * configuration properties. Each subscription built with {@code sagaRunner(runner)} delivers pages
 * of its own, so a runner bound to k subscriptions can have k pages, and k times {@link
 * #parallelism()} steps, in flight at once. Within a page, the events of one saga run one after the
 * other, in offset order, on one thread; different sagas at the same time. After a failed step
 * nothing more of that saga is started in the page, and the listener returns only when every item
 * of the page ended, throwing when one failed — so the subscription's checkpoint never passes an
 * event that was not handled, and a saga's row never records an event before an earlier event of
 * that saga. The sagas of a page share no state of the runner: a saga's row, its dead-letter
 * entries and its command-inbox keys are its own, so each saga of a page stepped in parallel ends
 * as in a serial delivery of it, and the page's redelivery answers every finished step from its
 * row. Between sagas there is no order: two sagas whose commands target one aggregate reach it in
 * whichever order the aggregate's lock grants, which a serial delivery or a redelivery need not
 * repeat; a command that committed answers with its recorded result on any rerun. See {@link
 * #asEventListener()}.
 *
 * <p><b>Shared slots.</b> Each step of a delivered page takes one of the runner's slots before its
 * saga's step lock and gives it back when it ends; the deadline claims of the {@link
 * SagaTimeoutRunner} built from this runner take the same slots. Steps and deadline claims of the
 * saga type together stay within the runner's budgeted steps, {@link #parallelism()} per bound
 * subscription, which is what the integrations' connection budget counts — except the claim of a
 * saga whose own step is in flight, which the timeout runner makes without a slot when none frees
 * within its claim wait. Replay feeds, operator calls and sweeper resumes take no slot. Steps and
 * claims get slots in arrival order. A page delivered on a thread that already holds a slot of the
 * runner — a claim's compensation delivered on the appending thread — runs its items on that
 * thread, one after the other, under that slot.
 *
 * <p><b>Concurrency:</b> Saga writes are CAS-protected ({@link SagaStore#createGenesisPending},
 * {@link SagaStore#applyEvent}, {@link SagaStore#claimCompensating}, {@link SagaStore#update},
 * {@link SagaStore#markFaulted}) — concurrent writers cannot silently lose updates. Start-event
 * deduplication is atomic: if two listeners race on the same start event, one wins the create and
 * the other reloads and routes on the recorded facts. A step whose write of the saga row loses its
 * compare-and-swap to another writer is run again in place, up to three attempts in all: the runner
 * reads the row that won and routes the event on it, with no backoff and without the subscription
 * redelivering the page. The router decides what the run again does — a twin that applied the event
 * makes it a redelivery that is dropped, a claim that won resumes the episode, an ended saga drops
 * it, a faulted saga holds it, a removed row drops a correlated event or is created again by a
 * start event. Any command already dispatched in the losing attempt is deduplicated by the command
 * inbox. A forward command's own version conflict is a retry-later failure and is not run again
 * here. After the third conflict in a row the conflict propagates out of {@code onEvents} like an
 * infrastructure failure, after one WARN line; the subscription retries the batch on fresh state
 * and the recorded {@code last_applied_offset} dedups the state advance. The commands' dedup holds
 * even when the duplicate dispatch overlaps the original: {@link VirtualThreadCommandBus} looks the
 * key up again under the aggregate lock and once more when a keyed decider rejects, so a
 * lock-serialized duplicate of a succeeded step is answered with the recorded outcome, never
 * classified as a business rejection and compensated. As a result, concurrent listeners are safe —
 * no leadership gate is required for correctness — but a single subscription per saga type remains
 * the recommended and most efficient topology. A runner built with {@link Builder#leadership}
 * carries the saga type's lease ({@link #lease()}): the subscription built with {@code
 * sagaRunner(runner)} delivers only on the replica that holds it, and the timeout runner and the
 * compensation-retry sweeper built from the runner act there only. A handover between replicas can
 * still overlap a step in flight on the deposed leader; everything said here holds then too. Within
 * one process the steps of one saga do not overlap at all: each delivery and each replay feed holds
 * the saga's {@link SagaStepLock} from before its read of the row to after its write, and a claim
 * that has no event of its own — the timeout runner's, the operator's compensation of a faulted
 * forward step — is not made while a step holds it. That lock is memory and reaches no other
 * process; between processes the compare-and-swaps, the inbox and the forward fence carry
 * everything said here. With the fence on ({@link Builder#forwardFence}, the default) each forward
 * command is dispatched with the saga named as its {@code SagaForward}, and the event store refuses
 * it in the transaction that would store it once the saga row carries a compensation episode, is
 * terminal or is gone; the refused step goes on from the row. No forward command is therefore
 * stored after a compensation another process claimed, or after the saga ended. A throwing {@code
 * compensate()} is treated as a poison event: the triggering event is quarantined and the saga is
 * marked {@code FAULTED}. The event path does not advance a claimed compensation: once a saga is
 * {@code COMPENSATING} (claimed by {@code SagaTimeoutRunner} or by a prior event-path delivery),
 * correlated events never re-run forward {@code evolve}/{@code handle}. They do, however,
 * <em>resume</em> the compensation episode when its owner crashed before the terminal write —
 * re-driving compensation at the existing version under the shared episode key, so a saga with no
 * timeout runner still reaches a terminal status; the resume is driven by {@link SagaStepExecutor}
 * (routed from {@link #processCorrelatedEvent}) and, out of band, by {@link #sweepCompensation}.
 * See {@link SagaStatus#COMPENSATING}.
 *
 * <p><b>Forward-command failures are classified before anything is undone.</b> A failed forward
 * command does <em>not</em> automatically mean "undo the business flow". {@link
 * SagaCommandDispatch#isRetryLater} splits it:
 *
 * <ul>
 *   <li><b>Proven retry-later</b> — an OPEN {@link CircuitBreakerCommandInterceptor} (a framework
 *       DEFAULT bean in all three integrations, so this is the common case during any downstream
 *       incident), a lock that could not be acquired, an {@code OptimisticLockException}, a JDBC
 *       failure ({@code SQLException} anywhere in the chain: connection reset, pool exhaustion,
 *       failover), or a key-store/Vault/KMS outage. The command was never durably applied, so the
 *       saga is left EXACTLY as it was — still {@code RUNNING}, same version, no compensation
 *       claimed — and the failure propagates out of {@code onEvents}. (On a FRESH start event the
 *       genesis-pending row — version 1, {@code genesis_applied = false} — already precedes the
 *       first dispatch, so the saga is visible to {@link SagaTimeoutRunner} for the bounded escape
 *       below, and the redelivered start event re-drives exactly that row.) The subscription
 *       checkpoint does not advance, the event is redelivered, and the re-drive re-dispatches under
 *       the SAME deterministic forward keys, so indices that already committed dedup in the command
 *       inbox and only the failed index re-runs. If the outage outlives the saga's configured
 *       {@code timeout()}, {@link SagaTimeoutRunner} compensates it on the operator's own deadline.
 *       Before this rule a momentary blip — most reliably an open circuit breaker — irreversibly
 *       compensated (refunded) healthy business flows and terminalized them, with no command-DLQ
 *       entry, no saga dead-letter entry, and a compensation metric indistinguishable from a
 *       legitimate business compensation.
 *   <li><b>Everything else</b> — a business rejection ({@code DomainException}/{@code
 *       IllegalArgumentException}, including an interceptor {@code VETO}), a crypto-shredded
 *       subject, and any failure the framework cannot <em>prove</em> retriable — takes the
 *       claim-first compensation branch below, unchanged. The default is deliberately NOT
 *       "everything non-business is transient" (the rule the compensation path uses): there,
 *       mis-binning costs one saga and is bounded by the sweeper's give-up; here, propagation is
 *       unbounded, so mis-binning a deterministic failure would retry the identical batch forever
 *       and wedge every listener on the subscription — the head-of-line class the poison handling
 *       exists to prevent. Compensating and terminalizing is halted, durable and operator-visible;
 *       a wedge is none of those.
 * </ul>
 *
 * <p><b>Compensation (claim-first):</b> If a primary command fails <em>and the failure is not
 * proven retry-later</em>, remaining primary commands in the batch are abandoned and the driving
 * path <em>claims</em> the compensation episode — persisting the saga as {@code COMPENSATING} —
 * <em>before</em> invoking {@link SagaOrchestrator#compensate} or dispatching any compensation
 * command. EVERY path that can drive compensation claims first: the correlated-event path and the
 * start-event path CAS-claim with {@link SagaStore#claimCompensating} at the row version (the row
 * always exists), both via {@link SagaStepExecutor}; and {@link SagaTimeoutRunner} CAS-claims
 * before dispatch. If a claim loses to a concurrent writer (a racing timeout runner, a concurrent
 * start listener, or another replica), that path dispatches ZERO compensation commands and abandons
 * — so exactly one path ever drives compensation for a given episode, and a crash between the claim
 * and the terminal write can never orphan an already-executed compensation against a saga that has
 * no durable row. Claiming first rules out a double compensation (an event path that dispatches
 * compensation before its terminal CAS while a timeout claim wins the CAS and compensates again
 * under a disjoint keyspace) and, on the start path, an orphaned compensation (compensation
 * executed, then a crash before the create lets redelivery re-run the full forward path to
 * completion behind an un-reconciled refund). Compensation commands on all paths are keyed with the
 * shared, episode-scoped {@link SagaCommandDispatch#episodeCompensationKey}, so a genuine
 * double-delivery across paths dedups in the command inbox rather than double-executing the side
 * effect. Individual compensation command failures are logged (not rethrown); a transient/infra
 * failure leaves the saga {@code COMPENSATING} for a later resume, a deterministic one terminalizes
 * it, so the saga always progresses toward a terminal state.
 *
 * @param <S> the saga state type
 */
public final class SagaRunner<S extends SagaState> {

  private static final Logger LOG = LoggerFactory.getLogger(SagaRunner.class);

  private static final String SAGA_ID_REQUIRED = "sagaId is required";

  /**
   * How many times a delivery or a replay feed runs its step for one event: the first attempt and
   * two re-runs.
   */
  static final int MAX_DELIVERY_ATTEMPTS = 3;

  /**
   * The prefix of a saga runner's lease name; the saga type ({@link SagaType#value()}) follows it.
   */
  public static final String LEASE_NAME_PREFIX = "saga:";

  /**
   * The parallelism of a runner that neither its builder, nor a subscription property, nor the
   * global property names.
   */
  public static final int DEFAULT_PARALLELISM = 4;

  /** The highest parallelism a runner accepts. */
  public static final int MAX_PARALLELISM = 64;

  private final SagaOrchestrator<S> orchestrator;

  /**
   * This runner's saga type: the saga state's fully-qualified class name. An event's saga origin
   * belongs to this runner only when its saga type equals this value exactly.
   */
  private final SagaType sagaType;

  private final SagaStore sagaStore;
  private final CommandBus commandBus;
  private final SagaDeadLetterStore sagaDeadLetterStore;
  private final StreamRuneMetrics metrics;

  /**
   * The builder's clock: stamps {@code faultedAt} on every dead-letter entry this runner publishes
   * (never {@code Instant.now()} — the replayer tests advance a {@code MutableClock} to prove an
   * entry was refreshed) and bounds the executor's event-path key-age guard.
   */
  private final Clock clock;

  /**
   * The single-step executor to which both event paths delegate all forward/compensation status
   * transitions and command dispatch. Built once here; {@link #processStartEvent} and {@link
   * #processCorrelatedEvent} are thin adapters that load the saga row, apply the live hold rules
   * (correlated path only) and route the rest to {@link SagaStepExecutor#execute}.
   */
  private final SagaStepExecutor<S> executor;

  /**
   * The step lock of this runner's sagas ({@link SagaStepLock}). Every path of this runner that
   * runs forward commands for an event holds it from before its read of the row to after the
   * executor returns: a live delivery ({@link #deliver}) and a replay feed ({@link #feedReplay}).
   * The operator's compensation of a forward-faulted saga takes it as a claim ({@link
   * #compensateForwardFault}). The compensation-retry sweeper and the dead-letter replayer built
   * from this runner enter through those methods, so they use this instance; a {@link
   * SagaTimeoutRunner} built with {@link SagaTimeoutRunner.Builder#sagaRunner} takes it from {@link
   * #stepLock()}. The resumes of an episode that is already claimed ({@link #sweepCompensation},
   * {@link #resumeFaultedEpisode}) take no lock: a stamped row is never the row of a forward step.
   */
  private final SagaStepLock stepLock;

  /**
   * Whether the application named {@link Builder#inboxRetentionMaxAge(Duration)} itself. When it
   * did, its value wins — including a deliberate zero/negative "disable this guard" — and {@link
   * SagaEventPathRetention#applyDefault} leaves this runner alone.
   */
  private final boolean inboxRetentionMaxAgeConfigured;

  /**
   * Whether the application named {@link Builder#forwardFence(boolean)} itself. When it did, its
   * value stands and {@code SagaForwardFenceDefault.applyDefault} leaves this runner alone.
   */
  private final boolean forwardFenceConfigured;

  /**
   * The lease this saga type is delivered under ({@link #lease()}): gated over the leadership the
   * builder named, ungated otherwise. Named {@code "saga:" + sagaType} either way.
   */
  private final SubscriptionLease lease;

  /** Set once the WARN of a gated listener delivering without its anchor has been written. */
  private final AtomicBoolean unanchoredDeliveryWarned = new AtomicBoolean();

  private final StreamRuneTracing tracing;

  /** Whether {@link #tracing} records the saga area, read once. */
  private final boolean traced;

  /** The parallelism the builder named; {@code null} when it named none. */
  private final Integer builderParallelism;

  /**
   * The parallelism the next page uses with where it came from and the subscriptions bound: one
   * immutable snapshot, published once per change, so a reader never pairs a value with another
   * value's source. A page reads it once.
   */
  private volatile SagaParallelismSnapshot snapshot;

  /** Guards the resolution: the names bound and the settings applied. */
  private final Object parallelismLock = new Object();

  /**
   * How many open subscriptions bound each name of {@link #snapshot}'s bound names; guarded by
   * {@link #parallelismLock}.
   */
  private final Map<String, Integer> bindCounts = new HashMap<>();

  /** The settings an integration applied; {@code null} on a hand-wired runner. */
  private SagaParallelismSettings parallelismSettings;

  /** Decides which failure of a step stops the page; asked by the page's workers at once. */
  private final SagaPageStopRule stopRule;

  /** Runs the work items of a page, the items of one saga in offset order on one thread. */
  private final KeyedPageExecutor pageExecutor;

  /** The meters and the WARN of a page at a parallelism above 1. */
  private final SagaPageAccounting pageAccounting;

  /**
   * The slots the steps of this runner's pages share with the deadline claims of the timeout runner
   * built from it ({@link #budgetedSteps()} at most at once); see {@link SagaSlots}.
   */
  private final SagaSlots slots = new SagaSlots(this::budgetedSteps);

  private SagaRunner(Builder<S> builder) {
    this.builderParallelism = builder.parallelism;
    this.orchestrator = builder.orchestrator;
    this.sagaType = SagaType.fromClass(builder.orchestrator.stateType());
    this.snapshot = resolveParallelism(List.of(), null);
    this.tracing = GuardedTracing.of(builder.tracing, builder.metrics);
    this.traced = tracing.enabled(TracingArea.SAGA);
    this.sagaStore = builder.sagaStore;
    this.commandBus = builder.commandBus;
    this.sagaDeadLetterStore = builder.sagaDeadLetterStore;
    this.metrics = builder.metrics;
    this.clock = builder.clock;
    this.inboxRetentionMaxAgeConfigured = builder.inboxRetentionMaxAge != null;
    this.stepLock = builder.stepLock != null ? builder.stepLock : new SagaStepLock();
    this.executor =
        new SagaStepExecutor<>(
            orchestrator,
            sagaStore,
            commandBus,
            metrics,
            builder.inboxRetentionMaxAge,
            builder.clock,
            this.tracing);
    this.forwardFenceConfigured = builder.forwardFence != null;
    this.executor.forwardFence(!Boolean.FALSE.equals(builder.forwardFence));
    this.lease =
        SubscriptionLease.of(
            builder.leadership, SubscriptionName.of(LEASE_NAME_PREFIX + sagaType.value()));
    this.stopRule =
        new SagaPageStopRule(
            builder.connectionFailureClassifier != null
                ? builder.connectionFailureClassifier
                : new StoreConnectionFailureClassifier(sagaStore));
    this.pageExecutor =
        builder.workerThreads != null
            ? new KeyedPageExecutor(builder.workerThreads, tracing)
            : new KeyedPageExecutor("saga-" + sagaType.value() + "-step-", tracing);
    this.pageAccounting = new SagaPageAccounting(sagaType, metrics, stopRule);
  }

  /**
   * The lease this runner's saga type is delivered under; ungated unless the builder named a
   * leadership ({@link Builder#leadership}). A subscription built with {@code sagaRunner(runner)}
   * anchors it; the {@link SagaTimeoutRunner} and the {@link SagaCompensationRetrySweeper} built
   * from this runner follow it.
   */
  public SubscriptionLease lease() {
    return lease;
  }

  /**
   * The parallelism the next page uses: how many of its steps run at once. Read once per page.
   *
   * <p>The steps of every page of this runner and the deadline claims of the {@link
   * SagaTimeoutRunner} built from it ({@link SagaTimeoutRunner.Builder#sagaRunner}) share one set
   * of slots: together they stay within the runner's budgeted steps, this parallelism per bound
   * subscription (one page per subscription built with {@code sagaRunner(runner)}, at least one),
   * read at each acquire. That is the number the connection budget of the integrations counts, so a
   * claim adds no connection the budget does not hold — except the claim of a saga whose own step
   * is in flight, which the timeout runner makes without a slot when none frees within its claim
   * wait, one connection per such step at most. A step and a claim queue for a slot in arrival
   * order; a slot is taken before the saga's step lock.
   *
   * @return between 1 and {@value #MAX_PARALLELISM}
   */
  public int parallelism() {
    return snapshot.value();
  }

  /**
   * The most steps and deadline claims of this runner holding a slot at once: the parallelism per
   * bound subscription, as of now.
   */
  int budgetedSteps() {
    SagaParallelismSnapshot now = snapshot;
    return now.value() * now.concurrentPages();
  }

  /** The slots shared by this runner's steps and its timeout runner's claims. */
  SagaSlots slots() {
    return slots;
  }

  /**
   * Records that a subscription delivering this runner's saga type was built with this runner, and
   * recomputes the parallelism. A name bound again is counted, not listed twice; the bind lasts
   * until the subscription is closed ({@link #releaseSubscription}). Called by the subscription
   * builders as the last step of {@code build()}.
   *
   * @param name the subscription's name
   * @throws IllegalStateException when two bound names set different parallelism
   * @hidden
   */
  @Internal
  public void bindSubscription(SubscriptionName name) {
    Objects.requireNonNull(name, "name");
    synchronized (parallelismLock) {
      List<String> current = snapshot.boundNames();
      if (current.contains(name.value())) {
        bindCounts.merge(name.value(), 1, Integer::sum);
        return;
      }
      List<String> candidate = new ArrayList<>(current);
      candidate.add(name.value());
      snapshot = resolveParallelism(candidate, parallelismSettings);
      bindCounts.put(name.value(), 1);
    }
    slots.limitChanged();
  }

  /**
   * Records that a subscription bound with {@link #bindSubscription} was closed, and recomputes the
   * parallelism without it. A name bound by several subscriptions leaves the bound names when the
   * last of them is released; a name that is not bound is ignored. Called by the subscriptions'
   * {@code close()}.
   *
   * @param name the subscription's name
   * @hidden
   */
  @Internal
  public void releaseSubscription(SubscriptionName name) {
    Objects.requireNonNull(name, "name");
    synchronized (parallelismLock) {
      Integer count = bindCounts.get(name.value());
      if (count == null) {
        return;
      }
      if (count > 1) {
        bindCounts.put(name.value(), count - 1);
        return;
      }
      List<String> candidate = new ArrayList<>(snapshot.boundNames());
      candidate.remove(name.value());
      // a subset of names that did not conflict cannot conflict: this resolution does not throw
      snapshot = resolveParallelism(candidate, parallelismSettings);
      bindCounts.remove(name.value());
    }
    slots.limitChanged();
  }

  /**
   * The names of the subscriptions bound to this runner and not closed, in bind order.
   *
   * @return an unmodifiable snapshot
   */
  public List<SubscriptionName> boundSubscriptionNames() {
    return snapshot.boundNames().stream().map(SubscriptionName::of).toList();
  }

  /** Applies the deployment's settings and recomputes the parallelism. */
  void applyParallelismSettings(SagaParallelismSettings settings) {
    Objects.requireNonNull(settings, "settings");
    synchronized (parallelismLock) {
      SagaParallelismSnapshot resolved = resolveParallelism(snapshot.boundNames(), settings);
      parallelismSettings = settings;
      snapshot = resolved;
    }
    slots.limitChanged();
  }

  /** Where the parallelism the next page uses came from. */
  SagaParallelismSource parallelismSource() {
    return snapshot.source();
  }

  /** The value, source, winning name and bound names as of one moment. */
  SagaParallelismSnapshot parallelismSnapshot() {
    return snapshot;
  }

  /**
   * The one resolution of the four sources: the value of the bound name that has an entry in the
   * settings, else the builder's, else the global one when settings were applied, else the default.
   */
  private SagaParallelismSnapshot resolveParallelism(
      List<String> names, SagaParallelismSettings settings) {
    List<String> bound = List.copyOf(names);
    Integer value = null;
    String winner = null;
    if (settings != null) {
      for (String name : bound) {
        Integer configured = settings.bySubscription().get(name);
        if (configured == null) {
          continue;
        }
        if (value != null && !value.equals(configured)) {
          throw new IllegalStateException(
              "Saga type "
                  + sagaType.value()
                  + " is bound to the subscriptions '"
                  + LogSanitizer.sanitizeForLog(winner)
                  + "' and '"
                  + LogSanitizer.sanitizeForLog(name)
                  + "', which set different parallelism ("
                  + value
                  + " and "
                  + configured
                  + "): set one value");
        }
        if (value == null) {
          value = configured;
          winner = name;
        }
      }
    }
    if (value != null) {
      return new SagaParallelismSnapshot(
          value, SagaParallelismSource.SUBSCRIPTION_PROPERTY, winner, bound);
    }
    if (builderParallelism != null) {
      return new SagaParallelismSnapshot(
          builderParallelism, SagaParallelismSource.BUILDER, null, bound);
    }
    if (settings != null) {
      return new SagaParallelismSnapshot(
          settings.global(), SagaParallelismSource.GLOBAL_PROPERTY, null, bound);
    }
    return new SagaParallelismSnapshot(
        DEFAULT_PARALLELISM, SagaParallelismSource.DEFAULT, null, bound);
  }

  /** The one range check of a parallelism, shared by the builder and the integrations' settings. */
  static int requireParallelismInRange(String name, int value) {
    if (value < 1 || value > MAX_PARALLELISM) {
      throw new IllegalArgumentException(
          name + " must be between 1 and " + MAX_PARALLELISM + ", got: " + value);
    }
    return value;
  }

  /**
   * Whether this runner's next forward command is fenced at its commit. Package-private; read
   * through {@link SagaForwardFenceDefault#effectiveOf}. The executor holds the value, so a default
   * applied after the build reaches the next dispatch.
   */
  boolean forwardFence() {
    return executor.forwardFence();
  }

  /**
   * Supplies {@code enabled} as this runner's fence unless the application named one on the
   * builder. Package-private; driven by {@code SagaForwardFenceDefault.applyDefault} from the
   * integrations' startup hook.
   *
   * @return {@code true} when this runner took the default
   */
  boolean applyForwardFenceDefault(boolean enabled) {
    if (forwardFenceConfigured) {
      return false;
    }
    executor.forwardFence(enabled);
    return true;
  }

  /**
   * The effective event-path command-inbox retention window ({@code null} = the key-age guard is
   * inert). Package-private; read through {@link SagaEventPathRetention}.
   */
  Duration inboxRetentionMaxAge() {
    return executor.inboxRetentionMaxAge();
  }

  /**
   * Supplies {@code window} as the event-path key-age bound unless the application already named
   * one on the builder. Package-private; driven by {@code SagaEventPathRetention.applyDefault} from
   * the three auto-configurations.
   *
   * @return {@code true} when this runner took the default
   */
  boolean applyInboxRetentionMaxAgeDefault(Duration window) {
    if (inboxRetentionMaxAgeConfigured) {
      return false;
    }
    executor.inboxRetentionMaxAge(window);
    return true;
  }

  /**
   * Returns the saga state class configured on this runner's orchestrator. Package-private accessor
   * for operator tooling (e.g. {@link SagaDeadLetterReplayer}) that needs to load/update sagas via
   * {@link SagaStore} without duplicating the orchestrator's own configuration.
   */
  Class<S> stateType() {
    return orchestrator.stateType();
  }

  /** This runner's saga type. Package-private; read through {@link SagaTypeOwnership}. */
  SagaType sagaType() {
    return sagaType;
  }

  /** The store this runner keeps its sagas in. Package-private, as {@link #sagaType()}. */
  SagaStore sagaStore() {
    return sagaStore;
  }

  /**
   * The step lock this runner's deliveries hold. Package-private: {@link SagaTimeoutRunner} takes
   * it from here so that its claims wait for this runner's steps.
   */
  SagaStepLock stepLock() {
    return stepLock;
  }

  /**
   * What this runner's logic is, for a startup message: the orchestrator class, or the decider
   * class when the runner is built from a {@link SagaDecider}.
   */
  String sagaLogicName() {
    return orchestrator instanceof SagaDeciderAdapter<?> adapter
        ? "the decider " + adapter.deciderType().getName()
        : "the orchestrator " + orchestrator.getClass().getName();
  }

  /**
   * The {@code saga.type} metric tag: {@link SagaType#value()}, the saga state's fully-qualified
   * class name. Never the simple class name — two saga-state classes may share one across packages,
   * and their series would merge.
   */
  private String sagaTypeTag() {
    return SagaType.fromClass(orchestrator.stateType()).value();
  }

  /** Where an event routes: the saga and whether it is that saga's start event. */
  record ReplayTarget(SagaId sagaId, boolean startPath) {}

  /**
   * The one place an event is routed to a saga; live delivery and the dead-letter replayer both
   * call it on the envelope read from the event store. In this order:
   *
   * <ol>
   *   <li>a start event ({@code isStartEvent}) names its saga through {@code extractSagaId}; a saga
   *       origin on a start event is ignored;
   *   <li>any other event whose metadata carries a saga origin of this runner's saga type goes to
   *       the saga that origin names. The saga types are compared exactly, before the origin's saga
   *       id is used, and the application is not asked;
   *   <li>every other event — no origin, or the origin of another saga type — is handed to {@code
   *       correlateByEventData(streamId, payload)}; {@code Optional.empty()} means the event
   *       belongs to no saga of this type.
   * </ol>
   *
   * <p>The origin is written by the command bus in the transaction that appends the event, and
   * nothing rewrites stored metadata, so every delivery and every replay of an event routes it to
   * the same saga. The correlation id is not read: a request chooses it.
   *
   * <p>A throwing application method is poison with no saga id. A null RETURN from {@code
   * extractSagaId} or {@code correlateByEventData} is the same deterministic contract violation as
   * a throw and is rejected INSIDE the poison supplier, so it becomes a null-saga quarantine of the
   * one event instead of an NPE that escapes the listener and wedges the subscription. {@code
   * Optional.empty()} from {@code correlateByEventData} remains the legitimate, quarantine-free
   * "not mine".
   */
  Optional<ReplayTarget> resolveTarget(EventEnvelope event) {
    boolean isStart = poison(null, () -> orchestrator.isStartEvent(event));
    if (isStart) {
      SagaId sagaId =
          poison(
              null,
              () ->
                  Objects.requireNonNull(
                      orchestrator.extractSagaId(event),
                      "extractSagaId returned null for a start event — the SagaOrchestrator"
                          + " contract requires a non-null SagaId"));
      return Optional.of(new ReplayTarget(sagaId, true));
    }
    SagaOrigin origin = event.metadata().sagaOrigin();
    if (origin != null && origin.sagaType().equals(sagaType)) {
      return Optional.of(new ReplayTarget(origin.sagaId(), false));
    }
    Optional<SagaId> routed =
        poison(
            null,
            () ->
                Objects.requireNonNull(
                    orchestrator.correlateByEventData(event.streamId(), event.event()),
                    "correlateByEventData returned null — the SagaOrchestrator contract requires a"
                        + " non-null Optional (return Optional.empty() for an event that belongs"
                        + " to no saga of this type)"));
    return routed.map(id -> new ReplayTarget(id, false));
  }

  /**
   * Returns an {@link EventListener} backed by this runner. A page is delivered in two phases.
   *
   * <ol>
   *   <li><b>Routing</b>, on the delivering thread, in page order, before any step starts: each
   *       event is routed by {@link #resolveTarget}. An event that belongs to no saga of this type
   *       is done at once; a routing method that throws or returns {@code null} quarantines the
   *       event as a null-saga entry ({@link #recordPoison}); every other event becomes a work item
   *       of its saga. This holds at every parallelism, 1 included: the whole page is routed before
   *       its first step, so a routing poison later in a page is quarantined even when an earlier
   *       step fails, and again on each redelivery of that page (a null-saga entry is published
   *       anew each time); a quarantine write that fails fails the page before any step ran.
   *   <li><b>Steps</b>, at most {@link #parallelism()} at once, read once per page. The items of
   *       one saga run in offset order on one thread, one after the other; the sagas of a page run
   *       at the same time on virtual threads named {@code saga-<saga type>-step-<n>}, which carry
   *       the delivering thread's request context and dispatch cause, and its trace context when
   *       the runner has a tracing ({@link Builder#tracing}); a runner without one carries no trace
   *       context to a worker, so the spans of the commands a parallel step dispatches are roots
   *       there, not children of the delivery's span. A page with one saga, or a parallelism of 1,
   *       runs on the delivering thread, and so does a page delivered on a thread that holds one of
   *       the runner's slots. Each step takes a slot first, shared with the timeout runner's
   *       deadline claims ({@link #parallelism()}), and then its saga's step lock. A poison event
   *       is quarantined by {@link #recordPoison} on the thread of its step and its saga's part of
   *       the page goes on. A step that loses the compare-and-swap of its saga row is run again in
   *       place, up to three attempts in all.
   * </ol>
   *
   * <p>A step that fails otherwise — a retry-later failure of a command, a store failure, a
   * conflict that survives the third attempt — ends its saga's part of the page: nothing more of
   * that saga is started in the page. The other sagas of the page run to their end, unless the
   * failure is connection-class (the saga store's {@link SagaStore#connectionFailureClassifier()},
   * or the builder's {@link Builder#connectionFailureClassifier}) or the third failure of the page:
   * then no further step is started and the steps in flight finish. The listener returns only when
   * every item of the page ran to its end; it throws the failure of the item with the lowest offset
   * (an {@link Error} first), the others attached as suppressed, so the subscription does not save
   * its checkpoint and delivers the page again. Every finished step is then answered from its saga
   * row. A page whose items did not all run without a failure is never acknowledged — also when a
   * worker thread ended without running its items (a tracing whose {@code propagating} wrapper
   * dropped the task, or a worker that died while it recorded a failure): the listener then throws
   * an {@link IllegalStateException}. With a parallelism of 1 the first failure ends the page at
   * once, as a plain loop does, and is thrown as the step threw it.
   *
   * <p>At a parallelism above 1 a page that is not acknowledged — also one whose routing phase
   * failed — counts one {@link MetricNames#SAGA_PAGE_FAILED} and is reported by the page WARN, the
   * first such page and then at most once per 5 minutes with the number of pages since; neither for
   * an {@link Error}, nor when the delivering thread was interrupted while it waited for the steps.
   * The gauges of the steps in flight and of the items not started are set to 0 after every page,
   * also when the page could not start its workers. A metrics backend or classifier that throws an
   * exception never replaces the page's failure: the exception is logged and dropped (a failing
   * gauge at most once per 5 minutes, on the delivering thread). An {@link Error} from either
   * always leaves the listener, the page's own failure attached to it as suppressed; the page is
   * then not acknowledged and its redelivery is answered from the rows.
   *
   * <p><b>{@code close()} from inside a step.</b> At a parallelism above 1 a step runs on a worker
   * thread, not on the subscription's poll thread: a subscription's {@code close()} called from
   * inside a step (a command handler or an orchestrator that shuts its own subscription down) does
   * not recognise the listener's thread and waits for the poll thread, which waits for the page —
   * up to the subscription's close timeout (30 s for {@link PollingEventSubscription}), after which
   * it returns with its WARN.
   */
  public EventListener asEventListener() {
    return this::deliverPage;
  }

  /** One page: routed on this thread, then stepped as {@link #asEventListener()} describes. */
  private void deliverPage(List<EventEnvelope> events) {
    warnIfDeliveringWithoutAnAnchor();
    // A page delivered on a thread that holds a slot of this runner — a claim whose compensation
    // is delivered on the appending thread, a delivery nested in a step — runs on that thread
    // under that slot: workers would queue for slots while this thread holds one and waits.
    int p = slots.heldByCurrentThread() ? 1 : snapshot.value();
    if (p == 1) {
      List<SagaWorkItem> items = route(events);
      // SagaStore (load/create/CAS) failures are NOT SagaPoisonException: they leave onEvents, the
      // subscription does not advance the offset, and the page is delivered again.
      failClosed(
              pageExecutor.run(
                  items, SagaWorkItem::sagaId, 1, this::deliverItem, stopRule, PageObserver.NONE))
          .rethrow();
      return;
    }
    List<SagaWorkItem> items;
    try {
      items = route(events);
    } catch (RuntimeException routingFailed) {
      // a quarantine write that failed: a failed page like any other, before any step started
      try {
        pageAccounting.afterRoutingFailure(routingFailed, events.size());
      } catch (Error metricsOrClassifierError) {
        metricsOrClassifierError.addSuppressed(routingFailed);
        throw metricsOrClassifierError;
      }
      throw routingFailed;
    }
    SagaPageAccounting.Page pageGauges = pageAccounting.startPage();
    PageResult ran;
    try {
      ran =
          pageExecutor.run(items, SagaWorkItem::sagaId, p, this::deliverItem, stopRule, pageGauges);
    } catch (Throwable runFailed) {
      // a worker thread that could not be started; run() waited for the workers that did start
      try {
        pageAccounting.endPage(pageGauges);
      } catch (Error metricsError) {
        metricsError.addSuppressed(runFailed);
        throw metricsError;
      }
      throw runFailed;
    }
    PageResult result = failClosed(ran);
    try {
      pageAccounting.endPage(pageGauges);
      pageAccounting.afterPage(result, items);
    } catch (Error metricsOrClassifierError) {
      if (result.failure() != null && result.failure() != metricsOrClassifierError) {
        metricsOrClassifierError.addSuppressed(result.failure());
      }
      throw metricsOrClassifierError;
    }
    result.rethrow();
  }

  /**
   * The page as it ran, with a failure when items were left unrun and nothing failed: a state no
   * path of the executor produces, so the page fails closed instead of being acknowledged.
   */
  private static PageResult failClosed(PageResult result) {
    if (result.failure() != null || result.notStarted() == 0) {
      return result;
    }
    return new PageResult(
        result.items(),
        result.groups(),
        result.failed(),
        result.notStarted(),
        new IllegalStateException(
            result.notStarted()
                + " of "
                + result.items()
                + " step(s) of the page did not run and none failed"),
        -1,
        result.stoppedBy(),
        result.interrupted(),
        result.stepNanos(),
        result.wallNanos());
  }

  /**
   * The routing phase of a page: the work items of the events that route to a saga, in page order.
   * A routing poison is quarantined here, before any step of the page starts.
   */
  private List<SagaWorkItem> route(List<EventEnvelope> events) {
    List<SagaWorkItem> items = new ArrayList<>(events.size());
    for (EventEnvelope event : events) {
      try {
        resolveTarget(event)
            .ifPresent(
                target -> items.add(new SagaWorkItem(target.sagaId(), target.startPath(), event)));
      } catch (SagaPoisonException poison) {
        quarantinePoisonAndWarn(poison, event);
      }
    }
    return items;
  }

  /**
   * The step of one work item, on whichever thread the page executor runs it. It takes one of the
   * runner's slots first ({@link SagaSlots}), then the saga's step lock inside {@link #deliver},
   * and gives the slot back when the step ends.
   *
   * @throws SagaStepLockException when the thread is interrupted while it waits for a slot (its
   *     interrupt status stays set); nothing was read, and the event is delivered again
   */
  private void deliverItem(SagaWorkItem item) {
    SagaSlots.Slot slot;
    try {
      slot = slots.acquire();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new SagaStepLockException(
          "Interrupted while a step of saga "
              + LogSanitizer.sanitizeForLog(item.sagaId().value())
              + " waited for a slot; this delivery read nothing and dispatched nothing",
          interrupted);
    }
    try (slot) {
      try {
        deliverTraced(item.target(), item.event());
      } catch (SagaPoisonException poison) {
        quarantinePoisonAndWarn(poison, item.event());
      }
    }
  }

  /** The single poison handler of the event path, and its WARN. */
  private void quarantinePoisonAndWarn(SagaPoisonException poison, EventEnvelope event) {
    SagaId poisonSagaId = poison.sagaId();
    recordPoison(poisonSagaId, event, poison.getCause());
    LOG.warn(
        "Saga poison event quarantined (saga={}, offset={})",
        (poisonSagaId == null ? "<null>" : LogSanitizer.sanitizeForLog(poisonSagaId.value())),
        event.globalOffset().value(),
        poison.getCause());
  }

  /**
   * Writes one WARN when a runner that carries a lease delivers while no subscription of this
   * process has ever anchored that lease: its subscription was built with {@code
   * listener(runner.asEventListener())} instead of {@code sagaRunner(runner)}.
   */
  private void warnIfDeliveringWithoutAnAnchor() {
    if (lease.isGated()
        && !lease.everAnchored()
        && unanchoredDeliveryWarned.compareAndSet(false, true)) {
      LOG.warn(
          "Saga type {} delivers without the anchor of its lease '{}': build the subscription with"
              + " .sagaRunner(runner). Its events are delivered on every replica that runs this"
              + " listener, and its timeout runner and compensation-retry sweeper act on none.",
          sagaType.value(),
          lease.name());
    }
  }

  /**
   * {@link #deliver}, inside the span {@code saga/{SagaType}/step} when the saga area is traced
   * ({@link #startStepSpan}). A poison event records its exception with the outcome {@code
   * FAULTED}. A step that waited past the step bound for another step of the saga ({@link
   * SagaStepLockException}) is the outcome {@code DEFERRED}, not an error: nothing was read, and
   * the event is delivered again. Any other failure, after which the event is delivered again — a
   * conflict that survived the last compare-and-swap attempt, a store or command failure — records
   * its exception alone.
   */
  private void deliverTraced(ReplayTarget target, EventEnvelope event) {
    if (!traced) {
      deliver(target, event);
      return;
    }
    TraceScope scope = startStepSpan(target.sagaId(), event);
    try {
      scope.setAttribute(TraceAttributes.SAGA_STEP_OUTCOME, deliver(target, event));
      scope.recordSuccess();
    } catch (SagaPoisonException poison) {
      scope.setAttribute(TraceAttributes.SAGA_STEP_OUTCOME, FAULTED);
      scope.recordError(poison.getCause() != null ? poison.getCause() : poison);
      throw poison;
    } catch (SagaStepLockException notNow) {
      scope.setAttribute(TraceAttributes.SAGA_STEP_OUTCOME, DEFERRED);
      throw notNow;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * Starts the span {@code saga/{SagaType}/step} of one event the saga {@code sagaId} handles,
   * linked to the span that wrote the event when its metadata names one.
   */
  private TraceScope startStepSpan(SagaId sagaId, EventEnvelope event) {
    return tracing.start(
        TracingArea.SAGA,
        TraceAttributes.sagaStepSpanName(sagaType),
        SpanKind.INTERNAL,
        SpanAttributes.builder()
            .put(TraceAttributes.SAGA_TYPE, sagaType.value())
            .put(TraceAttributes.SAGA_ID, LogSanitizer.sanitizeForTrace(sagaId.value()))
            .build(),
        SpanLink.of(event.metadata()),
        SpanBaggage.KEEP);
  }

  /**
   * The value of {@link TraceAttributes#SAGA_STEP_OUTCOME} for a step that ended in {@code
   * outcome}.
   */
  static String stepOutcome(StepOutcome outcome) {
    return switch (outcome) {
      case FORWARD_PROGRESSED -> "ADVANCED";
      case COMPLETED -> "COMPLETED";
      case COMPENSATING_LEFT, COMPENSATING_REFUSED -> "COMPENSATING";
      case COMPENSATED -> "COMPENSATED";
      case FAILED -> "FAILED";
      case FAULTED -> "FAULTED";
      case CLAIM_LOST,
          CLAIM_DEFERRED,
          SKIPPED_HALTED,
          SKIPPED_DEDUP,
          SKIPPED_NO_ROW,
          SKIPPED_GENESIS_PENDING,
          NOT_COMPENSATING ->
          "SKIPPED";
    };
  }

  /**
   * One live delivery, as a step of its saga: the row is read, the event's commands are dispatched
   * and the row is written while this thread holds the saga's step lock, so within this process no
   * claim without an event lands between that read and that write, and no second step of the saga
   * runs beside this one ({@link SagaStepLock}). The lock is released when the step returns or
   * throws — a poison, a retry-later failure, a lost compare-and-swap, an {@link Error} — and the
   * poison handler of {@link #asEventListener} runs after it.
   *
   * <p>A step whose write of the saga row loses the compare-and-swap is run again on the row that
   * won, up to {@value #MAX_DELIVERY_ATTEMPTS} attempts in all, while the lock stays held: the lock
   * is taken once, so no claim without an event lands between two attempts. The conflict of the
   * last attempt propagates.
   *
   * @throws SagaStepLockException when another step of the saga stays in flight past the step
   *     bound, or this thread is interrupted while it waits; nothing was read, and the failure
   *     leaves the listener so the event is delivered again
   */
  private String deliver(ReplayTarget target, EventEnvelope event) {
    try (SagaStepLock.Hold _ = stepLock.enterStep(sagaType, target.sagaId())) {
      for (int attempt = 1; ; attempt++) {
        try {
          return target.startPath()
              ? processStartEvent(target.sagaId(), event)
              : processCorrelatedEvent(target.sagaId(), event);
        } catch (SagaRowConflictException conflict) {
          rerunOrPropagate(target.sagaId(), event, attempt, conflict, /* replay= */ false);
        }
      }
    }
  }

  /**
   * The replayer's feed: the same executor call as live delivery with {@code replayRedrive=true},
   * never the live hold rules (the replay feed IS the recovery channel). Poison is recorded by the
   * single poison handler and reported as {@link StepOutcome#FAULTED}; a retry-later failure and
   * every infrastructure failure propagate to the replayer. A feed is a step of its saga and holds
   * the saga's step lock as a live delivery does ({@link #deliver}); a wait for it that ends
   * without the lock propagates as {@link SagaStepLockException}. A feed whose write of the saga
   * row loses the compare-and-swap is run again as a live delivery is, up to {@value
   * #MAX_DELIVERY_ATTEMPTS} attempts in all, inside the poison handling below; the conflict of the
   * last attempt reaches the caller.
   *
   * @param operatorForced the operator's {@code force} on {@link SagaDeadLetterReplayer#replay(
   *     SagaId, org.streamrune.core.types.GlobalOffset, boolean)} / {@link
   *     SagaDeadLetterReplayer#replayAll(SagaId, boolean)}, passed through as {@code
   *     ForwardStep.operatorForced}: the explicit, per-saga acknowledgement of at-least-once that
   *     lets the executor resume a compensation episode older than the command-inbox retention
   *     window instead of refusing it. This is the ONLY place it is set; no live path sets it
   */
  StepOutcome feedReplay(
      SagaId sagaId, EventEnvelope event, boolean startPath, boolean operatorForced) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    if (!traced) {
      return feedReplayStep(sagaId, event, startPath, operatorForced);
    }
    // The replayer's feed is a step of its saga like a live delivery: the same span, whose
    // outcome is FAULTED where the poison handler swallows the event's poison.
    TraceScope scope = startStepSpan(sagaId, event);
    try {
      StepOutcome outcome = feedReplayStep(sagaId, event, startPath, operatorForced);
      scope.setAttribute(TraceAttributes.SAGA_STEP_OUTCOME, stepOutcome(outcome));
      scope.recordSuccess();
      return outcome;
    } catch (SagaStepLockException notNow) {
      scope.setAttribute(TraceAttributes.SAGA_STEP_OUTCOME, DEFERRED);
      throw notNow;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private StepOutcome feedReplayStep(
      SagaId sagaId, EventEnvelope event, boolean startPath, boolean operatorForced) {
    try (SagaStepLock.Hold _ = stepLock.enterStep(sagaType, sagaId)) {
      for (int attempt = 1; ; attempt++) {
        try {
          // a deterministic conversion poison on the load lands in the catch below
          Optional<LoadedSaga<S>> row = loadForDelivery(sagaId);
          // the entry being fed exists, so a row the feed creates is born shielded
          return executor.execute(
              sagaId,
              row,
              new SagaTrigger.ForwardStep(event, startPath, true, true, operatorForced));
        } catch (SagaRowConflictException conflict) {
          rerunOrPropagate(sagaId, event, attempt, conflict, /* replay= */ true);
        }
      }
    } catch (SagaPoisonException poison) {
      recordPoison(sagaId, event, poison.getCause());
      LOG.warn(
          "Replayed saga event is still poison (saga={}, offset={}); the entry was refreshed and"
              + " the saga stays FAULTED",
          LogSanitizer.sanitizeForLog(sagaId.value()),
          event.globalOffset().value(),
          poison.getCause());
      return StepOutcome.FAULTED;
    }
  }

  /**
   * After a lost compare-and-swap of the saga row: returns, so that the caller runs the step again
   * on the row that won, or — on the last attempt — logs one WARN and rethrows the conflict, which
   * then takes the path of any other failure of the step (the subscription delivers the page again;
   * the replay call fails).
   */
  private void rerunOrPropagate(
      SagaId sagaId,
      EventEnvelope event,
      int attempt,
      SagaRowConflictException conflict,
      boolean replay) {
    String sanitized = LogSanitizer.sanitizeForLog(sagaId.value());
    long offset = event.globalOffset().value();
    if (attempt >= MAX_DELIVERY_ATTEMPTS) {
      LOG.warn(
          "Saga {} {} lost its compare-and-swap on {} attempts in a row for the {}event at offset"
              + " {}; {}",
          sagaType.value(),
          sanitized,
          MAX_DELIVERY_ATTEMPTS,
          replay ? "replayed " : "",
          offset,
          replay
              ? "the replay call fails with the conflict"
              : "the subscription delivers the page again");
      throw conflict;
    }
    recordMetric(
        () -> metrics.recordSagaDeliveryRetried(sagaTypeTag()), "recordSagaDeliveryRetried");
    LOG.debug(
        "Saga {} {}: the step for the event at offset {} lost its compare-and-swap to another"
            + " writer; attempt {} of {} runs it again on the row that won",
        sagaType.value(),
        sanitized,
        offset,
        attempt + 1,
        MAX_DELIVERY_ATTEMPTS);
  }

  /** Runs an orchestrator pure-logic step, converting any thrown exception to a poison signal. */
  private <T> T poison(SagaId sagaId, java.util.function.Supplier<T> step) {
    try {
      return step.get();
    } catch (RuntimeException e) {
      throw new SagaPoisonException(sagaId, e);
    }
  }

  /**
   * Every live start delivery is a live {@code ForwardStep}: the executor's guard subsumes the
   * routing (COMPENSATING row → resume the claimed episode; terminal or FAULTED row → skipped;
   * applied genesis → dedup; genesis-pending row → re-drive; absent → create-first genesis, born
   * shielded when the saga already owns a dead-letter record). {@code replayRedrive} is always
   * {@code false} here — {@link #feedReplay} is the replay channel.
   */
  private String processStartEvent(SagaId sagaId, EventEnvelope event) {
    var existing = loadForDelivery(sagaId); // INFRA load → propagates
    // A row the executor creates for an ABSENT saga must be born shielded
    // when the saga already owns dead-letter entries — the row-less residual and the
    // correlated events held BEFORE_START behind it — or the next live event applies ahead of them
    // and retention prunes them. One indexed findBySaga per FRESH start (row absent), the same
    // lookup the ABSENT correlated branch already pays; a present row never pays it.
    boolean ownsRecord = existing.isEmpty() && hasOwnDeadLetterRecord(sagaId);
    return stepOutcome(
        executor.execute(
            sagaId, existing, new SagaTrigger.ForwardStep(event, true, false, ownsRecord)));
  }

  /**
   * The live hold rules; see the class javadoc for the state-by-state rationale. Returns the step's
   * outcome for its span: {@code DEFERRED} for an event held, {@code SKIPPED} for one dropped.
   */
  private String processCorrelatedEvent(SagaId sagaId, EventEnvelope event) {
    var existing = loadForDelivery(sagaId); // INFRA load → propagates
    if (existing.isEmpty()) {
      // The row-less residual: only a saga whose initialState failed (threw, or
      // returned null) owns an entry without a row. A never-started saga's out-of-order event
      // keeps the silent drop.
      if (hasOwnDeadLetterRecord(sagaId)) {
        return heldOutcome(hold(sagaId, event, SagaEventHeldException.HoldReason.BEFORE_START));
      }
      return SKIPPED;
    }
    LoadedSaga<S> row = existing.get();
    if (row.status() != SagaStatus.COMPENSATING && isAppliedRedelivery(row, event)) {
      // The redelivery check precedes every hold. The subscription delivers in offset
      // order, so an event at or below the recorded applied maximum was delivered before and was
      // applied (dedup), held (its entry already exists; a re-publish would only refresh it) or
      // dropped (dropping again is right). Holding it instead would record an already-applied
      // event as a NEW entry that the drain — whose replay rule is exact-match by design — feeds
      // a second time. A COMPENSATING row is exempt on purpose: the executor RESUMES the claimed
      // episode and never consumes the event, so nothing can double-apply there and the
      // executor keeps deciding that route alone. A genesis-pending row carries no applied offset,
      // so its GENESIS_PENDING hold is untouched.
      LOG.debug(
          "Correlated event at offset {} for saga {} is at or below the recorded applied"
              + " maximum ({}); redelivery skipped",
          event.globalOffset().value(),
          LogSanitizer.sanitizeForLog(sagaId.value()),
          row.lastAppliedOffset());
      return SKIPPED;
    }
    if (row.status() == SagaStatus.FAULTED) {
      return heldOutcome(holdWhileFaulted(sagaId, event));
    }
    if (row.status().isTerminal()) {
      return SKIPPED; // legitimately done: drop silently
    }
    if (row.status() == SagaStatus.COMPENSATING) {
      // A resume never consumes the event, so ordering cannot be violated; the executor resumes
      // the claimed episode whatever the genesis flag says.
      return stepOutcome(
          executor.execute(sagaId, existing, new SagaTrigger.ForwardStep(event, false, false)));
    }
    if (!row.genesisApplied()) {
      return heldOutcome(hold(sagaId, event, SagaEventHeldException.HoldReason.GENESIS_PENDING));
    }
    if (row.deadLetterPending()) {
      SagaDeadLetterStore.HoldResult held =
          hold(sagaId, event, SagaEventHeldException.HoldReason.BACKLOG_PENDING);
      if (held instanceof SagaDeadLetterStore.HoldResult.Kept(var kept)
          && isUnfinishedPoison(kept)) {
        completeInterruptedFault(sagaId, event, row);
        return FAULTED;
      }
      return heldOutcome(held);
    }
    StepOutcome outcome =
        executor.execute(sagaId, existing, new SagaTrigger.ForwardStep(event, false, false));
    if (outcome == StepOutcome.SKIPPED_HALTED && holdIfFaultedUnderTheStep(sagaId, event)) {
      return DEFERRED;
    }
    return stepOutcome(outcome);
  }

  /** The step outcome of an event held for later, or of a step that will run again. */
  private static final String DEFERRED = "DEFERRED";

  /** The step outcome of a poison event. */
  private static final String FAULTED = "FAULTED";

  /** The step outcome of an event with nothing to do. */
  private static final String SKIPPED = "SKIPPED";

  /**
   * The step outcome of a hold: {@code SKIPPED} when the store found the event applied (a drain of
   * another process applied it after this delivery read the row), {@code DEFERRED} otherwise.
   */
  private static String heldOutcome(SagaDeadLetterStore.HoldResult held) {
    return held instanceof SagaDeadLetterStore.HoldResult.AlreadyApplied ? SKIPPED : DEFERRED;
  }

  /**
   * A live step over a forward row ends {@code SKIPPED_HALTED} only when one of its forward
   * commands was refused at its commit and the row it read again was terminal or {@code FAULTED}:
   * another path ended the saga, or faulted it inside the compensation episode it opened, while the
   * step was in flight. A terminal saga drops the event, as a delivery that finds it terminal does.
   * A {@code FAULTED} saga holds it ({@link #holdWhileFaulted}), as the delivery of the event after
   * the fault would: the event is not applied, and it is not lost either.
   */
  private boolean holdIfFaultedUnderTheStep(SagaId sagaId, EventEnvelope event) {
    Optional<LoadedSaga<S>> reread = loadForDelivery(sagaId);
    if (reread.isPresent() && reread.get().status() == SagaStatus.FAULTED) {
      holdWhileFaulted(sagaId, event);
      return true;
    }
    return false;
  }

  /**
   * The redelivery check on the live path: {@code offset <= last_applied_offset} — the same rule
   * {@code SagaStepExecutor.isRedelivery} applies once a row reaches it, evaluated here BEFORE the
   * holds so a FAULTED or shielded row never records an applied redelivery as a new entry.
   */
  private static boolean isAppliedRedelivery(LoadedSaga<?> row, EventEnvelope event) {
    return row.lastAppliedOffset() != null
        && event.globalOffset().value() <= row.lastAppliedOffset();
  }

  /**
   * {@code hold(reason)}: entry and shield, one write ({@link #recordHold} through {@link
   * SagaDeadLetterStore#publishShieldedIfAbsent}) → metric → WARNING. No shield is written on its
   * own before the entry: a drain's conditional clear that lands before the write is repaired by
   * the write, one that lands during it queues the write's shield behind it, one after it sees the
   * entry; and no crash can leave a shield without its entry, or an entry without its shield. On an
   * absent row ({@code BEFORE_START}) the shield write is the stores' documented no-op. The entry
   * is written only when the saga has none of this type at the event's offset and has not applied
   * the event, so a redelivery storm converges on one record, the first. Returns what the store
   * did.
   */
  private SagaDeadLetterStore.HoldResult hold(
      SagaId sagaId, EventEnvelope event, SagaEventHeldException.HoldReason reason) {
    SagaDeadLetterStore.HoldResult held =
        recordHold(sagaId, event, new SagaEventHeldException(sagaId, event.globalOffset(), reason));
    if (held instanceof SagaDeadLetterStore.HoldResult.Written) {
      recordMetric(
          () -> metrics.recordSagaEventHeld(sagaTypeTag(), reason.name()), "recordSagaEventHeld");
      LOG.warn(
          "Correlated event at offset {} for saga {} was HELD [{}] and recorded in the saga"
              + " dead-letter store; drain the saga with SagaDeadLetterReplayer.replayAll(sagaId)"
              + " once the blocker is gone.",
          event.globalOffset().value(),
          LogSanitizer.sanitizeForLog(sagaId.value()),
          reason);
    }
    return held;
  }

  /**
   * Whether an entry a hold kept on a shielded live row is the saga's own poison whose fault write
   * did not land: an entry of this runner's saga type that no hold wrote ({@link
   * #HOLD_ERROR_TYPES}). A store keys a named entry by saga id, saga type and offset, so the entry
   * it keeps is of the hold's type; the type check refuses an entry of another type from a store
   * that breaks that rule.
   */
  private boolean isUnfinishedPoison(SagaDeadLetterStore.SagaDeadLetterEntry kept) {
    return SagaType.fromClass(orchestrator.stateType()).equals(kept.sagaType())
        && !HOLD_ERROR_TYPES.contains(kept.errorType());
  }

  /**
   * The redelivery of a correlated event over a shielded live row found the event's own poison
   * recorded and the row not {@code FAULTED}: the fault write that follows the poison's entry did
   * not land. That is a process that stopped between the two writes, a fault write that lost its
   * compare-and-swap twice, a fault write skipped because the row could not be read then, or a
   * drain that un-faulted the row to feed an older entry and has not fed this one yet (the fault is
   * then written a second time, and the drain feeds the event over it). This delivery makes the
   * fault write at the version it read, so the saga ends {@code FAULTED} with the poison's own
   * entry, where a run that is not interrupted ends.
   *
   * <p>One compare-and-swap, never a reload and a second write: the decision was made on the row
   * this delivery read. A lost compare-and-swap leaves the listener as {@link
   * SagaRowConflictException}, and {@link #deliver} routes the event again on the row that won — a
   * drain of another process that applied the event makes it an applied redelivery, a fault another
   * path wrote makes it a kept hold, a claim resumes the episode, a terminal row drops it.
   */
  private void completeInterruptedFault(SagaId sagaId, EventEnvelope event, LoadedSaga<S> row) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    try {
      if (sagaStore.markFaulted(sagaId, type, row.version())) {
        recordFaultedMetric();
      }
    } catch (OptimisticLockException lost) {
      recordCasConflictMetric();
      throw new SagaRowConflictException(lost);
    }
    LOG.warn(
        "Correlated event at offset {} has a recorded poison entry and saga {} was not FAULTED;"
            + " the redelivery marked it FAULTED",
        event.globalOffset().value(),
        LogSanitizer.sanitizeForLog(sagaId.value()));
  }

  /**
   * The FAULTED hold keeps its own exception type and its own metric series (on top of the
   * quarantine counter the entry earns): folded into {@code streamrune.saga.quarantined} alone, an
   * already-known fault reads as a stream of NEW orchestrator defects, so {@code quarantined -
   * skipped_while_faulted - event_held} stays the genuine-poison count (apart from event-path
   * {@code SagaStaleCompensationEpisodeException} refusals — see {@code
   * MetricNames#SAGA_SKIPPED_WHILE_FAULTED}). The saga row is NOT faulted again (it already is),
   * only shielded — the row's own recorded {@link LoadedSaga#preFaultStatus()} is what the executor
   * resumes from on replay; the entry carries no transitional copy.
   *
   * <p>The page that carried the poison itself is delivered again whenever the subscription retries
   * it after the poison was recorded (a retry-later failure later in the page, a crash before the
   * checkpoint, a handover): the poison event then meets the FAULTED row it faulted. Its entry is
   * already recorded, so {@link #recordHold} keeps it — the poison's own exception class name and
   * message — and this delivery changes and counts nothing. Returns what the store did.
   */
  private SagaDeadLetterStore.HoldResult holdWhileFaulted(SagaId sagaId, EventEnvelope event) {
    SagaDeadLetterStore.HoldResult held =
        recordHold(
            sagaId, event, new SagaSkippedWhileFaultedException(sagaId, event.globalOffset()));
    if (held instanceof SagaDeadLetterStore.HoldResult.Written) {
      recordMetric(
          () -> metrics.recordSagaSkippedWhileFaulted(sagaTypeTag()),
          "recordSagaSkippedWhileFaulted");
      LOG.warn(
          "Correlated event at offset {} arrived while saga {} is FAULTED and was NOT processed;"
              + " recorded in the saga dead-letter store so it is not lost. Fix the orchestrator,"
              + " then drain the saga with SagaDeadLetterReplayer.replayAll(sagaId).",
          event.globalOffset().value(),
          LogSanitizer.sanitizeForLog(sagaId.value()));
    }
    return held;
  }

  /**
   * The single poison handler: row present → entry and shield (one write), {@code markFaulted}; row
   * absent and {@code initialState} succeeds → entry, {@code createFaulted}; row absent and {@code
   * initialState} throws → entry only (the documented residual). A deterministic conversion failure
   * on the load leaves the entry and the shield recorded and skips the fault write, exactly as it
   * always did. Package-private: {@link #feedReplay} and the live listener are its two callers.
   */
  void recordPoison(SagaId sagaId, EventEnvelope event, Throwable cause) {
    if (sagaId == null) {
      quarantine(null, event, cause);
      return;
    }
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    Optional<LoadedSaga<S>> row;
    boolean rowUnreadable = false;
    try {
      row = sagaStore.load(sagaId, type, orchestrator.stateType());
    } catch (SagaStateSerializationException conversionFailed) {
      if (!SagaStateConversion.isDeterministic(conversionFailed)) {
        throw conversionFailed;
      }
      logFaultWriteSkipped(sagaId, conversionFailed);
      row = Optional.empty();
      rowUnreadable = true;
    }
    quarantine(sagaId, event, cause);
    if (rowUnreadable) {
      return;
    }
    if (row.isPresent()) {
      faultExistingRow(sagaId, type, row.get().version());
    } else {
      createFaultedRow(sagaId, type);
    }
  }

  /**
   * {@code markFaulted} at the loaded version; on a conflict reload once — already FAULTED (a
   * concurrent path faulted it first) or terminal (a concurrent path completed it; both stores
   * refuse the write) → done; row gone → {@link #createFaultedRow}; else retry at the reloaded
   * version, and a second conflict is WARNed and left (the entry and the shield ARE recorded, so
   * live events are held and the next drain re-evaluates the saga). The fault metric counts the
   * TRANSITION only ({@code markFaulted} returns {@code false} on a still-poison refresh of an
   * already-FAULTED row).
   */
  private void faultExistingRow(SagaId sagaId, SagaType type, long version) {
    try {
      if (sagaStore.markFaulted(sagaId, type, version)) {
        recordFaultedMetric();
      }
      return;
    } catch (OptimisticLockException _) {
      recordCasConflictMetric();
    }
    Optional<LoadedSaga<S>> reloaded;
    try {
      reloaded = sagaStore.load(sagaId, type, orchestrator.stateType());
    } catch (SagaStateSerializationException conversionFailed) {
      if (!SagaStateConversion.isDeterministic(conversionFailed)) {
        throw conversionFailed;
      }
      logFaultWriteSkipped(sagaId, conversionFailed);
      return;
    }
    if (reloaded.isEmpty()) {
      createFaultedRow(sagaId, type);
      return;
    }
    if (reloaded.get().status() == SagaStatus.FAULTED) {
      return; // a concurrent path faulted it first
    }
    if (reloaded.get().status().isTerminal()) {
      // A concurrent sweeper / timeout runner / replica terminalized the saga between the delivery
      // load and the fault write. Both stores refuse markFaulted on a terminal row, so a second
      // attempt would only add a spurious cas-conflict sample and a misleading WARN; the row is
      // legitimately done and the entry is kept for the drain's discard.
      LOG.debug(
          "Saga {} completed concurrently while its poison event was being recorded; the row is"
              + " left terminal and the dead-letter entry is kept for the drain's discard",
          LogSanitizer.sanitizeForLog(sagaId.value()));
      return;
    }
    try {
      if (sagaStore.markFaulted(sagaId, type, reloaded.get().version())) {
        recordFaultedMetric();
      }
    } catch (OptimisticLockException _) {
      recordCasConflictMetric();
      LOG.warn(
          "Saga {} advanced concurrently twice while its poison event was being recorded; the"
              + " row is left as it is. The dead-letter entry and the shield ARE recorded, so live"
              + " correlated events are held and the next replayAll re-evaluates the saga.",
          LogSanitizer.sanitizeForLog(sagaId.value()));
    }
  }

  /**
   * {@code createFaulted(initialState)} for a poison with no row (the entry is already published,
   * so no FAULTED row ever exists without its entry). {@code initialState} is user pure-logic — a
   * throw OR a null return here is the same deterministic failure that quarantined the event, so it
   * is logged and the saga stays row-less (the one row-less case) rather than escaping the poison
   * handler and wedging the subscription. A conflict means a concurrent deliverer created the row:
   * it has no shield yet, so shield it and fault it as an existing row.
   */
  private void createFaultedRow(SagaId sagaId, SagaType type) {
    S initial;
    try {
      initial = orchestrator.initialState(sagaId);
    } catch (RuntimeException initialStateFailure) {
      logRowlessResidual(sagaId, initialStateFailure);
      return;
    }
    if (initial == null) {
      logRowlessResidual(
          sagaId,
          new NullPointerException(
              "SagaOrchestrator.initialState returned null — the SagaOrchestrator contract"
                  + " requires a non-null initial state"));
      return;
    }
    try {
      sagaStore.createFaulted(sagaId, type, initial);
      recordFaultedMetric();
    } catch (SagaStateSerializationException conversionFailed) {
      if (!SagaStateConversion.isDeterministic(conversionFailed)) {
        throw conversionFailed;
      }
      logFaultWriteSkipped(sagaId, conversionFailed);
    } catch (OptimisticLockException _) {
      recordCasConflictMetric();
      // the concurrent creator's row has no shield yet
      sagaStore.setDeadLetterPending(sagaId, type, true);
      Optional<LoadedSaga<S>> created;
      try {
        created = sagaStore.load(sagaId, type, orchestrator.stateType());
      } catch (SagaStateSerializationException conversionFailed) {
        // Inside the handler: an unreadable row is logged, never re-raised as poison
        // (that would escape the listener's catch and wedge the subscription).
        if (!SagaStateConversion.isDeterministic(conversionFailed)) {
          throw conversionFailed;
        }
        logFaultWriteSkipped(sagaId, conversionFailed);
        return;
      }
      created.ifPresent(row -> faultExistingRow(sagaId, type, row.version()));
    }
  }

  private void logRowlessResidual(SagaId sagaId, RuntimeException initialStateFailure) {
    LOG.warn(
        "orchestrator.initialState failed while creating a FAULTED row for saga {} — the poison"
            + " event is recorded in the dead-letter store and this saga stays"
            + " row-less until a replay of its start entry succeeds (the one row-less case)",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        initialStateFailure);
  }

  private void recordFaultedMetric() {
    recordMetric(() -> metrics.recordSagaFaulted(sagaTypeTag()), "recordSagaFaulted");
  }

  private void recordCasConflictMetric() {
    recordMetric(() -> metrics.recordSagaCasConflict(sagaTypeTag()), "recordSagaCasConflict");
  }

  /**
   * Sweeper entrypoint: re-drives a stuck {@link SagaStatus#COMPENSATING} saga's compensation
   * episode out of band — with no triggering event and no {@link SagaTimeoutRunner}. This is what
   * gives a saga with an empty {@code timeout()} automatic, durable recovery from a transient
   * compensation failure: the claim-first path ({@link SagaStepExecutor}) leaves such a saga {@code
   * COMPENSATING} (never force-{@code FAILED}), but for a no-timeout saga nothing else re-drives it
   * (the event's offset already advanced, so no redelivery arrives, and there is no timeout
   * runner). The {@code SagaCompensationRetrySweeper} polls the {@link SagaStore} for these and
   * calls this method.
   *
   * <p>Loads the saga, then:
   *
   * <ul>
   *   <li>if it is not {@code COMPENSATING} — a concurrent event-path/timeout resume already
   *       terminalized it, or it was deleted — returns {@link StepOutcome#NOT_COMPENSATING};
   *   <li>if {@code giveUp} is set (the sweeper's durable, age-based poison bound was exceeded),
   *       CAS-writes the saga {@code FAULTED} at its current version (halted → excluded from future
   *       sweeps) via {@link #faultStuckCompensation} and returns {@link StepOutcome#FAULTED} — a
   *       compensation that never durably completes is quarantined instead of looping forever;
   *   <li>otherwise delegates to {@link SagaStepExecutor} with a {@link
   *       SagaTrigger.ResumeCompensation} at the saga's current version. The executor re-drives the
   *       episode under the shared {@link SagaCommandDispatch#episodeCompensationKey}, so an
   *       already-succeeded compensation dedups in the command inbox and only the failed one
   *       re-runs; a transient failure leaves the saga {@code COMPENSATING} ({@link
   *       StepOutcome#COMPENSATING_LEFT}, retried next sweep), a success terminal-CAS-writes {@code
   *       COMPENSATED}/{@code FAILED}. Because the sweep has no triggering event to quarantine
   *       (like {@link SagaTimeoutRunner}'s timeout path), the resume runs with {@code
   *       catchPoison=true}: a throwing {@code compensate()} is CAS-{@code FAULTED} inside the
   *       executor (no dead-letter entry) and returns {@link StepOutcome#FAULTED}.
   * </ul>
   *
   * <p><b>Race-safety:</b> every write here is a version-CAS at the loaded row version, and every
   * path that can drive the SAME episode — this sweeper, a concurrent {@link SagaTimeoutRunner}
   * re-pick, an event-path resume on a redelivered correlated event, and the delivery that opened
   * the episode — reads the episode version from the saga row and feeds it into the shared
   * episode-scoped key ({@code SagaStepExecutor} never computes it). So a genuine double-dispatch
   * dedups in the command inbox (no double-compensation) and exactly one terminal write wins the
   * CAS while the losers skip with an {@link OptimisticLockException} (no compensation missed
   * <em>across the concurrent re-drivers of the same episode</em>). This is a race-coordination
   * guarantee, not a completeness one: whether the compensation list itself covers a forward
   * command that executed but is not yet recorded in {@code state} is the orchestrator's
   * responsibility — see the cancel/void contract on {@link SagaOrchestrator#compensate}.
   * Package-private; the sweeper lives in this package.
   *
   * @param sagaId the stuck compensating saga to re-drive
   * @param giveUp terminalize {@code FAULTED} instead of re-driving (the sweeper's poison bound)
   * @return the executor's {@link StepOutcome} (or {@link StepOutcome#NOT_COMPENSATING}/{@link
   *     StepOutcome#FAULTED} for the fast-path give-up), for the sweeper's metric/log
   */
  StepOutcome sweepCompensation(SagaId sagaId, boolean giveUp) {
    var existing =
        sagaStore.load(
            sagaId,
            SagaType.fromClass(orchestrator.stateType()),
            orchestrator.stateType()); // INFRA load → propagates
    if (existing.isEmpty() || existing.get().status() != SagaStatus.COMPENSATING) {
      return StepOutcome.NOT_COMPENSATING;
    }
    if (giveUp) {
      faultStuckCompensation(sagaId, existing.get().version());
      return StepOutcome.FAULTED;
    }
    return executor.execute(
        sagaId,
        existing,
        new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));
  }

  /**
   * The operator's resume of a give-up FAULTED compensation episode ({@link
   * SagaDeadLetterReplayer#resumeFaulted(SagaId, boolean)}) — the episode the compensation-retry
   * sweeper ({@link #sweepCompensation} with {@code giveUp}) or the timeout runner faulted with no
   * dead-letter entry. Loads the row and hands it to the executor as a {@link
   * SagaTrigger.ResumeFaultedEpisode}: the same key-age guard a replayed resume meets, then the
   * same resume as the sweeper's ({@code catchPoison = true}, current row version, durable episode
   * keys). A row that is not FAULTED with {@code pre_fault_status = COMPENSATING} answers {@link
   * StepOutcome#NOT_COMPENSATING}. A stale episode the operator did not force propagates the
   * executor's {@link SagaStaleCompensationEpisodeException} (nothing dispatched, nothing written);
   * infrastructure failures propagate. Package-private: the replayer is its only caller.
   *
   * @param operatorForced the operator's {@code force} — passed to the executor's key-age guard as
   *     the explicit at-least-once acknowledgement, exactly as {@link #feedReplay} passes it
   */
  StepOutcome resumeFaultedEpisode(SagaId sagaId, boolean operatorForced) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    var existing =
        sagaStore.load(
            sagaId,
            SagaType.fromClass(orchestrator.stateType()),
            orchestrator.stateType()); // INFRA load → propagates
    return executor.execute(sagaId, existing, new SagaTrigger.ResumeFaultedEpisode(operatorForced));
  }

  /**
   * The operator's compensation of a saga a forward step faulted ({@link
   * SagaDeadLetterReplayer#compensateFaulted(SagaId)}), once it has no dead-letter entry left to
   * drain. Loads the row and hands it to the executor as a {@link
   * SagaTrigger.CompensateForwardFault}: a fresh claim to {@code COMPENSATING} at the FAULTED row's
   * version, exactly as the timeout runner claims an overdue saga, then the compensation of the
   * persisted state ({@code catchPoison = true}). A row that is not FAULTED by a forward step
   * answers {@link StepOutcome#NOT_COMPENSATING}; a lost claim {@link StepOutcome#CLAIM_LOST}
   * (nothing dispatched). Infrastructure failures propagate. Package-private: the replayer is its
   * only caller.
   *
   * <p>The claim has no event of its own, so it takes the saga's step lock as a claim: it asks once
   * and, while a step of the saga is in flight in this process (a replay feed, or a delivery that
   * read the row before it was faulted), answers {@link StepOutcome#CLAIM_DEFERRED} with nothing
   * read and nothing written. Otherwise the row is read and claimed under the lock, and the
   * executor releases it before the compensation is dispatched.
   */
  StepOutcome compensateForwardFault(SagaId sagaId) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    SagaStepLock.Hold claimHold = stepLock.tryEnterClaim(sagaType, sagaId, Duration.ZERO);
    if (claimHold == null) {
      return StepOutcome.CLAIM_DEFERRED;
    }
    try {
      var existing =
          sagaStore.load(
              sagaId,
              SagaType.fromClass(orchestrator.stateType()),
              orchestrator.stateType()); // INFRA load → propagates
      return executor.execute(sagaId, existing, new SagaTrigger.CompensateForwardFault(claimHold));
    } finally {
      claimHold.close(); // every exit on which the executor did not reach its claim
    }
  }

  /**
   * {@code markFaulted}-writes a stuck {@code COMPENSATING} saga to {@link SagaStatus#FAULTED} at
   * {@code version} (halted → excluded from future timeout/compensation sweeps) — no state write;
   * records {@code pre_fault_status}. The fault metric is recorded only when {@code markFaulted}
   * reports the transition (mirrors {@link #faultExistingRow}). An {@link OptimisticLockException}
   * means a concurrent resumer terminalized the same episode first — the saga is already halted, so
   * the conflict is recorded and swallowed rather than propagated.
   */
  private void faultStuckCompensation(SagaId sagaId, long version) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    try {
      if (sagaStore.markFaulted(sagaId, type, version)) {
        recordFaultedMetric();
      }
    } catch (OptimisticLockException _) {
      recordCasConflictMetric();
      LOG.debug(
          "Saga {} advanced concurrently; skipping sweep FAULTED write",
          LogSanitizer.sanitizeForLog(sagaId.value()));
    }
  }

  /**
   * Quarantines the triggering event by metadata only. The event payload is deliberately NOT
   * serialized or copied here — {@code event_stream} remains the framework's single
   * crypto-governed, shreddable source of truth for event payloads. To inspect the offending event,
   * read the event store at {@link EventEnvelope#globalOffset()}. {@code faultedAt} is the
   * builder's clock, never {@code Instant.now()}: the replayer tests advance a {@code MutableClock}
   * to prove a still-poison feed refreshed the entry. {@code publish} is an upsert on (saga id,
   * saga type, offset) — a crash rerun and a still-poison replay converge on ONE record per event
   * and type, refreshed with the newest real failure, with the stored first-fault instant, replay
   * anchor and target preserved. A hold does not come through here: {@link #recordHold} never
   * replaces an entry.
   *
   * <p><b>The saga type is ALWAYS stamped — even when {@code sagaId} is null.</b> The quarantining
   * runner's type is well-defined regardless of whether the saga id could be resolved (it is the
   * type whose router poisoned), and every entry requires it. Two types poisoning on the same event
   * at the same offset therefore produce two NULL-SAGA entries, one per type, and {@code
   * findEntry}/{@code discard}/{@code establishFirstReplayAnchor} isolate them: one type's
   * replay-then-discard can never match — and delete — the other type's only record that the event
   * was never consumed. A NAMED entry is keyed by saga id, saga type and offset, so two types'
   * named entries at one saga id and offset are two rows, and neither type's write touches the
   * other's.
   */
  private void quarantine(SagaId sagaId, EventEnvelope event, Throwable cause) {
    var entry = entryOf(sagaId, event, cause);
    if (sagaId != null) {
      // The poison's entry and the owner's shield are ONE durable write at the store, and no
      // shield is written before it: a crash before this write leaves nothing (the redelivery runs
      // the event again and meets the poison again), a crash after it leaves the entry and the
      // shield together (the redelivery is held behind its own entry and completes the fault
      // write). A shield written on its own first would leave a shield without an entry after a
      // crash, and the redelivery would be held behind a backlog that does not exist instead of
      // running again. An entry recorded with its shield is never left unshielded either, which
      // a drain's conditional clear relies on. Absent row: the entry is recorded and the shield
      // write is the documented no-op.
      sagaDeadLetterStore.publishShielded(entry);
    } else {
      sagaDeadLetterStore.publish(entry);
    }
    recordMetric(() -> metrics.recordSagaQuarantined(sagaTypeTag()), "recordSagaQuarantined");
  }

  /**
   * The error types of the entries a hold writes — the class names of the two hold causes. They are
   * persisted and documented values: an entry with any other error type was written by the poison
   * handler ({@link #isUnfinishedPoison}), and {@link #recordHold} refuses a cause outside this
   * set, so a hold cause added later is classified as a hold only once it is listed here.
   */
  static final Set<String> HOLD_ERROR_TYPES =
      Set.of(
          SagaEventHeldException.class.getName(), SagaSkippedWhileFaultedException.class.getName());

  /**
   * Records a held event ({@link SagaDeadLetterStore#publishShieldedIfAbsent}): its entry and the
   * owner's shield as one write, unless the saga has applied the event by the time the store
   * decides (nothing is written: a drain of another process applied it after this delivery read the
   * row), or already has an entry of this runner's type at the event's offset — then that entry is
   * kept exactly as stored. An entry already there records this very event: an earlier hold of it,
   * or its own poison, whose exception class name and message are the only durable record of the
   * cause and must not be replaced by the hold's. A kept entry or an applied event is a redelivery,
   * not a new hold: it is logged at DEBUG and counted in no metric.
   */
  private SagaDeadLetterStore.HoldResult recordHold(
      SagaId sagaId, EventEnvelope event, RuntimeException holdCause) {
    requireHoldCause(holdCause);
    SagaDeadLetterStore.HoldResult held =
        sagaDeadLetterStore.publishShieldedIfAbsent(entryOf(sagaId, event, holdCause));
    switch (held) {
      case SagaDeadLetterStore.HoldResult.Written _ ->
          recordMetric(() -> metrics.recordSagaQuarantined(sagaTypeTag()), "recordSagaQuarantined");
      case SagaDeadLetterStore.HoldResult.Kept _ ->
          LOG.debug(
              "Correlated event at offset {} for saga {} is already recorded in the saga"
                  + " dead-letter store; the redelivery keeps that entry and changes nothing",
              event.globalOffset().value(),
              LogSanitizer.sanitizeForLog(sagaId.value()));
      case SagaDeadLetterStore.HoldResult.AlreadyApplied applied ->
          LOG.debug(
              "Correlated event at offset {} for saga {} is applied (last applied offset {});"
                  + " the redelivery records nothing",
              event.globalOffset().value(),
              LogSanitizer.sanitizeForLog(sagaId.value()),
              applied.lastAppliedOffset());
    }
    return held;
  }

  /**
   * Refuses a hold cause whose class is not one of {@link #HOLD_ERROR_TYPES}: its entries would be
   * taken for a poison's.
   *
   * @throws IllegalArgumentException if {@code holdCause}'s class is not a hold cause
   */
  static void requireHoldCause(Throwable holdCause) {
    if (!HOLD_ERROR_TYPES.contains(holdCause.getClass().getName())) {
      throw new IllegalArgumentException(
          holdCause.getClass().getName() + " is not a hold cause: " + HOLD_ERROR_TYPES);
    }
  }

  /** The dead-letter entry for {@code event} with {@code cause}, stamped by the builder's clock. */
  private SagaDeadLetterStore.SagaDeadLetterEntry entryOf(
      SagaId sagaId, EventEnvelope event, Throwable cause) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        sagaId,
        SagaType.fromClass(orchestrator.stateType()),
        event.globalOffset(),
        event.eventType(),
        cause.getClass().getName(),
        // Persisted free text, sanitized at this sink. A pure function of the cause, so a
        // re-quarantine of the same event (rerun, redelivery) writes the same text.
        LogSanitizer.sanitizeFreeText(cause.getMessage()),
        clock.instant());
  }

  /**
   * Does this saga own a dead-letter record of THIS runner's type? Read only on the ABSENT branch
   * of {@link #processCorrelatedEvent}, for the row-less residual: {@link #recordPoison} publishes
   * the entry BEFORE {@code createFaulted}, and skips the create only when {@code initialState}
   * itself fails, so a row-less saga that owns an entry is exactly one whose start could not
   * produce an initial state — its correlated events are held ({@code BEFORE_START}), not dropped.
   * A saga that genuinely never started owns none and keeps the silent drop. Type-scoped because
   * SagaIds are business ids shared across saga types — another type's fault under the same id must
   * not turn this type's legitimate never-started drop into a hold — the same ownership rule the
   * replayer applies.
   */
  private boolean hasOwnDeadLetterRecord(SagaId sagaId) {
    SagaType type = SagaType.fromClass(orchestrator.stateType());
    for (SagaDeadLetterStore.SagaDeadLetterEntry entry : sagaDeadLetterStore.findBySaga(sagaId)) {
      if (type.equals(entry.sagaType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Type-scoped load for a live delivery or a replay feed.
   *
   * <p>{@code doLoad} has the same laundering — and the same wedge — the write paths had: a state
   * whose stored JSON can never be bound back (a renamed field, a removed enum constant, a {@code
   * CryptoShreddingModule} refusal) fails identically on every retry, so propagating it as
   * infrastructure stops the batch, and with it every saga of every type on the subscription,
   * forever. A DETERMINISTIC read failure therefore becomes poison: the triggering event is
   * quarantined and the saga is shielded (the fault write is skipped — the row cannot be read), so
   * the damage is bounded to the one broken saga and the operator gets a durable, replayable
   * record. A transient one (a key-store outage, a JDBC blip inside {@code PostgresCryptoEngine})
   * still propagates so the batch retries.
   */
  private Optional<LoadedSaga<S>> loadForDelivery(SagaId sagaId) {
    try {
      return sagaStore.load(
          sagaId, SagaType.fromClass(orchestrator.stateType()), orchestrator.stateType());
    } catch (SagaStateSerializationException conversionFailed) {
      throw SagaStateConversion.classify(sagaId, conversionFailed);
    }
  }

  /**
   * The fault write was skipped because the saga's state cannot be converted, deterministically.
   * Reachable from the unreadable-row load paths — {@link #recordPoison}'s load, {@link
   * #faultExistingRow}'s reload and {@link #createFaultedRow}'s post-conflict reload, where the row
   * cannot be read back (the write itself, {@code markFaulted}, carries no state) — and from {@link
   * #createFaultedRow} when {@code initialState} cannot be written. The poison event is already
   * durably recorded and the shield is set, so this is loud-but-non-fatal — the alternative is
   * re-wedging the subscription on a conversion that can never succeed.
   */
  private void logFaultWriteSkipped(SagaId sagaId, Throwable conversionFailed) {
    LOG.warn(
        "Saga {} state cannot be converted to or from its stored form, so its row was not marked"
            + " FAULTED — the poison event IS recorded in the saga dead-letter store and the"
            + " dead-letter shield is set. Fix the saga state type (or the crypto configuration it"
            + " depends on), then replay the entry; the row is skipped rather than retried so the"
            + " subscription is not wedged.",
        LogSanitizer.sanitizeForLog(sagaId.value()),
        conversionFailed);
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

  public static <S extends SagaState> Builder<S> builder() {
    return new Builder<>();
  }

  /** Builder for {@link SagaRunner}. */
  public static final class Builder<S extends SagaState> {
    private SagaOrchestrator<S> orchestrator;
    private SagaStore sagaStore;
    private CommandBus commandBus;
    private SagaDecider<S> decider;
    private Predicate<EventEnvelope> startPredicate;
    private Function<EventEnvelope, SagaId> idExtractor;
    private BiFunction<StreamId, DomainEvent, Optional<SagaId>> eventDataRouter =
        (streamId, event) -> Optional.empty();
    private SagaDeadLetterStore sagaDeadLetterStore;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private Duration inboxRetentionMaxAge;
    private Clock clock = Clock.systemUTC();
    private SagaStepLock stepLock;
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private StreamRuneTracing tracing = StreamRuneTracing.NOOP;

    /** {@code null}: not named, which {@link #build()} treats as on. */
    private Boolean forwardFence;

    /** {@code null}: not named. */
    private Integer parallelism;

    /** {@code null}: not named, the saga store's classifier is used. */
    private ConnectionFailureClassifier connectionFailureClassifier;

    /** {@code null}: the page's virtual worker threads; a test seam. */
    private KeyedPageExecutor.WorkerThreads workerThreads;

    public Builder<S> orchestrator(SagaOrchestrator<S> orchestrator) {
      this.orchestrator = orchestrator;
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

    public Builder<S> decider(SagaDecider<S> decider) {
      this.decider = decider;
      return this;
    }

    public Builder<S> startWhen(Predicate<EventEnvelope> startPredicate) {
      this.startPredicate = startPredicate;
      return this;
    }

    public Builder<S> extractSagaId(Function<EventEnvelope, SagaId> idExtractor) {
      this.idExtractor = idExtractor;
      return this;
    }

    /**
     * Routes the events this saga type did not cause, when the runner is built from a {@link
     * #decider}: the builder form of {@link SagaOrchestrator#correlateByEventData}. Optional; left
     * unset, an event without an origin of this saga type belongs to no saga of this type.
     *
     * <p>The function receives the stream id and the payload of a non-start event that carries no
     * saga origin of this runner's saga type, and returns the saga the event's own data names, or
     * {@code Optional.empty()}. It does not receive the event's metadata. The replies to the saga's
     * own commands are not handed to it: the runner routes them by their origin.
     *
     * <p>The payload is its writer's data: for data that cannot name a saga the function returns
     * {@code Optional.empty()} and does not throw — a throw quarantines the event as a null-saga
     * dead-letter entry (see {@link SagaOrchestrator#correlateByEventData} for the check).
     *
     * <p>Ignored when the runner is built from an {@link #orchestrator}, which routes through its
     * own {@link SagaOrchestrator#correlateByEventData}.
     *
     * @param eventDataRouter the routing function; must not be {@code null} and must not return
     *     {@code null}
     */
    public Builder<S> correlateByEventData(
        BiFunction<StreamId, DomainEvent, Optional<SagaId>> eventDataRouter) {
      this.eventDataRouter = Objects.requireNonNull(eventDataRouter, "eventDataRouter is required");
      return this;
    }

    public Builder<S> sagaDeadLetterStore(SagaDeadLetterStore sagaDeadLetterStore) {
      this.sagaDeadLetterStore = sagaDeadLetterStore;
      return this;
    }

    /**
     * Sets the metrics collector. Optional; defaults to {@link StreamRuneMetrics#NOOP}. Records
     * saga quarantines, faults, compensation outcomes, and CAS conflicts.
     */
    public Builder<S> metrics(StreamRuneMetrics metrics) {
      this.metrics = Objects.requireNonNull(metrics, "metrics is required");
      return this;
    }

    /**
     * The tracing the runner starts its spans through when its {@link TracingArea#SAGA} area is on:
     * {@code saga/{SagaType}/step} for each event it delivers to a saga, linked to the span that
     * wrote the event and carrying the step's outcome, and {@code saga/{SagaType}/compensate} for
     * each compensation command it dispatches. The commands a step dispatches run on the thread of
     * the step, so their spans nest under the step's; the steps of a page that run in parallel
     * carry the delivering thread's trace context, so their spans are children of the page's.
     *
     * <p>The trace context reaches a step on a worker thread through this tracing only ({@link
     * StreamRuneTracing#propagating}). A runner without one carries the request context and the
     * dispatch cause to its workers but no trace context: the spans the commands of a parallel step
     * start through the command bus's own tracing are then roots, not children of the
     * subscription's delivery span. A page of one saga, or a parallelism of 1, runs on the
     * delivering thread, where the delivery's trace context is current either way.
     *
     * <p>Optional; defaults to {@link StreamRuneTracing#NOOP}, which traces nothing.
     *
     * @param tracing the tracing; not {@code null}
     * @return this builder
     */
    public Builder<S> tracing(StreamRuneTracing tracing) {
      this.tracing = Objects.requireNonNull(tracing, "tracing is required");
      return this;
    }

    /**
     * The command-inbox retention window (the same value as {@code
     * streamrune.inbox.retention-max-age}, which already configures {@link
     * SagaCompensationRetrySweeper}, {@link SagaTimeoutRunner} and {@link SagaDeadLetterReplayer}).
     *
     * <p><b>Optional — the Spring/Quarkus/Micronaut auto-configurations supply this property as the
     * default</b> at startup, on every discovered {@code SagaRunner} bean ({@link
     * SagaEventPathRetention}). Set it here only to override that, e.g. to leave this runner's
     * guard off (zero or negative) or to bound it more tightly than the inbox sweeper's own window.
     * An explicit value is never overwritten. Outside those integrations — a hand-wired runner —
     * this builder method is the only way to arm the guard, and leaving it unset keeps the
     * unguarded behaviour.
     *
     * <p>Zero or negative means the {@code InboxRetentionSweeper} is disabled and dedup keys are
     * never pruned, so the guard is inert — the same convention the three out-of-band re-drivers
     * apply.
     *
     * <p>The event path RESUMES a claimed {@code COMPENSATING} episode on a redelivered or late
     * correlated (or start) event, re-dispatching under the episode-scoped {@link
     * SagaCommandDispatch#episodeCompensationKey}. That is effectively-once only while the
     * episode's command-inbox rows still exist — and the inbox never refreshes {@code processed_at}
     * on a dedup hit, so once retention prunes them the resume MISSES the inbox and re-executes an
     * already-succeeded compensation (a double refund). Setting this makes the event path refuse
     * such a resume — quarantining the triggering event and marking the saga {@code FAULTED}
     * without dispatching anything — exactly as the three out-of-band re-drivers already do on the
     * same durable {@code episode_claimed_at} anchor. The event path is the ONLY re-driver with no
     * leadership gate, so it runs on every replica and is the likeliest of the four to fire — which
     * is exactly why the auto-configurations now arm it from the same property rather than leaving
     * it to each application.
     */
    public Builder<S> inboxRetentionMaxAge(Duration inboxRetentionMaxAge) {
      this.inboxRetentionMaxAge = inboxRetentionMaxAge;
      return this;
    }

    /** Clock for the {@link #inboxRetentionMaxAge} key-age bound. Defaults to system UTC. */
    public Builder<S> clock(Clock clock) {
      this.clock = Objects.requireNonNull(clock, "clock is required");
      return this;
    }

    /**
     * The step lock of this runner's sagas. Optional: a runner creates one, and everything built
     * from the runner uses it — the compensation-retry sweeper, the dead-letter replayer, and a
     * {@link SagaTimeoutRunner} built with {@link SagaTimeoutRunner.Builder#sagaRunner}. That is
     * the wiring to prefer, because it cannot end with two lock instances for one saga type.
     *
     * <p>Pass an instance only to arrange the sharing yourself, with the same instance on {@link
     * SagaTimeoutRunner.Builder#stepLock}. The lock works between the pieces that hold the same
     * instance and between no others, and within one process only ({@link SagaStepLock}).
     */
    public Builder<S> stepLock(SagaStepLock stepLock) {
      this.stepLock = Objects.requireNonNull(stepLock, "stepLock is required");
      return this;
    }

    /**
     * The single-active-consumer leadership this saga type is delivered under: pass the
     * application's {@link SubscriptionLeadership} bean, and build the saga's subscription with
     * {@code sagaRunner(runner)} ({@code PollingEventSubscription}, {@code
     * HybridEventSubscription}, {@code PostgresNotificationSubscription}). One replica then
     * delivers the saga type, and the {@link SagaTimeoutRunner} and the {@link
     * SagaCompensationRetrySweeper} built from this runner act on that replica only. With the
     * default {@link SubscriptionLeadership#NOOP} the runner is ungated: every replica that runs
     * the subscription delivers — correct too: the saga row's compare-and-swap, the command inbox
     * and the forward fence carry correctness either way.
     *
     * @param leadership the leadership; not {@code null}
     * @return this builder
     */
    public Builder<S> leadership(SubscriptionLeadership leadership) {
      this.leadership = Objects.requireNonNull(leadership, "leadership is required");
      return this;
    }

    /**
     * Whether this runner fences its forward commands at their commit. On unless switched off here.
     *
     * <p><b>What the fence does.</b> Each forward command is dispatched with the saga named as its
     * {@code StreamRuneContext.SagaForward}, and the event store reads the saga row in the
     * transaction that would store the command's events: once the row carries a compensation
     * episode, is terminal or is gone, the store refuses the command ({@code
     * SagaForwardFencedException}) and stores nothing — no event, no inbox key. Every write that
     * opens an episode, ends the saga or deletes its row waits for an append in flight, so each
     * forward command is stored wholly before such a write or not at all, on any number of
     * processes. A refused step goes on from the row: it resumes the open episode, or stops at a
     * saga that has ended. So no forward command of a saga is stored after its compensation was
     * computed, whatever runs the claim — a timeout runner of another process, a delivery of
     * another process whose own command was rejected, an operator — and however long the step
     * takes.
     *
     * <p><b>What it needs.</b> The command bus's event store must read the rows this runner's
     * {@link SagaStore} keeps: {@link CommandBus#sagaFenceStorageIdentity()} must equal {@link
     * SagaStore#storageIdentity()}. With PostgreSQL, build the {@code PostgresEventStore} and the
     * {@code PostgresSagaStore} over the same {@code DataSource} object; in tests, wire the {@code
     * InMemorySagaStore} into the {@code InMemoryEventStore} with {@code withSagaStore}. {@link
     * #build()} refuses a runner whose bus cannot fence against its rows.
     *
     * <p><b>What it costs.</b> One lock and one read of the saga row per forward command, inside
     * the command's append; no write. Compensation commands are not fenced.
     *
     * <p><b>Who switches it off.</b> A runner whose saga rows live in another database than the
     * event stream, or whose bus cannot fence (a custom event store). Without the fence, a forward
     * command a step is still dispatching can be stored after a compensation another process
     * claimed, and nothing undoes it: the target aggregates have to refuse such a command
     * themselves.
     *
     * <p>Named here, the value is final: a default an integration applies ({@link
     * SagaForwardFenceDefault}, from the property {@code streamrune.saga.forward-fence}) does not
     * change it. A runner that names nothing is fenced from its build and takes that default at
     * startup.
     */
    public Builder<S> forwardFence(boolean enabled) {
      this.forwardFence = enabled;
      return this;
    }

    /**
     * How many steps of this saga type run at once: the sagas of a page are stepped in parallel, up
     * to this many at a time, and the events of one saga always one after the other, in offset
     * order. Optional; {@value SagaRunner#DEFAULT_PARALLELISM} when not named. {@code 1} steps a
     * page one event after the other on the delivering thread, ending at its first failure; the
     * page is still routed whole before its first step, at every parallelism ({@link
     * SagaRunner#asEventListener()}).
     *
     * <p>When deployment settings are given to the runner (the integrations bind them to
     * configuration properties), the builder's value wins over the deployment's global setting and
     * loses to the setting of a subscription built with {@code sagaRunner(runner)}. It applies to
     * each such subscription: a runner bound to k subscriptions can have k pages in flight, so up
     * to k times the value steps at once.
     *
     * <p>A step in flight holds a database connection while its commands load and append, so the
     * value is bounded by the connection pool, not by the CPU count. A page is the unit that is
     * stepped in parallel: a subscription fetch size of at least four times the parallelism keeps
     * the steps busy.
     *
     * @param parallelism between 1 and {@value SagaRunner#MAX_PARALLELISM}
     * @return this builder
     * @throws IllegalArgumentException outside that range
     */
    public Builder<S> parallelism(int parallelism) {
      this.parallelism = requireParallelismInRange("parallelism", parallelism);
      return this;
    }

    /**
     * Replaces the saga store's classifier, for an event store on another backend than the saga
     * store. The classifier decides which failure of a step says the database, or the connection to
     * it, is gone: after such a failure the page starts no further step, while any other failure
     * ends only its own saga's part of the page. Optional; the runner reads {@link
     * SagaStore#connectionFailureClassifier()} on its first use otherwise. Several steps of a page
     * call it at once: it must be thread-safe.
     *
     * @param classifier the classifier; not {@code null}
     * @return this builder
     */
    public Builder<S> connectionFailureClassifier(ConnectionFailureClassifier classifier) {
      this.connectionFailureClassifier =
          Objects.requireNonNull(classifier, "connectionFailureClassifier is required");
      return this;
    }

    /** Test seam: how the page executor creates its worker threads. */
    Builder<S> workerThreads(KeyedPageExecutor.WorkerThreads workerThreads) {
      this.workerThreads = Objects.requireNonNull(workerThreads, "workerThreads is required");
      return this;
    }

    public SagaRunner<S> build() {
      if (orchestrator == null && decider != null) {
        Objects.requireNonNull(startPredicate, "startWhen is required when using decider");
        Objects.requireNonNull(idExtractor, "extractSagaId is required when using decider");
        orchestrator =
            new SagaDeciderAdapter<>(decider, startPredicate, idExtractor, eventDataRouter);
      }
      if (orchestrator == null) {
        throw new IllegalStateException("orchestrator is required");
      }
      // Refuse a state class whose fully-qualified name the saga_type column cannot
      // hold at wiring, not on the first delivered event (where every batch would fail again).
      // The SagaDeadLetterReplayer takes a built runner, so it inherits this check.
      SagaType.fromClass(orchestrator.stateType());
      if (sagaStore == null) {
        throw new IllegalStateException("sagaStore is required");
      }
      if (commandBus == null) {
        throw new IllegalStateException("commandBus is required");
      }
      // Every saga command dispatch (forward AND compensation) goes through the keyed
      // execute(command, key) with no unkeyed fallback (SagaCommandDispatch.dispatch). A
      // bus without a CommandInbox throws IllegalStateException on the first dispatch: the forward
      // classifier cannot prove that retriable, so the saga claims a compensation episode whose own
      // dispatch fails the same way and is classified TRANSIENT there — the saga wedges
      // COMPENSATING until the sweeper's give-up FAULTs it. The misconfiguration is fully
      // detectable here, so fail fast rather than silently at runtime.
      requireIdempotentDispatch(commandBus);
      if (!Boolean.FALSE.equals(forwardFence)) {
        requireAFencingBus(SagaType.fromClass(orchestrator.stateType()).value());
      }
      Objects.requireNonNull(sagaDeadLetterStore, "sagaDeadLetterStore is required");
      return new SagaRunner<>(this);
    }

    /**
     * Refuses a bus whose {@link CommandBus#supportsIdempotentExecution()} is {@code false}: every
     * saga command goes through the keyed {@code execute(command, key)}. {@link SagaTimeoutRunner}
     * makes the same check. The message names the bus's class and the decorator case, where the
     * inbox is configured on the wrapped bus and the decorator leaves the probe to its default.
     */
    static void requireIdempotentDispatch(CommandBus commandBus) {
      if (!commandBus.supportsIdempotentExecution()) {
        throw new IllegalStateException(
            "saga command dispatch requires idempotent execution, and "
                + commandBus.getClass().getName()
                + " reports none (CommandBus.supportsIdempotentExecution() is false). Configure a"
                + " CommandInbox on the CommandBus (VirtualThreadCommandBus.builder()"
                + ".commandInbox(...)). A decorator in front of such a bus forwards"
                + " supportsIdempotentExecution() and sagaFenceStorageIdentity() to it, as"
                + " ForwardingCommandBus of streamrune-test does.");
      }
    }

    /**
     * Refuses a bus that cannot fence this runner's forward commands against the rows its saga
     * store keeps ({@link #forwardFence(boolean)}). The check is made here, before an integration
     * can apply its default: a runner over such a bus switches the fence off itself.
     */
    private void requireAFencingBus(String type) {
      Object fence = commandBus.sagaFenceStorageIdentity();
      if (fence == null) {
        throw new IllegalStateException(
            "The SagaRunner for '"
                + type
                + "' fences its forward commands at their commit, and its command bus cannot: "
                + commandBus.getClass().getName()
                + " reports no saga rows to fence against (CommandBus.sagaFenceStorageIdentity()"
                + " is null). Without the fence a forward command of a saga can be stored after a"
                + " compensation that another process claimed, and nothing undoes it. Build the"
                + " bus over an event store that fences (PostgresEventStore; in tests"
                + " InMemoryEventStore.withSagaStore(theSagaStore)), or switch the fence off for"
                + " this runner with SagaRunner.Builder.forwardFence(false). A decorator in front"
                + " of a bus that fences forwards sagaFenceStorageIdentity() to it, as"
                + " ForwardingCommandBus of streamrune-test does.");
      }
      if (!fence.equals(sagaStore.storageIdentity())) {
        throw new IllegalStateException(
            "The SagaRunner for '"
                + type
                + "' fences its forward commands at their commit, and its command bus fences"
                + " against other saga rows than its SagaStore keeps"
                + " (CommandBus.sagaFenceStorageIdentity() does not equal"
                + " SagaStore.storageIdentity()). The saga row is read in the transaction of the"
                + " forward command's append: keep the saga rows in the database of the event"
                + " stream and build the PostgresSagaStore and the PostgresEventStore over the"
                + " same DataSource object (in tests InMemoryEventStore.withSagaStore("
                + "theSagaStore)), or switch the fence off for this runner with"
                + " SagaRunner.Builder.forwardFence(false).");
      }
    }
  }
}

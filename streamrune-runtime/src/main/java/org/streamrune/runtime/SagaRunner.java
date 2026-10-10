package org.streamrune.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * Bridges a {@link SagaOrchestrator} to an {@link EventListener}.
 *
 * <p>Call {@link #asEventListener()} to obtain an {@link EventListener} that can be registered with
 * any {@link org.streamrune.core.subscription.EventSubscription}. For each event received, the
 * runner routes exactly as {@link #resolveTarget} does — a start event ({@link
 * SagaOrchestrator#isStartEvent}) by {@link SagaOrchestrator#extractSagaId}, anything else by
 * {@link SagaOrchestrator#correlate} — and then:
 *
 * <ul>
 *   <li><b>Start event</b> → {@link SagaStepExecutor} with a live {@code ForwardStep(startPath =
 *       true)}: create-first genesis ({@link SagaStore#createGenesisPending}), dispatch, then
 *       {@link SagaStore#applyEvent}. A redelivery over an applied genesis is dedup-skipped; over a
 *       genesis-pending row it re-drives from {@code initialState} and the committed indices dedup
 *       on their offset-stable forward keys.
 *   <li><b>Correlated event</b> → the row is loaded and the event is either applied by the executor
 *       or <em>held</em>: recorded in the {@link SagaDeadLetterStore} under the saga's id, with the
 *       durable {@code dead_letter_pending} shield set on the row first — never processed and never
 *       dropped. Which, by the row's recovery state:
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
 *             entries that must drain first.
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
 * pure-logic methods ({@code isStartEvent}, {@code correlate}, {@code extractSagaId}, {@code
 * initialState}, {@code evolve}, {@code handle}, {@code compensate}) and their null returns are
 * poison: {@link #recordPoison} quarantines the offending event in the {@link SagaDeadLetterStore}
 * <em>by metadata only</em> (saga id/type, event offset/type, error details, fault time — the event
 * payload is never serialized or copied, so GDPR crypto-shredding stays authoritative in the event
 * store; inspect the event by reading {@code event_stream} at the entry's {@code eventOffset}) and
 * faults the saga, in one fixed order on every event path: row present → shield → entry → {@link
 * SagaStore#markFaulted} (the only fault write; no state write, it records {@code
 * pre_fault_status}); row absent and {@code initialState} succeeds → entry → {@link
 * SagaStore#createFaulted} (so no FAULTED row can ever exist without its entry); row absent and
 * {@code initialState} throws → entry only (the documented row-less residual). A fault write that
 * conflicts is retried once on a reload and otherwise left with a WARNING — the entry and the
 * shield ARE recorded, so live events are held and the next drain re-evaluates the saga. Processing
 * continues with the next event in the batch — no head-of-line blocking. {@link
 * SagaDeadLetterStore} is a <em>required</em> {@link Builder} field; {@link Builder#build()} throws
 * {@link NullPointerException} when it is absent. Infrastructure failures ({@link SagaStore}
 * load/create/update/CAS) are NOT caught; they propagate out of {@code onEvents} so the
 * subscription can retry — every write here is a single durable statement, and every crash point
 * between two of them converges on the rerun.
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
 * previously recorded result without re-running the handler or re-persisting events.
 *
 * <p><b>Concurrency:</b> Saga writes are CAS-protected ({@link SagaStore#createGenesisPending},
 * {@link SagaStore#applyEvent}, {@link SagaStore#claimCompensating}, {@link SagaStore#update},
 * {@link SagaStore#markFaulted}) — concurrent writers cannot silently lose updates. Start-event
 * deduplication is atomic: if two listeners race on the same start event, one wins the create and
 * the other reloads and routes on the recorded facts. A conflict on a correlated event propagates
 * out of {@code onEvents} like an infrastructure failure; the subscription retries the batch on
 * fresh state, the recorded {@code last_applied_offset} dedups the state advance, and any command
 * already dispatched in the losing attempt is deduplicated by the command inbox. That dedup holds
 * even when the duplicate dispatch overlaps the original: {@link VirtualThreadCommandBus} looks the
 * key up again under the aggregate lock and once more when a keyed decider rejects, so a
 * lock-serialized duplicate of a succeeded step is answered with the recorded outcome, never
 * classified as a business rejection and compensated. As a result, concurrent listeners are safe —
 * no leadership gate is required for correctness, and neither {@code PollingEventSubscription} nor
 * {@code HybridEventSubscription} has one — but a single subscription per saga type remains the
 * recommended and most efficient topology. A throwing {@code compensate()} is treated as a poison
 * event: the triggering event is quarantined and the saga is marked {@code FAULTED}. The event path
 * does not advance a claimed compensation: once a saga is {@code COMPENSATING} (claimed by {@code
 * SagaTimeoutRunner} or by a prior event-path delivery), correlated events never re-run forward
 * {@code evolve}/{@code handle}. They do, however, <em>resume</em> the compensation episode when
 * its owner crashed before the terminal write — re-driving compensation at the existing version
 * under the shared episode key, so a saga with no timeout runner still reaches a terminal status;
 * the resume is driven by {@link SagaStepExecutor} (routed from {@link #processCorrelatedEvent})
 * and, out of band, by {@link #sweepCompensation}. See {@link SagaStatus#COMPENSATING}.
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

  private final SagaOrchestrator<S> orchestrator;
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
   * Whether the application named {@link Builder#inboxRetentionMaxAge(Duration)} itself. When it
   * did, its value wins — including a deliberate zero/negative "disable this guard" — and {@link
   * SagaEventPathRetention#applyDefault} leaves this runner alone.
   */
  private final boolean inboxRetentionMaxAgeConfigured;

  private SagaRunner(Builder<S> builder) {
    this.orchestrator = builder.orchestrator;
    this.sagaStore = builder.sagaStore;
    this.commandBus = builder.commandBus;
    this.sagaDeadLetterStore = builder.sagaDeadLetterStore;
    this.metrics = builder.metrics;
    this.clock = builder.clock;
    this.inboxRetentionMaxAgeConfigured = builder.inboxRetentionMaxAge != null;
    this.executor =
        new SagaStepExecutor<>(
            orchestrator,
            sagaStore,
            commandBus,
            metrics,
            builder.inboxRetentionMaxAge,
            builder.clock);
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
   * one on the builder. Package-private; driven by {@link SagaEventPathRetention#applyDefault} from
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
   * Routes an event exactly as live delivery does: start events by {@code extractSagaId},
   * correlated events by {@code correlate}. A throwing router is poison with no saga id.
   *
   * <p>A null RETURN from either router is the same deterministic contract violation as a throw and
   * is rejected INSIDE the poison supplier, so it becomes a null-saga quarantine of the one event
   * instead of an NPE that escapes the listener and wedges the subscription. {@code
   * Optional.empty()} from {@code correlate} remains the legitimate, quarantine-free "not mine".
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
    Optional<SagaId> corr =
        poison(
            null,
            () ->
                Objects.requireNonNull(
                    orchestrator.correlate(event),
                    "correlate returned null — the SagaOrchestrator contract requires a non-null"
                        + " Optional (return Optional.empty() to ignore an event)"));
    return corr.map(id -> new ReplayTarget(id, false));
  }

  /**
   * Returns an {@link EventListener} backed by this runner. The listener processes events
   * sequentially within each batch. Poison events (orchestrator logic exceptions, deterministic
   * state-conversion failures) are recorded by {@link #recordPoison} — the batch continues.
   * Infrastructure failures (store I/O, CAS conflicts) propagate so the subscription retries.
   */
  public EventListener asEventListener() {
    return events -> {
      for (EventEnvelope event : events) {
        try {
          resolveTarget(event)
              .ifPresent(
                  target -> {
                    if (target.startPath()) {
                      processStartEvent(target.sagaId(), event);
                    } else {
                      processCorrelatedEvent(target.sagaId(), event);
                    }
                  });
        } catch (SagaPoisonException poison) {
          SagaId poisonSagaId = poison.sagaId();
          recordPoison(poisonSagaId, event, poison.getCause());
          LOG.warn(
              "Saga poison event quarantined (saga="
                  + (poisonSagaId == null
                      ? "<null>"
                      : LogSanitizer.sanitizeForLog(poisonSagaId.value()))
                  + ", offset="
                  + event.globalOffset().value()
                  + ")",
              poison.getCause());
        }
        // SagaStore (load/create/CAS) failures are NOT SagaPoisonException: they propagate out of
        // onEvents, the subscription does not advance the offset, and the batch is retried.
      }
    };
  }

  /**
   * The replayer's feed: the same executor call as live delivery with {@code replayRedrive=true},
   * never the live hold rules (the replay feed IS the recovery channel). Poison is recorded by the
   * single poison handler and reported as {@link StepOutcome#FAULTED}; a retry-later failure and
   * every infrastructure failure propagate to the replayer.
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
    try {
      // a deterministic conversion poison on the load lands in the catch below
      Optional<LoadedSaga<S>> row = loadForDelivery(sagaId);
      // the entry being fed exists, so a row the feed creates is born shielded
      return executor.execute(
          sagaId, row, new SagaTrigger.ForwardStep(event, startPath, true, true, operatorForced));
    } catch (SagaPoisonException poison) {
      recordPoison(sagaId, event, poison.getCause());
      LOG.warn(
          "Replayed saga event is still poison (saga="
              + LogSanitizer.sanitizeForLog(sagaId.value())
              + ", offset="
              + event.globalOffset().value()
              + "); the entry was refreshed and the saga stays FAULTED",
          poison.getCause());
      return StepOutcome.FAULTED;
    }
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
  private void processStartEvent(SagaId sagaId, EventEnvelope event) {
    var existing = loadForDelivery(sagaId); // INFRA load → propagates
    // A row the executor creates for an ABSENT saga must be born shielded
    // when the saga already owns dead-letter entries — the row-less residual and the
    // correlated events held BEFORE_START behind it — or the next live event applies ahead of them
    // and retention prunes them. One indexed findBySaga per FRESH start (row absent), the same
    // lookup the ABSENT correlated branch already pays; a present row never pays it.
    boolean ownsRecord = existing.isEmpty() && hasOwnDeadLetterRecord(sagaId);
    executor.execute(sagaId, existing, new SagaTrigger.ForwardStep(event, true, false, ownsRecord));
  }

  /** The live hold rules; see the class javadoc for the state-by-state rationale. */
  private void processCorrelatedEvent(SagaId sagaId, EventEnvelope event) {
    var existing = loadForDelivery(sagaId); // INFRA load → propagates
    if (existing.isEmpty()) {
      // The row-less residual: only a saga whose initialState failed (threw, or
      // returned null) owns an entry without a row. A never-started saga's out-of-order event
      // keeps the silent drop.
      if (hasOwnDeadLetterRecord(sagaId)) {
        hold(sagaId, event, SagaEventHeldException.HoldReason.BEFORE_START);
      }
      return;
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
      return;
    }
    if (row.status() == SagaStatus.FAULTED) {
      holdWhileFaulted(sagaId, event);
      return;
    }
    if (row.status().isTerminal()) {
      return; // legitimately done: drop silently
    }
    if (row.status() == SagaStatus.COMPENSATING) {
      // A resume never consumes the event, so ordering cannot be violated; the executor resumes
      // the claimed episode whatever the genesis flag says.
      executor.execute(sagaId, existing, new SagaTrigger.ForwardStep(event, false, false));
      return;
    }
    if (!row.genesisApplied()) {
      hold(sagaId, event, SagaEventHeldException.HoldReason.GENESIS_PENDING);
      return;
    }
    if (row.deadLetterPending()) {
      hold(sagaId, event, SagaEventHeldException.HoldReason.BACKLOG_PENDING);
      return;
    }
    executor.execute(sagaId, existing, new SagaTrigger.ForwardStep(event, false, false));
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
   * {@code hold(reason)}: shield → entry+shield (one write) → metric → WARNING. The shield is
   * written first (it may be TRUE without an entry after a crash in between, never the reverse) and
   * written AGAIN, atomically with the entry, by {@link #quarantine} through {@link
   * SagaDeadLetterStore#publishShielded} (a drain's conditional clear that observed an empty
   * backlog between the two must not leave the entry unshielded, and no crash may separate the
   * entry from its shield); on an absent row ({@code BEFORE_START}) both shield writes are the
   * stores' documented no-op. {@code publish} is an upsert on (saga, offset), so a redelivery storm
   * converges on one record.
   */
  private void hold(SagaId sagaId, EventEnvelope event, SagaEventHeldException.HoldReason reason) {
    sagaStore.setDeadLetterPending(sagaId, SagaType.fromClass(orchestrator.stateType()), true);
    quarantine(sagaId, event, new SagaEventHeldException(sagaId, event.globalOffset(), reason));
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

  /**
   * The FAULTED hold keeps its own exception type and its own metric series (on top of the
   * quarantine counter the entry earns): folded into {@code streamrune.saga.quarantined} alone, an
   * already-known fault reads as a stream of NEW orchestrator defects, so {@code quarantined -
   * skipped_while_faulted - event_held} stays the genuine-poison count (apart from event-path
   * {@code SagaStaleCompensationEpisodeException} refusals — see {@code
   * MetricNames#SAGA_SKIPPED_WHILE_FAULTED}). The saga row is NOT faulted again (it already is),
   * only shielded — the row's own recorded {@link LoadedSaga#preFaultStatus()} is what the executor
   * resumes from on replay; the entry carries no transitional copy.
   */
  private void holdWhileFaulted(SagaId sagaId, EventEnvelope event) {
    sagaStore.setDeadLetterPending(sagaId, SagaType.fromClass(orchestrator.stateType()), true);
    quarantine(sagaId, event, new SagaSkippedWhileFaultedException(sagaId, event.globalOffset()));
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

  /**
   * The single poison handler: row present → shield, entry, {@code markFaulted}; row absent and
   * {@code initialState} succeeds → entry, {@code createFaulted}; row absent and {@code
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
    sagaStore.setDeadLetterPending(sagaId, type, true);
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
        "orchestrator.initialState failed while creating a FAULTED row for saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " — the poison event is recorded in the dead-letter store and this saga stays"
            + " row-less until a replay of its start entry succeeds (the one row-less case)",
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
   *   <li>if it is no longer {@code COMPENSATING} — a concurrent event-path/timeout resume already
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
   * <p><b>Race-safety:</b> every write here is a version-CAS at the loaded episode version, and
   * every path that can drive the SAME episode — this sweeper, a concurrent {@link
   * SagaTimeoutRunner} re-pick, and an event-path resume on a redelivered correlated event — feeds
   * that same episode version into the shared episode-scoped key. So a genuine double-dispatch
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
   */
  StepOutcome compensateForwardFault(SagaId sagaId) {
    Objects.requireNonNull(sagaId, SAGA_ID_REQUIRED);
    var existing =
        sagaStore.load(
            sagaId,
            SagaType.fromClass(orchestrator.stateType()),
            orchestrator.stateType()); // INFRA load → propagates
    return executor.execute(sagaId, existing, new SagaTrigger.CompensateForwardFault());
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
   * to prove a still-poison feed refreshed the entry. {@code publish} is an upsert on (saga,
   * offset) — a hold, a redelivery, a crash rerun and a still-poison replay all converge on ONE
   * record per event, with the stored first-fault instant, replay anchor and target preserved.
   *
   * <p><b>The saga type is ALWAYS stamped — even when {@code sagaId} is null.</b> The quarantining
   * runner's type is well-defined regardless of whether the saga id could be resolved (it is the
   * type whose router poisoned), and every entry requires it. Two types poisoning on the same event
   * at the same offset therefore produce two entries, one per type, and {@code findEntry}/{@code
   * DISCARD_TYPED}/{@code establishFirstReplayAnchor} isolate them: one type's replay-then-discard
   * can never match — and delete — the other type's only record that the event was never consumed.
   */
  private void quarantine(SagaId sagaId, EventEnvelope event, Throwable cause) {
    var entry =
        new SagaDeadLetterStore.SagaDeadLetterEntry(
            sagaId,
            SagaType.fromClass(orchestrator.stateType()),
            event.globalOffset(),
            event.eventType(),
            cause.getClass().getName(),
            // Persisted free text, sanitized at this sink. A pure function of the cause, so
            // a re-quarantine of the same event (rerun, redelivery) writes the same text.
            LogSanitizer.sanitizeFreeText(cause.getMessage()),
            clock.instant());
    if (sagaId != null) {
      // Every named publish is preceded by a
      // shield write (T) in its caller, and the drain's conditional clear (C) may legitimately
      // land between T and this publish (it sees no entry yet). The entry and the shield are
      // therefore written as ONE durable write at the store: a separate re-assert after the
      // publish closed the race but left a crash window (entry recorded, shield FALSE) in which
      // the redelivery was applied live and the entry became a duplicate the next drain evolved
      // again. Now a crash before this write leaves T alone (shield without entry, the
      // redelivery is re-held) and a crash after it leaves entry + shield together (the
      // redelivery is held behind its own entry). Absent row: the entry is recorded and the
      // shield write is the documented no-op.
      sagaDeadLetterStore.publishShielded(entry);
    } else {
      sagaDeadLetterStore.publish(entry);
    }
    recordMetric(() -> metrics.recordSagaQuarantined(sagaTypeTag()), "recordSagaQuarantined");
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
        "Saga "
            + LogSanitizer.sanitizeForLog(sagaId.value())
            + " state cannot be converted to or from its stored form, so its row was not marked"
            + " FAULTED — the poison event IS recorded in the saga dead-letter store and the"
            + " dead-letter shield is set. Fix the saga state type (or the crypto configuration it"
            + " depends on), then replay the entry; the row is skipped rather than retried so the"
            + " subscription is not wedged.",
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
      LOG.warn("Metrics recording failed for " + metricName, e);
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
    private Function<EventEnvelope, Optional<SagaId>> correlator;
    private SagaDeadLetterStore sagaDeadLetterStore;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private Duration inboxRetentionMaxAge;
    private Clock clock = Clock.systemUTC();

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

    public Builder<S> correlateBy(Function<EventEnvelope, Optional<SagaId>> correlator) {
      this.correlator = correlator;
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
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
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

    public SagaRunner<S> build() {
      if (orchestrator == null && decider != null) {
        Objects.requireNonNull(startPredicate, "startWhen is required when using decider");
        Objects.requireNonNull(idExtractor, "extractSagaId is required when using decider");
        Objects.requireNonNull(correlator, "correlateBy is required when using decider");
        orchestrator = new SagaDeciderAdapter<>(decider, startPredicate, idExtractor, correlator);
      }
      if (orchestrator == null) throw new IllegalStateException("orchestrator is required");
      // Refuse a state class whose fully-qualified name the saga_type column cannot
      // hold at wiring, not on the first delivered event (where every batch would fail again).
      // The SagaDeadLetterReplayer takes a built runner, so it inherits this check.
      SagaType.fromClass(orchestrator.stateType());
      if (sagaStore == null) throw new IllegalStateException("sagaStore is required");
      if (commandBus == null) throw new IllegalStateException("commandBus is required");
      // Every saga command dispatch (forward AND compensation) goes through the keyed
      // execute(command, key) with no unkeyed fallback (SagaCommandDispatch.executeCorrelated). A
      // bus without a CommandInbox throws IllegalStateException on the first dispatch: the forward
      // classifier cannot prove that retriable, so the saga claims a compensation episode whose own
      // dispatch fails the same way and is classified TRANSIENT there — the saga wedges
      // COMPENSATING until the sweeper's give-up FAULTs it. The misconfiguration is fully
      // detectable here, so fail fast rather than silently at runtime.
      if (!commandBus.supportsIdempotentExecution()) {
        throw new IllegalStateException(
            "saga command dispatch requires idempotent execution — configure a CommandInbox on the"
                + " CommandBus (VirtualThreadCommandBus.builder().commandInbox(...))");
      }
      Objects.requireNonNull(sagaDeadLetterStore, "sagaDeadLetterStore is required");
      return new SagaRunner<>(this);
    }
  }
}

package org.streamrune.core.saga;

import java.util.List;
import java.util.Optional;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.StreamId;

/**
 * Orchestration-based saga coordinator.
 *
 * <p>Implement this interface to define a saga that reacts to domain events by dispatching
 * commands. The {@code SagaRunner} drives execution: it routes each event to a saga (see
 * Correlation below), then calls {@link #evolve} + {@link #handle} to advance the saga, and {@link
 * #compensate} on command failure.
 *
 * <h2>Correlation</h2>
 *
 * <p>A start event ({@link #isStartEvent}) names its saga through {@link #extractSagaId}. Any other
 * event reaches a saga in one of two ways.
 *
 * <p><b>Events the saga caused.</b> Every command a saga dispatches runs with the saga's origin
 * bound, and the command bus writes it into the metadata of each event the command produces: {@code
 * EventMetadata.sagaOrigin()}, holding the saga type, the saga id and a hop count. {@code
 * SagaRunner} routes an event whose origin names its saga type to the saga the origin names. An
 * implementation writes nothing for this. Only saga dispatch and {@code
 * StreamRuneContext.continuing(event)} set an origin; no request header, baggage entry or
 * dead-letter row can.
 *
 * <p><b>Events somebody else caused</b> — a user's own action, a webhook, a saga of another type.
 * They carry no origin of this saga type, and the runner asks {@link #correlateByEventData}, which
 * receives the stream id and the payload and nothing of the metadata.
 *
 * <p>When both could apply, the origin decides and {@link #correlateByEventData} is not called.
 * Every event a saga's command produces, and every event of a continuation of such an event, is
 * routed to the dispatching saga and to no other saga of its type — also an event about another
 * saga's entity. Such an event is one this saga's command caused, not necessarily one about its
 * entity, so {@link #evolve} and {@link #handle} take ids from the saga's state. A fact meant for
 * another saga of the same type is written by a command a listener dispatches outside {@code
 * StreamRuneContext.continuing}: its event carries no origin and is routed by its data.
 *
 * <p>A correlation id is tracing data: a client sets it with {@code X-Correlation-Id} in every
 * identity mode. Nothing routes by it, and no routing method may derive a saga id from it.
 *
 * <h2>Serialization</h2>
 *
 * <p>Saga state implementations ({@code S}) must be Jackson-serializable. Annotate constructors
 * with {@code @JsonCreator} and parameters with {@code @JsonProperty}.
 *
 * <h2>Failure contract: poison events, not retries</h2>
 *
 * <p>{@code SagaRunner} treats any exception thrown by {@link #initialState}, {@link
 * #isStartEvent}, {@link #correlateByEventData}, {@link #extractSagaId}, {@link #evolve}, {@link
 * #handle}, or {@link #compensate} as a <b>poison event</b>: the triggering event is quarantined
 * (metadata only — saga id/type, event offset/type, error details, fault time; the payload is never
 * copied out of the event store) and the saga is permanently marked {@code FAULTED}. A <b>{@code
 * null} return</b> from {@link #extractSagaId} or {@link #correlateByEventData} — both contract
 * violations; see their method docs — is classified identically (quarantined as a null-saga entry,
 * since no saga could be identified), never allowed to wedge the subscription. There is no
 * automatic retry or replay of a poison event — a faulted saga stops processing events and is
 * excluded from timeout sweeps until an operator drains it with {@code
 * SagaDeadLetterReplayer.replayAll}. (A compensation episode the compensation-retry sweeper or the
 * timeout runner gives up on is faulted too, with no dead-letter entry; {@code
 * SagaDeadLetterReplayer.resumeFaulted} resumes it. A forward fault whose entry an operator
 * discarded has nothing left to drain; {@code SagaDeadLetterReplayer.compensateFaulted} compensates
 * it.) Consequently, implementations must treat transient errors (a flaky downstream call made from
 * inside {@code evolve}/{@code handle}, for instance) as retryable <em>inside</em> the method — or
 * design them out entirely — because letting a transient exception escape faults the saga
 * irrecoverably rather than triggering a retry.
 *
 * <p>This is distinct from infrastructure failures ({@code SagaStore} I/O), which are not poison:
 * those propagate out of the runner so the subscription can retry the batch.
 *
 * @param <S> the saga state type
 */
public interface SagaOrchestrator<S extends SagaState> {

  /** Java type of the saga state; required for deserialization from storage. */
  Class<S> stateType();

  /**
   * Creates the initial state for a new saga instance identified by {@code sagaId}.
   *
   * <p>Any exception thrown here is treated as a poison event: see the class-level failure
   * contract.
   */
  S initialState(SagaId sagaId);

  /** Returns {@code true} if this event should trigger the creation of a new saga instance. */
  boolean isStartEvent(EventEnvelope event);

  /**
   * Extracts the saga ID from a start event. Only called when {@link #isStartEvent} returns {@code
   * true}.
   *
   * <p>Derive the saga id from the event's own data — the id of the order, the payment, the stream
   * — never from its correlation id or any other metadata a request can choose. Whoever may write
   * the start event names the saga it starts, so the access rule of that event is what protects the
   * saga.
   *
   * <p><b>Do not throw on the event's data.</b> A {@link SagaId} is at most {@link
   * SagaId#MAX_LENGTH} characters and not blank, and the event's data is its writer's: an id that
   * cannot be part of a saga id must not reach {@code SagaId.of} here, where the throw is poison
   * and each such event becomes a null-saga dead-letter entry. Decline the event in {@link
   * #isStartEvent} instead, which is where "this event starts no saga" is said.
   *
   * <p><b>Must not return {@code null}.</b> There is no "ignore this start event" signal here —
   * {@link #isStartEvent} already claimed the event for this saga type. A {@code null} return (e.g.
   * a metadata lookup missing on a malformed producer event) is treated exactly like a thrown
   * exception per the class-level failure contract: the event is quarantined as a
   * <em>null-saga</em> dead-letter entry and processing continues with the next event. Fix the
   * router (or the producer) and replay the entry.
   */
  SagaId extractSagaId(EventEnvelope event);

  /**
   * Routes an event this saga type did not cause — a user's own action, a webhook, a saga of
   * another type — by the event's own data. Called only for a non-start event that carries no
   * origin of this saga type. Whoever may write that event may address the saga it names.
   *
   * <p>Return the saga id derived from {@code streamId} or from the payload, or {@link
   * Optional#empty()} — the default — when the event belongs to no saga of this type: nothing is
   * loaded and nothing is written for it. The method does not receive the event's metadata, so it
   * cannot route by a correlation id, a trace id, a user id or baggage.
   *
   * <p><b>Do not throw on the event's data.</b> The payload is its writer's data. For data that
   * cannot name a saga — an id too long to be part of a {@link SagaId} ({@link SagaId#MAX_LENGTH}
   * characters), a blank one — return {@link Optional#empty()}: a throw is poison (see the
   * class-level failure contract), so every such event a caller writes becomes a null-saga
   * dead-letter entry, a WARNING and a quarantine count, and a replay of the entry throws again.
   * Check before {@code SagaId.of}:
   *
   * <pre>{@code
   * static Optional<SagaId> sagaOf(String orderId) {
   *   if (orderId == null || orderId.isBlank()) {
   *     return Optional.empty();
   *   }
   *   String id = "order-saga-" + orderId;
   *   return id.length() > SagaId.MAX_LENGTH ? Optional.empty() : Optional.of(SagaId.of(id));
   * }
   * }</pre>
   *
   * <p>The event's own access rule is what protects the saga this method names, and it must hold
   * for every writer of the event. A command a saga of another type dispatches runs as the system
   * principal, which the authorization interceptors admit by default, so a role requirement on the
   * writing command does not hold for it; a decider guard on the command's data does. An event of
   * another cause can arrive at any step, so {@link #evolve} and {@link #handle} decide by the
   * saga's state what it means.
   *
   * <p>Not called for the reply to a command this saga type dispatched: such an event carries the
   * dispatching saga's origin and reaches that saga (see the class-level Correlation section).
   *
   * <p><b>Must not return a bare {@code null}</b> — {@link Optional#empty()} is the one and only
   * "not mine" signal. A {@code null} return is treated exactly like a thrown exception per the
   * class-level failure contract: the event is quarantined as a <em>null-saga</em> dead-letter
   * entry and processing continues with the next event.
   *
   * @param streamId the stream the event belongs to
   * @param event the event payload
   * @return the saga the event belongs to, or empty when it belongs to no saga of this type
   */
  default Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
    return Optional.empty();
  }

  /**
   * Updates saga state based on the incoming event. Return the new (immutable) state.
   *
   * <p>Must be idempotent under at-least-once event redelivery: unlike command dispatch (which is
   * deduplicated via the command inbox and a deterministic {@code IdempotencyKey}), state
   * application here is <em>not</em> deduplicated by the framework. If the same event is delivered
   * twice — a redelivered correlated event after a retried batch, for example — {@code evolve} runs
   * again on it. Prefer "set field to X" derivations over "increment/append" ones for state derived
   * from a single event, so a re-application converges to the same state instead of
   * double-counting.
   *
   * <p>Any exception thrown here is treated as a poison event: see the class-level failure
   * contract.
   */
  S evolve(S state, EventEnvelope event);

  /**
   * Decides what commands to dispatch given the current state and triggering event. Return an empty
   * list when no action is needed.
   *
   * <p><b>Not called on a terminal step.</b> When {@code evolve} returns a state whose {@code
   * status()} is terminal ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}), the runner
   * persists that status and does not call {@code handle} for the event, so a command returned for
   * it would never be dispatched. Emit a saga's last command from a non-terminal state and let a
   * later event (its outcome) evolve the saga to the terminal status.
   *
   * <p>Any exception thrown here is treated as a poison event: see the class-level failure
   * contract.
   */
  List<SagaCommand> handle(S state, EventEnvelope event);

  /**
   * Called when a dispatched {@link SagaCommand} fails. Return compensation commands to undo prior
   * steps.
   *
   * <p>The returned list directly determines the saga's terminal classification: a <b>non-empty</b>
   * list whose commands all dispatch successfully is classified {@code COMPENSATED}. Returning an
   * <b>empty</b> list is classified {@code FAILED} — not {@code COMPENSATED} — even when "there is
   * nothing to undo" is the correct domain answer (e.g. the failed step had no side effects yet).
   * If a compensation command itself throws, the individual failure is logged and swallowed (not
   * poison) and the saga is still classified {@code FAILED}.
   *
   * <p>A <em>throwing</em> {@code compensate()} method, by contrast, is treated as a poison event
   * per the class-level failure contract: the saga is quarantined and marked {@code FAULTED}
   * instead of classified {@code FAILED}, because a compensation decision that cannot even be
   * computed is a deterministic orchestrator bug, not a per-command failure to tolerate.
   *
   * <p>Note: timeout-driven compensation ({@code SagaTimeoutRunner}) is a {@link SagaDecider}
   * concern — a plain {@code SagaOrchestrator} implementation is never invoked with a {@code
   * SagaTimeoutException} here. See {@link SagaDecider#compensate} for the timeout-path contract.
   *
   * <p><b>Crash-resume ({@code failedCommand} may be null):</b> if a compensation episode's owner
   * crashes between claiming {@code COMPENSATING} and writing the terminal status, the event path
   * resumes it on the next redelivery of a correlated event and re-invokes {@code compensate} with
   * a {@code null} {@code failedCommand} (the original failure did not survive the crash).
   * Implementations <b>must</b> therefore tolerate a {@code null} {@code failedCommand} and
   * recompute the undo from {@code state} — not by dereferencing {@code failedCommand}. Because the
   * resumed dispatch reuses the same episode-scoped idempotency keys, {@code compensate} must also
   * be a deterministic function of {@code state} so the resumed command list matches
   * (already-executed commands then dedup).
   *
   * <p><b>The {@code failure} argument must be ignored entirely — {@code compensate} must be a pure
   * function of {@code state}.</b> It is diagnostic only (safe to log), never a signal to branch
   * compensation logic on. On the original (first) attempt {@code failure} is the real exception
   * thrown by {@code CommandBus.execute()}; on a crash-resumed attempt it is an internal
   * resume-marker instead. Nothing about which object shows up here is meaningful beyond "some
   * failure occurred" — branching on its type or message risks resuming into different compensation
   * behaviour than the original (crashed) attempt took for the very same episode.
   *
   * <p><b>{@code state} may EXCLUDE a forward command that already executed — use cancel/void
   * semantics, not conditional undo.</b> {@code state} reflects only what {@code evolve} has
   * applied so far; it does <em>not</em> record which forward commands this dispatch loop already
   * ran. Two concrete cases: (1) a step emitted several forward commands — {@code handle()}
   * returned {@code [ChargePayment, ReserveStock]} — and a later one failed after an earlier one
   * committed; {@code state} was captured before either ran, so it cannot tell you {@code
   * ChargePayment} succeeded. (2) A forward command committed but its confirmation event (e.g.
   * {@code PaymentCharged}) is still in flight — not yet {@code evolve}d — when a timeout or
   * crash-resume computes compensation from the persisted state. In both cases a compensation that
   * <em>conditions on state</em> ("refund only if {@code state.charged}") silently misses the undo,
   * leaving the customer charged for a cancelled order. Compensation must therefore be phrased as
   * <b>cancel/void commands that are safe whether or not the step actually ran</b> — a {@code
   * CancelPayment}/{@code CancelPaymentIntent} the target aggregate handles idempotently (refund if
   * charged, no-op if not) — rather than a conditional undo derived from state completeness. The
   * only "no missed compensation" guarantee the framework makes is <em>for steps recorded in {@code
   * state}</em>; covering an already-dispatched-but-unrecorded forward step is the orchestrator's
   * responsibility, discharged by this cancel/void discipline (which also keeps {@code compensate}
   * a pure function of {@code state} — the same command list on every resume/timeout/sweep re-drive
   * so the shared episode-scoped idempotency keys dedup correctly).
   *
   * @param state current saga state at time of failure
   * @param failure the exception thrown by {@code CommandBus.execute()}, or an internal marker when
   *     a crashed compensation episode is resumed or an operator compensates a forward-faulted saga
   *     ({@code SagaDeadLetterReplayer.compensateFaulted}) — diagnostic only, MUST be ignored by
   *     the implementation
   * @param failedCommand the command that failed, or {@code null} when a crashed compensation
   *     episode is resumed or an operator compensates a forward-faulted saga
   */
  List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand);
}

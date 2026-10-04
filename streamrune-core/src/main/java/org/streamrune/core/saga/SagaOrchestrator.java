package org.streamrune.core.saga;

import java.util.List;
import java.util.Optional;
import org.streamrune.core.EventEnvelope;

/**
 * Orchestration-based saga coordinator.
 *
 * <p>Implement this interface to define a saga that reacts to domain events by dispatching
 * commands. The {@code SagaRunner} drives execution: it calls {@link #isStartEvent}/{@link
 * #correlate} to route events, then {@link #evolve} + {@link #handle} to advance the saga, and
 * {@link #compensate} on command failure.
 *
 * <h2>Correlation</h2>
 *
 * <p>When a saga dispatches a command, the resulting events carry the {@code correlationId} set on
 * the dispatching context. Implement {@link #correlate} to extract the saga ID from non-start
 * events — typically by reading {@code event.metadata().correlationId().value()}.
 *
 * <h2>Serialization</h2>
 *
 * <p>Saga state implementations ({@code S}) must be Jackson-serializable. Annotate constructors
 * with {@code @JsonCreator} and parameters with {@code @JsonProperty}.
 *
 * <h2>Failure contract: poison events, not retries</h2>
 *
 * <p>{@code SagaRunner} treats any exception thrown by {@link #initialState}, {@link
 * #isStartEvent}, {@link #correlate}, {@link #extractSagaId}, {@link #evolve}, {@link #handle}, or
 * {@link #compensate} as a <b>poison event</b>: the triggering event is quarantined (metadata only
 * — saga id/type, event offset/type, error details, fault time; the payload is never copied out of
 * the event store) and the saga is permanently marked {@code FAULTED}. A <b>{@code null} return</b>
 * from {@link #extractSagaId} or {@link #correlate} — both contract violations; see their method
 * docs — is classified identically (quarantined as a null-saga entry, since no saga could be
 * identified), never allowed to wedge the subscription. There is no automatic retry or replay of a
 * poison event — a faulted saga stops processing events and is excluded from timeout sweeps until
 * an operator drains it with {@code SagaDeadLetterReplayer.replayAll}. (A compensation episode the
 * compensation-retry sweeper or the timeout runner gives up on is faulted too, with no dead-letter
 * entry; {@code SagaDeadLetterReplayer.resumeFaulted} resumes it. A forward fault whose entry an
 * operator discarded has nothing left to drain; {@code SagaDeadLetterReplayer.compensateFaulted}
 * compensates it.) Consequently, implementations must treat transient errors (a flaky downstream
 * call made from inside {@code evolve}/{@code handle}, for instance) as retryable <em>inside</em>
 * the method — or design them out entirely — because letting a transient exception escape faults
 * the saga irrecoverably rather than triggering a retry.
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
   * <p><b>Must not return {@code null}.</b> There is no "ignore this start event" signal here —
   * {@link #isStartEvent} already claimed the event for this saga type. A {@code null} return (e.g.
   * a metadata lookup missing on a malformed producer event) is treated exactly like a thrown
   * exception per the class-level failure contract: the event is quarantined as a
   * <em>null-saga</em> dead-letter entry and processing continues with the next event. Fix the
   * router (or the producer) and replay the entry.
   */
  SagaId extractSagaId(EventEnvelope event);

  /**
   * Correlates a non-start event to a running saga. Return {@link Optional#empty()} to ignore the
   * event. Typical implementation: read {@code event.metadata().correlationId().value()} and return
   * a {@link SagaId} if it matches a known prefix or pattern.
   *
   * <p><b>Must not return a bare {@code null}</b> — {@link Optional#empty()} is the one and only
   * "not mine" signal. A {@code null} return is treated exactly like a thrown exception per the
   * class-level failure contract: the event is quarantined as a <em>null-saga</em> dead-letter
   * entry and processing continues with the next event.
   */
  Optional<SagaId> correlate(EventEnvelope event);

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

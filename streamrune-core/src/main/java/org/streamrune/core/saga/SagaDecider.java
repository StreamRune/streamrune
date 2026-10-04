package org.streamrune.core.saga;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.streamrune.core.EventEnvelope;

/**
 * Pure domain logic for saga orchestration, separated from event routing concerns. Unlike {@link
 * SagaOrchestrator}, which combines domain logic with routing (start/correlate), {@code
 * SagaDecider} contains only the functional core: state evolution, command decisions, and
 * compensation.
 *
 * <p>Routing is configured via {@code SagaRunner.Builder} methods ({@code startWhen}, {@code
 * extractSagaId}, {@code correlateBy}).
 *
 * <h2>Failure contract: poison events, not retries</h2>
 *
 * <p>When driven via {@code SagaRunner} (event path), any exception thrown by {@link #evolve},
 * {@link #handle}, or {@link #compensate} is treated as a poison event exactly as for {@link
 * SagaOrchestrator}: the triggering event is quarantined (metadata only) and the saga is
 * permanently marked {@code FAULTED} — no retry, no automatic replay. Transient errors must be
 * retried inside the method or designed out. See {@link SagaOrchestrator}'s class-level javadoc for
 * the full contract, which applies identically here.
 *
 * @param <S> the saga state type
 */
public interface SagaDecider<S extends SagaState> {

  /** Java type of the saga state; required for deserialization from storage. */
  Class<S> stateType();

  /** Creates the initial state for a new saga instance identified by {@code sagaId}. */
  S initialState(SagaId sagaId);

  /**
   * Updates saga state based on the incoming event. Return the new (immutable) state.
   *
   * <p>Must be idempotent under at-least-once event redelivery — state application is not
   * deduplicated by the framework the way command dispatch is (see {@link SagaOrchestrator#evolve}
   * for the full rationale).
   *
   * <p>Any exception thrown here is treated as a poison event: see the class-level failure
   * contract.
   */
  S evolve(S state, EventEnvelope event);

  /**
   * Decides what commands to dispatch given the current state and triggering event. Return an empty
   * list to dispatch nothing.
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
   * Called when a dispatched {@link SagaCommand} fails, or when the saga times out (see {@link
   * #timeout()}) — invoked by {@code SagaTimeoutRunner} with a {@link SagaTimeoutException} and a
   * {@code null} {@code failedCommand}, since a timeout has no single failed command. Return
   * compensation commands to undo prior steps, or an empty list to skip compensation.
   *
   * <p>The returned list determines the terminal classification exactly as for {@link
   * SagaOrchestrator#compensate}: a non-empty list whose commands all dispatch successfully is
   * classified {@code COMPENSATED}; an empty list — the default here — is classified {@code
   * FAILED}, not {@code COMPENSATED}. On the timeout path specifically, a throwing {@code
   * compensate()} has no triggering event to quarantine, so {@code SagaTimeoutRunner} marks the
   * saga {@code FAULTED} directly instead of going through the saga dead-letter store.
   *
   * <p>Default: no compensation (returns {@code List.of()}, which classifies the saga {@code
   * FAILED}).
   *
   * <p><b>Crash-resume ({@code failedCommand} may be null):</b> the event path also resumes a
   * compensation episode whose owner crashed between claiming {@code COMPENSATING} and the terminal
   * write, re-invoking {@code compensate} with a {@code null} {@code failedCommand} (the original
   * failure did not survive the crash). As on the timeout path, implementations must tolerate a
   * {@code null} {@code failedCommand} and recompute the undo from {@code state}; {@code
   * compensate} must be a deterministic function of {@code state} so the resumed dispatch — under
   * the same episode-scoped idempotency keys — matches and already-executed commands dedup.
   *
   * <p><b>{@code compensate} must ignore the {@code failure} argument entirely and be a pure
   * function of {@code state}.</b> {@code failure} is diagnostic only (safe to log) — never branch
   * compensation logic on its identity or type. This matters concretely because the same {@code
   * COMPENSATING} episode can be resumed by two different paths that construct two different
   * objects for this parameter: a re-picked {@code SagaTimeoutRunner} always passes a fresh {@link
   * SagaTimeoutException}, while an event-path resume (crash-resume, above) always passes an
   * internal resume-marker — never a {@code SagaTimeoutException}, even though the compensation
   * episode is identical to one a timeout could have started. An implementation that branches on
   * {@code failure}'s type would therefore compute different compensation commands depending on
   * which path happened to resume the episode, breaking the determinism the effectively-once
   * redispatch relies on.
   *
   * <p><b>{@code state} may EXCLUDE a forward command that already executed — use cancel/void
   * semantics, not conditional undo.</b> {@code state} reflects only what {@code evolve} has
   * applied; it does not record which forward commands the current dispatch loop already ran. When
   * {@code handle()} emits several forward commands and a later one fails after an earlier one
   * committed — {@code [ChargePayment, ReserveStock]} where {@code ReserveStock} fails — {@code
   * state} was captured before either ran and cannot tell you {@code ChargePayment} succeeded;
   * likewise on the timeout/crash-resume paths a forward command may have committed with its
   * confirmation event still in flight (not yet {@code evolve}d). A compensation that
   * <em>conditions on state</em> ("refund only if {@code state.charged}") silently misses the undo
   * and leaves the customer charged for a cancelled order. Phrase compensation as <b>cancel/void
   * commands the target aggregate handles idempotently</b> (a {@code CancelPayment} that refunds if
   * charged and no-ops if not) rather than a conditional undo derived from state completeness. The
   * framework's "no missed compensation" guarantee holds only <em>for steps recorded in {@code
   * state}</em>; covering an already-dispatched-but-unrecorded forward step is the decider's
   * responsibility, discharged by this cancel/void discipline (which is also what keeps {@code
   * compensate} a pure function of {@code state}).
   *
   * @param state current saga state at time of failure
   * @param failure the exception thrown by {@code CommandBus.execute()}, a {@link
   *     SagaTimeoutException} when invoked from the timeout path, or an internal marker when a
   *     crashed compensation episode is resumed or an operator compensates a forward-faulted saga
   *     ({@code SagaDeadLetterReplayer.compensateFaulted}) — diagnostic only, MUST be ignored by
   *     the implementation
   * @param failedCommand the command that failed, or {@code null} when invoked from the timeout
   *     path, when a crashed compensation episode is resumed, or when an operator compensates a
   *     forward-faulted saga
   */
  default List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand) {
    return List.of();
  }

  /**
   * Maximum duration a saga may remain in a non-terminal state before being considered timed out.
   *
   * <p><b>Absolute, measured from the saga's start instant.</b> This is a wall-clock bound on the
   * whole saga reaching a terminal state, NOT an inactivity window: a multi-step saga that keeps
   * receiving correlated events still times out once it has been non-terminal for longer than this
   * duration — activity does not reset the deadline. (The one exception is a {@code COMPENSATING}
   * crash-resume episode, which the {@code SagaTimeoutRunner}/{@code SagaCompensationRetrySweeper}
   * re-pick on last-write inactivity so re-drives are spaced out; see {@link
   * SagaStore#findTimedOut}.)
   *
   * <p>When a saga times out, {@code SagaTimeoutRunner} claims exclusive ownership of the
   * compensation episode (CAS to {@code SagaStatus#COMPENSATING}) and then calls {@link
   * #compensate} with a {@link SagaTimeoutException}. Default: no timeout (empty) — {@code
   * SagaTimeoutRunner.Builder.build()} refuses a decider without one, so no runner exists for such
   * a saga; a stuck {@code COMPENSATING} row is then recovered by {@code
   * SagaCompensationRetrySweeper}.
   */
  default Optional<Duration> timeout() {
    return Optional.empty();
  }
}

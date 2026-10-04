package org.streamrune.runtime;

/**
 * Signals that a saga-dispatched command was <b>vetoed</b> by a {@link
 * org.streamrune.core.CommandInterceptor} whose {@code before()} returned {@code false} — the bus
 * returned a {@link org.streamrune.core.CommandBus.ShortCircuitReason#VETOED VETOED} {@code
 * CommandResult} without throwing, meaning the command never executed and no events were produced
 * or persisted.
 *
 * <p><b>Why it exists.</b> {@link SagaCommandDispatch#executeCorrelated} used to discard the {@code
 * CommandResult}, so a veto returned "normally" and was indistinguishable from success: a vetoed
 * compensation counted zero failures and terminal-wrote {@code COMPENSATED} with the refund never
 * run (silent money loss — no inbox row, no DLQ entry, terminal-guarded against re-drive), and a
 * vetoed forward command persisted forward progress with the side effect never executed. This
 * exception makes the veto <em>visible</em>; it deliberately does NOT bypass interceptors —
 * SAGA_OWNED semantics are unchanged and user interceptors still run on saga commands (only {@code
 * AnnotationAuthorizationInterceptor} and {@code AuthorizationCommandInterceptor} honor the
 * saga-system principal).
 *
 * <h2>Taxonomy: a veto is EVIDENCE-FREE, not a rejection</h2>
 *
 * <p>This used to extend {@link org.streamrune.core.DomainException} — the permanent-rejection
 * bucket — on the reasoning that a veto is "deterministic, exactly like a thrown {@code
 * AuthorizationException}". That put it in direct contradiction with the circuit breaker's own
 * rule: a VETOED callback "carries no evidence" for the circuit breaker, because nothing was
 * executed and no infrastructure was touched, so the breaker must neither close on it nor reset its
 * failure count. Both cannot be right about the same signal, and the consequence of the permanent
 * reading was concrete: one load-shedding window (a rate limiter, a maintenance-mode kill switch)
 * terminally {@code FAILED} every saga that received an event during it, with the forward side
 * effects that already committed applied and the undo — vetoed by the same gate — never run.
 *
 * <p>The tie-breaker is inside the retry-later classification itself. {@code
 * CircuitBreakerOpenException} <em>is</em> a veto: it is the framework's own interceptor refusing
 * to admit the command in {@code before()}, differing only in that it throws instead of returning
 * {@code false} — and the classification treats it as <b>retry-later</b> ("a pure 'later' gate; the
 * command was never attempted and no side effect can have occurred"). Two spellings of one event
 * cannot have opposite verdicts.
 *
 * <p>So: an interceptor veto asserts <em>nothing</em> — not about the business (the domain never
 * saw the command) and not about the infrastructure (nothing was touched). Every consumer must
 * decline to conclude from it:
 *
 * <ul>
 *   <li><b>circuit breaker</b> — abstain: do not close the circuit, do not reset {@code
 *       failureCount}, do not count a failure (unchanged);
 *   <li><b>saga forward path</b> — abstain: leave the saga exactly as it was and propagate for
 *       redelivery ({@link SagaCommandDispatch#isRetryLater} lists this type among the proven
 *       retry-later markers), rather than asserting "the flow failed" and undoing it;
 *   <li><b>saga compensation path</b> — abstain: neither {@code COMPENSATED} (the undo did not run)
 *       nor terminal {@code FAILED} (the undo is not abandoned for a gate that lifts). The episode
 *       stays {@code COMPENSATING} and is re-driven, bounded exactly like every other transient
 *       compensation failure by {@code SagaCompensationRetrySweeper}'s age-based give-up → {@code
 *       FAULTED}.
 * </ul>
 *
 * <p>It therefore extends {@link RuntimeException} and NOT {@code DomainException}: {@code
 * CommandFailureClassification.PERMANENT_REJECTION} means "the domain gave a correct, final
 * answer", which a veto by definition is not. Leaving it inside that set would silently re-answer
 * this question wrongly in the next consumer that asks it.
 *
 * <p><b>What has not changed:</b> a veto is still never a success, and it is still <b>not</b>
 * poison ({@link SagaPoisonException}) — the triggering <em>event</em> is fine, the
 * <em>command</em> was refused, so nothing is quarantined.
 *
 * <p>Package-private like {@link SagaCompensationResumeException}: orchestrators receive it only as
 * an opaque diagnostic {@link Throwable} in {@code compensate(state, cause, failedCommand)} and
 * must not branch on it — {@code compensate} is a pure function of state. The message carries the
 * vetoing interceptor and command type for logs/operators.
 */
final class SagaCommandVetoedException extends RuntimeException {

  private final String interceptorName;

  /**
   * @param interceptorName the vetoing interceptor's name ({@code
   *     CommandResult.shortCircuitedBy()}), or {@code null} when the bus did not report one
   * @param commandType the vetoed command's simple class name
   */
  SagaCommandVetoedException(String interceptorName, String commandType) {
    super(
        "Saga command "
            + commandType
            + " was VETOED by interceptor '"
            + interceptorName
            + "' — the command did not run and produced no events. A veto refuses to ADMIT the"
            + " command; it decides nothing about the business and touches no infrastructure, so"
            + " the saga draws no conclusion from it: on the forward path the saga is left"
            + " untouched and the failure propagates for redelivery, and on the compensation path"
            + " the episode stays COMPENSATING to be re-driven. If saga-internal commands should"
            + " be exempt from this gate, have the interceptor consult"
            + " StreamRuneContext.isSagaDispatch().");
    this.interceptorName = interceptorName;
  }

  /** The vetoing interceptor's name, or {@code null} when the bus did not report one. */
  String interceptorName() {
    return interceptorName;
  }
}

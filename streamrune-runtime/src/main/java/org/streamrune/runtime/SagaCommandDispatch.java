package org.streamrune.runtime;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import org.slf4j.Logger;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.DomainException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.IdempotencyKey;

/**
 * Shared saga command dispatch + compensation classification used by SagaRunner and
 * SagaTimeoutRunner.
 */
final class SagaCommandDispatch {

  /** Depth cap so a cyclic {@code getCause()} chain cannot loop forever (matches the siblings). */
  private static final int MAX_CAUSE_DEPTH = 50;

  private SagaCommandDispatch() {}

  /**
   * Executes a saga-issued command with the dispatching saga stated as its cause: {@code origin}
   * and {@code causingEvent} — for a forward command the id of the triggering event, for a
   * compensation command the id of the event that opened the compensation episode — are bound as
   * {@link StreamRuneContext#CAUSE}, and the command bus copies them into {@link
   * org.streamrune.core.EventMetadata#sagaOrigin()} and {@link
   * org.streamrune.core.EventMetadata#causationId()} of every event the command produces. The
   * request correlation id bound is the origin's ({@link SagaOrigin#correlationId()}, {@code
   * streamrune:saga:<sagaId>}), a form the request edge refuses from a client; ambient
   * trace/user/baggage is preserved. The {@code key} is recorded atomically with the append so
   * redelivered correlated events short-circuit without re-running the handler.
   *
   * <p><b>The bound cause is the same on every attempt.</b> {@code origin} is a function of the
   * saga type and the saga id, and {@code causingEvent} is the id of a stored event — the
   * triggering event of a forward step, which is the same event on every delivery of it, or the
   * cause recorded with a compensation episode's stamp, which every resume reads from the saga row
   * ({@link org.streamrune.core.saga.LoadedSaga#episodeCauseEventId()}). A first attempt, a
   * redelivery, a replay feed, a timeout re-pick and a sweeper resume of the same command therefore
   * bind equal values: whichever attempt commits the command stores the same saga origin, the same
   * correlation id (a function of the origin's saga id) and the same causation id. The idempotency
   * keys are derived from the saga id and not from the correlation id ({@link #forwardKey}, {@link
   * #episodeCompensationKey}), so the form of the correlation id takes no part in what the command
   * inbox deduplicates. The trace id, the user id and the baggage of the stored events are those of
   * the ambient context of the thread that commits the command (none on a subscription thread, an
   * operator's on a replay run from a request). The ambient context is {@link
   * StreamRuneContext#CURRENT}, or — with a {@link VirtualThreadCommandBus} and {@code CURRENT}
   * unbound — the bus's {@link VirtualThreadCommandBus.Builder#unboundRequestContext
   * unbound-request-context source}, so a replay run from a request on a thread outside the request
   * filter's binding records the operator too. It contributes nothing else: the saga's correlation
   * id, {@link StreamRuneContext#SAGA_OWNED} and the saga origin are bound as always.
   *
   * <p><b>A VETO is never a silent success.</b> A user {@link
   * org.streamrune.core.CommandInterceptor} whose {@code before()} returns {@code false}
   * (maintenance mode / kill switch / rate limiter — a documented first-class bus feature) makes
   * the bus RETURN a {@code VETOED} {@link CommandBus.CommandResult} without throwing. Discarding
   * that result made a vetoed saga command indistinguishable from success: a vetoed compensation
   * terminal-wrote {@code COMPENSATED} with the refund never run (silent money loss), and a vetoed
   * forward command persisted forward progress with the side effect never executed. The result is
   * therefore captured and a veto surfaces as {@link SagaCommandVetoedException}. An {@code
   * IDEMPOTENT_REPLAY} short-circuit is a SUCCESS (its events were persisted by the prior keyed
   * execution) and returns normally. Consistent with the DLQ replay semantics. SAGA_OWNED semantics
   * are unchanged — the veto is made visible, interceptors are not bypassed.
   *
   * <p><b>And it is not a rejection either.</b> A veto refuses to ADMIT the command: the domain
   * never saw it, so no business answer exists, and no infrastructure was touched, so no health
   * evidence exists. That is exactly the reading the circuit breaker already gives it ("this
   * callback carries no evidence"), and exactly the reading given to the framework's OWN admission
   * gate, {@link CircuitBreakerOpenException} — a veto that happens to throw rather than return
   * {@code false}. Binning a user veto as a permanent business rejection therefore contradicted
   * both, and one maintenance window terminally FAILED every in-flight saga with its undo never
   * run. Every consumer now abstains instead: the forward loop leaves the saga untouched and
   * propagates for redelivery ({@link #isRetryLater}), and {@link #compensateAndClassify} keeps the
   * episode {@code COMPENSATING} for the resume paths — never {@code COMPENSATED} (that protection
   * is intact) and never terminal {@code FAILED}.
   *
   * <p><b>The context built here is a RECONSTRUCTION, not an ingress.</b> The correlation id is
   * {@code origin.correlationId()} — the reserved prefix, then the saga id verbatim — and every
   * background entry point (timeout runner, compensation sweeper, dead-letter replayer) got that
   * {@code SagaId} out of {@code saga_state} / {@code saga_dead_letters} via {@code
   * PostgresSagaStore}; the trace/user fields are copied from an ambient context that may itself be
   * a reconstruction. So this uses the plain {@code RequestContext} constructor and NOT {@code
   * RequestContext.fromRequest} — the {@code IdConstraints} bound and the refusal of the reserved
   * prefix belong only where an unauthenticated header first enters the system. Through {@code
   * fromRequest} no saga could dispatch at all: the door refuses the very prefix this method binds,
   * and the reserved form of a saga id longer than 239 characters exceeds its length bound.
   * Enforcing it here threw before the bus was ever called, which the forward classifier reads as
   * permanent evidence ({@link #isRetryLater} is {@code false} for {@code
   * IllegalArgumentException}) → claim-first compensation → every compensation dispatch fails
   * identically at the identical point → terminal {@code FAILED} with the undo never dispatched.
   * {@code SagaId}s are business ids chosen by application code and are not bound by {@code
   * IdConstraints} (a saga id may carry a control character), so any such saga would take exactly
   * that path.
   *
   * <p>Note also why "sanitize instead of reject" would have been the worse answer here: {@link
   * #forwardKey} and {@link #episodeCompensationKey} both embed {@code sagaId.value()} directly, so
   * rewriting the id would change every idempotency key derived from it. During a rolling upgrade
   * the two node versions would derive DIFFERENT keys for the same logical command, the command
   * inbox would not dedup them, and the side effect — a refund — would execute twice.
   * Reconstruction must be verbatim.
   *
   * @param commandBus the bus the command is executed on
   * @param origin the dispatching saga (required)
   * @param causingEvent the id of the event the command reacts to: the triggering event of a
   *     forward command, the event that opened the episode of a compensation command; {@code null}
   *     for a compensation command of an episode opened with no event in hand (a timeout claim, an
   *     operator's compensation of a faulted forward step)
   *     <p><b>The forward fence.</b> With {@code forwardFenced}, the dispatch also binds {@link
   *     StreamRuneContext#SAGA_FORWARD} to {@code origin} and {@code key}: the command bus hands
   *     the origin to the event store with the command's keyed append, and the store refuses the
   *     append ({@link SagaForwardFencedException}) once the saga row carries a compensation
   *     episode, is terminal or is gone, in the same transaction that would store the command's
   *     events. Only a forward command of a runner whose fence is on is dispatched so; a
   *     compensation command never is ({@link #compensateAndClassify}).
   * @param command the saga-issued command
   * @param key the command's idempotency key
   * @param forwardFenced whether the command is a forward command that the event store fences
   *     against the saga row
   * @return {@code true} when this call executed the command and the command stored events; {@code
   *     false} when the command inbox answered it with the outcome of an earlier execution under
   *     the same key ({@link CommandBus.CommandResult#idempotentReplay()}), whose events were
   *     stored by that execution, and when the command ran and stored nothing. A bus that returns
   *     no result is taken to have executed the command
   * @throws NullPointerException when {@code origin} is {@code null}; the bus is not called
   * @throws SagaCommandVetoedException when an interceptor vetoed the command (it never executed)
   * @throws SagaForwardFencedException when the event store refused a fenced forward command; it
   *     stored nothing
   */
  static boolean dispatch(
      CommandBus commandBus,
      SagaOrigin origin,
      EventId causingEvent,
      Command command,
      IdempotencyKey key,
      boolean forwardFenced) {
    Objects.requireNonNull(origin, "origin is required");
    // The bus's view of the caller: CURRENT when bound, else (VirtualThreadCommandBus) its
    // unbound-request-context source — the request an operator replays a saga from, on a thread
    // outside the request filter's binding. Null on every background entry point either way.
    StreamRuneContext.RequestContext ambient =
        commandBus instanceof VirtualThreadCommandBus bus
            ? bus.callerRequestContext()
            : StreamRuneContext.capture();
    StreamRuneContext.RequestContext ctx =
        new StreamRuneContext.RequestContext(
            ambient != null ? ambient.traceId() : null,
            ambient != null ? ambient.userId() : null,
            origin.correlationId(),
            Instant.now(),
            ambient != null ? ambient.baggage() : Map.of(),
            // Carry the request edge's captured authority when a
            // saga is driven synchronously from a request thread (SagaDeadLetterReplayer's operator
            // path). SAGA_OWNED already short-circuits AnnotationAuthorizationInterceptor ahead of
            // it, so this matters only for a deployment that opted out with
            // honorSagaSystemPrincipal=false — there, dropping it would deny the saga's commands on
            // the very path where the caller IS authenticated. Null on every background entry point
            // (subscription poll, timeout runner, compensation sweeper), where ambient is null too.
            ambient != null ? ambient.authority() : null);
    ScopedValue.Carrier carrier =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
            .where(
                StreamRuneContext.CAUSE,
                new StreamRuneContext.DispatchCause(
                    causingEvent == null ? null : CausationId.of(causingEvent.value()), origin));
    if (forwardFenced) {
      carrier =
          carrier.where(
              StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(origin, key));
    }
    CommandBus.CommandResult result = carrier.call(() -> commandBus.execute(command, key));
    // Null-tolerant like DeadLetterRetryRunner.processEntry: a third-party bus returning null is
    // treated as an executed command, never as a veto.
    if (result != null && result.vetoed()) {
      throw new SagaCommandVetoedException(
          result.shortCircuitedBy(), command.getClass().getSimpleName());
    }
    if (result == null) {
      return true;
    }
    return !result.idempotentReplay() && result.events() != null && !result.events().isEmpty();
  }

  /**
   * Forward command key: deterministic per (saga, triggering event offset, command index), with the
   * text {@code saga:<sagaId>:<triggerOffset>:<index>}.
   *
   * <p>Built through {@link IdempotencyKey}'s canonical constructor — its DECODE door — never
   * {@link IdempotencyKey#of}, the ingress factory that refuses control characters. The key is
   * derived from a stored saga id, and a {@code SagaId} is bounded in length, not in charset:
   * through {@code of}, a saga whose id carries a control character could never dispatch again, and
   * the derivation must stay byte-identical so a rerun presents the key an earlier run recorded.
   * The same holds for {@link #episodeCompensationKey}.
   */
  static IdempotencyKey forwardKey(SagaId sagaId, long triggerOffset, int index) {
    return new IdempotencyKey("saga:" + sagaId.value() + ":" + triggerOffset + ":" + index);
  }

  /**
   * The one compensation command key, shared by every path that dispatches a compensation command
   * of a saga: a start event's and a correlated event's delivery, a dead-letter replay feed, {@link
   * SagaTimeoutRunner}, {@link SagaCompensationRetrySweeper} and the operator's {@code
   * resumeFaulted} / {@code compensateFaulted}. A keyspace separate from the forward keys, scoped
   * by {@code episodeVersion} — the episode version stamped on the saga row by the write that
   * entered the episode ({@code LoadedSaga.episodeVersion()}). {@code SagaStepExecutor} reads it
   * from the row on every dispatch and never computes it.
   *
   * <p><b>Why every path must share this key:</b> more than one of them can drive compensation for
   * the same saga. The write that enters the episode — a claim, or a forward write that persists an
   * evolved {@code COMPENSATING} status — stamps the episode version on the row, and no later write
   * changes it. The path that opens the episode reads the stamp back before it dispatches; if it
   * then crashes (or loses the terminal write) before finishing, or while it is still dispatching,
   * another path resumes the <em>same</em> episode without re-claiming and reads the same stamp.
   * Because every path feeds the row's {@code episodeVersion} into this method, a genuine
   * double-delivery of the same logical compensation dedups in the command inbox instead of
   * double-executing the side effect.
   *
   * <p>The key text is {@code saga:<sagaId>:episode:<episodeVersion>:comp:<index>}.
   */
  static IdempotencyKey episodeCompensationKey(SagaId sagaId, long episodeVersion, int index) {
    // The decode door, as forwardKey explains.
    return new IdempotencyKey(
        "saga:" + sagaId.value() + ":episode:" + episodeVersion + ":comp:" + index);
  }

  /**
   * Whether {@code failure} is the event store's refusal of a fenced forward command: a {@link
   * SagaForwardFencedException} anywhere in the cause chain (bounded, so a cyclic chain cannot
   * loop). The command bus throws it unwrapped; an interceptor or a bus double may wrap it. The
   * step executor asks this before any other classification of a failed forward command: the
   * refusal is a fact about the saga row, not about the command, so it is neither a business
   * rejection to compensate nor a failure to retry.
   *
   * @param failure the exception a forward saga command dispatch threw (never null)
   */
  static boolean isForwardFenced(Throwable failure) {
    Throwable c = failure;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof SagaForwardFencedException) {
        return true;
      }
    }
    return false;
  }

  /**
   * The FORWARD dispatch classifier: whether a failed forward saga command is a proven
   * <b>retry-later</b> signal, meaning the saga must be left alone (nothing written, failure
   * propagated so the subscription redelivers) rather than compensated.
   *
   * <p><b>Why the forward path needs its OWN predicate rather than {@link
   * VirtualThreadCommandBus#DEFAULT_DLQ_ELIGIBLE}, which {@link #compensateAndClassify} uses one
   * method away.</b> The two paths ask the same question but pay wildly different prices for the
   * same mistake, so they cannot share the same fail-safe direction:
   *
   * <ul>
   *   <li>On the COMPENSATION path, "transient" means RETRY: the saga stays {@code COMPENSATING}
   *       and is re-driven. Mis-binning a deterministic failure as transient costs ONE saga, and
   *       even that is bounded — {@code SagaCompensationRetrySweeper}'s age-based give-up FAULTs
   *       it. So that path can afford the generous rule "everything that is not a business
   *       rejection is transient".
   *   <li>On the FORWARD path, "transient" means PROPAGATE, and propagation is the only way to get
   *       the event redelivered (a return would let the subscription checkpoint advance past it —
   *       the lost-event class). Nothing bounds it: {@code ResilientPollLoop} retries the identical
   *       batch with capped backoff forever, so mis-binning a deterministic failure as transient
   *       wedges EVERY listener on that subscription, with nothing dead-lettered — the exact
   *       head-of-line class the saga runner's poison handling exists to close. The generous rule
   *       would do that for a plain handler {@code NullPointerException} or an {@code
   *       EventStoreException} wrapping a Jackson mapping defect, neither of which is a {@code
   *       DomainException}.
   * </ul>
   *
   * <p><b>The rule therefore proves only the direction that CHANGES behaviour.</b> A verdict of
   * {@code false} is the framework's documented original contract, unchanged: claim {@code
   * COMPENSATING} first, then compensate and terminalize — a halted, durable, operator-visible
   * outcome that never wedges a subscription. Only provable retry-later evidence diverts to the new
   * propagate branch. The whole cause chain is scanned (bounded, so a self-referential chain cannot
   * spin) and PERMANENT evidence short-circuits, so mixed evidence always falls to the status quo:
   *
   * <ol>
   *   <li>{@link SubjectForgottenException} anywhere ⇒ <b>not</b> retry-later. The subject was
   *       crypto-shredded and the engine will refuse to re-encrypt until an operator calls {@code
   *       reinstate} — the same permanent verdict {@code DEFAULT_DLQ_ELIGIBLE} and {@link
   *       SagaStateConversion} already reach. Checked first because it surfaces wrapped, underneath
   *       infrastructure-looking frames.
   *   <li>A {@link DomainException} or an {@link IllegalArgumentException} anywhere ⇒ <b>not</b>
   *       retry-later. This is the ONE meaning of "a forward command failed" for which undoing the
   *       business flow is the correct answer, and it is exactly the set {@code
   *       DEFAULT_DLQ_ELIGIBLE} and {@code
   *       CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES} both exclude.
   *   <li>Otherwise, any of the following anywhere in the chain ⇒ <b>retry-later</b>. Every member
   *       is a signal the framework ALREADY classifies as transient somewhere else, so this
   *       introduces no new judgement:
   *       <ul>
   *         <li>{@link CircuitBreakerOpenException} — the breaker refused the command in {@code
   *             before()}, so it was never attempted and no side effect can have occurred. A pure
   *             "later" gate, bounded by the breaker's own cooldown. This is the trigger for the
   *             retry-later verdict: the breaker is a DEFAULT bean in all three integrations, so
   *             before this fix a downstream incident silently refunded every saga that touched it.
   *         <li>{@link SagaCommandVetoedException} — a USER interceptor refused the command in
   *             {@code before()} by returning {@code false}. Same event as the line above,
   *             different spelling: nothing was attempted, nothing was decided, nothing was
   *             touched. It used to sit in the permanent bucket above (as a {@code
   *             DomainException}), which meant a rate limiter or maintenance-mode window terminally
   *             FAILED every saga that received an event while it was on — with forward side
   *             effects already applied and the undo, vetoed by the same gate, never run. See
   *             {@link SagaCommandVetoedException} for the full argument; the operator remedy is to
   *             have such a gate consult {@code StreamRuneContext.isSagaDispatch()}.
   *         <li>{@link CommandBusClosedException} — the bus is shutting down, so the command was
   *             refused ADMISSION exactly like the two entries above: nothing was attempted,
   *             nothing was decided, nothing was touched. This used to be a bare {@code
   *             IllegalStateException} this classifier could not prove retriable, so a graceful
   *             node shutdown mid-flow claimed a compensation episode and issued the undo for a
   *             healthy saga — while {@link #compensateAndClassify} binned the very same exception
   *             RETRY. The redelivered event converges on the deterministic forward keys after
   *             restart.
   *         <li>{@link CommandBusOverloadedException} — the bus's {@code executeAsync} in-flight
   *             admission budget is exhausted. Same ADMISSION refusal shape as {@link
   *             CommandBusClosedException} directly above (nothing was attempted), so it gets the
   *             identical treatment for the identical reason.
   *         <li>The bus's OTHER {@code IllegalStateException} ("requires a CommandInbox") stays the
   *             plain type and stays out of this set: misconfiguration is not a "later".
   *         <li>{@link OptimisticLockException} / {@link LockException} — the bus's own retry
   *             ladder calls both {@code transientFailure} and retries them before giving up.
   *         <li>{@link SagaStepLockException} — a saga step did not get its saga's {@link
   *             SagaStepLock} within the step bound, or was interrupted while it waited. It read
   *             nothing and dispatched nothing. A step raises it before its forward loop, so it
   *             reaches this classifier only when an application's listener plumbing delivers an
   *             event inside a forward command's dispatch and lets the failure out through it.
   *         <li>{@link SQLException} — JDBC evidence: connection reset, pool exhaustion, statement
   *             timeout, failover. The same fail-safe marker {@link SagaStateConversion} and {@code
   *             ReadPoisonClassifier} key on.
   *         <li>A plain {@link CryptoOperationException} (i.e. NOT a {@link
   *             CryptoMappingException}) — an engine-raised backend or key-level failure: a
   *             key-store/Vault/KMS outage, or a key the engine cannot use until an operator acts
   *             (disabled, missing or inaccessible). Nothing in it proves the failure permanent, so
   *             it is retry-later like the rest of this set: the event is redelivered until the
   *             engine answers, which for a key an operator must fix means the subscription retries
   *             until the operator fixes it. Again the same rule as {@link SagaStateConversion}.
   *       </ul>
   *   <li>Anything else ⇒ <b>not</b> retry-later. Never divert a failure that could not be
   *       <em>proven</em> retriable onto an unbounded batch retry.
   * </ol>
   *
   * @param failure the exception a forward saga command dispatch threw (never null)
   */
  static boolean isRetryLater(Throwable failure) {
    boolean sawRetryLater = false;
    Throwable c = failure;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof SubjectForgottenException
          || c instanceof DomainException
          || c instanceof IllegalArgumentException) {
        return false; // permanent evidence outranks every retry-later marker
      }
      if (c instanceof CircuitBreakerOpenException
          || c instanceof SagaCommandVetoedException
          || c instanceof CommandBusClosedException
          || c instanceof CommandBusOverloadedException
          || c instanceof OptimisticLockException
          || c instanceof LockException
          || c instanceof SagaStepLockException
          || c instanceof SQLException
          || (c instanceof CryptoOperationException && !(c instanceof CryptoMappingException))) {
        sawRetryLater = true;
      }
    }
    return sawRetryLater;
  }

  /**
   * Outcome of dispatching a compensation episode's commands.
   *
   * <p>Distinguishing a transient/infra compensation failure from a permanent one is what stops a
   * momentary DB blip (connection reset, pool exhaustion, {@code EventStoreException}) from
   * force-terminalizing a saga {@code FAILED} and silently abandoning the undo (refund never
   * issued, stock never released). A transient failure is RETRIABLE: the saga stays {@code
   * COMPENSATING} and the shared, episode-scoped resume path re-drives it — already-succeeded
   * compensations dedup in the command inbox, only the failed one re-runs — until it durably
   * succeeds. Only a deterministic (business) compensation failure, or an empty compensation list,
   * terminalizes {@code FAILED}.
   */
  enum CompensationOutcome {
    /** At least one command, all dispatched successfully → terminal {@code COMPENSATED}. */
    COMPENSATED(SagaStatus.COMPENSATED),
    /**
     * A deterministic/permanent compensation failure (or none to run) → terminal {@code FAILED}.
     */
    FAILED(SagaStatus.FAILED),
    /**
     * A transient/infra compensation failure (and no permanent one) → NOT terminal: the saga must
     * be left {@code COMPENSATING} so the resume path re-drives it. Carries no terminal status.
     */
    RETRY(null),
    /**
     * Every failed compensation command was REFUSED ADMISSION — an open circuit breaker, a closing
     * bus, or an interceptor veto — so nothing was attempted and nothing can be learned about the
     * compensation itself. Semantically {@link #RETRY} (the episode stays {@code COMPENSATING} for
     * the resume paths), but callers that COUNT attempts toward a give-up bound ({@code
     * SagaCompensationRetrySweeper}) must not count this one: a sweeper that recorded a refused
     * re-drive as a real attempt would, one cycle past {@code giveUpAfter}, FAULT the saga with
     * ZERO actual dispatches — the same class the dead-letter retry runner guards against in {@code
     * DeadLetterRetryRunner}. A mix of refusals and genuine transient failures stays {@link
     * #RETRY}: a real attempt happened.
     */
    RETRY_REFUSED(null);

    private final SagaStatus terminalStatus;

    CompensationOutcome(SagaStatus terminalStatus) {
      this.terminalStatus = terminalStatus;
    }

    /**
     * Whether the caller must terminal-write {@link #terminalStatus()}. {@code false} for {@link
     * #RETRY}, which must leave the saga {@code COMPENSATING} for the resume path.
     */
    boolean isTerminal() {
      return terminalStatus != null;
    }

    /** Terminal status to persist; throws for {@link #RETRY} (never terminal-written). */
    SagaStatus terminalStatus() {
      if (terminalStatus == null) {
        throw new IllegalStateException("RETRY has no terminal status");
      }
      return terminalStatus;
    }
  }

  /**
   * Dispatches each compensation command through {@link #dispatch} under {@code origin} and {@code
   * causingEvent}, swallowing+classifying individual failures into a {@link CompensationOutcome}:
   * COMPENSATED if there was at least one command and all dispatched successfully; FAILED if a
   * command failed <em>deterministically</em> (a business rejection) or the list was empty; RETRY
   * if the only failures were transient/infra (so the undo must not be abandoned — the saga stays
   * {@code COMPENSATING} for the resume path). A permanent failure dominates a transient one:
   * retrying a deterministic failure forever is worse than terminalizing.
   *
   * <p>Caller-supplied key scheme: the one caller, {@code SagaStepExecutor}, supplies the shared,
   * episode-scoped {@link #episodeCompensationKey}-derived keys on every path, scoped by the
   * episode version the saga row carries (the row version at which {@code COMPENSATING} was first
   * persisted), so a genuine double-delivery across paths dedups in the command inbox instead of
   * double-executing the side effect. The classification logic is identical for all callers.
   */
  static CompensationOutcome compensateAndClassify(
      List<SagaCommand> compensations,
      CommandBus commandBus,
      SagaOrigin origin,
      EventId causingEvent,
      IntFunction<IdempotencyKey> keyForIndex,
      Logger log) {
    if (compensations.isEmpty()) {
      return CompensationOutcome.FAILED;
    }
    boolean hadTransientFailure = false;
    boolean hadPermanentFailure = false;
    boolean hadAttemptedTransientFailure = false;
    for (int i = 0; i < compensations.size(); i++) {
      try {
        // A compensation command is never fenced: it is the command the fence makes room for.
        dispatch(
            commandBus,
            origin,
            causingEvent,
            compensations.get(i).command(),
            keyForIndex.apply(i),
            /* forwardFenced= */ false);
      } catch (Exception e) {
        // Reuse the bus's DLQ-eligibility split: a business rejection
        // (DomainException/IllegalArgumentException) is deterministic and can never succeed on
        // retry, so it terminalizes FAILED; everything else (EventStoreException, a connection/pool
        // failure, any other infra RuntimeException) is transient and must be RETRIED so the undo
        // is never dropped on a momentary blip.
        //
        // An interceptor VETO surfaces here as SagaCommandVetoedException. It is
        // deliberately NOT a DomainException, so it lands in this RETRY branch: the
        // episode stays COMPENSATING and the resume paths re-drive it, bounded by
        // SagaCompensationRetrySweeper's age-based give-up -> FAULTED like any other transient
        // compensation failure. It used to terminalize FAILED on the argument that "a veto is
        // deterministic, re-driving would spin forever" — but a veto is a load-shedding GATE, and
        // the two ways it can be wrong are not symmetric: staying COMPENSATING through a
        // maintenance window costs a bounded dwell and then actually issues the refund, whereas
        // terminalizing abandoned the undo permanently for a gate that lifts in minutes. What is
        // unchanged is the guarantee that a veto is never COMPENSATED.
        if (VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(e)) {
          hadTransientFailure = true;
          // A refused admission attempted nothing; only a failure that actually reached
          // the command counts as an attempt for the sweeper's give-up bookkeeping.
          if (!isRefusedAdmission(e)) {
            hadAttemptedTransientFailure = true;
          }
          log.warn(
              "Saga compensation command failed transiently (or was refused admission by an"
                  + " interceptor veto); the episode will be retried (kept COMPENSATING), not"
                  + " force-FAILED",
              e);
        } else {
          hadPermanentFailure = true;
          log.warn(
              "Saga compensation command failed deterministically; terminalizing the episode"
                  + " FAILED",
              e);
        }
      }
    }
    if (!hadTransientFailure && !hadPermanentFailure) {
      return CompensationOutcome.COMPENSATED;
    }
    if (hadPermanentFailure) {
      return CompensationOutcome.FAILED;
    }
    return hadAttemptedTransientFailure
        ? CompensationOutcome.RETRY
        : CompensationOutcome.RETRY_REFUSED;
  }

  /**
   * Whether {@code failure}'s chain carries an ADMISSION refusal — the command was never handed to
   * its decider, so the failure says nothing about the compensation: an OPEN circuit breaker
   * ({@link CircuitBreakerOpenException}), a bus that is shutting down ({@link
   * CommandBusClosedException}), the bus's {@code executeAsync} in-flight budget being exhausted
   * ({@link CommandBusOverloadedException}), or an interceptor veto ({@link
   * SagaCommandVetoedException}, the same admission gate expressed by returning {@code false}). A
   * strict subset of {@link #isRetryLater}: a JDBC blip or a lost lock is retry-later too, but the
   * command WAS attempted.
   */
  static boolean isRefusedAdmission(Throwable failure) {
    Throwable c = failure;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CircuitBreakerOpenException
          || c instanceof CommandBusClosedException
          || c instanceof CommandBusOverloadedException
          || c instanceof SagaCommandVetoedException) {
        return true;
      }
    }
    return false;
  }
}

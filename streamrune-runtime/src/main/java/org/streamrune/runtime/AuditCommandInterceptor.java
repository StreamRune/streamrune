package org.streamrune.runtime;

import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * A {@link CommandInterceptor} that writes an {@link AuditEntry} to an {@link AuditStore} after
 * every command execution — both successful and failed.
 *
 * <p>The caller's {@link org.streamrune.core.types.UserId} is read from {@link
 * StreamRuneContext#CURRENT} in {@link #before(CommandContext)} and captured in a {@link
 * ThreadLocal} so it remains available in {@link #after(CommandContext)} and {@link
 * #onError(CommandContext, Throwable)}, even if the ScopedValue is no longer bound on the calling
 * thread at that point.
 *
 * <p>A {@code null} userId in the audit entry means the command was submitted without an
 * authenticated user (anonymous request).
 *
 * <p>The bound {@link org.streamrune.core.types.CorrelationId} is captured the same way and written
 * to {@link AuditEntry#correlationId()}, tying the audit row to the originating business flow. It
 * is {@code null} when no request context was bound.
 *
 * <p><strong>Calling convention precondition:</strong> This interceptor assumes that the {@link
 * org.streamrune.core.CommandBus} guarantees that exactly one of {@link #after(CommandContext)} or
 * {@link #onError(CommandContext, Throwable)} will always be called on the same thread after {@link
 * #before(CommandContext)} returns {@code true}. This is required for correct {@link ThreadLocal}
 * cleanup. Any {@link org.streamrune.core.CommandBus} implementation that violates this contract
 * may cause a ThreadLocal leak on pooled threads.
 *
 * <p><strong>Re-entrancy:</strong> the capture is a per-thread STACK, not a single slot. The bus
 * runs {@code after()} in reverse registration order, so {@link InlineProjectionInterceptor}
 * executes user projection code on the calling thread while this interceptor's callback is still
 * pending — and a process-manager projection that dispatches a follow-up command through the same
 * bus runs a NESTED execution inside it. With a flat {@code ThreadLocal} the nested run's {@code
 * before()} overwrote the outer capture and its {@code after()} removed it, so the outer command's
 * audit row was written with {@code userId == null} and {@code correlationId == null}: a
 * privileged, authenticated operation durably recorded as an anonymous request untraceable to its
 * business flow, with no error anywhere.
 *
 * <p><strong>The error message is persisted sanitized.</strong> {@code audit_log} is a
 * persisted-text sink: every message this interceptor writes — the exception's on {@code onError},
 * the veto text on {@code after} — goes through {@link LogSanitizer#sanitizeFreeText(String)}. An
 * exception message can carry caller-supplied text (an idempotency key a reuse rejection names, a
 * field a validation message echoes); verbatim, a CR/LF forges a line for every reader of the
 * table, and a NUL makes the PostgreSQL INSERT fail, so the FAILURE row was lost. The exception the
 * caller receives is untouched.
 */
public final class AuditCommandInterceptor implements CommandInterceptor {

  /** One execution's captured actor. */
  private record Captured(UserId userId, CorrelationId correlationId) {}

  /** What an unmatched {@code after()}/{@code onError()} reads — the pre-stack null behaviour. */
  private static final Captured ANONYMOUS = new Captured(null, null);

  private final AuditStore store;
  // A per-thread stack, not a slot: before(), after() and onError() are all called on the same
  // calling thread, but a nested command execution on that thread must push and pop rather than
  // clobber the frame the outer execution is still using. See the class javadoc.
  private final ThreadLocal<java.util.ArrayDeque<Captured>> captured =
      ThreadLocal.withInitial(java.util.ArrayDeque::new);

  public AuditCommandInterceptor(AuditStore store) {
    if (store == null) throw new IllegalArgumentException("store is required");
    this.store = store;
  }

  @Override
  public boolean before(CommandContext ctx) {
    UserId userId = null;
    CorrelationId correlationId = null;
    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext requestCtx = StreamRuneContext.CURRENT.get();
      if (requestCtx != null) {
        userId = requestCtx.userId();
        correlationId = requestCtx.correlationId();
      }
    }
    captured.get().push(new Captured(userId, correlationId));
    return true;
  }

  /**
   * Pops this execution's capture, removing the ThreadLocal once the stack drains so a pooled
   * platform thread carries no residue. Returns {@link #ANONYMOUS} when there is no matching {@code
   * before()} — the same nulls the pre-stack implementation produced.
   */
  private Captured pop() {
    java.util.ArrayDeque<Captured> stack = captured.get();
    Captured top = stack.poll();
    if (stack.isEmpty()) {
      captured.remove();
    }
    return top != null ? top : ANONYMOUS;
  }

  @Override
  public void after(CommandContext ctx) {
    // Pop FIRST: the frame must be released even if the store throws.
    Captured actor = pop();
    org.streamrune.core.CommandBus.CommandResult result = ctx.result();
    AuditOutcome outcome;
    String errorMessage;
    int eventCount;
    if (result != null && result.vetoed()) {
      // A veto is a REJECTION — a later interceptor's before() returned false, so the
      // command never executed and produced zero events. Recording it as SUCCESS is a false
      // audit record: a compliance or reconciliation reader would conclude the operation was
      // performed. Record a distinct VETOED outcome naming the vetoing interceptor. An
      // IDEMPOTENT_REPLAY, by contrast, IS a success (its events were persisted by the original
      // keyed execution) and stays SUCCESS.
      outcome = AuditOutcome.VETOED;
      errorMessage = LogSanitizer.sanitizeFreeText("vetoed by " + result.shortCircuitedBy());
      eventCount = 0;
    } else {
      outcome = AuditOutcome.SUCCESS;
      errorMessage = null;
      eventCount = result != null ? result.events().size() : 0;
    }
    store.save(
        new AuditEntry(
            ctx.commandId(),
            ctx.commandType(),
            ctx.aggregateType(),
            ctx.aggregateId(),
            actor.userId(),
            ctx.timestamp(),
            outcome,
            errorMessage,
            eventCount,
            actor.correlationId()));
  }

  @Override
  public void onError(CommandContext ctx, Throwable error) {
    Captured actor = pop();
    store.save(
        new AuditEntry(
            ctx.commandId(),
            ctx.commandType(),
            ctx.aggregateType(),
            ctx.aggregateId(),
            actor.userId(),
            ctx.timestamp(),
            AuditOutcome.FAILURE,
            // Persisted free text — sanitized at this sink (see the class javadoc).
            error != null ? LogSanitizer.sanitizeFreeText(error.getMessage()) : null,
            0,
            actor.correlationId()));
  }
}

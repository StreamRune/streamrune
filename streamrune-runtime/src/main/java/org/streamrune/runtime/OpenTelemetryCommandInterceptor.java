package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.streamrune.core.CommandInterceptor;

/**
 * OpenTelemetry {@link CommandInterceptor} that creates a span for each command execution.
 *
 * <p>Register via the framework integration or manually:
 *
 * <pre>{@code
 * var interceptor = new OpenTelemetryCommandInterceptor(openTelemetry);
 * var bus = VirtualThreadCommandBus.builder()
 *     .interceptor(interceptor)
 *     .build();
 * }</pre>
 *
 * <p>This class is opt-in — the {@code opentelemetry-api} dependency must be on the classpath.
 *
 * <p>For {@code executeAsync}: the command span is created on the spawned virtual thread, where no
 * caller context is current — the span would become an orphaned root. Configure {@code
 * asyncTaskWrapper(task -> Context.current().wrap(task))} on the bus builder so the submitting
 * thread's context is captured and made current around the async execution.
 *
 * <p><b>Re-entrancy:</b> the span/scope pair is held on a per-thread STACK. The bus runs {@code
 * after()} in reverse registration order, so {@code InlineProjectionInterceptor} executes user
 * projection code on the calling thread while this interceptor's callback is still pending; a
 * process-manager projection that dispatches a follow-up command through the same bus therefore
 * starts a NESTED span on that thread. With a flat {@code ThreadLocal} the nested run replaced both
 * slots and removed them on completion, so the outer span was never {@code end()}ed and the outer
 * {@link Scope} never {@code close()}d — a pooled platform thread returned to its pool carrying a
 * stale current Context, and every later operation on it parented to a dead span. The stack also
 * guarantees the Scopes close in LIFO order, which the OpenTelemetry context contract requires.
 */
public final class OpenTelemetryCommandInterceptor implements CommandInterceptor {

  private static final String INSTRUMENTATION_SCOPE = "org.streamrune";

  /** One execution's span and the Scope that made it current. */
  private record ActiveSpan(Span span, Scope scope) {}

  private final Tracer tracer;

  // A per-thread stack, not a slot: before()/after()/onError() all run on the same calling thread,
  // but a nested execution on that thread must push and pop rather than clobber the outer frame.
  private final ThreadLocal<java.util.ArrayDeque<ActiveSpan>> stack =
      ThreadLocal.withInitial(java.util.ArrayDeque::new);

  /** Creates an interceptor using the given {@link OpenTelemetry} instance. */
  public OpenTelemetryCommandInterceptor(OpenTelemetry openTelemetry) {
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
  }

  @Override
  public boolean before(CommandContext ctx) {
    Span span =
        tracer
            .spanBuilder(TraceAttributes.commandSpanName(ctx.commandType()))
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(TraceAttributes.COMMAND_TYPE, ctx.commandType())
            .setAttribute(TraceAttributes.COMMAND_ID, ctx.commandId().value())
            .setAttribute(
                TraceAttributes.AGGREGATE_ID,
                ctx.aggregateId() != null ? ctx.aggregateId().value() : null)
            .setAttribute(
                TraceAttributes.AGGREGATE_TYPE,
                ctx.aggregateType() != null ? ctx.aggregateType().value() : null)
            .setAttribute(
                TraceAttributes.STREAM_ID, ctx.streamId() != null ? ctx.streamId().value() : null)
            .startSpan();
    stack.get().push(new ActiveSpan(span, span.makeCurrent()));
    return true;
  }

  @Override
  public void after(CommandContext ctx) {
    finish(null);
  }

  @Override
  public void onError(CommandContext ctx, Throwable error) {
    finish(error);
  }

  private void finish(Throwable error) {
    ActiveSpan active = pop();
    if (active == null) {
      return; // no matching before() — nothing to end or close
    }
    try {
      Span span = active.span();
      if (error != null) {
        span.recordException(error, Attributes.empty());
        span.setStatus(StatusCode.ERROR, error.getMessage());
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end();
    } finally {
      active.scope().close();
    }
  }

  /**
   * Pops this execution's frame, removing the ThreadLocal once the stack drains so a pooled
   * platform thread carries no residue. Returns {@code null} when there is no matching {@code
   * before()}.
   */
  private ActiveSpan pop() {
    java.util.ArrayDeque<ActiveSpan> frames = stack.get();
    ActiveSpan top = frames.poll();
    if (frames.isEmpty()) {
      stack.remove();
    }
    return top;
  }
}

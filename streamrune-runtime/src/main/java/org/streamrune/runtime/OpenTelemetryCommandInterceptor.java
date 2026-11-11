package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayDeque;
import java.util.Objects;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

/**
 * The {@link CommandInterceptor} that starts the span {@code command/{CommandType}} for each
 * command execution, through a {@link StreamRuneTracing} (area {@link TracingArea#COMMAND}).
 *
 * <p>{@link VirtualThreadCommandBus.Builder#tracing(StreamRuneTracing)} adds it to the bus. An
 * application can also register it itself:
 *
 * <pre>{@code
 * var interceptor = new OpenTelemetryCommandInterceptor(openTelemetry);
 * var bus = VirtualThreadCommandBus.builder()
 *     .interceptors(interceptor)
 *     .build();
 * }</pre>
 *
 * <p>The {@link OpenTelemetry} constructor needs the {@code opentelemetry-api} dependency on the
 * classpath; it traces through {@link OpenTelemetryTracing} with every area on. The {@link
 * StreamRuneTracing} constructor follows the tracing's command switch: with the area off, {@code
 * before()} and {@code after()} return at once.
 *
 * <p>For {@code executeAsync}: the command span is created on the spawned virtual thread, where no
 * caller context is current — the span would become an orphaned root. Configure {@code
 * asyncTaskWrapper(task -> Context.current().wrap(task))} on the bus builder so the submitting
 * thread's context is captured and made current around the async execution.
 *
 * <p><b>Re-entrancy:</b> the span scopes are held on a per-thread STACK. The bus runs {@code
 * after()} in reverse registration order, so {@code InlineProjectionInterceptor} executes user
 * projection code on the calling thread while this interceptor's callback is still pending; a
 * process-manager projection that dispatches a follow-up command through the same bus therefore
 * starts a NESTED span on that thread. A flat {@code ThreadLocal} would let the nested run replace
 * the outer scope and remove it on completion, so the outer span would never end and a pooled
 * platform thread would carry a stale current context. The stack ends each span exactly once and
 * closes the scopes in LIFO order, which the OpenTelemetry context contract requires.
 */
public final class OpenTelemetryCommandInterceptor implements CommandInterceptor {

  private final StreamRuneTracing tracing;

  // Read once: before() and finish() must agree on whether a frame was pushed.
  private final boolean traced;

  // A per-thread stack, not a slot: before()/after()/onError() all run on the same calling thread,
  // but a nested execution on that thread must push and pop rather than clobber the outer frame.
  private final ThreadLocal<ArrayDeque<TraceScope>> stack =
      ThreadLocal.withInitial(ArrayDeque::new);

  /**
   * Creates an interceptor that traces through {@code openTelemetry}, as {@code
   * OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled())} does.
   *
   * @param openTelemetry the OpenTelemetry instance
   */
  public OpenTelemetryCommandInterceptor(OpenTelemetry openTelemetry) {
    this(OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled()));
  }

  /**
   * Creates an interceptor that traces through {@code tracing}, when its {@link
   * TracingArea#COMMAND} area is on.
   *
   * @param tracing the tracing
   * @throws NullPointerException if {@code tracing} is {@code null}
   */
  public OpenTelemetryCommandInterceptor(StreamRuneTracing tracing) {
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    this.traced = tracing.enabled(TracingArea.COMMAND);
  }

  @Override
  public boolean before(CommandContext ctx) {
    if (!traced) {
      return true;
    }
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put(TraceNames.COMMAND_TYPE, ctx.commandType())
            .put(TraceNames.COMMAND_ID, ctx.commandId().value())
            .put(
                TraceNames.AGGREGATE_ID,
                ctx.aggregateId() != null ? ctx.aggregateId().value() : null)
            .put(
                TraceNames.AGGREGATE_TYPE,
                ctx.aggregateType() != null ? ctx.aggregateType().value() : null)
            .put(TraceNames.STREAM_ID, ctx.streamId() != null ? ctx.streamId().value() : null)
            .build();
    stack
        .get()
        .push(
            tracing.start(
                TracingArea.COMMAND,
                TraceNames.commandSpanName(ctx.commandType()),
                SpanKind.INTERNAL,
                attributes));
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
    if (!traced) {
      return;
    }
    TraceScope scope = pop();
    if (scope == null) {
      return; // no matching before() — nothing to end or close
    }
    try {
      if (error != null) {
        scope.recordError(error);
      } else {
        scope.recordSuccess();
      }
    } finally {
      scope.close();
    }
  }

  /**
   * Pops this execution's frame, removing the ThreadLocal once the stack drains so a pooled
   * platform thread carries no residue. Returns {@code null} when there is no matching {@code
   * before()}.
   */
  private TraceScope pop() {
    ArrayDeque<TraceScope> frames = stack.get();
    TraceScope top = frames.poll();
    if (frames.isEmpty()) {
      stack.remove();
    }
    return top;
  }
}

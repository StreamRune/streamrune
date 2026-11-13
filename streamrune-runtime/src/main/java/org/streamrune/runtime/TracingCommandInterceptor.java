package org.streamrune.runtime;

import java.util.ArrayDeque;
import java.util.Objects;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.LogSanitizer;

/**
 * The {@link CommandInterceptor} that starts the span {@code command/{CommandType}} for each
 * command execution, through a {@link StreamRuneTracing} (area {@link TracingArea#COMMAND}).
 *
 * <p>{@link VirtualThreadCommandBus.Builder#tracing(StreamRuneTracing)} adds it to the bus. An
 * application can also register it itself:
 *
 * <pre>{@code
 * StreamRuneTracing tracing = OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled());
 * var interceptor = new TracingCommandInterceptor(tracing);
 * var bus = VirtualThreadCommandBus.builder()
 *     .interceptors(interceptor)
 *     .build();
 * }</pre>
 *
 * <p>The interceptor follows the tracing's command switch: with the area off, {@code before()} and
 * {@code after()} return at once. No signature of this class names an OpenTelemetry type, so code
 * that creates it compiles without the {@code opentelemetry-api} dependency; {@link
 * OpenTelemetryTracing#of} is where OpenTelemetry comes in.
 *
 * <p>The span name and attribute keys are those of {@link TraceAttributes}. The command id, the
 * aggregate id and the stream id pass {@link LogSanitizer#sanitizeForTrace(String)}.
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
public final class TracingCommandInterceptor implements CommandInterceptor {

  private final StreamRuneTracing tracing;

  // Read once: before() and finish() must agree on whether a frame was pushed.
  private final boolean traced;

  // A per-thread stack, not a slot: before()/after()/onError() all run on the same calling thread,
  // but a nested execution on that thread must push and pop rather than clobber the outer frame.
  private final ThreadLocal<ArrayDeque<TraceScope>> stack =
      ThreadLocal.withInitial(ArrayDeque::new);

  /**
   * Creates an interceptor that traces through {@code tracing}, when its {@link
   * TracingArea#COMMAND} area is on.
   *
   * @param tracing the tracing
   * @throws NullPointerException if {@code tracing} is {@code null}
   */
  public TracingCommandInterceptor(StreamRuneTracing tracing) {
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
            .put(TraceAttributes.COMMAND_TYPE, ctx.commandType())
            .put(TraceAttributes.COMMAND_ID, LogSanitizer.sanitizeForTrace(ctx.commandId().value()))
            .put(
                TraceAttributes.AGGREGATE_ID,
                ctx.aggregateId() != null
                    ? LogSanitizer.sanitizeForTrace(ctx.aggregateId().value())
                    : null)
            .put(
                TraceAttributes.AGGREGATE_TYPE,
                ctx.aggregateType() != null ? ctx.aggregateType().value() : null)
            .put(
                TraceAttributes.STREAM_ID,
                ctx.streamId() != null
                    ? LogSanitizer.sanitizeForTrace(ctx.streamId().value())
                    : null)
            .build();
    stack
        .get()
        .push(
            tracing.start(
                TracingArea.COMMAND,
                TraceAttributes.commandSpanName(ctx.commandType()),
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
   * The link to the span of the command executing on this thread: the innermost frame of this
   * interceptor's stack, so a nested command sees its own span. {@link SpanLink#NONE} when the
   * command area is off or no command of this interceptor is executing here. {@code
   * VirtualThreadCommandBus} reads it when it builds the metadata of the events a command appends,
   * on the thread that runs the command, between this interceptor's {@code before()} and {@code
   * after()}.
   */
  SpanLink currentLink() {
    if (!traced) {
      return SpanLink.NONE;
    }
    try {
      ArrayDeque<TraceScope> frames = stack.get();
      TraceScope top = frames.peek();
      if (top == null) {
        stack.remove();
        return SpanLink.NONE;
      }
      SpanLink link = top.link();
      return link != null && link.isW3c() ? link : SpanLink.NONE;
    } catch (RuntimeException e) {
      // A tracing that throws from link() leaves the events without a span context; it never
      // fails the command.
      return SpanLink.NONE;
    }
  }

  /** Whether this interceptor starts command spans: its tracing's command area is on. */
  boolean traced() {
    return traced;
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

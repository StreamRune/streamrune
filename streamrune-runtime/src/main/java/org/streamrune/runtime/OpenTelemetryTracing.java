package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.LogSanitizer;

/**
 * The {@link StreamRuneTracing} that records spans through the OpenTelemetry API, under the
 * instrumentation scope {@code org.streamrune}.
 *
 * <pre>{@code
 * StreamRuneTracing tracing = OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled());
 * VirtualThreadCommandBus bus = VirtualThreadCommandBus.builder().tracing(tracing)...build();
 * }</pre>
 *
 * <p>The {@link TracingSettings} are read once, into one {@code boolean} per area, so {@link
 * #enabled(TracingArea)} is an array read. A switched-off area starts no span and builds nothing.
 *
 * <p>An error a scope records ({@link TraceScope#recordError}) becomes the event {@code exception}
 * with the exception's type, its own message and its stack trace, in which a cause or a suppressed
 * exception is named by its type and frames alone: a nested message can quote the data the failing
 * code was handling. The span's status is ERROR, with the exception's own message. A message that
 * is the cause's {@code toString()}, as the cause-only constructors make it, counts as absent. The
 * message is free text that can quote what a caller sent, so it passes {@link
 * LogSanitizer#sanitizeFreeText(String)} and is cut as an identifier is ({@link
 * LogSanitizer#sanitizeForTrace(String)}, {@value LogSanitizer#MAX_TRACE_LENGTH} characters and
 * {@code ...}) wherever it is recorded: the event's message, the first line of its stack trace and
 * the status description. A run of line breaks or control characters becomes one space, and a
 * format character is removed.
 *
 * <p>This is the class of the seam that references OpenTelemetry, and the {@code opentelemetry-api}
 * dependency is optional: an application without it never loads this class as long as it passes
 * {@link StreamRuneTracing#NOOP}, the default of every builder.
 */
public final class OpenTelemetryTracing implements StreamRuneTracing {

  private static final String INSTRUMENTATION_SCOPE = "org.streamrune";

  private static final TracingArea[] AREAS = TracingArea.values();

  private final Tracer tracer;
  private final TracingSettings settings;
  private final boolean[] enabled;

  private OpenTelemetryTracing(OpenTelemetry openTelemetry, TracingSettings settings) {
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.settings = settings;
    this.enabled = new boolean[AREAS.length];
    for (TracingArea area : AREAS) {
      enabled[area.ordinal()] = settings.enabled(area);
    }
  }

  /**
   * The tracing that records the areas {@code settings} switches on through {@code openTelemetry}.
   *
   * @param openTelemetry the OpenTelemetry instance whose tracer records the spans
   * @param settings the switches
   * @return the tracing
   * @throws NullPointerException if either argument is {@code null}
   */
  public static StreamRuneTracing of(OpenTelemetry openTelemetry, TracingSettings settings) {
    Objects.requireNonNull(openTelemetry, "openTelemetry is required");
    Objects.requireNonNull(settings, "settings is required");
    return new OpenTelemetryTracing(openTelemetry, settings);
  }

  @Override
  public boolean enabled(TracingArea area) {
    return enabled[area.ordinal()];
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      java.time.Instant startTime) {
    Objects.requireNonNull(startTime, "startTime is required");
    if (!enabled[area.ordinal()]) {
      return TraceScope.NOOP;
    }
    SpanBuilder builder = tracer.spanBuilder(spanName).setSpanKind(otelKind(kind));
    attributes.forEach((key, value) -> setAttribute(builder, key, value));
    builder.setStartTimestamp(startTime);
    Span span = builder.startSpan();
    return new OpenTelemetryScope(span, Context.current().with(span).makeCurrent());
  }

  /**
   * Starts the span only when the OpenTelemetry context of this thread holds a valid span, one of
   * StreamRune's or another instrumentation's (a server request, a listener's own span); with none
   * it returns {@link TraceScope#NOOP} and starts nothing.
   */
  @Override
  public TraceScope startChild(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      java.time.Instant startTime) {
    Objects.requireNonNull(startTime, "startTime is required");
    if (!enabled[area.ordinal()] || !Span.current().getSpanContext().isValid()) {
      return TraceScope.NOOP;
    }
    return start(area, spanName, kind, attributes, startTime);
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      SpanBaggage baggage) {
    Objects.requireNonNull(link, "link is required; SpanLink.NONE for none");
    Objects.requireNonNull(baggage, "baggage is required; SpanBaggage.KEEP to keep it");
    if (!enabled[area.ordinal()]) {
      return TraceScope.NOOP;
    }
    SpanBuilder builder = tracer.spanBuilder(spanName).setSpanKind(otelKind(kind));
    attributes.forEach((key, value) -> setAttribute(builder, key, value));
    // NONE, and a link whose ids are not W3C ids, start the span without a link: OpenTelemetry
    // never receives a link built from them.
    if (link.isPresent() && link.isW3c()) {
      builder.addLink(
          SpanContext.createFromRemoteParent(
              link.traceId().value(),
              link.spanId().value(),
              TraceFlags.getSampled(),
              TraceState.getDefault()));
    }
    Span span = builder.startSpan();
    Context context = Context.current().with(span);
    if (!baggage.keepsCurrent()) {
      BaggageBuilder baggageBuilder = Baggage.builder();
      baggage.entries().forEach(baggageBuilder::put);
      context = context.with(baggageBuilder.build());
    }
    return new OpenTelemetryScope(span, context.makeCurrent());
  }

  /**
   * {@code task} wrapped in the OpenTelemetry context current on this thread, {@code
   * Context.current().wrap(task)}: the span and the baggage of the caller are current while it
   * runs, on any thread.
   */
  @Override
  public Runnable propagating(Runnable task) {
    return Context.current().wrap(Objects.requireNonNull(task, "task is required"));
  }

  @Override
  public String toString() {
    return "OpenTelemetryTracing{" + settings + "}";
  }

  /** Sets one attribute; {@link SpanAttributes} holds only strings, longs and booleans. */
  private static void setAttribute(SpanBuilder builder, String key, Object value) {
    if (value instanceof String s) {
      builder.setAttribute(key, s);
    } else if (value instanceof Long l) {
      builder.setAttribute(key, l.longValue());
    } else {
      builder.setAttribute(key, (Boolean) value);
    }
  }

  private static io.opentelemetry.api.trace.SpanKind otelKind(SpanKind kind) {
    return switch (kind) {
      case INTERNAL -> io.opentelemetry.api.trace.SpanKind.INTERNAL;
      case SERVER -> io.opentelemetry.api.trace.SpanKind.SERVER;
      case CLIENT -> io.opentelemetry.api.trace.SpanKind.CLIENT;
      case PRODUCER -> io.opentelemetry.api.trace.SpanKind.PRODUCER;
      case CONSUMER -> io.opentelemetry.api.trace.SpanKind.CONSUMER;
    };
  }

  /** The event OpenTelemetry's semantic conventions name for a recorded exception. */
  private static final String EXCEPTION_EVENT = "exception";

  private static final AttributeKey<String> EXCEPTION_TYPE =
      AttributeKey.stringKey("exception.type");
  private static final AttributeKey<String> EXCEPTION_MESSAGE =
      AttributeKey.stringKey("exception.message");
  private static final AttributeKey<String> EXCEPTION_STACKTRACE =
      AttributeKey.stringKey("exception.stacktrace");

  /** How deep the stack trace of a recorded error follows causes and suppressed exceptions. */
  private static final int MAX_CAUSE_DEPTH = 50;

  /**
   * The stack trace of {@code error} as {@link Throwable#printStackTrace()} writes it, but with the
   * message of {@code error} alone: a cause or a suppressed exception is named by its type and its
   * frames. A nested message can quote the data the failing code was handling (a payload value, an
   * SQL parameter), and a span must not carry it; the frames carry none.
   */
  static String stackTraceWithoutCauseMessages(Throwable error) {
    StringBuilder trace = new StringBuilder();
    String message = tracedMessage(error);
    trace.append(error.getClass().getName());
    if (message != null) {
      trace.append(": ").append(message);
    }
    trace.append('\n');
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    seen.add(error);
    appendFrames(trace, error, "", seen, 0);
    return trace.toString();
  }

  /**
   * The message of {@code error} itself, or {@code null}: an exception made with the cause-only
   * constructor ({@code new RuntimeException(cause)}, {@code UncheckedIOException}, {@code
   * CompletionException}) has its cause's {@code toString()} as its message, which quotes the
   * cause's message, so such a message counts as absent.
   */
  static String ownMessage(Throwable error) {
    String message = error.getMessage();
    Throwable cause = error.getCause();
    if (message != null && cause != null && message.equals(cause.toString())) {
      return null;
    }
    return message;
  }

  /**
   * The message of {@code error} as a span records it, or {@code null}: its {@linkplain #ownMessage
   * own message}, sanitized as free text and cut to {@value LogSanitizer#MAX_TRACE_LENGTH}
   * characters followed by {@code ...}.
   */
  static String tracedMessage(Throwable error) {
    return LogSanitizer.sanitizeForTrace(LogSanitizer.sanitizeFreeText(ownMessage(error)));
  }

  private static void appendFrames(
      StringBuilder trace, Throwable error, String indent, Set<Throwable> seen, int depth) {
    for (StackTraceElement frame : error.getStackTrace()) {
      trace.append(indent).append("\tat ").append(frame).append('\n');
    }
    if (depth >= MAX_CAUSE_DEPTH) {
      return;
    }
    for (Throwable suppressed : error.getSuppressed()) {
      appendNested(trace, suppressed, indent + "\t", "Suppressed: ", seen, depth);
    }
    Throwable cause = error.getCause();
    if (cause != null) {
      appendNested(trace, cause, indent, "Caused by: ", seen, depth);
    }
  }

  private static void appendNested(
      StringBuilder trace,
      Throwable nested,
      String indent,
      String label,
      Set<Throwable> seen,
      int depth) {
    trace.append(indent).append(label).append(nested.getClass().getName());
    if (!seen.add(nested)) {
      trace.append(" (already shown)\n");
      return;
    }
    trace.append('\n');
    appendFrames(trace, nested, indent, seen, depth + 1);
  }

  /** A started span and the scope that made it current. */
  private record OpenTelemetryScope(Span span, Scope scope) implements TraceScope {

    @Override
    public void setAttribute(String key, String value) {
      if (value != null) {
        span.setAttribute(key, value);
      }
    }

    @Override
    public void setAttribute(String key, long value) {
      span.setAttribute(key, value);
    }

    @Override
    public void setAttribute(String key, boolean value) {
      span.setAttribute(key, value);
    }

    @Override
    public void recordSuccess() {
      span.setStatus(StatusCode.OK);
    }

    @Override
    public void recordError(Throwable error) {
      String message = tracedMessage(error);
      AttributesBuilder event =
          Attributes.builder()
              .put(EXCEPTION_TYPE, error.getClass().getName())
              .put(EXCEPTION_STACKTRACE, stackTraceWithoutCauseMessages(error));
      if (message != null) {
        event.put(EXCEPTION_MESSAGE, message);
      }
      span.addEvent(EXCEPTION_EVENT, event.build());
      span.setStatus(StatusCode.ERROR, message);
    }

    /**
     * The span's own trace id and span id when its context is valid, sampled or not: a span the
     * sampler drops still has a valid context, so the events its command appends link to it under a
     * parent-based sampler. {@link SpanLink#NONE} for an invalid context.
     */
    @Override
    public SpanLink link() {
      SpanContext context = span.getSpanContext();
      return context.isValid()
          ? SpanLink.ofW3c(context.getTraceId(), context.getSpanId())
          : SpanLink.NONE;
    }

    @Override
    public void close() {
      try {
        span.end();
      } finally {
        scope.close();
      }
    }
  }
}

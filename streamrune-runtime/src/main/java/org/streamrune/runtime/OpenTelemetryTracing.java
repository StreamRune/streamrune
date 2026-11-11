package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

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
      SpanLink link,
      Map<String, String> baggage) {
    if (!enabled[area.ordinal()]) {
      return TraceScope.NOOP;
    }
    SpanBuilder builder = tracer.spanBuilder(spanName).setSpanKind(otelKind(kind));
    attributes.forEach((key, value) -> setAttribute(builder, key, value));
    if (link != null) {
      builder.addLink(
          SpanContext.createFromRemoteParent(
              link.traceId().value(),
              link.spanId().value(),
              TraceFlags.getSampled(),
              TraceState.getDefault()));
    }
    Span span = builder.startSpan();
    Context context = Context.current().with(span);
    if (baggage != null) {
      BaggageBuilder baggageBuilder = Baggage.builder();
      baggage.forEach(baggageBuilder::put);
      context = context.with(baggageBuilder.build());
    }
    return new OpenTelemetryScope(span, context.makeCurrent());
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
      span.recordException(error, Attributes.empty());
      span.setStatus(StatusCode.ERROR, error.getMessage());
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

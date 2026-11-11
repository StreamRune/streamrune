package org.streamrune.core.tracing;

import java.util.Map;

/**
 * Where StreamRune components start their spans: the seam between the framework and a tracing
 * library.
 *
 * <p>A component checks {@link #enabled(TracingArea)} before it builds the attributes of a span, so
 * a switched-off area costs one branch and no allocation:
 *
 * <pre>{@code
 * if (tracing.enabled(TracingArea.COMMAND)) {
 *   try (TraceScope scope = tracing.start(TracingArea.COMMAND, name, SpanKind.INTERNAL, attributes)) {
 *     ...
 *   }
 * }
 * }</pre>
 *
 * <p>{@link #NOOP} traces nothing and is the default of every builder that takes a tracing. {@code
 * org.streamrune.runtime.OpenTelemetryTracing} traces through the OpenTelemetry API, with a master
 * switch and one switch per area.
 *
 * <p>An implementation answers {@link #enabled(TracingArea)} the same way for its whole lifetime: a
 * component may read the answer once, when it is built, and a component that starts a span in one
 * call and ends it in another relies on both calls seeing the same answer. Implementations are
 * thread-safe.
 */
public interface StreamRuneTracing {

  /**
   * Traces nothing: {@link #enabled(TracingArea)} is {@code false} for every area and {@link
   * #start} returns {@link TraceScope#NOOP}. A shared instance that allocates nothing.
   */
  StreamRuneTracing NOOP = new NoopTracing();

  /**
   * Whether spans of {@code area} are recorded.
   *
   * @param area the area
   * @return {@code true} when a span of this area is started
   */
  boolean enabled(TracingArea area);

  /**
   * Starts a span and makes it current on this thread until the returned scope is closed. Returns
   * {@link TraceScope#NOOP} when {@code area} is not {@linkplain #enabled(TracingArea) enabled}.
   *
   * @param area the area the span belongs to
   * @param spanName the span name; it holds types and configured names, never ids
   * @param kind the span kind
   * @param attributes the attributes the span starts with
   * @return the scope to close when the work ends
   */
  default TraceScope start(
      TracingArea area, String spanName, SpanKind kind, SpanAttributes attributes) {
    return start(area, spanName, kind, attributes, null, null);
  }

  /**
   * Starts a span with a link and the baggage to make current with it, and makes it current on this
   * thread until the returned scope is closed. Returns {@link TraceScope#NOOP} when {@code area} is
   * not {@linkplain #enabled(TracingArea) enabled}.
   *
   * @param area the area the span belongs to
   * @param spanName the span name; it holds types and configured names, never ids
   * @param kind the span kind
   * @param attributes the attributes the span starts with
   * @param link the span of another trace this span links to, or {@code null} for none
   * @param baggage the baggage that replaces the current baggage while the scope is open (an empty
   *     map clears it), or {@code null} to keep the current baggage
   * @return the scope to close when the work ends
   */
  TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      Map<String, String> baggage);
}

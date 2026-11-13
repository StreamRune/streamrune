package org.streamrune.core.tracing;

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
 * thread-safe, and for valid arguments they must not throw: neither {@code start} nor any method of
 * the scopes it returns may fail the work it traces. The components guard themselves against an
 * implementation that throws all the same ({@code org.streamrune.runtime.GuardedTracing}): what it
 * throws is caught and logged, and the traced work runs on without the span, so tracing can never
 * change a command, a lock or a delivery.
 *
 * <p><b>No {@code null}s.</b> Callers pass no {@code null}: no attributes is {@link
 * SpanAttributes#empty()}, no link is {@link SpanLink#NONE} and leaving the baggage alone is {@link
 * SpanBaggage#KEEP}. An implementation rejects a {@code null} link, baggage or start time with a
 * {@link NullPointerException}, starts a span for {@code NONE} without any link, so {@code NONE}
 * never reaches an exporter, and changes nothing for {@code KEEP}.
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
   * <p>The span has no link ({@link SpanLink#NONE}) and keeps the current baggage ({@link
   * SpanBaggage#KEEP}).
   *
   * @param area the area the span belongs to
   * @param spanName the span name; it holds types and configured names, never ids
   * @param kind the span kind
   * @param attributes the attributes the span starts with
   * @return the scope to close when the work ends
   */
  default TraceScope start(
      TracingArea area, String spanName, SpanKind kind, SpanAttributes attributes) {
    return start(area, spanName, kind, attributes, SpanLink.NONE, SpanBaggage.KEEP);
  }

  /**
   * Starts a span whose start time is {@code startTime}, earlier than now, and makes it current on
   * this thread until the returned scope is closed. For work that starts a span only once it knows
   * there is something to trace: a read that turned out to return events starts its span with the
   * instant its query started. Returns {@link TraceScope#NOOP} when {@code area} is not {@linkplain
   * #enabled(TracingArea) enabled}.
   *
   * <p>The span has no link ({@link SpanLink#NONE}) and keeps the current baggage ({@link
   * SpanBaggage#KEEP}). The default starts the span now, through {@link #start(TracingArea, String,
   * SpanKind, SpanAttributes)}: an implementation that can set a start time overrides it.
   *
   * @param area the area the span belongs to
   * @param spanName the span name; it holds types and configured names, never ids
   * @param kind the span kind
   * @param attributes the attributes the span starts with
   * @param startTime when the work the span covers started
   * @return the scope to close when the work ends
   * @throws NullPointerException if {@code startTime} is {@code null}
   */
  default TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      java.time.Instant startTime) {
    java.util.Objects.requireNonNull(startTime, "startTime is required");
    return start(area, spanName, kind, attributes);
  }

  /**
   * Starts a span whose start time is {@code startTime} as a child of the span current on this
   * thread, and makes it current until the returned scope is closed; returns {@link
   * TraceScope#NOOP} when no span is current here, or when {@code area} is not {@linkplain
   * #enabled(TracingArea) enabled}. For a span worth having only inside the work that caused it:
   * the event store starts {@code eventstore/read} through it, so a read a request or a listener
   * makes nests under their span, and the poll of a subscription or a projection runner, which runs
   * with no span current, adds no trace of one span that names no consumer.
   *
   * <p>The span has no link and keeps the current baggage. The default starts the span through
   * {@link #start(TracingArea, String, SpanKind, SpanAttributes, java.time.Instant)} whether a span
   * is current or not: an implementation that can tell overrides it.
   *
   * @param area the area the span belongs to
   * @param spanName the span name; it holds types and configured names, never ids
   * @param kind the span kind
   * @param attributes the attributes the span starts with
   * @param startTime when the work the span covers started
   * @return the scope to close when the work ends
   * @throws NullPointerException if {@code startTime} is {@code null}
   */
  default TraceScope startChild(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      java.time.Instant startTime) {
    return start(area, spanName, kind, attributes, startTime);
  }

  /**
   * {@code task} wrapped to run under the trace context current on this thread now, on whichever
   * thread later runs it: work handed to another thread continues the caller's trace instead of
   * starting one of its own. The command bus wraps the task of each {@code executeAsync} command
   * with it, on the submitting thread, so the command's span is a child of the caller's.
   *
   * <p>The default returns {@code task} itself; an implementation whose trace context is bound to a
   * thread overrides it. {@code OpenTelemetryTracing} returns {@code Context.current().wrap(task)}.
   *
   * @param task the work to hand to another thread
   * @return the task to run there
   * @throws NullPointerException if {@code task} is {@code null}
   */
  default Runnable propagating(Runnable task) {
    return java.util.Objects.requireNonNull(task, "task is required");
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
   * @param link the span of another trace this span links to; {@link SpanLink#NONE} for none, which
   *     starts the span without a link
   * @param baggage what the span does with the current baggage while the scope is open: {@link
   *     SpanBaggage#KEEP} keeps it, {@link SpanBaggage#replace(java.util.Map)} replaces it (an
   *     empty map clears it)
   * @return the scope to close when the work ends
   * @throws NullPointerException if {@code link} or {@code baggage} is {@code null}
   */
  TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      SpanBaggage baggage);
}

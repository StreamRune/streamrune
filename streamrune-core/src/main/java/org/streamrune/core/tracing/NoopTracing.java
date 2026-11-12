package org.streamrune.core.tracing;

import java.util.Objects;

/**
 * {@link StreamRuneTracing#NOOP}. It holds no static state, so initializing it never initializes
 * {@link TraceScope}, and the two interfaces can be initialized in either order.
 */
final class NoopTracing implements StreamRuneTracing {

  @Override
  public boolean enabled(TracingArea area) {
    return false;
  }

  @Override
  public TraceScope start(
      TracingArea area, String spanName, SpanKind kind, SpanAttributes attributes) {
    return TraceScope.NOOP;
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
    return TraceScope.NOOP;
  }

  @Override
  public String toString() {
    return "StreamRuneTracing.NOOP";
  }
}

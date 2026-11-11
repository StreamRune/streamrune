package org.streamrune.postgres;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

/** A {@link StreamRuneTracing} that traces every area and records the name of each span started. */
final class RecordingTracing implements StreamRuneTracing {

  final List<String> started = new CopyOnWriteArrayList<>();

  @Override
  public boolean enabled(TracingArea area) {
    return true;
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      Map<String, String> baggage) {
    started.add(spanName);
    return TraceScope.NOOP;
  }
}

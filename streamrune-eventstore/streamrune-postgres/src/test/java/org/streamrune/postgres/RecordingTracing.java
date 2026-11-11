package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;

/**
 * A {@link StreamRuneTracing} that records every span it starts: its name, kind, attributes (those
 * it starts with and those set on its scope), link, status and the span that was open on the same
 * thread when it started, its parent. This module has no OpenTelemetry on its test classpath, so
 * its emitters are tested through the seam they use; {@code OpenTelemetryTracing}'s own tests cover
 * the mapping to OpenTelemetry.
 */
final class RecordingTracing implements StreamRuneTracing {

  /** The names of the spans started, in order. */
  final List<String> started = new CopyOnWriteArrayList<>();

  /** The spans started, in order. */
  final List<RecordedSpan> spans = new CopyOnWriteArrayList<>();

  private final Set<TracingArea> areas;
  private final ThreadLocal<Deque<RecordedSpan>> open = ThreadLocal.withInitial(ArrayDeque::new);

  /** Records every area. */
  RecordingTracing() {
    this(EnumSet.allOf(TracingArea.class));
  }

  private RecordingTracing(Set<TracingArea> areas) {
    this.areas = areas;
  }

  /** Records every area but {@code off}. */
  static RecordingTracing without(TracingArea off) {
    EnumSet<TracingArea> areas = EnumSet.allOf(TracingArea.class);
    areas.remove(off);
    return new RecordingTracing(areas);
  }

  @Override
  public boolean enabled(TracingArea area) {
    return areas.contains(area);
  }

  @Override
  public TraceScope start(
      TracingArea area,
      String spanName,
      SpanKind kind,
      SpanAttributes attributes,
      SpanLink link,
      Map<String, String> baggage) {
    if (!areas.contains(area)) {
      return TraceScope.NOOP;
    }
    started.add(spanName);
    RecordedSpan span = new RecordedSpan(spanName, kind, attributes, link, open.get().peek());
    spans.add(span);
    open.get().push(span);
    return span;
  }

  /** The one span named {@code name}. */
  RecordedSpan only(String name) {
    List<RecordedSpan> named = spans.stream().filter(s -> s.name.equals(name)).toList();
    assertThat(named).as("spans named %s", name).hasSize(1);
    return named.getFirst();
  }

  List<String> names() {
    return List.copyOf(started);
  }

  void clear() {
    started.clear();
    spans.clear();
  }

  /** One recorded span; its scope. */
  final class RecordedSpan implements TraceScope {
    final String name;
    final SpanKind kind;
    final SpanLink link;
    final RecordedSpan parent;
    private final Map<String, Object> attributes =
        Collections.synchronizedMap(new LinkedHashMap<>());
    volatile boolean ok;
    volatile Throwable error;
    volatile boolean closed;

    private RecordedSpan(
        String name, SpanKind kind, SpanAttributes initial, SpanLink link, RecordedSpan parent) {
      this.name = name;
      this.kind = kind;
      this.link = link;
      this.parent = parent;
      initial.forEach(attributes::put);
    }

    Map<String, Object> attributes() {
      synchronized (attributes) {
        return Map.copyOf(attributes);
      }
    }

    Object attribute(String key) {
      return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, String value) {
      if (value != null) {
        attributes.put(key, value);
      }
    }

    @Override
    public void setAttribute(String key, long value) {
      attributes.put(key, value);
    }

    @Override
    public void setAttribute(String key, boolean value) {
      attributes.put(key, value);
    }

    @Override
    public void recordSuccess() {
      ok = true;
    }

    @Override
    public void recordError(Throwable error) {
      this.error = error;
    }

    @Override
    public void close() {
      closed = true;
      open.get().remove(this);
    }
  }
}

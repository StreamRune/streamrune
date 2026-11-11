package org.streamrune.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

class StreamRuneTracingTest {

  @ParameterizedTest
  @EnumSource(TracingArea.class)
  void noop_tracesNoArea(TracingArea area) {
    assertThat(StreamRuneTracing.NOOP.enabled(area)).isFalse();
  }

  @Test
  void noop_startsTheSharedNoopScope_withEitherOverload() {
    TraceScope plain =
        StreamRuneTracing.NOOP.start(
            TracingArea.COMMAND, "command/X", SpanKind.INTERNAL, SpanAttributes.empty());
    TraceScope linked =
        StreamRuneTracing.NOOP.start(
            TracingArea.SUBSCRIPTION,
            "subscription/x/deliver",
            SpanKind.CONSUMER,
            SpanAttributes.builder().put("streamrune.batch.size", 3L).build(),
            new SpanLink(TraceId.of("t"), SpanId.of("s")),
            Map.of("tenant", "acme"));

    assertThat(plain).isSameAs(TraceScope.NOOP);
    assertThat(linked).isSameAs(TraceScope.NOOP);
  }

  @Test
  void theDefaultStart_passesNoLinkAndKeepsTheBaggage() {
    var seen = new Object[2];
    var seenCalled = new boolean[1];
    StreamRuneTracing recording =
        new StreamRuneTracing() {
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
            seenCalled[0] = true;
            seen[0] = link;
            seen[1] = baggage;
            return TraceScope.NOOP;
          }
        };

    recording.start(
        TracingArea.PROJECTION, "projection/p/process", SpanKind.INTERNAL, SpanAttributes.empty());

    assertThat(seenCalled[0]).isTrue();
    assertThat(seen[0]).isNull();
    assertThat(seen[1]).isNull();
  }

  @Test
  void noopScope_doesNothing() {
    TraceScope scope = TraceScope.NOOP;
    assertThatNoException()
        .isThrownBy(
            () -> {
              scope.setAttribute("streamrune.command.type", "X");
              scope.setAttribute("streamrune.command.type", (String) null);
              scope.setAttribute("streamrune.batch.size", 1L);
              scope.setAttribute("streamrune.flag", true);
              scope.recordSuccess();
              scope.recordError(new IllegalStateException("boom"));
              scope.close();
              scope.close();
            });
  }

  @Test
  void noopInstances_nameThemselves() {
    assertThat(StreamRuneTracing.NOOP).hasToString("StreamRuneTracing.NOOP");
    assertThat(TraceScope.NOOP).hasToString("TraceScope.NOOP");
  }

  @Test
  void spanLink_ofNullable_needsBothIds() {
    var trace = TraceId.of("0af7651916cd43dd8448eb211c80319c");
    var span = SpanId.of("b7ad6b7169203331");

    assertThat(SpanLink.ofNullable(trace, span)).isEqualTo(new SpanLink(trace, span));
    assertThat(SpanLink.ofNullable(null, span)).isNull();
    assertThat(SpanLink.ofNullable(trace, null)).isNull();
    assertThat(SpanLink.ofNullable(null, null)).isNull();
  }
}

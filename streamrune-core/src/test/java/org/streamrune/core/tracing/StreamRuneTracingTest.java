package org.streamrune.core.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            SpanBaggage.replace(Map.of("tenant", "acme")));

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
              SpanBaggage baggage) {
            seenCalled[0] = true;
            seen[0] = link;
            seen[1] = baggage;
            return TraceScope.NOOP;
          }
        };

    recording.start(
        TracingArea.PROJECTION, "projection/p/process", SpanKind.INTERNAL, SpanAttributes.empty());

    assertThat(seenCalled[0]).isTrue();
    assertThat(seen[0]).isSameAs(SpanLink.NONE);
    assertThat(seen[1]).isSameAs(SpanBaggage.KEEP);
  }

  @Test
  void theStartTimeDefault_passesNoLinkAndKeepsTheBaggage_andNeedsAStartTime() {
    var seen = new Object[2];
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
              SpanBaggage baggage) {
            seen[0] = link;
            seen[1] = baggage;
            return TraceScope.NOOP;
          }
        };

    recording.start(
        TracingArea.EVENT_STORE,
        "eventstore/read",
        SpanKind.INTERNAL,
        SpanAttributes.empty(),
        java.time.Instant.now());

    assertThat(seen).containsExactly(SpanLink.NONE, SpanBaggage.KEEP);
    assertThatThrownBy(
            () ->
                recording.start(
                    TracingArea.EVENT_STORE,
                    "eventstore/read",
                    SpanKind.INTERNAL,
                    SpanAttributes.empty(),
                    (java.time.Instant) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void noop_rejectsANullLinkOrBaggage() {
    assertThatThrownBy(
            () ->
                StreamRuneTracing.NOOP.start(
                    TracingArea.SUBSCRIPTION,
                    "subscription/x/deliver",
                    SpanKind.CONSUMER,
                    SpanAttributes.empty(),
                    null,
                    SpanBaggage.KEEP))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("SpanLink.NONE");
    assertThatThrownBy(
            () ->
                StreamRuneTracing.NOOP.start(
                    TracingArea.SUBSCRIPTION,
                    "subscription/x/deliver",
                    SpanKind.CONSUMER,
                    SpanAttributes.empty(),
                    SpanLink.NONE,
                    null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("SpanBaggage.KEEP");
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
  void spanLink_none_isTheOneEmptyLink() {
    assertThat(SpanLink.NONE.isPresent()).isFalse();
    assertThat(SpanLink.NONE.isW3c()).isFalse();
    assertThat(new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN)).isPresent()).isTrue();
    assertThatThrownBy(
            () ->
                new SpanLink(
                    TraceId.of("00000000000000000000000000000000"), SpanId.of("0000000000000000")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SpanLink.NONE");
    // One all-zero id alone is an ordinary, if invalid, link.
    assertThat(new SpanLink(TraceId.of("00000000000000000000000000000000"), SpanId.of(SPAN)))
        .satisfies(link -> assertThat(link.isPresent()).isTrue())
        .satisfies(link -> assertThat(link.isW3c()).isFalse());
  }

  @Test
  void spanLink_ofMetadata_readsTheSpanContextOnly() {
    var command = new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN));
    assertThat(SpanLink.of(metadata(null, null, command))).isEqualTo(command);
    assertThat(SpanLink.of(null)).isSameAs(SpanLink.NONE);
    assertThat(SpanLink.of(metadata(null, null, null))).isSameAs(SpanLink.NONE);
    // The caller-supplied trace id and span id are correlation data and never build a link.
    assertThat(SpanLink.of(metadata(TRACE, SPAN, null))).isSameAs(SpanLink.NONE);
    assertThat(
            SpanLink.of(
                metadata(
                    null, null, new SpanLink(TraceId.of("trace-from-a-header"), SpanId.of("s")))))
        .isSameAs(SpanLink.NONE);
  }

  private static org.streamrune.core.EventMetadata metadata(
      String traceId, String spanId, SpanLink spanContext) {
    return new org.streamrune.core.EventMetadata(
        org.streamrune.core.IdGenerator.generateEventId(),
        org.streamrune.core.IdGenerator.generateCommandId(),
        traceId == null ? null : TraceId.of(traceId),
        spanId == null ? null : SpanId.of(spanId),
        org.streamrune.core.types.CorrelationId.of("c"),
        null,
        null,
        java.time.Instant.now(),
        null,
        null,
        spanContext);
  }

  /** A holder of a link, as {@code EventMetadata} and {@code OutboxEntry} hold one. */
  record Holder(
      @com.fasterxml.jackson.annotation.JsonInclude(
              com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY)
          SpanLink link) {}

  @Test
  void spanLink_json_isAnObjectOfTheTwoIds_andNoneIsLeftOut() throws Exception {
    var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    var link = new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN));

    assertThat(mapper.writeValueAsString(new Holder(link)))
        .isEqualTo("{\"link\":{\"traceId\":\"" + TRACE + "\",\"spanId\":\"" + SPAN + "\"}}");
    assertThat(mapper.writeValueAsString(new Holder(SpanLink.NONE))).isEqualTo("{}");
    assertThat(mapper.writeValueAsString(SpanLink.NONE)).isEqualTo("null");
    assertThat(mapper.readValue(mapper.writeValueAsString(link), SpanLink.class)).isEqualTo(link);
    assertThat(mapper.readValue("{\"traceId\":\"zz\",\"spanId\":\"" + SPAN + "\"}", SpanLink.class))
        .isSameAs(SpanLink.NONE);
    assertThat(mapper.readValue("{}", SpanLink.class)).isSameAs(SpanLink.NONE);
  }

  @Test
  void spanBaggage_keepsOrReplaces() {
    assertThat(SpanBaggage.KEEP.keepsCurrent()).isTrue();
    assertThatThrownBy(SpanBaggage.KEEP::entries).isInstanceOf(IllegalStateException.class);
    var source = new java.util.LinkedHashMap<String, String>();
    source.put("tenant", "acme");
    source.put("region", "eu");
    SpanBaggage replace = SpanBaggage.replace(source);
    source.put("late", "x");
    assertThat(replace.keepsCurrent()).isFalse();
    assertThat(replace.entries())
        .containsExactly(Map.entry("tenant", "acme"), Map.entry("region", "eu"));
    assertThatThrownBy(() -> replace.entries().put("k", "v"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(SpanBaggage.replace(Map.of()).entries()).isEmpty();
    assertThat(SpanBaggage.replace(Map.of("a", "1")))
        .isEqualTo(SpanBaggage.replace(Map.of("a", "1")));
    assertThat(SpanBaggage.replace(Map.of())).isNotEqualTo(SpanBaggage.KEEP);
    assertThat(SpanBaggage.KEEP).hasToString("SpanBaggage.KEEP");
    assertThat(SpanBaggage.replace(Map.of("tenant", "secret-value")).toString())
        .contains("tenant")
        .doesNotContain("secret-value");
  }

  @Test
  void spanBaggage_rejectsNulls() {
    assertThatThrownBy(() -> SpanBaggage.replace(null)).isInstanceOf(NullPointerException.class);
    var nullKey = new java.util.HashMap<String, String>();
    nullKey.put(null, "v");
    assertThatThrownBy(() -> SpanBaggage.replace(nullKey)).isInstanceOf(NullPointerException.class);
    var nullValue = new java.util.HashMap<String, String>();
    nullValue.put("k", null);
    assertThatThrownBy(() -> SpanBaggage.replace(nullValue))
        .isInstanceOf(NullPointerException.class);
  }

  private static final String TRACE = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN = "b7ad6b7169203331";

  @Test
  void spanLink_ofW3c_linksValidIds() {
    assertThat(SpanLink.ofW3c(TRACE, SPAN))
        .isEqualTo(new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN)));
    assertThat(new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN)).isW3c()).isTrue();
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource(
      nullValues = "NULL",
      value = {
        "NULL, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, NULL",
        "0af7651916cd43dd8448eb211c80319, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319cc, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, b7ad6b716920333",
        "0af7651916cd43dd8448eb211c80319c, b7ad6b71692033311",
        "0AF7651916CD43DD8448EB211C80319C, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, B7AD6B7169203331",
        "0af7651916cd43dd8448eb211c80319g, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, b7ad6b716920333z",
        "0af7651916cd43dd8448eb211c80319/, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, b7ad6b716920333:",
        "00000000000000000000000000000000, b7ad6b7169203331",
        "0af7651916cd43dd8448eb211c80319c, 0000000000000000",
        "'', ''"
      })
  void spanLink_ofW3c_givesNoLinkForMalformedIds(String traceId, String spanId) {
    assertThat(SpanLink.ofW3c(traceId, spanId)).isSameAs(SpanLink.NONE);
    if (traceId != null && !traceId.isBlank() && spanId != null && !spanId.isBlank()) {
      assertThat(new SpanLink(TraceId.of(traceId), SpanId.of(spanId)).isW3c()).isFalse();
    }
  }
}

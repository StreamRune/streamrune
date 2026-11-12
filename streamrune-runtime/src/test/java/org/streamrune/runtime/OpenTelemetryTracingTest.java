package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

class OpenTelemetryTracingTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private StreamRuneTracing tracing(TracingSettings settings) {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), settings);
  }

  @Test
  void anEnabledArea_recordsTheSpanWithItsNameKindAndAttributes() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());

    try (TraceScope _ =
        tracing.start(
            TracingArea.COMMAND,
            "command/PlaceOrder",
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put("streamrune.command.type", "PlaceOrder")
                .put("streamrune.batch.size", 2L)
                .put("streamrune.snapshot.used", true)
                .build())) {
      // the work
    }

    SpanData span = onlySpan();
    assertThat(span.getName()).isEqualTo("command/PlaceOrder");
    assertThat(span.getKind()).isEqualTo(io.opentelemetry.api.trace.SpanKind.INTERNAL);
    assertThat(span.getInstrumentationScopeInfo().getName()).isEqualTo("org.streamrune");
    assertThat(span.getAttributes().get(AttributeKey.stringKey("streamrune.command.type")))
        .isEqualTo("PlaceOrder");
    assertThat(span.getAttributes().get(AttributeKey.longKey("streamrune.batch.size")))
        .isEqualTo(2L);
    assertThat(span.getAttributes().get(AttributeKey.booleanKey("streamrune.snapshot.used")))
        .isTrue();
    assertThat(span.getAttributes().size()).isEqualTo(3);
    assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
    assertThat(span.hasEnded()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(SpanKind.class)
  void everyKind_mapsToTheOpenTelemetryKindOfTheSameName(SpanKind kind) {
    tracing(TracingSettings.allEnabled())
        .start(TracingArea.OUTBOX, "outbox/publish", kind, SpanAttributes.empty())
        .close();

    assertThat(onlySpan().getKind().name()).isEqualTo(kind.name());
  }

  @Test
  void theMasterSwitchOff_recordsNothing() {
    StreamRuneTracing tracing = tracing(TracingSettings.disabled());

    for (TracingArea area : TracingArea.values()) {
      assertThat(tracing.enabled(area)).isFalse();
      TraceScope scope = tracing.start(area, "x", SpanKind.INTERNAL, SpanAttributes.empty());
      assertThat(scope).isSameAs(TraceScope.NOOP);
      scope.close();
    }
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void anAreaSwitchedOff_recordsNothingOfThatArea_andTheOthersStill() {
    StreamRuneTracing tracing =
        tracing(TracingSettings.builder().area(TracingArea.PROJECTION, false).build());

    assertThat(tracing.enabled(TracingArea.PROJECTION)).isFalse();
    assertThat(tracing.enabled(TracingArea.COMMAND)).isTrue();
    tracing
        .start(
            TracingArea.PROJECTION,
            "projection/p/process",
            SpanKind.INTERNAL,
            SpanAttributes.empty())
        .close();
    tracing
        .start(TracingArea.COMMAND, "command/X", SpanKind.INTERNAL, SpanAttributes.empty())
        .close();

    assertThat(otelTesting.getSpans()).extracting(SpanData::getName).containsExactly("command/X");
  }

  @Test
  void theSpanIsCurrentUntilTheScopeCloses() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    String inside;
    try (TraceScope _ =
        tracing.start(
            TracingArea.COMMAND, "command/X", SpanKind.INTERNAL, SpanAttributes.empty())) {
      inside = Span.current().getSpanContext().getSpanId();
    }

    assertThat(inside).isEqualTo(onlySpan().getSpanId());
    assertThat(Span.current().getSpanContext().isValid()).isFalse();
  }

  @Test
  void aLink_pointsAtTheLinkedSpan() {
    tracing(TracingSettings.allEnabled())
        .start(
            TracingArea.SUBSCRIPTION,
            "subscription/s/deliver",
            SpanKind.CONSUMER,
            SpanAttributes.empty(),
            new SpanLink(
                TraceId.of("0af7651916cd43dd8448eb211c80319c"), SpanId.of("b7ad6b7169203331")),
            SpanBaggage.KEEP)
        .close();

    SpanData span = onlySpan();
    assertThat(span.getLinks()).hasSize(1);
    assertThat(span.getLinks().getFirst().getSpanContext().getTraceId())
        .isEqualTo("0af7651916cd43dd8448eb211c80319c");
    assertThat(span.getLinks().getFirst().getSpanContext().getSpanId())
        .isEqualTo("b7ad6b7169203331");
  }

  @Test
  void aSpanTheSamplerDrops_stillHasALink_whenItsContextIsValid() {
    try (var provider =
        io.opentelemetry.sdk.trace.SdkTracerProvider.builder()
            .setSampler(io.opentelemetry.sdk.trace.samplers.Sampler.alwaysOff())
            .build()) {
      StreamRuneTracing dropped =
          OpenTelemetryTracing.of(
              io.opentelemetry.sdk.OpenTelemetrySdk.builder().setTracerProvider(provider).build(),
              TracingSettings.allEnabled());
      SpanLink link;
      boolean recording;
      try (TraceScope scope =
          dropped.start(
              TracingArea.COMMAND, "command/X", SpanKind.INTERNAL, SpanAttributes.empty())) {
        link = scope.link();
        recording = Span.current().isRecording();
      }
      assertThat(recording).isFalse();
      assertThat(link.isPresent()).isTrue();
      assertThat(link.isW3c()).isTrue();
    }
  }

  @Test
  void none_startsASpanWithNoLink() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    tracing
        .start(
            TracingArea.SUBSCRIPTION,
            "subscription/s/deliver",
            SpanKind.CONSUMER,
            SpanAttributes.empty(),
            SpanLink.NONE,
            SpanBaggage.KEEP)
        .close();
    tracing
        .start(
            TracingArea.SUBSCRIPTION,
            "subscription/s/deliver",
            SpanKind.CONSUMER,
            SpanAttributes.empty(),
            new SpanLink(TraceId.of("trace-from-a-header"), SpanId.of("span")),
            SpanBaggage.KEEP)
        .close();

    assertThat(otelTesting.getSpans())
        .hasSize(2)
        .allSatisfy(span -> assertThat(span.getLinks()).isEmpty());
  }

  @Test
  void aNullLinkOrBaggage_isRejected() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    assertThatThrownBy(
            () ->
                tracing.start(
                    TracingArea.SUBSCRIPTION,
                    "s",
                    SpanKind.CONSUMER,
                    SpanAttributes.empty(),
                    null,
                    SpanBaggage.KEEP))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("SpanLink.NONE");
    assertThatThrownBy(
            () ->
                tracing.start(
                    TracingArea.SUBSCRIPTION,
                    "s",
                    SpanKind.CONSUMER,
                    SpanAttributes.empty(),
                    SpanLink.NONE,
                    null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("SpanBaggage.KEEP");
    assertThatThrownBy(
            () ->
                tracing.start(
                    TracingArea.EVENT_STORE,
                    "eventstore/read",
                    SpanKind.INTERNAL,
                    SpanAttributes.empty(),
                    (java.time.Instant) null))
        .isInstanceOf(NullPointerException.class);
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void baggage_replacesTheCurrentBaggage_andKeepKeepsIt() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    try (Scope _ = Baggage.builder().put("outer", "kept").build().makeCurrent()) {
      try (TraceScope _ =
          tracing.start(
              TracingArea.SUBSCRIPTION,
              "s",
              SpanKind.CONSUMER,
              SpanAttributes.empty(),
              SpanLink.NONE,
              SpanBaggage.replace(Map.of("tenant", "acme")))) {
        assertThat(Baggage.current().getEntryValue("tenant")).isEqualTo("acme");
        assertThat(Baggage.current().getEntryValue("outer")).isNull();
      }
      try (TraceScope _ =
          tracing.start(
              TracingArea.SUBSCRIPTION,
              "s",
              SpanKind.CONSUMER,
              SpanAttributes.empty(),
              SpanLink.NONE,
              SpanBaggage.replace(Map.of()))) {
        assertThat(Baggage.current().isEmpty()).isTrue();
      }
      try (TraceScope _ =
          tracing.start(TracingArea.SUBSCRIPTION, "s", SpanKind.CONSUMER, SpanAttributes.empty())) {
        assertThat(Baggage.current().getEntryValue("outer")).isEqualTo("kept");
      }
      try (TraceScope _ =
          tracing.start(
              TracingArea.SUBSCRIPTION,
              "s",
              SpanKind.CONSUMER,
              SpanAttributes.empty(),
              SpanLink.NONE,
              SpanBaggage.KEEP)) {
        assertThat(Baggage.current().getEntryValue("outer")).isEqualTo("kept");
      }
      assertThat(Baggage.current().getEntryValue("outer")).isEqualTo("kept");
    }
  }

  @Test
  void recordSuccess_andRecordError_setTheStatus() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    try (TraceScope scope =
        tracing.start(TracingArea.COMMAND, "ok", SpanKind.INTERNAL, SpanAttributes.empty())) {
      scope.recordSuccess();
    }
    try (TraceScope scope =
        tracing.start(TracingArea.COMMAND, "failed", SpanKind.INTERNAL, SpanAttributes.empty())) {
      scope.recordError(new IllegalStateException("boom"));
    }

    SpanData ok = otelTesting.getSpans().get(0);
    SpanData failed = otelTesting.getSpans().get(1);
    assertThat(ok.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    assertThat(failed.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(failed.getStatus().getDescription()).isEqualTo("boom");
    assertThat(failed.getEvents()).extracting(e -> e.getName()).containsExactly("exception");
  }

  @Test
  void attributesSetOnTheScope_landOnTheSpan() {
    try (TraceScope scope =
        tracing(TracingSettings.allEnabled())
            .start(TracingArea.OUTBOX, "outbox/relay", SpanKind.INTERNAL, SpanAttributes.empty())) {
      scope.setAttribute("streamrune.outbox.destination", "orders");
      scope.setAttribute("streamrune.outbox.payload.type", (String) null);
      scope.setAttribute("streamrune.outbox.published", 4L);
      scope.setAttribute("streamrune.snapshot.used", false);
    }

    SpanData span = onlySpan();
    assertThat(span.getAttributes().size()).isEqualTo(3);
    assertThat(span.getAttributes().get(AttributeKey.stringKey("streamrune.outbox.destination")))
        .isEqualTo("orders");
    assertThat(span.getAttributes().get(AttributeKey.longKey("streamrune.outbox.published")))
        .isEqualTo(4L);
    assertThat(span.getAttributes().get(AttributeKey.booleanKey("streamrune.snapshot.used")))
        .isFalse();
  }

  @Test
  void of_refusesMissingArguments_andNamesItsSettings() {
    assertThatThrownBy(() -> OpenTelemetryTracing.of(null, TracingSettings.allEnabled()))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), null))
        .isInstanceOf(NullPointerException.class);
    assertThat(tracing(TracingSettings.disabled()).toString())
        .startsWith("OpenTelemetryTracing{")
        .contains("enabled=false");
  }

  private static SpanData onlySpan() {
    assertThat(otelTesting.getSpans()).hasSize(1);
    return otelTesting.getSpans().getFirst();
  }
}

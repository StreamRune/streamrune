package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.Version;

class TracingEventListenerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private EventEnvelope envelope(TraceId traceId) {
    return envelope(traceId, org.streamrune.core.tracing.SpanLink.NONE);
  }

  private EventEnvelope envelope(
      TraceId traceId, org.streamrune.core.tracing.SpanLink spanContext) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        StreamId.of(TYPE, AggregateId.of("stream-1")),
        new Version(1),
        new EventType("TestEvent"),
        new org.streamrune.core.DomainEvent() {},
        new EventMetadata(
            EventId.of("evt-1"),
            CommandId.of("cmd-1"),
            traceId,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now(),
            null,
            null,
            spanContext));
  }

  @Test
  void shouldCreateSpanWithCorrectNameAndKind() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    listener.onEvents(List.of(envelope(null)));

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(1, spans.size());
    SpanData span = spans.getFirst();
    assertEquals("subscription/orders/deliver", span.getName());
    assertEquals(SpanKind.CONSUMER, span.getKind());
  }

  @Test
  void shouldSetSubscriptionNameAndBatchSizeAttributes() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    listener.onEvents(List.of(envelope(null), envelope(null)));

    SpanData span = otelTesting.getSpans().getFirst();
    assertEquals(
        "orders",
        span.getAttributes().get(AttributeKey.stringKey(TraceAttributes.SUBSCRIPTION_NAME)));
    assertEquals(
        2L, (long) span.getAttributes().get(AttributeKey.longKey(TraceAttributes.BATCH_SIZE)));
    // Drift guard: the subscription span is now a published contract too, so pin its
    // exact key set the same way the command span's is pinned.
    assertEquals(
        java.util.Set.of("streamrune.subscription.name", "streamrune.batch.size"),
        span.getAttributes().asMap().keySet().stream()
            .map(AttributeKey::getKey)
            .collect(java.util.stream.Collectors.toSet()));
    assertEquals(
        TraceAttributes.subscriptionDeliverSpanName(SubscriptionName.of("orders")), span.getName());
  }

  @Test
  void shouldLinkToOriginatingCommandTrace() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    var commandSpan =
        new org.streamrune.core.tracing.SpanLink(
            new TraceId("0af7651916cd43dd8448eb211c80319c"), new SpanId("b7ad6b7169203331"));

    listener.onEvents(List.of(envelope(null, commandSpan)));

    SpanData span = otelTesting.getSpans().getFirst();
    assertEquals(1, span.getLinks().size());
    var link = span.getLinks().getFirst();
    assertEquals("0af7651916cd43dd8448eb211c80319c", link.getSpanContext().getTraceId());
    assertEquals("b7ad6b7169203331", link.getSpanContext().getSpanId());
  }

  @Test
  void theCallerSuppliedTraceId_neverBuildsALink() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    listener.onEvents(List.of(envelope(new TraceId("0af7651916cd43dd8448eb211c80319c"))));

    assertTrue(otelTesting.getSpans().getFirst().getLinks().isEmpty());
  }

  @Test
  void shouldNotLinkWhenTraceIdAbsent() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    listener.onEvents(List.of(envelope(null)));

    SpanData span = otelTesting.getSpans().getFirst();
    assertTrue(span.getLinks().isEmpty());
  }

  @Test
  void shouldRecordExceptionAndSetErrorStatus() {
    EventListener delegate = mock(EventListener.class);
    doThrow(new RuntimeException("delivery failed")).when(delegate).onEvents(any());
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    assertThrows(RuntimeException.class, () -> listener.onEvents(List.of(envelope(null))));

    SpanData span = otelTesting.getSpans().getFirst();
    assertEquals(StatusCode.ERROR, span.getStatus().getStatusCode());
    assertEquals("delivery failed", span.getStatus().getDescription());
    assertFalse(span.getEvents().isEmpty());
    assertEquals("exception", span.getEvents().getFirst().getName());
  }

  @Test
  void shouldInvokeDelegateExactlyOnce() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));
    var events = List.of(envelope(null));

    listener.onEvents(events);

    verify(delegate, times(1)).onEvents(events);
  }

  @Test
  void shouldRestoreBaggageFromEventMetadata() {
    EventListener delegate = mock(EventListener.class);
    var listener =
        new TracingEventListener(
            delegate,
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()),
            SubscriptionName.of("orders"));

    var envelope =
        new EventEnvelope(
            GlobalOffset.of(1),
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            new Version(1),
            new EventType("TestEvent"),
            new org.streamrune.core.DomainEvent() {},
            new EventMetadata(
                EventId.of("evt-1"),
                CommandId.of("cmd-1"),
                null,
                CorrelationId.of("corr-1"),
                null,
                null,
                Instant.now(),
                Map.of("tenant", "acme", "feature-flag", "enabled")));

    var capturedBaggage = new java.util.concurrent.atomic.AtomicReference<Map<String, String>>();
    doAnswer(
            invocation -> {
              var baggage = io.opentelemetry.api.baggage.Baggage.current();
              var entries = new java.util.HashMap<String, String>();
              baggage.forEach((key, entry) -> entries.put(key, entry.getValue()));
              capturedBaggage.set(entries);
              return null;
            })
        .when(delegate)
        .onEvents(any());

    listener.onEvents(List.of(envelope));

    assertNotNull(capturedBaggage.get());
    assertEquals("acme", capturedBaggage.get().get("tenant"));
    assertEquals("enabled", capturedBaggage.get().get("feature-flag"));
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryProjectionRepository;

class TracingProjectionDecoratorTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  @Test
  void shouldCreateSpanPerBatch() {
    Projection delegate = mock(Projection.class);
    var decorator =
        new TracingProjectionDecorator(
            ProjectionName.of("OrdersProjection"), delegate, otelTesting.getOpenTelemetry());

    decorator.process(List.of());

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);
    assertEquals("projection/OrdersProjection/process", span.getName());
    assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
    assertEquals(0L, (long) span.getAttributes().get(TraceAttributes.BATCH_SIZE));
    // Drift guard: pin the projection span's exact key set alongside the command and
    // subscription spans, so all three tracing emitters stay on the same published contract.
    assertEquals(
        java.util.Set.of("streamrune.projection.name", "streamrune.batch.size"),
        span.getAttributes().asMap().keySet().stream()
            .map(io.opentelemetry.api.common.AttributeKey::getKey)
            .collect(java.util.stream.Collectors.toSet()));

    verify(delegate).process(List.of());
  }

  @Test
  void shouldSetErrorStatusWhenDelegateFails() {
    Projection delegate = mock(Projection.class);
    doThrow(new RuntimeException("DB error")).when(delegate).process(any());
    var decorator =
        new TracingProjectionDecorator(
            ProjectionName.of("OrdersProjection"), delegate, otelTesting.getOpenTelemetry());

    assertThrows(RuntimeException.class, () -> decorator.process(List.of()));

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(1, spans.size());
    assertEquals(StatusCode.ERROR, spans.get(0).getStatus().getStatusCode());
    assertFalse(spans.get(0).getEvents().isEmpty()); // recordException called
    assertEquals("exception", spans.get(0).getEvents().get(0).getName());
    assertEquals("DB error", spans.get(0).getStatus().getDescription());
  }

  // This decorator overrides process(List, ProjectionRepository) only
  // to wrap it in a span, so writesThroughRepository() must forward to the delegate's own answer
  // rather than reading true reflectively off this class.
  @Test
  void writesThroughRepository_forwardsDelegatesAnswer() {
    Projection delegateTrue = mock(Projection.class);
    when(delegateTrue.writesThroughRepository()).thenReturn(true);
    var tracedTrue =
        new TracingProjectionDecorator(
            ProjectionName.of("A"), delegateTrue, otelTesting.getOpenTelemetry());
    assertTrue(tracedTrue.writesThroughRepository());

    Projection delegateFalse = mock(Projection.class);
    when(delegateFalse.writesThroughRepository()).thenReturn(false);
    var tracedFalse =
        new TracingProjectionDecorator(
            ProjectionName.of("B"), delegateFalse, otelTesting.getOpenTelemetry());
    assertFalse(tracedFalse.writesThroughRepository());
  }

  @Test
  void writeTarget_isForwardedToTheDelegate() {
    var repo = new InMemoryProjectionRepository();
    var writeThrough =
        new BaseProjection(repo, "orders") {
          @Override
          public void process(List<EventEnvelope> batch) {}
        };
    var traced =
        new TracingProjectionDecorator(
            ProjectionName.of("orders"), writeThrough, otelTesting.getOpenTelemetry());
    assertThat(traced.writeTarget()).containsSame(repo);

    Projection lambda = batch -> {};
    var tracedLambda =
        new TracingProjectionDecorator(
            ProjectionName.of("audit"), lambda, otelTesting.getOpenTelemetry());
    assertThat(tracedLambda.writeTarget()).isEmpty();
  }
}

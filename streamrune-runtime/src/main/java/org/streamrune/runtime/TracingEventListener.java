package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.SubscriptionName;

/**
 * Decorates an {@link EventListener} to create an OpenTelemetry span for each event delivery batch.
 *
 * <p>When the first event's {@link EventMetadata} carries a non-null {@code traceId} and {@code
 * spanId}, the span includes a link back to the originating command trace.
 */
public final class TracingEventListener implements EventListener {

  private static final String INSTRUMENTATION_SCOPE = "org.streamrune";

  private final EventListener delegate;
  private final Tracer tracer;
  private final SubscriptionName subscriptionName;

  /**
   * Creates a tracing event listener decorator.
   *
   * @param delegate the real event listener to delegate to
   * @param openTelemetry the OTel SDK instance
   * @param subscriptionName the subscription name, used in the span name
   */
  public TracingEventListener(
      EventListener delegate, OpenTelemetry openTelemetry, SubscriptionName subscriptionName) {
    this.delegate = delegate;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    this.subscriptionName = subscriptionName;
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    SpanBuilder builder =
        tracer
            .spanBuilder(TraceAttributes.subscriptionDeliverSpanName(subscriptionName))
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute(TraceAttributes.SUBSCRIPTION_NAME, subscriptionName.value())
            .setAttribute(TraceAttributes.BATCH_SIZE, (long) events.size());

    if (!events.isEmpty()) {
      EventMetadata metadata = events.getFirst().metadata();
      if (metadata.traceId() != null && metadata.spanId() != null) {
        SpanContext origin =
            SpanContext.createFromRemoteParent(
                metadata.traceId().value(),
                metadata.spanId().value(),
                TraceFlags.getSampled(),
                TraceState.getDefault());
        builder.addLink(origin);
      }
    }

    Span span = builder.startSpan();

    // Build baggage from event metadata
    BaggageBuilder baggageBuilder = Baggage.builder();
    if (!events.isEmpty()) {
      events.getFirst().metadata().baggage().forEach(baggageBuilder::put);
    }
    Context context = Context.current().with(span).with(baggageBuilder.build());

    try (var _ = context.makeCurrent()) {
      delegate.onEvents(events);
    } catch (Throwable t) {
      span.recordException(t, Attributes.empty());
      span.setStatus(StatusCode.ERROR, t.getMessage());
      throw t;
    } finally {
      span.end();
    }
  }
}

package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.SubscriptionName;

/**
 * Decorates an {@link EventListener} to start the span {@code subscription/{name}/deliver} for each
 * delivery batch, through a {@link StreamRuneTracing} (area {@link TracingArea#SUBSCRIPTION}).
 *
 * <p>When the first event's {@link EventMetadata} carries a {@code traceId} and a {@code spanId},
 * the span links back to the originating command trace. The first event's baggage is the current
 * baggage while the delegate runs.
 *
 * <p>The subscription builders wrap their listener through {@link #traced} when they are given a
 * tracing. An application can also wrap a listener itself; the {@link OpenTelemetry} constructor
 * needs the {@code opentelemetry-api} dependency on the classpath and traces with every area on.
 */
public final class TracingEventListener implements EventListener {

  private final EventListener delegate;
  private final StreamRuneTracing tracing;
  private final SubscriptionName subscriptionName;
  private final boolean traced;

  /**
   * Creates a tracing event listener decorator that traces through {@code openTelemetry}, as {@code
   * OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled())} does.
   *
   * @param delegate the real event listener to delegate to
   * @param openTelemetry the OTel SDK instance
   * @param subscriptionName the subscription name, used in the span name
   */
  public TracingEventListener(
      EventListener delegate, OpenTelemetry openTelemetry, SubscriptionName subscriptionName) {
    this(
        delegate,
        OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled()),
        subscriptionName);
  }

  /**
   * Creates a tracing event listener decorator that traces through {@code tracing}, when its {@link
   * TracingArea#SUBSCRIPTION} area is on; with the area off it only delegates.
   *
   * @param delegate the real event listener to delegate to
   * @param tracing the tracing
   * @param subscriptionName the subscription name, used in the span name
   * @throws NullPointerException if an argument is {@code null}
   */
  public TracingEventListener(
      EventListener delegate, StreamRuneTracing tracing, SubscriptionName subscriptionName) {
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    this.subscriptionName =
        Objects.requireNonNull(subscriptionName, "subscriptionName is required");
    this.traced = tracing.enabled(TracingArea.SUBSCRIPTION);
  }

  /**
   * The listener a subscription delivers to under {@code tracing}: {@code listener} wrapped in a
   * {@code TracingEventListener} when the {@link TracingArea#SUBSCRIPTION} area is on, else {@code
   * listener} itself. A listener that already is a {@code TracingEventListener}, or a {@code null}
   * listener or name, is returned as it is, so a subscription never traces a delivery twice and its
   * own checks still report a missing argument.
   *
   * @param listener the listener, or {@code null}
   * @param tracing the tracing; {@code null} traces nothing
   * @param subscriptionName the subscription name, or {@code null}
   * @return the listener to deliver to
   */
  public static EventListener traced(
      EventListener listener, StreamRuneTracing tracing, SubscriptionName subscriptionName) {
    if (listener == null
        || subscriptionName == null
        || tracing == null
        || !tracing.enabled(TracingArea.SUBSCRIPTION)
        || listener instanceof TracingEventListener) {
      return listener;
    }
    return new TracingEventListener(listener, tracing, subscriptionName);
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    if (!traced) {
      delegate.onEvents(events);
      return;
    }
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put(TraceNames.SUBSCRIPTION_NAME, subscriptionName.value())
            .put(TraceNames.BATCH_SIZE, (long) events.size())
            .build();
    SpanLink link = null;
    Map<String, String> baggage = Map.of();
    if (!events.isEmpty()) {
      EventMetadata metadata = events.getFirst().metadata();
      link = SpanLink.ofNullable(metadata.traceId(), metadata.spanId());
      baggage = metadata.baggage();
    }
    TraceScope scope =
        tracing.start(
            TracingArea.SUBSCRIPTION,
            TraceNames.subscriptionDeliverSpanName(subscriptionName),
            SpanKind.CONSUMER,
            attributes,
            link,
            baggage);
    try {
      delegate.onEvents(events);
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }
}

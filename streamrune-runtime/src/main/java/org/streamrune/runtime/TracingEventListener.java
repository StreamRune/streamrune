package org.streamrune.runtime;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.SubscriptionName;

/**
 * Decorates an {@link EventListener} to start the span {@code subscription/{name}/deliver} for each
 * delivery batch, through a {@link StreamRuneTracing} (area {@link TracingArea#SUBSCRIPTION}).
 *
 * <p>When the first event's {@link EventMetadata} carries the span of the command that appended it
 * ({@link EventMetadata#spanContext()}, which the command bus writes from its own command span),
 * the span links back to that command ({@link SpanLink#of(EventMetadata)}). The caller-supplied
 * {@code traceId} of the metadata never builds a link.
 *
 * <p>The delegate runs with no baggage: the span clears the current OpenTelemetry baggage and takes
 * none from the events. A batch holds events of many callers, and the baggage of one of them (the
 * allow-listed W3C entries and, in the trusted-gateway mode, the caller's {@code role}) would
 * otherwise travel, in the {@code baggage} header of every instrumented call the delegate makes, on
 * behalf of all the others. A delegate that needs an event's baggage reads it from that event's
 * {@link EventMetadata#baggage()}.
 *
 * <p>The subscription builders wrap their listener through {@link #traced} when they are given a
 * tracing. An application can also wrap a listener itself, with {@link #traced} or the constructor.
 * No signature of this class names an OpenTelemetry type, so code that creates it compiles without
 * the {@code opentelemetry-api} dependency; {@link OpenTelemetryTracing#of} is where OpenTelemetry
 * comes in.
 */
public final class TracingEventListener implements EventListener {

  /** The baggage of every delivery: none, replacing the current baggage. */
  private static final SpanBaggage NO_BAGGAGE = SpanBaggage.replace(Map.of());

  private final EventListener delegate;
  private final StreamRuneTracing tracing;
  private final SubscriptionName subscriptionName;
  private final boolean traced;

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
    this.tracing = GuardedTracing.of(Objects.requireNonNull(tracing, "tracing is required"));
    this.subscriptionName =
        Objects.requireNonNull(subscriptionName, "subscriptionName is required");
    this.traced = this.tracing.enabled(TracingArea.SUBSCRIPTION);
  }

  /**
   * The listener a subscription delivers to under {@code tracing}: {@code listener} wrapped in a
   * {@code TracingEventListener} when the {@link TracingArea#SUBSCRIPTION} area is on, else {@code
   * listener} itself. A listener that already is a {@code TracingEventListener}, or a {@link
   * HealthTrackingEventListener} around one (the health tracking a subscription with a health
   * contributor adds around the application's listener), or a {@code null} listener or name, is
   * returned as it is, so a subscription never traces a delivery twice and its own checks still
   * report a missing argument.
   *
   * @param listener the listener, or {@code null}
   * @param tracing the tracing; {@link StreamRuneTracing#NOOP} traces nothing, not {@code null}
   * @param subscriptionName the subscription name, or {@code null}
   * @return the listener to deliver to
   */
  public static EventListener traced(
      EventListener listener, StreamRuneTracing tracing, SubscriptionName subscriptionName) {
    StreamRuneTracing guarded = GuardedTracing.of(tracing);
    if (listener == null
        || subscriptionName == null
        || !guarded.enabled(TracingArea.SUBSCRIPTION)
        || isTraced(listener)) {
      return listener;
    }
    return new TracingEventListener(listener, guarded, subscriptionName);
  }

  /** Whether {@code listener} already traces: it is a tracing listener, or tracks health of one. */
  private static boolean isTraced(EventListener listener) {
    return listener instanceof TracingEventListener
        || (listener instanceof HealthTrackingEventListener tracking
            && tracking.delegate() instanceof TracingEventListener);
  }

  @Override
  public void onEvents(List<EventEnvelope> events) {
    if (!traced) {
      delegate.onEvents(events);
      return;
    }
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put(TraceAttributes.SUBSCRIPTION_NAME, subscriptionName.value())
            .put(TraceAttributes.BATCH_SIZE, (long) events.size())
            .build();
    SpanLink link = events.isEmpty() ? SpanLink.NONE : SpanLink.of(events.getFirst().metadata());
    TraceScope scope =
        tracing.start(
            TracingArea.SUBSCRIPTION,
            TraceAttributes.subscriptionDeliverSpanName(subscriptionName),
            SpanKind.CONSUMER,
            attributes,
            link,
            NO_BAGGAGE);
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

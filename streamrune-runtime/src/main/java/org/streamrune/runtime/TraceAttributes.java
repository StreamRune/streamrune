package org.streamrune.runtime;

import io.opentelemetry.api.common.AttributeKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * The published OpenTelemetry span contract: every span name shape and attribute key StreamRune
 * emits, in one place. The span analogue of {@link org.streamrune.core.metrics.MetricNames} — this
 * class is the contract, {@code docs/guide/production.md} mirrors it, and {@code
 * OpenTelemetryCommandInterceptorTest} / {@code TracingEventListenerTest} / {@code
 * TracingProjectionDecoratorTest} assert the emitted name and the EXACT attribute-key set against
 * it, so a rename cannot silently diverge from the documentation.
 *
 * <p><b>Documentation drift.</b> The documentation promised {@code streamrune.command.type}, {@code
 * streamrune.aggregate.id} and {@code streamrune.stream.id}, while the interceptor emitted {@code
 * command.type}, {@code command.id} and {@code aggregate.id} — no {@code streamrune.} prefix on any
 * of them — and never set a stream id at all. Every Jaeger/Tempo query written against the
 * published contract returned zero results, and an operator reading them during an incident
 * concluded that tracing was not wired rather than that the documentation was wrong. This is the
 * same kind of drift that metric NAMES had before they were guarded; there was no equivalent guard
 * for spans.
 *
 * <p>Every attribute key carries the {@code streamrune.} prefix so StreamRune's attributes never
 * collide with an application's or another instrumentation's. (Metric TAG keys are deliberately
 * unprefixed — the metric NAME already carries the prefix; a span attribute has no such carrier.)
 *
 * <p>Span names are path-shaped ({@code command/PlaceOrder}, {@code subscription/orders/deliver},
 * {@code projection/orders/process}) and consistent across all three emitters.
 *
 * <p>The emitters start their spans through a {@link org.streamrune.core.tracing.StreamRuneTracing}
 * and read the same names and keys as plain strings, so an emitter does not initialize this class
 * and its OpenTelemetry types.
 */
public final class TraceAttributes {

  private TraceAttributes() {}

  // ==================== Command span (OpenTelemetryCommandInterceptor) ====================

  /** Simple class name of the executing command. */
  public static final AttributeKey<String> COMMAND_TYPE =
      AttributeKey.stringKey(TraceNames.COMMAND_TYPE);

  /** The id assigned to this command execution. */
  public static final AttributeKey<String> COMMAND_ID =
      AttributeKey.stringKey(TraceNames.COMMAND_ID);

  /** The aggregate the command targets. */
  public static final AttributeKey<String> AGGREGATE_ID =
      AttributeKey.stringKey(TraceNames.AGGREGATE_ID);

  /** The aggregate type of the stream the command targets. */
  public static final AttributeKey<String> AGGREGATE_TYPE =
      AttributeKey.stringKey(TraceNames.AGGREGATE_TYPE);

  /**
   * The event stream the command targets, {@code <aggregateType>:<aggregateId>}. Set when the span
   * starts: the command bus composes the stream before any interceptor runs.
   */
  public static final AttributeKey<String> STREAM_ID = AttributeKey.stringKey(TraceNames.STREAM_ID);

  /** Span name for one command execution. */
  public static String commandSpanName(String commandType) {
    return TraceNames.commandSpanName(commandType);
  }

  // ==================== Subscription span (TracingEventListener) ====================

  /** The subscription the batch was delivered to. */
  public static final AttributeKey<String> SUBSCRIPTION_NAME =
      AttributeKey.stringKey(TraceNames.SUBSCRIPTION_NAME);

  /** Number of events in the delivered/processed batch. */
  public static final AttributeKey<Long> BATCH_SIZE = AttributeKey.longKey(TraceNames.BATCH_SIZE);

  /** Span name for one subscription delivery batch. */
  public static String subscriptionDeliverSpanName(SubscriptionName subscriptionName) {
    return TraceNames.subscriptionDeliverSpanName(subscriptionName);
  }

  // ==================== Projection span (TracingProjectionDecorator) ====================

  /** The projection that processed the batch. */
  public static final AttributeKey<String> PROJECTION_NAME =
      AttributeKey.stringKey(TraceNames.PROJECTION_NAME);

  /** Span name for one projection batch. */
  public static String projectionProcessSpanName(ProjectionName projectionName) {
    return TraceNames.projectionProcessSpanName(projectionName);
  }
}

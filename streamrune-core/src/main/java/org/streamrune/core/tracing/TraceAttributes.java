package org.streamrune.core.tracing;

import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * The published span contract: every span name shape and attribute key StreamRune emits, as plain
 * strings. The span analogue of {@link org.streamrune.core.metrics.MetricNames}: this class is the
 * contract, the "Tracing" section of {@code docs/guide/production.md} mirrors it, a doc-lint test
 * holds that table to these constants, and each emitter's test asserts the emitted name and the
 * EXACT attribute-key set against them.
 *
 * <p>No OpenTelemetry type appears here. An emitter builds its attributes with {@link
 * SpanAttributes#builder()} and these keys and starts its span through a {@link StreamRuneTracing},
 * so a module without the OpenTelemetry API — a store module for another database, for example —
 * emits the standard spans with the standard names and keys.
 *
 * <p><b>Prefix.</b> Every attribute key carries the {@code streamrune.} prefix, so StreamRune's
 * attributes never collide with an application's or another instrumentation's. The prefix is
 * reserved for the framework: an extension adds attributes of its own under a prefix of its own,
 * through {@link SpanAttributes.Builder#put(String, String)} and its overloads, and does not add
 * keys under {@code streamrune.}. (Metric TAG keys are unprefixed — the metric NAME already carries
 * the prefix; a span attribute has no such carrier.)
 *
 * <p><b>Identifiers.</b> An attribute that holds an identifier (a command id, an aggregate id, a
 * stream id) passes {@link LogSanitizer#sanitizeForTrace(String)}: control, separator and format
 * characters are removed, and a longer value is cut to its first {@value
 * LogSanitizer#MAX_TRACE_LENGTH} characters followed by {@code ...}, 259 characters in all. An
 * ordinary identifier is unchanged. Attributes never carry event payloads, metadata values,
 * baggage, encrypted values, crypto subject ids, user ids, roles or SQL.
 *
 * <p><b>Span names</b> are path-shaped ({@code command/PlaceOrder}, {@code
 * subscription/orders/deliver}, {@code projection/orders/process}) and hold types and configured
 * names, never ids.
 */
public final class TraceAttributes {

  private TraceAttributes() {}

  // ==================== Command span (OpenTelemetryCommandInterceptor) ====================

  /** Simple class name of the executing command. */
  public static final String COMMAND_TYPE = "streamrune.command.type";

  /** The id assigned to this command execution; an identifier. */
  public static final String COMMAND_ID = "streamrune.command.id";

  /** The aggregate the command targets; an identifier. */
  public static final String AGGREGATE_ID = "streamrune.aggregate.id";

  /** The aggregate type of the stream the command targets. */
  public static final String AGGREGATE_TYPE = "streamrune.aggregate.type";

  /**
   * The event stream the command targets, {@code <aggregateType>:<aggregateId>}; an identifier. Set
   * when the span starts: the command bus composes the stream before any interceptor runs.
   */
  public static final String STREAM_ID = "streamrune.stream.id";

  /**
   * Span name for one command execution.
   *
   * @param commandType the simple class name of the command
   * @return {@code command/{commandType}}
   */
  public static String commandSpanName(String commandType) {
    return "command/" + commandType;
  }

  // ==================== Subscription span (TracingEventListener) ====================

  /** The subscription the batch was delivered to. */
  public static final String SUBSCRIPTION_NAME = "streamrune.subscription.name";

  /** Number of events in the delivered or processed batch, a {@code long}. */
  public static final String BATCH_SIZE = "streamrune.batch.size";

  /**
   * Span name for one subscription delivery batch.
   *
   * @param subscriptionName the subscription
   * @return {@code subscription/{name}/deliver}
   */
  public static String subscriptionDeliverSpanName(SubscriptionName subscriptionName) {
    return "subscription/" + subscriptionName.value() + "/deliver";
  }

  // ==================== Projection span (TracingProjectionDecorator) ====================

  /** The projection that processed the batch. */
  public static final String PROJECTION_NAME = "streamrune.projection.name";

  /**
   * Span name for one projection batch.
   *
   * @param projectionName the projection
   * @return {@code projection/{name}/process}
   */
  public static String projectionProcessSpanName(ProjectionName projectionName) {
    return "projection/" + projectionName.value() + "/process";
  }
}

package org.streamrune.runtime;

import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * The span names and attribute keys of {@link TraceAttributes} as plain strings, for the emitters.
 * {@link TraceAttributes} holds them as OpenTelemetry {@code AttributeKey}s, so initializing it
 * loads OpenTelemetry; an emitter reads this class instead, and traces through a {@link
 * org.streamrune.core.tracing.StreamRuneTracing} that need not be OpenTelemetry's.
 */
final class TraceNames {

  private TraceNames() {}

  static final String COMMAND_TYPE = "streamrune.command.type";
  static final String COMMAND_ID = "streamrune.command.id";
  static final String AGGREGATE_ID = "streamrune.aggregate.id";
  static final String AGGREGATE_TYPE = "streamrune.aggregate.type";
  static final String STREAM_ID = "streamrune.stream.id";
  static final String SUBSCRIPTION_NAME = "streamrune.subscription.name";
  static final String BATCH_SIZE = "streamrune.batch.size";
  static final String PROJECTION_NAME = "streamrune.projection.name";

  static String commandSpanName(String commandType) {
    return "command/" + commandType;
  }

  static String subscriptionDeliverSpanName(SubscriptionName subscriptionName) {
    return "subscription/" + subscriptionName.value() + "/deliver";
  }

  static String projectionProcessSpanName(ProjectionName projectionName) {
    return "projection/" + projectionName.value() + "/process";
  }
}

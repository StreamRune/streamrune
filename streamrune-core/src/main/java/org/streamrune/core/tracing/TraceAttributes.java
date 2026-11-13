package org.streamrune.core.tracing;

import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
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
 * stream id, a saga id, an outbox entry id) passes {@link LogSanitizer#sanitizeForTrace(String)}:
 * control, separator and format characters are removed, and a longer value is cut to its first
 * {@value LogSanitizer#MAX_TRACE_LENGTH} characters followed by {@code ...}, 259 characters in all.
 * An ordinary identifier is unchanged. Attributes never carry event payloads, metadata values,
 * baggage, encrypted values, crypto subject ids, user ids, roles or SQL.
 *
 * <p><b>Span names</b> are path-shaped ({@code command/PlaceOrder}, {@code
 * subscription/orders/deliver}, {@code projection/orders/process}) and hold types and configured
 * names, never ids. A constant whose value holds a {@code /} is a span name; every other constant
 * is an attribute key.
 *
 * <p><b>Outcomes.</b> An expected outcome — a command its domain rejected, a lock not acquired in
 * time, an append that lost its optimistic-lock check or that the saga forward fence refused, a
 * saga event held for later, a saga step that waits for another step of its saga — is an attribute
 * value, and the span keeps an unset or OK status. A failure records the exception type and message
 * and the ERROR status.
 */
public final class TraceAttributes {

  private TraceAttributes() {}

  // ==================== Command span (TracingCommandInterceptor) ====================

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
   * The class name of the {@link org.streamrune.core.DomainException} that rejected the command,
   * its subtypes {@code ValidationException} and {@code AuthorizationException} included. Present
   * on a rejected command only: a rejection is the command's answer, not a failure, so its span
   * keeps the unset status and records no exception, as an HTTP server span does for a client
   * error. The rejection's message is not exported.
   */
  public static final String COMMAND_REJECTION_TYPE = "streamrune.command.rejection.type";

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

  // ==================== Event store spans (EventStore implementations) ====================

  /** Span name for one append of events to a stream. */
  public static final String EVENT_STORE_APPEND = "eventstore/append";

  /** Span name for serializing the events of an append, child of the append span. */
  public static final String EVENT_STORE_APPEND_SERIALIZE = "eventstore/append/serialize";

  /** Span name for one load of an aggregate's history. */
  public static final String EVENT_STORE_LOAD = "eventstore/load";

  /**
   * Span name for deserializing the events of a load (decrypt, upcast, bind), child of the load
   * span.
   */
  public static final String EVENT_STORE_LOAD_DESERIALIZE = "eventstore/load/deserialize";

  /**
   * Span name for resolving the stored snapshot of a load (deserialize, decrypt, migrate), child of
   * the load span; started only when the stream has a stored snapshot.
   */
  public static final String EVENT_STORE_SNAPSHOT_LOAD = "eventstore/snapshot/load";

  /** Span name for one snapshot write. */
  public static final String EVENT_STORE_SNAPSHOT_SAVE = "eventstore/snapshot/save";

  /**
   * Span name for one global or stream read that returns at least one event, from the start of its
   * query to the end of its deserialization, as a child of the span current on the reading thread.
   * A read that returns none starts no span, and neither does a read with no span current: the poll
   * of a subscription or a projection runner, whose delivery or processing span is the poll's one
   * trace.
   */
  public static final String EVENT_STORE_READ = "eventstore/read";

  /** Number of events appended, loaded or read, a {@code long}. */
  public static final String EVENT_COUNT = "streamrune.event.count";

  /** The stream version an append expects the stream to be at, a {@code long}. */
  public static final String EXPECTED_VERSION = "streamrune.expected.version";

  /** Number of outbox entries an append writes in its transaction, a {@code long}. */
  public static final String OUTBOX_ENTRY_COUNT = "streamrune.outbox.entry.count";

  /**
   * How an append ended: {@code APPENDED}, {@code CONFLICT} (the stream was not at the expected
   * version), {@code DUPLICATE} (a keyed append whose key was already recorded) or {@code FENCED}
   * (a saga's forward command refused because the saga has left its forward phase). Absent when the
   * append failed.
   */
  public static final String APPEND_OUTCOME = "streamrune.append.outcome";

  /** Whether a load started from a snapshot, a {@code boolean}. */
  public static final String SNAPSHOT_USED = "streamrune.snapshot.used";

  /**
   * Number of loaded events stored below the current schema version of their type, which the
   * upcaster chain upcast, a {@code long}.
   */
  public static final String UPCAST_COUNT = "streamrune.upcast.count";

  /**
   * The stream version a snapshot holds the state at, a {@code long}: on a snapshot load, the
   * version the load starts from ({@code 0} when it discards the snapshot); on a snapshot save, the
   * version saved. Not the snapshot's schema version.
   */
  public static final String STREAM_VERSION = "streamrune.stream.version";

  /**
   * What became of a snapshot: on a load {@code HIT} (used) or {@code DISCARDED} (stored but
   * unusable, and the stream replays from its start); on a save {@code SAVED} or {@code SKIPPED} (a
   * purge of the stream's snapshot intervened).
   */
  public static final String SNAPSHOT_OUTCOME = "streamrune.snapshot.outcome";

  /** Which read returned the events: {@code GLOBAL} or {@code STREAM}. */
  public static final String READ_KIND = "streamrune.read.kind";

  // ==================== Lock span (AggregateLocker implementations) ====================

  /** Span name for one acquisition of an aggregate lock. */
  public static final String LOCK_ACQUIRE = "lock/acquire";

  /**
   * How a lock acquisition ended: {@code ACQUIRED}, {@code TIMEOUT} (held elsewhere past the
   * timeout) or {@code INTERRUPTED}. Absent when the acquisition failed otherwise.
   */
  public static final String LOCK_OUTCOME = "streamrune.lock.outcome";

  // ==================== Outbox spans (OutboxPoller) ====================

  /** Span name for one relay batch that claimed at least one outbox entry. */
  public static final String OUTBOX_RELAY = "outbox/relay";

  /** Span name for the publish of one outbox entry, child of the relay span. */
  public static final String OUTBOX_PUBLISH = "outbox/publish";

  /** Number of entries of a relay batch the publisher confirmed, a {@code long}. */
  public static final String OUTBOX_PUBLISHED = "streamrune.outbox.published";

  /** Number of entries of a relay batch whose publish failed, a {@code long}. */
  public static final String OUTBOX_FAILED = "streamrune.outbox.failed";

  /** The outbox entry being published; an identifier. */
  public static final String OUTBOX_ENTRY_ID = "streamrune.outbox.entry.id";

  /** The payload type of the outbox entry being published. */
  public static final String OUTBOX_PAYLOAD_TYPE = "streamrune.outbox.payload.type";

  // ==================== Saga spans (SagaRunner, SagaTimeoutRunner) ====================

  /** The saga type. */
  public static final String SAGA_TYPE = "streamrune.saga.type";

  /** The saga instance; an identifier. */
  public static final String SAGA_ID = "streamrune.saga.id";

  /**
   * How a saga step ended: {@code ADVANCED}, {@code COMPLETED}, {@code COMPENSATING}, {@code
   * COMPENSATED}, {@code FAILED}, {@code FAULTED}, {@code SKIPPED} (nothing to do: a redelivery, a
   * saga that has ended, an event for no saga) or {@code DEFERRED} (the event is held for later).
   * Absent when the step failed and the event is delivered again.
   */
  public static final String SAGA_STEP_OUTCOME = "streamrune.saga.step.outcome";

  /** The position of a compensation command in the list the saga returned, a {@code long}. */
  public static final String SAGA_COMPENSATION_INDEX = "streamrune.saga.compensation.index";

  /**
   * Span name for one event a saga handles.
   *
   * @param sagaType the saga type
   * @return {@code saga/{sagaType}/step}
   */
  public static String sagaStepSpanName(SagaType sagaType) {
    return "saga/" + sagaType.value() + "/step";
  }

  /**
   * Span name for one compensation command a saga dispatches.
   *
   * @param sagaType the saga type
   * @return {@code saga/{sagaType}/compensate}
   */
  public static String sagaCompensateSpanName(SagaType sagaType) {
    return "saga/" + sagaType.value() + "/compensate";
  }

  /**
   * Span name for one saga the timeout runner acts on past its deadline.
   *
   * @param sagaType the saga type
   * @return {@code saga/{sagaType}/timeout}
   */
  public static String sagaTimeoutSpanName(SagaType sagaType) {
    return "saga/" + sagaType.value() + "/timeout";
  }
}

package org.streamrune.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Map;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * Causal metadata attached to every persisted event.
 *
 * @param eventId unique event identifier (required)
 * @param commandId identifier of the command that caused this event (required)
 * @param traceId OpenTelemetry Trace ID (nullable)
 * @param spanId OpenTelemetry Span ID (nullable)
 * @param correlationId identifies the entire business process (required)
 * @param causationId identifies the direct predecessor event ID (nullable)
 * @param userId who triggered the action (nullable)
 * @param timestamp when the event was created (required)
 * @param baggage OTel Baggage key-value pairs (nullable — normalized to empty map)
 * @param sagaOrigin the saga whose dispatch caused the event, {@code null} for every other event;
 *     written by the command bus
 * @param spanContext the span of the command that appended the event, written by the command bus
 *     from its own {@code command/{CommandType}} span and from nothing a client sends; {@link
 *     SpanLink#NONE} when the command was not traced. The spans that later handle the event link to
 *     it ({@link SpanLink#of(EventMetadata)}). Never {@code null}: a {@code null} argument, and a
 *     JSON form without the key, read as {@code NONE}, and {@code NONE} is left out of the JSON
 */
public record EventMetadata(
    EventId eventId,
    CommandId commandId,
    TraceId traceId,
    SpanId spanId,
    CorrelationId correlationId,
    CausationId causationId,
    UserId userId,
    Instant timestamp,
    Map<String, String> baggage,
    SagaOrigin sagaOrigin,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) SpanLink spanContext) {

  public EventMetadata {
    if (eventId == null) {
      throw new IllegalArgumentException("eventId is required");
    }
    if (commandId == null) {
      throw new IllegalArgumentException("commandId is required");
    }
    if (correlationId == null) {
      throw new IllegalArgumentException("correlationId is required");
    }
    if (timestamp == null) {
      throw new IllegalArgumentException("timestamp is required");
    }
    baggage = baggage == null ? Map.of() : Map.copyOf(baggage);
    spanContext = spanContext == null ? SpanLink.NONE : spanContext;
  }

  /**
   * Convenience constructor for metadata of an event no traced command span appended ({@code
   * spanContext} is {@link SpanLink#NONE}).
   */
  public EventMetadata(
      EventId eventId,
      CommandId commandId,
      TraceId traceId,
      SpanId spanId,
      CorrelationId correlationId,
      CausationId causationId,
      UserId userId,
      Instant timestamp,
      Map<String, String> baggage,
      SagaOrigin sagaOrigin) {
    this(
        eventId,
        commandId,
        traceId,
        spanId,
        correlationId,
        causationId,
        userId,
        timestamp,
        baggage,
        sagaOrigin,
        SpanLink.NONE);
  }

  /** Convenience constructor for metadata of an event no saga dispatch caused. */
  public EventMetadata(
      EventId eventId,
      CommandId commandId,
      TraceId traceId,
      SpanId spanId,
      CorrelationId correlationId,
      CausationId causationId,
      UserId userId,
      Instant timestamp,
      Map<String, String> baggage) {
    this(
        eventId,
        commandId,
        traceId,
        spanId,
        correlationId,
        causationId,
        userId,
        timestamp,
        baggage,
        null);
  }

  /**
   * Convenience constructor for metadata without baggage (baggage is an empty map) of an event no
   * saga dispatch caused.
   */
  public EventMetadata(
      EventId eventId,
      CommandId commandId,
      TraceId traceId,
      SpanId spanId,
      CorrelationId correlationId,
      CausationId causationId,
      UserId userId,
      Instant timestamp) {
    this(
        eventId,
        commandId,
        traceId,
        spanId,
        correlationId,
        causationId,
        userId,
        timestamp,
        null,
        null);
  }
}

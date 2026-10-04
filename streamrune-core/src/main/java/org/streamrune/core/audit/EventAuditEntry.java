package org.streamrune.core.audit;

import java.time.Instant;
import java.util.Objects;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * Immutable record of a single persisted event for audit purposes.
 *
 * <p>{@code causationId} is nullable — root events from external triggers may lack causation.
 * {@code userId} is nullable — anonymous requests produce events without a user identity.
 *
 * @param eventId unique identifier of the persisted event
 * @param eventType type of the domain event
 * @param streamId the aggregate stream to which the event was appended
 * @param version the stream version at which this event was written
 * @param commandId the command that produced this event
 * @param correlationId correlation token linking all events in the same logical operation
 * @param causationId ID of the direct predecessor event in the causal chain; {@code null} for root
 *     events
 * @param userId identity of the caller who issued the originating command; {@code null} for
 *     anonymous requests
 * @param occurredAt the instant the event was persisted
 */
public record EventAuditEntry(
    EventId eventId,
    EventType eventType,
    StreamId streamId,
    Version version,
    CommandId commandId,
    CorrelationId correlationId,
    CausationId causationId,
    UserId userId,
    Instant occurredAt) {

  public EventAuditEntry {
    Objects.requireNonNull(eventId, "eventId is required");
    Objects.requireNonNull(eventType, "eventType is required");
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(version, "version is required");
    Objects.requireNonNull(commandId, "commandId is required");
    Objects.requireNonNull(correlationId, "correlationId is required");
    Objects.requireNonNull(occurredAt, "occurredAt is required");
  }
}

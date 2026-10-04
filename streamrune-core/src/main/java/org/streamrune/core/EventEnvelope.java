package org.streamrune.core;

import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Wrapper around a domain event with positional and causal metadata.
 *
 * @param globalOffset global sequence number across all streams (required); set to {@link
 *     GlobalOffset#initial()} before append and replaced with the store-assigned value in {@link
 *     EventStore.AppendResult#globalOffsets()}
 * @param streamId the stream this event belongs to — {@code aggregateType():aggregateId()}
 *     (required)
 * @param version per-stream sequence number (required); before append the caller pre-sets {@code
 *     expectedVersion + 1 .. expectedVersion + n}, the same values the store assigns
 *     authoritatively — see {@link EventStore#append}
 * @param eventType the event type (required)
 * @param event the deserialized domain event (required)
 * @param metadata causal metadata (required)
 */
public record EventEnvelope(
    GlobalOffset globalOffset,
    StreamId streamId,
    Version version,
    EventType eventType,
    DomainEvent event,
    EventMetadata metadata) {

  public EventEnvelope {
    if (globalOffset == null) {
      throw new IllegalArgumentException("globalOffset is required");
    }
    if (streamId == null) {
      throw new IllegalArgumentException("streamId is required");
    }
    if (version == null) {
      throw new IllegalArgumentException("version is required");
    }
    if (eventType == null || eventType.name().isBlank()) {
      throw new IllegalArgumentException("eventType is required");
    }
    if (event == null) {
      throw new IllegalArgumentException("event is required");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata is required");
    }
  }

  /** The aggregate type of the stream this event belongs to. */
  public AggregateType aggregateType() {
    return streamId.aggregateType();
  }

  /** The aggregate id of the stream this event belongs to. */
  public AggregateId aggregateId() {
    return streamId.aggregateId();
  }
}

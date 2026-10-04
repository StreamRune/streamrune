package org.streamrune.core.outbox;

import java.util.Objects;
import org.streamrune.core.types.LogSanitizer;

/**
 * A {@code STRICT_PER_AGGREGATE} channel was asked to save an entry with a {@code null} {@code
 * streamId}. Thrown by {@code OutboxStore.save}/{@code saveAll} before any insert, so {@code
 * EventStore.append} rolls back and the command fails with THIS exception, unwrapped (the {@code
 * OutboxEventMapper} contract: a thrown exception fails the append). The message names the entry id
 * (sanitized — it is user-derived) and the payload type, never the payload.
 */
public final class OutboxOrderingViolationException extends IllegalArgumentException {

  private final OutboxEntryId entryId;
  private final String payloadType;

  public OutboxOrderingViolationException(OutboxEntryId entryId, String payloadType) {
    super(
        "Outbox entry "
            + LogSanitizer.sanitizeForLog(
                Objects.requireNonNull(entryId, "entryId is required").value())
            + " of type "
            + LogSanitizer.sanitizeForLog(
                Objects.requireNonNull(payloadType, "payloadType is required"))
            + " has no streamId, but this outbox channel is STRICT_PER_AGGREGATE: every entry"
            + " must carry the stream whose delivery order the consumer depends on (normally the"
            + " event's own stream). The append was rolled back; fix the OutboxEventMapper, or"
            + " declare the channel AVAILABILITY_FIRST if its consumers tolerate unordered"
            + " delivery.");
    this.entryId = entryId;
    this.payloadType = payloadType;
  }

  public OutboxEntryId entryId() {
    return entryId;
  }

  public String payloadType() {
    return payloadType;
  }
}

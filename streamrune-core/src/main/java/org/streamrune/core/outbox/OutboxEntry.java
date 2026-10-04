package org.streamrune.core.outbox;

import java.time.Instant;
import java.util.Objects;
import org.streamrune.core.types.StreamId;

/**
 * An immutable snapshot of a single outbox message.
 *
 * <p>Use the {@link #pending} factories to create a new entry ready for delivery. The {@code
 * payload} is a JSON string (produced by the caller) and {@code payloadType} names the event type
 * for receivers.
 *
 * <p>{@code streamId} names the stream whose delivery order the consumer depends on — normally the
 * event's own stream. On a {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} channel it is mandatory
 * (see {@link OutboxOrderingMode}); on an {@link OutboxOrderingMode#AVAILABILITY_FIRST} channel it
 * is an optional partition key. Publishers that support partitioned transports (e.g. Kafka) use its
 * text form {@code <aggregateType>:<aggregateId>} as the partition key, keeping all entries of one
 * stream on one partition. When {@code null}, publishers fall back to the entry id.
 *
 * <p>{@code lastError}, {@code processedAt}, and {@code nextRetryAt} are nullable — they are only
 * set after at least one delivery attempt. {@code skip} is the operator's audit record and is
 * non-null iff {@code status == SKIPPED}.
 */
public record OutboxEntry(
    OutboxEntryId id,
    String payload,
    String payloadType,
    StreamId streamId,
    OutboxStatus status,
    int attempts,
    String lastError,
    Instant createdAt,
    Instant processedAt,
    Instant nextRetryAt,
    SkipRecord skip) {

  /**
   * The audit of an operator skip: all three are set together in one statement and are non-null iff
   * {@code status == SKIPPED}. {@code processedAt} keeps the time the entry reached {@code FAILED};
   * {@code skippedAt} is the resolution time.
   */
  public record SkipRecord(Instant skippedAt, String skippedBy, String reason) {
    public SkipRecord {
      Objects.requireNonNull(skippedAt, "skippedAt is required");
      Objects.requireNonNull(skippedBy, "skippedBy is required");
      Objects.requireNonNull(reason, "reason is required");
    }
  }

  public OutboxEntry {
    Objects.requireNonNull(id, "id is required");
    Objects.requireNonNull(payload, "payload is required");
    Objects.requireNonNull(payloadType, "payloadType is required");
    Objects.requireNonNull(status, "status is required");
    Objects.requireNonNull(createdAt, "createdAt is required");
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must be non-negative");
    }
    // The skip audit exists exactly when the entry is SKIPPED.
    if ((status == OutboxStatus.SKIPPED) != (skip != null)) {
      throw new IllegalArgumentException(
          "skip record must be present iff status == SKIPPED (status=" + status + ")");
    }
    // streamId, lastError, processedAt, and nextRetryAt are nullable
  }

  /**
   * Creates a new {@code PENDING} entry with zero attempts, no error, no stream id, and {@code
   * createdAt} set to {@link Instant#now()}. Only an {@link OutboxOrderingMode#AVAILABILITY_FIRST}
   * channel accepts such an entry; a {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} store rejects
   * it at save time with {@link OutboxOrderingViolationException}.
   */
  public static OutboxEntry pending(OutboxEntryId id, String payload, String payloadType) {
    return pending(id, payload, payloadType, (StreamId) null);
  }

  /**
   * Creates a new {@code PENDING} entry with zero attempts, no error, and {@code createdAt} set to
   * {@link Instant#now()}.
   *
   * @param streamId the stream whose delivery order the consumer depends on (also the partition key
   *     in partitioned transports); may be {@code null} only on an {@link
   *     OutboxOrderingMode#AVAILABILITY_FIRST} channel
   */
  public static OutboxEntry pending(
      OutboxEntryId id, String payload, String payloadType, StreamId streamId) {
    return new OutboxEntry(
        id,
        payload,
        payloadType,
        streamId,
        OutboxStatus.PENDING,
        0,
        null,
        Instant.now(),
        null,
        null,
        null);
  }
}

package org.streamrune.core.outbox;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Objects;
import org.streamrune.core.tracing.SpanLink;
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
 *
 * <p>{@code traceLink} names the span of the command whose append wrote the entry, so the relay's
 * {@code outbox/publish} span links to that command's trace. It is never {@code null}: an entry
 * without a link carries {@link SpanLink#NONE}, which the {@link #pending} factories set and a
 * {@code null} argument reads as, and the span of such an entry has no link. The event store fills
 * it from the metadata of the event the entry was derived from (its {@link
 * org.streamrune.core.EventMetadata#spanContext() spanContext}, through {@link
 * SpanLink#of(org.streamrune.core.EventMetadata)}) when it writes the entry in the append
 * transaction, unless the entry already carries one; an {@link OutboxEventMapper} builds its
 * entries without it. It stays {@code NONE} when the command was not traced, and a store keeps only
 * a link whose ids are valid W3C ids ({@link SpanLink#isW3c()}): a stored link that is not one, or
 * none at all, reads back as {@code NONE}. In JSON the property is left out when it is {@code
 * NONE}.
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
    SkipRecord skip,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) SpanLink traceLink) {

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
    traceLink = traceLink == null ? SpanLink.NONE : traceLink;
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must be non-negative");
    }
    // The skip audit exists exactly when the entry is SKIPPED.
    if ((status == OutboxStatus.SKIPPED) != (skip != null)) {
      throw new IllegalArgumentException(
          "skip record must be present iff status == SKIPPED (status=" + status + ")");
    }
    // streamId, lastError, processedAt and nextRetryAt are nullable
  }

  /**
   * The same entry with {@code traceLink} as its link to the trace of the command that wrote it.
   *
   * @param traceLink the link; {@link SpanLink#NONE} for none, as {@code null} reads
   * @return the entry with that link
   */
  public OutboxEntry withTraceLink(SpanLink traceLink) {
    return new OutboxEntry(
        id,
        payload,
        payloadType,
        streamId,
        status,
        attempts,
        lastError,
        createdAt,
        processedAt,
        nextRetryAt,
        skip,
        traceLink);
  }

  /**
   * Creates a new {@code PENDING} entry with zero attempts, no error, no stream id, and {@code
   * createdAt} set to {@link Instant#now()}. Only an {@link OutboxOrderingMode#AVAILABILITY_FIRST}
   * channel accepts such an entry; a {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} store rejects
   * it at save time with {@link OutboxOrderingViolationException}.
   */
  public static OutboxEntry pending(OutboxEntryId id, String payload, String payloadType) {
    return pending(id, payload, payloadType, null);
  }

  /**
   * Creates a new {@code PENDING} entry with zero attempts, no error, no trace link ({@link
   * SpanLink#NONE}), and {@code createdAt} set to {@link Instant#now()}.
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
        null,
        SpanLink.NONE);
  }
}

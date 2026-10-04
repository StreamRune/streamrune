package org.streamrune.core.outbox;

import java.util.List;
import org.streamrune.core.EventEnvelope;

/**
 * Maps a persisted domain event to the outbox entries to publish for it, written in the SAME
 * transaction as the event append. Return an empty list to NOT publish an event.
 *
 * <p>The application owns selection and wire format: build each {@link OutboxEntry} with the
 * payload (JSON), {@code payloadType}, and {@code streamId} (ordering and partition key) it wants
 * downstream consumers to see. This is where integration-event selection, transformation, and PII
 * redaction happen.
 *
 * <p><b>Contract:</b> implementations run inside the append transaction and MUST be fast,
 * side-effect-free, and total — a thrown exception fails the append (no event and no entry are
 * persisted). Do no I/O. Publish only data verified free of personal data.
 *
 * <p><b>Ordering contract.</b> On a {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} channel every
 * returned entry carries a non-null {@code streamId}; a {@code null} one fails the append with
 * {@link OutboxOrderingViolationException}. Normally that is {@code event.streamId()}, the stream
 * the event was appended to. Name another stream ({@code StreamId.of(ORDER, new
 * AggregateId(payment.orderId()))} on a payment event) only when the consumer's order depends on
 * that aggregate; that cross-stream order holds because every append holds the global-offset
 * counter lock through commit.
 */
@FunctionalInterface
public interface OutboxEventMapper {
  List<OutboxEntry> toOutbox(EventEnvelope event);
}

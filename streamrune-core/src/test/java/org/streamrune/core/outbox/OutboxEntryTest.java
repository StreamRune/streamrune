package org.streamrune.core.outbox;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

class OutboxEntryTest {

  private static final StreamId STREAM = StreamId.of(AggregateType.of("agg"), AggregateId.of("a"));

  @Test
  void pending_factory_creates_pending_entry() {
    var id = OutboxEntryId.of("id-1");
    var entry = OutboxEntry.pending(id, "{\"key\":\"value\"}", "OrderCreated");

    assertEquals(id, entry.id());
    assertEquals("{\"key\":\"value\"}", entry.payload());
    assertEquals("OrderCreated", entry.payloadType());
    assertEquals(OutboxStatus.PENDING, entry.status());
    assertEquals(0, entry.attempts());
    assertNull(entry.lastError());
    assertNotNull(entry.createdAt());
    assertNull(entry.processedAt());
  }

  @Test
  void entry_rejects_null_id() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OutboxEntry(
                null,
                "{}",
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void entry_rejects_null_payload() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OutboxEntry(
                OutboxEntryId.of("id"),
                null,
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void entry_rejects_null_payload_type() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OutboxEntry(
                OutboxEntryId.of("id"),
                "{}",
                null,
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void entry_rejects_null_status() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OutboxEntry(
                OutboxEntryId.of("id"),
                "{}",
                "Event",
                null,
                null,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void entry_rejects_negative_attempts() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OutboxEntry(
                OutboxEntryId.of("id"),
                "{}",
                "Event",
                null,
                OutboxStatus.PENDING,
                -1,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void entry_allows_null_last_error_processed_at_and_nextRetryAt() {
    assertDoesNotThrow(
        () ->
            new OutboxEntry(
                OutboxEntryId.of("id"),
                "{}",
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void pending_factory_sets_nextRetryAt_to_null() {
    var entry = OutboxEntry.pending(OutboxEntryId.of("rt-1"), "{}", "Event");
    assertNull(entry.nextRetryAt());
  }

  @Test
  void pending_factory_without_stream_id_sets_it_to_null() {
    var entry = OutboxEntry.pending(OutboxEntryId.of("agg-0"), "{}", "Event");
    assertNull(entry.streamId());
  }

  @Test
  void pending_factory_with_stream_id_carries_it() {
    var entry = OutboxEntry.pending(OutboxEntryId.of("agg-1"), "{}", "Event", STREAM);
    assertEquals(STREAM, entry.streamId());
    assertEquals(OutboxStatus.PENDING, entry.status());
  }

  @Test
  void canonical_constructor_without_stream_id_sets_it_to_null() {
    var entry =
        new OutboxEntry(
            OutboxEntryId.of("compat"),
            "{}",
            "Event",
            null,
            OutboxStatus.PENDING,
            0,
            null,
            Instant.now(),
            null,
            null,
            null);
    assertNull(entry.streamId());
  }

  @Test
  void canonical_constructor_allows_null_stream_id() {
    assertDoesNotThrow(
        () ->
            new OutboxEntry(
                OutboxEntryId.of("agg-null"),
                "{}",
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null,
                null));
  }

  @Test
  void skipRecord_requiredIffSkipped() {
    var skip = new OutboxEntry.SkipRecord(Instant.now(), "ops@example", "poison payload");
    // SKIPPED without a record: the skip-audit invariant is violated.
    assertThrows(
        IllegalArgumentException.class,
        () -> entryWith(OutboxStatus.SKIPPED, null),
        "a SKIPPED entry must carry its audit record");
    // A record on a non-SKIPPED status: the skip-audit invariant is violated the other way.
    assertThrows(
        IllegalArgumentException.class,
        () -> entryWith(OutboxStatus.FAILED, skip),
        "only a SKIPPED entry carries a skip record");
    // Both halves present: fine.
    assertDoesNotThrow(() -> entryWith(OutboxStatus.SKIPPED, skip));
    assertDoesNotThrow(() -> entryWith(OutboxStatus.FAILED, null));
  }

  @Test
  void skipRecord_rejectsNullComponents() {
    var now = Instant.now();
    assertThrows(NullPointerException.class, () -> new OutboxEntry.SkipRecord(null, "by", "r"));
    assertThrows(NullPointerException.class, () -> new OutboxEntry.SkipRecord(now, null, "r"));
    assertThrows(NullPointerException.class, () -> new OutboxEntry.SkipRecord(now, "by", null));
  }

  @Test
  void pending_factories_carryNoSkipRecord() {
    assertNull(OutboxEntry.pending(OutboxEntryId.of("p"), "{}", "Event").skip());
    assertNull(OutboxEntry.pending(OutboxEntryId.of("p2"), "{}", "Event", STREAM).skip());
  }

  private static OutboxEntry entryWith(OutboxStatus status, OutboxEntry.SkipRecord skip) {
    return new OutboxEntry(
        OutboxEntryId.of("inv7"),
        "{}",
        "Event",
        STREAM,
        status,
        3,
        status == OutboxStatus.PENDING ? null : "boom",
        Instant.now(),
        status == OutboxStatus.PENDING ? null : Instant.now(),
        null,
        skip);
  }
}

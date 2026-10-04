package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;

class EventMetadataTest {

  @Test
  void constructsWithRequiredFields() {
    var now = Instant.now();
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            now);

    assertEquals(EventId.of("evt_1"), md.eventId());
    assertEquals(CommandId.of("cmd_1"), md.commandId());
    assertEquals(CorrelationId.of("corr_1"), md.correlationId());
    assertEquals(now, md.timestamp());
    assertNull(md.traceId());
    assertNull(md.spanId());
    assertNull(md.causationId());
    assertNull(md.userId());
  }

  @Test
  void rejectsNullEventId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                null,
                CommandId.of("cmd_1"),
                null,
                null,
                CorrelationId.of("corr_1"),
                null,
                null,
                Instant.now()));
  }

  @Test
  void rejectsNullCommandId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_1"),
                null,
                null,
                null,
                CorrelationId.of("corr_1"),
                null,
                null,
                Instant.now()));
  }

  @Test
  void rejectsNullCorrelationId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_1"),
                CommandId.of("cmd_1"),
                null,
                null,
                null,
                null,
                null,
                Instant.now()));
  }

  @Test
  void rejectsNullTimestamp() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_1"),
                CommandId.of("cmd_1"),
                null,
                null,
                CorrelationId.of("corr_1"),
                null,
                null,
                null));
  }

  @Test
  void shouldStoresBaggageAsImmutableCopy() {
    var mutable = new java.util.HashMap<String, String>();
    mutable.put("tenant", "acme");
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now(),
            mutable);

    assertEquals(Map.of("tenant", "acme"), md.baggage());
    mutable.put("extra", "value");
    assertEquals(1, md.baggage().size(), "Baggage should be an immutable copy");
    assertThrows(UnsupportedOperationException.class, () -> md.baggage().put("k", "v"));
  }

  @Test
  void shouldNormalizeNullBaggageToEmptyMap() {
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now(),
            null);

    assertNotNull(md.baggage());
    assertTrue(md.baggage().isEmpty());
  }

  @Test
  void convenienceConstructorWithoutBaggageSetsEmptyBaggage() {
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now());

    assertNotNull(md.baggage());
    assertTrue(md.baggage().isEmpty());
  }
}

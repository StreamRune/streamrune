package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SagaType;

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

  /** The mapper the PostgreSQL event store reads and writes {@code event_stream.metadata} with. */
  private static ObjectMapper storeMapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  private static final SagaOrigin ORIGIN =
      SagaOrigin.dispatchedBy(SagaType.of("com.shop.OrderSaga$State"), SagaId.of("order-saga-42"));

  @Test
  void theShorterConstructorsCarryNoSagaOrigin() {
    var withoutBaggage =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now());
    var withBaggage =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now(),
            Map.of("tenant", "acme"));

    assertNull(withoutBaggage.sagaOrigin());
    assertNull(withBaggage.sagaOrigin());
    assertEquals(Map.of("tenant", "acme"), withBaggage.baggage());
  }

  @Test
  void sagaOriginRoundTripsThroughJson() throws Exception {
    ObjectMapper mapper = storeMapper();
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("order-saga-42"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            Map.of(),
            ORIGIN);

    String json = mapper.writeValueAsString(md);

    assertTrue(
        json.endsWith(
            ",\"baggage\":{},\"sagaOrigin\":{\"sagaType\":\"com.shop.OrderSaga$State\","
                + "\"sagaId\":\"order-saga-42\",\"hop\":0}}"),
        json);
    assertEquals(md, mapper.readValue(json, EventMetadata.class));

    var noOrigin =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"));
    String noOriginJson = mapper.writeValueAsString(noOrigin);
    assertTrue(noOriginJson.endsWith(",\"baggage\":{},\"sagaOrigin\":null}"), noOriginJson);
    assertEquals(noOrigin, mapper.readValue(noOriginJson, EventMetadata.class));
  }

  @Test
  void jsonWithoutTheSagaOriginKeyReadsAsNoOrigin() throws Exception {
    ObjectMapper mapper = storeMapper();
    String stored =
        "{\"eventId\":\"evt_1\",\"commandId\":\"cmd_1\",\"traceId\":null,\"spanId\":null,"
            + "\"correlationId\":\"corr_1\",\"causationId\":null,\"userId\":null,"
            + "\"timestamp\":\"2026-01-01T00:00:00Z\",\"baggage\":{}}";

    EventMetadata md = mapper.readValue(stored, EventMetadata.class);

    assertNull(md.sagaOrigin());
    assertEquals(EventId.of("evt_1"), md.eventId());

    // An unknown key is still refused: the store reads metadata strictly.
    String withUnknownKey = stored.substring(0, stored.length() - 1) + ",\"sagaOrigins\":null}";
    assertThrows(
        UnrecognizedPropertyException.class,
        () -> mapper.readValue(withUnknownKey, EventMetadata.class));
  }
}

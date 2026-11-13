package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;

class EventMetadataTest {

  @Test
  void constructsWithRequiredFields() {
    var now = Instant.now();
    var md =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
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
                EventId.of("evt_1"), CommandId.of("cmd_1"), null, null, null, null, Instant.now()));
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

  /**
   * A mapper that may not override access modifiers, a hardening some applications apply to every
   * mapper they hand a component: it reaches public members only.
   */
  private static ObjectMapper accessCheckingMapper() {
    return com.fasterxml.jackson.databind.json.JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
        .build();
  }

  @Test
  void aMapperThatMayNotOverrideAccessModifiers_writesAndReadsTheSpanContext() throws Exception {
    ObjectMapper mapper = accessCheckingMapper();
    SpanLink link =
        new SpanLink(TraceId.of("4bf92f3577b34da6a3ce929d0e0e4736"), SpanId.of("00f067aa0ba902b7"));
    for (SpanLink spanContext : new SpanLink[] {link, SpanLink.NONE}) {
      var md =
          new EventMetadata(
              EventId.of("evt_1"),
              CommandId.of("cmd_1"),
              null,
              CorrelationId.of("corr_1"),
              null,
              null,
              Instant.parse("2026-11-12T00:00:00Z"),
              Map.of(),
              null,
              spanContext);

      String json = mapper.writeValueAsString(md);

      assertEquals(spanContext, mapper.readValue(json, EventMetadata.class).spanContext());
    }
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
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.now());
    var withBaggage =
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
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
            CorrelationId.of("corr_1"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"));
    String noOriginJson = mapper.writeValueAsString(noOrigin);
    assertTrue(noOriginJson.endsWith(",\"baggage\":{},\"sagaOrigin\":null}"), noOriginJson);
    assertEquals(noOrigin, mapper.readValue(noOriginJson, EventMetadata.class));
  }

  @Test
  void spanContext_roundTripsThroughJson_andNoneIsLeftOut() throws Exception {
    ObjectMapper mapper = storeMapper();
    var command =
        new SpanLink(TraceId.of("0af7651916cd43dd8448eb211c80319c"), SpanId.of("b7ad6b7169203331"));
    var traced =
        new EventMetadata(
            EventId.of("evt-s"),
            CommandId.of("cmd-s"),
            null,
            CorrelationId.of("corr-s"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            command);
    var untraced =
        new EventMetadata(
            EventId.of("evt-u"),
            CommandId.of("cmd-u"),
            null,
            CorrelationId.of("corr-u"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"));

    String tracedJson = mapper.writeValueAsString(traced);
    String untracedJson = mapper.writeValueAsString(untraced);

    assertTrue(
        tracedJson.contains("\"spanContext\":{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\""));
    assertEquals(traced, mapper.readValue(tracedJson, EventMetadata.class));
    assertFalse(untracedJson.contains("spanContext"), untracedJson);
    assertSame(SpanLink.NONE, untraced.spanContext());
    assertSame(SpanLink.NONE, mapper.readValue(untracedJson, EventMetadata.class).spanContext());
    // A stored span context that is not a pair of W3C ids reads back as no link.
    String corrupt = tracedJson.replace("0af7651916cd43dd8448eb211c80319c", "not-a-w3c-trace-id");
    assertSame(SpanLink.NONE, mapper.readValue(corrupt, EventMetadata.class).spanContext());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(
      strings = {
        "\"garbage\"",
        "[]",
        "5",
        "\"\"",
        "true",
        "{}",
        "{\"traceId\":{\"x\":1},\"spanId\":\"b7ad6b7169203331\"}",
        "{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":5}",
        "{\"traceId\":\"0af7651916cd43dd8448eb211c80319c\"}",
        "{\"traceId\":\"0AF7651916CD43DD8448EB211C80319C\",\"spanId\":\"b7ad6b7169203331\"}",
        "[\"0af7651916cd43dd8448eb211c80319c\",\"b7ad6b7169203331\"]",
        "null"
      })
  void aMalformedStoredSpanContext_readsAsNone_andNeverMakesTheEventUnreadable(String value)
      throws Exception {
    String stored =
        "{\"eventId\":\"evt-m\",\"commandId\":\"cmd-m\",\"correlationId\":\"corr-m\","
            + "\"timestamp\":\"2026-01-01T00:00:00Z\",\"spanContext\":"
            + value
            + "}";

    EventMetadata md = storeMapper().readValue(stored, EventMetadata.class);

    assertSame(SpanLink.NONE, md.spanContext());
    assertEquals(EventId.of("evt-m"), md.eventId());
  }

  @Test
  void aValidStoredSpanContext_roundTrips() throws Exception {
    String stored =
        "{\"eventId\":\"evt-v\",\"commandId\":\"cmd-v\",\"correlationId\":\"corr-v\","
            + "\"timestamp\":\"2026-01-01T00:00:00Z\",\"spanContext\":{\"traceId\":"
            + "\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\"}}";

    EventMetadata md = storeMapper().readValue(stored, EventMetadata.class);

    assertEquals(
        new SpanLink(TraceId.of("0af7651916cd43dd8448eb211c80319c"), SpanId.of("b7ad6b7169203331")),
        md.spanContext());
    assertEquals(
        md, storeMapper().readValue(storeMapper().writeValueAsString(md), EventMetadata.class));
  }

  @Test
  void aSpanContextWhoseIdsAreNotW3cIds_isLeftOutOfTheJson() throws Exception {
    var md =
        new EventMetadata(
            EventId.of("evt-n"),
            CommandId.of("cmd-n"),
            null,
            CorrelationId.of("corr-n"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            new SpanLink(TraceId.of("from-a-header"), SpanId.of("span")));

    assertFalse(storeMapper().writeValueAsString(md).contains("spanContext"));
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

  @Test
  void storedJsonWithASpanIdKey_reads_andIsWrittenBackWithoutIt() throws Exception {
    ObjectMapper mapper = storeMapper();
    String stored =
        "{\"eventId\":\"evt_1\",\"commandId\":\"cmd_1\","
            + "\"traceId\":\"0af7651916cd43dd8448eb211c80319c\",\"spanId\":\"b7ad6b7169203331\","
            + "\"correlationId\":\"corr_1\",\"causationId\":null,\"userId\":null,"
            + "\"timestamp\":\"2026-01-01T00:00:00Z\",\"baggage\":{},\"sagaOrigin\":null}";

    EventMetadata md = mapper.readValue(stored, EventMetadata.class);

    assertEquals(TraceId.of("0af7651916cd43dd8448eb211c80319c"), md.traceId());
    assertSame(SpanLink.NONE, md.spanContext(), "a stored spanId builds no link");
    String written = mapper.writeValueAsString(md);
    assertFalse(written.contains("spanId"), written);
    assertEquals(md, mapper.readValue(written, EventMetadata.class));
  }
}

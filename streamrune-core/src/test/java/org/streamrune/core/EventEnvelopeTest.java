package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

class EventEnvelopeTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  private static final Instant NOW = Instant.now();

  // Test domain event implementing DomainEvent marker interface
  record TestPayload(String value) implements DomainEvent {}

  private EventMetadata validMetadata() {
    return new EventMetadata(
        EventId.of("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        CommandId.of("cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        TraceId.of("trace-1"),
        SpanId.of("span-1"),
        CorrelationId.of("corr-1"),
        CausationId.of("cause-1"),
        UserId.of("user-1"),
        NOW);
  }

  // --- EventMetadata validation ---

  @Test
  void metadataShouldRejectNullEventId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                null,
                CommandId.of("cmd_x"),
                null,
                null,
                CorrelationId.of("corr"),
                null,
                null,
                NOW));
  }

  @Test
  void metadataShouldRejectNullCommandId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_x"), null, null, null, CorrelationId.of("corr"), null, null, NOW));
  }

  @Test
  void metadataShouldRejectNullCorrelationId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_x"), CommandId.of("cmd_x"), null, null, null, null, null, NOW));
  }

  @Test
  void blankCorrelationIdIsRejectedByCorrelationIdBeforeMetadataSeesIt() {
    // EventMetadata checks only for null: CorrelationId's own constructor rejects a blank value,
    // so a blank correlation id cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.of("  "));
  }

  @Test
  void metadataShouldRejectNullTimestamp() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventMetadata(
                EventId.of("evt_x"),
                CommandId.of("cmd_x"),
                null,
                null,
                CorrelationId.of("corr"),
                null,
                null,
                null));
  }

  @Test
  void metadataShouldAcceptNullableFields() {
    var m =
        new EventMetadata(
            EventId.of("evt_x"),
            CommandId.of("cmd_x"),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            NOW);
    assertEquals("evt_x", m.eventId().value());
    assertEquals("cmd_x", m.commandId().value());
    assertNull(m.traceId());
    assertNull(m.spanId());
    assertNull(m.causationId());
    assertNull(m.userId());
  }

  // --- EventEnvelope validation ---

  @Test
  void envelopeShouldRejectNullGlobalOffset() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope(
                null,
                StreamId.of(TYPE, AggregateId.of("stream-1")),
                Version.initial(),
                new EventType("TestEvent"),
                new TestPayload("test"),
                validMetadata()));
  }

  @Test
  void envelopeShouldRejectNullVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope(
                GlobalOffset.initial(),
                StreamId.of(TYPE, AggregateId.of("stream-1")),
                null,
                new EventType("TestEvent"),
                new TestPayload("test"),
                validMetadata()));
  }

  @Test
  void envelopeShouldRejectNullStreamId() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope(
                GlobalOffset.initial(),
                null,
                Version.initial(),
                new EventType("TestEvent"),
                new TestPayload("test"),
                validMetadata()));
  }

  @Test
  void blankStreamIdPartsAreRejectedBeforeTheEnvelopeSeesThem() {
    // EventEnvelope checks only for null: a stream id is an aggregate type plus an aggregate id,
    // and both refuse a blank value even through their lenient constructors, so a stream id with
    // a blank part cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> StreamId.of(TYPE, new AggregateId("  ")));
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamId.of(new AggregateType("  "), AggregateId.of("stream-1")));
  }

  @Test
  void envelopeShouldRejectNullEventType() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope(
                GlobalOffset.initial(),
                StreamId.of(TYPE, AggregateId.of("stream-1")),
                Version.initial(),
                null,
                new TestPayload("test"),
                validMetadata()));
  }

  @Test
  void envelopeShouldRejectNullEvent() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EventEnvelope(
                GlobalOffset.initial(),
                StreamId.of(TYPE, AggregateId.of("stream-1")),
                Version.initial(),
                new EventType("TestEvent"),
                null,
                validMetadata()));
  }

  @Test
  void envelopeShouldAcceptValidEnvelope() {
    var metadata = validMetadata();
    var version = Version.initial();
    var eventType = new EventType("TestEvent");
    var streamId = StreamId.of(TYPE, AggregateId.of("stream-1"));
    var envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            version,
            eventType,
            new TestPayload("test"),
            metadata);
    assertEquals(streamId, envelope.streamId());
    assertEquals(version, envelope.version());
    assertEquals(eventType, envelope.eventType());
    assertEquals("test", ((TestPayload) envelope.event()).value());
    assertEquals(metadata, envelope.metadata());
  }

  @Test
  void aggregateTypeAndAggregateId_areTheStreamsParts() {
    var stream = StreamId.of(AggregateType.of("cart"), AggregateId.of("c-1"));
    var envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            stream,
            new Version(1),
            new EventType("Added"),
            new TestPayload("added"),
            validMetadata());
    assertEquals(AggregateType.of("cart"), envelope.aggregateType());
    assertEquals(AggregateId.of("c-1"), envelope.aggregateId());
    assertEquals(stream, envelope.streamId());
  }
}

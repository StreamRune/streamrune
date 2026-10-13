package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class SagaEnvelopesTest {

  record State(SagaStatus status) implements SagaState {}

  record StockReserved(String orderId) implements DomainEvent {}

  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");

  @Test
  void metadataCausedBy_namesTheSagaAtHopZero() {
    EventMetadata metadata = SagaEnvelopes.metadataCausedBy(State.class, SAGA_ID);

    assertEquals(
        new SagaOrigin(SagaType.fromClass(State.class), SAGA_ID, 0), metadata.sagaOrigin());
    assertEquals(CorrelationId.of("order-saga-42"), metadata.correlationId());
    assertNotNull(metadata.eventId());
    assertNotNull(metadata.commandId());
    assertNotNull(metadata.timestamp());
    assertNull(metadata.traceId());
    assertNull(metadata.spanId());
    assertNull(metadata.userId());
    assertNull(metadata.causationId());
    assertTrue(metadata.baggage().isEmpty());
    assertNotEquals(
        metadata.eventId(), SagaEnvelopes.metadataCausedBy(State.class, SAGA_ID).eventId());
  }

  @Test
  void causedBy_wrapsTheEventAtTheInitialOffset() {
    StreamId streamId = StreamId.of(AggregateType.of("stock"), AggregateId.of("sku-1"));
    StockReserved event = new StockReserved("order-42");

    EventEnvelope envelope = SagaEnvelopes.causedBy(State.class, SAGA_ID, streamId, event);

    assertEquals(GlobalOffset.initial(), envelope.globalOffset());
    assertEquals(Version.initial(), envelope.version());
    assertEquals(streamId, envelope.streamId());
    assertEquals(EventType.fromClass(StockReserved.class), envelope.eventType());
    assertSame(event, envelope.event());
    assertEquals(
        SagaOrigin.dispatchedBy(SagaType.fromClass(State.class), SAGA_ID),
        envelope.metadata().sagaOrigin());
    assertEquals(CorrelationId.of("order-saga-42"), envelope.metadata().correlationId());
  }
}

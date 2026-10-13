package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
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
    assertEquals(CorrelationId.of("streamrune:saga:order-saga-42"), metadata.correlationId());
    assertEquals(metadata.sagaOrigin().correlationId(), metadata.correlationId());
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
    assertEquals(
        CorrelationId.of("streamrune:saga:order-saga-42"), envelope.metadata().correlationId());
  }

  // ==== The reply to a continuation: a later hop, with a causation id ====

  private static final StreamId PAYMENT =
      StreamId.of(AggregateType.of("payment"), AggregateId.of("pay-1"));

  record Captured(String orderId) implements DomainEvent {}

  @Test
  void continuationOf_isWhatTheBusStoresForACommandDispatchedInsideContinuing() {
    // The oracle is the real path: a command executed on the in-memory bus inside
    // StreamRuneContext.continuing(cause).
    EventEnvelope cause =
        SagaEnvelopes.causedBy(State.class, SAGA_ID, PAYMENT, new StockReserved("order-42"));
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus =
        InMemoryCommandBus.builder()
            .eventStore(eventStore)
            .register(
                new InMemoryCommandBus.DeciderRegistration<>(
                    CommandBusProvenanceContract.AGGREGATE_TYPE,
                    CommandBusProvenanceContract.Mark.class,
                    CommandBusProvenanceContract::aggregateIdOf,
                    CommandBusProvenanceContract.DECIDER))
            .build();
    StreamRuneContext.continuing(cause)
        .run(() -> bus.execute(new CommandBusProvenanceContract.Mark("m-1")));
    EventMetadata stored =
        eventStore.readGlobalStream(GlobalOffset.initial(), 10).getFirst().metadata();

    EventMetadata built = SagaEnvelopes.metadataOfContinuation(cause);

    assertEquals(stored.sagaOrigin(), built.sagaOrigin());
    assertEquals(stored.causationId(), built.causationId());
    assertEquals(stored.correlationId(), built.correlationId());
    assertEquals(stored.traceId(), built.traceId());
    assertEquals(stored.userId(), built.userId());
    assertEquals(stored.baggage(), built.baggage());
    assertEquals(stored.spanId(), built.spanId());
  }

  @Test
  void continuationOf_aSagaCausedEvent_isOneHopFurther_andCausedByThatEvent() {
    EventEnvelope cause =
        SagaEnvelopes.causedBy(State.class, SAGA_ID, PAYMENT, new StockReserved("order-42"));
    Captured event = new Captured("order-42");

    EventEnvelope reply = SagaEnvelopes.continuationOf(cause, PAYMENT, event);

    EventMetadata metadata = reply.metadata();
    assertEquals(
        new SagaOrigin(SagaType.fromClass(State.class), SAGA_ID, 1), metadata.sagaOrigin());
    assertEquals(CausationId.of(cause.metadata().eventId().value()), metadata.causationId());
    assertEquals(CorrelationId.of("streamrune:saga:order-saga-42"), metadata.correlationId());
    assertNotEquals(cause.metadata().eventId(), metadata.eventId());
    assertNotEquals(cause.metadata().commandId(), metadata.commandId());
    assertNull(metadata.userId());
    assertTrue(metadata.baggage().isEmpty());
    assertSame(event, reply.event());
    assertEquals(PAYMENT, reply.streamId());
    assertEquals(EventType.fromClass(Captured.class), reply.eventType());
    assertEquals(GlobalOffset.initial(), reply.globalOffset());
    assertEquals(Version.initial(), reply.version());
  }

  @Test
  void continuationOf_anEventNoSagaCaused_carriesNoOrigin_andTheCausesCorrelationId() {
    EventEnvelope byARequest =
        new EventEnvelope(
            GlobalOffset.of(7),
            PAYMENT,
            Version.initial(),
            EventType.fromClass(StockReserved.class),
            new StockReserved("order-42"),
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                TraceId.of("trace-1"),
                null,
                CorrelationId.of("client-chosen"),
                null,
                UserId.of("alice"),
                Instant.now(),
                Map.of("role", "admin"),
                null));

    EventMetadata metadata =
        SagaEnvelopes.continuationOf(byARequest, PAYMENT, new Captured("order-42")).metadata();

    assertNull(metadata.sagaOrigin());
    assertEquals(CausationId.of(byARequest.metadata().eventId().value()), metadata.causationId());
    assertEquals(CorrelationId.of("client-chosen"), metadata.correlationId());
    assertEquals(TraceId.of("trace-1"), metadata.traceId());
    assertNull(metadata.userId());
    assertTrue(metadata.baggage().isEmpty());
  }

  @Test
  void causedBy_atAHop_chainsThatManyContinuations() {
    Captured event = new Captured("order-42");

    EventEnvelope atHopZero = SagaEnvelopes.causedBy(State.class, SAGA_ID, 0, PAYMENT, event);
    EventEnvelope atHopThree = SagaEnvelopes.causedBy(State.class, SAGA_ID, 3, PAYMENT, event);

    assertEquals(
        new SagaOrigin(SagaType.fromClass(State.class), SAGA_ID, 0),
        atHopZero.metadata().sagaOrigin());
    assertNull(atHopZero.metadata().causationId());
    assertEquals(
        new SagaOrigin(SagaType.fromClass(State.class), SAGA_ID, 3),
        atHopThree.metadata().sagaOrigin());
    assertNotNull(atHopThree.metadata().causationId());
    assertEquals(
        CorrelationId.of("streamrune:saga:order-saga-42"), atHopThree.metadata().correlationId());
    assertSame(event, atHopThree.event());
  }

  @Test
  void causedBy_atANegativeHop_isRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SagaEnvelopes.causedBy(State.class, SAGA_ID, -1, PAYMENT, new Captured("order-42")));
  }

  @Test
  void continuationOf_refusesNulls() {
    EventEnvelope cause =
        SagaEnvelopes.causedBy(State.class, SAGA_ID, PAYMENT, new StockReserved("order-42"));
    assertThrows(NullPointerException.class, () -> SagaEnvelopes.metadataOfContinuation(null));
    assertThrows(
        NullPointerException.class,
        () -> SagaEnvelopes.continuationOf(null, PAYMENT, new Captured("o")));
    assertThrows(
        NullPointerException.class,
        () -> SagaEnvelopes.continuationOf(cause, null, new Captured("o")));
    assertThrows(
        NullPointerException.class, () -> SagaEnvelopes.continuationOf(cause, PAYMENT, null));
  }
}

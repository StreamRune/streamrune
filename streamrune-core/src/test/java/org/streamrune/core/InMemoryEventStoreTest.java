package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;

class InMemoryEventStoreTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  private InMemoryEventStore eventStore;

  @BeforeEach
  void setUp() {
    eventStore = new InMemoryEventStore();
  }

  private static EventMetadata metadata(String correlationId) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of(correlationId),
        null,
        null,
        Instant.now());
  }

  @Test
  void shouldAppendEventsToStream() {
    // Given
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var event =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));

    // When
    eventStore.append(streamId, List.of(event), Version.initial());

    // Then
    var history = eventStore.load(streamId);
    assertEquals(1, history.events().size());
    assertEquals("ItemAdded", history.events().get(0).eventType().name());
  }

  @Test
  void shouldLoadEventsFromStream() {
    // Given
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var event1 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));
    var event2 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(2),
            new EventType("ItemRemoved"),
            new ItemRemovedEvent("item-1"),
            metadata("corr-2"));
    eventStore.append(streamId, List.of(event1, event2), Version.initial());

    // When
    var history = eventStore.load(streamId);

    // Then
    assertEquals(2, history.events().size());
    assertEquals(2, history.version().value());
  }

  @Test
  void shouldReturnEmptyHistoryForUnknownStream() {
    // When
    var history = eventStore.load(StreamId.of(TYPE, AggregateId.of("unknown-stream")));

    // Then
    assertEquals(0, history.events().size());
    assertEquals(0, history.version().value());
    assertNull(history.snapshotState());
  }

  @Test
  void shouldSaveAndLoadSnapshot() {
    // Given
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var cartState = new Cart("cart-1", List.of("item-1"));
    eventStore.saveSnapshot(streamId, new Version(5), cartState);

    // When
    var history = eventStore.load(streamId);

    // Then
    assertEquals(cartState, history.snapshotState());
  }

  @Test
  void shouldAppendEventsAfterSnapshot() {
    // Given - first add some events to build version
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var event1 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));
    var event2 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(2),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-2"),
            metadata("corr-2"));
    eventStore.append(streamId, List.of(event1, event2), Version.initial());

    // Save snapshot at version 2
    var cartState = new Cart("cart-1", List.of("item-1", "item-2"));
    eventStore.saveSnapshot(streamId, new Version(2), cartState);

    // Now append new event - expected version is 2 (stream size)
    var event3 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(3),
            new EventType("ItemRemoved"),
            new ItemRemovedEvent("item-1"),
            metadata("corr-3"));
    eventStore.append(streamId, List.of(event3), new Version(2));

    // When
    var history = eventStore.load(streamId);

    // Then
    assertEquals(cartState, history.snapshotState());
    assertEquals(1, history.events().size());
    assertEquals("ItemRemoved", history.events().get(0).eventType().name());
  }

  @Test
  void shouldThrowOnVersionMismatch() {
    // Given
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var event =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));
    eventStore.append(streamId, List.of(event), Version.initial());

    // When/Then
    var event2 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(2),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-2"),
            metadata("corr-2"));
    assertThrows(
        OptimisticLockException.class,
        () -> eventStore.append(streamId, List.of(event2), Version.initial())); // Wrong version
  }

  @Test
  void shouldReadGlobalStream() {
    // Given
    StreamId streamId1 = StreamId.of(TYPE, AggregateId.of("cart-1"));
    StreamId streamId2 = StreamId.of(TYPE, AggregateId.of("cart-2"));
    var event1 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId1,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));
    var event2 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId2,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-2"),
            metadata("corr-2"));
    var event3 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId1,
            new Version(2),
            new EventType("ItemRemoved"),
            new ItemRemovedEvent("item-1"),
            metadata("corr-3"));
    eventStore.append(streamId1, List.of(event1, event3), Version.initial());
    eventStore.append(streamId2, List.of(event2), Version.initial());

    // When
    var globalEvents = eventStore.readGlobalStream(GlobalOffset.initial(), 10);

    // Then
    assertEquals(3, globalEvents.size());
  }

  @Test
  void shouldFilterGlobalStreamByOffset() {
    // Given
    StreamId streamId1 = StreamId.of(TYPE, AggregateId.of("cart-1"));
    StreamId streamId2 = StreamId.of(TYPE, AggregateId.of("cart-2"));
    var event1 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId1,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-1"),
            metadata("corr-1"));
    var event2 =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId2,
            new Version(1),
            new EventType("ItemAdded"),
            new ItemAddedEvent("item-2"),
            metadata("corr-2"));
    // Note: global offset is assigned automatically by InMemoryEventStore
    eventStore.append(streamId1, List.of(event1), Version.initial());
    eventStore.append(streamId2, List.of(event2), Version.initial());

    // When - read after first event (offset 1 means skip first)
    var globalEvents = eventStore.readGlobalStream(GlobalOffset.of(1), 10);

    // Then - should return event with globalOffset > 1
    assertEquals(1, globalEvents.size());
  }

  // Test events
  record ItemAddedEvent(String itemId) implements DomainEvent {}

  record ItemRemovedEvent(String itemId) implements DomainEvent {}

  record Cart(String id, List<String> items) implements AggregateState {}
}

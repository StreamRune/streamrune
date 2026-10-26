package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

class EventStreamFixturesTest {

  record OrderPlaced(String orderId, int quantity) implements DomainEvent {}

  record OrderShipped(String orderId) implements DomainEvent {}

  record OrderCancelled(String orderId) implements DomainEvent {}

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId ORDER_1 = StreamId.of(ORDER, AggregateId.of("1"));
  private static final StreamId ORDER_2 = StreamId.of(ORDER, AggregateId.of("2"));

  @TempDir Path dir;

  private static EventTypeRegistry registry(Class<?>... events) {
    var builder = SimpleEventTypeRegistry.builder();
    for (Class<?> event : events) {
      builder.registerEvent(event);
    }
    return builder.build();
  }

  private static EventTypeRegistry orders() {
    return registry(OrderPlaced.class, OrderShipped.class, OrderCancelled.class);
  }

  private static EventMetadata metadata(int n) {
    return new EventMetadata(
        EventId.of("event-" + n),
        CommandId.of("command-" + n),
        null,
        null,
        CorrelationId.of("correlation-" + n),
        null,
        null,
        Instant.parse("2026-01-01T00:00:00Z").plusSeconds(n),
        Map.of("tenant", "acme", "region", "eu", "locale", "cs", "channel", "web"));
  }

  private static EventEnvelope envelope(StreamId stream, long version, DomainEvent event, int n) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(version),
        EventType.fromClass(event.getClass()),
        event,
        metadata(n));
  }

  /** order 1: placed and shipped; order 2: placed and cancelled — four events on two streams. */
  private static InMemoryEventStore recorded() {
    var store = InMemoryEventStore.serializing(orders(), null);
    store.append(
        ORDER_1, List.of(envelope(ORDER_1, 1, new OrderPlaced("1", 2), 1)), Version.initial());
    store.append(
        ORDER_2, List.of(envelope(ORDER_2, 1, new OrderPlaced("2", 5), 2)), Version.initial());
    store.append(ORDER_1, List.of(envelope(ORDER_1, 2, new OrderShipped("1"), 3)), new Version(1));
    store.append(
        ORDER_2, List.of(envelope(ORDER_2, 2, new OrderCancelled("2"), 4)), new Version(1));
    return store;
  }

  private static List<EventEnvelope> all(InMemoryEventStore store) {
    return store.readGlobalStream(GlobalOffset.initial(), Integer.MAX_VALUE);
  }

  @Test
  void write_thenRead_givesTheStoredFormOfEveryEvent() throws IOException {
    Path file = dir.resolve("orders.json");

    EventStreamFixtures.write(recorded(), file);
    List<EventStreamFixtures.StoredEvent> events = EventStreamFixtures.read(file);

    assertEquals(4, events.size());
    EventStreamFixtures.StoredEvent first = events.getFirst();
    assertEquals(ORDER_1, first.streamId());
    assertEquals(new Version(1), first.version());
    assertEquals(EventType.fromClass(OrderPlaced.class), first.eventType());
    assertEquals(1, first.schemaVersion());
    assertEquals("1", first.payload().get("orderId").asText());
    assertEquals(2, first.payload().get("quantity").asInt());
    assertEquals(metadata(1), first.metadata());
    assertEquals(ORDER_2, events.get(3).streamId());
    assertEquals(new Version(2), events.get(3).version());
    String text = Files.readString(file);
    List<Integer> keys =
        List.of("channel", "locale", "region", "tenant").stream()
            .map(key -> text.indexOf("\"" + key + "\""))
            .toList();
    assertEquals(
        keys.stream().sorted().toList(),
        keys,
        "map keys are written sorted, so the file does not depend on a map's order: " + text);
    assertTrue(text.endsWith("}\n]\n"), "line ends are \\n and the file ends with one");
  }

  @Test
  void appendTo_aFreshStore_replaysTheRecordedStreamByteForByte() throws IOException {
    InMemoryEventStore original = recorded();
    Path file = dir.resolve("orders.json");
    EventStreamFixtures.write(original, file);

    var replayed = InMemoryEventStore.serializing(orders(), null);
    EventStreamFixtures.appendTo(replayed, EventStreamFixtures.read(file));

    assertEquals(all(original), all(replayed), "offsets, versions, payloads and metadata alike");
    Path again = dir.resolve("again.json");
    EventStreamFixtures.write(replayed, again);
    assertArrayEquals(Files.readAllBytes(file), Files.readAllBytes(again));
  }

  @Test
  void aProjectionReplayedFromAFixtureFile_reachesTheRecordedState() throws IOException {
    Map<String, String> recordedState = project(all(recorded()));
    Path file = dir.resolve("orders.json");
    try (InputStream checkedIn = getClass().getResourceAsStream("/event-streams/orders.json")) {
      Files.copy(checkedIn, file);
    }

    var replayed = InMemoryEventStore.serializing(orders(), null);
    EventStreamFixtures.appendTo(replayed, EventStreamFixtures.read(file));

    assertEquals(Map.of("1", "SHIPPED 2", "2", "CANCELLED 5"), project(all(replayed)));
    assertEquals(recordedState, project(all(replayed)));
  }

  /** The status and quantity of every order, as a projection keeps them. */
  private static Map<String, String> project(List<EventEnvelope> events) {
    Map<String, Integer> quantities = new HashMap<>();
    Map<String, String> orders = new HashMap<>();
    for (EventEnvelope event : events) {
      switch (event.event()) {
        case OrderPlaced e -> {
          quantities.put(e.orderId(), e.quantity());
          orders.put(e.orderId(), "PLACED " + e.quantity());
        }
        case OrderShipped e -> orders.put(e.orderId(), "SHIPPED " + quantities.get(e.orderId()));
        case OrderCancelled e ->
            orders.put(e.orderId(), "CANCELLED " + quantities.get(e.orderId()));
        default -> throw new AssertionError(event);
      }
    }
    return orders;
  }

  @Test
  void aFileWithAnUnknownEventType_failsWithTheRegistrysException() throws IOException {
    Path file = dir.resolve("orders.json");
    EventStreamFixtures.write(recorded(), file);
    var withoutCancellations =
        InMemoryEventStore.serializing(registry(OrderPlaced.class, OrderShipped.class), null);

    assertThrows(
        UnknownEventTypeException.class,
        () -> EventStreamFixtures.appendTo(withoutCancellations, EventStreamFixtures.read(file)));
  }

  /** An upcaster that adds the quantity version 1 of OrderPlaced did not carry. */
  private static final EventUpcaster QUANTITY =
      new EventUpcaster() {
        @Override
        public EventType eventType() {
          return EventType.fromClass(OrderPlaced.class);
        }

        @Override
        public int currentVersion() {
          return 2;
        }

        @Override
        public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
          var upcast = new HashMap<>(eventData);
          upcast.put("quantity", 1);
          return upcast;
        }
      };

  @Test
  void aPayloadBelowTheStoresSchemaVersion_isUpcast_andWriteRecordsTheCurrentVersion()
      throws IOException {
    Path file = dir.resolve("v1.json");
    Files.writeString(
        file,
        """
        [ {
          "streamId" : "order:1",
          "version" : 1,
          "eventType" : "%s",
          "schemaVersion" : 1,
          "payload" : { "orderId" : "1" },
          "metadata" : {
            "eventId" : "event-1",
            "commandId" : "command-1",
            "correlationId" : "correlation-1",
            "timestamp" : "2026-01-01T00:00:01Z"
          }
        } ]
        """
            .formatted(EventType.fromClass(OrderPlaced.class).name()));
    var store = InMemoryEventStore.serializing(orders(), null).withUpcasters(List.of(QUANTITY));

    EventStreamFixtures.appendTo(store, EventStreamFixtures.read(file));

    assertEquals(new OrderPlaced("1", 1), all(store).getFirst().event());
    Path written = dir.resolve("v2.json");
    EventStreamFixtures.write(store, written);
    assertEquals(2, EventStreamFixtures.read(written).getFirst().schemaVersion());
  }

  @Test
  void aPayloadAboveTheStoresSchemaVersion_isRefused() throws IOException {
    Path file = dir.resolve("v2.json");
    EventStreamFixtures.write(
        recordedWith(
            InMemoryEventStore.serializing(orders(), null).withUpcasters(List.of(QUANTITY))),
        file);

    var olderReader = InMemoryEventStore.serializing(orders(), null);

    assertThrows(
        EventSchemaAheadException.class,
        () -> EventStreamFixtures.appendTo(olderReader, EventStreamFixtures.read(file)));
  }

  private static InMemoryEventStore recordedWith(InMemoryEventStore store) {
    store.append(
        ORDER_1, List.of(envelope(ORDER_1, 1, new OrderPlaced("1", 2), 1)), Version.initial());
    return store;
  }

  @Test
  void appendTo_aStoreThatAlreadyHoldsTheStream_isRefusedByTheStore() throws IOException {
    Path file = dir.resolve("orders.json");
    EventStreamFixtures.write(recorded(), file);

    assertThrows(
        OptimisticLockException.class,
        () -> EventStreamFixtures.appendTo(recorded(), EventStreamFixtures.read(file)));
  }

  @Test
  void appendTo_aStoreThatKeepsEventsByReference_isRefused() throws IOException {
    Path file = dir.resolve("orders.json");
    EventStreamFixtures.write(recorded(), file);
    List<EventStreamFixtures.StoredEvent> events = EventStreamFixtures.read(file);

    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> EventStreamFixtures.appendTo(new InMemoryEventStore(), events));
    assertTrue(refused.getMessage().contains("serializing"), refused.getMessage());
  }

  @Test
  void write_ofAStoreThatKeepsEventsByReference_writesTheirJson() throws IOException {
    var store = new InMemoryEventStore();
    store.append(
        ORDER_1, List.of(envelope(ORDER_1, 1, new OrderPlaced("1", 3), 1)), Version.initial());
    Path file = dir.resolve("by-reference.json");

    EventStreamFixtures.write(store, file);

    EventStreamFixtures.StoredEvent event = EventStreamFixtures.read(file).getFirst();
    assertEquals(3, event.payload().get("quantity").asInt());
    assertEquals(1, event.schemaVersion());
  }

  @Test
  void read_ofAFileThatIsNoEventStream_failsNamingTheFile() throws IOException {
    Path file = dir.resolve("broken.json");
    Files.writeString(file, "{ \"not\": \"an array\" }");

    var failure = assertThrows(IOException.class, () -> EventStreamFixtures.read(file));
    assertTrue(failure.getMessage().contains("broken.json"), failure.getMessage());
  }
}

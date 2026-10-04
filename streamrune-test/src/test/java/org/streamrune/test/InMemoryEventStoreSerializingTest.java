package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Serializing-mode tests: {@link InMemoryEventStore#serializing} round-trips every payload through
 * Jackson, surfacing in unit tests the failures {@code PostgresEventStore} raises in production.
 */
class InMemoryEventStoreSerializingTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku, Instant at) implements DomainEvent {}

  record PiiRegistered(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  record PricedOrderPlaced(
      String customerId,
      @Encrypted(subjectId = "customerId") String shippingAddress,
      BigDecimal total,
      Instant placedAt)
      implements DomainEvent {}

  /** Jackson cannot serialize this: plain Object has no properties (FAIL_ON_EMPTY_BEANS). */
  record Unmappable(Object payload) implements DomainEvent {}

  /** Shape-incompatible with {@link Added} — deserializing Added JSON into this fails. */
  record Incompatible(int totallyDifferent) implements DomainEvent {}

  record CartState(int items) implements AggregateState {}

  private final EventTypeRegistry registry =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Added", Added.class)
          .registerEvent("PiiRegistered", PiiRegistered.class)
          .registerEvent("PricedOrderPlaced", PricedOrderPlaced.class)
          .registerEvent("Unmappable", Unmappable.class)
          .registerState("CartState", CartState.class)
          .build();

  private EventMetadata metadata(long v) {
    return new EventMetadata(
        EventId.of("evt_" + v),
        CommandId.of("cmd_" + v),
        null,
        null,
        CorrelationId.of("corr"),
        null,
        null,
        Instant.parse("2026-01-01T00:00:00Z"));
  }

  private EventEnvelope envelope(StreamId id, long v, DomainEvent event, String type) {
    return new EventEnvelope(
        GlobalOffset.of(1), id, new Version(v), new EventType(type), event, metadata(v));
  }

  @Test
  void roundTripsEventAndMetadataThroughJackson() {
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var original = new Added("A", Instant.parse("2026-02-03T04:05:06Z"));
    store.append(s, List.of(envelope(s, 1, original, "Added")), Version.initial());

    var loaded = store.load(s).events().getFirst();
    assertEquals(original, loaded.event(), "round-trip must preserve the event's value");
    assertNotSame(original, loaded.event(), "serializing mode must not store by reference");
    assertEquals(metadata(1), loaded.metadata());
  }

  @Test
  void unserializableEventFailsAtAppendLikePostgres() {
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var bad = new Unmappable(new Object());

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.append(s, List.of(envelope(s, 1, bad, "Unmappable")), Version.initial()));
    assertTrue(ex.getMessage().contains("cart-1"));
    assertTrue(store.load(s).events().isEmpty(), "failed append must leave nothing stored");
    assertEquals(GlobalOffset.of(0), store.lastGlobalOffset());
  }

  @Test
  void undeserializableEventFailsOnReadLikePostgres() {
    // Event type registered under a shape-incompatible class: serialization succeeds at append
    // (Postgres just writes the JSON), and the deserialization leg fails on READ — the same failure
    // PostgresEventStore raises when reading the event back, at the same point in the lifecycle.
    var badRegistry =
        SimpleEventTypeRegistry.builder().registerEvent("Added", Incompatible.class).build();
    var store = InMemoryEventStore.serializing(badRegistry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    // Append succeeds — serialization of a valid Added payload never fails.
    store.append(
        s, List.of(envelope(s, 1, new Added("A", Instant.EPOCH), "Added")), Version.initial());

    var ex = assertThrows(EventStoreException.class, () -> store.load(s));
    assertTrue(ex.getMessage().contains("deserialize"));
  }

  @Test
  void serializationFailureOnAnyEventLeavesNothingAppended() {
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var good = envelope(s, 1, new Added("A", Instant.EPOCH), "Added");
    var bad = envelope(s, 2, new Unmappable(new Object()), "Unmappable");

    assertThrows(
        EventStoreException.class, () -> store.append(s, List.of(good, bad), Version.initial()));
    assertTrue(store.load(s).events().isEmpty(), "atomicity: partial appends are not allowed");
  }

  @Test
  void unregisteredEventTypeFailsOnRead() {
    // The EventTypeRegistry is read-side only (nothing validates the event type at append), so an
    // unregistered type appends fine and surfaces UnknownEventTypeException only on read — matching
    // PostgresEventStore, whose append writes the event_type name without resolving the class.
    var emptyRegistry = SimpleEventTypeRegistry.builder().build();
    var store = InMemoryEventStore.serializing(emptyRegistry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    store.append(
        s, List.of(envelope(s, 1, new Added("A", Instant.EPOCH), "Added")), Version.initial());

    assertThrows(UnknownEventTypeException.class, () -> store.load(s));
  }

  @Test
  void encryptedFieldsRoundTripThroughTheCryptoModule() {
    var store = InMemoryEventStore.serializing(registry, new InMemoryCryptoEngine());
    StreamId s = StreamId.of(TYPE, AggregateId.of("user-1"));
    var original = new PiiRegistered("user-42", "alice@example.com");
    store.append(s, List.of(envelope(s, 1, original, "PiiRegistered")), Version.initial());

    var loaded = (PiiRegistered) store.load(s).events().getFirst().event();
    assertEquals("alice@example.com", loaded.email(), "encrypt-then-decrypt must round-trip");
    assertNotSame(original, loaded);
  }

  @Test
  void encryptedFieldWithNullSubjectIdFailsAtAppendNotInProduction() {
    var store = InMemoryEventStore.serializing(registry, new InMemoryCryptoEngine());
    StreamId s = StreamId.of(TYPE, AggregateId.of("user-1"));
    var bad = new PiiRegistered(null, "alice@example.com");

    // The CryptoShreddingModule fails fast rather than writing plaintext PII. Depending on
    // how Jackson surfaces the serializer failure it may or may not be wrapped — what matters
    // is that the append throws and stores nothing, instead of passing silently in-memory
    // and exploding in production.
    assertThrows(
        RuntimeException.class,
        () -> store.append(s, List.of(envelope(s, 1, bad, "PiiRegistered")), Version.initial()));
    assertTrue(store.load(s).events().isEmpty());
  }

  @Test
  void snapshotStateRoundTripsThroughJackson() {
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(
        s, List.of(envelope(s, 1, new Added("A", Instant.EPOCH), "Added")), Version.initial());
    var state = new CartState(7);
    store.saveSnapshot(s, new Version(1), state);

    var history = store.load(s);
    assertEquals(state, history.snapshotState());
    assertNotSame(state, history.snapshotState(), "snapshot must not be stored by reference");
  }

  @Test
  void snapshotWithUnregisteredStateTypeFailsLikePostgres() {
    var emptyRegistry = SimpleEventTypeRegistry.builder().build();
    var store = InMemoryEventStore.serializing(emptyRegistry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.saveSnapshot(s, new Version(1), new CartState(1)));
    assertTrue(ex.getMessage().contains("does not resolve"));
  }

  @Test
  void snapshotWithMismatchedRegistrationFailsLikePostgres() {
    var mismatched =
        SimpleEventTypeRegistry.builder()
            .registerState("CartState", InMemoryEventStoreSerializingTest.class)
            .build();
    var store = InMemoryEventStore.serializing(mismatched, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.saveSnapshot(s, new Version(1), new CartState(1)));
    assertTrue(ex.getMessage().contains("resolves to"));
  }

  @Test
  void byReferenceModeStoresTheSameInstance() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var original = new Added("A", Instant.EPOCH);
    store.append(s, List.of(envelope(s, 1, original, "Added")), Version.initial());

    assertSame(original, store.load(s).events().getFirst().event());
  }

  @Test
  void objectMapperConstructorValidatesArguments() {
    var mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    assertThrows(NullPointerException.class, () -> new InMemoryEventStore(null, registry));
    assertThrows(NullPointerException.class, () -> new InMemoryEventStore(mapper, null));
    assertDoesNotThrow(() -> new InMemoryEventStore(mapper, registry));
  }

  @Test
  void encryptedEventKeepsItsOtherComponentsExactThroughTheStore() {
    // The store's mapper writes an Instant as seconds.nanos and a BigDecimal with its scale; an
    // event carrying an @Encrypted component must read both back exactly, as a plain event does.
    var store = InMemoryEventStore.serializing(registry, new InMemoryCryptoEngine());
    StreamId s = StreamId.of(TYPE, AggregateId.of("order-19"));
    var original =
        new PricedOrderPlaced(
            "user-19",
            "1 Main St",
            new BigDecimal("19.90"),
            Instant.ofEpochSecond(1_700_000_000L, 123_456_789));
    store.append(s, List.of(envelope(s, 1, original, "PricedOrderPlaced")), Version.initial());

    var loaded = (PricedOrderPlaced) store.load(s).events().getFirst().event();

    assertEquals(original, loaded, "an encrypted event must round-trip its other components");
    assertEquals(2, loaded.total().scale(), "BigDecimal scale must survive the load");
  }
}

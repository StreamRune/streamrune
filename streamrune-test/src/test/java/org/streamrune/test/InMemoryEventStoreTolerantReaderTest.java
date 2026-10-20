package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * The serializing double reads event payloads as a tolerant reader, as {@code PostgresEventStore}
 * does, and {@link InMemoryEventStore#withStrictEventPayloads(boolean)} restores the strict reader.
 * Encrypted fields decrypt, and redact after a crypto-shred, exactly as before.
 */
class InMemoryEventStoreTolerantReaderTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  record Placed(String orderId) implements DomainEvent {}

  /** What a newer release writes for the same type: one more field. */
  record PlacedWithGiftWrap(String orderId, boolean giftWrap) implements DomainEvent {}

  record Registered(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  record RegisteredWithLocale(
      String userId, @Encrypted(subjectId = "userId") String email, String locale)
      implements DomainEvent {}

  private final StreamId stream = StreamId.of(TYPE, AggregateId.of("a-1"));

  @Test
  void aPayloadCarryingAPropertyTheClassLacks_readsByDefault() {
    var store = InMemoryEventStore.serializing(placedRegistry(), null);
    append(store, new PlacedWithGiftWrap("o-1", true), "Placed");

    assertEquals(new Placed("o-1"), store.load(stream).events().getFirst().event());
  }

  @Test
  void withStrictEventPayloads_refusesIt() {
    var store =
        InMemoryEventStore.serializing(placedRegistry(), null).withStrictEventPayloads(true);
    append(store, new PlacedWithGiftWrap("o-1", true), "Placed");

    assertThrows(EventDeserializationException.class, () -> store.load(stream));
  }

  @Test
  void anUpcastPayloadCarryingAPropertyTheClassLacks_bindsByDefault_andIsRefusedWhenStrict() {
    var store = InMemoryEventStore.serializing(placedRegistry(), null);
    append(store, new Placed("o-1"), "Placed");
    store.withUpcasters(List.of(addingChannel()));

    assertEquals(new Placed("o-1"), store.load(stream).events().getFirst().event());
    store.withStrictEventPayloads(true);
    assertThrows(EventDeserializationException.class, () -> store.load(stream));
  }

  @Test
  void anEncryptedPayloadCarryingAnUnknownProperty_decrypts_andRedactsAfterACryptoShred() {
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("Registered", Registered.class).build();
    var crypto = new InMemoryCryptoEngine();
    var store = InMemoryEventStore.serializing(registry, crypto);
    append(store, new RegisteredWithLocale("user-42", "alice@example.com", "cs"), "Registered");

    assertEquals(
        new Registered("user-42", "alice@example.com"),
        store.load(stream).events().getFirst().event());

    crypto.deleteKey(SubjectId.of("user-42"));
    assertEquals(
        new Registered("user-42", "[REDACTED]"), store.load(stream).events().getFirst().event());
  }

  private static SimpleEventTypeRegistry placedRegistry() {
    return SimpleEventTypeRegistry.builder().registerEvent("Placed", Placed.class).build();
  }

  private void append(InMemoryEventStore store, DomainEvent event, String type) {
    store.append(
        stream,
        List.of(
            new EventEnvelope(
                GlobalOffset.of(1),
                stream,
                new Version(1),
                new EventType(type),
                event,
                new EventMetadata(
                    EventId.of("evt_1"),
                    CommandId.of("cmd_1"),
                    null,
                    null,
                    CorrelationId.of("corr"),
                    null,
                    null,
                    Instant.parse("2026-01-01T00:00:00Z")))),
        Version.initial());
  }

  private static EventUpcaster addingChannel() {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("Placed");
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        var next = new HashMap<>(eventData);
        next.putIfAbsent("channel", "web");
        return next;
      }
    };
  }
}

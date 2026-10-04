package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
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
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * Read-time transformation parity between {@link InMemoryEventStore#serializing} and {@code
 * PostgresEventStore}: crypto-shredding-on-replay, upcasting-on-read, and retired-snapshot-type
 * discard. The double stores the SERIALIZED form (JSON/ciphertext) and re-runs decrypt + upcast +
 * registry-resolution on every read path — so GDPR-erasure and schema-evolution unit tests written
 * against the double behave exactly as they would against Postgres, instead of silently lying.
 */
class InMemoryEventStoreReadTransformParityTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  record PiiRegistered(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  record PiiState(String userId, @Encrypted(subjectId = "userId") String email)
      implements AggregateState {}

  // A V1 event class (only "value"); the registry below resolves "Evolving" to EvolvingV2 so a
  // stored V1 payload is upcast on read.
  record EvolvingV1(String value) implements DomainEvent {}

  record EvolvingV2(String value, String extra) implements DomainEvent {}

  record RetiredState(String name) implements AggregateState {}

  record CurrentState(String name) implements AggregateState {}

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
        GlobalOffset.of(v), id, new Version(v), new EventType(type), event, metadata(v));
  }

  // === Crypto-shredding on replay ===

  @Test
  void cryptoShredRedactsEncryptedEventOnReplay() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("PiiRegistered", PiiRegistered.class)
            .build();
    var crypto = new InMemoryCryptoEngine();
    var store = InMemoryEventStore.serializing(registry, crypto);
    StreamId s = StreamId.of(TYPE, AggregateId.of("user-1"));
    store.append(
        s,
        List.of(envelope(s, 1, new PiiRegistered("user-42", "alice@example.com"), "PiiRegistered")),
        Version.initial());

    // GDPR erasure: shred the subject's key.
    crypto.deleteKey(SubjectId.of("user-42"));

    // A replay must NOT resurface the plaintext PII: read-time decrypt fails → [REDACTED].
    var loaded = (PiiRegistered) store.load(s).events().getFirst().event();
    assertEquals("user-42", loaded.userId(), "the subjectId field itself is not encrypted");
    assertEquals(
        "[REDACTED]",
        loaded.email(),
        "after deleteKey a replay must return the crypto-shred tombstone, not the plaintext PII");
  }

  @Test
  void cryptoShredRedactsEncryptedSnapshotStateOnReplay() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("PiiRegistered", PiiRegistered.class)
            .registerState("PiiState", PiiState.class)
            .build();
    var crypto = new InMemoryCryptoEngine();
    var store = InMemoryEventStore.serializing(registry, crypto);
    StreamId s = StreamId.of(TYPE, AggregateId.of("user-1"));
    store.append(
        s,
        List.of(envelope(s, 1, new PiiRegistered("user-42", "alice@example.com"), "PiiRegistered")),
        Version.initial());
    store.saveSnapshot(s, new Version(1), new PiiState("user-42", "alice@example.com"), 1);

    crypto.deleteKey(SubjectId.of("user-42"));

    AggregateHistory history = store.load(s);
    var snapshot = (PiiState) history.snapshotState();
    assertEquals("user-42", snapshot.userId());
    assertEquals(
        "[REDACTED]",
        snapshot.email(),
        "a crypto-shredded subject's snapshot state must also redact on replay");
  }

  // === Upcasting on read ===

  @Test
  void upcastsEventOnReadInSerializingMode() {
    // Registry resolves "Evolving" to the CURRENT class (V2). The event is appended as a V1 shape.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("Evolving", EvolvingV2.class).build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("agg-1"));

    // Append a V1 payload BEFORE any upcaster is configured, so it is stamped schema version 1
    // (mirroring an event written by a prior deployment).
    store.append(
        s, List.of(envelope(s, 1, new EvolvingV1("original"), "Evolving")), Version.initial());

    // Now register an upcaster that adds "extra" when upgrading V1 -> V2.
    var upcaster =
        new EventUpcaster() {
          @Override
          public EventType eventType() {
            return new EventType("Evolving");
          }

          @Override
          public int currentVersion() {
            return 2;
          }

          @Override
          public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
            if (fromVersion == 1) {
              var result = new HashMap<>(eventData);
              result.put("extra", "added-by-upcaster");
              return result;
            }
            return eventData;
          }
        };
    store.withUpcasters(List.of(upcaster));

    var loaded = store.load(s).events().getFirst().event();
    assertInstanceOf(EvolvingV2.class, loaded);
    var v2 = (EvolvingV2) loaded;
    assertEquals("original", v2.value());
    assertEquals("added-by-upcaster", v2.extra(), "the upcaster must run on the read path");
  }

  private EventUpcaster upcaster(String type, int currentVersion) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType(type);
      }

      @Override
      public int currentVersion() {
        return currentVersion;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return eventData; // identity — the guard fires before any upcast would run
      }
    };
  }

  @Test
  void readFailsClosedWhenStoredSchemaVersionExceedsReaderChain() {
    // An event stamped ABOVE the reader's current upcaster version is never legitimate —
    // it proves the reading store is mis-wired (missing the upcasters the type was written with) or
    // downgraded. Reading on would silently mis-bind the payload; instead the read must fail
    // closed,
    // mirroring PostgresEventStore, so the double cannot mask the divergence.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("Evolving", EvolvingV1.class).build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("agg-ahead"));

    // Append WITH a chain that reports Evolving currentVersion = 2, so the row is stamped
    // schema_version = 2 (an event written by a store wired with these upcasters).
    store.withUpcasters(List.of(upcaster("Evolving", 2)));
    store.append(s, List.of(envelope(s, 1, new EvolvingV1("x"), "Evolving")), Version.initial());

    // Swap in a reader chain that no longer knows "Evolving" (currentVersion falls back to 1) — as
    // if
    // a second, chainless/older store read the same data. Stored 2 > reader's 1 → fail closed.
    store.withUpcasters(List.of(upcaster("Other", 2)));

    var ex = assertThrows(EventStoreException.class, () -> store.load(s));
    assertTrue(ex.getMessage().contains("Evolving"), ex.getMessage());
  }

  @Test
  void readFailsClosedWhenStoredSchemaVersionExceedsChainlessReader() {
    // The version guard once only branched when upcasterChain != null. A reader configured with NO
    // upcaster chain — a supported construction — skipped the
    // guard entirely, so a stored schema_version >= 2 fell through to a plain readValue and
    // silently mis-bound a newer-shaped payload as if it were current. A chainless reader
    // understands only v1 (the WRITE path already stamps schema_version = 1 when chainless, line
    // ~498), so a stored v2 must fail closed just as the chain-present path does — same durable
    // mis-bind class, same loud failure.
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("Evolving", EvolvingV1.class).build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("agg-ahead-chainless"));

    // Stamp schema_version = 2 by appending under a chain that reports Evolving currentVersion = 2
    // (an event written by a store wired with these upcasters).
    store.withUpcasters(List.of(upcaster("Evolving", 2)));
    store.append(s, List.of(envelope(s, 1, new EvolvingV1("x"), "Evolving")), Version.initial());

    // Now read with NO chain at all — the null-chain sibling path (a chainless store reading
    // the same data). Stored 2 > a chainless reader's known current version 1 → must fail closed.
    store.withUpcasters(null);

    var ex = assertThrows(EventStoreException.class, () -> store.load(s));
    assertTrue(ex.getMessage().contains("Evolving"), ex.getMessage());
  }

  // === Retired snapshot-type discard on read ===

  @Test
  void retiredSnapshotTypeIsDiscardedOnReadInSerializingMode() {
    // A toggleable registry: resolves "RetiredState" until the type is "retired", then throws
    // UnknownEventTypeException on read — mirroring a state class removed from the registry in a
    // later deployment (Postgres resolves the stored state_type via the non-throwing resolver and
    // DISCARDS the snapshot rather than crashing the load).
    var retired = new AtomicBoolean(false);
    EventTypeRegistry togglingRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return EvolvingV1.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            if ("RetiredState".equals(stateType) && !retired.get()) {
              return RetiredState.class;
            }
            if ("CurrentState".equals(stateType)) {
              return CurrentState.class;
            }
            throw new UnknownEventTypeException("state type", stateType, List.of("CurrentState"));
          }
        };
    var store = InMemoryEventStore.serializing(togglingRegistry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("agg-retired"));
    store.append(
        s,
        List.of(
            new EventEnvelope(
                GlobalOffset.of(1),
                s,
                new Version(1),
                new EventType("Evolving"),
                new EvolvingV1("a"),
                metadata(1)),
            new EventEnvelope(
                GlobalOffset.of(2),
                s,
                new Version(2),
                new EventType("Evolving"),
                new EvolvingV1("b"),
                metadata(2))),
        Version.initial());
    store.saveSnapshot(s, new Version(1), new RetiredState("gone"), 1);

    // Retire the state type: it no longer resolves in the registry.
    retired.set(true);

    AggregateHistory history = store.load(s);
    assertNull(history.snapshotState(), "a retired stored state type must be discarded, not crash");
    assertEquals(2, history.events().size(), "all events replay after the snapshot is discarded");
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  @Test
  void retiredSnapshotTypeWithLiveKeyStillCrashesNothing() {
    // Sanity companion: while the type IS registered, the snapshot rehydrates normally.
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Evolving", EvolvingV1.class)
            .registerState("CurrentState", CurrentState.class)
            .build();
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId s = StreamId.of(TYPE, AggregateId.of("agg-live"));
    store.append(s, List.of(envelope(s, 1, new EvolvingV1("a"), "Evolving")), Version.initial());
    store.saveSnapshot(s, new Version(1), new CurrentState("here"), 1);

    AggregateHistory history = store.load(s);
    assertInstanceOf(CurrentState.class, history.snapshotState());
    assertEquals("here", ((CurrentState) history.snapshotState()).name());
  }
}

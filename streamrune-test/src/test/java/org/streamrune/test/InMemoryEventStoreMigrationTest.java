package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
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

/**
 * Snapshot-migration parity tests for {@link InMemoryEventStore}, mirroring the store-level
 * behaviour exercised for {@code PostgresEventStore}: a registered chain is applied on load; a
 * gapped chain discards the snapshot and replays; a multi-step chain applies in order; and
 * registration rejects invalid chains.
 */
class InMemoryEventStoreMigrationTest {

  private static final AggregateType TYPE = AggregateType.of("user");

  record Renamed(String name) implements DomainEvent {}

  record UserState(String name) implements AggregateState {}

  record UserStateV2(String name, int age) implements AggregateState {}

  record UserStateV3(String name, int age, boolean active) implements AggregateState {}

  private static final SnapshotMigration ONE_TO_TWO =
      new SnapshotMigration() {
        @Override
        public int fromVersion() {
          return 1;
        }

        @Override
        public int toVersion() {
          return 2;
        }

        @Override
        public AggregateState migrate(AggregateState state) {
          return new UserStateV2(((UserState) state).name(), 42);
        }
      };

  private static final SnapshotMigration TWO_TO_THREE =
      new SnapshotMigration() {
        @Override
        public int fromVersion() {
          return 2;
        }

        @Override
        public int toVersion() {
          return 3;
        }

        @Override
        public AggregateState migrate(AggregateState state) {
          UserStateV2 v2 = (UserStateV2) state;
          return new UserStateV3(v2.name(), v2.age(), true);
        }
      };

  private final SimpleEventTypeRegistry registry =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Renamed", Renamed.class)
          .registerState("UserState", UserState.class)
          .registerState("UserStateV2", UserStateV2.class)
          .registerState("UserStateV3", UserStateV3.class)
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

  private EventEnvelope renamedEnvelope(StreamId id, long v, String name) {
    return new EventEnvelope(
        GlobalOffset.of(v),
        id,
        new Version(v),
        new EventType("Renamed"),
        new Renamed(name),
        metadata(v));
  }

  @Test
  void migratesSnapshotOnLoadAndReplaysOnlyLaterEvents() {
    var store = InMemoryEventStore.serializing(registry, null).withMigrations(List.of(ONE_TO_TWO));
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-1"));
    store.append(
        id,
        List.of(
            renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b"), renamedEnvelope(id, 3, "c")),
        Version.initial());
    // v1 snapshot at stream version 2
    store.saveSnapshot(id, new Version(2), new UserState("snapshot-name"), 1);

    AggregateHistory history = store.load(id, 2);

    // The v1 snapshot was migrated to v2, observably (age=42 from the migrate() transform).
    assertInstanceOf(UserStateV2.class, history.snapshotState());
    UserStateV2 migrated = (UserStateV2) history.snapshotState();
    assertEquals("snapshot-name", migrated.name());
    assertEquals(42, migrated.age());
    // Only the event after the snapshot (version 3) replays.
    assertEquals(1, history.events().size());
    assertEquals(3, history.events().getFirst().version().value());
  }

  @Test
  void gappedChainDiscardsSnapshotAndReplaysFromZero() {
    // Only 1->2 registered; expected 3 -> gap at 2->3 -> discard + full replay.
    var store = InMemoryEventStore.serializing(registry, null).withMigrations(List.of(ONE_TO_TWO));
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-2"));
    store.append(
        id,
        List.of(
            renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b"), renamedEnvelope(id, 3, "c")),
        Version.initial());
    store.saveSnapshot(id, new Version(2), new UserState("snapshot-name"), 1);

    AggregateHistory history = store.load(id, 3);

    assertNull(history.snapshotState(), "gapped chain must discard the snapshot");
    assertEquals(3, history.events().size(), "all events replay from version 0");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  @Test
  void multiStepChainAppliesAllStepsInOrder() {
    var store =
        InMemoryEventStore.serializing(registry, null)
            .withMigrations(List.of(ONE_TO_TWO, TWO_TO_THREE));
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-3"));
    store.append(
        id, List.of(renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b")), Version.initial());
    store.saveSnapshot(id, new Version(1), new UserState("multi"), 1);

    AggregateHistory history = store.load(id, 3);

    assertInstanceOf(UserStateV3.class, history.snapshotState());
    UserStateV3 v3 = (UserStateV3) history.snapshotState();
    assertEquals("multi", v3.name());
    assertEquals(42, v3.age());
    assertTrue(v3.active());
    // Snapshot was at version 1 -> event at version 2 replays.
    assertEquals(1, history.events().size());
    assertEquals(2, history.events().getFirst().version().value());
  }

  // ── A throwing migration step must discard-and-replay, not wedge the aggregate ────

  private static final SnapshotMigration THROWING_ONE_TO_TWO =
      new SnapshotMigration() {
        @Override
        public int fromVersion() {
          return 1;
        }

        @Override
        public int toVersion() {
          return 2;
        }

        @Override
        public AggregateState migrate(AggregateState state) {
          // Simulates a programming bug in the step (e.g. an unchecked cast against the wrong
          // input shape) — SnapshotMigrationChain propagates whatever RuntimeException the step
          // throws unchanged.
          throw new ClassCastException("simulated: step assumed the wrong input shape");
        }
      };

  @Test
  void throwingMigrationDiscardsSnapshotAndReplaysFromInitial() {
    // InMemoryEventStore parity with PostgresEventStore: SnapshotMigrationChain
    // .migrate() can throw (NullPointerException for a null-returning step, IllegalStateException
    // for a wrong-toType() step), and that propagated straight out of
    // load(), wedging the aggregate exactly like the undeserializable-snapshot guard exists
    // to prevent. A migration step is registered CODE, and a bug in it is exactly as
    // untrustworthy as a field renamed without a version bump — discard the derived cache and
    // rebuild from the source of truth (events) instead of crashing load().
    var store =
        InMemoryEventStore.serializing(registry, null).withMigrations(List.of(THROWING_ONE_TO_TWO));
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-throwing-migration"));
    store.append(
        id,
        List.of(
            renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b"), renamedEnvelope(id, 3, "c")),
        Version.initial());
    // v1 snapshot at stream version 2; load(id, 2) hits the migration path (canReach(1,2) holds).
    store.saveSnapshot(id, new Version(2), new UserState("snap"), 1);

    AggregateHistory history = store.load(id, 2);

    assertNull(history.snapshotState(), "a throwing migration step must discard, not crash load()");
    assertEquals(
        3, history.events().size(), "ALL events replay from initial, not only post-snapshot ones");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(3, history.events().getLast().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  private static final SnapshotMigration CRYPTO_THROWING_ONE_TO_TWO =
      new SnapshotMigration() {
        @Override
        public int fromVersion() {
          return 1;
        }

        @Override
        public int toVersion() {
          return 2;
        }

        @Override
        public AggregateState migrate(AggregateState state) {
          throw new CryptoOperationException(
              "simulated: KMS outage during migration-time re-encryption");
        }
      };

  @Test
  void cryptoCausedMigrationFailurePropagatesAndIsNotDiscarded() {
    // Fail-closed parity with snapshotCryptoDecryptFailurePropagatesAndIsNotDiscarded below:
    // discarding a snapshot whose migration failed for a CRYPTO reason risks silently dropping a
    // GDPR-erased subject's redaction, and replaying from events would hit the identical crypto
    // failure on the same encrypted fields anyway — crypto failures must stay fatal.
    var store =
        InMemoryEventStore.serializing(registry, null)
            .withMigrations(List.of(CRYPTO_THROWING_ONE_TO_TWO));
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-crypto-throwing-migration"));
    store.append(
        id, List.of(renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b")), Version.initial());
    store.saveSnapshot(id, new Version(1), new UserState("snap"), 1);

    var thrown = assertThrows(RuntimeException.class, () -> store.load(id, 2));
    boolean cryptoInChain = false;
    for (Throwable c = thrown; c != null; c = c.getCause()) {
      if (c instanceof CryptoOperationException) {
        cryptoInChain = true;
        break;
      }
    }
    assertTrue(cryptoInChain, "a crypto-caused migration failure must propagate, not be discarded");
  }

  @Test
  void noMigrationsRegisteredDiscardsMismatchedSnapshot() {
    // No migrations registered + version mismatch -> the snapshot is discarded and the stream
    // replays from the start.
    var store = InMemoryEventStore.serializing(registry, null);
    StreamId id = StreamId.of(TYPE, AggregateId.of("user-4"));
    store.append(
        id, List.of(renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b")), Version.initial());
    store.saveSnapshot(id, new Version(1), new UserState("compat"), 1);

    AggregateHistory history = store.load(id, 2);

    assertNull(history.snapshotState());
    assertEquals(2, history.events().size());
  }

  record WideState(String keep, String extra) implements AggregateState {}

  record NarrowState(String keep) implements AggregateState {}

  /**
   * Resolves state to whatever {@link #stateClass} currently holds — lets a test evolve the shape.
   */
  static final class SwappableRegistry implements org.streamrune.core.EventTypeRegistry {
    volatile Class<?> stateClass;

    SwappableRegistry(Class<?> initial) {
      this.stateClass = initial;
    }

    @Override
    public Class<?> resolveEventType(EventType eventType) {
      return Renamed.class;
    }

    @Override
    public Class<?> resolveStateType(String stateType) {
      return stateClass;
    }

    @Override
    public java.util.Collection<Class<?>> registeredTypes() {
      return List.of(stateClass);
    }
  }

  @Test
  void undeserializableSnapshotIsDiscardedAndReplaysFromInitial() {
    // Test-double parity with PostgresEventStore: a stored snapshot whose payload can no
    // longer
    // be deserialized (a field dropped without bumping the snapshot version) is a stale derived
    // CACHE — discard it and replay ALL events from the beginning, never crash the load. The
    // snapshot sits at stream version 2, so replay must still yield all three events (not just the
    // one after the snapshot).
    var reg = new SwappableRegistry(WideState.class);
    var store = InMemoryEventStore.serializing(reg, null);
    StreamId id = StreamId.of(TYPE, AggregateId.of("corrupt-1"));
    store.append(
        id,
        List.of(
            renamedEnvelope(id, 1, "a"), renamedEnvelope(id, 2, "b"), renamedEnvelope(id, 3, "c")),
        Version.initial());
    store.saveSnapshot(id, new Version(2), new WideState("keep", "dropme"), 1);

    // The aggregate-state record loses its 'extra' field without a snapshotVersion bump: the stored
    // JSON now carries an unknown property for NarrowState.
    reg.stateClass = NarrowState.class;

    AggregateHistory history = store.load(id, 0);

    assertNull(history.snapshotState(), "undeserializable snapshot must be discarded, not crash");
    assertEquals(3, history.events().size(), "ALL events replay from initial after discard");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(3, history.events().getLast().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  record Secret(String subject, @Encrypted(subjectId = "subject") String value) {
    @JsonCreator
    Secret(@JsonProperty("subject") String subject, @JsonProperty("value") String value) {
      this.subject = subject;
      this.value = value;
    }
  }

  record NestedSecretState(String id, Secret secret) implements AggregateState {
    @JsonCreator
    NestedSecretState(@JsonProperty("id") String id, @JsonProperty("secret") Secret secret) {
      this.id = id;
      this.secret = secret;
    }
  }

  static final class DecryptFailingEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return plaintext;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      throw new CryptoOperationException("transient outage during snapshot decrypt");
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  @Test
  void snapshotCryptoDecryptFailurePropagatesAndIsNotDiscarded() {
    // Fail-closed parity: a crypto decrypt failure on a NESTED @Encrypted field is wrapped
    // into
    // a JsonMappingException — the same exception the discard path sees for a plain unmappable
    // payload — so it must still PROPAGATE, never be swallowed as a discardable cache.
    var reg = new SwappableRegistry(NestedSecretState.class);
    var store = InMemoryEventStore.serializing(reg, new DecryptFailingEngine());
    StreamId id = StreamId.of(TYPE, AggregateId.of("nested-secret-1"));
    store.saveSnapshot(
        id, new Version(1), new NestedSecretState("agg-1", new Secret("subj-1", "top-secret")), 1);

    var thrown = assertThrows(RuntimeException.class, () -> store.load(id));
    boolean cryptoInChain = false;
    for (Throwable c = thrown; c != null; c = c.getCause()) {
      if (c instanceof CryptoOperationException) {
        cryptoInChain = true;
        break;
      }
    }
    assertTrue(cryptoInChain, "crypto decrypt failure must propagate, not be swallowed by discard");
  }

  @Test
  void registrationRejectsDuplicateFromVersion() {
    var dup =
        new SnapshotMigration() {
          @Override
          public int fromVersion() {
            return 1;
          }

          @Override
          public int toVersion() {
            return 9;
          }

          @Override
          public AggregateState migrate(AggregateState state) {
            return state;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () ->
            InMemoryEventStore.serializing(registry, null)
                .withMigrations(List.of(ONE_TO_TWO, dup)));
  }

  @Test
  void registrationRejectsNonIncreasingVersion() {
    var bad =
        new SnapshotMigration() {
          @Override
          public int fromVersion() {
            return 3;
          }

          @Override
          public int toVersion() {
            return 2;
          }

          @Override
          public AggregateState migrate(AggregateState state) {
            return state;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () -> InMemoryEventStore.serializing(registry, null).withMigrations(List.of(bad)));
  }
}

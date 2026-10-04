package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Store-level snapshot-migration tests for {@code PostgresEventStore} against a real PostgreSQL
 * container: a registered chain is applied on load (deserializing against the STORED — old — state
 * type); a gapped chain discards the snapshot and replays; a multi-step chain applies in order; a
 * retired stored type is discarded rather than crashing; and, with no migrations, a version
 * mismatch still discards and replays exactly as before.
 */
@Testcontainers
class PostgresEventStoreMigrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;

  record Renamed(String name) implements DomainEvent {}

  record UserState(String name) implements AggregateState {}

  record UserStateV2(String name, int age) implements AggregateState {}

  record UserStateV3(String name, int age, boolean active) implements AggregateState {}

  /** Registry knowing every state class — used to write snapshots and read migrated states. */
  static EventTypeRegistry fullRegistry;

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

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    fullRegistry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Renamed", Renamed.class)
            .registerState("UserState", UserState.class)
            .registerState("UserStateV2", UserStateV2.class)
            .registerState("UserStateV3", UserStateV3.class)
            .build();

    // Run Flyway migrations via factory
    new PostgresEventStoreFactory(dataSource, fullRegistry).create();
  }

  @BeforeEach
  void clean() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM event_audit_log");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  private static ObjectMapper mapper() {
    var m = new ObjectMapper();
    m.registerModule(new JavaTimeModule());
    return m;
  }

  private PostgresEventStore store(EventTypeRegistry registry, List<SnapshotMigration> migrations) {
    return new PostgresEventStore(
        dataSource,
        mapper(),
        registry,
        null,
        null,
        null,
        null,
        null,
        org.streamrune.core.SnapshotMigrationChain.of(migrations),
        StreamRuneMetrics.NOOP);
  }

  private PostgresEventStore storeWithMetrics(
      EventTypeRegistry registry, List<SnapshotMigration> migrations, StreamRuneMetrics metrics) {
    return new PostgresEventStore(
        dataSource,
        mapper(),
        registry,
        null,
        null,
        null,
        null,
        null,
        org.streamrune.core.SnapshotMigrationChain.of(migrations),
        metrics);
  }

  /**
   * Writes a snapshot row directly, so tests can plant a genuinely unmappable {@code
   * state_payload}.
   */
  private void insertRawSnapshot(
      StreamId id, long version, String stateType, String payloadJson, int snapshotVersion)
      throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, state_type,"
                    + " state_payload, snapshot_version) VALUES (?, ?, ?, ?, ?::jsonb, ?)")) {
      ps.setString(1, id.aggregateType().value());
      ps.setString(2, id.aggregateId().value());
      ps.setLong(3, version);
      ps.setString(4, stateType);
      ps.setString(5, payloadJson);
      ps.setInt(6, snapshotVersion);
      ps.executeUpdate();
    }
  }

  /** Captures {@link StreamRuneMetrics#recordSnapshotDiscarded(String)} calls. */
  static final class CountingSnapshotMetrics implements StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger discardCount =
        new java.util.concurrent.atomic.AtomicInteger();
    volatile String lastReason;

    @Override
    public void recordSnapshotDiscarded(String reason) {
      discardCount.incrementAndGet();
      lastReason = reason;
    }
  }

  private EventMetadata metadata() {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("corr-1"),
        null,
        null,
        Instant.now());
  }

  private EventEnvelope renamed(StreamId id, long v, String name) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        id,
        new Version(v),
        new EventType("Renamed"),
        new Renamed(name),
        metadata());
  }

  @Test
  void migratesSnapshotOnLoadAndReplaysOnlyLaterEvents() {
    var s = store(fullRegistry, List.of(ONE_TO_TWO));
    StreamId id = TestStreams.stream("user-1");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    // Store a v1 snapshot (old UserState) at stream version 2.
    s.saveSnapshot(id, new Version(2), new UserState("snap"), 1);

    AggregateHistory history = s.load(id, 2);

    assertInstanceOf(UserStateV2.class, history.snapshotState());
    UserStateV2 migrated = (UserStateV2) history.snapshotState();
    assertEquals("snap", migrated.name());
    assertEquals(42, migrated.age());
    // Only the event after the snapshot (version 3) replays.
    assertEquals(1, history.events().size());
    assertEquals(3, history.events().getFirst().version().value());
  }

  @Test
  void gappedChainDiscardsSnapshotAndReplaysFromZero() {
    // Only 1->2 registered; expected 3 -> gap -> discard + full replay, correct final state.
    var s = store(fullRegistry, List.of(ONE_TO_TWO));
    StreamId id = TestStreams.stream("user-2");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    s.saveSnapshot(id, new Version(2), new UserState("snap"), 1);

    AggregateHistory history = s.load(id, 3);

    assertNull(history.snapshotState(), "gapped chain discards the snapshot");
    assertEquals(3, history.events().size(), "all events replay from version 0");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  @Test
  void multiStepChainAppliesAllStepsInOrder() {
    var s = store(fullRegistry, List.of(ONE_TO_TWO, TWO_TO_THREE));
    StreamId id = TestStreams.stream("user-3");
    s.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    s.saveSnapshot(id, new Version(1), new UserState("multi"), 1);

    AggregateHistory history = s.load(id, 3);

    assertInstanceOf(UserStateV3.class, history.snapshotState());
    UserStateV3 v3 = (UserStateV3) history.snapshotState();
    assertEquals("multi", v3.name());
    assertEquals(42, v3.age());
    assertTrue(v3.active());
    assertEquals(1, history.events().size());
    assertEquals(2, history.events().getFirst().version().value());
  }

  @Test
  void retiredStoredTypeIsDiscardedNotCrashed() {
    // Save a v1 snapshot under UserState via the full registry.
    var writer = store(fullRegistry, List.of());
    StreamId id = TestStreams.stream("user-4");
    writer.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    writer.saveSnapshot(id, new Version(1), new UserState("gone"), 1);

    // Reader registry no longer knows "UserState" (retired), only the new types. The 1->2 migration
    // is registered, so canReach(1,2) holds — but the stored type is undeserializable → discard.
    EventTypeRegistry readerRegistry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Renamed", Renamed.class)
            .registerState("UserStateV2", UserStateV2.class)
            .build();
    var reader = store(readerRegistry, List.of(ONE_TO_TWO));

    AggregateHistory history = reader.load(id, 2);

    assertNull(history.snapshotState(), "retired stored type must discard, not crash");
    assertEquals(2, history.events().size());
    assertEquals(1, history.events().getFirst().version().value());
  }

  @Test
  void retiredStoredTypeOnCompatibleLoadIsDiscardedNotCrashed() {
    // Companion to retiredStoredTypeIsDiscardedNotCrashed (which covers the version-MISMATCH
    // branch). Here the load hits the COMPATIBLE branch — plain load(id) (expectedSnapshotVersion
    // == 0) and load(id, 1) (stored snapshot_version == expected). Before the fix this branch used
    // the THROWING typeRegistry.resolveStateType(...), so a retired/renamed stored state type made
    // every load() throw UnknownEventTypeException and the aggregate became permanently unloadable.
    var writer = store(fullRegistry, List.of());
    StreamId id = TestStreams.stream("user-compat-retired");
    writer.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    writer.saveSnapshot(id, new Version(1), new UserState("gone"), 1);

    // Reader registry no longer knows "UserState" (retired), only the newer types.
    EventTypeRegistry readerRegistry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Renamed", Renamed.class)
            .registerState("UserStateV2", UserStateV2.class)
            .build();
    var reader = store(readerRegistry, List.of());

    // (1) Plain load(id) → load(id, 0) → expectedSnapshotVersion == 0 → compatible branch.
    AggregateHistory plain = reader.load(id);
    assertNull(plain.snapshotState(), "retired stored type on plain load must discard, not crash");
    assertEquals(2, plain.events().size(), "all events replay after discard");
    assertEquals(1, plain.events().getFirst().version().value());
    assertEquals(Version.initial(), plain.lastSnapshotVersion());

    // (2) load(id, 1) → stored snapshot_version (1) == expected (1) → compatible branch.
    AggregateHistory matched = reader.load(id, 1);
    assertNull(
        matched.snapshotState(), "retired stored type on matching-version load must discard");
    assertEquals(2, matched.events().size());
    assertEquals(1, matched.events().getFirst().version().value());
    assertEquals(Version.initial(), matched.lastSnapshotVersion());
  }

  @Test
  void undeserializableSnapshotIsDiscardedAndReplaysFromInitialWithMetric() throws Exception {
    // A stored snapshot whose payload can no longer be deserialized (the classic mistake — an
    // aggregate-state field renamed/removed WITHOUT bumping snapshotVersion) must be treated as a
    // stale derived CACHE: discarded, with the aggregate rebuilt by replaying ALL events from the
    // beginning (the source of truth) — NOT crash load() and permanently wedge the aggregate.
    var metrics = new CountingSnapshotMetrics();
    var s = storeWithMetrics(fullRegistry, List.of(), metrics);
    StreamId id = TestStreams.stream("user-corrupt-snap");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    // Write the snapshot row directly so the payload is genuinely unmappable: it carries a property
    // ("removedField") that the CURRENT UserState record does not declare →
    // FAIL_ON_UNKNOWN_PROPERTIES.
    // The snapshot is stored at stream version 2, so a naive catch-and-null (without re-reading
    // from
    // the beginning) would drop events 1 and 2 — this test pins that ALL THREE events replay.
    insertRawSnapshot(id, 2, "UserState", "{\"name\":\"snap\",\"removedField\":\"boom\"}", 1);

    AggregateHistory history = s.load(id, 0);

    assertNull(history.snapshotState(), "undeserializable snapshot must be discarded, not crash");
    assertEquals(
        3, history.events().size(), "ALL events replay from initial, not only post-snapshot ones");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(3, history.events().getLast().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(1, metrics.discardCount.get(), "snapshot-discard metric emitted exactly once");
    assertEquals("deserialize_failure", metrics.lastReason);
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
  void throwingMigrationDiscardsSnapshotAndReplaysFromInitial() throws Exception {
    // SnapshotMigrationChain.migrate() can throw (a step that returns null throws
    // NullPointerException by contract; a step that returns the wrong toType() throws
    // IllegalStateException) and that propagated straight out of load()
    // uncaught, wedging the aggregate exactly like the comment warns against ("every command
    // load() fails, the bus dead-letters it, replays fail identically") — the one snapshot-read
    // failure mode the discard guard did not cover. A migration step is registered CODE, and a
    // bug in it is no different from a field being renamed without a version bump: the stored
    // snapshot is an untrustworthy DERIVED CACHE either way, and the source of truth (events) is
    // still there to rebuild from.
    var metrics = new CountingSnapshotMetrics();
    var s = storeWithMetrics(fullRegistry, List.of(THROWING_ONE_TO_TWO), metrics);
    StreamId id = TestStreams.stream("user-throwing-migration");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    // v1 snapshot at stream version 2; load(id, 2) hits the migration path (canReach(1,2) holds).
    s.saveSnapshot(id, new Version(2), new UserState("snap"), 1);

    AggregateHistory history = s.load(id, 2);

    assertNull(history.snapshotState(), "a throwing migration step must discard, not crash load()");
    assertEquals(
        3, history.events().size(), "ALL events replay from initial, not only post-snapshot ones");
    assertEquals(1, history.events().getFirst().version().value());
    assertEquals(3, history.events().getLast().version().value());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(1, metrics.discardCount.get(), "snapshot-discard metric emitted exactly once");
    assertEquals(
        "migration_failure",
        metrics.lastReason,
        "must use a distinct reason from deserialize_failure so an operator can tell a broken"
            + " migration step apart from a plain field-rename");
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
          throw new org.streamrune.core.crypto.CryptoOperationException(
              "simulated: KMS outage during migration-time re-encryption");
        }
      };

  @Test
  void cryptoCausedMigrationFailurePropagatesAndIsNotDiscarded() {
    // Fail-closed parity with snapshotCryptoDecryptFailurePropagatesAndIsNotDiscarded (the
    // deserialize-path crypto guard): discarding a snapshot whose migration failed for a CRYPTO
    // reason would either mask a genuine key-store outage as an ordinary cache miss, or — worse —
    // silently drop evidence of a GDPR-erased subject's redaction, since replaying from events hits
    // the identical crypto failure on the same encrypted fields. Crypto failures must stay fatal.
    var metrics = new CountingSnapshotMetrics();
    var s = storeWithMetrics(fullRegistry, List.of(CRYPTO_THROWING_ONE_TO_TWO), metrics);
    StreamId id = TestStreams.stream("user-crypto-throwing-migration");
    s.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    s.saveSnapshot(id, new Version(1), new UserState("snap"), 1);

    var ex = assertThrows(RuntimeException.class, () -> s.load(id, 2));
    assertTrue(
        PostgresEventStore.hasCryptoCause(ex),
        "the propagated exception must carry the CryptoOperationException cause");
    assertEquals(0, metrics.discardCount.get(), "a crypto-caused failure must never be discarded");
  }

  @Test
  void noMigrationsRegisteredDiscardsMismatchedSnapshot() {
    // No migrations registered + version mismatch -> discard + replay.
    var s = store(fullRegistry, List.of());
    StreamId id = TestStreams.stream("user-5");
    s.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    s.saveSnapshot(id, new Version(1), new UserState("compat"), 1);

    AggregateHistory history = s.load(id, 2);

    assertNull(history.snapshotState());
    assertEquals(2, history.events().size());
  }

  @Test
  void builderRejectsDuplicateFromVersion() {
    var dup =
        new SnapshotMigration() {
          @Override
          public int fromVersion() {
            return 1;
          }

          @Override
          public int toVersion() {
            return 5;
          }

          @Override
          public AggregateState migrate(AggregateState state) {
            return state;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgresEventStore.builder()
                .dataSource(dataSource)
                .typeRegistry(fullRegistry)
                .migrations(List.of(ONE_TO_TWO, dup))
                .build());
  }

  @Test
  void builderRejectsNonIncreasingVersion() {
    var bad =
        new SnapshotMigration() {
          @Override
          public int fromVersion() {
            return 3;
          }

          @Override
          public int toVersion() {
            return 3;
          }

          @Override
          public AggregateState migrate(AggregateState state) {
            return state;
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgresEventStore.builder()
                .dataSource(dataSource)
                .typeRegistry(fullRegistry)
                .migrations(List.of(bad))
                .build());
  }

  @Test
  void factorySnapshotMigrationsWireIntoStore() {
    // The factory registration point actually applies migrations on load.
    var factory =
        new PostgresEventStoreFactory(dataSource, fullRegistry)
            .autoInitializeSchema(false)
            .validateSchema(false)
            .snapshotMigrations(List.of(ONE_TO_TWO));
    var s = factory.create();
    StreamId id = TestStreams.stream("user-6");
    s.append(id, List.of(renamed(id, 1, "a"), renamed(id, 2, "b")), Version.initial());
    s.saveSnapshot(id, new Version(1), new UserState("via-factory"), 1);

    AggregateHistory history = s.load(id, 2);

    assertInstanceOf(UserStateV2.class, history.snapshotState());
    assertEquals("via-factory", ((UserStateV2) history.snapshotState()).name());
    assertEquals(42, ((UserStateV2) history.snapshotState()).age());
  }

  // ── A throwing metrics backend must not re-wedge the self-heal branches ─────────

  /**
   * Metrics double standing in for a broken observability backend (e.g. Micrometer registering the
   * snapshot-discard counter lazily against a closed or misconfigured registry). The discard
   * branches exist so an unreadable DERIVED-CACHE snapshot cannot wedge an aggregate; the
   * observability side channel must not be able to put that wedge back.
   */
  static final class ThrowingSnapshotMetrics implements StreamRuneMetrics {
    @Override
    public void recordSnapshotDiscarded(String reason) {
      throw new IllegalStateException("simulated metrics backend failure");
    }
  }

  @Test
  void undeserializableSnapshotDiscardSurvivesThrowingMetrics() throws Exception {
    // metrics.recordSnapshotDiscarded("deserialize_failure") ran bare inside the discard
    // branch, so a throwing backend propagated out of load() — exactly the permanent per-aggregate
    // wedge (every command load() fails, DLQ, identical replay failure) the branch was written to
    // prevent. Sibling PgNotificationListener.recordReconnect guards the same class of call.
    var s = storeWithMetrics(fullRegistry, List.of(), new ThrowingSnapshotMetrics());
    StreamId id = TestStreams.stream("user-corrupt-snap-throwing-metrics");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    insertRawSnapshot(id, 2, "UserState", "{\"name\":\"snap\",\"removedField\":\"boom\"}", 1);

    AggregateHistory history = s.load(id, 0);

    assertNull(history.snapshotState(), "the discard must still happen when the metric throws");
    assertEquals(3, history.events().size(), "ALL events still replay from initial");
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }

  @Test
  void throwingMigrationDiscardSurvivesThrowingMetrics() throws Exception {
    // metrics.recordSnapshotDiscarded("migration_failure") ran bare
    // inside the branch with the identical consequence.
    var s =
        storeWithMetrics(fullRegistry, List.of(THROWING_ONE_TO_TWO), new ThrowingSnapshotMetrics());
    StreamId id = TestStreams.stream("user-throwing-migration-throwing-metrics");
    s.append(
        id,
        List.of(renamed(id, 1, "a"), renamed(id, 2, "b"), renamed(id, 3, "c")),
        Version.initial());
    s.saveSnapshot(id, new Version(2), new UserState("snap"), 1);

    AggregateHistory history = s.load(id, 2);

    assertNull(history.snapshotState(), "the discard must still happen when the metric throws");
    assertEquals(3, history.events().size(), "ALL events still replay from initial");
    assertEquals(Version.initial(), history.lastSnapshotVersion());
  }
}

package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** {@link PostgresSnapshotStorePurger}. */
@Testcontainers
class PostgresSnapshotStorePurgerTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;
  PostgresEventStore store;

  record PurgerTestState(String value) implements AggregateState {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return null;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return PurgerTestState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(PurgerTestState.class);
          }
        };

    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM snapshot_store");
    }
  }

  /** The streams that still have a stored state; a purged stream keeps a row without one. */
  private static Set<String> remainingSnapshotStreamIds() throws Exception {
    var ids = new java.util.HashSet<String>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT aggregate_type, aggregate_id FROM snapshot_store"
                    + " WHERE state_payload IS NOT NULL")) {
      while (rs.next()) {
        ids.add(rs.getString(1) + ":" + rs.getString(2));
      }
    }
    return ids;
  }

  @Test
  void purgeDeletesOnlyTheMappedStreamsSnapshots() throws Exception {
    StreamId subjectStream = TestStreams.stream("order-subject-1");
    StreamId otherStream = TestStreams.stream("order-other");
    store.saveSnapshot(subjectStream, new Version(1), new PurgerTestState("v1"));
    store.saveSnapshot(otherStream, new Version(1), new PurgerTestState("v1"));

    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(subjectStream));

    purger.purge(SubjectId.of("subject-1"));

    assertEquals(
        Set.of("test:order-other"),
        remainingSnapshotStreamIds(),
        "only the mapped stream's snapshot must be deleted");
  }

  @Test
  void purgeLeavesTheSnapshotOfAnotherTypeThatSharesTheIdValue() throws Exception {
    StreamId customer = StreamId.of(AggregateType.of("customer"), new AggregateId("c-1"));
    StreamId order = StreamId.of(AggregateType.of("order"), new AggregateId("c-1"));
    store.saveSnapshot(customer, new Version(1), new PurgerTestState("v1"));
    store.saveSnapshot(order, new Version(1), new PurgerTestState("v1"));

    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(customer));

    purger.purge(SubjectId.of("c-1"));

    assertEquals(Set.of("order:c-1"), remainingSnapshotStreamIds());
  }

  @Test
  void purgeDeletesEveryMappedStream() throws Exception {
    StreamId streamA = TestStreams.stream("order-a");
    StreamId streamB = TestStreams.stream("order-b");
    StreamId untouched = TestStreams.stream("order-untouched");
    store.saveSnapshot(streamA, new Version(1), new PurgerTestState("v1"));
    store.saveSnapshot(streamB, new Version(1), new PurgerTestState("v1"));
    store.saveSnapshot(untouched, new Version(1), new PurgerTestState("v1"));

    var purger =
        new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(streamA, streamB));

    purger.purge(SubjectId.of("subject-multi"));

    assertEquals(Set.of("test:order-untouched"), remainingSnapshotStreamIds());
  }

  @Test
  void purgeIsIdempotent_secondCallIsANoOp() throws Exception {
    StreamId subjectStream = TestStreams.stream("order-idempotent");
    store.saveSnapshot(subjectStream, new Version(1), new PurgerTestState("v1"));

    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(subjectStream));

    purger.purge(SubjectId.of("subject-1"));
    assertDoesNotThrow(() -> purger.purge(SubjectId.of("subject-1")));
    assertTrue(remainingSnapshotStreamIds().isEmpty());
  }

  @Test
  void purgeWithNoMappedStreamsIsANoOp() throws Exception {
    StreamId untouched = TestStreams.stream("order-no-mapping");
    store.saveSnapshot(untouched, new Version(1), new PurgerTestState("v1"));

    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of());
    purger.purge(SubjectId.of("subject-unmapped"));

    assertEquals(Set.of("test:order-no-mapping"), remainingSnapshotStreamIds());
  }

  @Test
  void purgeWithNullMappingResultIsANoOp() throws Exception {
    StreamId untouched = TestStreams.stream("order-null-mapping");
    store.saveSnapshot(untouched, new Version(1), new PurgerTestState("v1"));

    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> null);
    assertDoesNotThrow(() -> purger.purge(SubjectId.of("subject-x")));

    assertEquals(Set.of("test:order-null-mapping"), remainingSnapshotStreamIds());
  }

  private static long purgeGeneration(StreamId stream) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT purge_generation FROM snapshot_store"
                    + " WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, stream.aggregateType().value());
      ps.setString(2, stream.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the purged stream keeps its row");
        return rs.getLong(1);
      }
    }
  }

  @Test
  void purgeRaisesTheGenerationOfTheStreamEveryTime() throws Exception {
    StreamId stream = TestStreams.stream("order-generation");
    store.saveSnapshot(stream, new Version(1), new PurgerTestState("v1"));
    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(stream));

    purger.purge(SubjectId.of("subject-gen"));
    assertEquals(1, purgeGeneration(stream));
    purger.purge(SubjectId.of("subject-gen"));
    assertEquals(2, purgeGeneration(stream));
  }

  @Test
  void purgeOfAStreamWithoutASnapshotLeavesARowWithoutAState() throws Exception {
    StreamId stream = TestStreams.stream("order-never-snapshotted");
    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(stream));

    purger.purge(SubjectId.of("subject-none"));

    assertEquals(1, purgeGeneration(stream));
    assertTrue(remainingSnapshotStreamIds().isEmpty());
  }

  @Test
  void aStreamNamedTwiceByTheMappingIsPurgedOnce() throws Exception {
    StreamId stream = TestStreams.stream("order-twice");
    store.saveSnapshot(stream, new Version(1), new PurgerTestState("v1"));
    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(stream, stream));

    purger.purge(SubjectId.of("subject-twice"));

    assertEquals(1, purgeGeneration(stream));
    assertTrue(remainingSnapshotStreamIds().isEmpty());
  }

  @Test
  void aSaveFromALoadBeforeThePurgeWritesNothingAndOneFromALoadAfterItWrites() throws Exception {
    StreamId stream = TestStreams.stream("order-save-after-purge");
    store.saveSnapshot(stream, new Version(1), new PurgerTestState("before"));
    long seenBefore = store.load(stream).snapshotPurgeGeneration();
    new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of(stream))
        .purge(SubjectId.of("subject-save"));

    assertFalse(
        store.saveSnapshot(stream, new Version(2), new PurgerTestState("stale"), 1, seenBefore));
    assertTrue(remainingSnapshotStreamIds().isEmpty());

    var afterPurge = store.load(stream);
    assertNull(afterPurge.snapshotState(), "a purged row is no snapshot");
    assertEquals(1, afterPurge.snapshotPurgeGeneration());
    assertTrue(
        store.saveSnapshot(
            stream,
            new Version(3),
            new PurgerTestState("after"),
            1,
            afterPurge.snapshotPurgeGeneration()));
    assertEquals(new PurgerTestState("after"), store.load(stream).snapshotState());
    assertEquals(1, purgeGeneration(stream), "a save never changes the purge generation");
  }

  /**
   * The forget service's builder refuses a service without a {@link
   * org.streamrune.core.gdpr.SnapshotPurger} unless the application states it stores no snapshots;
   * this purger is the one that satisfies it for {@code snapshot_store}.
   */
  @Test
  void isTheSnapshotPurgerTheForgetServiceRequires() {
    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of());
    assertInstanceOf(org.streamrune.core.gdpr.SnapshotPurger.class, purger);
  }

  @Test
  void nameIsStable() {
    var purger = new PostgresSnapshotStorePurger(dataSource, subjectId -> List.of());
    assertEquals("snapshot_store", purger.name());
  }

  @Test
  void constructorRejectsNullArguments() {
    assertThrows(
        NullPointerException.class,
        () -> new PostgresSnapshotStorePurger(null, subjectId -> List.of()));
    assertThrows(
        NullPointerException.class, () -> new PostgresSnapshotStorePurger(dataSource, null));
  }
}

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

  private static Set<String> remainingSnapshotStreamIds() throws Exception {
    var ids = new java.util.HashSet<String>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT aggregate_type, aggregate_id FROM snapshot_store")) {
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

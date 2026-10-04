package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresOffsetStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  PostgresOffsetStore offsetStore;

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // The shipped event-store baseline, applied the way the factory applies it.
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @BeforeEach
  void setUp() throws Exception {
    offsetStore = new PostgresOffsetStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_offset");
    }
  }

  @Test
  void returnsInitialForUnknownProjection() {
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("unknown")));
  }

  @Test
  void savesAndReadsOffset() {
    offsetStore.saveOffset(ProjectionName.of("my-projection"), GlobalOffset.of(42));

    assertEquals(
        GlobalOffset.of(42), offsetStore.getLastOffset(ProjectionName.of("my-projection")));
  }

  @Test
  void upsertUpdatesExistingOffset() {
    offsetStore.saveOffset(ProjectionName.of("my-projection"), GlobalOffset.of(10));
    offsetStore.saveOffset(ProjectionName.of("my-projection"), GlobalOffset.of(50));

    assertEquals(
        GlobalOffset.of(50), offsetStore.getLastOffset(ProjectionName.of("my-projection")));
  }

  @Test
  void isolatesProjectionsByName() {
    offsetStore.saveOffset(ProjectionName.of("proj-a"), GlobalOffset.of(100));
    offsetStore.saveOffset(ProjectionName.of("proj-b"), GlobalOffset.of(200));

    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(ProjectionName.of("proj-a")));
    assertEquals(GlobalOffset.of(200), offsetStore.getLastOffset(ProjectionName.of("proj-b")));
  }

  @Test
  void savesZeroOffsetOnInsert() {
    // A first save of offset 0 (initial) inserts the row; there is nothing to regress against.
    offsetStore.saveOffset(ProjectionName.of("my-projection"), GlobalOffset.initial());

    assertEquals(
        GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("my-projection")));
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresOffsetStore(null));
  }

  @Test
  void saveOffsetRejectsRegressionSilentNoOp() {
    ProjectionName name = ProjectionName.of("guarded");
    offsetStore.saveOffset(name, GlobalOffset.of(300));
    offsetStore.saveOffset(name, GlobalOffset.of(200)); // backward → no-op, no throw
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
    offsetStore.saveOffset(name, GlobalOffset.of(301)); // forward advances
    assertEquals(GlobalOffset.of(301), offsetStore.getLastOffset(name));
  }

  @Test
  void saveOffsetRejectsSidewaysWriteSilentNoOp() {
    ProjectionName name = ProjectionName.of("guarded-eq");
    offsetStore.saveOffset(name, GlobalOffset.of(300));
    offsetStore.saveOffset(name, GlobalOffset.of(300)); // equal → no-op, no throw
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
  }

  @Test
  void resetRewindsBypassingGuard() {
    // reset() must rewind to 0 even though the monotonic guard would reject a regression via
    // saveOffset. This would FAIL if reset() delegated to saveOffset(initial()) (0-row no-op).
    ProjectionName name = ProjectionName.of("reset-me");
    offsetStore.saveOffset(name, GlobalOffset.of(500));
    offsetStore.reset(name);
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
    // After reset, forward saves resume normally (the row still exists at 0).
    offsetStore.saveOffset(name, GlobalOffset.of(10));
    assertEquals(GlobalOffset.of(10), offsetStore.getLastOffset(name));
  }

  @Test
  void resetOnUnknownProjectionIsNoOpAndStaysInitial() {
    ProjectionName name = ProjectionName.of("never-seen");
    offsetStore.reset(name); // inserts/updates to 0; no prior row
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }
}

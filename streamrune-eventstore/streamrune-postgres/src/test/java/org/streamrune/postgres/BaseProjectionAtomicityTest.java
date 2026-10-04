package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Proves the framework's own {@link BaseProjection} base class is protected by the Monotonic guard
 * when driven through {@link JdbcProjectionRepository#executeAtomically}. A {@code BaseProjection}
 * subclass writes only through its inherited {@code save()/findById()} helpers; those must route
 * through the transaction-scoped repository the runner supplies (via the 2-arg {@code
 * process(batch, repository)} override) so a re-delivered/already-applied batch rolls back with the
 * offset guard instead of committing a second read-model write on a separate auto-commit
 * connection.
 */
@Testcontainers
class BaseProjectionAtomicityTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_baseproj_test");

  static PGSimpleDataSource dataSource;
  static PostgresOffsetStore offsetStore;

  record Counter(String id, int applyCount) {}

  /**
   * Non-idempotent accumulating projection: each processed batch increments a persisted counter by
   * one, using only the {@link BaseProjection}-inherited {@code save()/findById()} helpers (no
   * direct transaction-scoped repository access). This is the exact composition that was flagged.
   */
  static final class CountingProjection extends BaseProjection {
    CountingProjection(ProjectionRepository repository) {
      super(repository, "base_counting");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      int current = findById("counter", Counter.class).map(Counter::applyCount).orElse(0);
      save("counter", new Counter("counter", current + 1));
    }
  }

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    createOffsetTable();
    offsetStore = new PostgresOffsetStore(dataSource);
  }

  static void createOffsetTable() {
    // The shipped event-store baseline, applied the way the factory applies it. This test only
    // uses projection_offset (with its epoch fencing column).
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @BeforeEach
  void clean() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_offset");
      stmt.execute("DROP TABLE IF EXISTS base_counting_view");
    }
  }

  @Test
  void baseProjectionReDeliveredBatchIsNotDoubleApplied() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("base_counting");
    var projection = new CountingProjection(repo);

    // Apply once at offset 300 → applyCount 1.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(300),
        0L,
        tx -> projection.process(List.of(), tx),
        offsetStore);
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
    assertEquals(1, repo.findById(name, "counter", Counter.class).orElseThrow().applyCount());

    // Re-deliver an already-applied range (offset 200 <= 300): the monotonic guard no-ops the
    // offset advance and executeAtomically rolls the whole transaction back. Before the fix
    // the counter write went through the captured auto-commit repository and had ALREADY committed
    // (applyCount 2) by the time the guard fired — a silent double-apply. It must stay 1.
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(200),
                0L,
                tx -> projection.process(List.of(), tx),
                offsetStore));

    assertEquals(
        1,
        repo.findById(name, "counter", Counter.class).orElseThrow().applyCount(),
        "a BaseProjection re-delivery must not double-apply the read model");
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
  }

  @Test
  void baseProjectionForwardBatchesAccumulateInTheTransaction() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("base_counting");
    var projection = new CountingProjection(repo);

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(10),
        0L,
        tx -> projection.process(List.of(), tx),
        offsetStore);
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(20),
        0L,
        tx -> projection.process(List.of(), tx),
        offsetStore);

    // Two forward batches → applyCount 2, offset 20. findById inside process must observe the
    // in-transaction write from the same batch (routes through the tx-scoped repository).
    assertEquals(2, repo.findById(name, "counter", Counter.class).orElseThrow().applyCount());
    assertEquals(GlobalOffset.of(20), offsetStore.getLastOffset(name));
  }

  @Test
  void baseProjectionUpdaterFailureRollsBackTheReadModelWrite() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("base_counting");
    var projection = new CountingProjection(repo);

    assertThrows(
        IllegalStateException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(5),
                0L,
                tx -> {
                  projection.process(List.of(), tx);
                  throw new IllegalStateException("boom after the base-projection write");
                },
                offsetStore));

    // The write ran on the transaction connection, so the failure rolled it back with the offset.
    assertTrue(repo.findById(name, "counter", Counter.class).isEmpty());
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }
}

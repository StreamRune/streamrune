package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionCommitFencedException.Guard;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Black-box characterization of {@link PostgresCheckpointFence} as it behaves today, before its
 * mechanics move to shared code: the exact exception type, guard and message of each rejection, the
 * order the guards run in, the statements one takeover stamp runs, and the wrapping of a failed
 * connection. These assertions are frozen while the fence is extracted; a change to any of them is
 * a behaviour change.
 */
@Testcontainers
class PostgresCheckpointFenceCharacterizationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_fence_characterization");

  private static final ProjectionName P = ProjectionName.of("p");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void applyBothSeries() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    ShippedMigrationSeries.EVENT_STORE
        .configure(dataSource)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
    ShippedMigrationSeries.READ_MODEL
        .configure(dataSource)
        .table("flyway_schema_history_readmodel")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate();
  }

  /** Every test starts from no checkpoint row for projection {@code p} in either table. */
  @BeforeEach
  void deleteCheckpoints() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var st = conn.createStatement()) {
      for (CheckpointTable table : CheckpointTable.values()) {
        st.executeUpdate("DELETE FROM " + table.tableName() + " WHERE projection_name = 'p'");
      }
    }
  }

  /** Runs {@code work} in a manual transaction, the way the batch path does, and rolls it back. */
  private static void inTransaction(TransactionWork work) throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        work.run(conn);
      } finally {
        conn.rollback();
      }
    }
  }

  @FunctionalInterface
  private interface TransactionWork {
    void run(Connection conn) throws Exception;
  }

  /** Stores offset {@code offset} and epoch {@code epoch} for {@code p}, committed. */
  private static void store(PostgresCheckpointFence fence, long offset, long epoch) {
    fence.stampEpoch(dataSource, P, epoch);
    if (offset > 0) {
      fence.saveUnlocked(dataSource, P, GlobalOffset.of(offset));
    }
  }

  private static ProjectionCommitFencedException fenced(Executable call) {
    var e = assertThrows(ProjectionCommitFencedException.class, call);
    assertSame(ProjectionCommitFencedException.class, e.getClass());
    assertNull(e.getCause());
    assertEquals(0, e.getSuppressed().length);
    return e;
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpoch_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 0L, 5L);

    inTransaction(
        c -> {
          var e = fenced(() -> fence.lockForBatch(c, P, 4L, null));
          assertSame(Guard.EPOCH_FENCE, e.guard());
          assertEquals(
              "Projection 'p' commit fenced out: caller epoch 4 is below the stored epoch 5 — a"
                  + " newer leader has taken over the lease. Rolling back the stale leader's write"
                  + " (epoch fencing).",
              e.getMessage());
        });
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void overlappingBatch_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 10L, 0L);

    inTransaction(
        c -> {
          var e = fenced(() -> fence.lockForBatch(c, P, 0L, GlobalOffset.of(10)));
          assertSame(Guard.OVERLAP, e.guard());
          assertEquals(
              "Projection 'p' batch starting at offset 10 overlaps the committed checkpoint (10):"
                  + " those events were already applied by another writer (a concurrent or"
                  + " failed-over stale leader, or a redelivery). Rolling back so the read-model"
                  + " write is not applied a second time.",
              e.getMessage());
        });
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void monotonicRejection_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 10L, 0L);

    for (long offset : new long[] {10L, 9L}) {
      inTransaction(
          c -> {
            assertEquals(10L, fence.lockForBatch(c, P, 0L, null));
            var e = fenced(() -> fence.saveLocked(c, P, GlobalOffset.of(offset), 0L));
            assertSame(Guard.MONOTONIC, e.guard());
            assertEquals(
                "Projection 'p' offset advance to "
                    + offset
                    + " was rejected by the monotonic guard (stored offset already at/past it):"
                    + " the batch is already applied. Rolling back so the read-model write is not"
                    + " committed a second time.",
                e.getMessage());
          });
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpochIsCheckedBeforeOverlap(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 10L, 5L);

    inTransaction(
        c -> {
          var e = fenced(() -> fence.lockForBatch(c, P, 4L, GlobalOffset.of(10)));
          assertSame(Guard.EPOCH_FENCE, e.guard());
          assertEquals(
              "Projection 'p' commit fenced out: caller epoch 4 is below the stored epoch 5 — a"
                  + " newer leader has taken over the lease. Rolling back the stale leader's write"
                  + " (epoch fencing).",
              e.getMessage());
        });
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void zeroCallerEpochNeverFences(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 10L, 5L);

    try (Connection c = dataSource.getConnection()) {
      c.setAutoCommit(false);
      assertEquals(10L, fence.lockForBatch(c, P, 0L, GlobalOffset.of(11)));
      fence.saveLocked(c, P, GlobalOffset.of(11), 0L);
      c.commit();
    }

    assertEquals(GlobalOffset.of(11), fence.lastOffset(dataSource, P));
    assertEquals(5L, fence.readEpoch(dataSource, P), "an epoch-0 advance never lowers the epoch");
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void epochRegression_isTheRegressionTypeUnwrapped(CheckpointTable table) {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 0L, 7L);

    var e =
        assertThrows(
            ProjectionEpochRegressionException.class, () -> fence.stampEpoch(dataSource, P, 3L));
    assertSame(ProjectionEpochRegressionException.class, e.getClass());
    assertNull(e.getCause());
    assertEquals(0, e.getSuppressed().length);
    assertEquals(P, e.projectionName());
    assertEquals(3L, e.acquiredEpoch());
    assertEquals(7L, e.storedEpoch());
    assertEquals(7L, fence.readEpoch(dataSource, P), "a losing stamp writes nothing");
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void stampEpoch_transcript(CheckpointTable table) {
    var fence = new PostgresCheckpointFence(table);
    store(fence, 0L, 7L);
    var recorder = new RecordingDataSource(dataSource);

    assertThrows(ProjectionEpochRegressionException.class, () -> fence.stampEpoch(recorder, P, 3L));

    String t = table.tableName();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare INSERT INTO "
                + t
                + " (projection_name, last_offset, epoch, updated_at) VALUES (?, 0, ?, NOW()) ON"
                + " CONFLICT (projection_name) DO UPDATE SET epoch = GREATEST("
                + t
                + ".epoch, EXCLUDED.epoch), updated_at = NOW() WHERE EXCLUDED.epoch > "
                + t
                + ".epoch",
            "#1 prepare SELECT epoch FROM " + t + " WHERE projection_name = ?",
            "#1 close");
    assertEquals(0, recorder.openConnections());
  }

  @Test
  void failureMessages() {
    var refused = new SQLException("connection refused");
    DataSource down = refusingDataSource(refused);
    var fence = new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);

    assertWrapped(
        refused, "Failed to read fencing epoch for projection: p", () -> fence.readEpoch(down, P));
    assertWrapped(
        refused, "Failed to read offset for projection: p", () -> fence.lastOffset(down, P));
    assertWrapped(refused, "Failed to reset offset for projection: p", () -> fence.reset(down, P));
    assertWrapped(
        refused,
        "Failed to save offset for projection: p",
        () -> fence.saveUnlocked(down, P, GlobalOffset.of(1)));
    assertWrapped(
        refused,
        "Failed to stamp fencing epoch 3 for projection: p",
        () -> fence.stampEpoch(down, P, 3L));
  }

  private static void assertWrapped(SQLException cause, String message, Executable call) {
    var e = assertThrows(EventStoreException.class, call);
    assertSame(EventStoreException.class, e.getClass());
    assertEquals(message, e.getMessage());
    assertSame(cause, e.getCause());
    assertEquals(0, e.getSuppressed().length);
  }

  /** A data source whose every {@code getConnection} throws {@code failure}. */
  private static DataSource refusingDataSource(SQLException failure) {
    return (DataSource)
        Proxy.newProxyInstance(
            PostgresCheckpointFenceCharacterizationTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw failure;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}

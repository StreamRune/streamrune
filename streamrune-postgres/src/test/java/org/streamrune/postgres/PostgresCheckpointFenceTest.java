package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The checkpoint-row fence over each table it can touch: the epoch fence and the overlap guard
 * under the row lock, the monotonic offset and epoch writes, the unguarded reset, and that the two
 * tables never see each other's rows. The database carries both shipped series, the way an
 * event-store database that also holds its read models does.
 */
@Testcontainers
class PostgresCheckpointFenceTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_fence_test");

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
    // A second series in a non-empty schema: baseline 0, as the factory applies its crypto series
    // (tolerates the event-store tables, skips no read-model migration).
    ShippedMigrationSeries.READ_MODEL
        .configure(dataSource)
        .table("flyway_schema_history_readmodel")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate();
  }

  private static ProjectionName freshName() {
    return ProjectionName.of("fence_" + UUID.randomUUID().toString().replace("-", ""));
  }

  /** Opens a transaction the way the batch path does: autocommit off. */
  private static Connection begin() throws SQLException {
    Connection conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    return conn;
  }

  /** Whether another connection finds the checkpoint row locked (NOWAIT fails with 55P03). */
  private static boolean rowLocked(CheckpointTable table, ProjectionName name) throws SQLException {
    try (Connection other = begin();
        var ps =
            other.prepareStatement(
                "SELECT 1 FROM "
                    + table.tableName()
                    + " WHERE projection_name = ? FOR UPDATE NOWAIT")) {
      ps.setString(1, name.value());
      try {
        ps.executeQuery().close();
        return false;
      } catch (SQLException e) {
        if ("55P03".equals(e.getSQLState())) {
          return true;
        }
        throw e;
      } finally {
        other.rollback();
      }
    }
  }

  private static boolean rowExists(CheckpointTable table, ProjectionName name) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT 1 FROM " + table.tableName() + " WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpochIsRejectedUnderLock(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 5L);

    try (Connection conn = begin()) {
      var fenced =
          assertThrows(
              ProjectionCommitFencedException.class,
              () -> fence.lockForBatch(conn, name, 3L, GlobalOffset.of(1)));
      assertSame(ProjectionCommitFencedException.Guard.EPOCH_FENCE, fenced.guard());
      assertTrue(
          fenced.getMessage().contains("caller epoch 3 is below the stored epoch 5"),
          fenced.getMessage());
      // The check ran under the row lock, which the transaction still holds.
      assertTrue(rowLocked(table, name), "the stale-epoch check must run under the row lock");
      conn.rollback();
    }

    // The current epoch, a newer one and the unfenced epoch 0 all pass, and hold the lock.
    for (long epoch : new long[] {5L, 6L, 0L}) {
      try (Connection conn = begin()) {
        assertEquals(0L, fence.lockForBatch(conn, name, epoch, GlobalOffset.of(1)));
        assertTrue(rowLocked(table, name));
        conn.rollback();
      }
    }
    assertEquals(5L, fence.readEpoch(dataSource, name), "a lock never stamps the epoch");
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void overlapIsRejected(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));

    for (long first : new long[] {10L, 4L}) {
      try (Connection conn = begin()) {
        var overlap =
            assertThrows(
                ProjectionCommitFencedException.class,
                () -> fence.lockForBatch(conn, name, 0L, GlobalOffset.of(first)));
        assertSame(ProjectionCommitFencedException.Guard.OVERLAP, overlap.guard());
        assertTrue(
            overlap
                .getMessage()
                .contains(
                    "batch starting at offset "
                        + first
                        + " overlaps the committed checkpoint (10)"),
            overlap.getMessage());
        conn.rollback();
      }
    }

    // A batch starting right after the checkpoint passes; so does an empty batch (no first offset).
    try (Connection conn = begin()) {
      assertEquals(10L, fence.lockForBatch(conn, name, 0L, GlobalOffset.of(11)));
      conn.rollback();
    }
    try (Connection conn = begin()) {
      assertEquals(10L, fence.lockForBatch(conn, name, 0L, null));
      conn.rollback();
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void saveIsMonotonic(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();

    try (Connection conn = begin()) {
      fence.lockForBatch(conn, name, 7L, GlobalOffset.of(1));
      fence.saveLocked(conn, name, GlobalOffset.of(10), 7L);
      conn.commit();
    }
    assertEquals(GlobalOffset.of(10), fence.lastOffset(dataSource, name));
    assertEquals(7L, fence.readEpoch(dataSource, name));

    // A locked save at or below the stored offset is rejected, and its transaction rolls back.
    for (long lower : new long[] {10L, 5L}) {
      try (Connection conn = begin()) {
        fence.lockForBatch(conn, name, 7L, null);
        var monotonic =
            assertThrows(
                ProjectionCommitFencedException.class,
                () -> fence.saveLocked(conn, name, GlobalOffset.of(lower), 7L));
        assertSame(ProjectionCommitFencedException.Guard.MONOTONIC, monotonic.guard());
        assertTrue(
            monotonic.getMessage().contains("offset advance to " + lower + " was rejected"),
            monotonic.getMessage());
        conn.rollback();
      }
    }

    // An unlocked save below the stored offset is a silent no-op.
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(3));
    assertEquals(GlobalOffset.of(10), fence.lastOffset(dataSource, name));

    // An advance stamps GREATEST(stored, caller): a lower caller epoch never lowers it.
    try (Connection conn = begin()) {
      fence.lockForBatch(conn, name, 0L, GlobalOffset.of(11));
      fence.saveLocked(conn, name, GlobalOffset.of(12), 2L);
      conn.commit();
    }
    assertEquals(GlobalOffset.of(12), fence.lastOffset(dataSource, name));
    assertEquals(7L, fence.readEpoch(dataSource, name));

    // An unlocked save advances, and seeds a row for a projection never seen.
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(20));
    assertEquals(GlobalOffset.of(20), fence.lastOffset(dataSource, name));
    var unseen = freshName();
    assertEquals(GlobalOffset.initial(), fence.lastOffset(dataSource, unseen));
    fence.saveUnlocked(dataSource, unseen, GlobalOffset.of(4));
    assertEquals(GlobalOffset.of(4), fence.lastOffset(dataSource, unseen));
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void stampIsMonotonic(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();

    // Epoch 0 is a no-op: no row appears.
    fence.stampEpoch(dataSource, name, 0L);
    assertEquals(0L, fence.readEpoch(dataSource, name));
    assertEquals(GlobalOffset.initial(), fence.lastOffset(dataSource, name));

    // The first stamp seeds the row at offset 0 and never moves the offset afterwards.
    fence.stampEpoch(dataSource, name, 5L);
    assertEquals(5L, fence.readEpoch(dataSource, name));
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(9));
    fence.stampEpoch(dataSource, name, 6L);
    assertEquals(6L, fence.readEpoch(dataSource, name));
    assertEquals(GlobalOffset.of(9), fence.lastOffset(dataSource, name));

    // A re-stamp of the stored epoch returns quietly; a lower one leaves it and says so.
    fence.stampEpoch(dataSource, name, 6L);
    var regression =
        assertThrows(
            ProjectionEpochRegressionException.class, () -> fence.stampEpoch(dataSource, name, 4L));
    assertTrue(regression.getMessage().contains("6"), regression.getMessage());
    assertEquals(6L, fence.readEpoch(dataSource, name));
    assertEquals(GlobalOffset.of(9), fence.lastOffset(dataSource, name));
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void resetRewindsUnguarded(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 3L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));

    fence.reset(dataSource, name);

    assertEquals(GlobalOffset.initial(), fence.lastOffset(dataSource, name));
    assertEquals(3L, fence.readEpoch(dataSource, name), "a reset leaves the epoch alone");

    // A reset of a projection never seen seeds it at 0.
    var unseen = freshName();
    fence.reset(dataSource, unseen);
    try (Connection conn = begin()) {
      assertEquals(0L, fence.lockForBatch(conn, unseen, 0L, GlobalOffset.of(1)));
      conn.rollback();
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void replayLocksTheRowAndWritesNothing(CheckpointTable table) throws Exception {
    var fence = new PostgresCheckpointFence(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 4L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(8));

    try (Connection conn = begin()) {
      fence.lockForReplay(conn, name);
      assertTrue(rowLocked(table, name), "a replay holds the lock a batch takes");
      conn.commit();
    }
    assertEquals(GlobalOffset.of(8), fence.lastOffset(dataSource, name));
    assertEquals(4L, fence.readEpoch(dataSource, name));

    // A replay of a projection that never committed seeds the 0 row it locks (uncommitted, so
    // invisible to another connection until the replay commits).
    var unseen = freshName();
    try (Connection conn = begin()) {
      fence.lockForReplay(conn, unseen);
      conn.commit();
    }
    assertTrue(rowExists(table, unseen), "the replay's seed row is committed with it");
    assertEquals(GlobalOffset.initial(), fence.lastOffset(dataSource, unseen));
    assertEquals(0L, fence.readEpoch(dataSource, unseen));
  }

  @Test
  void tablesAreIsolated() throws Exception {
    var subscription = new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);
    var readModel = new PostgresCheckpointFence(CheckpointTable.READ_MODEL);
    var name = freshName();

    subscription.stampEpoch(dataSource, name, 9L);
    subscription.saveUnlocked(dataSource, name, GlobalOffset.of(30));

    assertEquals(GlobalOffset.initial(), readModel.lastOffset(dataSource, name));
    assertEquals(0L, readModel.readEpoch(dataSource, name));
    try (Connection conn = begin()) {
      // Not fenced by the other table's epoch, not overlapped by its offset.
      assertEquals(0L, readModel.lockForBatch(conn, name, 1L, GlobalOffset.of(1)));
      readModel.saveLocked(conn, name, GlobalOffset.of(2), 1L);
      conn.commit();
    }
    assertEquals(GlobalOffset.of(2), readModel.lastOffset(dataSource, name));
    assertEquals(1L, readModel.readEpoch(dataSource, name));
    readModel.reset(dataSource, name);

    assertEquals(GlobalOffset.of(30), subscription.lastOffset(dataSource, name));
    assertEquals(9L, subscription.readEpoch(dataSource, name));
  }

  /**
   * The fence over {@code subscription_checkpoint} runs exactly the statements {@code
   * JdbcProjectionRepository} and {@code PostgresOffsetStore} ran before the extraction, character
   * for character; over {@code readmodel_checkpoint} only the table name differs.
   */
  @Test
  void statementsAreThePreExtractionOnesWithOnlyTheTableSubstituted() {
    var fence = new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);
    assertEquals(
        "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at) "
            + "VALUES (?, 0, NOW()) ON CONFLICT (projection_name) DO NOTHING",
        fence.seedSql);
    assertEquals(
        "SELECT last_offset, epoch FROM subscription_checkpoint WHERE projection_name = ? FOR UPDATE",
        fence.lockSql);
    assertEquals(
        "INSERT INTO subscription_checkpoint (projection_name, last_offset, epoch, updated_at) "
            + "VALUES (?, ?, ?, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
            + "epoch = GREATEST(subscription_checkpoint.epoch, EXCLUDED.epoch), "
            + "updated_at = NOW() "
            + "WHERE EXCLUDED.last_offset > subscription_checkpoint.last_offset",
        fence.saveLockedSql);
    assertEquals(
        "INSERT INTO subscription_checkpoint (projection_name, last_offset, epoch, updated_at) "
            + "VALUES (?, 0, ?, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET "
            + "epoch = GREATEST(subscription_checkpoint.epoch, EXCLUDED.epoch), updated_at = NOW() "
            + "WHERE EXCLUDED.epoch > subscription_checkpoint.epoch",
        fence.stampSql);
    assertEquals(
        "SELECT epoch FROM subscription_checkpoint WHERE projection_name = ?", fence.epochSql);
    assertEquals(
        "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at) "
            + "VALUES (?, 0, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0, updated_at = NOW()",
        fence.resetSql);
    assertEquals(
        "SELECT last_offset FROM subscription_checkpoint WHERE projection_name = ?",
        fence.lastOffsetSql);
    assertEquals(
        "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at) "
            + "VALUES (?, ?, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
            + "updated_at = NOW() "
            + "WHERE EXCLUDED.last_offset > subscription_checkpoint.last_offset",
        fence.saveUnlockedSql);

    var readModel = new PostgresCheckpointFence(CheckpointTable.READ_MODEL);
    assertEquals(
        fence.seedSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.seedSql);
    assertEquals(
        fence.lockSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.lockSql);
    assertEquals(
        fence.saveLockedSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.saveLockedSql);
    assertEquals(
        fence.stampSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.stampSql);
    assertEquals(
        fence.epochSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.epochSql);
    assertEquals(
        fence.resetSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.resetSql);
    assertEquals(
        fence.lastOffsetSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.lastOffsetSql);
    assertEquals(
        fence.saveUnlockedSql.replace("subscription_checkpoint", "readmodel_checkpoint"),
        readModel.saveUnlockedSql);
  }

  @Test
  void tableNamesAreTheTwoShippedTables() {
    assertEquals("subscription_checkpoint", CheckpointTable.SUBSCRIPTION.tableName());
    assertEquals("readmodel_checkpoint", CheckpointTable.READ_MODEL.tableName());
    assertThrows(IllegalArgumentException.class, () -> new PostgresCheckpointFence(null));
  }

  /** Every connection-level failure of a pooled operation is an EventStoreException naming it. */
  @Test
  void aFailedConnectionIsReportedWithTheOperation() {
    var fence = new PostgresCheckpointFence(CheckpointTable.READ_MODEL);
    var name = ProjectionName.of("fence_down");
    DataSource down = new FaultInjectingDataSource(dataSource).failFirst(Integer.MAX_VALUE);

    assertFailure(
        "Failed to read offset for projection: fence_down", () -> fence.lastOffset(down, name));
    assertFailure(
        "Failed to save offset for projection: fence_down",
        () -> fence.saveUnlocked(down, name, GlobalOffset.of(1)));
    assertFailure(
        "Failed to reset offset for projection: fence_down", () -> fence.reset(down, name));
    assertFailure(
        "Failed to stamp fencing epoch 2 for projection: fence_down",
        () -> fence.stampEpoch(down, name, 2L));
    assertFailure(
        "Failed to read fencing epoch for projection: fence_down",
        () -> fence.readEpoch(down, name));
  }

  private static void assertFailure(String message, org.junit.jupiter.api.function.Executable op) {
    var failure = assertThrows(EventStoreException.class, op);
    assertEquals(message, failure.getMessage());
    assertTrue(
        failure.getCause() instanceof SQLException, () -> String.valueOf(failure.getCause()));
  }
}

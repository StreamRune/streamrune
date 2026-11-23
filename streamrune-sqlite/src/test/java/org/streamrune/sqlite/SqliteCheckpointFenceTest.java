package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The checkpoint-row fence over each table it can touch: the epoch fence and the overlap guard
 * under the database's write lock, the monotonic offset and epoch writes, the unguarded reset, and
 * that the two tables never see each other's rows. The database carries both series, the way an
 * event-store file that also holds its read models does.
 *
 * <p>SQLite has no leadership, so the epoch a real lease hands out never reaches the fence there;
 * the cases stamp epochs directly, which exercises the same statements a lease would.
 */
class SqliteCheckpointFenceTest {

  private static final Instant NOW = Instant.parse("2026-11-23T10:00:00.123456Z");

  @TempDir Path dir;

  private SqliteDatabase database;
  private DataSource dataSource;
  private SqliteCheckpointFence fence;
  private final List<SqliteDatabase> extraHandles = new ArrayList<>();
  private final List<SingleConnection> singles = new ArrayList<>();

  @BeforeEach
  void applyBothSeries() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("fence.db")));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    dataSource = database.dataSource();
    fence = fenceOver(CheckpointTable.READ_MODEL);
  }

  /** A fence over {@code table} on the fixed clock. */
  private static SqliteCheckpointFence fenceOver(CheckpointTable table) {
    return new SqliteCheckpointFence(table, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @AfterEach
  void close() throws SQLException {
    extraHandles.forEach(SqliteDatabase::close);
    for (SingleConnection single : singles) {
      single.close();
    }
    database.close();
  }

  private static ProjectionName freshName() {
    return ProjectionName.of("fence_" + UUID.randomUUID().toString().replace("-", ""));
  }

  /** Opens a write transaction the way the batch path does: BEGIN IMMEDIATE on a borrowed one. */
  private Connection begin() throws SQLException {
    Connection conn = dataSource.getConnection();
    SqliteTransactions.begin(conn);
    return conn;
  }

  private static void rollback(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("ROLLBACK");
    }
  }

  private static void commit(Connection conn) throws SQLException {
    SqliteTransactions.commit(conn);
  }

  /**
   * Whether another connection finds the database's write lock taken: its {@code BEGIN IMMEDIATE}
   * fails with SQLITE_BUSY after a busy_timeout of one millisecond.
   */
  private boolean writeLockHeld() throws SQLException {
    try (Connection other =
            TestDatabases.dataSource(database.file(), "busy_timeout=1").getConnection();
        Statement st = other.createStatement()) {
      try {
        st.execute("BEGIN IMMEDIATE");
        st.execute("ROLLBACK");
        return false;
      } catch (SQLiteException e) {
        if (e.getResultCode() == SQLiteErrorCode.SQLITE_BUSY) {
          return true;
        }
        throw e;
      }
    }
  }

  private boolean rowExists(CheckpointTable table, ProjectionName name) throws SQLException {
    return rowExists(table.tableName(), name);
  }

  private boolean rowExists(String table, ProjectionName name) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT 1 FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private Instant updatedAt(ProjectionName name) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT updated_at FROM readmodel_checkpoint WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).as("the row exists").isTrue();
        return SqliteTimestamps.fromMicros(rs.getLong(1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpochIsRejectedUnderTheWriteLock(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 5L);

    try (Connection conn = begin()) {
      assertThatThrownBy(() -> fence.lockForBatch(conn, name, 3L, GlobalOffset.of(1)))
          .isExactlyInstanceOf(ProjectionCommitFencedException.class)
          .hasMessageContaining("caller epoch 3 is below the stored epoch 5")
          .satisfies(
              e ->
                  assertThat(((ProjectionCommitFencedException) e).guard())
                      .isSameAs(ProjectionCommitFencedException.Guard.EPOCH_FENCE));
      // The check ran under the write lock, which the transaction still holds.
      assertThat(writeLockHeld()).as("the stale-epoch check runs under the write lock").isTrue();
      rollback(conn);
    }

    // The current epoch, a newer one and the unfenced epoch 0 all pass, and hold the lock.
    for (long epoch : new long[] {5L, 6L, 0L}) {
      try (Connection conn = begin()) {
        assertThat(fence.lockForBatch(conn, name, epoch, GlobalOffset.of(1))).isZero();
        assertThat(writeLockHeld()).isTrue();
        rollback(conn);
      }
    }
    assertThat(fence.readEpoch(dataSource, name)).as("a lock never stamps the epoch").isEqualTo(5L);
  }

  /**
   * A batch that is both stale and overlapping is the epoch fence's: the runners treat that guard
   * as lost leadership, so the epoch check runs first and names it.
   */
  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void epochFenceWinsOverOverlap(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 5L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));

    try (Connection conn = begin()) {
      assertGuard(
          ProjectionCommitFencedException.Guard.EPOCH_FENCE,
          "caller epoch 3 is below the stored epoch 5",
          () -> fence.lockForBatch(conn, name, 3L, GlobalOffset.of(4)));
      rollback(conn);
    }

    // The same overlapping batch at the current epoch is the overlap guard's.
    try (Connection conn = begin()) {
      assertGuard(
          ProjectionCommitFencedException.Guard.OVERLAP,
          "overlaps the committed checkpoint (10)",
          () -> fence.lockForBatch(conn, name, 5L, GlobalOffset.of(4)));
      rollback(conn);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void overlapIsRejected(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));

    for (long first : new long[] {10L, 4L}) {
      try (Connection conn = begin()) {
        assertGuard(
            ProjectionCommitFencedException.Guard.OVERLAP,
            "batch starting at offset " + first + " overlaps the committed checkpoint (10)",
            () -> fence.lockForBatch(conn, name, 0L, GlobalOffset.of(first)));
        rollback(conn);
      }
    }

    // A batch starting right after the checkpoint passes; so does an empty batch (no first offset).
    try (Connection conn = begin()) {
      assertThat(fence.lockForBatch(conn, name, 0L, GlobalOffset.of(11))).isEqualTo(10L);
      rollback(conn);
    }
    try (Connection conn = begin()) {
      assertThat(fence.lockForBatch(conn, name, 0L, null)).isEqualTo(10L);
      rollback(conn);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void saveIsMonotonic(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();

    try (Connection conn = begin()) {
      fence.lockForBatch(conn, name, 7L, GlobalOffset.of(1));
      fence.saveLocked(conn, name, GlobalOffset.of(10), 7L);
      commit(conn);
    }
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(10));
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(7L);

    // A locked save at or below the stored offset is rejected, and its transaction rolls back.
    for (long lower : new long[] {10L, 5L}) {
      try (Connection conn = begin()) {
        fence.lockForBatch(conn, name, 7L, null);
        assertGuard(
            ProjectionCommitFencedException.Guard.MONOTONIC,
            "offset advance to " + lower + " was rejected",
            () -> fence.saveLocked(conn, name, GlobalOffset.of(lower), 7L));
        rollback(conn);
      }
    }

    // An unlocked save below the stored offset is a silent no-op.
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(3));
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(10));

    // An advance stamps the greater of the stored and the caller epoch: a lower caller epoch never
    // lowers it.
    try (Connection conn = begin()) {
      fence.lockForBatch(conn, name, 0L, GlobalOffset.of(11));
      fence.saveLocked(conn, name, GlobalOffset.of(12), 2L);
      commit(conn);
    }
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(12));
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(7L);

    // An unlocked save advances, and seeds a row for a projection never seen.
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(20));
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(20));
    var unseen = freshName();
    assertThat(rowExists(table, unseen)).isFalse();
    assertThat(fence.lastOffset(dataSource, unseen)).isEqualTo(GlobalOffset.initial());
    fence.saveUnlocked(dataSource, unseen, GlobalOffset.of(4));
    assertThat(rowExists(table, unseen)).isTrue();
    assertThat(fence.lastOffset(dataSource, unseen)).isEqualTo(GlobalOffset.of(4));
  }

  /** A greater caller epoch on an advance is stamped: the stored epoch rises with the leader. */
  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void anAdvanceStampsAGreaterCallerEpoch(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 3L);

    try (Connection conn = begin()) {
      fence.lockForBatch(conn, name, 4L, GlobalOffset.of(1));
      fence.saveLocked(conn, name, GlobalOffset.of(2), 4L);
      commit(conn);
    }

    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(4L);
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(2));
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void stampIsMonotonic(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();

    // Epoch 0 is a no-op: no row appears.
    fence.stampEpoch(dataSource, name, 0L);
    assertThat(rowExists(table, name)).as("an epoch-0 stamp writes no row").isFalse();

    // The first stamp seeds the row at offset 0 and never moves the offset afterwards.
    fence.stampEpoch(dataSource, name, 5L);
    assertThat(rowExists(table, name)).as("the first stamp seeds the row").isTrue();
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.initial());
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(5L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(9));
    fence.stampEpoch(dataSource, name, 6L);
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(6L);
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(9));

    // A re-stamp of the stored epoch returns quietly; a lower one leaves it and says so.
    fence.stampEpoch(dataSource, name, 6L);
    assertThatThrownBy(() -> fence.stampEpoch(dataSource, name, 4L))
        .isExactlyInstanceOf(ProjectionEpochRegressionException.class)
        .hasMessageContaining(
            "was handed leadership epoch 4 but its checkpoint row already stores epoch 6");
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(6L);
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(9));
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void resetRewindsUnguarded(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 3L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));

    fence.reset(dataSource, name);

    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.initial());
    assertThat(fence.readEpoch(dataSource, name))
        .as("a reset leaves the epoch alone")
        .isEqualTo(3L);

    // A reset of a projection never seen seeds it at 0.
    var unseen = freshName();
    assertThat(rowExists(table, unseen)).isFalse();
    fence.reset(dataSource, unseen);
    assertThat(rowExists(table, unseen)).as("the reset seeds the row").isTrue();
    try (Connection conn = begin()) {
      assertThat(fence.lockForBatch(conn, unseen, 0L, GlobalOffset.of(1))).isZero();
      rollback(conn);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void replayHoldsTheWriteLockAndWritesNothing(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    var name = freshName();
    fence.stampEpoch(dataSource, name, 4L);
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(8));

    try (Connection conn = begin()) {
      fence.lockForReplay(conn, name);
      assertThat(writeLockHeld()).as("a replay holds the lock a batch takes").isTrue();
      commit(conn);
    }
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(8));
    assertThat(fence.readEpoch(dataSource, name)).isEqualTo(4L);

    // A replay of a projection that never committed seeds the 0 row (uncommitted, so invisible to
    // another connection until the replay commits).
    var unseen = freshName();
    assertThat(rowExists(table, unseen)).isFalse();
    try (Connection conn = begin()) {
      fence.lockForReplay(conn, unseen);
      assertThat(rowExists(table, unseen))
          .as("the seed is not visible before the replay commits")
          .isFalse();
      commit(conn);
    }
    assertThat(rowExists(table, unseen)).as("the replay's seed row is committed with it").isTrue();
    assertThat(fence.lastOffset(dataSource, unseen)).isEqualTo(GlobalOffset.initial());
    assertThat(fence.readEpoch(dataSource, unseen)).isZero();
  }

  /**
   * The two fences never see each other's rows, though one file holds both tables; the event
   * store's {@link SqliteOffsetStore} reads the subscription fence's row.
   */
  @Test
  void tablesAreIsolated() throws Exception {
    var subscription = fenceOver(CheckpointTable.SUBSCRIPTION);
    var readModel = fenceOver(CheckpointTable.READ_MODEL);
    var name = freshName();

    subscription.stampEpoch(dataSource, name, 9L);
    subscription.saveUnlocked(dataSource, name, GlobalOffset.of(30));

    assertThat(rowExists(CheckpointTable.SUBSCRIPTION, name)).isTrue();
    assertThat(rowExists(CheckpointTable.READ_MODEL, name))
        .as("the other table has no row")
        .isFalse();
    assertThat(readModel.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.initial());
    assertThat(readModel.readEpoch(dataSource, name)).isZero();
    try (Connection conn = begin()) {
      // Not fenced by the other table's epoch, not overlapped by its offset.
      assertThat(readModel.lockForBatch(conn, name, 1L, GlobalOffset.of(1))).isZero();
      readModel.saveLocked(conn, name, GlobalOffset.of(2), 1L);
      commit(conn);
    }
    assertThat(readModel.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(2));
    assertThat(readModel.readEpoch(dataSource, name)).isEqualTo(1L);
    readModel.reset(dataSource, name);

    assertThat(subscription.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(30));
    assertThat(subscription.readEpoch(dataSource, name)).isEqualTo(9L);
    assertThat(new SqliteOffsetStore(database).getLastOffset(name)).isEqualTo(GlobalOffset.of(30));
  }

  /**
   * The statements are the Postgres fence's, translated: the application clock's value bound for
   * {@code NOW()}, {@code max()} for {@code GREATEST()}, and no {@code FOR UPDATE} (the caller's
   * transaction holds the database's write lock). Over {@code readmodel_checkpoint} only the table
   * name differs.
   */
  @Test
  void statementsAreThePostgresOnesTranslatedWithOnlyTheTableSubstituted() {
    var fence = fenceOver(CheckpointTable.SUBSCRIPTION);
    assertThat(fence.seedSql)
        .isEqualTo(
            "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at)"
                + " VALUES (?, 0, ?) ON CONFLICT (projection_name) DO NOTHING");
    assertThat(fence.readForBatchSql)
        .isEqualTo(
            "SELECT last_offset, epoch FROM subscription_checkpoint WHERE projection_name = ?");
    assertThat(fence.saveLockedSql)
        .isEqualTo(
            "INSERT INTO subscription_checkpoint (projection_name, last_offset, epoch, updated_at)"
                + " VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = excluded.last_offset,"
                + " epoch = max(subscription_checkpoint.epoch, excluded.epoch),"
                + " updated_at = excluded.updated_at"
                + " WHERE excluded.last_offset > subscription_checkpoint.last_offset");
    assertThat(fence.stampSql)
        .isEqualTo(
            "INSERT INTO subscription_checkpoint (projection_name, last_offset, epoch, updated_at)"
                + " VALUES (?, 0, ?, ?)"
                + " ON CONFLICT (projection_name) DO UPDATE SET"
                + " epoch = max(subscription_checkpoint.epoch, excluded.epoch),"
                + " updated_at = excluded.updated_at"
                + " WHERE excluded.epoch > subscription_checkpoint.epoch");
    assertThat(fence.epochSql)
        .isEqualTo("SELECT epoch FROM subscription_checkpoint WHERE projection_name = ?");
    assertThat(fence.resetSql)
        .isEqualTo(
            "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at)"
                + " VALUES (?, 0, ?)"
                + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0,"
                + " updated_at = excluded.updated_at");
    assertThat(fence.lastOffsetSql)
        .isEqualTo("SELECT last_offset FROM subscription_checkpoint WHERE projection_name = ?");
    assertThat(fence.saveUnlockedSql)
        .isEqualTo(
            "INSERT INTO subscription_checkpoint (projection_name, last_offset, updated_at)"
                + " VALUES (?, ?, ?)"
                + " ON CONFLICT (projection_name) DO UPDATE SET last_offset = excluded.last_offset,"
                + " updated_at = excluded.updated_at"
                + " WHERE excluded.last_offset > subscription_checkpoint.last_offset");

    var readModel = fenceOver(CheckpointTable.READ_MODEL);
    List<String> subscriptionSql =
        List.of(
            fence.seedSql,
            fence.readForBatchSql,
            fence.saveLockedSql,
            fence.stampSql,
            fence.epochSql,
            fence.resetSql,
            fence.lastOffsetSql,
            fence.saveUnlockedSql);
    List<String> readModelSql =
        List.of(
            readModel.seedSql,
            readModel.readForBatchSql,
            readModel.saveLockedSql,
            readModel.stampSql,
            readModel.epochSql,
            readModel.resetSql,
            readModel.lastOffsetSql,
            readModel.saveUnlockedSql);
    assertThat(readModelSql)
        .isEqualTo(
            subscriptionSql.stream()
                .map(sql -> sql.replace("subscription_checkpoint", "readmodel_checkpoint"))
                .toList());
  }

  @Test
  void tableNamesAreTheTwoShippedTablesAndNullsAreRefused() {
    assertThat(CheckpointTable.SUBSCRIPTION.tableName()).isEqualTo("subscription_checkpoint");
    assertThat(CheckpointTable.READ_MODEL.tableName()).isEqualTo("readmodel_checkpoint");
    assertThat(fenceOver(CheckpointTable.READ_MODEL).table()).isSameAs(CheckpointTable.READ_MODEL);
    assertThatThrownBy(() -> new SqliteCheckpointFence(null, Clock.systemUTC()))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("table is required");
    assertThatThrownBy(() -> new SqliteCheckpointFence(CheckpointTable.READ_MODEL, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("clock is required");
  }

  /** Every connection-level failure of a pooled operation is an EventStoreException naming it. */
  @Test
  void aFailedConnectionIsReportedWithTheOperation() {
    var name = ProjectionName.of("fence_down");
    DataSource down = TestDatabases.url("jdbc:sqlite:" + dir.resolve("missing/dir/x.db"));

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

  /**
   * Every standalone write runs in its own {@code BEGIN IMMEDIATE} ... {@code COMMIT}: on a
   * connection where the application left a {@code BEGIN} open, a bare statement would join that
   * transaction and not be durable. The write is refused instead, writes nothing, and neither
   * commits nor rolls back the application's transaction.
   */
  @Test
  void aStandaloneWriteOnAConnectionWithAnOpenTransactionIsRefused() throws Exception {
    var name = freshName();
    SingleConnection single = new SingleConnection(TestDatabases.dataSource(database.file()));
    singles.add(single);
    try (Connection app = single.getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      st.execute("CREATE TABLE app_marker (x INTEGER) STRICT");

      assertThatThrownBy(() -> fence.saveUnlocked(single, name, GlobalOffset.of(5)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save offset for projection: " + name.value());
      assertThatThrownBy(() -> fence.reset(single, name))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to reset offset for projection: " + name.value());
      assertThatThrownBy(() -> fence.stampEpoch(single, name, 2L))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to stamp fencing epoch 2 for projection: " + name.value());

      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM app_marker")) {
        assertThat(rs.next()).as("the application's transaction is still open").isTrue();
      }
      st.execute("ROLLBACK");
    }
    assertThat(rowExists("readmodel_checkpoint", name)).isFalse();
  }

  /**
   * A pool that hands out manual-commit connections is a configuration error, refused unwrapped.
   */
  @Test
  void aStandaloneWriteOnAManualCommitConnectionIsRefusedAsAConfigurationError() throws Exception {
    var name = freshName();
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    source.manualCommit();

    for (Executable write :
        List.<Executable>of(
            () -> fence.saveUnlocked(source, name, GlobalOffset.of(5)),
            () -> fence.reset(source, name),
            () -> fence.stampEpoch(source, name, 2L))) {
      assertThatThrownBy(write::execute)
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    }
    assertThat(rowExists("readmodel_checkpoint", name)).isFalse();
  }

  @Test
  void aStandaloneWriteWhileAnotherConnectionHoldsTheLockFailsAsBusyAndWritesNothing()
      throws Exception {
    var name = freshName();
    DataSource impatient = TestDatabases.dataSource(database.file(), "busy_timeout=100");
    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");

      assertThatThrownBy(() -> fence.stampEpoch(impatient, name, 2L))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("fencing stamp of projection " + name.value())
          .hasMessageContaining("busy_timeout");
      assertThatThrownBy(() -> fence.saveUnlocked(impatient, name, GlobalOffset.of(3)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("offset save of projection " + name.value())
          .hasMessageContaining("busy_timeout");
      assertThatThrownBy(() -> fence.reset(impatient, name))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("offset reset of projection " + name.value())
          .hasMessageContaining("busy_timeout");

      st.execute("ROLLBACK");
    }
    assertThat(rowExists("readmodel_checkpoint", name)).isFalse();
  }

  /** Null arguments are caller errors, refused before any I/O and never wrapped. */
  @Test
  void nullArgumentsAreRefusedBeforeAnyIo() {
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    source.failWith(() -> new IllegalStateException("no I/O expected"));
    var name = freshName();

    for (Executable call :
        List.<Executable>of(
            () -> fence.lastOffset(source, null),
            () -> fence.readEpoch(source, null),
            () -> fence.reset(source, null),
            () -> fence.saveUnlocked(source, null, GlobalOffset.of(1)),
            () -> fence.stampEpoch(source, null, 1L))) {
      assertThatThrownBy(call::execute)
          .isExactlyInstanceOf(NullPointerException.class)
          .hasMessage("projectionName is required");
    }
    assertThatThrownBy(() -> fence.saveUnlocked(source, name, null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("offset is required");
  }

  /**
   * Every write stamps {@code updated_at} with the application clock, in epoch microseconds; a
   * write the guard refuses stamps nothing.
   */
  @Test
  void updatedAtIsStampedWithTheClockAndOnlyByAWriteThatHappens() throws Exception {
    var name = freshName();
    Instant later = NOW.plusSeconds(60);
    var laterFence =
        new SqliteCheckpointFence(CheckpointTable.READ_MODEL, Clock.fixed(later, ZoneOffset.UTC));

    fence.stampEpoch(dataSource, name, 2L);
    assertThat(updatedAt(name)).isEqualTo(NOW);
    laterFence.stampEpoch(dataSource, name, 2L);
    assertThat(updatedAt(name)).as("a same-epoch re-stamp stamps nothing").isEqualTo(NOW);

    fence.saveUnlocked(dataSource, name, GlobalOffset.of(5));
    laterFence.saveUnlocked(dataSource, name, GlobalOffset.of(5));
    assertThat(updatedAt(name)).as("a refused save stamps nothing").isEqualTo(NOW);

    try (Connection conn = begin()) {
      laterFence.lockForBatch(conn, name, 2L, GlobalOffset.of(6));
      laterFence.saveLocked(conn, name, GlobalOffset.of(7), 2L);
      commit(conn);
    }
    assertThat(updatedAt(name)).as("an advance stamps").isEqualTo(later);

    Instant latest = later.plusSeconds(60);
    new SqliteCheckpointFence(CheckpointTable.READ_MODEL, Clock.fixed(latest, ZoneOffset.UTC))
        .reset(dataSource, name);
    assertThat(updatedAt(name)).as("a reset stamps").isEqualTo(latest);
  }

  /**
   * Two writers of one projection that both read the same checkpoint before either commits — a
   * stale leader and a newer one — serialize on the write lock: the second reads what the first
   * committed and is fenced out by the overlap guard, so the range is applied once.
   */
  @Test
  void concurrentBatchesOfOneRangeApplyItOnce() throws Exception {
    var name = freshName();
    fence.saveUnlocked(dataSource, name, GlobalOffset.of(10));
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE app_view (offset_applied INTEGER NOT NULL) STRICT");
    }
    int writers = 4;
    CyclicBarrier start = new CyclicBarrier(writers);
    List<Future<String>> outcomes = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(writers);
    try {
      for (int i = 0; i < writers; i++) {
        outcomes.add(
            executor.submit(
                () -> {
                  start.await(30, TimeUnit.SECONDS);
                  try (Connection conn = dataSource.getConnection()) {
                    SqliteTransactions.inTransaction(
                        conn,
                        "test batch",
                        c -> {
                          fence.lockForBatch(c, name, 0L, GlobalOffset.of(11));
                          try (PreparedStatement ps =
                              c.prepareStatement(
                                  "INSERT INTO app_view (offset_applied) VALUES (11)")) {
                            ps.executeUpdate();
                          }
                          fence.saveLocked(c, name, GlobalOffset.of(11), 0L);
                          return null;
                        });
                    return "committed";
                  } catch (ProjectionCommitFencedException e) {
                    return e.guard().name();
                  }
                }));
      }
      List<String> results = new ArrayList<>();
      for (Future<String> outcome : outcomes) {
        results.add(outcome.get(60, TimeUnit.SECONDS));
      }
      assertThat(results).containsExactlyInAnyOrder("committed", "OVERLAP", "OVERLAP", "OVERLAP");
    } finally {
      executor.shutdownNow();
    }
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM app_view")) {
      rs.next();
      assertThat(rs.getLong(1)).as("the range is applied once").isEqualTo(1);
    }
    assertThat(fence.lastOffset(dataSource, name)).isEqualTo(GlobalOffset.of(11));
  }

  private static void assertGuard(
      ProjectionCommitFencedException.Guard guard, String messagePart, Executable call) {
    assertThatThrownBy(call::execute)
        .isExactlyInstanceOf(ProjectionCommitFencedException.class)
        .hasMessageContaining(messagePart)
        .satisfies(e -> assertThat(((ProjectionCommitFencedException) e).guard()).isSameAs(guard));
  }

  private static void assertFailure(String message, Executable op) {
    assertThatThrownBy(op::execute)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(message)
        .hasCauseInstanceOf(SQLException.class);
  }
}

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
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;

/**
 * The crash points of the checkpoint fence's durable writes, each killed there the way the death of
 * the process would kill it (SQLite rolls back the open transaction of a connection that closes),
 * then run again:
 *
 * <ul>
 *   <li>a fenced batch — a read-model write and the checkpoint advance in one {@code BEGIN
 *       IMMEDIATE} transaction — killed inside the advance, before the {@code COMMIT}: neither the
 *       read-model write nor the advance remains, and the rerun applies the batch once;
 *   <li>the same batch killed right after its {@code COMMIT}: both are durable, and the rerun of
 *       the same range is fenced out by the overlap guard, so nothing is applied twice;
 *   <li>a reset killed inside its statement or right after its {@code COMMIT}: the offset is the
 *       old one or {@code 0}, and the rerun leaves {@code 0};
 *   <li>a fencing stamp killed inside its statement: the stored epoch is unchanged, and the rerun
 *       stamps it.
 * </ul>
 */
class SqliteCheckpointFenceCrashTest {

  private static final ProjectionName P = ProjectionName.of("crash-projection");
  private static final Point CHECKPOINT_ROW_1 = new Point("readmodel_checkpoint", 1);
  private static final Point CHECKPOINT_ROW_2 = new Point("readmodel_checkpoint", 2);

  @TempDir Path dir;

  private SqliteDatabase database;
  private DataSource clean;
  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private DataSource crashing;
  private final SqliteCheckpointFence fence = new SqliteCheckpointFence(Clock.systemUTC());

  @BeforeEach
  void open() throws SQLException {
    Path file = dir.resolve("crash.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    clean = TestDatabases.dataSource(file);
    // A new physical connection on every borrow, so a killed one is never handed out again.
    crashing = crashPoints.connections(TestDatabases.dataSource(file));
    try (Connection conn = clean.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE app_view (offset_applied INTEGER NOT NULL) STRICT");
    }
  }

  @AfterEach
  void close() {
    database.close();
  }

  /**
   * One fenced batch of the events at {@code first..last}, as the projection repository runs it.
   */
  private void batch(DataSource source, long first, long last) throws SQLException {
    try (Connection conn = source.getConnection()) {
      SqliteTransactions.inTransaction(
          conn,
          "projection batch",
          c -> {
            fence.lockForBatch(c, P, 0L, GlobalOffset.of(first));
            try (PreparedStatement ps =
                c.prepareStatement("INSERT INTO app_view (offset_applied) VALUES (?)")) {
              for (long offset = first; offset <= last; offset++) {
                ps.setLong(1, offset);
                ps.executeUpdate();
              }
            }
            fence.saveLocked(c, P, GlobalOffset.of(last), 0L);
            return null;
          });
    }
  }

  private long applied() throws SQLException {
    try (Connection conn = clean.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM app_view")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private boolean checkpointRowExists() throws SQLException {
    try (Connection conn = clean.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM readmodel_checkpoint")) {
      rs.next();
      return rs.getLong(1) > 0;
    }
  }

  @Test
  void aBatchKilledInsideItsCheckpointAdvanceLeavesNeitherItsWritesNorTheAdvance()
      throws Exception {
    fence.saveUnlocked(clean, P, GlobalOffset.of(10));
    crashPoints.arm(CHECKPOINT_ROW_1);

    assertThatThrownBy(() -> batch(crashing, 11, 12)).isInstanceOf(SQLException.class);

    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(10));
    assertThat(applied()).as("the read-model writes rolled back with the advance").isZero();

    batch(crashing, 11, 12);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(12));
    assertThat(applied()).as("the rerun applies the batch once").isEqualTo(2);
  }

  /** A first-ever batch dies after its seed row and inside its advance: no seed row remains. */
  @Test
  void aFirstBatchKilledInsideItsAdvanceLeavesNoSeedRow() throws Exception {
    crashPoints.arm(CHECKPOINT_ROW_2);

    assertThatThrownBy(() -> batch(crashing, 1, 3)).isInstanceOf(SQLException.class);

    assertThat(checkpointRowExists()).as("the seed rolled back with the batch").isFalse();
    assertThat(applied()).isZero();

    batch(crashing, 1, 3);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(3));
    assertThat(applied()).isEqualTo(3);
  }

  @Test
  void aBatchKilledRightAfterItsCommitIsDurableWholeAndItsRerunIsFencedOut() throws Exception {
    fence.saveUnlocked(clean, P, GlobalOffset.of(10));
    crashPoints.arm(Point.AFTER_COMMIT);

    assertThatThrownBy(() -> batch(crashing, 11, 12)).isInstanceOf(SQLException.class);

    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(12));
    assertThat(applied()).as("the committed batch is durable whole").isEqualTo(2);

    // The caller saw a failure and runs the same range again: the overlap guard refuses it.
    assertThatThrownBy(() -> batch(crashing, 11, 12))
        .isExactlyInstanceOf(ProjectionCommitFencedException.class)
        .satisfies(
            e ->
                assertThat(((ProjectionCommitFencedException) e).guard())
                    .isSameAs(ProjectionCommitFencedException.Guard.OVERLAP));
    assertThat(applied()).as("nothing is applied twice").isEqualTo(2);

    // Reading on from the stored checkpoint resumes.
    batch(crashing, 13, 13);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(13));
    assertThat(applied()).isEqualTo(3);
  }

  @Test
  void aResetKilledInsideItsStatementKeepsTheOffsetAndItsRerunRewinds() throws Exception {
    fence.saveUnlocked(clean, P, GlobalOffset.of(10));
    crashPoints.arm(CHECKPOINT_ROW_1);

    assertThatThrownBy(() -> fence.reset(crashing, P))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to reset offset for projection: crash-projection");
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.of(10));

    fence.reset(crashing, P);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void aResetKilledRightAfterItsCommitIsDurableAndItsRerunChangesNothing() throws Exception {
    fence.saveUnlocked(clean, P, GlobalOffset.of(10));
    crashPoints.arm(Point.AFTER_COMMIT);

    assertThatThrownBy(() -> fence.reset(crashing, P))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.initial());

    fence.reset(crashing, P);
    assertThat(fence.lastOffset(clean, P)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void aStampKilledInsideItsStatementKeepsTheEpochAndItsRerunStamps() throws Exception {
    fence.stampEpoch(clean, P, 2L);
    crashPoints.arm(CHECKPOINT_ROW_1);

    assertThatThrownBy(() -> fence.stampEpoch(crashing, P, 3L))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to stamp fencing epoch 3 for projection: crash-projection");
    assertThat(fence.readEpoch(clean, P)).isEqualTo(2L);

    fence.stampEpoch(crashing, P, 3L);
    assertThat(fence.readEpoch(clean, P)).isEqualTo(3L);
  }
}

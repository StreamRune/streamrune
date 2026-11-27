package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.Level;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
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
import org.streamrune.core.projection.ProjectionCommitFencedException.Guard;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Characterization of the SQLite checkpoint fence: the exact exception type, guard, message, cause
 * and suppressed exceptions of every refusal and failure, where each one surfaces relative to the
 * standalone write ({@link SqliteTransactions#write}), and the statements one fencing stamp runs.
 * The values are what the fence does on a real database file, written down as they come out; these
 * tests stay unchanged while the fence's mechanics move into shared helpers.
 *
 * <p>The connection methods run inside a {@code BEGIN IMMEDIATE} transaction, as the batch path
 * opens it; the data source methods borrow from a {@link RecordingDataSource}.
 */
class SqliteCheckpointFenceCharacterizationTest {

  private static final Instant NOW = Instant.parse("2026-11-27T09:00:00.654321Z");
  private static final ProjectionName P = ProjectionName.of("p");

  private static final String BUSY_ADVICE =
      " stored nothing. Most likely an application transaction on the same database file holds"
          + " the write lock: StreamRune's stores never join an application's transaction (for"
          + " example a Spring @Transactional method on the same DataSource), so call them outside"
          + " it. Otherwise another connection of this process held the lock that long: a schema"
          + " migration or a stalled write. Retry later, or raise busy_timeout.";

  @TempDir Path dir;

  private SqliteDatabase database;
  private RecordingDataSource recorder;

  @BeforeEach
  void openDatabase() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("fence.db")));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    recorder = new RecordingDataSource(database.dataSource());
  }

  @AfterEach
  void closeDatabase() {
    database.close();
  }

  private static SqliteCheckpointFence fenceOver(CheckpointTable table) {
    return new SqliteCheckpointFence(table, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  /** A connection with a {@code BEGIN IMMEDIATE} transaction open, as the batch path opens it. */
  private Connection beginImmediate() throws SQLException {
    Connection conn = recorder.getConnection();
    SqliteTransactions.begin(conn);
    return conn;
  }

  private static void rollback(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("ROLLBACK");
    }
  }

  private static ProjectionCommitFencedException fenced(Executable call) {
    Throwable thrown = catchThrowable(call::execute);
    assertThat(thrown).isNotNull();
    assertThat(thrown.getClass()).isSameAs(ProjectionCommitFencedException.class);
    assertThat(thrown.getCause()).isNull();
    assertThat(thrown.getSuppressed()).isEmpty();
    return (ProjectionCommitFencedException) thrown;
  }

  private static String staleEpochMessage(long caller, long stored) {
    return "Projection 'p' commit fenced out: caller epoch "
        + caller
        + " is below the stored epoch "
        + stored
        + " — a newer leader has taken over the lease. Rolling back the stale leader's write"
        + " (epoch fencing).";
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpoch_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    fence.stampEpoch(recorder, P, 5L);

    try (Connection c = beginImmediate()) {
      var e = fenced(() -> fence.lockForBatch(c, P, 4L, null));
      assertThat(e.guard()).isSameAs(Guard.EPOCH_FENCE);
      assertThat(e.getMessage()).isEqualTo(staleEpochMessage(4, 5));
      rollback(c);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void overlappingBatch_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    fence.saveUnlocked(recorder, P, GlobalOffset.of(10));

    try (Connection c = beginImmediate()) {
      var e = fenced(() -> fence.lockForBatch(c, P, 0L, GlobalOffset.of(10)));
      assertThat(e.guard()).isSameAs(Guard.OVERLAP);
      assertThat(e.getMessage())
          .isEqualTo(
              "Projection 'p' batch starting at offset 10 overlaps the committed checkpoint (10):"
                  + " those events were already applied by another writer (a concurrent or"
                  + " failed-over stale leader, or a redelivery). Rolling back so the read-model"
                  + " write is not applied a second time.");
      rollback(c);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void monotonicRejection_exactMessageAndGuard(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    fence.saveUnlocked(recorder, P, GlobalOffset.of(10));

    for (long atOrBelow : new long[] {10L, 5L}) {
      try (Connection c = beginImmediate()) {
        var e = fenced(() -> fence.saveLocked(c, P, GlobalOffset.of(atOrBelow), 0L));
        assertThat(e.guard()).isSameAs(Guard.MONOTONIC);
        assertThat(e.getMessage())
            .isEqualTo(
                "Projection 'p' offset advance to "
                    + atOrBelow
                    + " was rejected by the monotonic guard (stored offset already at/past it):"
                    + " the batch is already applied. Rolling back so the read-model write is not"
                    + " committed a second time.");
        rollback(c);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void staleEpochIsCheckedBeforeOverlap(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    fence.stampEpoch(recorder, P, 5L);
    fence.saveUnlocked(recorder, P, GlobalOffset.of(10));

    try (Connection c = beginImmediate()) {
      var e = fenced(() -> fence.lockForBatch(c, P, 4L, GlobalOffset.of(10)));
      assertThat(e.guard()).isSameAs(Guard.EPOCH_FENCE);
      assertThat(e.getMessage()).isEqualTo(staleEpochMessage(4, 5));
      rollback(c);
    }
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void zeroCallerEpochNeverFences(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    fence.stampEpoch(recorder, P, 5L);
    fence.saveUnlocked(recorder, P, GlobalOffset.of(10));

    try (Connection c = beginImmediate()) {
      assertThat(fence.lockForBatch(c, P, 0L, GlobalOffset.of(11))).isEqualTo(10L);
      fence.saveLocked(c, P, GlobalOffset.of(12), 0L);
      SqliteTransactions.commit(c);
    }
    assertThat(fence.lastOffset(recorder, P)).isEqualTo(GlobalOffset.of(12));
    assertThat(fence.readEpoch(recorder, P)).as("epoch 0 stamps nothing").isEqualTo(5L);
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void epochRegression_isTheRegressionTypeUnwrapped(CheckpointTable table) {
    var fence = fenceOver(table);
    fence.stampEpoch(recorder, P, 7L);

    var e =
        assertThrows(
            ProjectionEpochRegressionException.class, () -> fence.stampEpoch(recorder, P, 3L));
    assertThat(e.getClass()).isSameAs(ProjectionEpochRegressionException.class);
    assertThat(e.getMessage())
        .isEqualTo(
            "Projection 'p' was handed leadership epoch 3 but its checkpoint row already stores"
                + " epoch 7. The lease table and the projection checkpoint have diverged —"
                + " typically because subscription_leases was cleared or restored independently of"
                + " subscription_checkpoint. Every commit under epoch 3 would be rejected by the"
                + " epoch fence, so the projection cannot make progress. Recover by raising the"
                + " lease epoch above 7, or (with every replica of this projection stopped) by"
                + " resetting the checkpoint epoch to 0 so the next takeover re-arms the fence.");
    assertThat(e.projectionName()).isEqualTo(P);
    assertThat(e.acquiredEpoch()).isEqualTo(3L);
    assertThat(e.storedEpoch()).isEqualTo(7L);
    assertThat(fence.readEpoch(recorder, P)).isEqualTo(7L);
  }

  /**
   * {@link SqliteTransactions#write} wraps every exception its work throws in an {@link
   * EventStoreException}. The regression is decided after the write committed and returned, so it
   * reaches the caller as itself: no wrapper, no cause, nothing suppressed, and the stamp's
   * transaction committed and its connection closed before the throw.
   */
  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void epochRegression_isThrownOutsideTheWrite(CheckpointTable table) {
    var fence = fenceOver(table);
    fence.stampEpoch(recorder, P, 7L);
    recorder.clear();

    Throwable thrown = catchThrowable(() -> fence.stampEpoch(recorder, P, 3L));

    assertThat(thrown.getClass()).isSameAs(ProjectionEpochRegressionException.class);
    assertThat(thrown.getCause()).isNull();
    assertThat(thrown.getSuppressed()).isEmpty();
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      assertThat(t).isNotInstanceOf(EventStoreException.class);
    }
    assertThat(recorder.transcript()).endsWith("#1 execute COMMIT", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void stampEpoch_transcript(CheckpointTable table) {
    var fence = fenceOver(table);
    String name = table.tableName();
    String stamp =
        "INSERT INTO "
            + name
            + " (projection_name, last_offset, epoch, updated_at) VALUES (?, 0, ?, ?)"
            + " ON CONFLICT (projection_name) DO UPDATE SET epoch = max("
            + name
            + ".epoch, excluded.epoch), updated_at = excluded.updated_at WHERE excluded.epoch > "
            + name
            + ".epoch";
    String readEpoch = "SELECT epoch FROM " + name + " WHERE projection_name = ?";

    // A stamp that is written: no epoch read.
    fence.stampEpoch(recorder, P, 7L);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + stamp,
            "#1 execute COMMIT",
            "#1 close");

    // A stamp that loses to a higher stored epoch: the epoch read on the same connection, in the
    // same transaction, then the commit.
    recorder.clear();
    catchThrowable(() -> fence.stampEpoch(recorder, P, 3L));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + stamp,
            "#1 prepare " + readEpoch,
            "#1 execute COMMIT",
            "#1 close");
    assertThat(recorder.connectionsOpened()).isEqualTo(1);
  }

  /** A data source whose every connection request throws {@code failure}. */
  private static DataSource failingWith(Exception failure) {
    return (DataSource)
        Proxy.newProxyInstance(
            SqliteCheckpointFenceCharacterizationTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw failure;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  @Test
  void failureMessages() {
    var fence = fenceOver(CheckpointTable.SUBSCRIPTION);
    SQLException down = new SQLException("connection refused");
    DataSource ds = failingWith(down);

    Map<String, Executable> calls =
        Map.of(
            "Failed to read fencing epoch for projection: p", () -> fence.readEpoch(ds, P),
            "Failed to read offset for projection: p", () -> fence.lastOffset(ds, P),
            "Failed to reset offset for projection: p", () -> fence.reset(ds, P),
            "Failed to save offset for projection: p",
                () -> fence.saveUnlocked(ds, P, GlobalOffset.of(1)),
            "Failed to stamp fencing epoch 3 for projection: p", () -> fence.stampEpoch(ds, P, 3L));
    calls.forEach(
        (message, call) -> {
          Throwable thrown = catchThrowable(call::execute);
          assertThat(thrown.getClass()).as(message).isSameAs(EventStoreException.class);
          assertThat(thrown.getMessage()).isEqualTo(message);
          assertThat(thrown.getCause()).as(message).isSameAs(down);
          assertThat(thrown.getSuppressed()).as(message).isEmpty();
        });
  }

  /**
   * An unchecked failure of the connection request: the writes run it inside {@link
   * SqliteTransactions#write}, which wraps it with the operation's message; the reads catch only
   * {@link SQLException}, so it reaches their caller as itself.
   */
  @Test
  void anUncheckedConnectionFailure_isWrappedByTheWritesOnly() {
    var fence = fenceOver(CheckpointTable.SUBSCRIPTION);
    IllegalStateException closed = new IllegalStateException("pool closed");
    DataSource ds = failingWith(closed);

    Map<String, Executable> writes =
        Map.of(
            "Failed to reset offset for projection: p", () -> fence.reset(ds, P),
            "Failed to save offset for projection: p",
                () -> fence.saveUnlocked(ds, P, GlobalOffset.of(1)),
            "Failed to stamp fencing epoch 3 for projection: p", () -> fence.stampEpoch(ds, P, 3L));
    writes.forEach(
        (message, call) -> {
          Throwable thrown = catchThrowable(call::execute);
          assertThat(thrown.getClass()).as(message).isSameAs(EventStoreException.class);
          assertThat(thrown.getMessage()).isEqualTo(message);
          assertThat(thrown.getCause()).as(message).isSameAs(closed);
          assertThat(thrown.getSuppressed()).as(message).isEmpty();
        });
    for (Executable read :
        List.<Executable>of(() -> fence.readEpoch(ds, P), () -> fence.lastOffset(ds, P))) {
      assertThat(catchThrowable(read::execute)).isSameAs(closed);
    }
  }

  /**
   * A write that SQLite's write lock is not granted to within busy_timeout fails with the lock
   * advice naming the operation, the driver's busy exception as its cause.
   */
  @Test
  void busyWrite_exactLockAdviceMessage() throws Exception {
    var fence = fenceOver(CheckpointTable.READ_MODEL);
    DataSource impatient = TestDatabases.dataSource(database.file(), "busy_timeout=100");
    String lead =
        "SQLite's write lock was not granted within the connection's busy_timeout, so the ";

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");

      Map<String, Executable> writes =
          Map.of(
              "fencing stamp of projection p", () -> fence.stampEpoch(impatient, P, 2L),
              "offset save of projection p",
                  () -> fence.saveUnlocked(impatient, P, GlobalOffset.of(3)),
              "offset reset of projection p", () -> fence.reset(impatient, P));
      writes.forEach(
          (what, call) -> {
            Throwable thrown = catchThrowable(call::execute);
            assertThat(thrown.getClass()).as(what).isSameAs(EventStoreException.class);
            assertThat(thrown.getMessage()).isEqualTo(lead + what + BUSY_ADVICE);
            assertThat(thrown.getCause()).as(what).isExactlyInstanceOf(SQLiteException.class);
            assertThat(((SQLiteException) thrown.getCause()).getResultCode())
                .isSameAs(SQLiteErrorCode.SQLITE_BUSY);
            assertThat(thrown.getSuppressed()).as(what).isEmpty();
          });

      st.execute("ROLLBACK");
    }
  }

  /**
   * A connection in manual-commit mode is a configuration error the write refuses after it closed
   * the connection, outside its exception mapping: the {@link IllegalStateException} is not
   * wrapped.
   */
  @Test
  void manualCommitConnection_isRefusedUnwrapped() {
    var fence = fenceOver(CheckpointTable.READ_MODEL);
    SwitchableDataSource source = new SwitchableDataSource(database.dataSource());
    source.manualCommit();

    for (Executable write :
        List.<Executable>of(
            () -> fence.stampEpoch(source, P, 2L),
            () -> fence.saveUnlocked(source, P, GlobalOffset.of(5)),
            () -> fence.reset(source, P))) {
      Throwable thrown = catchThrowable(write::execute);
      assertThat(thrown.getClass()).isSameAs(IllegalStateException.class);
      assertThat(thrown.getMessage()).isEqualTo(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
      assertThat(thrown.getCause()).isNull();
    }
  }

  /** The connection methods refuse a connection with no transaction open, before writing. */
  @ParameterizedTest
  @EnumSource(CheckpointTable.class)
  void connectionMethodsOutsideATransaction_areRefused(CheckpointTable table) throws Exception {
    var fence = fenceOver(table);
    try (Connection c = recorder.getConnection()) {
      for (Executable call :
          List.<Executable>of(
              () -> fence.lockForBatch(c, P, 0L, GlobalOffset.of(1)),
              () -> fence.saveLocked(c, P, GlobalOffset.of(1), 0L),
              () -> fence.lockForReplay(c, P))) {
        Throwable thrown = catchThrowable(call::execute);
        assertThat(thrown.getClass()).isSameAs(IllegalStateException.class);
        assertThat(thrown.getMessage())
            .isEqualTo(
                "This SQLite step must run inside the caller's open write transaction (BEGIN"
                    + " IMMEDIATE ... COMMIT, as SqliteTransactions.inTransaction runs it, together"
                    + " with the writes it guards), but the connection has no transaction open."
                    + " Outside one, each statement commits on its own and a check reads a row"
                    + " another writer can change before the guarded write lands; nothing was"
                    + " written.");
        assertThat(thrown.getCause()).isNull();
      }
    }
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM " + table.tableName())) {
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        assertThat(rs.getLong(1)).as("nothing was written").isZero();
      }
    }
  }

  @Test
  void nullNameIsRefusedWithItsMessage() {
    var fence = fenceOver(CheckpointTable.READ_MODEL);

    Throwable thrown = catchThrowable(() -> fence.stampEpoch(recorder, null, 1L));

    assertThat(thrown.getClass()).isSameAs(NullPointerException.class);
    assertThat(thrown.getMessage()).isEqualTo("projectionName is required");
    assertThat(recorder.connectionsOpened()).as("refused before any I/O").isZero();
  }

  /** The fence's refusals and failures reach the caller as exceptions and log nothing. */
  @Test
  void refusalsAndFailuresLogNothing() throws Exception {
    var fence = fenceOver(CheckpointTable.READ_MODEL);
    DataSource down = failingWith(new SQLException("connection refused"));
    try (LogCapture fenceLog = LogCapture.attach(SqliteCheckpointFence.class);
        LogCapture transactionsLog = LogCapture.attach(SqliteTransactions.class)) {
      fence.stampEpoch(recorder, P, 7L);
      catchThrowable(() -> fence.stampEpoch(recorder, P, 3L));
      try (Connection c = beginImmediate()) {
        catchThrowable(() -> fence.lockForBatch(c, P, 4L, null));
        rollback(c);
      }
      catchThrowable(() -> fence.stampEpoch(down, P, 3L));
      catchThrowable(() -> fence.readEpoch(down, P));

      for (LogCapture capture : List.of(fenceLog, transactionsLog)) {
        assertThat(capture.events(Level.WARN)).isEmpty();
        assertThat(capture.events(Level.ERROR)).isEmpty();
      }
    }
  }
}

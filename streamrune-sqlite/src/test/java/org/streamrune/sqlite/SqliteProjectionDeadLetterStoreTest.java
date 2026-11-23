package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;

/**
 * What {@link SqliteProjectionDeadLetterStore} adds to the shared contract: every write is durable
 * on return and refuses a connection that carries an application's open transaction or is in
 * manual-commit mode, every write waits for the write lock at most {@code busy_timeout}, and a save
 * killed at any point leaves the store as before or as after it.
 */
class SqliteProjectionDeadLetterStoreTest {

  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");
  private static final ProjectionName ORDERS = ProjectionName.of("orders");
  private static final long BUSY_MILLIS = 200;
  private static final long SLACK_MILLIS = 1_500;

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final List<SingleConnection> connections = new ArrayList<>();

  @AfterEach
  void close() throws SQLException {
    handles.forEach(SqliteDatabase::close);
    for (SingleConnection connection : connections) {
      connection.close();
    }
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private SqliteDatabase migrated(String file, String override) {
    SqliteDatabase database =
        open(
            override.isEmpty()
                ? TestDatabases.dataSource(dir.resolve(file))
                : TestDatabases.dataSource(dir.resolve(file), override));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  private static ProjectionDeadLetterEntry entry(long from, int attempts, Instant failedAt) {
    return new ProjectionDeadLetterEntry(
        ORDERS,
        GlobalOffset.of(from),
        GlobalOffset.of(from + 9),
        10,
        "java.lang.IllegalStateException",
        "boom",
        attempts,
        failedAt);
  }

  @Test
  void rejectsANullDatabase() {
    assertThatThrownBy(() -> new SqliteProjectionDeadLetterStore(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
  }

  /**
   * A write on a connection that carries a transaction the application opened is refused, not
   * joined, and the refusal leaves the application's transaction open.
   */
  @Test
  void everyWriteRefusesAConnectionCarryingAnApplicationTransaction() throws SQLException {
    SqliteDatabase plain = migrated("foreign.db", "");
    var store = new SqliteProjectionDeadLetterStore(plain);
    store.save(entry(1, 1, T0));
    SingleConnection single = new SingleConnection(TestDatabases.dataSource(plain.file()));
    connections.add(single);
    var onShared = new SqliteProjectionDeadLetterStore(open(single));
    try (Connection app = single.getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      st.execute("UPDATE projection_dead_letters SET error_message = 'app'");
      assertThatThrownBy(() -> onShared.save(entry(2, 1, T0)))
          .isExactlyInstanceOf(EventStoreException.class)
          .rootCause()
          .hasMessageContaining("cannot start a transaction within a transaction");
      assertThatThrownBy(() -> onShared.discard(ORDERS, GlobalOffset.of(1)))
          .isExactlyInstanceOf(EventStoreException.class)
          .rootCause()
          .hasMessageContaining("cannot start a transaction within a transaction");
      st.execute("COMMIT"); // still the application's transaction: the refusal left it open
    }

    assertThat(store.readAll(10))
        .singleElement()
        .satisfies(e -> assertThat(e.errorMessage()).isEqualTo("app"));
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedBeforeAnythingIsWritten() {
    SqliteDatabase database = migrated("manual.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    var store = new SqliteProjectionDeadLetterStore(open(source));
    new SqliteProjectionDeadLetterStore(database).save(entry(1, 1, T0));
    source.manualCommit();

    assertThatThrownBy(() -> store.save(entry(2, 1, T0)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    assertThatThrownBy(() -> store.discard(ORDERS, GlobalOffset.of(1)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    assertThat(new SqliteProjectionDeadLetterStore(database).countPending()).isEqualTo(1);
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void everyWriteWaitsAtMostBusyTimeoutForTheWriteLock_andReadsDoNotWait() throws Exception {
    SqliteDatabase database = migrated("busy.db", "busy_timeout=" + BUSY_MILLIS);
    var store = new SqliteProjectionDeadLetterStore(database);
    store.save(entry(1, 1, T0));
    Map<String, Runnable> writes = new LinkedHashMap<>();
    writes.put("save", () -> store.save(entry(1, 5, T0.plusSeconds(1))));
    writes.put("discard", () -> store.discard(ORDERS, GlobalOffset.of(1)));

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      for (var write : writes.entrySet()) {
        long started = System.nanoTime();
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("busy_timeout");
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
            .as(write.getKey() + " waited")
            .isLessThan(BUSY_MILLIS + SLACK_MILLIS);
      }
      assertThat(store.read(ORDERS, 10)).hasSize(1);
      assertThat(store.readAll(10)).hasSize(1);
      assertThat(store.countPending()).isEqualTo(1);
      st.execute("ROLLBACK");
    }

    assertThat(store.readAll(10)).as("nothing was written").containsExactly(entry(1, 1, T0));
  }

  /**
   * A save killed inside its row write leaves the store as it was, for a new batch and for a
   * refresh; killed right after its COMMIT, the save is done. A rerun after a commit counts one
   * attempt more, as a rerun after a lost acknowledgement does on Postgres; attempts never go down.
   */
  @Test
  void aSaveKilledAtAnyPointLeavesTheStoreAsBeforeOrAsAfter() {
    Path file = dir.resolve("crash-save.db");
    var check = new SqliteProjectionDeadLetterStore(migrated("crash-save.db", ""));
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    var store =
        new SqliteProjectionDeadLetterStore(
            open(crash.connections(TestDatabases.dataSource(file))));

    crash.arm(new Point("projection_dead_letters", 1));
    assertThatThrownBy(() -> store.save(entry(1, 1, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.countPending()).isZero();

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.save(entry(1, 1, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.readAll(10)).containsExactly(entry(1, 1, T0));

    crash.arm(new Point("projection_dead_letters", 1));
    assertThatThrownBy(() -> store.save(entry(1, 1, T0.plusSeconds(1))))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(check.readAll(10))
        .as("a killed refresh changes nothing")
        .containsExactly(entry(1, 1, T0));

    store.save(entry(1, 1, T0.plusSeconds(1)));
    assertThat(check.readAll(10)).containsExactly(entry(1, 2, T0.plusSeconds(1)));
  }

  @Test
  void aStorageFailureIsAnEventStoreExceptionForEveryReadAndWrite() throws SQLException {
    SqliteDatabase database = migrated("missing.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    var store = new SqliteProjectionDeadLetterStore(open(source));
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE projection_dead_letters");
    }
    List<Runnable> calls =
        List.of(
            () -> store.save(entry(1, 1, T0)),
            () -> store.read(ORDERS, 1),
            () -> store.readAll(1),
            () -> store.discard(ORDERS, GlobalOffset.of(1)),
            () -> store.countPending());
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(SQLException.class);
    }

    source.failWith(() -> new IllegalStateException("pool closed"));
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
    }
  }
}

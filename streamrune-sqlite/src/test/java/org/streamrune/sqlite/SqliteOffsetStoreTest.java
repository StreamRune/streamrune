package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.EventStoreContract;

/**
 * What {@link SqliteOffsetStore} adds to the shared contract: how its writes meet the connection's
 * state, the write lock and a broken table, and what it stamps.
 */
class SqliteOffsetStoreTest {

  private static final ProjectionName P = ProjectionName.of("offset-test");

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final List<SingleConnection> connections = new ArrayList<>();

  @AfterEach
  void closeHandles() throws SQLException {
    handles.forEach(SqliteDatabase::close);
    for (SingleConnection connection : connections) {
      connection.close();
    }
  }

  private SqliteDatabase open(String file, String override) {
    DataSource ds = TestDatabases.dataSource(dir.resolve(file), override);
    SqliteDatabase database = SqliteDatabase.open(ds);
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  /**
   * The application left a BEGIN open on the connection: a bare statement would join that
   * transaction and not be durable. The write is refused instead, writes nothing, and neither
   * commits nor rolls back the application's transaction.
   */
  @Test
  void aWriteOnAConnectionWithAnOpenTransactionIsRefusedAndLeavesThatTransactionAlone()
      throws Exception {
    Path file = dir.resolve("foreign.db");
    SqliteDatabase database = open("foreign.db", "");
    // One physical connection, so the store borrows the one the application left a BEGIN on.
    SingleConnection connection = new SingleConnection(TestDatabases.dataSource(file));
    connections.add(connection);
    SqliteDatabase single = SqliteDatabase.open(connection);
    handles.add(single);
    SqliteOffsetStore store = new SqliteOffsetStore(single);
    try (Connection app = single.dataSource().getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      st.execute("CREATE TABLE app_marker (x INTEGER) STRICT");

      assertThatThrownBy(() -> store.saveOffset(P, GlobalOffset.of(5)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("Failed to save offset");
      assertThatThrownBy(() -> store.reset(P))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("Failed to reset offset");

      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM app_marker")) {
        assertThat(rs.next()).as("the application's transaction is still open").isTrue();
      }
      st.execute("ROLLBACK");
    }
    assertThat(new SqliteOffsetStore(database).getLastOffset(P)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void aWriteIsDurableWhenItReturns() throws Exception {
    SqliteDatabase database = open("durable.db", "");
    new SqliteOffsetStore(database).saveOffset(P, GlobalOffset.of(9));

    try (Connection other = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = other.createStatement();
        ResultSet rs = st.executeQuery("SELECT last_offset FROM subscription_checkpoint")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getLong(1)).isEqualTo(9);
    }
  }

  @Test
  void aWriteWhileAnotherConnectionHoldsTheLockFailsAsBusyAndWritesNothing() throws Exception {
    SqliteDatabase database = open("busy.db", "busy_timeout=100");
    SqliteOffsetStore store = new SqliteOffsetStore(database);
    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");

      assertThatThrownBy(() -> store.saveOffset(P, GlobalOffset.of(3)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("busy_timeout");

      st.execute("ROLLBACK");
    }
    assertThat(store.getLastOffset(P)).isEqualTo(GlobalOffset.initial());
    store.saveOffset(P, GlobalOffset.of(3));
    assertThat(store.getLastOffset(P)).isEqualTo(GlobalOffset.of(3));
  }

  @Test
  void aMissingTableFailsAsAnEventStoreExceptionForReadAndWrite() throws Exception {
    SqliteDatabase database = open("missing.db", "");
    SqliteOffsetStore store = new SqliteOffsetStore(database);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE subscription_checkpoint");
    }

    assertThatThrownBy(() -> store.getLastOffset(P))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to read offset")
        .hasCauseInstanceOf(SQLException.class);
    assertThatThrownBy(() -> store.saveOffset(P, GlobalOffset.of(1)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to save offset");
    assertThatThrownBy(() -> store.reset(P))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to reset offset");
  }

  @Test
  void updatedAtIsStampedWithTheClockInMicrosAndOnlyAdvancingSavesRestampIt() throws Exception {
    SqliteDatabase database = open("clock.db", "");
    Instant first = Instant.parse("2026-11-23T10:00:00.123456Z");
    Instant later = Instant.parse("2026-11-23T11:00:00Z");
    new SqliteOffsetStore(database, Clock.fixed(first, ZoneOffset.UTC))
        .saveOffset(P, GlobalOffset.of(4));
    assertThat(updatedAt(database)).isEqualTo(first);

    SqliteOffsetStore afterwards =
        new SqliteOffsetStore(database, Clock.fixed(later, ZoneOffset.UTC));
    afterwards.saveOffset(P, GlobalOffset.of(4));
    assertThat(updatedAt(database)).as("a no-op save restamps nothing").isEqualTo(first);

    afterwards.reset(P);
    assertThat(updatedAt(database)).as("a reset restamps").isEqualTo(later);
  }

  @Test
  void rejectsNullClock() {
    SqliteDatabase database = open("nullclock.db", "");
    assertThatThrownBy(() -> new SqliteOffsetStore(database, null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessageContaining("clock");
  }

  private static Instant updatedAt(SqliteDatabase database) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT updated_at FROM subscription_checkpoint")) {
      rs.next();
      return SqliteTimestamps.fromMicros(rs.getLong(1));
    }
  }
}

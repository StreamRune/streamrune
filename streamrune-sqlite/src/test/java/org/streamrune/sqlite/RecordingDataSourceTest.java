package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteConnection;

/** The recorder the characterization tests read: its events, its hook and its close failure. */
class RecordingDataSourceTest {

  @TempDir Path dir;

  private RecordingDataSource recorder;

  @BeforeEach
  void createTable() throws SQLException {
    recorder = new RecordingDataSource(TestDatabases.dataSource(dir.resolve("recorder.db")));
    try (Connection conn = recorder.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT");
    }
    recorder.clear();
  }

  @Test
  void recordsTheEventVocabularyInOrder() throws SQLException {
    try (Connection first = recorder.getConnection()) {
      first.setAutoCommit(false);
      try (PreparedStatement ps = first.prepareStatement("INSERT INTO t (v) VALUES (?)")) {
        ps.setFetchSize(50);
        ps.setQueryTimeout(7);
        ps.setString(1, "a");
        ps.executeUpdate();
      }
      first.commit();
      try (Statement st = first.createStatement()) {
        st.executeUpdate("DELETE FROM t");
      }
      first.rollback();
      first.setAutoCommit(true);
      assertThat(recorder.openConnections()).isEqualTo(1);
      try (Connection second = recorder.getConnection();
          Statement st = second.createStatement();
          ResultSet rs = st.executeQuery("SELECT count(*) FROM t")) {
        rs.next();
        assertThat(rs.getLong(1)).isEqualTo(1);
        assertThat(st.getConnection())
            .as("a statement answers its recorded connection")
            .isSameAs(second);
        assertThat(recorder.openConnections()).isEqualTo(2);
      }
      try (Statement st = first.createStatement()) {
        st.execute("SELECT 1");
      }
    }

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare INSERT INTO t (v) VALUES (?)",
            "#1 fetchSize=50",
            "#1 queryTimeout=7",
            "#1 commit",
            "#1 execute DELETE FROM t",
            "#1 rollback",
            "#1 autoCommit=true",
            "open #2",
            "#2 execute SELECT count(*) FROM t",
            "#2 close",
            "#1 execute SELECT 1",
            "#1 close");
    assertThat(recorder.sql())
        .containsExactly(
            "INSERT INTO t (v) VALUES (?)", "DELETE FROM t", "SELECT count(*) FROM t", "SELECT 1");
    assertThat(recorder.connectionsOpened()).isEqualTo(2);
    assertThat(recorder.openConnections()).isZero();

    recorder.clear();
    assertThat(recorder.transcript()).isEmpty();
    assertThat(recorder.connectionsOpened()).isZero();
    try (Connection again = recorder.getConnection()) {
      assertThat(again).isNotNull();
    }
    assertThat(recorder.transcript())
        .as("the numbering restarts after clear")
        .containsExactly("open #1", "#1 close");
  }

  @Test
  void beforePrepareRunsOnceOnTheSameConnection() throws SQLException {
    List<Connection> seen = new ArrayList<>();
    recorder.beforePrepare(
        "INSERT INTO t",
        same -> {
          seen.add(same);
          try (Statement st = same.createStatement()) {
            st.execute("INSERT INTO t (id, v) VALUES (100, 'hook')");
          }
        });

    try (Connection conn = recorder.getConnection()) {
      SqliteTransactions.begin(conn);
      try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM t")) {
        ps.executeQuery().close();
      }
      assertThat(seen).as("a statement of another prefix runs no hook").isEmpty();
      for (String v : new String[] {"a", "b"}) {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (v) VALUES (?)")) {
          ps.setString(1, v);
          ps.executeUpdate();
        }
      }
      assertThat(seen).as("the hook runs once").hasSize(1);
      assertThat(seen.getFirst().unwrap(SQLiteConnection.class))
          .as("on the same connection")
          .isSameAs(conn.unwrap(SQLiteConnection.class));
      // The hook's insert is part of the connection's open transaction: a rollback undoes it.
      SqliteTransactions.rollback(conn, "test");
    }

    try (Connection conn = recorder.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM t")) {
      rs.next();
      assertThat(rs.getLong(1)).isZero();
    }
    assertThat(recorder.transcript())
        .as("the hook's own statement is not recorded")
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare SELECT count(*) FROM t",
            "#1 prepare INSERT INTO t (v) VALUES (?)",
            "#1 prepare INSERT INTO t (v) VALUES (?)",
            "#1 execute ROLLBACK",
            "#1 close",
            "open #2",
            "#2 execute SELECT count(*) FROM t",
            "#2 close");
  }

  @Test
  void failNextCloseAfterCommitThrowsAfterDelegating() throws SQLException {
    recorder.failNextCloseAfterCommit();

    // A connection that never committed closes quietly and leaves the failure armed.
    Connection reader = recorder.getConnection();
    reader.close();

    // Connection.commit() counts as a commit.
    Connection committer = recorder.getConnection();
    committer.setAutoCommit(false);
    committer.commit();
    assertThatThrownBy(committer::close)
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(committer.unwrap(SQLiteConnection.class).isClosed())
        .as("the real connection was closed before the failure")
        .isTrue();
    assertThat(recorder.openConnections()).isZero();

    // The failure is spent: the next committed close succeeds.
    Connection next = recorder.getConnection();
    next.setAutoCommit(false);
    next.commit();
    next.close();

    // A COMMIT statement, how the SQLite stores commit, counts as a commit as well.
    recorder.failNextCloseAfterCommit();
    Connection statementCommitter = recorder.getConnection();
    SqliteTransactions.begin(statementCommitter);
    SqliteTransactions.commit(statementCommitter);
    assertThatThrownBy(statementCommitter::close)
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(statementCommitter.unwrap(SQLiteConnection.class).isClosed()).isTrue();
    assertThat(recorder.openConnections()).isZero();

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 close",
            "open #2",
            "#2 autoCommit=false",
            "#2 commit",
            "#2 close",
            "open #3",
            "#3 autoCommit=false",
            "#3 commit",
            "#3 close",
            "open #4",
            "#4 execute BEGIN IMMEDIATE",
            "#4 execute COMMIT",
            "#4 close");
  }

  /**
   * {@link SqliteTransactions} unwraps a connection to the driver's {@link SQLiteConnection} for
   * the in-process write mutex: the recorded connection hands out the real one.
   */
  @Test
  void unwrapReachesTheRealSqliteConnection() throws SQLException {
    try (Connection conn = recorder.getConnection()) {
      assertThat(conn).isNotInstanceOf(SQLiteConnection.class);
      assertThat(conn.isWrapperFor(SQLiteConnection.class)).isTrue();
      SQLiteConnection sqlite = conn.unwrap(SQLiteConnection.class);
      assertThat(sqlite).isNotNull();
      assertThat(sqlite.getUrl()).contains("recorder.db");

      // A write through SqliteTransactions takes the write mutex through the unwrapped connection.
      int inserted =
          SqliteTransactions.inTransaction(
              conn,
              "test insert",
              c -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO t (v) VALUES ('x')")) {
                  return ps.executeUpdate();
                }
              });
      assertThat(inserted).isEqualTo(1);
    }
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare INSERT INTO t (v) VALUES ('x')",
            "#1 execute COMMIT",
            "#1 close");
  }

  /**
   * {@code clear()} forgets the events and restarts the numbering, but a hook registered and a
   * close failure armed before it stay in force: a test arms them during setup and clears the
   * setup's events afterwards.
   */
  @Test
  void clearKeepsRegisteredHooksAndAnArmedCloseFailure() throws SQLException {
    List<String> hookRuns = new ArrayList<>();
    recorder.beforePrepare("INSERT INTO t", same -> hookRuns.add("ran"));
    recorder.failNextCloseAfterCommit();
    try (Connection setup = recorder.getConnection();
        Statement st = setup.createStatement()) {
      st.execute("SELECT 1");
    }

    recorder.clear();
    assertThat(recorder.transcript()).isEmpty();
    assertThat(recorder.sql()).isEmpty();
    assertThat(recorder.connectionsOpened()).isZero();

    Connection conn = recorder.getConnection();
    SqliteTransactions.begin(conn);
    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (v) VALUES ('y')")) {
      ps.executeUpdate();
    }
    SqliteTransactions.commit(conn);
    assertThat(hookRuns).as("the hook registered before clear() runs").containsExactly("ran");
    assertThatThrownBy(conn::close)
        .as("the close failure armed before clear() fires")
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare INSERT INTO t (v) VALUES ('y')",
            "#1 execute COMMIT",
            "#1 close");
  }

  /**
   * {@code commit} and {@code rollback} are recorded before they are delegated, so a failing one
   * leaves its event; only a commit that succeeded counts as one for the close failure. SQLite
   * refuses both on a connection in autocommit, and a {@code COMMIT} statement with no transaction
   * open.
   */
  @Test
  void aFailedCommitIsRecordedButIsNoCommit() throws SQLException {
    recorder.failNextCloseAfterCommit();
    Connection conn = recorder.getConnection();
    assertThatThrownBy(conn::commit).isInstanceOf(SQLException.class);
    assertThatThrownBy(conn::rollback).isInstanceOf(SQLException.class);
    assertThatThrownBy(() -> SqliteTransactions.commit(conn)).isInstanceOf(SQLException.class);
    conn.close();

    assertThat(recorder.openConnections()).isZero();
    assertThat(recorder.transcript())
        .as("no commit succeeded, so the armed close failure did not fire")
        .containsExactly("open #1", "#1 commit", "#1 rollback", "#1 execute COMMIT", "#1 close");
  }

  @Test
  void aRollbackToASavepointIsDistinctFromARollback() throws SQLException {
    try (Connection conn = recorder.getConnection()) {
      conn.setAutoCommit(false);
      var savepoint = conn.setSavepoint();
      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (v) VALUES ('s')")) {
        ps.executeUpdate();
      }
      conn.rollback(savepoint);
      conn.rollback();
      conn.setAutoCommit(true);
    }
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare INSERT INTO t (v) VALUES ('s')",
            "#1 rollback to savepoint",
            "#1 rollback",
            "#1 autoCommit=true",
            "#1 close");
  }

  /** Every hook that matches and has not run yet runs once, in registration order. */
  @Test
  void matchingHooksRunOnceEachInRegistrationOrder() throws SQLException {
    List<String> runs = new ArrayList<>();
    recorder.beforePrepare("INSERT INTO t", same -> runs.add("specific"));
    recorder.beforePrepare("SELECT", same -> runs.add("other prefix"));
    recorder.beforePrepare("INSERT", same -> runs.add("general"));

    try (Connection conn = recorder.getConnection()) {
      for (int i = 0; i < 2; i++) {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t (v) VALUES ('h')")) {
          ps.executeUpdate();
        }
      }
    }
    assertThat(runs).containsExactly("specific", "general");
  }
}

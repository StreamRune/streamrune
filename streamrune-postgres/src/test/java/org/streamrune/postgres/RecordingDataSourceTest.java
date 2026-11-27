package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The recording decorator itself: its event vocabulary and its two fault and race hooks. */
@Testcontainers
class RecordingDataSourceTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_recorder_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void connect() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  private static long backendPid(Connection conn) throws SQLException {
    try (var ps = conn.prepareStatement("SELECT pg_backend_pid()");
        var rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Test
  void recordsTheEventVocabularyInOrder() throws Exception {
    var recorder = new RecordingDataSource(dataSource);

    try (Connection first = recorder.getConnection()) {
      first.setAutoCommit(false);
      try (var ps = first.prepareStatement("SELECT 1")) {
        ps.setFetchSize(500);
        ps.setQueryTimeout(3);
        StatementTimeout.of(Duration.ofSeconds(7)).applyTo(ps);
        assertSame(first, ps.getConnection());
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next());
        }
      }
      try (Connection second = recorder.getConnection()) {
        assertEquals(2, recorder.openConnections());
        try (var st = second.createStatement()) {
          st.execute("SELECT 2");
          st.executeQuery("SELECT 3").close();
          st.executeUpdate("SET application_name = 'recorder'");
        }
      }
      first.commit();
      first.rollback();
      first.setAutoCommit(true);
    }

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare SELECT 1",
            "#1 fetchSize=500",
            "#1 queryTimeout=3",
            "#1 queryTimeout=driver",
            "open #2",
            "#2 execute SELECT 2",
            "#2 execute SELECT 3",
            "#2 execute SET application_name = 'recorder'",
            "#2 close",
            "#1 commit",
            "#1 rollback",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(recorder.sql())
        .containsExactly("SELECT 1", "SELECT 2", "SELECT 3", "SET application_name = 'recorder'");
    assertEquals(2, recorder.connectionsOpened());
    assertEquals(0, recorder.openConnections());

    // clear() restarts the numbering and the opened count; a connection still open keeps counting.
    try (Connection held = recorder.getConnection()) {
      recorder.clear();
      assertThat(recorder.transcript()).isEmpty();
      assertEquals(0, recorder.connectionsOpened());
      assertEquals(1, recorder.openConnections());
      try (Connection next = recorder.getConnection()) {
        assertTrue(next.isValid(1));
      }
      assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
      assertEquals(1, recorder.connectionsOpened());
      assertTrue(held.isValid(1));
    }
    assertEquals(0, recorder.openConnections());
  }

  @Test
  void beforePrepareRunsOnceOnTheSameConnection() throws Exception {
    var recorder = new RecordingDataSource(dataSource);
    var runs = new AtomicInteger();
    var hookPid = new AtomicLong();
    recorder.beforePrepare(
        "SELECT pg_backend_pid",
        c -> {
          runs.incrementAndGet();
          hookPid.set(backendPid(c));
        });

    long statementPid;
    try (Connection conn = recorder.getConnection()) {
      statementPid = backendPid(conn);
      assertEquals(statementPid, backendPid(conn));
    }

    assertEquals(1, runs.get(), "the hook is one-shot");
    assertEquals(statementPid, hookPid.get(), "the hook runs on the statement's own connection");
    // The hook's own statement is not recorded; the matching prepare follows it.
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare SELECT pg_backend_pid()",
            "#1 prepare SELECT pg_backend_pid()",
            "#1 close");
  }

  @Test
  void failNextCloseAfterCommitThrowsAfterDelegating() throws Exception {
    var recorder = new RecordingDataSource(dataSource);
    recorder.failNextCloseAfterCommit();

    // A connection that never committed closes cleanly and leaves the arm in place.
    Connection uncommitted = recorder.getConnection();
    uncommitted.close();

    Connection committed = recorder.getConnection();
    committed.setAutoCommit(false);
    committed.commit();
    PGConnection physical = committed.unwrap(PGConnection.class);
    var failure = assertThrows(SQLException.class, committed::close);
    assertSame(SQLException.class, failure.getClass());
    assertEquals("injected close failure", failure.getMessage());
    assertNull(failure.getCause());
    assertTrue(((Connection) physical).isClosed(), "the real connection was closed first");
    assertEquals(0, recorder.openConnections());

    // One-shot: the next committed connection closes cleanly.
    try (Connection again = recorder.getConnection()) {
      again.setAutoCommit(false);
      again.commit();
    }

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
            "#3 close");
  }
}

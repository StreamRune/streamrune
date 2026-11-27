package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;

/**
 * {@link SqliteAppendFailures} on a real database file: the failures SQLite raises for a duplicate
 * stream version, a duplicate global offset and a write lock held by another connection, classified
 * as the event store classifies them, and the desync message naming {@code global_offset_counter}.
 */
class SqliteAppendFailuresTest {

  private static final String INSERT_EVENT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, created_at, schema_version) VALUES (?, 'order', ?, ?, 'Placed',"
          + " '{}', '{}', 0, 1)";

  private final SqliteAppendFailures classifier = SqliteAppendFailures.INSTANCE;

  @TempDir Path dir;

  private SqliteDatabase database;

  @BeforeEach
  void storeOneEvent() throws SQLException {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("failures.db")));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      // The trigger refuses an event row above the counter: leave room for the rows below.
      st.executeUpdate("UPDATE global_offset_counter SET next_value = 5 WHERE id = 1");
    }
    insert(1, "A", 1);
  }

  @AfterEach
  void close() {
    database.close();
  }

  @Test
  void aDuplicateStreamVersionIsARetryableConflictAndNotACollision() {
    SQLException duplicate = catchThrowableOfType(SQLException.class, () -> insert(2, "A", 1));

    assertThat((Throwable) duplicate).isInstanceOf(SQLiteException.class);
    assertThat(((SQLiteException) duplicate).getResultCode())
        .isEqualTo(SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE);
    assertThat(classifier.isRetryableConflict(duplicate)).isTrue();
    assertThat(classifier.isGlobalOffsetCollision(duplicate)).isFalse();
    assertThat(classifier.lockFailure(duplicate, "append to stream 'order-A'")).isNull();
  }

  @Test
  void aDuplicateGlobalOffsetIsACollisionAndNotRetryable() {
    SQLException duplicate = catchThrowableOfType(SQLException.class, () -> insert(1, "B", 1));

    assertThat((Throwable) duplicate).isInstanceOf(SQLiteException.class);
    assertThat(((SQLiteException) duplicate).getResultCode())
        .isEqualTo(SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY);
    assertThat(classifier.isGlobalOffsetCollision(duplicate)).isTrue();
    assertThat(classifier.isRetryableConflict(duplicate)).isFalse();
    assertThat(classifier.lockFailure(duplicate, "append to stream 'order-B'")).isNull();
  }

  /** Another connection holds the write lock; one whose busy_timeout is a millisecond gives up. */
  @Test
  void aWriteLockNotGrantedWithinBusyTimeoutIsTheStoresLockFailure() throws SQLException {
    SQLException busy;
    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement hold = holder.createStatement()) {
      hold.execute("BEGIN IMMEDIATE");
      try (Connection impatient =
          TestDatabases.dataSource(database.file(), "busy_timeout=1").getConnection()) {
        busy = catchThrowableOfType(SQLException.class, () -> insert(impatient, 2, "B", 1));
      } finally {
        hold.execute("ROLLBACK");
      }
    }

    assertThat((Throwable) busy).isInstanceOf(SQLiteException.class);
    assertThat(classifier.isRetryableConflict(busy)).isFalse();
    assertThat(classifier.isGlobalOffsetCollision(busy)).isFalse();
    EventStoreException failure = classifier.lockFailure(busy, "append to stream 'order-B'");
    assertThat(failure)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "SQLite's write lock was not granted within the connection's busy_timeout, so the"
                + " append to stream 'order-B' stored nothing. Most likely an application"
                + " transaction on the same database file holds the write lock: StreamRune's"
                + " stores never join an application's transaction (for example a Spring"
                + " @Transactional method on the same DataSource), so call them outside it."
                + " Otherwise another connection of this process held the lock that long: a"
                + " schema migration or a stalled write. Retry later, or raise busy_timeout.")
        .hasCause(busy);
  }

  @Test
  void theDesyncMessageNamesTheCounterAndHowToReseedIt() {
    assertThat(classifier.globalOffsetDesyncMessage("order-A"))
        .isEqualTo(
            "global_offset collision while appending to stream 'order-A':"
                + " global_offset_counter.next_value has fallen at or below max(global_offset), so"
                + " this append reserved offsets that are already stored. This is NOT a version"
                + " conflict and retrying cannot resolve it: the reservation rolls back with the"
                + " append, so every attempt reserves the same used offsets. Re-seed the counter"
                + " with: UPDATE global_offset_counter SET next_value = (SELECT"
                + " COALESCE(max(global_offset), 0) FROM event_stream) WHERE id = 1");
  }

  private void insert(long offset, String aggregateId, long version) throws SQLException {
    try (Connection conn = database.dataSource().getConnection()) {
      insert(conn, offset, aggregateId, version);
    }
  }

  private static void insert(Connection conn, long offset, String aggregateId, long version)
      throws SQLException {
    try (var ps = conn.prepareStatement(INSERT_EVENT)) {
      ps.setLong(1, offset);
      ps.setString(2, aggregateId);
      ps.setLong(3, version);
      ps.executeUpdate();
    }
  }
}

package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.test.CommandInboxContract;
import org.streamrune.test.EventStoreContract.CrashPoint;

/**
 * The shared command-inbox contract against {@code command_inbox}, written by the keyed append of a
 * {@link SqliteEventStore} over the same database handle, a fresh file for each pair, its keyed
 * appends killed by {@link SqliteCrashPoints}.
 */
class SqliteCommandInboxContractTest extends CommandInboxContract {

  private static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder().registerEvent(EVENT_TYPE, InboxEvent.class).build();

  @TempDir Path dir;

  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private final List<SqliteDatabase> handles = new ArrayList<>();
  private Path file;
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected Inboxed newStore() {
    crashPoints.disarm();
    file = dir.resolve("inbox-" + files++ + ".db");
    return build();
  }

  /** A pair over {@link #file}, through a handle of its own whose connections can be killed. */
  private Inboxed build() {
    SqliteDatabase database =
        SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(file)));
    handles.add(database);
    var inbox = new SqliteCommandInbox(database);
    var events = new SqliteEventStoreFactory(database, REGISTRY).commandInbox(inbox).create();
    return new Inboxed(events, inbox);
  }

  @Override
  protected boolean supportsCrashInjection() {
    return true;
  }

  @Override
  protected void crashAt(Inboxed inboxed, CrashPoint point) {
    crashPoints.arm(point);
  }

  /** A new pair over the same file; nothing is emptied. */
  @Override
  protected Inboxed reopen(Inboxed inboxed) {
    return build();
  }

  @Override
  protected void setProcessedAt(Inboxed inboxed, IdempotencyKey key, Instant processedAt) {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "UPDATE command_inbox SET processed_at = ? WHERE idempotency_key = ?")) {
      ps.setLong(1, SqliteTimestamps.toMicros(processedAt));
      ps.setString(2, key.value());
      if (ps.executeUpdate() != 1) {
        throw new IllegalStateException("no row is recorded under " + key);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}

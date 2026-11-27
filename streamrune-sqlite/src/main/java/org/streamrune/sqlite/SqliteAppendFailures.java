package org.streamrune.sqlite;

import java.sql.SQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.runtime.jdbc.AppendFailureClassifier;
import org.streamrune.runtime.jdbc.EventAppendSupport;

/**
 * How {@link SqliteEventStore} classifies the failure of an append that did not commit, for {@link
 * EventAppendSupport#appendConflict} and {@link EventAppendSupport#keyedAppendFailure}: a collision
 * on {@code global_offset} ({@code SQLITE_CONSTRAINT_PRIMARYKEY} naming it) is a desynced counter,
 * a unique violation or a stale snapshot a version conflict, and a write lock not granted within
 * {@code busy_timeout} ({@code SQLITE_BUSY}) or refused with {@code SQLITE_LOCKED} an
 * infrastructure failure that says so. Each answer is the event store's own rule; this class only
 * hands them to the shared mapping.
 */
final class SqliteAppendFailures implements AppendFailureClassifier {

  /** The one instance: the classifier holds no state. */
  static final SqliteAppendFailures INSTANCE = new SqliteAppendFailures();

  private SqliteAppendFailures() {}

  /** {@link SqliteEventStore#isGlobalOffsetCollision}. */
  @Override
  public boolean isGlobalOffsetCollision(SQLException e) {
    return SqliteEventStore.isGlobalOffsetCollision(e);
  }

  /** {@link SqliteEventStore#isRetryableConflict}. */
  @Override
  public boolean isRetryableConflict(SQLException e) {
    return SqliteEventStore.isRetryableConflict(e);
  }

  /** {@link SqliteEventStore#lockFailure}. */
  @Override
  public EventStoreException lockFailure(SQLException e, String what) {
    return SqliteEventStore.lockFailure(e, what);
  }

  /** {@link SqliteEventStore#globalOffsetDesyncMessage}: names {@code global_offset_counter}. */
  @Override
  public String globalOffsetDesyncMessage(String shownStream) {
    return SqliteEventStore.globalOffsetDesyncMessage(shownStream);
  }
}

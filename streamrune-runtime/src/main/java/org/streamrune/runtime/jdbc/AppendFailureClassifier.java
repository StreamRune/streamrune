package org.streamrune.runtime.jdbc;

import java.sql.SQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Internal;

/**
 * What an event store's backend knows about a failed append and the shared mapping does not: which
 * driver failures are a collision on {@code global_offset}, which are a conflict a retry resolves,
 * which are a write lock the database did not grant, and how the desynced offset counter is named
 * and re-seeded. {@link EventAppendSupport#appendConflict} and {@link
 * EventAppendSupport#keyedAppendFailure} ask it, in that order, and never branch on the backend
 * themselves.
 *
 * <p>Each backend's event store implements it once, over its own driver's error codes; the
 * classification of a failure is the backend's alone.
 *
 * @hidden
 */
@Internal
public interface AppendFailureClassifier {

  /**
   * Whether {@code e} is a positively identified collision on the {@code global_offset} key: the
   * offset counter has fallen at or below a stored offset. No retry resolves it.
   *
   * @param e the failure of an append that did not commit
   * @return {@code true} for a collision on {@code global_offset}
   */
  boolean isGlobalOffsetCollision(SQLException e);

  /**
   * Whether {@code e} is a conflict a retry can resolve: a version conflict on the stream, or a
   * transaction aborted by a concurrent append.
   *
   * @param e the failure of an append that did not commit
   * @return {@code true} for a retryable conflict
   */
  boolean isRetryableConflict(SQLException e);

  /**
   * The exception of a write the database refused for a lock, naming {@code what}, with {@code e}
   * as its cause; {@code null} when {@code e} is not a lock failure. A backend whose lock failures
   * the shared mapping does not single out (PostgreSQL) always returns {@code null}.
   *
   * @param e the failure of an append that did not commit
   * @param what the write that failed, for the message
   * @return the exception to throw, or {@code null}
   */
  EventStoreException lockFailure(SQLException e, String what);

  /**
   * The operator-facing message of a collision on {@code global_offset}: it names the desynced
   * counter and the statement that re-seeds it.
   *
   * @param shownStream the stream being appended to, already sanitized for a message
   * @return the message
   */
  String globalOffsetDesyncMessage(String shownStream);
}

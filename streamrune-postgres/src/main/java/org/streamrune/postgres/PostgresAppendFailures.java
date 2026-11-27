package org.streamrune.postgres;

import java.sql.SQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.runtime.jdbc.AppendFailureClassifier;
import org.streamrune.runtime.jdbc.EventAppendSupport;

/**
 * What PostgreSQL knows about a failed append, for {@link EventAppendSupport#appendConflict} and
 * {@link EventAppendSupport#keyedAppendFailure}: the classification stays {@link
 * PostgresEventStore}'s own ({@link PostgresEventStore#isGlobalOffsetCollision}, {@link
 * PostgresEventStore#isRetryableConflict} and its desync message); this class only hands it to the
 * shared mapping. A lock PostgreSQL did not grant is not singled out: it surfaces as the store's
 * plain append failure, so {@link #lockFailure} is always {@code null}.
 */
final class PostgresAppendFailures implements AppendFailureClassifier {

  /** The one instance; it has no state. */
  static final PostgresAppendFailures INSTANCE = new PostgresAppendFailures();

  private PostgresAppendFailures() {}

  @Override
  public boolean isGlobalOffsetCollision(SQLException e) {
    return PostgresEventStore.isGlobalOffsetCollision(e);
  }

  @Override
  public boolean isRetryableConflict(SQLException e) {
    return PostgresEventStore.isRetryableConflict(e);
  }

  /** Always {@code null}: PostgreSQL's lock failures take the store's plain failure path. */
  @Override
  public EventStoreException lockFailure(SQLException e, String what) {
    return null;
  }

  @Override
  public String globalOffsetDesyncMessage(String shownStream) {
    return PostgresEventStore.globalOffsetDesyncMessage(shownStream);
  }
}

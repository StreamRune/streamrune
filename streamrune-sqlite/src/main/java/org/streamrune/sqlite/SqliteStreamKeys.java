package org.streamrune.sqlite;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.streamrune.core.types.StreamId;

/**
 * The one place the stream key's column pair {@code (aggregate_type, aggregate_id)} is bound. The
 * same columns and the same order as on Postgres.
 */
final class SqliteStreamKeys {

  private SqliteStreamKeys() {}

  /** Binds {@code aggregate_type} at {@code firstIndex} and {@code aggregate_id} after it. */
  static void bind(PreparedStatement ps, int firstIndex, StreamId streamId) throws SQLException {
    ps.setString(firstIndex, streamId.aggregateType().value());
    ps.setString(firstIndex + 1, streamId.aggregateId().value());
  }
}

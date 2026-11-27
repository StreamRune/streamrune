package org.streamrune.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * The stream key's column pair {@code (aggregate_type, aggregate_id)} binding and reading, kept as
 * a test-only delegate to {@link StreamKeys}, which holds the one copy; production code calls
 * {@link StreamKeys} directly.
 */
final class PostgresStreamKeys {

  private PostgresStreamKeys() {}

  /** Test-only delegate to {@link StreamKeys#bind}. */
  static void bind(PreparedStatement ps, int firstIndex, StreamId streamId) throws SQLException {
    StreamKeys.bind(ps, firstIndex, streamId);
  }

  /** Test-only delegate to {@link StreamKeys#read}. */
  static StreamId read(ResultSet rs) throws SQLException {
    return StreamKeys.read(rs);
  }
}

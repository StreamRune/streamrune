package org.streamrune.sqlite;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * Binds the stream key for the tests that seed rows by hand. Production code binds and reads the
 * stream key's column pair through {@link StreamKeys}.
 */
final class SqliteStreamKeys {

  private SqliteStreamKeys() {}

  /** {@link StreamKeys#bind}: a test-only delegate. */
  static void bind(PreparedStatement ps, int firstIndex, StreamId streamId) throws SQLException {
    StreamKeys.bind(ps, firstIndex, streamId);
  }
}

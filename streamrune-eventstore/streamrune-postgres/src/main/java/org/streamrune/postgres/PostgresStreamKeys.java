package org.streamrune.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

/**
 * The one place the stream key's column pair {@code (aggregate_type, aggregate_id)} is bound and
 * read. Stored values are rebuilt through the lenient constructors — the decode door: a stored row
 * must never become unreadable over a rule that applies at ingress.
 */
final class PostgresStreamKeys {

  private PostgresStreamKeys() {}

  /**
   * Binds {@code aggregate_type} at {@code firstIndex} and {@code aggregate_id} at {@code
   * firstIndex + 1}; both NULL for a null stream.
   */
  static void bind(PreparedStatement ps, int firstIndex, StreamId streamId) throws SQLException {
    if (streamId == null) {
      ps.setNull(firstIndex, Types.VARCHAR);
      ps.setNull(firstIndex + 1, Types.VARCHAR);
      return;
    }
    ps.setString(firstIndex, streamId.aggregateType().value());
    ps.setString(firstIndex + 1, streamId.aggregateId().value());
  }

  /**
   * The pair from the current row's {@code aggregate_type}/{@code aggregate_id} columns (lenient
   * constructors — stored values); {@code null} when {@code aggregate_id} is NULL.
   */
  static StreamId read(ResultSet rs) throws SQLException {
    String aggregateId = rs.getString("aggregate_id");
    if (aggregateId == null) {
      return null;
    }
    return StreamId.of(
        new AggregateType(rs.getString("aggregate_type")), new AggregateId(aggregateId));
  }
}

package org.streamrune.runtime.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.streamrune.core.Internal;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

/**
 * The one place the stream key's column pair {@code (aggregate_type, aggregate_id)} is bound and
 * read, the same columns in the same order on every backend. Stored values are rebuilt through the
 * lenient constructors — the decode door: a stored row must never become unreadable over a rule
 * that applies at ingress.
 *
 * <p>Works on the caller's statement or result set; lets every {@link SQLException} propagate
 * unwrapped.
 *
 * @hidden
 */
@Internal
public final class StreamKeys {

  private StreamKeys() {}

  /**
   * Binds {@code aggregate_type} at {@code firstIndex} and {@code aggregate_id} at {@code
   * firstIndex + 1}; both NULL for a null stream (an outbox entry without one).
   *
   * @param ps the caller's statement
   * @param firstIndex the parameter index of {@code aggregate_type}
   * @param streamId the stream, or {@code null}
   * @throws SQLException if a bind fails
   */
  public static void bind(PreparedStatement ps, int firstIndex, StreamId streamId)
      throws SQLException {
    if (streamId == null) {
      ps.setNull(firstIndex, Types.VARCHAR);
      ps.setNull(firstIndex + 1, Types.VARCHAR);
      return;
    }
    ps.setString(firstIndex, streamId.aggregateType().value());
    ps.setString(firstIndex + 1, streamId.aggregateId().value());
  }

  /**
   * The pair from the current row's {@code aggregate_type}/{@code aggregate_id} columns, read by
   * name (lenient constructors — stored values); {@code null} when {@code aggregate_id} is NULL.
   *
   * @param rs the caller's result set, positioned on a row
   * @return the stream, or {@code null}
   * @throws SQLException if a read fails
   */
  public static StreamId read(ResultSet rs) throws SQLException {
    String aggregateId = rs.getString("aggregate_id");
    if (aggregateId == null) {
      return null;
    }
    return StreamId.of(
        new AggregateType(rs.getString("aggregate_type")), new AggregateId(aggregateId));
  }
}

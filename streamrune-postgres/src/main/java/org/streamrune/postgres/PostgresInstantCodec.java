package org.streamrune.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import org.streamrune.runtime.jdbc.JdbcInstantCodec;

/**
 * The PostgreSQL implementation of {@link JdbcInstantCodec}: a {@code TIMESTAMPTZ} column read
 * through {@link ResultSet#getTimestamp} and bound as a {@link Timestamp}, a {@code BOOLEAN} column
 * read through {@link ResultSet#getBoolean} — exactly the decoding the Postgres stores use. A
 * required read of an unexpected SQL NULL throws {@link NullPointerException}, as {@code
 * getTimestamp(column).toInstant()} does in the stores.
 *
 * <p>Stateless; every method works on the caller's statement or result set and lets {@link
 * SQLException} propagate unwrapped.
 */
final class PostgresInstantCodec implements JdbcInstantCodec {

  /** The one instance. */
  static final PostgresInstantCodec INSTANCE = new PostgresInstantCodec();

  private PostgresInstantCodec() {}

  @Override
  public Instant readInstant(ResultSet rs, String column) throws SQLException {
    return rs.getTimestamp(column).toInstant();
  }

  @Override
  public Instant readNullableInstant(ResultSet rs, String column) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(column);
    return timestamp == null ? null : timestamp.toInstant();
  }

  @Override
  public boolean readFlag(ResultSet rs, String column) throws SQLException {
    return rs.getBoolean(column);
  }

  @Override
  public void bindInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
    ps.setTimestamp(index, Timestamp.from(instant));
  }
}

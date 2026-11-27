package org.streamrune.sqlite;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.streamrune.runtime.jdbc.JdbcInstantCodec;

/**
 * SQLite's {@link JdbcInstantCodec}: a timestamp is epoch microseconds in an {@code INTEGER} column
 * ({@link SqliteTimestamps}), a flag an {@code INTEGER} that is true when non-zero.
 *
 * <p>Two instances, because SQLite's stores read their NOT NULL timestamp columns differently: the
 * saga store reads {@code getLong} without a NULL check, so an unexpected SQL NULL decodes as the
 * epoch ({@link #SAGA_ROWS}); the outbox store reads every timestamp column through {@code getLong}
 * and {@code wasNull}, so a SQL NULL is {@code null} ({@link #OUTBOX_ROWS}). Nullable reads, flags
 * and binds are the same on both.
 */
final class SqliteInstantCodec implements JdbcInstantCodec {

  /** The saga store's reads: a required column's SQL NULL decodes as {@link Instant#EPOCH}. */
  static final SqliteInstantCodec SAGA_ROWS = new SqliteInstantCodec(false);

  /** The outbox store's reads: a required column's SQL NULL reads as {@code null}. */
  static final SqliteInstantCodec OUTBOX_ROWS = new SqliteInstantCodec(true);

  private final boolean nullTolerantRequiredRead;

  /**
   * A codec whose {@link #readInstant} is {@link #readNullableInstant} when {@code
   * nullTolerantRequiredRead}, otherwise a plain {@code getLong} decode.
   *
   * @param nullTolerantRequiredRead whether a required read maps SQL NULL to {@code null}
   */
  SqliteInstantCodec(boolean nullTolerantRequiredRead) {
    this.nullTolerantRequiredRead = nullTolerantRequiredRead;
  }

  @Override
  public Instant readInstant(ResultSet rs, String column) throws SQLException {
    if (nullTolerantRequiredRead) {
      return readNullableInstant(rs, column);
    }
    return SqliteTimestamps.fromMicros(rs.getLong(column));
  }

  @Override
  public Instant readNullableInstant(ResultSet rs, String column) throws SQLException {
    long micros = rs.getLong(column);
    return rs.wasNull() ? null : SqliteTimestamps.fromMicros(micros);
  }

  @Override
  public boolean readFlag(ResultSet rs, String column) throws SQLException {
    return rs.getInt(column) != 0;
  }

  @Override
  public void bindInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
    ps.setLong(index, SqliteTimestamps.toMicros(instant));
  }
}

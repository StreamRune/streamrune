package org.streamrune.runtime.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import org.streamrune.core.Internal;

/**
 * How one backend stores a timestamp and a flag column: the seam through which shared row mapping
 * reads and binds them without knowing the backend. Each backend implements it with exactly the
 * decoding its stores use today (for example a {@code TIMESTAMPTZ} read through {@link
 * ResultSet#getTimestamp} on PostgreSQL, epoch microseconds in an {@code INTEGER} on SQLite).
 *
 * <p>Each method works on the caller's statement or result set and lets every {@link SQLException}
 * propagate unwrapped. A shared read says, per column, whether the store reads it as required or as
 * nullable today, and calls {@link #readInstant} or {@link #readNullableInstant} accordingly.
 *
 * @hidden
 */
@Internal
public interface JdbcInstantCodec {

  /**
   * Reads a column the store reads as NOT NULL today, decoded exactly as that store does, including
   * what it does on an unexpected SQL NULL (on PostgreSQL a {@link NullPointerException} from
   * {@code getTimestamp(column).toInstant()}).
   *
   * @param rs the caller's result set, positioned on a row
   * @param column the column name
   * @return the instant
   * @throws SQLException if the read fails
   */
  Instant readInstant(ResultSet rs, String column) throws SQLException;

  /**
   * Reads a nullable timestamp column.
   *
   * @param rs the caller's result set, positioned on a row
   * @param column the column name
   * @return the instant, or {@code null} on SQL NULL
   * @throws SQLException if the read fails
   */
  Instant readNullableInstant(ResultSet rs, String column) throws SQLException;

  /**
   * Reads a flag column ({@code getBoolean} on PostgreSQL, {@code getInt(column) != 0} on SQLite).
   *
   * @param rs the caller's result set, positioned on a row
   * @param column the column name
   * @return the flag
   * @throws SQLException if the read fails
   */
  boolean readFlag(ResultSet rs, String column) throws SQLException;

  /**
   * Binds a timestamp parameter as the backend stores it.
   *
   * @param ps the caller's statement
   * @param index the parameter index
   * @param instant the instant to bind
   * @throws SQLException if the bind fails
   */
  void bindInstant(PreparedStatement ps, int index, Instant instant) throws SQLException;
}

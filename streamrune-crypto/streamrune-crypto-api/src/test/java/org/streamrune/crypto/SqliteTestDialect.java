package org.streamrune.crypto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * A test fixture that mirrors the statements of {@code SqliteCryptoDialect} in {@code
 * streamrune-sqlite-crypto}, so the shared JDBC helpers of this module are covered on a SQLite file
 * by this module's own tests, with no Docker. This module cannot depend on {@code
 * streamrune-sqlite-crypto} (that module depends on this one), so the statements are repeated here;
 * the real dialect is tested in its own module against the real database.
 *
 * <p>Timestamps are microseconds since the epoch, stamped with the given clock, as {@code
 * SqliteTimestamps} stores them.
 */
class SqliteTestDialect implements JdbcCryptoDialect {

  private final Clock clock;

  SqliteTestDialect(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void lockSubject(Connection conn, String storedSubjectId) throws SQLException {
    // The write transaction's lock is the lock on SQLite.
  }

  @Override
  public boolean insertKey(
      Connection conn, String keyTable, String storedSubjectId, byte[] keyBytes, int keyVersion)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "INSERT INTO "
                + keyTable
                + " (subject_id, key_bytes, created_at, key_version) VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (subject_id) DO NOTHING")) {
      ps.setString(1, storedSubjectId);
      ps.setBytes(2, keyBytes);
      ps.setLong(3, toMicros(clock.instant()));
      ps.setInt(4, keyVersion);
      return ps.executeUpdate() == 1;
    }
  }

  @Override
  public void recordErasedGeneration(Connection conn, String storedSubjectId, int erasedVersion)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
                + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
                + " max(erased_key_generations.max_erased_version,"
                + " excluded.max_erased_version)")) {
      ps.setString(1, storedSubjectId);
      ps.setInt(2, erasedVersion);
      ps.executeUpdate();
    }
  }

  /** The one tombstone statement: the first forget's instant and evidence stay. */
  @Override
  public void insertTombstone(Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "INSERT INTO forgotten_subjects (subject_id, forgotten_at, key_created_at)"
                + " VALUES (?, ?, ?) ON CONFLICT (subject_id) DO NOTHING")) {
      ps.setString(1, storedSubjectId);
      ps.setLong(2, toMicros(clock.instant()));
      if (keyCreatedAtOrNull == null) {
        ps.setNull(3, Types.INTEGER);
      } else {
        ps.setLong(3, toMicros(keyCreatedAtOrNull));
      }
      ps.executeUpdate();
    }
  }

  @Override
  public Optional<Instant> readInstant(ResultSet rs, String column) throws SQLException {
    long micros = rs.getLong(column);
    return rs.wasNull() ? Optional.empty() : Optional.of(fromMicros(micros));
  }

  static long toMicros(Instant instant) {
    return Math.addExact(
        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
        (instant.getNano() + 500L) / 1_000L);
  }

  static Instant fromMicros(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
  }
}

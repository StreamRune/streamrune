package org.streamrune.crypto.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.crypto.JdbcCryptoDialect;
import org.streamrune.sqlite.SqliteTimestamps;

/**
 * The SQLite statements of the shared JDBC key store and forgotten-subject SQL, used by {@link
 * SqliteCryptoEngine} and {@link SqliteForgottenSubjectStore}: {@code ON CONFLICT} inserts, the
 * {@code max} upsert of the erased generation, and timestamps stored as microseconds since the
 * epoch ({@link SqliteTimestamps}), stamped with the application clock where PostgreSQL takes
 * {@code NOW()}.
 *
 * <p>There is no per-subject lock: every write runs inside a write transaction ({@code BEGIN
 * IMMEDIATE}, or a projection batch's), whose database write lock excludes every other writer until
 * it ends. Each method runs one statement (or none) on the caller's connection and lets every
 * {@link SQLException} propagate; the caller owns the transaction.
 *
 * <p>One instance per engine or store: it carries that instance's clock.
 */
final class SqliteCryptoDialect implements JdbcCryptoDialect {

  private static final String RECORD_ERASED_GENERATION_SQL =
      "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
          + " max(erased_key_generations.max_erased_version,"
          + " excluded.max_erased_version)";

  // The forgotten-subject store's tombstone: the erased key's creation instant is recorded with
  // it (NULL = no evidence); ON CONFLICT DO NOTHING keeps the FIRST forget's instant and evidence.
  private static final String TOMBSTONE_WITH_EVIDENCE_SQL =
      "INSERT INTO forgotten_subjects (subject_id, forgotten_at, key_created_at)"
          + " VALUES (?, ?, ?) ON CONFLICT (subject_id) DO NOTHING";

  // The engine's erasure tombstone. The first erasure's forgotten_at stays: a repeated erasure
  // inserts nothing.
  private static final String ERASE_TOMBSTONE_SQL =
      "INSERT INTO forgotten_subjects (subject_id, forgotten_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";

  private final Clock clock;

  /**
   * @param clock stamps a key's {@code created_at} and a tombstone's {@code forgotten_at}
   */
  SqliteCryptoDialect(Clock clock) {
    this.clock = clock;
  }

  /** Nothing to do: the caller's write transaction holds the database's write lock. */
  @Override
  public void lockSubject(Connection conn, String storedSubjectId) {
    // The write lock taken at BEGIN IMMEDIATE (or held by the joined batch) is the lock.
  }

  /**
   * Inserts the key stamped with the clock. The insert never overwrites an existing key, which
   * would orphan every value encrypted under it.
   */
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
      ps.setLong(3, SqliteTimestamps.toMicros(clock.instant()));
      ps.setInt(4, keyVersion);
      return ps.executeUpdate() == 1;
    }
  }

  @Override
  public void recordErasedGeneration(Connection conn, String storedSubjectId, int erasedVersion)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(RECORD_ERASED_GENERATION_SQL)) {
      ps.setString(1, storedSubjectId);
      ps.setInt(2, erasedVersion);
      ps.executeUpdate();
    }
  }

  /** The store's tombstone without evidence: the three-column insert, binding NULL evidence. */
  @Override
  public void insertTombstone(Connection conn, String storedSubjectId) throws SQLException {
    insertTombstoneWithEvidence(conn, storedSubjectId, null);
  }

  @Override
  public void insertTombstoneWithEvidence(
      Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(TOMBSTONE_WITH_EVIDENCE_SQL)) {
      ps.setString(1, storedSubjectId);
      ps.setLong(2, SqliteTimestamps.toMicros(clock.instant()));
      if (keyCreatedAtOrNull == null) {
        ps.setNull(3, Types.INTEGER);
      } else {
        ps.setLong(3, SqliteTimestamps.toMicros(keyCreatedAtOrNull));
      }
      ps.executeUpdate();
    }
  }

  @Override
  public void insertEraseTombstone(Connection conn, String storedSubjectId) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(ERASE_TOMBSTONE_SQL)) {
      ps.setString(1, storedSubjectId);
      ps.setLong(2, SqliteTimestamps.toMicros(clock.instant()));
      ps.executeUpdate();
    }
  }

  @Override
  public Optional<Instant> readInstant(ResultSet rs, String column) throws SQLException {
    long micros = rs.getLong(column);
    return rs.wasNull() ? Optional.empty() : Optional.of(SqliteTimestamps.fromMicros(micros));
  }
}

package org.streamrune.crypto.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.crypto.JdbcCryptoDialect;

/**
 * The PostgreSQL statements of the shared JDBC key store and forgotten-subject SQL, used by {@link
 * PostgresCryptoEngine} and {@link JdbcForgottenSubjectStore}: a transaction-scoped advisory lock
 * per subject, {@code ON CONFLICT} inserts and the {@code GREATEST} upsert of the erased
 * generation.
 *
 * <p>Each method runs one statement on the caller's connection and lets every {@link SQLException}
 * propagate; the caller owns the transaction. {@link #lockSubject} holds its lock only until the
 * caller's transaction ends, so the caller runs it with autoCommit off.
 */
final class PostgresCryptoDialect implements JdbcCryptoDialect {

  /** The one instance: the dialect holds no state. */
  static final PostgresCryptoDialect INSTANCE = new PostgresCryptoDialect();

  private static final String LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(?))";

  private static final String RECORD_ERASED_GENERATION_SQL =
      "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?) "
          + "ON CONFLICT (subject_id) DO UPDATE SET max_erased_version = "
          + "GREATEST(erased_key_generations.max_erased_version, EXCLUDED.max_erased_version)";

  private static final String TOMBSTONE_SQL =
      "INSERT INTO forgotten_subjects (subject_id) VALUES (?) ON CONFLICT (subject_id) DO NOTHING";

  // The erased key's backend-sourced creation instant, recorded with the tombstone. Nullable
  // (NULL = no evidence); ON CONFLICT DO NOTHING keeps the FIRST erasure's evidence, matching
  // forgotten_at's first-write-wins.
  private static final String FORGET_WITH_KEY_EVIDENCE_SQL =
      "INSERT INTO forgotten_subjects (subject_id, key_created_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";

  private PostgresCryptoDialect() {}

  /**
   * Takes a transaction-scoped advisory lock keyed by a hash of the stored subject id. The lock is
   * per subject, not global: other subjects take other lock keys. It is released when the caller's
   * transaction commits or rolls back.
   */
  @Override
  public void lockSubject(Connection conn, String storedSubjectId) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(LOCK_SQL)) {
      ps.setString(1, storedSubjectId);
      // executeQuery(), not execute(): pg_advisory_xact_lock() returns a single row (its result
      // column is irrelevant — only the call's side effect, taking the lock, matters). The row
      // still has to be drained for the driver to consider the statement complete.
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }

  @Override
  public boolean insertKey(
      Connection conn, String keyTable, String storedSubjectId, byte[] keyBytes, int keyVersion)
      throws SQLException {
    // First writer wins: a concurrent create must never overwrite an existing key — that would
    // permanently orphan all data already encrypted under the old key.
    String sql =
        "INSERT INTO "
            + keyTable
            + " (subject_id, key_bytes, key_version) VALUES (?, ?, ?) "
            + "ON CONFLICT (subject_id) DO NOTHING";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId);
      ps.setBytes(2, keyBytes);
      ps.setInt(3, keyVersion);
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

  @Override
  public void insertTombstone(Connection conn, String storedSubjectId) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(TOMBSTONE_SQL)) {
      ps.setString(1, storedSubjectId);
      ps.executeUpdate();
    }
  }

  @Override
  public void insertTombstoneWithEvidence(
      Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(FORGET_WITH_KEY_EVIDENCE_SQL)) {
      ps.setString(1, storedSubjectId);
      ps.setTimestamp(2, keyCreatedAtOrNull == null ? null : Timestamp.from(keyCreatedAtOrNull));
      ps.executeUpdate();
    }
  }

  /** The engine's erasure writes the same tombstone as a plain forget. */
  @Override
  public void insertEraseTombstone(Connection conn, String storedSubjectId) throws SQLException {
    insertTombstone(conn, storedSubjectId);
  }

  @Override
  public Optional<Instant> readInstant(ResultSet rs, String column) throws SQLException {
    Timestamp ts = rs.getTimestamp(column);
    return Optional.ofNullable(ts).map(Timestamp::toInstant);
  }
}

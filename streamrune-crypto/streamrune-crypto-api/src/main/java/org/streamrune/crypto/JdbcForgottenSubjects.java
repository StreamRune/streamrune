package org.streamrune.crypto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.core.Internal;

/**
 * The forgotten-subject SQL over the {@code forgotten_subjects} table, run on a caller's {@link
 * Connection}: the tombstone a GDPR erasure leaves, which makes a later encrypt of the subject fail
 * instead of minting a fresh key. The tombstone inserts and the timestamp reads go through the
 * {@link JdbcCryptoDialect}; the existence check, the delete and the timestamp selects are the same
 * on every backend.
 *
 * <p><b>Contract.</b>
 *
 * <ul>
 *   <li>The caller owns the transaction: this class never opens, commits, rolls back or closes a
 *       connection.
 *   <li>Every {@link SQLException} propagates unwrapped, so the caller's own exception mapping (and
 *       a SQLite projection batch's abort) sees it as it was thrown.
 *   <li>It takes no lock. When {@link JdbcSubjectKeyStore} calls it, the key store has already
 *       taken the subject's lock on the same connection.
 * </ul>
 *
 * <p>Subject ids arrive already hashed: each caller hashes with its own failure message.
 *
 * @hidden
 */
@Internal
public final class JdbcForgottenSubjects {

  private final JdbcCryptoDialect dialect;

  /**
   * Forgotten-subject SQL whose backend-specific statements go through {@code dialect}.
   *
   * @param dialect the backend's statements
   * @throws IllegalArgumentException if {@code dialect} is {@code null}
   */
  public JdbcForgottenSubjects(JdbcCryptoDialect dialect) {
    if (dialect == null) {
      throw new IllegalArgumentException("dialect must not be null");
    }
    this.dialect = dialect;
  }

  /**
   * Tombstones the subject without erased-key evidence. A repeated forget changes nothing.
   *
   * @param c the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the insert fails
   */
  public void forget(Connection c, String storedSubjectId) throws SQLException {
    dialect.insertTombstone(c, storedSubjectId);
  }

  /**
   * Tombstones the subject with the erased key's creation instant ({@code null}: no evidence). A
   * repeated forget changes nothing: the first forget's instant and evidence stay.
   *
   * @param c the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @param keyCreatedAtOrNull the erased key's creation instant, or {@code null}
   * @throws SQLException if the insert fails
   */
  public void forget(Connection c, String storedSubjectId, Instant keyCreatedAtOrNull)
      throws SQLException {
    dialect.insertTombstoneWithEvidence(c, storedSubjectId, keyCreatedAtOrNull);
  }

  /**
   * Tombstones the subject with a crypto engine's erasure statement. A repeated erasure changes
   * nothing.
   *
   * @param c the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the insert fails
   */
  public void forgetForErase(Connection c, String storedSubjectId) throws SQLException {
    dialect.insertEraseTombstone(c, storedSubjectId);
  }

  /**
   * Whether the subject has a tombstone.
   *
   * @param c the caller's connection
   * @param storedSubjectId the hashed subject id
   * @return {@code true} if the subject is tombstoned
   * @throws SQLException if the select fails
   */
  public boolean isForgotten(Connection c, String storedSubjectId) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT 1 FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Deletes the subject's tombstone; a subject without one is left as it is.
   *
   * @param c the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the delete fails
   */
  public void reinstate(Connection c, String storedSubjectId) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("DELETE FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId);
      ps.executeUpdate();
    }
  }

  /**
   * The tombstone's first-forget instant.
   *
   * @param c the caller's connection
   * @param storedSubjectId the hashed subject id
   * @return the instant, or empty when the subject is not tombstoned
   * @throws SQLException if the select fails
   */
  public Optional<Instant> forgottenAt(Connection c, String storedSubjectId) throws SQLException {
    return instant(c, storedSubjectId, "forgotten_at");
  }

  /**
   * The erased key's creation instant recorded with the tombstone.
   *
   * @param c the caller's connection
   * @param storedSubjectId the hashed subject id
   * @return the instant, or empty when the subject is not tombstoned or the tombstone carries no
   *     evidence
   * @throws SQLException if the select fails: a storage failure never reads as "no evidence"
   */
  public Optional<Instant> keyCreatedAt(Connection c, String storedSubjectId) throws SQLException {
    return instant(c, storedSubjectId, "key_created_at");
  }

  /** The instant in {@code column} of the subject's tombstone, or empty (no row, or NULL). */
  private Optional<Instant> instant(Connection c, String storedSubjectId, String column)
      throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT " + column + " FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        return dialect.readInstant(rs, column);
      }
    }
  }
}

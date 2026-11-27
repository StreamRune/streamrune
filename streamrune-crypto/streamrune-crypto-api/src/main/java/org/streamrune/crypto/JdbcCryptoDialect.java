package org.streamrune.crypto;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import org.streamrune.core.Internal;

/**
 * The statements of the crypto key store and the forgotten-subject store that differ between
 * database backends: the per-subject lock, the key insert, the erased-generation upsert, the
 * tombstone inserts and the timestamp read. {@link JdbcSubjectKeyStore} and {@link
 * JdbcForgottenSubjects} run everything else themselves, the same on every backend.
 *
 * <p>Each method runs one statement (or none) on the caller's connection, inside the caller's
 * transaction. It never opens, commits, rolls back or closes the connection, and lets every {@link
 * SQLException} propagate unwrapped. Subject ids arrive already hashed ({@link
 * JdbcSubjectKeyStore#storedSubjectId}).
 *
 * @hidden
 */
@Internal
public interface JdbcCryptoDialect {

  /**
   * Takes the per-subject lock that serializes a mint, an erasure and a reinstate of the subject
   * until the caller's transaction ends. A backend whose write transaction already excludes every
   * other writer does nothing here.
   *
   * @param conn the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the lock cannot be taken
   */
  void lockSubject(Connection conn, String storedSubjectId) throws SQLException;

  /**
   * Inserts a key row unless the subject already has one; an existing key is never overwritten.
   *
   * @param conn the caller's connection, inside its transaction
   * @param keyTable the validated key-table name
   * @param storedSubjectId the hashed subject id
   * @param keyBytes the encoded key; the caller zeroes it afterwards
   * @param keyVersion the key's generation
   * @return {@code true} if the row was inserted, {@code false} if the subject already had a key
   * @throws SQLException if the insert fails
   */
  boolean insertKey(
      Connection conn, String keyTable, String storedSubjectId, byte[] keyBytes, int keyVersion)
      throws SQLException;

  /**
   * Records that generation {@code erasedVersion} of the subject's key was destroyed, keeping the
   * highest recorded generation, so re-running an erasure is idempotent and generations stay
   * monotonic.
   *
   * @param conn the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @param erasedVersion the destroyed key's generation
   * @throws SQLException if the upsert fails
   */
  void recordErasedGeneration(Connection conn, String storedSubjectId, int erasedVersion)
      throws SQLException;

  /**
   * Inserts the forgotten-subject store's tombstone without erased-key evidence; an existing
   * tombstone stays as it is.
   *
   * @param conn the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the insert fails
   */
  void insertTombstone(Connection conn, String storedSubjectId) throws SQLException;

  /**
   * Inserts the forgotten-subject store's tombstone with the erased key's creation instant, or with
   * no evidence when it is {@code null}; an existing tombstone stays as it is, evidence included.
   *
   * @param conn the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @param keyCreatedAtOrNull the erased key's creation instant, or {@code null}
   * @throws SQLException if the insert fails
   */
  void insertTombstoneWithEvidence(
      Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull) throws SQLException;

  /**
   * Inserts the tombstone of a crypto engine's erasure; an existing tombstone stays as it is.
   *
   * @param conn the caller's connection, inside its transaction
   * @param storedSubjectId the hashed subject id
   * @throws SQLException if the insert fails
   */
  void insertEraseTombstone(Connection conn, String storedSubjectId) throws SQLException;

  /**
   * Reads the timestamp in {@code column} of the current row of {@code rs}.
   *
   * @param rs a result set positioned on a row
   * @param column the column's label
   * @return the instant, or empty when the column is SQL {@code NULL}
   * @throws SQLException if the column cannot be read
   */
  Optional<Instant> readInstant(ResultSet rs, String column) throws SQLException;
}

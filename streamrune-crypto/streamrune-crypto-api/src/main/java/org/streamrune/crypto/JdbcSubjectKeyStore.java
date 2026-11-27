package org.streamrune.crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Set;
import java.util.regex.Pattern;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.streamrune.core.Internal;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * The per-subject key store of the JDBC crypto engines, run on a caller's {@link Connection}: one
 * AES-256 key per subject in the key table, its generation, the erased generations in {@code
 * erased_key_generations} and the erasure tombstones in {@code forgotten_subjects} (through {@link
 * JdbcForgottenSubjects}). Statements that differ between backends go through the {@link
 * JdbcCryptoDialect}; everything else is the same SQL on every backend.
 *
 * <p><b>Contract.</b>
 *
 * <ul>
 *   <li>The caller owns the transaction: this class never opens, commits, rolls back or closes a
 *       connection. A method that takes the subject lock ({@link #loadOrCreateKey}, {@link
 *       #eraseKey}, {@link #liftTombstone}) relies on the caller's transaction to hold it: on
 *       PostgreSQL the connection must have autoCommit off, or the transaction-scoped advisory lock
 *       is released when its own statement ends; on SQLite the caller is inside its write
 *       transaction, whose lock is the lock.
 *   <li>Every {@link SQLException} propagates unwrapped: the engines' exception mapping and a
 *       SQLite projection batch's abort key on it.
 *   <li>The key-table name is concatenated into SQL, so the constructor validates it again as a
 *       plain SQL identifier ({@code [A-Za-z_][A-Za-z0-9_]*}), whatever the engine checked before.
 * </ul>
 *
 * <p><b>Generations.</b> A subject's first key is generation {@code 1}. An erasure records the
 * generation it destroys, and the next mint uses the generation above the highest recorded one, up
 * to {@link AesGcmV1Format#MAX_KEY_VERSION}; a mint beyond that fails instead of wrapping onto a
 * destroyed generation.
 *
 * <p><b>Race hook.</b> The {@code raceHook} runs inside {@link #loadOrCreateKey}, under the subject
 * lock, after the tombstone check and before a key is minted. Production passes a no-op or an
 * indirection to a test seam; a test uses it to force a concurrent erasure into that window.
 *
 * <p>Key material is zeroed where this class handles it: the key bytes read from the driver once
 * the {@link SecretKeySpec} has copied them, and the encoded key of a mint once its insert ran.
 *
 * @hidden
 */
@Internal
public final class JdbcSubjectKeyStore {

  private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private final String keyTable;
  private final JdbcCryptoDialect dialect;
  private final JdbcForgottenSubjects forgotten;
  private final Runnable raceHook;

  /**
   * A key store over {@code keyTable}.
   *
   * @param keyTable the key-table name, a plain SQL identifier
   * @param dialect the backend's statements
   * @param forgotten the forgotten-subject SQL over the same backend
   * @param raceHook run under the subject lock between the tombstone check and a mint
   * @throws IllegalArgumentException if an argument is {@code null} or {@code keyTable} is not a
   *     plain SQL identifier
   */
  public JdbcSubjectKeyStore(
      String keyTable,
      JdbcCryptoDialect dialect,
      JdbcForgottenSubjects forgotten,
      Runnable raceHook) {
    if (keyTable == null) {
      throw new IllegalArgumentException("keyTable must not be null");
    }
    if (!SAFE_IDENTIFIER.matcher(keyTable).matches()) {
      throw new IllegalArgumentException(
          "Invalid key-table name '"
              + keyTable
              + "': must be a plain SQL identifier matching [A-Za-z_][A-Za-z0-9_]*");
    }
    if (dialect == null) {
      throw new IllegalArgumentException("dialect must not be null");
    }
    if (forgotten == null) {
      throw new IllegalArgumentException("forgotten must not be null");
    }
    if (raceHook == null) {
      throw new IllegalArgumentException("raceHook must not be null");
    }
    this.keyTable = keyTable;
    this.dialect = dialect;
    this.forgotten = forgotten;
    this.raceHook = raceHook;
  }

  /**
   * The value written to the {@code subject_id} columns: the SHA-256 hex of the subject id's UTF-8
   * bytes, so a subject id that is itself personal data never lands verbatim in a stored column. It
   * does not reuse {@link SubjectId#redacted()}, although the hash is the same: that method falls
   * back to a fixed marker, which as a storage key would merge every subject's rows. A hashing
   * failure throws instead.
   *
   * @param subjectId the subject
   * @return the hex SHA-256 of the subject id
   * @throws CryptoOperationException if hashing fails
   */
  public static String storedSubjectId(SubjectId subjectId) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      return HexFormat.of()
          .formatHex(md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8)));
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException(
          "SHA-256 hashing failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * The subject's key, read without the subject lock.
   *
   * @param c the caller's connection
   * @param s the subject
   * @return the key and its generation, or {@code null} when the subject has no key
   * @throws SQLException if the select fails
   */
  public VersionedKey loadKeyOrNull(Connection c, SubjectId s) throws SQLException {
    return loadStoredKeyOrNull(c, storedSubjectId(s));
  }

  /**
   * The subject's key, minted when it has none. Under the subject lock it loads the key, then
   * checks the tombstone, runs the race hook and either refuses a tombstoned subject or mints a key
   * of the next generation. A mint whose insert finds another writer's key returns that key, so
   * both writers use the same one.
   *
   * @param c the caller's connection, inside its transaction (autoCommit off on PostgreSQL)
   * @param s the subject
   * @return the subject's key and its generation
   * @throws SubjectForgottenException if the subject has no key and is tombstoned
   * @throws CryptoOperationException if the generation counter is exhausted, key generation fails,
   *     or the key that won an insert conflict is gone
   * @throws SQLException if a statement fails
   */
  public VersionedKey loadOrCreateKey(Connection c, SubjectId s) throws SQLException {
    String subject = storedSubjectId(s);
    dialect.lockSubject(c, subject);
    VersionedKey key = loadStoredKeyOrNull(c, subject);
    if (key != null) {
      return key;
    }
    // No key: either a new subject or an erased one. An erased subject is never given a fresh
    // key; reinstate is the only way to lift the tombstone.
    boolean isForgotten = forgotten.isForgotten(c, subject);
    raceHook.run();
    if (isForgotten) {
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + s.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    return createAndStoreKey(c, s, subject);
  }

  /**
   * Erases the subject's key. Under the subject lock it records the generation of the key it
   * destroys, deletes the key and tombstones the subject without erased-key evidence, all in the
   * caller's transaction, so the three commit together or not at all. A subject without a key is
   * only tombstoned; a rerun changes nothing.
   *
   * @param c the caller's connection, inside its transaction (autoCommit off on PostgreSQL)
   * @param s the subject
   * @throws SQLException if a statement fails
   */
  public void eraseKey(Connection c, SubjectId s) throws SQLException {
    String subject = storedSubjectId(s);
    dialect.lockSubject(c, subject);
    Integer erasedVersion = selectKeyVersion(c, subject);
    if (erasedVersion != null) {
      dialect.recordErasedGeneration(c, subject, erasedVersion);
    }
    try (PreparedStatement ps =
        c.prepareStatement("DELETE FROM " + keyTable + " WHERE subject_id = ?")) {
      ps.setString(1, subject);
      ps.executeUpdate();
    }
    forgotten.forget(c, subject, null);
  }

  /**
   * Deletes the subject's tombstone under the subject lock, so the next mint may run. The erased
   * generations stay: the next key uses the generation above them.
   *
   * @param c the caller's connection, inside its transaction (autoCommit off on PostgreSQL)
   * @param s the subject
   * @throws SQLException if a statement fails
   */
  public void liftTombstone(Connection c, SubjectId s) throws SQLException {
    String subject = storedSubjectId(s);
    dialect.lockSubject(c, subject);
    forgotten.reinstate(c, subject);
  }

  /**
   * Whether the subject has a key.
   *
   * @param c the caller's connection
   * @param s the subject
   * @return {@code true} if the key table has a row for the subject
   * @throws SQLException if the select fails
   */
  public boolean hasKey(Connection c, SubjectId s) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT 1 FROM " + keyTable + " WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId(s));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * The tables this store uses: the key table, {@code forgotten_subjects} and {@code
   * erased_key_generations}.
   *
   * @return the three table names
   */
  public Set<String> requiredTables() {
    return Set.of(keyTable, "forgotten_subjects", "erased_key_generations");
  }

  private VersionedKey loadStoredKeyOrNull(Connection c, String subject) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT key_bytes, key_version FROM " + keyTable + " WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        byte[] bytes = rs.getBytes("key_bytes");
        try {
          // SecretKeySpec copies the array, so the driver's buffer can be zeroed at once.
          return new VersionedKey(new SecretKeySpec(bytes, "AES"), rs.getInt("key_version"));
        } finally {
          Arrays.fill(bytes, (byte) 0);
        }
      }
    }
  }

  /** The current key's generation, or {@code null} when the subject has no key row. */
  private Integer selectKeyVersion(Connection c, String subject) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT key_version FROM " + keyTable + " WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt("key_version") : null;
      }
    }
  }

  /**
   * The generation the next mint must use: one above the highest generation an erasure destroyed
   * ({@code 1} for a subject never erased). Fails at the single-byte cap instead of wrapping onto a
   * destroyed generation, whose ciphertext would then fail the tag check under the new key.
   */
  private static int nextKeyVersion(Connection c, SubjectId s, String subject) throws SQLException {
    int maxErased = 0;
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          maxErased = rs.getInt("max_erased_version");
        }
      }
    }
    int next = maxErased + 1;
    if (next > AesGcmV1Format.MAX_KEY_VERSION) {
      throw new CryptoOperationException(
          "Key-generation limit reached for subject: "
              + s.redacted()
              + " — the subject has been erased and re-registered "
              + (AesGcmV1Format.MAX_KEY_VERSION - 1)
              + " times, exhausting the single-byte generation counter. Re-register this person"
              + " under a fresh subject id.");
    }
    return next;
  }

  /**
   * Generates a key, reads its generation and inserts it. The insert never overwrites an existing
   * key, which would orphan every value encrypted under it: when another writer's key is already
   * there, that key is read back and returned instead.
   */
  private VersionedKey createAndStoreKey(Connection c, SubjectId s, String subject)
      throws SQLException {
    SecretKey key;
    try {
      KeyGenerator generator = KeyGenerator.getInstance("AES");
      generator.init(256);
      key = generator.generateKey();
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException("Key generation failed for subject: " + s.redacted(), e);
    }
    int version = nextKeyVersion(c, s, subject);
    byte[] encoded = key.getEncoded();
    try {
      if (dialect.insertKey(c, keyTable, subject, encoded, version)) {
        return new VersionedKey(key, version);
      }
    } finally {
      // getEncoded() returns a fresh copy: zero it once the insert ran.
      Arrays.fill(encoded, (byte) 0);
    }
    VersionedKey winner = loadStoredKeyOrNull(c, subject);
    if (winner == null) {
      throw new CryptoOperationException(
          "Key for subject vanished after insert conflict: " + s.redacted());
    }
    return winner;
  }
}

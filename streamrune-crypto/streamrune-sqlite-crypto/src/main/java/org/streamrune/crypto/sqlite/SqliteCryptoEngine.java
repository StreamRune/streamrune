package org.streamrune.crypto.sqlite;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteSchemaSeries;
import org.streamrune.sqlite.SqliteTimestamps;

/**
 * SQLite-backed {@link CryptoEngine} storing one AES-256 key per subject in the {@code
 * encryption_keys} table of a SQLite database: the API and semantics of {@code
 * PostgresCryptoEngine}, on one file.
 *
 * <p><b>Ciphertext wire format</b> (versioned for algorithm agility and key generations): {@code
 * [format version (1 byte)][key version (1 byte)][GCM IV (12 bytes)][AES-256-GCM ciphertext+tag]},
 * the format of {@code PostgresCryptoEngine}. The key-version byte carries the <b>generation</b> of
 * the key the value was encrypted under (first mint = {@code 1}; each mint after an erasure
 * increments it). Decrypt compares the value's generation to the current key's: equal proceeds
 * normally (an AES-GCM tag failure stays a loud tamper or corruption error); a LOWER generation
 * ({@code >= 1}) means the value's key was destroyed by a GDPR erasure and yields {@link
 * KeyNotFoundException} (mapped to {@code [REDACTED]} by {@code CryptoShreddingModule}); anything
 * else (an unknown format version, a generation below {@code 1} or above the current key's) fails
 * with the deterministic {@link CryptoMappingException}.
 *
 * <p><b>Verified encryption, retried decryption.</b> Every encryption, its check and every
 * decryption attempt take a cipher of their own. The engine checks each encryption the way decrypt
 * reads it before it returns it; a value that does not decrypt to the plaintext, or a check that
 * fails in any way, is discarded with the transient {@link CryptoOperationException}, so the
 * caller's retry encrypts again. Outside a projection batch, the key a first encryption mints is
 * committed before any cipher work, so the retry loads that key and mints no second one (inside a
 * batch, see below). A decryption whose authentication tag does not match is retried once with a
 * fresh cipher before it fails with {@link CryptoMappingException}. Both events are counted ({@code
 * streamrune.crypto.encrypt_self_check_failed}; {@code streamrune.crypto.decrypt_retried}, tagged
 * {@code RECOVERED} or {@code FAILED}) through {@link Builder#metrics(StreamRuneMetrics)} and
 * logged at {@code WARN}.
 *
 * <p><b>Terminal erasure.</b> {@link #deleteKey(SubjectId)} records the destroyed key's generation
 * in {@code erased_key_generations}, deletes the key row and records a tombstone in {@code
 * forgotten_subjects}, in one transaction. A later {@link #encrypt(SubjectId, byte[])} for a
 * tombstoned subject throws {@link SubjectForgottenException} instead of minting a fresh key. Only
 * {@link #reinstate(SubjectId)}, which deletes the tombstone, lifts it; the generation row is never
 * deleted, so the next mint uses a higher generation and the subject's pre-erasure ciphertext keeps
 * reading as {@code [REDACTED]}. A subject supports at most 126 erase and re-register cycles (the
 * generation byte caps at 127).
 *
 * <p><b>The write lock replaces the per-subject lock.</b> {@code PostgresCryptoEngine} serializes
 * the mint ("load the key, check the tombstone, insert a key") with an erasure and a reinstate of
 * the same subject under a per-subject advisory lock. On SQLite every write transaction holds the
 * database's write lock from its {@code BEGIN IMMEDIATE} to its {@code COMMIT}, so a mint, an
 * erasure and a reinstate never interleave, for any subject: an encrypt either mints before the
 * erasure starts, or reads its tombstone. Encrypting under an existing key, decrypting and {@link
 * #isKeyAvailable} only read, and in WAL mode a read never waits for a writer. A key read just
 * before a concurrent erasure commits encrypts one more value under the destroyed key, which reads
 * as {@code [REDACTED]}, as on PostgreSQL, where the lock is released before the cipher work.
 *
 * <p><b>Inside a projection batch.</b> A projection batch or replay holds the write lock while the
 * application's updater serializes read models, so a key minted for an {@code @Encrypted} field
 * there could not be written on another connection. When the current thread runs such a batch on
 * this engine's database file, the engine's reads and its mint run on the batch's connection,
 * inside its transaction ({@code SqliteWriteTransaction} of {@code streamrune-sqlite}): a key
 * minted there commits with the batch, or rolls back with it together with every value encrypted
 * under it, and a failed statement aborts the batch. So inside a batch a minted key is not
 * committed before the cipher work: when the encryption's check discards the value, the caller's
 * failure rolls the batch back with the key, and the batch's rerun mints the key again, which
 * converges. {@link #deleteKey} and {@link #reinstate} never join: inside such a batch they fail at
 * once with {@link IllegalStateException}, because an erasure that commits or rolls back with the
 * batch is not durable when it returns (a rollback would bring the key back, and a cache or a key
 * backend acting on the return would act on an erasure that may never commit); run them after the
 * batch, from a command handler or a saga step. Inside a transaction of the application's own on
 * the same file, the engine cannot write: a mint waits {@code busy_timeout} and fails, and the
 * message says so.
 *
 * <p><b>Schema.</b> The tables are those of {@link SqliteSchemaSeries#CRYPTO}, which this module
 * ships ({@code db/sqlite-crypto-migration}); {@code SqliteEventStoreFactory} applies it when the
 * engine reports {@link #requiredCryptoTables()}. Timestamps ({@code created_at}, {@code
 * forgotten_at}) are microseconds since the epoch, stamped with the {@linkplain
 * Builder#clock(Clock) application clock}.
 *
 * <p><b>Subject ids are stored hashed, never verbatim:</b> every {@code subject_id} column holds
 * the SHA-256 hex of the subject id, as on PostgreSQL and the filesystem.
 *
 * <p><b>Key material handling is best-effort:</b> intermediate key buffers are zeroed after use,
 * but {@link SecretKeySpec} retains an internal copy on the heap until GC.
 *
 * <p>The engine holds no connection and nothing that needs releasing: it borrows a connection from
 * the database's data source per operation, and keeps working over that data source after the
 * {@link SqliteDatabase} handle it was built from is closed.
 */
public final class SqliteCryptoEngine implements CryptoEngine {

  /** Default key-table name, matching the table the crypto series creates. */
  public static final String DEFAULT_TABLE_NAME = "encryption_keys";

  // Table names are concatenated into SQL (an identifier cannot be a bind parameter), so a
  // configured name must be a plain identifier; names starting with sqlite_ are SQLite's own.
  private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128;
  private static final int GCM_IV_LENGTH = 12;
  private static final byte FORMAT_VERSION = 1;
  private static final int HEADER_LENGTH = 2;

  /** Highest key generation the single-byte header can carry. */
  private static final int MAX_KEY_VERSION = Byte.MAX_VALUE;

  private static final Logger log = LoggerFactory.getLogger(SqliteCryptoEngine.class);

  /** The {@code outcome} of {@code streamrune.crypto.decrypt_retried}. */
  private static final String RETRY_RECOVERED = "RECOVERED";

  private static final String RETRY_FAILED = "FAILED";

  /**
   * Creates the cipher of one operation, initialised for {@code mode} under {@code key}.
   * Package-private so a test can inject a fault; production always uses {@link #FRESH_CIPHERS}.
   */
  @FunctionalInterface
  interface CipherFactory {
    Cipher create(int mode, Key key, AlgorithmParameterSpec params) throws GeneralSecurityException;
  }

  /** A new cipher for every encryption, every check of one and every decryption attempt. */
  static final CipherFactory FRESH_CIPHERS =
      (mode, key, params) -> {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(mode, key, params);
        return cipher;
      };

  private final SqliteCryptoStatements statements;
  private final String tableName;
  private final Clock clock;
  private final SecureRandom random = new SecureRandom();
  private final StreamRuneMetrics metrics;
  private final CipherFactory cipherFactory;

  private SqliteCryptoEngine(Builder builder) {
    this.tableName = validateTableName(builder.tableName);
    this.statements =
        new SqliteCryptoStatements(
            builder.database.dataSource(), builder.database.file(), "crypto key-store write");
    this.clock = builder.clock;
    this.metrics = builder.metrics;
    this.cipherFactory = builder.cipherFactory;
  }

  /**
   * Validates a configured key-table name before it is concatenated into SQL: only names matching
   * {@code [A-Za-z_][A-Za-z0-9_]*} that do not start with {@code sqlite_} (reserved by SQLite) are
   * accepted, folded to lower case as {@code PostgresCryptoEngine} folds them; {@code null} or
   * blank means {@link #DEFAULT_TABLE_NAME}. SQLite matches identifiers without regard to case, so
   * the folded name names the same table.
   */
  private static String validateTableName(String tableName) {
    if (tableName == null || tableName.isBlank()) {
      return DEFAULT_TABLE_NAME;
    }
    if (!SAFE_IDENTIFIER.matcher(tableName).matches()
        || tableName.toLowerCase(Locale.ROOT).startsWith("sqlite_")) {
      throw new IllegalArgumentException(
          "Invalid SQLite crypto key-table name '"
              + tableName
              + "': it must be a plain SQL identifier matching [A-Za-z_][A-Za-z0-9_]* that does"
              + " not start with sqlite_ (schema qualification is not supported).");
    }
    return tableName.toLowerCase(Locale.ROOT);
  }

  /**
   * A builder; {@link Builder#database(SqliteDatabase)} is required.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A subject's AES key together with its generation (the ciphertext key-version byte). */
  private record VersionedKey(SecretKey key, int version) {}

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    VersionedKey key;
    byte[] result;
    try {
      // Outside a batch the mint commits before any cipher work: an encryption discarded below
      // leaves a committed key that the caller's retry loads (no second mint). Inside a batch the
      // mint shares the batch's fate (see the class Javadoc).
      key = keyForEncryption(subjectId);
      byte[] iv = new byte[GCM_IV_LENGTH];
      random.nextBytes(iv);
      Cipher cipher =
          cipherFactory.create(
              Cipher.ENCRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);
      result = new byte[HEADER_LENGTH + GCM_IV_LENGTH + ciphertext.length];
      result[0] = FORMAT_VERSION;
      result[1] = (byte) key.version();
      System.arraycopy(iv, 0, result, HEADER_LENGTH, GCM_IV_LENGTH);
      System.arraycopy(ciphertext, 0, result, HEADER_LENGTH + GCM_IV_LENGTH, ciphertext.length);
    } catch (KeyNotFoundException | SubjectForgottenException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Encryption failed for subject: "
              + subjectId.redacted()
              + SqliteCryptoStatements.lockAdvice(e),
          e);
    }
    verifyEncryption(subjectId, key, result, plaintext);
    return result;
  }

  /**
   * The subject's current key: read without the write lock when it exists, otherwise minted in a
   * write transaction that reads the key again, checks the tombstone and inserts the key.
   */
  private VersionedKey keyForEncryption(SubjectId subjectId) throws SQLException {
    VersionedKey existing = statements.read(conn -> loadKeyOrNull(conn, subjectId));
    if (existing != null) {
      return existing;
    }
    return statements.write(conn -> loadOrCreateKey(conn, subjectId));
  }

  /**
   * Checks a fresh encryption the way decrypt reads it: its header against the key's generation,
   * then its value with an independent cipher, compared with the plaintext. Any failure throws the
   * transient {@link CryptoOperationException} before the value is returned.
   */
  private void verifyEncryption(
      SubjectId subjectId, VersionedKey key, byte[] encrypted, byte[] plaintext) {
    Exception failure = null;
    try {
      validateCiphertextHeader(subjectId.redacted(), encrypted, key.version());
      if (MessageDigest.isEqual(plaintext, open(key.key(), encrypted))) {
        return;
      }
    } catch (GeneralSecurityException | RuntimeException e) {
      failure = e;
    }
    throw selfCheckFailed(subjectId, failure);
  }

  /**
   * Counts and logs a failed encryption check; the returned exception is the transient type. A
   * checked failure of the check is its cause; an unchecked one is attached as suppressed, so the
   * classifiers that walk the cause chain do not read it as permanent evidence.
   */
  private CryptoOperationException selfCheckFailed(SubjectId subjectId, Exception failure) {
    metrics.recordEncryptSelfCheckFailed();
    log.warn(
        "Encryption self-check failed for subject {}: the encrypted value did not decrypt to its"
            + " plaintext and is discarded; the operation fails as transient and is retried",
        subjectId.redacted());
    boolean unchecked = failure instanceof RuntimeException;
    CryptoOperationException transientFailure =
        new CryptoOperationException(
            "Encryption self-check failed for subject: "
                + subjectId.redacted()
                + " — the encrypted value did not decrypt to its plaintext and is discarded"
                + " (transient; retrying encrypts it again)",
            unchecked ? null : failure);
    if (unchecked) {
      transientFailure.addSuppressed(failure);
    }
    return transientFailure;
  }

  /** One decryption attempt with a cipher of its own. */
  private byte[] open(SecretKey key, byte[] encrypted) throws GeneralSecurityException {
    Cipher cipher =
        cipherFactory.create(
            Cipher.DECRYPT_MODE,
            key,
            new GCMParameterSpec(GCM_TAG_LENGTH, encrypted, HEADER_LENGTH, GCM_IV_LENGTH));
    return cipher.doFinal(
        encrypted, HEADER_LENGTH + GCM_IV_LENGTH, encrypted.length - HEADER_LENGTH - GCM_IV_LENGTH);
  }

  /**
   * Decrypts, and retries once with a fresh cipher when the authentication tag does not match. A
   * second mismatch propagates with the first attached as suppressed: the value itself is bad.
   */
  private byte[] openWithOneRetry(SubjectId subjectId, SecretKey key, byte[] encrypted)
      throws GeneralSecurityException {
    try {
      return open(key, encrypted);
    } catch (AEADBadTagException first) {
      try {
        byte[] plaintext = open(key, encrypted);
        retried(subjectId, RETRY_RECOVERED);
        return plaintext;
      } catch (AEADBadTagException second) {
        retried(subjectId, RETRY_FAILED);
        second.addSuppressed(first);
        throw second;
      }
    }
  }

  /** Counts and logs a retried decryption with its outcome. */
  private void retried(SubjectId subjectId, String outcome) {
    metrics.recordDecryptRetried(outcome);
    log.warn(
        "Decryption for subject {} failed its authentication tag and was retried once with a fresh"
            + " cipher; outcome {}",
        subjectId.redacted(),
        outcome);
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (ciphertext == null) {
      throw new IllegalArgumentException("ciphertext must not be null");
    }
    try {
      VersionedKey key = statements.read(conn -> loadKeyOrNull(conn, subjectId));
      if (key == null) {
        throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
      }
      validateCiphertextHeader(subjectId.redacted(), ciphertext, key.version());
      return openWithOneRetry(subjectId, key.key(), ciphertext);
    } catch (KeyNotFoundException | CryptoOperationException e) {
      throw e;
    } catch (AEADBadTagException e) {
      // A tag mismatch on both attempts is deterministic: retrying can never succeed, so it is the
      // subtype the classifiers route to poison, never the bare type they retry.
      throw new CryptoMappingException(
          "Decryption failed for subject: "
              + subjectId.redacted()
              + " — AEAD tag mismatch: the ciphertext is corrupted, tampered, or was written under"
              + " a different key (deterministic; retrying cannot succeed)",
          e);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Decryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Rejects values this engine cannot have produced before touching the cipher, and routes a value
   * written under a DESTROYED key generation ({@code [1, currentKeyVersion)}) to {@link
   * KeyNotFoundException}. Every other rejection is the deterministic {@link
   * CryptoMappingException}. The destroyed-generation check runs before the length check: a value
   * too short to decrypt can still carry a readable two-byte header naming an erased generation.
   */
  private static void validateCiphertextHeader(
      String subjectDigest, byte[] ciphertext, int currentKeyVersion) {
    if (ciphertext.length >= HEADER_LENGTH && ciphertext[0] == FORMAT_VERSION) {
      int blobVersion = ciphertext[1];
      if (blobVersion >= 1 && blobVersion < currentKeyVersion) {
        throw new KeyNotFoundException(
            "Ciphertext was written under key generation "
                + blobVersion
                + ", which a GDPR erasure destroyed; the current key generation is "
                + currentKeyVersion
                + " for subject: "
                + subjectDigest);
      }
    }
    int minLength = HEADER_LENGTH + GCM_IV_LENGTH + GCM_TAG_LENGTH / 8;
    if (ciphertext.length < minLength) {
      throw new CryptoMappingException(
          "Ciphertext too short for subject: "
              + subjectDigest
              + " — expected at least "
              + minLength
              + " bytes, got "
              + ciphertext.length);
    }
    if (ciphertext[0] != FORMAT_VERSION) {
      throw new CryptoMappingException(
          "Unsupported ciphertext format version "
              + ciphertext[0]
              + " for subject: "
              + subjectDigest
              + " — this engine reads format "
              + FORMAT_VERSION
              + "; the blob may come from a different CryptoEngine or a newer StreamRune version");
    }
    int blobVersion = ciphertext[1];
    if (blobVersion != currentKeyVersion) {
      throw new CryptoMappingException(
          "Unsupported key version "
              + blobVersion
              + " for subject: "
              + subjectDigest
              + " — this subject's current key generation is "
              + currentKeyVersion
              + " and destroyed generations are 1.."
              + (currentKeyVersion - 1)
              + "; the blob may be corrupted or come from a different CryptoEngine");
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One write transaction: the destroyed key's generation is recorded, the key row deleted and
   * the tombstone recorded, all or nothing. Crash points: before the {@code COMMIT} nothing of it
   * remains (SQLite rolls back an uncommitted transaction when the connection or the process dies)
   * and the rerun erases from the start; after it, the erasure is durable and a rerun only finds
   * the tombstone again. Never inside a projection batch on the same file (see the class Javadoc):
   * the erasure is committed when this returns.
   *
   * @throws IllegalStateException inside a projection batch on this engine's database file, before
   *     anything runs: an erasure there would commit or roll back with the batch
   */
  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    statements.refuseInsideABatch();
    try {
      statements.writeAlone(
          conn -> {
            eraseKey(conn, subjectId);
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Failed to delete key for subject: "
              + subjectId.redacted()
              + SqliteCryptoStatements.lockAdvice(e),
          e);
    }
  }

  /** The body of {@link #deleteKey}'s single transaction. */
  private void eraseKey(Connection conn, SubjectId subjectId) throws SQLException {
    String subject = storedSubjectId(subjectId);
    // Record the destroyed generation first (max-upsert: idempotent and monotonic), so the next
    // mint uses a higher one and decrypt can tell a destroyed generation from a tampered value.
    Integer erasedVersion = selectKeyVersion(conn, subject);
    if (erasedVersion != null) {
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
                  + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
                  + " max(erased_key_generations.max_erased_version,"
                  + " excluded.max_erased_version)")) {
        ps.setString(1, subject);
        ps.setInt(2, erasedVersion);
        ps.executeUpdate();
      }
    }
    try (PreparedStatement ps =
        conn.prepareStatement("DELETE FROM " + tableName + " WHERE subject_id = ?")) {
      ps.setString(1, subject);
      ps.executeUpdate();
    }
    // The first erasure's forgotten_at stays: a repeated erasure inserts nothing.
    try (PreparedStatement ps =
        conn.prepareStatement(
            "INSERT INTO forgotten_subjects (subject_id, forgotten_at) VALUES (?, ?)"
                + " ON CONFLICT (subject_id) DO NOTHING")) {
      ps.setString(1, subject);
      ps.setLong(2, SqliteTimestamps.toMicros(clock.instant()));
      ps.executeUpdate();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Deletes the subject's tombstone in one write transaction and keeps its {@code
   * erased_key_generations} row, so the next mint uses the next generation. Crash points: before
   * the {@code COMMIT} the tombstone stays and the rerun deletes it; after it, the rerun deletes
   * nothing, and a mint after either one uses the generation above the recorded one. Never inside a
   * projection batch on the same file, like {@link #deleteKey}.
   *
   * @throws IllegalStateException inside a projection batch on this engine's database file, before
   *     anything runs: an erasure there would commit or roll back with the batch
   */
  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    statements.refuseInsideABatch();
    try {
      statements.writeAlone(
          conn -> {
            try (PreparedStatement ps =
                conn.prepareStatement("DELETE FROM forgotten_subjects WHERE subject_id = ?")) {
              ps.setString(1, storedSubjectId(subjectId));
              ps.executeUpdate();
            }
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Failed to reinstate subject: "
              + subjectId.redacted()
              + SqliteCryptoStatements.lockAdvice(e),
          e);
    }
  }

  /**
   * The value written to the {@code subject_id} columns: the SHA-256 hex of the subject id's UTF-8
   * bytes, as {@code PostgresCryptoEngine} writes it.
   */
  static String storedSubjectId(SubjectId subjectId) {
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
   * {@inheritDoc}
   *
   * <p>The configured key table, {@code forgotten_subjects} and {@code erased_key_generations}: the
   * tables of {@link SqliteSchemaSeries#CRYPTO}, the key table under its configured name.
   */
  @Override
  public Set<String> requiredCryptoTables() {
    return Set.of(tableName, "forgotten_subjects", "erased_key_generations");
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    try {
      return statements.read(
          conn -> {
            try (PreparedStatement ps =
                conn.prepareStatement("SELECT 1 FROM " + tableName + " WHERE subject_id = ?")) {
              ps.setString(1, storedSubjectId(subjectId));
              try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
              }
            }
          });
    } catch (Exception e) {
      throw new CryptoOperationException(
          "isKeyAvailable failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * The body of a mint's write transaction: the key again (another writer may have minted it since
   * the read outside the transaction), the tombstone, then a new key. The write lock held from the
   * transaction's start keeps an erasure and a reinstate of the subject out until it commits.
   */
  private VersionedKey loadOrCreateKey(Connection conn, SubjectId subjectId) throws SQLException {
    VersionedKey key = loadKeyOrNull(conn, subjectId);
    if (key != null) {
      return key;
    }
    if (isForgotten(conn, subjectId)) {
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    return createAndStoreKey(conn, subjectId);
  }

  /** The current key's generation, or {@code null} when the subject has no key row. */
  private Integer selectKeyVersion(Connection conn, String subject) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT key_version FROM " + tableName + " WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt(1) : null;
      }
    }
  }

  /**
   * The generation the next mint for this subject must use: one above the highest generation any
   * erasure destroyed (1 for a subject never erased). Fails loudly at the single-byte cap instead
   * of wrapping onto a destroyed generation.
   */
  private static int nextKeyVersion(Connection conn, SubjectId subjectId) throws SQLException {
    int maxErased = 0;
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          maxErased = rs.getInt(1);
        }
      }
    }
    int next = maxErased + 1;
    if (next > MAX_KEY_VERSION) {
      throw new CryptoOperationException(
          "Key-generation limit reached for subject: "
              + subjectId.redacted()
              + " — the subject has been erased and re-registered "
              + (MAX_KEY_VERSION - 1)
              + " times, exhausting the single-byte generation counter. Re-register this person"
              + " under a fresh subject id.");
    }
    return next;
  }

  private static boolean isForgotten(Connection conn, SubjectId subjectId) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT 1 FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private VersionedKey loadKeyOrNull(Connection conn, SubjectId subjectId) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "SELECT key_bytes, key_version FROM " + tableName + " WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        byte[] bytes = rs.getBytes(1);
        try {
          // SecretKeySpec copies the array, so the driver's buffer can be zeroed at once.
          return new VersionedKey(new SecretKeySpec(bytes, "AES"), rs.getInt(2));
        } finally {
          Arrays.fill(bytes, (byte) 0);
        }
      }
    }
  }

  /**
   * Mints and inserts a key with the next generation, stamped with the clock. Inside the write
   * transaction no other writer can have inserted one since {@link #loadOrCreateKey} read none; the
   * insert still never overwrites an existing key, which would orphan every value encrypted under
   * it. Crash points: before the transaction's {@code COMMIT} no key remains and the rerun mints
   * one; after it the key is durable and the rerun loads it.
   */
  private VersionedKey createAndStoreKey(Connection conn, SubjectId subjectId) throws SQLException {
    SecretKey key;
    try {
      KeyGenerator generator = KeyGenerator.getInstance("AES");
      generator.init(256);
      key = generator.generateKey();
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException(
          "Key generation failed for subject: " + subjectId.redacted(), e);
    }
    int version = nextKeyVersion(conn, subjectId);
    byte[] encoded = key.getEncoded();
    try (PreparedStatement ps =
        conn.prepareStatement(
            "INSERT INTO "
                + tableName
                + " (subject_id, key_bytes, created_at, key_version) VALUES (?, ?, ?, ?)"
                + " ON CONFLICT (subject_id) DO NOTHING")) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.setBytes(2, encoded);
      ps.setLong(3, SqliteTimestamps.toMicros(clock.instant()));
      ps.setInt(4, version);
      if (ps.executeUpdate() == 1) {
        return new VersionedKey(key, version);
      }
    } finally {
      Arrays.fill(encoded, (byte) 0);
    }
    VersionedKey winner = loadKeyOrNull(conn, subjectId);
    if (winner == null) {
      throw new CryptoOperationException(
          "Key for subject vanished after insert conflict: " + subjectId.redacted());
    }
    return winner;
  }

  @Override
  public String toString() {
    return "SqliteCryptoEngine[" + tableName + "]";
  }

  /** Builds a {@link SqliteCryptoEngine}. */
  public static final class Builder {
    private SqliteDatabase database;
    private String tableName = DEFAULT_TABLE_NAME;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private Clock clock = Clock.systemUTC();
    private CipherFactory cipherFactory = FRESH_CIPHERS;

    private Builder() {}

    /**
     * The database that holds the keys; its crypto series ({@link SqliteSchemaSeries#CRYPTO}) must
     * be applied. The engine keeps its data source and file, not the handle: it works after the
     * handle is closed, and closing the handle stays the caller's.
     *
     * @param database the database; required
     * @return this builder
     */
    public Builder database(SqliteDatabase database) {
      this.database = database;
      return this;
    }

    /**
     * The metrics collector that counts discarded encryptions ({@code
     * streamrune.crypto.encrypt_self_check_failed}) and retried decryptions ({@code
     * streamrune.crypto.decrypt_retried}). Defaults to {@link StreamRuneMetrics#NOOP}.
     *
     * @param metrics the metrics; not {@code null}
     * @return this builder
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = Objects.requireNonNull(metrics, "metrics is required");
      return this;
    }

    /**
     * The clock that stamps a key's {@code created_at} and a tombstone's {@code forgotten_at}.
     * Defaults to {@link Clock#systemUTC()}.
     *
     * @param clock the clock; not {@code null}
     * @return this builder
     */
    public Builder clock(Clock clock) {
      this.clock = Objects.requireNonNull(clock, "clock is required");
      return this;
    }

    /** Test-only seam: creates the cipher of every operation. */
    Builder cipherFactory(CipherFactory cipherFactory) {
      this.cipherFactory = cipherFactory;
      return this;
    }

    /**
     * Overrides the key-table name (default {@link #DEFAULT_TABLE_NAME}). The value must be a plain
     * SQL identifier ({@code [A-Za-z_][A-Za-z0-9_]*}) not starting with {@code sqlite_}: it is
     * concatenated into SQL and validated at {@link #build()}. A blank or {@code null} value keeps
     * the default; the name is folded to lower case. The crypto series creates only the
     * default-named table, so a different table is the application's to create (the columns of
     * {@code encryption_keys}) and to evolve; {@link SqliteCryptoEngine#requiredCryptoTables()}
     * reports the configured name, so a missing one fails at startup.
     *
     * @param tableName the key table's name
     * @return this builder
     */
    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    /**
     * Builds the engine. It touches no table.
     *
     * @return the engine
     * @throws IllegalStateException if no database is set
     * @throws IllegalArgumentException if the table name is not a valid identifier; the message
     *     names the rejected value
     */
    public SqliteCryptoEngine build() {
      if (database == null) {
        throw new IllegalStateException("database is required");
      }
      return new SqliteCryptoEngine(this);
    }
  }
}

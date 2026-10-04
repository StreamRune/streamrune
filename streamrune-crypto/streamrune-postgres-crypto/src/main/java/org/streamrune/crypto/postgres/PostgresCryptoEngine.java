package org.streamrune.crypto.postgres;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.Locale;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * PostgreSQL-backed {@link CryptoEngine} storing one AES-256 key per subject in the {@code
 * encryption_keys} table.
 *
 * <p><b>Ciphertext wire format</b> (versioned for algorithm agility and key generations): {@code
 * [format version (1 byte)][key version (1 byte)][GCM IV (12 bytes)][AES-256-GCM ciphertext+tag]}.
 * The key-version byte carries the <b>generation</b> of the key the blob was encrypted under (first
 * mint = {@code 1}; each post-erasure re-mint increments it — see below). Decrypt compares the
 * blob's generation to the current key's: equal proceeds normally (an AES-GCM tag failure stays a
 * loud tamper/corruption error); a LOWER generation ({@code >= 1}) means the blob's key was
 * destroyed by a GDPR erasure and yields {@link KeyNotFoundException} (mapped to {@code [REDACTED]}
 * by {@code CryptoShreddingModule}, keeping aggregate loads and rebuilds working); anything else —
 * an unknown format version, a generation of {@code < 1} or above the current key's — fails with a
 * clear {@link CryptoMappingException} (the deterministic subtype of {@link
 * CryptoOperationException}: retrying cannot succeed) instead of a generic GCM tag failure, so a
 * misconfigured engine swap, a rolled-back version byte, or a future format change is detectable.
 *
 * <p><b>Terminal erasure.</b> {@link #deleteKey(SubjectId)} deletes the {@code encryption_keys} row
 * and records a durable tombstone in the {@code forgotten_subjects} table (atomically, in one
 * transaction). A later {@link #encrypt(SubjectId, byte[])} for a tombstoned subject throws {@link
 * SubjectForgottenException} instead of minting a fresh key, so a GDPR erasure cannot be silently
 * un-done by a retried command or re-imported event. The tombstone survives restarts (it is a row).
 * The only way to legitimately re-register a forgotten subject is {@link #reinstate(SubjectId)},
 * which deletes that subject's tombstone row; there is no engine-wide opt-out of terminal erasure.
 * The {@code forgotten_subjects} table is created by the crypto Flyway baseline shipped at {@code
 * classpath:db/crypto-migration}.
 *
 * <p><b>Key generations across erasures.</b> {@link #deleteKey(SubjectId)} records the destroyed
 * key's generation in {@code erased_key_generations} (SHA-256 subject hash + a small integer — no
 * PII, no key material; the row is deliberately never DELETED by {@link #reinstate(SubjectId)} so
 * generations stay monotonic across repeated erase/reinstate cycles). The next mint for that
 * subject uses {@code max_erased_version + 1}, and decrypt maps a blob whose generation is lower
 * than the current key's to {@link KeyNotFoundException} — so after the documented reinstate +
 * re-encrypt flow, the returning subject's PRE-erasure ciphertext keeps reading as {@code
 * [REDACTED]} (replay- and rebuild-tolerant) instead of failing the GCM tag check and permanently
 * blocking the stream. A subject supports at most 126 completed erase/re-register cycles (the
 * generation byte caps at 127); the mint fails loudly with guidance to use a fresh subject id
 * beyond that. Residual, documented in {@code docs/guide/advanced/gdpr-erasure.md}: the generation
 * byte is routing metadata outside the GCM tag, so an attacker with write access to stored
 * ciphertext can roll a reinstated subject's CURRENT-generation blob down to a destroyed generation
 * and turn it into {@code [REDACTED]} (surfaced via the redaction metric/WARN) instead of a loud
 * tag failure — bounded to subjects that actually went through erasure + reinstate.
 *
 * <p><b>Subject ids are stored hashed, never verbatim.</b> The {@code subject_id} columns of both
 * {@code encryption_keys} and the permanent {@code forgotten_subjects} tombstone hold the SHA-256
 * hex of the subject id, not the raw value — so a subject id that is itself PII (an email or
 * username) never persists in cleartext, matching {@code FileSystemCryptoEngine} and {@code
 * VaultCryptoEngine}. All lookups hash first, so callers still pass the natural {@link SubjectId}.
 *
 * <p><b>Key material handling is best-effort:</b> intermediate key buffers are zeroed after use,
 * but {@link SecretKeySpec} retains an internal copy on the heap until GC — full zeroization is not
 * possible with the standard JCA provider.
 */
public final class PostgresCryptoEngine implements CryptoEngine {

  /** Default key-table name, matching the table the shipped crypto Flyway baseline creates. */
  public static final String DEFAULT_TABLE_NAME = "encryption_keys";

  // Table names are concatenated into SQL (an identifier cannot be a JDBC bind parameter), so a
  // configured name MUST be a plain SQL identifier: letters/digits/underscore, starting with a
  // letter or underscore. This allowlist rejects whitespace, quotes, dots (schema qualification is
  // unsupported — use the connection search_path) and every other injection vector at construction.
  // An accepted name is folded to lower case (validateTableName), as PostgreSQL folds it.
  private static final java.util.regex.Pattern SAFE_IDENTIFIER =
      java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128;
  private static final int GCM_IV_LENGTH = 12;
  private static final byte FORMAT_VERSION = 1;
  private static final int HEADER_LENGTH = 2;

  /**
   * Highest key generation the single-byte header can carry. Generations start at 1 and bump only
   * on a post-erasure re-mint, so the cap allows 126 completed erase/re-register cycles per subject
   * before the mint refuses with guidance to use a fresh subject id.
   */
  private static final int MAX_KEY_VERSION = Byte.MAX_VALUE;

  // Cipher.getInstance() does an SPI provider lookup that measured ~8.6us — ~3.7x the cost of the
  // actual encrypt — so the AES/GCM cipher is reused per thread instead of re-resolved per call.
  // Cipher is not thread-safe, hence ThreadLocal; init() fully re-initializes it on each use, so
  // reuse across encrypt/decrypt calls on the same thread is safe.
  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(
          () -> {
            try {
              return Cipher.getInstance(ALGORITHM);
            } catch (GeneralSecurityException e) {
              throw new CryptoOperationException("AES/GCM cipher unavailable", e);
            }
          });

  private final DataSource dataSource;
  private final String tableName;
  private final SecureRandom random = new SecureRandom();

  private PostgresCryptoEngine(DataSource dataSource, String tableName) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource must not be null");
    this.dataSource = dataSource;
    this.tableName = validateTableName(tableName);
  }

  /**
   * Validates a configured key-table name as a plain SQL identifier before it is concatenated into
   * SQL. A table name cannot be a JDBC bind parameter, so an unvalidated value would be a SQL
   * injection vector via configuration ({@code streamrune.crypto.postgres.table-name}). Only names
   * matching {@code [A-Za-z_][A-Za-z0-9_]*} are accepted; a blank/null value falls back to {@link
   * #DEFAULT_TABLE_NAME}, and anything else (quoted, schema-qualified, or containing
   * whitespace/punctuation) is rejected at construction.
   *
   * <p>An accepted name is folded to lower case ({@link Locale#ROOT}), which is what PostgreSQL
   * does with the unquoted identifier in the engine's SQL: {@code CryptoKeys} names the table
   * {@code cryptokeys}. {@link #requiredCryptoTables()} reports the folded name, because {@code
   * SchemaValidator} looks it up through {@code DatabaseMetaData.getTables}, which matches case
   * sensitively; the unfolded name failed the boot with a false "missing crypto table" error.
   */
  private static String validateTableName(String tableName) {
    if (tableName == null || tableName.isBlank()) {
      return DEFAULT_TABLE_NAME;
    }
    if (!SAFE_IDENTIFIER.matcher(tableName).matches()) {
      throw new IllegalArgumentException(
          "Invalid streamrune.crypto.postgres.table-name '"
              + tableName
              + "': must be a plain SQL identifier matching [A-Za-z_][A-Za-z0-9_]* (schema"
              + " qualification is not supported — set the connection search_path instead).");
    }
    return tableName.toLowerCase(Locale.ROOT);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A subject's AES key together with its generation (the ciphertext key-version byte). */
  private record VersionedKey(SecretKey key, int version) {}

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (plaintext == null) throw new IllegalArgumentException("plaintext must not be null");
    try (Connection conn = dataSource.getConnection()) {
      VersionedKey key = inTransaction(conn, c -> loadOrCreateKey(c, subjectId));
      byte[] iv = new byte[GCM_IV_LENGTH];
      random.nextBytes(iv);
      Cipher cipher = CIPHER.get();
      cipher.init(Cipher.ENCRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);
      byte[] result = new byte[HEADER_LENGTH + GCM_IV_LENGTH + ciphertext.length];
      result[0] = FORMAT_VERSION;
      result[1] = (byte) key.version();
      System.arraycopy(iv, 0, result, HEADER_LENGTH, GCM_IV_LENGTH);
      System.arraycopy(ciphertext, 0, result, HEADER_LENGTH + GCM_IV_LENGTH, ciphertext.length);
      return result;
    } catch (KeyNotFoundException | SubjectForgottenException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Encryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (ciphertext == null) throw new IllegalArgumentException("ciphertext must not be null");
    try (Connection conn = dataSource.getConnection()) {
      VersionedKey key = loadKey(conn, subjectId);
      validateCiphertextHeader(subjectId.redacted(), ciphertext, key.version());
      byte[] iv = new byte[GCM_IV_LENGTH];
      System.arraycopy(ciphertext, HEADER_LENGTH, iv, 0, GCM_IV_LENGTH);
      Cipher cipher = CIPHER.get();
      cipher.init(Cipher.DECRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      return cipher.doFinal(
          ciphertext,
          HEADER_LENGTH + GCM_IV_LENGTH,
          ciphertext.length - HEADER_LENGTH - GCM_IV_LENGTH);
    } catch (KeyNotFoundException | CryptoOperationException e) {
      throw e;
    } catch (javax.crypto.AEADBadTagException e) {
      // A GCM tag mismatch is DETERMINISTIC — the blob is corrupted, tampered, or was
      // written under a different key — and retrying can never succeed. The family classifiers
      // (SagaStateConversion, ReadPoisonClassifier, hasTransientCryptoCause) read a BARE
      // CryptoOperationException as key-store OUTAGE evidence and retry forever; the deterministic
      // subtype is what routes this to poison/quarantine instead.
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
   * Rejects blobs that cannot have been produced by this engine before touching the cipher, so a
   * format mismatch (different engine, tampering, future version) fails with a clear message
   * instead of a generic GCM tag-check failure — and routes a blob written under a DESTROYED key
   * generation to {@link KeyNotFoundException} so it reads as {@code [REDACTED]} instead of failing
   * the tag check under the wrong key.
   *
   * <p>The destroyed-generation tolerance is strictly bounded to {@code [1, currentKeyVersion)}: a
   * version byte of {@code < 1} or {@code > currentKeyVersion} is not a generation this engine ever
   * minted for the subject, so it stays a loud corruption error — otherwise flipping the byte low
   * would silently redact ANY subject's field, including never-erased ones.
   */
  private static void validateCiphertextHeader(
      String subjectDigest, byte[] ciphertext, int currentKeyVersion) {
    // Every rejection below is DETERMINISTIC (a property of the blob, not of the
    // key store), so it is raised as CryptoMappingException — the subtype the family classifiers
    // treat as poison — never as the bare CryptoOperationException they read as a transient
    // key-store outage and retry forever.
    //
    // The erased-generation
    // check below must run BEFORE the full-length rejection — a blob too short to ever decrypt
    // (missing IV/tag) can still carry a whole, readable 2-byte header (FORMAT_VERSION +
    // generation), and a generation the current key has already outlived is still the designed
    // KeyNotFoundException ([REDACTED]) signal for GDPR-erased data, not a length-corruption
    // verdict. Guard on length >= HEADER_LENGTH first: only the two header bytes this reads must
    // be safely present; a genuinely sub-header blob (or one whose format byte doesn't match)
    // falls through unchanged to the full checks below.
    if (ciphertext.length >= HEADER_LENGTH && ciphertext[0] == FORMAT_VERSION) {
      int erasedCheckBlobVersion = ciphertext[1];
      if (erasedCheckBlobVersion >= 1 && erasedCheckBlobVersion < currentKeyVersion) {
        throw new KeyNotFoundException(
            "Ciphertext was written under key generation "
                + erasedCheckBlobVersion
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
    if (blobVersion >= 1 && blobVersion < currentKeyVersion) {
      // Written under an earlier key generation of this subject — destroyed by a GDPR erasure
      // (the current key was minted after reinstate/re-registration). No key can decrypt it
      // anymore; surface the designed erased-data signal instead of a wrong-key GCM tag failure
      // that would block replay and rebuilds.
      throw new KeyNotFoundException(
          "Ciphertext was written under key generation "
              + blobVersion
              + ", which a GDPR erasure destroyed; the current key generation is "
              + currentKeyVersion
              + " for subject: "
              + subjectDigest);
    }
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

  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      inTransaction(
          conn,
          c -> {
            eraseKey(c, subjectId);
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Failed to delete key for subject: " + subjectId.redacted(), e);
    } finally {
      closeQuietly(conn);
    }
  }

  /** The body of {@link #deleteKey}'s single transaction ({@link #inTransaction} commits it). */
  private void eraseKey(Connection conn, SubjectId subjectId) throws SQLException {
    String deleteKeySql = "DELETE FROM " + tableName + " WHERE subject_id = ?";
    String tombstoneSql =
        "INSERT INTO forgotten_subjects (subject_id) VALUES (?) ON CONFLICT (subject_id) DO NOTHING";
    // Take the same per-subject advisory lock loadOrCreateKey() takes, inside the same
    // transaction, before touching either table. This makes "check tombstone, then write key"
    // (encrypt) and "delete key, then write tombstone" (this method) mutually exclusive per
    // subject — a concurrent encrypt() either completes entirely before this delete starts, or
    // waits for this delete's tombstone to commit before it can even read forgotten_subjects.
    // Other subjects take a different lock key, so throughput for unrelated subjects is
    // unaffected — this is NOT a global lock.
    acquireSubjectLock(conn, subjectId);
    // Before destroying the key, record its generation
    // in erased_key_generations (GREATEST-upsert, so re-running an erasure is idempotent and
    // generations stay monotonic across repeated erase/reinstate cycles). The next mint for
    // this subject uses max_erased_version + 1, which is what lets decrypt tell "written
    // under a destroyed generation" ([REDACTED]) apart from "tampered current-generation
    // blob" (loud). Same transaction as the delete + tombstone: all three commit or none.
    Integer erasedVersion = selectKeyVersion(conn, subjectId);
    if (erasedVersion != null) {
      recordErasedGeneration(conn, subjectId, erasedVersion);
    }
    // Delete the key and record the tombstone in one transaction: erasure and its terminal
    // marker must commit together, or neither does. A committed delete without a tombstone
    // would silently allow resurrection on the next encrypt — which is why inTransaction rolls
    // back on an Error too, before restoring autoCommit.
    try (PreparedStatement ps = conn.prepareStatement(deleteKeySql)) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.executeUpdate();
    }
    try (PreparedStatement ps = conn.prepareStatement(tombstoneSql)) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.executeUpdate();
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    // Clearing the tombstone lets the next encrypt mint a fresh key. The old key is gone; only
    // future writes are re-enabled. The subject's erased_key_generations row, which deleteKey
    // wrote when it destroyed the key, is deliberately NEVER REMOVED: it is what makes the next
    // mint use a HIGHER generation, so the subject's pre-erasure ciphertext keeps reading as
    // [REDACTED] (KeyNotFoundException) instead of failing the GCM tag check under the re-minted
    // key. A tombstone with no such row is a forget of a
    // subject that never encrypted (no ciphertext exists, so the next mint is generation 1) or an
    // operator fault — a partial restore, a key row deleted outside the engine — which this method
    // does not repair (docs/guide/advanced/gdpr-erasure.md).
    //
    // The DELETE runs under the same per-subject advisory lock deleteKey() and loadOrCreateKey()
    // take, so a concurrent erasure or mint for this subject cannot interleave with it.
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      inTransaction(
          conn,
          c -> {
            liftTombstone(c, subjectId);
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException("Failed to reinstate subject: " + subjectId.redacted(), e);
    } finally {
      closeQuietly(conn);
    }
  }

  /** The body of {@link #reinstate}'s single transaction ({@link #inTransaction} commits it). */
  private static void liftTombstone(Connection conn, SubjectId subjectId) throws SQLException {
    acquireSubjectLock(conn, subjectId);
    try (PreparedStatement ps =
        conn.prepareStatement("DELETE FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.executeUpdate();
    }
  }

  /** One transaction's work, run by {@link #inTransaction}. */
  @FunctionalInterface
  private interface TransactionWork<T> {
    T run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code work} as one transaction on {@code conn} and commits it. Every engine write goes
   * through here: {@link #encrypt}, {@link #deleteKey} and {@link #reinstate}.
   *
   * <p>The rollback follows the pattern of {@code PostgresEventStore.append}: a {@code committed}
   * flag, and a single rollback point in {@code finally} that runs BEFORE autoCommit is restored
   * and covers every non-committed exit, an {@link Error} included. The order matters because
   * pgjdbc's {@code setAutoCommit(true)} COMMITS an open transaction. With the rollback only in
   * {@code catch (Exception)}, an {@code OutOfMemoryError} between deleteKey's key DELETE and its
   * tombstone INSERT committed the erasure without its tombstone, so the next encrypt minted a new
   * key for a forgotten subject; one after reinstate's tombstone DELETE committed a reinstate that
   * had failed.
   *
   * <p>If the rollback itself fails (a {@code SQLException}, a {@code RuntimeException} or an
   * {@link Error}), the transaction may still be open, and nothing after it may commit it: not an
   * autoCommit restore here, and not the pool's reset when the caller closes the connection.
   * HikariCP rolls a dirty connection back on return, but Agroal, the Quarkus pool, resets a
   * changed autoCommit with {@code setAutoCommit(true)} and no rollback first, and pgjdbc COMMITS
   * the open transaction there: an erasure's key DELETE without its tombstone. So a failed rollback
   * aborts the physical connection instead ({@link PostgresCryptoTransactions#rollbackOrAbort});
   * the server discards the transaction, and a rerun converges as after any other failed write. On
   * a failure path, neither the rollback's, the restore's nor the abort's {@code SQLException} or
   * {@code RuntimeException} replaces the exception already in flight; an {@code Error} from the
   * rollback propagates once the connection is aborted.
   */
  private static <T> T inTransaction(Connection conn, TransactionWork<T> work) throws SQLException {
    boolean autoCommit = conn.getAutoCommit();
    conn.setAutoCommit(false);
    boolean committed = false;
    try {
      T result = work.run(conn);
      conn.commit();
      committed = true;
      return result;
    } finally {
      if (committed) {
        conn.setAutoCommit(autoCommit);
      } else if (PostgresCryptoTransactions.rollbackOrAbort(conn)) {
        PostgresCryptoTransactions.restoreAutoCommitQuietly(conn, autoCommit);
      }
    }
  }

  private static void closeQuietly(Connection conn) {
    if (conn == null) return;
    try {
      conn.close();
    } catch (java.sql.SQLException _) {
      // Nothing actionable on close failure.
    }
  }

  /**
   * The opaque value actually written to the {@code subject_id} columns of {@code encryption_keys}
   * and {@code forgotten_subjects}: the SHA-256 hex of the subject id's UTF-8 bytes. Every lookup,
   * insert, delete and tombstone hashes first, so a caller's subject id — which may itself be PII
   * (email, username) — never lands verbatim in a stored column (and never in the permanent
   * forgotten-subjects tombstone). This mirrors {@code FileSystemCryptoEngine.sha256Hex} and {@code
   * VaultCryptoEngine.keyName} for cross-backend parity; UTF-8 is fixed so the hash is stable
   * across JVMs and platform default charsets.
   */
  private static String storedSubjectId(SubjectId subjectId) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "SHA-256 hashing failed for subject: " + subjectId.redacted(), e);
    }
  }

  @Override
  public java.util.Set<String> requiredCryptoTables() {
    // This backend stores per-subject keys and crypto-shredding tombstones in the
    // shared crypto schema (the crypto Flyway baseline at db/crypto-migration). Report
    // the CONFIGURED key-table name so SchemaValidator fails fast at boot if a custom-named table
    // was configured but not created (the migration only creates the default-named table — see
    // Builder#tableName).
    return java.util.Set.of(tableName, "forgotten_subjects", "erased_key_generations");
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    String sql = "SELECT 1 FROM " + tableName + " WHERE subject_id = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (Exception e) {
      throw new CryptoOperationException(
          "isKeyAvailable failed for subject: " + subjectId.redacted(), e);
    }
  }

  private VersionedKey loadOrCreateKey(Connection conn, SubjectId subjectId)
      throws java.sql.SQLException {
    // Per-subject advisory lock, held for the rest of this transaction (released on commit/
    // rollback). deleteKey() takes the SAME lock key before it deletes the key row / inserts the
    // tombstone, so the "load key, check tombstone, insert key" sequence below can never interleave
    // with a concurrent delete for the SAME subject: either this whole method runs to completion
    // (commit) before deleteKey()'s transaction can acquire the lock, or deleteKey() commits first
    // and this method observes its result (no key, tombstone present) atomically. Different
    // subjects hash to different lock keys, so unrelated traffic is never blocked — this is not a
    // global lock.
    acquireSubjectLock(conn, subjectId);
    VersionedKey key = loadKeyOrNull(conn, subjectId);
    if (key != null) return key;
    // No key: either brand new, or crypto-shredded. Terminal erasure refuses to mint a fresh key
    // for a forgotten subject; reinstate(subjectId) is the only way to lift that.
    boolean forgotten = isForgotten(conn, subjectId);
    if (raceHook != null) {
      // Test-only seam (see setLoadOrCreateKeyRaceHook): lets a test force a concurrent deleteKey()
      // to run to completion at the exact TOCTOU point this fix closes, proving the two operations
      // are now mutually exclusive rather than relying on probabilistic timing.
      raceHook.run();
    }
    if (forgotten) {
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    return createAndStoreKey(conn, subjectId);
  }

  /**
   * Serializes {@link #loadOrCreateKey} and {@link #deleteKey} for the same subject using a
   * Postgres transaction-scoped advisory lock keyed by a hash of the subject id. The lock is
   * per-subject (not global): concurrent traffic for other subjects is never blocked, so this adds
   * no cross-subject contention. Released automatically on transaction commit or rollback.
   */
  private static void acquireSubjectLock(Connection conn, SubjectId subjectId)
      throws java.sql.SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?))")) {
      ps.setString(1, storedSubjectId(subjectId));
      // executeQuery(), not execute(): pg_advisory_xact_lock() returns a single row (its result
      // column is irrelevant — only the call's side effect, taking the lock, matters). The row
      // still has to be drained for the driver to consider the statement complete.
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }

  /** The current key's generation, or {@code null} when the subject has no key row. */
  private Integer selectKeyVersion(Connection conn, SubjectId subjectId)
      throws java.sql.SQLException {
    String sql = "SELECT key_version FROM " + tableName + " WHERE subject_id = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt("key_version") : null;
      }
    }
  }

  /**
   * Records that {@code erasedVersion} was destroyed for the subject. GREATEST keeps the counter
   * monotonic under idempotent forget re-runs and concurrent erasures.
   */
  private static void recordErasedGeneration(
      Connection conn, SubjectId subjectId, int erasedVersion) throws java.sql.SQLException {
    String sql =
        "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?) "
            + "ON CONFLICT (subject_id) DO UPDATE SET max_erased_version = "
            + "GREATEST(erased_key_generations.max_erased_version, EXCLUDED.max_erased_version)";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.setInt(2, erasedVersion);
      ps.executeUpdate();
    }
  }

  /**
   * The generation the next mint for this subject must use: one above the highest generation any
   * completed erasure destroyed (1 for a subject never erased). Fails loudly at the single-byte cap
   * instead of wrapping — a wrapped generation would collide with a destroyed one and turn its
   * ciphertext into wrong-key GCM failures.
   */
  private int nextKeyVersion(Connection conn, SubjectId subjectId) throws java.sql.SQLException {
    String sql = "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?";
    int maxErased = 0;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          maxErased = rs.getInt("max_erased_version");
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

  // Test-only hook invoked (on the calling thread, while the per-subject advisory lock is held)
  // right after the forgotten-subjects check and right before a fresh key would be inserted. Not
  // used by production code paths; defaults to null (no-op). Package-visible static state is safe
  // here because it exists solely so integration tests can force a deterministic interleave against
  // this exact TOCTOU window — see PostgresCryptoEngineIntegrationTest.encryptRacingDeleteKey_*.
  private static volatile Runnable raceHook;

  static void setLoadOrCreateKeyRaceHook(Runnable hook) {
    raceHook = hook;
  }

  private boolean isForgotten(Connection conn, SubjectId subjectId) throws java.sql.SQLException {
    String sql = "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private VersionedKey loadKey(Connection conn, SubjectId subjectId) throws java.sql.SQLException {
    VersionedKey key = loadKeyOrNull(conn, subjectId);
    if (key == null) {
      throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
    }
    return key;
  }

  private VersionedKey loadKeyOrNull(Connection conn, SubjectId subjectId)
      throws java.sql.SQLException {
    String sql = "SELECT key_bytes, key_version FROM " + tableName + " WHERE subject_id = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          byte[] bytes = rs.getBytes("key_bytes");
          try {
            // SecretKeySpec copies the array, so the JDBC buffer can be zeroed immediately.
            return new VersionedKey(new SecretKeySpec(bytes, "AES"), rs.getInt("key_version"));
          } finally {
            java.util.Arrays.fill(bytes, (byte) 0);
          }
        }
        return null;
      }
    }
  }

  private VersionedKey createAndStoreKey(Connection conn, SubjectId subjectId)
      throws java.sql.SQLException {
    SecretKey key;
    try {
      var gen = KeyGenerator.getInstance("AES");
      gen.init(256);
      key = gen.generateKey();
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Key generation failed for subject: " + subjectId.redacted(), e);
    }
    // The mint's generation is one above the highest generation any completed
    // erasure destroyed for this subject (1 for a never-erased subject). Read under the same
    // per-subject advisory lock + transaction as the insert, so it cannot interleave with a
    // concurrent deleteKey's GREATEST-upsert for this subject.
    int version = nextKeyVersion(conn, subjectId);
    // First writer wins: a concurrent create must never overwrite an existing key —
    // that would permanently orphan all data already encrypted under the old key.
    String sql =
        "INSERT INTO "
            + tableName
            + " (subject_id, key_bytes, key_version) VALUES (?, ?, ?) "
            + "ON CONFLICT (subject_id) DO NOTHING";
    byte[] encoded = key.getEncoded();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, storedSubjectId(subjectId));
      ps.setBytes(2, encoded);
      ps.setInt(3, version);
      if (ps.executeUpdate() == 1) {
        return new VersionedKey(key, version);
      }
    } finally {
      // Best-effort: getEncoded() returns a fresh copy — zero it once the INSERT has executed.
      java.util.Arrays.fill(encoded, (byte) 0);
    }
    // Lost the race: re-read the winning row so both writers use the same key.
    VersionedKey winner = loadKeyOrNull(conn, subjectId);
    if (winner == null) {
      throw new CryptoOperationException(
          "Key for subject vanished after insert conflict: " + subjectId.redacted());
    }
    return winner;
  }

  public static final class Builder {
    private DataSource dataSource;
    private String tableName = DEFAULT_TABLE_NAME;

    public Builder dataSource(DataSource ds) {
      this.dataSource = ds;
      return this;
    }

    /**
     * Overrides the key-table name (default {@link #DEFAULT_TABLE_NAME}), fed from {@code
     * streamrune.crypto.postgres.table-name}. The value must be a plain SQL identifier ({@code
     * [A-Za-z_][A-Za-z0-9_]*}) — it is concatenated into SQL and validated at {@link #build()},
     * which throws {@link IllegalArgumentException} on anything else (SQL-injection guard). A blank
     * or {@code null} value keeps the default. The name is folded to lower case, as PostgreSQL
     * folds an unquoted identifier: {@code CryptoKeys} names the table {@code cryptokeys}. The
     * shipped Flyway migration always creates the default-named {@code encryption_keys} table, so
     * if you point this at a different table you are responsible for creating it (same DDL as the
     * baseline's {@code encryption_keys}, including the {@code key_version} column); {@link
     * PostgresCryptoEngine#requiredCryptoTables()} reports the configured name, so a missing custom
     * table is caught at startup by {@code SchemaValidator} rather than at first use.
     *
     * <p><b>You also own its schema evolution</b>. The crypto series only ever targets the default
     * name {@code encryption_keys}, so a future migration that adds a key-table column will NOT
     * reach a custom-named table, and re-applying the series is not a remediation for one. {@code
     * SchemaValidator} fails the boot with the exact {@code ALTER TABLE} to run; {@code
     * forgotten_subjects} and {@code erased_key_generations} keep their fixed names and are created
     * by the series as usual.
     */
    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public PostgresCryptoEngine build() {
      if (dataSource == null) throw new IllegalStateException("dataSource is required");
      return new PostgresCryptoEngine(dataSource, tableName);
    }
  }
}

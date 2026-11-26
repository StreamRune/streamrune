package org.streamrune.crypto.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;
import javax.crypto.AEADBadTagException;
import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.AesGcmV1Format;
import org.streamrune.crypto.JdbcForgottenSubjects;
import org.streamrune.crypto.JdbcSubjectKeyStore;
import org.streamrune.crypto.VerifiedCipher;
import org.streamrune.crypto.VersionedKey;

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
 * <p><b>Verified encryption, retried decryption.</b> Every encryption, its check and every
 * decryption attempt take a cipher of their own. The engine checks each encryption the way decrypt
 * reads it — the header against the key's generation, then the value with a second, independent
 * cipher — before it returns it; a result that does not decrypt to the plaintext, or a check that
 * fails in any way, discards the value and throws the transient {@link CryptoOperationException},
 * so a wrong cipher result is never returned and the caller's retry encrypts again. The key a first
 * encryption mints is committed in its own transaction before any cipher work, so the retry loads
 * that key and mints no second one. A decryption whose authentication tag does not match is retried
 * once with a fresh cipher before it fails with the deterministic {@link CryptoMappingException}: a
 * corrupted, tampered or foreign value fails both attempts, while a one-off wrong cipher result,
 * whatever its cause, cannot wedge a reader. Both events are counted ({@code
 * streamrune.crypto.encrypt_self_check_failed}; {@code streamrune.crypto.decrypt_retried}, tagged
 * {@code RECOVERED} or {@code FAILED}) through the {@link StreamRuneMetrics} given to {@link
 * Builder#metrics(StreamRuneMetrics)} and logged at {@code WARN}.
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

  private static final Logger log = LoggerFactory.getLogger(PostgresCryptoEngine.class);

  private final DataSource dataSource;
  private final String tableName;

  // The key store's statements (per-subject advisory lock, load, mint, erase, lift tombstone),
  // shared with the SQLite engine; its writes run on this engine's connection, inside its
  // transactions,
  // while decrypt and isKeyAvailable read outside one.
  private final JdbcSubjectKeyStore keys;

  // The cipher seam, the encryption self-check and the one-retry decryption, shared with the other
  // AES-GCM engines; its WARN lines go to this engine's logger.
  private final VerifiedCipher verified;

  // The v1 wire format (see the class comment), sealing and opening through the cipher seam above.
  private final AesGcmV1Format format;

  private PostgresCryptoEngine(
      DataSource dataSource,
      String tableName,
      StreamRuneMetrics metrics,
      VerifiedCipher.CipherFactory cipherFactory) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource must not be null");
    }
    this.dataSource = dataSource;
    this.tableName = validateTableName(tableName);
    // The race hook is read when it runs, not here, so a hook a test sets after construction
    // (setLoadOrCreateKeyRaceHook) still runs.
    this.keys =
        new JdbcSubjectKeyStore(
            this.tableName,
            PostgresCryptoDialect.INSTANCE,
            new JdbcForgottenSubjects(PostgresCryptoDialect.INSTANCE),
            () -> {
              Runnable h = raceHook;
              if (h != null) {
                h.run();
              }
            });
    this.verified = new VerifiedCipher(metrics, log, cipherFactory);
    this.format = new AesGcmV1Format(verified);
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
    try (Connection conn = dataSource.getConnection()) {
      // The mint commits in its own transaction before any cipher work: an encryption discarded
      // below leaves a committed key that the caller's retry loads (no second mint).
      key = inTransaction(conn, c -> keys.loadOrCreateKey(c, subjectId));
      result = format.seal(key.key(), key.version(), plaintext);
    } catch (KeyNotFoundException | SubjectForgottenException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Encryption failed for subject: " + subjectId.redacted(), e);
    }
    // The check reads the value the way decrypt does — its header against the key's generation,
    // then its value with a second, independent cipher. The header check runs inside the one
    // attempt, so a rejection fails the check as transient like any other failure of it.
    verified.verify(
        subjectId,
        plaintext,
        () -> {
          AesGcmV1Format.validateHeader(subjectId.redacted(), result, key.version());
          return format.openOnce(key.key(), result);
        });
    return result;
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (ciphertext == null) {
      throw new IllegalArgumentException("ciphertext must not be null");
    }
    try (Connection conn = dataSource.getConnection()) {
      VersionedKey key = loadKey(conn, subjectId);
      AesGcmV1Format.validateHeader(subjectId.redacted(), ciphertext, key.version());
      return verified.openWithOneRetry(subjectId, () -> format.openOnce(key.key(), ciphertext));
    } catch (KeyNotFoundException | CryptoOperationException e) {
      throw e;
    } catch (AEADBadTagException e) {
      // A GCM tag mismatch on both attempts is DETERMINISTIC — the blob is corrupted, tampered,
      // or was written under a different key — and retrying can never succeed. The family
      // classifiers (SagaStateConversion, ReadPoisonClassifier, hasTransientCryptoCause) read a
      // BARE CryptoOperationException as key-store OUTAGE evidence and retry forever; the
      // deterministic subtype is what routes this to poison/quarantine instead.
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

  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    // One transaction: under the per-subject advisory lock that encrypt's mint and reinstate also
    // take, record the destroyed key's generation (erased_key_generations), delete the key and
    // write the tombstone. All three commit together or none does: a committed delete without its
    // tombstone would let the next encrypt mint a fresh key for a forgotten subject.
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      inTransaction(
          conn,
          c -> {
            keys.eraseKey(c, subjectId);
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Failed to delete key for subject: " + subjectId.redacted(), e);
    } finally {
      closeQuietly(conn);
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
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
    // The DELETE runs under the same per-subject advisory lock deleteKey() and encrypt()'s mint
    // take, so a concurrent erasure or mint for this subject cannot interleave with it.
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      inTransaction(
          conn,
          c -> {
            keys.liftTombstone(c, subjectId);
            return null;
          });
    } catch (Exception e) {
      throw new CryptoOperationException("Failed to reinstate subject: " + subjectId.redacted(), e);
    } finally {
      closeQuietly(conn);
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
    if (conn == null) {
      return;
    }
    try {
      conn.close();
    } catch (java.sql.SQLException _) {
      // Nothing actionable on close failure.
    }
  }

  @Override
  public java.util.Set<String> requiredCryptoTables() {
    // This backend stores per-subject keys and crypto-shredding tombstones in the
    // shared crypto schema (the crypto Flyway baseline at db/crypto-migration). Report
    // the CONFIGURED key-table name so SchemaValidator fails fast at boot if a custom-named table
    // was configured but not created (the migration only creates the default-named table — see
    // Builder#tableName).
    return keys.requiredTables();
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    try (Connection conn = dataSource.getConnection()) {
      return keys.hasKey(conn, subjectId);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "isKeyAvailable failed for subject: " + subjectId.redacted(), e);
    }
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

  private VersionedKey loadKey(Connection conn, SubjectId subjectId) throws SQLException {
    VersionedKey key = keys.loadKeyOrNull(conn, subjectId);
    if (key == null) {
      throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
    }
    return key;
  }

  public static final class Builder {
    private DataSource dataSource;
    private String tableName = DEFAULT_TABLE_NAME;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private VerifiedCipher.CipherFactory cipherFactory = VerifiedCipher.FRESH_CIPHERS;

    public Builder dataSource(DataSource ds) {
      this.dataSource = ds;
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

    /** Test-only seam (package-private): creates the cipher of every operation. */
    Builder cipherFactory(VerifiedCipher.CipherFactory cipherFactory) {
      this.cipherFactory = cipherFactory;
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
      if (dataSource == null) {
        throw new IllegalStateException("dataSource is required");
      }
      return new PostgresCryptoEngine(dataSource, tableName, metrics, cipherFactory);
    }
  }
}

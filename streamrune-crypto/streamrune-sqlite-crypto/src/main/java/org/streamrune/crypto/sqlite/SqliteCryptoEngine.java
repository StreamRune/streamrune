package org.streamrune.crypto.sqlite;

import java.sql.SQLException;
import java.time.Clock;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import javax.crypto.AEADBadTagException;
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
import org.streamrune.crypto.AesGcmV1Format;
import org.streamrune.crypto.JdbcForgottenSubjects;
import org.streamrune.crypto.JdbcSubjectKeyStore;
import org.streamrune.crypto.VerifiedCipher;
import org.streamrune.crypto.VersionedKey;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteSchemaSeries;

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

  private static final Logger log = LoggerFactory.getLogger(SqliteCryptoEngine.class);

  private final SqliteCryptoStatements statements;
  private final String tableName;

  // The key store's statements (load, mint, erase, lift tombstone), shared with the PostgreSQL
  // engine; its writes run on the connection SqliteCryptoStatements hands out, inside this engine's
  // transactions, while decrypt and isKeyAvailable read outside one.
  // The SQLite dialect carries this engine's clock and takes no per-subject lock: the write lock
  // of the transaction is the lock (see the class Javadoc).
  private final JdbcSubjectKeyStore keys;

  // The cipher seam, the encryption self-check and the one-retry decryption, shared with the other
  // AES-GCM engines; its WARN lines go to this engine's logger.
  private final VerifiedCipher verified;

  // The v1 wire format (see the class comment), sealing and opening through the cipher seam above.
  private final AesGcmV1Format format;

  private SqliteCryptoEngine(Builder builder) {
    this.tableName = validateTableName(builder.tableName);
    this.statements =
        new SqliteCryptoStatements(
            builder.database.dataSource(), builder.database.file(), "crypto key-store write");
    SqliteCryptoDialect dialect = new SqliteCryptoDialect(builder.clock);
    this.keys =
        new JdbcSubjectKeyStore(
            this.tableName, dialect, new JdbcForgottenSubjects(dialect), () -> {});
    this.verified = new VerifiedCipher(builder.metrics, log, builder.cipherFactory);
    this.format = new AesGcmV1Format(verified);
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
      result = format.seal(key.key(), key.version(), plaintext);
    } catch (KeyNotFoundException | SubjectForgottenException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Encryption failed for subject: "
              + subjectId.redacted()
              + SqliteCryptoStatements.lockAdvice(e),
          e);
    }
    // The check reads the value the way decrypt does: its header against the key's generation,
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

  /**
   * The subject's current key: read without the write lock when it exists, otherwise minted in a
   * write transaction that reads the key again, checks the tombstone and inserts the key.
   */
  private VersionedKey keyForEncryption(SubjectId subjectId) throws SQLException {
    VersionedKey existing = statements.read(conn -> keys.loadKeyOrNull(conn, subjectId));
    if (existing != null) {
      return existing;
    }
    return statements.write(conn -> keys.loadOrCreateKey(conn, subjectId));
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
      VersionedKey key = statements.read(conn -> keys.loadKeyOrNull(conn, subjectId));
      if (key == null) {
        throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
      }
      AesGcmV1Format.validateHeader(subjectId.redacted(), ciphertext, key.version());
      return verified.openWithOneRetry(subjectId, () -> format.openOnce(key.key(), ciphertext));
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
            keys.eraseKey(conn, subjectId);
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
            keys.liftTombstone(conn, subjectId);
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
   * {@inheritDoc}
   *
   * <p>The configured key table, {@code forgotten_subjects} and {@code erased_key_generations}: the
   * tables of {@link SqliteSchemaSeries#CRYPTO}, the key table under its configured name.
   */
  @Override
  public Set<String> requiredCryptoTables() {
    return keys.requiredTables();
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    try {
      return statements.read(conn -> keys.hasKey(conn, subjectId));
    } catch (Exception e) {
      throw new CryptoOperationException(
          "isKeyAvailable failed for subject: " + subjectId.redacted(), e);
    }
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
    private VerifiedCipher.CipherFactory cipherFactory = VerifiedCipher.FRESH_CIPHERS;

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
    Builder cipherFactory(VerifiedCipher.CipherFactory cipherFactory) {
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

package org.streamrune.crypto.sqlite;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.projection.AtomicBatchProcessor.ProjectionUpdater;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.VerifiedCipher;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteProjectionRepository;
import org.streamrune.sqlite.SqliteReadModelCheckpoints;
import org.streamrune.sqlite.SqliteSchemaSeries;

/**
 * Freezes today's observable behaviour of {@link SqliteCryptoEngine} and {@link
 * SqliteForgottenSubjectStore} before the crypto twin extraction moves their cipher, wire-format
 * and JDBC logic into shared helpers. Every assertion here pins what the code does today —
 * exception type, exact message (with the {@code lockAdvice} suffix), cause versus suppressed,
 * metric calls, WARN text and logger category, the statements of each transaction in order and the
 * connection they ran on, batch-join semantics, key-buffer zeroing and a committed v1 blob. The
 * refactoring must keep it green; only the seam-installation lines ({@link #engineWith} and {@link
 * FaultyCiphers}) may change.
 *
 * <p>Everything runs on a real SQLite file through the {@link RecordingDataSource}, which observes
 * each connection's statements and transaction control without changing them.
 */
class SqliteCryptoCharacterizationTest {

  // ── v1 fixture: the Postgres constants verbatim, so both backends prove the same blob ────────

  static final String FIXTURE_KEY_HEX =
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
  static final int FIXTURE_KEY_VERSION = 1;
  static final String FIXTURE_PLAINTEXT = "characterization fixture v1";
  static final String FIXTURE_BLOB_HEX =
      "01011e775be467702a2cb2ee23d5d5b02a29ddca96d6cd67f87d797a0172ae355c330edf58fa9ddcac9d7262"
          + "272d28e6c9a1fd844b0edcc91c";

  private static final SubjectId S = SubjectId.of("characterization-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);
  private static final ProjectionName P = ProjectionName.of("characterization");

  // ── the engine's SQL, verbatim (default key table) ──────────────────────────────────────────

  private static final String SELECT_KEY =
      "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?";
  private static final String SELECT_TOMBSTONE =
      "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
  private static final String SELECT_MAX_ERASED =
      "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?";
  private static final String INSERT_KEY =
      "INSERT INTO encryption_keys (subject_id, key_bytes, created_at, key_version)"
          + " VALUES (?, ?, ?, ?) ON CONFLICT (subject_id) DO NOTHING";
  private static final String SELECT_KEY_AVAILABLE =
      "SELECT 1 FROM encryption_keys WHERE subject_id = ?";
  private static final String SELECT_KEY_VERSION =
      "SELECT key_version FROM encryption_keys WHERE subject_id = ?";
  private static final String UPSERT_ERASED_GENERATION =
      "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
          + " max(erased_key_generations.max_erased_version,"
          + " excluded.max_erased_version)";
  private static final String DELETE_KEY = "DELETE FROM encryption_keys WHERE subject_id = ?";
  private static final String INSERT_TOMBSTONE =
      "INSERT INTO forgotten_subjects (subject_id, forgotten_at, key_created_at)"
          + " VALUES (?, ?, ?) ON CONFLICT (subject_id) DO NOTHING";
  private static final String DELETE_TOMBSTONE =
      "DELETE FROM forgotten_subjects WHERE subject_id = ?";

  // ── SqliteForgottenSubjectStore's SQL, verbatim ─────────────────────────────────────────────

  private static final String FORGET_SQL =
      "INSERT INTO forgotten_subjects (subject_id, forgotten_at, key_created_at)"
          + " VALUES (?, ?, ?) ON CONFLICT (subject_id) DO NOTHING";
  private static final String IS_FORGOTTEN_SQL =
      "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
  private static final String REINSTATE_SQL = "DELETE FROM forgotten_subjects WHERE subject_id = ?";
  private static final String FORGOTTEN_AT_SQL =
      "SELECT forgotten_at FROM forgotten_subjects WHERE subject_id = ?";
  private static final String KEY_CREATED_AT_SQL =
      "SELECT key_created_at FROM forgotten_subjects WHERE subject_id = ?";

  // ── messages, verbatim ──────────────────────────────────────────────────────────────────────

  /** {@code SqliteCryptoStatements.lockAdvice} for a refused write lock. */
  private static final String LOCK_ADVICE =
      " — SQLite's write lock was not granted within busy_timeout. Another connection held"
          + " it that long, or this thread holds it itself: inside an application transaction on"
          + " the same database file (a connection in manual-commit mode), the crypto store cannot"
          + " write. Write @Encrypted values of a subject without a key outside such a"
          + " transaction, or raise busy_timeout";

  /** {@code SqliteCryptoStatements.ERASURE_INSIDE_A_BATCH}. */
  private static final String ERASURE_INSIDE_A_BATCH =
      "An erasure (deleteKey, reinstate, or a forgotten-subject tombstone write) cannot run inside a"
          + " projection batch on the same SQLite database file: it would commit or roll back with"
          + " the batch, so it would not be durable when it returns, and a cache or a key backend"
          + " acting on its return could act on an erasure that never commits. Nothing was written."
          + " Run the erasure after the batch, from a command handler or a saga step.";

  @TempDir Path dir;

  private RecordingDataSource dataSource;
  private SqliteDatabase database;
  private SqliteCryptoEngine plainEngine;
  private final CountingMetrics metrics = new CountingMetrics();

  @BeforeEach
  void setUp() {
    dataSource = new RecordingDataSource(SqliteFiles.dataSource(dir.resolve("character.db")));
    database = SqliteFiles.migrated(dataSource);
    plainEngine = SqliteCryptoEngine.builder().database(database).build();
    dataSource.resetRecording();
  }

  @AfterEach
  void tearDown() {
    database.close();
  }

  /**
   * The seam installation: how a faulty {@link VerifiedCipher.CipherFactory} reaches the engine —
   * today the package-private builder method, as {@code SqliteCryptoEngineTest} installs it.
   */
  private SqliteCryptoEngine engineWith(FaultyCiphers ciphers) {
    return SqliteCryptoEngine.builder()
        .database(database)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  // ── encrypt / decrypt failure ladder ────────────────────────────────────────────────────────

  @Test
  void encrypt_cipherFailure_isTransientWithTheCipherExceptionAsCause() {
    GeneralSecurityException injected = new GeneralSecurityException("injected encrypt failure");
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.ENCRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      // lockAdvice of a cipher failure is the empty string: nothing follows the subject.
      assertEquals("Encryption failed for subject: " + S.redacted(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(List.of(), log.warnings());
    }
  }

  @Test
  void encrypt_selfCheckMismatch_isTransient_noCause_countsAndWarns() {
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptingInstead(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals(selfCheckMessage(), failure.getMessage());
      assertNull(failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(1, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries);
      assertSelfCheckWarning(log.warnings());
    }
  }

  @Test
  void encrypt_selfCheckCheckedFailure_isTheCause() {
    AEADBadTagException injected = new AEADBadTagException("injected check failure");
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals(selfCheckMessage(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(0, injected.getSuppressed().length);
      assertEquals(1, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries, "a failed self-check is never retried");
      assertSelfCheckWarning(log.warnings());
    }
  }

  @Test
  void encrypt_selfCheckUncheckedFailure_isSuppressed() {
    IllegalStateException injected = new IllegalStateException("injected check failure");
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals(selfCheckMessage(), failure.getMessage());
      assertNull(failure.getCause());
      assertEquals(1, failure.getSuppressed().length);
      assertSame(injected, failure.getSuppressed()[0]);
      assertEquals(1, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries, "a failed self-check is never retried");
      assertSelfCheckWarning(log.warnings());
    }
  }

  @Test
  void decrypt_tagFailsTwice_isCryptoMapping_withFirstSuppressed_countsFailed() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE).wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoMappingException failure =
          assertThrows(CryptoMappingException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoMappingException.class, failure.getClass());
      assertEquals(
          "Decryption failed for subject: "
              + S.redacted()
              + " — AEAD tag mismatch: the ciphertext is corrupted, tampered, or was written under"
              + " a different key (deterministic; retrying cannot succeed)",
          failure.getMessage());
      assertEquals(AEADBadTagException.class, failure.getCause().getClass());
      assertEquals(0, failure.getSuppressed().length);
      Throwable[] suppressed = failure.getCause().getSuppressed();
      assertEquals(1, suppressed.length, "the first attempt's failure");
      assertEquals(AEADBadTagException.class, suppressed[0].getClass());
      assertNotSame(failure.getCause(), suppressed[0]);
      assertEquals(List.of("FAILED"), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertRetryWarning(log.warnings(), "FAILED");
    }
  }

  @Test
  void decrypt_tagFailsOnce_recovers_countsRecovered() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    SqliteCryptoEngine engine = engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      assertArrayEquals(PLAINTEXT, engine.decrypt(S, blob));

      assertEquals(List.of("RECOVERED"), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertRetryWarning(log.warnings(), "RECOVERED");
    }
  }

  @Test
  void decrypt_nonTagCipherFailure_isTransient_notRetried() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    NoSuchPaddingException injected = new NoSuchPaddingException("injected decrypt failure");
    SqliteCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals("Decryption failed for subject: " + S.redacted(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertEquals(List.of(), log.warnings());
    }
  }

  /**
   * An application transaction ({@code BEGIN IMMEDIATE} on a connection of its own) holds the
   * file's write lock on this thread: the mint's {@code BEGIN IMMEDIATE} waits {@code busy_timeout}
   * and fails, and the message carries the lock advice.
   */
  @Test
  void encrypt_mintInsideApplicationTransactionOnTheSameFile_failsWithLockAdvice()
      throws Exception {
    try (SqliteDatabase impatient =
        SqliteFiles.migrated(
            SqliteFiles.dataSource(dir.resolve("application.db"), "busy_timeout=300"))) {
      SqliteCryptoEngine engine =
          SqliteCryptoEngine.builder().database(impatient).metrics(metrics).build();

      try (LogCapture log = LogCapture.attach(SqliteCryptoEngine.class);
          Connection application = impatient.dataSource().getConnection();
          Statement statement = application.createStatement()) {
        statement.execute("BEGIN IMMEDIATE");
        CryptoOperationException failure;
        try {
          failure =
              assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));
        } finally {
          statement.execute("ROLLBACK");
        }

        assertEquals(CryptoOperationException.class, failure.getClass());
        assertEquals(
            "Encryption failed for subject: " + S.redacted() + LOCK_ADVICE, failure.getMessage());
        assertEquals(SQLiteException.class, failure.getCause().getClass());
        assertEquals(
            SQLiteErrorCode.SQLITE_BUSY.code,
            ((SQLiteException) failure.getCause()).getResultCode().code & 0xff);
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(0, metrics.selfCheckFailures.get());
        assertEquals(List.of(), metrics.decryptRetries);
        assertEquals(List.of(), log.warnings());
      }
      assertFalse(engine.isKeyAvailable(S), "the refused mint wrote no key");
    }
  }

  /**
   * The engine's reads and writes when the database is unreachable: the write-path messages carry
   * {@code lockAdvice} (empty for a plain failure, the advice for {@code SQLITE_BUSY}); decrypt and
   * isKeyAvailable never do.
   */
  @Test
  void engineStorageFailures_pinTheirMessages_withLockAdviceOnTheWritePathOnly() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    SQLException outage = new SQLException("injected outage");
    SQLException busy = new SQLiteException("injected busy", SQLiteErrorCode.SQLITE_BUSY);
    String encrypt = "Encryption failed for subject: " + S.redacted();
    String delete = "Failed to delete key for subject: " + S.redacted();
    String reinstate = "Failed to reinstate subject: " + S.redacted();
    String decrypt = "Decryption failed for subject: " + S.redacted();
    String available = "isKeyAvailable failed for subject: " + S.redacted();

    dataSource.failConnections(outage);
    assertFailure(outage, encrypt, () -> plainEngine.encrypt(S, PLAINTEXT));
    assertFailure(outage, delete, () -> plainEngine.deleteKey(S));
    assertFailure(outage, reinstate, () -> plainEngine.reinstate(S));
    assertFailure(outage, decrypt, () -> plainEngine.decrypt(S, blob));
    assertFailure(outage, available, () -> plainEngine.isKeyAvailable(S));

    dataSource.failConnections(busy);
    assertFailure(busy, encrypt + LOCK_ADVICE, () -> plainEngine.encrypt(S, PLAINTEXT));
    assertFailure(busy, delete + LOCK_ADVICE, () -> plainEngine.deleteKey(S));
    assertFailure(busy, reinstate + LOCK_ADVICE, () -> plainEngine.reinstate(S));
    assertFailure(busy, decrypt, () -> plainEngine.decrypt(S, blob));
    assertFailure(busy, available, () -> plainEngine.isKeyAvailable(S));
  }

  private static String selfCheckMessage() {
    return "Encryption self-check failed for subject: "
        + S.redacted()
        + " — the encrypted value did not decrypt to its plaintext and is discarded"
        + " (transient; retrying encrypts it again)";
  }

  private static void assertSelfCheckWarning(List<ILoggingEvent> warnings) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(SqliteCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Encryption self-check failed for subject "
            + S.redacted()
            + ": the encrypted value did not decrypt to its plaintext and is discarded; the"
            + " operation fails as transient and is retried",
        warnings.get(0).getFormattedMessage());
  }

  private static void assertRetryWarning(List<ILoggingEvent> warnings, String outcome) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(SqliteCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Decryption for subject "
            + S.redacted()
            + " failed its authentication tag and was retried once with a fresh cipher; outcome "
            + outcome,
        warnings.get(0).getFormattedMessage());
  }

  // ── golden SQL: the engine ──────────────────────────────────────────────────────────────────

  /**
   * The read outside a transaction on one connection, then the mint's write transaction on a
   * second: {@code BEGIN IMMEDIATE}, every statement, {@code COMMIT}, all on that connection.
   */
  @Test
  void goldenSql_firstEncryptMints() {
    plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(
        List.of(SELECT_KEY, SELECT_KEY, SELECT_TOMBSTONE, SELECT_MAX_ERASED, INSERT_KEY),
        dataSource.sqlSeen());
    assertEquals(
        concat(
            readOn(1, SELECT_KEY),
            writeTransactionOn(
                2, "COMMIT", SELECT_KEY, SELECT_TOMBSTONE, SELECT_MAX_ERASED, INSERT_KEY)),
        dataSource.transcript());
  }

  @Test
  void goldenSql_encryptWithExistingKey() {
    plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(List.of(SELECT_KEY), dataSource.sqlSeen());
    assertEquals(readOn(1, SELECT_KEY), dataSource.transcript());
  }

  @Test
  void goldenSql_decrypt() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.decrypt(S, blob);

    assertEquals(List.of(SELECT_KEY), dataSource.sqlSeen());
    assertEquals(readOn(1, SELECT_KEY), dataSource.transcript());
  }

  @Test
  void goldenSql_isKeyAvailable() {
    plainEngine.isKeyAvailable(S);

    assertEquals(List.of(SELECT_KEY_AVAILABLE), dataSource.sqlSeen());
    assertEquals(readOn(1, SELECT_KEY_AVAILABLE), dataSource.transcript());
  }

  @Test
  void goldenSql_deleteKeyOfASubjectWithAKey() {
    plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.deleteKey(S);

    assertEquals(
        List.of(SELECT_KEY_VERSION, UPSERT_ERASED_GENERATION, DELETE_KEY, INSERT_TOMBSTONE),
        dataSource.sqlSeen());
    assertEquals(
        writeTransactionOn(
            1,
            "COMMIT",
            SELECT_KEY_VERSION,
            UPSERT_ERASED_GENERATION,
            DELETE_KEY,
            INSERT_TOMBSTONE),
        dataSource.transcript());
  }

  @Test
  void goldenSql_deleteKeyOfASubjectWithoutAKey() {
    plainEngine.deleteKey(S);

    assertEquals(List.of(SELECT_KEY_VERSION, DELETE_KEY, INSERT_TOMBSTONE), dataSource.sqlSeen());
    assertEquals(
        writeTransactionOn(1, "COMMIT", SELECT_KEY_VERSION, DELETE_KEY, INSERT_TOMBSTONE),
        dataSource.transcript());
  }

  @Test
  void goldenSql_reinstate() {
    plainEngine.reinstate(S);

    assertEquals(List.of(DELETE_TOMBSTONE), dataSource.sqlSeen());
    assertEquals(writeTransactionOn(1, "COMMIT", DELETE_TOMBSTONE), dataSource.transcript());
  }

  // ── golden SQL: the store ───────────────────────────────────────────────────────────────────

  @Test
  void goldenSql_storeForget() throws Exception {
    new SqliteForgottenSubjectStore(database).forget(S);

    assertEquals(List.of(FORGET_SQL), dataSource.sqlSeen());
    assertEquals(writeTransactionOn(1, "COMMIT", FORGET_SQL), dataSource.transcript());
    assertEquals(
        List.of(Arrays.asList(S.redacted(), null)),
        SqliteFiles.query(dataSource, "SELECT subject_id, key_created_at FROM forgotten_subjects"),
        "forget(s) is the three-column insert with NULL evidence");
  }

  @Test
  void goldenSql_storeForgetWithKeyEvidence() throws Exception {
    Instant now = Instant.parse("2026-11-26T09:00:00.000001Z");
    Instant evidence = Instant.parse("2026-11-26T10:00:00.000002Z");
    new SqliteForgottenSubjectStore(database, Clock.fixed(now, ZoneOffset.UTC)).forget(S, evidence);

    assertEquals(List.of(FORGET_SQL), dataSource.sqlSeen());
    assertEquals(writeTransactionOn(1, "COMMIT", FORGET_SQL), dataSource.transcript());
    assertEquals(
        List.of(List.of(S.redacted(), micros(now), micros(evidence))),
        SqliteFiles.query(
            dataSource, "SELECT subject_id, forgotten_at, key_created_at FROM forgotten_subjects"));
  }

  @Test
  void goldenSql_storeIsForgotten() {
    new SqliteForgottenSubjectStore(database).isForgotten(S);

    assertEquals(List.of(IS_FORGOTTEN_SQL), dataSource.sqlSeen());
    assertEquals(readOn(1, IS_FORGOTTEN_SQL), dataSource.transcript());
  }

  @Test
  void goldenSql_storeReinstate() {
    new SqliteForgottenSubjectStore(database).reinstate(S);

    assertEquals(List.of(REINSTATE_SQL), dataSource.sqlSeen());
    assertEquals(writeTransactionOn(1, "COMMIT", REINSTATE_SQL), dataSource.transcript());
  }

  @Test
  void goldenSql_storeForgottenAt() {
    new SqliteForgottenSubjectStore(database).forgottenAt(S);

    assertEquals(List.of(FORGOTTEN_AT_SQL), dataSource.sqlSeen());
    assertEquals(readOn(1, FORGOTTEN_AT_SQL), dataSource.transcript());
  }

  @Test
  void goldenSql_storeKeyCreatedAt() {
    new SqliteForgottenSubjectStore(database).keyCreatedAt(S);

    assertEquals(List.of(KEY_CREATED_AT_SQL), dataSource.sqlSeen());
    assertEquals(readOn(1, KEY_CREATED_AT_SQL), dataSource.transcript());
  }

  /** A read: borrow connection {@code c}, prepare each statement on it (autocommit), return it. */
  static List<String> readOn(int c, String... sql) {
    List<String> calls = new ArrayList<>();
    calls.add("#" + c + " getConnection");
    for (String statement : sql) {
      calls.add("#" + c + " prepare " + statement);
    }
    calls.add("#" + c + " close");
    return calls;
  }

  /**
   * ONE write transaction on ONE connection {@code c}, as {@code SqliteTransactions.inTransaction}
   * runs it: borrow, {@code BEGIN IMMEDIATE} as a plain statement, every statement prepared on that
   * connection, {@code end} ({@code COMMIT} or {@code ROLLBACK}) as a plain statement, return it.
   * The driver's autocommit flag is never touched. A statement sent through another connection, or
   * a second transaction, fails the comparison.
   */
  static List<String> writeTransactionOn(int c, String end, String... sql) {
    List<String> calls = new ArrayList<>();
    calls.add("#" + c + " getConnection");
    calls.add("#" + c + " execute BEGIN IMMEDIATE");
    for (String statement : sql) {
      calls.add("#" + c + " prepare " + statement);
    }
    calls.add("#" + c + " execute " + end);
    calls.add("#" + c + " close");
    return calls;
  }

  @SafeVarargs
  private static List<String> concat(List<String>... parts) {
    List<String> all = new ArrayList<>();
    for (List<String> part : parts) {
      all.addAll(part);
    }
    return all;
  }

  // ── inside a projection batch ───────────────────────────────────────────────────────────────

  /**
   * A mint inside a running projection batch joins the batch's connection and transaction: no
   * connection is borrowed, no BEGIN IMMEDIATE or COMMIT of its own (only the open-transaction
   * probe {@code BEGIN}, which SQLite refuses inside the open transaction). A {@link SQLException}
   * of its key INSERT aborts the batch with that very exception, unwrapped, as the cause; the
   * engine's own failure carries it as its cause too; the batch rolls back and writes no key.
   */
  @Test
  void aWriteInsideABatchThatFailsWithSqlException_abortsTheBatch_unwrapped() throws Exception {
    Batch batch = new Batch();
    SQLException injected = new SQLException("injected key insert failure");
    dataSource.failAt(INSERT_KEY, injected);
    dataSource.resetRecording();
    var engineFailure = new AtomicReference<RuntimeException>();
    var during = new AtomicReference<List<RecordingDataSource.Call>>();
    var before = new AtomicReference<List<RecordingDataSource.Call>>();

    EventStoreException failure =
        assertThrows(
            EventStoreException.class,
            () ->
                batch.run(
                    1,
                    tx -> {
                      before.set(dataSource.calls());
                      try {
                        plainEngine.encrypt(S, PLAINTEXT);
                      } catch (RuntimeException e) {
                        engineFailure.set(e); // the updater swallows it and returns
                      }
                      List<RecordingDataSource.Call> all = dataSource.calls();
                      during.set(all.subList(before.get().size(), all.size()));
                    }));

    // The engine: its failure wraps the injected exception directly.
    RuntimeException engineError = engineFailure.get();
    assertEquals(CryptoOperationException.class, engineError.getClass());
    assertEquals("Encryption failed for subject: " + S.redacted(), engineError.getMessage());
    assertSame(injected, engineError.getCause());

    // The batch: aborted, with the very SQLException the statement threw at the bottom.
    String joinedFailure =
        "A statement run on the projection transaction's connection by a component joining it"
            + " failed";
    assertEquals(EventStoreException.class, failure.getClass());
    assertEquals(
        "The projection transaction was aborted by an earlier failure, so it runs no further"
            + " statement and rolls back: "
            + joinedFailure,
        failure.getMessage());
    assertEquals(EventStoreException.class, failure.getCause().getClass());
    assertEquals(joinedFailure, failure.getCause().getMessage());
    assertSame(injected, failure.getCause().getCause(), "unwrapped");

    // The statements: on the batch's open connection, inside its transaction.
    assertJoinedTheBatch(
        before.get(),
        during.get(),
        List.of(
            "prepare " + SELECT_KEY,
            "execute BEGIN",
            "prepare " + SELECT_KEY,
            "prepare " + SELECT_TOMBSTONE,
            "prepare " + SELECT_MAX_ERASED,
            "prepare " + INSERT_KEY));

    assertFalse(plainEngine.isKeyAvailable(S), "no key: the batch rolled back");
    assertEquals(GlobalOffset.initial(), batch.checkpoints.getLastOffset(P));
  }

  /** The successful mint inside a batch: the same joined statements, committed with the batch. */
  @Test
  void goldenSql_mintInsideABatch_joinsTheBatchTransaction() {
    Batch batch = new Batch();
    dataSource.resetRecording();
    var during = new AtomicReference<List<RecordingDataSource.Call>>();
    var before = new AtomicReference<List<RecordingDataSource.Call>>();

    batch.run(
        1,
        tx -> {
          before.set(dataSource.calls());
          plainEngine.encrypt(S, PLAINTEXT);
          List<RecordingDataSource.Call> all = dataSource.calls();
          during.set(all.subList(before.get().size(), all.size()));
        });

    assertJoinedTheBatch(
        before.get(),
        during.get(),
        List.of(
            "prepare " + SELECT_KEY,
            "execute BEGIN",
            "prepare " + SELECT_KEY,
            "prepare " + SELECT_TOMBSTONE,
            "prepare " + SELECT_MAX_ERASED,
            "prepare " + INSERT_KEY));
    assertTrue(plainEngine.isKeyAvailable(S), "the key committed with the batch");
    assertEquals(GlobalOffset.of(1), batch.checkpoints.getLastOffset(P));
  }

  @Test
  void deleteKeyAndReinstateInsideABatch_throwIllegalState() {
    Batch batch = new Batch();
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    plainEngine.deleteKey(SubjectId.of("still-forgotten"));
    dataSource.resetRecording();
    List<Throwable> refusals = new ArrayList<>();
    var during = new AtomicReference<List<RecordingDataSource.Call>>();

    batch.run(
        1,
        tx -> {
          int before = dataSource.calls().size();
          refusals.add(refusal(() -> plainEngine.deleteKey(S)));
          refusals.add(refusal(() -> plainEngine.reinstate(SubjectId.of("still-forgotten"))));
          List<RecordingDataSource.Call> all = dataSource.calls();
          during.set(all.subList(before, all.size()));
        });

    assertRefusals(refusals, 2);
    assertEquals(List.of(), during.get(), "nothing ran");
    assertEquals(GlobalOffset.of(1), batch.checkpoints.getLastOffset(P), "the batch committed");
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob), "the key is untouched");
    assertThrows(
        SubjectForgottenException.class,
        () -> plainEngine.encrypt(SubjectId.of("still-forgotten"), PLAINTEXT));
  }

  @Test
  void storeForgetAndReinstateInsideABatch_throwIllegalState() {
    Batch batch = new Batch();
    var store = new SqliteForgottenSubjectStore(database);
    store.forget(SubjectId.of("forgotten"));
    dataSource.resetRecording();
    List<Throwable> refusals = new ArrayList<>();
    var during = new AtomicReference<List<RecordingDataSource.Call>>();

    batch.run(
        1,
        tx -> {
          int before = dataSource.calls().size();
          refusals.add(refusal(() -> store.forget(S)));
          refusals.add(refusal(() -> store.forget(S, Instant.EPOCH)));
          refusals.add(refusal(() -> store.reinstate(SubjectId.of("forgotten"))));
          List<RecordingDataSource.Call> all = dataSource.calls();
          during.set(all.subList(before, all.size()));
        });

    assertRefusals(refusals, 3);
    assertEquals(List.of(), during.get(), "nothing ran");
    assertFalse(store.isForgotten(S));
    assertTrue(store.isForgotten(SubjectId.of("forgotten")));
  }

  /**
   * The calls {@code during} the engine's work ran on one connection, the one the batch borrowed
   * before (and has not returned), borrowed none of their own, and are {@code expected} in order.
   */
  private static void assertJoinedTheBatch(
      List<RecordingDataSource.Call> before,
      List<RecordingDataSource.Call> during,
      List<String> expected) {
    assertEquals(
        expected,
        during.stream().map(RecordingDataSource.Call::call).toList(),
        () -> "the engine's calls inside the batch: " + during);
    assertEquals(
        1,
        during.stream().mapToInt(RecordingDataSource.Call::connection).distinct().count(),
        () -> "one connection: " + during);
    int batchConnection = during.get(0).connection();
    assertTrue(
        before.contains(new RecordingDataSource.Call(batchConnection, "getConnection")),
        () -> "the batch's own connection: " + before);
    assertFalse(
        before.contains(new RecordingDataSource.Call(batchConnection, "close")),
        () -> "still open: " + before);
  }

  private static Throwable refusal(Runnable erasure) {
    try {
      erasure.run();
      return null;
    } catch (RuntimeException e) {
      return e;
    }
  }

  private static void assertRefusals(List<Throwable> refusals, int count) {
    assertEquals(count, refusals.size());
    for (Throwable refusal : refusals) {
      assertEquals(IllegalStateException.class, refusal.getClass(), () -> "refusal: " + refusal);
      assertEquals(ERASURE_INSIDE_A_BATCH, refusal.getMessage());
      assertNull(refusal.getCause());
      assertEquals(0, refusal.getSuppressed().length);
    }
  }

  /** A projection batch on the test's database file, through the recording data source. */
  private final class Batch {
    final SqliteReadModelCheckpoints checkpoints;
    final SqliteProjectionRepository repo;

    Batch() {
      database.migrate(SqliteSchemaSeries.READ_MODEL);
      checkpoints = new SqliteReadModelCheckpoints(database);
      repo =
          new SqliteProjectionRepository(
              database,
              SqliteProjectionRepository.createObjectMapper(plainEngine, StreamRuneMetrics.NOOP),
              checkpoints);
    }

    void run(long offset, ProjectionUpdater updater) {
      repo.executeAtomically(P, List.of(), GlobalOffset.of(offset), 0L, updater, checkpoints);
    }
  }

  // ── key-buffer zeroing ──────────────────────────────────────────────────────────────────────

  @Test
  void loadedKeyBytesAreZeroedAfterUse() throws Exception {
    seedFixtureKey();
    dataSource.resetRecording();

    byte[] plaintext = plainEngine.decrypt(S, HexFormat.of().parseHex(FIXTURE_BLOB_HEX));

    assertEquals(FIXTURE_PLAINTEXT, new String(plaintext, StandardCharsets.UTF_8));
    assertEquals(1, dataSource.keyBytesHandedOut().size(), "only key_bytes is read as bytes");
    assertArrayEquals(new byte[32], dataSource.keyBytesHandedOut().get(0));
  }

  @Test
  void loadedKeyBytesAreZeroedAfterUse_onTheEncryptPath() throws Exception {
    seedFixtureKey();
    dataSource.resetRecording();

    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(1, dataSource.keyBytesHandedOut().size(), "the existing key, read once");
    assertArrayEquals(new byte[32], dataSource.keyBytesHandedOut().get(0));
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob));
  }

  @Test
  void mintedEncodedKeyIsZeroedAfterTheInsert() throws Exception {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(1, dataSource.keyBytesBound().size(), "only the key INSERT binds bytes");
    assertArrayEquals(new byte[32], dataSource.keyBytesBound().get(0));
    byte[] stored =
        (byte[])
            SqliteFiles.query(dataSource, "SELECT key_bytes FROM encryption_keys").get(0).get(0);
    assertEquals(32, stored.length);
    assertFalse(
        Arrays.equals(new byte[32], stored),
        "the stored key is the real one; only the engine's own buffer is zeroed");
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob));
  }

  // ── v1 wire format ──────────────────────────────────────────────────────────────────────────

  @Test
  void v1FixtureDecrypts() throws Exception {
    byte[] blob = HexFormat.of().parseHex(FIXTURE_BLOB_HEX);
    assertEquals(FIXTURE_KEY_VERSION, blob[1], "the fixture carries key version 1");
    seedFixtureKey();

    assertEquals(
        FIXTURE_PLAINTEXT, new String(plainEngine.decrypt(S, blob), StandardCharsets.UTF_8));
  }

  @Test
  void freshSealDecryptsWithAnIndependentJcaCipher() throws Exception {
    byte[] key = HexFormat.of().parseHex(FIXTURE_KEY_HEX);
    seedFixtureKey();
    byte[] plaintext = FIXTURE_PLAINTEXT.getBytes(StandardCharsets.UTF_8);

    byte[] blob = plainEngine.encrypt(S, plaintext);

    assertEquals(1, blob[0], "format version");
    assertEquals(FIXTURE_KEY_VERSION, blob[1], "key version");
    assertEquals(2 + 12 + plaintext.length + 16, blob.length);
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(
        Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, blob, 2, 12));
    assertArrayEquals(plaintext, cipher.doFinal(blob, 14, blob.length - 14));
  }

  /** The fixture key as S's key row, at generation {@link #FIXTURE_KEY_VERSION}. */
  private void seedFixtureKey() throws SQLException {
    SqliteFiles.execute(
        dataSource,
        "INSERT INTO encryption_keys (subject_id, key_bytes, created_at, key_version) VALUES ('"
            + S.redacted()
            + "', X'"
            + FIXTURE_KEY_HEX
            + "', 0, "
            + FIXTURE_KEY_VERSION
            + ")");
  }

  // ── store failure ladder ────────────────────────────────────────────────────────────────────

  @Test
  void storeForget_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to record forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).forget(S));
  }

  @Test
  void storeForgetWithKeyEvidence_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to record forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).forget(S, Instant.EPOCH));
  }

  @Test
  void storeIsForgotten_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to check forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).isForgotten(S));
  }

  @Test
  void storeReinstate_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to remove forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).reinstate(S));
  }

  @Test
  void storeForgottenAt_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to read forgotten-at timestamp for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).forgottenAt(S));
  }

  @Test
  void storeKeyCreatedAt_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = outage();

    assertFailure(
        outage,
        "Failed to read erased-key creation evidence for subject-hash=" + S.redacted(),
        () -> new SqliteForgottenSubjectStore(database).keyCreatedAt(S));
  }

  /** Every store method's message carries the lock advice when the failure is SQLITE_BUSY. */
  @Test
  void storeFailures_whenTheWriteLockIsNotGranted_carryTheLockAdvice() {
    var store = new SqliteForgottenSubjectStore(database);
    SQLException busy = new SQLiteException("injected busy", SQLiteErrorCode.SQLITE_BUSY);
    dataSource.failConnections(busy);
    String hash = " subject-hash=" + S.redacted() + LOCK_ADVICE;

    assertFailure(
        busy, "Failed to record forgotten-subject tombstone for" + hash, () -> store.forget(S));
    assertFailure(
        busy,
        "Failed to record forgotten-subject tombstone for" + hash,
        () -> store.forget(S, Instant.EPOCH));
    assertFailure(
        busy, "Failed to check forgotten-subject tombstone for" + hash, () -> store.isForgotten(S));
    assertFailure(
        busy, "Failed to remove forgotten-subject tombstone for" + hash, () -> store.reinstate(S));
    assertFailure(
        busy, "Failed to read forgotten-at timestamp for" + hash, () -> store.forgottenAt(S));
    assertFailure(
        busy,
        "Failed to read erased-key creation evidence for" + hash,
        () -> store.keyCreatedAt(S));
  }

  /** From now on every connection request fails with a plain {@link SQLException}. */
  private SQLException outage() {
    SQLException outage = new SQLException("injected outage");
    dataSource.failConnections(outage);
    return outage;
  }

  private static void assertFailure(SQLException cause, String message, Executable call) {
    CryptoOperationException failure = assertThrows(CryptoOperationException.class, call);
    assertEquals(CryptoOperationException.class, failure.getClass());
    assertEquals(message, failure.getMessage());
    assertSame(cause, failure.getCause());
    assertEquals(0, failure.getSuppressed().length);
  }

  private static long micros(Instant instant) {
    return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000;
  }

  // ── test doubles ────────────────────────────────────────────────────────────────────────────

  /** Counts the two meters the engine records. */
  private static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger selfCheckFailures = new AtomicInteger();
    final List<String> decryptRetries = new ArrayList<>();

    @Override
    public void recordEncryptSelfCheckFailed() {
      selfCheckFailures.incrementAndGet();
    }

    @Override
    public void recordDecryptRetried(String outcome) {
      decryptRetries.add(outcome);
    }
  }

  /** One queued fault for the next cipher of a mode. */
  private sealed interface Fault {
    record Throw(Exception failure) implements Fault {}

    record WrongKey() implements Fault {}

    record EncryptInstead() implements Fault {}
  }

  /**
   * Creates real AES/GCM ciphers; each queued fault applies to the next cipher of its mode, in
   * order: throw from {@code create}, initialise under a key of its own (a tag failure on decrypt),
   * or initialise in encrypt mode instead of decrypt mode (other bytes, no tag failure).
   */
  private static final class FaultyCiphers implements VerifiedCipher.CipherFactory {
    private final Map<Integer, Deque<Fault>> faults = new HashMap<>();

    FaultyCiphers throwing(int mode, Exception failure) {
      if (!(failure instanceof GeneralSecurityException || failure instanceof RuntimeException)) {
        throw new IllegalArgumentException("create throws only these: " + failure);
      }
      return queue(mode, new Fault.Throw(failure));
    }

    FaultyCiphers wrongKey(int mode) {
      return queue(mode, new Fault.WrongKey());
    }

    FaultyCiphers encryptingInstead(int mode) {
      return queue(mode, new Fault.EncryptInstead());
    }

    private FaultyCiphers queue(int mode, Fault fault) {
      faults.computeIfAbsent(mode, m -> new ArrayDeque<>()).add(fault);
      return this;
    }

    @Override
    public Cipher create(int mode, Key key, AlgorithmParameterSpec params)
        throws GeneralSecurityException {
      Fault fault = faults.getOrDefault(mode, new ArrayDeque<>()).poll();
      Key used = key;
      int usedMode = mode;
      switch (fault) {
        case null -> {}
        case Fault.Throw(Exception failure) -> {
          if (failure instanceof GeneralSecurityException checked) {
            throw checked;
          }
          throw (RuntimeException) failure;
        }
        case Fault.WrongKey() -> {
          KeyGenerator generator = KeyGenerator.getInstance("AES");
          generator.init(256);
          used = generator.generateKey();
        }
        case Fault.EncryptInstead() -> usedMode = Cipher.ENCRYPT_MODE;
      }
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(usedMode, used, params);
      return cipher;
    }
  }

  @Test
  void faultyCiphersWithoutFaultsBehavesLikeTheProductionFactory() {
    // Guards the test double itself: with nothing queued, the seam is transparent.
    SqliteCryptoEngine engine = engineWith(new FaultyCiphers());

    byte[] blob = engine.encrypt(S, PLAINTEXT);

    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob));
    assertEquals(0, metrics.selfCheckFailures.get());
    assertEquals(List.of(), metrics.decryptRetries);
  }
}

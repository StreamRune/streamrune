package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.VerifiedCipher;

/**
 * Freezes today's observable behaviour of {@link PostgresCryptoEngine} and {@link
 * JdbcForgottenSubjectStore} before the crypto twin extraction moves their cipher, wire-format and
 * JDBC logic into shared helpers. Every assertion here pins what the code does today — exception
 * type, exact message, cause versus suppressed, metric calls, WARN text and logger category, the
 * statements of each transaction in order, key-buffer zeroing and a committed v1 blob. The
 * refactoring must keep it green; only the seam-installation lines ({@link #engineWith} and {@link
 * FaultyCiphers}) may change.
 *
 * <p>Everything runs on the in-memory {@link RecordingDataSource}, so no Docker is needed.
 */
class PostgresCryptoCharacterizationTest {

  // ── v1 fixture: sealed once by today's engine; never regenerate ────────────────────────────

  static final String FIXTURE_KEY_HEX =
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
  static final int FIXTURE_KEY_VERSION = 1;
  static final String FIXTURE_PLAINTEXT = "characterization fixture v1";
  static final String FIXTURE_BLOB_HEX =
      "01011e775be467702a2cb2ee23d5d5b02a29ddca96d6cd67f87d797a0172ae355c330edf58fa9ddcac9d7262"
          + "272d28e6c9a1fd844b0edcc91c";

  private static final SubjectId S = SubjectId.of("characterization-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  // ── the engine's SQL, verbatim (default key table) ──────────────────────────────────────────

  private static final String LOCK = "SELECT pg_advisory_xact_lock(hashtext(?))";
  private static final String SELECT_KEY =
      "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?";
  private static final String SELECT_TOMBSTONE =
      "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
  private static final String SELECT_MAX_ERASED =
      "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?";
  private static final String INSERT_KEY =
      "INSERT INTO encryption_keys (subject_id, key_bytes, key_version) VALUES (?, ?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";
  private static final String SELECT_KEY_AVAILABLE =
      "SELECT 1 FROM encryption_keys WHERE subject_id = ?";
  private static final String SELECT_KEY_VERSION =
      "SELECT key_version FROM encryption_keys WHERE subject_id = ?";
  private static final String UPSERT_ERASED_GENERATION =
      "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
          + " GREATEST(erased_key_generations.max_erased_version,"
          + " EXCLUDED.max_erased_version)";
  private static final String DELETE_KEY = "DELETE FROM encryption_keys WHERE subject_id = ?";
  private static final String INSERT_TOMBSTONE =
      "INSERT INTO forgotten_subjects (subject_id, key_created_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";
  private static final String DELETE_TOMBSTONE =
      "DELETE FROM forgotten_subjects WHERE subject_id = ?";

  // ── JdbcForgottenSubjectStore's *_SQL constants, copied as literals ─────────────────────────

  private static final String FORGET_SQL =
      "INSERT INTO forgotten_subjects (subject_id, key_created_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";
  private static final String IS_FORGOTTEN_SQL =
      "SELECT 1 FROM forgotten_subjects WHERE subject_id = ?";
  private static final String REINSTATE_SQL = "DELETE FROM forgotten_subjects WHERE subject_id = ?";
  private static final String FORGOTTEN_AT_SQL =
      "SELECT forgotten_at FROM forgotten_subjects WHERE subject_id = ?";
  private static final String FORGET_WITH_KEY_EVIDENCE_SQL =
      "INSERT INTO forgotten_subjects (subject_id, key_created_at) VALUES (?, ?)"
          + " ON CONFLICT (subject_id) DO NOTHING";
  private static final String KEY_CREATED_AT_SQL =
      "SELECT key_created_at FROM forgotten_subjects WHERE subject_id = ?";

  private RecordingDataSource dataSource;
  private PostgresCryptoEngine plainEngine;
  private final CountingMetrics metrics = new CountingMetrics();

  @BeforeEach
  void setUp() {
    dataSource = new RecordingDataSource();
    plainEngine = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  /**
   * The seam installation: how a faulty {@link VerifiedCipher.CipherFactory} reaches the engine —
   * today the package-private builder method, as {@code PostgresCryptoEngineCipherIntegrationTest}
   * installs it.
   */
  private PostgresCryptoEngine engineWith(FaultyCiphers ciphers) {
    return PostgresCryptoEngine.builder()
        .dataSource(dataSource)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  // ── encrypt / decrypt failure ladder ────────────────────────────────────────────────────────

  @Test
  void encrypt_cipherFailure_isTransientWithTheCipherExceptionAsCause() {
    GeneralSecurityException injected = new GeneralSecurityException("injected encrypt failure");
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.ENCRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
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
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptingInstead(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
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
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals(selfCheckMessage(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(1, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries, "a failed self-check is never retried");
      assertSelfCheckWarning(log.warnings());
    }
  }

  @Test
  void encrypt_selfCheckUncheckedFailure_isSuppressed() {
    IllegalStateException injected = new IllegalStateException("injected check failure");
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
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
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE).wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
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
    PostgresCryptoEngine engine = engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
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
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(PostgresCryptoEngine.class)) {
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

  private static String selfCheckMessage() {
    return "Encryption self-check failed for subject: "
        + S.redacted()
        + " — the encrypted value did not decrypt to its plaintext and is discarded"
        + " (transient; retrying encrypts it again)";
  }

  private static void assertSelfCheckWarning(List<ILoggingEvent> warnings) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(PostgresCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Encryption self-check failed for subject "
            + S.redacted()
            + ": the encrypted value did not decrypt to its plaintext and is discarded; the"
            + " operation fails as transient and is retried",
        warnings.get(0).getFormattedMessage());
  }

  private static void assertRetryWarning(List<ILoggingEvent> warnings, String outcome) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(PostgresCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Decryption for subject "
            + S.redacted()
            + " failed its authentication tag and was retried once with a fresh cipher; outcome "
            + outcome,
        warnings.get(0).getFormattedMessage());
  }

  // ── golden SQL: the engine ──────────────────────────────────────────────────────────────────

  @Test
  void goldenSql_firstEncryptMints() {
    plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(
        List.of(LOCK, SELECT_KEY, SELECT_TOMBSTONE, SELECT_MAX_ERASED, INSERT_KEY),
        dataSource.sqlSeen());
    assertOneTransaction(
        List.of(LOCK, SELECT_KEY, SELECT_TOMBSTONE, SELECT_MAX_ERASED, INSERT_KEY), "commit");
  }

  @Test
  void goldenSql_encryptWithExistingKey() {
    plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(List.of(LOCK, SELECT_KEY), dataSource.sqlSeen());
    assertOneTransaction(List.of(LOCK, SELECT_KEY), "commit");
  }

  @Test
  void goldenSql_decrypt() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.decrypt(S, blob);

    assertEquals(List.of(SELECT_KEY), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_isKeyAvailable() {
    plainEngine.isKeyAvailable(S);

    assertEquals(List.of(SELECT_KEY_AVAILABLE), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_deleteKeyOfASubjectWithAKey() {
    plainEngine.encrypt(S, PLAINTEXT);
    dataSource.resetRecording();

    plainEngine.deleteKey(S);

    assertEquals(
        List.of(LOCK, SELECT_KEY_VERSION, UPSERT_ERASED_GENERATION, DELETE_KEY, INSERT_TOMBSTONE),
        dataSource.sqlSeen());
    assertOneTransaction(
        List.of(LOCK, SELECT_KEY_VERSION, UPSERT_ERASED_GENERATION, DELETE_KEY, INSERT_TOMBSTONE),
        "commit");
  }

  @Test
  void goldenSql_deleteKeyOfASubjectWithoutAKey() {
    plainEngine.deleteKey(S);

    assertEquals(
        List.of(LOCK, SELECT_KEY_VERSION, DELETE_KEY, INSERT_TOMBSTONE), dataSource.sqlSeen());
    assertOneTransaction(List.of(LOCK, SELECT_KEY_VERSION, DELETE_KEY, INSERT_TOMBSTONE), "commit");
  }

  @Test
  void goldenSql_reinstate() {
    plainEngine.reinstate(S);

    assertEquals(List.of(LOCK, DELETE_TOMBSTONE), dataSource.sqlSeen());
    assertOneTransaction(List.of(LOCK, DELETE_TOMBSTONE), "commit");
  }

  /**
   * Everything recorded since the last reset ran as ONE transaction on ONE physical connection:
   * opened, autoCommit switched off, every statement prepared there with autoCommit off, then
   * {@code end} ({@code commit} or {@code rollback}), the autoCommit restore and the close. A
   * statement sent through another (autoCommit) connection fails this even when the fake's global
   * rollback snapshot hides its effect on the tables.
   */
  private void assertOneTransaction(List<String> sql, String end) {
    List<RecordingDataSource.Call> calls = dataSource.calls();
    assertEquals(
        1,
        calls.stream().map(RecordingDataSource.Call::connection).distinct().count(),
        () -> "one connection: " + calls);
    assertTrue(
        dataSource.statements().stream().noneMatch(RecordingDataSource.PreparedSql::autoCommit),
        () -> "every statement runs with autoCommit off: " + dataSource.statements());
    List<String> expected = new ArrayList<>();
    expected.add("getConnection");
    expected.add("setAutoCommit(false)");
    sql.forEach(statement -> expected.add("prepare " + statement));
    expected.add(end);
    expected.add("setAutoCommit(true)");
    expected.add("close");
    assertEquals(expected, calls.stream().map(RecordingDataSource.Call::call).toList());
  }

  // ── golden SQL: the store ───────────────────────────────────────────────────────────────────

  @Test
  void goldenSql_storeForget() {
    new JdbcForgottenSubjectStore(dataSource).forget(S);

    assertEquals(List.of(FORGET_SQL), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_storeForgetWithKeyEvidence() {
    new JdbcForgottenSubjectStore(dataSource).forget(S, Instant.parse("2026-11-26T10:00:00Z"));

    assertEquals(List.of(FORGET_WITH_KEY_EVIDENCE_SQL), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_storeIsForgotten() {
    new JdbcForgottenSubjectStore(dataSource).isForgotten(S);

    assertEquals(List.of(IS_FORGOTTEN_SQL), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_storeReinstate() {
    new JdbcForgottenSubjectStore(dataSource).reinstate(S);

    assertEquals(List.of(REINSTATE_SQL), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_storeForgottenAt() {
    new JdbcForgottenSubjectStore(dataSource).forgottenAt(S);

    assertEquals(List.of(FORGOTTEN_AT_SQL), dataSource.sqlSeen());
  }

  @Test
  void goldenSql_storeKeyCreatedAt() {
    new JdbcForgottenSubjectStore(dataSource).keyCreatedAt(S);

    assertEquals(List.of(KEY_CREATED_AT_SQL), dataSource.sqlSeen());
  }

  // ── crash point: after the tombstone INSERT, before COMMIT ──────────────────────────────────

  /** Stands in for an OutOfMemoryError or a LinkageError thrown inside the transaction. */
  private static final class InjectedCrash extends Error {
    InjectedCrash(String where) {
      super("injected crash " + where);
    }
  }

  @Test
  void errorAfterTheTombstoneInsert_commitsNothing_andARerunConverges() {
    byte[] ciphertext = plainEngine.encrypt(S, PLAINTEXT);
    byte[] keyBefore = dataSource.getKey(S.value()).clone();
    JdbcForgottenSubjectStore store = new JdbcForgottenSubjectStore(dataSource);
    InjectedCrash crash = new InjectedCrash("after the tombstone INSERT");
    dataSource.failAfter("INSERT INTO forgotten_subjects", crash);
    dataSource.resetRecording();

    assertSame(crash, assertThrows(InjectedCrash.class, () -> plainEngine.deleteKey(S)));

    assertOneTransaction(
        List.of(LOCK, SELECT_KEY_VERSION, UPSERT_ERASED_GENERATION, DELETE_KEY, INSERT_TOMBSTONE),
        "rollback");

    assertArrayEquals(keyBefore, dataSource.getKey(S.value()), "the key survives");
    assertFalse(dataSource.isForgotten(S.value()), "no tombstone is committed");
    assertFalse(store.isForgotten(S), "the store sees no tombstone either");
    assertNull(dataSource.erasedGeneration(S.value()), "no erased-generation row is committed");
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, ciphertext));
    dataSource.resetRecording();

    plainEngine.deleteKey(S);

    assertOneTransaction(
        List.of(LOCK, SELECT_KEY_VERSION, UPSERT_ERASED_GENERATION, DELETE_KEY, INSERT_TOMBSTONE),
        "commit");
    assertFalse(dataSource.containsKey(S.value()), "the rerun deletes the key");
    assertTrue(dataSource.isForgotten(S.value()), "the rerun writes the tombstone");
    assertTrue(store.isForgotten(S));
    assertEquals(1, dataSource.erasedGeneration(S.value()), "the rerun records generation 1");
    assertThrows(SubjectForgottenException.class, () -> plainEngine.encrypt(S, PLAINTEXT));
  }

  // ── key-buffer zeroing ──────────────────────────────────────────────────────────────────────

  @Test
  void loadedKeyBytesAreZeroedAfterUse() {
    dataSource.putKey(S.value(), HexFormat.of().parseHex(FIXTURE_KEY_HEX));

    byte[] plaintext = plainEngine.decrypt(S, HexFormat.of().parseHex(FIXTURE_BLOB_HEX));

    assertEquals(FIXTURE_PLAINTEXT, new String(plaintext, StandardCharsets.UTF_8));
    assertEquals(1, dataSource.keyBytesHandedOut().size());
    assertArrayEquals(new byte[32], dataSource.keyBytesHandedOut().get(0));
  }

  @Test
  void mintedEncodedKeyIsZeroedAfterTheInsert() {
    byte[] blob = plainEngine.encrypt(S, PLAINTEXT);

    assertEquals(1, dataSource.keyBytesBound().size(), "only the key INSERT binds bytes");
    assertArrayEquals(new byte[32], dataSource.keyBytesBound().get(0));
    assertFalse(
        java.util.Arrays.equals(new byte[32], dataSource.getKey(S.value())),
        "the stored key is the real one; only the engine's own buffer is zeroed");
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob));
  }

  // ── v1 wire format ──────────────────────────────────────────────────────────────────────────

  @Test
  void v1FixtureDecrypts() {
    byte[] blob = HexFormat.of().parseHex(FIXTURE_BLOB_HEX);
    assertEquals(FIXTURE_KEY_VERSION, blob[1], "the fixture carries key version 1");
    dataSource.putKey(S.value(), HexFormat.of().parseHex(FIXTURE_KEY_HEX)); // version 1

    assertEquals(
        FIXTURE_PLAINTEXT, new String(plainEngine.decrypt(S, blob), StandardCharsets.UTF_8));
  }

  @Test
  void freshSealDecryptsWithAnIndependentJcaCipher() throws Exception {
    byte[] key = HexFormat.of().parseHex(FIXTURE_KEY_HEX);
    dataSource.putKey(S.value(), key.clone());
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

  // ── store failure ladder ────────────────────────────────────────────────────────────────────

  @Test
  void storeForget_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to record forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> store.forget(S));
  }

  @Test
  void storeForgetWithKeyEvidence_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to record forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> store.forget(S, Instant.parse("2026-11-26T10:00:00Z")));
  }

  @Test
  void storeIsForgotten_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to check forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> store.isForgotten(S));
  }

  @Test
  void storeReinstate_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to remove forgotten-subject tombstone for subject-hash=" + S.redacted(),
        () -> store.reinstate(S));
  }

  @Test
  void storeForgottenAt_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to read forgotten-at timestamp for subject-hash=" + S.redacted(),
        () -> store.forgottenAt(S));
  }

  @Test
  void storeKeyCreatedAt_onAnOutage_isTransientWithTheSqlExceptionAsCause() {
    SQLException outage = new SQLException("injected outage");
    var store = new JdbcForgottenSubjectStore(failingDataSource(outage));

    assertStoreFailure(
        outage,
        "Failed to read erased-key creation evidence for subject-hash=" + S.redacted(),
        () -> store.keyCreatedAt(S));
  }

  private static void assertStoreFailure(
      SQLException outage, String message, org.junit.jupiter.api.function.Executable call) {
    CryptoOperationException failure = assertThrows(CryptoOperationException.class, call);
    assertEquals(CryptoOperationException.class, failure.getClass());
    assertEquals(message, failure.getMessage());
    assertSame(outage, failure.getCause());
    assertEquals(0, failure.getSuppressed().length);
  }

  /** A DataSource whose every {@code getConnection} throws {@code outage}. */
  private static DataSource failingDataSource(SQLException outage) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw outage;
              }
              throw new UnsupportedOperationException(method.getName());
            });
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
    PostgresCryptoEngine engine = engineWith(new FaultyCiphers());

    byte[] blob = engine.encrypt(S, PLAINTEXT);

    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(S, blob));
    assertEquals(0, metrics.selfCheckFailures.get());
    assertEquals(List.of(), metrics.decryptRetries);
  }
}

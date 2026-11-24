package org.streamrune.crypto.sqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.ProviderException;
import java.security.spec.AlgorithmParameterSpec;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * What the key-store contract leaves to the SQLite engine: the builder and its table name, the
 * stored form (hashed subject ids, microsecond timestamps stamped with the engine's clock, the
 * generation of each key), the ciphertext header checks, the verified encryption and the retried
 * decryption (faults injected through the package-private cipher factory), and the failures.
 */
class SqliteCryptoEngineTest {

  private static final Instant NOW = Instant.parse("2026-11-24T10:15:30.654321Z");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(UTF_8);

  @TempDir Path dir;

  private SqliteFiles.Switchable dataSource;
  private SqliteDatabase database;
  private SqliteCryptoEngine engine;
  private final CountingMetrics metrics = new CountingMetrics();

  @BeforeEach
  void open() {
    dataSource = new SqliteFiles.Switchable(SqliteFiles.dataSource(dir.resolve("engine.db")));
    database = SqliteFiles.migrated(dataSource);
    engine = SqliteCryptoEngine.builder().database(database).build();
  }

  @AfterEach
  void close() {
    database.close();
  }

  // ---- the builder and the table name ----

  @Test
  void theBuilderNeedsADatabase_andRefusesNullMetricsAndClock() {
    assertThatThrownBy(() -> SqliteCryptoEngine.builder().build())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("database is required");
    assertThatThrownBy(() -> SqliteCryptoEngine.builder().metrics(null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("metrics is required");
    assertThatThrownBy(() -> SqliteCryptoEngine.builder().clock(null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("clock is required");
  }

  @Test
  void theDefaultTableIsUsedForANullOrBlankName_andEveryTableIsRequired() {
    for (String name : new String[] {null, "", "  "}) {
      assertThat(SqliteCryptoEngine.builder().database(database).tableName(name).build())
          .extracting(SqliteCryptoEngine::requiredCryptoTables)
          .isEqualTo(
              java.util.Set.of("encryption_keys", "forgotten_subjects", "erased_key_generations"));
    }
  }

  @Test
  void aConfiguredTableIsFoldedToLowerCase_andUsedInEveryStatement() throws Exception {
    SqliteFiles.execute(
        dataSource,
        "CREATE TABLE custom_keys (subject_id TEXT PRIMARY KEY, key_bytes BLOB NOT NULL,"
            + " created_at INTEGER NOT NULL, key_version INTEGER NOT NULL) STRICT");
    var custom = SqliteCryptoEngine.builder().database(database).tableName("Custom_Keys").build();
    var subject = SubjectId.of("custom-table");

    byte[] ciphertext = custom.encrypt(subject, PLAINTEXT);

    assertThat(custom.requiredCryptoTables()).contains("custom_keys");
    assertThat(custom.toString()).contains("custom_keys");
    assertThat(rows("SELECT count(*) FROM custom_keys")).containsExactly(List.of(1));
    assertThat(rows("SELECT count(*) FROM encryption_keys")).containsExactly(List.of(0));
    assertThat(custom.isKeyAvailable(subject)).isTrue();
    assertThat(custom.decrypt(subject, ciphertext)).isEqualTo(PLAINTEXT);
    custom.deleteKey(subject);
    assertThat(custom.isKeyAvailable(subject)).isFalse();
    assertThat(rows("SELECT count(*) FROM custom_keys")).containsExactly(List.of(0));
  }

  /**
   * A refused name is reported by its value alone: SQLite has no table-name property, and the
   * message names no property of another backend.
   */
  @Test
  void anInvalidOrReservedTableNameIsRefusedByItsValue() {
    for (String name :
        new String[] {"main.keys", "keys; DROP TABLE x", "\"keys\"", "1keys", "sqlite_keys"}) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> SqliteCryptoEngine.builder().database(database).tableName(name).build())
          .withMessageContaining("'" + name + "'")
          .withMessageNotContaining("streamrune.crypto.postgres")
          .withMessageNotContaining("table-name");
    }
  }

  // ---- the stored form ----

  @Test
  void aMintedKeyIsStoredUnderTheSubjectsHash_withTheClocksInstantAndGenerationOne()
      throws Exception {
    var clocked =
        SqliteCryptoEngine.builder()
            .database(database)
            .clock(Clock.fixed(NOW, ZoneOffset.UTC))
            .build();
    var subject = SubjectId.of("jane@example.com");

    clocked.encrypt(subject, PLAINTEXT);
    clocked.deleteKey(subject);
    clocked.reinstate(subject);
    byte[] second = clocked.encrypt(subject, PLAINTEXT);

    long micros = NOW.getEpochSecond() * 1_000_000L + 654_321L;
    assertThat(rows("SELECT subject_id, created_at, key_version FROM encryption_keys"))
        .containsExactly(List.of(subject.redacted(), micros, 2));
    assertThat(rows("SELECT subject_id, max_erased_version FROM erased_key_generations"))
        .containsExactly(List.of(subject.redacted(), 1));
    assertThat(rows("SELECT count(*) FROM forgotten_subjects")).containsExactly(List.of(0));
    assertThat(second[1]).as("the second generation").isEqualTo((byte) 2);
    assertThat(rows("SELECT count(*) FROM encryption_keys WHERE subject_id LIKE '%jane%'"))
        .containsExactly(List.of(0));
  }

  @Test
  void anErasureStampsItsTombstoneWithTheClock_andARepeatedErasureKeepsTheFirst() throws Exception {
    var subject = SubjectId.of("erased-twice");
    var first =
        SqliteCryptoEngine.builder()
            .database(database)
            .clock(Clock.fixed(NOW, ZoneOffset.UTC))
            .build();
    var later =
        SqliteCryptoEngine.builder()
            .database(database)
            .clock(Clock.fixed(NOW.plusSeconds(60), ZoneOffset.UTC))
            .build();
    first.encrypt(subject, PLAINTEXT);

    first.deleteKey(subject);
    later.deleteKey(subject);

    assertThat(new SqliteForgottenSubjectStore(database).forgottenAt(subject)).contains(NOW);
    assertThat(rows("SELECT max_erased_version FROM erased_key_generations"))
        .containsExactly(List.of(1));
  }

  @Test
  void theGenerationCapRefusesTheMintWithGuidance() throws Exception {
    var subject = SubjectId.of("erased-too-often");
    SqliteFiles.execute(
        dataSource,
        "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES ('"
            + subject.redacted()
            + "', 127)");

    assertThatThrownBy(() -> engine.encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(CryptoOperationException.class)
        .hasRootCauseMessage(
            "Key-generation limit reached for subject: "
                + subject.redacted()
                + " — the subject has been erased and re-registered 126 times, exhausting the"
                + " single-byte generation counter. Re-register this person under a fresh subject"
                + " id.");
    assertThat(engine.isKeyAvailable(subject)).isFalse();
  }

  // ---- the ciphertext header ----

  @Test
  void anUnknownFormatVersionIsADeterministicFailure() {
    var subject = SubjectId.of("format");
    byte[] blob = engine.encrypt(subject, PLAINTEXT);
    blob[0] = 9;

    assertThatThrownBy(() -> engine.decrypt(subject, blob))
        .isExactlyInstanceOf(CryptoMappingException.class)
        .hasMessageContaining("Unsupported ciphertext format version 9");
  }

  @Test
  void aKeyVersionAboveTheCurrentOrBelowOneIsADeterministicFailure() {
    var subject = SubjectId.of("versions");
    byte[] blob = engine.encrypt(subject, PLAINTEXT);
    for (byte version : new byte[] {2, 0, -1}) {
      byte[] altered = blob.clone();
      altered[1] = version;
      assertThatThrownBy(() -> engine.decrypt(subject, altered))
          .isExactlyInstanceOf(CryptoMappingException.class)
          .hasMessageContaining("Unsupported key version " + version);
    }
  }

  @Test
  void aTruncatedValueIsADeterministicFailure_unlessItNamesAnErasedGeneration() {
    var subject = SubjectId.of("short");
    engine.encrypt(subject, PLAINTEXT);
    assertThatThrownBy(() -> engine.decrypt(subject, new byte[] {1, 1, 0}))
        .isExactlyInstanceOf(CryptoMappingException.class)
        .hasMessageContaining("Ciphertext too short");
    assertThatThrownBy(() -> engine.decrypt(subject, new byte[] {1}))
        .isExactlyInstanceOf(CryptoMappingException.class)
        .hasMessageContaining("got 1");

    engine.deleteKey(subject);
    engine.reinstate(subject);
    engine.encrypt(subject, PLAINTEXT);
    assertThatThrownBy(() -> engine.decrypt(subject, new byte[] {1, 1, 0}))
        .as("a short value of a destroyed generation reads as erased")
        .isExactlyInstanceOf(KeyNotFoundException.class);
  }

  // ---- verified encryption, retried decryption ----

  @Test
  void aDiscardedEncryptionKeepsTheMintedKey_andTheRetryMintsNoSecond() throws Exception {
    var subject = SubjectId.of("discarded");
    var faulty = withCiphers(new FaultyCiphers().wrongKeyOnce(Cipher.ENCRYPT_MODE));

    var failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            CryptoOperationException.class, () -> faulty.encrypt(subject, PLAINTEXT));

    assertThat(failure.getClass()).isEqualTo(CryptoOperationException.class);
    assertThat(metrics.selfCheckFailures).hasValue(1);
    List<List<Object>> minted = rows("SELECT key_bytes, key_version FROM encryption_keys");
    byte[] retried = faulty.encrypt(subject, PLAINTEXT);
    assertThat(rows("SELECT key_bytes, key_version FROM encryption_keys")).isEqualTo(minted);
    assertThat(engine.decrypt(subject, retried)).isEqualTo(PLAINTEXT);
  }

  @Test
  void aCheckThatDecryptsToOtherBytesOrThrowsIsDiscardedAsTransient() {
    var otherBytes = withCiphers(new FaultyCiphers().encryptInsteadOnce(Cipher.DECRYPT_MODE));
    assertThatThrownBy(() -> otherBytes.encrypt(SubjectId.of("other-bytes"), PLAINTEXT))
        .isExactlyInstanceOf(CryptoOperationException.class)
        .hasNoCause();

    var injected = new IllegalArgumentException("injected");
    var throwing = withCiphers(new FaultyCiphers().throwOnce(Cipher.DECRYPT_MODE, injected));
    var failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            CryptoOperationException.class,
            () -> throwing.encrypt(SubjectId.of("check-throws"), PLAINTEXT));
    assertThat(failure.getCause()).isNull();
    assertThat(failure.getSuppressed()).containsExactly(injected);
    assertThat(metrics.selfCheckFailures).hasValue(2);
  }

  @Test
  void aDecryptionWhoseFirstAttemptFailsItsTagIsRetried_andATamperedValueFailsTwice() {
    var subject = SubjectId.of("retried");
    byte[] blob = engine.encrypt(subject, PLAINTEXT);

    var ciphers = new FaultyCiphers().wrongKeyOnce(Cipher.DECRYPT_MODE);
    assertThat(withCiphers(ciphers).decrypt(subject, blob)).isEqualTo(PLAINTEXT);
    assertThat(metrics.decryptRetries).containsExactly("RECOVERED");

    byte[] tampered = blob.clone();
    tampered[tampered.length - 1] ^= 0x01;
    var failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            CryptoMappingException.class,
            () -> withCiphers(new FaultyCiphers()).decrypt(subject, tampered));
    assertThat(failure.getCause()).isInstanceOf(AEADBadTagException.class);
    assertThat(failure.getCause().getSuppressed()).hasSize(1);
    assertThat(metrics.decryptRetries).containsExactly("RECOVERED", "FAILED");
  }

  @Test
  void aCipherThatCannotBeCreatedFailsTheEncryptionAndTheDecryptionAsTransient() {
    var subject = SubjectId.of("no-cipher");
    byte[] blob = engine.encrypt(subject, PLAINTEXT);
    var broken = new ProviderException("no provider");

    assertThatThrownBy(
            () ->
                withCiphers(new FaultyCiphers().throwOnce(Cipher.ENCRYPT_MODE, broken))
                    .encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(CryptoOperationException.class)
        .hasCause(broken);
    assertThatThrownBy(
            () ->
                withCiphers(new FaultyCiphers().throwOnce(Cipher.DECRYPT_MODE, broken))
                    .decrypt(subject, blob))
        .isExactlyInstanceOf(CryptoOperationException.class)
        .hasCause(broken);
  }

  @Test
  void theProductionEngineUsesTheFreshCipherFactory() throws Exception {
    Field factory = SqliteCryptoEngine.class.getDeclaredField("cipherFactory");
    factory.setAccessible(true);
    assertThat(factory.get(engine)).isSameAs(SqliteCryptoEngine.FRESH_CIPHERS);
    KeyGenerator generator = KeyGenerator.getInstance("AES");
    generator.init(256);
    Key key = generator.generateKey();
    var params = new javax.crypto.spec.GCMParameterSpec(128, new byte[12]);
    assertThat(SqliteCryptoEngine.FRESH_CIPHERS.create(Cipher.DECRYPT_MODE, key, params))
        .isNotSameAs(SqliteCryptoEngine.FRESH_CIPHERS.create(Cipher.DECRYPT_MODE, key, params));
  }

  // ---- failures ----

  @Test
  void anUnreachableDatabaseFailsEveryOperationWithTheSubjectsHashOnly() {
    var subject = SubjectId.of("unreachable@example.com");
    byte[] blob = engine.encrypt(subject, PLAINTEXT);
    dataSource.fail();

    List<org.assertj.core.api.ThrowableAssert.ThrowingCallable> calls =
        List.of(
            () -> engine.encrypt(subject, PLAINTEXT),
            () -> engine.decrypt(subject, blob),
            () -> engine.deleteKey(subject),
            () -> engine.reinstate(subject),
            () -> engine.isKeyAvailable(subject));
    for (var call : calls) {
      assertThatThrownBy(call)
          .isExactlyInstanceOf(CryptoOperationException.class)
          .hasMessageContaining(subject.redacted())
          .hasMessageNotContaining(subject.value());
    }
  }

  @Test
  void theEngineKeepsWorkingAfterTheHandleIsClosed() {
    var subject = SubjectId.of("after-close");
    database.close();

    assertThat(engine.decrypt(subject, engine.encrypt(subject, PLAINTEXT))).isEqualTo(PLAINTEXT);
  }

  private SqliteCryptoEngine withCiphers(FaultyCiphers ciphers) {
    return SqliteCryptoEngine.builder()
        .database(database)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  private List<List<Object>> rows(String sql) throws Exception {
    List<List<Object>> rows = SqliteFiles.query(dataSource, sql);
    // Compare blobs by content.
    List<List<Object>> comparable = new ArrayList<>();
    for (List<Object> row : rows) {
      List<Object> copy = new ArrayList<>();
      for (Object value : row) {
        copy.add(value instanceof byte[] bytes ? java.util.HexFormat.of().formatHex(bytes) : value);
      }
      comparable.add(copy);
    }
    return comparable;
  }

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

  /**
   * Creates real AES/GCM ciphers; once armed, the first cipher of a mode is initialised under a key
   * of its own (a wrong result), in encrypt mode instead of decrypt mode (a result that is not the
   * plaintext and raises no tag failure), or not created at all.
   */
  private static final class FaultyCiphers implements SqliteCryptoEngine.CipherFactory {
    private int wrongKeyMode = -1;
    private int encryptInsteadMode = -1;
    private int throwMode = -1;
    private RuntimeException toThrow;

    FaultyCiphers wrongKeyOnce(int mode) {
      this.wrongKeyMode = mode;
      return this;
    }

    FaultyCiphers encryptInsteadOnce(int mode) {
      this.encryptInsteadMode = mode;
      return this;
    }

    FaultyCiphers throwOnce(int mode, RuntimeException failure) {
      this.throwMode = mode;
      this.toThrow = failure;
      return this;
    }

    @Override
    public Cipher create(int mode, Key key, AlgorithmParameterSpec params)
        throws GeneralSecurityException {
      if (mode == throwMode) {
        throwMode = -1;
        throw toThrow;
      }
      Key used = key;
      int usedMode = mode;
      if (mode == wrongKeyMode) {
        wrongKeyMode = -1;
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        used = generator.generateKey();
      }
      if (mode == encryptInsteadMode) {
        encryptInsteadMode = -1;
        usedMode = Cipher.ENCRYPT_MODE;
      }
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(usedMode, used, params);
      return cipher;
    }
  }
}

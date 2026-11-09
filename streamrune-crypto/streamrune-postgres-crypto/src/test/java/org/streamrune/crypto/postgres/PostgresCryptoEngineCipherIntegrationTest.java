package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.ProviderException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.GCMParameterSpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The engine verifies each encryption before it returns it and retries a failed decryption once
 * with a fresh cipher, so a wrong cipher result is caught before the value is returned and a
 * one-off wrong cipher result, whatever its cause, cannot wedge a reader. The faults are injected
 * through the package-private cipher factory.
 *
 * <p>Crash points of an encrypt: (1) after the key mint commits, (2) after the cipher work, before
 * the value is returned. A discarded encryption stops the flow at (2) with nothing returned, so no
 * row can reference the value; the retry loads the key committed at (1), mints no second key and
 * returns a value that decrypts.
 */
@Testcontainers
class PostgresCryptoEngineCipherIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  private static PGSimpleDataSource dataSource;
  private static PostgresCryptoEngine plainEngine;

  private final CountingMetrics metrics = new CountingMetrics();

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    String ddl;
    try (var in =
        PostgresCryptoEngineCipherIntegrationTest.class.getResourceAsStream(
            "/db/crypto-migration/V001__crypto_baseline.sql")) {
      assertNotNull(in);
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
    plainEngine = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  private PostgresCryptoEngine engineWith(FaultyCiphers ciphers) {
    return PostgresCryptoEngine.builder()
        .dataSource(dataSource)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  @Test
  void aDiscardedEncryptionKeepsTheMintedKeyAndTheRetryConvergesWithoutASecondMint()
      throws Exception {
    SubjectId subject = SubjectId.of("pg-cipher-discarded");
    PostgresCryptoEngine engine = engineWith(new FaultyCiphers().wrongKeyOnce(Cipher.ENCRYPT_MODE));

    CryptoOperationException failure =
        assertThrows(CryptoOperationException.class, () -> engine.encrypt(subject, PLAINTEXT));

    assertEquals(
        CryptoOperationException.class,
        failure.getClass(),
        "a failed self-check is transient (the bare type), so the caller retries");
    assertEquals(1, metrics.selfCheckFailures.get());
    StoredKey minted = storedKey(subject);
    assertEquals(1, minted.version());

    byte[] retried = engine.encrypt(subject, PLAINTEXT);

    StoredKey afterRetry = storedKey(subject);
    assertArrayEquals(minted.bytes(), afterRetry.bytes(), "the retry loads the committed key");
    assertEquals(1, afterRetry.version());
    assertEquals(1, retried[1], "the value carries the first generation");
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(subject, retried));
  }

  @Test
  void anEncryptionWhoseCheckDecryptsToOtherBytesIsDiscardedAsTransient() {
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptInsteadOnce(Cipher.DECRYPT_MODE));

    CryptoOperationException failure =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("pg-cipher-other-bytes"), PLAINTEXT));

    assertEquals(CryptoOperationException.class, failure.getClass());
    assertEquals(1, metrics.selfCheckFailures.get());
  }

  @Test
  void aDecryptionWhoseFirstAttemptFailsItsTagIsRetriedWithAFreshCipher() {
    SubjectId subject = SubjectId.of("pg-cipher-retried");
    byte[] blob = plainEngine.encrypt(subject, PLAINTEXT);
    FaultyCiphers ciphers = new FaultyCiphers().wrongKeyOnce(Cipher.DECRYPT_MODE);

    byte[] plaintext = engineWith(ciphers).decrypt(subject, blob);

    assertArrayEquals(PLAINTEXT, plaintext);
    assertEquals(List.of("RECOVERED"), metrics.decryptRetries);
    assertEquals(2, distinct(ciphers.created), "the retry takes a cipher of its own");
  }

  @Test
  void aValueWithAnAlteredTagStillFailsDeterministicallyAfterTwoAttempts() {
    SubjectId subject = SubjectId.of("pg-cipher-tampered");
    byte[] blob = plainEngine.encrypt(subject, PLAINTEXT);
    blob[blob.length - 1] ^= 0x01;
    FaultyCiphers ciphers = new FaultyCiphers();
    PostgresCryptoEngine engine = engineWith(ciphers);

    CryptoMappingException failure =
        assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, blob));

    assertEquals(2, ciphers.created.size());
    assertEquals(List.of("FAILED"), metrics.decryptRetries);
    assertEquals(AEADBadTagException.class, failure.getCause().getClass());
    assertEquals(1, failure.getCause().getSuppressed().length, "the first attempt's failure");
  }

  @Test
  void everyOperationTakesFreshCiphers() {
    SubjectId subject = SubjectId.of("pg-cipher-fresh");
    FaultyCiphers ciphers = new FaultyCiphers();
    PostgresCryptoEngine engine = engineWith(ciphers);

    byte[] blob = engine.encrypt(subject, PLAINTEXT);
    engine.encrypt(subject, PLAINTEXT);
    engine.decrypt(subject, blob);

    assertEquals(
        List.of(
            Cipher.ENCRYPT_MODE,
            Cipher.DECRYPT_MODE,
            Cipher.ENCRYPT_MODE,
            Cipher.DECRYPT_MODE,
            Cipher.DECRYPT_MODE),
        ciphers.modes,
        "an encryption and its check, twice, then a decryption");
    assertEquals(5, distinct(ciphers.created));
    assertEquals(0, metrics.selfCheckFailures.get());
    assertEquals(List.of(), metrics.decryptRetries);
  }

  @Test
  void aFailureInsideTheEncryptionCheckIsDiscardedAsTransient() {
    ProviderException injected = new ProviderException("injected");
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwOnce(Cipher.DECRYPT_MODE, injected));

    CryptoOperationException failure =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("pg-cipher-check-throws"), PLAINTEXT));

    assertEquals(
        CryptoOperationException.class,
        failure.getClass(),
        "an unchecked failure of the check is transient (the bare type), so the caller retries");
    assertNull(failure.getCause());
    assertSame(injected, failure.getSuppressed()[0]);
    assertEquals(1, metrics.selfCheckFailures.get());
  }

  @Test
  void anIllegalArgumentExceptionInsideTheEncryptionCheckIsTransientAndNothingIsStored()
      throws Exception {
    SubjectId subject = SubjectId.of("pg-cipher-check-iae");
    IllegalArgumentException injected = new IllegalArgumentException("injected");
    PostgresCryptoEngine engine =
        engineWith(new FaultyCiphers().throwOnce(Cipher.DECRYPT_MODE, injected));

    CryptoOperationException failure =
        assertThrows(CryptoOperationException.class, () -> engine.encrypt(subject, PLAINTEXT));

    assertEquals(CryptoOperationException.class, failure.getClass());
    assertSame(injected, failure.getSuppressed()[0]);
    assertNoPermanentEvidenceInTheCauseChain(failure);
    assertEquals(1, metrics.selfCheckFailures.get());
    StoredKey minted = storedKey(subject);
    byte[] retried = engine.encrypt(subject, PLAINTEXT);
    assertArrayEquals(minted.bytes(), storedKey(subject).bytes(), "no second mint");
    assertEquals(minted.version(), storedKey(subject).version());
    assertArrayEquals(PLAINTEXT, plainEngine.decrypt(subject, retried));
  }

  @Test
  void theProductionFactoryCreatesAFreshCipherForEveryCall() throws Exception {
    KeyGenerator generator = KeyGenerator.getInstance("AES");
    generator.init(256);
    Key key = generator.generateKey();
    GCMParameterSpec params = new GCMParameterSpec(128, new byte[12]);

    assertNotSame(
        PostgresCryptoEngine.FRESH_CIPHERS.create(Cipher.DECRYPT_MODE, key, params),
        PostgresCryptoEngine.FRESH_CIPHERS.create(Cipher.DECRYPT_MODE, key, params));
    Field factory = PostgresCryptoEngine.class.getDeclaredField("cipherFactory");
    factory.setAccessible(true);
    assertSame(
        PostgresCryptoEngine.FRESH_CIPHERS,
        factory.get(plainEngine),
        "an engine built without the test seam uses the production factory");
  }

  @Test
  void theEngineKeepsNoCipherBetweenOperations() {
    for (Field field : PostgresCryptoEngine.class.getDeclaredFields()) {
      assertFalse(
          ThreadLocal.class.isAssignableFrom(field.getType())
              || Cipher.class.isAssignableFrom(field.getType()),
          "field " + field.getName() + " would carry cipher state from one operation to the next");
    }
  }

  private record StoredKey(byte[] bytes, int version) {}

  private static StoredKey storedKey(SubjectId subject) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?")) {
      stmt.setString(1, subject.redacted());
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "the key is committed");
        StoredKey key = new StoredKey(rs.getBytes(1), rs.getInt(2));
        assertFalse(rs.next());
        return key;
      }
    }
  }

  private static int distinct(List<Cipher> ciphers) {
    Set<Cipher> identities = Collections.newSetFromMap(new IdentityHashMap<>());
    identities.addAll(ciphers);
    return identities.size();
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
   * Creates real AES/GCM ciphers and records each one; once armed, the first cipher of a mode is
   * initialised under a key of its own (a wrong result) or in encrypt mode instead of decrypt mode
   * (a result that is not the plaintext and raises no tag failure).
   */
  private static final class FaultyCiphers implements PostgresCryptoEngine.CipherFactory {
    final List<Cipher> created = new ArrayList<>();
    final List<Integer> modes = new ArrayList<>();
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
      created.add(cipher);
      modes.add(mode);
      return cipher;
    }
  }

  /** No exception in the chain a classifier walks reads as permanent evidence. */
  private static void assertNoPermanentEvidenceInTheCauseChain(Throwable failure) {
    for (Throwable c = failure.getCause(); c != null; c = c.getCause()) {
      assertFalse(
          c instanceof IllegalArgumentException || c instanceof CryptoMappingException,
          "cause " + c + " would classify the failed check as permanent");
    }
  }
}

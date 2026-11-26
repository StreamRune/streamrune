package org.streamrune.crypto.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
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
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;

/**
 * Freezes today's observable behaviour of {@link AwsKmsCryptoEngine}'s cipher seam before the
 * crypto twin extraction moves the self-check and the one-retry decryption into a shared helper.
 * The engine keeps its own v2 envelope (AAD, wrapped data key) and its own decrypt catch ladder;
 * every assertion here pins what the code does today — exception type, exact message, cause versus
 * suppressed, metric calls, WARN text and logger category, and a committed v2 blob. The refactoring
 * must keep it green; only the seam-installation lines ({@link #engineWith} and {@link
 * FaultyCiphers}) may change.
 */
class AwsKmsCryptoCharacterizationTest {

  // ── v2 fixture: sealed once by today's engine; never regenerate ─────────────────────────────
  // Sealed against a KMS that issued FIXTURE_V2_DATA_KEY_HEX, wrapped as FIXTURE_V2_WRAPPED_HEX,
  // under key id FIXTURE_V2_KMS_KEY_ID, for subject S. FakeKms#issueFixed lets the fake decrypt it.

  static final String FIXTURE_V2_DATA_KEY_HEX =
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
  static final String FIXTURE_V2_WRAPPED_HEX =
      "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
          + "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf";
  static final String FIXTURE_V2_KMS_KEY_ID = "test-key-id";
  static final String FIXTURE_V2_PLAINTEXT = "characterization fixture v2";
  static final String FIXTURE_V2_BLOB_HEX =
      "53524b43020040a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6"
          + "c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf6d0a3fcd94575a91d874f1909b1881b385a57"
          + "a098f4ed2bea02b5632219213f6a4d400da98bf4bd30d1a466cf4f906631dccbbe16717b6";

  private static final SubjectId S = SubjectId.of("characterization-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  private final FakeKms kms = new FakeKms();
  private final InMemoryForgottenSubjectStore tombstones = new InMemoryForgottenSubjectStore();
  private final CountingMetrics metrics = new CountingMetrics();

  /**
   * The seam installation: how a faulty {@link AwsKmsCryptoEngine.CipherFactory} reaches the engine
   * — today the package-private builder method, as {@code AwsKmsCryptoEngineCipherTest} installs
   * it.
   */
  private AwsKmsCryptoEngine engineWith(FaultyCiphers ciphers) {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(kms.client())
        .kmsKeyId("test-key-id")
        .forgottenSubjectStore(tombstones)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  private AwsKmsCryptoEngine plainEngine() {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(kms.client())
        .kmsKeyId("test-key-id")
        .forgottenSubjectStore(tombstones)
        .build();
  }

  // ── encrypt / decrypt failure ladder ────────────────────────────────────────────────────────

  @Test
  void encrypt_cipherFailure_isTransientWithTheCipherExceptionAsCause() {
    GeneralSecurityException injected = new GeneralSecurityException("injected encrypt failure");
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.ENCRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals("AES-GCM encryption failed for subject: " + S.redacted(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(List.of(), log.warnings());
    }
  }

  @Test
  void encrypt_selfCheckMismatch_isTransient_noCause_countsAndWarns() {
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptingInstead(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.encrypt(S, PLAINTEXT));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals(selfCheckMessage(), failure.getMessage());
      assertNull(failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(1, metrics.selfCheckFailures.get());
      assertEquals(List.of(), metrics.decryptRetries);
      assertSelfCheckWarning(log.warnings());
      assertFalse(engine.holdsDataKeysOf(S), "the data key of a discarded value is not cached");
    }
  }

  @Test
  void encrypt_selfCheckCheckedFailure_isTheCause() {
    AEADBadTagException injected = new AEADBadTagException("injected check failure");
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
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
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
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
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE).wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      CryptoMappingException failure =
          assertThrows(CryptoMappingException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoMappingException.class, failure.getClass());
      assertEquals(
          "AES-GCM tag mismatch for subject: "
              + S.redacted()
              + " — the ciphertext is corrupted, tampered, or belongs to another subject"
              + " (deterministic, retrying cannot succeed)",
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
      assertFalse(engine.holdsDataKeysOf(S), "the key that did not open the value is not cached");
    }
  }

  @Test
  void decrypt_tagFailsOnce_recovers_countsRecovered() {
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    AwsKmsCryptoEngine engine = engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      assertArrayEquals(PLAINTEXT, engine.decrypt(S, blob));

      assertEquals(List.of("RECOVERED"), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertRetryWarning(log.warnings(), "RECOVERED");
    }
  }

  @Test
  void decrypt_nonTagFailure_isTransient() {
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    NoSuchPaddingException injected = new NoSuchPaddingException("injected decrypt failure");
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals("AES-GCM decryption failed for subject: " + S.redacted(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertEquals(List.of(), log.warnings());
    }
  }

  @Test
  void decrypt_uncheckedCipherFailure_isTransient_notRetried() {
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    IllegalStateException injected = new IllegalStateException("injected decrypt failure");
    AwsKmsCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(AwsKmsCryptoEngine.class)) {
      CryptoOperationException failure =
          assertThrows(CryptoOperationException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoOperationException.class, failure.getClass());
      assertEquals("AES-GCM decryption failed for subject: " + S.redacted(), failure.getMessage());
      assertSame(injected, failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(List.of(), log.warnings());
    }
  }

  // ── v2 fixture ──────────────────────────────────────────────────────────────────────────────

  @Test
  void v2FixtureDecrypts() {
    HexFormat hex = HexFormat.of();
    kms.issueFixed(
        hex.parseHex(FIXTURE_V2_WRAPPED_HEX),
        hex.parseHex(FIXTURE_V2_DATA_KEY_HEX),
        FIXTURE_V2_KMS_KEY_ID,
        Map.of("streamrune-subject", S.redacted()));

    byte[] plaintext = plainEngine().decrypt(S, hex.parseHex(FIXTURE_V2_BLOB_HEX));

    assertEquals(FIXTURE_V2_PLAINTEXT, new String(plaintext, StandardCharsets.UTF_8));
  }

  @Test
  void faultyCiphersWithoutFaultsBehavesLikeTheProductionFactory() {
    // Guards the test double itself: with nothing queued, the seam is transparent.
    AwsKmsCryptoEngine engine = engineWith(new FaultyCiphers());

    byte[] blob = engine.encrypt(S, PLAINTEXT);

    assertArrayEquals(PLAINTEXT, plainEngine().decrypt(S, blob));
    assertEquals(0, metrics.selfCheckFailures.get());
    assertEquals(List.of(), metrics.decryptRetries);
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  private static String selfCheckMessage() {
    return "Encryption self-check failed for subject: "
        + S.redacted()
        + " — the encrypted value did not decrypt to its plaintext and is discarded"
        + " (transient; retrying encrypts it again)";
  }

  private static void assertSelfCheckWarning(List<ILoggingEvent> warnings) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(AwsKmsCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Encryption self-check failed for subject "
            + S.redacted()
            + ": the encrypted value did not decrypt to its plaintext and is discarded; the"
            + " operation fails as transient and is retried",
        warnings.get(0).getFormattedMessage());
  }

  private static void assertRetryWarning(List<ILoggingEvent> warnings, String outcome) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(AwsKmsCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Decryption for subject "
            + S.redacted()
            + " failed its authentication tag and was retried once with a fresh cipher; outcome "
            + outcome,
        warnings.get(0).getFormattedMessage());
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

  /** One queued fault for the next cipher of a mode. */
  private sealed interface Fault {
    record Throw(Exception failure) implements Fault {}

    record WrongKey() implements Fault {}

    record EncryptInstead() implements Fault {}
  }

  /**
   * Creates real AES/GCM ciphers; faults queued per mode apply to the next cipher of that mode, in
   * order. With nothing queued it is transparent.
   */
  private static final class FaultyCiphers implements AwsKmsCryptoEngine.CipherFactory {
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
}

package org.streamrune.crypto.filesystem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Stream;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;

/**
 * Freezes today's observable cipher and wire-format behaviour of {@link FileSystemCryptoEngine}
 * before the crypto twin extraction moves its cipher, self-check, retry and v1 format logic into
 * shared helpers. Every assertion pins what the code does today: exception type, message, cause
 * versus suppressed placement, metric calls and the WARN text with its logger category. Key-file
 * storage is out of scope for the refactoring; the test touches it only to plant the fixture key.
 */
class FileSystemCryptoCharacterizationTest {

  // ── v1 fixture: the Postgres constants verbatim, so both backends prove the same blob ─────

  static final String FIXTURE_KEY_HEX =
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
  static final int FIXTURE_KEY_VERSION = 1;
  static final String FIXTURE_PLAINTEXT = "characterization fixture v1";
  static final String FIXTURE_BLOB_HEX =
      "01011e775be467702a2cb2ee23d5d5b02a29ddca96d6cd67f87d797a0172ae355c330edf58fa9ddcac9d7262"
          + "272d28e6c9a1fd844b0edcc91c";

  private static final SubjectId S = SubjectId.of("characterization-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  @TempDir Path keyDirectory;

  private final CountingMetrics metrics = new CountingMetrics();

  /**
   * The seam installation: how a faulty {@link FileSystemCryptoEngine.CipherFactory} reaches the
   * engine — today the package-private builder method, as {@code FileSystemCryptoEngineCipherTest}
   * installs it.
   */
  private FileSystemCryptoEngine engineWith(FaultyCiphers ciphers) {
    return FileSystemCryptoEngine.builder()
        .keyDirectory(keyDirectory)
        .metrics(metrics)
        .cipherFactory(ciphers)
        .build();
  }

  private FileSystemCryptoEngine plainEngine() {
    return FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).build();
  }

  // ── encrypt / decrypt failure ladder ────────────────────────────────────────────────────────

  @Test
  void encrypt_cipherFailure_isTransientWithTheCipherExceptionAsCause() {
    GeneralSecurityException injected = new GeneralSecurityException("injected encrypt failure");
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.ENCRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptingInstead(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE).wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    FileSystemCryptoEngine engine = engineWith(new FaultyCiphers().wrongKey(Cipher.DECRYPT_MODE));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
      assertArrayEquals(PLAINTEXT, engine.decrypt(S, blob));

      assertEquals(List.of("RECOVERED"), metrics.decryptRetries);
      assertEquals(0, metrics.selfCheckFailures.get());
      assertRetryWarning(log.warnings(), "RECOVERED");
    }
  }

  @Test
  void decrypt_nonTagCipherFailure_isTransient_notRetried() {
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    NoSuchPaddingException injected = new NoSuchPaddingException("injected decrypt failure");
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().throwing(Cipher.DECRYPT_MODE, injected));

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
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

  // ── header validation runs outside the try: rejections surface unwrapped ────────────────────

  @Test
  void decrypt_unsupportedFormatVersion_surfacesAsItsOwnTypeUnwrapped() {
    FileSystemCryptoEngine engine = engineWith(new FaultyCiphers());
    byte[] blob = plainEngine().encrypt(S, PLAINTEXT);
    blob[0] = 2;

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
      CryptoMappingException failure =
          assertThrows(CryptoMappingException.class, () -> engine.decrypt(S, blob));

      assertEquals(CryptoMappingException.class, failure.getClass());
      assertEquals(
          "Unsupported ciphertext format version 2 for subject: "
              + S.redacted()
              + " — this engine reads format 1; the blob may come from a different CryptoEngine or"
              + " a newer StreamRune version",
          failure.getMessage());
      assertNull(failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(List.of(), metrics.decryptRetries);
      assertEquals(List.of(), log.warnings());
    }
  }

  @Test
  void decrypt_erasedGeneration_surfacesAsKeyNotFoundUnwrapped() throws IOException {
    FileSystemCryptoEngine engine = engineWith(new FaultyCiphers());
    byte[] blob = HexFormat.of().parseHex(FIXTURE_BLOB_HEX); // sealed under generation 1
    plantKeyFile(HexFormat.of().parseHex(FIXTURE_KEY_HEX), 2); // current generation is 2

    try (LogCapture log = LogCapture.attach(FileSystemCryptoEngine.class)) {
      KeyNotFoundException failure =
          assertThrows(KeyNotFoundException.class, () -> engine.decrypt(S, blob));

      assertEquals(KeyNotFoundException.class, failure.getClass());
      assertEquals(
          "Ciphertext was written under key generation 1, which a GDPR erasure destroyed; the"
              + " current key generation is 2 for subject: "
              + S.redacted(),
          failure.getMessage());
      assertNull(failure.getCause());
      assertEquals(0, failure.getSuppressed().length);
      assertEquals(List.of(), metrics.decryptRetries);
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
    assertEquals(FileSystemCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Encryption self-check failed for subject "
            + S.redacted()
            + ": the encrypted value did not decrypt to its plaintext and is discarded; the"
            + " operation fails as transient and is retried",
        warnings.get(0).getFormattedMessage());
  }

  private static void assertRetryWarning(List<ILoggingEvent> warnings, String outcome) {
    assertEquals(1, warnings.size(), () -> "exactly one WARN: " + warnings);
    assertEquals(FileSystemCryptoEngine.class.getName(), warnings.get(0).getLoggerName());
    assertEquals(
        "Decryption for subject "
            + S.redacted()
            + " failed its authentication tag and was retried once with a fresh cipher; outcome "
            + outcome,
        warnings.get(0).getFormattedMessage());
  }

  // ── v1 wire format ──────────────────────────────────────────────────────────────────────────

  /**
   * Plants key material for {@link #S} the way the engine stores it: it lets the engine mint the
   * subject's key file (so the shard directory and file name stay the engine's business), then
   * overwrites that file. Generation 1 is the bare 32 key bytes; a later generation appends its
   * number as a 33rd byte.
   */
  private void plantKeyFile(byte[] key, int generation) throws IOException {
    plainEngine().encrypt(S, PLAINTEXT); // mints <shard>/<hash>.subjectId
    Path keyFile;
    try (Stream<Path> files = Files.walk(keyDirectory)) {
      keyFile = files.filter(p -> p.toString().endsWith(".subjectId")).findFirst().orElseThrow();
    }
    byte[] content = generation == 1 ? key : java.util.Arrays.copyOf(key, key.length + 1);
    if (generation != 1) {
      content[key.length] = (byte) generation;
    }
    Files.write(keyFile, content);
  }

  @Test
  void v1FixtureDecrypts() throws IOException {
    byte[] blob = HexFormat.of().parseHex(FIXTURE_BLOB_HEX);
    assertEquals(FIXTURE_KEY_VERSION, blob[1], "the fixture carries key version 1");
    plantKeyFile(HexFormat.of().parseHex(FIXTURE_KEY_HEX), FIXTURE_KEY_VERSION);

    assertEquals(
        FIXTURE_PLAINTEXT, new String(plainEngine().decrypt(S, blob), StandardCharsets.UTF_8));
  }

  @Test
  void freshSealDecryptsWithAnIndependentJcaCipher() throws Exception {
    byte[] key = HexFormat.of().parseHex(FIXTURE_KEY_HEX);
    plantKeyFile(key.clone(), FIXTURE_KEY_VERSION);
    byte[] plaintext = FIXTURE_PLAINTEXT.getBytes(StandardCharsets.UTF_8);

    byte[] blob = plainEngine().encrypt(S, plaintext);

    assertEquals(1, blob[0], "format version");
    assertEquals(FIXTURE_KEY_VERSION, blob[1], "key version");
    assertEquals(2 + 12 + plaintext.length + 16, blob.length);
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(
        Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, blob, 2, 12));
    assertArrayEquals(plaintext, cipher.doFinal(blob, 14, blob.length - 14));
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
  private static final class FaultyCiphers implements FileSystemCryptoEngine.CipherFactory {
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
    FileSystemCryptoEngine engine = engineWith(new FaultyCiphers());

    byte[] blob = engine.encrypt(S, PLAINTEXT);

    assertArrayEquals(PLAINTEXT, plainEngine().decrypt(S, blob));
    assertEquals(0, metrics.selfCheckFailures.get());
    assertEquals(List.of(), metrics.decryptRetries);
  }
}

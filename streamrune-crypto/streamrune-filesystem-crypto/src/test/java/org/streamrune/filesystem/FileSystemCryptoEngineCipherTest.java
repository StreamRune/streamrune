package org.streamrune.filesystem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;

/**
 * The engine verifies each encryption before it returns it and retries a failed decryption once
 * with a fresh cipher, so a transient fault of the JVM or the hardware cannot persist an unreadable
 * value or wedge a reader. The faults are injected through the package-private cipher factory.
 */
class FileSystemCryptoEngineCipherTest {

  private static final SubjectId SUBJECT = SubjectId.of("cipher-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  @TempDir Path keyDirectory;

  private final CountingMetrics metrics = new CountingMetrics();

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

  @Test
  void anEncryptionThatDoesNotDecryptIsDiscardedAsTransientAndTheRetryReturnsAReadableValue() {
    FaultyCiphers ciphers = new FaultyCiphers().wrongKeyOnce(Cipher.ENCRYPT_MODE);
    FileSystemCryptoEngine engine = engineWith(ciphers);

    CryptoOperationException failure =
        assertThrows(CryptoOperationException.class, () -> engine.encrypt(SUBJECT, PLAINTEXT));

    assertEquals(
        CryptoOperationException.class,
        failure.getClass(),
        "a failed self-check is transient (the bare type), so the caller retries");
    assertEquals(1, metrics.selfCheckFailures.get());
    byte[] retried = engine.encrypt(SUBJECT, PLAINTEXT);
    assertArrayEquals(PLAINTEXT, plainEngine().decrypt(SUBJECT, retried));
    assertEquals(1, metrics.selfCheckFailures.get());
  }

  @Test
  void anEncryptionWhoseCheckDecryptsToOtherBytesIsDiscardedAsTransient() {
    FileSystemCryptoEngine engine =
        engineWith(new FaultyCiphers().encryptInsteadOnce(Cipher.DECRYPT_MODE));

    CryptoOperationException failure =
        assertThrows(CryptoOperationException.class, () -> engine.encrypt(SUBJECT, PLAINTEXT));

    assertEquals(CryptoOperationException.class, failure.getClass());
    assertEquals(1, metrics.selfCheckFailures.get());
  }

  @Test
  void aDecryptionWhoseFirstAttemptFailsItsTagIsRetriedWithAFreshCipher() {
    byte[] blob = plainEngine().encrypt(SUBJECT, PLAINTEXT);
    FaultyCiphers ciphers = new FaultyCiphers().wrongKeyOnce(Cipher.DECRYPT_MODE);

    byte[] plaintext = engineWith(ciphers).decrypt(SUBJECT, blob);

    assertArrayEquals(PLAINTEXT, plaintext);
    assertEquals(1, metrics.decryptRetries.get());
    assertEquals(2, ciphers.created.size(), "the retry takes a cipher of its own");
    assertEquals(2, distinct(ciphers.created));
  }

  @Test
  void aValueWithAnAlteredTagStillFailsDeterministicallyAfterTwoAttempts() {
    byte[] blob = plainEngine().encrypt(SUBJECT, PLAINTEXT);
    blob[blob.length - 1] ^= 0x01;
    FaultyCiphers ciphers = new FaultyCiphers();
    FileSystemCryptoEngine engine = engineWith(ciphers);

    CryptoMappingException failure =
        assertThrows(CryptoMappingException.class, () -> engine.decrypt(SUBJECT, blob));

    assertEquals(2, ciphers.created.size());
    assertEquals(1, metrics.decryptRetries.get());
    assertEquals(AEADBadTagException.class, failure.getCause().getClass());
    assertEquals(1, failure.getCause().getSuppressed().length, "the first attempt's failure");
  }

  @Test
  void everyOperationTakesFreshCiphers() {
    FaultyCiphers ciphers = new FaultyCiphers();
    FileSystemCryptoEngine engine = engineWith(ciphers);

    byte[] blob = engine.encrypt(SUBJECT, PLAINTEXT);
    engine.encrypt(SUBJECT, PLAINTEXT);
    engine.decrypt(SUBJECT, blob);

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
    assertEquals(0, metrics.decryptRetries.get());
  }

  @Test
  void theEngineKeepsNoCipherBetweenOperations() {
    for (Field field : FileSystemCryptoEngine.class.getDeclaredFields()) {
      assertFalse(
          ThreadLocal.class.isAssignableFrom(field.getType())
              || Cipher.class.isAssignableFrom(field.getType()),
          "field " + field.getName() + " would carry cipher state from one operation to the next");
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
    final AtomicInteger decryptRetries = new AtomicInteger();

    @Override
    public void recordEncryptSelfCheckFailed() {
      selfCheckFailures.incrementAndGet();
    }

    @Override
    public void recordDecryptRetried() {
      decryptRetries.incrementAndGet();
    }
  }

  /**
   * Creates real AES/GCM ciphers and records each one; once armed, the first cipher of a mode is
   * initialised under a key of its own (a wrong result) or in encrypt mode instead of decrypt mode
   * (a result that is not the plaintext and raises no tag failure).
   */
  static final class FaultyCiphers implements FileSystemCryptoEngine.CipherFactory {
    final List<Cipher> created = new ArrayList<>();
    final List<Integer> modes = new ArrayList<>();
    private int wrongKeyMode = -1;
    private int encryptInsteadMode = -1;

    FaultyCiphers wrongKeyOnce(int mode) {
      this.wrongKeyMode = mode;
      return this;
    }

    FaultyCiphers encryptInsteadOnce(int mode) {
      this.encryptInsteadMode = mode;
      return this;
    }

    @Override
    public Cipher create(int mode, Key key, AlgorithmParameterSpec params)
        throws GeneralSecurityException {
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
}

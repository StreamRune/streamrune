package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
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
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;

/**
 * The engine verifies each encryption before it returns it and retries a failed decryption once
 * with a fresh cipher, so a transient fault of the JVM or the hardware cannot persist an unreadable
 * value or wedge a reader. The faults are injected through the package-private cipher factory.
 */
class AwsKmsCryptoEngineCipherTest {

  private static final SubjectId SUBJECT = SubjectId.of("kms-cipher-subject");
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);

  private final FakeKms kms = new FakeKms();
  private final InMemoryForgottenSubjectStore tombstones = new InMemoryForgottenSubjectStore();
  private final CountingMetrics metrics = new CountingMetrics();

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

  @Test
  void anEncryptionThatDoesNotDecryptIsDiscardedWithItsDataKeyAndTheRetryReturnsAReadableValue() {
    AwsKmsCryptoEngine engine = engineWith(new FaultyCiphers().wrongKeyOnce(Cipher.ENCRYPT_MODE));

    CryptoOperationException failure =
        assertThrows(CryptoOperationException.class, () -> engine.encrypt(SUBJECT, PLAINTEXT));

    assertEquals(
        CryptoOperationException.class,
        failure.getClass(),
        "a failed self-check is transient (the bare type), so the caller retries");
    assertEquals(1, metrics.selfCheckFailures.get());
    assertFalse(engine.holdsDataKeysOf(SUBJECT), "the data key of a discarded value is not cached");
    byte[] retried = engine.encrypt(SUBJECT, PLAINTEXT);
    assertTrue(engine.holdsDataKeysOf(SUBJECT));
    verify(kms.client(), times(2)).generateDataKey(any(GenerateDataKeyRequest.class));
    assertArrayEquals(PLAINTEXT, plainEngine().decrypt(SUBJECT, retried));
  }

  @Test
  void anEncryptionWhoseCheckDecryptsToOtherBytesIsDiscardedAsTransient() {
    AwsKmsCryptoEngine engine =
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
    AwsKmsCryptoEngine engine = engineWith(ciphers);

    byte[] plaintext = engine.decrypt(SUBJECT, blob);

    assertArrayEquals(PLAINTEXT, plaintext);
    assertEquals(1, metrics.decryptRetries.get());
    assertEquals(2, distinct(ciphers.created), "the retry takes a cipher of its own");
    assertTrue(engine.holdsDataKeysOf(SUBJECT), "the unwrapped key opened the value");
  }

  @Test
  void aValueWithAnAlteredTagStillFailsDeterministicallyAfterTwoAttempts() {
    byte[] blob = plainEngine().encrypt(SUBJECT, PLAINTEXT);
    blob[blob.length - 1] ^= 0x01;
    FaultyCiphers ciphers = new FaultyCiphers();
    AwsKmsCryptoEngine engine = engineWith(ciphers);

    CryptoMappingException failure =
        assertThrows(CryptoMappingException.class, () -> engine.decrypt(SUBJECT, blob));

    assertEquals(2, ciphers.created.size());
    assertEquals(1, metrics.decryptRetries.get());
    assertEquals(AEADBadTagException.class, failure.getCause().getClass());
    assertEquals(1, failure.getCause().getSuppressed().length, "the first attempt's failure");
    assertFalse(engine.holdsDataKeysOf(SUBJECT));
  }

  @Test
  void everyOperationTakesFreshCiphers() {
    FaultyCiphers ciphers = new FaultyCiphers();
    AwsKmsCryptoEngine engine = engineWith(ciphers);

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
    for (Field field : AwsKmsCryptoEngine.class.getDeclaredFields()) {
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
  private static final class FaultyCiphers implements AwsKmsCryptoEngine.CipherFactory {
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

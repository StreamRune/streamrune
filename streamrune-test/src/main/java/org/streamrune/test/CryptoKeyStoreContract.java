package org.streamrune.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * What every key-store-backed {@link CryptoEngine} that records erasure durably (filesystem,
 * PostgreSQL, and any backend that persists keys and tombstones itself) must honour, taken from the
 * interface's Javadoc: per-subject keys minted on first use; a key and an erasure survive a
 * restart; {@code deleteKey} is terminal and idempotent (decrypting old ciphertext throws {@link
 * KeyNotFoundException}, encrypting throws {@link SubjectForgottenException}, even for a subject
 * that never had a key); {@code reinstate} lifts the refusal but never brings pre-erasure
 * ciphertext back (it keeps throwing {@link KeyNotFoundException}, also after a restart); subjects
 * are isolated; null arguments fail with {@link IllegalArgumentException}; the engine is safe for
 * concurrent use. Run by {@code PostgresCryptoEngineContractTest}.
 *
 * <p>The suite pins the PostgreSQL/filesystem rules for {@code reinstate}. A shared-key engine (AWS
 * KMS, Vault) documents other outcomes there and is not run against it.
 *
 * <p>Every test uses subject ids of its own, so the engines behind {@link #newEngine()} may share
 * their storage between tests.
 */
public abstract class CryptoKeyStoreContract {

  /**
   * An engine over key storage that holds no key or tombstone for the subject ids the tests use.
   */
  protected abstract CryptoEngine newEngine();

  /**
   * A second engine over the same key storage as {@code engine}, as after a process restart:
   * nothing held in the first engine's memory (cached keys, counters) carries over.
   */
  protected abstract CryptoEngine sameKeysNewInstance(CryptoEngine engine);

  private CryptoEngine engine;

  @BeforeEach
  void freshEngine() {
    engine = newEngine();
  }

  private static SubjectId subject() {
    return SubjectId.of("crypto-contract-" + UUID.randomUUID());
  }

  private static byte[] bytes(String text) {
    return text.getBytes(UTF_8);
  }

  @Test
  void encryptThenDecryptReturnsThePlaintext() {
    var subject = subject();
    byte[] plaintext = bytes("jane@example.com");

    byte[] ciphertext = engine.encrypt(subject, plaintext);

    assertNotNull(ciphertext);
    assertFalse(
        java.util.Arrays.equals(plaintext, ciphertext), "the plaintext is not stored as is");
    assertArrayEquals(plaintext, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aSubjectsKeyIsReusedAcrossEncryptions() {
    var subject = subject();
    byte[] first = engine.encrypt(subject, bytes("one"));
    byte[] second = engine.encrypt(subject, bytes("two"));

    assertArrayEquals(bytes("one"), engine.decrypt(subject, first));
    assertArrayEquals(bytes("two"), engine.decrypt(subject, second));
  }

  @Test
  void emptyAndBinaryPlaintextRoundTrip() {
    var subject = subject();
    byte[] empty = new byte[0];
    byte[] binary = new byte[256];
    for (int i = 0; i < binary.length; i++) {
      binary[i] = (byte) i;
    }

    assertArrayEquals(empty, engine.decrypt(subject, engine.encrypt(subject, empty)));
    assertArrayEquals(binary, engine.decrypt(subject, engine.encrypt(subject, binary)));
  }

  @Test
  void decryptForASubjectWithoutAKeyThrowsKeyNotFound() {
    var neverKeyed = subject();
    byte[] foreign = engine.encrypt(subject(), bytes("someone else"));

    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(neverKeyed, foreign));
    assertFalse(engine.isKeyAvailable(neverKeyed), "decrypt must not mint a key");
  }

  @Test
  void aSubjectsCiphertextIsNotReadableUnderAnotherSubjectsKey() {
    var owner = subject();
    var other = subject();
    byte[] ciphertext = engine.encrypt(owner, bytes("owner pii"));
    engine.encrypt(other, bytes("other pii"));

    assertThrows(CryptoOperationException.class, () -> engine.decrypt(other, ciphertext));
    assertArrayEquals(bytes("owner pii"), engine.decrypt(owner, ciphertext));
  }

  @Test
  void corruptedCiphertextFailsLoudlyAndIsNotMistakenForAnErasure() {
    var subject = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("tamper me"));
    byte[] tampered = ciphertext.clone();
    tampered[tampered.length - 1] ^= 0x01;

    assertThrows(CryptoOperationException.class, () -> engine.decrypt(subject, tampered));
    assertArrayEquals(bytes("tamper me"), engine.decrypt(subject, ciphertext));
  }

  @Test
  void isKeyAvailableFollowsTheKeysLifecycle() {
    var subject = subject();
    assertFalse(engine.isKeyAvailable(subject));

    engine.encrypt(subject, bytes("x"));
    assertTrue(engine.isKeyAvailable(subject));
    assertFalse(engine.isKeyAvailable(subject()), "keys are per subject");

    engine.deleteKey(subject);
    assertFalse(engine.isKeyAvailable(subject));
  }

  @Test
  void aKeySurvivesARestart() {
    var subject = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("before restart"));

    var restarted = sameKeysNewInstance(engine);

    assertTrue(restarted.isKeyAvailable(subject));
    assertArrayEquals(bytes("before restart"), restarted.decrypt(subject, ciphertext));
    byte[] after = restarted.encrypt(subject, bytes("after restart"));
    assertArrayEquals(bytes("after restart"), engine.decrypt(subject, after));
  }

  @Test
  void deleteKeyMakesTheSubjectsCiphertextUnreadable() {
    var subject = subject();
    var bystander = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("forget me"));
    byte[] bystanders = engine.encrypt(bystander, bytes("keep me"));

    engine.deleteKey(subject);

    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, ciphertext));
    assertArrayEquals(bytes("keep me"), engine.decrypt(bystander, bystanders));
  }

  @Test
  void erasureIsTerminal_encryptAfterDeleteKeyThrowsSubjectForgotten() {
    var subject = subject();
    engine.encrypt(subject, bytes("pii"));
    engine.deleteKey(subject);

    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("new pii")));
    assertFalse(engine.isKeyAvailable(subject), "a refused encrypt must not mint a key");
  }

  @Test
  void aSubjectThatNeverHadAKeyIsTombstonedToo() {
    var subject = subject();

    engine.deleteKey(subject);

    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
  }

  @Test
  void deleteKeyIsIdempotent() {
    var subject = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("pii"));

    engine.deleteKey(subject);
    assertDoesNotThrow(() -> engine.deleteKey(subject));

    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, ciphertext));
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
  }

  @Test
  void erasureSurvivesARestart() {
    var subject = subject();
    var bystander = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("forget me"));
    byte[] bystanders = engine.encrypt(bystander, bytes("keep me"));
    engine.deleteKey(subject);

    var restarted = sameKeysNewInstance(engine);

    assertFalse(restarted.isKeyAvailable(subject));
    assertThrows(KeyNotFoundException.class, () -> restarted.decrypt(subject, ciphertext));
    assertThrows(
        SubjectForgottenException.class, () -> restarted.encrypt(subject, bytes("new pii")));
    assertDoesNotThrow(() -> restarted.deleteKey(subject));
    assertArrayEquals(bytes("keep me"), restarted.decrypt(bystander, bystanders));
  }

  @Test
  void reinstateAllowsANewKey_butPreErasureCiphertextStaysUnreadable() {
    var subject = subject();
    byte[] preForget = engine.encrypt(subject, bytes("old pii"));
    engine.deleteKey(subject);

    engine.reinstate(subject);
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(subject, preForget),
        "reinstate does not bring the destroyed key back");
    byte[] postReinstate = engine.encrypt(subject, bytes("new pii"));

    assertArrayEquals(bytes("new pii"), engine.decrypt(subject, postReinstate));
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(subject, preForget),
        "the re-minted key must not read what the destroyed one wrote");
  }

  @Test
  void reinstateAndReMintSurviveARestart_andEarlierGenerationsStayRedacted() {
    var subject = subject();
    byte[] preForget = engine.encrypt(subject, bytes("old pii"));
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, bytes("new pii"));
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] third = engine.encrypt(subject, bytes("third pii"));

    var restarted = sameKeysNewInstance(engine);

    assertThrows(KeyNotFoundException.class, () -> restarted.decrypt(subject, preForget));
    assertThrows(KeyNotFoundException.class, () -> restarted.decrypt(subject, postReinstate));
    assertArrayEquals(bytes("third pii"), restarted.decrypt(subject, third));
  }

  @Test
  void reinstateIsIdempotent_andANoOpForASubjectThatWasNeverForgotten() {
    var subject = subject();
    byte[] ciphertext = engine.encrypt(subject, bytes("pii"));

    assertDoesNotThrow(() -> engine.reinstate(subject));
    assertArrayEquals(bytes("pii"), engine.decrypt(subject, ciphertext));

    engine.deleteKey(subject);
    engine.reinstate(subject);
    assertDoesNotThrow(() -> engine.reinstate(subject));
    assertDoesNotThrow(() -> engine.encrypt(subject, bytes("again")));
  }

  @Test
  void subjectsAreIsolated_erasingOneLeavesTheOtherEncryptable() {
    var erased = subject();
    var other = subject();
    engine.encrypt(erased, bytes("a"));
    engine.encrypt(other, bytes("b"));

    engine.deleteKey(erased);

    byte[] later = engine.encrypt(other, bytes("c"));
    assertArrayEquals(bytes("c"), engine.decrypt(other, later));
    assertTrue(engine.isKeyAvailable(other));
  }

  @Test
  void nullArgumentsAreRefusedWithIllegalArgumentException() {
    var subject = subject();
    byte[] data = bytes("x");
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, data));
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(subject, null));
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, data));
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(subject, null));
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
    assertThrows(IllegalArgumentException.class, () -> engine.isKeyAvailable(null));
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentFirstEncryptionsOfOneSubjectShareOneKey() throws Exception {
    int threads = 6;
    for (int round = 0; round < 5; round++) {
      var subject = subject();
      var barrier = new CyclicBarrier(threads);
      var failures = new ConcurrentLinkedQueue<Throwable>();
      var ciphertexts = new ConcurrentLinkedQueue<byte[]>();
      List<Thread> workers = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        byte[] plaintext = bytes("pii-" + i);
        workers.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      try {
                        barrier.await();
                        ciphertexts.add(engine.encrypt(subject, plaintext));
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      for (Thread worker : workers) {
        worker.join();
      }

      assertTrue(failures.isEmpty(), "no encryption may fail: " + failures);
      var restarted = sameKeysNewInstance(engine);
      var readable = new ArrayList<String>();
      for (byte[] ciphertext : ciphertexts) {
        readable.add(new String(restarted.decrypt(subject, ciphertext), UTF_8));
      }
      assertTrue(readable.size() == threads, "every ciphertext is readable with the stored key");
    }
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentErasuresOfOneSubjectAllSucceed_andTheSubjectEndsForgotten() throws Exception {
    int threads = 6;
    for (int round = 0; round < 5; round++) {
      var subject = subject();
      byte[] ciphertext = engine.encrypt(subject, bytes("pii"));
      var barrier = new CyclicBarrier(threads);
      var failures = new ConcurrentLinkedQueue<Throwable>();
      List<Thread> workers = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        workers.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      try {
                        barrier.await();
                        engine.deleteKey(subject);
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      for (Thread worker : workers) {
        worker.join();
      }

      assertTrue(failures.isEmpty(), "no erasure may fail: " + failures);
      assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, ciphertext));
      assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
    }
  }
}

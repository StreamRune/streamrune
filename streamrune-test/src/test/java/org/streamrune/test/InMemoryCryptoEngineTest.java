package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

class InMemoryCryptoEngineTest {

  private final InMemoryCryptoEngine engine = new InMemoryCryptoEngine();

  @Test
  void preForgetCiphertextDegradesToKeyNotFoundAfterReinstateAndRemint() {
    // Production-engine parity: this double's stated purpose is that unit
    // tests built on it cannot pass while the real engines would reject the operation — so the
    // destroyed-generation semantics of the postgres/filesystem backends must hold here too:
    // erase -> reinstate -> re-mint leaves pre-forget ciphertext reading as KeyNotFoundException
    // ([REDACTED]), never a replay-blocking CryptoOperationException.
    var engine = new InMemoryCryptoEngine();
    var subject = SubjectId.of("returning-customer");
    byte[] preForget = engine.encrypt(subject, "old-pii".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    // The generation comes from the erasure itself: deleteKey recorded the destroyed generation 1,
    // so the re-mint is generation 2 without reinstate writing anything.
    assertEquals(1, preForget[0], "the first mint is generation 1");
    assertEquals(2, postReinstate[0], "the re-mint is one above the destroyed generation");
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preForget));
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void forgetOfANeverEncryptedSubject_reinstateThenEncrypt_mintsGenerationOne() {
    // Parity with the durable engines: deleteKey records a destroyed generation only when a key
    // existed, and reinstate records none. A subject forgotten before it ever encrypted has no
    // ciphertext under any generation, so its first key after reinstate is generation 1 — no
    // erase/re-register cycle is burned.
    var engine = new InMemoryCryptoEngine();
    var subject = SubjectId.of("erased-before-any-key");
    engine.deleteKey(subject); // tombstone, no key ever minted -> no recorded generation
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    assertEquals(1, postReinstate[0], "no key was destroyed, so the first mint is generation 1");
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void encryptAndDecrypt() {
    byte[] plaintext = "secret@test.com".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext = engine.encrypt(SubjectId.of("user-1"), plaintext);
    byte[] decrypted = engine.decrypt(SubjectId.of("user-1"), ciphertext);

    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  void deleteKeyMakesDecryptFail() {
    byte[] plaintext = "secret@test.com".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = engine.encrypt(SubjectId.of("user-2"), plaintext);

    engine.deleteKey(SubjectId.of("user-2"));

    assertThrows(
        KeyNotFoundException.class, () -> engine.decrypt(SubjectId.of("user-2"), ciphertext));
  }

  // This engine ships in streamrune-test and stands in for the production engines in
  // application test suites, so it honours the same deterministic-failure contract they do —
  // every blob-property rejection is raised as the CryptoMappingException subtype, never as the
  // bare CryptoOperationException the read-path classifiers read as a key-store outage (and retry
  // forever). Otherwise a test suite built on this double could not reproduce the production
  // engines' poison/quarantine routing for a corrupted blob.

  @Test
  void decryptRejectsTruncatedCiphertextAsDeterministic() {
    engine.encrypt(SubjectId.of("short"), "data".getBytes(StandardCharsets.UTF_8));

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () ->
                engine.decrypt(
                    SubjectId.of("short"), new byte[] {1})); // right generation, no IV/tag
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
  }

  @Test
  void decryptOfATooShortBlobCarryingAnErasedGenerationByteStillReadsAsKeyNotFound() {
    // The erased-generation check must win over the too-short rejection — a blob this
    // short can never decrypt regardless of generation, but a generation the current key has
    // already outlived is still the designed [REDACTED] signal (GDPR erasure), not a
    // length-corruption verdict. The erasure destroys a real generation-1 key; the synthetic
    // 1-byte blob (far below minLength) carries that destroyed generation.
    var engine = new InMemoryCryptoEngine();
    var subject = SubjectId.of("short-and-erased");
    engine.encrypt(subject, "old-pii".getBytes(StandardCharsets.UTF_8)); // mints generation 1
    engine.deleteKey(subject); // destroys generation 1 and records it
    engine.reinstate(subject);
    engine.encrypt(subject, "new-pii".getBytes(StandardCharsets.UTF_8)); // mints generation 2

    assertThrows(
        KeyNotFoundException.class,
        () ->
            engine.decrypt(subject, new byte[] {1}), // too short to ever decrypt; generation 1 < 2
        "a short blob carrying a destroyed generation must read as [REDACTED], never \"too"
            + " short\"");
  }

  @Test
  void decryptOfATrulyEmptyBlobStillReadsAsTooShort() {
    // Boundary for the reorder above: an empty blob has no generation byte to read at all,
    // so the guard must fall through to "too short" rather than an ArrayIndexOutOfBoundsException.
    engine.encrypt(SubjectId.of("empty"), "data".getBytes(StandardCharsets.UTF_8));

    var thrown =
        assertThrows(
            CryptoMappingException.class, () -> engine.decrypt(SubjectId.of("empty"), new byte[0]));
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
  }

  @Test
  void decryptRejectsUnknownKeyVersionAsDeterministic() {
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("keyver"), "data".getBytes(StandardCharsets.UTF_8));
    ciphertext[0] = 7; // no such key generation exists

    var thrown =
        assertThrows(
            CryptoMappingException.class, () -> engine.decrypt(SubjectId.of("keyver"), ciphertext));
    assertTrue(thrown.getMessage().contains("key version"), thrown.getMessage());
  }

  @Test
  void decryptRejectsTamperedTagAsDeterministic() {
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("tag"), "data".getBytes(StandardCharsets.UTF_8));
    ciphertext[ciphertext.length - 1] ^= 0x01; // corrupt the GCM tag

    var thrown =
        assertThrows(
            CryptoMappingException.class, () -> engine.decrypt(SubjectId.of("tag"), ciphertext));
    assertInstanceOf(javax.crypto.AEADBadTagException.class, thrown.getCause());
  }

  @Test
  void isKeyAvailable() {
    assertFalse(engine.isKeyAvailable(SubjectId.of("user-3")));

    engine.encrypt(SubjectId.of("user-3"), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(engine.isKeyAvailable(SubjectId.of("user-3")));

    engine.deleteKey(SubjectId.of("user-3"));
    assertFalse(engine.isKeyAvailable(SubjectId.of("user-3")));
  }

  @Test
  void differentSubjectsGetDifferentKeys() {
    byte[] plaintext = "same-plaintext".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext1 = engine.encrypt(SubjectId.of("subject-a"), plaintext);
    byte[] ciphertext2 = engine.encrypt(SubjectId.of("subject-b"), plaintext);

    // Ciphertexts must differ (different keys + different random IVs)
    assertFalse(Arrays.equals(ciphertext1, ciphertext2));
  }

  @Test
  void encryptProducesDifferentCiphertextEachTime() {
    byte[] plaintext = "same-data".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext1 = engine.encrypt(SubjectId.of("user-4"), plaintext);
    byte[] ciphertext2 = engine.encrypt(SubjectId.of("user-4"), plaintext);

    // Same key but different IV should produce different ciphertext
    assertFalse(Arrays.equals(ciphertext1, ciphertext2));

    // Both should decrypt to the same plaintext
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("user-4"), ciphertext1));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("user-4"), ciphertext2));
  }

  @Test
  void encryptRejectsNullSubjectId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> engine.encrypt(null, "data".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void decryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, new byte[] {1, 2, 3}));
  }

  @Test
  void encryptRejectsNullPlaintext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("user-5"), null));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("user-6"), null));
  }

  @Test
  void encryptAfterDeleteThrowsSubjectForgotten() {
    // Terminal erasure (mirrors PostgresCryptoEngine / FileSystemCryptoEngine): once a subject is
    // crypto-shredded, a later encrypt must fail loudly instead of silently minting a fresh key.
    engine.encrypt(SubjectId.of("forgotten-1"), "data".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("forgotten-1"));

    assertThrows(
        SubjectForgottenException.class,
        () ->
            engine.encrypt(SubjectId.of("forgotten-1"), "again".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void decryptAfterDeleteThrowsKeyNotFound() {
    // A forget makes old ciphertext undecryptable — KeyNotFoundException, never
    // SubjectForgottenException (that is reserved for the encrypt path).
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("forgotten-2"), "data".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("forgotten-2"));

    assertThrows(
        KeyNotFoundException.class, () -> engine.decrypt(SubjectId.of("forgotten-2"), ciphertext));
  }

  @Test
  void reinstateAllowsRecreation() {
    // The only re-registration path: re-registration is refused until reinstate lifts the
    // tombstone, after which encrypt mints a new key again.
    engine.encrypt(SubjectId.of("forgotten-3"), "data".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("forgotten-3"));
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forgotten-3"), "x".getBytes(StandardCharsets.UTF_8)));
    engine.reinstate(SubjectId.of("forgotten-3"));

    byte[] ciphertext =
        engine.encrypt(SubjectId.of("forgotten-3"), "fresh".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(
        "fresh".getBytes(StandardCharsets.UTF_8),
        engine.decrypt(SubjectId.of("forgotten-3"), ciphertext));
  }

  @Test
  void reinstateIsNoOpForNeverForgottenSubject() {
    // Idempotent: reinstating a subject that was never forgotten does nothing and does not throw.
    assertDoesNotThrow(() -> engine.reinstate(SubjectId.of("never-forgotten")));
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("never-forgotten"), "data".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(
        "data".getBytes(StandardCharsets.UTF_8),
        engine.decrypt(SubjectId.of("never-forgotten"), ciphertext));
  }

  @Test
  void reinstateRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
  }

  @Test
  void deleteKeyRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
  }
}

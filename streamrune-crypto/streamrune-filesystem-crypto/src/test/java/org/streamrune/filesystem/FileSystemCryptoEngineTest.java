package org.streamrune.filesystem;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

class FileSystemCryptoEngineTest {

  @TempDir Path tempDir;

  private FileSystemCryptoEngine engine;

  @BeforeEach
  void setUp() {
    engine = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
  }

  @Test
  void perSubjectLockStructureIsBounded_notAnUnboundedMap() throws Exception {
    // Encrypting a high-cardinality subject space must not leak one lock per distinct
    // subject. The per-subject locks must be a fixed-size stripe array, not an unbounded map.
    for (int i = 0; i < 500; i++) {
      engine.encrypt(SubjectId.of("subject-" + i), ("d" + i).getBytes(StandardCharsets.UTF_8));
    }
    var field = FileSystemCryptoEngine.class.getDeclaredField("subjectLocks");
    field.setAccessible(true);
    Object locks = field.get(engine);
    assertInstanceOf(
        java.util.concurrent.locks.ReentrantLock[].class,
        locks,
        "per-subject locks must be a fixed-size stripe array, not an unbounded map that leaks a"
            + " lock per distinct subject id");
    int stripes = java.lang.reflect.Array.getLength(locks);
    assertTrue(
        stripes > 0 && stripes <= 1024,
        "stripe count must stay bounded regardless of subject cardinality; was " + stripes);
  }

  @Test
  void encryptAndDecryptRoundtrip() {
    byte[] plaintext = "hello world".getBytes();

    byte[] ciphertext = engine.encrypt(SubjectId.of("user-1"), plaintext);
    assertNotNull(ciphertext);
    assertNotEquals(plaintext, ciphertext);

    byte[] decrypted = engine.decrypt(SubjectId.of("user-1"), ciphertext);
    assertEquals("hello world", new String(decrypted));
  }

  @Test
  void encryptProducesDifferentCiphertextEachTime() {
    byte[] plaintext = "hello".getBytes();
    byte[] c1 = engine.encrypt(SubjectId.of("user-2"), plaintext);
    byte[] c2 = engine.encrypt(SubjectId.of("user-2"), plaintext);
    assertFalse(java.util.Arrays.equals(c1, c2), "IV ensures different ciphertext");
  }

  @Test
  void isKeyAvailableReturnsTrueAfterEncrypt() {
    engine.encrypt(SubjectId.of("user-3"), "data".getBytes());
    assertTrue(engine.isKeyAvailable(SubjectId.of("user-3")));
  }

  @Test
  void isKeyAvailableReturnsFalseForUnknownSubject() {
    assertFalse(engine.isKeyAvailable(SubjectId.of("unknown-subject")));
  }

  @Test
  void decryptThrowsKeyNotFoundExceptionForUnknownSubject() {
    byte[] ciphertext = "somebytes".getBytes();
    assertThrows(
        KeyNotFoundException.class, () -> engine.decrypt(SubjectId.of("nonexistent"), ciphertext));
  }

  @Test
  void deleteKeyRemovesKeyFile() {
    engine.encrypt(SubjectId.of("user-4"), "data".getBytes());
    assertTrue(engine.isKeyAvailable(SubjectId.of("user-4")));

    engine.deleteKey(SubjectId.of("user-4"));
    assertFalse(engine.isKeyAvailable(SubjectId.of("user-4")));
  }

  @Test
  void nonAsciiSubjectIdRoundtripsAndUsesUtf8KeyPath() throws Exception {
    SubjectId subjectId = SubjectId.of("müller-žížala");
    byte[] plaintext = "data".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext = engine.encrypt(subjectId, plaintext);
    assertArrayEquals(plaintext, engine.decrypt(subjectId, ciphertext));

    // The key path must derive from the UTF-8 bytes of the subjectId, never the platform default
    // charset — otherwise two JVMs with different file.encoding resolve different paths and one
    // of them reports the key as missing (the subject's data would appear crypto-shredded).
    var md = MessageDigest.getInstance("SHA-256");
    String hash =
        HexFormat.of().formatHex(md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8)));
    Path expected = tempDir.resolve(hash.substring(0, 2)).resolve(hash + ".subjectId");
    assertTrue(Files.exists(expected), "key file must live at the UTF-8-derived path");
  }

  // --- Terminal erasure (crypto-shredding tombstone) ---

  @Test
  void encryptAfterDeleteThrowsSubjectForgotten() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("forget-me"), "secret".getBytes());
    assertNotNull(ciphertext);

    engine.deleteKey(SubjectId.of("forget-me"));

    // The terminal intent of GDPR erasure: a later encrypt must not mint a fresh key.
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forget-me"), "more PII".getBytes()));
  }

  @Test
  void tombstoneSurvivesAcrossEngineInstances() {
    engine.encrypt(SubjectId.of("persist-forget"), "secret".getBytes());
    engine.deleteKey(SubjectId.of("persist-forget"));

    // A fresh engine over the same directory must still see the tombstone — erasure is durable.
    var reopened = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    assertThrows(
        SubjectForgottenException.class,
        () -> reopened.encrypt(SubjectId.of("persist-forget"), "data".getBytes()));
  }

  @Test
  void decryptAfterDeleteStillThrowsKeyNotFound() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("decrypt-forget"), "secret".getBytes());
    engine.deleteKey(SubjectId.of("decrypt-forget"));

    // Old ciphertext maps to the [REDACTED] tombstone via KeyNotFoundException, NOT
    // SubjectForgottenException — that distinction is the contract.
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("decrypt-forget"), ciphertext));
  }

  @Test
  void reinstateAllowsRecreationAfterForget() {
    engine.encrypt(SubjectId.of("comeback"), "old".getBytes());
    engine.deleteKey(SubjectId.of("comeback"));
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("comeback"), "x".getBytes()));

    engine.reinstate(SubjectId.of("comeback"));

    // After an explicit reinstate a brand-new key is minted and a full roundtrip works again.
    byte[] ciphertext = engine.encrypt(SubjectId.of("comeback"), "new".getBytes());
    assertArrayEquals("new".getBytes(), engine.decrypt(SubjectId.of("comeback"), ciphertext));
  }

  @Test
  void preForgetCiphertextDegradesToKeyNotFoundAfterReinstateAndRemint() {
    // The documented reinstate flow must not convert
    // lawfully erased history into replay-blocking poison. After erase -> reinstate -> first
    // re-encrypt, decrypting PRE-forget ciphertext must yield KeyNotFoundException (mapped to
    // [REDACTED] by CryptoShreddingModule, keeping aggregate loads and rebuilds working) — not a
    // GCM tag failure wrapped in CryptoOperationException, which propagates and permanently
    // blocks replay of the returning subject's stream.
    var subject = SubjectId.of("returning-customer");
    byte[] preForget = engine.encrypt(subject, "old-pii".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    // The generation comes from the erasure itself: deleteKey recorded the destroyed generation 1
    // in the .generation file, so the re-mint is generation 2 without reinstate writing anything.
    assertEquals(1, preForget[1], "the first mint is generation 1");
    assertEquals(2, postReinstate[1], "the re-mint is one above the destroyed generation");
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preForget));
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void everyPriorGenerationStaysRedactedAcrossRepeatedEraseReinstateCycles() {
    var subject = SubjectId.of("twice-returning");
    byte[] gen1 = engine.encrypt(subject, "gen1".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] gen2 = engine.encrypt(subject, "gen2".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] gen3 = engine.encrypt(subject, "gen3".getBytes());

    // Every destroyed generation stays replay-tolerant; the live one round-trips.
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, gen1));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, gen2));
    assertArrayEquals("gen3".getBytes(), engine.decrypt(subject, gen3));
  }

  @Test
  void tamperedCurrentGenerationCiphertextStillFailsLoudAfterReinstate() {
    // Taxonomy preserved: only DESTROYED-generation ciphertext degrades to [REDACTED].
    // Tampering with the CURRENT generation's ciphertext keeps failing loudly — even for a
    // reinstated subject — so corruption can never masquerade as a lawful forget.
    var subject = SubjectId.of("tamper-after-reinstate");
    engine.encrypt(subject, "old".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] current = engine.encrypt(subject, "new".getBytes());
    current[current.length - 1] ^= 0x01; // corrupt the GCM tag

    // KeyNotFoundException is NOT a subtype of CryptoOperationException, so this assertThrows
    // by itself proves the tamper did not degrade to the replay-tolerant forget signal.
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(subject, current));
  }

  @Test
  void versionByteBelowOneNeverRedacts_failsLoud() {
    // The destroyed-generation tolerance is bounded to [1, currentKeyVersion): version byte 0 or
    // negative is not a generation this engine ever minted, so it must stay a loud corruption
    // error — otherwise flipping the byte low would silently redact ANY subject's field,
    // including never-erased ones.
    var subject = SubjectId.of("rollback-guard");
    byte[] ciphertext = engine.encrypt(subject, "data".getBytes());
    byte[] zeroVersion = ciphertext.clone();
    zeroVersion[1] = 0;
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(subject, zeroVersion));
    byte[] negativeVersion = ciphertext.clone();
    negativeVersion[1] = -3;
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(subject, negativeVersion));
  }

  @Test
  void crashedErasureRevivalStillDecryptsPreErasureCiphertext() throws Exception {
    // The documented crashed-deleteKey residue: tombstone written, key file survived. reinstate
    // clears the tombstone and returns the ORIGINAL key to service — pre-erasure ciphertext is
    // readable again (gdpr-erasure.md, "reinstate — undoing an erasure"). The generation
    // machinery must not break this: no re-mint happened, so the revived key's generation is
    // unchanged and its ciphertext still matches.
    var subject = SubjectId.of("crashed-erasure-revival");
    byte[] preErasure = engine.encrypt(subject, "still-mine".getBytes());
    // Simulate deleteKey dying right after its tombstone write (before the key delete).
    Files.createDirectories(resolveTombstonePathForTest(subject).getParent());
    Files.createFile(resolveTombstonePathForTest(subject));

    engine.reinstate(subject);
    assertArrayEquals("still-mine".getBytes(), engine.decrypt(subject, preErasure));

    // A later COMPLETED erasure + reinstate + re-mint then redacts that same old blob.
    engine.deleteKey(subject);
    engine.reinstate(subject);
    engine.encrypt(subject, "new-life".getBytes());
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preErasure));
  }

  @Test
  void corruptKeyFileNeverBlocksErasure_andBurnsTheGenerationCounter() throws Exception {
    // GDPR erasure must succeed even when the key file is corrupt — but the corrupt key's
    // generation is unknowable, so the counter is burned: a later same-id re-mint is refused
    // (fresh-subject-id guidance) rather than risking a generation collision with whatever the
    // corrupt key's ciphertext claims.
    var subject = SubjectId.of("corrupt-key-erasure");
    engine.encrypt(subject, "data".getBytes());
    Files.write(resolveKeyPathForTest(subject), new byte[] {1, 2, 3}); // corrupt the key file

    assertDoesNotThrow(() -> engine.deleteKey(subject), "erasure must not be blocked");
    assertFalse(Files.exists(resolveKeyPathForTest(subject)), "key file must be gone");

    engine.reinstate(subject);
    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.encrypt(subject, "new".getBytes()));
    assertTrue(
        thrown.getMessage().contains("fresh subject id"),
        "the burned counter must surface fresh-subject-id guidance: " + thrown.getMessage());
  }

  @Test
  void corruptGenerationRecordFailsTheMintLoud() throws Exception {
    // Minting with an unreadable counter must fail LOUD — silently assuming 0 would reuse a
    // destroyed generation and flip its [REDACTED] blobs back into wrong-key GCM failures.
    var subject = SubjectId.of("corrupt-generation-mint");
    engine.encrypt(subject, "old".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    Files.writeString(generationPathFor(subject), "garbage"); // disk corruption of the record

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.encrypt(subject, "new".getBytes()));
    assertTrue(
        thrown.getMessage().contains("key-generation record"),
        "the mint must name the unreadable record: " + thrown.getMessage());
  }

  @Test
  void corruptGenerationRecordNeverBlocksErasure_counterIsBurned() throws Exception {
    // An erasure that destroys a live key over a CORRUPT generation record must still succeed —
    // GDPR erasure is never blocked by record corruption. The record's true value is unknowable,
    // so the counter is burned (recorded at the maximum): no future mint can collide with any
    // destroyed generation, and same-id re-registration is refused with actionable guidance.
    var subject = SubjectId.of("corrupt-generation-erasure");
    byte[] preForget = engine.encrypt(subject, "old".getBytes());
    Files.writeString(generationPathFor(subject), "garbage"); // disk corruption of the record

    assertDoesNotThrow(() -> engine.deleteKey(subject), "erasure must not be blocked");

    engine.reinstate(subject);
    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.encrypt(subject, "new".getBytes()));
    assertTrue(thrown.getMessage().contains("fresh subject id"), thrown.getMessage());
    // And the pre-forget blob stays non-resurrectable.
    assertThrows(Exception.class, () -> engine.decrypt(subject, preForget));
  }

  private Path generationPathFor(SubjectId subject) throws Exception {
    Path keyPath = resolveKeyPathForTest(subject);
    return keyPath.resolveSibling(
        keyPath.getFileName().toString().replace(".subjectId", ".generation"));
  }

  @Test
  void forgetOfANeverEncryptedSubject_reinstateThenEncrypt_mintsGenerationOne() throws Exception {
    // deleteKey records a destroyed generation only when it destroys a key file, and reinstate
    // records none. A subject forgotten before it ever encrypted has no ciphertext under any
    // generation, so its first key after reinstate is generation 1 and no .generation record is
    // written — no erase/re-register cycle is burned.
    var subject = SubjectId.of("fs-erased-before-any-key");
    engine.deleteKey(subject); // tombstone, no key file ever minted -> no generation record
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    assertEquals(1, postReinstate[1], "no key was destroyed, so the first mint is generation 1");
    assertFalse(Files.exists(generationPathFor(subject)), "reinstate must record no generation");
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void reinstateIsIdempotentForNeverForgottenSubject() {
    // Reinstating a subject that was never forgotten is a harmless no-op.
    engine.reinstate(SubjectId.of("never-forgotten"));
    byte[] ciphertext = engine.encrypt(SubjectId.of("never-forgotten"), "data".getBytes());
    assertArrayEquals(
        "data".getBytes(), engine.decrypt(SubjectId.of("never-forgotten"), ciphertext));
  }

  @Test
  void reinstateRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
  }

  @Test
  void blankSubjectIdIsRejectedBySubjectIdBeforeTheEngineSeesIt() {
    // The engine checks only for null: SubjectId's own constructor rejects an empty or blank
    // value, so a blank subject id cannot be constructed, let alone passed to encrypt, decrypt,
    // deleteKey, reinstate or isKeyAvailable.
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of("  "));
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of(""));
  }

  @Test
  void isKeyAvailableIsFalseAfterForget() {
    engine.encrypt(SubjectId.of("avail-forget"), "data".getBytes());
    engine.deleteKey(SubjectId.of("avail-forget"));
    // The key file is gone; a tombstone does not count as an available key.
    assertFalse(engine.isKeyAvailable(SubjectId.of("avail-forget")));
  }

  @Test
  void deleteKeyRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
  }

  @Test
  void keyDirectoryIsCreatedIfMissing() {
    Path nestedDir = tempDir.resolve("nested/deep");
    var nestedEngine = FileSystemCryptoEngine.builder().keyDirectory(nestedDir).build();

    nestedEngine.encrypt(SubjectId.of("user-5"), "data".getBytes());

    assertTrue(Files.exists(nestedDir));
  }

  @Test
  void differentSubjectsHaveDifferentKeys() {
    byte[] ct1 = engine.encrypt(SubjectId.of("subject-A"), "same".getBytes());
    byte[] ct2 = engine.encrypt(SubjectId.of("subject-B"), "same".getBytes());

    byte[] pt1 = engine.decrypt(SubjectId.of("subject-A"), ct1);
    byte[] pt2 = engine.decrypt(SubjectId.of("subject-B"), ct2);

    assertEquals("same", new String(pt1));
    assertEquals("same", new String(pt2));
  }

  @Test
  void builderSetsKeyDirectory() {
    var engine = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();

    engine.encrypt(SubjectId.of("user-6"), "data".getBytes());
    assertTrue(engine.isKeyAvailable(SubjectId.of("user-6")));
  }

  @Test
  void builderRejectsMissingKeyDirectory() {
    assertThrows(IllegalStateException.class, () -> FileSystemCryptoEngine.builder().build());
  }

  @Test
  void encryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, "data".getBytes()));
  }

  @Test
  void encryptRejectsNullPlaintext() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("user"), null));
  }

  @Test
  void decryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, "cipher".getBytes()));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("user"), null));
  }

  @Test
  void isKeyAvailableRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.isKeyAvailable(null));
  }

  @Test
  void concurrentFirstEncryptsForOneSubjectNeverOrphanCiphertext() throws Exception {
    // Race the first-key creation: every thread encrypts before any key file exists. Exactly one
    // key may win; every produced ciphertext must remain decryptable afterwards.
    int threads = 8;
    var barrier = new java.util.concurrent.CyclicBarrier(threads);
    var ciphertexts = new java.util.concurrent.ConcurrentLinkedQueue<byte[]>();

    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(threads)) {
      var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < threads; i++) {
        final int n = i;
        futures.add(
            executor.submit(
                () -> {
                  barrier.await();
                  ciphertexts.add(
                      engine.encrypt(SubjectId.of("race-subject"), ("payload-" + n).getBytes()));
                  return null;
                }));
      }
      for (var f : futures) {
        f.get();
      }
    }

    assertEquals(threads, ciphertexts.size());
    for (byte[] ciphertext : ciphertexts) {
      String plaintext = new String(engine.decrypt(SubjectId.of("race-subject"), ciphertext));
      assertTrue(plaintext.startsWith("payload-"), "ciphertext must decrypt under the winning key");
    }
  }

  // --- encrypt() racing deleteKey() (TOCTOU: tombstone-check vs. key-file create) ---

  /**
   * Forces the exact TOCTOU interleave via a test-only seam ({@link
   * FileSystemCryptoEngine#setEncryptRaceHook}): a hook invoked by the engine right after the
   * tombstone-file check and right before a fresh key file would be created. The hook signals that
   * encrypt() has reached the checkpoint, then gives deleteKey() (running on a second thread) a
   * bounded window to run.
   *
   * <p>On the unfixed engine, deleteKey() is unsynchronized with this checkpoint, so it writes its
   * tombstone and removes the key file within the window — BEFORE encrypt() resumes and creates a
   * fresh key file, reproducing the bug: a live key file ends up coexisting with a tombstone file.
   * On the fixed engine, both operations hold the same per-subject lock, so deleteKey() cannot even
   * start its critical section until encrypt() releases the lock; the bounded wait simply elapses
   * and deleteKey() runs afterwards. Either way the interleave is deterministic — no probabilistic
   * timing, no flakiness — and the post-condition (never a live key file + tombstone file together)
   * is checked after both threads finish.
   */
  @Test
  void encryptRacingDeleteKey_forgottenAlwaysWins() throws Exception {
    int iterations = 20;
    for (int i = 0; i < iterations; i++) {
      // Brand-new subject: no key file, no tombstone file yet. This is what makes encrypt() take
      // the tombstone-check/create-key branch (where the hook fires) instead of the
      // load-existing-key fast path.
      SubjectId subject = SubjectId.of("fs-race-forget-" + i);

      var reachedCheckpoint = new java.util.concurrent.CountDownLatch(1);
      FileSystemCryptoEngine.setEncryptRaceHook(
          () -> {
            reachedCheckpoint.countDown();
            // Bounded, not unbounded: once the fix serializes the two operations via the
            // per-subject lock, deleteKey() cannot complete while this hook (holding the lock) is
            // running — waiting unboundedly for it here would deadlock. A short sleep is enough to
            // give the racy (unfixed) implementation its window to interleave; the fixed
            // implementation just lets the sleep elapse harmlessly.
            sleep(300);
          });
      try {
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
          var encryptFuture =
              executor.submit(() -> engine.encrypt(subject, "resurrected PII".getBytes()));
          var deleteFuture =
              executor.submit(
                  (java.util.concurrent.Callable<Void>)
                      () -> {
                        await(reachedCheckpoint);
                        engine.deleteKey(subject);
                        return null;
                      });

          Boolean encryptThrewForgotten;
          try {
            encryptFuture.get(15, java.util.concurrent.TimeUnit.SECONDS);
            encryptThrewForgotten = false;
          } catch (java.util.concurrent.ExecutionException e) {
            encryptThrewForgotten = e.getCause() instanceof SubjectForgottenException;
            if (!encryptThrewForgotten) {
              throw new AssertionError("unexpected encrypt failure", e.getCause());
            }
          }
          deleteFuture.get(15, java.util.concurrent.TimeUnit.SECONDS);

          // Post-condition: "forgotten wins" must hold no matter how the race resolved. Either
          // encrypt observed the tombstone and threw, or it created its key file before the
          // delete's tombstone landed — but a live key file must NEVER coexist with a tombstone
          // file for the same subject afterwards.
          boolean keyExists = Files.exists(resolveKeyPathForTest(subject));
          boolean tombstoneExists = Files.exists(resolveTombstonePathForTest(subject));
          assertFalse(
              keyExists && tombstoneExists,
              "iteration "
                  + i
                  + ": a live key file must never coexist with a tombstone file"
                  + " (encryptThrewForgotten="
                  + encryptThrewForgotten
                  + ")");
        } finally {
          executor.shutdownNow();
        }
      } finally {
        FileSystemCryptoEngine.setEncryptRaceHook(null);
      }
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void await(java.util.concurrent.CountDownLatch latch) {
    try {
      if (!latch.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private Path resolveKeyPathForTest(SubjectId subjectId) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    String hash =
        HexFormat.of().formatHex(md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8)));
    return tempDir.resolve(hash.substring(0, 2)).resolve(hash + ".subjectId");
  }

  private Path resolveTombstonePathForTest(SubjectId subjectId) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    String hash =
        HexFormat.of().formatHex(md.digest(subjectId.value().getBytes(StandardCharsets.UTF_8)));
    return tempDir.resolve(hash.substring(0, 2)).resolve(hash + ".forgotten");
  }

  @Test
  void encryptPrependsVersionHeader() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("header-subject"), "hello".getBytes());

    // header(2) + GCM_IV(12) + GCM_TAG(16) + plaintext(5) = 35
    assertEquals(35, ciphertext.length);
    assertEquals(1, ciphertext[0], "format version");
    assertEquals(1, ciphertext[1], "key version");
  }

  // Every blob-property rejection below is DETERMINISTIC and must be raised as the
  // CryptoMappingException subtype. The read-path classifiers (SagaStateConversion,
  // ReadPoisonClassifier, SagaCommandDispatch.isRetryLater, the event stores'
  // hasTransientCryptoCause) read a BARE CryptoOperationException as key-store OUTAGE evidence and
  // retry forever — on this engine, wired by all three integrations, a corrupted blob would wedge
  // the subscription instead of being quarantined. PostgresCryptoEngine already has this; the
  // filesystem engine is its sibling.

  @Test
  void decryptRejectsUnknownFormatVersion() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("format-subject"), "data".getBytes());
    ciphertext[0] = 9; // a future/foreign format

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("format-subject"), ciphertext));
    assertTrue(thrown.getMessage().contains("format version"), thrown.getMessage());
  }

  @Test
  void decryptRejectsUnknownKeyVersion() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("keyver-subject"), "data".getBytes());
    ciphertext[1] = 7; // no such key version exists

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("keyver-subject"), ciphertext));
    assertTrue(thrown.getMessage().contains("key version"), thrown.getMessage());
  }

  @Test
  void decryptRejectsTruncatedCiphertext() {
    engine.encrypt(SubjectId.of("short-subject"), "data".getBytes());

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("short-subject"), new byte[10]));
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
  }

  @Test
  void decryptOfATooShortBlobCarryingAnErasedGenerationByteStillReadsAsKeyNotFound() {
    // The erased-generation check must win
    // over the too-short rejection — a blob this short can never decrypt regardless of
    // generation, but a generation the current key has already outlived is still the designed
    // [REDACTED] signal (GDPR erasure), not a length-corruption verdict. This engine's header is
    // 2 bytes (FORMAT_VERSION, generation), unlike InMemoryCryptoEngine's 1-byte header — the
    // synthetic blob below carries both. The erasure destroys a real generation-1 key.
    var subject = SubjectId.of("short-and-erased");
    engine.encrypt(subject, "old-pii".getBytes()); // mints generation 1
    engine.deleteKey(subject); // destroys generation 1 and records it
    engine.reinstate(subject);
    engine.encrypt(subject, "new-pii".getBytes()); // mints generation 2

    assertThrows(
        KeyNotFoundException.class,
        // format=1 (valid), generation=1 (< current generation 2); far below the IV+tag minimum.
        () -> engine.decrypt(subject, new byte[] {1, 1}),
        "a short blob carrying a destroyed generation must read as [REDACTED], never \"too"
            + " short\"");
  }

  @Test
  void decryptOfABlobBelowHeaderLengthStillReadsAsTooShort() {
    // Boundary for the reorder above: a blob shorter than the 2-byte header has no
    // generation byte safely readable at all, so the guard must fall through to "too short"
    // rather than an ArrayIndexOutOfBoundsException.
    engine.encrypt(SubjectId.of("sub-header"), "data".getBytes());

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("sub-header"), new byte[] {1}));
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
  }

  @Test
  void decryptRejectsTamperedTagAsDeterministic() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("tag-subject"), "data".getBytes());
    ciphertext[ciphertext.length - 1] ^= 0x01; // corrupt the GCM tag

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("tag-subject"), ciphertext));
    assertInstanceOf(
        javax.crypto.AEADBadTagException.class,
        thrown.getCause(),
        "the AEAD tag mismatch is the cause, raised through the deterministic subtype");
  }

  @Test
  void corruptedKeyFileFailsWithClearErrorInsteadOfSilentGarbage() throws Exception {
    byte[] ciphertext = engine.encrypt(SubjectId.of("corrupt-subject"), "data".getBytes());

    // Corrupt the stored key on disk: decryption must fail loudly, never return garbage.
    var md = MessageDigest.getInstance("SHA-256");
    String hash =
        HexFormat.of().formatHex(md.digest("corrupt-subject".getBytes(StandardCharsets.UTF_8)));
    Path keyFile = tempDir.resolve(hash.substring(0, 2)).resolve(hash + ".subjectId");
    Files.write(keyFile, new byte[] {1, 2, 3, 4, 5});

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("corrupt-subject"), ciphertext));
  }

  // ----- Stray tmp hard links must not outlive the erasure -----

  /** The subject's key path, mirroring {@code FileSystemCryptoEngine#resolveKeyPath}. */
  private Path keyPathFor(String subjectId) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    String hash = HexFormat.of().formatHex(md.digest(subjectId.getBytes(StandardCharsets.UTF_8)));
    return tempDir.resolve(hash.substring(0, 2)).resolve(hash + ".subjectId");
  }

  @Test
  void deleteKeySweepsStrayTmpHardLinksFromACrashedMint() throws Exception {
    byte[] ciphertext = engine.encrypt(SubjectId.of("crashed-mint"), "secret PII".getBytes());
    Path keyPath = keyPathFor("crashed-mint");
    assertTrue(Files.exists(keyPath));

    // Simulate a hard crash between Files.createLink(path, tmp) and the finally-block tmp cleanup
    // in getOrCreateKey: a SECOND directory entry for the SAME key inode survives, named exactly
    // like a Files.createTempFile(dir, keyFileName, ".tmp") product.
    Path strayTmp = keyPath.resolveSibling(keyPath.getFileName() + "8123456789012345678.tmp");
    Files.createLink(strayTmp, keyPath);

    engine.deleteKey(SubjectId.of("crashed-mint"));

    // GDPR erasure must remove EVERY directory entry of the key inode — deleting only keyPath
    // leaves the key material readable through the stray link forever, while isKeyAvailable
    // reports false and the erasure reports success.
    assertFalse(Files.exists(keyPath), "key file must be deleted");
    assertFalse(
        Files.exists(strayTmp),
        "the stray tmp hard link must be swept — it keeps the AES key inode alive after erasure");
    try (var entries = Files.list(keyPath.getParent())) {
      // The per-subject .generation record legitimately survives erasure (it is what
      // keeps a post-reinstate re-mint on a higher generation so destroyed-generation ciphertext
      // stays [REDACTED]); it holds a small integer — no key material, no PII.
      assertTrue(
          entries.allMatch(
              p ->
                  p.getFileName().toString().endsWith(".forgotten")
                      || p.getFileName().toString().endsWith(".generation")),
          "after erasure the shard dir must hold nothing but the tombstone and the generation"
              + " record");
    }
    // And the ciphertext is genuinely dead: no surviving key file can decrypt it.
    assertThrows(
        KeyNotFoundException.class, () -> engine.decrypt(SubjectId.of("crashed-mint"), ciphertext));
  }

  @Test
  void deleteKeySweepLeavesUnrelatedTmpFilesAlone() throws Exception {
    engine.encrypt(SubjectId.of("sweep-precise"), "secret".getBytes());
    Path keyPath = keyPathFor("sweep-precise");
    Path strayTmp = keyPath.resolveSibling(keyPath.getFileName() + "42.tmp");
    Files.createLink(strayTmp, keyPath);
    // An unrelated tmp in the same shard dir (different prefix — e.g. another subject's residue)
    // must survive: the sweep is scoped to THIS subject's key-file prefix.
    Path unrelatedTmp = keyPath.resolveSibling("0000unrelated.subjectId7.tmp");
    Files.write(unrelatedTmp, new byte[] {1, 2, 3});

    engine.deleteKey(SubjectId.of("sweep-precise"));

    assertFalse(Files.exists(strayTmp), "the subject's stray tmp link must be swept");
    assertTrue(
        Files.exists(unrelatedTmp),
        "an unrelated tmp file must not be swept by another subject's erasure");
  }

  // ----- A mint must never delete a peer's in-flight tmp -----

  /**
   * The cross-process repro: two engine instances (= two processes; per-instance stripe arrays,
   * only disk state shared) first-encrypt the same brand-new subject concurrently. The tmp-written
   * seam ({@link FileSystemCryptoEngine#setMintTmpWrittenHook}) forces the exact interleaving: A
   * writes its pre-link tmp → B runs its whole mint prologue (which, pre-fix, included the
   * mint-time stray-tmp heal) and parks just before B's own {@code createLink} → A resumes into its
   * {@code createLink} → B is released.
   *
   * <p>Pre-fix, B's heal deleted A's LIVE pre-link tmp (its safety argument — "no other thread can
   * be mid-mint for this subject" — is only true in-JVM), so A's {@code createLink} threw {@code
   * NoSuchFileException} and a legitimate first encrypt failed spuriously with {@code
   * CryptoOperationException}, burning the command that carried it. Post-fix no mint deletes tmps
   * at all: A's link wins, B's link loses ({@code FileAlreadyExistsException}) and adopts A's key —
   * the designed first-writer-wins outcome, with exactly one key file and no spurious failure.
   */
  @Test
  void crossProcessConcurrentFirstMintsDoNotDeleteEachOthersInFlightTmp() throws Exception {
    var processA = engine;
    var processB = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    SubjectId subject = SubjectId.of("xproc-live-tmp");

    var phase = new java.util.concurrent.atomic.AtomicInteger();
    var bParkedBeforeItsLink = new java.util.concurrent.CountDownLatch(1);
    var releaseB = new java.util.concurrent.CountDownLatch(1);
    var bResult = new java.util.concurrent.CompletableFuture<byte[]>();
    var bThread =
        new Thread(
            () -> {
              try {
                bResult.complete(processB.encrypt(subject, "b data".getBytes()));
              } catch (Throwable t) {
                bResult.completeExceptionally(t);
              }
            },
            "process-B-mint");

    FileSystemCryptoEngine.setMintTmpWrittenHook(
        () -> {
          int p = phase.getAndIncrement();
          if (p == 0) {
            // Process A has written its tmp and is about to createLink. Run process B's mint up
            // to (pre-fix: through the stray-tmp heal and) B's own tmp write, then park B right
            // before B's createLink — so the key path is still absent when A's createLink runs.
            bThread.start();
            await(bParkedBeforeItsLink);
          } else if (p == 1) {
            bParkedBeforeItsLink.countDown();
            await(releaseB);
          }
        });
    byte[] aCiphertext;
    try {
      // Pre-fix this threw CryptoOperationException (cause: NoSuchFileException — B's mint-time
      // heal had deleted A's live tmp). A legitimate concurrent cross-process first-mint must
      // never fail spuriously: succeeding-or-adopting is the link(2) first-writer-wins contract.
      aCiphertext = processA.encrypt(subject, "a data".getBytes());
    } finally {
      releaseB.countDown();
      FileSystemCryptoEngine.setMintTmpWrittenHook(null);
    }
    byte[] bCiphertext = bResult.get(10, java.util.concurrent.TimeUnit.SECONDS);
    bThread.join(10_000);

    // Both mints succeed-or-adopt under exactly one shared key: each side decrypts the other's
    // ciphertext, and the shard directory holds the key file alone (both tmps cleaned up).
    assertArrayEquals("a data".getBytes(), processB.decrypt(subject, aCiphertext));
    assertArrayEquals("b data".getBytes(), processA.decrypt(subject, bCiphertext));
    Path keyPath = keyPathFor(subject.value());
    assertTrue(Files.exists(keyPath));
    try (var entries = Files.list(keyPath.getParent())) {
      assertEquals(
          java.util.List.of(keyPath),
          entries.toList(),
          "exactly one key file and no leftover tmp may remain after both mints");
    }
    assertFalse(Files.exists(resolveTombstonePathForTest(subject)));
  }

  @Test
  void staleTmpsFromCrashedMintsAreInertAndSweptByTheErasureNotTheMint() throws Exception {
    // Crashed pre-link mints (JVM died between writing the tmp and createLink) leave orphaned
    // tmps holding never-claimed key material — no ciphertext exists under them. The mint no
    // longer deletes them: a matching tmp seen from a mint can
    // equally be a peer PROCESS's live in-flight mint, and shooting it fails that legitimate
    // encrypt spuriously. Stale orphans are inert to every operation and are removed by the
    // GDPR-relevant sweep — deleteKey, under the governing tombstone — or an operator find.
    Path keyPath = keyPathFor("stale-tmp-accumulation");
    Files.createDirectories(keyPath.getParent());
    Path orphan1 = keyPath.resolveSibling(keyPath.getFileName() + "111.tmp");
    Path orphan2 = keyPath.resolveSibling(keyPath.getFileName() + "222.tmp");
    Files.write(orphan1, new byte[32]);
    Files.write(orphan2, new byte[32]);
    SubjectId subject = SubjectId.of("stale-tmp-accumulation");

    // The mint is unaffected by stale tmps (createTempFile picks a fresh random name; createLink
    // targets the key path) and leaves them alone — hygiene is not worth shooting what might be a
    // peer's live mint.
    byte[] ciphertext = engine.encrypt(subject, "data".getBytes());
    assertTrue(Files.exists(keyPath), "a fresh key must have been minted");
    assertTrue(
        Files.exists(orphan1),
        "the mint must not delete stale tmps — cross-process, a matching tmp can be a peer's live"
            + " in-flight mint");
    assertTrue(Files.exists(orphan2), "accumulated orphans persist until the erasure sweep");
    assertArrayEquals("data".getBytes(), engine.decrypt(subject, ciphertext));

    // The erasure sweep — where deletion is governed by the tombstone and killing in-flight mints
    // is the documented erasure-wins semantics — removes every accumulated leak.
    engine.deleteKey(subject);
    assertFalse(Files.exists(keyPath));
    assertFalse(Files.exists(orphan1), "erasure must sweep accumulated pre-erasure tmp leaks");
    assertFalse(Files.exists(orphan2), "erasure must sweep accumulated pre-erasure tmp leaks");
    try (var entries = Files.list(keyPath.getParent())) {
      // The per-subject .generation record legitimately survives erasure (it is what
      // keeps a post-reinstate re-mint on a higher generation so destroyed-generation ciphertext
      // stays [REDACTED]); it holds a small integer — no key material, no PII.
      assertTrue(
          entries.allMatch(
              p ->
                  p.getFileName().toString().endsWith(".forgotten")
                      || p.getFileName().toString().endsWith(".generation")),
          "after erasure the shard dir must hold nothing but the tombstone and the generation"
              + " record");
    }
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, ciphertext));
  }

  // ----- Cross-process encrypt vs completed deleteKey -----
  //
  // Two engine INSTANCES over one key directory are the faithful single-JVM model of two
  // PROCESSES sharing the directory (the deployment link(2) first-writer-wins minting exists
  // for): each instance has its own stripe-lock array, exactly as two JVMs have unrelated
  // locks — the in-JVM mutual exclusion pinned by encryptRacingDeleteKey_forgottenAlwaysWins
  // does not exist between them. Only the on-disk state (key file, tombstone, tmp links) is
  // shared, which is precisely the cross-process reality.

  @Test
  void crossProcessEncryptRacingCompletedDeleteKey_refusesAndLeavesNoKey() throws Exception {
    var processA = engine;
    var processB = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    SubjectId subject = SubjectId.of("xproc-resurrect");

    var fired = new java.util.concurrent.atomic.AtomicBoolean();
    FileSystemCryptoEngine.setEncryptRaceHook(
        () -> {
          if (!fired.compareAndSet(false, true)) {
            return; // only process A's mint, not any nested engine call
          }
          // Process B's COMPLETE deleteKey lands between A's tombstone pre-check (already read:
          // no tombstone) and A's mint: tombstone written, deleteIfExists finds no key, sweep
          // finds no tmp, returns — ForgetSubjectService now reports the erasure complete to the
          // data subject. Nothing will ever delete a key minted after this point.
          processB.deleteKey(subject);
        });
    try {
      assertThrows(
          SubjectForgottenException.class,
          () -> processA.encrypt(subject, "post-erasure PII".getBytes()),
          "a mint that completes after a COMPLETED cross-process erasure must be refused — "
              + "returning ciphertext here silently un-does a GDPR erasure that was already "
              + "reported to the data subject");
    } finally {
      FileSystemCryptoEngine.setEncryptRaceHook(null);
    }

    Path keyPath = keyPathFor(subject.value());
    assertFalse(
        Files.exists(keyPath),
        "the just-minted key file must be removed — after the erasure completed, no live key may"
            + " remain for the subject");
    assertTrue(
        Files.exists(resolveTombstonePathForTest(subject)), "the erasure tombstone must survive");
    try (var entries = Files.list(keyPath.getParent())) {
      assertTrue(
          entries.allMatch(p -> p.getFileName().toString().endsWith(".forgotten")),
          "no key file and no stray tmp link may survive the refused mint");
    }
    assertFalse(processA.isKeyAvailable(subject));
    // Both "processes" agree afterwards: the subject is terminally erased.
    assertThrows(
        SubjectForgottenException.class, () -> processB.encrypt(subject, "again".getBytes()));
  }

  @Test
  void crossProcessMintLoserRefusesAndRemovesACrashedWinnersPostErasureKey() throws Exception {
    var processA = engine;
    var processB = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    SubjectId subject = SubjectId.of("xproc-loser-resurrect");
    Path keyPath = keyPathFor(subject.value());
    Path crashedWinnersTmp = keyPath.resolveSibling(keyPath.getFileName() + "555.tmp");

    var fired = new java.util.concurrent.atomic.AtomicBoolean();
    FileSystemCryptoEngine.setEncryptRaceHook(
        () -> {
          if (!fired.compareAndSet(false, true)) {
            return;
          }
          try {
            // 1. Process B completes the erasure: tombstone written, no key to delete.
            processB.deleteKey(subject);
            // 2. A THIRD process's mint (its pre-check also predated the tombstone) claims the
            //    key file via createLink AFTER the completed erasure and dies before its
            //    post-mint re-check: a fully written key file (createLink is atomic) plus its
            //    never-cleaned tmp hard link appear, returned to nobody.
            Files.createDirectories(keyPath.getParent());
            byte[] orphanKey = new byte[32];
            new java.util.Random(42).nextBytes(orphanKey);
            Files.write(keyPath, orphanKey);
            Files.createLink(crashedWinnersTmp, keyPath);
          } catch (Exception e) {
            throw new AssertionError(e);
          }
        });
    try {
      // Process A resumes with its stale pre-check: its createLink fails (FileAlreadyExists),
      // making A the LOSER of the mint race. The loser must not adopt the crashed winner's
      // post-erasure key — the tombstone is visible, the subject is erased.
      assertThrows(
          SubjectForgottenException.class,
          () -> processA.encrypt(subject, "post-erasure PII".getBytes()),
          "the LOSER of a cross-process mint race must also re-check the tombstone — otherwise it"
              + " returns a key created after the erasure completed");
    } finally {
      FileSystemCryptoEngine.setEncryptRaceHook(null);
    }

    assertFalse(
        Files.exists(keyPath),
        "the loser must remove the crashed winner's post-erasure key file — nothing else ever"
            + " will (the erasure already reported success)");
    assertFalse(
        Files.exists(crashedWinnersTmp),
        "the crashed winner's stray tmp hard link must be swept too — it keeps the key inode"
            + " alive");
    assertTrue(Files.exists(resolveTombstonePathForTest(subject)));
  }

  @Test
  void encryptRefusesToUseAKeyThatCoexistsWithATombstone() throws Exception {
    // The tombstone+key disk state is always erasure residue: a deleteKey that crashed between
    // its tombstone write and its key delete, or (cross-process) a mint that crashed between
    // createLink and its post-mint re-check after a completed erasure. In both, erasure is the
    // governing intent: encrypt must refuse to write fresh PII under the leftover key — this is
    // what keeps a crashed-post-link orphan inert (nothing ever encrypts under it).
    SubjectId subject = SubjectId.of("tombstone-key-residue");
    byte[] preErasureCiphertext = engine.encrypt(subject, "pre-erasure".getBytes());
    Path tombstone = resolveTombstonePathForTest(subject);
    Files.createFile(tombstone); // the crashed/mid-flight deleteKey's tombstone

    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(subject, "new PII".getBytes()),
        "a key file coexisting with a tombstone is erasure residue — encrypt must refuse, not"
            + " happily load it");

    // The documented crashed-deleteKey semantics hold: the leftover key still decrypts
    // pre-erasure ciphertext (the erasure has not completed), and encrypt's refusal must NOT
    // have deleted it — only a re-run of the forget completes the erasure.
    assertTrue(Files.exists(keyPathFor(subject.value())), "refusal must not delete the key file");
    assertArrayEquals("pre-erasure".getBytes(), engine.decrypt(subject, preErasureCiphertext));

    // The forget retry converges: key gone, decrypt dead, encrypt still refused.
    engine.deleteKey(subject);
    assertFalse(Files.exists(keyPathFor(subject.value())));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preErasureCiphertext));
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "again".getBytes()));
  }

  @Test
  void crossProcessConcurrentFirstMintsShareOneKeyWithoutFalseRefusal() throws Exception {
    // Regression pin for the tombstone-free mint race: the loser adopting the winner's key is
    // the designed first-writer-wins behavior and must not trip the new post-mint re-check.
    var processA = engine;
    var processB = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    SubjectId subject = SubjectId.of("xproc-benign-race");

    var fired = new java.util.concurrent.atomic.AtomicBoolean();
    FileSystemCryptoEngine.setEncryptRaceHook(
        () -> {
          if (!fired.compareAndSet(false, true)) {
            return;
          }
          // Process B mints first, after A's pre-check: A's createLink will lose the race.
          processB.encrypt(subject, "winner data".getBytes());
        });
    byte[] loserCiphertext;
    try {
      loserCiphertext = processA.encrypt(subject, "loser data".getBytes());
    } finally {
      FileSystemCryptoEngine.setEncryptRaceHook(null);
    }

    // Both processes share the single winning key: each decrypts the other's ciphertext.
    assertArrayEquals("loser data".getBytes(), processB.decrypt(subject, loserCiphertext));
    assertTrue(Files.exists(keyPathFor(subject.value())));
    assertFalse(Files.exists(resolveTombstonePathForTest(subject)));
  }

  // ----- An UNANSWERABLE key-existence probe must fail CLOSED, never as "erased" -----

  /**
   * Makes the subject's shard directory unreadable/unsearchable for the duration of {@code probe},
   * then restores it (so @TempDir cleanup still works). Skips when POSIX permissions are
   * unavailable, and when the JVM user is root — root bypasses permission bits, so the probe would
   * simply succeed and the test would prove nothing.
   */
  private void withUnprobeableShardDir(SubjectId subject, ThrowingRunnable probe) throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
        "requires POSIX permissions");
    org.junit.jupiter.api.Assumptions.assumeFalse(
        "root".equals(System.getProperty("user.name")), "root bypasses permission bits");

    Path shardDir = keyPathFor(subject.value()).getParent();
    var original = Files.getPosixFilePermissions(shardDir);
    Files.setPosixFilePermissions(shardDir, java.util.Set.of());
    // Guard against a filesystem/user combination where the chmod does not actually make the
    // probe fail (e.g. an overlay honoring capabilities): without this the test would silently
    // stop exercising the fail-closed path.
    org.junit.jupiter.api.Assumptions.assumeFalse(
        Files.exists(keyPathFor(subject.value())),
        "the shard directory is still searchable — cannot make the probe unanswerable here");
    try {
      probe.run();
    } finally {
      Files.setPosixFilePermissions(shardDir, original);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Test
  void decryptFailsLoudlyWhenKeyExistenceCannotBeDetermined() throws Exception {
    // Files.exists(Path) is SPECIFIED to answer false whenever existence cannot be
    // determined: it calls provider.checkAccess and swallows the IOException. So an unreadable
    // parent directory (ops action, SELinux relabel) or a stale/EIO network volume made decrypt
    // take the "!Files.exists(keyPath)" branch and throw KeyNotFoundException — which
    // CryptoShreddingModule.decryptValue converts into the [REDACTED] tombstone. A projection or
    // replay running at that moment WRITES [REDACTED] into the read model and ADVANCES its
    // checkpoint: durable corruption of a key that was never deleted, never re-read once the
    // volume recovers, and surfaced only by a metric that does not stop it.
    //
    // Every sibling backend already enforces the opposite: PostgresCryptoEngine throws
    // KeyNotFoundException ONLY on an empty ResultSet and maps every SQLException to
    // CryptoOperationException; AwsKmsCryptoEngine refuses to map any KMS-reported key state to
    // KeyNotFoundException; VaultCryptoEngine states outright that a
    // live key must never be misreported as absent because callers read "no key" as "already
    // crypto-shredded". CryptoShreddingModule's own javadoc promises REDACTED "only when the
    // subject's key is definitively gone".
    var subject = SubjectId.of("unprobeable-decrypt");
    byte[] ciphertext = engine.encrypt(subject, "live PII".getBytes());

    withUnprobeableShardDir(
        subject,
        () -> {
          RuntimeException thrown =
              assertThrows(RuntimeException.class, () -> engine.decrypt(subject, ciphertext));
          assertFalse(
              thrown instanceof KeyNotFoundException,
              "an unanswerable existence probe must NOT be reported as an erased key — that is the"
                  + " one type CryptoShreddingModule turns into the [REDACTED] tombstone: "
                  + thrown);
          assertInstanceOf(
              org.streamrune.core.crypto.CryptoOperationException.class,
              thrown,
              "an unanswerable existence probe must propagate as a CryptoOperationException so"
                  + " replay blocks and retries instead of writing tombstones");
        });
  }

  @Test
  void isKeyAvailableFailsLoudlyWhenKeyExistenceCannotBeDetermined() throws Exception {
    // Mirror path. isKeyAvailable is the GDPR erasure-VERIFICATION query: answering
    // false for a key that was never deleted reports a live subject as already crypto-shredded.
    // CryptoEngine#isKeyAvailable declares `@throws CryptoOperationException if the lookup fails`,
    // so failing closed is in contract (this is the same rule VaultCryptoEngine documents).
    var subject = SubjectId.of("unprobeable-available");
    engine.encrypt(subject, "live PII".getBytes());

    withUnprobeableShardDir(
        subject,
        () ->
            assertThrows(
                org.streamrune.core.crypto.CryptoOperationException.class,
                () -> engine.isKeyAvailable(subject),
                "reporting a still-present key as ABSENT answers an erasure-verification query"
                    + " with 'erased' for a subject that was never erased"));
  }

  @Test
  void keyFilesAreOwnerOnlyOnPosixFilesystems() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));

    engine.encrypt(SubjectId.of("perm-subject"), "secret".getBytes());

    try (var stream = Files.walk(tempDir)) {
      var keyFile =
          stream.filter(p -> p.getFileName().toString().endsWith(".subjectId")).findFirst();
      assertTrue(keyFile.isPresent(), "key file must exist");
      var perms = Files.getPosixFilePermissions(keyFile.get());
      assertEquals(
          java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"),
          perms,
          "key file must be owner-only");
    }
  }
}

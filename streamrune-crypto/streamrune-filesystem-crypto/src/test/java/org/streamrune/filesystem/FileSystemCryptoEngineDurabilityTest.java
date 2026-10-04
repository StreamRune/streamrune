package org.streamrune.filesystem;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * {@link FileSystemCryptoEngine#deleteKey(SubjectId)} must make each erasure step durable before
 * the next one runs, and the key unlink durable before it returns.
 *
 * <p>Without any fsync, nothing orders the tombstone create, the {@code .generation} rename and the
 * key unlink on disk. A power loss after {@code deleteKey} returned — after {@code
 * ForgetSubjectService} durably audited the erasure as complete, so nobody re-runs it — could keep
 * the unlink but lose the generation record (the next reinstate + encrypt re-mints the destroyed
 * generation, and the pre-erasure blobs fail the GCM tag check instead of reading as {@code
 * [REDACTED]}), lose the tombstone too (a retried command re-mints with no reinstate at all), or
 * lose the unlink (the key file comes back and pre-erasure PII decrypts again). A power loss cannot
 * be reproduced in a unit test, so these tests pin the order through the engine's package-private
 * {@code Durability} seam: every force is recorded together with whether the key file was still on
 * disk at that moment, and a failing force must stop the erasure before the key is unlinked.
 *
 * <p>The key MINT is forced too. Un-forced, a power loss after an event was encrypted under a
 * freshly minted key could lose the key file (or leave it empty) while the ciphertext is already
 * committed to the event store, making that ciphertext unreadable for good. The mint forces the key
 * content while it is still only a temp file, publishes it, and forces the directories that hold
 * the new entries before it returns the key — on the winning and on the losing leg of a mint race.
 * A refused mint forces its cleanup before it reports the refusal, and {@code reinstate} forces the
 * tombstone removal before it returns. The crash points are named in the engine's javadoc
 * (CP-M1..CP-M4, CP-R1, CP-I1); a failed force returns no key.
 *
 * <p>The same rule covers a key file this engine instance did NOT mint — published by a peer
 * instance or process sharing the directory, or by a previous run that died between the link and
 * its forces. The first encrypt in this instance that loads it forces the key file, its shard and
 * the key directory before the key is used (CP-L1/CP-L2); later loads of that very file force
 * nothing, a file replaced under the same name is forced again, and a decrypt never forces.
 */
class FileSystemCryptoEngineDurabilityTest {

  private static final byte[] PII = "pii".getBytes(StandardCharsets.UTF_8);

  @TempDir Path tempDir;

  private FileSystemCryptoEngine engine;

  @BeforeEach
  void setUp() {
    engine = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
  }

  @Test
  void deleteKeyForcesTombstoneThenGenerationBeforeTheUnlink_andTheUnlinkBeforeReturning()
      throws Exception {
    var subject = SubjectId.of("durable-erasure");
    engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    var recorder = new RecordingDurability(this, subject);

    engineWith(recorder).deleteKey(subject);

    assertEquals(
        List.of(
            "file tombstone [key=present generation=absent]",
            "dir shard [key=present generation=absent]",
            "dir keyDirectory [key=present generation=absent]",
            // written, forced, and only then renamed into place: never a torn or empty record
            "file generation-tmp [key=present generation=absent]",
            "file generation [key=present generation=present]",
            "dir shard [key=present generation=present]",
            // the unlink (and the stray-tmp sweep) made durable before deleteKey returns
            "dir shard [key=absent generation=present]"),
        recorder.events());
    assertFalse(Files.exists(keyPath(subject)));
  }

  @Test
  void aRerunOverAnAlreadyRecordedGenerationStillForcesItBeforeTheUnlink() throws Exception {
    // Crash residue of a deleteKey whose JVM died after renaming the generation record into place
    // but before forcing it (and before the unlink): the record is in the page cache, maybe not on
    // disk. The re-run takes recordErasedGeneration's idempotent early return — it must still force
    // the record before the unlink, or the unlink could reach the disk without it.
    var subject = SubjectId.of("rerun-over-recorded-generation");
    engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    Files.createFile(tombstonePath(subject));
    Files.writeString(generationPath(subject), "1", StandardCharsets.US_ASCII);
    var recorder = new RecordingDurability(this, subject);

    engineWith(recorder).deleteKey(subject);

    assertEquals(
        List.of(
            "file tombstone [key=present generation=present]",
            "dir shard [key=present generation=present]",
            "dir keyDirectory [key=present generation=present]",
            "file generation [key=present generation=present]",
            "dir shard [key=present generation=present]",
            "dir shard [key=absent generation=present]"),
        recorder.events());
  }

  @Test
  void forgetOfANeverEncryptedSubjectForcesTheTombstoneAndTheShardItCreated() throws Exception {
    // The shard directory may be created by this very erasure, so its entry in the key directory
    // is forced too — otherwise the tombstone could be durable inside a directory that is not.
    var subject = SubjectId.of("never-encrypted-durable-forget");
    var recorder = new RecordingDurability(this, subject);

    engineWith(recorder).deleteKey(subject);

    assertEquals(
        List.of(
            "file tombstone [key=absent generation=absent]",
            "dir shard [key=absent generation=absent]",
            "dir keyDirectory [key=absent generation=absent]",
            "dir shard [key=absent generation=absent]"),
        recorder.events());
  }

  @Test
  void aTombstoneForceFailureFailsTheErasureBeforeTheKeyIsUnlinked_andTheRerunConverges()
      throws Exception {
    var subject = SubjectId.of("tombstone-force-fails");
    byte[] preErasure = engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    Path tombstone = tombstonePath(subject);

    var failing = engineWith(new FailingDurability(tombstone::equals));
    assertThrows(CryptoOperationException.class, () -> failing.deleteKey(subject));

    assertTrue(Files.exists(keyPath(subject)), "the key must not be unlinked unforced");
    assertFalse(Files.exists(generationPath(subject)), "nothing past the tombstone ran");

    engine.deleteKey(subject); // the idempotent re-run converges
    assertFalse(Files.exists(keyPath(subject)));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preErasure));
  }

  @Test
  void aGenerationForceFailureFailsTheErasureBeforeTheKeyIsUnlinked_andTheRerunConverges()
      throws Exception {
    var subject = SubjectId.of("generation-force-fails");
    byte[] preErasure = engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    String generationName = generationPath(subject).getFileName().toString();

    var failing =
        engineWith(
            new FailingDurability(p -> p.getFileName().toString().startsWith(generationName)));
    assertThrows(CryptoOperationException.class, () -> failing.deleteKey(subject));

    assertTrue(Files.exists(keyPath(subject)), "the key must not be unlinked unforced");
    assertFalse(
        Files.exists(generationPath(subject)), "an unforced record must never be renamed in");

    engine.deleteKey(subject);
    assertEquals("1", Files.readString(generationPath(subject), StandardCharsets.US_ASCII).trim());
    // ...so the reinstate + re-mint flow still redacts the pre-erasure blob.
    engine.reinstate(subject);
    engine.encrypt(subject, "new life".getBytes(StandardCharsets.UTF_8));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preErasure));
  }

  @Test
  void aFailureToForceTheUnlinkFailsTheErasureLoudly_andTheRerunConverges() throws Exception {
    // The final force makes the unlink durable before deleteKey reports success. If it fails, the
    // caller must not record the erasure as complete: deleteKey throws, and the re-run (key
    // already gone from the page cache) forces the shard again and succeeds.
    var subject = SubjectId.of("unlink-force-fails");
    engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    Path keyPath = keyPath(subject);
    Path shard = keyPath.getParent();

    var failing = engineWith(new FailingDurability(p -> p.equals(shard) && !Files.exists(keyPath)));
    assertThrows(CryptoOperationException.class, () -> failing.deleteKey(subject));

    assertDoesNotThrow(() -> engine.deleteKey(subject));
    assertFalse(Files.exists(keyPath));
  }

  @Test
  void platformDurabilityForcesRealFilesAndDirectories() throws Exception {
    var durability = FileSystemCryptoEngine.PlatformDurability.forFileSystemOf(tempDir);
    Path file = Files.writeString(tempDir.resolve("forced"), "x");

    assertDoesNotThrow(() -> durability.forceFile(file));
    assertDoesNotThrow(() -> durability.forceDirectory(tempDir));
    assertThrows(NoSuchFileException.class, () -> durability.forceFile(tempDir.resolve("gone")));
  }

  @Test
  void platformDurabilityForcesDirectoriesOnlyWhereTheFileSystemCan() {
    Path missing = tempDir.resolve("no-such-directory");

    // Where directories can be forced, a failure is loud — never silently skipped...
    assertThrows(
        NoSuchFileException.class,
        () -> new FileSystemCryptoEngine.PlatformDurability(true).forceDirectory(missing));
    // ...and where they cannot (no POSIX view: Windows/NTFS), the force is skipped, documented.
    assertDoesNotThrow(
        () -> new FileSystemCryptoEngine.PlatformDurability(false).forceDirectory(missing));
  }

  @Test
  void aReForgetOverReadOnlyErasureMarkersStillConverges_onPosix() throws Exception {
    // A tombstone or .generation record an operator hardened to 0400 (or a restore left read-only)
    // must not fail every re-run of the forget: on a POSIX filesystem the force opens the file for
    // reading (fsync needs no write access), so the idempotent re-run still converges.
    assumeTrue(tempDir.getFileSystem().supportedFileAttributeViews().contains("posix"));
    var subject = SubjectId.of("read-only-markers");
    engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(subject);
    var readOnly = PosixFilePermissions.fromString("r--------");
    Files.setPosixFilePermissions(tombstonePath(subject), readOnly);
    Files.setPosixFilePermissions(generationPath(subject), readOnly);

    assertDoesNotThrow(() -> engine.deleteKey(subject));
    assertFalse(Files.exists(keyPath(subject)));
    assertTrue(Files.exists(tombstonePath(subject)));
  }

  // ----- The key mint (and reinstate) is forced before the key is returned -----

  @Test
  void aFirstMintForcesTheKeyContentBeforePublishing_andItsDirectoriesBeforeReturningTheKey()
      throws Exception {
    var subject = SubjectId.of("durable-first-mint");
    var recorder = RecordingDurability.mintView(this, subject);

    byte[] ciphertext = engineWith(recorder).encrypt(subject, PII);

    assertEquals(
        List.of(
            // CP-M1 boundary: the key bytes and inode are durable while the key is only a temp file
            "file key-tmp [key=absent tombstone=absent tmps=1]",
            // published by link(2), the temp name removed, and both made durable before the key is
            // returned (CP-M2, CP-M3): the key's entry in its shard, the shard's in the key
            // directory
            "dir shard [key=present tombstone=absent tmps=0]",
            "dir keyDirectory [key=present tombstone=absent tmps=0]"),
        recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aKeyThisInstanceMintedIsLoadedWithoutForcingAnything() throws Exception {
    var subject = SubjectId.of("durable-load");
    var recorder = RecordingDurability.mintView(this, subject);
    var minting = engineWith(recorder);
    minting.encrypt(subject, PII);
    recorder.clear();

    minting.encrypt(subject, PII);

    assertEquals(List.of(), recorder.events(), "the steady-state encrypt must not fsync");
  }

  @Test
  void aMintThatLosesTheRaceForcesTheWinnersKeyBeforeAdoptingIt() throws Exception {
    // The loser adopts a key another process published; if that winner died between its link and
    // its own forces, the loser's forces are the only ones that make the key durable before
    // ciphertext is produced under it.
    var subject = SubjectId.of("durable-mint-loser");
    var recorder = RecordingDurability.mintView(this, subject);
    var loser = engineWith(recorder);
    var winner = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    var fired = new AtomicBoolean();
    var winnerCiphertext = new AtomicReference<byte[]>();
    FileSystemCryptoEngine.setMintTmpWrittenHook(
        () -> {
          if (fired.compareAndSet(false, true)) {
            // the loser's key bytes sit forced in its temp; the winner publishes first
            winnerCiphertext.set(winner.encrypt(subject, PII));
          }
        });
    byte[] loserCiphertext;
    try {
      loserCiphertext = loser.encrypt(subject, PII);
    } finally {
      FileSystemCryptoEngine.setMintTmpWrittenHook(null);
    }

    assertEquals(
        List.of(
            "file key-tmp [key=absent tombstone=absent tmps=1]",
            // The adopted key is a file this instance did not write — forced like any
            // first load of a foreign key, the file itself included
            "file key [key=present tombstone=absent tmps=0]",
            "dir shard [key=present tombstone=absent tmps=0]",
            "dir keyDirectory [key=present tombstone=absent tmps=0]"),
        recorder.events());
    assertArrayEquals(PII, winner.decrypt(subject, loserCiphertext));
    assertArrayEquals(PII, loser.decrypt(subject, winnerCiphertext.get()));
  }

  @Test
  void aFailedKeyContentForceFailsTheMintBeforeItPublishes_andTheRetryConverges() throws Exception {
    var subject = SubjectId.of("key-tmp-force-fails");
    var failing =
        engineWith(new FailingDurability(p -> p.getFileName().toString().endsWith(".tmp")));

    assertThrows(CryptoOperationException.class, () -> failing.encrypt(subject, PII));

    // CP-M1: nothing published, nothing returned, so no ciphertext depends on those key bytes
    assertFalse(Files.exists(keyPath(subject)), "an unforced key must never be published");
    assertEquals(0, strayTmps(subject), "the failed mint removes its temp file");
    byte[] ciphertext = engine.encrypt(subject, PII); // the retry mints afresh
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aFailedShardForceAfterPublishingReturnsNoKey_andTheRetryForcesItBeforeUsingIt()
      throws Exception {
    assertAFailedPostPublishForceReturnsNoKey_andTheRetryForcesTheKeyFirst("shard");
  }

  @Test
  void aFailedKeyDirectoryForceAfterPublishingReturnsNoKey_andTheRetryForcesItBeforeUsingIt()
      throws Exception {
    assertAFailedPostPublishForceReturnsNoKey_andTheRetryForcesTheKeyFirst("keyDirectory");
  }

  private void assertAFailedPostPublishForceReturnsNoKey_andTheRetryForcesTheKeyFirst(
      String failingDirectory) throws Exception {
    var subject = SubjectId.of("post-publish-force-fails-" + failingDirectory);
    var recorder = RecordingDurability.mintView(this, subject).failingOnceOn(failingDirectory);
    var flaky = engineWith(recorder);

    assertThrows(CryptoOperationException.class, () -> flaky.encrypt(subject, PII));
    // CP-M2/CP-M3: the key file is published but not known to be durable, and was not returned
    assertTrue(Files.exists(keyPath(subject)));

    recorder.clear();
    byte[] ciphertext = flaky.encrypt(subject, PII);
    assertEquals(
        List.of(
            // no new mint (no temp): the published key is forced before this encrypt uses it
            "file key [key=present tombstone=absent tmps=0]",
            "dir shard [key=present tombstone=absent tmps=0]",
            "dir keyDirectory [key=present tombstone=absent tmps=0]"),
        recorder.events());
    recorder.clear();
    flaky.encrypt(subject, PII);
    assertEquals(List.of(), recorder.events(), "once forced, the key is loaded without a force");
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aMintRefusedByAConcurrentErasureForcesItsCleanupBeforeReportingTheRefusal()
      throws Exception {
    var subject = SubjectId.of("durable-refusal");
    var recorder = RecordingDurability.mintView(this, subject);
    var minting = engineWith(recorder);

    assertThrows(
        SubjectForgottenException.class, () -> encryptWhileAnotherProcessErases(minting, subject));

    assertEquals(
        List.of(
            "file key-tmp [key=absent tombstone=present tmps=1]",
            // CP-R1: the refused key's unlink and the temp sweep are durable before the refusal;
            // the refused key itself was never forced into the directory
            "dir shard [key=absent tombstone=present tmps=0]"),
        recorder.events());
  }

  @Test
  void aRefusedMintLeavesNothingRegistered_andAKeyLaterMintedElsewhereIsForcedOnceLikeAnyOther()
      throws Exception {
    // The refused mint registered its key as published-but-unforced before the claim; the
    // refusal removed that key durably, so the registration is dead and must go with it. The key
    // a peer later publishes under the same name (here: after a reinstate) is a key this engine
    // did not mint: its first load forces it, and only once.
    var subject = SubjectId.of("refusal-registration-dropped");
    var recorder = RecordingDurability.mintView(this, subject);
    var minting = engineWith(recorder);
    assertThrows(
        SubjectForgottenException.class, () -> encryptWhileAnotherProcessErases(minting, subject));
    assertEquals(0, minting.keyDurabilityMemoSize(), "a refused mint leaves nothing registered");
    engine.reinstate(subject);
    engine.encrypt(subject, PII);
    recorder.clear();

    assertEquals(
        "pii",
        new String(
            minting.decrypt(subject, minting.encrypt(subject, PII)), StandardCharsets.UTF_8));
    assertEquals(FIRST_LOAD_FORCES, recorder.events());
    recorder.clear();
    minting.encrypt(subject, PII);
    assertEquals(List.of(), recorder.events(), "the peer key is forced once, not on every load");
  }

  @Test
  void aFailureToForceARefusedMintsCleanupFailsLoudlyAndReturnsNoKey() throws Exception {
    var subject = SubjectId.of("refusal-force-fails");
    Path keyPath = keyPath(subject);
    var failing =
        engineWith(
            new FailingDurability(p -> p.equals(keyPath.getParent()) && !Files.exists(keyPath)));

    assertThrows(
        CryptoOperationException.class, () -> encryptWhileAnotherProcessErases(failing, subject));

    assertFalse(Files.exists(keyPath));
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, PII));
  }

  @Test
  void reinstateForcesTheTombstoneRemovalBeforeReturning() throws Exception {
    var subject = SubjectId.of("durable-reinstate");
    engine.encrypt(subject, PII);
    engine.deleteKey(subject);
    var recorder = RecordingDurability.mintView(this, subject);

    engineWith(recorder).reinstate(subject);

    assertEquals(List.of("dir shard [key=absent tombstone=absent tmps=0]"), recorder.events());
  }

  @Test
  void aFailureToForceReinstateFailsLoudly_andTheRerunForcesAgain() throws Exception {
    var subject = SubjectId.of("reinstate-force-fails");
    engine.deleteKey(subject);
    var failing = engineWith(new FailingDurability(shardOf(subject)::equals));

    assertThrows(CryptoOperationException.class, () -> failing.reinstate(subject));

    // CP-I1: the tombstone is unlinked but not known durable; the re-run has nothing left to
    // unlink and still forces the shard before it reports success
    var recorder = RecordingDurability.mintView(this, subject);
    engineWith(recorder).reinstate(subject);
    assertEquals(List.of("dir shard [key=absent tombstone=absent tmps=0]"), recorder.events());
  }

  @Test
  void reinstateOfASubjectThatNeverHadAShardForcesNothing() throws Exception {
    var subject = SubjectId.of("reinstate-nothing-there");
    var recorder = RecordingDurability.mintView(this, subject);

    engineWith(recorder).reinstate(subject);

    assertEquals(List.of(), recorder.events());
  }

  @Test
  void aMintIntoAMissingKeyDirectoryForcesTheParentOfEveryDirectoryItCreated() throws Exception {
    Path keyDirectory = tempDir.resolve("a").resolve("keys");
    var subject = SubjectId.of("durable-mint-new-key-directory");
    var recorder = new PathRecorder();

    FileSystemCryptoEngine.builder()
        .keyDirectory(keyDirectory)
        .durability(recorder)
        .build()
        .encrypt(subject, PII);

    String shard = "a/keys/" + hash(subject).substring(0, 2);
    assertEquals(
        List.of(
            "file " + shard + "/key-tmp", "dir " + shard, "dir a/keys", "dir a", "dir <tempDir>"),
        recorder.events);
  }

  @Test
  void aForgetIntoAMissingKeyDirectoryForcesTheParentOfEveryDirectoryItCreated() throws Exception {
    Path keyDirectory = tempDir.resolve("a").resolve("keys");
    var subject = SubjectId.of("durable-forget-new-key-directory");
    var recorder = new PathRecorder();

    FileSystemCryptoEngine.builder()
        .keyDirectory(keyDirectory)
        .durability(recorder)
        .build()
        .deleteKey(subject);

    String shard = "a/keys/" + hash(subject).substring(0, 2);
    assertEquals(
        List.of(
            "file " + shard + "/" + hash(subject) + ".forgotten",
            "dir " + shard,
            "dir a/keys",
            "dir a",
            "dir <tempDir>",
            "dir " + shard),
        recorder.events);
  }

  // ----- The first load of a key this engine instance did not mint is forced -----

  /** What the first load of a key file this instance did not mint forces, in order. */
  private static final List<String> FIRST_LOAD_FORCES =
      List.of(
          "file key [key=present tombstone=absent tmps=0]",
          "dir shard [key=present tombstone=absent tmps=0]",
          "dir keyDirectory [key=present tombstone=absent tmps=0]");

  @Test
  void theFirstLoadOfAKeyAPeerMintedForcesItBeforeUse_andTheSecondLoadForcesNothing()
      throws Exception {
    // The peer (another engine instance on the same directory: another process, or a sibling in
    // this one) may be between its link and its own forces, or may have died there. Until this
    // instance forces the key, nothing it knows makes the key durable.
    var subject = SubjectId.of("foreign-first-load");
    engine.encrypt(subject, PII);
    var recorder = RecordingDurability.mintView(this, subject);
    var loading = engineWith(recorder);

    byte[] ciphertext = loading.encrypt(subject, PII);

    // CP-L1/CP-L2: the key file, its entry in the shard and the shard's entry in the key directory
    // are forced inside encrypt, before the key produced the ciphertext it returns
    assertEquals(FIRST_LOAD_FORCES, recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));

    recorder.clear();
    loading.encrypt(subject, PII);
    assertEquals(
        List.of(), recorder.events(), "once forced by this instance, a load costs nothing");
  }

  @Test
  void aKeyAPreviousRunPublishedButNeverForcedIsForcedByTheFirstLoadOfTheNextRun()
      throws Exception {
    // The previous run's mint died between its link and its directory forces (here: the force
    // failed, and that engine instance is then discarded like the JVM that crashed). Its memory of
    // the unforced key died with it; the next run's first load must force the key itself.
    var subject = SubjectId.of("previous-run-unforced");
    var previousRun = engineWith(new FailingDurability(shardOf(subject)::equals));
    assertThrows(CryptoOperationException.class, () -> previousRun.encrypt(subject, PII));
    assertTrue(Files.exists(keyPath(subject)), "published, not known durable");
    var recorder = RecordingDurability.mintView(this, subject);

    byte[] ciphertext = engineWith(recorder).encrypt(subject, PII);

    assertEquals(FIRST_LOAD_FORCES, recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aKeyFileWrittenOutsideThisInstanceIsForcedOnItsFirstLoad() throws Exception {
    // Any key file this instance did not write — here one laid down directly in the engine's
    // format, as a restore or another tool would — is forced before its first use.
    var subject = SubjectId.of("written-outside");
    Files.createDirectories(shardOf(subject));
    byte[] rawKey = new byte[32];
    new java.security.SecureRandom().nextBytes(rawKey);
    Files.write(keyPath(subject), rawKey);
    var recorder = RecordingDurability.mintView(this, subject);

    byte[] ciphertext = engineWith(recorder).encrypt(subject, PII);

    assertEquals(FIRST_LOAD_FORCES, recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aFailedForceOfAForeignKeyFailsTheEncryptWithNoKey_andTheRetryForcesItAgain()
      throws Exception {
    assertAFailedFirstLoadForceReturnsNoKey_andTheRetryForcesAgain("key");
    assertAFailedFirstLoadForceReturnsNoKey_andTheRetryForcesAgain("shard");
    assertAFailedFirstLoadForceReturnsNoKey_andTheRetryForcesAgain("keyDirectory");
  }

  private void assertAFailedFirstLoadForceReturnsNoKey_andTheRetryForcesAgain(String failing)
      throws Exception {
    var subject = SubjectId.of("foreign-force-fails-" + failing);
    engine.encrypt(subject, PII);
    var recorder = RecordingDurability.mintView(this, subject).failingOnceOn(failing);
    var loading = engineWith(recorder);

    // no key: the encrypt throws, so no ciphertext exists under a key this instance did not force
    var thrown = assertThrows(CryptoOperationException.class, () -> loading.encrypt(subject, PII));
    assertTrue(thrown.getMessage().contains("could not be made durable"), thrown.getMessage());
    assertTrue(Files.exists(keyPath(subject)), "a failed force never removes the peer's key");
    assertEquals(0, loading.keyDurabilityMemoSize(), "a failed force records nothing");

    recorder.clear();
    byte[] ciphertext = loading.encrypt(subject, PII);
    assertEquals(FIRST_LOAD_FORCES, recorder.events(), "the retry forces the whole set again");
    recorder.clear();
    loading.encrypt(subject, PII);
    assertEquals(List.of(), recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
  }

  @Test
  void aDecryptOnlyLoadForcesNothing_andTheFirstEncryptAfterItStillForces() throws Exception {
    // A decrypt produces no ciphertext, and every ciphertext it can read was produced by an
    // encrypt that forced its key first — so the key a decrypt needs is already durable. The
    // decrypt therefore neither forces nor counts as this instance having forced the key.
    var subject = SubjectId.of("decrypt-only-load");
    byte[] peerCiphertext = engine.encrypt(subject, PII);
    var recorder = RecordingDurability.mintView(this, subject);
    var reading = engineWith(recorder);

    assertArrayEquals(PII, reading.decrypt(subject, peerCiphertext));
    assertArrayEquals(PII, reading.decrypt(subject, peerCiphertext));

    assertEquals(List.of(), recorder.events(), "a decrypt never forces");
    assertEquals(0, reading.keyDurabilityMemoSize(), "a decrypt records nothing");
    reading.encrypt(subject, PII);
    assertEquals(FIRST_LOAD_FORCES, recorder.events());
  }

  @Test
  void aKeyFileReplacedUnderTheSameNameIsForcedAgainOnItsFirstLoad() throws Exception {
    // This instance forced the subject's key; another process then erased the subject,
    // reinstated it and minted a new key under the same file name. That file is not the one this
    // instance forced, so its first load forces it again.
    var subject = SubjectId.of("replaced-key");
    engine.encrypt(subject, PII);
    var recorder = RecordingDurability.mintView(this, subject);
    var loading = engineWith(recorder);
    loading.encrypt(subject, PII);
    engine.deleteKey(subject);
    engine.reinstate(subject);
    engine.encrypt(subject, PII);
    recorder.clear();

    byte[] ciphertext = loading.encrypt(subject, PII);

    assertEquals(FIRST_LOAD_FORCES, recorder.events());
    assertArrayEquals(PII, engine.decrypt(subject, ciphertext));
    recorder.clear();
    loading.encrypt(subject, PII);
    assertEquals(List.of(), recorder.events());
  }

  @Test
  void theMemoryOfForcedKeysIsBounded_andAnEvictedKeyIsForcedAgainOnItsNextEncrypt()
      throws Exception {
    var first = SubjectId.of("memo-first");
    var second = SubjectId.of("memo-second");
    var third = SubjectId.of("memo-third");
    for (var subject : List.of(first, second, third)) {
      engine.encrypt(subject, PII);
    }
    var recorder = new PathRecorder();
    var loading =
        FileSystemCryptoEngine.builder()
            .keyDirectory(tempDir)
            .durability(recorder)
            .keyDurabilityMemoCapacity(2)
            .build();

    for (var subject : List.of(first, second, third)) {
      loading.encrypt(subject, PII);
    }
    assertEquals(2, loading.keyDurabilityMemoSize(), "capped at its capacity");
    recorder.events.clear();

    loading.encrypt(third, PII); // most recently used: still remembered
    assertEquals(List.of(), recorder.events);
    loading.encrypt(first, PII); // evicted: forced again — a cost, never a gap
    assertEquals(
        List.of(
            "file " + tempDir.relativize(keyPath(first)),
            "dir " + tempDir.relativize(shardOf(first)),
            "dir <tempDir>"),
        recorder.events);
  }

  @Test
  void aMintWhoseShardForceFailedForcesTheKeyAndEveryDirectoryItCreatedBeforeTheNextEncryptUsesIt()
      throws Exception {
    // The mint creates a/keys and the shard, publishes the key, and its first directory force (the
    // shard) fails: the key is published, unforced and not returned. This engine instance remembers
    // the key as unforced together with the mint's directories, so the next encrypt forces the key
    // file, the shard, the key directory AND the parents the failed mint created, and only then
    // uses the key; the encrypt after that forces nothing.
    Path keyDirectory = tempDir.resolve("a").resolve("keys");
    var subject = SubjectId.of("failed-nested-mint");
    String shard = "a/keys/" + hash(subject).substring(0, 2);
    var recorder = new PathRecorder().failingOnceOn("dir " + shard);
    var flaky =
        FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).durability(recorder).build();

    var thrown = assertThrows(CryptoOperationException.class, () -> flaky.encrypt(subject, PII));

    assertTrue(thrown.getMessage().contains("could not be made durable"), thrown.getMessage());
    // the mint got as far as the shard force: the key bytes were forced while still a temp file,
    // and nothing after the failed shard force ran
    assertEquals(List.of("file " + shard + "/key-tmp", "dir " + shard), recorder.events);
    Path keyFile =
        keyDirectory.resolve(hash(subject).substring(0, 2)).resolve(hash(subject) + ".subjectId");
    assertTrue(Files.exists(keyFile), "the key is published, unforced");
    assertEquals(1, flaky.keyDurabilityMemoSize(), "the unforced key is remembered as such");
    recorder.events.clear();

    byte[] ciphertext = flaky.encrypt(subject, PII);

    assertEquals(
        List.of(
            // the published key is forced before this encrypt uses it, with the whole set of
            // directories the failed mint created
            "file " + shard + "/" + hash(subject) + ".subjectId",
            "dir " + shard,
            "dir a/keys",
            "dir a",
            "dir <tempDir>"),
        recorder.events);
    assertEquals(1, flaky.keyDurabilityMemoSize(), "forced: the entry is replaced, not added");
    recorder.events.clear();

    byte[] third = flaky.encrypt(subject, PII);

    assertEquals(List.of(), recorder.events, "once forced, the key is loaded without a force");
    var reader = FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).build();
    assertArrayEquals(PII, reader.decrypt(subject, ciphertext));
    assertArrayEquals(PII, reader.decrypt(subject, third));
  }

  @Test
  void aFailedForceOfAParentTheFailedMintCreatedFailsTheNextEncryptAndTheSetIsForcedAgain()
      throws Exception {
    // The unforced key stays registered with the mint's full directory set until one encrypt forces
    // all of it: a force that fails on the way (here the parent a) returns no key either, and the
    // encrypt after that forces the whole set again, parents included.
    Path keyDirectory = tempDir.resolve("a").resolve("keys");
    var subject = SubjectId.of("failed-nested-mint-parent");
    String shard = "a/keys/" + hash(subject).substring(0, 2);
    var recorder = new PathRecorder().failingOnceOn("dir " + shard);
    var flaky =
        FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).durability(recorder).build();
    assertThrows(CryptoOperationException.class, () -> flaky.encrypt(subject, PII));
    List<String> wholeSet =
        List.of(
            "file " + shard + "/" + hash(subject) + ".subjectId",
            "dir " + shard,
            "dir a/keys",
            "dir a",
            "dir <tempDir>");
    recorder.events.clear();
    recorder.failingOnceOn("dir a");

    // no ciphertext: the encrypt stops at the parent's failed force, before the key is used
    assertThrows(CryptoOperationException.class, () -> flaky.encrypt(subject, PII));
    assertEquals(wholeSet.subList(0, 4), recorder.events, "the forces stopped at the parent");
    recorder.events.clear();

    byte[] ciphertext = flaky.encrypt(subject, PII);

    assertEquals(wholeSet, recorder.events, "the retry forces the whole set again");
    recorder.events.clear();
    flaky.encrypt(subject, PII);
    assertEquals(List.of(), recorder.events);
    var reader = FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).build();
    assertArrayEquals(PII, reader.decrypt(subject, ciphertext));
  }

  /**
   * Runs {@code minting.encrypt} while another process's complete {@code deleteKey} lands between
   * the mint's tombstone pre-check and its publish, so the mint's post-publish re-check refuses.
   */
  private void encryptWhileAnotherProcessErases(FileSystemCryptoEngine minting, SubjectId subject) {
    var eraser = FileSystemCryptoEngine.builder().keyDirectory(tempDir).build();
    var fired = new AtomicBoolean();
    FileSystemCryptoEngine.setEncryptRaceHook(
        () -> {
          if (fired.compareAndSet(false, true)) {
            eraser.deleteKey(subject);
          }
        });
    try {
      minting.encrypt(subject, PII);
    } finally {
      FileSystemCryptoEngine.setEncryptRaceHook(null);
    }
  }

  private long strayTmps(SubjectId subject) throws Exception {
    Path keyPath = keyPath(subject);
    if (!Files.exists(keyPath.getParent())) {
      return 0;
    }
    try (var entries = Files.list(keyPath.getParent())) {
      String prefix = keyPath.getFileName().toString();
      return entries
          .map(p -> p.getFileName().toString())
          .filter(n -> n.startsWith(prefix) && n.endsWith(".tmp"))
          .count();
    }
  }

  private FileSystemCryptoEngine engineWith(FileSystemCryptoEngine.Durability durability) {
    return FileSystemCryptoEngine.builder().keyDirectory(tempDir).durability(durability).build();
  }

  /**
   * Records every force (delegating to the real one) with the subject's on-disk state at that time:
   * key and generation record for the erasure tests; key, tombstone and the number of the key's
   * temp files ({@link #mintView}) for the mint and reinstate tests.
   */
  private static final class RecordingDurability implements FileSystemCryptoEngine.Durability {
    private final FileSystemCryptoEngine.Durability real;
    private final List<String> events = new ArrayList<>();
    private final Path keyDirectory;
    private final Path keyPath;
    private final Path tombstone;
    private final Path generation;
    private final boolean mintView;
    private String failOnceOn;

    RecordingDurability(FileSystemCryptoEngineDurabilityTest test, SubjectId subject)
        throws Exception {
      this(test, subject, false);
    }

    private RecordingDurability(
        FileSystemCryptoEngineDurabilityTest test, SubjectId subject, boolean mintView)
        throws Exception {
      this.real = FileSystemCryptoEngine.PlatformDurability.forFileSystemOf(test.tempDir);
      this.keyDirectory = test.tempDir;
      this.keyPath = test.keyPath(subject);
      this.tombstone = test.tombstonePath(subject);
      this.generation = test.generationPath(subject);
      this.mintView = mintView;
    }

    static RecordingDurability mintView(
        FileSystemCryptoEngineDurabilityTest test, SubjectId subject) throws Exception {
      return new RecordingDurability(test, subject, true);
    }

    /** Fails the first force of the path with this label (recorded like any other force). */
    RecordingDurability failingOnceOn(String label) {
      this.failOnceOn = label;
      return this;
    }

    @Override
    public void forceFile(Path file) throws IOException {
      record("file ", label(file));
      real.forceFile(file);
    }

    @Override
    public void forceDirectory(Path directory) throws IOException {
      record("dir ", label(directory));
      real.forceDirectory(directory);
    }

    private void record(String kind, String label) throws IOException {
      events.add(kind + label + state());
      if (label.equals(failOnceOn)) {
        failOnceOn = null;
        throw new IOException("injected fsync failure: " + label);
      }
    }

    List<String> events() {
      return events;
    }

    void clear() {
      events.clear();
    }

    private String label(Path path) {
      if (path.equals(keyPath)) return "key";
      if (path.equals(tombstone)) return "tombstone";
      if (path.equals(generation)) return "generation";
      if (path.equals(keyPath.getParent())) return "shard";
      if (path.equals(keyDirectory)) return "keyDirectory";
      String name = path.getFileName().toString();
      if (name.startsWith(generation.getFileName().toString()) && name.endsWith(".gen")) {
        return "generation-tmp";
      }
      if (name.startsWith(keyPath.getFileName().toString()) && name.endsWith(".tmp")) {
        return "key-tmp";
      }
      return path.toString();
    }

    private String state() {
      if (mintView) {
        return " [key="
            + presence(keyPath)
            + " tombstone="
            + presence(tombstone)
            + " tmps="
            + keyTmps()
            + "]";
      }
      return " [key=" + presence(keyPath) + " generation=" + presence(generation) + "]";
    }

    private static String presence(Path path) {
      return Files.exists(path) ? "present" : "absent";
    }

    private long keyTmps() {
      Path shard = keyPath.getParent();
      if (!Files.exists(shard)) {
        return 0;
      }
      String prefix = keyPath.getFileName().toString();
      try (var entries = Files.list(shard)) {
        return entries
            .map(p -> p.getFileName().toString())
            .filter(n -> n.startsWith(prefix) && n.endsWith(".tmp"))
            .count();
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    }
  }

  /**
   * Records every force by its path relative to the temp directory (delegating to the real one).
   */
  private final class PathRecorder implements FileSystemCryptoEngine.Durability {
    private final FileSystemCryptoEngine.Durability real =
        FileSystemCryptoEngine.PlatformDurability.forFileSystemOf(tempDir);
    private final List<String> events = new ArrayList<>();
    private String failOnceOn;

    /** Fails the first force that records exactly this event (recorded like any other force). */
    PathRecorder failingOnceOn(String event) {
      this.failOnceOn = event;
      return this;
    }

    @Override
    public void forceFile(Path file) throws IOException {
      record("file " + relative(file));
      real.forceFile(file);
    }

    @Override
    public void forceDirectory(Path directory) throws IOException {
      record("dir " + relative(directory));
      real.forceDirectory(directory);
    }

    private void record(String event) throws IOException {
      events.add(event);
      if (event.equals(failOnceOn)) {
        failOnceOn = null;
        throw new IOException("injected fsync failure: " + event);
      }
    }

    private String relative(Path path) {
      if (path.equals(tempDir)) return "<tempDir>";
      if (path.getFileName().toString().endsWith(".tmp")) {
        return tempDir.relativize(path.getParent()) + "/key-tmp";
      }
      return tempDir.relativize(path).toString();
    }
  }

  /** Fails the force of every path the predicate selects; forces the rest for real. */
  private final class FailingDurability implements FileSystemCryptoEngine.Durability {
    private final FileSystemCryptoEngine.Durability real =
        FileSystemCryptoEngine.PlatformDurability.forFileSystemOf(tempDir);
    private final Predicate<Path> failOn;

    FailingDurability(Predicate<Path> failOn) {
      this.failOn = failOn;
    }

    @Override
    public void forceFile(Path file) throws IOException {
      if (failOn.test(file)) throw new IOException("injected fsync failure: " + file);
      real.forceFile(file);
    }

    @Override
    public void forceDirectory(Path directory) throws IOException {
      if (failOn.test(directory)) throw new IOException("injected fsync failure: " + directory);
      real.forceDirectory(directory);
    }
  }

  private Path keyPath(SubjectId subject) throws Exception {
    return shardOf(subject).resolve(hash(subject) + ".subjectId");
  }

  private Path tombstonePath(SubjectId subject) throws Exception {
    return shardOf(subject).resolve(hash(subject) + ".forgotten");
  }

  private Path generationPath(SubjectId subject) throws Exception {
    return shardOf(subject).resolve(hash(subject) + ".generation");
  }

  private Path shardOf(SubjectId subject) throws Exception {
    return tempDir.resolve(hash(subject).substring(0, 2));
  }

  private static String hash(SubjectId subject) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    return HexFormat.of().formatHex(md.digest(subject.value().getBytes(StandardCharsets.UTF_8)));
  }
}

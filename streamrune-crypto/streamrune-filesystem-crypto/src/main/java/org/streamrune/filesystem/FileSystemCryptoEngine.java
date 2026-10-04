package org.streamrune.filesystem;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * File-backed {@link CryptoEngine} storing one AES-256 key per subject.
 *
 * <p><b>Ciphertext wire format</b> (versioned for algorithm agility and key generations): {@code
 * [format version (1 byte)][key version (1 byte)][GCM IV (12 bytes)][AES-256-GCM ciphertext+tag]}.
 * The key-version byte carries the <b>generation</b> of the key the blob was encrypted under (first
 * mint = {@code 1}; each post-erasure re-mint increments it — see the key-generations section
 * below). Decrypt compares the blob's generation to the current key's: equal proceeds normally (an
 * AES-GCM tag failure stays a loud tamper/corruption error); a LOWER generation ({@code >= 1})
 * means the blob's key was destroyed by a GDPR erasure and yields {@link KeyNotFoundException}
 * (mapped to {@code [REDACTED]} by {@code CryptoShreddingModule}); anything else — an unknown
 * format version, a generation of {@code < 1} or above the current key's — fails with a clear
 * {@link CryptoMappingException} (the deterministic subtype of {@link CryptoOperationException}:
 * retrying cannot succeed) instead of a generic GCM tag failure.
 *
 * <p><b>Key generations across erasures.</b> {@link #deleteKey(SubjectId)} records the destroyed
 * key's generation in a per-subject {@code <hash>.generation} file (max-merge, so idempotent forget
 * re-runs converge; the file deliberately survives {@link #reinstate(SubjectId)} to keep
 * generations monotonic across repeated erase/reinstate cycles — it holds a SHA-256 subject hash in
 * its NAME and a small integer as content: no PII, no key material). The next mint uses {@code
 * maxErased + 1}, so after the documented reinstate + re-encrypt flow the returning subject's
 * PRE-erasure ciphertext keeps reading as {@code [REDACTED]} instead of failing the GCM tag check
 * under the re-minted key and permanently blocking replay/rebuilds. A generation-1 key file is the
 * 32-byte AES key; a generation {@code >= 2} key file is 33 bytes (key + generation byte); any
 * other length or generation byte is rejected as a corrupt key file. A subject supports at most 126
 * completed erase/re-register cycles (the generation byte caps at 127); the mint fails loudly with
 * guidance to use a fresh subject id beyond that. Residual, documented in {@code
 * docs/guide/advanced/gdpr-erasure.md}: the generation byte is routing metadata outside the GCM
 * tag, so an attacker with write access to stored ciphertext can roll a reinstated subject's
 * current-generation blob down to a destroyed generation and turn it into {@code [REDACTED]}
 * (surfaced via the redaction metric/WARN) instead of a loud tag failure.
 *
 * <p><b>Terminal erasure.</b> {@link #deleteKey(SubjectId)} writes a durable tombstone marker
 * ({@code <hash>.forgotten}), removes the key file, and sweeps any stray {@code *.tmp} hard links
 * to the key inode left by a mint that crashed mid-claim (without the sweep such a link would keep
 * the key material readable after the erasure). A later {@link #encrypt(SubjectId, byte[])} for a
 * tombstoned subject throws {@link SubjectForgottenException} instead of minting a fresh key, so a
 * GDPR erasure cannot be silently un-done by a retried command or re-imported event. The tombstone
 * survives JVM restarts (it lives on disk). The only way to legitimately re-register a forgotten
 * subject is {@link #reinstate(SubjectId)}, which removes that subject's tombstone; there is no
 * engine-wide opt-out of terminal erasure.
 *
 * <p><b>Cross-process sharing.</b> Several JVMs may share one key directory (the mint claims its
 * key file via {@code link(2)} first-writer-wins precisely so concurrent cross-process mints are
 * safe). The per-subject stripe locks serialize {@code encrypt} against {@code deleteKey} only
 * <em>within one JVM</em>, so cross-process erasure/mint ordering rests on disk state alone: {@code
 * deleteKey} writes the tombstone BEFORE deleting the key file, and a mint re-checks the tombstone
 * AFTER claiming the key file — a claim landing after a completed erasure necessarily observes the
 * tombstone and refuses (removing its own claim), while a claim landing before the erasure's delete
 * is removed by that delete. An {@code encrypt} that finds a key file coexisting with a tombstone
 * (crash residue of an incomplete erasure) likewise refuses. These guarantees assume POSIX-coherent
 * metadata visibility — one kernel: a local filesystem, several JVMs on one host, or a volume
 * mounted by a single node. On NFS, client-side attribute and dentry caching can delay another
 * client's tombstone becoming visible to a fresh existence probe, so across NFS <em>clients</em>
 * the re-check narrows the race window but cannot close it — see {@code
 * docs/guide/advanced/gdpr-erasure.md} for the operational guidance. Concurrent cross-process mints
 * never delete each other's in-flight temp files: stray {@code *.tmp} sweeping happens only under
 * the subject's governing tombstone (in {@link #deleteKey(SubjectId)} and in the post-mint
 * refusal), never at mint time — a matching tmp seen from a mint may be a peer process's live
 * pre-link state, and deleting it would spuriously fail that legitimate encrypt. The cost is
 * bounded hygiene debt: a mint that crashes before its {@code createLink} leaves an inert orphan
 * tmp (never-claimed key material no ciphertext was produced under) that persists until the
 * subject's erasure sweeps it or an operator removes it.
 *
 * <p><b>Erasure durability.</b> {@link #deleteKey(SubjectId)} forces each step to stable storage
 * before the next one runs — the tombstone (file, shard directory, key directory), then the {@code
 * .generation} record (forced before its rename, then the renamed entry), then the key unlink and
 * stray-tmp sweep (shard directory) before it returns. Un-fsynced, nothing orders those steps on
 * disk, and a power loss after the erasure was reported complete could keep the unlink but lose the
 * generation record or the tombstone (a re-mint then reuses the destroyed generation), or lose the
 * unlink itself (the key file returns). Forced, a power loss leaves only the states a JVM crash at
 * a step boundary leaves, which re-running the forget converges. Directory forcing needs a POSIX
 * filesystem; where there is none (Windows/NTFS) it is skipped and documented.
 *
 * <p><b>Mint durability.</b> A key is durable before {@link #encrypt(SubjectId, byte[])} can
 * produce ciphertext under it. The mint forces the key bytes while the key is still only a temp
 * file, publishes it with {@code link(2)}, removes the temp name, and forces the shard directory,
 * the key directory and the parent of every directory the mint created — on the winning leg and on
 * the losing leg that adopts a peer's key — before the key is returned. Un-forced, a power loss
 * after an event was committed under a fresh key could lose the key file (or leave it empty), and
 * that ciphertext could never be decrypted again. Crash points (power loss):
 *
 * <ul>
 *   <li><b>CP-M1</b>, before the temp file is forced: at most a stray temp holding a partial key,
 *       no key file. Nothing was returned, so no ciphertext depends on it; the retry mints afresh
 *       under a new random temp name, and the stray temp stays inert until the subject's erasure
 *       sweeps it.
 *   <li><b>CP-M2</b>, after the publish and before the shard directory is forced: the key entry,
 *       the temp entry, both or neither may survive, and a survivor is complete because its content
 *       was forced before the link. Nothing was returned; the retry loads the surviving key or
 *       mints afresh, and a surviving temp link is swept by the subject's erasure.
 *   <li><b>CP-M3</b>, after the shard and before the key directory (or a parent the mint created)
 *       is forced: a shard the mint created may vanish with its entries — CP-M2 with nothing left.
 *   <li><b>CP-M4</b>, after the last force: the key is durable before it is returned.
 * </ul>
 *
 * <p>A failed force throws {@link CryptoOperationException} and returns no key, and this engine
 * instance forces the published key again before its next encrypt uses it. The mint never writes a
 * {@code .generation} record (it only reads one), so nothing else needs forcing. A mint refused by
 * a concurrent erasure forces its cleanup before it reports the refusal (CP-R1, {@link
 * #refuseMintIfErasedMidRace}), and {@link #reinstate(SubjectId)} forces the tombstone removal
 * before it returns (CP-I1).
 *
 * <p><b>First-load durability.</b> The mint's rule covers every key an encrypt uses: an engine
 * instance forces a key file before its first encrypt under it. A key file this instance has not
 * forced — one a peer instance or process sharing the directory published (possibly still between
 * its link and its forces, or dead there), one this application's previous run published, or one
 * this instance's own mint published before its forces failed — is forced by the first load that
 * would hand it to an encrypt, before the key is returned: the file, its shard directory and the
 * key directory (the directories every mint forces; after a failed mint of this instance, also the
 * parents that mint created). The losing leg of a mint race adopts such a file and forces it the
 * same way. The instance then remembers the file by a fingerprint of its content, so later loads
 * force nothing, while a file replaced under the same name (erased, reinstated and re-minted
 * elsewhere) has a different fingerprint and is forced again on its first load. That memory is per
 * instance and bounded (16,384 key files, least recently used evicted first); an evicted key is
 * forced again on its next encrypt. Crash points:
 *
 * <ul>
 *   <li><b>CP-L1</b>, before the load's last force completes: this instance has encrypted nothing
 *       under the key. A power loss can lose the key file only if no instance had forced it yet,
 *       and then no ciphertext anywhere depends on it, because every instance forces a key before
 *       its first encrypt under it. The next encrypt — this run's retry, or the next run's, whose
 *       memory starts empty — loads the surviving key and forces it, or mints a fresh one.
 *   <li><b>CP-L2</b>, after the last force: the key file and the entries that lead to it from the
 *       key directory are durable before the key is used.
 * </ul>
 *
 * <p>A load never forces the key directory's own entry in its parent: the key directory is assumed
 * provisioned, or created by a mint that completed its forces. A key directory a peer's first-ever
 * mint created and died before forcing can still be lost with everything under it, so a shared
 * directory should be created ahead of first use.
 *
 * <p>A decrypt never forces and never counts as this instance having forced a key: it produces no
 * ciphertext, and every ciphertext it can read was produced by an encrypt that forced its key
 * first, so a key that decrypts anything is already durable. (A key no instance has forced yet has
 * no ciphertext under it, so a decrypt that reads one is reading ciphertext written under another
 * key, and its answer does not depend on that key's durability.)
 *
 * <p><b>Key material handling is best-effort:</b> intermediate key buffers are zeroed after use,
 * but {@link SecretKeySpec} retains an internal copy on the heap until GC — full zeroization is not
 * possible with the standard JCA provider.
 */
public final class FileSystemCryptoEngine implements CryptoEngine {

  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128;
  private static final int GCM_IV_LENGTH = 12;
  private static final byte FORMAT_VERSION = 1;
  private static final int HEADER_LENGTH = 2;

  /** AES-256 key length; also the exact size of a generation-1 key file. */
  private static final int KEY_LENGTH = 32;

  /**
   * Highest key generation the single-byte header can carry. Generations start at 1 and bump only
   * on a post-erasure re-mint, so the cap allows 126 completed erase/re-register cycles per subject
   * before the mint refuses with guidance to use a fresh subject id.
   */
  private static final int MAX_KEY_VERSION = Byte.MAX_VALUE;

  // Cipher.getInstance() does an SPI provider lookup that measured ~8.6us — ~3.7x the cost of the
  // actual encrypt — so the AES/GCM cipher is reused per thread instead of re-resolved per call.
  // Cipher is not thread-safe, hence ThreadLocal; init() fully re-initializes it on each use, so
  // reuse across encrypt/decrypt calls on the same thread is safe.
  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(
          () -> {
            try {
              return Cipher.getInstance(ALGORITHM);
            } catch (GeneralSecurityException e) {
              throw new CryptoOperationException("AES/GCM cipher unavailable", e);
            }
          });

  /** Number of lock stripes. Bounds lock memory; each subject maps to one stripe by hash. */
  private static final int LOCK_STRIPES = 64;

  private final Path keyDirectory;
  private final SecureRandom random = new SecureRandom();

  // Per-subject (not global) IN-JVM lock guarding the tombstone-check + key-file-create sequence
  // in encrypt() against deleteKey() for the SAME subject id. A FIXED-SIZE striped array keyed by
  // a hash of the subject id: a given subject always maps to the same stripe, so the
  // in-JVM TOCTOU protection between encrypt() and deleteKey() is preserved, while memory stays
  // bounded regardless of subject cardinality (the previous per-subject map leaked one
  // ReentrantLock per distinct subject forever). Two subjects colliding on a stripe only serialize
  // harmlessly. These locks provide NO exclusion across processes sharing the key directory —
  // cross-process safety comes from deleteKey's tombstone-before-delete ordering plus the
  // post-mint tombstone re-check, not from locks. The
  // Postgres/Vault/AWS-KMS backends avoid all of this by locking in the shared database.
  private final ReentrantLock[] subjectLocks = createStripes(LOCK_STRIPES);

  private static ReentrantLock[] createStripes(int count) {
    ReentrantLock[] stripes = new ReentrantLock[count];
    for (int i = 0; i < count; i++) {
      stripes[i] = new ReentrantLock();
    }
    return stripes;
  }

  // The fsync primitives the erasure, the mint and reinstate and the first load of a key
  // rest on. Always the platform implementation in production; a test substitutes a
  // recorder or a failing implementation through the package-private Builder#durability.
  private final Durability durability;

  /**
   * How many key files one engine instance remembers having forced ({@link #keyDurability}). Bounds
   * that memory regardless of subject cardinality to a few MiB; a key evicted from it is forced
   * again on its next encrypt — one more fsync round, never a gap.
   */
  static final int DEFAULT_KEY_DURABILITY_MEMO_CAPACITY = 16_384;

  // ONE rule: an encrypt uses a key file only after THIS engine instance has forced
  // that very file. What the instance knows, by key path:
  //  - Unforced(directories): this instance's mint registered the key name just before its claim
  //    and has not forced it yet (the forces failed, or are still to come); the directories are
  //    the mint's own set, including the parent of every directory it created.
  //  - Forced(fingerprint): this instance forced the key file whose content has this fingerprint.
  // No entry, or a Forced entry for other content (the file was replaced under the same name), is a
  // key file this instance has not made durable, and the load that would hand it to an encrypt
  // forces it first (forcePublishedKey). Per instance on purpose: another instance, in this process
  // or another, and this application's next run start empty and force every key on first use.
  // Bounded, access-order LRU (synchronized: subjects on different stripes use it concurrently;
  // one subject's check-then-force runs under its stripe lock). An evicted Forced entry costs one
  // more force; an evicted Unforced entry degrades to the default set (file, shard, key
  // directory), losing only parents a failed mint created above the key directory. A refused mint
  // drops its entry once the key's removal is durable.
  private final Map<Path, KeyDurability> keyDurability;

  /** What this engine instance knows about the durability of one key file. */
  private sealed interface KeyDurability {
    /**
     * Registered by this instance's mint before its claim; these directories are not forced yet.
     */
    record Unforced(List<Path> directories) implements KeyDurability {}

    /** Forced by this instance; {@code fingerprint} identifies the file content it forced. */
    record Forced(byte[] fingerprint) implements KeyDurability {}
  }

  private FileSystemCryptoEngine(
      Path keyDirectory, Durability durability, int keyDurabilityMemoCapacity) {
    if (keyDirectory == null) throw new IllegalArgumentException("keyDirectory must not be null");
    this.keyDirectory = keyDirectory;
    this.durability =
        durability != null ? durability : PlatformDurability.forFileSystemOf(keyDirectory);
    this.keyDurability = boundedLru(keyDurabilityMemoCapacity);
  }

  private static Map<Path, KeyDurability> boundedLru(int capacity) {
    return Collections.synchronizedMap(
        new LinkedHashMap<>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<Path, KeyDurability> eldest) {
            return size() > capacity;
          }
        });
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Number of key files this engine instance currently remembers (forced, or registered by a mint
   * and not yet forced). Package-private test seam pinning the memo's bound and that a refused or
   * failed operation leaves nothing behind; production code never reads it.
   */
  int keyDurabilityMemoSize() {
    return keyDurability.size();
  }

  /**
   * Serializes {@link #encrypt} and {@link #deleteKey} for the same subject id <em>within this
   * JVM</em>. Both acquire this lock before touching the subject's key file or tombstone file, so
   * "check tombstone, then create key" (encrypt) and "write tombstone, then delete key" (deleteKey)
   * can never interleave for the same subject in one process — closing the in-JVM TOCTOU window
   * that let a concurrent deleteKey resurrect a just-forgotten subject. A given subject id always
   * maps to the same stripe; distinct subjects usually take different stripes, and a hash collision
   * only serializes two unrelated subjects harmlessly (bounded memory, no per-subject lock leak).
   * Across processes this lock is meaningless — see the class javadoc's cross-process section for
   * the disk-state ordering that covers that case.
   */
  private ReentrantLock subjectLock(String subjectId) {
    return subjectLocks[Math.floorMod(subjectId.hashCode(), subjectLocks.length)];
  }

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (plaintext == null) throw new IllegalArgumentException("plaintext must not be null");
    Path keyPath = resolveKeyPath(subjectId.value());
    VersionedKey key = loadOrCreateKey(subjectId, keyPath);
    byte[] iv = new byte[GCM_IV_LENGTH];
    random.nextBytes(iv);
    try {
      Cipher cipher = CIPHER.get();
      cipher.init(Cipher.ENCRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);
      byte[] result = new byte[HEADER_LENGTH + GCM_IV_LENGTH + ciphertext.length];
      result[0] = FORMAT_VERSION;
      result[1] = (byte) key.version();
      System.arraycopy(iv, 0, result, HEADER_LENGTH, GCM_IV_LENGTH);
      System.arraycopy(ciphertext, 0, result, HEADER_LENGTH + GCM_IV_LENGTH, ciphertext.length);
      return result;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Encryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (ciphertext == null) throw new IllegalArgumentException("ciphertext must not be null");
    Path keyPath = resolveKeyPath(subjectId.value());
    if (!exists(keyPath)) {
      throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
    }
    VersionedKey key = loadKey(keyPath);
    validateCiphertextHeader(subjectId.redacted(), ciphertext, key.version());
    try {
      byte[] iv = new byte[GCM_IV_LENGTH];
      System.arraycopy(ciphertext, HEADER_LENGTH, iv, 0, GCM_IV_LENGTH);
      Cipher cipher = CIPHER.get();
      cipher.init(Cipher.DECRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      return cipher.doFinal(
          ciphertext,
          HEADER_LENGTH + GCM_IV_LENGTH,
          ciphertext.length - HEADER_LENGTH - GCM_IV_LENGTH);
    } catch (AEADBadTagException e) {
      // A GCM tag mismatch is DETERMINISTIC — the blob is
      // corrupted, tampered, or was written under a different key — and retrying can never
      // succeed. The family classifiers (SagaStateConversion, ReadPoisonClassifier,
      // SagaCommandDispatch.isRetryLater, the event stores' hasTransientCryptoCause) read a BARE
      // CryptoOperationException as key-store OUTAGE evidence and retry forever; the deterministic
      // subtype is what routes this to poison/quarantine instead. This engine is the default
      // backend all three integrations wire, so the bare type here wedged subscriptions.
      throw new CryptoMappingException(
          "Decryption failed for subject: "
              + subjectId.redacted()
              + " — AEAD tag mismatch: the ciphertext is corrupted, tampered, or was written under"
              + " a different key (deterministic; retrying cannot succeed)",
          e);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Decryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Rejects blobs that cannot have been produced by this engine before touching the cipher, so a
   * format mismatch (different engine, tampering, future version) fails with a clear message
   * instead of a generic GCM tag-check failure — and routes a blob written under a DESTROYED key
   * generation to {@link KeyNotFoundException} so it reads as {@code [REDACTED]} instead of failing
   * the tag check under the wrong key.
   *
   * <p>The destroyed-generation tolerance is strictly bounded to {@code [1, currentKeyVersion)}: a
   * version byte of {@code < 1} or {@code > currentKeyVersion} is not a generation this engine ever
   * minted for the subject, so it stays a loud corruption error — otherwise flipping the byte low
   * would silently redact ANY subject's field, including never-erased ones.
   */
  private static void validateCiphertextHeader(
      String subjectDigest, byte[] ciphertext, int currentKeyVersion) {
    // Every rejection below is DETERMINISTIC (a property of the blob, not of the key
    // store), so it is raised as CryptoMappingException — the subtype the family classifiers
    // treat as poison — never as the bare CryptoOperationException they read as a transient
    // key-store outage and retry forever (mirrors PostgresCryptoEngine).
    //
    // The erased-generation
    // check below must run BEFORE the full-length rejection — a blob too short to ever decrypt
    // (missing IV/tag) can still carry a whole, readable 2-byte header (FORMAT_VERSION +
    // generation), and a generation the current key has already outlived is still the designed
    // KeyNotFoundException ([REDACTED]) signal for GDPR-erased data, not a length-corruption
    // verdict. Guard on length >= HEADER_LENGTH first: only the two header bytes this reads must
    // be safely present; a genuinely sub-header blob (or one whose format byte doesn't match)
    // falls through unchanged to the full checks below.
    if (ciphertext.length >= HEADER_LENGTH && ciphertext[0] == FORMAT_VERSION) {
      int erasedCheckBlobVersion = ciphertext[1];
      if (erasedCheckBlobVersion >= 1 && erasedCheckBlobVersion < currentKeyVersion) {
        throw new KeyNotFoundException(
            "Ciphertext was written under key generation "
                + erasedCheckBlobVersion
                + ", which a GDPR erasure destroyed; the current key generation is "
                + currentKeyVersion
                + " for subject: "
                + subjectDigest);
      }
    }
    int minLength = HEADER_LENGTH + GCM_IV_LENGTH + GCM_TAG_LENGTH / 8;
    if (ciphertext.length < minLength) {
      throw new CryptoMappingException(
          "Ciphertext too short for subject: "
              + subjectDigest
              + " — expected at least "
              + minLength
              + " bytes, got "
              + ciphertext.length);
    }
    if (ciphertext[0] != FORMAT_VERSION) {
      throw new CryptoMappingException(
          "Unsupported ciphertext format version "
              + ciphertext[0]
              + " for subject: "
              + subjectDigest
              + " — this engine reads format "
              + FORMAT_VERSION
              + "; the blob may come from a different CryptoEngine or a newer StreamRune version");
    }
    int blobVersion = ciphertext[1];
    if (blobVersion >= 1 && blobVersion < currentKeyVersion) {
      // Written under an earlier key generation of this subject — destroyed by a GDPR erasure
      // (the current key was minted after reinstate/re-registration). No key can decrypt it
      // anymore; surface the designed erased-data signal instead of a wrong-key GCM tag failure
      // that would block replay and rebuilds.
      throw new KeyNotFoundException(
          "Ciphertext was written under key generation "
              + blobVersion
              + ", which a GDPR erasure destroyed; the current key generation is "
              + currentKeyVersion
              + " for subject: "
              + subjectDigest);
    }
    if (blobVersion != currentKeyVersion) {
      throw new CryptoMappingException(
          "Unsupported key version "
              + blobVersion
              + " for subject: "
              + subjectDigest
              + " — this subject's current key generation is "
              + currentKeyVersion
              + " and destroyed generations are 1.."
              + (currentKeyVersion - 1)
              + "; the blob may be corrupted or come from a different CryptoEngine");
    }
  }

  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    Path keyPath = resolveKeyPath(subjectId.value());
    Path tombstonePath = resolveTombstonePath(subjectId.value());
    ReentrantLock lock = subjectLock(subjectId.value());
    lock.lock();
    try {
      // Same per-subject lock encrypt()/loadOrCreateKey() takes, held for this entire method. This
      // makes "check tombstone, then create key" (encrypt) and "write tombstone, then delete key"
      // (this method) mutually exclusive per subject WITHIN THIS JVM: an encrypt() in flight for
      // this subject either finishes (and possibly creates a key) entirely before this delete
      // starts, or this delete finishes (tombstone + key gone) before that encrypt can even check
      // the tombstone. Other subjects use a different lock, so this adds no cross-subject
      // contention. Another PROCESS's encrypt is not excluded by this lock — that case is covered
      // by the ordering invariant below plus the mint's post-claim re-check.
      //
      // ORDERING INVARIANT — DO NOT REORDER: the durable
      // tombstone is written BEFORE the key file is deleted. getOrCreateKey's post-mint re-check
      // is sound only because of this order: a cross-process mint whose createLink lands after
      // the deleteIfExists below necessarily runs its re-check after this tombstone already
      // exists, so it observes the tombstone and removes its own freshly minted key; a createLink
      // landing before the deleteIfExists is removed by the deleteIfExists itself. The order also
      // keeps crash semantics safe: if the JVM dies between the two steps, a stale key file is
      // the safe failure (pre-erasure ciphertext still decryptable, re-encrypt refused by the
      // tombstone-coexistence check in loadOrCreateKey, and re-running the forget converges). The
      // reverse order could leave a deleted key with no tombstone, silently allowing
      // resurrection.
      //
      // DURABILITY — every step below is forced to stable storage before the next one runs,
      // and the unlink before this method returns. Un-fsynced, nothing orders the tombstone
      // create, the .generation rename and the key unlink ON DISK: POSIX promises neither
      // durability nor order for them, filesystems without an ordered journal (ext2, FAT/exFAT,
      // some FUSE/NFS servers) do reorder, and even ext4's default delayed allocation can commit
      // the rename of a fresh .generation before its data (an empty record). A power loss after
      // this method returned — after ForgetSubjectService durably audited the erasure as complete,
      // so nobody re-runs it — could then keep the unlink but lose the generation record (reinstate
      // + encrypt re-mints the destroyed generation, and the pre-erasure blobs fail the GCM tag
      // check instead of reading as [REDACTED]), lose the tombstone too (a retried command re-mints
      // with no reinstate at all), or lose the unlink itself (the key file comes back and
      // pre-erasure PII decrypts again). Forced, a power loss leaves exactly the states a JVM crash
      // at a step boundary leaves — a prefix of tombstone, generation, unlink — and the crash story
      // in these comments proves each of those convergent by re-running the forget. A failed force
      // throws before the next step runs: the erasure fails loudly and the re-run converges.
      List<Path> tombstoneDirectories = writeTombstone(tombstonePath);
      // Step 1: the tombstone's inode, its entry in the shard, the shard's entry in the key
      // directory (this erasure may have just created the shard), and the entry of every directory
      // it created above the key directory are durable before anything else.
      durability.forceFile(tombstonePath);
      forceDirectories(tombstoneDirectories);
      // Before destroying the key, record its generation in
      // the per-subject .generation file (max-merge — idempotent under forget re-runs, monotonic
      // across erase/reinstate cycles). Ordering: AFTER the tombstone (a crash before this line
      // leaves the documented tombstoned-but-live-key residue, whose reinstate REVIVES the
      // original key with its original generation — consistent), BEFORE the key delete (a crash
      // between bump and delete also leaves the live key; its generation still matches its
      // ciphertext, and the next forget re-run max-merges the same value — converges). Only after
      // the delete does a mint ever observe "no key + generation N", minting N+1.
      if (exists(keyPath)) {
        int erasedVersion;
        try {
          erasedVersion = loadKey(keyPath).version();
        } catch (CryptoOperationException _) {
          // A corrupt/unreadable key file must never block a GDPR erasure — but its generation is
          // unknowable, so conservatively burn the counter: no future mint may collide with
          // whatever generation the corrupt key's ciphertext claims. Same-id re-registration is
          // refused afterwards (the mint's fresh-subject-id guidance).
          erasedVersion = MAX_KEY_VERSION;
        }
        recordErasedGeneration(subjectId, erasedVersion);
      }
      // Step 2: the generation record — just written (its tmp was forced before the rename), or
      // found already recorded by an earlier run that may have died before forcing it — and its
      // directory entry are durable before the key is unlinked.
      Path generationPath = resolveGenerationPath(subjectId.value());
      if (exists(generationPath)) {
        durability.forceFile(generationPath);
        durability.forceDirectory(generationPath.getParent());
      }
      Files.deleteIfExists(keyPath);
      // Also sweep stray temp HARD LINKS to the key inode. A hard crash
      // in getOrCreateKey between Files.createLink(path, tmp) and the finally-block tmp cleanup
      // leaves a second directory entry for the same key inode; deleting only keyPath would leave
      // the AES key readable through that stray name forever while isKeyAvailable reports false —
      // a silently incomplete GDPR erasure. Within this JVM no mint for this subject can be
      // racing the sweep (its tmp only exists inside loadOrCreateKey, which holds this stripe
      // lock). A mint in ANOTHER process can lose its live pre-link tmp to this sweep — its
      // createLink then fails loudly, the desired erasure-wins outcome while an erasure is
      // executing, and its retry is refused by the tombstone. This sweep and the one in
      // refuseMintIfErasedMidRace are the only tmp deleters: both run under the subject's
      // governing tombstone, never from a plain mint. An
      // IOException here propagates like a failed key delete: erasure fails loudly and a retried
      // forget converges (tombstone write and sweep are idempotent).
      deleteStrayTempLinks(keyPath);
      // Step 3: the unlink and the sweep are durable before the caller is told the erasure is
      // complete. If this force fails the erasure reports failure, and the re-run (key already
      // unlinked) re-forces the shard and converges.
      durability.forceDirectory(keyPath.getParent());
    } catch (IOException e) {
      throw new CryptoOperationException(
          "Failed to delete key for subject: " + subjectId.redacted(), e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    ReentrantLock lock = subjectLock(subjectId.value());
    lock.lock();
    try {
      // Clearing the tombstone lets the next encrypt mint a fresh key. After a COMPLETED erasure
      // the old key file is gone, so only future writes are re-enabled. After a CRASHED one it is
      // NOT: deleteKey writes the tombstone BEFORE unlinking the key file (the documented safe
      // ordering above), so a JVM/host death between the two steps leaves the key file behind —
      // inert only for as long as the tombstone gates it. Clearing the tombstone here therefore
      // returns that PRE-ERASURE key to service: loadOrCreateKey finds the surviving file and
      // loads it instead of minting, and its ciphertext is readable again. If an erasure may have
      // crashed, re-run deleteKey first — it is idempotent and converges; isKeyAvailable(subjectId)
      // reports true for exactly this residue. The subject's .generation file, which deleteKey
      // wrote when it destroyed the key file, is deliberately NOT deleted: it is what makes the
      // next mint use a HIGHER generation, so pre-erasure ciphertext keeps reading as [REDACTED]
      // (KeyNotFoundException) instead of failing the GCM tag check under the re-minted key.
      // A tombstone with no .generation file is a forget of
      // a subject that never encrypted (no ciphertext exists, so the next mint is generation 1) or
      // an operator fault — a partial restore, a key file deleted outside the engine — which this
      // method does not repair (docs/guide/advanced/gdpr-erasure.md).
      Path tombstonePath = resolveTombstonePath(subjectId.value());
      Files.deleteIfExists(tombstonePath);
      // The removal is durable before reinstate reports success. A power loss before
      // this force can bring the tombstone back — fail-closed: encrypt keeps refusing, and no
      // success was reported — and the re-run converges: it forces the shard again even when the
      // unlink already happened. A subject that never had a shard has nothing to force.
      Path shard = tombstonePath.getParent();
      if (exists(shard)) {
        durability.forceDirectory(shard);
      }
    } catch (IOException e) {
      throw new CryptoOperationException("Failed to reinstate subject: " + subjectId.redacted(), e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    return exists(resolveKeyPath(subjectId.value()));
  }

  /**
   * Definitive existence probe. Answers {@code true} or {@code false} only when the filesystem
   * actually said so, and throws {@link CryptoOperationException} when the question could not be
   * answered at all.
   *
   * <p>{@link Files#exists(Path, java.nio.file.LinkOption...)} must never be used in this engine:
   * it is <em>specified</em> to answer {@code false} whenever existence cannot be determined — it
   * calls {@code provider.checkAccess(path)} and swallows the {@link IOException}. Every probe here
   * asks either "does this subject still have a key?" or "is this subject tombstoned?", and on both
   * questions a swallowed error turns into the most dangerous possible answer:
   *
   * <ul>
   *   <li>a live key reported ABSENT reads as a completed crypto-shred — {@link #decrypt(SubjectId,
   *       byte[])} would throw {@link KeyNotFoundException}, {@code CryptoShreddingModule} would
   *       substitute {@code [REDACTED]}, and a projection or replay running at that moment writes
   *       that tombstone into the read model and ADVANCES its checkpoint: durable corruption of
   *       data that was never erased, never re-read once the volume recovers;
   *   <li>a live tombstone reported ABSENT lifts the erasure gate, letting a mint resurrect a
   *       crypto-shredded subject;
   *   <li>a missing {@code .generation} record reported as "no erasure history" lets the next mint
   *       reuse a DESTROYED generation, flipping that generation's replay-tolerant {@code
   *       [REDACTED]} blobs into wrong-key GCM failures.
   * </ul>
   *
   * <p>An unreadable parent directory (an ops permission change, an SELinux relabel) or an
   * unavailable network volume (ESTALE/EIO) produces exactly that swallowed error while the process
   * keeps running. Failing closed here matches every sibling backend: {@code PostgresCryptoEngine}
   * throws {@link KeyNotFoundException} only on an empty {@code ResultSet} and maps every {@code
   * SQLException} to {@link CryptoOperationException}; {@code AwsKmsCryptoEngine} refuses to map
   * any KMS-reported key state to {@link KeyNotFoundException}; {@code VaultCryptoEngine} answers
   * {@code false} only on a definitive 404 because "callers may treat 'no key' as 'already
   * crypto-shredded'".
   *
   * <p>The message names the path, whose file name is the subject's SHA-256 hash — never the raw
   * subject id.
   */
  private static boolean exists(Path path) {
    try {
      Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes.class);
      return true;
    } catch (java.nio.file.NoSuchFileException _) {
      // The only definitive "no": the filesystem resolved the path and it is not there.
      return false;
    } catch (IOException e) {
      throw new CryptoOperationException(
          "Cannot determine whether "
              + path
              + " exists — refusing to answer an unanswerable probe with 'absent', because an"
              + " absent key or tombstone reads as a completed GDPR erasure. Check the key"
              + " directory's permissions and that its volume is mounted and healthy.",
          e);
    }
  }

  private Path resolveKeyPath(String subjectId) {
    String hash = sha256Hex(subjectId);
    String dirName = hash.substring(0, 2);
    Path dir = keyDirectory.resolve(dirName);
    return dir.resolve(hash + ".subjectId");
  }

  private Path resolveTombstonePath(String subjectId) {
    String hash = sha256Hex(subjectId);
    String dirName = hash.substring(0, 2);
    Path dir = keyDirectory.resolve(dirName);
    return dir.resolve(hash + ".forgotten");
  }

  /**
   * The per-subject key-generation record: {@code <shard>/<hash>.generation}, holding the highest
   * generation any completed erasure destroyed, as ASCII decimal. Never deleted — surviving both
   * the key file and the tombstone across {@link #reinstate(SubjectId)} is what keeps generations
   * monotonic. Its name never matches the stray-tmp sweep glob ({@code <hash>.subjectId*.tmp}), so
   * erasure hygiene cannot remove it.
   */
  private Path resolveGenerationPath(String subjectId) {
    String hash = sha256Hex(subjectId);
    String dirName = hash.substring(0, 2);
    Path dir = keyDirectory.resolve(dirName);
    return dir.resolve(hash + ".generation");
  }

  /**
   * The highest key generation a completed erasure destroyed for this subject ({@code 0} when none
   * was recorded). A corrupt or unreadable record fails LOUDLY: silently treating it as 0 would let
   * the next mint reuse a destroyed generation, turning that generation's [REDACTED] blobs back
   * into wrong-key GCM failures.
   */
  private int readMaxErasedGeneration(String subjectId) {
    Path generationPath = resolveGenerationPath(subjectId);
    if (!exists(generationPath)) {
      return 0;
    }
    try {
      String content = Files.readString(generationPath, StandardCharsets.US_ASCII).trim();
      int value = Integer.parseInt(content);
      if (value < 1 || value > MAX_KEY_VERSION) {
        throw new CryptoOperationException(
            "Corrupt key-generation record " + generationPath + ": out-of-range value " + value);
      }
      return value;
    } catch (IOException | NumberFormatException e) {
      throw new CryptoOperationException(
          "Failed to read key-generation record " + generationPath, e);
    }
  }

  /**
   * Max-merges {@code erasedVersion} into the subject's generation record (write-temp, force,
   * ATOMIC_MOVE, so neither a reader nor a power loss ever observes a torn or empty value).
   * Idempotent: re-running an erasure re-records the same maximum. {@link #deleteKey} forces the
   * renamed entry (or an already-present record) before it unlinks the key.
   */
  private void recordErasedGeneration(SubjectId subjectId, int erasedVersion) {
    Path generationPath = resolveGenerationPath(subjectId.value());
    try {
      boolean recordReadable = true;
      int current;
      try {
        current = readMaxErasedGeneration(subjectId.value());
      } catch (CryptoOperationException _) {
        // A corrupt generation record must never block a GDPR erasure — its true value is
        // unknowable, so burn the counter (record the maximum): no future mint can collide with
        // any destroyed generation. A transient WRITE failure below still fails the erasure
        // loudly, and the idempotent re-run converges.
        current = MAX_KEY_VERSION;
        recordReadable = false;
      }
      int merged = Math.max(current, erasedVersion);
      if (recordReadable && merged == current && exists(generationPath)) {
        return; // already recorded — idempotent forget re-run (deleteKey still forces it)
      }
      // A corrupt record always falls through to the rewrite below, replacing the garbage with
      // the burned maximum.
      Files.createDirectories(generationPath.getParent(), OWNER_ONLY_DIR);
      Path tmp =
          Files.createTempFile(
              generationPath.getParent(),
              generationPath.getFileName().toString(),
              ".gen",
              OWNER_ONLY_FILE);
      try {
        Files.writeString(tmp, Integer.toString(merged), StandardCharsets.US_ASCII);
        // Forced BEFORE the rename publishes it, so the record is never empty on disk — a
        // fresh name is not a replace, and nothing else orders its data ahead of the rename.
        durability.forceFile(tmp);
        Files.move(
            tmp,
            generationPath,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
      } finally {
        Files.deleteIfExists(tmp);
      }
    } catch (IOException e) {
      // Propagates out of deleteKey as a failed erasure: loud, and the re-run converges.
      throw new CryptoOperationException(
          "Failed to record erased key generation at " + generationPath, e);
    }
  }

  /**
   * Creates the tombstone marker for a forgotten subject. Owner-only where POSIX is supported,
   * mirroring key files. The file's mere existence is the signal; its contents are irrelevant.
   * Returns the directories {@link #deleteKey} forces to make the marker's entry durable (see
   * {@link #createDirectoriesForPublish}).
   */
  private List<Path> writeTombstone(Path tombstonePath) throws IOException {
    List<Path> directories = createDirectoriesForPublish(tombstonePath.getParent());
    if (exists(tombstonePath)) {
      return directories;
    }
    try {
      Files.createFile(tombstonePath, OWNER_ONLY_FILE);
    } catch (java.nio.file.FileAlreadyExistsException _) {
      // A concurrent deleteKey won the race — the tombstone exists, which is all we need.
    }
    return directories;
  }

  /**
   * Creates {@code directory} (owner-only, with any missing ancestors) and returns the directories
   * to force so that an entry published in it survives a power loss, in forcing order: {@code
   * directory} itself; the key directory, which holds the shard's entry — forced even when the
   * shard already existed, because a peer process may have created it and died before forcing it;
   * and the parent of every directory this call created above the key directory, bottom-up.
   */
  private List<Path> createDirectoriesForPublish(Path directory) throws IOException {
    List<Path> missing = new ArrayList<>();
    for (Path p = directory; p != null && !exists(p); p = p.getParent()) {
      missing.add(p);
    }
    Files.createDirectories(directory, OWNER_ONLY_DIR);
    Set<Path> toForce = new LinkedHashSet<>();
    toForce.add(directory);
    toForce.add(keyDirectory);
    for (Path created : missing) {
      Path parent = created.getParent();
      if (parent == null) {
        // a relative key directory: its own entry lives in the working directory
        parent = created.toAbsolutePath().getParent();
      }
      if (parent != null) {
        toForce.add(parent);
      }
    }
    return List.copyOf(toForce);
  }

  private void forceDirectories(List<Path> directories) throws IOException {
    for (Path directory : directories) {
      durability.forceDirectory(directory);
    }
  }

  private String sha256Hex(String input) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      // Always hash the UTF-8 bytes: the platform default charset would make the key path
      // JVM-dependent for non-ASCII subject IDs — a key written by one process would be "not
      // found" by another (data appearing crypto-shredded) or a duplicate key would be created.
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new CryptoOperationException("SHA-256 hashing failed", e);
    }
  }

  /** A subject's AES key together with its generation (the ciphertext key-version byte). */
  private record VersionedKey(SecretKey key, int version) {}

  /**
   * A key as an encrypt loads it: the key, and the {@link #fingerprint} of the file content it came
   * from, which {@link #keyDurability} identifies the file by.
   */
  private record LoadedKey(VersionedKey key, byte[] fingerprint) {}

  /** Loads a key for decrypt or erasure, which never force it: no fingerprint is computed. */
  private VersionedKey loadKey(Path path) {
    return readKeyFile(path, false).key();
  }

  /** Loads a key for an encrypt, with the fingerprint its first-load force is decided by. */
  private LoadedKey loadKeyForEncrypt(Path path) {
    return readKeyFile(path, true);
  }

  /**
   * Reads a key file in either format: 32 bytes = generation 1 (every first mint); 33 bytes = 32
   * key bytes + a generation byte {@code >= 2} (only post-reinstate re-mints produce these).
   * Anything else is corrupt.
   */
  private LoadedKey readKeyFile(Path path, boolean withFingerprint) {
    try {
      byte[] bytes = Files.readAllBytes(path);
      try {
        byte[] fingerprint = withFingerprint ? fingerprint(bytes) : null;
        if (bytes.length == KEY_LENGTH) {
          // SecretKeySpec copies the array, so the read buffer can be zeroed immediately.
          return new LoadedKey(new VersionedKey(new SecretKeySpec(bytes, "AES"), 1), fingerprint);
        }
        if (bytes.length == KEY_LENGTH + 1) {
          int version = bytes[KEY_LENGTH];
          if (version < 2 || version > MAX_KEY_VERSION) {
            throw new CryptoOperationException(
                "Corrupt key file " + path + ": invalid key generation byte " + version);
          }
          return new LoadedKey(
              new VersionedKey(new SecretKeySpec(bytes, 0, KEY_LENGTH, "AES"), version),
              fingerprint);
        }
        throw new CryptoOperationException(
            "Corrupt key file " + path + ": unexpected length " + bytes.length);
      } finally {
        java.util.Arrays.fill(bytes, (byte) 0);
      }
    } catch (CryptoOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException("Failed to load key from " + path, e);
    }
  }

  private static final byte[] KEY_FINGERPRINT_LABEL =
      "streamrune/filesystem-key-durability/v1\0".getBytes(StandardCharsets.US_ASCII);

  /**
   * Identifies a key file's content for {@link #keyDurability} without retaining key material: a
   * SHA-256 over a domain label and the file bytes. Every mint writes 256 fresh random key bits, so
   * a file re-minted under the same name never carries its predecessor's fingerprint.
   */
  private static byte[] fingerprint(byte[] keyFileContent) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      md.update(KEY_FINGERPRINT_LABEL);
      return md.digest(keyFileContent);
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException("SHA-256 hashing failed", e);
    }
  }

  // Owner-only attributes where the default filesystem is POSIX; empty (default permissions) on
  // non-POSIX filesystems such as NTFS, where asFileAttribute would throw at use time.
  private static final java.nio.file.attribute.FileAttribute<?>[] OWNER_ONLY_FILE =
      posixAttributesOrNone("rw-------");
  private static final java.nio.file.attribute.FileAttribute<?>[] OWNER_ONLY_DIR =
      posixAttributesOrNone("rwx------");

  private static java.nio.file.attribute.FileAttribute<?>[] posixAttributesOrNone(String perms) {
    if (!java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      return new java.nio.file.attribute.FileAttribute<?>[0];
    }
    return new java.nio.file.attribute.FileAttribute<?>[] {
      java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
          java.nio.file.attribute.PosixFilePermissions.fromString(perms))
    };
  }

  /**
   * Loads the subject's key file if it exists, or — under the per-subject lock — checks the
   * tombstone and mints a fresh key. Holding {@link #subjectLock} across the whole
   * check-tombstone-then-create-key sequence (not just the create) is what closes the in-JVM TOCTOU
   * window: {@link #deleteKey} takes the SAME lock before it writes its tombstone / deletes the key
   * file, so the two operations can never interleave for this subject within one process. Against a
   * {@code deleteKey} in ANOTHER process the lock is meaningless; that case is covered by the
   * tombstone-coexistence check on the load path here plus the post-mint re-check in {@link
   * #getOrCreateKey} — see the class javadoc.
   */
  private VersionedKey loadOrCreateKey(SubjectId subjectId, Path keyPath) {
    ReentrantLock lock = subjectLock(subjectId.value());
    lock.lock();
    try {
      Path tombstonePath = resolveTombstonePath(subjectId.value());
      if (exists(keyPath)) {
        if (exists(tombstonePath)) {
          // A key file coexisting with a tombstone is always
          // residue of an incomplete or crashed erasure — a deleteKey that died between its
          // tombstone write and its key delete (re-running the forget completes it), or a mint in
          // another process that died between createLink and its post-mint re-check. Erasure is
          // the governing intent: refuse to write fresh PII under the leftover key. This check is
          // also what keeps a crashed-post-link orphan key inert — no encrypt path ever returns
          // it, so nothing is ever encrypted under it. decrypt() stays deliberately un-gated
          // (pre-erasure ciphertext remains readable until a re-run of the forget removes the key
          // file — the documented crashed-deleteKey semantics), and for the same reason the
          // refusal here does NOT delete the key file; only deleteKey does.
          throw new SubjectForgottenException(
              "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
                  + subjectId.redacted()
                  + " — a key file left by an incomplete erasure still coexists with the"
                  + " tombstone; re-run the forget to remove it, or call reinstate(subjectId) to"
                  + " re-register");
        }
        // A key file this instance has not forced — published by a peer instance or
        // process (possibly still between its link and its forces, or dead there), by this
        // application's previous run, or by this instance's own mint whose forces failed — is
        // forced before this encrypt can produce ciphertext under it (CP-L1/CP-L2). Read first,
        // then force, then remember the content READ: were the file replaced in between, the
        // replacement's different fingerprint gets it forced on its own first load.
        LoadedKey loaded = loadKeyForEncrypt(keyPath);
        KeyDurability known = keyDurability.get(keyPath);
        if (!(known instanceof KeyDurability.Forced forced
            && MessageDigest.isEqual(forced.fingerprint(), loaded.fingerprint()))) {
          List<Path> directories =
              known instanceof KeyDurability.Unforced unforced
                  ? unforced.directories()
                  : directoriesHolding(keyPath);
          forcePublishedKey(subjectId, keyPath, true, directories, loaded.fingerprint());
        }
        return loaded.key();
      }
      // No key file: either the subject is brand new, or it was crypto-shredded. Terminal erasure
      // refuses to mint a fresh key for a forgotten subject; reinstate(subjectId) is the only way
      // to lift that.
      boolean forgotten = exists(tombstonePath);
      if (encryptRaceHook != null) {
        // Test-only seam (see setEncryptRaceHook): lets a test force a concurrent deleteKey() to
        // run — or attempt to run — at the exact TOCTOU point this fix closes, proving the two
        // operations are now mutually exclusive rather than relying on probabilistic timing.
        encryptRaceHook.run();
      }
      if (forgotten) {
        throw new SubjectForgottenException(
            "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
                + subjectId.redacted()
                + " — call reinstate(subjectId) to re-register");
      }
      return getOrCreateKey(subjectId, keyPath, tombstonePath);
    } finally {
      lock.unlock();
    }
  }

  // Test-only hook invoked (on the calling thread, while the per-subject lock is held) right after
  // the tombstone check and right before a fresh key file would be created. Not used by production
  // code paths; defaults to null (no-op). Static state is safe here because it exists solely so
  // tests can force a deterministic interleave against this exact TOCTOU window — see
  // FileSystemCryptoEngineTest.encryptRacingDeleteKey_forgottenAlwaysWins.
  private static volatile Runnable encryptRaceHook;

  static void setEncryptRaceHook(Runnable hook) {
    encryptRaceHook = hook;
  }

  // Test-only hook invoked (on the minting thread, under the per-subject stripe lock) right after
  // the fresh key bytes land in this mint's tmp file and are forced, and right before
  // Files.createLink claims the key path. Not used by production code paths; defaults to null
  // (no-op). Exists so a test can deterministically interleave a SECOND engine instance (= another
  // process) into the tmp-write → createLink window — see FileSystemCryptoEngineTest
  // .crossProcessConcurrentFirstMintsDoNotDeleteEachOthersInFlightTmp.
  private static volatile Runnable mintTmpWrittenHook;

  static void setMintTmpWrittenHook(Runnable hook) {
    mintTmpWrittenHook = hook;
  }

  /**
   * The generation the next mint for this subject must use: one above the highest generation any
   * completed erasure destroyed (1 for a subject never erased). Fails loudly at the single-byte cap
   * instead of wrapping — a wrapped generation would collide with a destroyed one and turn its
   * ciphertext into wrong-key GCM failures.
   */
  private int nextKeyVersion(SubjectId subjectId) {
    int maxErased = readMaxErasedGeneration(subjectId.value());
    int next = maxErased + 1;
    if (next > MAX_KEY_VERSION) {
      throw new CryptoOperationException(
          "Key-generation limit reached for subject: "
              + subjectId.redacted()
              + " — the subject has been erased and re-registered "
              + (MAX_KEY_VERSION - 1)
              + " times, exhausting the single-byte generation counter. Re-register this person"
              + " under a fresh subject id.");
    }
    return next;
  }

  /**
   * Atomically creates the key file, or returns the concurrent winner's key. The key is written to
   * a temp file first and claimed via {@link Files#createLink} — {@code link(2)} fails atomically
   * when the target exists, so an existing key can never be overwritten (overwriting would
   * permanently orphan every ciphertext produced under the old key) and readers never observe a
   * partially written file. ({@code ATOMIC_MOVE} is NOT suitable here: POSIX {@code rename(2)}
   * silently replaces an existing target.) Key files and directories are created owner-only ({@code
   * rw-------} / {@code rwx------}) where the filesystem supports POSIX permissions.
   *
   * <p><b>Post-mint tombstone re-check</b> (mirroring the Vault engine's post-check): after the
   * mint race resolves — whether this call won the {@code createLink} or lost it to a concurrent
   * writer — the tombstone is re-checked before any key is returned. A {@code deleteKey} in another
   * process may have run to completion between the pre-check in {@link #loadOrCreateKey} and the
   * {@code createLink} (per-JVM stripe locks cannot exclude it); without the re-check this mint
   * would resurrect a live key for a subject whose erasure was already reported complete, and
   * nothing would ever delete it again. The loser leg needs the re-check just as much as the winner
   * leg: the key the loser adopts was created inside the race window by SOME process's mint, and if
   * that winner died between its {@code createLink} and its own re-check, the loser is the only
   * live guard left between the orphaned key and returned ciphertext.
   *
   * <p><b>Durability</b> (crash points CP-M1..CP-M4 in the class javadoc): the temp file is forced
   * before the link publishes it, and the directories holding the published key are forced after
   * the re-check and before the key is returned — on both legs, since the winner may be a peer that
   * died between its link and its own forces. The losing leg adopts a key file this instance did
   * not write, so it also forces the file itself. A failed force throws and returns no key.
   */
  private VersionedKey getOrCreateKey(SubjectId subjectId, Path path, Path tombstonePath) {
    SecretKey candidate = generateKey();
    // The mint's generation is one above the highest generation any completed erasure
    // destroyed for this subject (1 for a never-erased subject — whose key file is then written
    // in the 32-byte format). Read under the per-subject stripe lock, so it cannot interleave
    // with an in-JVM deleteKey's max-merge; a cross-process erasure racing this mint is caught by
    // the post-mint tombstone re-check exactly as before (the erased key is removed / the mint is
    // refused), so a stale-generation key never survives an overlapping erasure.
    int version = nextKeyVersion(subjectId);
    try {
      List<Path> publishDirectories = createDirectoriesForPublish(path.getParent());
      // Deliberately NO stray-tmp sweep here. The earlier mint-time
      // heal argued "no other thread can be mid-mint for this subject, so every matching tmp is
      // leaked" — true only within one JVM. Cross-process (a supported deployment; the link(2)
      // claim below exists precisely for it), a matching tmp can be a peer process's LIVE
      // pre-link mint, and deleting it makes that peer's createLink throw NoSuchFileException — a
      // legitimate first encrypt failing spuriously instead of adopting the race winner's key. A
      // genuinely stale tmp (a mint that crashed before its createLink) breaks nothing if left in
      // place: createTempFile below picks a fresh random name, createLink targets the key path,
      // and the orphan holds a never-claimed key no ciphertext was ever produced under. Stray
      // tmps are swept only where deletion has a governing tombstone — deleteKey and
      // refuseMintIfErasedMidRace — or by an operator find (see
      // docs/guide/advanced/gdpr-erasure.md); pre-erasure they are bounded hygiene debt, not a
      // hazard.
      Path tmp =
          Files.createTempFile(
              path.getParent(), path.getFileName().toString(), ".tmp", OWNER_ONLY_FILE);
      boolean won;
      byte[] mintedFingerprint;
      try {
        byte[] encoded = candidate.getEncoded();
        // Generation 1 is written as a 32-byte file; generation >= 2 appends its generation
        // byte (see loadKey). The link(2) claim below is unchanged — one file, atomic.
        byte[] fileContent =
            version == 1 ? encoded : java.util.Arrays.copyOf(encoded, KEY_LENGTH + 1);
        if (version != 1) {
          fileContent[KEY_LENGTH] = (byte) version;
        }
        try {
          mintedFingerprint = fingerprint(fileContent);
          Files.write(tmp, fileContent);
        } finally {
          // Best-effort: getEncoded() returns a fresh copy — zero both buffers once written.
          java.util.Arrays.fill(encoded, (byte) 0);
          java.util.Arrays.fill(fileContent, (byte) 0);
        }
        // The key bytes and the inode are durable while the key is still only a temp
        // file. The link below publishes this very inode under the key name, so the name can
        // never reach the disk ahead of the content: a published key file is never empty or torn
        // after a power loss. A failure here throws before anything is published.
        durability.forceFile(tmp);
        if (mintTmpWrittenHook != null) {
          // Test-only seam (see setMintTmpWrittenHook): parks this mint between its tmp write and
          // its createLink — the exact window a cross-process repro interleaves
          // a second engine instance into.
          mintTmpWrittenHook.run();
        }
        // From the claim on, a key this engine has not forced may be published under this name
        // (ours, or the winner's we adopt). Registered first, with this mint's directories, so
        // whatever throws before forcePublishedKey completes leaves the next load of the key in
        // this instance forcing them all — the parents this mint created included.
        keyDurability.put(path, new KeyDurability.Unforced(publishDirectories));
        won = claimKeyFile(path, tmp);
      } finally {
        // Removed BEFORE the directories are forced below, so the forced shard holds the key
        // entry alone and no stray temp hard link to the key inode survives a power loss.
        Files.deleteIfExists(tmp);
      }
      // Post-mint tombstone re-check, on both legs. Won: SOUNDNESS rests on deleteKey's ordering
      // invariant (tombstone BEFORE deleteIfExists): if a cross-process deleteKey's deleteIfExists
      // ran before this call's createLink, its tombstone was already on disk, so the re-check
      // observes it; if it runs after, it removes the key file this call just linked. Either way
      // no live key survives a completed erasure. (POSIX-local visibility — see the class javadoc
      // for the NFS caveat.) Lost: a concurrent writer won — use its key, never overwrite it. The
      // winner's key was created inside this race window (no key existed at the pre-check), so if
      // the tombstone is visible now, that key post-dates or races the erasure and must not be
      // adopted — and if its creator crashed before its own re-check, this loser is the only guard
      // that removes the orphan. The re-check runs before the forces below, so this mint never
      // makes a refused key durable.
      refuseMintIfErasedMidRace(subjectId, path, tombstonePath);
      if (won) {
        // The key's entry and the directories holding it are durable before
        // the key is returned. The file itself is not forced again: its content and inode were
        // forced as the temp file, and the link published that very inode.
        forcePublishedKey(subjectId, path, false, publishDirectories, mintedFingerprint);
        return new VersionedKey(candidate, version);
      }
      // Lost: a concurrent writer won, and this instance adopts a key file it did not write — a
      // first load. The winner may be a peer process that is still between its link and
      // its own forces, or died there, so the file and its directories are forced before the key
      // is returned; read first, so the memo names exactly the content that is returned.
      LoadedKey adopted = loadKeyForEncrypt(path);
      forcePublishedKey(subjectId, path, true, publishDirectories, adopted.fingerprint());
      return adopted.key();
    } catch (IOException e) {
      throw new CryptoOperationException("Failed to write key to " + path, e);
    }
  }

  /**
   * Claims the key name for {@code tmp}'s inode via {@code link(2)}, which fails atomically when
   * the name exists. Returns {@code false} when a concurrent writer claimed it first.
   */
  private static boolean claimKeyFile(Path path, Path tmp) throws IOException {
    try {
      Files.createLink(path, tmp);
      return true;
    } catch (java.nio.file.FileAlreadyExistsException _) {
      return false;
    }
  }

  /**
   * The one force every key file goes through before an encrypt in this engine instance may use it
   * — the mint's, the losing leg's and the first load's. Forces the key file itself (unless {@code
   * forceKeyFile} is false: the winning mint forced that very inode as its temp file), then the
   * directories that hold it, and only then records the file in {@link #keyDurability} as forced,
   * by the fingerprint of its content. On failure nothing is recorded and the key is NOT returned
   * (the caller throws), so the next encrypt in this engine instance that loads it forces it again
   * before using it.
   */
  private void forcePublishedKey(
      SubjectId subjectId,
      Path keyPath,
      boolean forceKeyFile,
      List<Path> directories,
      byte[] fingerprint) {
    try {
      if (forceKeyFile) {
        durability.forceFile(keyPath);
      }
      forceDirectories(directories);
    } catch (IOException e) {
      throw new CryptoOperationException(
          "The key file for subject "
              + subjectId.redacted()
              + " is published at "
              + keyPath
              + " but could not be made durable; no key is returned, so this encrypt produced"
              + " nothing under it, and the next encrypt for this subject in this engine instance"
              + " forces it again before using it",
          e);
    }
    keyDurability.put(keyPath, new KeyDurability.Forced(fingerprint));
  }

  /**
   * The directories every load forces for a key file this instance did not mint — the two every
   * mint forces: the shard, which holds the key's entry, and the key directory, which holds the
   * shard's (a peer may have created the shard and died before forcing it).
   */
  private List<Path> directoriesHolding(Path keyPath) {
    return List.of(keyPath.getParent(), keyDirectory);
  }

  /**
   * The post-mint half of the cross-process erasure guard: if the subject's tombstone exists once
   * the mint race has resolved, the freshly created key file (and every stray tmp hard link to its
   * inode) is removed and the encrypt is refused. Removing the key here can only converge toward
   * the erased terminal state, never destroy legitimate data: any key present at this point was
   * created inside this mint race — the pre-check saw no key file — so it is either this call's own
   * fresh link, a concurrent winner's key that the racing {@code deleteKey}'s {@code
   * deleteIfExists} targets anyway, or a crashed winner's orphan that nothing else would ever
   * remove (the erasure already reported success). A subject with a PRE-existing key never reaches
   * the mint at all. If the cleanup itself fails, the encrypt still fails loudly (never returns the
   * key) and the residue stays inert: the tombstone-coexistence check in {@link #loadOrCreateKey}
   * refuses every later encrypt, and re-running the idempotent forget removes it.
   *
   * <p><b>Durability</b> (CP-R1). The refusal runs before the mint forces the directories of the
   * published key, so this mint never made the refused key's entry durable; the cleanup — the key
   * unlink and the temp sweep — is then forced (shard directory) before the refusal is reported. A
   * power loss before that force can bring the key entry, or a temp link to it, back next to the
   * tombstone: the key was never returned, so no ciphertext exists under it, and the residue is the
   * same inert coexistence state as a failed cleanup — every encrypt refuses it, and re-running the
   * forget removes it. After the force the cleanup is durable. A failed force is a failed cleanup:
   * loud, no key returned.
   */
  private void refuseMintIfErasedMidRace(SubjectId subjectId, Path keyPath, Path tombstonePath) {
    if (!exists(tombstonePath)) {
      return;
    }
    try {
      Files.deleteIfExists(keyPath);
      // Also sweep stray tmp hard links to the key inode (the deleteKey sweep): a
      // winner that died between createLink and its re-check leaves its tmp link behind, and the
      // completed erasure will never re-run to sweep it. Deleting another in-flight mint's
      // pre-link tmp here is acceptable: that mint's createLink then fails loudly, which is the
      // desired outcome while a tombstone exists.
      deleteStrayTempLinks(keyPath);
      // The cleanup is durable before the refusal is reported.
      durability.forceDirectory(keyPath.getParent());
      // The refused key is durably gone, so this mint's registration of it is dead.
      keyDurability.remove(keyPath);
    } catch (IOException e) {
      throw new CryptoOperationException(
          "Subject was crypto-shredded (GDPR erasure) while this encrypt was minting a key, and"
              + " removing the freshly created key file (or making that removal durable) failed: "
              + keyPath
              + " — no ciphertext is returned and the subject stays gated by its tombstone;"
              + " re-run the forget to remove the residue",
          e);
    }
    throw new SubjectForgottenException(
        "Subject was crypto-shredded (GDPR erasure) while this encrypt was minting its key — the"
            + " freshly created key file was removed and no ciphertext is returned: "
            + subjectId.redacted()
            + " — call reinstate(subjectId) to re-register");
  }

  /**
   * Deletes every leaked temp entry for this subject's key: files named {@code
   * <keyFileName><anything>.tmp} in the key's shard directory. The glob is coupled to the exact
   * {@link Files#createTempFile(Path, String, String, java.nio.file.attribute.FileAttribute...)}
   * call in {@link #getOrCreateKey} (prefix = the full key file name, suffix = {@code ".tmp"}, a
   * random component between them) — change one, change both. The 64-hex-char subject hash prefix
   * makes the glob subject-precise: another subject's residue, the key file itself ({@code
   * .subjectId}) and the tombstone ({@code .forgotten}) can never match. Callers must hold the
   * subject's stripe lock AND be executing erasure intent under the subject's tombstone ({@link
   * #deleteKey}, {@link #refuseMintIfErasedMidRace}) — never a plain mint: the stripe lock excludes
   * no other PROCESS, so a matching tmp can be a peer's live pre-link mint, and deleting it fails
   * that mint's {@code createLink} spuriously. Under a tombstone that loud failure is the desired
   * erasure-wins outcome; without one it would break a legitimate concurrent first-encrypt.
   * Durability is the caller's: both callers force the shard directory after this sweep — {@link
   * #deleteKey} before the erasure reports success, {@link #refuseMintIfErasedMidRace} before the
   * refusal is reported (CP-R1).
   */
  private static void deleteStrayTempLinks(Path keyPath) throws IOException {
    Path shardDir = keyPath.getParent();
    // NOT Files.isDirectory, which — like Files.exists — swallows the IOException and
    // answers false. This sweep only ever runs under the subject's governing tombstone, so a
    // silently skipped sweep leaves stray tmp HARD LINKS to the key inode alive after an erasure
    // that reports success — the hazard, re-opened by an unreadable
    // directory instead of by a crash. Both callers guarantee the shard directory exists by this
    // point (deleteKey's writeTombstone and the mint both create it), so a probe failure here is
    // a real fault and must be loud. If the path somehow is not a directory, newDirectoryStream
    // below throws NotDirectoryException — also loud, never silent.
    if (shardDir == null || !exists(shardDir)) {
      return;
    }
    try (var strays =
        Files.newDirectoryStream(shardDir, keyPath.getFileName().toString() + "*.tmp")) {
      for (Path stray : strays) {
        Files.deleteIfExists(stray);
      }
    }
  }

  private SecretKey generateKey() {
    try {
      var gen = KeyGenerator.getInstance("AES");
      gen.init(256);
      return gen.generateKey();
    } catch (Exception e) {
      throw new CryptoOperationException("AES-256 key generation failed", e);
    }
  }

  /**
   * The fsync primitives the power-loss ordering of {@link #deleteKey(SubjectId)}, of the key mint
   * and of {@link #reinstate(SubjectId)}, and of the first load of a key rests on. Package-private
   * and per instance: production always uses {@link PlatformDurability}; a test substitutes a
   * recorder (to pin which file and directory is forced, in which order, relative to the key
   * unlink, the mint's publish and its return) or a failing implementation (to pin that a failed
   * force stops the erasure before the key is unlinked, and that a mint whose force fails returns
   * no key). A power loss itself cannot be reproduced in a unit test.
   */
  interface Durability {
    /** Forces {@code file}'s data and its inode to stable storage. */
    void forceFile(Path file) throws IOException;

    /** Forces {@code directory}'s entries (the creates, renames and unlinks in it). */
    void forceDirectory(Path directory) throws IOException;
  }

  /**
   * {@link Durability} through {@link FileChannel#force(boolean)}. Where the filesystem has a POSIX
   * view a file is opened for reading, as a directory is: {@code fsync} needs no write access, so a
   * tombstone or {@code .generation} record an operator made read-only (or a restore left
   * read-only) never fails a re-run of an erasure that already wrote it. Elsewhere (Windows/NTFS) a
   * file is opened for writing, because {@code FlushFileBuffers} needs a writable handle; the
   * engine created, owner-only, every file it forces. A directory is opened for reading and forced
   * the same way — the Linux and macOS idiom for making the entries in it durable. Where the
   * filesystem has no POSIX view (Windows/NTFS) a directory cannot be opened as a channel at all,
   * so directory forcing is skipped there and directory-entry durability is whatever that
   * filesystem provides (documented in the module README). Every other failure is thrown: an
   * erasure, a mint or a reinstate whose durability cannot be established fails loudly, and the
   * re-run converges.
   */
  static final class PlatformDurability implements Durability {
    private final boolean directoriesForceable;

    PlatformDurability(boolean directoriesForceable) {
      this.directoriesForceable = directoriesForceable;
    }

    static PlatformDurability forFileSystemOf(Path path) {
      return new PlatformDurability(
          path.getFileSystem().supportedFileAttributeViews().contains("posix"));
    }

    @Override
    public void forceFile(Path file) throws IOException {
      var access = directoriesForceable ? StandardOpenOption.READ : StandardOpenOption.WRITE;
      try (FileChannel channel = FileChannel.open(file, access)) {
        channel.force(true);
      }
    }

    @Override
    public void forceDirectory(Path directory) throws IOException {
      if (!directoriesForceable) {
        return;
      }
      try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
        channel.force(true);
      }
    }
  }

  public static final class Builder {
    private Path keyDirectory;
    private Durability durability;
    private int keyDurabilityMemoCapacity = DEFAULT_KEY_DURABILITY_MEMO_CAPACITY;

    public Builder keyDirectory(Path path) {
      this.keyDirectory = path;
      return this;
    }

    /** Test-only seam (package-private): replaces the platform fsync primitives. */
    Builder durability(Durability durability) {
      this.durability = durability;
      return this;
    }

    /**
     * Test-only seam (package-private): how many forced key files the engine remembers (default
     * 16,384).
     */
    Builder keyDurabilityMemoCapacity(int capacity) {
      if (capacity < 1) throw new IllegalArgumentException("capacity must be at least 1");
      this.keyDurabilityMemoCapacity = capacity;
      return this;
    }

    public FileSystemCryptoEngine build() {
      if (keyDirectory == null) throw new IllegalStateException("keyDirectory is required");
      return new FileSystemCryptoEngine(keyDirectory, durability, keyDurabilityMemoCapacity);
    }
  }
}

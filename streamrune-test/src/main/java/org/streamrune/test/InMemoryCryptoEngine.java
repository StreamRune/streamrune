package org.streamrune.test;

import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * In-memory AES-256-GCM implementation of {@link CryptoEngine} for development and testing.
 *
 * <p>Keys are stored in a {@link ConcurrentHashMap} and lost on JVM shutdown. Do not use in
 * production — use a proper KMS-backed implementation instead.
 *
 * <p><b>Terminal erasure.</b> This double honors the same terminal-erasure contract as the
 * tombstone-backed production engines ({@code FileSystemCryptoEngine}, {@code
 * PostgresCryptoEngine}), so unit tests built on it cannot pass while the real engines would reject
 * the operation. {@link #deleteKey(SubjectId)} removes the key <em>and</em> records an in-memory
 * tombstone; a later {@link #encrypt(SubjectId, byte[])} for the tombstoned subject throws {@link
 * SubjectForgottenException} instead of silently minting a fresh key. The tombstone does not
 * survive JVM restarts (it is in memory), which is the only divergence from the durable backends.
 * As on every production engine, the only way to legitimately re-register a forgotten subject is
 * {@link #reinstate(SubjectId)}, which lifts that subject's tombstone.
 *
 * <p><b>Key generations across erasures (parity with the production engines).</b> Mirroring the
 * postgres/filesystem backends, each mint carries a per-subject generation (first mint = 1; a
 * post-erasure re-mint increments it — the highest erased generation is remembered for the JVM's
 * lifetime), the ciphertext's leading byte records the generation it was written under, and decrypt
 * maps a blob from a DESTROYED generation to {@link KeyNotFoundException} ({@code [REDACTED]})
 * while a tampered current-generation blob still fails loudly. So the erase → reinstate →
 * re-encrypt flow behaves in tests exactly as in production: pre-erasure ciphertext stays
 * replay-tolerant instead of throwing.
 */
public final class InMemoryCryptoEngine implements CryptoEngine {

  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH = 128;
  private static final int GCM_IV_LENGTH = 12;

  /** A subject's AES key together with its generation (the leading ciphertext byte). */
  private record VersionedKey(SecretKey key, int version) {}

  /** Highest generation the single-byte prefix carries; mirrors the production engines' cap. */
  private static final int MAX_KEY_VERSION = Byte.MAX_VALUE;

  private final ConcurrentHashMap<String, VersionedKey> keys = new ConcurrentHashMap<>();
  // Set semantics via a map with a sentinel value: subjectId present == crypto-shredded.
  private final ConcurrentHashMap<String, Boolean> forgotten = new ConcurrentHashMap<>();
  // Highest key generation an erasure destroyed, per subject. Deliberately never removed (not
  // even by reinstate): monotonic generations are what keep destroyed-generation ciphertext
  // reading as [REDACTED] across repeated erase/reinstate cycles.
  private final ConcurrentHashMap<String, Integer> erasedGenerations = new ConcurrentHashMap<>();
  private final SecureRandom random = new SecureRandom();

  /** Creates an engine that enforces terminal erasure, mirroring the real engines. */
  public InMemoryCryptoEngine() {}

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (plaintext == null) throw new IllegalArgumentException("plaintext must not be null");

    VersionedKey key =
        keys.compute(
            subjectId.value(),
            (k, existing) -> {
              if (existing != null) return existing;
              // No key: either brand new, or crypto-shredded. Terminal erasure refuses to mint a
              // fresh key for a forgotten subject; reinstate(subjectId) is the only way to lift
              // that.
              if (forgotten.containsKey(k)) {
                throw new SubjectForgottenException(
                    "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
                        + subjectId.redacted()
                        + " — call reinstate(subjectId) to re-register");
              }
              // One above the highest erased generation (1 for a never-erased subject), so the
              // destroyed generations' ciphertext stays distinguishable — and [REDACTED].
              int version = erasedGenerations.getOrDefault(k, 0) + 1;
              if (version > MAX_KEY_VERSION) {
                throw new CryptoOperationException(
                    "Key-generation limit reached for subject: " + subjectId.redacted());
              }
              return new VersionedKey(generateKey(), version);
            });
    byte[] iv = new byte[GCM_IV_LENGTH];
    random.nextBytes(iv);
    try {
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      byte[] ciphertext = cipher.doFinal(plaintext);
      byte[] result = new byte[1 + GCM_IV_LENGTH + ciphertext.length];
      result[0] = (byte) key.version();
      System.arraycopy(iv, 0, result, 1, GCM_IV_LENGTH);
      System.arraycopy(ciphertext, 0, result, 1 + GCM_IV_LENGTH, ciphertext.length);
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

    VersionedKey key = keys.get(subjectId.value());
    if (key == null) {
      // A forgotten subject's old ciphertext is undecryptable: KeyNotFoundException, never
      // SubjectForgottenException (that is reserved for the encrypt path).
      throw new KeyNotFoundException("Key not found for subject: " + subjectId.redacted());
    }
    // Production-engine parity: every blob-property rejection below is DETERMINISTIC
    // and raised as CryptoMappingException — the subtype the read-path classifiers treat as
    // poison — never as the bare CryptoOperationException they read as a key-store outage and
    // retry forever. A test suite built on this double must reproduce the production engines'
    // poison/quarantine routing for a corrupted blob. The minimum length mirrors the filesystem
    // engine's (generation byte + IV + tag), so a truncated blob is rejected here rather than by
    // an array-bounds failure inside the cipher call.
    int minLength = 1 + GCM_IV_LENGTH + GCM_TAG_LENGTH / 8;
    // The erased-generation check must run BEFORE the full-length rejection below — a
    // blob too short to ever decrypt can still carry a whole, readable generation byte, and a
    // generation the current key has already outlived is the designed [REDACTED] signal (GDPR
    // erasure), not a length-corruption verdict. Guard on length >= 1 first: only the one byte
    // this reads must be safely present, the rest of the body is validated next.
    if (ciphertext.length >= 1) {
      int blobVersion = ciphertext[0];
      if (blobVersion >= 1 && blobVersion < key.version()) {
        // Written under an earlier key generation, destroyed by a GDPR erasure — the designed
        // erased-data signal ([REDACTED]), not a wrong-key GCM failure that blocks replay.
        throw new KeyNotFoundException(
            "Ciphertext was written under key generation "
                + blobVersion
                + ", which a GDPR erasure destroyed; the current key generation is "
                + key.version()
                + " for subject: "
                + subjectId.redacted());
      }
    }
    if (ciphertext.length < minLength) {
      throw new CryptoMappingException(
          "Ciphertext too short for subject: "
              + subjectId.redacted()
              + " — expected at least "
              + minLength
              + " bytes, got "
              + ciphertext.length);
    }
    int blobVersion = ciphertext[0];
    if (blobVersion != key.version()) {
      throw new CryptoMappingException(
          "Unsupported key version " + blobVersion + " for subject: " + subjectId.redacted());
    }
    try {
      byte[] iv = new byte[GCM_IV_LENGTH];
      System.arraycopy(ciphertext, 1, iv, 0, GCM_IV_LENGTH);
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, key.key(), new GCMParameterSpec(GCM_TAG_LENGTH, iv));
      return cipher.doFinal(ciphertext, 1 + GCM_IV_LENGTH, ciphertext.length - 1 - GCM_IV_LENGTH);
    } catch (AEADBadTagException e) {
      // Deterministic: corrupted, tampered, or written under a different key.
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

  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    // Record the tombstone first, then drop the key: the reverse order would briefly leave a
    // forgotten subject with no key and no tombstone, allowing a racing encrypt to resurrect it.
    forgotten.put(subjectId.value(), Boolean.TRUE);
    VersionedKey removed = keys.remove(subjectId.value());
    if (removed != null) {
      // Remember the destroyed generation (max-merge) so a post-reinstate mint uses a higher one
      // and this generation's ciphertext reads as [REDACTED] instead of failing the tag check.
      erasedGenerations.merge(subjectId.value(), removed.version(), Math::max);
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    // Lifting the tombstone lets the next encrypt mint a fresh key. The old key is gone; only
    // future writes are re-enabled. Idempotent: reinstating a never-forgotten subject is a no-op.
    // The erased-generations record deleteKey wrote deliberately survives, keeping generations
    // monotonic. A forget of a never-encrypted subject recorded none, so its next mint is
    // generation 1 — the same numbering as the filesystem and postgres engines.
    forgotten.remove(subjectId.value());
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    return keys.containsKey(subjectId.value());
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
}

package org.streamrune.crypto;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.streamrune.core.Internal;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.KeyNotFoundException;

/**
 * The AES-GCM v1 wire format every key-store engine shares: {@code [FORMAT_VERSION][keyVersion
 * byte][12-byte IV][ciphertext + 128-bit tag]}, its single-attempt open and its header validation.
 *
 * <p>It obtains every cipher from the {@link VerifiedCipher} it is built with, and does no
 * self-check and no retry: the owning engine runs {@link VerifiedCipher#verify} and {@link
 * VerifiedCipher#openWithOneRetry} around {@link #seal} and {@link #openOnce}.
 *
 * @hidden
 */
@Internal
public final class AesGcmV1Format {

  /** The format byte of this layout. */
  public static final byte FORMAT_VERSION = 1;

  /** Length of the header: the format byte and the key-version byte. */
  public static final int HEADER_LENGTH = 2;

  /** Length of the GCM IV in bytes. */
  public static final int GCM_IV_LENGTH = 12;

  /** Length of the GCM tag in bits. */
  public static final int GCM_TAG_LENGTH = 128;

  /** The highest key generation the one-byte header field can name. */
  public static final int MAX_KEY_VERSION = Byte.MAX_VALUE;

  private final VerifiedCipher verified;
  private final SecureRandom random = new SecureRandom();

  /** Creates the format over the cipher seam of the owning engine. */
  public AesGcmV1Format(VerifiedCipher cipher) {
    this.verified = Objects.requireNonNull(cipher, "cipher must not be null");
  }

  /** Seals {@code plaintext} under {@code key}, naming {@code keyVersion} in the header. */
  public byte[] seal(SecretKey key, int keyVersion, byte[] plaintext)
      throws GeneralSecurityException {
    byte[] iv = new byte[GCM_IV_LENGTH];
    random.nextBytes(iv);
    Cipher cipher =
        verified.cipher(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
    byte[] ciphertext = cipher.doFinal(plaintext);
    byte[] result = new byte[HEADER_LENGTH + GCM_IV_LENGTH + ciphertext.length];
    result[0] = FORMAT_VERSION;
    result[1] = (byte) keyVersion;
    System.arraycopy(iv, 0, result, HEADER_LENGTH, GCM_IV_LENGTH);
    System.arraycopy(ciphertext, 0, result, HEADER_LENGTH + GCM_IV_LENGTH, ciphertext.length);
    return result;
  }

  /** One decryption attempt with a cipher of its own; the caller owns any retry. */
  public byte[] openOnce(SecretKey key, byte[] blob) throws GeneralSecurityException {
    Cipher cipher =
        verified.cipher(
            Cipher.DECRYPT_MODE,
            key,
            new GCMParameterSpec(GCM_TAG_LENGTH, blob, HEADER_LENGTH, GCM_IV_LENGTH));
    return cipher.doFinal(
        blob, HEADER_LENGTH + GCM_IV_LENGTH, blob.length - HEADER_LENGTH - GCM_IV_LENGTH);
  }

  /**
   * Rejects blobs that cannot have been produced by this format before touching the cipher, so a
   * format mismatch (different engine, tampering, future version) fails with a clear message
   * instead of a generic GCM tag-check failure — and routes a blob written under a DESTROYED key
   * generation to {@link KeyNotFoundException} so it reads as {@code [REDACTED]} instead of failing
   * the tag check under the wrong key.
   *
   * <p>The destroyed-generation tolerance is strictly bounded to {@code [1, currentKeyVersion)}: a
   * version byte of {@code < 1} or {@code > currentKeyVersion} is not a generation an engine ever
   * minted for the subject, so it stays a loud corruption error — otherwise flipping the byte low
   * would silently redact ANY subject's field, including never-erased ones.
   */
  public static void validateHeader(String subjectDigest, byte[] blob, int currentKeyVersion) {
    // Every rejection below is DETERMINISTIC (a property of the blob, not of the key store), so it
    // is raised as CryptoMappingException — the subtype the family classifiers treat as poison —
    // never as the bare CryptoOperationException they read as a transient key-store outage and
    // retry forever.
    //
    // The erased-generation check below must run BEFORE the full-length rejection — a blob too
    // short to ever decrypt (missing IV/tag) can still carry a whole, readable 2-byte header
    // (FORMAT_VERSION + generation), and a generation the current key has already outlived is
    // still the designed KeyNotFoundException ([REDACTED]) signal for GDPR-erased data, not a
    // length-corruption verdict. Guard on length >= HEADER_LENGTH first: only the two header bytes
    // this reads must be safely present; a genuinely sub-header blob (or one whose format byte
    // doesn't match) falls through unchanged to the full checks below.
    if (blob.length >= HEADER_LENGTH && blob[0] == FORMAT_VERSION) {
      int blobVersion = blob[1];
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
    }
    int minLength = HEADER_LENGTH + GCM_IV_LENGTH + GCM_TAG_LENGTH / 8;
    if (blob.length < minLength) {
      throw new CryptoMappingException(
          "Ciphertext too short for subject: "
              + subjectDigest
              + " — expected at least "
              + minLength
              + " bytes, got "
              + blob.length);
    }
    if (blob[0] != FORMAT_VERSION) {
      throw new CryptoMappingException(
          "Unsupported ciphertext format version "
              + blob[0]
              + " for subject: "
              + subjectDigest
              + " — this engine reads format "
              + FORMAT_VERSION
              + "; the blob may come from a different CryptoEngine or a newer StreamRune version");
    }
    int blobVersion = blob[1];
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
}

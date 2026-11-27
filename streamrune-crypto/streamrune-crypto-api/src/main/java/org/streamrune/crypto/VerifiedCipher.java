package org.streamrune.crypto;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Objects;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import org.slf4j.Logger;
import org.streamrune.core.Internal;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;

/**
 * The cipher seam, the encryption self-check and the one-retry decryption that every AES-GCM crypto
 * engine shares. It holds nothing format-specific: the owning engine supplies the attempt that
 * opens one value (header validation, offsets, AAD) and keeps its own mapping of a raw {@link
 * AEADBadTagException} in its {@code decrypt}.
 *
 * <p><b>Verified encryption.</b> {@link #verify} runs <em>one</em> attempt and compares its result
 * with the plaintext in constant time ({@link MessageDigest#isEqual}). A mismatch, or an attempt
 * that throws, counts {@code streamrune.crypto.encrypt_self_check_failed}, logs a WARN and throws
 * the transient {@link CryptoOperationException}, so a wrong cipher result is never handed to the
 * caller, whose retry encrypts it again. It never retries.
 *
 * <p><b>Cause versus suppressed.</b> A checked failure of the check (a tag failure) is the cause of
 * the transient exception. An unchecked one — any {@code RuntimeException}, an {@code
 * IllegalArgumentException} or a {@link CryptoMappingException} among them — is attached as
 * suppressed instead: the classifiers walk the cause chain and read those types as permanent
 * evidence, while a failed check is transient whatever raised it.
 *
 * <p><b>Retried decryption.</b> {@link #openWithOneRetry} retries once with a fresh cipher when the
 * authentication tag does not match. A second mismatch propagates raw with the first attached as
 * suppressed: the value itself is bad. Either outcome counts {@code
 * streamrune.crypto.decrypt_retried}, tagged {@code RECOVERED} or {@code FAILED}, and logs a WARN.
 * Every other exception propagates unchanged.
 *
 * <p>The WARN lines go to the logger the owning engine passes in, so their category stays that
 * engine's class.
 *
 * @hidden
 */
@Internal
public final class VerifiedCipher {

  private static final String ALGORITHM = "AES/GCM/NoPadding";

  /** The {@code outcome} of {@code streamrune.crypto.decrypt_retried}. */
  private static final String RETRY_RECOVERED = "RECOVERED";

  private static final String RETRY_FAILED = "FAILED";

  /**
   * Creates the cipher of one operation, initialised for {@code mode} under {@code key}. A test can
   * inject a fault through it; production always uses {@link #FRESH_CIPHERS}.
   */
  @FunctionalInterface
  public interface CipherFactory {
    /** Creates a cipher initialised for {@code mode} under {@code key} with {@code params}. */
    Cipher create(int mode, Key key, AlgorithmParameterSpec params) throws GeneralSecurityException;
  }

  /** One attempt to open a value with a cipher of its own. */
  @FunctionalInterface
  public interface Attempt {
    /** Runs the attempt and returns the plaintext it opened. */
    byte[] run() throws GeneralSecurityException;
  }

  // A new Cipher for every encryption, every check of an encryption and every decryption attempt.
  // Cipher.getInstance costs about a microsecond, a few times the encryption of a short value
  // (PerfCandidatesBenchmark, candidate C), and buys isolation: a cipher reused across operations
  // carries internal state from one operation to the next, so a fault in one operation could
  // spoil later ones on the same thread. A fresh cipher confines a fault to one operation, which
  // the encryption check or the decryption retry then catches.
  /** Creates a new {@code AES/GCM/NoPadding} cipher for every call. */
  public static final CipherFactory FRESH_CIPHERS =
      (mode, key, params) -> {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(mode, key, params);
        return cipher;
      };

  private final StreamRuneMetrics metrics;
  private final Logger log;
  private final CipherFactory ciphers;

  /**
   * @param metrics counts failed self-checks and retried decryptions
   * @param log the owning engine's logger; the WARN lines are logged under its category
   * @param ciphers creates the cipher of each operation; production passes {@link #FRESH_CIPHERS}
   */
  public VerifiedCipher(StreamRuneMetrics metrics, Logger log, CipherFactory ciphers) {
    this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    this.log = Objects.requireNonNull(log, "log must not be null");
    this.ciphers = Objects.requireNonNull(ciphers, "ciphers must not be null");
  }

  /** One fresh cipher per call, from the factory. */
  public Cipher cipher(int mode, Key key, AlgorithmParameterSpec params)
      throws GeneralSecurityException {
    return ciphers.create(mode, key, params);
  }

  /**
   * Checks a fresh encryption: runs {@code openOnce} once and compares its result with {@code
   * plaintext}. Any failure — a mismatch, a tag failure or an exception of the check itself —
   * throws the transient {@link CryptoOperationException}; see the class comment for where the
   * failure is attached.
   */
  public void verify(SubjectId subjectId, byte[] plaintext, Attempt openOnce) {
    Exception failure = null;
    try {
      if (MessageDigest.isEqual(plaintext, openOnce.run())) {
        return;
      }
    } catch (GeneralSecurityException | RuntimeException e) {
      failure = e;
    }
    throw selfCheckFailed(subjectId, failure);
  }

  /**
   * Counts and logs a failed encryption check; the returned exception is the transient type. A
   * checked failure of the check (a tag failure) is its cause. An unchecked one — any {@code
   * RuntimeException}, an {@code IllegalArgumentException} or a {@link CryptoMappingException}
   * among them — is attached as suppressed instead: the classifiers walk the cause chain and read
   * those types as permanent evidence, while a failed check is transient whatever raised it.
   */
  private CryptoOperationException selfCheckFailed(SubjectId subjectId, Exception failure) {
    metrics.recordEncryptSelfCheckFailed();
    log.warn(
        "Encryption self-check failed for subject {}: the encrypted value did not decrypt to its"
            + " plaintext and is discarded; the operation fails as transient and is retried",
        subjectId.redacted());
    boolean unchecked = failure instanceof RuntimeException;
    CryptoOperationException transientFailure =
        new CryptoOperationException(
            "Encryption self-check failed for subject: "
                + subjectId.redacted()
                + " — the encrypted value did not decrypt to its plaintext and is discarded"
                + " (transient; retrying encrypts it again)",
            unchecked ? null : failure);
    if (unchecked) {
      transientFailure.addSuppressed(failure);
    }
    return transientFailure;
  }

  /**
   * Decrypts, and retries once with a fresh cipher when the authentication tag does not match. A
   * second mismatch propagates with the first attached as suppressed: the value itself is bad.
   */
  public byte[] openWithOneRetry(SubjectId subjectId, Attempt openOnce)
      throws GeneralSecurityException {
    try {
      return openOnce.run();
    } catch (AEADBadTagException first) {
      try {
        byte[] plaintext = openOnce.run();
        retried(subjectId, RETRY_RECOVERED);
        return plaintext;
      } catch (AEADBadTagException second) {
        retried(subjectId, RETRY_FAILED);
        second.addSuppressed(first);
        throw second;
      }
    }
  }

  /** Counts and logs a retried decryption with its outcome. */
  private void retried(SubjectId subjectId, String outcome) {
    metrics.recordDecryptRetried(outcome);
    log.warn(
        "Decryption for subject {} failed its authentication tag and was retried once with a fresh"
            + " cipher; outcome {}",
        subjectId.redacted(),
        outcome);
  }
}

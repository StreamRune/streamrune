package org.streamrune.aws;

import java.util.Arrays;
import java.util.Map;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.DisabledException;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

/**
 * AWS KMS-backed {@link CryptoEngine}. Encryption and decryption are performed by AWS KMS using a
 * single Customer Managed Key (the {@code subjectId} is not a per-subject key here); key material
 * never leaves AWS.
 *
 * <p><b>Terminal erasure via a per-subject tombstone.</b> A single shared CMK encrypts every
 * subject, so there is no per-subject key to hard-delete. {@link #deleteKey(SubjectId)} therefore
 * records a tombstone in a {@link ForgottenSubjectStore} instead of throwing: after it, {@link
 * #decrypt(SubjectId, byte[])} throws {@link KeyNotFoundException} (mapped to the {@code
 * [REDACTED]} tombstone by {@code CryptoShreddingModule}) and {@link #encrypt(SubjectId, byte[])}
 * throws {@link SubjectForgottenException}, matching the terminal- erasure behavior of the
 * filesystem and PostgreSQL backends. This makes the auto-wired {@code ForgetSubjectService}
 * complete on KMS — the crypto-shred step succeeds and the read-model purgers run. {@link
 * #reinstate(SubjectId)} clears the tombstone for legitimate re-registration.
 *
 * <p><b>Durability is mandatory — the engine fails closed.</b> The shared CMK itself is never
 * modified, so erasure is only as durable as the {@link ForgottenSubjectStore tombstone store}. A
 * process-local store would silently un-forget erased subjects on restart (PII resurrection), so
 * {@link Builder#build()} <b>throws</b> unless a store is supplied via {@link
 * Builder#forgottenSubjectStore(ForgottenSubjectStore)}: there is no silent in-memory default. Wire
 * the durable {@code JdbcForgottenSubjectStore} (in {@code streamrune-postgres-crypto}) in
 * production (the framework auto-configs do this automatically when a {@code DataSource} is
 * present); {@link InMemoryForgottenSubjectStore} may be passed explicitly for tests and
 * single-process/ephemeral use.
 *
 * <p><b>Every CMK-level failure fails closed, never redacts.</b> On this backend the ONLY thing
 * that produces the {@code [REDACTED]} tombstone is the per-subject tombstone short-circuit at the
 * top of {@link #decrypt(SubjectId, byte[])} — a lawful erasure never reaches AWS KMS at all.
 * Consequently no KMS-reported key state is ever treated as a crypto-shred: a <em>disabled</em> or
 * invalid/transient state ({@code DisabledException}/{@code KmsInvalidStateException} — disable,
 * pending import, unavailable multi-Region replica, disconnected XKS store, pending deletion) is
 * REVERSIBLE, and a <em>missing or inaccessible</em> CMK ({@code NotFoundException} — mistyped
 * {@code kms-key-id}, a key ARN from another region/account, a revoked {@code kms:Decrypt} grant,
 * or a destroyed CMK, which the SDK does not distinguish) is an infrastructure/configuration fault.
 * Both throw {@link CryptoOperationException} and propagate. With one shared CMK, mapping either to
 * {@link KeyNotFoundException} would false-redact every subject at once and permanently corrupt any
 * read model rebuilt in that window, since fixing the configuration or re-enabling the key does not
 * heal already-written {@code [REDACTED]} rows. {@link #isKeyAvailable(SubjectId)} applies the same
 * taxonomy: it answers {@code false} only for a tombstoned subject or a Disabled/PendingDeletion
 * CMK state, and throws when the CMK cannot be described at all.
 *
 * <p><b>Blob-level verdicts are deterministic.</b> {@code InvalidCiphertextException} — KMS's
 * rejection of the ciphertext itself (corrupted, tampered, or bound to a different encryption
 * context) — is a decrypt failure no retry, key change or configuration fix can heal, so it is
 * raised as {@link CryptoMappingException}: the read-path classifiers then quarantine the one blob
 * instead of treating it as a KMS outage and retrying forever. A blob without this engine's
 * envelope, or with an envelope version it does not write, gets the same verdict before any KMS
 * call. Key-level and service-level failures (above; throttling, internal errors, an {@code
 * IncorrectKeyException} from a wrong {@code kms-key-id}) stay the bare {@link
 * CryptoOperationException}.
 *
 * <p><b>Encrypt re-checks the tombstone after the KMS call.</b> The pre-check alone leaves a TOCTOU
 * window in which a concurrent erasure completes in full — tombstone committed, purgers run, the
 * data subject told "erased" — while this encrypt is between its pre-check and its KMS response.
 * Because {@link #deleteKey(SubjectId)} never touches the shared CMK, the ciphertext such an
 * encrypt would return stays fully decryptable by anyone holding {@code kms:Decrypt} plus DB read,
 * and no rebuild, sweep or audit surfaces it ({@link #decrypt(SubjectId, byte[])} short-circuits on
 * the tombstone). {@link #encrypt(SubjectId, byte[])} therefore re-checks {@code isForgotten} after
 * a successful KMS response and throws {@link SubjectForgottenException} instead of returning the
 * bytes — the same guard the vault and filesystem engines already carry, minus their cleanup leg
 * (this engine's encrypt creates no key material). Sound because {@code deleteKey}'s only durable
 * step is the tombstone commit, which completes before it returns.
 */
public final class AwsKmsCryptoEngine implements CryptoEngine {

  /**
   * Subject-binding envelope. A ciphertext written by this engine is {@code [magic 'S' 'R' 'K'
   * 'C'][version byte][raw KMS ciphertext blob]}, and the KMS ciphertext is produced under an
   * {@link #SUBJECT_CONTEXT_KEY} EncryptionContext equal to {@code sha256hex(subjectId)}. Decrypt
   * requires the envelope and always re-supplies that context, so KMS itself rejects a blob
   * presented under another subject. A blob without the magic was not written by this engine and is
   * refused before any KMS call: decrypting it without a context would let a context-less
   * ciphertext produced under the shared CMK by any other producer decrypt under any subject id.
   */
  private static final byte[] CONTEXT_ENVELOPE_MAGIC = {'S', 'R', 'K', 'C'};

  private static final byte CONTEXT_ENVELOPE_VERSION = 1;

  /**
   * EncryptionContext key carrying the subject hash — recorded (hashed, never raw) in CloudTrail.
   */
  private static final String SUBJECT_CONTEXT_KEY = "streamrune-subject";

  private final KmsClient kmsClient;
  private final String kmsKeyId;
  private final ForgottenSubjectStore forgottenSubjects;

  private AwsKmsCryptoEngine(Builder builder) {
    this.kmsClient = builder.kmsClient;
    this.kmsKeyId = builder.kmsKeyId;
    this.forgottenSubjects = builder.forgottenSubjects;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Terminal erasure: refuse to re-encrypt for a crypto-shredded subject, matching the
      // filesystem/PostgreSQL backends. Lift via reinstate(subjectId) for legitimate
      // re-registration.
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    try {
      // Bind the ciphertext to this subject via an EncryptionContext of its SHA-256
      // hash, so KMS itself rejects any subject/ciphertext mismatch on decrypt (and CloudTrail
      // records the subject hash, never the raw id). Prefix a versioned magic so decrypt knows to
      // re-supply the same context.
      EncryptRequest request =
          EncryptRequest.builder()
              .keyId(kmsKeyId)
              .plaintext(SdkBytes.fromByteArray(plaintext))
              .encryptionContext(subjectContext(subjectId))
              .build();
      EncryptResponse response = kmsClient.encrypt(request);
      // Re-check the tombstone AFTER the KMS call, before the ciphertext is handed
      // to the caller. The pre-check alone leaves a TOCTOU window: a ForgetSubjectService running
      // concurrently can complete the ENTIRE erasure — tombstone committed, purgers run, the data
      // subject told "erased", ForgetResult.fullyErased() true — between that pre-check and this
      // request landing at KMS. Returning here would hand back a brand-new at-rest ciphertext of
      // that subject's personal data, written after the Article-17 erasure was reported complete.
      //
      // This backend needs the check MORE than its siblings, not less: deleteKey never touches the
      // shared CMK, so unlike vault (transit key deleted) and filesystem/postgres (per-subject key
      // destroyed) the raced ciphertext stays FULLY DECRYPTABLE by anyone holding kms:Decrypt plus
      // DB read — and it is invisible to every remediation path, because decrypt short-circuits on
      // the tombstone and answers [REDACTED] without ever looking at it.
      //
      // Soundness is the Vault argument: deleteKey's ONLY durable
      // step is the tombstone commit, and it completes before deleteKey returns — so an erasure
      // that finished before this line necessarily has its tombstone visible to this read, while
      // an encrypt whose post-check ran before that commit completed entirely before the erasure.
      // Unlike Vault there is no cleanup leg: encrypt creates nothing (the CMK pre-exists and is
      // never per-subject), so refusing to return the bytes is the whole remedy. Residual, shared
      // with every backend: the CALLER persists the returned ciphertext, so an erasure committing
      // between this check and that write is still not caught here — ForgetSubjectService is
      // idempotent and a re-run converges.
      if (forgottenSubjects.isForgotten(subjectId)) {
        throw new SubjectForgottenException(
            "Subject was crypto-shredded (GDPR erasure) while this encrypt was in flight — the"
                + " ciphertext is discarded and never returned: "
                + subjectId.redacted()
                + " — call reinstate(subjectId) to re-register");
      }
      return wrapWithContextEnvelope(response.ciphertextBlob().asByteArray());
    } catch (IllegalArgumentException | SubjectForgottenException e) {
      // SubjectForgottenException must escape the blanket wrap below: laundering it into a
      // CryptoOperationException would hide the terminal-erasure signal that CryptoShreddingModule
      // and the command bus's DLQ classification both key off.
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "AWS KMS encrypt failed for subject: " + subjectId.redacted(), e);
    }
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (ciphertext == null) {
      throw new IllegalArgumentException("ciphertext must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Crypto-shredded subject: old ciphertext is unreadable. KeyNotFoundException is mapped to
      // the [REDACTED] tombstone by CryptoShreddingModule, so replay/read models surface no PII.
      throw new KeyNotFoundException(
          "AWS KMS subject was crypto-shredded (GDPR erasure): " + subjectId.redacted());
    }
    try {
      // A ciphertext written by this engine carries the version envelope and must be
      // decrypted under the SAME subject EncryptionContext (KMS rejects a mismatch, so subjectB
      // cannot decrypt subjectA's ciphertext).
      //
      // Both envelope rejections below are DETERMINISTIC blob-property failures (decided
      // before any KMS call — retrying can never succeed), raised as the CryptoMappingException
      // subtype the read-path classifiers treat as poison; the bare CryptoOperationException reads
      // to them as a KMS OUTAGE and is retried forever.
      if (!hasContextEnvelope(ciphertext)) {
        throw new CryptoMappingException(
            "Ciphertext is missing the StreamRune AWS KMS envelope — not written by this engine"
                + " (deterministic, not retried)");
      }
      byte version = ciphertext[CONTEXT_ENVELOPE_MAGIC.length];
      if (version != CONTEXT_ENVELOPE_VERSION) {
        throw new CryptoMappingException(
            "Unsupported AWS KMS ciphertext envelope version "
                + version
                + " — this engine writes version "
                + CONTEXT_ENVELOPE_VERSION
                + "; the blob may come from a newer StreamRune version");
      }
      byte[] kmsBlob =
          Arrays.copyOfRange(ciphertext, CONTEXT_ENVELOPE_MAGIC.length + 1, ciphertext.length);
      DecryptRequest request =
          DecryptRequest.builder()
              .ciphertextBlob(SdkBytes.fromByteArray(kmsBlob))
              .keyId(kmsKeyId)
              .encryptionContext(subjectContext(subjectId))
              .build();
      DecryptResponse response = kmsClient.decrypt(request);
      return response.plaintext().asByteArray();
    } catch (IllegalArgumentException | CryptoOperationException e) {
      throw e;
    } catch (InvalidCiphertextException e) {
      // KMS's verdict on the blob ITSELF — corrupted, tampered, an encryption context that
      // does not match the one it was bound to, or material KMS can no longer verify.
      // Deterministic: retrying can never succeed, so raise the CryptoMappingException subtype the
      // read-path classifiers treat as poison; the bare type reads to them as a KMS OUTAGE and is
      // retried forever. Every KEY-level or service-level failure below (a missing, disabled or
      // invalid CMK; a wrong CMK for the blob — IncorrectKeyException is a configuration fault;
      // throttling; internal errors) stays bare on purpose: those are reversible or transient.
      throw new CryptoMappingException(
          "AWS KMS rejected the ciphertext for subject: "
              + subjectId.redacted()
              + " (InvalidCiphertextException — corrupted, tampered, or bound to a different"
              + " encryption context; deterministic, retrying cannot succeed)",
          e);
    } catch (NotFoundException e) {
      // A missing CMK is an INFRASTRUCTURE/CONFIGURATION fault, never a crypto-shred.
      // KMS raises NotFoundException identically for a mistyped kms-key-id, a key ARN from another
      // region/account, a key this principal has lost kms:Decrypt visibility on (SCP or key-policy
      // change), and a genuinely destroyed CMK — the SDK gives us nothing to tell them apart. And
      // on this backend a LAWFUL erasure can never reach this line at all: deleteKey only records a
      // per-subject tombstone (the shared CMK is never touched) and decrypt short-circuits on that
      // tombstone above, before any KMS call. So every reachable occurrence here is an operational
      // fault, and mapping it to KeyNotFoundException — the one type CryptoShreddingModule turns
      // into the [REDACTED] tombstone — would false-redact EVERY subject at once under the shared
      // CMK: a projection rebuild / saga load / DLQ replay running in that window writes those
      // tombstones into read models and reports SUCCESS, and fixing the key id afterwards does NOT
      // heal them (silent, permanent corruption). Fail closed for exactly the reason the
      // Disabled/InvalidState branch below already does — a mistyped key id is even
      // more recoverable than a disabled one — and matching the Vault engine's taxonomy.
      //
      // Deliberately NOT offered: an opt-in "treat a missing CMK as a fleet-wide erasure" flag.
      // It would serve zero cases this branch can identify (the fault is indistinguishable from a
      // typo) while re-arming the silent mass-redaction. Destroying the CMK as a bulk erasure is an
      // out-of-band operator act whose in-framework expression is per-subject tombstones via
      // deleteKey/ForgetSubjectService, which short-circuit before KMS and are auditable.
      throw new CryptoOperationException(
          "AWS KMS key not found or not accessible (infrastructure/configuration fault — NOT a"
              + " crypto-shred): "
              + kmsKeyId
              + " — check streamrune.crypto.aws.kms-key-id (typo, wrong region/account) and this"
              + " principal's kms:Decrypt grant. Decryption fails closed rather than redacting"
              + " every subject: a lawful GDPR erasure never reaches AWS KMS on this backend (it"
              + " is a per-subject tombstone), so this can only be an operational fault. If the"
              + " CMK really was destroyed, record the affected subjects via deleteKey /"
              + " ForgetSubjectService so the erasure is explicit and auditable.",
          e);
    } catch (DisabledException | KmsInvalidStateException e) {
      // A DISABLED CMK or a KmsInvalidState (pending import, unavailable multi-Region
      // replica, disconnected XKS store, pending deletion) is REVERSIBLE — re-enabling / cancelling
      // deletion restores every subject. It must NOT be mapped to KeyNotFoundException: that would
      // make CryptoShreddingModule substitute [REDACTED] for EVERY subject (one shared CMK), so a
      // projection rebuild / saga load / DLQ replay running in that window would write [REDACTED]
      // into read models as if the subjects were lawfully erased — and re-enabling the key would
      // NOT heal those rows (silent, permanent corruption). Fail closed instead: a
      // CryptoOperationException propagates (not caught by decryptValue's KeyNotFound handler), so
      // replay blocks and retries until the key is usable again — matching the Vault engine's
      // fail-closed taxonomy.
      throw new CryptoOperationException(
          "AWS KMS key is disabled or in an invalid/transient state (reversible — NOT a"
              + " crypto-shred): "
              + kmsKeyId,
          e);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "AWS KMS decrypt failed for subject: " + subjectId.redacted(), e);
    }
  }

  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    // Shared CMK: no per-subject key to hard-delete. Record a per-subject tombstone so decrypt
    // yields [REDACTED] and encrypt throws SubjectForgottenException — the auto-wired
    // ForgetSubjectService then completes (crypto-shred step + read-model purge). Idempotent.
    forgottenSubjects.forget(subjectId);
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    // Clears the tombstone so a future encrypt is allowed again. The shared CMK was never modified,
    // so pre-erasure ciphertext this subject had is decryptable once more.
    forgottenSubjects.reinstate(subjectId);
  }

  @Override
  public java.util.Set<String> requiredCryptoTables() {
    // KMS uses a shared CMK (no per-subject key table); only a durable, DB-backed
    // tombstone store needs the forgotten_subjects table provisioned/validated. An in-memory store
    // needs no schema.
    return forgottenSubjects.requiresDatabaseSchema()
        ? java.util.Set.of("forgotten_subjects")
        : java.util.Set.of();
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      return false;
    }
    try {
      DescribeKeyRequest request = DescribeKeyRequest.builder().keyId(kmsKeyId).build();
      DescribeKeyResponse response = kmsClient.describeKey(request);
      KeyMetadata metadata = response.keyMetadata();
      String state = metadata.keyStateAsString();
      return !"Disabled".equals(state) && !"PendingDeletion".equals(state);
    } catch (NotFoundException e) {
      // Advisory path, same taxonomy as decrypt above. A NotFoundException from
      // DescribeKey is about the CONFIGURED SHARED CMK, not about this subject — a lawfully erased
      // subject already returned false on its tombstone before any KMS call. Answering false here
      // reports EVERY subject's key as absent, and "no key" is documented as readable as "already
      // crypto-shredded" (VaultCryptoEngine: "a live key must never be misreported as absent").
      // CryptoEngine#isKeyAvailable declares @throws CryptoOperationException if the lookup fails,
      // so failing closed is in contract.
      throw new CryptoOperationException(
          "AWS KMS key not found or not accessible (infrastructure/configuration fault — NOT a"
              + " crypto-shred): "
              + kmsKeyId
              + " — check streamrune.crypto.aws.kms-key-id and this principal's kms:DescribeKey"
              + " grant. Reporting the key as absent would misread an operational fault as a"
              + " completed erasure for every subject under this shared CMK.",
          e);
    } catch (Exception e) {
      throw new CryptoOperationException(
          "AWS KMS isKeyAvailable failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * The subject-binding EncryptionContext: {@code {"streamrune-subject": sha256hex(subjectId)}}.
   */
  private static Map<String, String> subjectContext(SubjectId subjectId) {
    return Map.of(SUBJECT_CONTEXT_KEY, subjectId.redacted());
  }

  /** Prepends the {@code [magic][version]} envelope to a raw KMS ciphertext blob. */
  private static byte[] wrapWithContextEnvelope(byte[] kmsBlob) {
    byte[] result = new byte[CONTEXT_ENVELOPE_MAGIC.length + 1 + kmsBlob.length];
    System.arraycopy(CONTEXT_ENVELOPE_MAGIC, 0, result, 0, CONTEXT_ENVELOPE_MAGIC.length);
    result[CONTEXT_ENVELOPE_MAGIC.length] = CONTEXT_ENVELOPE_VERSION;
    System.arraycopy(kmsBlob, 0, result, CONTEXT_ENVELOPE_MAGIC.length + 1, kmsBlob.length);
    return result;
  }

  /**
   * True when {@code blob} carries this engine's context envelope magic and a version byte (so it
   * was written by this engine, with a subject EncryptionContext). Anything else — a raw KMS
   * ciphertext, a blob of another engine, or a blob too short to hold the envelope — returns false
   * and is refused by {@link #decrypt(SubjectId, byte[])}.
   */
  private static boolean hasContextEnvelope(byte[] blob) {
    if (blob.length < CONTEXT_ENVELOPE_MAGIC.length + 1) {
      return false;
    }
    for (int i = 0; i < CONTEXT_ENVELOPE_MAGIC.length; i++) {
      if (blob[i] != CONTEXT_ENVELOPE_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  public static final class Builder {

    private KmsClient kmsClient;
    private String region;
    private String kmsKeyId;
    // No silent in-memory default — build() fails closed unless a store is supplied, so KMS
    // erasure can never be silently non-durable (which would resurrect erased PII on restart).
    private ForgottenSubjectStore forgottenSubjects = null;

    public Builder kmsClient(KmsClient client) {
      this.kmsClient = client;
      return this;
    }

    public Builder region(String region) {
      this.region = region;
      return this;
    }

    public Builder kmsKeyId(String keyId) {
      this.kmsKeyId = keyId;
      return this;
    }

    /**
     * Sets the crypto-shredding tombstone store used for GDPR erasure. <b>Required:</b> there is no
     * default — {@link #build()} fails closed if none is supplied. Use the durable {@code
     * JdbcForgottenSubjectStore} (in {@code streamrune-postgres-crypto}) in production so erasure
     * survives restarts; {@link InMemoryForgottenSubjectStore} is acceptable only for tests /
     * single-process ephemeral use.
     *
     * @param forgottenSubjects the tombstone store; must not be null
     */
    public Builder forgottenSubjectStore(ForgottenSubjectStore forgottenSubjects) {
      if (forgottenSubjects == null) {
        throw new IllegalArgumentException("forgottenSubjectStore must not be null");
      }
      this.forgottenSubjects = forgottenSubjects;
      return this;
    }

    public AwsKmsCryptoEngine build() {
      if (kmsKeyId == null || kmsKeyId.isBlank()) {
        throw new IllegalStateException("kmsKeyId is required");
      }
      if (forgottenSubjects == null) {
        // Fail-closed: refuse to build an engine whose GDPR erasure would be non-durable. A
        // silent in-memory tombstone is lost on restart, resurrecting erased PII under the intact
        // shared CMK.
        throw new IllegalStateException(
            "AWS-KMS GDPR erasure requires a persistent ForgottenSubjectStore: the shared CMK is"
                + " never deleted, so an erased subject stays forgotten only as long as its"
                + " tombstone does. Configure a persistent ForgottenSubjectStore for durable GDPR"
                + " erasure on AWS-KMS (e.g. JdbcForgottenSubjectStore backed by your DataSource)"
                + " via forgottenSubjectStore(...); InMemoryForgottenSubjectStore is for tests"
                + " only and must never be the production default.");
      }
      KmsClient client = kmsClient;
      if (client == null) {
        if (region == null || region.isBlank()) {
          throw new IllegalStateException("region is required when kmsClient is not provided");
        }
        client = KmsClient.builder().region(Region.of(region)).build();
      }
      this.kmsClient = client;
      return new AwsKmsCryptoEngine(this);
    }
  }
}

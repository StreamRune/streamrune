package org.streamrune.aws;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.LongSupplier;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
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
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.DisabledException;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

/**
 * AWS KMS-backed {@link CryptoEngine} using envelope encryption under a single Customer Managed Key
 * (CMK): the CMK never leaves AWS; each value is encrypted locally with AES-256-GCM under a data
 * key that KMS generates and wraps.
 *
 * <p><b>Envelope encryption.</b> {@link #encrypt(SubjectId, byte[])} asks KMS {@code
 * GenerateDataKey} for an AES-256 data key under the CMK, with the encryption context {@code
 * {"streamrune-subject": sha256hex(subjectId)}}, encrypts the value locally, and stores the wrapped
 * data key in the ciphertext. {@link #decrypt(SubjectId, byte[])} asks KMS {@code Decrypt} to
 * unwrap that data key under the same context and key id, then decrypts locally. KMS only ever sees
 * data keys, so a value of any size is encrypted (KMS {@code Encrypt} refuses a plaintext above
 * 4,096 bytes), and a data key serves many values. The ciphertext is
 *
 * <pre>
 * [magic 'S' 'R' 'K' 'C'][version 2][wrapped-key length, 2 bytes big-endian][wrapped data key]
 * [12-byte nonce][AES-256-GCM ciphertext][16-byte tag]
 * </pre>
 *
 * with the GCM associated data {@code header || sha256hex(subjectId)}, where the header is every
 * byte before the nonce. A value is therefore bound to its subject twice: KMS refuses to unwrap the
 * data key under another subject's context, and the GCM tag fails for another subject even if a
 * data key were unwrapped. The {@link CryptoEngine} contract carries no field identity, so values
 * of one subject are not bound to the field they were written for — on every backend.
 *
 * <p><b>Data-key cache.</b> The engine keeps a bounded in-process cache ({@link
 * Builder#dataKeyCacheMaximumSize(int)}, default {@value #DEFAULT_DATA_KEY_CACHE_MAXIMUM_SIZE}
 * entries for encryption and as many for decryption; {@code 0} disables it): per subject, the data
 * key it encrypts under, and the data keys it unwrapped. Each entry lives at most {@link
 * Builder#dataKeyCacheExpireAfterWrite(Duration)} (default 5 minutes) after it was stored, and an
 * encrypting key is replaced after {@value #MAX_ENCRYPTIONS_PER_DATA_KEY} values, far below the
 * 2<sup>32</sup> random-nonce limit of AES-GCM. A subject therefore costs one {@code
 * GenerateDataKey} per window on the replica that writes it and one {@code Decrypt} per data key
 * and window on each replica that reads it. With the cache disabled every value costs one KMS call,
 * and every value carries its own data key.
 *
 * <p><b>Terminal erasure via a per-subject tombstone.</b> A single shared CMK wraps every subject's
 * data keys, so there is no per-subject key to hard-delete. {@link #deleteKey(SubjectId)} therefore
 * records a tombstone in a {@link ForgottenSubjectStore} and drops the subject's cached data keys:
 * after it, {@link #decrypt(SubjectId, byte[])} throws {@link KeyNotFoundException} (mapped to the
 * {@code [REDACTED]} tombstone by {@code CryptoShreddingModule}) and {@link #encrypt(SubjectId,
 * byte[])} throws {@link SubjectForgottenException}, matching the terminal- erasure behavior of the
 * filesystem and PostgreSQL backends. Both read the tombstone before they look at the cache or call
 * KMS, on every call, so a data key another replica still caches is never used for a subject
 * forgotten anywhere: the tombstone store is shared, and it is the only thing that decides. This
 * makes the auto-wired {@code ForgetSubjectService} complete on KMS — the crypto-shred step
 * succeeds and the read-model purgers run. {@link #reinstate(SubjectId)} clears the tombstone for
 * legitimate re-registration.
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
 * Both throw {@link CryptoOperationException} and propagate, from {@code GenerateDataKey} on
 * encrypt as from {@code Decrypt} on decrypt. With one shared CMK, mapping either to {@link
 * KeyNotFoundException} would false-redact every subject at once and permanently corrupt any read
 * model rebuilt in that window, since fixing the configuration or re-enabling the key does not heal
 * already-written {@code [REDACTED]} rows. {@link #isKeyAvailable(SubjectId)} applies the same
 * taxonomy: it answers {@code false} only for a tombstoned subject or a Disabled/PendingDeletion
 * CMK state, and throws when the CMK cannot be described at all.
 *
 * <p><b>Verdicts on the value are deterministic.</b> A blob without this engine's envelope, with an
 * envelope version it does not write, or too short for its own header is refused before any KMS
 * call; KMS's {@code InvalidCiphertextException} on the wrapped data key (corrupted, tampered, or
 * wrapped under another subject's context), an unwrapped key that is not a 256-bit key, and a GCM
 * tag failure are refused after it. All of them are {@link CryptoMappingException}: no retry, key
 * change or configuration fix can heal them, so the read-path classifiers quarantine the one blob
 * instead of treating it as a KMS outage and retrying forever. So is a value too large for the
 * envelope ({@link #MAX_PLAINTEXT_LENGTH} bytes, the Java array limit less the envelope), refused
 * on encrypt before any KMS call. Key-level and service-level failures (above; throttling, internal
 * errors, an {@code IncorrectKeyException} from a wrong {@code kms-key-id}) stay the bare {@link
 * CryptoOperationException}.
 *
 * <p><b>Encrypt re-checks the tombstone before it returns.</b> The pre-check alone leaves a TOCTOU
 * window in which a concurrent erasure completes in full — tombstone committed, purgers run, the
 * data subject told "erased" — while this encrypt is between its pre-check and its result. Because
 * {@link #deleteKey(SubjectId)} never touches the shared CMK, the ciphertext such an encrypt would
 * return stays fully decryptable by anyone holding {@code kms:Decrypt} plus DB read, and no
 * rebuild, sweep or audit surfaces it ({@link #decrypt(SubjectId, byte[])} short-circuits on the
 * tombstone). {@link #encrypt(SubjectId, byte[])} therefore re-checks {@code isForgotten} after the
 * value is encrypted and throws {@link SubjectForgottenException} instead of returning the bytes,
 * and does not cache a data key it generated for that encrypt — the same guard the vault and
 * filesystem engines carry, minus their cleanup leg (this engine creates no durable key material).
 * Sound because {@code deleteKey}'s only durable step is the tombstone commit, which completes
 * before it returns.
 */
public final class AwsKmsCryptoEngine implements CryptoEngine {

  /** Default number of data keys the engine caches for encryption, and as many for decryption. */
  public static final int DEFAULT_DATA_KEY_CACHE_MAXIMUM_SIZE = 1_000;

  /** Default lifetime of a cached data key, counted from when it was stored. */
  public static final Duration DEFAULT_DATA_KEY_CACHE_EXPIRE_AFTER_WRITE = Duration.ofMinutes(5);

  /** Values one cached data key encrypts before the engine asks KMS for a new one. */
  public static final long MAX_ENCRYPTIONS_PER_DATA_KEY = 1L << 24;

  private static final byte[] MAGIC = {'S', 'R', 'K', 'C'};

  /** The envelope version this engine writes and reads. */
  static final byte FORMAT_VERSION = 2;

  /** Magic, version byte and the two-byte wrapped-key length. */
  private static final int FIXED_HEADER_LENGTH = MAGIC.length + 1 + 2;

  private static final int MAX_WRAPPED_KEY_LENGTH = 0xFFFF;
  private static final int NONCE_LENGTH = 12;
  private static final int TAG_LENGTH = 16;
  private static final int DATA_KEY_LENGTH = 32;

  /** The longest Java array the JVM reliably allocates. */
  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

  /**
   * The longest value the envelope can hold: one Java array carries the header, the longest wrapped
   * key, the nonce, the ciphertext and the tag. A longer value is refused on encrypt with {@link
   * CryptoMappingException}, before any KMS call.
   */
  public static final int MAX_PLAINTEXT_LENGTH =
      MAX_ARRAY_LENGTH - FIXED_HEADER_LENGTH - MAX_WRAPPED_KEY_LENGTH - NONCE_LENGTH - TAG_LENGTH;

  /**
   * EncryptionContext key carrying the subject hash — recorded (hashed, never raw) in CloudTrail.
   */
  private static final String SUBJECT_CONTEXT_KEY = "streamrune-subject";

  private static final String ALGORITHM = "AES/GCM/NoPadding";

  // Cipher.getInstance() does an SPI provider lookup that costs more than the encryption of a
  // short value, so the cipher is reused per thread, as in the PostgreSQL and filesystem engines.
  // Cipher is not thread-safe, hence ThreadLocal; init() fully re-initializes it on each use.
  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(
          () -> {
            try {
              return Cipher.getInstance(ALGORITHM);
            } catch (GeneralSecurityException e) {
              throw new CryptoOperationException("AES/GCM cipher unavailable", e);
            }
          });

  private final KmsClient kmsClient;
  private final String kmsKeyId;
  private final ForgottenSubjectStore forgottenSubjects;
  private final DataKeyCache dataKeys;
  private final SecureRandom random = new SecureRandom();

  private final int dataKeyCacheMaximumSize;
  private final Duration dataKeyCacheExpireAfterWrite;

  private AwsKmsCryptoEngine(Builder builder) {
    this.kmsClient = builder.kmsClient;
    this.kmsKeyId = builder.kmsKeyId;
    this.forgottenSubjects = builder.forgottenSubjects;
    this.dataKeyCacheMaximumSize = builder.dataKeyCacheMaximumSize;
    this.dataKeyCacheExpireAfterWrite = builder.dataKeyCacheExpireAfterWrite;
    this.dataKeys =
        new DataKeyCache(
            builder.dataKeyCacheMaximumSize,
            builder.dataKeyCacheExpireAfterWrite.toNanos(),
            builder.maxEncryptionsPerDataKey,
            builder.nanoClock);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * The configured size of each data-key map; {@code 0} when the data-key cache is off.
   *
   * @return the number of entries each map holds at most
   */
  public int dataKeyCacheMaximumSize() {
    return dataKeyCacheMaximumSize;
  }

  /**
   * How long a cached data key lives after it was stored.
   *
   * @return the configured lifetime
   */
  public Duration dataKeyCacheExpireAfterWrite() {
    return dataKeyCacheExpireAfterWrite;
  }

  /** Whether this engine's cache holds a data key of the subject; for tests. */
  boolean holdsDataKeysOf(SubjectId subjectId) {
    return dataKeys.holds(subjectId);
  }

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    checkPlaintextLength(plaintext.length);
    // Read before the tombstone: a forget that evicts after this read makes the store below a
    // no-op, so a data key generated while that forget ran is never cached after its eviction.
    long epoch = dataKeys.epoch();
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Terminal erasure: refuse to re-encrypt for a crypto-shredded subject, matching the
      // filesystem/PostgreSQL backends. Lift via reinstate(subjectId) for legitimate
      // re-registration.
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    DataKeyCache.EncryptionKey dataKey = dataKeys.takeEncryptionKey(subjectId);
    boolean generated = dataKey == null;
    if (generated) {
      dataKey = generateDataKey(subjectId);
    }
    byte[] ciphertext = seal(subjectId, dataKey, plaintext);
    // Re-check the tombstone AFTER the data key was obtained and the value encrypted, before the
    // ciphertext is handed to the caller. The pre-check alone leaves a TOCTOU window: a
    // ForgetSubjectService running concurrently can complete the ENTIRE erasure — tombstone
    // committed, purgers run, the data subject told "erased", ForgetResult.fullyErased() true —
    // between that pre-check and this line. Returning here would hand back a brand-new at-rest
    // ciphertext of that subject's personal data, written after the Article-17 erasure was
    // reported complete.
    //
    // This backend needs the check MORE than its siblings, not less: deleteKey never touches the
    // shared CMK, so unlike vault (transit key deleted) and filesystem/postgres (per-subject key
    // destroyed) the raced ciphertext stays FULLY DECRYPTABLE by anyone holding kms:Decrypt plus
    // DB read — and it is invisible to every remediation path, because decrypt short-circuits on
    // the tombstone and answers [REDACTED] without ever looking at it.
    //
    // Soundness: deleteKey's ONLY durable step is the tombstone commit, and it completes before
    // deleteKey returns — so an erasure that finished before this line necessarily has its
    // tombstone visible to this read, while an encrypt whose post-check ran before that commit
    // completed entirely before the erasure. There is no cleanup leg: the data key exists only in
    // this ciphertext, which is discarded, and it is not cached. Residual, shared with every
    // backend: the CALLER persists the returned ciphertext, so an erasure committing between this
    // check and that write is still not caught here — ForgetSubjectService is idempotent and a
    // re-run converges.
    if (forgottenSubjects.isForgotten(subjectId)) {
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) while this encrypt was in flight — the"
              + " ciphertext is discarded and never returned: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    if (generated) {
      dataKeys.storeEncryptionKey(subjectId, dataKey, epoch);
    }
    return ciphertext;
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (ciphertext == null) {
      throw new IllegalArgumentException("ciphertext must not be null");
    }
    long epoch = dataKeys.epoch();
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Crypto-shredded subject: old ciphertext is unreadable. KeyNotFoundException is mapped to
      // the [REDACTED] tombstone by CryptoShreddingModule, so replay/read models surface no PII.
      // This read precedes every cache lookup, so a data key still cached here or on any other
      // replica is never used for a forgotten subject.
      throw new KeyNotFoundException(
          "AWS KMS subject was crypto-shredded (GDPR erasure): " + subjectId.redacted());
    }
    int wrappedLength = envelopeWrappedKeyLength(ciphertext);
    byte[] wrapped =
        Arrays.copyOfRange(ciphertext, FIXED_HEADER_LENGTH, FIXED_HEADER_LENGTH + wrappedLength);
    SecretKey key = dataKeys.unwrappedKey(subjectId, wrapped);
    boolean unwrapped = key == null;
    if (unwrapped) {
      key = unwrapDataKey(subjectId, wrapped);
    }
    byte[] plaintext = open(subjectId, key, ciphertext, FIXED_HEADER_LENGTH + wrappedLength);
    // Cached only once the tag proved the key opens this subject's value.
    if (unwrapped) {
      dataKeys.storeUnwrappedKey(subjectId, wrapped, key, epoch);
    }
    return plaintext;
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
    // The tombstone already stops every use of the subject's data keys on every replica; this
    // drops this replica's copies from heap. Other replicas drop theirs when the entries expire.
    dataKeys.evict(subjectId);
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
      // Advisory path, same taxonomy as decrypt. A NotFoundException from DescribeKey is about the
      // CONFIGURED SHARED CMK, not about this subject — a lawfully erased subject already returned
      // false on its tombstone before any KMS call. Answering false here reports EVERY subject's
      // key as absent, and "no key" is documented as readable as "already crypto-shredded"
      // (VaultCryptoEngine: "a live key must never be misreported as absent").
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
   * Refuses a value whose envelope cannot exist as one Java array. Deterministic: the same value
   * fails the same way on every attempt, so it is the type the classifiers never retry.
   */
  static void checkPlaintextLength(long length) {
    if (length > MAX_PLAINTEXT_LENGTH) {
      throw new CryptoMappingException(
          "Value of "
              + length
              + " bytes exceeds the AWS KMS engine's envelope limit of "
              + MAX_PLAINTEXT_LENGTH
              + " bytes (deterministic, not retried)");
    }
  }

  /** A new data key from KMS GenerateDataKey, bound to the subject's encryption context. */
  private DataKeyCache.EncryptionKey generateDataKey(SubjectId subjectId) {
    byte[] wrapped;
    byte[] material;
    try {
      GenerateDataKeyResponse response =
          kmsClient.generateDataKey(
              GenerateDataKeyRequest.builder()
                  .keyId(kmsKeyId)
                  .keySpec(DataKeySpec.AES_256)
                  .encryptionContext(subjectContext(subjectId))
                  .build());
      wrapped = response.ciphertextBlob().asByteArray();
      material = response.plaintext().asByteArray();
    } catch (RuntimeException e) {
      throw kmsFailure("GenerateDataKey", subjectId, e);
    }
    try {
      if (wrapped.length == 0 || wrapped.length > MAX_WRAPPED_KEY_LENGTH) {
        throw new CryptoOperationException(
            "AWS KMS GenerateDataKey returned a wrapped data key of "
                + wrapped.length
                + " bytes for subject: "
                + subjectId.redacted()
                + " — the envelope holds 1 to "
                + MAX_WRAPPED_KEY_LENGTH);
      }
      if (material.length != DATA_KEY_LENGTH) {
        throw new CryptoOperationException(
            "AWS KMS GenerateDataKey returned a "
                + material.length
                + "-byte data key for subject: "
                + subjectId.redacted()
                + " — expected an AES-256 key");
      }
      return new DataKeyCache.EncryptionKey(
          new SecretKeySpec(material, "AES"), wrapped, dataKeys.now());
    } finally {
      Arrays.fill(material, (byte) 0);
    }
  }

  /** The data key KMS unwraps from {@code wrapped} under the subject's encryption context. */
  private SecretKey unwrapDataKey(SubjectId subjectId, byte[] wrapped) {
    byte[] material;
    try {
      material =
          kmsClient
              .decrypt(
                  DecryptRequest.builder()
                      .ciphertextBlob(SdkBytes.fromByteArray(wrapped))
                      .keyId(kmsKeyId)
                      .encryptionContext(subjectContext(subjectId))
                      .build())
              .plaintext()
              .asByteArray();
    } catch (RuntimeException e) {
      throw kmsFailure("Decrypt", subjectId, e);
    }
    try {
      if (material.length != DATA_KEY_LENGTH) {
        // Only a producer holding kms:Encrypt on the CMK with this subject's context can have made
        // a wrapped blob that unwraps to anything but a data key; no retry changes it.
        throw new CryptoMappingException(
            "AWS KMS unwrapped a "
                + material.length
                + "-byte value instead of an AES-256 data key for subject: "
                + subjectId.redacted()
                + " — the ciphertext was not written by this engine (deterministic, not retried)");
      }
      return new SecretKeySpec(material, "AES");
    } finally {
      Arrays.fill(material, (byte) 0);
    }
  }

  /**
   * Maps a KMS failure. Only {@code InvalidCiphertextException} — KMS's verdict on the wrapped data
   * key itself — is deterministic; every key-level and service-level failure stays bare, because it
   * is reversible or transient.
   */
  private CryptoOperationException kmsFailure(
      String operation, SubjectId subjectId, RuntimeException e) {
    if (e instanceof InvalidCiphertextException) {
      // Corrupted, tampered, wrapped under another subject's encryption context, or material KMS
      // can no longer verify. Retrying can never succeed, so raise the CryptoMappingException
      // subtype the read-path classifiers treat as poison; the bare type reads to them as a KMS
      // OUTAGE and is retried forever.
      return new CryptoMappingException(
          "AWS KMS rejected the wrapped data key for subject: "
              + subjectId.redacted()
              + " (InvalidCiphertextException — corrupted, tampered, or bound to a different"
              + " encryption context; deterministic, retrying cannot succeed)",
          e);
    }
    if (e instanceof NotFoundException) {
      // A missing CMK is an INFRASTRUCTURE/CONFIGURATION fault, never a crypto-shred.
      // KMS raises NotFoundException identically for a mistyped kms-key-id, a key ARN from another
      // region/account, a key this principal has lost visibility on (SCP or key-policy change),
      // and a genuinely destroyed CMK — the SDK gives us nothing to tell them apart. And on this
      // backend a LAWFUL erasure can never reach KMS at all: deleteKey only records a per-subject
      // tombstone (the shared CMK is never touched) and both encrypt and decrypt short-circuit on
      // it before any KMS call. So every reachable occurrence is an operational fault, and mapping
      // it to KeyNotFoundException — the one type CryptoShreddingModule turns into the [REDACTED]
      // tombstone — would false-redact EVERY subject at once under the shared CMK: a projection
      // rebuild / saga load / DLQ replay running in that window writes those tombstones into read
      // models and reports SUCCESS, and fixing the key id afterwards does NOT heal them.
      //
      // Deliberately NOT offered: an opt-in "treat a missing CMK as a fleet-wide erasure" flag.
      // It would serve zero cases this branch can identify (the fault is indistinguishable from a
      // typo) while re-arming the silent mass-redaction. Destroying the CMK as a bulk erasure is an
      // out-of-band operator act whose in-framework expression is per-subject tombstones via
      // deleteKey/ForgetSubjectService, which short-circuit before KMS and are auditable.
      return new CryptoOperationException(
          "AWS KMS key not found or not accessible (infrastructure/configuration fault — NOT a"
              + " crypto-shred): "
              + kmsKeyId
              + " — check streamrune.crypto.aws.kms-key-id (typo, wrong region/account) and this"
              + " principal's kms:GenerateDataKey and kms:Decrypt grants. The engine fails closed"
              + " rather than redacting every subject: a lawful GDPR erasure never reaches AWS KMS"
              + " on this backend (it is a per-subject tombstone), so this can only be an"
              + " operational fault. If the CMK really was destroyed, record the affected"
              + " subjects via deleteKey / ForgetSubjectService so the erasure is explicit and"
              + " auditable.",
          e);
    }
    if (e instanceof DisabledException || e instanceof KmsInvalidStateException) {
      // A DISABLED CMK or a KmsInvalidState (pending import, unavailable multi-Region replica,
      // disconnected XKS store, pending deletion) is REVERSIBLE — re-enabling / cancelling deletion
      // restores every subject. It must NOT be mapped to KeyNotFoundException: that would make
      // CryptoShreddingModule substitute [REDACTED] for EVERY subject (one shared CMK), and
      // re-enabling the key would NOT heal the rows written in that window. Fail closed instead: a
      // CryptoOperationException propagates, so replay blocks and retries until the key is usable
      // again — matching the Vault engine's fail-closed taxonomy.
      return new CryptoOperationException(
          "AWS KMS key is disabled or in an invalid/transient state (reversible — NOT a"
              + " crypto-shred): "
              + kmsKeyId,
          e);
    }
    return new CryptoOperationException(
        "AWS KMS " + operation + " failed for subject: " + subjectId.redacted(), e);
  }

  /** Encrypts {@code plaintext} into a version-2 envelope carrying the wrapped data key. */
  private byte[] seal(SubjectId subjectId, DataKeyCache.EncryptionKey dataKey, byte[] plaintext) {
    byte[] wrapped = dataKey.wrapped();
    int headerLength = FIXED_HEADER_LENGTH + wrapped.length;
    byte[] result = new byte[headerLength + NONCE_LENGTH + plaintext.length + TAG_LENGTH];
    System.arraycopy(MAGIC, 0, result, 0, MAGIC.length);
    result[MAGIC.length] = FORMAT_VERSION;
    result[MAGIC.length + 1] = (byte) (wrapped.length >>> 8);
    result[MAGIC.length + 2] = (byte) wrapped.length;
    System.arraycopy(wrapped, 0, result, FIXED_HEADER_LENGTH, wrapped.length);
    byte[] nonce = new byte[NONCE_LENGTH];
    random.nextBytes(nonce);
    System.arraycopy(nonce, 0, result, headerLength, NONCE_LENGTH);
    try {
      Cipher cipher = CIPHER.get();
      cipher.init(Cipher.ENCRYPT_MODE, dataKey.key(), new GCMParameterSpec(TAG_LENGTH * 8, nonce));
      cipher.updateAAD(result, 0, headerLength);
      cipher.updateAAD(subjectBinding(subjectId));
      cipher.doFinal(plaintext, 0, plaintext.length, result, headerLength + NONCE_LENGTH);
      return result;
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException(
          "AES-GCM encryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  /** Decrypts the envelope's value under {@code key}; {@code headerLength} precedes the nonce. */
  private byte[] open(SubjectId subjectId, SecretKey key, byte[] ciphertext, int headerLength) {
    try {
      Cipher cipher = CIPHER.get();
      cipher.init(
          Cipher.DECRYPT_MODE,
          key,
          new GCMParameterSpec(TAG_LENGTH * 8, ciphertext, headerLength, NONCE_LENGTH));
      cipher.updateAAD(ciphertext, 0, headerLength);
      cipher.updateAAD(subjectBinding(subjectId));
      int bodyStart = headerLength + NONCE_LENGTH;
      return cipher.doFinal(ciphertext, bodyStart, ciphertext.length - bodyStart);
    } catch (AEADBadTagException e) {
      // Deterministic: the value or its header was altered, or it belongs to another subject.
      throw new CryptoMappingException(
          "AES-GCM tag mismatch for subject: "
              + subjectId.redacted()
              + " — the ciphertext is corrupted, tampered, or belongs to another subject"
              + " (deterministic, retrying cannot succeed)",
          e);
    } catch (GeneralSecurityException e) {
      throw new CryptoOperationException(
          "AES-GCM decryption failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Validates the envelope's header and returns the wrapped-key length. Every refusal is a property
   * of the blob, decided before any KMS call, so it is the deterministic {@link
   * CryptoMappingException}: a blob without the magic was not written by this engine — decrypting
   * it would let a ciphertext produced under the shared CMK by any other producer reach KMS under
   * any subject id.
   */
  private static int envelopeWrappedKeyLength(byte[] blob) {
    if (blob.length < FIXED_HEADER_LENGTH || !hasMagic(blob)) {
      throw new CryptoMappingException(
          "Ciphertext is missing the StreamRune AWS KMS envelope — not written by this engine"
              + " (deterministic, not retried)");
    }
    byte version = blob[MAGIC.length];
    if (version != FORMAT_VERSION) {
      throw new CryptoMappingException(
          "Unsupported AWS KMS ciphertext envelope version "
              + version
              + " — this engine reads and writes version "
              + FORMAT_VERSION
              + " (deterministic, not retried)");
    }
    int wrappedLength = ((blob[MAGIC.length + 1] & 0xFF) << 8) | (blob[MAGIC.length + 2] & 0xFF);
    if (wrappedLength == 0
        || blob.length < FIXED_HEADER_LENGTH + wrappedLength + NONCE_LENGTH + TAG_LENGTH) {
      throw new CryptoMappingException(
          "AWS KMS ciphertext envelope is truncated or declares a wrapped data key of "
              + wrappedLength
              + " bytes that it does not hold (deterministic, not retried)");
    }
    return wrappedLength;
  }

  private static boolean hasMagic(byte[] blob) {
    for (int i = 0; i < MAGIC.length; i++) {
      if (blob[i] != MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * The subject-binding EncryptionContext: {@code {"streamrune-subject": sha256hex(subjectId)}}.
   */
  private static Map<String, String> subjectContext(SubjectId subjectId) {
    return Map.of(SUBJECT_CONTEXT_KEY, subjectId.redacted());
  }

  /** The subject part of the GCM associated data: the UTF-8 bytes of the subject hash. */
  private static byte[] subjectBinding(SubjectId subjectId) {
    return subjectId.redacted().getBytes(StandardCharsets.UTF_8);
  }

  public static final class Builder {

    private KmsClient kmsClient;
    private String region;
    private String kmsKeyId;
    // No silent in-memory default — build() fails closed unless a store is supplied, so KMS
    // erasure can never be silently non-durable (which would resurrect erased PII on restart).
    private ForgottenSubjectStore forgottenSubjects = null;
    private int dataKeyCacheMaximumSize = DEFAULT_DATA_KEY_CACHE_MAXIMUM_SIZE;
    private Duration dataKeyCacheExpireAfterWrite = DEFAULT_DATA_KEY_CACHE_EXPIRE_AFTER_WRITE;
    private long maxEncryptionsPerDataKey = MAX_ENCRYPTIONS_PER_DATA_KEY;
    private LongSupplier nanoClock = System::nanoTime;

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

    /**
     * Sets how many data keys the engine caches, for encryption (one per subject) and, separately,
     * for decryption (one per subject and data key), default {@value
     * AwsKmsCryptoEngine#DEFAULT_DATA_KEY_CACHE_MAXIMUM_SIZE} each. {@code 0} disables the cache:
     * every value then costs one KMS call and carries its own data key.
     *
     * @param maximumSize the number of entries of each map, at least 0
     */
    public Builder dataKeyCacheMaximumSize(int maximumSize) {
      if (maximumSize < 0) {
        throw new IllegalArgumentException("dataKeyCacheMaximumSize must not be negative");
      }
      this.dataKeyCacheMaximumSize = maximumSize;
      return this;
    }

    /**
     * Sets how long a cached data key lives after it was stored, however often it is used (default
     * 5 minutes). It bounds how long a plaintext data key stays in this process's heap, and how
     * long a replica that did not run a forget keeps the forgotten subject's keys in heap — never
     * whether it uses them, which the tombstone decides on every call.
     *
     * @param expireAfterWrite a positive duration
     */
    public Builder dataKeyCacheExpireAfterWrite(Duration expireAfterWrite) {
      if (expireAfterWrite == null || expireAfterWrite.isZero() || expireAfterWrite.isNegative()) {
        throw new IllegalArgumentException("dataKeyCacheExpireAfterWrite must be positive");
      }
      this.dataKeyCacheExpireAfterWrite = expireAfterWrite;
      return this;
    }

    /** Lowers the per-data-key encryption budget; for tests. */
    Builder maxEncryptionsPerDataKey(long maxEncryptions) {
      if (maxEncryptions < 1) {
        throw new IllegalArgumentException("maxEncryptionsPerDataKey must be at least 1");
      }
      this.maxEncryptionsPerDataKey = maxEncryptions;
      return this;
    }

    /** Replaces the cache's nanosecond clock; for tests. */
    Builder nanoClock(LongSupplier nanoClock) {
      if (nanoClock == null) {
        throw new IllegalArgumentException("nanoClock must not be null");
      }
      this.nanoClock = nanoClock;
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

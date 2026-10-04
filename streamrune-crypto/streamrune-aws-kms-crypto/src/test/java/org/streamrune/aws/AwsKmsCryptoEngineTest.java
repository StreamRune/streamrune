package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsInternalException;

class AwsKmsCryptoEngineTest {

  private KmsClient kmsClient;
  private AwsKmsCryptoEngine engine;

  /**
   * {@code kmsBlob} inside this engine's {@code [magic 'S' 'R' 'K' 'C'][version 1]} envelope — the
   * shape every ciphertext the engine returns has, and the only shape decrypt passes to KMS.
   */
  private static byte[] enveloped(byte[] kmsBlob) {
    byte[] blob = new byte[5 + kmsBlob.length];
    blob[0] = 'S';
    blob[1] = 'R';
    blob[2] = 'K';
    blob[3] = 'C';
    blob[4] = 1;
    System.arraycopy(kmsBlob, 0, blob, 5, kmsBlob.length);
    return blob;
  }

  @BeforeEach
  void setUp() {
    kmsClient = mock(KmsClient.class);
    engine =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .region("eu-central-1")
            .kmsKeyId("test-key-id")
            // The engine fails closed without a store; tests opt into the in-memory one.
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
  }

  @Test
  void encryptCallsKmsEncrypt() {
    byte[] plaintext = "secret".getBytes();
    byte[] ciphertext = "encrypted".getBytes();
    EncryptResponse response = mock(EncryptResponse.class);
    when(response.ciphertextBlob()).thenReturn(SdkBytes.fromByteArray(ciphertext));
    when(kmsClient.encrypt(any(EncryptRequest.class))).thenReturn(response);

    byte[] result = engine.encrypt(SubjectId.of("subject-1"), plaintext);

    // The returned blob is [magic 'S''R''K''C'][version 1][raw KMS ciphertext].
    assertEquals("SRKC", new String(result, 0, 4, java.nio.charset.StandardCharsets.US_ASCII));
    assertEquals(1, result[4]);
    assertEquals("encrypted", new String(result, 5, result.length - 5));
    verify(kmsClient).encrypt(any(EncryptRequest.class));
  }

  @Test
  void encryptBindsCiphertextToSubjectViaEncryptionContext() {
    // Encrypt must pass an EncryptionContext of {streamrune-subject: sha256(subjectId)}
    // so KMS cryptographically binds the ciphertext to its subject — and CloudTrail records only
    // the subject hash, never the raw id.
    EncryptResponse response = mock(EncryptResponse.class);
    when(response.ciphertextBlob()).thenReturn(SdkBytes.fromByteArray("ct".getBytes()));
    var captor = org.mockito.ArgumentCaptor.forClass(EncryptRequest.class);
    when(kmsClient.encrypt(captor.capture())).thenReturn(response);

    var subject = SubjectId.of("alice@example.com");
    engine.encrypt(subject, "pii".getBytes());

    var ctx = captor.getValue().encryptionContext();
    assertEquals(
        subject.redacted(),
        ctx.get("streamrune-subject"),
        "EncryptionContext must bind the subject hash");
    assertFalse(
        ctx.toString().contains("alice@example.com"),
        "EncryptionContext must not carry the raw subject id");
  }

  @Test
  void decryptOfVersionedBlobSuppliesMatchingSubjectContext() {
    // Decrypting an engine-written (versioned) blob must strip the envelope and
    // re-supply the SAME subject EncryptionContext, so KMS enforces the subject binding.
    DecryptResponse response = mock(DecryptResponse.class);
    when(response.plaintext()).thenReturn(SdkBytes.fromByteArray("pii".getBytes()));
    var captor = org.mockito.ArgumentCaptor.forClass(DecryptRequest.class);
    when(kmsClient.decrypt(captor.capture())).thenReturn(response);

    var subject = SubjectId.of("bob@example.com");
    byte[] versioned = new byte[] {'S', 'R', 'K', 'C', 1, 'r', 'a', 'w'};
    engine.decrypt(subject, versioned);

    var req = captor.getValue();
    assertEquals(subject.redacted(), req.encryptionContext().get("streamrune-subject"));
    assertArrayEquals(
        "raw".getBytes(),
        req.ciphertextBlob().asByteArray(),
        "the KMS blob passed on must be the bytes after the 5-byte envelope");
  }

  @Test
  void decryptRefusesABlobWithoutTheEnvelope_beforeAnyKmsCall() {
    // Every ciphertext this engine returns carries the envelope, and decrypt always
    // re-supplies the subject EncryptionContext. A blob without the envelope was not written by
    // this engine — decrypting it without a context would let a context-less ciphertext produced
    // under the shared CMK by any other producer decrypt under ANY subject id. It is refused before
    // KMS is called, as the deterministic CryptoMappingException: retrying cannot help.
    var subject = SubjectId.of("subject-no-envelope");
    // A raw KMS ciphertext (it starts with a KMS version byte, never the 'S' magic).
    assertThrows(
        CryptoMappingException.class, () -> engine.decrypt(subject, new byte[] {0x01, 0x02, 0x03}));
    // Too short to hold the 5-byte envelope, even though it starts with the magic.
    assertThrows(
        CryptoMappingException.class, () -> engine.decrypt(subject, new byte[] {'S', 'R', 'K'}));
    assertThrows(
        CryptoMappingException.class,
        () -> engine.decrypt(subject, new byte[] {'S', 'R', 'K', 'C'}));
    assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, new byte[0]));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  @Test
  void decryptRejectsUnknownEnvelopeVersion() {
    // Forward-compat: a blob carrying the magic but an unknown version (e.g. written by a
    // newer StreamRune) must fail loudly, never be silently mis-decrypted. The version guard fires
    // before any KMS call. It is a DETERMINISTIC blob-property failure, so it is raised as
    // the CryptoMappingException subtype — a bare CryptoOperationException reads as a KMS outage to
    // the read-path classifiers and is retried forever.
    byte[] futureBlob = new byte[] {'S', 'R', 'K', 'C', (byte) 2, 'x'};
    assertThrows(
        CryptoMappingException.class, () -> engine.decrypt(SubjectId.of("subject-v2"), futureBlob));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  @Test
  void decryptRaisesTheDeterministicSubtypeOnInvalidCiphertext() {
    // KMS's verdict on the blob itself (corrupted, tampered, or bound to a different
    // encryption context) can never be healed by a retry — the CryptoMappingException subtype.
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(InvalidCiphertextException.builder().message("ciphertext is invalid").build());

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("subject-bad"), enveloped("garbage".getBytes())));
    assertInstanceOf(InvalidCiphertextException.class, thrown.getCause());
  }

  @Test
  void decryptKeepsServiceLevelFailuresBare() {
    // A throttled or internally failing KMS is an OUTAGE: the bare CryptoOperationException, so the
    // read path blocks and retries instead of quarantining a healthy blob.
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(KmsInternalException.builder().message("internal error").build());

    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-outage"), enveloped("garbage".getBytes())));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
  }

  @Test
  void decryptCallsKmsDecrypt() {
    byte[] ciphertext = "encrypted".getBytes();
    byte[] plaintext = "secret".getBytes();
    DecryptResponse response = mock(DecryptResponse.class);
    when(response.plaintext()).thenReturn(SdkBytes.fromByteArray(plaintext));
    when(kmsClient.decrypt(any(DecryptRequest.class))).thenReturn(response);

    byte[] result = engine.decrypt(SubjectId.of("subject-2"), enveloped(ciphertext));

    assertEquals("secret", new String(result));
    verify(kmsClient).decrypt(any(DecryptRequest.class));
  }

  @Test
  void isKeyAvailableReturnsTrueWhenKeyEnabled() {
    DescribeKeyResponse response = mock(DescribeKeyResponse.class);
    KeyMetadata metadata = mock(KeyMetadata.class);
    when(metadata.keyStateAsString()).thenReturn("Enabled");
    when(response.keyMetadata()).thenReturn(metadata);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(response);

    assertTrue(engine.isKeyAvailable(SubjectId.of("subject-3")));
  }

  // `isKeyAvailableReturnsFalseWhenKeyNotFound` used to pin the opposite (fail-open)
  // behaviour here. It is replaced by
  // isKeyAvailableOnMissingCmkFailsClosed_neverReportsEverySubjectAsErased below — a shared-CMK
  // DescribeKey NotFoundException says nothing about the SUBJECT, and answering `false` reads as
  // "already crypto-shredded" for every one of them.

  @Test
  void encryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, "data".getBytes()));
  }

  @Test
  void encryptRejectsNullPlaintext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("subject"), null));
  }

  @Test
  void builderSetsAllFields() {
    var e =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .region("us-east-1")
            .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/1234abcd")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
    assertNotNull(e);
  }

  // deleteKey records a per-subject tombstone (shared CMK has no per-subject key to
  // hard-delete) so the auto-wired ForgetSubjectService completes instead of aborting.

  @Test
  void deleteKeyTombstonesSubject_soDecryptYieldsRedacted() {
    var subject = SubjectId.of("subject-4");
    engine.deleteKey(subject);

    // Decrypt now short-circuits with KeyNotFoundException (mapped to [REDACTED]) without ever
    // calling KMS — the shared CMK is intact but this subject is crypto-shredded.
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, "old-ct".getBytes()));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  @Test
  void encryptAfterDeleteKeyThrowsSubjectForgotten() {
    var subject = SubjectId.of("subject-4b");
    engine.deleteKey(subject);

    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "pii".getBytes()));
    verify(kmsClient, never()).encrypt(any(EncryptRequest.class));
  }

  @Test
  void isKeyAvailableReturnsFalseAfterDeleteKey() {
    var subject = SubjectId.of("subject-4c");
    engine.deleteKey(subject);

    assertFalse(engine.isKeyAvailable(subject));
    verify(kmsClient, never()).describeKey(any(DescribeKeyRequest.class));
  }

  @Test
  void forgottenSubjectExceptionMessagesRedactTheRawSubjectId() {
    // A subject id may itself be PII (an email). The SubjectForgottenException (encrypt)
    // and KeyNotFoundException (decrypt) messages must carry only the SHA-256 hash, never the raw
    // value — those messages flow into logs and (for the encrypt failure) DLQ error_message.
    var subject = SubjectId.of("alice@example.com");
    engine.deleteKey(subject);

    var forgotten =
        assertThrows(
            org.streamrune.core.crypto.SubjectForgottenException.class,
            () -> engine.encrypt(subject, "pii".getBytes()));
    assertFalse(
        forgotten.getMessage().contains("alice@example.com"),
        "encrypt message must not leak the raw subject id: " + forgotten.getMessage());
    assertTrue(
        forgotten.getMessage().contains(subject.redacted()),
        "encrypt message must carry the subject hash");

    var notFound =
        assertThrows(
            KeyNotFoundException.class, () -> engine.decrypt(subject, "old-ct".getBytes()));
    assertFalse(
        notFound.getMessage().contains("alice@example.com"),
        "decrypt message must not leak the raw subject id: " + notFound.getMessage());
    assertTrue(
        notFound.getMessage().contains(subject.redacted()),
        "decrypt message must carry the subject hash");
  }

  @Test
  void deleteKeyIsIdempotentAndRejectsNull() {
    var subject = SubjectId.of("subject-4d");
    engine.deleteKey(subject);
    engine.deleteKey(subject); // no throw on repeat
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
  }

  @Test
  void reinstateLiftsTheTombstone() {
    var subject = SubjectId.of("subject-4e");
    engine.deleteKey(subject);
    // Re-registration is refused until the subject is explicitly reinstated.
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "pii".getBytes()));
    verify(kmsClient, never()).encrypt(any(EncryptRequest.class));

    engine.reinstate(subject);

    // Encryption is allowed again after reinstatement.
    EncryptResponse response = mock(EncryptResponse.class);
    when(response.ciphertextBlob()).thenReturn(SdkBytes.fromByteArray("ct".getBytes()));
    when(kmsClient.encrypt(any(EncryptRequest.class))).thenReturn(response);

    // Strip the [magic][version] envelope to recover the raw KMS ciphertext ("ct").
    byte[] out = engine.encrypt(subject, "pii".getBytes());
    assertEquals("ct", new String(out, 5, out.length - 5));
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
  }

  @Test
  void isKeyAvailableReturnsFalseWhenKeyDisabled() {
    DescribeKeyResponse response = mock(DescribeKeyResponse.class);
    KeyMetadata metadata = mock(KeyMetadata.class);
    when(metadata.keyStateAsString()).thenReturn("Disabled");
    when(response.keyMetadata()).thenReturn(metadata);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(response);

    assertFalse(engine.isKeyAvailable(SubjectId.of("subject-5")));
  }

  @Test
  void isKeyAvailableReturnsFalseWhenKeyPendingDeletion() {
    DescribeKeyResponse response = mock(DescribeKeyResponse.class);
    KeyMetadata metadata = mock(KeyMetadata.class);
    when(metadata.keyStateAsString()).thenReturn("PendingDeletion");
    when(response.keyMetadata()).thenReturn(metadata);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(response);

    assertFalse(engine.isKeyAvailable(SubjectId.of("subject-6")));
  }

  @Test
  void decryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, "data".getBytes()));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("subject"), null));
  }

  @Test
  void isKeyAvailableRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.isKeyAvailable(null));
  }

  @Test
  void encryptWrapsSdkExceptionInCryptoOperationException() {
    when(kmsClient.encrypt(any(EncryptRequest.class)))
        .thenThrow(new RuntimeException("KMS unavailable"));

    assertThrows(
        CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-7"), "data".getBytes()));
  }

  @Test
  void decryptWrapsSdkExceptionInCryptoOperationException() {
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(new RuntimeException("KMS unavailable"));

    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-8"), enveloped("data".getBytes())));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
    verify(kmsClient).decrypt(any(DecryptRequest.class));
  }

  @Test
  void decryptOnMissingCmkFailsClosed_soARebuildBlocksInsteadOfWritingTombstones() {
    // KMS answers NotFoundException for a MISTYPED key id, a key ARN from another
    // region/account, and a key whose kms:Decrypt grant an SCP/key-policy change revoked — exactly
    // the same exception it raises for a genuinely destroyed CMK. On this backend a LAWFUL erasure
    // can never reach the KMS call at all (deleteKey only writes a per-subject tombstone, and
    // decrypt short-circuits on it before touching KMS), so every reachable NotFoundException here
    // is an infrastructure/configuration fault. Mapping it to KeyNotFoundException made
    // CryptoShreddingModule substitute [REDACTED] for EVERY subject under the one shared CMK, and
    // a projection rebuild / saga load / DLQ replay running in that window would write those
    // tombstones into read models and report SUCCESS — fixing the key id afterwards does NOT heal
    // them. Fail closed, matching the Disabled/InvalidState branch and the Vault
    // engine's taxonomy.
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(
            software.amazon.awssdk.services.kms.model.NotFoundException.builder()
                .message("Key not found")
                .build());

    // Captured as RuntimeException so the assertion below can name the WRONG outcome explicitly:
    // KeyNotFoundException is the one type CryptoShreddingModule.decryptValue converts into the
    // [REDACTED] tombstone.
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> engine.decrypt(SubjectId.of("subject-10"), enveloped("x".getBytes())));
    assertFalse(
        thrown instanceof KeyNotFoundException,
        "a missing/inaccessible CMK must NOT be convertible to the [REDACTED] tombstone by"
            + " CryptoShreddingModule: "
            + thrown);
    assertInstanceOf(
        CryptoOperationException.class,
        thrown,
        "a missing/inaccessible CMK is an infrastructure fault and must propagate as a"
            + " CryptoOperationException so replay blocks and retries");
    assertTrue(
        thrown.getMessage().contains("test-key-id"),
        "the failure must name the configured key id so the misconfiguration is actionable: "
            + thrown.getMessage());
  }

  @Test
  void isKeyAvailableOnMissingCmkFailsClosed_neverReportsEverySubjectAsErased() {
    // A shared-CMK NotFoundException from DescribeKey
    // says nothing about THIS subject — a lawfully erased subject already short-circuited on its
    // tombstone above. Answering `false` reports every subject's key as absent, and callers are
    // documented to read "no key" as "already crypto-shredded" (VaultCryptoEngine's contract:
    // "a live key must never be misreported as absent"). CryptoEngine#isKeyAvailable declares
    // `@throws CryptoOperationException if the lookup fails`, so failing closed is in contract.
    when(kmsClient.describeKey(any(DescribeKeyRequest.class)))
        .thenThrow(
            software.amazon.awssdk.services.kms.model.NotFoundException.builder()
                .message("Key not found")
                .build());

    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.isKeyAvailable(SubjectId.of("subject-10b")));
    assertTrue(
        thrown.getMessage().contains("test-key-id"),
        "the failure must name the configured key id: " + thrown.getMessage());
  }

  @Test
  void encryptRacingDeleteKey_refusesAndReturnsNoCiphertext() {
    // Encrypt only pre-checks the tombstone. A concurrent ForgetSubjectService can
    // complete the whole erasure — tombstone committed, purgers run, the data subject told
    // "erased" — between that pre-check and the KMS call landing. The shared CMK is never touched
    // by deleteKey, so the ciphertext this encrypt would return is FULLY RECOVERABLE by anyone
    // holding kms:Decrypt plus DB read: new at-rest PII written after an Article-17 erasure was
    // reported complete, and invisible to every remediation path (decrypt short-circuits on the
    // tombstone and answers [REDACTED], so no rebuild, sweep or audit ever surfaces it).
    //
    // The Answer runs the erasure INSIDE the KMS call, i.e. exactly in the pre-check -> response
    // window, which is the interleaving the post-check must catch. Direct analogue of
    // VaultCryptoEngineTest.encryptRacingDeleteKey_neverReturnsCiphertextEvenWhenCleanupIsDenied
    // and FileSystemCryptoEngine's refuseMintIfErasedMidRace.
    // KMS is the ONLY backend where the raced ciphertext stays
    // decryptable, which is why it needs the post-check most.
    var subject = SubjectId.of("kms-encrypt-race");
    EncryptResponse response = mock(EncryptResponse.class);
    when(response.ciphertextBlob()).thenReturn(SdkBytes.fromByteArray("raced-ct".getBytes()));
    when(kmsClient.encrypt(any(EncryptRequest.class)))
        .thenAnswer(
            invocation -> {
              engine.deleteKey(subject); // the concurrent erasure completes mid-flight
              return response;
            });

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.SubjectForgottenException.class,
            () -> engine.encrypt(subject, "pii".getBytes()),
            "encrypt must NOT return the ciphertext KMS produced for a subject whose erasure"
                + " completed while the call was in flight");
    assertTrue(
        thrown.getMessage().contains(subject.redacted()),
        "the refusal must carry the subject hash, never the raw id: " + thrown.getMessage());
    assertFalse(
        thrown.getMessage().contains("kms-encrypt-race"),
        "the refusal must not leak the raw subject id: " + thrown.getMessage());
  }

  @Test
  void decryptOnDisabledKeyPropagatesCryptoOperationException_neverRedacts() {
    // A DISABLED CMK is a REVERSIBLE operator/SCP/compliance action — re-enabling it
    // restores every subject. It must NOT map to KeyNotFoundException, which CryptoShreddingModule
    // turns into the [REDACTED] tombstone: a projection rebuild / saga load / DLQ replay running in
    // the disabled window would then write [REDACTED] into read models as if the subject were
    // lawfully erased, and re-enabling the key would NOT heal those rows (silent, permanent
    // corruption across EVERY subject, since one shared CMK encrypts all of them). Fail closed:
    // propagate a CryptoOperationException so replay blocks and retries, matching the Vault engine.
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(
            software.amazon.awssdk.services.kms.model.DisabledException.builder()
                .message("Key is disabled")
                .build());

    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-11"), enveloped("x".getBytes())));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
    verify(kmsClient).decrypt(any(DecryptRequest.class));
  }

  @Test
  void decryptOnInvalidKeyStatePropagatesCryptoOperationException_neverRedacts() {
    // KmsInvalidStateException covers reversible/transient states (pending import, an
    // unavailable multi-Region replica, a disconnected XKS store, pending deletion — itself
    // recoverable via CancelKeyDeletion during the waiting period). Treating it as a definitive
    // KeyNotFound would false-redact every subject at once. Fail closed: propagate so recoverable
    // data is never silently turned into a tombstone.
    when(kmsClient.decrypt(any(DecryptRequest.class)))
        .thenThrow(
            software.amazon.awssdk.services.kms.model.KmsInvalidStateException.builder()
                .message("Key is pending deletion")
                .build());

    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-12"), enveloped("x".getBytes())));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
    verify(kmsClient).decrypt(any(DecryptRequest.class));
  }

  @Test
  void isKeyAvailableWrapsUnexpectedExceptionInCryptoOperationException() {
    when(kmsClient.describeKey(any(DescribeKeyRequest.class)))
        .thenThrow(new RuntimeException("unexpected"));

    assertThrows(
        CryptoOperationException.class, () -> engine.isKeyAvailable(SubjectId.of("subject-9")));
  }

  @Test
  void builderRejectsNullForgottenSubjectStore() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AwsKmsCryptoEngine.builder().kmsKeyId("k").forgottenSubjectStore(null));
  }

  @Test
  void requiredCryptoTables_reflectsTombstoneStoreDurability() {
    // An in-memory tombstone store (setUp) needs no crypto schema.
    assertTrue(engine.requiredCryptoTables().isEmpty());
    // A durable (DB-backed) tombstone store means forgotten_subjects must be provisioned/validated.
    var durable =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId("test-key-id")
            .forgottenSubjectStore(
                new org.streamrune.crypto.ForgottenSubjectStore() {
                  @Override
                  public void forget(SubjectId subjectId) {}

                  @Override
                  public boolean isForgotten(SubjectId subjectId) {
                    return false;
                  }

                  @Override
                  public void reinstate(SubjectId subjectId) {}

                  @Override
                  public boolean requiresDatabaseSchema() {
                    return true;
                  }
                })
            .build();
    assertEquals(java.util.Set.of("forgotten_subjects"), durable.requiredCryptoTables());
  }

  @Test
  void customForgottenSubjectStoreIsConsulted() {
    var store = new InMemoryForgottenSubjectStore();
    var subject = SubjectId.of("pre-forgotten");
    store.forget(subject);
    var e =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId("test-key-id")
            .forgottenSubjectStore(store)
            .build();

    // Engine consults the supplied store: a pre-tombstoned subject is redacted without hitting KMS.
    assertThrows(KeyNotFoundException.class, () -> e.decrypt(subject, "ct".getBytes()));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  @Test
  void buildFailsClosed_whenNoForgottenSubjectStoreConfigured() {
    // On the shared-CMK KMS backend, GDPR erasure is only as durable as the tombstone store.
    // Silently defaulting to a process-local in-memory store un-forgets erased subjects on restart
    // (PII resurrection). build() must instead FAIL CLOSED when no persistent store is supplied —
    // never construct an engine that records a tombstone which will be lost. Fail-first: before the
    // fix build() succeeds with a silent in-memory default.
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> AwsKmsCryptoEngine.builder().kmsClient(kmsClient).kmsKeyId("k").build());
    assertTrue(
        ex.getMessage().contains("ForgottenSubjectStore"),
        "the config error must name the missing ForgottenSubjectStore: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("durable GDPR erasure"),
        "the config error must explain durable GDPR erasure on AWS-KMS: " + ex.getMessage());
  }

  @Test
  void builderRequiresKmsKeyId() {
    assertThrows(
        IllegalStateException.class,
        () -> AwsKmsCryptoEngine.builder().kmsClient(kmsClient).build());
  }

  @Test
  void builderRequiresRegionWhenKmsClientNotProvided() {
    assertThrows(
        IllegalStateException.class,
        () -> AwsKmsCryptoEngine.builder().kmsKeyId("some-key").build());
  }

  @Test
  void builderRequiresRegionWhenKmsClientNotProvidedBlank() {
    // Covers the region.isBlank() branch in build()
    assertThrows(
        IllegalStateException.class,
        () -> AwsKmsCryptoEngine.builder().kmsKeyId("some-key").region("  ").build());
  }
}

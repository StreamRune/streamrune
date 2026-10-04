package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;

@Testcontainers
class AwsKmsCryptoEngineIntegrationTest {

  @Container
  static final LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.4"))
          .withServices(LocalStackContainer.Service.KMS);

  private static KmsClient kmsClient;
  private static String keyId;
  private static AwsKmsCryptoEngine engine;

  @BeforeAll
  static void init() {
    URI endpoint = LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.KMS);
    kmsClient =
        KmsClient.builder()
            .endpointOverride(endpoint)
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();

    CreateKeyResponse createKeyResponse = kmsClient.createKey(CreateKeyRequest.builder().build());
    keyId = createKeyResponse.keyMetadata().keyId();

    engine =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(keyId)
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
  }

  @Test
  void roundtrip_encrypt_decrypt() {
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = engine.encrypt(SubjectId.of("subject-1"), plaintext);
    assertNotNull(ciphertext);
    assertTrue(ciphertext.length > 0);
    byte[] decrypted = engine.decrypt(SubjectId.of("subject-1"), ciphertext);
    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  void encrypt_produces_different_ciphertext_each_call() {
    byte[] plaintext = "same data".getBytes(StandardCharsets.UTF_8);
    byte[] ct1 = engine.encrypt(SubjectId.of("subject-2"), plaintext);
    byte[] ct2 = engine.encrypt(SubjectId.of("subject-2"), plaintext);
    assertFalse(
        java.util.Arrays.equals(ct1, ct2), "KMS should produce different ciphertext each call");
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-2"), ct1));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-2"), ct2));
  }

  @Test
  void kms_key_is_enabled_and_available() {
    assertTrue(engine.isKeyAvailable(SubjectId.of("any-subject")));
  }

  @Test
  void ciphertext_is_bound_to_subject_kms_rejects_cross_subject_decrypt() {
    // KMS ciphertext must be cryptographically bound to its subject via an
    // EncryptionContext, so decrypting subjectA's ciphertext as subjectB is rejected by KMS itself
    // — not only by the in-process tombstone/policy check. Any bug that decoupled ciphertext from
    // its subject (an upcaster renaming the subjectId field, the chain, a wrong sibling
    // field) would otherwise let KMS happily decrypt a forgotten subject's PII.
    byte[] pii = "top secret".getBytes(StandardCharsets.UTF_8);
    byte[] ct = engine.encrypt(SubjectId.of("subject-A"), pii);

    assertThrows(
        CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("subject-B"), ct),
        "KMS must reject a subject/ciphertext mismatch via EncryptionContext, not decrypt it");

    // The rightful subject still decrypts.
    assertArrayEquals(pii, engine.decrypt(SubjectId.of("subject-A"), ct));
  }

  @Test
  void raw_kms_ciphertext_without_the_envelope_is_refused() {
    // A raw KMS ciphertext made under the SAME shared CMK but without this engine's
    // envelope and subject EncryptionContext must not decrypt under any subject id. It is refused
    // before KMS is called, as the deterministic CryptoMappingException.
    byte[] pii = "foreign data".getBytes(StandardCharsets.UTF_8);
    byte[] rawBlob =
        kmsClient
            .encrypt(
                EncryptRequest.builder()
                    .keyId(keyId)
                    .plaintext(SdkBytes.fromByteArray(pii))
                    .build())
            .ciphertextBlob()
            .asByteArray();

    assertThrows(
        CryptoMappingException.class, () -> engine.decrypt(SubjectId.of("subject-raw"), rawBlob));
  }

  /**
   * Against a REAL KMS implementation: a key id that does not resolve is an
   * infrastructure/configuration fault, so both the read path and the advisory probe must fail
   * CLOSED rather than answering "erased". This is exactly what a mistyped {@code
   * streamrune.crypto.aws.kms-key-id}, a cross-region/account ARN, or a revoked grant produces —
   * and with the previous mapping ({@code isKeyAvailable} → {@code false}, {@code decrypt} → {@code
   * KeyNotFoundException} → {@code [REDACTED]}) a projection rebuild running in that window
   * silently wrote tombstones for every subject under the shared CMK and reported success.
   */
  @Test
  void unknown_key_fails_closed_on_both_decrypt_and_isKeyAvailable() {
    var bogusEngine =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId("arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();

    assertThrows(
        CryptoOperationException.class,
        () -> bogusEngine.isKeyAvailable(SubjectId.of("any-subject")));

    RuntimeException decryptFailure =
        assertThrows(
            RuntimeException.class,
            // An enveloped blob, so the call reaches KMS and meets the unresolvable key id.
            () ->
                bogusEngine.decrypt(
                    SubjectId.of("any-subject"), new byte[] {'S', 'R', 'K', 'C', 1, 1, 2, 3, 4}));
    assertFalse(
        decryptFailure instanceof org.streamrune.core.crypto.KeyNotFoundException,
        "an unresolvable key id must never be convertible to the [REDACTED] tombstone: "
            + decryptFailure);
  }
}

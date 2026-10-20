package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
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
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException;

@Testcontainers
class AwsKmsCryptoEngineIntegrationTest {

  @Container
  static final LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.4"))
          .withServices(LocalStackContainer.Service.KMS);

  private static KmsClient kmsClient;
  private static String keyId;
  private static InMemoryForgottenSubjectStore store;
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

    store = new InMemoryForgottenSubjectStore();
    engine = newEngine();
  }

  /** A fresh engine (an empty data-key cache) on the shared KMS key and tombstone store. */
  private static AwsKmsCryptoEngine newEngine() {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(kmsClient)
        .kmsKeyId(keyId)
        .forgottenSubjectStore(store)
        .build();
  }

  private static byte[] value(int length) {
    byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('A' + i % 26);
    }
    return value;
  }

  @Test
  void roundtrip_encrypt_decrypt() {
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = engine.encrypt(SubjectId.of("subject-1"), plaintext);
    assertNotNull(ciphertext);
    assertTrue(ciphertext.length > 0);
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-1"), ciphertext));
    assertArrayEquals(plaintext, newEngine().decrypt(SubjectId.of("subject-1"), ciphertext));
  }

  @Test
  void values_beyond_the_kms_encrypt_limit_round_trip() {
    // KMS Encrypt refuses a plaintext above 4,096 bytes; envelope encryption has no such limit.
    var subject = SubjectId.of("subject-large");
    for (int length : new int[] {4_096, 4_097, 300 * 1024}) {
      byte[] plaintext = value(length);
      byte[] ciphertext = engine.encrypt(subject, plaintext);
      assertArrayEquals(plaintext, engine.decrypt(subject, ciphertext), "length " + length);
      assertArrayEquals(
          plaintext, newEngine().decrypt(subject, ciphertext), "cold engine, length " + length);
    }
  }

  @Test
  void encrypt_produces_different_ciphertext_each_call() {
    byte[] plaintext = "same data".getBytes(StandardCharsets.UTF_8);
    byte[] ct1 = engine.encrypt(SubjectId.of("subject-2"), plaintext);
    byte[] ct2 = engine.encrypt(SubjectId.of("subject-2"), plaintext);
    assertFalse(java.util.Arrays.equals(ct1, ct2), "a fresh nonce per value");
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-2"), ct1));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-2"), ct2));
  }

  @Test
  void kms_key_is_enabled_and_available() {
    assertTrue(engine.isKeyAvailable(SubjectId.of("any-subject")));
  }

  @Test
  void ciphertext_is_bound_to_subject_kms_rejects_cross_subject_decrypt() {
    // The data key is wrapped under the subject's encryption context, so KMS itself refuses to
    // unwrap subject A's data key for subject B — a deterministic verdict on the blob.
    byte[] pii = "top secret".getBytes(StandardCharsets.UTF_8);
    byte[] ct = engine.encrypt(SubjectId.of("subject-A"), pii);
    engine.encrypt(SubjectId.of("subject-B"), pii); // B has a cached key of its own

    for (var e : List.of(engine, newEngine())) {
      var thrown =
          assertThrows(
              CryptoMappingException.class,
              () -> e.decrypt(SubjectId.of("subject-B"), ct),
              "a subject/ciphertext mismatch must be refused, not decrypted");
      assertInstanceOf(
          InvalidCiphertextException.class,
          thrown.getCause(),
          "KMS itself refuses the data key under another subject's encryption context");
    }
    assertArrayEquals(pii, engine.decrypt(SubjectId.of("subject-A"), ct));
  }

  @Test
  void forget_makes_every_value_of_the_subject_undecryptable() {
    var subject = SubjectId.of("subject-forget");
    var bystander = SubjectId.of("subject-bystander");
    var writer = newEngine();
    var reader = newEngine();
    List<byte[]> blobs = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      blobs.add(writer.encrypt(subject, value(5_000 + i)));
    }
    assertArrayEquals(value(5_000), reader.decrypt(subject, blobs.get(0))); // reader caches too
    byte[] kept = writer.encrypt(bystander, value(10));

    writer.deleteKey(subject);

    for (var e : List.of(writer, reader, newEngine())) {
      for (byte[] blob : blobs) {
        assertThrows(KeyNotFoundException.class, () -> e.decrypt(subject, blob));
      }
      assertThrows(SubjectForgottenException.class, () -> e.encrypt(subject, value(1)));
      assertArrayEquals(value(10), e.decrypt(bystander, kept));
    }

    // Reinstating lifts the tombstone, and with it the erasure: the CMK was never touched.
    writer.reinstate(subject);
    assertArrayEquals(value(5_001), newEngine().decrypt(subject, blobs.get(1)));
  }

  @Test
  void a_value_survives_re_wrapping_its_data_key_under_another_cmk() {
    // The migration to another CMK: kms:ReEncrypt each wrapped data key under the same encryption
    // context, splice it into the envelope, then point the engine at the new key. LocalStack does
    // not emulate ReEncrypt, so the test does what it does server side: Decrypt under the old key,
    // Encrypt under the new one, both with the subject's encryption context.
    var subject = SubjectId.of("subject-migrated");
    byte[] blob = engine.encrypt(subject, value(6_000));
    int wrappedLength = ((blob[5] & 0xFF) << 8) | (blob[6] & 0xFF);
    byte[] wrapped = java.util.Arrays.copyOfRange(blob, 7, 7 + wrappedLength);
    String newKeyId = kmsClient.createKey(CreateKeyRequest.builder().build()).keyMetadata().keyId();
    var context = java.util.Map.of("streamrune-subject", subject.redacted());
    SdkBytes dataKey =
        kmsClient
            .decrypt(
                DecryptRequest.builder()
                    .ciphertextBlob(SdkBytes.fromByteArray(wrapped))
                    .keyId(keyId)
                    .encryptionContext(context)
                    .build())
            .plaintext();
    byte[] rewrapped =
        kmsClient
            .encrypt(
                EncryptRequest.builder()
                    .keyId(newKeyId)
                    .plaintext(dataKey)
                    .encryptionContext(context)
                    .build())
            .ciphertextBlob()
            .asByteArray();
    byte[] migrated = AwsKmsCryptoEngineTest.withWrappedKey(blob, rewrapped);

    var onNewKey =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(newKeyId)
            .forgottenSubjectStore(store)
            .build();
    assertArrayEquals(value(6_000), onNewKey.decrypt(subject, migrated));
    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> onNewKey.decrypt(SubjectId.of("subject-not-migrated"), migrated));
    assertInstanceOf(InvalidCiphertextException.class, thrown.getCause());
  }

  @Test
  void raw_kms_ciphertext_without_the_envelope_is_refused() {
    // A raw KMS ciphertext made under the SAME shared CMK but without this engine's envelope must
    // not decrypt under any subject id. It is refused before KMS is called.
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
   * Against a real KMS implementation: a key id that does not resolve is an
   * infrastructure/configuration fault, so encrypt, decrypt and the advisory probe fail closed with
   * the bare type rather than answering "erased" or "poison".
   */
  @Test
  void unknown_key_fails_closed_on_encrypt_decrypt_and_isKeyAvailable() {
    var bogusEngine =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId("arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();

    assertThrows(
        CryptoOperationException.class,
        () -> bogusEngine.isKeyAvailable(SubjectId.of("any-subject")));

    var encryptFailure =
        assertThrows(
            CryptoOperationException.class,
            () -> bogusEngine.encrypt(SubjectId.of("any-subject"), value(10)));
    assertFalse(encryptFailure instanceof CryptoMappingException, encryptFailure.toString());

    // A well-formed envelope from the real key, so the call reaches KMS and meets the unresolvable
    // key id.
    byte[] blob = engine.encrypt(SubjectId.of("any-subject"), value(10));
    RuntimeException decryptFailure =
        assertThrows(
            RuntimeException.class, () -> bogusEngine.decrypt(SubjectId.of("any-subject"), blob));
    assertFalse(
        decryptFailure instanceof KeyNotFoundException,
        "an unresolvable key id must never be convertible to the [REDACTED] tombstone: "
            + decryptFailure);
    assertInstanceOf(CryptoOperationException.class, decryptFailure);
  }
}

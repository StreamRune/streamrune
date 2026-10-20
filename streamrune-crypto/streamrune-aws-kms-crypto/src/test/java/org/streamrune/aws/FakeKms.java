package org.streamrune.aws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.IncorrectKeyException;
import software.amazon.awssdk.services.kms.model.InvalidCiphertextException;

/**
 * A Mockito-backed {@link KmsClient} that behaves like AWS KMS for the two calls the engine makes
 * on the data path: {@code GenerateDataKey} returns a fresh random AES-256 key with an opaque
 * wrapped form, and {@code Decrypt} returns the key only for a wrapped form it issued, under the
 * same key id and the same encryption context — {@code InvalidCiphertextException} otherwise, as
 * KMS answers.
 *
 * <p>Failures are injected with {@code doThrow(...).when(fake.client()).generateDataKey(...)}: a
 * {@code when(...)} on the stubbed call would run the answer while stubbing.
 */
final class FakeKms {

  private record Issued(byte[] key, Map<String, String> context, String keyId) {}

  private final KmsClient client = mock(KmsClient.class);
  private final SecureRandom random = new SecureRandom();
  private final Map<ByteBuffer, Issued> issued = new ConcurrentHashMap<>();

  /** When false, {@code Decrypt} ignores the encryption context, as a broken KMS would. */
  private volatile boolean enforceContext = true;

  FakeKms() {
    when(client.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenAnswer(invocation -> generate(invocation.getArgument(0)));
    when(client.decrypt(any(DecryptRequest.class)))
        .thenAnswer(invocation -> decrypt(invocation.getArgument(0)));
  }

  KmsClient client() {
    return client;
  }

  void ignoreEncryptionContext() {
    enforceContext = false;
  }

  /**
   * What {@code kms:ReEncrypt} does to a wrapped data key: the same key and encryption context,
   * wrapped anew under {@code destinationKeyId}.
   */
  byte[] reEncrypt(byte[] wrapped, String destinationKeyId) {
    Issued i = issued.get(ByteBuffer.wrap(wrapped));
    if (i == null) {
      throw InvalidCiphertextException.builder().message("unknown ciphertext").build();
    }
    byte[] rewrapped = new byte[150];
    random.nextBytes(rewrapped);
    issued.put(
        ByteBuffer.wrap(rewrapped.clone()),
        new Issued(i.key().clone(), i.context(), destinationKeyId));
    return rewrapped;
  }

  private GenerateDataKeyResponse generate(GenerateDataKeyRequest request) {
    if (request.keySpec() != DataKeySpec.AES_256) {
      throw new IllegalArgumentException("the fake issues AES_256 data keys only");
    }
    byte[] key = new byte[32];
    random.nextBytes(key);
    // A real wrapped AES-256 data key under a symmetric CMK is about 184 bytes.
    byte[] wrapped = new byte[184];
    random.nextBytes(wrapped);
    issued.put(
        ByteBuffer.wrap(wrapped.clone()),
        new Issued(key.clone(), Map.copyOf(request.encryptionContext()), request.keyId()));
    return GenerateDataKeyResponse.builder()
        .keyId(request.keyId())
        .plaintext(SdkBytes.fromByteArray(key))
        .ciphertextBlob(SdkBytes.fromByteArray(wrapped))
        .build();
  }

  private DecryptResponse decrypt(DecryptRequest request) {
    Issued i = issued.get(ByteBuffer.wrap(request.ciphertextBlob().asByteArray()));
    if (i == null) {
      throw InvalidCiphertextException.builder().message("unknown ciphertext").build();
    }
    if (!i.keyId().equals(request.keyId())) {
      throw IncorrectKeyException.builder().message("wrong key for the ciphertext").build();
    }
    if (enforceContext && !i.context().equals(request.encryptionContext())) {
      throw InvalidCiphertextException.builder().message("encryption context mismatch").build();
    }
    return DecryptResponse.builder()
        .keyId(i.keyId())
        .plaintext(SdkBytes.fromByteArray(i.key().clone()))
        .build();
  }
}

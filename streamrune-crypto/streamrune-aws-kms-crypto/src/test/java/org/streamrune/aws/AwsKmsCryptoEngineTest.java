package org.streamrune.aws;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.DisabledException;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsInternalException;
import software.amazon.awssdk.services.kms.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

class AwsKmsCryptoEngineTest {

  private static final String KEY_ID = "test-key-id";

  /** Bytes before the wrapped data key: magic (4), version (1), wrapped-key length (2). */
  private static final int FIXED_HEADER = 7;

  private FakeKms kms;
  private KmsClient kmsClient;
  private InMemoryForgottenSubjectStore store;
  private AwsKmsCryptoEngine engine;

  @BeforeEach
  void setUp() {
    kms = new FakeKms();
    kmsClient = kms.client();
    store = new InMemoryForgottenSubjectStore();
    engine = engineOn(store);
  }

  private AwsKmsCryptoEngine engineOn(ForgottenSubjectStore tombstones) {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(kmsClient)
        .kmsKeyId(KEY_ID)
        // The engine fails closed without a store; tests opt into the in-memory one.
        .forgottenSubjectStore(tombstones)
        .build();
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] largeValue(int length) {
    byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) ('a' + i % 26);
    }
    return value;
  }

  private static int wrappedKeyLength(byte[] blob) {
    return ((blob[5] & 0xFF) << 8) | (blob[6] & 0xFF);
  }

  private static byte[] wrappedKey(byte[] blob) {
    return Arrays.copyOfRange(blob, FIXED_HEADER, FIXED_HEADER + wrappedKeyLength(blob));
  }

  // ---- envelope encryption: values of any size, one KMS call per data key -------------------

  @Test
  void aValueLargerThanTheKmsEncryptLimitRoundTrips() {
    // KMS Encrypt refuses a plaintext above 4,096 bytes. The engine encrypts values locally under a
    // data key, so a free-text field of any size round-trips.
    var subject = SubjectId.of("customer-large");
    for (int length : new int[] {0, 1, 4_096, 4_097, 64 * 1024, 1024 * 1024}) {
      byte[] value = largeValue(length);
      byte[] blob = engine.encrypt(subject, value);
      assertArrayEquals(value, engine.decrypt(subject, blob), "length " + length);
      assertArrayEquals(
          value, engineOn(store).decrypt(subject, blob), "cold engine, length " + length);
    }
    verify(kmsClient, never()).encrypt(any(EncryptRequest.class));
  }

  @Test
  void encryptWritesTheVersionedEnvelopeWithTheWrappedDataKey() {
    var captor = org.mockito.ArgumentCaptor.forClass(GenerateDataKeyRequest.class);
    var subject = SubjectId.of("alice@example.com");
    byte[] value = bytes("pii");

    byte[] blob = engine.encrypt(subject, value);

    verify(kmsClient).generateDataKey(captor.capture());
    GenerateDataKeyRequest request = captor.getValue();
    assertEquals(KEY_ID, request.keyId());
    assertEquals(DataKeySpec.AES_256, request.keySpec());
    assertEquals(
        java.util.Map.of("streamrune-subject", subject.redacted()),
        request.encryptionContext(),
        "the data key is bound to the subject hash");
    assertFalse(
        request.encryptionContext().toString().contains("alice@example.com"),
        "the encryption context must not carry the raw subject id");

    assertEquals("SRKC", new String(blob, 0, 4, StandardCharsets.US_ASCII));
    assertEquals(2, blob[4], "envelope version");
    assertEquals(184, wrappedKeyLength(blob), "the wrapped key length the fake KMS returns");
    // [header][wrapped key][12-byte nonce][ciphertext][16-byte tag]
    assertEquals(FIXED_HEADER + 184 + 12 + value.length + 16, blob.length);
    assertFalse(
        new String(blob, StandardCharsets.ISO_8859_1).contains("pii"),
        "the value is encrypted, not copied");
  }

  @Test
  void encryptUsesOneDataKeyPerSubjectAndARandomNonce() {
    var subject = SubjectId.of("customer-many");
    List<byte[]> blobs = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      blobs.add(engine.encrypt(subject, bytes("same value")));
    }
    verify(kmsClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
    assertArrayEquals(wrappedKey(blobs.get(0)), wrappedKey(blobs.get(49)), "one data key");
    assertFalse(Arrays.equals(blobs.get(0), blobs.get(1)), "a fresh nonce per value");

    // The encrypting engine already holds the key: decrypt needs no KMS call.
    for (byte[] blob : blobs) {
      assertArrayEquals(bytes("same value"), engine.decrypt(subject, blob));
    }
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));

    // Another subject gets its own data key.
    engine.encrypt(SubjectId.of("customer-other"), bytes("x"));
    verify(kmsClient, times(2)).generateDataKey(any(GenerateDataKeyRequest.class));
  }

  @Test
  void aColdEngineUnwrapsEachDataKeyOnceUnderTheSubjectContext() {
    var subject = SubjectId.of("bob@example.com");
    byte[] first = engine.encrypt(subject, bytes("one"));
    byte[] second = engine.encrypt(subject, bytes("two"));

    var cold = engineOn(store);
    var captor = org.mockito.ArgumentCaptor.forClass(DecryptRequest.class);
    assertArrayEquals(bytes("one"), cold.decrypt(subject, first));
    assertArrayEquals(bytes("two"), cold.decrypt(subject, second));
    assertArrayEquals(bytes("one"), cold.decrypt(subject, first));

    verify(kmsClient, times(1)).decrypt(captor.capture());
    DecryptRequest request = captor.getValue();
    assertEquals(KEY_ID, request.keyId(), "decrypt pins the configured key");
    assertEquals(subject.redacted(), request.encryptionContext().get("streamrune-subject"));
    assertArrayEquals(wrappedKey(first), request.ciphertextBlob().asByteArray());
  }

  @Test
  void withTheCacheDisabledEveryValueCostsOneKmsCall() {
    var uncached =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(KEY_ID)
            .forgottenSubjectStore(store)
            .dataKeyCacheMaximumSize(0)
            .build();
    var subject = SubjectId.of("customer-uncached");
    byte[] a = uncached.encrypt(subject, bytes("a"));
    byte[] b = uncached.encrypt(subject, bytes("b"));
    assertFalse(Arrays.equals(wrappedKey(a), wrappedKey(b)), "a data key per value");
    assertArrayEquals(bytes("a"), uncached.decrypt(subject, a));
    assertArrayEquals(bytes("a"), uncached.decrypt(subject, a));
    verify(kmsClient, times(2)).generateDataKey(any(GenerateDataKeyRequest.class));
    verify(kmsClient, times(2)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void aCachedDataKeyExpiresAfterTheConfiguredWindow() {
    AtomicLong now = new AtomicLong(1_000);
    var clocked =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(KEY_ID)
            .forgottenSubjectStore(store)
            .dataKeyCacheExpireAfterWrite(Duration.ofSeconds(10))
            .nanoClock(now::get)
            .build();
    var subject = SubjectId.of("customer-ttl");
    byte[] blob = clocked.encrypt(subject, bytes("v"));

    now.addAndGet(Duration.ofSeconds(10).toNanos() - 1);
    clocked.encrypt(subject, bytes("v"));
    assertArrayEquals(bytes("v"), clocked.decrypt(subject, blob));
    verify(kmsClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));

    now.addAndGet(1);
    clocked.encrypt(subject, bytes("v"));
    assertArrayEquals(bytes("v"), clocked.decrypt(subject, blob));
    verify(kmsClient, times(2)).generateDataKey(any(GenerateDataKeyRequest.class));
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void aDataKeyIsReplacedAfterItsEncryptionBudget() {
    var budgeted =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(KEY_ID)
            .forgottenSubjectStore(store)
            .maxEncryptionsPerDataKey(3)
            .build();
    var subject = SubjectId.of("customer-budget");
    List<byte[]> blobs = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      blobs.add(budgeted.encrypt(subject, bytes("v" + i)));
    }
    verify(kmsClient, times(3)).generateDataKey(any(GenerateDataKeyRequest.class));
    assertArrayEquals(wrappedKey(blobs.get(0)), wrappedKey(blobs.get(2)));
    assertFalse(Arrays.equals(wrappedKey(blobs.get(2)), wrappedKey(blobs.get(3))));
    assertArrayEquals(wrappedKey(blobs.get(3)), wrappedKey(blobs.get(5)));
    assertFalse(Arrays.equals(wrappedKey(blobs.get(5)), wrappedKey(blobs.get(6))));
    for (int i = 0; i < 7; i++) {
      assertArrayEquals(bytes("v" + i), budgeted.decrypt(subject, blobs.get(i)));
    }
  }

  @Test
  void theCacheHoldsAtMostTheConfiguredNumberOfSubjects() {
    var small =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(KEY_ID)
            .forgottenSubjectStore(store)
            .dataKeyCacheMaximumSize(2)
            .build();
    var a = SubjectId.of("a");
    var b = SubjectId.of("b");
    var c = SubjectId.of("c");
    byte[] blobA = small.encrypt(a, bytes("a"));
    small.encrypt(b, bytes("b"));
    small.encrypt(a, bytes("a")); // a is now the most recently used
    small.encrypt(c, bytes("c")); // evicts b, the least recently used
    verify(kmsClient, times(3)).generateDataKey(any(GenerateDataKeyRequest.class));
    small.encrypt(a, bytes("a"));
    verify(kmsClient, times(3)).generateDataKey(any(GenerateDataKeyRequest.class));
    small.encrypt(b, bytes("b"));
    verify(kmsClient, times(4)).generateDataKey(any(GenerateDataKeyRequest.class));
    // The unwrap side is bounded the same way: a's key was pushed out by b and c.
    assertArrayEquals(bytes("a"), small.decrypt(a, blobA));
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void anExpiredDataKeyLeavesThePeersHeapOnceThePeerStoresAnotherKey() {
    // A replica that did not run the forget never looks the forgotten subject up again (the
    // tombstone refuses first), so expiry checked on lookup alone would keep the key forever.
    AtomicLong now = new AtomicLong(0);
    var peer = clockedEngine(now);
    var forgetter = clockedEngine(now);
    var forgotten = SubjectId.of("customer-forgotten");
    byte[] blob = peer.encrypt(forgotten, bytes("pii"));
    assertArrayEquals(bytes("pii"), peer.decrypt(forgotten, blob));
    assertTrue(peer.holdsDataKeysOf(forgotten));

    forgetter.deleteKey(forgotten);
    assertTrue(peer.holdsDataKeysOf(forgotten), "the peer has not expired the keys yet");

    now.addAndGet(Duration.ofMinutes(5).toNanos());
    for (int i = 0; i < 100; i++) {
      peer.encrypt(SubjectId.of("customer-" + i), bytes("v"));
    }
    assertFalse(peer.holdsDataKeysOf(forgotten), "expired keys are swept on the next store");
    assertTrue(peer.holdsDataKeysOf(SubjectId.of("customer-99")));
  }

  @Test
  void anExpiredDataKeyLeavesTheHeapOfAPeerThatOnlyReadsCachedKeys() {
    AtomicLong now = new AtomicLong(0);
    var peer = clockedEngine(now);
    var forgotten = SubjectId.of("customer-forgotten-read");
    var reader = SubjectId.of("customer-read-only");
    peer.encrypt(forgotten, bytes("pii"));
    now.addAndGet(Duration.ofMinutes(4).toNanos());
    byte[] blob = peer.encrypt(reader, bytes("kept")); // stored at 4 min: live until 9 min
    engine.deleteKey(forgotten);

    now.addAndGet(Duration.ofMinutes(1).toNanos() + Duration.ofSeconds(1).toNanos());
    clearInvocations(kmsClient);
    assertArrayEquals(bytes("kept"), peer.decrypt(reader, blob)); // a cache hit, no store
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
    assertFalse(peer.holdsDataKeysOf(forgotten), "a lookup sweeps at most once a second");
    assertTrue(peer.holdsDataKeysOf(reader));
  }

  @Test
  void aLookupSweepsAtMostOnceASecond() {
    AtomicLong now = new AtomicLong(0);
    var peer = clockedEngine(now);
    var stale = SubjectId.of("customer-stale");
    var reader = SubjectId.of("customer-reader");
    peer.encrypt(stale, bytes("pii"));
    now.addAndGet(Duration.ofMinutes(5).toNanos() - Duration.ofMillis(500).toNanos());
    byte[] blob = peer.encrypt(reader, bytes("kept")); // a store sweeps: stale is still live
    now.addAndGet(Duration.ofMillis(500).toNanos()); // stale expires now
    peer.decrypt(reader, blob); // less than a second after the store's sweep: no sweep
    assertTrue(peer.holdsDataKeysOf(stale));
    now.addAndGet(Duration.ofMillis(500).toNanos());
    peer.decrypt(reader, blob); // a second after the last sweep
    assertFalse(peer.holdsDataKeysOf(stale));
  }

  @Test
  void deleteKeySweepsExpiredKeysOfOtherSubjects() {
    AtomicLong now = new AtomicLong(0);
    var peer = clockedEngine(now);
    var stale = SubjectId.of("customer-stale-2");
    peer.encrypt(stale, bytes("pii"));
    now.addAndGet(Duration.ofMinutes(5).toNanos());
    peer.deleteKey(SubjectId.of("someone-else"));
    assertFalse(peer.holdsDataKeysOf(stale));
  }

  private AwsKmsCryptoEngine clockedEngine(AtomicLong now) {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(kmsClient)
        .kmsKeyId(KEY_ID)
        .forgottenSubjectStore(store)
        .nanoClock(now::get)
        .build();
  }

  @Test
  void theEngineReportsItsDataKeyCacheSettings() {
    assertEquals(1_000, engine.dataKeyCacheMaximumSize());
    assertEquals(Duration.ofMinutes(5), engine.dataKeyCacheExpireAfterWrite());
    var tuned =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId(KEY_ID)
            .forgottenSubjectStore(store)
            .dataKeyCacheMaximumSize(0)
            .dataKeyCacheExpireAfterWrite(Duration.ofSeconds(30))
            .build();
    assertEquals(0, tuned.dataKeyCacheMaximumSize());
    assertEquals(Duration.ofSeconds(30), tuned.dataKeyCacheExpireAfterWrite());
  }

  // ---- re-wrapping under another CMK ---------------------------------------------------------

  /** {@code blob} with its wrapped data key replaced, as a {@code kms:ReEncrypt} migration does. */
  static byte[] withWrappedKey(byte[] blob, byte[] rewrapped) {
    int oldLength = ((blob[5] & 0xFF) << 8) | (blob[6] & 0xFF);
    int tail = blob.length - FIXED_HEADER - oldLength;
    byte[] out = new byte[FIXED_HEADER + rewrapped.length + tail];
    System.arraycopy(blob, 0, out, 0, 5);
    out[5] = (byte) (rewrapped.length >>> 8);
    out[6] = (byte) rewrapped.length;
    System.arraycopy(rewrapped, 0, out, FIXED_HEADER, rewrapped.length);
    System.arraycopy(blob, FIXED_HEADER + oldLength, out, FIXED_HEADER + rewrapped.length, tail);
    return out;
  }

  @Test
  void aDataKeyReWrappedUnderAnotherCmkStillOpensTheValue() {
    // Migrating to another CMK re-wraps each data key in place (kms:ReEncrypt) and leaves the
    // AES-GCM layer alone: the wrapped key is not part of the associated data.
    var subject = SubjectId.of("customer-migrated");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    byte[] migrated = withWrappedKey(blob, kms.reEncrypt(wrappedKey(blob), "new-key-id"));
    assertNotEquals(184, wrappedKeyLength(migrated));

    var onNewKey =
        AwsKmsCryptoEngine.builder()
            .kmsClient(kmsClient)
            .kmsKeyId("new-key-id")
            .forgottenSubjectStore(store)
            .build();
    assertArrayEquals(bytes("pii"), onNewKey.decrypt(subject, migrated));
    // Still bound to the subject: KMS refuses another subject's context.
    var other = SubjectId.of("customer-other-migrated");
    var thrown =
        assertThrows(CryptoMappingException.class, () -> onNewKey.decrypt(other, migrated));
    assertInstanceOf(
        software.amazon.awssdk.services.kms.model.InvalidCiphertextException.class,
        thrown.getCause());
  }

  // ---- subject binding --------------------------------------------------------------------

  @Test
  void aCiphertextOfOneSubjectNeverDecryptsAsAnother() {
    // "Aa" and "BB" have the same String hash code, so the two subjects' cache entries share a
    // bucket and only equality on the subject keeps them apart.
    var alice = SubjectId.of("Aa");
    var mallory = SubjectId.of("BB");
    assertEquals(alice.hashCode(), mallory.hashCode());
    byte[] blob = engine.encrypt(alice, bytes("alice's address"));
    engine.encrypt(mallory, bytes("warm the cache for mallory too"));
    engine.decrypt(alice, blob); // alice's key is in the unwrap map under alice

    // Warm engine (alice's key is cached) and cold engine: KMS refuses to unwrap alice's data key
    // under mallory's context — a deterministic verdict on the blob.
    for (var e : List.of(engine, engineOn(store))) {
      var thrown = assertThrows(CryptoMappingException.class, () -> e.decrypt(mallory, blob));
      assertTrue(thrown.getMessage().contains(mallory.redacted()), thrown.getMessage());
      // KMS's verdict, not the GCM tag: a data key is cached only for the subject it was
      // unwrapped (or generated) for, so mallory's lookup never finds alice's key.
      assertInstanceOf(
          software.amazon.awssdk.services.kms.model.InvalidCiphertextException.class,
          thrown.getCause());
    }
    assertArrayEquals(bytes("alice's address"), engine.decrypt(alice, blob));
  }

  @Test
  void theSubjectIsBoundIntoTheValueEvenIfKmsIgnoredTheContext() {
    // Defence in depth: the AES-GCM associated data carries the subject hash, so a data key that
    // somehow unwraps under another subject still cannot open the value.
    kms.ignoreEncryptionContext();
    byte[] blob = engine.encrypt(SubjectId.of("alice"), bytes("pii"));
    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engineOn(store).decrypt(SubjectId.of("mallory"), blob));
    assertInstanceOf(javax.crypto.AEADBadTagException.class, thrown.getCause());
  }

  @Test
  void aTamperedEnvelopeIsADeterministicFailure() {
    var subject = SubjectId.of("customer-tamper");
    byte[] blob = engine.encrypt(subject, bytes("pii"));

    byte[] body = blob.clone();
    body[body.length - 1] ^= 1; // the tag
    assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, body));

    byte[] nonce = blob.clone();
    nonce[FIXED_HEADER + 184] ^= 1;
    assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, nonce));

    // A wrapped key KMS never issued: KMS's InvalidCiphertextException.
    byte[] wrapped = blob.clone();
    wrapped[FIXED_HEADER] ^= 1;
    var thrown = assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, wrapped));
    assertInstanceOf(
        software.amazon.awssdk.services.kms.model.InvalidCiphertextException.class,
        thrown.getCause());
  }

  @Test
  void malformedEnvelopesAreRefusedBeforeAnyKmsCall() {
    var subject = SubjectId.of("subject-malformed");
    byte[] good = engineOn(store).encrypt(subject, bytes("pii"));
    clearInvocations(kmsClient);
    var cold = engineOn(store);

    List<byte[]> malformed = new ArrayList<>();
    malformed.add(new byte[0]);
    malformed.add(new byte[] {0x01, 0x02, 0x03}); // a raw KMS ciphertext
    malformed.add(new byte[] {'S', 'R', 'K'});
    malformed.add(new byte[] {'S', 'R', 'K', 'C'});
    malformed.add(new byte[] {'S', 'R', 'K', 'C', 2});
    malformed.add(new byte[] {'S', 'R', 'K', 'C', 2, 0});
    malformed.add(new byte[] {'X', 'R', 'K', 'C', 2, 0, 1, 1});
    malformed.add(new byte[] {'S', 'X', 'K', 'C', 2, 0, 1, 1});
    malformed.add(new byte[] {'S', 'R', 'X', 'C', 2, 0, 1, 1});
    malformed.add(new byte[] {'S', 'R', 'K', 'X', 2, 0, 1, 1});
    for (int i = 0; i < 4; i++) {
      byte[] foreignMagic = good.clone(); // well-formed apart from one byte of the magic
      foreignMagic[i] ^= 0x20;
      malformed.add(foreignMagic);
    }
    byte[] zeroLength = good.clone();
    zeroLength[5] = 0;
    zeroLength[6] = 0;
    malformed.add(zeroLength);
    byte[] lengthBeyondBlob = good.clone();
    lengthBeyondBlob[5] = (byte) 0xFF;
    malformed.add(lengthBeyondBlob);
    // Header and wrapped key intact, but one byte short of nonce + tag.
    malformed.add(Arrays.copyOf(good, FIXED_HEADER + 184 + 12 + 15));
    byte[] previousFormat = good.clone();
    previousFormat[4] = 1;
    malformed.add(previousFormat);
    byte[] futureFormat = good.clone();
    futureFormat[4] = 3;
    malformed.add(futureFormat);

    for (byte[] blob : malformed) {
      assertThrows(
          CryptoMappingException.class,
          () -> cold.decrypt(subject, blob),
          () -> "blob of length " + blob.length);
    }
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));

    // The shortest well-formed envelope (empty value) still decrypts.
    byte[] empty = cold.encrypt(subject, new byte[0]);
    assertEquals(FIXED_HEADER + 184 + 12 + 16, empty.length);
    assertArrayEquals(new byte[0], engineOn(store).decrypt(subject, empty));
  }

  @Test
  void anUnsupportedVersionNamesTheVersion() {
    var subject = SubjectId.of("subject-version");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    blob[4] = 1;
    var thrown = assertThrows(CryptoMappingException.class, () -> engine.decrypt(subject, blob));
    assertTrue(thrown.getMessage().contains("version 1"), thrown.getMessage());
  }

  @Test
  void aWrappedBlobThatIsNoDataKeyIsADeterministicFailure() {
    // KMS unwraps something that is not a 256-bit key: only a producer with kms:Encrypt on the CMK
    // and this subject's context could have made it, and no retry changes it.
    var subject = SubjectId.of("subject-short-key");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    doReturn(
            DecryptResponse.builder()
                .keyId(KEY_ID)
                .plaintext(SdkBytes.fromByteArray(new byte[7]))
                .build())
        .when(kmsClient)
        .decrypt(any(DecryptRequest.class));
    var thrown =
        assertThrows(CryptoMappingException.class, () -> engineOn(store).decrypt(subject, blob));
    assertTrue(thrown.getMessage().contains("7-byte value"), thrown.getMessage());
  }

  // ---- crypto-shredding ----------------------------------------------------------------------

  @Test
  void forgetMakesEveryValueOfTheSubjectUndecryptable_cachedKeysIncluded() {
    var subject = SubjectId.of("customer-forget");
    var bystander = SubjectId.of("customer-bystander");
    List<byte[]> blobs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      blobs.add(engine.encrypt(subject, largeValue(5_000 + i)));
    }
    var peer = engineOn(store); // another replica on the same tombstone store
    byte[] warmed = engine.encrypt(subject, bytes("v"));
    assertArrayEquals(bytes("v"), peer.decrypt(subject, warmed)); // the peer caches the key too
    byte[] bystanderBlob = engine.encrypt(bystander, bytes("kept"));
    clearInvocations(kmsClient);

    engine.deleteKey(subject);

    for (var e : List.of(engine, peer, engineOn(store))) {
      for (byte[] blob : blobs) {
        assertThrows(KeyNotFoundException.class, () -> e.decrypt(subject, blob));
      }
      assertThrows(KeyNotFoundException.class, () -> e.decrypt(subject, warmed));
      assertThrows(SubjectForgottenException.class, () -> e.encrypt(subject, bytes("new")));
      assertArrayEquals(bytes("kept"), e.decrypt(bystander, bystanderBlob));
    }
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
    // Only the bystander reads of the two engines that had not cached its key reached KMS.
    verify(kmsClient, times(2)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void deleteKeyDropsTheSubjectsCachedDataKeys() {
    // Observable through reinstate: the forgetting engine must fetch the keys from KMS again.
    var subject = SubjectId.of("customer-evict");
    var bystander = SubjectId.of("customer-stays");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    byte[] bystanderBlob = engine.encrypt(bystander, bytes("kept"));

    engine.deleteKey(subject);
    engine.reinstate(subject);
    clearInvocations(kmsClient);

    // Reinstating restores the pre-erasure ciphertext (the CMK was never touched) ...
    assertArrayEquals(bytes("pii"), engine.decrypt(subject, blob));
    engine.encrypt(subject, bytes("again"));
    // ... but only through KMS: the forget dropped the unwrapped and the encrypting key.
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
    verify(kmsClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
    // The bystander's keys stayed cached.
    assertArrayEquals(bytes("kept"), engine.decrypt(bystander, bystanderBlob));
    engine.encrypt(bystander, bytes("more"));
    verify(kmsClient, times(1)).decrypt(any(DecryptRequest.class));
    verify(kmsClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
  }

  /** A client that forwards the data-path calls to the fake, so a test can re-stub one. */
  private KmsClient forwardingClient() {
    KmsClient client = mock(KmsClient.class);
    forwardGenerateDataKey(client);
    forwardDecrypt(client);
    return client;
  }

  private void forwardGenerateDataKey(KmsClient client) {
    doAnswer(
            invocation ->
                kmsClient.generateDataKey((GenerateDataKeyRequest) invocation.getArgument(0)))
        .when(client)
        .generateDataKey(any(GenerateDataKeyRequest.class));
  }

  private void forwardDecrypt(KmsClient client) {
    doAnswer(invocation -> kmsClient.decrypt((DecryptRequest) invocation.getArgument(0)))
        .when(client)
        .decrypt(any(DecryptRequest.class));
  }

  private AwsKmsCryptoEngine engineWith(KmsClient client) {
    return AwsKmsCryptoEngine.builder()
        .kmsClient(client)
        .kmsKeyId(KEY_ID)
        .forgottenSubjectStore(store)
        .build();
  }

  @Test
  void aDecryptRacingDeleteKeyDoesNotCacheTheKeyItUnwrapped() {
    var subject = SubjectId.of("customer-decrypt-race");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    KmsClient racingClient = forwardingClient();
    var racer = engineWith(racingClient);
    doAnswer(
            invocation -> {
              Object unwrapped = kmsClient.decrypt((DecryptRequest) invocation.getArgument(0));
              racer.deleteKey(subject); // the forget completes before the decrypt stores the key
              return unwrapped;
            })
        .when(racingClient)
        .decrypt(any(DecryptRequest.class));

    // The decrypt read the tombstone before the forget, so it answers what it was asked.
    assertArrayEquals(bytes("pii"), racer.decrypt(subject, blob));

    racer.reinstate(subject);
    forwardDecrypt(racingClient);
    clearInvocations(racingClient);
    // The key it unwrapped while the forget ran was not cached: KMS is asked again.
    assertArrayEquals(bytes("pii"), racer.decrypt(subject, blob));
    verify(racingClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void encryptRacingDeleteKey_refusesAndCachesNothing() {
    // A concurrent ForgetSubjectService can complete the whole erasure between encrypt's tombstone
    // pre-check and its KMS response. deleteKey never touches the shared CMK, so a ciphertext
    // returned here would stay decryptable by anyone holding kms:Decrypt plus DB read: encrypt
    // re-checks the tombstone after the KMS call and returns nothing.
    var subject = SubjectId.of("kms-encrypt-race");
    KmsClient racingClient = forwardingClient();
    var racer = engineWith(racingClient);
    doAnswer(
            invocation -> {
              Object response =
                  kmsClient.generateDataKey((GenerateDataKeyRequest) invocation.getArgument(0));
              racer.deleteKey(subject); // the concurrent erasure completes mid-flight
              return response;
            })
        .when(racingClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));

    var thrown =
        assertThrows(SubjectForgottenException.class, () -> racer.encrypt(subject, bytes("pii")));
    assertTrue(thrown.getMessage().contains(subject.redacted()), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("kms-encrypt-race"), thrown.getMessage());

    // The data key of the refused encrypt was not cached: after reinstate KMS is asked again.
    racer.reinstate(subject);
    forwardGenerateDataKey(racingClient);
    clearInvocations(racingClient);
    racer.encrypt(subject, bytes("pii"));
    verify(racingClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
  }

  @Test
  void aDataKeyGeneratedAcrossAForgetIsNotCached() {
    // The subject is forgotten and reinstated while GenerateDataKey runs: the encrypt may return
    // (the subject is live again), but the key it generated before the forget's eviction is not
    // cached after it.
    var subject = SubjectId.of("customer-forget-reinstate");
    KmsClient racingClient = forwardingClient();
    var racer = engineWith(racingClient);
    doAnswer(
            invocation -> {
              Object response =
                  kmsClient.generateDataKey((GenerateDataKeyRequest) invocation.getArgument(0));
              racer.deleteKey(subject);
              racer.reinstate(subject);
              return response;
            })
        .when(racingClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));

    byte[] blob = racer.encrypt(subject, bytes("pii"));
    forwardGenerateDataKey(racingClient);
    clearInvocations(racingClient);

    racer.encrypt(subject, bytes("pii"));
    verify(racingClient, times(1)).generateDataKey(any(GenerateDataKeyRequest.class));
    assertArrayEquals(bytes("pii"), racer.decrypt(subject, blob));
    verify(racingClient, times(1)).decrypt(any(DecryptRequest.class));
  }

  @Test
  void deleteKeyTombstonesSubject_soDecryptYieldsRedacted() {
    var subject = SubjectId.of("subject-4");
    engine.deleteKey(subject);
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, bytes("old-ct")));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  @Test
  void encryptAfterDeleteKeyThrowsSubjectForgotten() {
    var subject = SubjectId.of("subject-4b");
    engine.deleteKey(subject);
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
    verify(kmsClient, never()).generateDataKey(any(GenerateDataKeyRequest.class));
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
    var subject = SubjectId.of("alice@example.com");
    engine.deleteKey(subject);

    var forgotten =
        assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
    assertFalse(forgotten.getMessage().contains("alice@example.com"), forgotten.getMessage());
    assertTrue(forgotten.getMessage().contains(subject.redacted()), forgotten.getMessage());

    var notFound =
        assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, bytes("old-ct")));
    assertFalse(notFound.getMessage().contains("alice@example.com"), notFound.getMessage());
    assertTrue(notFound.getMessage().contains(subject.redacted()), notFound.getMessage());
  }

  @Test
  void deleteKeyIsIdempotentAndRejectsNull() {
    var subject = SubjectId.of("subject-4d");
    engine.deleteKey(subject);
    engine.deleteKey(subject);
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
  }

  @Test
  void reinstateLiftsTheTombstone() {
    var subject = SubjectId.of("subject-4e");
    engine.deleteKey(subject);
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, bytes("pii")));
    engine.reinstate(subject);
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    assertArrayEquals(bytes("pii"), engine.decrypt(subject, blob));
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
  }

  @Test
  void customForgottenSubjectStoreIsConsulted() {
    var preForgotten = new InMemoryForgottenSubjectStore();
    var subject = SubjectId.of("pre-forgotten");
    preForgotten.forget(subject);
    var e = engineOn(preForgotten);
    assertThrows(KeyNotFoundException.class, () -> e.decrypt(subject, bytes("ct")));
    verify(kmsClient, never()).decrypt(any(DecryptRequest.class));
  }

  // ---- size limit ----------------------------------------------------------------------------

  @Test
  void aValueTooLargeForTheEnvelopeFailsDeterministically_beforeAnyKmsCall() {
    // The envelope is one Java array; a value whose envelope cannot exist is refused as the
    // deterministic type, never as the bare one a classifier retries.
    AwsKmsCryptoEngine.checkPlaintextLength(AwsKmsCryptoEngine.MAX_PLAINTEXT_LENGTH);
    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () ->
                AwsKmsCryptoEngine.checkPlaintextLength(
                    AwsKmsCryptoEngine.MAX_PLAINTEXT_LENGTH + 1L));
    assertTrue(
        thrown.getMessage().contains(String.valueOf(AwsKmsCryptoEngine.MAX_PLAINTEXT_LENGTH)),
        thrown.getMessage());
    // Envelope: 7-byte header + up to 65,535 wrapped-key bytes + 12-byte nonce + 16-byte tag, in
    // an array no longer than Integer.MAX_VALUE - 8.
    assertEquals(
        Integer.MAX_VALUE - 8 - 7 - 65_535 - 12 - 16, AwsKmsCryptoEngine.MAX_PLAINTEXT_LENGTH);
  }

  @Test
  void aWrappedKeyTooLongForTheEnvelopeIsRefused() {
    doReturn(
            GenerateDataKeyResponse.builder()
                .keyId(KEY_ID)
                .plaintext(SdkBytes.fromByteArray(new byte[32]))
                .ciphertextBlob(SdkBytes.fromByteArray(new byte[65_536]))
                .build())
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("subject-long-wrap"), bytes("pii")));
    assertTrue(thrown.getMessage().contains("65536"), thrown.getMessage());

    doReturn(
            GenerateDataKeyResponse.builder()
                .keyId(KEY_ID)
                .plaintext(SdkBytes.fromByteArray(new byte[32]))
                .ciphertextBlob(SdkBytes.fromByteArray(new byte[0]))
                .build())
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    var empty =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("subject-empty-wrap"), bytes("pii")));
    assertTrue(empty.getMessage().contains("of 0 bytes"), empty.getMessage());
  }

  @Test
  void aDataKeyOfTheWrongLengthIsRefusedOnEncrypt() {
    doReturn(
            GenerateDataKeyResponse.builder()
                .keyId(KEY_ID)
                .plaintext(SdkBytes.fromByteArray(new byte[16]))
                .ciphertextBlob(SdkBytes.fromByteArray(new byte[184]))
                .build())
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    assertThrows(
        CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-short-dk"), bytes("pii")));
  }

  // ---- failure classification ----------------------------------------------------------------

  @Test
  void encryptKeepsEveryKmsFailureBare() {
    // GenerateDataKey failures are about the CMK or the service — throttling, an outage, a missing,
    // disabled or invalid-state key — so they stay the bare type a classifier retries.
    List<RuntimeException> failures =
        List.of(
            KmsInternalException.builder().message("internal").build(),
            NotFoundException.builder().message("no key").build(),
            DisabledException.builder().message("disabled").build(),
            KmsInvalidStateException.builder().message("pending import").build(),
            new RuntimeException("KMS unavailable"));
    for (RuntimeException failure : failures) {
      doThrow(failure).when(kmsClient).generateDataKey(any(GenerateDataKeyRequest.class));
      var thrown =
          assertThrows(
              CryptoOperationException.class,
              () -> engine.encrypt(SubjectId.of("subject-encrypt-fails"), bytes("pii")));
      assertFalse(thrown instanceof CryptoMappingException, failure.toString());
      assertSame(failure, thrown.getCause());
    }
  }

  @Test
  void encryptNamesTheKeyIdWhenTheCmkIsMissingOrDisabled() {
    doThrow(NotFoundException.builder().message("no key").build())
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    var missing =
        assertThrows(
            CryptoOperationException.class, () -> engine.encrypt(SubjectId.of("s"), bytes("p")));
    assertTrue(missing.getMessage().contains(KEY_ID), missing.getMessage());
    assertTrue(missing.getMessage().contains("NOT a crypto-shred"), missing.getMessage());

    doThrow(DisabledException.builder().message("disabled").build())
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    var disabled =
        assertThrows(
            CryptoOperationException.class, () -> engine.encrypt(SubjectId.of("s"), bytes("p")));
    assertTrue(disabled.getMessage().contains(KEY_ID), disabled.getMessage());
    assertTrue(disabled.getMessage().contains("reversible"), disabled.getMessage());
  }

  @Test
  void decryptKeepsKeyAndServiceLevelFailuresBare() {
    var subject = SubjectId.of("subject-decrypt-fails");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    List<RuntimeException> failures =
        List.of(
            KmsInternalException.builder().message("internal").build(),
            NotFoundException.builder().message("no key").build(),
            DisabledException.builder().message("disabled").build(),
            KmsInvalidStateException.builder().message("pending deletion").build(),
            software.amazon.awssdk.services.kms.model.IncorrectKeyException.builder()
                .message("wrong key")
                .build(),
            new RuntimeException("KMS unavailable"));
    for (RuntimeException failure : failures) {
      doThrow(failure).when(kmsClient).decrypt(any(DecryptRequest.class));
      RuntimeException thrown =
          assertThrows(RuntimeException.class, () -> engineOn(store).decrypt(subject, blob));
      assertInstanceOf(CryptoOperationException.class, thrown, failure.toString());
      assertFalse(thrown instanceof CryptoMappingException, failure.toString());
      assertFalse(thrown instanceof KeyNotFoundException, "never a crypto-shred: " + failure);
      assertSame(failure, thrown.getCause());
    }
  }

  @Test
  void decryptOnMissingCmkNamesTheKeyId() {
    var subject = SubjectId.of("subject-10");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    doThrow(NotFoundException.builder().message("Key not found").build())
        .when(kmsClient)
        .decrypt(any(DecryptRequest.class));
    var thrown =
        assertThrows(CryptoOperationException.class, () -> engineOn(store).decrypt(subject, blob));
    assertTrue(thrown.getMessage().contains(KEY_ID), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("NOT a crypto-shred"), thrown.getMessage());
  }

  @Test
  void decryptOnDisabledKeyNamesTheKeyId() {
    var subject = SubjectId.of("subject-11");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    doThrow(DisabledException.builder().message("Key is disabled").build())
        .when(kmsClient)
        .decrypt(any(DecryptRequest.class));
    var thrown =
        assertThrows(CryptoOperationException.class, () -> engineOn(store).decrypt(subject, blob));
    assertTrue(thrown.getMessage().contains(KEY_ID), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("reversible"), thrown.getMessage());
  }

  @Test
  void aKmsFailureMessageNeverCarriesTheRawSubjectId() {
    var subject = SubjectId.of("alice@example.com");
    byte[] blob = engine.encrypt(subject, bytes("pii"));
    doThrow(new RuntimeException("boom")).when(kmsClient).decrypt(any(DecryptRequest.class));
    doThrow(new RuntimeException("boom"))
        .when(kmsClient)
        .generateDataKey(any(GenerateDataKeyRequest.class));
    var cold = engineOn(store);
    for (Exception e :
        List.of(
            assertThrows(CryptoOperationException.class, () -> cold.decrypt(subject, blob)),
            assertThrows(
                CryptoOperationException.class, () -> cold.encrypt(subject, bytes("x"))))) {
      assertFalse(e.getMessage().contains("alice@example.com"), e.getMessage());
      assertTrue(e.getMessage().contains(subject.redacted()), e.getMessage());
    }
  }

  // ---- isKeyAvailable (unchanged taxonomy) ---------------------------------------------------

  private void describeKeyState(String state) {
    DescribeKeyResponse response = mock(DescribeKeyResponse.class);
    KeyMetadata metadata = mock(KeyMetadata.class);
    when(metadata.keyStateAsString()).thenReturn(state);
    when(response.keyMetadata()).thenReturn(metadata);
    when(kmsClient.describeKey(any(DescribeKeyRequest.class))).thenReturn(response);
  }

  @Test
  void isKeyAvailableFollowsTheCmkState() {
    describeKeyState("Enabled");
    assertTrue(engine.isKeyAvailable(SubjectId.of("subject-3")));
    describeKeyState("Disabled");
    assertFalse(engine.isKeyAvailable(SubjectId.of("subject-5")));
    describeKeyState("PendingDeletion");
    assertFalse(engine.isKeyAvailable(SubjectId.of("subject-6")));
  }

  @Test
  void isKeyAvailableOnMissingCmkFailsClosed_neverReportsEverySubjectAsErased() {
    when(kmsClient.describeKey(any(DescribeKeyRequest.class)))
        .thenThrow(NotFoundException.builder().message("Key not found").build());
    var thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.isKeyAvailable(SubjectId.of("subject-10b")));
    assertTrue(thrown.getMessage().contains(KEY_ID), thrown.getMessage());
  }

  @Test
  void isKeyAvailableWrapsUnexpectedExceptionInCryptoOperationException() {
    when(kmsClient.describeKey(any(DescribeKeyRequest.class)))
        .thenThrow(new RuntimeException("unexpected"));
    assertThrows(
        CryptoOperationException.class, () -> engine.isKeyAvailable(SubjectId.of("subject-9")));
  }

  // ---- argument checks and builder -----------------------------------------------------------

  @Test
  void nullArgumentsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, bytes("data")));
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("s"), null));
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, bytes("data")));
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("s"), null));
    assertThrows(IllegalArgumentException.class, () -> engine.isKeyAvailable(null));
  }

  @Test
  void builderValidatesTheDataKeyCacheSettings() {
    var builder = AwsKmsCryptoEngine.builder();
    assertThrows(IllegalArgumentException.class, () -> builder.dataKeyCacheMaximumSize(-1));
    assertThrows(IllegalArgumentException.class, () -> builder.dataKeyCacheExpireAfterWrite(null));
    assertThrows(
        IllegalArgumentException.class, () -> builder.dataKeyCacheExpireAfterWrite(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.dataKeyCacheExpireAfterWrite(Duration.ofSeconds(-1)));
    assertThrows(IllegalArgumentException.class, () -> builder.maxEncryptionsPerDataKey(0));
    assertThrows(IllegalArgumentException.class, () -> builder.nanoClock(null));
    assertEquals(1_000, AwsKmsCryptoEngine.DEFAULT_DATA_KEY_CACHE_MAXIMUM_SIZE);
    assertEquals(
        Duration.ofMinutes(5), AwsKmsCryptoEngine.DEFAULT_DATA_KEY_CACHE_EXPIRE_AFTER_WRITE);
    assertEquals(1L << 24, AwsKmsCryptoEngine.MAX_ENCRYPTIONS_PER_DATA_KEY);
  }

  @Test
  void builderRejectsNullForgottenSubjectStore() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AwsKmsCryptoEngine.builder().kmsKeyId("k").forgottenSubjectStore(null));
  }

  @Test
  void requiredCryptoTables_reflectsTombstoneStoreDurability() {
    assertTrue(engine.requiredCryptoTables().isEmpty());
    var durable =
        engineOn(
            new ForgottenSubjectStore() {
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
            });
    assertEquals(java.util.Set.of("forgotten_subjects"), durable.requiredCryptoTables());
  }

  @Test
  void buildFailsClosed_whenNoForgottenSubjectStoreConfigured() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> AwsKmsCryptoEngine.builder().kmsClient(kmsClient).kmsKeyId("k").build());
    assertTrue(ex.getMessage().contains("ForgottenSubjectStore"), ex.getMessage());
    assertTrue(ex.getMessage().contains("durable GDPR erasure"), ex.getMessage());
  }

  @Test
  void builderRequiresKmsKeyIdAndARegionWithoutAClient() {
    assertThrows(
        IllegalStateException.class,
        () -> AwsKmsCryptoEngine.builder().kmsClient(kmsClient).build());
    assertThrows(
        IllegalStateException.class,
        () ->
            AwsKmsCryptoEngine.builder()
                .kmsClient(kmsClient)
                .kmsKeyId(" ")
                .forgottenSubjectStore(store)
                .build());
    assertThrows(
        IllegalStateException.class,
        () ->
            AwsKmsCryptoEngine.builder().kmsKeyId("some-key").forgottenSubjectStore(store).build());
    assertThrows(
        IllegalStateException.class,
        () ->
            AwsKmsCryptoEngine.builder()
                .kmsKeyId("some-key")
                .region("  ")
                .forgottenSubjectStore(store)
                .build());
    assertNotNull(
        AwsKmsCryptoEngine.builder()
            .kmsKeyId("some-key")
            .region("eu-central-1")
            .forgottenSubjectStore(store)
            .build());
  }
}

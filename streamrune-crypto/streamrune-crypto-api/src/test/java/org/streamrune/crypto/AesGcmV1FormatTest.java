package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.KeyNotFoundException;

/** Pins the v1 framing and header validation every AES-GCM engine shares. */
class AesGcmV1FormatTest {

  // The v1 fixture sealed once by the engines before the extraction; never regenerate.
  private static final String FIXTURE_KEY_HEX =
      "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
  private static final int FIXTURE_KEY_VERSION = 1;
  private static final String FIXTURE_PLAINTEXT = "characterization fixture v1";
  private static final String FIXTURE_BLOB_HEX =
      "01011e775be467702a2cb2ee23d5d5b02a29ddca96d6cd67f87d797a0172ae355c330edf58fa9ddcac9d7262"
          + "272d28e6c9a1fd844b0edcc91c";

  private static final String DIGEST = "digest-1";
  private static final byte[] PLAINTEXT = "alice@example.com".getBytes(StandardCharsets.UTF_8);
  private static final SecretKey KEY = new SecretKeySpec(new byte[32], "AES");

  private static final int FULL = AesGcmV1Format.HEADER_LENGTH + AesGcmV1Format.GCM_IV_LENGTH + 16;

  private final AesGcmV1Format format =
      new AesGcmV1Format(
          new VerifiedCipher(
              StreamRuneMetrics.NOOP,
              LoggerFactory.getLogger("test.aes-gcm-v1"),
              VerifiedCipher.FRESH_CIPHERS));

  private static byte[] blob(int length, int formatByte, int keyVersion) {
    byte[] b = new byte[length];
    if (length > 0) {
      b[0] = (byte) formatByte;
    }
    if (length > 1) {
      b[1] = (byte) keyVersion;
    }
    return b;
  }

  // ------------------------------------------------------------ validateHeader

  @Test
  void validateHeader_branches() {
    // too short, no readable header
    CryptoMappingException tooShort =
        assertThrows(
            CryptoMappingException.class,
            () -> AesGcmV1Format.validateHeader(DIGEST, new byte[1], 1));
    assertTrue(tooShort.getMessage().contains("Ciphertext too short for subject: digest-1"));
    assertTrue(tooShort.getMessage().contains("expected at least 30 bytes, got 1"));

    // a short blob with a readable erased generation is still the erased signal
    KeyNotFoundException erasedShort =
        assertThrows(
            KeyNotFoundException.class,
            () -> AesGcmV1Format.validateHeader(DIGEST, blob(2, 1, 1), 2));
    assertEquals(
        "Ciphertext was written under key generation 1, which a GDPR erasure destroyed; the"
            + " current key generation is 2 for subject: digest-1",
        erasedShort.getMessage());

    // a short blob of another format is length corruption
    assertThrows(
        CryptoMappingException.class,
        () -> AesGcmV1Format.validateHeader(DIGEST, blob(2, 9, 1), 2));

    CryptoMappingException badFormat =
        assertThrows(
            CryptoMappingException.class,
            () -> AesGcmV1Format.validateHeader(DIGEST, blob(FULL, 9, 1), 1));
    assertTrue(badFormat.getMessage().startsWith("Unsupported ciphertext format version 9"));
    assertTrue(badFormat.getMessage().contains("this engine reads format 1;"));

    CryptoMappingException zero =
        assertThrows(
            CryptoMappingException.class,
            () -> AesGcmV1Format.validateHeader(DIGEST, blob(FULL, 1, 0), 1));
    assertTrue(zero.getMessage().startsWith("Unsupported key version 0 for subject: digest-1"));
    assertTrue(zero.getMessage().contains("destroyed generations are 1..0;"));

    assertThrows(
        KeyNotFoundException.class,
        () -> AesGcmV1Format.validateHeader(DIGEST, blob(FULL, 1, 1), 3));

    AesGcmV1Format.validateHeader(DIGEST, blob(FULL, 1, 3), 3);

    CryptoMappingException higher =
        assertThrows(
            CryptoMappingException.class,
            () -> AesGcmV1Format.validateHeader(DIGEST, blob(FULL, 1, 4), 3));
    assertTrue(higher.getMessage().startsWith("Unsupported key version 4"));
    assertTrue(higher.getMessage().contains("destroyed generations are 1..2;"));
  }

  // ------------------------------------------------------------ seal / open

  @Test
  void seal_writesHeaderThenIv_andDecryptsWithAnIndependentJcaCipher() throws Exception {
    byte[] sealed = format.seal(KEY, 5, PLAINTEXT);

    assertEquals(AesGcmV1Format.HEADER_LENGTH + 12 + PLAINTEXT.length + 16, sealed.length);
    assertEquals(AesGcmV1Format.FORMAT_VERSION, sealed[0]);
    assertEquals(5, sealed[1]);
    Cipher jca = Cipher.getInstance("AES/GCM/NoPadding");
    jca.init(Cipher.DECRYPT_MODE, KEY, new GCMParameterSpec(128, sealed, 2, 12));
    assertArrayEquals(PLAINTEXT, jca.doFinal(sealed, 14, sealed.length - 14));
  }

  @Test
  void seal_twoCallsUseDifferentIvs() throws Exception {
    byte[] a = format.seal(KEY, 1, PLAINTEXT);
    byte[] b = format.seal(KEY, 1, PLAINTEXT);

    assertFalse(Arrays.equals(Arrays.copyOfRange(a, 2, 14), Arrays.copyOfRange(b, 2, 14)));
  }

  @Test
  void openOnce_roundTripsSeal() throws Exception {
    assertArrayEquals(PLAINTEXT, format.openOnce(KEY, format.seal(KEY, 1, PLAINTEXT)));
  }

  @Test
  void openOnce_isASingleAttempt() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AesGcmV1Format faulty =
        new AesGcmV1Format(
            new VerifiedCipher(
                StreamRuneMetrics.NOOP,
                LoggerFactory.getLogger("test.aes-gcm-v1"),
                (mode, key, params) -> {
                  calls.incrementAndGet();
                  throw new AEADBadTagException("boom");
                }));

    assertThrows(AEADBadTagException.class, () -> faulty.openOnce(KEY, blob(FULL, 1, 1)));
    assertEquals(1, calls.get());
  }

  @Test
  void openOnce_propagatesGeneralSecurityException() {
    byte[] sealed;
    try {
      sealed = format.seal(KEY, 1, PLAINTEXT);
    } catch (GeneralSecurityException e) {
      throw new AssertionError(e);
    }
    sealed[sealed.length - 1] ^= 1;
    assertThrows(AEADBadTagException.class, () -> format.openOnce(KEY, sealed));
  }

  @Test
  void v1FixtureOpens() throws Exception {
    SecretKey key = new SecretKeySpec(HexFormat.of().parseHex(FIXTURE_KEY_HEX), "AES");
    byte[] blob = HexFormat.of().parseHex(FIXTURE_BLOB_HEX);

    assertEquals(FIXTURE_KEY_VERSION, blob[1]);
    AesGcmV1Format.validateHeader(DIGEST, blob, FIXTURE_KEY_VERSION);
    assertEquals(FIXTURE_PLAINTEXT, new String(format.openOnce(key, blob), StandardCharsets.UTF_8));
    assertNotNull(AesGcmV1Format.MAX_KEY_VERSION);
    assertEquals(127, AesGcmV1Format.MAX_KEY_VERSION);
  }
}

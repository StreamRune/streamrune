package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;

/**
 * Pins the shared cipher seam, the encryption self-check and the one-retry decryption. The
 * messages, WARN texts and metric calls are the ones each engine carried before the extraction; the
 * engines' characterization tests prove the same from the outside.
 */
class VerifiedCipherTest {

  private static final SubjectId SUBJECT = SubjectId.of("subject-1");
  private static final String CATEGORY = "test.verified-cipher";
  private static final byte[] PLAINTEXT = "plaintext".getBytes(StandardCharsets.UTF_8);

  private static final String SELF_CHECK_MESSAGE =
      "Encryption self-check failed for subject: "
          + SUBJECT.redacted()
          + " — the encrypted value did not decrypt to its plaintext and is discarded"
          + " (transient; retrying encrypts it again)";
  private static final String SELF_CHECK_WARN =
      "Encryption self-check failed for subject "
          + SUBJECT.redacted()
          + ": the encrypted value did not decrypt to its plaintext and is discarded; the"
          + " operation fails as transient and is retried";

  private static String retryWarn(String outcome) {
    return "Decryption for subject "
        + SUBJECT.redacted()
        + " failed its authentication tag and was retried once with a fresh cipher; outcome "
        + outcome;
  }

  private final CountingMetrics metrics = new CountingMetrics();
  private LogCapture logs;
  private VerifiedCipher cipher;

  @BeforeEach
  void setUp() {
    logs = LogCapture.attach(CATEGORY);
    cipher =
        new VerifiedCipher(
            metrics, LoggerFactory.getLogger(CATEGORY), VerifiedCipher.FRESH_CIPHERS);
  }

  @AfterEach
  void tearDown() {
    logs.close();
  }

  // ---------------------------------------------------------------- verify

  @Test
  void verify_matchingPlaintext_returns_countsNothing() {
    AtomicInteger runs = new AtomicInteger();

    cipher.verify(
        SUBJECT,
        PLAINTEXT,
        () -> {
          runs.incrementAndGet();
          return PLAINTEXT.clone();
        });

    assertEquals(1, runs.get());
    assertEquals(0, metrics.selfCheckFailed);
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(), warnTexts());
  }

  @Test
  void verify_mismatch_throwsTransient_noCause_noSuppressed() {
    CryptoOperationException ex =
        assertThrows(
            CryptoOperationException.class,
            () ->
                cipher.verify(
                    SUBJECT, PLAINTEXT, () -> "different".getBytes(StandardCharsets.UTF_8)));

    assertEquals(CryptoOperationException.class, ex.getClass());
    assertEquals(SELF_CHECK_MESSAGE, ex.getMessage());
    assertNull(ex.getCause());
    assertEquals(0, ex.getSuppressed().length);
    assertEquals(1, metrics.selfCheckFailed);
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(SELF_CHECK_WARN), warnTexts());
  }

  @Test
  void verify_checkedFailure_isTheCause() {
    AEADBadTagException tagFailure = new AEADBadTagException("tag mismatch");

    CryptoOperationException ex =
        assertThrows(
            CryptoOperationException.class,
            () ->
                cipher.verify(
                    SUBJECT,
                    PLAINTEXT,
                    () -> {
                      throw tagFailure;
                    }));

    assertEquals(CryptoOperationException.class, ex.getClass());
    assertEquals(SELF_CHECK_MESSAGE, ex.getMessage());
    assertSame(tagFailure, ex.getCause());
    assertEquals(0, ex.getSuppressed().length);
    assertEquals(1, metrics.selfCheckFailed);
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(SELF_CHECK_WARN), warnTexts());
  }

  @Test
  void verify_uncheckedFailure_isSuppressed_causeNull() {
    IllegalArgumentException unchecked = new IllegalArgumentException("bad header");

    CryptoOperationException ex =
        assertThrows(
            CryptoOperationException.class,
            () ->
                cipher.verify(
                    SUBJECT,
                    PLAINTEXT,
                    () -> {
                      throw unchecked;
                    }));

    assertEquals(CryptoOperationException.class, ex.getClass());
    assertEquals(SELF_CHECK_MESSAGE, ex.getMessage());
    assertNull(ex.getCause());
    assertEquals(1, ex.getSuppressed().length);
    assertSame(unchecked, ex.getSuppressed()[0]);
    assertEquals(1, metrics.selfCheckFailed);
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(SELF_CHECK_WARN), warnTexts());
  }

  @Test
  void verify_neverRetries() {
    AtomicInteger runs = new AtomicInteger();

    assertThrows(
        CryptoOperationException.class,
        () ->
            cipher.verify(
                SUBJECT,
                PLAINTEXT,
                () -> {
                  runs.incrementAndGet();
                  throw new AEADBadTagException("tag mismatch");
                }));

    assertEquals(1, runs.get());
    assertEquals(1, metrics.selfCheckFailed);
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(SELF_CHECK_WARN), warnTexts());
  }

  // ---------------------------------------------------------------- openWithOneRetry

  @Test
  void retry_firstAttemptSucceeds_countsNothing() throws GeneralSecurityException {
    AtomicInteger runs = new AtomicInteger();

    byte[] result =
        cipher.openWithOneRetry(
            SUBJECT,
            () -> {
              runs.incrementAndGet();
              return PLAINTEXT;
            });

    assertArrayEquals(PLAINTEXT, result);
    assertEquals(1, runs.get());
    assertEquals(List.of(), metrics.retried);
    assertEquals(0, metrics.selfCheckFailed);
    assertEquals(List.of(), warnTexts());
  }

  @Test
  void retry_recoversOnSecondAttempt_countsRecovered_warnsOnce() throws GeneralSecurityException {
    AtomicInteger runs = new AtomicInteger();

    byte[] result =
        cipher.openWithOneRetry(
            SUBJECT,
            () -> {
              if (runs.incrementAndGet() == 1) {
                throw new AEADBadTagException("transient tag mismatch");
              }
              return PLAINTEXT;
            });

    assertArrayEquals(PLAINTEXT, result);
    assertEquals(2, runs.get());
    assertEquals(List.of("RECOVERED"), metrics.retried);
    assertEquals(0, metrics.selfCheckFailed);
    assertEquals(List.of(retryWarn("RECOVERED")), warnTexts());
  }

  @Test
  void retry_failsTwice_rethrowsSecondRaw_withFirstSuppressed_countsFailed() {
    AEADBadTagException first = new AEADBadTagException("first");
    AEADBadTagException second = new AEADBadTagException("second");
    AtomicInteger runs = new AtomicInteger();

    AEADBadTagException thrown =
        assertThrows(
            AEADBadTagException.class,
            () ->
                cipher.openWithOneRetry(
                    SUBJECT,
                    () -> {
                      throw runs.incrementAndGet() == 1 ? first : second;
                    }));

    assertSame(second, thrown);
    assertEquals(2, runs.get());
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(first, thrown.getSuppressed()[0]);
    assertNull(thrown.getCause());
    assertEquals(List.of("FAILED"), metrics.retried);
    assertEquals(0, metrics.selfCheckFailed);
    assertEquals(List.of(retryWarn("FAILED")), warnTexts());
  }

  @Test
  void retry_nonTagFailure_propagatesUnchanged_noRetry_noCount() {
    BadPaddingException checked = new BadPaddingException("not a tag failure");
    IllegalStateException unchecked = new IllegalStateException("not a tag failure either");
    AtomicInteger runs = new AtomicInteger();

    BadPaddingException thrownChecked =
        assertThrows(
            BadPaddingException.class,
            () ->
                cipher.openWithOneRetry(
                    SUBJECT,
                    () -> {
                      runs.incrementAndGet();
                      throw checked;
                    }));
    IllegalStateException thrownUnchecked =
        assertThrows(
            IllegalStateException.class,
            () ->
                cipher.openWithOneRetry(
                    SUBJECT,
                    () -> {
                      runs.incrementAndGet();
                      throw unchecked;
                    }));

    assertSame(checked, thrownChecked);
    assertSame(unchecked, thrownUnchecked);
    assertEquals(0, thrownChecked.getSuppressed().length);
    assertEquals(0, thrownUnchecked.getSuppressed().length);
    assertEquals(2, runs.get());
    assertEquals(List.of(), metrics.retried);
    assertEquals(0, metrics.selfCheckFailed);
    assertEquals(List.of(), warnTexts());
  }

  @Test
  void retry_nonTagFailureOnSecondAttempt_propagatesUnchanged_noCount() {
    IllegalStateException secondFailure = new IllegalStateException("second attempt broke");
    AtomicInteger runs = new AtomicInteger();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                cipher.openWithOneRetry(
                    SUBJECT,
                    () -> {
                      if (runs.incrementAndGet() == 1) {
                        throw new AEADBadTagException("first");
                      }
                      throw secondFailure;
                    }));

    assertSame(secondFailure, thrown);
    assertEquals(0, thrown.getSuppressed().length);
    assertEquals(2, runs.get());
    assertEquals(List.of(), metrics.retried);
    assertEquals(List.of(), warnTexts());
  }

  // ---------------------------------------------------------------- logger and cipher seam

  @Test
  void logsUnderTheLoggerItWasGiven() {
    String category = "test.category";
    Logger given = LoggerFactory.getLogger(category);
    VerifiedCipher owned = new VerifiedCipher(metrics, given, VerifiedCipher.FRESH_CIPHERS);

    try (LogCapture captured = LogCapture.attach(category)) {
      assertThrows(
          CryptoOperationException.class,
          () -> owned.verify(SUBJECT, PLAINTEXT, () -> new byte[0]));

      List<ILoggingEvent> warnings = captured.warnings();
      assertEquals(1, warnings.size());
      assertEquals(category, warnings.get(0).getLoggerName());
      assertEquals(SELF_CHECK_WARN, warnings.get(0).getFormattedMessage());
    }
    assertEquals(List.of(), warnTexts());
  }

  @Test
  void freshCiphers_returnsANewCipherPerCall() throws GeneralSecurityException {
    SecretKeySpec key = new SecretKeySpec(new byte[32], "AES");
    GCMParameterSpec params = new GCMParameterSpec(128, new byte[12]);

    Cipher encrypt = cipher.cipher(Cipher.ENCRYPT_MODE, key, params);
    Cipher again = cipher.cipher(Cipher.ENCRYPT_MODE, key, params);
    Cipher decrypt = cipher.cipher(Cipher.DECRYPT_MODE, key, params);

    assertNotSame(encrypt, again);
    assertNotSame(encrypt, decrypt);
    assertEquals("AES/GCM/NoPadding", encrypt.getAlgorithm());
    byte[] sealed = encrypt.doFinal(PLAINTEXT);
    assertArrayEquals(sealed, again.doFinal(PLAINTEXT));
    assertArrayEquals(PLAINTEXT, decrypt.doFinal(sealed));
  }

  @Test
  void cipher_delegatesToTheGivenFactory() throws GeneralSecurityException {
    List<Integer> modes = new ArrayList<>();
    VerifiedCipher.CipherFactory recording =
        (mode, key, params) -> {
          modes.add(mode);
          return VerifiedCipher.FRESH_CIPHERS.create(mode, key, params);
        };
    VerifiedCipher seamed =
        new VerifiedCipher(metrics, LoggerFactory.getLogger(CATEGORY), recording);

    seamed.cipher(
        Cipher.DECRYPT_MODE,
        new SecretKeySpec(new byte[16], "AES"),
        new GCMParameterSpec(128, new byte[12]));

    assertEquals(List.of(Cipher.DECRYPT_MODE), modes);
  }

  private List<String> warnTexts() {
    return logs.warnings().stream().map(ILoggingEvent::getFormattedMessage).toList();
  }

  private static final class CountingMetrics implements StreamRuneMetrics {
    int selfCheckFailed;
    final List<String> retried = new ArrayList<>();

    @Override
    public void recordEncryptSelfCheckFailed() {
      selfCheckFailed++;
    }

    @Override
    public void recordDecryptRetried(String outcome) {
      retried.add(outcome);
    }
  }
}

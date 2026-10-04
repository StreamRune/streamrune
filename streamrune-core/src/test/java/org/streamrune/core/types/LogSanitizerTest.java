package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LogSanitizer}.
 *
 * <p>Lives in {@code streamrune-core} because the sanitizer does: it is the single implementation
 * every module renders identifiers through, replacing the former integration-api copy plus the
 * private {@code SseEventPublisher.logSafe} duplicate that {@code streamrune-runtime} carried
 * because it cannot depend on the integration API.
 */
class LogSanitizerTest {

  @Test
  void nullPassesThrough() {
    assertNull(LogSanitizer.sanitizeForLog(null));
  }

  @Test
  void plainValueUnchanged() {
    assertEquals("alice", LogSanitizer.sanitizeForLog("alice"));
  }

  @Test
  void emptyStringUnchanged() {
    assertEquals("", LogSanitizer.sanitizeForLog(""));
  }

  @Test
  void crAndLfAreStripped() {
    // The exact CWE-117 payload shape: a header value carrying a newline followed by text shaped
    // like a genuine log line, so the forged line is indistinguishable from a real one once
    // interpolated by a pattern layout ("%d %level %logger - %msg%n").
    String payload =
        "x\n2026-08-07 10:00:00 WARN o.s.ScopedValueFilter - X-User-Id header 'admin' matches"
            + " authenticated principal 'admin'; authorizing as admin";
    String sanitized = LogSanitizer.sanitizeForLog(payload);
    assertFalse(sanitized.contains("\n"), "no LF must survive sanitization");
    assertFalse(sanitized.contains("\r"), "no CR must survive sanitization");
  }

  @Test
  void crlfCombinationIsStripped() {
    String sanitized = LogSanitizer.sanitizeForLog("a\r\nb\r\nc");
    assertEquals("abc", sanitized);
  }

  @Test
  void otherIsoControlCharactersAreStripped() {
    // Tab, NUL, and a C1 control character (0x85, NEL) — not just CR/LF. The NUL and the
    // NEL are written as escapes rather than as raw bytes: a literal 0x00 in the source makes
    // git classify the file as BINARY, so its diffs are invisible to every review tool and the
    // repo's grep tooling silently skips it.
    String sanitized = LogSanitizer.sanitizeForLog("a\tb\u0000c\u0085d");
    assertEquals("abcd", sanitized);
  }

  @Test
  void ordinaryPunctuationAndUnicodeSurvive() {
    assertEquals("Ünïcödé-Ok_123!", LogSanitizer.sanitizeForLog("Ünïcödé-Ok_123!"));
  }

  /**
   * U+2028 and U+2029 are not ISO controls, so the C0/C1 rule never looked at them — yet a
   * JSON-encoding appender's output is normally read by a JavaScript runtime, where both have
   * historically terminated a string literal, and several log viewers render them as hard line
   * breaks. Same forged-line outcome as the CR/LF payload above, through a character the old filter
   * passed. Written as escapes, like the NUL and NEL above, so the source stays reviewable.
   */
  @Test
  void unicodeLineAndParagraphSeparatorsAreStripped() {
    String forged = "x\u20282026-08-07 10:00:00 WARN o.s.ScopedValueFilter - authorizing as admin";
    String sanitized = LogSanitizer.sanitizeForLog(forged);
    assertFalse(sanitized.contains("\u2028"), "U+2028 LINE SEPARATOR must not survive");
    assertEquals("ab", LogSanitizer.sanitizeForLog("a\u2028b"));
    assertEquals("ab", LogSanitizer.sanitizeForLog("a\u2029b"));
  }

  /**
   * U+202E RIGHT-TO-LEFT OVERRIDE visually reverses everything after it, so an operator reading a
   * console sees a different identifier than the one that was logged. The whole {@code Cf} category
   * goes, not just the code point that motivated it — zero-widths and bidi isolates hide and
   * re-order text just as invisibly, and enumerating the known-bad is how the fourth one gets in.
   */
  @Test
  void bidiOverridesAndOtherInvisibleFormatCharactersAreStripped() {
    assertEquals("aliceadmin", LogSanitizer.sanitizeForLog("alice\u202Eadmin"), "U+202E RLO");
    assertEquals("alice", LogSanitizer.sanitizeForLog("al\u200Bi\u200Dce"), "zero-widths");
    assertEquals("alice", LogSanitizer.sanitizeForLog("\u2066alice\u2069"), "bidi isolates");
    assertEquals("alice", LogSanitizer.sanitizeForLog("\uFEFFalice"), "BOM / ZWNBSP");
    assertEquals("alice", LogSanitizer.sanitizeForLog("ali\u00ADce"), "soft hyphen");
  }

  /** A surrogate PAIR is one character to a reader and must survive intact; a lone one cannot. */
  @Test
  void surrogatePairsSurviveButUnpairedSurrogatesDoNot() {
    assertEquals("a😀b", LogSanitizer.sanitizeForLog("a😀b"));
    assertEquals("ab", LogSanitizer.sanitizeForLog("a\uD83Db"), "a lone high surrogate goes");
    assertEquals("ab", LogSanitizer.sanitizeForLog("a\uDE00b"), "a lone low surrogate goes");
  }

  /**
   * The cap counts characters, so checking {@code length() >= MAX_LENGTH} after appending could
   * leave a surrogate pair straddling it — a record ending in half a character, which no UTF-8 or
   * JSON encoder can represent. The pair is dropped whole instead.
   */
  @Test
  void truncationNeverSplitsASurrogatePair() {
    String value = "a".repeat(LogSanitizer.MAX_LENGTH - 1) + "😀" + "tail";
    String sanitized = LogSanitizer.sanitizeForLog(value);
    assertTrue(sanitized.endsWith("..."), "must still be marked truncated");
    String body = sanitized.substring(0, sanitized.length() - 3);
    assertEquals(LogSanitizer.MAX_LENGTH - 1, body.length());
    assertFalse(
        Character.isHighSurrogate(body.charAt(body.length() - 1)),
        "the record must not end on half a character: " + body);
    // One character earlier the pair fits whole and is kept.
    String fits = "a".repeat(LogSanitizer.MAX_LENGTH - 2) + "😀" + "tail";
    assertTrue(LogSanitizer.sanitizeForLog(fits).contains("😀"));
  }

  @Test
  void withinLengthCapIsNotTruncated() {
    String value = "a".repeat(LogSanitizer.MAX_LENGTH);
    assertEquals(value, LogSanitizer.sanitizeForLog(value));
  }

  @Test
  void beyondLengthCapIsTruncatedWithEllipsis() {
    String value = "a".repeat(LogSanitizer.MAX_LENGTH + 40);
    String sanitized = LogSanitizer.sanitizeForLog(value);
    assertTrue(sanitized.endsWith("..."), "truncated value must end with an ellipsis marker");
    assertEquals(LogSanitizer.MAX_LENGTH + 3, sanitized.length());
    assertEquals(
        "a".repeat(LogSanitizer.MAX_LENGTH), sanitized.substring(0, LogSanitizer.MAX_LENGTH));
  }

  @Test
  void lengthCapAppliesAfterStrippingControlCharacters() {
    // A value that is short once control characters are removed must NOT be truncated, even if
    // its raw (unsanitized) length exceeds the cap.
    String value = "ab" + "\n".repeat(200) + "cd";
    String sanitized = LogSanitizer.sanitizeForLog(value);
    assertEquals("abcd", sanitized);
  }

  @Test
  void unboundedLogVolumeAmplificationIsBoundedByTruncation() {
    // A pathologically long header (the secondary "unauthenticated log-volume amplifier" concern
    // from the finding) must not produce an unbounded log line.
    String hugePayload = "x".repeat(1_000_000);
    String sanitized = LogSanitizer.sanitizeForLog(hugePayload);
    assertTrue(sanitized.length() <= LogSanitizer.MAX_LENGTH + 3);
  }

  // === sanitizeFreeText — persisted free text: an audit row's or a dead letter's error message ===

  @Test
  void freeText_nullPassesThrough() {
    assertNull(LogSanitizer.sanitizeFreeText(null));
  }

  /**
   * The id cap would destroy the very thing an audit reader needs: an error message routinely runs
   * past {@value LogSanitizer#MAX_LENGTH} characters, so free text keeps its whole content.
   */
  @Test
  void freeText_ordinaryMessageLongerThanTheIdCap_isUnchanged() {
    String message =
        "Order ord-42 cannot be shipped: it has not been paid (status PENDING_PAYMENT, "
            + "Ünïcödé 😀, \"quoted\" and 'single' punctuation; 100% <ok> & done)";
    assertTrue(message.length() > LogSanitizer.MAX_LENGTH);
    assertEquals(message, LogSanitizer.sanitizeFreeText(message));
  }

  /**
   * A line break is the forging vector, but in free text it is also a word boundary — a PostgreSQL
   * message carries its {@code Detail:} on the next line. Each run of line-breaking or control code
   * points becomes ONE space, so the message stays readable on a single line.
   */
  @Test
  void freeText_lineBreaksAndControls_becomeOneSpacePerRun() {
    assertEquals(
        "ERROR: duplicate key   Detail: Key (id)=(1) already exists.",
        LogSanitizer.sanitizeFreeText(
            "ERROR: duplicate key\n  Detail: Key (id)=(1) already exists."));
    assertEquals("a b", LogSanitizer.sanitizeFreeText("a\r\nb"));
    assertEquals("a b c d e", LogSanitizer.sanitizeFreeText("a\u0000b\u0085c\u007Fd\te"));
    assertEquals("a b c", LogSanitizer.sanitizeFreeText("a\u2028b\u2029c"));
    String forged = "rejected\n2026-10-02 10:00:00 INFO o.s.Audit - payment approved by admin";
    assertFalse(LogSanitizer.sanitizeFreeText(forged).contains("\n"));
  }

  /**
   * NUL in particular: PostgreSQL refuses it in a {@code TEXT} column, so an unsanitized message
   * carrying one makes the audit INSERT fail and the row is lost — the sanitizer is what keeps the
   * failure on the record at all.
   */
  @Test
  void freeText_nulNeverSurvives() {
    assertFalse(LogSanitizer.sanitizeFreeText("key\u0000tail").contains("\u0000"));
  }

  @Test
  void freeText_formatCharactersAndUnpairedSurrogates_areRemovedWithoutASpace() {
    assertEquals("aliceadmin", LogSanitizer.sanitizeFreeText("alice\u202Eadmin"), "U+202E RLO");
    assertEquals("alice", LogSanitizer.sanitizeFreeText("al\u200Bi\u200Dce"), "zero-widths");
    assertEquals("ab", LogSanitizer.sanitizeFreeText("a\uD83Db"), "a lone high surrogate");
    assertEquals("a😀b", LogSanitizer.sanitizeFreeText("a😀b"), "a pair is one character");
  }

  @Test
  void freeText_withinTheTextCap_isNotTruncated() {
    String value = "a".repeat(LogSanitizer.MAX_TEXT_LENGTH);
    assertEquals(value, LogSanitizer.sanitizeFreeText(value));
  }

  @Test
  void freeText_beyondTheTextCap_isTruncatedWithEllipsis() {
    String sanitized =
        LogSanitizer.sanitizeFreeText("a".repeat(LogSanitizer.MAX_TEXT_LENGTH + 500));
    assertEquals("a".repeat(LogSanitizer.MAX_TEXT_LENGTH) + "...", sanitized);
  }

  @Test
  void freeText_truncationNeverSplitsASurrogatePair() {
    String value = "a".repeat(LogSanitizer.MAX_TEXT_LENGTH - 1) + "😀" + "tail";
    String sanitized = LogSanitizer.sanitizeFreeText(value);
    assertEquals("a".repeat(LogSanitizer.MAX_TEXT_LENGTH - 1) + "...", sanitized);
  }

  /** A message one sink already sanitized reads back identical through the next one. */
  @Test
  void freeText_isIdempotentOnAnUntruncatedValue() {
    String once = LogSanitizer.sanitizeFreeText("x\r\n\u0000y\u202Ez\u2028 w");
    assertEquals(once, LogSanitizer.sanitizeFreeText(once));
  }
}

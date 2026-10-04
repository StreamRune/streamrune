package org.streamrune.core.types;

/**
 * SINK-side rendering guard for an identifier that is about to be written into a log record
 * (CWE-117, log forging). The counterpart to {@link IdConstraints}, which guards the INGRESS side.
 *
 * <p><b>The policy, in one sentence.</b> Every identifier value the framework renders into a log
 * record — a log argument, a concatenated message, a thread name, or an exception message the
 * framework itself logs — passes through {@link #sanitizeForLog(String)} at the sink, without the
 * call site first deciding whether that particular value is trusted.
 *
 * <p><b>Why at the sink and not at the source.</b> Bounding the charset inside an identifier's
 * constructor is the shape this codebase has already been bitten by twice, and {@link
 * IdConstraints} records the post-mortem: those constructors also run on the READ path, where
 * {@code EventMetadata} is rebuilt by Jackson on every load, projection page and subscription
 * delivery, and where the DLQ/saga/audit row mappers rebuild ids from stored columns. A constraint
 * enforced there stops no write — the write already happened, through a path that never crossed the
 * ingress door — it only makes already-persisted data permanently unreadable. {@code StreamId} is
 * the same story: its four reconstruction sites ({@code PostgresEventStore}, {@code
 * PostgresCommandInbox}, {@code PostgresDeadLetterQueue}, {@code PostgresEventAuditQuery}) all
 * rebuild it from the stored {@code (aggregate_type, aggregate_id)} column pair, so a charset bound
 * in its constructor would render every already-written stream it refuses unloadable forever (its
 * only bound is the {@code aggregate_id} column's own length, which a stored value always
 * satisfies), with no self-heal because the log is append-only. Ingress validation must never be
 * repurposed as a decoding rule for data already inside.
 *
 * <p>Log forging is therefore a property of the SINK. A value is dangerous at the moment it is
 * rendered, not at the moment it is constructed, and the same stored value can be rendered by a
 * dozen sinks that the ingress door never sees.
 *
 * <p><b>Why uniformly, and not "only the untrusted ones".</b> The two gaps this class closes were
 * both produced by the previous shape — a helper each call site had to remember to call, after
 * first judging whether the value was attacker-influenced. Both judgements were locally reasonable
 * and both were wrong: {@code SagaTimeoutRunner}'s WARNING interpolated a stored {@code sagaId}
 * that originally came from correlated event data, and the three request filters sanitized the
 * {@code X-User-Id} header while leaving the authenticated principal name unsanitized in the very
 * same statement. A rule with a per-site trust judgement in it rots at the first new call site.
 * This rule has none: for a framework-generated id (a {@code cmd_}-prefixed {@link CommandId}, a
 * UUID) the transform is the identity function, so applying it uniformly costs nothing and removes
 * the judgement that was the actual defect.
 *
 * <p><b>What this does.</b> Strips every {@link Character#isISOControl(char) ISO control character}
 * — {@code \r}, {@code \n} and the rest of the C0/C1 ranges, none of which has a legitimate place
 * in an identifier — and truncates to {@value #MAX_LENGTH} characters with a trailing {@code "..."}
 * marker. With any conventional pattern layout ({@code %d %level %logger - %msg%n}), an unstripped
 * {@code \n} followed by text shaped like a genuine log line makes the logging framework emit that
 * forged line as a real entry, indistinguishable to an operator or a downstream SIEM. Unbounded
 * length is a secondary log-volume amplification vector.
 *
 * <p><b>A control character is not the only way to break or reorder a log line.</b> "ISO control"
 * is a C0/C1 rule, and three Unicode categories outside it do the same damage in the tools
 * operators actually read:
 *
 * <ul>
 *   <li><b>{@code Zl} / {@code Zp}</b> — U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR.
 *       Inert under a plain pattern layout, but a JSON-encoding appender's output is normally
 *       consumed by a JavaScript runtime, where both have historically terminated a string literal,
 *       and several log viewers render them as hard line breaks — the same forged-line outcome
 *       CR/LF is stripped for.
 *   <li><b>{@code Cf}</b> — format characters. U+202E RIGHT-TO-LEFT OVERRIDE visually reverses
 *       everything after it, so an identifier can be made to read in a console as an entirely
 *       different one; the zero-width characters and bidi isolates in the same category hide or
 *       re-order text just as invisibly. The whole category is stripped rather than the three code
 *       points that motivated this, because "enumerate the known-bad and miss the fourth" is the
 *       failure mode this class exists because of.
 *   <li><b>{@code Cs}</b> — an UNPAIRED surrogate, which has no UTF-8 encoding at all and makes a
 *       JSON-encoding appender emit a replacement character or drop the record.
 * </ul>
 *
 * <p>The scan is by CODE POINT rather than by {@code char} for the same reason: a surrogate PAIR is
 * one character to the reader, and the length cap must not cut it in half and leave the record
 * ending in half a character.
 *
 * <p><b>Rendering only.</b> The result silently differs from the input, so it must never be used
 * for comparison, storage, key derivation or any other business logic.
 *
 * <p><b>Free text persisted into a row.</b> An exception message the framework writes into an
 * {@code audit_log} or dead-letter row is rendered by every tool that later reads that row — an
 * admin console, a SIEM export, a log line quoting it — and it can carry caller-supplied text (an
 * idempotency key, a field value a validation message echoes). {@link #sanitizeFreeText(String)} is
 * the persisted-text sink's counterpart to {@link #sanitizeForLog(String)}: the same code-point
 * classification, but each run of line-breaking or control characters becomes ONE space (in free
 * text a line break is also a word boundary) and the cap is {@value #MAX_TEXT_LENGTH} characters,
 * because an error message is the whole content of the column, not an id quoted inside a line. A
 * NUL in particular never reaches PostgreSQL, which refuses it in a {@code TEXT} column — an
 * unsanitized one made the audit INSERT fail, and the row was lost.
 *
 * <p><b>What this deliberately does not cover.</b> A {@link SubjectId} is not sanitized but {@link
 * SubjectId#redacted() redacted}: it may itself be personal data, so the sink needs a hash, not a
 * control-stripped copy. Values that reach a log record through a channel with no argument — a
 * thread name rendered by {@code %thread} on every line the thread emits — must be sanitized where
 * the name is built, since no argument-level transform can reach them.
 */
public final class LogSanitizer {

  /**
   * Maximum length of a sanitized value before truncation (an ellipsis is appended beyond it).
   * Deliberately far below {@link IdConstraints#MAX_LENGTH}: the ingress bound exists to keep a
   * legitimate identifier intact, while this bound exists to keep one log line readable.
   */
  static final int MAX_LENGTH = 64;

  /**
   * Maximum length of sanitized free text (an error message persisted into an audit or dead-letter
   * row) before truncation (an ellipsis is appended beyond it). Far above any message a reader
   * needs in full, far below the unbounded {@code TEXT} column it guards.
   */
  static final int MAX_TEXT_LENGTH = 2048;

  private LogSanitizer() {}

  /**
   * Strips control, line/paragraph-separator, format and unpaired-surrogate code points, and
   * truncates, for safe rendering into a log record.
   *
   * @param value the value about to be rendered; {@code null} is passed through so a nullable id
   *     still renders as {@code "null"} rather than throwing inside a log statement
   * @return a value safe to render on a single log line, or {@code null} if {@code value} was
   *     {@code null}
   */
  public static String sanitizeForLog(String value) {
    if (value == null) {
      return null;
    }
    StringBuilder stripped = new StringBuilder(Math.min(value.length(), MAX_LENGTH));
    boolean truncated = false;
    int i = 0;
    while (i < value.length()) {
      int codePoint = value.codePointAt(i);
      int width = Character.charCount(codePoint);
      i += width;
      if (isUnsafeForALogLine(codePoint)) {
        continue;
      }
      // Reserve room for the WHOLE code point. Checking `length() >= MAX_LENGTH`
      // after the fact would let a surrogate pair straddle the cap and leave the record ending in
      // half a character.
      if (stripped.length() + width > MAX_LENGTH) {
        truncated = true;
        break;
      }
      stripped.appendCodePoint(codePoint);
    }
    if (truncated) {
      stripped.append("...");
    }
    return stripped.toString();
  }

  /**
   * Makes free text — an exception message about to be persisted into an audit or dead-letter row —
   * safe for every reader of that row: each run of control (C0, DEL, C1) and line/paragraph
   * separator code points becomes one space, format and unpaired-surrogate code points are removed,
   * and the result is truncated to {@value #MAX_TEXT_LENGTH} characters with a trailing {@code
   * "..."} marker. Applying it to its own output changes nothing unless that output was truncated.
   *
   * <p><b>Persistence only at the sink.</b> Like {@link #sanitizeForLog(String)}, the result
   * differs from the input; it is what the framework WRITES into a text column meant for human
   * readers, never a value to compare, key on or derive anything from.
   *
   * @param value the text about to be persisted; {@code null} passes through
   * @return the sanitized text, or {@code null} if {@code value} was {@code null}
   */
  public static String sanitizeFreeText(String value) {
    if (value == null) {
      return null;
    }
    StringBuilder out = new StringBuilder(Math.min(value.length(), MAX_TEXT_LENGTH));
    boolean truncated = false;
    boolean inBreakRun = false;
    int i = 0;
    while (i < value.length()) {
      int codePoint = value.codePointAt(i);
      int width = Character.charCount(codePoint);
      i += width;
      int rendered;
      if (breaksTheLine(codePoint)) {
        if (inBreakRun) {
          continue;
        }
        inBreakRun = true;
        rendered = ' ';
      } else if (isUnsafeForALogLine(codePoint)) {
        // Cf / unpaired Cs: invisible, so removed outright — and without ending a break run, so
        // "\n<ZWSP>\n" is still one space.
        continue;
      } else {
        inBreakRun = false;
        rendered = codePoint;
      }
      int renderedWidth = Character.charCount(rendered);
      // Reserve room for the WHOLE code point, as sanitizeForLog does.
      if (out.length() + renderedWidth > MAX_TEXT_LENGTH) {
        truncated = true;
        break;
      }
      out.appendCodePoint(rendered);
    }
    if (truncated) {
      out.append("...");
    }
    return out.toString();
  }

  /**
   * ISO controls (C0, DEL, C1) and the Unicode line/paragraph separators: a line break to a reader.
   */
  private static boolean breaksTheLine(int codePoint) {
    if (Character.isISOControl(codePoint)) {
      return true;
    }
    int type = Character.getType(codePoint);
    return type == Character.LINE_SEPARATOR || type == Character.PARAGRAPH_SEPARATOR;
  }

  /**
   * Whether {@code codePoint} can break, reorder or corrupt the log line it is rendered into. See
   * the class-level notes for why the three Unicode categories below sit alongside the ISO controls
   * rather than being enumerated code point by code point.
   */
  private static boolean isUnsafeForALogLine(int codePoint) {
    if (Character.isISOControl(codePoint)) {
      return true;
    }
    int type = Character.getType(codePoint);
    return type == Character.LINE_SEPARATOR // Zl — U+2028
        || type == Character.PARAGRAPH_SEPARATOR // Zp — U+2029
        || type == Character.FORMAT // Cf — U+202E RLO, zero-widths, bidi isolates
        || type == Character.SURROGATE; // Cs — unpaired only; a pair decodes to its real category
  }
}

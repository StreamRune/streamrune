package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * Log forging — the identity-mismatch WARN in {@link ScopedValueFilter} logs the raw {@code
 * X-User-Id} header value, which is attacker-controlled and unvalidated beyond non-blank. Drives
 * the real filter with a header carrying CR/LF shaped like a forged log line and captures the
 * ACTUAL formatted log message via a real Logback appender (this codebase's existing convention for
 * asserting on log content, see e.g. {@code PostgresNotificationSubscriptionTest}), proving the
 * forged line never reaches the log record.
 */
class ScopedValueFilterLogSanitizationTest {

  /** Captures WARN+ events of the given logger for the duration of {@code body}. */
  private static List<String> warnMessagesDuring(Class<?> loggerOwner, Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(loggerOwner);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void mismatchWarningDoesNotEmitClientSuppliedNewlines() throws Exception {
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    // The exact CWE-117 shape from the finding: a newline followed by text shaped like a genuine
    // log line, so a pattern layout would emit it as if it were a real, separate log entry.
    String forgedPayload =
        "attacker\n2026-08-07 10:00:00 WARN o.s.ScopedValueFilter - X-User-Id header 'admin'"
            + " matches authenticated principal 'admin'; authorizing as admin";
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader(forgedPayload));
    FilterChain chain = (req, res) -> {};

    List<String> warnings =
        warnMessagesDuring(
            ScopedValueFilter.class,
            () -> {
              try {
                filter.doFilter(request, response, chain);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    assertEquals(1, warnings.size(), "exactly one WARN must be emitted for the mismatch");
    String formatted = warnings.get(0);
    assertFalse(formatted.contains("\n"), "no LF from the header must survive into the log record");
    assertFalse(formatted.contains("\r"), "no CR from the header must survive into the log record");
    // The sanitized (stripped) header content is still present, just without the newline —
    // confirms this is sanitization, not a silently dropped/replaced log entirely.
    assertTrue(formatted.contains("attacker"));
  }

  @Test
  void mismatchWarningTruncatesAPathologicallyLongHeader() throws Exception {
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter = new ScopedValueFilter(Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    var request = mock(HttpServletRequest.class);
    var response = mock(HttpServletResponse.class);
    when(request.getHeaders("X-User-Id"))
        .thenAnswer(invocation -> TestServletHeaders.userIdHeader("x".repeat(10_000)));
    FilterChain chain = (req, res) -> {};

    List<String> warnings =
        warnMessagesDuring(
            ScopedValueFilter.class,
            () -> {
              try {
                filter.doFilter(request, response, chain);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    assertEquals(1, warnings.size());
    // The formatted message contains fixed surrounding text plus the (truncated) header value —
    // it must not be anywhere near 10,000 characters long (the unbounded-log-volume concern).
    assertTrue(
        warnings.get(0).length() < 500,
        "a pathologically long header must not inflate the log line");
  }
}

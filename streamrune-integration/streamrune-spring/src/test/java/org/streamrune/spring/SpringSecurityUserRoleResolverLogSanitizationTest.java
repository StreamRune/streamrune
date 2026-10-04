package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.types.UserId;

/**
 * Log forging — {@link SpringSecurityUserRoleResolver}'s identity-mismatch DEBUG log carries the
 * requested {@code userId}, which per the class javadoc "may originate from a different source than
 * the SecurityContext (e.g. the X-User-Id header read by ScopedValueFilter)" — i.e.
 * attacker-controlled. Proves the forged-log-line payload never reaches the log record, mirroring
 * {@code ScopedValueFilterLogSanitizationTest}.
 */
class SpringSecurityUserRoleResolverLogSanitizationTest {

  private final SpringSecurityUserRoleResolver resolver = new SpringSecurityUserRoleResolver();

  @AfterEach
  void clearSecurityContext() {
    SecurityContextHolder.clearContext();
  }

  private static List<String> debugMessagesDuring(Class<?> loggerOwner, Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(loggerOwner);
    var originalLevel = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.DEBUG);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(originalLevel);
    }
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.DEBUG))
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void mismatchDebugLogDoesNotEmitClientSuppliedNewlines() {
    var auth = new TestingAuthenticationToken("bob", null, List.of());
    SecurityContextHolder.getContext().setAuthentication(auth);
    String forgedPayload =
        "attacker\n2026-08-07 10:00:00 DEBUG o.s.SpringSecurityUserRoleResolver - Requested"
            + " userId 'admin' does not match authenticated principal 'admin'";
    UserId maliciousUserId = UserId.of(forgedPayload);

    List<String> logs =
        debugMessagesDuring(
            SpringSecurityUserRoleResolver.class, () -> resolver.resolve(maliciousUserId));

    assertEquals(1, logs.size(), "exactly one DEBUG must be emitted for the mismatch");
    assertFalse(logs.get(0).contains("\n"), "no LF from userId must survive into the log record");
    assertFalse(logs.get(0).contains("\r"), "no CR from userId must survive into the log record");
  }

  /**
   * The second identifier in the SAME statement. Until this branch, {@code auth.getName()} was
   * rendered raw next to the sanitized header value, justified by a comment calling it "the
   * trusted, already-authenticated principal". Authentication proves the identity provider asserted
   * the name; it proves nothing about its charset. A JWT {@code sub} or a self-service signup
   * username carrying CR/LF is authenticated AND forges a log line, and the per-site trust
   * judgement is precisely what let it through.
   */
  @Test
  void mismatchDebugLogDoesNotEmitAForgedAuthenticatedPrincipalName() {
    String forgedPrincipal =
        "bob\n2026-08-07 10:00:00 DEBUG o.s.SpringSecurityUserRoleResolver - Requested userId"
            + " 'admin' matches authenticated principal 'admin'";
    SecurityContextHolder.getContext()
        .setAuthentication(new TestingAuthenticationToken(forgedPrincipal, null, List.of()));

    List<String> logs =
        debugMessagesDuring(
            SpringSecurityUserRoleResolver.class, () -> resolver.resolve(UserId.of("alice")));

    assertEquals(1, logs.size(), "exactly one DEBUG must be emitted for the mismatch");
    assertFalse(
        logs.get(0).contains("\n") || logs.get(0).contains("\r"),
        "the authenticated principal name must not open a second log line: " + logs.get(0));
    assertTrue(
        logs.get(0).contains("bob"), "the principal must still be identifiable, just declawed");
  }
}

package org.streamrune.quarkus;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * Log forging — {@link StreamRuneRequestFilter}'s identity-mismatch WARN and {@link
 * QuarkusSecurityUserRoleResolver}'s identity-mismatch DEBUG both log an attacker-controlled value
 * (the {@code X-User-Id} header, or a {@code userId} that may originate from it). Proves each call
 * site actually routes that value through {@link LogSanitizer#sanitizeForLog(String)} before it
 * reaches the logger.
 *
 * <p>Uses {@code Mockito.mockStatic} rather than capturing real log output: this module's test
 * environment binds SLF4J to JBoss Logging ({@code org.slf4j.impl.Slf4jLogger}), not Logback, so
 * the {@code ch.qos.logback.core.read.ListAppender} capture this codebase otherwise uses (see
 * {@code ScopedValueFilterLogSanitizationTest} on Spring, where the binding IS Logback) is not
 * available here. Static-mocking the sanitizer call site is binding-independent and verifies
 * exactly what changed: that the raw header value is routed through {@code sanitizeForLog} before
 * being handed to the logger, wherever the logger sends it.
 */
class LogSanitizationTest {

  @Test
  void filterMismatchWarningRoutesHeaderThroughSanitizer() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter =
        new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(resolver, false), null);
    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    String forgedPayload = "attacker\n2026-08-07 10:00:00 WARN forged line";
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader(forgedPayload));
    when(rc.getHeaderString("X-Trace-Id")).thenReturn(null);
    when(rc.getHeaderString("X-Correlation-Id")).thenReturn(null);
    when(rc.getHeaderString("X-User-Role")).thenReturn(null);

    try (var mocked = mockStatic(LogSanitizer.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      filter.filter(rc);
      mocked.verify(() -> LogSanitizer.sanitizeForLog(eq(forgedPayload)));
    }
  }

  @Test
  void resolverMismatchDebugRoutesUserIdThroughSanitizer() {
    var identity = mock(io.quarkus.security.identity.SecurityIdentity.class);
    var principal = mock(java.security.Principal.class);
    when(identity.isAnonymous()).thenReturn(false);
    when(identity.getPrincipal()).thenReturn(principal);
    when(principal.getName()).thenReturn("admin");
    var resolver = new QuarkusSecurityUserRoleResolver(identity);
    String forgedPayload = "attacker\n2026-08-07 10:00:00 DEBUG forged line";
    UserId maliciousUserId = UserId.of(forgedPayload);

    try (var mocked = mockStatic(LogSanitizer.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      resolver.resolve(maliciousUserId);
      mocked.verify(() -> LogSanitizer.sanitizeForLog(eq(forgedPayload)));
    }
  }
}

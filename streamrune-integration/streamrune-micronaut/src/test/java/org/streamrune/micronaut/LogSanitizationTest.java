package org.streamrune.micronaut;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * Log forging — {@link StreamRuneContextFilter}'s identity-mismatch WARN and {@link
 * MicronautSecurityUserRoleResolver}'s identity-mismatch DEBUG both log an attacker-controlled
 * value. Proves each call site routes that value through {@link
 * LogSanitizer#sanitizeForLog(String)} before it reaches the logger.
 *
 * <p>Uses {@code Mockito.mockStatic} rather than capturing real log output: when this was written
 * this module's test environment bound SLF4J to a NOP logger ({@code org.slf4j.helpers.NOPLogger}),
 * so no appender-based capture could observe anything here (logback-classic joined the test
 * classpath later, for the SseController WARN tests). Static-mocking the sanitizer call site is
 * independent of the logging backend and verifies exactly what changed: that the raw value is
 * routed through {@code sanitizeForLog} before being handed to the logger.
 */
class LogSanitizationTest {

  @AfterEach
  void cleanup() {
    StreamRuneContextHelper.remove();
  }

  @SuppressWarnings("unchecked")
  private static HttpRequest<Object> requestWithUserId(String userIdHeader) {
    HttpRequest<Object> request = mock(HttpRequest.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(request.getHeaders()).thenReturn(headers);
    when(headers.getAll("X-User-Id"))
        .thenReturn(userIdHeader == null ? List.of() : List.of(userIdHeader));
    return request;
  }

  @Test
  void filterMismatchWarningRoutesHeaderThroughSanitizer() {
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var filter =
        new StreamRuneContextFilter(
            StreamRuneMicronautProperties.withDefaults(),
            RequestIdentityPolicy.of(resolver, false),
            null);
    String forgedPayload = "attacker\n2026-08-07 10:00:00 WARN forged line";

    try (var mocked = mockStatic(LogSanitizer.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      filter.buildContext(requestWithUserId(forgedPayload));
      mocked.verify(() -> LogSanitizer.sanitizeForLog(eq(forgedPayload)));
    }
  }

  @Test
  void resolverMismatchDebugRoutesUserIdThroughSanitizer() {
    var securityService = mock(io.micronaut.security.utils.SecurityService.class);
    var authentication = mock(io.micronaut.security.authentication.Authentication.class);
    when(securityService.getAuthentication()).thenReturn(Optional.of(authentication));
    when(authentication.getName()).thenReturn("admin");
    var resolver = new MicronautSecurityUserRoleResolver(securityService);
    String forgedPayload = "attacker\n2026-08-07 10:00:00 DEBUG forged line";
    UserId maliciousUserId = UserId.of(forgedPayload);

    try (var mocked = mockStatic(LogSanitizer.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      resolver.resolve(maliciousUserId);
      mocked.verify(() -> LogSanitizer.sanitizeForLog(eq(forgedPayload)));
    }
  }
}

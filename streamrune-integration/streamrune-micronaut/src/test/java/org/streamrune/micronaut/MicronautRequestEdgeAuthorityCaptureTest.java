package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * Off-request authorization, Micronaut half of the three-integration parity. Mirrors {@code
 * org.streamrune.quarkus.QuarkusRequestEdgeAuthorityCaptureTest} and the Spring {@code
 * OffRequestAuthorizationTest}: the request edge resolves the caller's authorities once, while
 * {@code SecurityService} can still see this request's authentication.
 */
class MicronautRequestEdgeAuthorityCaptureTest {

  private static final UserId ADMIN = UserId.of("admin-user");
  private static final UserAuthority ADMIN_AUTHORITY = new UserAuthority(Set.of("ADMIN"), Set.of());

  private static HttpRequest<?> request() {
    HttpRequest<?> request = mock(HttpRequest.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(request.getHeaders()).thenReturn(headers);
    when(headers.get("X-Trace-Id")).thenReturn("trace-1");
    when(headers.get("X-Correlation-Id")).thenReturn("corr-1");
    when(headers.getAll("X-User-Id")).thenReturn(List.of("admin-user"));
    when(headers.getAll("X-User-Role")).thenReturn(List.of());
    return request;
  }

  private static UserRoleResolver requestScopedResolver() {
    return new UserRoleResolver() {
      @Override
      public UserAuthority resolve(UserId userId) {
        return ADMIN.equals(userId) ? ADMIN_AUTHORITY : UserAuthority.EMPTY;
      }

      @Override
      public boolean requiresRequestContext() {
        return true;
      }
    };
  }

  /**
   * Capture ordering, Micronaut half. {@code buildContext} captures before it hands the context to
   * the filter, which only then binds the {@code ScopedValue} around the chain — so without an
   * explicit binding inside the capture the resolver ran with nothing bound.
   */
  @Test
  void filterBindsTheRequestContextWhileTheResolverRuns() {
    UserRoleResolver readsTheRequest =
        new UserRoleResolver() {
          @Override
          public UserAuthority resolve(UserId userId) {
            var bound = StreamRuneContext.capture();
            if (bound == null) {
              return UserAuthority.EMPTY;
            }
            String role = bound.baggage().get("role");
            return role == null ? UserAuthority.EMPTY : new UserAuthority(Set.of(role), Set.of());
          }

          @Override
          public boolean requiresRequestContext() {
            return true;
          }
        };

    // The baggage role it reads is the X-User-Role header, which only a trusted gateway forwards
    // into baggage; the identity is the gateway's X-User-Id too.
    HttpRequest<?> request = request();
    when(request.getHeaders().getAll("X-User-Role")).thenReturn(List.of("ADMIN"));

    var ctx =
        new StreamRuneContextFilter(
                StreamRuneMicronautProperties.withDefaults(),
                RequestIdentityPolicy.of(null, true),
                readsTheRequest)
            .buildContext(request);

    assertEquals(ADMIN_AUTHORITY, ctx.authority());
    assertNull(
        StreamRuneContext.capture(),
        "the capture binding must not outlive buildContext — the filter binds its own copy, the one"
            + " that actually carries the authority, around the downstream chain");
  }

  @Test
  void filterCapturesTheAuthorityForARequestScopedResolver() {
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);
    var filter =
        new StreamRuneContextFilter(
            StreamRuneMicronautProperties.withDefaults(),
            RequestIdentityPolicy.of(principal, false),
            requestScopedResolver());

    var ctx = filter.buildContext(request());

    assertEquals(ADMIN, ctx.userId());
    assertEquals(ADMIN_AUTHORITY, ctx.authority());
  }

  @Test
  void filterCapturesNothingForAResolverThatWorksOffRequest() {
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);
    UserRoleResolver pureResolver = userId -> ADMIN_AUTHORITY;
    var filter =
        new StreamRuneContextFilter(
            StreamRuneMicronautProperties.withDefaults(),
            RequestIdentityPolicy.of(principal, false),
            pureResolver);

    assertNull(
        filter.buildContext(request()).authority(),
        "a resolver that already answers off-request must not be called on every inbound request");
  }

  @Test
  void filterWithNoRoleResolverCapturesNoAuthority() {
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);
    var filter =
        new StreamRuneContextFilter(
            StreamRuneMicronautProperties.withDefaults(),
            RequestIdentityPolicy.of(principal, false),
            null);

    var ctx = filter.buildContext(request());

    assertEquals(ADMIN, ctx.userId());
    assertNull(ctx.authority());
  }

  @Test
  void theShippedResolverDeclaresThatItNeedsARequestContext() {
    SecurityService securityService = mock(SecurityService.class);
    Authentication authentication = mock(Authentication.class);
    when(authentication.getName()).thenReturn("admin-user");
    when(authentication.getRoles()).thenReturn(List.of("ADMIN"));
    when(securityService.getAuthentication()).thenReturn(Optional.of(authentication));
    var resolver = new MicronautSecurityUserRoleResolver(securityService);

    assertTrue(
        resolver.requiresRequestContext(),
        "SecurityService resolves the CURRENT request's authentication; declaring otherwise would"
            + " let the framework consult this resolver on a background thread and obey its empty"
            + " answer");
    assertEquals(ADMIN_AUTHORITY, resolver.resolve(ADMIN));
  }
}

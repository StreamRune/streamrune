package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.security.Principal;
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
 * Off-request authorization, Quarkus half of the three-integration parity.
 *
 * <p>Quarkus is the integration where carrying the authority inside the {@code RequestContext} is
 * not merely convenient but necessary: a JAX-RS {@code ContainerRequestFilter} cannot wrap the
 * resource-method invocation, so it cannot bind a {@code ScopedValue} around anything. The context
 * object it stores in the request-scoped holder is the ONLY thing that reaches the code which later
 * calls {@code withContext(...)} — so the authority has to travel in it. These tests pin that the
 * filter puts it there while the CDI request scope is still active.
 */
class QuarkusRequestEdgeAuthorityCaptureTest {

  private static final UserId ADMIN = UserId.of("admin-user");
  private static final UserAuthority ADMIN_AUTHORITY = new UserAuthority(Set.of("ADMIN"), Set.of());

  private static ContainerRequestContext requestFor(String userIdHeader) {
    ContainerRequestContext rc = mock(ContainerRequestContext.class);
    when(rc.getHeaders()).thenReturn(TestRequestHeaders.userIdHeader(userIdHeader));
    return rc;
  }

  @Test
  void filterCapturesTheAuthorityForARequestScopedResolver() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);
    UserRoleResolver roleResolver =
        new UserRoleResolver() {
          @Override
          public UserAuthority resolve(UserId userId) {
            return ADMIN.equals(userId) ? ADMIN_AUTHORITY : UserAuthority.EMPTY;
          }

          @Override
          public boolean requiresRequestContext() {
            return true;
          }
        };

    new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(principal, false), roleResolver)
        .filter(requestFor("admin-user"));

    assertEquals(
        ADMIN_AUTHORITY,
        holder.context().authority(),
        "the authority must be captured while the CDI request scope is still active — nothing"
            + " downstream can revive SecurityIdentity off-request");
  }

  /**
   * Capture ordering, Quarkus half. A resolver declaring {@code requiresRequestContext()} must be
   * able to read the framework's own per-request state — {@code StreamRuneContext.CURRENT} — during
   * the capture. On Quarkus this is the only place it ever could: a JAX-RS filter cannot wrap the
   * resource method, so the binding happens later, inside {@code withContext(...)}, long after the
   * filter has returned.
   */
  @Test
  void filterBindsTheRequestContextWhileTheResolverRuns() {
    var holder = new StreamRuneRequestContextHolder();
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
    ContainerRequestContext rc = requestFor("admin-user");
    rc.getHeaders().add("X-User-Role", "ADMIN");

    new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(null, true), readsTheRequest)
        .filter(rc);

    assertEquals(ADMIN_AUTHORITY, holder.context().authority());
    assertNull(
        StreamRuneContext.capture(),
        "the capture binding must not leak past the filter — Quarkus binds the context explicitly"
            + " later, and a leaked binding would be a different (authority-less) object");
  }

  @Test
  void filterCapturesNothingForAResolverThatWorksOffRequest() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);
    UserRoleResolver pureResolver = userId -> ADMIN_AUTHORITY;

    new StreamRuneRequestFilter(
            holder, Set.of(), RequestIdentityPolicy.of(principal, false), pureResolver)
        .filter(requestFor("admin-user"));

    assertNull(
        holder.context().authority(),
        "a resolver that already answers off-request must not be called on every inbound request");
  }

  @Test
  void filterWithNoRoleResolverCapturesNoAuthority() {
    var holder = new StreamRuneRequestContextHolder();
    AuthenticatedUserResolver principal = () -> Optional.of(ADMIN);

    new StreamRuneRequestFilter(holder, Set.of(), RequestIdentityPolicy.of(principal, false), null)
        .filter(requestFor("admin-user"));

    assertNull(holder.context().authority());
    assertEquals(ADMIN, holder.context().userId());
  }

  @Test
  void theShippedResolverDeclaresThatItNeedsARequestContext() {
    SecurityIdentity identity = mock(SecurityIdentity.class);
    Principal principal = mock(Principal.class);
    when(principal.getName()).thenReturn("admin-user");
    when(identity.getPrincipal()).thenReturn(principal);
    when(identity.getRoles()).thenReturn(Set.of("ADMIN"));
    var resolver = new QuarkusSecurityUserRoleResolver(identity);

    assertTrue(
        resolver.requiresRequestContext(),
        "SecurityIdentity is CDI request-scoped; declaring otherwise would let the framework"
            + " consult this resolver on a background thread and obey its empty answer");
    assertEquals(ADMIN_AUTHORITY, resolver.resolve(ADMIN));
  }
}

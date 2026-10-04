package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.UserId;

class MicronautSecurityUserRoleResolverTest {

  private static Authentication authentication(String name, Collection<String> roles) {
    Authentication auth = mock(Authentication.class);
    lenient().when(auth.getName()).thenReturn(name);
    lenient().when(auth.getRoles()).thenReturn(roles);
    return auth;
  }

  private static SecurityService securityServiceFor(Authentication auth) {
    SecurityService securityService = mock(SecurityService.class);
    when(securityService.getAuthentication()).thenReturn(Optional.of(auth));
    return securityService;
  }

  @Test
  void resolves_roles_from_current_authentication() {
    Authentication auth = authentication("user", List.of("ADMIN", "USER"));

    var resolver = new MicronautSecurityUserRoleResolver(securityServiceFor(auth));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertTrue(authority.hasRole("ADMIN"));
    assertTrue(authority.hasRole("USER"));
  }

  @Test
  void returns_empty_permissions() {
    Authentication auth = authentication("user", List.of("ADMIN"));

    var resolver = new MicronautSecurityUserRoleResolver(securityServiceFor(auth));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertTrue(authority.permissions().isEmpty());
  }

  @Test
  void returns_EMPTY_when_no_roles() {
    Authentication auth = authentication("user", List.of());

    var resolver = new MicronautSecurityUserRoleResolver(securityServiceFor(auth));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_roles_null() {
    Authentication auth = authentication("user", null);

    var resolver = new MicronautSecurityUserRoleResolver(securityServiceFor(auth));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_unauthenticated() {
    SecurityService securityService = mock(SecurityService.class);
    when(securityService.getAuthentication()).thenReturn(Optional.empty());

    var resolver = new MicronautSecurityUserRoleResolver(securityService);
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_does_not_match_authenticated_principal() {
    // userId may come from a different source (e.g. X-User-Id header) than the security context;
    // roles must never be attributed — and cached — under a mismatched user ID
    Authentication auth = authentication("alice", List.of("ADMIN"));

    var resolver = new MicronautSecurityUserRoleResolver(securityServiceFor(auth));
    UserAuthority authority = resolver.resolve(UserId.of("bob"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_null() {
    SecurityService securityService = mock(SecurityService.class);

    var resolver = new MicronautSecurityUserRoleResolver(securityService);
    UserAuthority authority = resolver.resolve(null);

    assertEquals(UserAuthority.EMPTY, authority);
    verifyNoInteractions(securityService);
  }

  @Test
  void resolves_per_call_not_per_instance() {
    Authentication first = authentication("a", List.of("ADMIN"));
    Authentication second = authentication("b", List.of("USER"));
    SecurityService securityService = mock(SecurityService.class);
    when(securityService.getAuthentication())
        .thenReturn(Optional.of(first))
        .thenReturn(Optional.of(second));

    var resolver = new MicronautSecurityUserRoleResolver(securityService);

    assertTrue(resolver.resolve(UserId.of("a")).hasRole("ADMIN"));
    assertTrue(resolver.resolve(UserId.of("b")).hasRole("USER"));
  }
}

package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.quarkus.security.identity.SecurityIdentity;
import java.security.Principal;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.UserId;

class QuarkusSecurityUserRoleResolverTest {

  private SecurityIdentity identityFor(String principalName, Set<String> roles) {
    Principal principal = mock(Principal.class);
    lenient().when(principal.getName()).thenReturn(principalName);
    SecurityIdentity identity = mock(SecurityIdentity.class);
    lenient().when(identity.isAnonymous()).thenReturn(false);
    lenient().when(identity.getPrincipal()).thenReturn(principal);
    lenient().when(identity.getRoles()).thenReturn(roles);
    return identity;
  }

  @Test
  void resolves_roles_from_security_identity() {
    var resolver =
        new QuarkusSecurityUserRoleResolver(identityFor("user", Set.of("ADMIN", "USER")));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertTrue(authority.hasRole("ADMIN"));
    assertTrue(authority.hasRole("USER"));
  }

  @Test
  void returns_empty_permissions() {
    var resolver = new QuarkusSecurityUserRoleResolver(identityFor("user", Set.of("ADMIN")));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertTrue(authority.roles().contains("ADMIN"));
    assertTrue(authority.permissions().isEmpty());
  }

  @Test
  void returns_EMPTY_when_identity_has_no_roles() {
    var resolver = new QuarkusSecurityUserRoleResolver(identityFor("user", Set.of()));
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_does_not_match_principal() {
    // userId may come from a different source (e.g. X-User-Id header) than the SecurityIdentity;
    // roles must never be attributed — and cached — under a mismatched user ID
    var resolver = new QuarkusSecurityUserRoleResolver(identityFor("alice", Set.of("ADMIN")));
    UserAuthority authority = resolver.resolve(UserId.of("bob"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_null() {
    var resolver = new QuarkusSecurityUserRoleResolver(identityFor("alice", Set.of("ADMIN")));
    UserAuthority authority = resolver.resolve(null);

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_identity_anonymous() {
    SecurityIdentity identity = mock(SecurityIdentity.class);
    when(identity.isAnonymous()).thenReturn(true);

    var resolver = new QuarkusSecurityUserRoleResolver(identity);
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_principal_missing() {
    SecurityIdentity identity = mock(SecurityIdentity.class);
    when(identity.isAnonymous()).thenReturn(false);
    when(identity.getPrincipal()).thenReturn(null);

    var resolver = new QuarkusSecurityUserRoleResolver(identity);
    UserAuthority authority = resolver.resolve(UserId.of("user"));

    assertEquals(UserAuthority.EMPTY, authority);
  }
}

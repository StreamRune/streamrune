package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.UserId;

class SpringSecurityUserRoleResolverTest {

  private final SpringSecurityUserRoleResolver resolver = new SpringSecurityUserRoleResolver();

  @AfterEach
  void clearSecurityContext() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void resolves_roles_from_ROLE_prefixed_authorities() {
    var auth =
        new TestingAuthenticationToken(
            "user",
            null,
            List.of(
                new SimpleGrantedAuthority("ROLE_ADMIN"), new SimpleGrantedAuthority("ROLE_USER")));
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertTrue(authority.hasRole("ADMIN"));
    assertTrue(authority.hasRole("USER"));
    assertFalse(authority.hasRole("ROLE_ADMIN")); // prefix stripped
  }

  @Test
  void resolves_permissions_from_non_ROLE_prefixed_authorities() {
    var auth =
        new TestingAuthenticationToken(
            "user",
            null,
            List.of(
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("orders:read"),
                new SimpleGrantedAuthority("orders:write")));
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertTrue(authority.hasRole("ADMIN"));
    assertTrue(authority.hasPermission("orders:read"));
    assertTrue(authority.hasPermission("orders:write"));
  }

  @Test
  void skips_an_authority_whose_name_is_null() {
    // GrantedAuthority.getAuthority() is @Nullable in spring-security 7. One
    // custom authority answering null made authority.startsWith(...) throw an NPE, so every
    // @RequireRole/@RequirePermission command for that user failed with it, even when a sibling
    // authority granted exactly the required role. A null authority names no role and no
    // permission, so it is skipped and the other authorities still resolve.
    GrantedAuthority nameless = () -> null;
    var auth =
        new TestingAuthenticationToken(
            "user",
            null,
            List.of(
                nameless,
                new SimpleGrantedAuthority("ROLE_ADMIN"),
                new SimpleGrantedAuthority("orders:read")));
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertEquals(Set.of("ADMIN"), authority.roles());
    assertEquals(Set.of("orders:read"), authority.permissions());
  }

  @Test
  void returns_EMPTY_when_no_authentication() {
    // no SecurityContext set
    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_authentication_has_no_authorities() {
    var auth = new TestingAuthenticationToken("user", null, List.of());
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_does_not_match_authenticated_principal() {
    // userId may come from a different source (e.g. X-User-Id header) than the SecurityContext;
    // authorities must never be attributed — and cached — under a mismatched user ID
    var auth =
        new TestingAuthenticationToken(
            "alice", null, List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("bob"));
    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_userId_null() {
    var auth =
        new TestingAuthenticationToken(
            "alice", null, List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(null);
    assertEquals(UserAuthority.EMPTY, authority);
  }

  @Test
  void returns_EMPTY_when_authentication_not_authenticated() {
    var auth =
        new TestingAuthenticationToken(
            "user", null, List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
    auth.setAuthenticated(false);
    SecurityContextHolder.getContext().setAuthentication(auth);

    UserAuthority authority = resolver.resolve(UserId.of("user"));
    assertEquals(UserAuthority.EMPTY, authority);
  }
}

package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Test;

class UserAuthorityTest {

  @Test
  void hasRole_returns_true_when_role_present() {
    var authority = new UserAuthority(Set.of("ADMIN", "USER"), Set.of());
    assertTrue(authority.hasRole("ADMIN"));
  }

  @Test
  void hasRole_returns_false_when_role_absent() {
    var authority = new UserAuthority(Set.of("USER"), Set.of());
    assertFalse(authority.hasRole("ADMIN"));
  }

  @Test
  void hasAnyRole_returns_true_when_at_least_one_matches() {
    var authority = new UserAuthority(Set.of("USER"), Set.of());
    assertTrue(authority.hasAnyRole("ADMIN", "USER"));
  }

  @Test
  void hasAnyRole_returns_false_when_none_match() {
    var authority = new UserAuthority(Set.of("USER"), Set.of());
    assertFalse(authority.hasAnyRole("ADMIN", "MANAGER"));
  }

  @Test
  void hasPermission_returns_true_when_permission_present() {
    var authority = new UserAuthority(Set.of(), Set.of("orders:read", "orders:write"));
    assertTrue(authority.hasPermission("orders:read"));
  }

  @Test
  void hasPermission_returns_false_when_permission_absent() {
    var authority = new UserAuthority(Set.of(), Set.of("orders:read"));
    assertFalse(authority.hasPermission("orders:delete"));
  }

  @Test
  void hasAnyPermission_returns_true_when_at_least_one_matches() {
    var authority = new UserAuthority(Set.of(), Set.of("orders:read"));
    assertTrue(authority.hasAnyPermission("orders:read", "orders:write"));
  }

  @Test
  void hasAnyPermission_returns_false_when_none_match() {
    var authority = new UserAuthority(Set.of(), Set.of("orders:read"));
    assertFalse(authority.hasAnyPermission("orders:write", "orders:delete"));
  }

  @Test
  void EMPTY_has_no_roles_or_permissions() {
    assertFalse(UserAuthority.EMPTY.hasRole("anything"));
    assertFalse(UserAuthority.EMPTY.hasPermission("anything"));
  }

  @Test
  void defensive_copy_prevents_external_mutation() {
    var roles = new java.util.HashSet<>(Set.of("ADMIN"));
    var authority = new UserAuthority(roles, Set.of());
    roles.add("HACKER");
    assertFalse(authority.hasRole("HACKER"));
  }

  @Test
  void null_sets_are_rejected() {
    assertThrows(NullPointerException.class, () -> new UserAuthority(null, Set.of()));
    assertThrows(NullPointerException.class, () -> new UserAuthority(Set.of(), null));
  }
}

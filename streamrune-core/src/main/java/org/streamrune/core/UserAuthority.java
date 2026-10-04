package org.streamrune.core;

import java.util.Objects;
import java.util.Set;

/**
 * Immutable container for a user's roles and permissions, returned by {@link UserRoleResolver}.
 *
 * @param roles the user's role names (e.g., "ADMIN", "USER")
 * @param permissions the user's permission names (e.g., "orders:read", "orders:write")
 */
public record UserAuthority(Set<String> roles, Set<String> permissions) {

  /** An authority with no roles and no permissions. */
  public static final UserAuthority EMPTY = new UserAuthority(Set.of(), Set.of());

  public UserAuthority {
    Objects.requireNonNull(roles, "roles must not be null");
    Objects.requireNonNull(permissions, "permissions must not be null");
    roles = Set.copyOf(roles);
    permissions = Set.copyOf(permissions);
  }

  /** Returns {@code true} if this authority includes the given role. */
  public boolean hasRole(String role) {
    return roles.contains(role);
  }

  /** Returns {@code true} if this authority includes at least one of the given roles. */
  public boolean hasAnyRole(String... requiredRoles) {
    for (String r : requiredRoles) {
      if (roles.contains(r)) return true;
    }
    return false;
  }

  /** Returns {@code true} if this authority includes the given permission. */
  public boolean hasPermission(String permission) {
    return permissions.contains(permission);
  }

  /** Returns {@code true} if this authority includes at least one of the given permissions. */
  public boolean hasAnyPermission(String... requiredPermissions) {
    for (String p : requiredPermissions) {
      if (permissions.contains(p)) return true;
    }
    return false;
  }
}

package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AuthorizationAnnotationsTest {

  // --- Direct annotation on the class ---

  @RequireRole("ADMIN")
  record DirectRoleCommand() {}

  @RequirePermission("orders:delete")
  record DirectPermissionCommand() {}

  // --- Annotation on a sealed command interface (the documented pattern) ---

  @RequireRole("ADMIN")
  sealed interface AdminCommand permits CloseAccountCommand, PurgeDataCommand {}

  record CloseAccountCommand() implements AdminCommand {}

  record PurgeDataCommand() implements AdminCommand {}

  // --- Annotation on a superclass ---

  @RequirePermission("audit:write")
  abstract static class AuditedCommand {}

  static final class AppendAuditCommand extends AuditedCommand {}

  // --- Annotation on an interface of the superclass (transitive) ---

  @RequireRole("MANAGER")
  interface ManagerOperation {}

  abstract static class ManagerCommandBase implements ManagerOperation {}

  static final class ApproveBudgetCommand extends ManagerCommandBase {}

  // --- Nearest declaration wins ---

  @RequireRole("OPERATOR")
  record OverridingCommand() implements AdminCommand2 {}

  @RequireRole("ADMIN")
  interface AdminCommand2 {}

  // --- Mixed levels: role on interface, permission on class ---

  @RequirePermission("orders:cancel")
  record CancelOrderCommand() implements AdminCommand2 {}

  // --- Unannotated ---

  record PlainCommand() {}

  @Test
  void findsAnnotationDeclaredOnClass() {
    RequireRole role = AuthorizationAnnotations.findRequireRole(DirectRoleCommand.class);
    assertNotNull(role);
    assertArrayEquals(new String[] {"ADMIN"}, role.value());

    RequirePermission perm =
        AuthorizationAnnotations.findRequirePermission(DirectPermissionCommand.class);
    assertNotNull(perm);
    assertArrayEquals(new String[] {"orders:delete"}, perm.value());
  }

  @Test
  void findsAnnotationDeclaredOnSealedCommandInterface() {
    RequireRole role = AuthorizationAnnotations.findRequireRole(CloseAccountCommand.class);
    assertNotNull(role);
    assertArrayEquals(new String[] {"ADMIN"}, role.value());

    assertNotNull(AuthorizationAnnotations.findRequireRole(PurgeDataCommand.class));
  }

  @Test
  void findsAnnotationDeclaredOnSuperclass() {
    RequirePermission perm =
        AuthorizationAnnotations.findRequirePermission(AppendAuditCommand.class);
    assertNotNull(perm);
    assertArrayEquals(new String[] {"audit:write"}, perm.value());
  }

  @Test
  void findsAnnotationDeclaredOnInterfaceOfSuperclass() {
    RequireRole role = AuthorizationAnnotations.findRequireRole(ApproveBudgetCommand.class);
    assertNotNull(role);
    assertArrayEquals(new String[] {"MANAGER"}, role.value());
  }

  @Test
  void nearestDeclarationWins() {
    RequireRole role = AuthorizationAnnotations.findRequireRole(OverridingCommand.class);
    assertNotNull(role);
    assertArrayEquals(new String[] {"OPERATOR"}, role.value());
  }

  @Test
  void combinesAnnotationsFromDifferentLevels() {
    RequireRole role = AuthorizationAnnotations.findRequireRole(CancelOrderCommand.class);
    assertNotNull(role);
    assertArrayEquals(new String[] {"ADMIN"}, role.value());

    RequirePermission perm =
        AuthorizationAnnotations.findRequirePermission(CancelOrderCommand.class);
    assertNotNull(perm);
    assertArrayEquals(new String[] {"orders:cancel"}, perm.value());
  }

  @Test
  void returnsNullWhenUnannotated() {
    assertNull(AuthorizationAnnotations.findRequireRole(PlainCommand.class));
    assertNull(AuthorizationAnnotations.findRequirePermission(PlainCommand.class));
  }

  @Test
  void requiresAuthorization_trueForAnyAnnotationAnywhereInHierarchy() {
    assertTrue(AuthorizationAnnotations.requiresAuthorization(DirectRoleCommand.class));
    assertTrue(AuthorizationAnnotations.requiresAuthorization(DirectPermissionCommand.class));
    assertTrue(AuthorizationAnnotations.requiresAuthorization(CloseAccountCommand.class));
    assertTrue(AuthorizationAnnotations.requiresAuthorization(AppendAuditCommand.class));
    assertTrue(AuthorizationAnnotations.requiresAuthorization(AdminCommand.class));
  }

  @Test
  void requiresAuthorization_falseForUnannotated() {
    assertFalse(AuthorizationAnnotations.requiresAuthorization(PlainCommand.class));
    assertFalse(AuthorizationAnnotations.requiresAuthorization(String.class));
  }

  @Test
  void rejectsNullType() {
    assertThrows(
        IllegalArgumentException.class, () -> AuthorizationAnnotations.findRequireRole(null));
    assertThrows(
        IllegalArgumentException.class, () -> AuthorizationAnnotations.findRequirePermission(null));
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.RequirePermission;
import org.streamrune.core.RequireRole;
import org.streamrune.core.UserAuthority;
import org.streamrune.test.UnregisteredSealedTypes;

class AuthorizationConfigurationValidatorTest {

  // --- Command fixtures ---

  record PlainCommand() {}

  @RequireRole("ADMIN")
  record AnnotatedCommand() {}

  @RequireRole("ADMIN")
  sealed interface AdminCommands permits DeleteCommand {}

  record DeleteCommand() implements AdminCommands {}

  /** Sealed hierarchy where only one subtype is annotated. */
  sealed interface MixedCommands permits UnguardedSubCommand, GuardedSubCommand {}

  record UnguardedSubCommand() implements MixedCommands {}

  @RequirePermission("orders:purge")
  record GuardedSubCommand() implements MixedCommands {}

  private final AnnotationAuthorizationInterceptor enforcement =
      new AnnotationAuthorizationInterceptor(userId -> UserAuthority.EMPTY);

  // --- Unreadable sealed hierarchies ---

  @Test
  void refuses_aSealedCommandTypeWhosePermittedSubclassesCannotBeRead() {
    // A native image reports a sealed type that is not registered for reflection as sealed with NO
    // permitted subclasses; walking only the root would miss an annotated subtype and pass the
    // fail-open configuration this validator exists to refuse.
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                AuthorizationConfigurationValidator.requireEnforcement(
                    List.of(unregistered), List.of()));

    assertTrue(thrown.getMessage().contains(unregistered.getName()), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("reports no permitted subclasses"), thrown.getMessage());
  }

  @Test
  void passes_anUnreadableSealedCommandTypeWhenEnforcementIsConfigured() {
    // With the enforcement interceptor present nothing is walked: every command is checked at
    // dispatch time by its concrete class, so the hierarchy need not be enumerable.
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());

    assertDoesNotThrow(
        () ->
            AuthorizationConfigurationValidator.requireEnforcement(
                List.of(unregistered), List.of(enforcement)));
  }

  // --- Passing configurations ---

  @Test
  void passes_whenNoCommandsRegistered() {
    assertDoesNotThrow(
        () -> AuthorizationConfigurationValidator.requireEnforcement(List.of(), List.of()));
  }

  @Test
  void passes_whenNoAnnotatedCommands() {
    assertDoesNotThrow(
        () ->
            AuthorizationConfigurationValidator.requireEnforcement(
                List.of(PlainCommand.class), List.of()));
  }

  @Test
  void passes_whenEnforcementInterceptorPresent() {
    assertDoesNotThrow(
        () ->
            AuthorizationConfigurationValidator.requireEnforcement(
                List.of(AnnotatedCommand.class, AdminCommands.class), List.of(enforcement)));
  }

  @Test
  void passes_whenEnforcementPresentAmongNullInterceptors() {
    // Optional CDI producers may yield null elements — they must be skipped, not NPE
    assertDoesNotThrow(
        () ->
            AuthorizationConfigurationValidator.requireEnforcement(
                List.of(AnnotatedCommand.class),
                Arrays.asList(null, CommandInterceptor.noop(), enforcement)));
  }

  // --- Fail-closed configurations ---

  @Test
  void throws_whenAnnotatedCommandWithoutEnforcement() {
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                AuthorizationConfigurationValidator.requireEnforcement(
                    List.of(AnnotatedCommand.class), List.of()));
    assertTrue(ex.getMessage().contains(AnnotatedCommand.class.getName()));
    assertTrue(ex.getMessage().contains("UNGUARDED"));
    assertTrue(ex.getMessage().contains("UserRoleResolver"));
  }

  @Test
  void throws_whenAnnotationOnRegisteredSealedInterface() {
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                AuthorizationConfigurationValidator.requireEnforcement(
                    List.of(AdminCommands.class), List.of(CommandInterceptor.noop())));
    assertTrue(ex.getMessage().contains(AdminCommands.class.getName()));
  }

  @Test
  void throws_whenAnnotationOnlyOnSealedSubtype() {
    // Registered type is the unannotated sealed interface; one permitted record is annotated
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                AuthorizationConfigurationValidator.requireEnforcement(
                    List.of(MixedCommands.class), List.of()));
    assertTrue(ex.getMessage().contains(GuardedSubCommand.class.getName()));
    assertFalse(ex.getMessage().contains(UnguardedSubCommand.class.getName()));
  }

  @Test
  void throws_whenAnnotatedCommandAndOnlyNullInterceptors() {
    assertThrows(
        IllegalStateException.class,
        () ->
            AuthorizationConfigurationValidator.requireEnforcement(
                List.of(AnnotatedCommand.class), Arrays.asList((CommandInterceptor) null)));
  }

  // --- Argument validation ---

  @Test
  void rejectsNullArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AuthorizationConfigurationValidator.requireEnforcement(null, List.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> AuthorizationConfigurationValidator.requireEnforcement(List.of(), null));
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.RequirePermission;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

class AnnotationAuthorizationInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private final UserId testUser = UserId.of("user-42");
  private final UserRoleResolver resolver =
      userId -> new UserAuthority(Set.of("ADMIN", "USER"), Set.of("orders:read", "orders:write"));

  private CommandContext ctxFor(Command command) {
    return new CommandContext(
        command,
        command.getClass().getSimpleName(),
        CommandId.of("cmd-1"),
        TYPE,
        AggregateId.of("agg-1"),
        null,
        Instant.now());
  }

  private void runWithUser(UserId userId, Runnable action) {
    ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, userId, CorrelationId.of("corr-1"), Instant.now(), null))
        .run(action);
  }

  // --- No annotations ---

  record PlainCommand() implements Command {}

  @Test
  void passes_when_command_has_no_annotations() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    assertTrue(interceptor.before(ctxFor(new PlainCommand())));
  }

  // --- @RequireRole ---

  @RequireRole("ADMIN")
  record AdminCommand() implements Command {}

  @RequireRole({"ADMIN", "MANAGER"})
  record AdminOrManagerCommand() implements Command {}

  @RequireRole("SUPERADMIN")
  record SuperAdminCommand() implements Command {}

  @Test
  void passes_when_user_has_required_role() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(testUser, () -> assertTrue(interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void passes_when_user_has_any_of_required_roles() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(
        testUser, () -> assertTrue(interceptor.before(ctxFor(new AdminOrManagerCommand()))));
  }

  @Test
  void throws_when_user_lacks_required_role() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(
        testUser,
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new SuperAdminCommand()))));
  }

  @Test
  void throws_when_no_user_and_role_required() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    assertThrows(
        AuthorizationException.class, () -> interceptor.before(ctxFor(new AdminCommand())));
  }

  // --- @RequirePermission ---

  @RequirePermission("orders:read")
  record ReadOrderCommand() implements Command {}

  @RequirePermission("orders:delete")
  record DeleteOrderCommand() implements Command {}

  @Test
  void passes_when_user_has_required_permission() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(testUser, () -> assertTrue(interceptor.before(ctxFor(new ReadOrderCommand()))));
  }

  @Test
  void throws_when_user_lacks_required_permission() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(
        testUser,
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new DeleteOrderCommand()))));
  }

  // --- Annotation on the command interface (sealed-interface pattern) ---

  @RequireRole("ADMIN")
  sealed interface SealedAdminCommand extends Command permits SealedDeleteCommand {}

  record SealedDeleteCommand() implements SealedAdminCommand {}

  @RequireRole("SUPERADMIN")
  sealed interface SealedSuperAdminCommand extends Command permits SealedPurgeCommand {}

  record SealedPurgeCommand() implements SealedSuperAdminCommand {}

  @Test
  void passes_when_annotation_on_command_interface_and_user_has_role() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(testUser, () -> assertTrue(interceptor.before(ctxFor(new SealedDeleteCommand()))));
  }

  @Test
  void throws_when_annotation_on_command_interface_and_user_lacks_role() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(
        testUser,
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new SealedPurgeCommand()))));
  }

  @Test
  void throws_when_no_user_and_annotation_on_command_interface() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    assertThrows(
        AuthorizationException.class, () -> interceptor.before(ctxFor(new SealedDeleteCommand())));
  }

  // --- Combination ---

  @RequireRole("ADMIN")
  @RequirePermission("orders:read")
  record AdminReadCommand() implements Command {}

  @RequireRole("SUPERADMIN")
  @RequirePermission("orders:read")
  record SuperAdminReadCommand() implements Command {}

  @RequireRole("ADMIN")
  @RequirePermission("orders:delete")
  record AdminDeleteCommand() implements Command {}

  @Test
  void passes_when_both_role_and_permission_satisfied() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(testUser, () -> assertTrue(interceptor.before(ctxFor(new AdminReadCommand()))));
  }

  @Test
  void throws_when_role_satisfied_but_permission_not() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runWithUser(
        testUser,
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminDeleteCommand()))));
  }

  @Test
  void throws_when_role_not_satisfied_on_combined_command() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    // Using SuperAdminReadCommand: lacks SUPERADMIN role
    runWithUser(
        testUser,
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new SuperAdminReadCommand()))));
  }

  // --- saga-owned commands run as the system principal ---

  private void runSagaOwned(Runnable action) {
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE).run(action);
  }

  @Test
  void sagaOwnedCommand_withNoBoundUser_isAllowedAsSystemPrincipal() {
    // A saga's compensation command (e.g. RefundPayment) is dispatched on a subscription / timeout
    // /
    // sweeper thread with NO bound user. Without the saga-system-principal it is rejected
    // "Authentication required" and the saga is silently terminalized FAILED — the refund never
    // issues. The interceptor must honor SAGA_OWNED and let the trusted saga-system command
    // through.
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runSagaOwned(() -> assertTrue(interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void sagaOwnedCommand_onInterfaceAnnotation_isAllowedAsSystemPrincipal() {
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    runSagaOwned(() -> assertTrue(interceptor.before(ctxFor(new SealedDeleteCommand()))));
  }

  @Test
  void sagaOwnedCommand_withOptOut_stillEnforcesPerUserAuthz() {
    // Documented opt-out: a team that insists on per-user authz even for saga-dispatched commands
    // constructs the interceptor with honorSagaSystemPrincipal=false; then a saga-owned annotated
    // command with no bound user is still rejected (and such commands must not be authz-gated).
    var interceptor =
        new AnnotationAuthorizationInterceptor(resolver, /* honorSagaSystemPrincipal= */ false);
    runSagaOwned(
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void nonSagaCommand_withNoBoundUser_stillThrows_systemPrincipalDoesNotLeak() {
    // Regression guard: the SAGA_OWNED bypass must not leak to ordinary (non-saga) command
    // execution — a direct execute with no bound user still fails closed.
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    assertThrows(
        AuthorizationException.class, () -> interceptor.before(ctxFor(new AdminCommand())));
  }

  // --- Constructor validation ---

  @Test
  void constructor_rejects_null_resolver() {
    assertThrows(
        IllegalArgumentException.class, () -> new AnnotationAuthorizationInterceptor(null));
  }
}

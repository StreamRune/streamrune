package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.streamrune.core.AuthorizationAnnotations;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.RequirePermission;
import org.streamrune.core.RequireRole;
import org.streamrune.core.SealedHierarchy;
import org.streamrune.core.UserRoleResolver;

/**
 * Fails fast when commands declare {@link RequireRole}/{@link RequirePermission} but no {@link
 * AnnotationAuthorizationInterceptor} is configured to enforce them.
 *
 * <p>Without this check the annotations are silently ignored — annotated commands execute unguarded
 * whenever no {@link UserRoleResolver} (and therefore no enforcement interceptor) is wired, which
 * is fail-open. {@link VirtualThreadCommandBus.Builder#build()} runs it against the command types
 * registered with that builder and the interceptors handed to it, so every bus is held to it — the
 * one a framework integration assembles and one an application builds by hand. The integrations'
 * startup validators run it again against every command bus bean ({@link
 * VirtualThreadCommandBus#registeredCommandTypes()}, {@link
 * VirtualThreadCommandBus#interceptors()}); resolving the beans there creates a bus nothing has
 * created yet, so the refusal fails startup rather than the first command.
 *
 * <p>For each registered command type the validator inspects the type itself (including annotations
 * inherited from its interfaces and superclasses, via {@link AuthorizationAnnotations}) and, for
 * sealed types, every permitted subclass recursively. Non-sealed subtypes of a registered command
 * type cannot be enumerated without classpath scanning and are not inspected — annotate the
 * registered type or seal the hierarchy to get startup validation. A sealed type whose permitted
 * subclasses cannot be read — what a GraalVM native image reports for a sealed type not registered
 * for reflection — is refused with an {@link IllegalStateException} (see {@link SealedHierarchy}).
 */
public final class AuthorizationConfigurationValidator {

  private AuthorizationConfigurationValidator() {}

  /**
   * Throws {@link IllegalStateException} when any registered command type requires authorization
   * and {@code interceptors} contains no {@link AnnotationAuthorizationInterceptor}.
   *
   * @param registeredCommandTypes the command types registered with the command bus
   * @param interceptors the interceptor chain of that same command bus; {@code null} elements are
   *     ignored
   * @throws IllegalStateException when annotated commands exist without an enforcement interceptor
   */
  public static void requireEnforcement(
      Collection<Class<?>> registeredCommandTypes, Collection<CommandInterceptor> interceptors) {
    if (registeredCommandTypes == null) {
      throw new IllegalArgumentException("registeredCommandTypes is required");
    }
    if (interceptors == null) {
      throw new IllegalArgumentException("interceptors is required");
    }
    for (CommandInterceptor interceptor : interceptors) {
      if (interceptor instanceof AnnotationAuthorizationInterceptor) {
        return;
      }
    }

    List<String> annotated = findAnnotatedCommandTypes(registeredCommandTypes);
    if (annotated.isEmpty()) {
      return;
    }

    throw new IllegalStateException(
        "Commands declare @RequireRole/@RequirePermission but no AnnotationAuthorizationInterceptor "
            + "is configured — the annotations would be silently ignored and these commands would "
            + "execute UNGUARDED: "
            + String.join(", ", annotated)
            + ". Provide a UserRoleResolver bean (or enable your framework's security integration "
            + "so the default resolver is registered), register an AnnotationAuthorizationInterceptor "
            + "yourself, or remove the annotations.");
  }

  /** Returns the names of all registered command types that require authorization. */
  private static List<String> findAnnotatedCommandTypes(Collection<Class<?>> commandTypes) {
    List<String> annotated = new ArrayList<>();
    Set<Class<?>> candidates = new LinkedHashSet<>();
    for (Class<?> commandType : commandTypes) {
      collectSealedHierarchy(commandType, candidates);
    }
    for (Class<?> candidate : candidates) {
      if (AuthorizationAnnotations.requiresAuthorization(candidate)) {
        annotated.add(candidate.getName());
      }
    }
    return annotated;
  }

  /**
   * Collects the type and, for sealed types, all permitted subclasses recursively. A sealed type
   * whose permitted subclasses cannot be read (a GraalVM native image without reflection metadata
   * for it) is refused by {@link SealedHierarchy} rather than collected as a leaf: a leaf would
   * hide an annotated subtype and let the fail-open configuration this check exists for pass.
   */
  private static void collectSealedHierarchy(Class<?> type, Set<Class<?>> out) {
    if (type == null || !out.add(type)) {
      return;
    }
    for (Class<?> permitted :
        SealedHierarchy.permittedSubclasses(
            type, "the @RequireRole/@RequirePermission startup check")) {
      collectSealedHierarchy(permitted, out);
    }
  }
}

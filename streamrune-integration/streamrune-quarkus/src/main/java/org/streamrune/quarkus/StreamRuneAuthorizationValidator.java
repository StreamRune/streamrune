package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import org.streamrune.runtime.AuthorizationConfigurationValidator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Fails application startup when a command bus registers commands that declare
 * {@code @RequireRole}/{@code @RequirePermission} but its interceptor chain has no {@code
 * AnnotationAuthorizationInterceptor} — without enforcement those annotations would be silently
 * ignored and the commands would execute unguarded (fail-open). Delegates to {@link
 * AuthorizationConfigurationValidator}.
 *
 * <p>Checks every {@link VirtualThreadCommandBus} bean — the produced one or the application's own
 * — against the command types registered with that bus and the chain that bus runs, not against the
 * {@code DeciderRegistration} and {@code CommandInterceptor} beans of the container: a bus built by
 * hand registers its deciders through its builder and picks its own interceptors. The bus is a
 * lazily created singleton, so resolving it here builds it at startup, where its builder applies
 * the same rule; the refusal therefore fails startup rather than the first command.
 */
@ApplicationScoped
public class StreamRuneAuthorizationValidator {

  private final Instance<VirtualThreadCommandBus> commandBuses;

  /**
   * @param commandBuses every command bus of the application
   */
  @jakarta.inject.Inject
  public StreamRuneAuthorizationValidator(Instance<VirtualThreadCommandBus> commandBuses) {
    this.commandBuses = commandBuses;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected StreamRuneAuthorizationValidator() {
    this.commandBuses = null;
  }

  /**
   * Checks every command bus. {@code @Priority} sits on the EVENT PARAMETER (Arc ignores a
   * method-level one for observer ordering), at {@code LIBRARY_BEFORE}, so the check runs before
   * {@link StreamRuneLifecycle} starts any runner; {@code public} for the cross-classloader reason
   * given on {@link StreamRuneConfigValidator#validate}.
   *
   * @param event the Quarkus startup event
   * @throws IllegalStateException if a command bus would run an annotated command unguarded
   */
  public void validate(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    for (VirtualThreadCommandBus bus : commandBuses) {
      AuthorizationConfigurationValidator.requireEnforcement(
          bus.registeredCommandTypes(), bus.interceptors());
    }
  }
}

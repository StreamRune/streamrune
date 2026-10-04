package org.streamrune.spring;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.streamrune.runtime.AuthorizationConfigurationValidator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Fails application startup when a command bus registers commands that declare
 * {@code @RequireRole}/{@code @RequirePermission} but its interceptor chain has no {@code
 * AnnotationAuthorizationInterceptor} — without enforcement those annotations would be silently
 * ignored and the commands would execute unguarded (fail-open). Delegates to {@link
 * AuthorizationConfigurationValidator}.
 *
 * <p>Checks every {@link VirtualThreadCommandBus} bean — the auto-configured one or the
 * application's own — against the command types registered with that bus and the chain that bus
 * runs, not against the {@code DeciderRegistration} and {@code CommandInterceptor} beans of the
 * context: a bus built by hand registers its deciders through its builder and picks its own
 * interceptors. The bus's builder applies the same rule, so a bus created during refresh was
 * already refused there; resolving the beans here also creates a bus nothing has created yet (lazy
 * initialization), so the refusal happens at startup rather than on the first command.
 *
 * <p>Runs after all singletons are instantiated.
 */
public class StreamRuneAuthorizationValidator implements SmartInitializingSingleton {

  private final ObjectProvider<VirtualThreadCommandBus> commandBuses;

  /**
   * @param commandBuses every command bus of the application context
   */
  public StreamRuneAuthorizationValidator(ObjectProvider<VirtualThreadCommandBus> commandBuses) {
    this.commandBuses = commandBuses;
  }

  @Override
  public void afterSingletonsInstantiated() {
    for (VirtualThreadCommandBus bus : commandBuses.orderedStream().toList()) {
      AuthorizationConfigurationValidator.requireEnforcement(
          bus.registeredCommandTypes(), bus.interceptors());
    }
  }
}

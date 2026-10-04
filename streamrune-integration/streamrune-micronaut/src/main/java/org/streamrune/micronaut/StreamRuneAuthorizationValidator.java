package org.streamrune.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import org.streamrune.runtime.AuthorizationConfigurationValidator;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Fails application startup when a command bus registers commands that declare
 * {@code @RequireRole}/{@code @RequirePermission} but its interceptor chain has no {@code
 * AnnotationAuthorizationInterceptor} — without enforcement those annotations would be silently
 * ignored and the commands would execute unguarded (fail-open). Delegates to {@link
 * AuthorizationConfigurationValidator}.
 *
 * <p>Checks every {@link VirtualThreadCommandBus} bean — the module-built one or the application's
 * own — against the command types registered with that bus and the chain that bus runs, not against
 * the {@code DeciderRegistration} and {@code CommandInterceptor} beans of the context: a bus built
 * by hand registers its deciders through its builder and picks its own interceptors. The bus is a
 * lazily created singleton, so resolving it here builds it at startup, where its builder applies
 * the same rule; the refusal therefore fails startup rather than the first command.
 */
@Singleton
public class StreamRuneAuthorizationValidator
    implements ApplicationEventListener<StartupEvent>, Ordered {

  private final BeanProvider<VirtualThreadCommandBus> commandBuses;

  /**
   * Creates the validator.
   *
   * @param commandBuses every command bus of the application
   */
  public StreamRuneAuthorizationValidator(BeanProvider<VirtualThreadCommandBus> commandBuses) {
    this.commandBuses = commandBuses;
  }

  /**
   * Orders this listener in the validators' tier: without {@link Ordered}, Micronaut's ordering
   * among the several {@code ApplicationEventListener<StartupEvent>} beans of this module is
   * unspecified, so this fail-closed check could run after {@link StreamRuneLifecycle} had already
   * started the runners, whose first poll can persist data before an invalid configuration is ever
   * reported. A value strictly below {@link StreamRuneLifecycle}'s own puts every validator ahead
   * of it (lower value runs first), mirroring Spring, where a {@code SmartInitializingSingleton}
   * always completes before any {@code SmartLifecycle} starts.
   */
  @Override
  public int getOrder() {
    return 1000;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    for (VirtualThreadCommandBus bus : commandBuses) {
      AuthorizationConfigurationValidator.requireEnforcement(
          bus.registeredCommandTypes(), bus.interceptors());
    }
  }
}

package org.streamrune.spring;

import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Refuses startup when the command bus authorizes against the request identity but HTTP requests
 * can never carry one, and logs once where request identities come from.
 *
 * <p><b>The rule.</b> Registered only in a servlet web application (where {@link ScopedValueFilter}
 * derives {@code RequestContext.userId} from each request). Startup fails when the interceptor
 * chain of a {@link VirtualThreadCommandBus} bean — the auto-configured bus or the application's
 * own, whose interceptors need not be beans — includes an identity-consuming authorization
 * interceptor — an {@link AuthorizationCommandInterceptor} (registered for a {@code
 * CommandAuthorizationPolicy} bean) or an {@link AnnotationAuthorizationInterceptor} (registered
 * for a {@code UserRoleResolver} bean, and enforcing
 * {@code @RequireRole}/{@code @RequirePermission}) — while the {@link RequestIdentityPolicy} is
 * {@link RequestIdentityPolicy.Mode#ANONYMOUS}: no {@code AuthenticatedUserResolver} and {@code
 * streamrune.security.trust-user-id-header=false}. In that state every gated command would be
 * denied. An interceptor bean that no bus runs gates nothing and is not a signal.
 *
 * <p>A headless worker (no servlet stack) does not derive identities from HTTP requests and is not
 * checked. The SSE endpoint is not a signal: its {@code SseAuthorizer} receives {@code null} for an
 * anonymous caller, which an ownership check already denies.
 *
 * <p>Runs after all singletons are instantiated.
 */
public class StreamRuneRequestIdentityValidator implements SmartInitializingSingleton {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamRuneRequestIdentityValidator.class);

  private final RequestIdentityPolicy identityPolicy;
  private final ObjectProvider<VirtualThreadCommandBus> commandBuses;

  /**
   * @param identityPolicy the policy the request filter and SSE endpoint resolve identities with
   * @param commandBuses every command bus of the application context
   */
  public StreamRuneRequestIdentityValidator(
      RequestIdentityPolicy identityPolicy, ObjectProvider<VirtualThreadCommandBus> commandBuses) {
    this.identityPolicy = identityPolicy;
    this.commandBuses = commandBuses;
  }

  @Override
  public void afterSingletonsInstantiated() {
    Set<String> identityConsuming = new LinkedHashSet<>();
    for (VirtualThreadCommandBus bus : commandBuses.orderedStream().toList()) {
      for (CommandInterceptor interceptor : bus.interceptors()) {
        // Named by the framework class, not getClass(): a container proxy is a generated subclass.
        if (interceptor instanceof AuthorizationCommandInterceptor) {
          identityConsuming.add(AuthorizationCommandInterceptor.class.getSimpleName());
        } else if (interceptor instanceof AnnotationAuthorizationInterceptor) {
          identityConsuming.add(AnnotationAuthorizationInterceptor.class.getSimpleName());
        }
      }
    }
    identityPolicy.requireIdentitySourceFor(identityConsuming);
    if (identityPolicy.mode() == RequestIdentityPolicy.Mode.TRUSTED_GATEWAY) {
      LOG.warn(identityPolicy.describe());
    } else {
      LOG.info(identityPolicy.describe());
    }
  }
}

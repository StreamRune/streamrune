package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Refuses startup when the command bus authorizes against the request identity but HTTP requests
 * can never carry one, and logs once where request identities come from — the Quarkus counterpart
 * of the Spring integration's {@code StreamRuneRequestIdentityValidator}, applying the same {@link
 * RequestIdentityPolicy#requireIdentitySourceFor} rule.
 *
 * <p><b>The rule.</b> Checked only where this integration derives identities from HTTP requests:
 * when the {@link StreamRuneRequestFilter} is a bean, which Quarkus REST makes it (it registers
 * every {@code @Provider} of the index as a CDI bean). Startup fails when the interceptor chain of
 * a {@link VirtualThreadCommandBus} bean — the produced bus or the application's own, whose
 * interceptors need not be beans — includes an identity-consuming authorization interceptor — an
 * {@link AuthorizationCommandInterceptor} (produced for a {@code CommandAuthorizationPolicy} bean)
 * or an {@link AnnotationAuthorizationInterceptor} (produced for a {@code UserRoleResolver} bean,
 * and enforcing {@code @RequireRole}/{@code @RequirePermission}) — while the {@link
 * RequestIdentityPolicy} is {@link RequestIdentityPolicy.Mode#ANONYMOUS}: no {@code
 * AuthenticatedUserResolver} (no Quarkus Security) and {@code
 * streamrune.security.trust-user-id-header=false}. In that state every gated command would be
 * denied. An interceptor bean that no bus runs gates nothing and is not a signal.
 *
 * <p>An application without Quarkus REST (a headless worker) does not derive identities from HTTP
 * requests and is not checked. The SSE endpoint is not a signal: its {@code SseAuthorizer} receives
 * {@code null} for an anonymous caller, which an ownership check already denies.
 *
 * <p>Runs at {@link Interceptor.Priority#LIBRARY_BEFORE}, with the other startup validators and
 * strictly before {@link StreamRuneLifecycle} starts any runner.
 */
@ApplicationScoped
public class StreamRuneRequestIdentityValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamRuneRequestIdentityValidator.class);

  private final RequestIdentityPolicy identityPolicy;
  private final Instance<VirtualThreadCommandBus> commandBuses;
  private final Instance<StreamRuneRequestFilter> requestFilter;

  /**
   * @param identityPolicy the policy the request filter and the SSE endpoint resolve identities
   *     with
   * @param commandBuses every command bus of the application
   * @param requestFilter the request filter; satisfied only when Quarkus REST registered it
   */
  @Inject
  public StreamRuneRequestIdentityValidator(
      RequestIdentityPolicy identityPolicy,
      Instance<VirtualThreadCommandBus> commandBuses,
      Instance<StreamRuneRequestFilter> requestFilter) {
    this.identityPolicy = identityPolicy;
    this.commandBuses = commandBuses;
    this.requestFilter = requestFilter;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected StreamRuneRequestIdentityValidator() {
    this.identityPolicy = null;
    this.commandBuses = null;
    this.requestFilter = null;
  }

  /**
   * Applies the startup rule and logs the identity mode once.
   *
   * <p>{@code @Priority} sits on the EVENT PARAMETER (Arc ignores a method-level one for observer
   * ordering), at {@code LIBRARY_BEFORE} so the check runs before any runner starts; {@code public}
   * for the cross-classloader reason given on {@link StreamRuneConfigValidator#validate}. Resolving
   * the command buses builds a bus nothing has created yet.
   *
   * @param event the Quarkus startup event
   * @throws IllegalStateException if command authorization is configured but requests have no
   *     identity source
   */
  public void validate(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    if (requestFilter.isUnsatisfied()) {
      // No Quarkus REST: no request filter derives identities from HTTP requests.
      return;
    }
    Set<String> identityConsuming = new LinkedHashSet<>();
    for (VirtualThreadCommandBus bus : commandBuses) {
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

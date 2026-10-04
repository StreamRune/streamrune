package org.streamrune.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
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
 * can never carry one, and logs once where request identities come from — the Micronaut counterpart
 * of the Spring and Quarkus integrations' {@code StreamRuneRequestIdentityValidator}, applying the
 * same {@link RequestIdentityPolicy#requireIdentitySourceFor} rule.
 *
 * <p><b>The rule.</b> Checked where this integration derives identities from HTTP requests: when
 * {@link StreamRuneContextFilter} is a bean. This module brings the Netty HTTP server, so the
 * filter is registered in every Micronaut application that has not replaced it. Startup fails when
 * the interceptor chain of a {@link VirtualThreadCommandBus} bean — the module-built bus or the
 * application's own, whose interceptors need not be beans — includes an identity-consuming
 * authorization interceptor — an {@link AuthorizationCommandInterceptor} (produced for a {@code
 * CommandAuthorizationPolicy} bean) or an {@link AnnotationAuthorizationInterceptor} (produced for
 * a {@code UserRoleResolver} bean, and enforcing {@code @RequireRole}/{@code @RequirePermission}) —
 * while the {@link RequestIdentityPolicy} is {@link RequestIdentityPolicy.Mode#ANONYMOUS}: no
 * {@code AuthenticatedUserResolver} (Micronaut Security absent or disabled) and {@code
 * streamrune.security.trust-user-id-header=false}. In that state every gated command would be
 * denied. An interceptor bean that no bus runs gates nothing and is not a signal.
 *
 * <p>The SSE endpoint is not a signal: its {@code SseAuthorizer} receives {@code null} for an
 * anonymous caller, which an ownership check already denies.
 *
 * <p>Listens on the context-level {@link StartupEvent} with the other startup validators, at an
 * order strictly below {@link StreamRuneLifecycle}'s, so the check runs before any runner starts.
 */
@Singleton
public class StreamRuneRequestIdentityValidator
    implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final Logger LOG =
      LoggerFactory.getLogger(StreamRuneRequestIdentityValidator.class);

  private final RequestIdentityPolicy identityPolicy;
  private final BeanProvider<VirtualThreadCommandBus> commandBuses;
  private final BeanProvider<StreamRuneContextFilter> requestFilter;

  /**
   * Creates the validator.
   *
   * @param identityPolicy the policy the request filter and the SSE endpoint resolve identities
   *     with
   * @param commandBuses every command bus of the application
   * @param requestFilter the request filter; present whenever the application derives identities
   *     from HTTP requests through this integration
   */
  public StreamRuneRequestIdentityValidator(
      RequestIdentityPolicy identityPolicy,
      BeanProvider<VirtualThreadCommandBus> commandBuses,
      BeanProvider<StreamRuneContextFilter> requestFilter) {
    this.identityPolicy = identityPolicy;
    this.commandBuses = commandBuses;
    this.requestFilter = requestFilter;
  }

  // The validators' tier (StreamRuneConfigValidator, StreamRuneAuthorizationValidator,
  // SagaStateCryptoValidator), strictly below StreamRuneLifecycle (1100) and
  // SagaCompensationRetryLifecycle — Ordered: LOWER value runs first — so a refused startup never
  // starts a runner.
  @Override
  public int getOrder() {
    return 1000;
  }

  /**
   * Applies the startup rule and logs the identity mode once.
   *
   * @param event the Micronaut context startup event
   * @throws IllegalStateException if command authorization is configured but requests have no
   *     identity source
   */
  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (!requestFilter.isPresent()) {
      // StreamRuneContextFilter replaced: this integration derives no identities from requests.
      return;
    }
    Set<String> identityConsuming = new LinkedHashSet<>();
    // Resolving the command buses builds a bus nothing has created yet.
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

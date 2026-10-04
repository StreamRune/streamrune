package org.streamrune.micronaut;

import io.micronaut.security.utils.SecurityService;
import java.util.Optional;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;

/**
 * Resolves the authenticated principal for the current request from Micronaut Security's {@link
 * SecurityService}. Registered only when Micronaut Security is on the classpath.
 *
 * <p>The {@link SecurityService} looks up the {@code Authentication} of the current request on
 * every call, so this resolver is safe to use as a singleton. Returns the authentication name as
 * the {@link UserId}; a request with no authentication resolves to {@link Optional#empty()} so the
 * framework fails closed rather than trusting the client-supplied {@code X-User-Id} header.
 */
public class MicronautAuthenticatedUserResolver implements AuthenticatedUserResolver {

  private final SecurityService securityService;

  public MicronautAuthenticatedUserResolver(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public Optional<UserId> currentUser() {
    return securityService
        .getAuthentication()
        .map(io.micronaut.security.authentication.Authentication::getName)
        .filter(name -> name != null && !name.isBlank())
        .map(UserId::of);
  }
}

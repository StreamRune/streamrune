package org.streamrune.spring;

import java.util.Optional;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;

/**
 * Resolves the authenticated principal for the current request from Spring Security's {@link
 * SecurityContextHolder}. Registered only when Spring Security is on the classpath.
 *
 * <p>Returns the principal name ({@link Authentication#getName()}) as the {@link UserId}. An
 * absent, unauthenticated, or {@linkplain AnonymousAuthenticationToken anonymous} authentication
 * resolves to {@link Optional#empty()} so the framework fails closed rather than trusting the
 * client-supplied {@code X-User-Id} header.
 */
public class SpringSecurityAuthenticatedUserResolver implements AuthenticatedUserResolver {

  @Override
  public Optional<UserId> currentUser() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth == null
        || !auth.isAuthenticated()
        || auth instanceof AnonymousAuthenticationToken
        || auth.getName() == null
        || auth.getName().isBlank()) {
      return Optional.empty();
    }
    return Optional.of(UserId.of(auth.getName()));
  }
}

package org.streamrune.quarkus;

import io.quarkus.security.identity.SecurityIdentity;
import java.security.Principal;
import java.util.Optional;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;

/**
 * Resolves the authenticated principal for the current request from the Quarkus {@link
 * SecurityIdentity}. Produced only when Quarkus Security is available.
 *
 * <p>Returns the principal name as the {@link UserId}. An anonymous identity or missing principal
 * resolves to {@link Optional#empty()} so the framework fails closed rather than trusting the
 * client-supplied {@code X-User-Id} header.
 */
public class QuarkusAuthenticatedUserResolver implements AuthenticatedUserResolver {

  private final SecurityIdentity identity;

  public QuarkusAuthenticatedUserResolver(SecurityIdentity identity) {
    this.identity = identity;
  }

  @Override
  public Optional<UserId> currentUser() {
    if (identity == null || identity.isAnonymous()) {
      return Optional.empty();
    }
    Principal principal = identity.getPrincipal();
    if (principal == null || principal.getName() == null || principal.getName().isBlank()) {
      return Optional.empty();
    }
    return Optional.of(UserId.of(principal.getName()));
  }
}

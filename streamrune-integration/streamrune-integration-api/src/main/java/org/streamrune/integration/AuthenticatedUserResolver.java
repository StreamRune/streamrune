package org.streamrune.integration;

import java.util.Optional;
import org.streamrune.core.types.UserId;

/**
 * Resolves the identity of the <em>authenticated</em> principal for the current request from the
 * framework's security context (Spring Security's {@code SecurityContextHolder}, Quarkus {@code
 * SecurityIdentity}, or Micronaut {@code SecurityService}) — never from a client-supplied header.
 *
 * <p>This is the trust anchor that lets the framework bind {@link
 * org.streamrune.core.StreamRuneContext.RequestContext#userId()} to a verified identity instead of
 * the raw, spoofable {@code X-User-Id} request header. The header can be set by any client, so
 * authorizing off it (via {@code CommandAuthorizationPolicy} or {@code
 * Authorization.requireOwner(...)}) would let an attacker authenticated as one user act as another.
 * When an implementation of this SPI is present, the request filters derive the request-context
 * user ID from the authenticated principal returned here rather than from the header.
 *
 * <p>Implementations must return {@link Optional#empty()} for an unauthenticated request (anonymous
 * or missing authentication) so the framework fails closed — an unauthenticated caller gets no
 * identity and cannot assume one via the header.
 */
@FunctionalInterface
public interface AuthenticatedUserResolver {

  /**
   * Returns the authenticated principal's {@link UserId} for the current request, or {@link
   * Optional#empty()} when the request is unauthenticated (anonymous or no authentication present).
   *
   * @return the authenticated user ID, or empty if the caller is not authenticated
   */
  Optional<UserId> currentUser();
}

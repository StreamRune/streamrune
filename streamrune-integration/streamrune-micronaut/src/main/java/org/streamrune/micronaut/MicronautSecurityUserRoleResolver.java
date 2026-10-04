package org.streamrune.micronaut;

import io.micronaut.security.utils.SecurityService;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * Resolves user roles from Micronaut Security's {@link SecurityService}.
 *
 * <p>The {@link SecurityService} looks up the {@code Authentication} of the current request on
 * every {@link #resolve(UserId)} call, so this resolver is safe to use as a singleton. Maps {@code
 * Authentication#getRoles()} to roles. Permissions are not natively supported by Micronaut Security
 * — applications that need permissions should provide a custom {@link UserRoleResolver}.
 *
 * <p>Identity-source consistency: roles are returned only when the requested {@code userId} matches
 * the authenticated principal's name ({@code Authentication#getName()}). The {@code userId} may
 * originate from a different source than the security context (e.g. an {@code X-User-Id} header),
 * and resolved authorities may be cached per {@code userId} ({@code CachingUserRoleResolver}) —
 * without this check, one principal's roles could be cached under an unrelated user ID and served
 * to other callers. On any mismatch or missing authentication the resolver returns {@link
 * UserAuthority#EMPTY} (fail-closed).
 */
public class MicronautSecurityUserRoleResolver implements UserRoleResolver {

  private static final Logger logger =
      LoggerFactory.getLogger(MicronautSecurityUserRoleResolver.class);

  private final SecurityService securityService;

  /**
   * Creates a resolver backed by the given security service.
   *
   * @param securityService the Micronaut Security service
   */
  public MicronautSecurityUserRoleResolver(SecurityService securityService) {
    this.securityService = securityService;
  }

  /**
   * {@code true} — {@link SecurityService#getAuthentication()} looks up the authentication of the
   * <em>current request</em>, so it returns empty on the command bus's async virtual thread and on
   * the dead-letter retry thread, where this resolver could only ever answer {@link
   * UserAuthority#EMPTY}. See {@code RequestEdgeAuthority}.
   */
  @Override
  public boolean requiresRequestContext() {
    return true;
  }

  @Override
  public UserAuthority resolve(UserId userId) {
    if (userId == null) {
      return UserAuthority.EMPTY;
    }
    return securityService
        .getAuthentication()
        .map(
            authentication -> {
              if (!userId.value().equals(authentication.getName())) {
                // BOTH rendered identifiers go through the sanitizer, not
                // just the header one. "Already authenticated" is not "charset-safe": the principal
                // name is whatever the identity provider asserted (a JWT `sub`, a self-service
                // signup username), and nothing in this framework validates its charset. Sanitizing
                // one argument and leaving its neighbour raw in the same statement is exactly the
                // per-site trust judgement the sink policy exists to delete.
                logger.debug(
                    "Requested userId '{}' does not match authenticated principal '{}' — "
                        + "returning no authorities",
                    LogSanitizer.sanitizeForLog(userId.value()),
                    LogSanitizer.sanitizeForLog(authentication.getName()));
                return UserAuthority.EMPTY;
              }
              Collection<String> roles = authentication.getRoles();
              if (roles == null || roles.isEmpty()) {
                return UserAuthority.EMPTY;
              }
              return new UserAuthority(new HashSet<>(roles), Set.of());
            })
        .orElse(UserAuthority.EMPTY);
  }
}

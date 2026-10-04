package org.streamrune.quarkus;

import io.quarkus.security.identity.SecurityIdentity;
import java.security.Principal;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * Resolves user roles from Quarkus {@link SecurityIdentity}.
 *
 * <p>Maps {@link SecurityIdentity#getRoles()} to roles. Permissions are not natively supported by
 * Quarkus Security — applications that need permissions should provide a custom {@link
 * UserRoleResolver}.
 *
 * <p>Identity-source consistency: roles are returned only when the requested {@code userId} matches
 * the identity's principal name. The {@code userId} may originate from a different source than the
 * {@link SecurityIdentity} (e.g. an {@code X-User-Id} header), and resolved authorities may be
 * cached per {@code userId} ({@code CachingUserRoleResolver}) — without this check, one principal's
 * roles could be cached under an unrelated user ID and served to other callers. On any mismatch,
 * anonymous identity, or missing principal the resolver returns {@link UserAuthority#EMPTY}
 * (fail-closed).
 */
public class QuarkusSecurityUserRoleResolver implements UserRoleResolver {

  private static final Logger logger =
      LoggerFactory.getLogger(QuarkusSecurityUserRoleResolver.class);

  private final SecurityIdentity identity;

  public QuarkusSecurityUserRoleResolver(SecurityIdentity identity) {
    this.identity = identity;
  }

  /**
   * {@code true} — {@link SecurityIdentity} is a CDI request-scoped bean. Off-request it is at best
   * anonymous and at worst throws {@code ContextNotActiveException} when touched, so this resolver
   * could only ever answer {@link UserAuthority#EMPTY} on the command bus's async virtual thread or
   * on the dead-letter retry thread. See {@code RequestEdgeAuthority}.
   */
  @Override
  public boolean requiresRequestContext() {
    return true;
  }

  @Override
  public UserAuthority resolve(UserId userId) {
    if (userId == null || identity.isAnonymous()) {
      return UserAuthority.EMPTY;
    }
    Principal principal = identity.getPrincipal();
    if (principal == null || !userId.value().equals(principal.getName())) {
      // BOTH rendered identifiers go through the sanitizer, not just
      // the header one. "Already authenticated" is not "charset-safe": the principal name is
      // whatever the identity provider asserted (a JWT `sub`, a self-service signup username),
      // and nothing in this framework validates its charset. Sanitizing one argument and
      // leaving its neighbour raw in the same statement is exactly the per-site trust
      // judgement the sink policy exists to delete.
      logger.debug(
          "Requested userId '{}' does not match authenticated principal '{}' — returning no "
              + "authorities",
          LogSanitizer.sanitizeForLog(userId.value()),
          principal == null ? null : LogSanitizer.sanitizeForLog(principal.getName()));
      return UserAuthority.EMPTY;
    }
    Set<String> roles = identity.getRoles();
    if (roles == null || roles.isEmpty()) {
      return UserAuthority.EMPTY;
    }
    return new UserAuthority(roles, Set.of());
  }
}

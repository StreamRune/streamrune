package org.streamrune.spring;

import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.UserId;

/**
 * Resolves user roles and permissions from Spring Security's {@link SecurityContextHolder}.
 *
 * <p>Convention: authorities with a {@code ROLE_} prefix are treated as roles (prefix stripped).
 * All other authorities are treated as permissions. An authority whose {@code getAuthority()} is
 * {@code null} is neither, and is ignored.
 *
 * <p>Identity-source consistency: authorities are returned only when the requested {@code userId}
 * matches the authenticated principal's name ({@link Authentication#getName()}). The {@code userId}
 * may originate from a different source than the {@code SecurityContext} (e.g. the {@code
 * X-User-Id} header read by {@link ScopedValueFilter}), and resolved authorities may be cached per
 * {@code userId} ({@code CachingUserRoleResolver}) — without this check, one principal's
 * authorities could be cached under an unrelated user ID and served to other callers. On any
 * mismatch or missing authentication the resolver returns {@link UserAuthority#EMPTY}
 * (fail-closed).
 */
public class SpringSecurityUserRoleResolver implements UserRoleResolver {

  private static final Logger logger =
      LoggerFactory.getLogger(SpringSecurityUserRoleResolver.class);

  private static final String ROLE_PREFIX = "ROLE_";

  /**
   * {@code true} — {@link SecurityContextHolder} is a plain {@link ThreadLocal} that Spring
   * Security's filter chain populates for the duration of one request and clears afterwards. It is
   * unreadable on the command bus's async virtual thread and on the dead-letter retry thread, where
   * this resolver could only ever answer {@link UserAuthority#EMPTY}. See {@code
   * RequestEdgeAuthority}.
   */
  @Override
  public boolean requiresRequestContext() {
    return true;
  }

  @Override
  public UserAuthority resolve(UserId userId) {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth == null || !auth.isAuthenticated()) {
      return UserAuthority.EMPTY;
    }
    if (userId == null || !userId.value().equals(auth.getName())) {
      // BOTH rendered identifiers go through the sanitizer, not just
      // the header one. "Already authenticated" is not "charset-safe": the principal name is
      // whatever the identity provider asserted (a JWT `sub`, a self-service signup username),
      // and nothing in this framework validates its charset. Sanitizing one argument and
      // leaving its neighbour raw in the same statement is exactly the per-site trust
      // judgement the sink policy exists to delete.
      logger.debug(
          "Requested userId '{}' does not match authenticated principal '{}' — returning no "
              + "authorities",
          LogSanitizer.sanitizeForLog(userId == null ? null : userId.value()),
          LogSanitizer.sanitizeForLog(auth.getName()));
      return UserAuthority.EMPTY;
    }
    if (auth.getAuthorities() == null || auth.getAuthorities().isEmpty()) {
      return UserAuthority.EMPTY;
    }

    Set<String> roles = new HashSet<>();
    Set<String> permissions = new HashSet<>();

    for (GrantedAuthority ga : auth.getAuthorities()) {
      String authority = ga.getAuthority();
      if (authority == null) {
        // GrantedAuthority.getAuthority() is @Nullable (spring-security 7): a custom authority
        // type may answer null. It names no role and no permission, so skip it instead of
        // failing the whole resolution with an NPE and denying the user's other authorities.
        continue;
      }
      if (authority.startsWith(ROLE_PREFIX)) {
        roles.add(authority.substring(ROLE_PREFIX.length()));
      } else {
        permissions.add(authority);
      }
    }

    return new UserAuthority(roles, permissions);
  }
}

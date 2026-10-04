package org.streamrune.integration;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.streamrune.core.BaggageAllowlist;
import org.streamrune.core.types.UserId;

/**
 * The one rule by which an HTTP integration derives a request's authorization identity — the {@code
 * RequestContext.userId} that every {@code CommandAuthorizationPolicy}, {@code
 * AnnotationAuthorizationInterceptor} and {@code Authorization.requireOwner(...)} check trusts, and
 * the user id the command audit records. Every place that derives a user from a request (the
 * request filter, the SSE endpoint) resolves through the same instance, so the modes below behave
 * identically everywhere.
 *
 * <p>The mode is fixed at startup by two inputs: whether an {@link AuthenticatedUserResolver} is
 * available (the framework's security module, or an application bean) and the {@value
 * #TRUST_USER_ID_HEADER_PROPERTY} flag.
 *
 * <ul>
 *   <li>{@link Mode#TRUSTED_GATEWAY} — the flag is {@code true}. The {@value #USER_ID_HEADER}
 *       header is the identity, even when a resolver is present: the flag is the explicit statement
 *       that an upstream gateway authenticates every caller, overwrites the header with the
 *       authenticated id, and strips any client-supplied value — and that this service is reachable
 *       only through that gateway. Without those three guarantees any client can act as any user.
 *   <li>{@link Mode#AUTHENTICATED_PRINCIPAL} — a resolver is available and the flag is {@code
 *       false}. The identity is the authenticated principal; the header is ignored, and a header
 *       that disagrees with the principal is reported through the caller's {@link
 *       MismatchListener}. An unauthenticated request is anonymous whatever the header says.
 *   <li>{@link Mode#ANONYMOUS} — neither. The header is ignored and every request is anonymous
 *       ({@code userId == null}), so every identity check fails closed. Nothing is reported per
 *       request; {@link #describe()} gives the one startup line that explains the mode.
 * </ul>
 *
 * <p><b>Every value of the header.</b> {@link #resolve(List, MismatchListener)} takes every {@value
 * #USER_ID_HEADER} value the request carried, as received — never a first-value or comma-joined
 * view, which the servlet, JAX-RS and Netty APIs each produce differently. A request with more than
 * one value is ambiguous: a gateway that appends its own value after the client's instead of
 * replacing it would otherwise let whichever value the entry point happened to read become the
 * identity. In {@link Mode#TRUSTED_GATEWAY} such a request is anonymous; in {@link
 * Mode#AUTHENTICATED_PRINCIPAL} the header was never the identity, and each value that disagrees
 * with the principal is reported.
 *
 * <p><b>The role header.</b> {@link #applyRole(Map, List)} is the one rule for the request
 * context's baggage {@value BaggageAllowlist#ROLE_BAGGAGE_KEY} entry, which every event the request
 * produces carries into its plaintext, append-only metadata. The {@value #USER_ROLE_HEADER} header
 * is read <em>only</em> in {@link Mode#TRUSTED_GATEWAY}, under the same contract as the identity
 * header: the gateway overwrites or strips it, so a client cannot choose its own role. In every
 * other mode the header is ignored, and the entry is absent — an authenticated principal's verified
 * roles are not copied into baggage either: they are the request context's captured {@code
 * authority} (when the configured {@code UserRoleResolver} reads request-scoped state), which is
 * never persisted. A {@code role} entry the baggage already carried — a client's W3C {@code
 * baggage: role=} entry, even one an application allow-listed — never survives in any mode: behind
 * a trusted gateway the header is the only source. As with the identity header, more than one value
 * is ambiguous and is no role.
 *
 * <p><b>Startup rule.</b> An application whose command bus enforces authorization against the
 * request identity, but whose requests can never carry one, is misconfigured: every gated command
 * would be denied. {@link #requireIdentitySourceFor(Collection)} refuses startup in exactly that
 * case — the integration passes the identity-consuming authorization components it found
 * configured, and the policy throws when that list is non-empty and the mode is {@link
 * Mode#ANONYMOUS}.
 */
public final class RequestIdentityPolicy {

  /** The request header a trusted gateway forwards the authenticated user id in. */
  public static final String USER_ID_HEADER = "X-User-Id";

  /**
   * The request header a trusted gateway forwards the caller's role in; read only in {@link
   * Mode#TRUSTED_GATEWAY} (see {@link #applyRole(Map, List)}).
   */
  public static final String USER_ROLE_HEADER = "X-User-Role";

  /** The property that selects {@link Mode#TRUSTED_GATEWAY}, identical on every integration. */
  public static final String TRUST_USER_ID_HEADER_PROPERTY =
      "streamrune.security.trust-user-id-header";

  /** Where a request's identity comes from. */
  public enum Mode {
    /** The authenticated principal from an {@link AuthenticatedUserResolver}; header ignored. */
    AUTHENTICATED_PRINCIPAL,
    /** The {@value RequestIdentityPolicy#USER_ID_HEADER} header, set by a trusted gateway. */
    TRUSTED_GATEWAY,
    /** No identity source: the header is ignored and every request is anonymous. */
    ANONYMOUS
  }

  /**
   * Told when, in {@link Mode#AUTHENTICATED_PRINCIPAL}, a request's {@value #USER_ID_HEADER} header
   * names a different user than the authenticated principal. The header value is
   * attacker-controlled: a listener that logs it must sanitize it first.
   */
  @FunctionalInterface
  public interface MismatchListener {

    /**
     * @param headerUserId the user the header claimed (ignored for identity)
     * @param principal the authenticated principal the request runs as
     */
    void onMismatch(UserId headerUserId, UserId principal);
  }

  private final Mode mode;
  private final AuthenticatedUserResolver resolver;

  private RequestIdentityPolicy(Mode mode, AuthenticatedUserResolver resolver) {
    this.mode = mode;
    this.resolver = resolver;
  }

  /**
   * Selects the mode from the two startup inputs.
   *
   * @param resolver the authenticated-principal resolver, or {@code null} when none is available
   * @param trustUserIdHeader the value of {@value #TRUST_USER_ID_HEADER_PROPERTY}
   * @return {@link Mode#TRUSTED_GATEWAY} when the flag is set; otherwise {@link
   *     Mode#AUTHENTICATED_PRINCIPAL} with a resolver and {@link Mode#ANONYMOUS} without one
   */
  public static RequestIdentityPolicy of(
      AuthenticatedUserResolver resolver, boolean trustUserIdHeader) {
    if (trustUserIdHeader) {
      // The resolver is kept only so describe() can say the header overrides it; resolve() never
      // asks it in this mode.
      return new RequestIdentityPolicy(Mode.TRUSTED_GATEWAY, resolver);
    }
    if (resolver != null) {
      return new RequestIdentityPolicy(Mode.AUTHENTICATED_PRINCIPAL, resolver);
    }
    return new RequestIdentityPolicy(Mode.ANONYMOUS, null);
  }

  /**
   * @return where this policy takes a request's identity from
   */
  public Mode mode() {
    return mode;
  }

  /**
   * Resolves one request's identity.
   *
   * @param userIdHeaderValues every {@value #USER_ID_HEADER} value the request carried, in the
   *     order received and unsplit (a single value containing a comma is one value); {@code null}
   *     or empty when the header is absent. More than one value is ambiguous and never an identity
   * @param onMismatch told, once per distinct value, when a header value disagrees with the
   *     authenticated principal; never called outside {@link Mode#AUTHENTICATED_PRINCIPAL}
   * @return the request's user, or {@code null} for an anonymous request
   * @throws IllegalArgumentException if {@code onMismatch} is {@code null}
   */
  public UserId resolve(List<String> userIdHeaderValues, MismatchListener onMismatch) {
    if (onMismatch == null) {
      throw new IllegalArgumentException("onMismatch is required");
    }
    List<String> values = userIdHeaderValues == null ? List.of() : userIdHeaderValues;
    return switch (mode) {
      case ANONYMOUS -> null;
      case TRUSTED_GATEWAY -> values.size() == 1 ? headerUserId(values.getFirst()) : null;
      case AUTHENTICATED_PRINCIPAL -> {
        UserId principal = resolver.currentUser().orElse(null);
        if (principal != null) {
          reportMismatches(values, principal, onMismatch);
        }
        yield principal;
      }
    };
  }

  /**
   * Applies the role rule to one request's baggage: the returned map is {@code baggage} without any
   * {@value BaggageAllowlist#ROLE_BAGGAGE_KEY} entry it carried, plus — in {@link
   * Mode#TRUSTED_GATEWAY} only — the {@value #USER_ROLE_HEADER} header's value under that key.
   *
   * <ul>
   *   <li>{@link Mode#TRUSTED_GATEWAY}: exactly one non-blank header value is the role; no value, a
   *       blank value or more than one value (a gateway that appended instead of replacing) is no
   *       role.
   *   <li>{@link Mode#AUTHENTICATED_PRINCIPAL} and {@link Mode#ANONYMOUS}: the header is ignored
   *       and there is no role. The resolver is never consulted for it — a principal's verified
   *       roles belong in the request context's captured authority, not in the event metadata.
   * </ul>
   *
   * <p>The value is not otherwise checked here: the request context applies the baggage value
   * policy (length cap, control characters) to every entry, this one included.
   *
   * @param baggage the request's baggage after the key allowlist ({@link
   *     BaggageAllowlist#filter(Map, Set)}); {@code null} is treated as empty
   * @param userRoleHeaderValues every {@value #USER_ROLE_HEADER} value the request carried, in the
   *     order received and unsplit (a single value containing a comma is one value); {@code null}
   *     or empty when the header is absent
   * @return an immutable copy of {@code baggage} whose role entry follows the rule above
   */
  public Map<String, String> applyRole(
      Map<String, String> baggage, List<String> userRoleHeaderValues) {
    Map<String, String> result = new HashMap<>(baggage == null ? Map.of() : baggage);
    result.remove(BaggageAllowlist.ROLE_BAGGAGE_KEY);
    List<String> values = userRoleHeaderValues == null ? List.of() : userRoleHeaderValues;
    if (mode == Mode.TRUSTED_GATEWAY && values.size() == 1) {
      String role = values.getFirst();
      if (role != null && !role.isBlank()) {
        result.put(BaggageAllowlist.ROLE_BAGGAGE_KEY, role);
      }
    }
    return Map.copyOf(result);
  }

  private static void reportMismatches(
      List<String> values, UserId principal, MismatchListener onMismatch) {
    Set<UserId> claimed = new LinkedHashSet<>();
    for (String value : values) {
      UserId candidate = headerUserId(value);
      if (candidate != null && !candidate.equals(principal)) {
        claimed.add(candidate);
      }
    }
    claimed.forEach(c -> onMismatch.onMismatch(c, principal));
  }

  /**
   * Refuses startup when identity-consuming authorization is configured but requests can never
   * carry an identity ({@link Mode#ANONYMOUS}).
   *
   * @param identityConsumingAuthorization the authorization components the integration found
   *     configured that authorize against the request identity (for example {@code
   *     AuthorizationCommandInterceptor}, {@code AnnotationAuthorizationInterceptor}); empty when
   *     none
   * @throws IllegalArgumentException if {@code identityConsumingAuthorization} is {@code null}
   * @throws IllegalStateException if the list is non-empty and the mode is {@link Mode#ANONYMOUS}
   */
  public void requireIdentitySourceFor(Collection<String> identityConsumingAuthorization) {
    if (identityConsumingAuthorization == null) {
      throw new IllegalArgumentException("identityConsumingAuthorization is required");
    }
    if (mode != Mode.ANONYMOUS || identityConsumingAuthorization.isEmpty()) {
      return;
    }
    throw new IllegalStateException(
        "Command authorization is configured ("
            + String.join(", ", List.copyOf(identityConsumingAuthorization))
            + ") but HTTP requests have no identity source: there is no AuthenticatedUserResolver"
            + " and "
            + TRUST_USER_ID_HEADER_PROPERTY
            + " is false, so the "
            + USER_ID_HEADER
            + " header is ignored and every request is anonymous — every authorization-gated"
            + " command would be denied. Either add your framework's security module (or define an"
            + " AuthenticatedUserResolver bean) so the identity is the authenticated principal, or,"
            + " only behind a gateway that authenticates every caller, overwrites "
            + USER_ID_HEADER
            + " and strips any client-supplied value, set "
            + TRUST_USER_ID_HEADER_PROPERTY
            + "=true.");
  }

  /**
   * @return the one startup line that explains where request identities come from
   */
  public String describe() {
    return switch (mode) {
      case AUTHENTICATED_PRINCIPAL ->
          "Request identity: AUTHENTICATED_PRINCIPAL — RequestContext.userId is the principal"
              + " resolved by "
              + resolver.getClass().getName()
              + "; the "
              + USER_ID_HEADER
              + " header is ignored (a header naming another user is logged). The "
              + USER_ROLE_HEADER
              + " header is ignored too: baggage role is never set.";
      case TRUSTED_GATEWAY ->
          "Request identity: TRUSTED_GATEWAY ("
              + TRUST_USER_ID_HEADER_PROPERTY
              + "=true) — RequestContext.userId is taken from the "
              + USER_ID_HEADER
              + " header. The gateway in front of this service must authenticate every caller,"
              + " overwrite "
              + USER_ID_HEADER
              + " with the authenticated id and strip any client-supplied value, and the service"
              + " must be reachable only through it; otherwise any client can act as any user."
              + " A request carrying more than one "
              + USER_ID_HEADER
              + " value is anonymous. The "
              + USER_ROLE_HEADER
              + " header is baggage role (one value; repeated is no role), and every event"
              + " records it, so the gateway must also overwrite or strip "
              + USER_ROLE_HEADER
              + "."
              + (resolver == null
                  ? ""
                  : " An AuthenticatedUserResolver ("
                      + resolver.getClass().getName()
                      + ") is present but is NOT consulted: the header overrides any authenticated"
                      + " principal.");
      case ANONYMOUS ->
          "Request identity: ANONYMOUS — no AuthenticatedUserResolver and "
              + TRUST_USER_ID_HEADER_PROPERTY
              + "=false, so the "
              + USER_ID_HEADER
              + " header is ignored and every HTTP request runs without a user id. The "
              + USER_ROLE_HEADER
              + " header is ignored too: baggage role is never set.";
    };
  }

  private static UserId headerUserId(String userIdHeader) {
    return userIdHeader != null && !userIdHeader.isBlank() ? UserId.of(userIdHeader) : null;
  }
}

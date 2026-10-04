package org.streamrune.core;

import org.streamrune.core.types.UserId;

/**
 * Resolves the roles and permissions for a given user.
 *
 * <p>Framework integrations provide default implementations that read from the native security
 * context (Spring Security, Quarkus Security, Micronaut Security). Applications can supply their
 * own implementation as a bean to override or extend the default behavior.
 */
@FunctionalInterface
public interface UserRoleResolver {

  /**
   * Returns the roles and permissions for the given user.
   *
   * @param userId the caller's identity, never {@code null}
   * @return the user's authority, never {@code null}
   */
  UserAuthority resolve(UserId userId);

  /**
   * Whether {@link #resolve(UserId)} can only answer while an inbound request is in flight on the
   * calling thread — i.e. it reads framework request-scoped state (Spring's {@code
   * SecurityContextHolder} ThreadLocal, Quarkus's request-scoped {@code SecurityIdentity},
   * Micronaut's {@code SecurityService}) rather than being a pure function of the {@code userId}.
   *
   * <p>Commands do not only execute on the request thread: {@code executeAsync} runs on a fresh
   * virtual thread and {@code DeadLetterRetryRunner} replays on its own poll thread. A
   * request-scoped resolver called there returns {@link UserAuthority#EMPTY}, which is
   * indistinguishable from an honest "this user has no roles" — so fail-closed authorization denies
   * a caller it authorized moments earlier, and a dead-lettered command that already passed
   * authorization burns its whole retry ladder on {@code AuthorizationException} and is permanently
   * lost.
   *
   * <p>Declaring {@code true} lets the framework stop guessing. It has exactly two effects, and
   * both of them exist to avoid treating "cannot answer" as "answered: denied":
   *
   * <ul>
   *   <li>Each integration's request filter captures {@link #resolve(UserId)} once at the request
   *       edge into {@link StreamRuneContext.RequestContext#authority()}, so off-request execution
   *       inside that request (notably {@code executeAsync}) decides exactly as the request thread
   *       would have. A resolver returning {@code false} is never called at the request edge, so an
   *       expensive user-supplied resolver pays nothing for a feature it does not need.
   *   <li>On a DLQ replay — a background thread that never had a request and never can — {@code
   *       AnnotationAuthorizationInterceptor} executes the already-authorized command under a
   *       RECORDED system principal instead of denying it. A resolver returning {@code false} is
   *       still consulted normally there, so a genuine revocation still denies the replay.
   * </ul>
   *
   * <p>The default is {@code false}: a resolver that is a pure function of {@code userId} (a
   * database lookup, a static map, a JWT-claims decoder over the persisted id) already works
   * off-request and keeps its exact pre-existing behaviour. Override it to {@code true} only when
   * the resolver genuinely reads ambient per-request state. Declaring {@code true} for a resolver
   * that is actually a pure function would silently downgrade DLQ replays to the system principal
   * and skip a check that could have been made.
   *
   * @return {@code true} if this resolver reads ambient request-scoped state; {@code false} (the
   *     default) if it is a pure function of the {@code userId}
   */
  default boolean requiresRequestContext() {
    return false;
  }
}

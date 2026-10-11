package org.streamrune.integration;

import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.StreamId;

/**
 * Authorizes access to a Server-Sent Events (SSE) stream. Invoked by the framework's SSE controller
 * before it subscribes a caller to a stream, so an application can enforce per-stream ownership or
 * tenant isolation.
 *
 * <p>The auto-registered {@code GET /api/sse/{aggregateType}/{aggregateId}} endpoint streams an
 * aggregate's decrypted domain events verbatim from the path-supplied stream. Without an
 * authorization check any caller could read any aggregate's events (an IDOR/PII-disclosure). The
 * SSE endpoint is therefore disabled by default and, when enabled, is guarded by an implementation
 * of this SPI. If SSE is enabled but the application provides no {@code SseAuthorizer} bean, the
 * framework installs {@link #DENY_ALL} (fail closed) so a misconfiguration cannot silently expose
 * every stream.
 *
 * <p><b>The caller is the request context.</b> The authorizer receives the {@link
 * StreamRuneContext.RequestContext} of the request that asks for the stream: the one object the
 * integration's request filter built for that request, and the same one a command executed by that
 * request authorizes against. It is the only source for who the caller is: {@code caller.userId()}
 * is the identity the {@link RequestIdentityPolicy} resolved, and {@code caller.authority()} holds
 * the roles and permissions the configured {@link org.streamrune.core.UserRoleResolver} returned
 * for that user at the request edge. An authorizer needs no framework-specific request state (a
 * {@code ScopedValue}, a request-scoped bean, a thread-local) to decide by a role.
 *
 * <p><b>A request without a request context is refused.</b> The endpoint resolves the caller
 * itself, through the {@link RequestIdentityPolicy}, and takes the request context the request
 * filter bound for the request. It asks the authorizer only when that context carries exactly the
 * caller it resolved. A request no context is bound for (the integration's request filter is
 * replaced by one that binds none, or does not cover the endpoint's path) and a request whose bound
 * context names another user are answered with {@code 403 Forbidden} and the authorizer is not
 * called: the endpoint never hands over an identity it did not establish.
 *
 * <p><b>Decided once, when the stream opens.</b> The authorizer is asked before the subscription
 * and not again while the stream is open. A caller whose access ends afterwards (a role removed, a
 * token expired, an aggregate handed to another owner) keeps receiving the stream's decrypted
 * events until the stream ends. {@code streamrune.sse.timeout} (default five minutes) ends every
 * stream, and a client that reconnects is authorized again: the timeout is therefore the bound on
 * how long a revoked caller keeps reading. Keep it finite wherever access can change; {@code
 * streamrune.sse.timeout=0} removes that bound.
 */
@FunctionalInterface
public interface SseAuthorizer {

  /**
   * Returns whether {@code caller} may subscribe to the events of {@code streamId}.
   *
   * <p>{@code streamId} is the typed pair, so an implementation can decide per aggregate type. A
   * rule that admits a support role to every stream and a customer to the customer's own:
   *
   * <pre>{@code
   * UserId user = caller.userId();
   * if (user == null) {
   *   return false; // unauthenticated
   * }
   * UserAuthority authority = caller.authority();
   * if (authority != null && authority.hasRole("SUPPORT")) {
   *   return true;
   * }
   * return switch (streamId.aggregateType().value()) {
   *   case "customer" -> streamId.aggregateId().value().equals(user.value());
   *   default -> false;
   * };
   * }</pre>
   *
   * <p><b>What {@code caller} holds.</b>
   *
   * <ul>
   *   <li>{@code caller.userId()} is the caller: the authenticated principal, or the {@code
   *       X-User-Id} header behind a trusted gateway. It is {@code null} for an unauthenticated
   *       request, and for every request of an application with no identity source.
   *   <li>{@code caller.authority()} is what the configured {@link
   *       org.streamrune.core.UserRoleResolver} resolved for {@code caller.userId()} while the
   *       request filter ran. The request filter captures it only from a resolver that declares
   *       {@link org.streamrune.core.UserRoleResolver#requiresRequestContext()}, which the
   *       resolvers the three integrations ship for their security module do. It is {@code null}
   *       when nothing was captured: the caller is unauthenticated, the application has no {@code
   *       UserRoleResolver}, the resolver is a function of the user id alone (it does not declare
   *       {@code requiresRequestContext()}), or the resolver threw at the request edge. {@code
   *       null} is not "no roles": an authorizer whose resolver is a function of the user id asks
   *       that resolver itself, {@code userRoleResolver.resolve(caller.userId())}. A captured
   *       authority with no roles is {@link org.streamrune.core.UserAuthority#EMPTY}.
   *   <li>{@code caller.baggage()} holds the {@code role} entry only behind a trusted gateway: the
   *       {@code X-User-Role} header the gateway forwarded. The roles of an authenticated principal
   *       are never there.
   * </ul>
   *
   * <p>An implementation may block, for example to look the owner of the stream up in a database:
   * the integrations call it on a thread that may block, not on an event loop. On Spring that is
   * the servlet request thread and on Quarkus a worker thread, whatever the application adds. On
   * Micronaut it is the thread the request filter chain leaves the request on: the blocking
   * executor with the framework's {@code StreamRuneContextFilter}; an application that replaces
   * that filter, or adds one behind it that runs on another executor, decides the thread itself.
   *
   * <p>The answer holds for the life of the stream: see the class documentation.
   *
   * @param caller the request context of the request that asks for the stream, never {@code null};
   *     its {@code userId()} is {@code null} when the request is unauthenticated
   * @param streamId the requested aggregate stream
   * @return {@code true} to permit the subscription; {@code false} to deny it
   */
  boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId);

  /**
   * Fail-closed authorizer that denies every stream. Installed by the framework when SSE is enabled
   * without an application-provided {@link SseAuthorizer}, so enabling SSE never exposes streams
   * without an explicit access decision.
   */
  SseAuthorizer DENY_ALL = (caller, streamId) -> false;
}

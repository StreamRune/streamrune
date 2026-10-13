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
 * {@code ScopedValue}, a request-scoped bean, a thread-local) to decide by a role. Those two are
 * what a decision rests on. {@code caller.baggage()} travels in the same object and is not
 * established by the request edge: apart from the {@code role} entry behind a trusted gateway it is
 * the client's own {@code baggage} header (see {@link #isAuthorized}).
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
   * if (authority == null) {
   *   return false; // the caller's roles are unknown: refuse before any role is read
   * }
   * if (authority.hasRole("SUPPORT")) {
   *   return true;
   * }
   * return switch (streamId.aggregateType().value()) {
   *   case "customer" -> streamId.aggregateId().value().equals(user.value());
   *   default -> false;
   * };
   * }</pre>
   *
   * <p>The rule refuses an unknown authority before it reads a role, so it stays closed whichever
   * way a role is used: a line added to it that <em>refuses</em> by role ({@code if
   * (authority.hasRole("SUSPENDED")) return false;}) cannot admit a caller whose roles nobody
   * captured, which {@code authority != null && authority.hasRole("SUSPENDED")} would. A rule that
   * reads no role at all (ownership by {@code caller.userId()} alone) does not read the authority
   * and needs neither line.
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
   *       when nothing was captured, which has four causes: the caller is unauthenticated; the
   *       application has no {@code UserRoleResolver}; the resolver is a function of the user id
   *       alone (it does not declare {@code requiresRequestContext()}); or the resolver threw at
   *       the request edge. {@code null} means the roles are unknown, never "no roles" (a captured
   *       authority with no roles is {@link org.streamrune.core.UserAuthority#EMPTY}), so a rule
   *       that reads a role refuses on {@code null}, whether the role would admit or refuse. An
   *       authorizer whose resolver is a function of the user id resolves instead of refusing,
   *       {@code userRoleResolver.resolve(caller.userId())}; a resolver that threw at the request
   *       edge throws there again, and the exception ends the request without a stream.
   *   <li>{@code caller.baggage()} carries no access decision, with one exception that depends on
   *       the identity mode. The {@code role} entry is the {@code X-User-Role} header the gateway
   *       forwarded in trusted-gateway mode, as trustworthy as the gateway's handling of that
   *       header, and absent in every other mode: the roles of an authenticated principal are never
   *       there. Every other entry (the keys the application allow-lists in {@code
   *       streamrune.metadata.baggage-allowlist}) is copied from the client's own W3C {@code
   *       baggage} header in every identity mode. Any caller sets those to any value, so a rule
   *       that compares one of them (a tenant, an owner, a flag) admits whoever names the value it
   *       looks for.
   * </ul>
   *
   * <p>In short: {@code userId()} and {@code authority()} are established at the request edge, in
   * every identity mode; the {@code role} baggage entry is the forwarded role in trusted-gateway
   * mode only; nothing else in the baggage is.
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

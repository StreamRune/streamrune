package org.streamrune.integration;

import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;

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
   * Returns whether {@code principal} may subscribe to the events of {@code streamId}.
   *
   * <p>{@code streamId} is the typed pair, so an implementation can decide per aggregate type:
   *
   * <pre>{@code
   * return switch (streamId.aggregateType().value()) {
   *   case "customer" ->
   *       principal != null && streamId.aggregateId().value().equals(principal.value());
   *   default -> false;
   * };
   * }</pre>
   *
   * <p>An implementation may block, for example to look the owner of the stream up in a database:
   * the integrations call it on a thread that may block, not on an event loop. On Spring that is
   * the servlet request thread and on Quarkus a worker thread, whatever the application adds. On
   * Micronaut it is the thread the request filter chain leaves the request on: the blocking
   * executor with the framework's {@code StreamRuneContextFilter}, and whatever an application's
   * own filter chooses once it replaces that one.
   *
   * <p>The answer holds for the life of the stream: see the class documentation.
   *
   * @param principal the authenticated caller, or {@code null} when the request is unauthenticated
   * @param streamId the requested aggregate stream
   * @return {@code true} to permit the subscription; {@code false} to deny it
   */
  boolean isAuthorized(UserId principal, StreamId streamId);

  /**
   * Fail-closed authorizer that denies every stream. Installed by the framework when SSE is enabled
   * without an application-provided {@link SseAuthorizer}, so enabling SSE never exposes streams
   * without an explicit access decision.
   */
  SseAuthorizer DENY_ALL = (principal, streamId) -> false;
}

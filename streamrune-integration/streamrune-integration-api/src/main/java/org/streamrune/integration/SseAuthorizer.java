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
   * every integration calls it on a thread that may block (the servlet request thread on Spring, a
   * worker thread on Quarkus, the blocking executor on Micronaut), never on an event loop.
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

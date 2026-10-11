package org.streamrune.micronaut;

import java.time.Instant;
import java.util.Map;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * Request contexts for the tests that call the Server-Sent Events controller without a request
 * filter in front of it: what the filter would have bound for the request.
 */
final class SseCallers {

  private SseCallers() {}

  /** The request context of an unauthenticated request. */
  static StreamRuneContext.RequestContext anonymous() {
    return context(null);
  }

  /** The request context of a request of {@code userId}, with no captured authority. */
  static StreamRuneContext.RequestContext of(String userId) {
    return context(UserId.of(userId));
  }

  private static StreamRuneContext.RequestContext context(UserId userId) {
    return new StreamRuneContext.RequestContext(
        null, userId, CorrelationId.of("sse-test"), Instant.now(), Map.of());
  }
}

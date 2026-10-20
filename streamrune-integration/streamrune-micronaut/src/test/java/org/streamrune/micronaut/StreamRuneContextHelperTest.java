package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.context.ServerRequestContext;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/** Tests for {@link StreamRuneContextHelper}. */
class StreamRuneContextHelperTest {

  @AfterEach
  void cleanup() {
    StreamRuneContextHelper.remove();
  }

  @Test
  void getReturnsNullWhenUnset() {
    assertNull(StreamRuneContextHelper.get());
  }

  @Test
  void setAndGetRoundTrip() {
    var ctx = context("corr-1", null);
    StreamRuneContextHelper.set(ctx);
    assertSame(ctx, StreamRuneContextHelper.get());
  }

  @Test
  void removeClearsContext() {
    StreamRuneContextHelper.set(context("corr-1", null));
    StreamRuneContextHelper.remove();
    assertNull(StreamRuneContextHelper.get());
  }

  @Test
  void getFallsBackToTheContextTheFilterStoredOnTheCurrentRequest() {
    var ctx = context("corr-request", "alice");
    var seen = new AtomicReference<StreamRuneContext.RequestContext>();

    ServerRequestContext.with(requestCarrying(ctx), () -> seen.set(StreamRuneContextHelper.get()));

    assertSame(ctx, seen.get());
    assertNull(StreamRuneContextHelper.get(), "outside the request there is none");
  }

  @Test
  void theCurrentRequestWinsOverAThreadLocalMirrorLeftOnTheThread() {
    // A mirror left behind on a pooled thread can never name the caller of the request the thread
    // serves now: the request's own context comes first.
    var ofRequest = context("corr-request", "bob");
    var seen = new AtomicReference<StreamRuneContext.RequestContext>();
    StreamRuneContextHelper.set(context("corr-stale", "mallory"));

    ServerRequestContext.with(
        requestCarrying(ofRequest), () -> seen.set(StreamRuneContextHelper.get()));

    assertSame(ofRequest, seen.get());
  }

  @Test
  void theMirrorAnswersWhenNoCurrentRequestCarriesAContext() {
    var mirrored = context("corr-mirror", "alice");
    var seen = new AtomicReference<StreamRuneContext.RequestContext>();
    StreamRuneContextHelper.set(mirrored);

    ServerRequestContext.with(requestCarrying(null), () -> seen.set(StreamRuneContextHelper.get()));

    assertSame(mirrored, seen.get());
    assertSame(mirrored, StreamRuneContextHelper.get(), "no request current at all");
  }

  @Test
  void ofCurrentRequestIsNullWithoutARequestOrWithoutTheAttribute() {
    var seen = new AtomicReference<StreamRuneContext.RequestContext>(context("corr-x", null));

    assertNull(StreamRuneContextHelper.ofCurrentRequest());
    ServerRequestContext.with(
        requestCarrying(null), () -> seen.set(StreamRuneContextHelper.ofCurrentRequest()));

    assertNull(seen.get(), "a request the filter did not run for carries no context");
  }

  @Test
  void ofCurrentRequestIgnoresTheThreadLocalMirror() {
    StreamRuneContextHelper.set(context("corr-mirror", "alice"));

    assertNull(StreamRuneContextHelper.ofCurrentRequest());
  }

  @Test
  void thePropagatedElementNeverLeavesTheRequestsContextBehindOnTheThread() {
    // The element saves the mirror alone. Were it to save get() — which falls back to the current
    // request — it would put the request's context into the ThreadLocal on restore, and the next
    // request served by this (pooled) thread outside any propagation would read that caller.
    var ctx = context("corr-request", "alice");
    var element = new StreamRuneContextFilter.RequestContextElement(ctx);
    var mirroredAfterRestore = new AtomicReference<StreamRuneContext.RequestContext>(ctx);

    ServerRequestContext.with(
        requestCarrying(ctx),
        () -> {
          var previous = element.updateThreadContext();
          assertNull(previous, "nothing was mirrored on this thread before");
          element.restoreThreadContext(previous);
          mirroredAfterRestore.set(StreamRuneContextHelper.mirrored());
        });

    assertNull(mirroredAfterRestore.get());
  }

  private static StreamRuneContext.RequestContext context(String correlationId, String userId) {
    return new StreamRuneContext.RequestContext(
        null,
        userId == null ? null : UserId.of(userId),
        CorrelationId.of(correlationId),
        Instant.now(),
        Map.of());
  }

  @SuppressWarnings("unchecked")
  private static HttpRequest<Object> requestCarrying(StreamRuneContext.RequestContext ctx) {
    HttpRequest<Object> request = mock(HttpRequest.class);
    when(request.getAttribute(
            StreamRuneContextFilter.CONTEXT_ATTRIBUTE, StreamRuneContext.RequestContext.class))
        .thenReturn(Optional.ofNullable(ctx));
    return request;
  }
}

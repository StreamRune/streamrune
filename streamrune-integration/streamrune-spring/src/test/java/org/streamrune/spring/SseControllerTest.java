package org.streamrune.spring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/** Tests for {@link SseController}. */
class SseControllerTest {

  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  @Test
  void aControllerWithoutAnIdentityPolicyIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SseController(mock(SseEventPublisher.class), ALLOW_ALL, null));
  }

  @Test
  void theTrustedGatewayModeHandsTheHeaderToTheAuthorizer() {
    var seen = new java.util.concurrent.atomic.AtomicReference<UserId>();
    SseAuthorizer recording =
        (principal, streamId) -> {
          seen.set(principal);
          return true;
        };
    var controller =
        new SseController(
            mock(SseEventPublisher.class), recording, RequestIdentityPolicy.of(null, true));
    controller.stream("order", "order-1", List.of("alice"));
    assertEquals(UserId.of("alice"), seen.get());
    controller.stream("order", "order-1", List.of());
    assertNull(seen.get());
  }

  @Test
  void theEndpointHandsEveryHeaderValueToThePolicy() {
    // The HTTP endpoint reads the header exactly as the filter does: every value, unsplit.
    var seen = new java.util.concurrent.atomic.AtomicReference<UserId>(UserId.of("sentinel"));
    SseAuthorizer recording =
        (principal, streamId) -> {
          seen.set(principal);
          return true;
        };
    var controller =
        new SseController(
            mock(SseEventPublisher.class), recording, RequestIdentityPolicy.of(null, true));
    var repeated =
        new org.springframework.mock.web.MockHttpServletRequest("GET", "/api/sse/order/o-1");
    repeated.addHeader("X-User-Id", "victim");
    repeated.addHeader("X-User-Id", "real");
    controller.stream("order", "order-1", repeated);
    assertNull(seen.get());
    var single =
        new org.springframework.mock.web.MockHttpServletRequest("GET", "/api/sse/order/o-1");
    single.addHeader("X-User-Id", "a,b");
    controller.stream("order", "order-1", single);
    assertEquals(UserId.of("a,b"), seen.get());
  }

  @Test
  void aContainerThatWithholdsHeadersReadsAsAnAbsentHeader() {
    // HttpServletRequest#getHeaders may return null when the container denies header access.
    var request = mock(jakarta.servlet.http.HttpServletRequest.class);
    when(request.getHeaders("X-User-Id")).thenReturn(null);
    assertEquals(List.of(), ScopedValueFilter.userIdHeaderValues(request));
  }

  @Test
  void theAnonymousModeIgnoresTheHeader() {
    var seen = new java.util.concurrent.atomic.AtomicReference<UserId>(UserId.of("sentinel"));
    SseAuthorizer recording =
        (principal, streamId) -> {
          seen.set(principal);
          return true;
        };
    var controller = new SseController(mock(SseEventPublisher.class), recording, ANONYMOUS);
    controller.stream("order", "order-1", List.of("alice"));
    assertNull(seen.get());
  }

  @Test
  void streamSubscribesToPublisher() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter = controller.stream("cart", "cart-1", List.of());
    assertNotNull(emitter);

    var streamIdCaptor = ArgumentCaptor.forClass(StreamId.class);
    var subscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher).subscribe(streamIdCaptor.capture(), subscriberCaptor.capture(), any());
    assertEquals("cart:cart-1", streamIdCaptor.getValue().value());
    assertNotNull(subscriberCaptor.getValue());
  }

  @Test
  void deniedStreamIsRejectedAndNeverSubscribes() {
    // An unauthorized caller must be rejected with 403 and never subscribed.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, SseAuthorizer.DENY_ALL, ANONYMOUS);

    var ex =
        assertThrows(
            ResponseStatusException.class,
            () -> controller.stream("order", "order-victim", List.of()));
    assertEquals(403, ex.getStatusCode().value());
    verifyNoInteractions(publisher);
  }

  @Test
  void authorizerReceivesAuthenticatedPrincipalAndStreamId() {
    // The ownership decision is made with the authenticated principal, not a header value.
    var publisher = mock(SseEventPublisher.class);
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var seenPrincipal = new java.util.concurrent.atomic.AtomicReference<UserId>();
    var seenStream = new java.util.concurrent.atomic.AtomicReference<StreamId>();
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          seenStream.set(streamId);
          return principal != null && principal.value().equals("bob");
        };
    var controller =
        new SseController(publisher, authorizer, RequestIdentityPolicy.of(resolver, false));

    // A mismatching X-User-Id header is ignored: the principal decides.
    controller.stream("order", "order-1", List.of("alice"));

    assertEquals("bob", seenPrincipal.get().value());
    assertEquals("order:order-1", seenStream.get().value());
  }

  @Test
  void onCompletionUnsubscribesSameSubscriber() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter = controller.stream("cart", "cart-1", List.of());
    var subscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher).subscribe(any(), subscriberCaptor.capture(), any());

    // SseEmitter#complete() does not directly invoke onCompletion callbacks (that is driven
    // by the servlet container). Reach into the private completionCallback field and run it
    // so we can verify the wiring end-to-end.
    runEmitterCallback(emitter, "completionCallback");
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-1")),
            subscriberCaptor.getValue());
  }

  @Test
  void onTimeoutUnsubscribesSameSubscriber() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter = controller.stream("cart", "cart-2", List.of());
    var subscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher).subscribe(any(), subscriberCaptor.capture(), any());

    runEmitterCallback(emitter, "timeoutCallback");
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-2")),
            subscriberCaptor.getValue());
  }

  @Test
  void emitterUsesAFiniteConfiguredTimeout() {
    // An infinite (Long.MAX_VALUE) timeout lets a silently-dead client's subscription live
    // forever. The emitter must carry the configured finite timeout so onTimeout can reap it.
    var publisher = mock(SseEventPublisher.class);
    try (var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, Duration.ofSeconds(90), Duration.ZERO)) {
      var emitter = controller.stream("cart", "cart-1", List.of());
      assertEquals(90_000L, emitter.getTimeout());
      assertNotEquals(Long.MAX_VALUE, emitter.getTimeout());
    }
  }

  @Test
  void nonPositiveTimeoutDisablesTheDeadline() {
    // One contract across the three ports — zero or negative disables the deadline (the
    // keepalive remains the reaper), instead of silently falling back to the 5-minute default and
    // leaving an operator with no way to turn the cap off. SseEmitter(0) is the servlet-async
    // "never time out" value, matching Quarkus/Micronaut's positiveOrNull.
    var publisher = mock(SseEventPublisher.class);
    try (var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, Duration.ZERO, Duration.ofSeconds(30))) {
      assertEquals(0L, controller.stream("cart", "cart-zero", List.of()).getTimeout());
    }
    try (var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, Duration.ofSeconds(-1), Duration.ofSeconds(30))) {
      assertEquals(0L, controller.stream("cart", "cart-negative", List.of()).getTimeout());
    }
  }

  @Test
  void nullTimeoutKeepsTheFiniteDefault() {
    // Unset (null) is NOT "disabled": it means "no explicit value", so the finite default
    // applies — only an explicit non-positive value opts out of the backstop.
    var publisher = mock(SseEventPublisher.class);
    try (var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, null, Duration.ofSeconds(30))) {
      assertEquals(
          Duration.ofMinutes(5).toMillis(),
          controller.stream("cart", "cart-null", List.of()).getTimeout());
    }
  }

  @Test
  void keepAliveTickReapsASubscriberWhoseConnectionIsDead() {
    // A keepalive comment write to a dead (half-open) connection fails, so the subscriber is
    // evicted promptly instead of lingering until the timeout backstop. The publisher subscription
    // must be removed.
    var publisher = mock(SseEventPublisher.class);
    try (var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, Duration.ofMinutes(5), Duration.ofSeconds(30))) {
      var emitter = controller.stream("cart", "cart-9", List.of());
      var subscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
      verify(publisher).subscribe(any(), subscriberCaptor.capture(), any());

      // Sever the connection: a completed emitter makes the next send() throw, mimicking a dead
      // client on an idle stream that never triggered a normal completion.
      emitter.complete();

      controller.sendKeepAlives(); // one keepalive tick

      verify(publisher)
          .unsubscribe(
              StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-9")),
              subscriberCaptor.getValue());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void slowConsumerEviction_completesTheEmitterAndDropsTheRegistration() {
    // The publisher evicts a slow consumer by removing it from the stream and firing
    // its disconnect hook. This controller passed the 2-argument subscribe(), whose hook is a
    // NO-OP, so eviction left the emitter open and the ActiveEmitter registered: the keepalive
    // kept writing successfully (the client is slow, not dead) so the dead-client reaper never
    // reaped it,
    // and with streamrune.sse.timeout=0 no deadline fired either — an open, apparently-healthy SSE
    // stream that receives ZERO further domain events, forever.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter = controller.stream("cart", "cart-slow", List.of());
    assertEquals(1, controller.activeCountForTest());

    var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
    var subscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-slow"))),
            subscriberCaptor.capture(),
            hookCaptor.capture());
    assertNotNull(hookCaptor.getValue(), "the controller must supply a real disconnect hook");

    // The publisher evicts the slow consumer.
    hookCaptor.getValue().accept(new RuntimeException("queue of 256 full"));

    assertEquals(
        0, controller.activeCountForTest(), "the evicted registration must leave the active set");
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-slow")),
            subscriberCaptor.getValue());
    // The hook hands the completion to a thread of the stream's own and does not wait for it.
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThrows(
                    IllegalStateException.class,
                    () -> emitter.send("anything"),
                    "the emitter must be completed so the client reconnects instead of holding a"
                        + " silent stream"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void slowConsumerEviction_returnsWhileAWriteStillHoldsTheEmitter() throws Exception {
    // Spring's emitter completes under the lock a send holds, and a send to a client that has
    // stopped reading holds it until the container's write timeout. The hook runs on the
    // publisher's thread, which serves every stream: it must return while that lock is held.
    var publisher = mock(SseEventPublisher.class);
    try (var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS)) {
      var emitter = controller.stream("cart", "cart-stalled", List.of());
      var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
      verify(publisher).subscribe(any(), any(), hookCaptor.capture());

      // The test thread stands in for a delivery worker inside a blocked servlet write.
      Lock emitterLock = emitterLockOf(emitter);
      emitterLock.lock();
      try {
        var hookReturned = new CountDownLatch(1);
        Thread.ofVirtual()
            .name("publishing-thread")
            .start(
                () -> {
                  hookCaptor.getValue().accept(new RuntimeException("queue of 256 full"));
                  hookReturned.countDown();
                });

        assertTrue(
            hookReturned.await(5, TimeUnit.SECONDS),
            "the eviction hook must return while a write still holds the emitter");
        assertEquals(
            0, controller.activeCountForTest(), "the registration is dropped by the hook itself");
      } finally {
        emitterLock.unlock();
      }

      // With the write over, the stream's own thread completes the emitter.
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> assertThrows(IllegalStateException.class, () -> emitter.send("anything")));
    }
  }

  @Test
  void aClientEvictedBeforeItsOpeningFrameIsAnsweredWithACompletedStream() {
    // The publisher may evict a client the moment it is registered. The opening frame then finds
    // an emitter the eviction has completed and cleaned up after: nothing is written, nothing is
    // cleaned up twice, and the request is answered with that completed emitter.
    var unsubscribed = new java.util.concurrent.atomic.AtomicInteger();
    var publisher =
        new SseEventPublisher() {
          @Override
          public void subscribe(
              StreamId streamId,
              SseSubscriber subscriber,
              java.util.function.Consumer<Throwable> onDisconnect) {
            onDisconnect.accept(new IllegalStateException("queue of 256 full"));
          }

          @Override
          public void unsubscribe(StreamId streamId, SseSubscriber subscriber) {
            unsubscribed.incrementAndGet();
          }
        };
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter =
        assertDoesNotThrow(() -> controller.stream("cart", "cart-evicted-at-once", List.of()));

    assertEquals(0, controller.activeCountForTest());
    assertEquals(1, unsubscribed.get());
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThrows(IllegalStateException.class, () -> emitter.send("anything")));
  }

  @Test
  @SuppressWarnings("unchecked")
  void evictionHook_isIdempotentAgainstAnAlreadyCompletedEmitter() {
    // The transport may already have gone (client closed, deadline fired) when the eviction
    // arrives; the hook must not propagate out of the publisher's eviction path.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var emitter = controller.stream("cart", "cart-done", List.of());
    emitter.complete();

    var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
    verify(publisher).subscribe(any(), any(), hookCaptor.capture());

    assertDoesNotThrow(() -> hookCaptor.getValue().accept(new RuntimeException("evicted")));
    assertEquals(0, controller.activeCountForTest());
  }

  @Test
  void anInvalidTypeOrId_isA400_andNeverReachesTheAuthorizer() {
    var publisher = mock(SseEventPublisher.class);
    var authorizerCalls = new java.util.concurrent.atomic.AtomicInteger();
    SseAuthorizer counting =
        (principal, streamId) -> {
          authorizerCalls.incrementAndGet();
          return true;
        };
    var controller = new SseController(publisher, counting, ANONYMOUS);

    for (var bad :
        List.of(List.of("Order", "o-1"), List.of("order-line", "o-1"), List.of("order", "o\n1"))) {
      var ex =
          assertThrows(
              ResponseStatusException.class,
              () -> controller.stream(bad.get(0), bad.get(1), List.of()));
      assertEquals(400, ex.getStatusCode().value());
    }
    assertEquals(0, authorizerCalls.get());
    verifyNoInteractions(publisher);
  }

  @Test
  void theAuthorizerSeesTheTypedPair() {
    var seen = new java.util.concurrent.atomic.AtomicReference<StreamId>();
    var controller =
        new SseController(
            mock(SseEventPublisher.class),
            (principal, streamId) -> {
              seen.set(streamId);
              return true;
            },
            ANONYMOUS);
    controller.stream("customer", "c-1", List.of());
    assertEquals(AggregateType.of("customer"), seen.get().aggregateType());
    assertEquals(AggregateId.of("c-1"), seen.get().aggregateId());
  }

  /** The lock Spring's emitter holds across a send and takes to complete. */
  private static Lock emitterLockOf(SseEmitter emitter) {
    try {
      var field = ResponseBodyEmitter.class.getDeclaredField("writeLock");
      field.setAccessible(true);
      return (Lock) field.get(emitter);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(
          "Spring ResponseBodyEmitter internals changed: no accessible 'writeLock' field", e);
    }
  }

  private static void runEmitterCallback(SseEmitter emitter, String fieldName) {
    try {
      var field = ResponseBodyEmitter.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      var value = field.get(emitter);
      if (!(value instanceof Runnable r)) {
        throw new AssertionError(
            "Spring ResponseBodyEmitter internals changed: field '"
                + fieldName
                + "' is no longer a Runnable. Consider migrating SseControllerTest to "
                + "MockMvc asyncDispatch.");
      }
      r.run();
    } catch (NoSuchFieldException e) {
      throw new AssertionError(
          "Spring ResponseBodyEmitter internals changed: field '"
              + fieldName
              + "' no longer exists. Consider migrating SseControllerTest to "
              + "MockMvc asyncDispatch.",
          e);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Failed to access " + fieldName, e);
    }
  }
}

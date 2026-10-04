package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.http.sse.Event;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.StepVerifier;

/** Tests for {@link SseController}. */
class SseControllerTest {

  record ProductCreated(String productId) implements DomainEvent {}

  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;

  /** No resolver, no trust flag: every caller is anonymous. */
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  /** The stream every {@code cart} test subscribes to. */
  private static StreamId cartStream(String id) {
    return StreamId.of(AggregateType.of("cart"), AggregateId.of(id));
  }

  private static EventEnvelope envelope(long offset, DomainEvent event) {
    var envelope = mock(EventEnvelope.class);
    when(envelope.globalOffset()).thenReturn(GlobalOffset.of(offset));
    when(envelope.event()).thenReturn(event);
    return envelope;
  }

  @Test
  void deniedStreamIsRejectedAndNeverSubscribes() {
    // An unauthorized caller must be rejected with 403 and never subscribed.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, SseAuthorizer.DENY_ALL, ANONYMOUS);

    var ex =
        assertThrows(
            HttpStatusException.class, () -> controller.stream("order", "order-victim", List.of()));
    assertEquals(403, ex.getStatus().getCode());
    verifyNoInteractions(publisher);
  }

  @Test
  void authorizerReceivesAuthenticatedPrincipalAndStreamId() {
    var publisher = mock(SseEventPublisher.class);
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var seenPrincipal = new AtomicReference<UserId>();
    var seenStream = new AtomicReference<StreamId>();
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          seenStream.set(streamId);
          return true;
        };
    var controller =
        new SseController(publisher, authorizer, RequestIdentityPolicy.of(resolver, false));

    controller.stream("order", "order-1", List.of());

    assertEquals("bob", seenPrincipal.get().value());
    assertEquals("order:order-1", seenStream.get().value());
  }

  /**
   * The SSE caller is resolved by the same {@link RequestIdentityPolicy} as the request filter.
   * Behind a trusted gateway the header is the caller — this endpoint used to ignore it and
   * authorize nobody while a command from the same request ran as the header's user.
   */
  @Test
  void theTrustedGatewayHandsTheHeaderCallerToTheAuthorizer() {
    var seenPrincipal = new AtomicReference<UserId>();
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          return true;
        };
    var controller =
        new SseController(
            mock(SseEventPublisher.class), authorizer, RequestIdentityPolicy.of(null, true));

    controller.stream("order", "order-1", List.of("alice"));

    assertEquals(UserId.of("alice"), seenPrincipal.get());
  }

  /** No resolver and no trust flag — the header is ignored and the caller is anonymous. */
  @Test
  void withoutAResolverOrTheTrustFlagTheHeaderIsNotTheCaller() {
    var seenPrincipal = new AtomicReference<UserId>(UserId.of("sentinel"));
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          return false;
        };
    var controller = new SseController(mock(SseEventPublisher.class), authorizer, ANONYMOUS);

    assertThrows(
        HttpStatusException.class, () -> controller.stream("order", "order-1", List.of("alice")));

    assertNull(seenPrincipal.get());
  }

  /**
   * With a principal, a mismatching header is ignored and the mismatch is logged with both values
   * routed through the sanitizer.
   */
  @Test
  void aMismatchingHeaderIsIgnoredAndLoggedSanitized() {
    var seenPrincipal = new AtomicReference<UserId>();
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          return true;
        };
    AuthenticatedUserResolver resolver = () -> Optional.of(UserId.of("bob"));
    var controller =
        new SseController(
            mock(SseEventPublisher.class), authorizer, RequestIdentityPolicy.of(resolver, false));
    String forged = "alice\n2026-10-02 10:00:00 WARN forged line";

    try (var mocked =
        mockStatic(
            org.streamrune.core.types.LogSanitizer.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      controller.stream("order", "order-1", List.of(forged));
      mocked.verify(() -> org.streamrune.core.types.LogSanitizer.sanitizeForLog(eq(forged)));
      mocked.verify(() -> org.streamrune.core.types.LogSanitizer.sanitizeForLog(eq("bob")));
    }
    assertEquals(UserId.of("bob"), seenPrincipal.get());
  }

  @Test
  void anInvalidTypeOrId_isA400_andNeverReachesTheAuthorizer() {
    var publisher = mock(SseEventPublisher.class);
    var authorizerCalls = new AtomicInteger();
    SseAuthorizer counting =
        (principal, streamId) -> {
          authorizerCalls.incrementAndGet();
          return true;
        };
    var controller = new SseController(publisher, counting, ANONYMOUS);

    for (var bad :
        List.of(
            List.of("Order", "o-1"),
            List.of("order-line", "o-1"),
            List.of("", "o-1"),
            List.of("order", "o\n1"),
            List.of("order", " "))) {
      var ex =
          assertThrows(
              HttpStatusException.class,
              () -> controller.stream(bad.get(0), bad.get(1), List.of()),
              bad.toString());
      assertEquals(400, ex.getStatus().getCode(), bad.toString());
      assertEquals("Invalid aggregate type or aggregate id", ex.getMessage());
    }
    assertEquals(0, authorizerCalls.get());
    verifyNoInteractions(publisher);
  }

  @Test
  void theAuthorizerSeesTheTypedPair() {
    var seen = new AtomicReference<StreamId>();
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

  @Test
  void twoTypesSharingAnIdValueSubscribeToTwoStreams() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var product = controller.stream("product", "p-1", List.of()).subscribe();
    var inventory = controller.stream("inventory", "p-1", List.of()).subscribe();

    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("product"), AggregateId.of("p-1"))), any(), any());
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"))), any(), any());
    product.dispose();
    inventory.dispose();
  }

  @Test
  void theEndpointBindsTheTwoPathSegments() throws Exception {
    var endpoint =
        SseController.class.getMethod(
            "stream", String.class, String.class, io.micronaut.http.HttpRequest.class);
    assertEquals(
        "/{aggregateType}/{aggregateId}",
        endpoint.getAnnotation(io.micronaut.http.annotation.Get.class).value());
    var parameters = endpoint.getParameters();
    assertEquals(
        "aggregateType",
        parameters[0].getAnnotation(io.micronaut.http.annotation.PathVariable.class).value());
    assertEquals(
        "aggregateId",
        parameters[1].getAnnotation(io.micronaut.http.annotation.PathVariable.class).value());
  }

  @Test
  void aNullIdentityPolicyIsRejected() {
    var publisher = mock(SseEventPublisher.class);
    assertThrows(
        IllegalArgumentException.class, () -> new SseController(publisher, ALLOW_ALL, null));
  }

  /**
   * The HTTP endpoint takes the whole request, not a single-valued {@code @Header}: every {@code
   * X-User-Id} value reaches the policy, exactly as the filter reads it.
   */
  @Test
  void theEndpointHandsEveryUserIdHeaderValueToThePolicy() throws Exception {
    var endpoint =
        SseController.class.getMethod(
            "stream", String.class, String.class, io.micronaut.http.HttpRequest.class);
    assertNotNull(endpoint.getAnnotation(io.micronaut.http.annotation.Get.class));

    var seenPrincipal = new AtomicReference<UserId>(UserId.of("sentinel"));
    SseAuthorizer authorizer =
        (principal, streamId) -> {
          seenPrincipal.set(principal);
          return false;
        };
    var controller =
        new SseController(
            mock(SseEventPublisher.class), authorizer, RequestIdentityPolicy.of(null, true));
    io.micronaut.http.MutableHttpRequest<?> repeated =
        io.micronaut.http.HttpRequest.GET("/api/sse/order/order-1");
    repeated.getHeaders().add(RequestIdentityPolicy.USER_ID_HEADER, "victim");
    repeated.getHeaders().add(RequestIdentityPolicy.USER_ID_HEADER, "real");

    assertThrows(HttpStatusException.class, () -> controller.stream("order", "order-1", repeated));

    assertNull(seenPrincipal.get());
  }

  @Test
  void streamSubscribesToPublisher() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    Flux<Event<?>> flux = controller.stream("cart", "cart-1", List.of());
    assertNotNull(flux);

    // Subscribe to activate
    var subscription = flux.subscribe();

    var streamIdCaptor = ArgumentCaptor.forClass(StreamId.class);
    verify(publisher).subscribe(streamIdCaptor.capture(), any(), any());
    assertEquals("cart:cart-1", streamIdCaptor.getValue().value());

    subscription.dispose();
  }

  @Test
  void disposingFluxUnsubscribesFromPublisher() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    Flux<Event<?>> flux = controller.stream("cart", "cart-2", List.of());
    var subscription = flux.subscribe();

    var captor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher).subscribe(eq(cartStream("cart-2")), captor.capture(), any());

    subscription.dispose();

    verify(publisher).unsubscribe(cartStream("cart-2"), captor.getValue());
  }

  @Test
  void publishedEventBecomesFrameworkSerializedSseEvent() {
    var realPublisher = new SseEventPublisher();
    var controller = new SseController(realPublisher, ALLOW_ALL, ANONYMOUS);

    var event = new ProductCreated("product-1");

    StepVerifier.create(controller.stream("cart", "cart-3", List.of()).take(1))
        .then(() -> realPublisher.publish(cartStream("cart-3"), envelope(99L, event)))
        .assertNext(
            sse -> {
              // Framework-serialized event: id = global offset, data = full domain event.
              // No hand-rolled "id:...\ndata:..." framing — Micronaut writes the wire format.
              assertEquals("99", sse.getId());
              assertSame(event, sse.getData());
              assertNull(sse.getName());
            })
        .verifyComplete();
  }

  @Test
  void slowClientOverflowTerminatesStreamAndUnsubscribes() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var event = new ProductCreated("product-2");

    // Subscriber that requests nothing at first — every published event lands in the bounded
    // buffer. Publishing one event more than the buffer capacity overflows it; Reactor's
    // onBackpressureBuffer delivers the overflow error after the buffered items are drained.
    StepVerifier.create(controller.stream("cart", "cart-4", List.of()), 0)
        .then(
            () -> {
              var captor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
              verify(publisher).subscribe(eq(cartStream("cart-4")), captor.capture(), any());
              var sseSubscriber = captor.getValue();
              for (int i = 0; i <= SseController.SLOW_CLIENT_BUFFER; i++) {
                sseSubscriber.send(envelope(i, event));
              }
            })
        .thenRequest(Long.MAX_VALUE)
        .expectNextCount(SseController.SLOW_CLIENT_BUFFER)
        .expectError(IllegalStateException.class)
        .verify(java.time.Duration.ofSeconds(10));

    verify(publisher).unsubscribe(eq(cartStream("cart-4")), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void publisherSideEvictionErrorsTheFluxAndUnsubscribes() {
    // The buffer above bounds a stream this controller is still EMITTING into. The
    // OTHER slow-consumer bound is the publisher's own per-subscriber queue, and it evicts by
    // firing the disconnect hook. This controller passed the 2-argument subscribe(), whose hook is
    // a NO-OP, so eviction left the Flux live and the client on an open, apparently-healthy stream
    // that received ZERO further domain events — while its keepalive kept succeeding (the client
    // is slow, not dead) and, with streamrune.sse.timeout=0, no deadline ever fired.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    StepVerifier.create(controller.stream("cart", "cart-evicted", List.of()))
        .then(
            () -> {
              var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
              verify(publisher)
                  .subscribe(eq(cartStream("cart-evicted")), any(), hookCaptor.capture());
              assertNotNull(
                  hookCaptor.getValue(), "the controller must supply a real disconnect hook");
              hookCaptor.getValue().accept(new IllegalStateException("queue of 256 full"));
            })
        .expectError(IllegalStateException.class)
        .verify(java.time.Duration.ofSeconds(10));

    verify(publisher).unsubscribe(eq(cartStream("cart-evicted")), any());
  }

  // ── dead-client reaping (keepalive + finite timeout) ───────────────────────

  @Test
  void idleStreamReceivesKeepAliveCommentFramesAtTheConfiguredCadence() {
    // A half-open TCP peer (mobile/NAT drop, no FIN/RST) on an IDLE stream produces no writes, so
    // nothing ever detects it: SLOW_CLIENT_BUFFER only bounds a stream that is still EMITTING. The
    // keepalive comment frame forces a periodic socket write, which fails for a dead peer and lets
    // the server cancel the subscription (releasing the publisher subscription, worker and FD).
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ofMillis(50));

    // No domain event is ever published — every frame below is a keepalive.
    StepVerifier.create(controller.stream("cart", "cart-keepalive", List.of()).take(2))
        .assertNext(SseControllerTest::assertKeepAliveFrame)
        .assertNext(SseControllerTest::assertKeepAliveFrame)
        .expectComplete()
        // Bounded: a regression must fail the test, not hang the build waiting for a frame that
        // never comes.
        .verify(java.time.Duration.ofSeconds(10));
  }

  private static void assertKeepAliveFrame(Event<?> frame) {
    assertEquals("keepalive", frame.getComment(), "a keepalive must be a comment frame");
    // TextStreamCodec writes CharSequence data verbatim, so an EMPTY payload emits no data: line at
    // all — the wire form is a bare ":keepalive" comment that every SSE client ignores.
    assertEquals("", frame.getData(), "a keepalive must carry an empty payload");
    assertNull(frame.getId(), "a keepalive must not disturb the client's Last-Event-ID");
    assertNull(frame.getName());
  }

  @Test
  void streamCompletesAtTheConfiguredTimeoutAndUnsubscribes() {
    // The finite-timeout backstop: even with the keepalive disabled, a stream must not stay open
    // forever. Completion must release the publisher subscription; SSE clients auto-reconnect.
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, java.time.Duration.ofMillis(100), null);

    // Virtual time, not a wall-clock margin. The reaping backstop is `stream.take(timeout)`, whose
    // timer resolves Schedulers.parallel() at ASSEMBLY time — i.e. inside this supplier, which
    // withVirtualTime runs only after installing the VirtualTimeScheduler — so the 100ms deadline
    // runs on a clock this test advances by hand. The previous form raced a real 100ms timer
    // against a real `expectNoEvent(90ms)` window whose measurement starts only once the
    // subscription step has been processed: any hiccup >10ms between the two put the onComplete
    // INSIDE the window and failed with "expected no event: onComplete()" (reproduced here by
    // inserting a 20ms pause after expectSubscription). Both halves of the assertion survive, now
    // exactly rather than probabilistically: nothing is emitted through 99ms, and the completion
    // lands the moment the clock crosses the configured 100ms.
    StepVerifier.withVirtualTime(() -> controller.stream("cart", "cart-timeout", List.of()))
        .expectSubscription()
        .expectNoEvent(java.time.Duration.ofMillis(99))
        .thenAwait(java.time.Duration.ofMillis(1))
        .expectComplete()
        .verify(java.time.Duration.ofSeconds(10));

    verify(publisher, timeout(5000)).unsubscribe(eq(cartStream("cart-timeout")), any());
  }

  @Test
  void keepAliveDisabledByZeroInterval_streamStaysSilent() {
    // Zero/negative disables the keepalive, leaving the timeout as the sole reaper (the documented
    // knob semantics, shared with Spring and Quarkus).
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ZERO);

    // Long enough that a 50ms-style keepalive would have fired several times had one been
    // scheduled.
    StepVerifier.create(controller.stream("cart", "cart-no-keepalive", List.of()))
        .expectSubscription()
        .expectNoEvent(java.time.Duration.ofMillis(300))
        .thenCancel()
        .verify(java.time.Duration.ofSeconds(5));
  }

  // ── the failure-path WARNs render the throwable into the message ───────────
  //
  // Each WARN below used to pass the Throwable as the argument for its LAST "{}". SLF4J treats a
  // trailing Throwable as the exception to attach, not as a format argument, so the operator saw
  // the placeholder itself ("...: {}") plus a full stack trace instead of the cause in the message.
  // The Spring sibling renders t.toString(); these pin the same rendered line on Micronaut.

  @Test
  void failedKeepAliveTickWarnNamesTheCause() throws InterruptedException {
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, null, java.time.Duration.ofMillis(50));
    // Reactor's serialized sink cancels and swallows a RuntimeException thrown downstream of
    // sink.next; only a JVM-fatal error (a LinkageError here) escapes it to the tick's catch.
    var cause = new LinkageError("client gone");

    var subscriber = new ThrowingSubscriber(Callback.NEXT, cause);
    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-dead-tick", List.of()).subscribe(subscriber);

      assertEquals(
          List.of("Evicting an SSE subscriber after a failed keepalive tick: " + cause),
          warnings.awaitMessages(1));
    } finally {
      // Belt and braces: the controller releases the stream itself after a fatal tick (pinned
      // below); cancelling guarantees no 50 ms ticker outlives this test even if that regresses.
      subscriber.cancel();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void failedEvictionWarnNamesTheStreamAndTheCause() throws InterruptedException {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);
    var cause = new IllegalStateException("client gone");

    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-dead-evict", List.of())
          .subscribe(new ThrowingSubscriber(Callback.ERROR, cause));
      var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
      verify(publisher).subscribe(eq(cartStream("cart-dead-evict")), any(), hookCaptor.capture());

      hookCaptor.getValue().accept(new IllegalStateException("queue of 256 full"));

      assertEquals(
          List.of("Failed to terminate an evicted SSE stream for cart:cart-dead-evict: " + cause),
          warnings.awaitMessages(1));
    }
  }

  // ── a JVM-fatal error out of sink.next poisons Reactor's serialized sink ──────────────────
  //
  // FluxCreate's SerializedFluxSink claims its work-in-progress flag around every next(); a
  // downstream RuntimeException is caught there (cancel + swallow), but a JVM-fatal error (a
  // LinkageError or VirtualMachineError) is rethrown by Exceptions.throwIfFatal BEFORE the flag is
  // released. The flag then stays claimed forever, so every later sink.error() only records the
  // error and returns: no terminal signal, no onDispose — the publisher subscription and the
  // keepalive ticker were never released (the ticker kept firing into the dead sink for the life
  // of the JVM, the publisher kept the subscriber registered). Reactive Streams §2.13 says a
  // subscriber that throws from onNext must be considered cancelled; these pin that the controller
  // runs the cancel teardown itself on both paths that can observe the fatal error.

  @Test
  void fatalKeepAliveFailureReleasesThePublisherSubscriptionAndStopsTheTicker()
      throws InterruptedException {
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, null, java.time.Duration.ofMillis(50));
    var subscriber = new ThrowingSubscriber(Callback.NEXT, new LinkageError("client gone"));
    var droppedKeepAlives = countDroppedKeepAlives();
    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-fatal-tick", List.of()).subscribe(subscriber);
      assertEquals(1, warnings.awaitMessages(1).size(), "the first keepalive tick must fail");

      assertStreamReleased(publisher, "cart-fatal-tick", droppedKeepAlives);
    } finally {
      Hooks.resetOnNextDropped();
      subscriber.cancel();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void fatalDeliveryFailureEvictionReleasesThePublisherSubscriptionAndStopsTheTicker()
      throws InterruptedException {
    // The delivery-path twin: SseEventPublisher's worker catches the fatal error out of send(),
    // evicts, and fires the disconnect hook — whose sink.error() the poisoned sink swallowed too.
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, null, java.time.Duration.ofMillis(50));
    var cause = new LinkageError("client gone");
    var subscriber = new ThrowingSubscriber(Callback.EVENT, cause);
    var droppedKeepAlives = countDroppedKeepAlives();
    try {
      controller.stream("cart", "cart-fatal-send", List.of()).subscribe(subscriber);
      var sseCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
      var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
      verify(publisher)
          .subscribe(eq(cartStream("cart-fatal-send")), sseCaptor.capture(), hookCaptor.capture());

      // What SseEventPublisher's delivery worker does: send throws, the worker evicts.
      var thrown =
          assertThrows(
              LinkageError.class,
              () -> sseCaptor.getValue().send(envelope(7, new ProductCreated("product-7"))));
      assertSame(cause, thrown);
      hookCaptor.getValue().accept(thrown);

      assertStreamReleased(publisher, "cart-fatal-send", droppedKeepAlives);
    } finally {
      Hooks.resetOnNextDropped();
      subscriber.cancel();
    }
  }

  /** Counts keepalive frames Reactor drops on a terminated/poisoned sink: a live ticker's ticks. */
  private static AtomicInteger countDroppedKeepAlives() {
    var dropped = new AtomicInteger();
    Hooks.onNextDropped(
        item -> {
          if (item == SseController.KEEP_ALIVE) {
            dropped.incrementAndGet();
          }
        });
    return dropped;
  }

  private static void assertStreamReleased(
      SseEventPublisher publisher, String streamId, AtomicInteger droppedKeepAlives)
      throws InterruptedException {
    // Let a tick that was already in flight when the stream was released settle, then watch for
    // six keepalive intervals: a live ticker drops one frame into the dead sink every 50 ms.
    Thread.sleep(100);
    int settled = droppedKeepAlives.get();
    Thread.sleep(300);
    int later = droppedKeepAlives.get();
    assertAll(
        () ->
            assertEquals(
                settled,
                later,
                "the keepalive ticker must be stopped once the stream failed fatally"),
        () -> verify(publisher, timeout(1000)).unsubscribe(eq(cartStream(streamId)), any()));
  }

  private enum Callback {
    NEXT,
    EVENT,
    ERROR
  }

  /**
   * A downstream that throws from one callback, so the controller's sink call driving it ({@code
   * next} or {@code error}) throws in turn: the failure each WARN reports. NEXT throws for
   * keepalive frames only, EVENT for domain-event frames only.
   */
  private static final class ThrowingSubscriber implements reactor.core.CoreSubscriber<Event<?>> {
    private final Callback callback;
    private final Throwable cause;
    private volatile org.reactivestreams.Subscription subscription;

    ThrowingSubscriber(Callback callback, Throwable cause) {
      this.callback = callback;
      this.cause = cause;
    }

    @Override
    public void onSubscribe(org.reactivestreams.Subscription subscription) {
      this.subscription = subscription;
      subscription.request(Long.MAX_VALUE);
    }

    void cancel() {
      var held = subscription;
      if (held != null) {
        held.cancel();
      }
    }

    @Override
    public void onNext(Event<?> item) {
      if (callback == Callback.NEXT && item == SseController.KEEP_ALIVE) {
        sneakyThrow(cause);
      }
      if (callback == Callback.EVENT && item != SseController.KEEP_ALIVE) {
        sneakyThrow(cause);
      }
    }

    @Override
    public void onError(Throwable failure) {
      if (callback == Callback.ERROR) {
        sneakyThrow(cause);
      }
    }

    @Override
    public void onComplete() {}

    private static void sneakyThrow(Throwable t) {
      if (t instanceof Error e) {
        throw e;
      }
      throw (RuntimeException) t;
    }
  }

  /** Captures the rendered messages of {@link SseController}'s log events (logback, WARN+). */
  private static final class CapturedSseControllerLog implements AutoCloseable {
    private final ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SseController.class);
    private final ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
        appender = new ch.qos.logback.core.read.ListAppender<>();

    CapturedSseControllerLog() {
      appender.start();
      logger.addAppender(appender);
    }

    /** Waits (bounded) until {@code count} events arrived, then returns all their messages. */
    List<String> awaitMessages(int count) throws InterruptedException {
      long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
      while (snapshot().size() < count && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      return snapshot();
    }

    // AppenderBase.doAppend appends under the appender's monitor (the keepalive WARN arrives on a
    // Reactor scheduler thread), so read the list under the same monitor.
    private List<String> snapshot() {
      synchronized (appender) {
        return appender.list.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .toList();
      }
    }

    @Override
    public void close() {
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}

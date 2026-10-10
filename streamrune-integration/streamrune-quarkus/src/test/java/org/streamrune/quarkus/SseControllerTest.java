package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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

/** Tests for {@link SseController}. */
class SseControllerTest {

  record OrderCreated(String orderId) implements DomainEvent {}

  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;

  private static EventEnvelope envelope(long offset, DomainEvent event) {
    return envelope(null, offset, event);
  }

  private static EventEnvelope envelope(StreamId stream, long offset, DomainEvent event) {
    var envelope = mock(EventEnvelope.class);
    when(envelope.streamId()).thenReturn(stream);
    when(envelope.globalOffset()).thenReturn(GlobalOffset.of(offset));
    when(envelope.event()).thenReturn(event);
    return envelope;
  }

  /** No resolver, no trust flag: every caller is anonymous. */
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  @Test
  void disabledAtRuntimeReturns404AndNeverSubscribes() {
    // streamrune.sse.enabled=false at runtime must disable the endpoint (404) even though
    // the
    // @IfBuildProperty build gate registered the resource — mirroring Spring/Micronaut, where the
    // bean simply does not exist when disabled. The 404 fires before any
    // authorization/subscription.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS, () -> Boolean.FALSE);

    assertThrows(NotFoundException.class, () -> controller.stream("order", "order-1", List.of()));
    verifyNoInteractions(publisher);
  }

  @Test
  void enabledAtRuntimeStreams() {
    // Companion: with the runtime flag true, the endpoint serves (reaches the publisher).
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS, () -> Boolean.TRUE);

    assertNotNull(controller.stream("order", "order-1", List.of()));
  }

  @Test
  void deniedStreamIsRejectedAndNeverSubscribes() {
    // An unauthorized caller must be rejected with 403 and never subscribed.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, SseAuthorizer.DENY_ALL, ANONYMOUS);

    assertThrows(
        ForbiddenException.class, () -> controller.stream("order", "order-victim", List.of()));
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

  @Test
  void streamSubscribesToPublisher() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var multi = controller.stream("cart", "cart-1", List.of());
    assertNotNull(multi);

    // Subscribe to drive the Multi (emitter-based Multis require a subscriber to activate)
    var subscriber = AssertSubscriber.create(Long.MAX_VALUE);
    multi.subscribe(subscriber);

    var streamIdCaptor = ArgumentCaptor.forClass(StreamId.class);
    verify(publisher).subscribe(streamIdCaptor.capture(), any(), any());
    assertEquals("cart:cart-1", streamIdCaptor.getValue().value());
  }

  @Test
  void cancellingMultiUnsubscribesFromPublisher() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var multi = controller.stream("cart", "cart-2", List.of());
    var subscriber = AssertSubscriber.create(Long.MAX_VALUE);
    multi.subscribe(subscriber);

    var sseSubscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-2"))),
            sseSubscriberCaptor.capture(),
            any());

    // Cancel the Multi subscription — triggers onTermination
    subscriber.cancel();

    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-2")),
            sseSubscriberCaptor.getValue());
  }

  @Test
  void publishedEventBecomesFrameworkSerializedSseEvent() {
    var publisher = new SseEventPublisher();
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var multi = controller.stream("cart", "cart-3", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    var event = new OrderCreated("order-1");
    publisher.publish(
        envelope(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-3")), 42L, event));

    // Delivery is asynchronous (per-subscriber queue drained by a worker thread), so await it.
    testSubscriber.awaitItems(2, java.time.Duration.ofSeconds(5));
    testSubscriber.assertNotTerminated();
    assertEquals(2, testSubscriber.getItems().size());
    assertSame(
        SseController.KEEP_ALIVE,
        testSubscriber.getItems().get(0),
        "the stream opens with the comment frame, emitted once the client is registered");
    OutboundSseEvent item = testSubscriber.getItems().get(1);
    // Framework-serialized event: id = global offset, data = full domain event as JSON.
    // No hand-rolled "id:...\ndata:..." framing — Quarkus REST writes the wire format.
    assertEquals("42", item.getId());
    assertSame(event, item.getData());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, item.getMediaType());
    assertEquals(OrderCreated.class, item.getType());
    assertEquals(OrderCreated.class, item.getGenericType());
    // Unnamed event — clients receive default "message" events, matching Spring.
    assertNull(item.getName());
    assertNull(item.getComment());
    assertFalse(item.isReconnectDelaySet());
    assertEquals(-1L, item.getReconnectDelay());
  }

  @Test
  void slowClientOverflowTerminatesStreamAndUnsubscribes() {
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var multi = controller.stream("cart", "cart-4", List.of());
    // Subscriber that never requests — every published event lands in the bounded buffer
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(0);
    multi.subscribe(testSubscriber);

    var sseSubscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-4"))),
            sseSubscriberCaptor.capture(),
            any());
    var sseSubscriber = sseSubscriberCaptor.getValue();

    var event = new OrderCreated("order-2");
    for (int i = 0; i <= SseController.SLOW_CLIENT_BUFFER; i++) {
      sseSubscriber.send(envelope(i, event));
    }

    testSubscriber.assertFailedWith(BackPressureFailure.class);
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-4")), sseSubscriber);
  }

  @Test
  @SuppressWarnings("unchecked")
  void publisherSideEvictionFailsTheMultiAndUnsubscribes() {
    // The buffer above bounds a stream this controller is still EMITTING into. The
    // OTHER slow-consumer bound is the publisher's own per-subscriber queue, and it evicts by
    // firing the disconnect hook. This controller passed the 2-argument subscribe(), whose hook is
    // a NO-OP, so eviction left the Multi live and the client on an open, apparently-healthy
    // stream that received ZERO further domain events — while its keepalive kept succeeding (the
    // client is slow, not dead) and, with streamrune.sse.timeout=0, no deadline ever fired.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);

    var multi = controller.stream("cart", "cart-evicted", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
    var sseSubscriberCaptor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-evicted"))),
            sseSubscriberCaptor.capture(),
            hookCaptor.capture());
    assertNotNull(hookCaptor.getValue(), "the controller must supply a real disconnect hook");

    hookCaptor.getValue().accept(new RuntimeException("queue of 256 full"));

    testSubscriber.assertFailedWith(RuntimeException.class, "queue of 256 full");
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-evicted")),
            sseSubscriberCaptor.getValue());
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

    var multi = controller.stream("cart", "cart-keepalive", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    // No domain event is ever published — every item below is a keepalive: the opening frame and
    // two ticks.
    testSubscriber.awaitItems(3, java.time.Duration.ofSeconds(5));
    testSubscriber.assertNotTerminated();

    for (OutboundSseEvent frame : testSubscriber.getItems()) {
      assertEquals(" keepalive", frame.getComment(), "a keepalive must be a comment frame");
      assertNull(frame.getData(), "a keepalive must carry no data — clients must ignore it");
      assertNull(frame.getId(), "a keepalive must not disturb the client's Last-Event-ID");
      assertNull(frame.getName());
    }
    testSubscriber.cancel();
    controller.shutdown();
  }

  @Test
  void streamCompletesAtTheConfiguredTimeoutAndUnsubscribes() {
    // The finite-timeout backstop: even with the keepalive disabled, a stream must not stay open
    // forever. Completion must release the publisher subscription; SSE clients auto-reconnect.
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, java.time.Duration.ofMillis(100), null);

    var multi = controller.stream("cart", "cart-timeout", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    var captor = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-timeout"))),
            captor.capture(),
            any());

    testSubscriber.awaitCompletion(java.time.Duration.ofSeconds(5));
    // The downstream completion and the upstream cancellation that fires onTermination both happen
    // on the timer thread, in that order — so allow a moment for the unsubscribe to land.
    verify(publisher, timeout(5000))
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-timeout")),
            captor.getValue());
    controller.shutdown();
  }

  @Test
  void keepAliveDisabledByZeroInterval_theOpeningFrameIsTheOnlyComment()
      throws InterruptedException {
    // Zero/negative disables the keepalive, leaving the timeout as the sole reaper (the documented
    // knob semantics, shared with Spring and Micronaut). The opening frame is not a keepalive tick:
    // it is written whatever the interval.
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ZERO);

    var multi = controller.stream("cart", "cart-no-keepalive", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    // Long enough that a 50ms-style keepalive would have fired many times had one been scheduled.
    Thread.sleep(300);
    assertEquals(List.of(SseController.KEEP_ALIVE), testSubscriber.getItems());
    testSubscriber.assertNotTerminated();
    testSubscriber.cancel();
    controller.shutdown();
  }

  @Test
  void keepAliveWritesAreSerializedWithEventDelivery() throws InterruptedException {
    // Both the delivery worker and the keepalive scheduler drive the same Mutiny emitter; Reactive
    // Streams requires serial onNext, so the controller must serialize them. With a tight keepalive
    // and a burst of events, no domain event may be lost and the stream must stay healthy.
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ofMillis(1));

    var multi = controller.stream("cart", "cart-race", List.of());
    var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    multi.subscribe(testSubscriber);

    var event = new OrderCreated("order-race");
    for (int i = 0; i < 50; i++) {
      publisher.publish(
          envelope(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-race")), i, event));
    }

    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
    long delivered = 0;
    while (System.nanoTime() < deadline) {
      delivered = testSubscriber.getItems().stream().filter(i -> i.getData() != null).count();
      if (delivered >= 50) {
        break;
      }
      Thread.sleep(20);
    }
    assertEquals(50L, delivered, "every domain event must survive interleaved keepalive writes");
    testSubscriber.assertNotTerminated();
    testSubscriber.cancel();
    controller.shutdown();
  }

  // ── the shared scheduler must not retain cancelled tasks or stall on a slow stream ──

  @Test
  void cancelledDeadlineTasksArePurgedFromTheSchedulerQueue() {
    // Executors.newSingleThreadScheduledExecutor's DelegatedScheduledExecutorService
    // wrapper defaults removeOnCancelPolicy to false and exposes no setter to change it, so a
    // cancelled deadline task (every normal disconnect calls Future.cancel(false) in
    // emitter.onTermination) stayed in the delay queue until it would have fired — up to the full
    // configured timeout. At churny reconnect rates that retains one dead emitter chain per
    // disconnect for the whole timeout window: heap growth proportional to (reconnect rate x
    // timeout) that looks exactly like a leak. Long timeout + keepalive disabled isolates the
    // deadline-task queue from keepalive ticks, matching the finding's own proposed repro.
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, java.time.Duration.ofMinutes(5), null);

    int streams = 200;
    for (int i = 0; i < streams; i++) {
      var multi = controller.stream("cart", "cart-cancel-" + i, List.of());
      var testSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
      multi.subscribe(testSubscriber);
      testSubscriber.cancel();
    }

    assertEquals(
        0,
        controller.testKeepAliveExecutor().getQueue().size(),
        "every cancelled deadline task must be purged from the delay queue immediately, not"
            + " linger until it would have fired");
    controller.shutdown();
  }

  @Test
  void slowStreamsKeepAliveTickDoesNotStallTheSharedSchedulerForOtherStreams()
      throws InterruptedException {
    // keepAlive is the process's SINGLE
    // shared scheduler thread for every stream's keepalive ticks AND deadline tasks. Before this
    // fix, a keepalive tick took `synchronized (lock)` UNCONDITIONALLY, so if a stream's OWN
    // delivery (holding that stream's lock) was slow/blocked when its tick fired, the tick — and
    // therefore the ONE shared scheduler thread — blocked for as long as the slow send did,
    // starving every OTHER stream's keepalive ticks and deadline tasks too. The fix makes the tick
    // tryLock() and skip when contended, exactly like Spring's sendKeepAlives.
    //
    // Measures the MAXIMUM GAP between consecutive "fast" keepalive arrivals rather than a total
    // count in a fixed window: scheduleAtFixedRate self-heals from a stall by firing catch-up
    // executions once the thread frees up, so a raw count over a long-enough window converges
    // regardless of the bug — the actual harm is a LATENCY spike (a truly dead
    // client's failed keepalive write, and every other stream's dead-client detection, is delayed
    // by up to the full stalled duration), which only a gap/latency measurement exposes.
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ofMillis(15));

    // "slow" stream: its downstream onItem blocks for blockMillis on the domain event (not on
    // keepalives, which carry null data) — simulating a delivery worker stuck inside a slow
    // emitter.emit() call while still holding that stream's sendLock. Long enough that at least
    // one of "slow"'s OWN keepalive ticks (interval 15ms) is virtually certain to fire while the
    // lock is held and contend for it.
    long blockMillis = 500;
    var slowMulti = controller.stream("cart", "cart-slow", List.of());
    slowMulti
        .subscribe()
        .with(
            item -> {
              if (item.getData() != null) {
                try {
                  Thread.sleep(blockMillis);
                } catch (InterruptedException _) {
                  Thread.currentThread().interrupt();
                }
              }
            },
            failure -> {});

    // "fast" stream: timestamps every keepalive arrival so the gap between consecutive arrivals
    // can be measured.
    var fastMulti = controller.stream("cart", "cart-fast", List.of());
    var fastArrivalNanos = java.util.Collections.synchronizedList(new java.util.ArrayList<Long>());
    fastMulti.subscribe().with(item -> fastArrivalNanos.add(System.nanoTime()), failure -> {});

    // Trigger the slow stream's blocking onItem.
    publisher.publish(
        envelope(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-slow")),
            1L,
            new OrderCreated("order-slow")));

    // Observe for well past the block window, then inspect the largest inter-arrival gap on the
    // FAST stream. A shared-thread stall shows up as one gap close to blockMillis; a healthy
    // scheduler never gaps much beyond the configured 15ms interval (generous slack for CI/GC
    // jitter and the shared build load noted for this run).
    Thread.sleep(blockMillis + 400);

    List<Long> arrivals;
    synchronized (fastArrivalNanos) {
      arrivals = new java.util.ArrayList<>(fastArrivalNanos);
    }
    assertTrue(
        arrivals.size() >= 10,
        "the fast stream must have received multiple keepalives to measure a gap; got "
            + arrivals.size());
    long maxGapMillis = 0;
    for (int i = 1; i < arrivals.size(); i++) {
      maxGapMillis =
          Math.max(
              maxGapMillis, TimeUnit.NANOSECONDS.toMillis(arrivals.get(i) - arrivals.get(i - 1)));
    }
    assertTrue(
        maxGapMillis < blockMillis / 2,
        "the fast stream's keepalive arrivals must never gap anywhere near the slow stream's"
            + " blockMillis="
            + blockMillis
            + " — the shared scheduler thread must not stall behind a contended lock on another"
            + " stream; observed max gap "
            + maxGapMillis
            + "ms across arrivals "
            + arrivals.size());
    controller.shutdown();
  }

  @Test
  void slowStreamsDeadlineDoesNotStallTheSharedSchedulerForOtherStreams()
      throws InterruptedException {
    // The residual half of the keepalive fix: the keepalive tick was converted to tryLock()
    // on the stated premise that "keepAlive is the process's SINGLE shared scheduler thread for
    // every stream's keepalive ticks AND deadline tasks", so parking it on one stream's send lock
    // starves every other stream. The DEADLINE task on that same single thread kept taking the
    // lock UNCONDITIONALLY, so the fleet-wide stall the commit removed on one path survived on the
    // other: one stream stuck inside a slow emitter.emit() delays every other stream's keepalives
    // AND their deadline completions — reopening, for all of them, exactly the FD-leak window
    // the deadline exists to close.
    //
    // Deterministic, no sleeps in the assertion path: the keepalive is disabled so DEADLINE tasks
    // are the only work on the scheduler, and the slow stream is subscribed FIRST so its deadline
    // holds the lower sequence number in the delay queue and therefore runs first at the shared
    // deadline instant. Whether the fast stream's deadline fires on time is then purely a question
    // of whether the slow stream's deadline blocked the thread.
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, java.time.Duration.ofSeconds(1), null);

    var insideBlockingSend = new java.util.concurrent.CountDownLatch(1);
    var releaseBlockingSend = new java.util.concurrent.CountDownLatch(1);

    // Slow stream: its downstream parks inside the domain-event delivery, so the delivery worker
    // holds this stream's sendLock for the whole window — a stalled emitter.emit(), the
    // premise of the keepalive fix.
    var slowMulti = controller.stream("cart", "cart-slow-deadline", List.of());
    slowMulti
        .subscribe()
        .with(
            item -> {
              if (item.getData() == null) {
                return;
              }
              insideBlockingSend.countDown();
              try {
                releaseBlockingSend.await(30, TimeUnit.SECONDS);
              } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
              }
            },
            failure -> {});

    var fastMulti = controller.stream("cart", "cart-fast-deadline", List.of());
    var fastSubscriber = AssertSubscriber.<OutboundSseEvent>create(Long.MAX_VALUE);
    fastMulti.subscribe(fastSubscriber);

    publisher.publish(
        envelope(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-slow-deadline")),
            1L,
            new OrderCreated("order-slow")));
    assertTrue(
        insideBlockingSend.await(5, TimeUnit.SECONDS),
        "the slow stream's delivery worker must be inside its blocking send (holding its sendLock)"
            + " before the deadline instant");

    try {
      // The fast stream's own deadline is 1s after ITS subscribe; 4s is generous slack for CI
      // scheduling jitter yet far below the 30s the slow stream can hold its lock. Failing here
      // means the fast stream's completion was queued behind another stream's blocked deadline.
      fastSubscriber.awaitCompletion(java.time.Duration.ofSeconds(4));
    } finally {
      releaseBlockingSend.countDown();
      controller.shutdown();
    }
  }

  // ── the failure-path WARNs render the throwable into the message ─────────
  //
  // Each WARN below used to pass the Throwable as the argument for its LAST "{}". SLF4J treats a
  // trailing Throwable as the exception to attach, not as a format argument, so the operator saw
  // the placeholder itself ("...: {}") plus a full stack trace instead of the cause in the message.
  // The Spring sibling renders t.toString(); these pin the same rendered line on Quarkus.

  @Test
  void failedKeepAliveTickWarnNamesTheCause() throws InterruptedException {
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(
            publisher,
            ALLOW_ALL,
            ANONYMOUS,
            java.time.Duration.ofMinutes(5),
            java.time.Duration.ofMillis(50));
    var cause = new IllegalStateException("client gone");

    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-dead-tick", List.of())
          .subscribe(new ThrowingSubscriber(Callback.TICK, cause));

      assertEquals(
          List.of("Evicting an SSE subscriber after a failed keepalive tick: " + cause),
          warnings.awaitMessages(1));
      // The downstream threw from onNext, so the emitter cannot terminate: the eviction itself
      // releases the stream.
      verify(publisher, timeout(1000))
          .unsubscribe(
              eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-dead-tick"))), any());
      assertEquals(0, controller.openStreamCount());
    } finally {
      controller.shutdown();
    }
  }

  @Test
  void aFailedOpeningFrameEvictsTheSubscriberLikeAFailedKeepAliveTick()
      throws InterruptedException {
    // The opening frame is the first write to the client. When it fails the client is already
    // registered with the publisher, so the stream must be torn down the way a failed tick tears
    // it down: failed, removed from the open streams, unsubscribed.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS);
    var cause = new IllegalStateException("client gone");

    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-dead-opening", List.of())
          .subscribe(new ThrowingSubscriber(Callback.ITEM, cause));

      assertEquals(
          List.of("Evicting an SSE subscriber after a failed opening frame: " + cause),
          warnings.awaitMessages(1));
      var registered = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
      var stream = StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-dead-opening"));
      var order = inOrder(publisher);
      order.verify(publisher).subscribe(eq(stream), registered.capture(), any());
      order.verify(publisher).unsubscribe(stream, registered.getValue());
      assertEquals(0, controller.openStreamCount());
    } finally {
      controller.shutdown();
    }
  }

  @Test
  void failedTimeoutCompletionWarnNamesTheCause() throws InterruptedException {
    var publisher = mock(SseEventPublisher.class);
    var controller =
        new SseController(publisher, ALLOW_ALL, ANONYMOUS, java.time.Duration.ofMillis(100), null);
    var cause = new IllegalStateException("client gone");

    try (var warnings = new CapturedSseControllerLog()) {
      controller.stream("cart", "cart-dead-deadline", List.of())
          .subscribe(new ThrowingSubscriber(Callback.COMPLETION, cause));

      assertEquals(
          List.of("Failed to complete an SSE stream at its configured timeout: " + cause),
          warnings.awaitMessages(1));
    } finally {
      controller.shutdown();
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
          .subscribe(new ThrowingSubscriber(Callback.FAILURE, cause));
      var hookCaptor = ArgumentCaptor.forClass(java.util.function.Consumer.class);
      verify(publisher)
          .subscribe(
              eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-dead-evict"))),
              any(),
              hookCaptor.capture());

      hookCaptor.getValue().accept(new RuntimeException("queue of 256 full"));

      assertEquals(
          List.of("Failed to fail an evicted SSE stream for cart:cart-dead-evict: " + cause),
          warnings.awaitMessages(1));
      // The emitter could not be failed, so its termination hook is not what releases the stream.
      verify(publisher)
          .unsubscribe(
              eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-dead-evict"))), any());
      assertEquals(0, controller.openStreamCount());
    } finally {
      controller.shutdown();
    }
  }

  private enum Callback {
    ITEM,
    TICK,
    FAILURE,
    COMPLETION
  }

  /**
   * A downstream that throws from one callback, so the controller's emitter call driving it ({@code
   * emit}, {@code fail} or {@code complete}) throws in turn: the failure each WARN reports. ITEM
   * throws for every keepalive frame, so for the opening frame; TICK lets the opening frame pass
   * and throws for the keepalive frames after it, the periodic ticks.
   */
  private static final class ThrowingSubscriber
      implements io.smallrye.mutiny.subscription.MultiSubscriber<OutboundSseEvent> {
    private final Callback callback;
    private final RuntimeException cause;
    private final java.util.concurrent.atomic.AtomicInteger keepAlives =
        new java.util.concurrent.atomic.AtomicInteger();

    ThrowingSubscriber(Callback callback, RuntimeException cause) {
      this.callback = callback;
      this.cause = cause;
    }

    @Override
    public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onItem(OutboundSseEvent item) {
      if (item != SseController.KEEP_ALIVE) {
        return;
      }
      int seen = keepAlives.incrementAndGet();
      if (callback == Callback.ITEM || (callback == Callback.TICK && seen > 1)) {
        throw cause;
      }
    }

    @Override
    public void onFailure(Throwable failure) {
      if (callback == Callback.FAILURE) {
        throw cause;
      }
    }

    @Override
    public void onCompletion() {
      if (callback == Callback.COMPLETION) {
        throw cause;
      }
    }
  }

  /**
   * Captures the rendered messages of {@link SseController}'s log records. A java.util.logging
   * Handler, not a Logback ListAppender: this module's tests bind SLF4J to JBoss LogManager, a JUL
   * provider (as in {@code
   * ProjectionProducerTest#inlineProjectionWarnsAboutTheMissingRecoveryPath}).
   */
  private static final class CapturedSseControllerLog implements AutoCloseable {
    private final java.util.logging.Logger julLogger =
        java.util.logging.Logger.getLogger(SseController.class.getName());
    private final java.util.logging.Level originalLevel = julLogger.getLevel();
    private final List<String> messages = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.logging.Handler handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord logRecord) {
            messages.add(logRecord.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    CapturedSseControllerLog() {
      julLogger.addHandler(handler);
      julLogger.setLevel(java.util.logging.Level.ALL);
    }

    /** Waits (bounded) until {@code count} records arrived, then returns all of them. */
    List<String> awaitMessages(int count) throws InterruptedException {
      long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
      while (messages.size() < count && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      return List.copyOf(messages);
    }

    @Override
    public void close() {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
    }
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
              BadRequestException.class,
              () -> controller.stream(bad.get(0), bad.get(1), List.of()));
      assertEquals(400, ex.getResponse().getStatus());
      assertEquals("Invalid aggregate type or aggregate id", ex.getMessage());
    }
    assertEquals(0, authorizerCalls.get());
    verifyNoInteractions(publisher);
  }

  @Test
  void theRuntimeKillSwitchIsCheckedBeforeTheParts() {
    // A disabled endpoint answers 404 even for a malformed path: the kill switch comes first.
    var publisher = mock(SseEventPublisher.class);
    var controller = new SseController(publisher, ALLOW_ALL, ANONYMOUS, () -> Boolean.FALSE);

    assertThrows(NotFoundException.class, () -> controller.stream("Order", "o-1", List.of()));
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
  void theEndpointBindsTheTwoPathSegments() throws Exception {
    // Quarkus REST cannot boot in this module, so the JAX-RS binding is pinned structurally.
    var stream =
        SseController.class.getMethod(
            "stream", String.class, String.class, jakarta.ws.rs.core.HttpHeaders.class);
    assertEquals(
        "/{aggregateType}/{aggregateId}", stream.getAnnotation(jakarta.ws.rs.Path.class).value());
    var parameters = stream.getParameters();
    assertEquals(
        "aggregateType", parameters[0].getAnnotation(jakarta.ws.rs.PathParam.class).value());
    assertEquals("aggregateId", parameters[1].getAnnotation(jakarta.ws.rs.PathParam.class).value());
  }

  @Test
  void theHttpEndpointHandsBothSegmentsAndTheHeadersOn() {
    var seen = new AtomicReference<StreamId>();
    var controller =
        new SseController(
            mock(SseEventPublisher.class),
            (principal, streamId) -> {
              seen.set(streamId);
              return true;
            },
            ANONYMOUS);
    var headers = mock(jakarta.ws.rs.core.HttpHeaders.class);
    when(headers.getRequestHeader(RequestIdentityPolicy.USER_ID_HEADER)).thenReturn(List.of());

    assertNotNull(controller.stream("order", "o-1", headers));
    assertEquals("order:o-1", seen.get().value());
  }
}

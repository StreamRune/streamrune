package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * The SSE controller ends its open streams when the application context stops it, before the web
 * server drains in-flight requests (see {@link SseGracefulShutdownTest} for the real Tomcat run).
 */
class SseControllerShutdownTest {

  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  private static SseController controller(SseEventPublisher publisher) {
    // No keepalive scheduler: nothing but the test drives the controller.
    return new SseController(publisher, ALLOW_ALL, ANONYMOUS, Duration.ofMinutes(5), null);
  }

  @Test
  void theControllerStopsBeforeTheWebServersGracefulShutdownPhase() {
    // Lifecycle beans stop in descending phase order; a lower phase would complete the streams
    // only after the drain had already waited for them.
    assertTrue(SseController.PHASE > WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE);
    SmartLifecycle lifecycle = controller(mock(SseEventPublisher.class));
    assertEquals(SseController.PHASE, lifecycle.getPhase());
    assertTrue(lifecycle.isAutoStartup());
    assertTrue(lifecycle.isRunning(), "a new controller admits subscriptions");
  }

  @Test
  void stoppingEndsEveryOpenStreamAndUnsubscribesIt() throws Exception {
    var publisher = mock(SseEventPublisher.class);
    var controller = controller(publisher);
    var first = controller.stream("cart", "cart-1", List.of());
    var second = controller.stream("cart", "cart-2", List.of());
    var subscribers = ArgumentCaptor.forClass(SseEventPublisher.SseSubscriber.class);
    verify(publisher, times(2)).subscribe(any(), subscribers.capture(), any());

    var stopped = new CountDownLatch(1);
    controller.stop(stopped::countDown);

    assertTrue(stopped.await(5, TimeUnit.SECONDS), "the stop must report done");
    assertFalse(controller.isRunning());
    assertEquals(0, controller.activeCountForTest());
    assertCompleted(first);
    assertCompleted(second);
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-1")),
            subscribers.getAllValues().get(0));
    verify(publisher)
        .unsubscribe(
            StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-2")),
            subscribers.getAllValues().get(1));
  }

  @Test
  void aStopWithNoOpenStreamReportsDoneAtOnce() {
    var controller = controller(mock(SseEventPublisher.class));
    var done = new AtomicInteger();
    controller.stop(done::incrementAndGet);
    assertEquals(1, done.get());
  }

  @Test
  void aSubscriptionAfterTheStopIsAnsweredWithACompletedStream() {
    var publisher = mock(SseEventPublisher.class);
    var controller = controller(publisher);
    controller.stop();

    var emitter = controller.stream("cart", "cart-late", List.of());

    assertCompleted(emitter);
    assertEquals(0, controller.activeCountForTest());
    verify(publisher, never()).subscribe(any(), any(), any());
  }

  @Test
  void aDeniedSubscriptionAfterTheStopIsStillForbidden() {
    var controller =
        new SseController(
            mock(SseEventPublisher.class),
            SseAuthorizer.DENY_ALL,
            ANONYMOUS,
            Duration.ofMinutes(5),
            null);
    controller.stop();
    var denied =
        assertThrows(
            org.springframework.web.server.ResponseStatusException.class,
            () -> controller.stream("order", "order-victim", List.of()));
    assertEquals(403, denied.getStatusCode().value());
  }

  @Test
  void startAdmitsSubscriptionsAgain() {
    var publisher = mock(SseEventPublisher.class);
    var controller = controller(publisher);
    controller.stop();
    controller.start();

    assertTrue(controller.isRunning());
    controller.stream("cart", "cart-again", List.of());
    verify(publisher)
        .subscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-again"))), any(), any());
    assertEquals(1, controller.activeCountForTest());
  }

  @Test
  void aStalledStreamHoldsNeitherTheOtherStreamsNorTheStoppingThread() throws Exception {
    // Completing an emitter waits for a write in progress on it, and a client that stopped reading
    // can hold that write for the container's whole write timeout.
    var controller = controller(mock(SseEventPublisher.class));
    var release = new CountDownLatch(1);
    var stalledEntered = new CountDownLatch(1);
    SseEmitter stalled =
        new SseEmitter() {
          @Override
          public void complete() {
            stalledEntered.countDown();
            try {
              release.await();
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
            super.complete();
          }
        };
    var healthyCompleted = new CountDownLatch(1);
    SseEmitter healthy =
        new SseEmitter() {
          @Override
          public void complete() {
            super.complete();
            healthyCompleted.countDown();
          }
        };
    controller.trackForTest(stalled, () -> {});
    controller.trackForTest(healthy, () -> {});

    var stopped = new CountDownLatch(1);
    long started = System.nanoTime();
    controller.stop(stopped::countDown);
    long stopReturnedAfterMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    try {
      assertTrue(stopReturnedAfterMillis < 1_000, "the stopping thread must not wait on a stream");
      assertTrue(healthyCompleted.await(5, TimeUnit.SECONDS), "the other stream must still end");
      assertTrue(stalledEntered.await(5, TimeUnit.SECONDS));
      assertFalse(
          stopped.await(200, TimeUnit.MILLISECONDS),
          "done is reported only once every stream has ended");
    } finally {
      release.countDown();
    }
    assertTrue(stopped.await(5, TimeUnit.SECONDS));
    assertCompleted(stalled);
  }

  @Test
  void aThrowingUnsubscribeStillCompletesTheStreamAndReportsDone() throws Exception {
    var controller = controller(mock(SseEventPublisher.class));
    var emitter = new SseEmitter();
    controller.trackForTest(
        emitter,
        () -> {
          throw new IllegalStateException("unsubscribe blew up");
        });
    var stopped = new CountDownLatch(1);
    controller.stop(stopped::countDown);
    assertTrue(stopped.await(5, TimeUnit.SECONDS));
    assertCompleted(emitter);
  }

  @Test
  void closeEndsStreamsNoLifecycleStopped() throws Exception {
    var publisher = mock(SseEventPublisher.class);
    var controller = controller(publisher);
    var emitter = controller.stream("cart", "cart-close", List.of());
    controller.close();
    // A destroy method must not block, so close() leaves the completion to a virtual thread.
    verify(publisher, timeout(5_000))
        .unsubscribe(
            eq(StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-close"))), any());
    awaitCompleted(emitter);
    assertFalse(controller.isRunning());
  }

  /**
   * A subscription racing the stop either is ended by the stop or sees it and is answered with a
   * completed stream: no emitter stays open and every publisher subscription is removed.
   */
  @Test
  void aSubscriptionRacingTheStopNeverStaysOpen() throws Exception {
    for (int round = 0; round < 200; round++) {
      var publisher = new CountingPublisher();
      var controller = controller(publisher);
      var barrier = new CyclicBarrier(2);
      var emitters = new ArrayList<SseEmitter>();
      var stopped = new CountDownLatch(1);

      Thread subscriberThread =
          Thread.ofPlatform()
              .start(
                  () -> {
                    await(barrier);
                    for (int i = 0; i < 4; i++) {
                      emitters.add(controller.stream("cart", "cart-" + i, List.of()));
                    }
                  });
      Thread stopperThread =
          Thread.ofPlatform()
              .start(
                  () -> {
                    await(barrier);
                    controller.stop(stopped::countDown);
                  });
      subscriberThread.join(5_000);
      stopperThread.join(5_000);
      // Done is reported only after every stream the stop took over has been ended.
      assertTrue(stopped.await(5, TimeUnit.SECONDS), "round " + round);

      assertEquals(4, emitters.size(), "round " + round);
      for (SseEmitter emitter : emitters) {
        assertCompleted(emitter);
      }
      assertEquals(0, controller.activeCountForTest(), "round " + round);
      assertEquals(
          publisher.subscribed.get(),
          publisher.unsubscribed.get(),
          "round " + round + ": every subscription must be removed");
    }
  }

  private static void await(CyclicBarrier barrier) {
    try {
      barrier.await(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void awaitCompleted(SseEmitter emitter) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      try {
        emitter.send(SseEmitter.event().comment("probe"));
      } catch (IllegalStateException _) {
        return;
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
      Thread.sleep(5);
    }
    assertCompleted(emitter);
  }

  private static void assertCompleted(SseEmitter emitter) {
    assertThrows(
        IllegalStateException.class,
        () -> emitter.send("anything"),
        "the stream must be complete, so the client reconnects");
  }

  /** Counts subscriptions without delivering anything. */
  private static final class CountingPublisher extends SseEventPublisher {
    final AtomicInteger subscribed = new AtomicInteger();
    final AtomicInteger unsubscribed = new AtomicInteger();

    @Override
    public void subscribe(
        StreamId streamId, SseSubscriber subscriber, Consumer<Throwable> onDisconnect) {
      subscribed.incrementAndGet();
    }

    @Override
    public void unsubscribe(StreamId streamId, SseSubscriber subscriber) {
      unsubscribed.incrementAndGet();
    }
  }
}

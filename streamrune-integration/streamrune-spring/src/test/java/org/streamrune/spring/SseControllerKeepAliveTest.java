package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.streamrune.integration.RequestIdentityPolicy;

/**
 * The keepalive reaper ({@link SseController#sendKeepAlives()}) runs as a scheduleAtFixedRate task,
 * so any throwable it lets escape silently cancels ALL future keepalive ticks for the process. The
 * tick must therefore catch Throwable (not just Exception) and guard the unsubscribe hook, so one
 * bad subscriber can never kill the keepalive for every other subscriber.
 */
class SseControllerKeepAliveTest {

  /**
   * keepAliveInterval = null → no background scheduler; the test drives sendKeepAlives() directly.
   */
  private SseController controllerWithoutScheduler() {
    return new SseController(
        null, null, RequestIdentityPolicy.of(null, false), Duration.ofMinutes(5), null);
  }

  @Test
  void sendKeepAlives_errorFromSendAndThrowingUnsubscribe_reaperSurvivesAndContinues() {
    var controller = controllerWithoutScheduler();

    // A subscriber whose keepalive send throws an ERROR (not an Exception, so the old
    // catch(Exception) would let it escape) AND whose unsubscribe hook ALSO throws (unguarded in
    // the old code) — both escape routes.
    var badUnsubscribeRan = new AtomicBoolean(false);
    SseEmitter badEmitter =
        new SseEmitter() {
          @Override
          public void send(SseEmitter.SseEventBuilder builder) {
            throw new StackOverflowError("simulated fatal send");
          }
        };
    controller.trackForTest(
        badEmitter,
        () -> {
          badUnsubscribeRan.set(true);
          throw new RuntimeException("unsubscribe blew up");
        });

    // A healthy subscriber that must still receive its keepalive (the loop continued past the bad
    // one). Overriding send() to just count avoids any real servlet I/O.
    var healthySends = new AtomicInteger(0);
    SseEmitter healthyEmitter =
        new SseEmitter() {
          @Override
          public void send(SseEmitter.SseEventBuilder builder) {
            healthySends.incrementAndGet();
          }
        };
    controller.trackForTest(healthyEmitter, () -> {});

    // The reaper tick must not propagate the Error or the throwing unsubscribe.
    assertDoesNotThrow(controller::sendKeepAlives);

    assertTrue(badUnsubscribeRan.get(), "the bad subscriber's unsubscribe hook must be attempted");
    assertEquals(1, healthySends.get(), "the healthy subscriber must still receive its keepalive");
    assertEquals(1, controller.activeCountForTest(), "the bad subscriber must have been reaped");
  }

  @Test
  void sendKeepAlives_subscriberMidSend_isSkippedNotBlockedOn() throws Exception {
    // One subscriber's delivery worker is parked inside a blocking servlet emitter.send()
    // (TCP zero-window client; Tomcat's write timeout can be minutes) while holding that
    // registration's send lock. The SINGLE shared keepalive tick previously blocked on that lock —
    // the tick never completed, scheduleAtFixedRate never ran it again, and NO other subscriber
    // received keepalives: the dead-client eviction was suspended process-wide by one slow
    // client. A registration whose send lock is held is mid-send — the in-flight write IS its
    // liveness probe — so the tick must skip it non-blockingly and serve everyone else.
    var controller = controllerWithoutScheduler();

    var stalledSends = new AtomicInteger(0);
    SseEmitter stalled =
        new SseEmitter() {
          @Override
          public void send(SseEmitter.SseEventBuilder builder) {
            stalledSends.incrementAndGet();
          }
        };
    ReentrantLock stalledSendLock = controller.trackForTest(stalled, () -> {});

    var healthySends = new AtomicInteger(0);
    SseEmitter healthy =
        new SseEmitter() {
          @Override
          public void send(SseEmitter.SseEventBuilder builder) {
            healthySends.incrementAndGet();
          }
        };
    controller.trackForTest(healthy, () -> {});

    // Simulate the mid-send delivery worker: a thread that owns the stalled registration's send
    // lock and stays parked inside "send()" until released.
    var workerHoldsLock = new CountDownLatch(1);
    var releaseWorker = new CountDownLatch(1);
    Thread deliveryWorker =
        new Thread(
            () -> {
              stalledSendLock.lock();
              try {
                workerHoldsLock.countDown();
                releaseWorker.await();
              } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
              } finally {
                stalledSendLock.unlock();
              }
            },
            "stalled-delivery-worker");
    deliveryWorker.start();
    assertTrue(workerHoldsLock.await(5, TimeUnit.SECONDS));

    // Drive one keepalive tick on its own thread — it must COMPLETE while the lock is held.
    var tickCompleted = new CountDownLatch(1);
    Thread tick =
        new Thread(
            () -> {
              controller.sendKeepAlives();
              tickCompleted.countDown();
            },
            "keepalive-tick");
    tick.start();
    try {
      assertTrue(
          tickCompleted.await(3, TimeUnit.SECONDS),
          "the keepalive tick must not block on a mid-send subscriber's lock");
      assertEquals(
          1, healthySends.get(), "every other subscriber must still receive its keepalive");
      assertEquals(0, stalledSends.get(), "a mid-send emitter needs no liveness probe this tick");
      assertEquals(
          2, controller.activeCountForTest(), "a mid-send subscriber must NOT be reaped as dead");
    } finally {
      releaseWorker.countDown();
      deliveryWorker.join(5000);
      tick.join(5000);
    }

    // The skip is per-tick only: once the in-flight send finishes, the next tick probes it again.
    controller.sendKeepAlives();
    assertEquals(
        1, stalledSends.get(), "the previously mid-send subscriber is probed on the next tick");
    assertEquals(2, healthySends.get());
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The disconnect hook is the transport's only eviction signal, so it must be usable.
 *
 * <p>On the send-failure path {@code deliverLoop} calls {@code evict(this, e)} → {@code fail} →
 * {@code stop()}, and {@code stop()} interrupted {@code worker} — which on that path IS the current
 * thread. The hook then ran with its own interrupt flag set, so any interruptible work it does (a
 * lock acquisition, a socket write, a queue offer) fails immediately with the transport teardown
 * half done.
 */
class SseEventPublisherEvictionTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  private static final StreamId STREAM = StreamId.of(TYPE, AggregateId.of("cart-1"));

  record Added(String sku) implements DomainEvent {}

  private static EventEnvelope envelope(long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        STREAM,
        new Version(offset),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt-" + offset),
            CommandId.of("cmd-" + offset),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  @Test
  void sendFailureEviction_runsTheDisconnectHookWithAClearInterruptFlag() throws Exception {
    var publisher = new SseEventPublisher(4);
    var hookRan = new CountDownLatch(1);
    var interruptedInHook = new AtomicBoolean(true);
    var seenCause = new AtomicReference<Throwable>();

    publisher.subscribe(
        STREAM,
        envelope -> {
          throw new IllegalStateException("emitter already completed");
        },
        cause -> {
          interruptedInHook.set(Thread.currentThread().isInterrupted());
          seenCause.set(cause);
          hookRan.countDown();
        });

    publisher.publish(STREAM, envelope(1));

    assertTrue(
        hookRan.await(5, TimeUnit.SECONDS), "the disconnect hook must fire on a send failure");
    assertInstanceOf(IllegalStateException.class, seenCause.get());
    assertFalse(
        interruptedInHook.get(),
        "the eviction must not interrupt the very thread that is about to run the disconnect hook"
            + " — the transport teardown the hook performs is interruptible work");
    publisher.close();
  }

  @Test
  void sendThrowingAnError_evictsTheSubscriberInsteadOfLeavingItRegisteredWithADeadWorker()
      throws Exception {
    // The worker caught only Exception around send. An Error
    // from send — Spring's emitter.send rethrowing a NoClassDefFoundError or
    // ExceptionInInitializerError out of Jackson, or an OOM — killed the worker WITHOUT evict():
    // the subscription stayed registered and running, the disconnect hook never fired, and the
    // transport's keepalives kept the client's connection apparently healthy, so on a quiet stream
    // with no timeout the client never reconnected and never received another event (the
    // silent gap). An Error must evict exactly like an Exception does.
    int capacity = 2;
    var publisher = new SseEventPublisher(capacity);
    var thrown = new AssertionError("emitter.send failed with an Error");
    var sends = new AtomicInteger();
    var hookCalls = new AtomicInteger();
    var seenCause = new AtomicReference<Throwable>();
    var hookRan = new CountDownLatch(1);

    publisher.subscribe(
        STREAM,
        envelope -> {
          sends.incrementAndGet();
          throw thrown;
        },
        cause -> {
          hookCalls.incrementAndGet();
          seenCause.set(cause);
          hookRan.countDown();
        });

    publisher.publish(STREAM, envelope(1));

    assertTrue(
        hookRan.await(5, TimeUnit.SECONDS),
        "an Error thrown by send must still evict the subscriber and fire its disconnect hook");
    assertSame(thrown, seenCause.get());
    // Unregistered, not merely hooked: evict() removes the subscription from the stream before it
    // fires the hook, so by now the stream has no subscription left.
    assertEquals(0, publisher.subscriberCount(STREAM), "the evicted subscription must be removed");

    // Publishing well past the queue capacity reaches neither send nor the hook again. (This alone
    // does not prove the removal: the failed subscription is stopped, and a stopped one accepts
    // and drops every envelope.)
    for (int i = 0; i < capacity + 2; i++) {
      publisher.publish(STREAM, envelope(2 + i));
    }
    assertEquals(1, sends.get(), "the evicted subscriber must receive nothing further");
    assertEquals(1, hookCalls.get(), "the disconnect hook fires exactly once");
    publisher.close();
  }

  @Test
  void slowConsumerEviction_firesTheHookOnceWithoutBlockingPublish() throws Exception {
    int capacity = 4;
    var publisher = new SseEventPublisher(capacity);
    var workerParked = new CountDownLatch(1);
    var releaseWorker = new CountDownLatch(1);
    var hookCalls = new AtomicInteger();
    var seenCause = new AtomicReference<Throwable>();
    var hookRan = new CountDownLatch(1);

    publisher.subscribe(
        STREAM,
        envelope -> {
          workerParked.countDown();
          try {
            releaseWorker.await();
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        },
        cause -> {
          hookCalls.incrementAndGet();
          seenCause.set(cause);
          hookRan.countDown();
        });

    // First envelope parks the worker inside send(); the rest fill the bounded queue.
    publisher.publish(STREAM, envelope(1));
    assertTrue(workerParked.await(5, TimeUnit.SECONDS), "the worker must be parked inside send()");
    for (int i = 0; i < capacity; i++) {
      publisher.publish(STREAM, envelope(2 + i));
    }
    // Overflow: the queue is full and the worker cannot drain.
    publisher.publish(STREAM, envelope(100));

    assertTrue(hookRan.await(5, TimeUnit.SECONDS), "the slow consumer must be evicted");
    assertInstanceOf(SseEventPublisher.SlowConsumerException.class, seenCause.get());
    assertEquals(STREAM, ((SseEventPublisher.SlowConsumerException) seenCause.get()).streamId());

    // Further publishes on the evicted stream must be no-ops, not repeated evictions.
    publisher.publish(STREAM, envelope(101));
    assertEquals(1, hookCalls.get(), "the disconnect hook fires exactly once per subscriber");

    releaseWorker.countDown();
    publisher.close();
  }

  @Test
  void disconnectHookMayUnsubscribeReentrantly_theEvictionOrderIsLoadBearing() throws Exception {
    // Residual: every shipped SSE controller's disconnect hook runs the same
    // cleanup the transport's completion callback runs, and that cleanup calls
    // publisher.unsubscribe(streamId, subscriber) — subscribers.compute(...) on the key evict() is
    // already computing (Spring's hook does exactly this, at SseController#stream's
    // `cause -> { cleanup.run(); ... }`). Re-entrancy into the publisher from the hook is therefore
    // part of the contract, and nothing pinned it.
    //
    // Measured, not assumed: moving subscription.fail(cause) INSIDE the computeIfPresent lambda
    // does NOT break this today — Java monitors are reentrant, so the recursive compute on the same
    // bin succeeds. The current ordering is still the right shape (ConcurrentHashMap's contract
    // says a mapping function must not update the map, and the alternative runs an arbitrary
    // transport callback while holding the bin lock, blocking every other subscribe/unsubscribe on
    // that stream), but this test pins the CONTRACT — a hook may unsubscribe re-entrantly — rather
    // than a deadlock that does not currently occur.
    //
    // Capacity 1 with the worker parked inside send() makes the third publish overflow, which is
    // the slow-consumer eviction path.
    var publisher = new SseEventPublisher(1);
    var releaseSend = new CountDownLatch(1);
    var insideSend = new CountDownLatch(1);
    var hookCompleted = new CountDownLatch(1);
    var hookFailure = new AtomicReference<Throwable>();
    var subscriberRef = new AtomicReference<SseEventPublisher.SseSubscriber>();

    SseEventPublisher.SseSubscriber subscriber =
        envelope -> {
          insideSend.countDown();
          try {
            releaseSend.await(10, TimeUnit.SECONDS);
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        };
    subscriberRef.set(subscriber);

    publisher.subscribe(
        STREAM,
        subscriber,
        cause -> {
          try {
            // Re-entrant into the same map key, exactly like the shipped controllers' cleanup.
            publisher.unsubscribe(STREAM, subscriberRef.get());
          } catch (Throwable t) {
            hookFailure.set(t);
          } finally {
            hookCompleted.countDown();
          }
        });

    publisher.publish(STREAM, envelope(1));
    assertTrue(insideSend.await(5, TimeUnit.SECONDS), "the worker must be parked inside send()");
    publisher.publish(STREAM, envelope(2));
    publisher.publish(STREAM, envelope(3));

    assertTrue(hookCompleted.await(5, TimeUnit.SECONDS), "the disconnect hook must have run");
    releaseSend.countDown();
    assertNull(
        hookFailure.get(),
        "a disconnect hook that unsubscribes must not hit a recursive ConcurrentHashMap update —"
            + " evict() must fire the hook OUTSIDE its computeIfPresent lambda");
    publisher.close();
  }
}

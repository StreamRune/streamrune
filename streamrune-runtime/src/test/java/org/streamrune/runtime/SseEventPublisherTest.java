package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class SseEventPublisherTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  private EventEnvelope createEnvelope(StreamId streamId) {
    return createEnvelope(streamId, GlobalOffset.initial());
  }

  private EventEnvelope createEnvelope(StreamId streamId, GlobalOffset offset) {
    return new EventEnvelope(
        offset,
        streamId,
        Version.initial(),
        new EventType("TestEvent"),
        new SimpleTestEvent("test"),
        new EventMetadata(
            EventId.of("test-event-id"),
            org.streamrune.core.types.CommandId.of("test-command-id"),
            null,
            null,
            CorrelationId.of("test-correlation-id"),
            null,
            null,
            Instant.now()));
  }

  // Simple event record for testing
  private static record SimpleTestEvent(String data) implements DomainEvent {}

  @Test
  void publisherCanBeCreated() {
    try (var publisher = new SseEventPublisher()) {
      assertNotNull(publisher);
    }
  }

  @Test
  void rejectsNonPositiveQueueCapacity() {
    assertThrows(IllegalArgumentException.class, () -> new SseEventPublisher(0));
    assertThrows(IllegalArgumentException.class, () -> new SseEventPublisher(-1));
  }

  @Test
  void subscribeAndPublishDeliversEvent() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var received = new CopyOnWriteArrayList<EventEnvelope>();

      publisher.subscribe(streamId, received::add);

      var envelope = createEnvelope(streamId);
      publisher.publish(envelope);

      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 1);
      assertSame(envelope, received.get(0));
    }
  }

  @Test
  void publishToNonexistentStreamDoesNotThrow() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("nonexistent"));
      assertDoesNotThrow(() -> publisher.publish(createEnvelope(streamId)));
    }
  }

  @Test
  void unsubscribeRemovesSubscriber() throws InterruptedException {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var received = new CopyOnWriteArrayList<EventEnvelope>();

      SseEventPublisher.SseSubscriber subscriber = received::add;
      publisher.subscribe(streamId, subscriber);
      publisher.unsubscribe(streamId, subscriber);

      publisher.publish(createEnvelope(streamId));

      // Give any (incorrect) async delivery a chance to land, then assert nothing arrived.
      Thread.sleep(200);
      assertTrue(received.isEmpty());
    }
  }

  // ---- hasSubscribers: what the feed asks before it reads the global stream --------------------

  @Test
  void hasSubscribersFollowsEverySubscribeAndUnsubscribe() {
    try (var publisher = new SseEventPublisher()) {
      var one = StreamId.of(TYPE, AggregateId.of("one"));
      var two = StreamId.of(TYPE, AggregateId.of("two"));
      SseEventPublisher.SseSubscriber first = envelope -> {};
      SseEventPublisher.SseSubscriber second = envelope -> {};
      assertFalse(publisher.hasSubscribers(), "a new publisher has none");

      publisher.subscribe(one, first);
      publisher.subscribe(two, second);
      assertTrue(publisher.hasSubscribers());

      publisher.unsubscribe(one, first);
      assertTrue(publisher.hasSubscribers(), "the subscriber of the other stream is left");
      publisher.unsubscribe(one, first);
      assertTrue(publisher.hasSubscribers(), "a second unsubscribe of the same one removes nobody");
      publisher.unsubscribe(two, first);
      assertTrue(publisher.hasSubscribers(), "an unsubscribe that matches nobody removes nobody");

      publisher.unsubscribe(two, second);
      assertFalse(publisher.hasSubscribers());
    }
  }

  @Test
  void hasSubscribersCountsASubscriberRegisteredTwiceTwice() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("twice"));
      SseEventPublisher.SseSubscriber subscriber = envelope -> {};
      publisher.subscribe(streamId, subscriber);
      publisher.subscribe(streamId, subscriber);

      // One unsubscribe removes every registration of that subscriber on the stream.
      publisher.unsubscribe(streamId, subscriber);

      assertFalse(publisher.hasSubscribers());
      assertEquals(0, publisher.subscriberCount(streamId));
    }
  }

  @Test
  void hasSubscribersIsFalseOnceTheOnlySubscriberWasEvicted() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("evicted"));
      var evicted = new CountDownLatch(1);
      publisher.subscribe(
          streamId,
          envelope -> {
            throw new IllegalStateException("the client is gone");
          },
          cause -> evicted.countDown());
      assertTrue(publisher.hasSubscribers());

      publisher.publish(createEnvelope(streamId));

      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> evicted.getCount() == 0);
      assertFalse(publisher.hasSubscribers());
    }
  }

  @Test
  void hasSubscribersIsFalseAfterClose() {
    var publisher = new SseEventPublisher();
    publisher.subscribe(StreamId.of(TYPE, AggregateId.of("one")), envelope -> {});
    publisher.subscribe(StreamId.of(TYPE, AggregateId.of("two")), envelope -> {});
    publisher.subscribe(StreamId.of(TYPE, AggregateId.of("two")), envelope -> {});

    publisher.close();

    assertFalse(publisher.hasSubscribers());
    publisher.subscribe(StreamId.of(TYPE, AggregateId.of("three")), envelope -> {});
    assertTrue(publisher.hasSubscribers(), "a closed publisher still counts a later subscriber");
    publisher.close();
    assertFalse(publisher.hasSubscribers());
  }

  /**
   * One client stays subscribed while others come and go on other streams: the answer never reads
   * {@code false}, which the size of a concurrent map can.
   */
  @Test
  void hasSubscribersNeverMissesAClientThatStaysWhileOthersComeAndGo() throws Exception {
    try (var publisher = new SseEventPublisher()) {
      publisher.subscribe(StreamId.of(TYPE, AggregateId.of("stays")), envelope -> {});
      var stop = new java.util.concurrent.atomic.AtomicBoolean();
      var churners = new java.util.ArrayList<Thread>();
      for (int t = 0; t < 4; t++) {
        int id = t;
        churners.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      var streamId = StreamId.of(TYPE, AggregateId.of("churn-" + id));
                      SseEventPublisher.SseSubscriber subscriber = envelope -> {};
                      while (!stop.get()) {
                        publisher.subscribe(streamId, subscriber);
                        publisher.unsubscribe(streamId, subscriber);
                      }
                    }));
      }
      try {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (System.nanoTime() < deadline) {
          assertTrue(publisher.hasSubscribers(), "one client is subscribed throughout");
        }
      } finally {
        stop.set(true);
        for (Thread churner : churners) {
          churner.join(5_000);
        }
      }
      assertTrue(publisher.hasSubscribers());
    }
  }

  @Test
  void unsubscribeCleansUpEmptyStream() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));

      SseEventPublisher.SseSubscriber subscriber = envelope -> {};
      publisher.subscribe(streamId, subscriber);
      publisher.unsubscribe(streamId, subscriber);

      // Publishing to cleaned up stream should not throw
      assertDoesNotThrow(() -> publisher.publish(createEnvelope(streamId)));
    }
  }

  @Test
  void multipleSubscribersReceiveEvent() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var received1 = new CopyOnWriteArrayList<EventEnvelope>();
      var received2 = new CopyOnWriteArrayList<EventEnvelope>();

      publisher.subscribe(streamId, received1::add);
      publisher.subscribe(streamId, received2::add);

      var envelope = createEnvelope(streamId);
      publisher.publish(envelope);

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> received1.size() == 1 && received2.size() == 1);
    }
  }

  @Test
  void subscriberErrorDoesNotAffectOthers() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var received = new CopyOnWriteArrayList<EventEnvelope>();

      publisher.subscribe(
          streamId,
          envelope -> {
            throw new RuntimeException("Subscriber error");
          });
      publisher.subscribe(streamId, received::add);

      var envelope = createEnvelope(streamId);
      publisher.publish(envelope);

      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 1);
    }
  }

  @Test
  void subscriberErrorEvictsAndFiresDisconnectHook() {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var cause = new AtomicReference<Throwable>();
      var calls = new AtomicInteger(0);

      var thrown = new RuntimeException("boom");
      publisher.subscribe(
          streamId,
          envelope -> {
            calls.incrementAndGet();
            throw thrown;
          },
          cause::set);

      publisher.publish(createEnvelope(streamId));

      // Hook fired with the thrown exception, subscriber evicted.
      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> cause.get() != null);
      assertSame(thrown, cause.get());

      // A second publish must not reach the evicted subscriber.
      publisher.publish(createEnvelope(streamId, GlobalOffset.of(1)));
      Awaitility.await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .until(() -> calls.get() == 1);
    }
  }

  @Test
  void slowSubscriberDoesNotStallFastSubscriber() throws InterruptedException {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var release = new CountDownLatch(1);
      var slowEntered = new CountDownLatch(1);
      var fastReceived = new CopyOnWriteArrayList<EventEnvelope>();

      // Slow subscriber: blocks on its first send until released.
      publisher.subscribe(
          streamId,
          envelope -> {
            slowEntered.countDown();
            try {
              release.await();
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          });
      publisher.subscribe(streamId, fastReceived::add);

      publisher.publish(createEnvelope(streamId));

      // The fast subscriber gets its event even though the slow one is still blocked.
      assertTrue(slowEntered.await(5, TimeUnit.SECONDS), "slow subscriber should have started");
      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fastReceived.size() == 1);

      release.countDown();
    }
  }

  @Test
  void publishNeverBlocksOnStuckConsumer() throws InterruptedException {
    // Capacity 1: the worker takes one event (and blocks forever in send), the queue then holds
    // one,
    // and every further publish offers to a full queue -> evicts without blocking the publisher.
    try (var publisher = new SseEventPublisher(1)) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var forever = new CountDownLatch(1);
      var evicted = new AtomicReference<Throwable>();

      publisher.subscribe(
          streamId,
          envelope -> {
            try {
              forever.await();
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          },
          evicted::set);

      // Fire many publishes from this thread; none may block.
      var publisherThread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    for (long i = 0; i < 1_000; i++) {
                      publisher.publish(createEnvelope(streamId, GlobalOffset.of(i)));
                    }
                  });

      assertTrue(
          publisherThread.join(Duration.ofSeconds(5)),
          "publish() must not block on a stuck consumer");

      // The stuck subscriber was evicted per the slow-consumer policy.
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> evicted.get() instanceof SseEventPublisher.SlowConsumerException);
      var slow = (SseEventPublisher.SlowConsumerException) evicted.get();
      assertEquals(streamId, slow.streamId());
      assertEquals(1, slow.queueCapacity());

      forever.countDown();
    }
  }

  @Test
  void evictedSlowSubscriberIsReapedAndReceivesNoMoreEvents() throws InterruptedException {
    try (var publisher = new SseEventPublisher(1)) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var gate = new CountDownLatch(1);
      var delivered = new AtomicInteger(0);
      var evicted = new CountDownLatch(1);

      publisher.subscribe(
          streamId,
          envelope -> {
            delivered.incrementAndGet();
            try {
              gate.await();
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          },
          cause -> evicted.countDown());

      // Overflow the capacity-1 queue to trigger eviction.
      for (long i = 0; i < 50; i++) {
        publisher.publish(createEnvelope(streamId, GlobalOffset.of(i)));
      }
      assertTrue(evicted.await(5, TimeUnit.SECONDS), "slow subscriber should be evicted");

      // After eviction the subscriber is reaped: further publishes are no-ops for it.
      gate.countDown();
      int afterEviction = delivered.get();
      for (long i = 50; i < 100; i++) {
        publisher.publish(createEnvelope(streamId, GlobalOffset.of(i)));
      }
      Awaitility.await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(2))
          .until(() -> delivered.get() <= afterEviction + 1);
    }
  }

  @Test
  void unsubscribeDoesNotFireDisconnectHook() throws InterruptedException {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var hookCalls = new AtomicInteger(0);

      SseEventPublisher.SseSubscriber subscriber = envelope -> {};
      publisher.subscribe(streamId, subscriber, cause -> hookCalls.incrementAndGet());
      publisher.unsubscribe(streamId, subscriber);

      Thread.sleep(200);
      assertEquals(0, hookCalls.get());
    }
  }

  @Test
  void concurrentSubscribeUnsubscribeDoesNotThrow() throws InterruptedException {
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
      var errors = new AtomicReference<Exception>(null);

      Thread subscribeThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 100; i++) {
                    publisher.subscribe(streamId, e -> {});
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.set(e);
                }
              });

      Thread unsubscribeThread =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < 100; i++) {
                    publisher.unsubscribe(streamId, e -> {});
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.set(e);
                }
              });

      subscribeThread.start();
      unsubscribeThread.start();
      subscribeThread.join();
      unsubscribeThread.join();

      assertNull(errors.get());
    }
  }

  @Test
  void subscribeRacingWithUnsubscribeNeverLosesTheNewSubscriber() throws InterruptedException {
    // Regression: subscribe() used computeIfAbsent(...).add(...), where the add ran outside the
    // map's per-key lock. A concurrent unsubscribe() of the last remaining subscriber could
    // remove the mapping in between, leaving the new subscriber on an orphaned list — registered
    // but never receiving events. subscribe() now adds inside compute(), serialized per key.
    try (var publisher = new SseEventPublisher()) {
      var streamId = StreamId.of(TYPE, AggregateId.of("race-stream"));

      for (int i = 0; i < 200; i++) {
        SseEventPublisher.SseSubscriber leaving = envelope -> {};
        publisher.subscribe(streamId, leaving);

        var received = new AtomicInteger();
        SseEventPublisher.SseSubscriber arriving = envelope -> received.incrementAndGet();

        var start = new CountDownLatch(1);
        Thread unsubscriber =
            Thread.ofVirtual()
                .start(
                    () -> {
                      awaitUninterruptibly(start);
                      publisher.unsubscribe(streamId, leaving);
                    });
        Thread subscriber =
            Thread.ofVirtual()
                .start(
                    () -> {
                      awaitUninterruptibly(start);
                      publisher.subscribe(streamId, arriving);
                    });
        start.countDown();
        unsubscriber.join();
        subscriber.join();

        publisher.publish(createEnvelope(streamId));
        Awaitility.await(
                "subscriber added concurrently with an unsubscribe must receive events (iteration "
                    + i
                    + ")")
            .atMost(Duration.ofSeconds(5))
            .until(() -> received.get() == 1);
        publisher.unsubscribe(streamId, arriving);
      }
    }
  }

  @Test
  void anEventIsRoutedByItsOwnStreamAndNeverToTheSubscribersOfAnother() {
    try (var publisher = new SseEventPublisher()) {
      var order = StreamId.of(AggregateType.of("order"), AggregateId.of("p-1"));
      var inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"));
      var orderFrames = new CopyOnWriteArrayList<EventEnvelope>();
      var inventoryFrames = new CopyOnWriteArrayList<EventEnvelope>();
      publisher.subscribe(order, orderFrames::add);
      publisher.subscribe(inventory, inventoryFrames::add);

      publisher.publish(createEnvelope(inventory, GlobalOffset.of(1)));
      publisher.publish(createEnvelope(order, GlobalOffset.of(2)));

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> orderFrames.size() == 1 && inventoryFrames.size() == 1);
      assertEquals(order, orderFrames.getFirst().streamId());
      assertEquals(inventory, inventoryFrames.getFirst().streamId());
    }
  }

  @Test
  void closeStopsAllWorkers() {
    var publisher = new SseEventPublisher();
    var streamId = StreamId.of(TYPE, AggregateId.of("test-stream"));
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    publisher.subscribe(streamId, received::add);

    publisher.close();

    // After close, the stream has no subscribers and publish is a no-op.
    assertDoesNotThrow(() -> publisher.publish(createEnvelope(streamId)));
  }

  private void awaitUninterruptibly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}

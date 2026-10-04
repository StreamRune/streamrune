package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;

/** Unit tests for PollingEventSubscription validation and lifecycle. */
class PollingEventSubscriptionTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  private final SubscriptionConfig config =
      new SubscriptionConfig(false, Duration.ofMillis(100), Duration.ofMillis(10));

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(5));
  }

  record Added(String sku) implements DomainEvent {}

  static class InMemoryOffsetStore implements OffsetStore {
    final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      offsets.put(projectionName.value(), offset);
    }
  }

  private static EventEnvelope envelope(StreamId s, long v) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        s,
        new Version(v),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt_" + v),
            CommandId.of("cmd_" + v),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  // ==================== Validation tests ====================

  @Test
  void shouldThrowOnMissingSubscriptionName() {
    var builder =
        PollingEventSubscription.builder()
            .eventStore(null)
            .offsetStore(null)
            .listener(null)
            .config(config);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void shouldThrowOnMissingEventStore() {
    var builder =
        PollingEventSubscription.builder()
            .subscriptionName("test")
            .offsetStore(null)
            .listener(null)
            .config(config);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void shouldThrowOnMissingOffsetStore() {
    var builder =
        PollingEventSubscription.builder()
            .subscriptionName("test")
            .eventStore(null)
            .listener(null)
            .config(config);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void shouldThrowOnMissingListener() {
    var builder =
        PollingEventSubscription.builder()
            .subscriptionName("test")
            .eventStore(null)
            .offsetStore(null)
            .config(config);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void shouldThrowOnMissingConfig() {
    var builder =
        PollingEventSubscription.builder()
            .subscriptionName("test")
            .eventStore(null)
            .offsetStore(null)
            .listener(null)
            .config(null);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void shouldThrowOnInvalidFetchSize() {
    var builder =
        PollingEventSubscription.builder()
            .subscriptionName("test")
            .eventStore(null)
            .offsetStore(null)
            .listener(null)
            .config(config)
            .fetchSize(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  // ==================== Behavior tests ====================

  @Test
  void pollOncePersistsEventsAndAdvancesOffset() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1), envelope(s, 2)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    EventListener listener = received::addAll;
    var offsetStore = new InMemoryOffsetStore();

    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("test-sub")
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(listener)
            .config(fastConfig())
            .fetchSize(10)
            .build();

    int delivered = sub.pollOnce();
    assertEquals(2, delivered);
    assertEquals(2, received.size());
    assertTrue(offsetStore.getLastOffset(ProjectionName.of("test-sub")).value() >= 2);
  }

  @Test
  void pollOnceReturnsZeroWhenNoEvents() {
    var store = new InMemoryEventStore();
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("empty"),
            store,
            new InMemoryOffsetStore(),
            events -> {},
            new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)),
            10);
    assertEquals(0, sub.pollOnce());
  }

  @Test
  void triggerImmediatePollDelegatesToPollOnce() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-imm"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("imm"),
            store,
            new InMemoryOffsetStore(),
            received::addAll,
            fastConfig(),
            10);

    int delivered = sub.triggerImmediatePoll();
    assertEquals(1, delivered);
    assertEquals(1, received.size());
  }

  @Test
  void startAndCloseToggleRunningFlag() throws InterruptedException {
    var store = new InMemoryEventStore();
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("start-close"),
            store,
            new InMemoryOffsetStore(),
            events -> {},
            fastConfig(),
            10);

    assertFalse(sub.isRunning());
    sub.start();
    assertTrue(sub.isRunning());
    sub.close();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (sub.isRunning() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(sub.isRunning());
  }

  @Test
  void startTwiceThrows() {
    var store = new InMemoryEventStore();
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("twice"),
            store,
            new InMemoryOffsetStore(),
            events -> {},
            fastConfig(),
            10);
    try (sub) {
      sub.start();
      assertThrows(IllegalStateException.class, sub::start);
    }
  }

  @Test
  void closeWaitsForInFlightBatch() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("close-wait"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var inListener = new CountDownLatch(1);
    var release = new AtomicBoolean(false);
    var batchCompleted = new AtomicBoolean(false);
    EventListener listener =
        events -> {
          inListener.countDown();
          // Simulates non-interruptible in-flight work (pollOnce has no interruptible points).
          while (!release.get()) {
            Thread.onSpinWait();
          }
          batchCompleted.set(true);
        };

    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("close-wait"),
            store,
            new InMemoryOffsetStore(),
            listener,
            fastConfig(),
            10);
    sub.start();
    assertTrue(inListener.await(3, TimeUnit.SECONDS), "listener should receive the batch");

    var closeReturned = new CountDownLatch(1);
    Thread closer =
        Thread.ofVirtual()
            .start(
                () -> {
                  sub.close();
                  closeReturned.countDown();
                });

    assertFalse(
        closeReturned.await(200, TimeUnit.MILLISECONDS),
        "close() must not return while a batch is in flight");

    release.set(true);
    assertTrue(
        closeReturned.await(3, TimeUnit.SECONDS),
        "close() should return once the in-flight batch finishes");
    assertTrue(batchCompleted.get(), "close() returned only after the batch completed");
    closer.join(TimeUnit.SECONDS.toMillis(1));
  }

  @Test
  void listenerErrorIsSurfacedFromPollOnce() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var thrown = new AtomicReference<Throwable>();
    EventListener listener =
        events -> {
          throw new RuntimeException("listener-boom");
        };

    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("err-sub"),
            store,
            new InMemoryOffsetStore(),
            listener,
            fastConfig(),
            10);

    try {
      sub.pollOnce();
    } catch (RuntimeException e) {
      thrown.set(e);
    }
    assertNotNull(thrown.get());
    assertEquals("listener-boom", thrown.get().getMessage());
  }

  @Test
  void backgroundPollLoopDeliversEvents() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-bg"));
    store.append(s, List.of(envelope(s, 1), envelope(s, 2)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("bg"),
            store,
            new InMemoryOffsetStore(),
            received::addAll,
            new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)),
            10);

    sub.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (received.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    sub.close();
    assertTrue(received.size() >= 2);
  }

  @Test
  void backgroundPollLoopSurvivesListenerExceptionAndRecovers() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-err"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var failuresLeft = new java.util.concurrent.atomic.AtomicInteger(2);
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("bg-err"),
            store,
            new InMemoryOffsetStore(),
            events -> {
              if (failuresLeft.getAndDecrement() > 0) {
                throw new RuntimeException("transient listener failure");
              }
              received.addAll(events);
            },
            new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)),
            10);

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      // Wait for BOTH observable effects of the recovery, not just the first one. The listener
      // appends to `received` from inside the poll cycle, while the loop clears its failure
      // counter only after that cycle returns — sampling the counter as soon as `received` fills
      // races that window and intermittently observes the pre-reset value.
      while ((received.isEmpty() || sub.consecutiveFailures() != 0)
          && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      // The loop must retry through the failures and stay RUNNING the whole time.
      assertTrue(sub.isRunning(), "transient failures must not stop the subscription");
      assertEquals(1, received.size(), "events must be delivered after the failures pass");
      assertEquals(0, sub.consecutiveFailures(), "failure counter resets after recovery");
    } finally {
      sub.close();
    }
  }

  @Test
  void pollThreadDyingOnUncaughtErrorFlipsIsRunningFalse() throws InterruptedException {
    // ResilientPollLoop retries only RuntimeException; a java.lang.Error (AssertionError,
    // NoClassDefFoundError/LinkageError, StackOverflowError) escapes it and kills the virtual poll
    // thread. Without a finally on runPollLoop() the lifecycle stays RUNNING, so isRunning() keeps
    // returning true even though no events flow — the owning ContinuousProjectionRunner never
    // detects
    // the dead thread and /health stays UP while the read model silently freezes. This exercises
    // the
    // REAL poll loop (not a hand-flipped fake): a live listener throws an AssertionError.
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-error"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("bg-error"),
            store,
            new InMemoryOffsetStore(),
            events -> {
              throw new AssertionError("boom — an unrecoverable Error, not a RuntimeException");
            },
            new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)),
            10);

    sub.start();
    try {
      assertTrue(sub.isRunning(), "subscription must be RUNNING right after start()");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (sub.isRunning() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(
          sub.isRunning(),
          "an uncaught Error that kills the poll thread must flip isRunning() to false so the"
              + " owning runner surfaces a terminal ERROR — not leave it stuck RUNNING");
    } finally {
      sub.close();
    }
  }

  @Test
  void consecutiveFailuresReportedWhileListenerKeepsFailing() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-degraded"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("bg-degraded"),
            store,
            new InMemoryOffsetStore(),
            events -> {
              throw new RuntimeException("poison batch");
            },
            new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)),
            10);

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (sub.consecutiveFailures() < 1 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(sub.isRunning(), "subscription stays RUNNING (degraded) while retrying");
      assertTrue(sub.consecutiveFailures() >= 1, "degraded status must be observable");
    } finally {
      sub.close();
    }
  }

  @Test
  void triggerImmediatePollReturnsZeroWhenAnotherPollIsInProgress() throws Exception {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-locked"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var listenerEntered = new java.util.concurrent.CountDownLatch(1);
    var releaseListener = new java.util.concurrent.CountDownLatch(1);
    var sub =
        new PollingEventSubscription(
            SubscriptionName.of("locked"),
            store,
            new InMemoryOffsetStore(),
            events -> {
              listenerEntered.countDown();
              try {
                releaseListener.await();
              } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
              }
            },
            fastConfig(),
            10);

    // Occupy the poll lock with a long-running pollOnce on another thread.
    Thread poller = Thread.ofVirtual().start(sub::pollOnce);
    assertTrue(listenerEntered.await(5, TimeUnit.SECONDS), "first poll must be in progress");

    // A notification-driven immediate poll must not race the in-flight poll on the same offset.
    assertEquals(0, sub.triggerImmediatePoll(), "concurrent immediate poll must be skipped");

    releaseListener.countDown();
    poller.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(poller.isAlive());
  }

  @Test
  void builderBuildsWithDefaults() {
    var store = new InMemoryEventStore();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("with-defaults")
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .build();
    assertNotNull(sub);
    assertFalse(sub.isRunning());
  }
}

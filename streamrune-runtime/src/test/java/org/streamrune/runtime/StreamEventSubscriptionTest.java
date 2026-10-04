package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.StreamSubscription;
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

class StreamEventSubscriptionTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

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

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10));
  }

  @Test
  void pollOnceDeliversStreamEventsAndAdvancesOffset() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-42"));
    store.append(s, List.of(envelope(s, 1), envelope(s, 2)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    EventListener listener = received::addAll;

    var offsetStore = new InMemoryOffsetStore();
    var sub =
        StreamEventSubscription.builder()
            .subscriptionName("cart-42-watcher")
            .streamId(s)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(listener)
            .config(fastConfig())
            .fetchSize(10)
            .build();

    int delivered = sub.pollOnce();
    assertEquals(2, delivered);
    assertEquals(2, received.size());
    assertTrue(offsetStore.getLastOffset(ProjectionName.of("cart-42-watcher")).value() > 0);
  }

  @Test
  void pollOnceReturnsZeroWhenNoEvents() {
    var store = new InMemoryEventStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("empty-sub"),
            StreamId.of(TYPE, AggregateId.of("empty-stream")),
            store,
            new InMemoryOffsetStore(),
            e -> {},
            fastConfig(),
            10);
    assertEquals(0, sub.pollOnce());
  }

  @Test
  void pollOnceAdvancesCheckpointPastForeignOnlyPage() {
    var store = new InMemoryEventStore();
    StreamId target = StreamId.of(TYPE, AggregateId.of("cart-target"));
    StreamId other = StreamId.of(TYPE, AggregateId.of("cart-other"));
    // Page 1 (fetchSize=2) contains ONLY foreign events; the target's event is beyond it.
    store.append(other, List.of(envelope(other, 1), envelope(other, 2)), Version.initial());
    store.append(target, List.of(envelope(target, 1)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("target-sub"),
            target,
            store,
            offsetStore,
            received::addAll,
            fastConfig(),
            2);

    // The first page contains only foreign events. The checkpoint MUST advance past them,
    // otherwise the subscription stalls on this page forever — and the cycle
    // DRAINS, so the very same cycle goes on to reach the target's event on the next page instead
    // of sleeping a whole polling interval first. (Before the drain this asserted "0 then
    // checkpoint 2" and "1 then checkpoint 3" across two separate pollOnce() calls; the anti-stall
    // intent is unchanged and the drain is now pinned on top of it.)
    assertEquals(1, sub.pollOnce());
    assertEquals(1, received.size());
    assertEquals(target, received.getFirst().streamId());
    assertEquals(3, offsetStore.getLastOffset(ProjectionName.of("target-sub")).value());

    // Nothing left: a further cycle is a no-op, proving the drain terminated at the head.
    assertEquals(0, sub.pollOnce());
    assertEquals(3, offsetStore.getLastOffset(ProjectionName.of("target-sub")).value());
  }

  @Test
  void pollOnceDrainsABacklogLargerThanFetchSizeInASingleCycle() {
    var store = new InMemoryEventStore();
    StreamId target = StreamId.of(TYPE, AggregateId.of("cart-drain-target"));
    StreamId other = StreamId.of(TYPE, AggregateId.of("cart-drain-other"));
    // 60 foreign events then 5 of the target's — with fetchSize 10 that is 7 pages.
    var foreign = new java.util.ArrayList<EventEnvelope>();
    for (int i = 1; i <= 60; i++) {
      foreign.add(envelope(other, i));
    }
    store.append(other, foreign, Version.initial());
    var mine = new java.util.ArrayList<EventEnvelope>();
    for (int i = 1; i <= 5; i++) {
      mine.add(envelope(target, i));
    }
    store.append(target, mine, Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("drain-target-sub"),
            target,
            store,
            offsetStore,
            received::addAll,
            fastConfig(),
            10);

    // One cycle must reach the head. One page per cycle would deliver 0 here and make
    // this subscription's latency proportional to the GLOBAL write rate, not to its own stream's.
    assertEquals(5, sub.pollOnce());
    assertEquals(5, received.size());
    assertEquals(65, offsetStore.getLastOffset(ProjectionName.of("drain-target-sub")).value());
  }

  @Test
  void pollOnceAbandonsTheDrainWhenClosedMidBacklog() {
    var store = new InMemoryEventStore();
    StreamId target = StreamId.of(TYPE, AggregateId.of("cart-close-target"));
    var all = new java.util.ArrayList<EventEnvelope>();
    for (int i = 1; i <= 50; i++) {
      all.add(envelope(target, i));
    }
    store.append(target, all, Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var subRef = new StreamEventSubscription[1];
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("close-target-sub"),
            target,
            store,
            offsetStore,
            events -> {
              received.addAll(events);
              subRef[0].close();
            },
            fastConfig(),
            10);
    subRef[0] = sub;

    assertEquals(10, sub.pollOnce());
    assertEquals(10, received.size());
    assertEquals(10, offsetStore.getLastOffset(ProjectionName.of("close-target-sub")).value());
  }

  @Test
  void pollOnceDoesNotAdvanceCheckpointWhenListenerFails() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-fail"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var offsetStore = new InMemoryOffsetStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("fail-sub"),
            s,
            store,
            offsetStore,
            e -> {
              throw new RuntimeException("listener failure");
            },
            fastConfig(),
            10);

    assertThrows(RuntimeException.class, sub::pollOnce);
    assertEquals(
        0,
        offsetStore.getLastOffset(ProjectionName.of("fail-sub")).value(),
        "at-least-once: no advance");
  }

  @Test
  void backgroundPollLoopSurvivesListenerExceptionAndRecovers() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-resilient"));
    store.append(s, List.of(envelope(s, 1)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var failuresLeft = new java.util.concurrent.atomic.AtomicInteger(2);
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("resilient-sub"),
            s,
            store,
            new InMemoryOffsetStore(),
            events -> {
              if (failuresLeft.getAndDecrement() > 0) {
                throw new RuntimeException("transient failure");
              }
              received.addAll(events);
            },
            new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)),
            10);

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (received.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(sub.isRunning(), "transient failures must not stop the subscription");
      assertEquals(1, received.size(), "events must be delivered after the failures pass");
      assertEquals(0, sub.consecutiveFailures(), "failure counter resets after recovery");
    } finally {
      sub.close();
    }
  }

  @Test
  void pollOnceFiltersOutOtherStreams() {
    var store = new InMemoryEventStore();
    StreamId target = StreamId.of(TYPE, AggregateId.of("cart-1"));
    StreamId other = StreamId.of(TYPE, AggregateId.of("other-1"));
    store.append(target, List.of(envelope(target, 1)), Version.initial());
    store.append(other, List.of(envelope(other, 1)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("filter-sub"),
            target,
            store,
            new InMemoryOffsetStore(),
            received::addAll,
            fastConfig(),
            10);

    int delivered = sub.pollOnce();
    assertEquals(1, delivered);
    assertEquals(1, received.size());
    assertEquals(target, received.get(0).streamId());
  }

  @Test
  void startCloseToggleRunning() throws InterruptedException {
    var store = new InMemoryEventStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("toggle-sub"),
            StreamId.of(TYPE, AggregateId.of("cart-1")),
            store,
            new InMemoryOffsetStore(),
            e -> {},
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
        new StreamEventSubscription(
            SubscriptionName.of("twice-sub"),
            StreamId.of(TYPE, AggregateId.of("cart-1")),
            store,
            new InMemoryOffsetStore(),
            e -> {},
            fastConfig(),
            10);
    try (sub) {
      sub.start();
      assertThrows(IllegalStateException.class, sub::start);
    }
  }

  @Test
  void streamIdAccessor() {
    var store = new InMemoryEventStore();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("accessor-sub"),
            StreamId.of(TYPE, AggregateId.of("cart-77")),
            store,
            new InMemoryOffsetStore(),
            e -> {},
            fastConfig(),
            1);
    assertEquals("cart:cart-77", sub.streamId().value());
  }

  @Test
  void constructorRejectsInvalidArguments() {
    var store = new InMemoryEventStore();
    var os = new InMemoryOffsetStore();
    EventListener listener = e -> {};
    var cfg = fastConfig();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                (SubscriptionName) null,
                StreamId.of(TYPE, AggregateId.of("a")),
                store,
                os,
                listener,
                cfg,
                10));
    // A blank subscription name never reaches the constructor: SubscriptionName rejects it first.
    assertThrows(IllegalArgumentException.class, () -> SubscriptionName.of("  "));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"), null, store, os, listener, cfg, 10));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"),
                StreamId.of(TYPE, AggregateId.of("a")),
                null,
                os,
                listener,
                cfg,
                10));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"),
                StreamId.of(TYPE, AggregateId.of("a")),
                store,
                null,
                listener,
                cfg,
                10));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"),
                StreamId.of(TYPE, AggregateId.of("a")),
                store,
                os,
                null,
                cfg,
                10));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"),
                StreamId.of(TYPE, AggregateId.of("a")),
                store,
                os,
                listener,
                null,
                10));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamEventSubscription(
                SubscriptionName.of("sub"),
                StreamId.of(TYPE, AggregateId.of("a")),
                store,
                os,
                listener,
                cfg,
                0));
  }

  @Test
  void builderBuildsWithDefaults() {
    var store = new InMemoryEventStore();
    var sub =
        StreamEventSubscription.builder()
            .subscriptionName("cart-9-watcher")
            .streamId(StreamId.of(TYPE, AggregateId.of("cart-9")))
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .build();
    assertEquals("cart:cart-9", sub.streamId().value());
  }

  @Test
  void builderRequiresSubscriptionName() {
    var store = new InMemoryEventStore();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamEventSubscription.builder()
                .streamId(StreamId.of(TYPE, AggregateId.of("cart-9")))
                .eventStore(store)
                .offsetStore(new InMemoryOffsetStore())
                .listener(e -> {})
                .build());
  }

  @Test
  void twoSubscriptionsOnSameStreamKeepIndependentCheckpoints() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-shared"));
    store.append(s, List.of(envelope(s, 1), envelope(s, 2)), Version.initial());

    var offsetStore = new InMemoryOffsetStore();
    var receivedA = new CopyOnWriteArrayList<EventEnvelope>();
    var receivedB = new CopyOnWriteArrayList<EventEnvelope>();
    var subA =
        new StreamEventSubscription(
            SubscriptionName.of("audit-listener"),
            s,
            store,
            offsetStore,
            receivedA::addAll,
            fastConfig(),
            10);
    var subB =
        new StreamEventSubscription(
            SubscriptionName.of("email-listener"),
            s,
            store,
            offsetStore,
            receivedB::addAll,
            fastConfig(),
            10);

    // A polls first and advances ITS checkpoint; B must still see every event — with a shared
    // checkpoint (the old streamId-keyed behavior) B would find nothing to read.
    assertEquals(2, subA.pollOnce());
    assertEquals(2, subB.pollOnce());
    assertEquals(2, receivedA.size());
    assertEquals(2, receivedB.size());
    assertEquals(2, offsetStore.getLastOffset(ProjectionName.of("audit-listener")).value());
    assertEquals(2, offsetStore.getLastOffset(ProjectionName.of("email-listener")).value());
  }

  @Test
  void backgroundPollLoopDeliversEvents() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-bg"));
    store.append(s, List.of(envelope(s, 1), envelope(s, 2)), Version.initial());

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        new StreamEventSubscription(
            SubscriptionName.of("bg-sub"),
            s,
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

  // ── A deterministic read poison must stop the subscription terminally, not spin ─────

  /**
   * A store whose global read fails deterministically because a FOREIGN stream's event type is not
   * registered — the poison this subscription can never filter out, because {@code
   * readGlobalStream} deserializes the whole page before {@code pollOnce()} filters by stream.
   */
  private static EventStore poisonedStore() {
    var store = mock(EventStore.class);
    when(store.readGlobalStream(any(), anyInt()))
        .thenThrow(
            new UnknownEventTypeException("event type", "other.ForeignEvent", List.of("Added")));
    return store;
  }

  @Test
  void foreignReadPoisonStopsTheSubscriptionTerminallyAfterTheBound() throws InterruptedException {
    // pollOnce() reads the GLOBAL page and filters by streamId only AFTER
    // readGlobalStream has deserialized every foreign event, so an unregistered FOREIGN event
    // type throws inside the read. ResilientPollLoop treated it as transient and retried forever:
    // the checkpoint never advanced, isRunning() stayed true, and — with no ReadPoisonAware /
    // readPoisonBound like the sibling PollingEventSubscription — nothing could tell "wedged on a
    // poison" from "idle".
    StreamSubscription sub =
        StreamEventSubscription.builder()
            .subscriptionName("poison-sub")
            .streamId(StreamId.of(TYPE, AggregateId.of("cart-1")))
            .eventStore(poisonedStore())
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)))
            .fetchSize(10)
            .build();

    sub.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (sub.isRunning() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(
          sub.isRunning(),
          "a deterministic read poison must stop the subscription terminally after the bound");
      assertInstanceOf(
          ReadPoisonAware.class,
          sub,
          "the terminal poison must be exposed via ReadPoisonAware like the sibling subscriptions");
      ProjectionReadPoisonException poison = ((ReadPoisonAware) sub).readPoisonError();
      assertNotNull(poison, "the terminal read-poison error must be exposed");
      assertInstanceOf(UnknownEventTypeException.class, poison.getCause());
    } finally {
      sub.close();
    }
  }

  @Test
  void foreignReadPoisonBound_thenStartSucceedsPromptly_notAfterTheStaleBackoff()
      throws InterruptedException {
    // onReadFailure sets running=false THEN throws the terminal poison (~:402-432);
    // ResilientPollLoop caught it as an ordinary RuntimeException and slept the 5th-failure
    // backoff (initial x 16, capped at max(pollingInterval, 60s)) before ever rechecking
    // keepRunning, so start()'s "Previous polling thread is still running" guard refused a
    // restart for that whole window — contradicting the documented remedy ("repair the event,
    // then start() the subscription again"). pollingInterval=150ms here makes the terminal (5th)
    // backoff 150ms*16=2400ms, jittered to at least 1200ms — comfortably longer than the 900ms
    // window this test allows for start() to succeed, so a stale-backoff regression fails loudly
    // without the test itself waiting out a full production-sized (60s-capped) backoff.
    StreamSubscription sub =
        StreamEventSubscription.builder()
            .subscriptionName("poison-restart-sub")
            .streamId(StreamId.of(TYPE, AggregateId.of("cart-1")))
            .eventStore(poisonedStore())
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .config(new SubscriptionConfig(false, Duration.ofMillis(150), Duration.ofMillis(1)))
            .fetchSize(10)
            .build();

    sub.start();
    try {
      long boundDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (sub.isRunning() && System.nanoTime() < boundDeadline) {
        Thread.sleep(20);
      }
      assertFalse(sub.isRunning(), "the read-poison bound must stop the subscription terminally");

      long startDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(900);
      boolean started = false;
      IllegalStateException lastFailure = null;
      while (System.nanoTime() < startDeadline) {
        try {
          sub.start();
          started = true;
          break;
        } catch (IllegalStateException e) {
          lastFailure = e;
          Thread.sleep(20);
        }
      }
      assertTrue(
          started,
          "start() must succeed within ~900ms of the bound stopping the subscription — not block"
              + " for the stale backoff window the dead terminal thread used to sleep out; last"
              + " failure: "
              + lastFailure);
    } finally {
      sub.close();
    }
  }

  @Test
  void pollOnceThrowsTheTerminalPoisonWhenTheBoundIsReached() {
    // Direct-drive variant of the bound: reads 1..4 rethrow the store's own poison (still under
    // the default bound of 5); the 5th surfaces the terminal ProjectionReadPoisonException, which
    // readPoisonError() then exposes.
    var impl =
        StreamEventSubscription.builder()
            .subscriptionName("poison-direct")
            .streamId(StreamId.of(TYPE, AggregateId.of("cart-1")))
            .eventStore(poisonedStore())
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .config(fastConfig())
            .fetchSize(10)
            .build();

    for (int attempt = 1; attempt < StreamEventSubscription.DEFAULT_READ_POISON_BOUND; attempt++) {
      assertThrows(
          UnknownEventTypeException.class,
          impl::pollOnce,
          "attempt " + attempt + " is still under the bound and rethrows the store's error");
      assertNull(impl.readPoisonError(), "no terminal poison under the bound");
    }
    var poison = assertThrows(ProjectionReadPoisonException.class, impl::pollOnce);
    assertSame(poison, impl.readPoisonError());
    assertInstanceOf(UnknownEventTypeException.class, poison.getCause());
    assertFalse(impl.isRunning());
  }

  @Test
  void transientReadFailuresNeverCountTowardThePoisonBound() throws InterruptedException {
    // A SQLException-caused read failure is infrastructure, not poison: the streak resets and the
    // resilient loop's unbounded retry applies — the subscription recovers and delivers even after
    // MORE consecutive failures than the bound.
    var real = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-blip"));
    real.append(s, List.of(envelope(s, 1)), Version.initial());
    var failuresLeft = new AtomicInteger(StreamEventSubscription.DEFAULT_READ_POISON_BOUND + 2);
    var store = mock(EventStore.class);
    when(store.readGlobalStream(any(), anyInt()))
        .thenAnswer(
            inv -> {
              if (failuresLeft.getAndDecrement() > 0) {
                throw new EventStoreException("db blip", new SQLException("connection reset"));
              }
              return real.readGlobalStream(inv.getArgument(0), inv.getArgument(1));
            });
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var impl =
        StreamEventSubscription.builder()
            .subscriptionName("blip-sub")
            .streamId(s)
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(received::addAll)
            .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)))
            .fetchSize(10)
            .build();

    impl.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (received.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(1, received.size(), "delivery resumes once the transient read failures pass");
      assertTrue(impl.isRunning(), "transient read failures never stop the subscription");
      assertNull(impl.readPoisonError());
    } finally {
      impl.close();
    }
  }

  @Test
  void readPoisonBoundZeroKeepsTheUnboundedRetry() throws InterruptedException {
    // Opt-out: readPoisonBound(0) keeps the unbounded retry for callers that prefer it.
    var impl =
        StreamEventSubscription.builder()
            .subscriptionName("unbounded-sub")
            .streamId(StreamId.of(TYPE, AggregateId.of("cart-1")))
            .eventStore(poisonedStore())
            .offsetStore(new InMemoryOffsetStore())
            .listener(e -> {})
            .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)))
            .fetchSize(10)
            .readPoisonBound(0)
            .build();

    impl.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (impl.consecutiveFailures() <= StreamEventSubscription.DEFAULT_READ_POISON_BOUND
          && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(
          impl.consecutiveFailures() > StreamEventSubscription.DEFAULT_READ_POISON_BOUND,
          "the loop keeps retrying past the default bound");
      assertTrue(impl.isRunning(), "an unbounded subscription never stops itself");
      assertNull(impl.readPoisonError());
    } finally {
      impl.close();
    }
  }
}

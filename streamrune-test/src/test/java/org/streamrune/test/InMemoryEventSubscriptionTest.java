package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

class InMemoryEventSubscriptionTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  private final InMemoryEventStore store = new InMemoryEventStore();
  private final List<EventEnvelope> received = new ArrayList<>();

  private EventEnvelope envelope(StreamId id, long v, String sku) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        id,
        new Version(v),
        new EventType("Added"),
        new Added(sku),
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

  private void append(StreamId s, long version, String sku) {
    store.append(s, List.of(envelope(s, version, sku)), new Version(version - 1));
  }

  @Test
  void deliversEventsAppendedAfterStart() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A");
    append(s, 2, "B");

    assertEquals(2, received.size());
    assertEquals(2, sub.lastDeliveredOffset());
  }

  @Test
  void startCatchesUpOnEventsAppendedBeforeStart() {
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A"); // appended before LISTEN — its notification is missed
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();

    assertEquals(1, received.size(), "poll-after-LISTEN must catch up missed events");
  }

  @Test
  void droppedNotificationsAreLostUntilPollRecovers() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    sub.dropNotifications(true);
    append(s, 1, "A");
    assertTrue(received.isEmpty(), "dropped notification must not deliver");
    assertEquals(1, sub.droppedNotificationCount());

    sub.dropNotifications(false);
    sub.poll(); // fallback polling cycle recovers the missed events
    assertEquals(1, received.size());
    assertEquals(1, sub.lastDeliveredOffset());
  }

  @Test
  void nextNotificationDeliversEventsMissedByDroppedOnes() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    sub.dropNotifications(true);
    append(s, 1, "A");
    sub.dropNotifications(false);
    append(s, 2, "B"); // this notification wakes the subscription...

    // ...and the catch-up read delivers BOTH events (payload is only an offset hint).
    assertEquals(2, received.size());
  }

  @Test
  void throwingListenerDoesNotAdvanceCheckpointAndBatchIsRedelivered() {
    var failNext = new AtomicBoolean(true);
    var sub =
        new InMemoryEventSubscription(
            store,
            events -> {
              if (failNext.getAndSet(false)) {
                throw new IllegalStateException("projection store down");
              }
              received.addAll(events);
            });
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A");

    assertTrue(received.isEmpty(), "failed batch must not be acknowledged");
    assertEquals(0, sub.lastDeliveredOffset());
    assertEquals(1, sub.deliveryFailureCount());

    sub.poll(); // retry — same batch is redelivered
    assertEquals(1, received.size());
    assertEquals(1, sub.lastDeliveredOffset());
  }

  @Test
  void deliversInBatchesOfAtMostBatchSize() {
    var batchSizes = new ArrayList<Integer>();
    var sub =
        new InMemoryEventSubscription(
            store,
            events -> {
              batchSizes.add(events.size());
              received.addAll(events);
            },
            2);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(
        s,
        List.of(envelope(s, 1, "A"), envelope(s, 2, "B"), envelope(s, 3, "C")),
        Version.initial());
    sub.start();

    assertEquals(3, received.size());
    assertEquals(List.of(2, 1), batchSizes);
  }

  @Test
  void lifecycleFollowsTheEventSubscriptionContract() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    assertFalse(sub.isRunning());

    sub.start();
    assertTrue(sub.isRunning());
    assertThrows(IllegalStateException.class, sub::start);

    sub.close();
    assertFalse(sub.isRunning());
    assertThrows(IllegalStateException.class, sub::start);
    assertDoesNotThrow(sub::close); // close is idempotent
  }

  @Test
  void startAfterCloseThrowsEvenWhenNeverStarted() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.close();
    assertThrows(IllegalStateException.class, sub::start);
  }

  @Test
  void closedSubscriptionReceivesNothingAndPollThrows() {
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();
    sub.close();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A");

    assertTrue(received.isEmpty());
    assertThrows(IllegalStateException.class, sub::poll);
  }

  @Test
  void notificationsBeforeStartAreIgnored() {
    var listening = new AtomicLong();
    store.addAppendListener(listening::set); // unrelated listener keeps the hook exercised
    var sub = new InMemoryEventSubscription(store, received::addAll);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A"); // subscription not started — nothing delivered

    assertTrue(received.isEmpty());
    assertEquals(1, listening.get());
    sub.start(); // catch-up delivers it
    assertEquals(1, received.size());
  }

  @Test
  void constructorValidatesArguments() {
    assertThrows(NullPointerException.class, () -> new InMemoryEventSubscription(null, e -> {}));
    assertThrows(NullPointerException.class, () -> new InMemoryEventSubscription(store, null));
    assertThrows(
        IllegalArgumentException.class, () -> new InMemoryEventSubscription(store, e -> {}, 0));
  }

  @Test
  void appendSucceedsEvenWhenARawAppendListenerThrows() {
    store.addAppendListener(
        offset -> {
          throw new IllegalStateException("broken listener");
        });
    var sub = new InMemoryEventSubscription(store, received::addAll);
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    assertDoesNotThrow(() -> append(s, 1, "A"));
    assertEquals(1, received.size(), "other listeners must still be notified");
  }

  @Test
  void removedAppendListenerIsNoLongerNotified() {
    var count = new AtomicLong();
    InMemoryEventStore.AppendListener listener = offset -> count.incrementAndGet();
    store.addAppendListener(listener);
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A");
    store.removeAppendListener(listener);
    append(s, 2, "B");

    assertEquals(1, count.get());
  }

  @Test
  void appendListenerRegistrationRejectsNull() {
    assertThrows(NullPointerException.class, () -> store.addAppendListener(null));
    assertThrows(NullPointerException.class, () -> store.removeAppendListener(null));
  }
}

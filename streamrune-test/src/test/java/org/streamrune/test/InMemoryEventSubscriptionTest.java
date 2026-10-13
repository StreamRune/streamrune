package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
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

  /** What a listener finds bound on its thread while a batch is delivered to it. */
  record Bound(
      StreamRuneContext.RequestContext context,
      boolean sagaDispatch,
      StreamRuneContext.DispatchCause cause) {

    static final Bound NOTHING = new Bound(null, false, null);

    static Bound now() {
      return new Bound(
          StreamRuneContext.capture(),
          StreamRuneContext.isSagaDispatch(),
          StreamRuneContext.cause());
    }
  }

  private static final CorrelationId APPENDER_CORRELATION = CorrelationId.of("order-saga-42");

  /**
   * The bindings of a saga dispatch driven from a request thread: what the thread that appends a
   * saga's reply event can hold.
   */
  private static ScopedValue.Carrier sagaDispatchBindings() {
    var context =
        new StreamRuneContext.RequestContext(
            TraceId.of("trace-1"),
            UserId.of("operator-1"),
            APPENDER_CORRELATION,
            Instant.now(),
            Map.of("role", "ADMIN"),
            new UserAuthority(Set.of("ADMIN"), Set.of()));
    var cause =
        new StreamRuneContext.DispatchCause(
            CausationId.of("evt-trigger"),
            new SagaOrigin(SagaType.of("com.shop.OrderSaga$State"), SagaId.of("order-saga-42"), 0));
    return ScopedValue.where(StreamRuneContext.CURRENT, context)
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(StreamRuneContext.CAUSE, cause);
  }

  /**
   * Nothing of the appender reaches the listener: no saga flag, no cause, and in place of the
   * appender's request context an empty one.
   */
  private static void assertDetached(Bound bound) {
    assertFalse(bound.sagaDispatch(), "the saga flag of the appender must not reach the listener");
    assertNull(bound.cause(), "the dispatch cause of the appender must not reach the listener");
    assertNotNull(bound.context(), "a bound context is replaced, since it cannot be unbound");
    assertNotEquals(APPENDER_CORRELATION, bound.context().correlationId());
    assertNull(bound.context().traceId());
    assertNull(bound.context().userId());
    assertNull(bound.context().authority());
    assertTrue(bound.context().baggage().isEmpty());
  }

  @Test
  void aListenerRunsOutsideTheAppendersBindings_onANotificationAndOnAPoll() {
    var seen = new CopyOnWriteArrayList<Bound>();
    var sub = new InMemoryEventSubscription(store, events -> seen.add(Bound.now()));
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    ScopedValue.Carrier sagaDispatch = sagaDispatchBindings();

    // A notification: the append happens inside a saga dispatch's bindings.
    sagaDispatch.run(() -> append(s, 1, "A"));
    // A poll, called from inside the same bindings.
    sub.dropNotifications(true);
    sagaDispatch.run(() -> append(s, 2, "B"));
    sub.dropNotifications(false);
    sagaDispatch.run(sub::poll);
    // A notification from a thread with nothing bound.
    append(s, 3, "C");

    assertEquals(3, seen.size());
    assertDetached(seen.get(0));
    assertDetached(seen.get(1));
    assertEquals(
        Bound.NOTHING,
        seen.get(2),
        "with nothing bound on the appending thread, nothing is bound for the listener");
    assertEquals(3, sub.lastDeliveredOffset());
  }

  @Test
  void eachOfTheThreeBindingsIsTakenAwayOnItsOwn() {
    var seen = new CopyOnWriteArrayList<Bound>();
    var sub = new InMemoryEventSubscription(store, events -> seen.add(Bound.now()));
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    var cause = new StreamRuneContext.DispatchCause(CausationId.of("evt-trigger"), null);

    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE).run(() -> append(s, 1, "A"));
    ScopedValue.where(StreamRuneContext.CAUSE, cause).run(() -> append(s, 2, "B"));
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(StreamRuneContext.CAUSE, cause)
        .run(() -> append(s, 3, "C"));

    assertEquals(
        List.of(Bound.NOTHING, Bound.NOTHING, Bound.NOTHING),
        seen,
        "no context was bound by the appender, so none is bound for the listener");
  }

  @Test
  void startDeliversTheCatchUpOutsideTheCallersBindings() {
    var seen = new CopyOnWriteArrayList<Bound>();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    append(s, 1, "A");
    var sub = new InMemoryEventSubscription(store, events -> seen.add(Bound.now()));

    sagaDispatchBindings().run(sub::start);

    assertEquals(1, seen.size());
    assertDetached(seen.getFirst());
  }

  @Test
  void anAppendFromInsideTheListenerIsDeliveredAfterTheBatchInHand_notInsideIt() {
    var order = new CopyOnWriteArrayList<String>();
    var followUps = new AtomicLong();
    var sub =
        new InMemoryEventSubscription(
            store,
            events -> {
              for (EventEnvelope event : events) {
                String sku = ((Added) event.event()).sku();
                order.add("start " + sku);
                if (!sku.startsWith("follow-up")) {
                  // What a listener that executes an unkeyed command does: every call appends.
                  StreamId own =
                      StreamId.of(TYPE, AggregateId.of("follow-" + followUps.incrementAndGet()));
                  append(own, 1, "follow-up of " + sku);
                }
                order.add("end " + sku);
              }
            });
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    append(s, 1, "A");

    assertEquals(
        List.of("start A", "end A", "start follow-up of A", "end follow-up of A"),
        order,
        "the listener's own append wakes no nested delivery: its event follows the batch in hand");
    assertEquals(1, followUps.get(), "each event is delivered once");
    assertEquals(2, sub.lastDeliveredOffset(), "delivered before the outer append returned");
    assertEquals(0, sub.deliveryFailureCount());
  }

  @Test
  void aPollFromInsideTheListenerStartsNoNestedDelivery() {
    var deliveries = new AtomicLong();
    var holder = new InMemoryEventSubscription[1];
    var sub =
        new InMemoryEventSubscription(
            store,
            events -> {
              deliveries.incrementAndGet();
              holder[0].poll();
            });
    holder[0] = sub;
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    append(s, 1, "A");

    assertEquals(1, deliveries.get());
    assertEquals(1, sub.lastDeliveredOffset());
  }

  @Test
  void aFailedBatchLeavesTheSubscriptionReadyForTheNextAppend() {
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
    sagaDispatchBindings().run(() -> append(s, 1, "A"));
    assertEquals(1, sub.deliveryFailureCount());

    // The next append, on the same thread, delivers both events: the failed delivery left no
    // delivery marked as in progress.
    append(s, 2, "B");

    assertEquals(2, received.size());
    assertEquals(2, sub.lastDeliveredOffset());
  }

  @Test
  void anErrorThrownByTheListenerReachesTheAppender() {
    var sub =
        new InMemoryEventSubscription(
            store,
            events -> {
              throw new AssertionError("listener assertion");
            });
    sub.start();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    AssertionError thrown = assertThrows(AssertionError.class, () -> append(s, 1, "A"));

    assertEquals("listener assertion", thrown.getMessage());
    assertEquals(0, sub.lastDeliveredOffset());
    assertEquals(0, sub.deliveryFailureCount(), "only a listener's exception counts as a failure");
    sub.close();
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

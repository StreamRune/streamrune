package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class ForwardingEventStoreTest {

  record OrderPlaced(String orderId) implements DomainEvent {}

  private static final StreamId ORDER =
      StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));

  private static EventEnvelope placed() {
    return EventStoreFixture.event(
        ORDER, new Version(1), EventType.fromClass(OrderPlaced.class), new OrderPlaced("order-1"));
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theThrowingDefaultsIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        EventStore.class, ForwardingEventStore.class, 12);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        EventStore.class, ForwardingEventStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        EventStore.class, ForwardingEventStore::new);
  }

  @Test
  void theSamplesOfTheChecksHaveTheirTypes() {
    ForwardingDecoratorChecks.assertTheSamplesHaveTheirTypes();
  }

  @Test
  void failOnce_failsTheFirstAppendBeforeItIsStored_andTheSecondSucceeds() {
    var store = new InMemoryEventStore();
    var faults = FaultSchedule.none().failOnce("append");
    var forwarding = new ForwardingEventStore(store, faults);

    assertThrows(
        InjectedFaultException.class,
        () -> forwarding.append(ORDER, List.of(placed()), Version.initial()));
    assertTrue(store.readStream(ORDER, Version.initial(), 10).isEmpty(), "nothing was stored");

    var result = forwarding.append(ORDER, List.of(placed()), Version.initial());

    assertEquals(new Version(1), result.finalVersion());
    assertEquals(1, store.readStream(ORDER, Version.initial(), 10).size());
    assertEquals(2, faults.calls("append"));
  }

  @Test
  void crashAfter_leavesTheEventStored_andThrows() {
    var store = new InMemoryEventStore();
    var forwarding = new ForwardingEventStore(store, FaultSchedule.none().crashAfter("append"));

    assertThrows(
        InjectedFaultException.class,
        () -> forwarding.append(ORDER, List.of(placed()), Version.initial()));

    assertEquals(1, store.readStream(ORDER, Version.initial(), 10).size(), "the write happened");
    assertEquals(GlobalOffset.of(1), forwarding.lastGlobalOffset());
  }

  @Test
  void theDefaultMethods_reachTheDelegatesImplementation() {
    var rows = new InMemorySagaStore();
    var store = new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox());
    store.withSagaStore(rows);
    var forwarding = new ForwardingEventStore(store);

    var first =
        forwarding.appendWithKey(
            ORDER, List.of(placed()), Version.initial(), IdempotencyKey.of("k-1"), "PlaceOrder");
    var replay =
        forwarding.appendWithKey(
            ORDER, List.of(placed()), Version.initial(), IdempotencyKey.of("k-1"), "PlaceOrder");

    assertTrue(replay.alreadyApplied(), "the delegate's inbox answered the replay");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(GlobalOffset.of(1), forwarding.lastGlobalOffset());
    assertSame(store.sagaFenceStorageIdentity(), forwarding.sagaFenceStorageIdentity());
    assertEquals(1, forwarding.load(ORDER, EventStore.IGNORE_SNAPSHOT).events().size());
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("apend");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingEventStore(new InMemoryEventStore(), misspelled));
  }
}

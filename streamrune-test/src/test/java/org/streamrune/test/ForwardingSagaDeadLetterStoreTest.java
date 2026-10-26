package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

class ForwardingSagaDeadLetterStoreTest {

  private static final SagaType TYPE = SagaType.of("com.example.OrderSaga$State");
  private static final SagaId SAGA = SagaId.of("order-saga-1");

  private static SagaDeadLetterEntry entry() {
    return new SagaDeadLetterEntry(
        SAGA,
        TYPE,
        GlobalOffset.of(7),
        EventType.of("OrderPlaced"),
        "java.lang.IllegalStateException",
        "poison",
        Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theThrowingDefaultsIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        SagaDeadLetterStore.class, ForwardingSagaDeadLetterStore.class, 12);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        SagaDeadLetterStore.class, ForwardingSagaDeadLetterStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        SagaDeadLetterStore.class, ForwardingSagaDeadLetterStore::new);
  }

  @Test
  void failOnce_failsTheFirstPublishBeforeTheEntryIsWritten_andTheSecondSucceeds() {
    var store = new InMemorySagaDeadLetterStore(new InMemorySagaStore());
    var forwarding =
        new ForwardingSagaDeadLetterStore(store, FaultSchedule.none().failOnce("publish"));

    assertThrows(InjectedFaultException.class, () -> forwarding.publish(entry()));
    assertTrue(store.all().isEmpty(), "nothing was written");

    forwarding.publish(entry());

    assertEquals(1, store.all().size());
  }

  @Test
  void crashAfter_leavesTheEntryWritten_andThrows() {
    var store = new InMemorySagaDeadLetterStore(new InMemorySagaStore());
    var forwarding =
        new ForwardingSagaDeadLetterStore(store, FaultSchedule.none().crashAfter("publish"));

    assertThrows(InjectedFaultException.class, () -> forwarding.publish(entry()));

    assertEquals(1, store.all().size(), "the write happened");
    assertEquals(1, forwarding.findBySaga(SAGA).size());
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("publsh");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ForwardingSagaDeadLetterStore(
                new InMemorySagaDeadLetterStore(new InMemorySagaStore()), misspelled));
  }
}

package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;

class ForwardingSagaStoreTest {

  record State(SagaStatus status) implements SagaState {}

  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final SagaId SAGA = SagaId.of("order-saga-1");

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theDefaultsIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        SagaStore.class, ForwardingSagaStore.class, 18);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(SagaStore.class, ForwardingSagaStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        SagaStore.class, ForwardingSagaStore::new);
  }

  @Test
  void failOnce_failsTheFirstCreateBeforeTheRowIsWritten_andTheSecondSucceeds() {
    var rows = new InMemorySagaStore();
    var forwarding = new ForwardingSagaStore(rows, FaultSchedule.none().failOnce("create"));

    assertThrows(
        InjectedFaultException.class,
        () -> forwarding.create(SAGA, TYPE, new State(SagaStatus.STARTED), SagaStatus.STARTED));
    assertTrue(rows.load(SAGA, TYPE, State.class).isEmpty(), "no row was written");

    forwarding.create(SAGA, TYPE, new State(SagaStatus.STARTED), SagaStatus.STARTED);

    assertEquals(SagaStatus.STARTED, rows.load(SAGA, TYPE, State.class).orElseThrow().status());
  }

  @Test
  void crashAfter_leavesTheRowWritten_andThrows() {
    var rows = new InMemorySagaStore();
    var forwarding = new ForwardingSagaStore(rows, FaultSchedule.none().crashAfter("create"));

    assertThrows(
        InjectedFaultException.class,
        () -> forwarding.create(SAGA, TYPE, new State(SagaStatus.STARTED), SagaStatus.STARTED));

    assertTrue(rows.load(SAGA, TYPE, State.class).isPresent(), "the write happened");
  }

  @Test
  void theStorageIdentity_isTheDelegatesRows() {
    var rows = new InMemorySagaStore();

    assertSame(rows.storageIdentity(), new ForwardingSagaStore(rows).storageIdentity());
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().crashAfter("applyEvents");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingSagaStore(new InMemorySagaStore(), misspelled));
  }
}

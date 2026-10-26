package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ForwardingOffsetStoreTest {

  private static final ProjectionName ORDERS = ProjectionName.of("orders");

  @Test
  void everyMethodOfTheInterfaceIsOverridden_resetIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        OffsetStore.class, ForwardingOffsetStore.class, 3);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        OffsetStore.class, ForwardingOffsetStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        OffsetStore.class, ForwardingOffsetStore::new);
  }

  @Test
  void failOnce_failsTheFirstSaveBeforeTheCheckpointMoves_andTheSecondSucceeds() {
    var offsets = new InMemoryOffsetStore();
    var forwarding =
        new ForwardingOffsetStore(offsets, FaultSchedule.none().failOnce("saveOffset"));

    assertThrows(
        InjectedFaultException.class, () -> forwarding.saveOffset(ORDERS, GlobalOffset.of(5)));
    assertEquals(GlobalOffset.initial(), offsets.getLastOffset(ORDERS), "nothing was saved");

    forwarding.saveOffset(ORDERS, GlobalOffset.of(5));

    assertEquals(GlobalOffset.of(5), offsets.getLastOffset(ORDERS));
  }

  @Test
  void crashAfter_leavesTheCheckpointSaved_andThrows() {
    var offsets = new InMemoryOffsetStore();
    var forwarding =
        new ForwardingOffsetStore(offsets, FaultSchedule.none().crashAfter("saveOffset"));

    assertThrows(
        InjectedFaultException.class, () -> forwarding.saveOffset(ORDERS, GlobalOffset.of(5)));

    assertEquals(GlobalOffset.of(5), forwarding.getLastOffset(ORDERS), "the write happened");
  }

  @Test
  void reset_reachesTheDelegatesRewind() {
    var offsets = new InMemoryOffsetStore();
    var forwarding = new ForwardingOffsetStore(offsets);
    forwarding.saveOffset(ORDERS, GlobalOffset.of(5));

    forwarding.reset(ORDERS);

    assertEquals(GlobalOffset.initial(), offsets.getLastOffset(ORDERS));
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("save");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingOffsetStore(new InMemoryOffsetStore(), misspelled));
  }
}

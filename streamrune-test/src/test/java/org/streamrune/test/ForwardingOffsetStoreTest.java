package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCheckpointSaveException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ForwardingOffsetStoreTest {

  private static final ProjectionName ORDERS = ProjectionName.of("orders");

  @Test
  void everyMethodOfTheInterfaceIsOverridden_resetAndSubscriptionCheckpointIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        OffsetStore.class, ForwardingOffsetStore.class, 4);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        OffsetStore.class,
        ForwardingOffsetStore::new,
        Map.of(
            "subscriptionCheckpoint",
            (answer, result, decorator) -> {
              var delegated = (CheckpointTarget) answer;
              var pair = (CheckpointTarget) result;
              assertEquals(delegated.name(), pair.name(), "the delegate's name");
              assertSame(delegated.processor(), pair.processor(), "the delegate's processor");
              assertSame(decorator, pair.checkpoints(), "read through the decorator");
            }));
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        OffsetStore.class, ForwardingOffsetStore::new);
  }

  /**
   * The subscription checkpoint of a forwarding store keeps the schedule: a commit through the pair
   * saves through the decorator, so a fault scheduled on {@code saveOffset} fires, and the
   * checkpoint does not move until a commit succeeds.
   */
  @Test
  void aFaultOnSaveOffset_firesThroughTheSubscriptionCheckpointPair() {
    var offsets = new InMemoryOffsetStore();
    var forwarding =
        new ForwardingOffsetStore(offsets, FaultSchedule.none().failOnce("saveOffset"));
    CheckpointTarget pair = forwarding.subscriptionCheckpoint();

    var failure =
        assertThrows(
            ProjectionCheckpointSaveException.class,
            () ->
                pair.processor()
                    .executeAtomically(
                        ORDERS, List.of(), GlobalOffset.of(5), 0L, repo -> {}, pair.checkpoints()));
    assertInstanceOf(InjectedFaultException.class, failure.getCause());
    assertEquals(GlobalOffset.initial(), pair.checkpoints().getLastOffset(ORDERS));

    pair.processor()
        .executeAtomically(
            ORDERS, List.of(), GlobalOffset.of(5), 0L, repo -> {}, pair.checkpoints());

    assertEquals(GlobalOffset.of(5), offsets.getLastOffset(ORDERS));
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

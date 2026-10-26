package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ForwardingProjectionDeadLetterStoreTest {

  private static ProjectionDeadLetterEntry entry() {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of("orders"),
        GlobalOffset.of(1),
        GlobalOffset.of(5),
        5,
        "java.lang.IllegalStateException",
        "poison",
        1,
        Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_countPendingIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        ProjectionDeadLetterStore.class, ForwardingProjectionDeadLetterStore.class, 5);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        ProjectionDeadLetterStore.class, ForwardingProjectionDeadLetterStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        ProjectionDeadLetterStore.class, ForwardingProjectionDeadLetterStore::new);
  }

  @Test
  void failOnce_failsTheFirstSaveBeforeTheEntryIsWritten_andTheSecondSucceeds() {
    var store = new InMemoryProjectionDeadLetterStore();
    var forwarding =
        new ForwardingProjectionDeadLetterStore(store, FaultSchedule.none().failOnce("save"));

    assertThrows(InjectedFaultException.class, () -> forwarding.save(entry()));
    assertTrue(store.all().isEmpty(), "nothing was written");

    forwarding.save(entry());

    assertEquals(1, store.all().size());
  }

  @Test
  void crashAfter_leavesTheEntryWritten_andThrows() {
    var store = new InMemoryProjectionDeadLetterStore();
    var forwarding =
        new ForwardingProjectionDeadLetterStore(store, FaultSchedule.none().crashAfter("save"));

    assertThrows(InjectedFaultException.class, () -> forwarding.save(entry()));

    assertEquals(1, store.all().size(), "the write happened");
    assertEquals(1L, forwarding.countPending());
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("publish");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ForwardingProjectionDeadLetterStore(
                new InMemoryProjectionDeadLetterStore(), misspelled));
  }
}

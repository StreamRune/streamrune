package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

class ForwardingOutboxStoreTest {

  private static OutboxEntry entry() {
    return OutboxEntry.pending(
        OutboxEntryId.of("entry-1"),
        "{}",
        "OrderPlaced",
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-1")));
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theDefaultsIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        OutboxStore.class, ForwardingOutboxStore.class, 19);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        OutboxStore.class, ForwardingOutboxStore::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        OutboxStore.class, ForwardingOutboxStore::new);
  }

  @Test
  void failOnce_failsTheFirstSaveBeforeTheEntryIsWritten_andTheSecondSucceeds() {
    var outbox = new InMemoryOutboxStore();
    var forwarding = new ForwardingOutboxStore(outbox, FaultSchedule.none().failOnce("save"));

    assertThrows(InjectedFaultException.class, () -> forwarding.save(entry()));
    assertTrue(outbox.all().isEmpty(), "nothing was written");

    forwarding.save(entry());

    assertEquals(1L, outbox.countByStatus(OutboxStatus.PENDING));
  }

  @Test
  void crashAfter_leavesTheEntryWritten_andThrows() {
    var outbox = new InMemoryOutboxStore();
    var forwarding = new ForwardingOutboxStore(outbox, FaultSchedule.none().crashAfter("save"));

    assertThrows(InjectedFaultException.class, () -> forwarding.save(entry()));

    assertEquals(1, outbox.all().size(), "the write happened");
  }

  @Test
  void aSubclass_overridesOneCall_andTheRestKeepsTheDelegatesBehaviour() {
    var outbox = new InMemoryOutboxStore();
    var noBlockageSample =
        new ForwardingOutboxStore(outbox) {
          @Override
          public BlockageSample sampleBlockage() {
            throw new UnsupportedOperationException("this store cannot sample its blockage");
          }
        };

    noBlockageSample.save(entry());

    assertThrows(UnsupportedOperationException.class, noBlockageSample::sampleBlockage);
    assertEquals(1L, noBlockageSample.countByStatus(OutboxStatus.PENDING));
  }

  @Test
  void aScheduleNamingAMethodTheStoreLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("markDeliverd");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingOutboxStore(new InMemoryOutboxStore(), misspelled));
  }
}

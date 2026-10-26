package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;

class ForwardingDeadLetterQueueTest {

  private static DeadLetterQueue.DeadLetterPublishRequest request() {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        "PlaceOrder",
        CommandId.of("command-1"),
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-1")),
        "java.lang.IllegalStateException",
        "failed",
        3,
        Instant.parse("2026-01-01T00:00:00Z"),
        null,
        null,
        null,
        null);
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden_theThrowingDefaultsIncluded() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        DeadLetterQueue.class, ForwardingDeadLetterQueue.class, 9);
  }

  @Test
  void everyMethodForwardsToTheSameMethodOfTheDelegate() {
    ForwardingDecoratorChecks.assertEveryMethodForwards(
        DeadLetterQueue.class, ForwardingDeadLetterQueue::new);
  }

  @Test
  void everyMethodConsultsTheScheduleUnderItsOwnName() {
    ForwardingDecoratorChecks.assertEveryMethodConsultsTheSchedule(
        DeadLetterQueue.class, ForwardingDeadLetterQueue::new);
  }

  @Test
  void failOnce_failsTheFirstPublishBeforeTheEntryIsWritten_andTheSecondSucceeds() {
    var queue = new InMemoryDeadLetterQueue();
    var forwarding = new ForwardingDeadLetterQueue(queue, FaultSchedule.none().failOnce("publish"));

    assertThrows(InjectedFaultException.class, () -> forwarding.publish(request()));
    assertTrue(queue.all().isEmpty(), "nothing was written");

    forwarding.publish(request());

    assertEquals(1, queue.all().size());
  }

  @Test
  void crashAfter_leavesTheEntryWritten_andThrows() {
    var queue = new InMemoryDeadLetterQueue();
    var forwarding =
        new ForwardingDeadLetterQueue(queue, FaultSchedule.none().crashAfter("publish"));

    assertThrows(InjectedFaultException.class, () -> forwarding.publish(request()));

    assertEquals(1, queue.all().size(), "the write happened");
    assertEquals(1L, forwarding.countPending());
  }

  @Test
  void aScheduleNamingAMethodTheQueueLacks_isRefusedWhenTheDecoratorIsBuilt() {
    var misspelled = FaultSchedule.none().failOnce("pubish");

    assertThrows(
        IllegalArgumentException.class,
        () -> new ForwardingDeadLetterQueue(new InMemoryDeadLetterQueue(), misspelled));
  }
}

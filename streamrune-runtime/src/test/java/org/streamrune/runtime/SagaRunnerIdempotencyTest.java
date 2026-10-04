package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Verifies that a redelivered correlated event (same {@link GlobalOffset}) must not produce
 * duplicate commands or spurious compensation. The deterministic idempotency key computed by {@link
 * SagaCommandDispatch} deduplicates at the command-inbox level so the second delivery
 * short-circuits without re-running the decider.
 */
class SagaRunnerIdempotencyTest {

  private static final AggregateType TYPE = AggregateType.of("inventory");

  // =========================================================================
  // Minimal domain — inventory reservation
  // =========================================================================

  /** A correlated event that triggers reservation. */
  record OrderConfirmed(String orderId) implements DomainEvent {}

  /** Command dispatched by the saga. */
  record ReserveStock(String itemId) implements Command {}

  /** Event produced when stock is reserved. */
  sealed interface StockEvent extends DomainEvent {
    record StockReserved(String itemId) implements StockEvent {}
  }

  record StockState(int reserved) implements AggregateState {}

  /** Decider that appends one StockReserved event per ReserveStock command. */
  static class StockDecider implements Decider<ReserveStock, StockState, StockEvent> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<StockEvent> decide(ReserveStock command, StockState state) {
      return List.of(new StockEvent.StockReserved(command.itemId()));
    }

    @Override
    public StockState evolve(StockState state, StockEvent event) {
      return new StockState(state.reserved() + 1);
    }
  }

  // =========================================================================
  // Saga state
  // =========================================================================

  record FulfillmentState(SagaStatus status, String orderId) implements SagaState {}

  // =========================================================================
  // Saga orchestrator — single-command variant
  // =========================================================================

  static class SingleCommandOrchestrator implements SagaOrchestrator<FulfillmentState> {

    @Override
    public Class<FulfillmentState> stateType() {
      return FulfillmentState.class;
    }

    @Override
    public FulfillmentState initialState(SagaId sagaId) {
      return new FulfillmentState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false; // correlated-only in these tests
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      throw new UnsupportedOperationException("start events not used");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (event.event() instanceof OrderConfirmed oc) {
        return Optional.of(SagaId.of("fulfill-" + oc.orderId()));
      }
      return Optional.empty();
    }

    @Override
    public FulfillmentState evolve(FulfillmentState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FulfillmentState state, EventEnvelope event) {
      if (event.event() instanceof OrderConfirmed oc) {
        return List.of(
            SagaCommand.of(new ReserveStock(oc.orderId()), AggregateId.of(oc.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillmentState state, Throwable failure, SagaCommand failedCmd) {
      return List.of(); // no compensation needed for this test
    }
  }

  // =========================================================================
  // Saga orchestrator — two-command variant
  // =========================================================================

  static class TwoCommandOrchestrator implements SagaOrchestrator<FulfillmentState> {

    @Override
    public Class<FulfillmentState> stateType() {
      return FulfillmentState.class;
    }

    @Override
    public FulfillmentState initialState(SagaId sagaId) {
      return new FulfillmentState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      throw new UnsupportedOperationException("start events not used");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (event.event() instanceof OrderConfirmed oc) {
        return Optional.of(SagaId.of("fulfill2-" + oc.orderId()));
      }
      return Optional.empty();
    }

    @Override
    public FulfillmentState evolve(FulfillmentState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FulfillmentState state, EventEnvelope event) {
      if (event.event() instanceof OrderConfirmed oc) {
        return List.of(
            SagaCommand.of(
                new ReserveStock(oc.orderId() + "-a"), AggregateId.of(oc.orderId() + "-a")),
            SagaCommand.of(
                new ReserveStock(oc.orderId() + "-b"), AggregateId.of(oc.orderId() + "-b")));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillmentState state, Throwable failure, SagaCommand failedCmd) {
      return List.of();
    }
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /** Builds a correlated EventEnvelope with the given stable globalOffset. */
  private EventEnvelope correlatedEnvelope(String orderId, long globalOffset) {
    OrderConfirmed event = new OrderConfirmed(orderId);
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("order-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("fulfill-" + orderId),
            null,
            null,
            Instant.now()));
  }

  /** Same as above but for the two-command orchestrator's correlation id. */
  private EventEnvelope correlatedEnvelope2(String orderId, long globalOffset) {
    OrderConfirmed event = new OrderConfirmed(orderId);
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("order-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("fulfill2-" + orderId),
            null,
            null,
            Instant.now()));
  }

  // =========================================================================
  // Tests
  // =========================================================================

  /**
   * Regression: deliver the same correlated event (same globalOffset) twice. The command must be
   * dispatched exactly once — the second delivery short-circuits via the inbox. The saga must NOT
   * enter compensation.
   */
  @Test
  void redeliveredCorrelatedEvent_commandDispatchedOnce_noSpuriousCompensation() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var sagaStore = new InMemorySagaStore();

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .register(
                TYPE, ReserveStock.class, cmd -> AggregateId.of(cmd.itemId()), new StockDecider())
            .build();

    var sagaId = SagaId.of("fulfill-order-99");
    sagaStore.create(
        sagaId,
        SagaType.fromClass(FulfillmentState.class),
        new FulfillmentState(SagaStatus.RUNNING, "order-99"),
        SagaStatus.RUNNING);

    var runner =
        SagaRunner.<FulfillmentState>builder()
            .orchestrator(new SingleCommandOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var listener = runner.asEventListener();

    // First delivery — globalOffset = 10
    var event = correlatedEnvelope("order-99", 10L);
    listener.onEvents(List.of(event));

    // Count events in the reservation stream after first delivery
    var streamId = StreamId.of(TYPE, AggregateId.of("order-99"));
    var afterFirst = store.readStream(streamId, Version.initial(), 100);
    assertEquals(
        1, afterFirst.size(), "first delivery must produce exactly one StockReserved event");

    // Second delivery — SAME globalOffset, simulating at-least-once redelivery
    listener.onEvents(List.of(event));

    var afterSecond = store.readStream(streamId, Version.initial(), 100);
    assertEquals(
        1,
        afterSecond.size(),
        "redelivered event must NOT produce a duplicate StockReserved event (idempotency key deduplicates)");

    // Saga must remain RUNNING — not spuriously compensated
    var loaded =
        sagaStore.load(sagaId, SagaType.fromClass(FulfillmentState.class), FulfillmentState.class);
    assertTrue(loaded.isPresent());
    assertEquals(
        SagaStatus.RUNNING,
        loaded.get().status(),
        "saga must remain RUNNING — redelivery must not trigger compensation");
  }

  /**
   * Multi-command step crash-simulation: a step produces two commands; the event is redelivered
   * (simulating a crash between the two commands). Both commands must be applied exactly once
   * across both deliveries.
   */
  @Test
  void twoCommandStep_redelivery_eachCommandAppliedOnce() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var sagaStore = new InMemorySagaStore();

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .register(
                TYPE, ReserveStock.class, cmd -> AggregateId.of(cmd.itemId()), new StockDecider())
            .build();

    var sagaId = SagaId.of("fulfill2-order-77");
    sagaStore.create(
        sagaId,
        SagaType.fromClass(FulfillmentState.class),
        new FulfillmentState(SagaStatus.RUNNING, "order-77"),
        SagaStatus.RUNNING);

    var runner =
        SagaRunner.<FulfillmentState>builder()
            .orchestrator(new TwoCommandOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var listener = runner.asEventListener();

    // First delivery — globalOffset = 20
    var event = correlatedEnvelope2("order-77", 20L);
    listener.onEvents(List.of(event));

    // After first delivery: two streams, each with one event
    var streamA = StreamId.of(TYPE, AggregateId.of("order-77-a"));
    var streamB = StreamId.of(TYPE, AggregateId.of("order-77-b"));
    assertEquals(
        1, store.readStream(streamA, Version.initial(), 100).size(), "stream A must have 1 event");
    assertEquals(
        1, store.readStream(streamB, Version.initial(), 100).size(), "stream B must have 1 event");

    // Redelivery — same globalOffset = 20
    listener.onEvents(List.of(event));

    assertEquals(
        1,
        store.readStream(streamA, Version.initial(), 100).size(),
        "stream A must still have exactly 1 event after redelivery (forward key index 0 deduplicates)");
    assertEquals(
        1,
        store.readStream(streamB, Version.initial(), 100).size(),
        "stream B must still have exactly 1 event after redelivery (forward key index 1 deduplicates)");

    // Saga must not be spuriously compensated
    var loaded =
        sagaStore.load(sagaId, SagaType.fromClass(FulfillmentState.class), FulfillmentState.class);
    assertTrue(loaded.isPresent());
    assertEquals(
        SagaStatus.RUNNING, loaded.get().status(), "saga must remain RUNNING after redelivery");
  }
}

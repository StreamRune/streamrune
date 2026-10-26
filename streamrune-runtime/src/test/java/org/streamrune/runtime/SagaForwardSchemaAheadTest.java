package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.LoadedSaga;
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
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A saga's forward command on a replica that is behind the stored schema, during the rolling
 * deployment of an event-schema change.
 *
 * <p>The next release ("release B") bumps {@code StockReserved} to schema version 2 with an
 * identity upcaster, the production guide's first procedure, and one of its replicas has stored a
 * version-2 {@code StockReserved} on the stock aggregate {@code S}. The replica under test still
 * runs the previous release ("release A"): its upcaster chain reaches version 1, so loading {@code
 * S} is refused with {@link EventSchemaAheadException}. The saga reacts to {@code OrderPlaced(O,
 * S)} by charging the payment of {@code O} and reserving stock on {@code S}, and undoes the charge
 * with a refund.
 *
 * <p>The refusal says nothing about the order: the same command succeeds on an upgraded replica. So
 * the step is left exactly as it was and the failure propagates for redelivery, on the start path
 * and on the correlated path alike, and the redelivery on the upgraded replica completes the step
 * under the same forward keys: the payment is charged once and never refunded. A command a saga
 * dispatches is never dead-lettered, so redelivery is its only recovery.
 */
class SagaForwardSchemaAheadTest {

  private static final AggregateType PAYMENT = AggregateType.of("payment");
  private static final AggregateType STOCK = AggregateType.of("stock");
  private static final AggregateType ORDER = AggregateType.of("order");

  @Test
  void
      startEvent_aForwardCommandBehindTheStoredSchema_holdsTheSagaAndTheUpgradedRedeliveryCompletesIt() {
    var f = new Fixture();
    f.releaseBStoresAVersionTwoStockReservedOn("S");
    f.replicaRunsReleaseA();
    var listener = f.runner(new OrderSaga()).asEventListener();
    EventEnvelope orderPlaced = f.storeOrderPlaced("O", "S");

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(orderPlaced)));

    assertThat(thrown)
        .as("the refusal propagates, so the subscription delivers the start event again")
        .isInstanceOf(EventSchemaAheadException.class);
    LoadedSaga<OrderSagaState> held = f.saga("O");
    assertThat(held.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(held.version())
        .as("the genesis-pending row, written before the first dispatch")
        .isEqualTo(1L);
    assertThat(held.genesisApplied()).isFalse();
    assertThat(held.episodeVersion()).as("no compensation episode is claimed").isNull();
    assertThat(f.eventNames(PAYMENT, "O"))
        .as("the payment is charged and not refunded")
        .containsExactly("PaymentCharged");
    assertThat(f.deadLetterQueue.all()).as("a saga's command is never dead-lettered").isEmpty();

    f.replicaRunsReleaseB();
    listener.onEvents(List.of(orderPlaced));

    LoadedSaga<OrderSagaState> completed = f.saga("O");
    assertThat(completed.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(completed.version()).isEqualTo(2L);
    assertThat(completed.genesisApplied()).isTrue();
    assertThat(completed.episodeVersion()).isNull();
    assertThat(f.eventNames(PAYMENT, "O"))
        .as("the redelivered charge is answered from the command inbox under its forward key")
        .containsExactly("PaymentCharged");
    assertThat(f.eventNames(STOCK, "S"))
        .as("the reservation of the order lands beside the event release B stored")
        .containsExactly("StockReserved", "StockReserved");
    assertThat(f.deadLetterQueue.all()).isEmpty();
  }

  @Test
  void
      correlatedEvent_aForwardCommandBehindTheStoredSchema_holdsTheSagaAndTheUpgradedRedeliveryCompletesIt() {
    var f = new Fixture();
    f.releaseBStoresAVersionTwoStockReservedOn("S");
    f.replicaRunsReleaseA();
    var listener = f.runner(new TwoStepOrderSaga()).asEventListener();
    listener.onEvents(List.of(f.storeOrderPlaced("O", "S")));
    LoadedSaga<OrderSagaState> afterTheStart = f.saga("O");
    assertThat(afterTheStart.status()).isEqualTo(SagaStatus.RUNNING);
    EventEnvelope paymentCharged = f.firstEventOf(PAYMENT, "O");
    assertThat(paymentCharged.metadata().sagaOrigin())
        .as("the reply of the saga's own command, routed to it by its origin")
        .isNotNull();

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(paymentCharged)));

    assertThat(thrown)
        .as("the refusal propagates, so the subscription delivers the reply again")
        .isInstanceOf(EventSchemaAheadException.class);
    LoadedSaga<OrderSagaState> held = f.saga("O");
    assertThat(held.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(held.version())
        .as("nothing is written for the failed delivery")
        .isEqualTo(afterTheStart.version());
    assertThat(held.episodeVersion()).as("no compensation episode is claimed").isNull();
    assertThat(f.eventNames(PAYMENT, "O"))
        .as("the payment is charged and not refunded")
        .containsExactly("PaymentCharged");
    assertThat(f.deadLetterQueue.all()).as("a saga's command is never dead-lettered").isEmpty();

    f.replicaRunsReleaseB();
    listener.onEvents(List.of(paymentCharged));

    LoadedSaga<OrderSagaState> completed = f.saga("O");
    assertThat(completed.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(completed.version()).isEqualTo(afterTheStart.version() + 1);
    assertThat(completed.lastAppliedOffset()).isEqualTo(paymentCharged.globalOffset().value());
    assertThat(completed.episodeVersion()).isNull();
    assertThat(f.eventNames(PAYMENT, "O")).containsExactly("PaymentCharged");
    assertThat(f.eventNames(STOCK, "S")).containsExactly("StockReserved", "StockReserved");
    assertThat(f.deadLetterQueue.all()).isEmpty();
  }

  // ---- domain ------------------------------------------------------------------------------

  record OrderPlaced(String orderId, String itemId) implements DomainEvent {}

  sealed interface PaymentCommand extends Command {
    record ChargePayment(String orderId) implements PaymentCommand {}

    record RefundPayment(String orderId) implements PaymentCommand {}
  }

  record PaymentCharged(String orderId) implements DomainEvent {}

  record PaymentRefunded(String orderId) implements DomainEvent {}

  record PaymentState(boolean charged) implements AggregateState {}

  static final class PaymentDecider implements Decider<PaymentCommand, PaymentState, DomainEvent> {
    @Override
    public PaymentState initialState() {
      return new PaymentState(false);
    }

    @Override
    public List<DomainEvent> decide(PaymentCommand command, PaymentState state) {
      return switch (command) {
        case PaymentCommand.ChargePayment c -> List.of(new PaymentCharged(c.orderId()));
        case PaymentCommand.RefundPayment r -> List.of(new PaymentRefunded(r.orderId()));
      };
    }

    @Override
    public PaymentState evolve(PaymentState state, DomainEvent event) {
      return new PaymentState(event instanceof PaymentCharged);
    }
  }

  record ReserveStock(String itemId, String orderId) implements Command {}

  record StockReserved(String itemId, String orderId) implements DomainEvent {}

  record StockState(int reserved) implements AggregateState {}

  static final class StockDecider implements Decider<ReserveStock, StockState, StockReserved> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<StockReserved> decide(ReserveStock command, StockState state) {
      return List.of(new StockReserved(command.itemId(), command.orderId()));
    }

    @Override
    public StockState evolve(StockState state, StockReserved event) {
      return new StockState(state.reserved() + 1);
    }
  }

  /** Release B's upcaster: version 2 changed nothing that needs rewriting. */
  static EventUpcaster stockReservedAtVersionTwo() {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return EventType.fromClass(StockReserved.class);
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return eventData;
      }
    };
  }

  // ---- sagas -------------------------------------------------------------------------------

  record OrderSagaState(SagaStatus status, String orderId, String itemId) implements SagaState {}

  /** {@code OrderPlaced} dispatches the charge and the reservation; the undo is the refund. */
  static class OrderSaga implements SagaOrchestrator<OrderSagaState> {
    @Override
    public Class<OrderSagaState> stateType() {
      return OrderSagaState.class;
    }

    @Override
    public OrderSagaState initialState(SagaId sagaId) {
      return new OrderSagaState(SagaStatus.STARTED, null, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderPlaced;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return sagaIdOf(((OrderPlaced) event.event()).orderId());
    }

    @Override
    public OrderSagaState evolve(OrderSagaState state, EventEnvelope event) {
      return event.event() instanceof OrderPlaced placed
          ? new OrderSagaState(SagaStatus.RUNNING, placed.orderId(), placed.itemId())
          : state;
    }

    @Override
    public List<SagaCommand> handle(OrderSagaState state, EventEnvelope event) {
      if (event.event() instanceof OrderPlaced placed) {
        return List.of(charge(placed.orderId()), reserve(placed.itemId(), placed.orderId()));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(
              new PaymentCommand.RefundPayment(state.orderId()), AggregateId.of(state.orderId())));
    }

    static SagaCommand charge(String orderId) {
      return SagaCommand.of(new PaymentCommand.ChargePayment(orderId), AggregateId.of(orderId));
    }

    static SagaCommand reserve(String itemId, String orderId) {
      return SagaCommand.of(new ReserveStock(itemId, orderId), AggregateId.of(itemId));
    }
  }

  /**
   * {@code OrderPlaced} dispatches the charge; the reply {@code PaymentCharged} dispatches the
   * reservation.
   */
  static final class TwoStepOrderSaga extends OrderSaga {
    @Override
    public List<SagaCommand> handle(OrderSagaState state, EventEnvelope event) {
      if (event.event() instanceof OrderPlaced placed) {
        return List.of(charge(placed.orderId()));
      }
      if (event.event() instanceof PaymentCharged) {
        return List.of(reserve(state.itemId(), state.orderId()));
      }
      return List.of();
    }
  }

  static SagaId sagaIdOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  // ---- wiring ------------------------------------------------------------------------------

  /** One event store and one command bus, switched between the two releases' upcaster chains. */
  static final class Fixture {
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemorySagaStore sagaStore = new InMemorySagaStore();
    final InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
    final InMemoryEventStore store =
        InMemoryEventStore.serializing(
                SimpleEventTypeRegistry.builder()
                    .registerEvent(OrderPlaced.class)
                    .registerEvent(PaymentCharged.class)
                    .registerEvent(PaymentRefunded.class)
                    .registerEvent(StockReserved.class)
                    .build(),
                null)
            .withCommandInbox(inbox)
            .withSagaStore(sagaStore);
    final VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .deadLetterQueue(deadLetterQueue)
            .objectMapper(new ObjectMapper())
            .register(
                PAYMENT,
                PaymentCommand.class,
                c ->
                    switch (c) {
                      case PaymentCommand.ChargePayment charge -> AggregateId.of(charge.orderId());
                      case PaymentCommand.RefundPayment refund -> AggregateId.of(refund.orderId());
                    },
                new PaymentDecider())
            .register(
                STOCK, ReserveStock.class, c -> AggregateId.of(c.itemId()), new StockDecider())
            .build();

    /** A replica of release B serves a client's reservation of another order on {@code itemId}. */
    void releaseBStoresAVersionTwoStockReservedOn(String itemId) {
      replicaRunsReleaseB();
      bus.execute(new ReserveStock(itemId, "another-order"));
    }

    /** Release A has no upcaster for {@code StockReserved}: its chain reaches version 1. */
    void replicaRunsReleaseA() {
      store.withUpcasters(null);
    }

    void replicaRunsReleaseB() {
      store.withUpcasters(List.of(stockReservedAtVersionTwo()));
    }

    SagaRunner<OrderSagaState> runner(SagaOrchestrator<OrderSagaState> saga) {
      return SagaRunner.<OrderSagaState>builder()
          .orchestrator(saga)
          .sagaStore(sagaStore)
          .commandBus(bus)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
          .build();
    }

    EventEnvelope storeOrderPlaced(String orderId, String itemId) {
      StreamId orderStream = StreamId.of(ORDER, AggregateId.of(orderId));
      store.append(
          orderStream,
          List.of(
              new EventEnvelope(
                  GlobalOffset.initial(),
                  orderStream,
                  Version.initial(),
                  EventType.fromClass(OrderPlaced.class),
                  new OrderPlaced(orderId, itemId),
                  new EventMetadata(
                      EventId.of("evt-order-" + orderId),
                      CommandId.of("cmd-order-" + orderId),
                      null,
                      null,
                      CorrelationId.of("client-" + orderId),
                      null,
                      null,
                      Instant.now()))),
          Version.initial());
      return store.readStream(orderStream, Version.initial(), 10).getFirst();
    }

    EventEnvelope firstEventOf(AggregateType type, String id) {
      return store
          .readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 10)
          .getFirst();
    }

    /**
     * The event type names of a stream, read through the chain the replica runs: the stock stream
     * holds a version-2 event, so it is read only once the replica runs release B.
     */
    List<String> eventNames(AggregateType type, String id) {
      return store
          .readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100)
          .stream()
          .map(e -> e.eventType().name())
          .toList();
    }

    LoadedSaga<OrderSagaState> saga(String orderId) {
      return sagaStore
          .load(sagaIdOf(orderId), SagaType.fromClass(OrderSagaState.class), OrderSagaState.class)
          .orElseThrow();
    }
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.SagaFixture;

/**
 * The saga of the saga guide's quick example, and its builder form, behave as the guide says — in
 * particular for an event whose data cannot name a saga.
 *
 * <p>The payload of an event is its writer's data. An order id of 245 characters is a valid
 * aggregate id, and {@code "order-saga-" + orderId} is longer than a {@link SagaId} may be. A
 * routing method that handed such an id to {@code SagaId.of} would throw, the throw would be
 * poison, and every such event a caller writes would become a null-saga dead-letter entry. The
 * example's {@code sagaOf} answers "no saga" instead: nothing is filed, loaded or written.
 *
 * <p>{@link OrderSaga} is the guide's class, statement for statement; the documentation ratchet in
 * {@code streamrune-core} keeps the guide's routing samples from building a saga id unchecked. The
 * tests drive it through {@link SagaFixture}, as the guide's test of the example does.
 */
class SagaGuideQuickExampleTest {

  record OrderPlaced(String orderId) implements DomainEvent {}

  record StockReserved(String orderId) implements DomainEvent {}

  record OrderCancelled(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record CancelOrder(String orderId) implements Command {}

  /** The quick example of the saga guide. */
  public static class OrderSaga implements SagaOrchestrator<OrderSaga.State> {

    public record State(SagaStatus status, String orderId) implements SagaState {}

    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      // An order whose id cannot be part of a saga id starts no saga
      return event.event() instanceof OrderPlaced e && sagaOf(e.orderId()).isPresent();
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      // From the event's own data, never from its metadata
      return sagaOf(((OrderPlaced) event.event()).orderId()).orElseThrow();
    }

    // StockReserved needs no routing: the saga's ReserveStock command caused it, so it carries
    // this saga's origin. OrderCancelled is written by the customer's request: route it by its
    // data.
    @Override
    public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
      return event instanceof OrderCancelled e ? sagaOf(e.orderId()) : Optional.empty();
    }

    // The order id is its writer's data. An id that no saga id can hold names no saga: answer
    // "no saga" for it. A throw here would quarantine every such event a caller writes.
    static Optional<SagaId> sagaOf(String orderId) {
      if (orderId == null || orderId.isBlank()) {
        return Optional.empty();
      }
      String id = "order-saga-" + orderId;
      return id.length() > SagaId.MAX_LENGTH ? Optional.empty() : Optional.of(SagaId.of(id));
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return switch (event.event()) {
        case OrderPlaced e -> new State(SagaStatus.RUNNING, e.orderId());
        // The order id comes from the saga's state: this event is the reply to its command
        case StockReserved e -> new State(SagaStatus.COMPLETED, state.orderId());
        case OrderCancelled e when e.orderId().equals(state.orderId()) ->
            new State(SagaStatus.FAILED, state.orderId());
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      if (event.event() instanceof OrderPlaced e) {
        return List.of(SagaCommand.of(new ReserveStock(e.orderId()), new AggregateId(e.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failed) {
      return List.of(
          SagaCommand.of(new CancelOrder(state.orderId()), new AggregateId(state.orderId())));
    }
  }

  /** The logic of {@link OrderSaga} without its routing: the guide's decider form. */
  static final class OrderSagaDecider implements SagaDecider<OrderSaga.State> {
    private final OrderSaga saga = new OrderSaga();

    @Override
    public Class<OrderSaga.State> stateType() {
      return OrderSaga.State.class;
    }

    @Override
    public OrderSaga.State initialState(SagaId sagaId) {
      return saga.initialState(sagaId);
    }

    @Override
    public OrderSaga.State evolve(OrderSaga.State state, EventEnvelope event) {
      return saga.evolve(state, event);
    }

    @Override
    public List<SagaCommand> handle(OrderSaga.State state, EventEnvelope event) {
      return saga.handle(state, event);
    }
  }

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final AggregateType STOCK = AggregateType.of("stock");

  /** 250 characters: an aggregate id holds it, {@code "order-saga-" + it} is 261 characters. */
  private static final String AN_ID_NO_SAGA_ID_CAN_HOLD = "x".repeat(250);

  /** The guide's first wiring: the orchestrator. */
  private static SagaFixture<OrderSaga.State> orchestratorFixture() {
    return SagaFixture.of(OrderSaga.State.class, builder -> builder.orchestrator(new OrderSaga()));
  }

  /** The guide's second wiring: the decider, with the routing on the builder. */
  private static SagaFixture<OrderSaga.State> deciderFixture() {
    return SagaFixture.of(
        OrderSaga.State.class,
        builder ->
            builder
                .decider(new OrderSagaDecider())
                // OrderSaga.sagaOf is the quick example's helper: the saga id of an order, or
                // nothing for an order id no saga id can hold. None of the three functions throws
                // on the event's data.
                .startWhen(
                    event ->
                        event.event() instanceof OrderPlaced e
                            && OrderSaga.sagaOf(e.orderId()).isPresent())
                .extractSagaId(
                    event ->
                        OrderSaga.sagaOf(((OrderPlaced) event.event()).orderId()).orElseThrow())
                // Optional. Events the saga caused are routed by their origin; this is only for
                // the others.
                .correlateByEventData(
                    (streamId, event) ->
                        event instanceof OrderCancelled e
                            ? OrderSaga.sagaOf(e.orderId())
                            : Optional.empty()));
  }

  /**
   * An event a request stored: no origin, and the given saga's id as its correlation id; the
   * fixture gives it its offset.
   */
  private static EventEnvelope storedByARequest(
      DomainEvent event, String streamOrderId, SagaId named) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        StreamId.of(ORDER, AggregateId.of(streamOrderId)),
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of(named.value()),
            null,
            null,
            Instant.now(),
            Map.of()));
  }

  /** The stream the reply to the saga of {@code orderId}'s command is stored on. */
  private static StreamId stockOf(String orderId) {
    return StreamId.of(STOCK, AggregateId.of(orderId));
  }

  private static SagaId sagaOf(String orderId) {
    return OrderSaga.sagaOf(orderId).orElseThrow();
  }

  private static LoadedSaga<OrderSaga.State> row(
      SagaFixture<OrderSaga.State> saga, String orderId) {
    return saga.saga(sagaOf(orderId)).orElseThrow();
  }

  private void theExampleRunsAsTheGuideSays(SagaFixture<OrderSaga.State> saga) {
    saga.when(storedByARequest(new OrderPlaced("1"), "1", sagaOf("9")))
        .expectCommands(new ReserveStock("1"));
    assertThat(row(saga, "1").state()).isEqualTo(new OrderSaga.State(SagaStatus.RUNNING, "1"));

    saga.replyingTo(sagaOf("1"), stockOf("1"), new StockReserved("1")).expectCompleted(sagaOf("1"));

    saga.when(storedByARequest(new OrderPlaced("2"), "2", sagaOf("9")))
        .when(storedByARequest(new OrderCancelled("2"), "2", sagaOf("9")))
        .expectStatus(sagaOf("2"), SagaStatus.FAILED);
    assertThat(saga.deadLetterStore().all()).isEmpty();
  }

  @Test
  void theOrchestrator_reservesOnOrderPlaced_completesOnItsReply_andEndsOnACancellation() {
    theExampleRunsAsTheGuideSays(orchestratorFixture());
  }

  @Test
  void theDeciderWithItsBuilderRouting_runsTheSameFlow() {
    theExampleRunsAsTheGuideSays(deciderFixture());
  }

  /**
   * A caller cancels orders of its own whose ids no saga id can hold, three times, each request
   * naming a running saga as its correlation id: no event is quarantined, no saga is loaded or
   * written, and the batch goes on.
   */
  private void aCancellationWithAnIdNoSagaIdCanHoldIsNoSagasEvent(
      SagaFixture<OrderSaga.State> saga) {
    saga.when(storedByARequest(new OrderPlaced("1"), "1", sagaOf("1")));
    LoadedSaga<OrderSaga.State> running = row(saga, "1");
    int storeCalls = saga.sagaStoreFaults().calls();
    List<EventEnvelope> cancellations = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      cancellations.add(
          storedByARequest(
              new OrderCancelled(AN_ID_NO_SAGA_ID_CAN_HOLD),
              AN_ID_NO_SAGA_ID_CAN_HOLD,
              sagaOf("1")));
    }
    assertThat(saga.runner().resolveTarget(cancellations.getFirst())).isEmpty();

    saga.given(cancellations.toArray(EventEnvelope[]::new));

    assertThat(saga.deadLetterStore().all())
        .as("no null-saga entry for the caller's events")
        .isEmpty();
    assertThat(saga.sagaStoreFaults().calls())
        .as("nothing loaded, nothing written")
        .isEqualTo(storeCalls);
    assertThat(row(saga, "1")).isEqualTo(running);

    // In one batch with the saga's own reply: the batch goes on past the caller's event.
    saga.given(
        storedByARequest(
            new OrderCancelled(AN_ID_NO_SAGA_ID_CAN_HOLD), AN_ID_NO_SAGA_ID_CAN_HOLD, sagaOf("1")),
        saga.reply(sagaOf("1"), stockOf("1"), new StockReserved("1")));

    assertThat(saga.deadLetterStore().all()).isEmpty();
    saga.expectCompleted(sagaOf("1"));
    assertThat(row(saga, "1").version()).isEqualTo(running.version() + 1);
  }

  @Test
  void theOrchestrator_answersNoSagaForACancellationWhoseIdNoSagaIdCanHold() {
    aCancellationWithAnIdNoSagaIdCanHoldIsNoSagasEvent(orchestratorFixture());
  }

  @Test
  void theBuilderRouting_answersNoSagaForACancellationWhoseIdNoSagaIdCanHold() {
    aCancellationWithAnIdNoSagaIdCanHoldIsNoSagasEvent(deciderFixture());
  }

  private void anOrderWithAnIdNoSagaIdCanHoldStartsNoSaga(SagaFixture<OrderSaga.State> saga) {
    var placed =
        storedByARequest(
            new OrderPlaced(AN_ID_NO_SAGA_ID_CAN_HOLD), AN_ID_NO_SAGA_ID_CAN_HOLD, sagaOf("1"));

    assertThat(saga.runner().resolveTarget(placed)).isEmpty();
    saga.when(placed).expectNoCommands();

    assertThat(saga.deadLetterStore().all()).isEmpty();
    assertThat(saga.sagaStoreFaults().calls()).as("nothing loaded, nothing written").isZero();
  }

  @Test
  void theOrchestrator_startsNoSagaForAnOrderWhoseIdNoSagaIdCanHold() {
    anOrderWithAnIdNoSagaIdCanHoldStartsNoSaga(orchestratorFixture());
  }

  @Test
  void theBuilderRouting_startsNoSagaForAnOrderWhoseIdNoSagaIdCanHold() {
    anOrderWithAnIdNoSagaIdCanHoldStartsNoSaga(deciderFixture());
  }

  @Test
  void sagaOf_answersNothingForABlankAMissingAndAnOverLongOrderId_andTheLongestIdThatFits() {
    assertThat(OrderSaga.sagaOf(null)).isEmpty();
    assertThat(OrderSaga.sagaOf(" ")).isEmpty();
    assertThat(OrderSaga.sagaOf(AN_ID_NO_SAGA_ID_CAN_HOLD)).isEmpty();
    String longestThatFits = "x".repeat(SagaId.MAX_LENGTH - "order-saga-".length());
    assertThat(OrderSaga.sagaOf(longestThatFits))
        .contains(SagaId.of("order-saga-" + longestThatFits));
    assertThat(OrderSaga.sagaOf(longestThatFits + "x")).isEmpty();
  }

  /**
   * What the check is there for: the same routing without it. Each of the caller's events is
   * quarantined as a null-saga entry, whose replay would throw again.
   */
  @Test
  void theSameRoutingWithoutTheCheck_filesANullSagaEntryForEveryEventTheCallerWrites() {
    var unchecked =
        SagaFixture.of(
            OrderSaga.State.class,
            builder ->
                builder.orchestrator(
                    new OrderSaga() {
                      @Override
                      public Optional<SagaId> correlateByEventData(
                          StreamId streamId, DomainEvent event) {
                        return event instanceof OrderCancelled e
                            ? Optional.of(SagaId.of("order-saga-" + e.orderId()))
                            : Optional.empty();
                      }
                    }));
    List<EventEnvelope> cancellations = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      cancellations.add(
          storedByARequest(
              new OrderCancelled(AN_ID_NO_SAGA_ID_CAN_HOLD),
              AN_ID_NO_SAGA_ID_CAN_HOLD,
              sagaOf("1")));
    }

    unchecked.given(cancellations.toArray(EventEnvelope[]::new));

    assertThat(unchecked.deadLetterStore().all())
        .hasSize(3)
        .allSatisfy(entry -> assertThat(entry.sagaId()).isNull());
  }
}

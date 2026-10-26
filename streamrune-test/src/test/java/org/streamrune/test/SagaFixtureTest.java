package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;

class SagaFixtureTest {

  record OrderPlaced(String orderId) implements DomainEvent {}

  record StockReserved(String orderId) implements DomainEvent {}

  record PaymentCaptured(String orderId) implements DomainEvent {}

  record OrderCancelled(String orderId) implements DomainEvent {}

  record Poison(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record CapturePayment(String orderId) implements Command {}

  record ReleaseStock(String orderId) implements Command {}

  record State(SagaStatus status, String orderId, boolean reserved) implements SagaState {}

  /**
   * Place, reserve, capture: OrderPlaced reserves the stock, its reply captures the payment, whose
   * reply completes the saga. A cancellation, a rejected command or the deadline releases the stock
   * that was reserved.
   */
  static final class OrderSaga implements SagaDecider<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, null, false);
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return switch (event.event()) {
        case OrderPlaced e -> new State(SagaStatus.RUNNING, e.orderId(), false);
        case StockReserved e -> new State(SagaStatus.RUNNING, state.orderId(), true);
        case PaymentCaptured e -> new State(SagaStatus.COMPLETED, state.orderId(), true);
        case OrderCancelled e -> new State(SagaStatus.COMPENSATING, state.orderId(), true);
        case Poison e -> throw new IllegalStateException("cannot evolve " + e);
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return switch (event.event()) {
        case OrderPlaced e ->
            List.of(SagaCommand.of(new ReserveStock(e.orderId()), new AggregateId(e.orderId())));
        case StockReserved e ->
            List.of(SagaCommand.of(new CapturePayment(e.orderId()), new AggregateId(e.orderId())));
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failed) {
      return List.of(
          SagaCommand.of(new ReleaseStock(state.orderId()), new AggregateId(state.orderId())));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(30));
    }
  }

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId ORDER_1 = StreamId.of(ORDER, AggregateId.of("1"));
  private static final SagaId SAGA_1 = SagaId.of("order-saga-1");

  private static SagaId sagaOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  private static String orderOf(DomainEvent event) {
    return switch (event) {
      case OrderPlaced e -> e.orderId();
      case OrderCancelled e -> e.orderId();
      case Poison e -> e.orderId();
      default -> null;
    };
  }

  private static SagaFixture<State> fixture() {
    var saga = new OrderSaga();
    return SagaFixture.of(
            State.class,
            builder ->
                builder
                    .decider(saga)
                    .startWhen(event -> event.event() instanceof OrderPlaced)
                    .extractSagaId(event -> sagaOf(((OrderPlaced) event.event()).orderId()))
                    .correlateByEventData(
                        (streamId, event) ->
                            event instanceof OrderCancelled || event instanceof Poison
                                ? Optional.of(sagaOf(orderOf(event)))
                                : Optional.empty()))
        .withTimeouts(builder -> builder.decider(saga));
  }

  @Test
  void when_deliversAStoredEvent_andTheSagaDispatchesItsCommand() {
    fixture()
        .when(ORDER_1, new OrderPlaced("1"))
        .expectCommands(new ReserveStock("1"))
        .expectState(SAGA_1, state -> state.status() == SagaStatus.RUNNING && !state.reserved())
        .expectStatus(SAGA_1, SagaStatus.RUNNING);
  }

  @Test
  void replyingTo_drivesTheContinuation_toTheEnd() {
    fixture()
        .when(ORDER_1, new OrderPlaced("1"))
        .replyingTo(SAGA_1, new StockReserved("1"))
        .expectCommands(new CapturePayment("1"))
        .expectState(SAGA_1, State::reserved)
        .replyingTo(
            SAGA_1,
            StreamId.of(AggregateType.of("payment"), AggregateId.of("1")),
            new PaymentCaptured("1"))
        .expectNoCommands()
        .expectCompleted(SAGA_1);
  }

  @Test
  void given_deliversItsEventsAsOneBatch_andTheNextStepExpectsOnlyItsOwnCommands() {
    var fixture = fixture();
    fixture.given(fixture.event(ORDER_1, new OrderPlaced("1")));

    fixture.expectCommands(new ReserveStock("1"));
    fixture.replyingTo(SAGA_1, new StockReserved("1")).expectCommands(new CapturePayment("1"));
    assertEquals(
        List.of(new ReserveStock("1"), new CapturePayment("1")),
        fixture.dispatched(),
        "dispatched() keeps every command");
  }

  @Test
  void aCancellationByARequest_opensTheEpisode_whichTheTimeoutRunnerDrivesToItsEnd() {
    fixture()
        .when(ORDER_1, new OrderPlaced("1"))
        .replyingTo(SAGA_1, new StockReserved("1"))
        .when(ORDER_1, new OrderCancelled("1"))
        .expectNoCommands()
        .expectCompensating(SAGA_1)
        .advance(Duration.ofMinutes(31))
        .runTimeouts()
        .expectCommands(new ReleaseStock("1"))
        .expectStatus(SAGA_1, SagaStatus.COMPENSATED);
  }

  @Test
  void aRejectedForwardCommand_compensates() {
    fixture()
        .failing(CapturePayment.class, () -> new DomainException("card declined"))
        .when(ORDER_1, new OrderPlaced("1"))
        .replyingTo(SAGA_1, new StockReserved("1"))
        .expectCommands(new CapturePayment("1"), new ReleaseStock("1"))
        .expectStatus(SAGA_1, SagaStatus.COMPENSATED);
  }

  @Test
  void aCompensationThatCannotBeDispatched_leavesTheSagaCompensating() {
    var fixture =
        fixture()
            .failing(CapturePayment.class, () -> new DomainException("card declined"))
            // Not a rejection: the framework retries it, so the episode stays open
            .failing(ReleaseStock.class, () -> new IllegalStateException("stock service down"))
            .when(ORDER_1, new OrderPlaced("1"));

    fixture
        .replyingTo(SAGA_1, new StockReserved("1"))
        .expectCommands(new CapturePayment("1"), new ReleaseStock("1"))
        .expectCompensating(SAGA_1);
  }

  @Test
  void advance_thenRunTimeouts_compensatesASagaPastItsDeadline() {
    fixture()
        .when(ORDER_1, new OrderPlaced("1"))
        .advance(Duration.ofMinutes(29))
        .runTimeouts()
        .expectNoCommands()
        .expectStatus(SAGA_1, SagaStatus.RUNNING)
        .advance(Duration.ofMinutes(2))
        .runTimeouts()
        .expectCommands(new ReleaseStock("1"))
        .expectStatus(SAGA_1, SagaStatus.COMPENSATED);
  }

  @Test
  void runTimeouts_withoutWithTimeouts_isRefusedNamingWhatIsMissing() {
    var fixture =
        SagaFixture.of(
            State.class,
            builder ->
                builder
                    .decider(new OrderSaga())
                    .startWhen(event -> event.event() instanceof OrderPlaced)
                    .extractSagaId(event -> SAGA_1));

    var refused = assertThrows(IllegalStateException.class, fixture::runTimeouts);
    assertTrue(refused.getMessage().contains("withTimeouts"), refused.getMessage());
  }

  @Test
  void aPoisonEvent_isDeadLettered() {
    fixture()
        .when(ORDER_1, new OrderPlaced("1"))
        .when(ORDER_1, new Poison("1"))
        .expectDeadLettered(SAGA_1);
  }

  @Test
  void redeliver_afterACrashBetweenTwoWrites_convergesOnTheSagaOfAnUncrashedRun() {
    var fixture = fixture();
    fixture.sagaStoreFaults().crashAfter("createGenesisPending");
    assertThrows(InjectedFaultException.class, () -> fixture.when(ORDER_1, new OrderPlaced("1")));
    fixture.expectNoCommands();
    assertFalse(fixture.saga(SAGA_1).orElseThrow().genesisApplied(), "the crash left the genesis");

    fixture
        .redeliver()
        .expectCommands(new ReserveStock("1"))
        .expectStatus(SAGA_1, SagaStatus.RUNNING);
    assertTrue(fixture.saga(SAGA_1).orElseThrow().genesisApplied());

    fixture.redeliver().expectNoCommands().expectStatus(SAGA_1, SagaStatus.RUNNING);
    assertEquals(GlobalOffset.of(1), fixture.delivered().getLast().globalOffset());
    assertEquals(3, fixture.delivered().size());
  }

  @Test
  void redeliver_beforeAnyDelivery_isRefused() {
    assertThrows(IllegalStateException.class, () -> fixture().redeliver());
  }

  @Test
  void eventsGetIncreasingOffsets_andAnEnvelopeAtAnOffsetOfItsOwnKeepsIt() {
    var fixture = fixture();
    EventEnvelope own =
        new EventEnvelope(
            GlobalOffset.of(40),
            ORDER_1,
            fixture.event(ORDER_1, new OrderPlaced("1")).version(),
            fixture.event(ORDER_1, new OrderPlaced("1")).eventType(),
            new OrderPlaced("1"),
            fixture.event(ORDER_1, new OrderPlaced("1")).metadata());

    fixture.given(own).when(ORDER_1, new OrderCancelled("1"));

    List<EventEnvelope> delivered = fixture.delivered();
    assertEquals(GlobalOffset.of(40), delivered.get(0).globalOffset());
    assertEquals(GlobalOffset.of(41), delivered.get(1).globalOffset());
  }

  @Test
  void anEventStoredByARequest_carriesNoOrigin_andTheFixtureClocksTimestamp() {
    var fixture = fixture();
    fixture.advance(Duration.ofSeconds(5));

    EventEnvelope event = fixture.event(ORDER_1, new OrderPlaced("1"));
    EventEnvelope reply = fixture.reply(SAGA_1, ORDER_1, new StockReserved("1"));

    assertEquals(null, event.metadata().sagaOrigin());
    assertEquals(SagaFixture.START.plusSeconds(5), event.metadata().timestamp());
    assertEquals(SAGA_1, reply.metadata().sagaOrigin().sagaId());
    assertEquals(GlobalOffset.initial(), event.globalOffset(), "a step gives it its offset");
  }

  @Test
  void theExpectations_failWithWhatTheyFound() {
    var fixture = fixture().when(ORDER_1, new OrderPlaced("1"));

    var commands =
        assertThrows(AssertionError.class, () -> fixture.expectCommands(new ReleaseStock("1")));
    assertTrue(commands.getMessage().contains("ReserveStock"), commands.getMessage());
    var none = assertThrows(AssertionError.class, fixture::expectNoCommands);
    assertTrue(none.getMessage().contains("ReserveStock"), none.getMessage());
    var state =
        assertThrows(AssertionError.class, () -> fixture.expectState(SAGA_1, State::reserved));
    assertTrue(state.getMessage().contains("RUNNING"), state.getMessage());
    var status = assertThrows(AssertionError.class, () -> fixture.expectCompleted(SAGA_1));
    assertTrue(status.getMessage().contains("RUNNING"), status.getMessage());
    var missing =
        assertThrows(
            AssertionError.class, () -> fixture.expectStatus(sagaOf("2"), SagaStatus.RUNNING));
    assertTrue(missing.getMessage().contains("order-saga-2"), missing.getMessage());
    var deadLettered = assertThrows(AssertionError.class, () -> fixture.expectDeadLettered(SAGA_1));
    assertTrue(deadLettered.getMessage().contains("order-saga-1"), deadLettered.getMessage());
  }

  @Test
  void theFixtureHandsOutItsParts() {
    var fixture = fixture().when(ORDER_1, new OrderPlaced("1"));

    assertNotNull(fixture.runner());
    assertTrue(fixture.saga(SAGA_1).isPresent());
    assertFalse(fixture.saga(sagaOf("2")).isPresent());
    assertTrue(fixture.sagaStore().all().contains(SAGA_1));
    assertTrue(fixture.deadLetterStore().all().isEmpty());
    assertEquals(SagaFixture.START, fixture.clock().instant());
    assertTrue(fixture.sagaStoreFaults().calls() > 0, "the runner wrote through the schedule");
  }

  @Test
  void theBusRefusesAnUnkeyedDispatch_asASagaNeverMakesOne() {
    var fixture = fixture();

    var refused =
        assertThrows(
            UnsupportedOperationException.class,
            () -> fixture.commandBus().execute(new ReserveStock("1")));
    assertTrue(refused.getMessage().contains("idempotency key"), refused.getMessage());
  }
}

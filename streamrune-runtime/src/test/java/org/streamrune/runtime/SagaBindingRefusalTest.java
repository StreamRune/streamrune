package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * A saga command that the command bus refuses for the bindings it reads, not for anything about the
 * business: a {@link ReservedCorrelationIdException} under the saga's own dispatch.
 *
 * <p>Saga dispatch binds the saga's reserved correlation id and the saga's origin together, in one
 * carrier ({@link SagaCommandDispatch#dispatch}), and the bus refuses a reserved correlation id
 * that is not the bound origin's. So a refusal of a saga's own command says that the bindings the
 * bus read are not the ones the dispatch made: an invariant of the framework failed on that
 * dispatch. The command was never looked up, authorized, executed or stored, and nothing about the
 * order was decided. Here a bus in front of the real {@link VirtualThreadCommandBus} binds another
 * saga's context over the dispatch's own for the commands a test names, and the real bus refuses
 * the command for it. A bus that reads another saga's context and cause as a pair passes that
 * check; the saga dispatch also hands its saga to the bus beside the scoped values, and the bus
 * refuses a dispatch whose bindings name another saga ({@link SagaBindingsMismatchException}),
 * which the step treats the same way.
 *
 * <p>The refusal itself never compensates. A forward command refused this way leaves the saga as it
 * was and propagates, so the subscription delivers the event again: no compensation episode is
 * claimed, the order is not cancelled; a refusal that persists ends as the saga's timeout. A
 * compensation command refused this way keeps the episode {@code COMPENSATING} as an attempted
 * re-drive: the undo is never abandoned as {@code FAILED}, the next re-drive completes it, and a
 * refusal that persists is faulted for an operator once the compensation retry sweeper's give-up
 * horizon has passed. Either way it is logged at {@code ERROR}.
 *
 * <p><b>Crash points of the refused forward step, and why every rerun converges.</b> The step
 * writes in this order: the genesis-pending row (start event only), forward command 0, ..., forward
 * command n-1, the event write. A refusal of command k propagates before anything of k is stored,
 * so the durable state it leaves is the state of a crash right after command k-1:
 *
 * <ul>
 *   <li>start event, k = 0: the genesis-pending row at version 1 and nothing else. The redelivery
 *       re-evolves from the initial state, dispatches every command and writes the event ({@link
 *       #startEvent_theGenesisCommandRefused_claimsNoEpisode_andTheRedeliveryCompletesTheStep});
 *   <li>start event, k &gt; 0: the row and commands 0..k-1 stored under their forward keys. The
 *       redelivery presents the same keys, so the command inbox answers 0..k-1 and only k and the
 *       rest run ({@link
 *       #startEvent_aLaterCommandRefused_theRedeliveryAnswersTheEarlierOneFromTheInbox});
 *   <li>correlated event: nothing written for the delivery; the row stays at its version and the
 *       redelivery runs the step ({@link
 *       #correlatedEvent_aForwardCommandRefused_claimsNoEpisode_andTheRedeliveryCompletesTheStep});
 *   <li>a crash after the refusal and before the subscription retries: the same states, with the
 *       same redelivery.
 * </ul>
 *
 * <p>The keys are a function of the saga id, the offset of the triggering event and the command's
 * index, so no attempt can store a forward command twice, and the flow never reaches the
 * compensation path on a refusal.
 *
 * <p><b>Crash points of the refused compensation command.</b> The episode is stamped by the write
 * that claimed it, before the first compensation command; a refusal of compensation command k
 * leaves the stamped {@code COMPENSATING} row and compensation commands 0..k-1 stored under the
 * episode's keys, which is the state of a crash after command k-1. Every resume path (a delivery,
 * the timeout runner, the sweeper, an operator) reads the same stamp and presents the same keys, so
 * the commands already stored answer from the inbox and only k and the rest run ({@link
 * #compensationCommandRefused_keepsTheEpisodeCompensating_andTheNextReDriveCompletesTheUndo}); the
 * terminal write is a compare-and-swap at the episode's version, so of two resumes one writes it.
 */
class SagaBindingRefusalTest {

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final AggregateType PAYMENT = AggregateType.of("payment");
  private static final AggregateType STOCK = AggregateType.of("stock");

  private final ListAppender<ILoggingEvent> executorLog = new ListAppender<>();
  private Logger executorLogger;

  @BeforeEach
  void captureTheExecutorsLog() {
    executorLogger = (Logger) LoggerFactory.getLogger(SagaStepExecutor.class);
    executorLog.start();
    executorLogger.addAppender(executorLog);
  }

  @AfterEach
  void releaseTheExecutorsLog() {
    executorLogger.detachAppender(executorLog);
    executorLog.stop();
  }

  @Test
  void startEvent_theGenesisCommandRefused_claimsNoEpisode_andTheRedeliveryCompletesTheStep() {
    var f = new Fixture();
    var listener = f.runner().asEventListener();
    EventEnvelope orderPlaced = f.storeOrderPlaced("o-1");
    f.bus.refuseTheNext(InitiatePayment.class, 1);

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(orderPlaced)));

    assertThat(thrown)
        .as("the refusal propagates, so the subscription delivers the start event again")
        .isInstanceOf(ReservedCorrelationIdException.class);
    LoadedSaga<OrderSagaState> held = f.saga("o-1");
    assertThat(held.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(held.version())
        .as("the genesis-pending row, written before the first dispatch")
        .isEqualTo(1L);
    assertThat(held.genesisApplied()).isFalse();
    assertThat(held.episodeVersion()).as("no compensation episode is claimed").isNull();
    assertThat(f.eventNames(PAYMENT, "o-1")).as("the refused command stored nothing").isEmpty();
    assertThat(f.eventNames(ORDER, "o-1"))
        .as("the order is not cancelled")
        .containsExactly("OrderPlaced");
    assertThat(f.sagaDeadLetters.findBySaga(sagaIdOf("o-1")))
        .as("nothing is quarantined")
        .isEmpty();
    assertThat(f.deadLetterQueue.all()).as("a saga's command is never dead-lettered").isEmpty();
    assertThat(errorLines())
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("order-saga-o-1")
                    .contains("InitiatePayment")
                    .contains("index 0"));
    assertThat(warnLines()).noneMatch(line -> line.contains("claiming the compensation episode"));

    listener.onEvents(List.of(orderPlaced));

    LoadedSaga<OrderSagaState> completed = f.saga("o-1");
    assertThat(completed.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(completed.version()).isEqualTo(2L);
    assertThat(completed.genesisApplied()).isTrue();
    assertThat(completed.episodeVersion()).isNull();
    assertThat(f.eventNames(PAYMENT, "o-1")).containsExactly("PaymentInitiated");
    assertThat(f.eventNames(STOCK, "o-1")).containsExactly("StockReserved");
    assertThat(f.eventNames(ORDER, "o-1")).containsExactly("OrderPlaced");
    assertThat(f.firstEventOf(PAYMENT, "o-1").metadata().sagaOrigin())
        .as("the redelivered command carries its own saga's origin")
        .isEqualTo(f.originOf("o-1"));
    assertThat(f.firstEventOf(PAYMENT, "o-1").metadata().correlationId())
        .isEqualTo(f.originOf("o-1").correlationId());
  }

  @Test
  void startEvent_anotherSagasContextAndCauseReadAsAPair_isRefusedBeforeAnythingIsStored() {
    var f = new Fixture();
    var listener = f.runner().asEventListener();
    EventEnvelope orderPlaced = f.storeOrderPlaced("o-4");
    f.bus.pairTheNext(InitiatePayment.class, 1);

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(orderPlaced)));

    assertThat(thrown)
        .as("the bus compares what it read with the saga the dispatch handed it, and refuses")
        .isInstanceOf(SagaBindingsMismatchException.class);
    assertThat(f.eventNames(PAYMENT, "o-4")).as("nothing is stored for the command").isEmpty();
    assertThat(f.eventsCausedBy(sagaIdOf("another-order")))
        .as("nothing is stored under the other saga's origin")
        .isEmpty();
    LoadedSaga<OrderSagaState> held = f.saga("o-4");
    assertThat(held.version()).isEqualTo(1L);
    assertThat(held.genesisApplied()).isFalse();
    assertThat(held.episodeVersion()).as("no compensation episode is claimed").isNull();
    assertThat(f.eventNames(ORDER, "o-4")).containsExactly("OrderPlaced");
    assertThat(errorLines())
        .singleElement()
        .satisfies(line -> assertThat(line).contains("order-saga-o-4").contains("InitiatePayment"));

    listener.onEvents(List.of(orderPlaced));

    assertThat(f.eventNames(PAYMENT, "o-4")).containsExactly("PaymentInitiated");
    assertThat(f.firstEventOf(PAYMENT, "o-4").metadata().sagaOrigin()).isEqualTo(f.originOf("o-4"));
    assertThat(f.saga("o-4").genesisApplied()).isTrue();
  }

  @Test
  void theSagaHandedToTheBus_isTakenByItsOwnDispatch_andLeftToNoLaterOne() {
    var f = new Fixture();
    var listener = f.runner().asEventListener();

    listener.onEvents(List.of(f.storeOrderPlaced("o-5")));

    assertThat(SagaDispatchOffer.pending()).as("nothing is left on the thread").isFalse();
    assertThat(f.firstEventOf(PAYMENT, "o-5").metadata().sagaOrigin()).isEqualTo(f.originOf("o-5"));
    // A keyed command the thread dispatches later, outside any saga's dispatch, is not compared.
    assertThat(f.bus.execute(new InitiatePayment("o-6"), IdempotencyKey.of("client-key-o-6")))
        .isNotNull();
    assertThat(f.eventNames(PAYMENT, "o-6")).containsExactly("PaymentInitiated");
  }

  @Test
  void startEvent_aLaterCommandRefused_theRedeliveryAnswersTheEarlierOneFromTheInbox() {
    var f = new Fixture();
    var listener = f.runner().asEventListener();
    EventEnvelope orderPlaced = f.storeOrderPlaced("o-2");
    f.bus.refuseTheNext(ReserveStock.class, 1);

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(orderPlaced)));

    assertThat(thrown).isInstanceOf(ReservedCorrelationIdException.class);
    LoadedSaga<OrderSagaState> held = f.saga("o-2");
    assertThat(held.version()).isEqualTo(1L);
    assertThat(held.genesisApplied()).isFalse();
    assertThat(held.episodeVersion()).isNull();
    assertThat(f.eventNames(PAYMENT, "o-2"))
        .as("command 0 was stored before command 1 was refused")
        .containsExactly("PaymentInitiated");
    assertThat(f.eventNames(STOCK, "o-2")).isEmpty();
    assertThat(f.eventNames(ORDER, "o-2")).containsExactly("OrderPlaced");

    listener.onEvents(List.of(orderPlaced));

    assertThat(f.eventNames(PAYMENT, "o-2"))
        .as("the inbox answers command 0 under its forward key: the payment is initiated once")
        .containsExactly("PaymentInitiated");
    assertThat(f.eventNames(STOCK, "o-2")).containsExactly("StockReserved");
    LoadedSaga<OrderSagaState> completed = f.saga("o-2");
    assertThat(completed.version()).isEqualTo(2L);
    assertThat(completed.genesisApplied()).isTrue();
    assertThat(completed.episodeVersion()).isNull();
  }

  @Test
  void correlatedEvent_aForwardCommandRefused_claimsNoEpisode_andTheRedeliveryCompletesTheStep() {
    var f = new Fixture();
    var listener = f.runner().asEventListener();
    listener.onEvents(List.of(f.storeOrderPlaced("o-3")));
    LoadedSaga<OrderSagaState> afterTheStart = f.saga("o-3");
    EventEnvelope paymentInitiated = f.firstEventOf(PAYMENT, "o-3");
    f.bus.refuseTheNext(ConfirmOrder.class, 1);

    Throwable thrown = catchThrowable(() -> listener.onEvents(List.of(paymentInitiated)));

    assertThat(thrown).isInstanceOf(ReservedCorrelationIdException.class);
    LoadedSaga<OrderSagaState> held = f.saga("o-3");
    assertThat(held.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(held.version())
        .as("nothing is written for the refused delivery")
        .isEqualTo(afterTheStart.version());
    assertThat(held.episodeVersion()).as("no compensation episode is claimed").isNull();
    assertThat(f.eventNames(ORDER, "o-3"))
        .as("the order is neither confirmed nor cancelled")
        .containsExactly("OrderPlaced");

    listener.onEvents(List.of(paymentInitiated));

    LoadedSaga<OrderSagaState> completed = f.saga("o-3");
    assertThat(completed.version()).isEqualTo(afterTheStart.version() + 1);
    assertThat(completed.lastAppliedOffset()).isEqualTo(paymentInitiated.globalOffset().value());
    assertThat(completed.episodeVersion()).isNull();
    assertThat(f.eventNames(ORDER, "o-3")).containsExactly("OrderPlaced", "OrderConfirmed");
  }

  @Test
  void compensationCommandRefused_keepsTheEpisodeCompensating_andTheNextReDriveCompletesTheUndo() {
    var f = new Fixture();
    var runner = f.runner();
    var sweeper = f.sweeper(runner);
    f.bus.refuseTheNext(CancelOrder.class, 1);

    // The stock is out: the reservation is rejected by the domain, which opens the episode.
    runner.asEventListener().onEvents(List.of(f.storeOrderPlaced("doomed-1")));

    LoadedSaga<OrderSagaState> stuck = f.saga("doomed-1");
    assertThat(stuck.status())
        .as("the refused undo is not abandoned: the episode stays open")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(stuck.episodeVersion()).isNotNull();
    assertThat(f.eventNames(ORDER, "doomed-1")).containsExactly("OrderPlaced");
    assertThat(errorLines())
        .anySatisfy(line -> assertThat(line).contains("doomed-1").contains("CancelOrder"));

    f.clock.advance(Duration.ofSeconds(31));
    sweeper.sweepOnce();

    LoadedSaga<OrderSagaState> compensated = f.saga("doomed-1");
    assertThat(compensated.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(f.eventNames(ORDER, "doomed-1"))
        .as("the re-drive under the episode's key stores the cancellation once")
        .containsExactly("OrderPlaced", "OrderCancelled");
    assertThat(f.firstEventOf(PAYMENT, "doomed-1").metadata().sagaOrigin())
        .isEqualTo(f.originOf("doomed-1"));
  }

  @Test
  void aCompensationCommandRefusedOnEveryReDrive_isFaultedForAnOperator_neverFailed() {
    var f = new Fixture();
    var runner = f.runner();
    var sweeper = f.sweeper(runner);
    f.bus.refuseTheNext(CancelOrder.class, Integer.MAX_VALUE);
    runner.asEventListener().onEvents(List.of(f.storeOrderPlaced("doomed-2")));
    assertThat(f.saga("doomed-2").status()).isEqualTo(SagaStatus.COMPENSATING);

    f.clock.advance(Duration.ofSeconds(31));
    sweeper.sweepOnce();
    assertThat(f.saga("doomed-2").status())
        .as("a refused re-drive is an attempt that keeps the episode open")
        .isEqualTo(SagaStatus.COMPENSATING);

    f.clock.advance(Duration.ofMinutes(61));
    sweeper.sweepOnce();

    LoadedSaga<OrderSagaState> faulted = f.saga("doomed-2");
    assertThat(faulted.status())
        .as("past the give-up horizon the sweeper faults the episode for an operator")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.effectiveStatus())
        .as("an operator's resume continues the episode under its stamp")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(f.eventNames(ORDER, "doomed-2")).containsExactly("OrderPlaced");
  }

  private List<String> errorLines() {
    return executorLog.list.stream()
        .filter(event -> event.getLevel() == Level.ERROR)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  private List<String> warnLines() {
    return executorLog.list.stream()
        .filter(event -> event.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  // ---- domain ------------------------------------------------------------------------------

  record OrderPlaced(String orderId) implements DomainEvent {}

  record OrderConfirmed(String orderId) implements DomainEvent {}

  record OrderCancelled(String orderId) implements DomainEvent {}

  sealed interface OrderCommand extends Command {}

  record ConfirmOrder(String orderId) implements OrderCommand {}

  record CancelOrder(String orderId) implements OrderCommand {}

  record OrderAggregate(boolean cancelled) implements AggregateState {}

  static final class OrderDecider implements Decider<OrderCommand, OrderAggregate, DomainEvent> {
    @Override
    public OrderAggregate initialState() {
      return new OrderAggregate(false);
    }

    @Override
    public List<DomainEvent> decide(OrderCommand command, OrderAggregate state) {
      return switch (command) {
        case ConfirmOrder c -> List.of(new OrderConfirmed(c.orderId()));
        case CancelOrder c -> List.of(new OrderCancelled(c.orderId()));
      };
    }

    @Override
    public OrderAggregate evolve(OrderAggregate state, DomainEvent event) {
      return new OrderAggregate(state.cancelled() || event instanceof OrderCancelled);
    }
  }

  record InitiatePayment(String orderId) implements Command {}

  record PaymentInitiated(String orderId) implements DomainEvent {}

  record PaymentAggregate(boolean initiated) implements AggregateState {}

  static final class PaymentDecider
      implements Decider<InitiatePayment, PaymentAggregate, PaymentInitiated> {
    @Override
    public PaymentAggregate initialState() {
      return new PaymentAggregate(false);
    }

    @Override
    public List<PaymentInitiated> decide(InitiatePayment command, PaymentAggregate state) {
      return List.of(new PaymentInitiated(command.orderId()));
    }

    @Override
    public PaymentAggregate evolve(PaymentAggregate state, PaymentInitiated event) {
      return new PaymentAggregate(true);
    }
  }

  record ReserveStock(String orderId) implements Command {}

  record StockReserved(String orderId) implements DomainEvent {}

  record StockAggregate(int reserved) implements AggregateState {}

  /** Rejects the reservation of an order whose id starts with {@code doomed}: the stock is out. */
  static final class StockDecider implements Decider<ReserveStock, StockAggregate, StockReserved> {
    @Override
    public StockAggregate initialState() {
      return new StockAggregate(0);
    }

    @Override
    public List<StockReserved> decide(ReserveStock command, StockAggregate state) {
      if (command.orderId().startsWith("doomed")) {
        throw new DomainException("Insufficient stock for " + command.orderId());
      }
      return List.of(new StockReserved(command.orderId()));
    }

    @Override
    public StockAggregate evolve(StockAggregate state, StockReserved event) {
      return new StockAggregate(state.reserved() + 1);
    }
  }

  // ---- the saga ----------------------------------------------------------------------------

  record OrderSagaState(SagaStatus status, String orderId) implements SagaState {}

  /**
   * {@code OrderPlaced} initiates the payment and reserves the stock; the reply {@code
   * PaymentInitiated} confirms the order; the undo cancels it.
   */
  static final class FulfilmentSaga implements SagaOrchestrator<OrderSagaState> {
    @Override
    public Class<OrderSagaState> stateType() {
      return OrderSagaState.class;
    }

    @Override
    public OrderSagaState initialState(SagaId sagaId) {
      return new OrderSagaState(SagaStatus.STARTED, null);
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
          ? new OrderSagaState(SagaStatus.RUNNING, placed.orderId())
          : state;
    }

    @Override
    public List<SagaCommand> handle(OrderSagaState state, EventEnvelope event) {
      return switch (event.event()) {
        case OrderPlaced placed ->
            List.of(
                SagaCommand.of(
                    new InitiatePayment(placed.orderId()), AggregateId.of(placed.orderId())),
                SagaCommand.of(
                    new ReserveStock(placed.orderId()), AggregateId.of(placed.orderId())));
        case PaymentInitiated initiated ->
            List.of(
                SagaCommand.of(
                    new ConfirmOrder(initiated.orderId()), AggregateId.of(initiated.orderId())));
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(
        OrderSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new CancelOrder(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  static SagaId sagaIdOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  // ---- wiring ------------------------------------------------------------------------------

  /**
   * In front of the real bus, on the calling thread: for the next {@code n} commands of a type,
   * binds the context of another saga over the one the saga dispatch bound, and leaves the
   * dispatch's cause as it is ({@link #refuseTheNext}), or binds the context and the cause of
   * another saga as a pair ({@link #pairTheNext}). The real bus then reads a reserved correlation
   * id that is not the bound origin's, or bindings that name another saga than the one the saga
   * dispatch handed it, and refuses the command.
   */
  static final class AnotherSagasContextInFront implements CommandBus {
    private final VirtualThreadCommandBus delegate;
    private final Map<Class<?>, AtomicInteger> refusals = new ConcurrentHashMap<>();
    private final Map<Class<?>, AtomicInteger> pairs = new ConcurrentHashMap<>();

    AnotherSagasContextInFront(VirtualThreadCommandBus delegate) {
      this.delegate = delegate;
    }

    void refuseTheNext(Class<? extends Command> type, int n) {
      refusals.put(type, new AtomicInteger(n));
    }

    void pairTheNext(Class<? extends Command> type, int n) {
      pairs.put(type, new AtomicInteger(n));
    }

    private static boolean takeOne(Map<Class<?>, AtomicInteger> counters, Command command) {
      AtomicInteger remaining = counters.get(command.getClass());
      return remaining != null && remaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      SagaOrigin another =
          SagaOrigin.dispatchedBy(
              SagaType.fromClass(OrderSagaState.class), sagaIdOf("another-order"));
      StreamRuneContext.RequestContext anotherSagas =
          new StreamRuneContext.RequestContext(
              null, null, another.correlationId(), Instant.now(), Map.of());
      if (takeOne(refusals, command)) {
        return ScopedValue.where(StreamRuneContext.CURRENT, anotherSagas)
            .call(() -> delegate.execute(command, key));
      }
      if (takeOne(pairs, command)) {
        return ScopedValue.where(StreamRuneContext.CURRENT, anotherSagas)
            .where(
                StreamRuneContext.CAUSE,
                new StreamRuneContext.DispatchCause(CausationId.of("evt-another"), another))
            .call(() -> delegate.execute(command, key));
      }
      return delegate.execute(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }

    @Override
    public Object sagaFenceStorageIdentity() {
      return delegate.sagaFenceStorageIdentity();
    }
  }

  static final class Fixture {
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemorySagaStore sagaStore = new InMemorySagaStore(clock);
    final InMemorySagaDeadLetterStore sagaDeadLetters = new InMemorySagaDeadLetterStore(sagaStore);
    final InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
    final InMemoryEventStore store =
        new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(sagaStore);
    final AnotherSagasContextInFront bus =
        new AnotherSagasContextInFront(
            VirtualThreadCommandBus.builder()
                .eventStore(store)
                .commandInbox(inbox)
                .deadLetterQueue(deadLetterQueue)
                .objectMapper(new ObjectMapper())
                .register(
                    ORDER,
                    OrderCommand.class,
                    c ->
                        switch (c) {
                          case ConfirmOrder confirm -> AggregateId.of(confirm.orderId());
                          case CancelOrder cancel -> AggregateId.of(cancel.orderId());
                        },
                    new OrderDecider())
                .register(
                    PAYMENT,
                    InitiatePayment.class,
                    c -> AggregateId.of(c.orderId()),
                    new PaymentDecider())
                .register(
                    STOCK, ReserveStock.class, c -> AggregateId.of(c.orderId()), new StockDecider())
                .build());

    SagaRunner<OrderSagaState> runner() {
      return SagaRunner.<OrderSagaState>builder()
          .orchestrator(new FulfilmentSaga())
          .sagaStore(sagaStore)
          .commandBus(bus)
          .sagaDeadLetterStore(sagaDeadLetters)
          .build();
    }

    SagaCompensationRetrySweeper<OrderSagaState> sweeper(SagaRunner<OrderSagaState> runner) {
      return SagaCompensationRetrySweeper.<OrderSagaState>builder()
          .sagaRunner(runner)
          .sagaStore(sagaStore)
          .leadership(SubscriptionLeadership.NOOP)
          .retryInterval(Duration.ofSeconds(30))
          .giveUpAfter(Duration.ofHours(1))
          .clock(clock)
          .build();
    }

    SagaOrigin originOf(String orderId) {
      return SagaOrigin.dispatchedBy(SagaType.fromClass(OrderSagaState.class), sagaIdOf(orderId));
    }

    EventEnvelope storeOrderPlaced(String orderId) {
      StreamId orderStream = StreamId.of(ORDER, AggregateId.of(orderId));
      store.append(
          orderStream,
          List.of(
              new EventEnvelope(
                  GlobalOffset.initial(),
                  orderStream,
                  Version.initial(),
                  EventType.fromClass(OrderPlaced.class),
                  new OrderPlaced(orderId),
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

    List<String> eventNames(AggregateType type, String id) {
      return store
          .readStream(StreamId.of(type, AggregateId.of(id)), Version.initial(), 100)
          .stream()
          .map(e -> e.eventType().name())
          .toList();
    }

    /** Every stored event whose saga origin names {@code sagaId}. */
    List<EventEnvelope> eventsCausedBy(SagaId sagaId) {
      return store.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .filter(
              e ->
                  e.metadata().sagaOrigin() != null
                      && e.metadata().sagaOrigin().sagaId().equals(sagaId))
          .toList();
    }

    LoadedSaga<OrderSagaState> saga(String orderId) {
      return sagaStore
          .load(sagaIdOf(orderId), SagaType.fromClass(OrderSagaState.class), OrderSagaState.class)
          .orElseThrow();
    }
  }
}

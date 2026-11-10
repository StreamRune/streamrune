package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
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
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * One saga runner and its compensation retry sweeper on one command bus, each on a virtual thread
 * of its own, the way a saga leader runs them: the runner's thread delivers the start events of new
 * orders while the sweeper's thread re-drives the compensations of orders whose refund keeps
 * failing, and request threads dispatch commands of their own through the bus's
 * unbound-request-context source, as the routes of an application do.
 *
 * <p>Each dispatch binds the context, the cause and the saga markers of its own saga, and every
 * thread blocks inside commands (the aggregate lock, the inbox, the store) all the time. Every
 * forward command must be admitted and stored under the origin and the correlation id of its own
 * saga, and no healthy order may be compensated. {@code streamrune.test.sagaConcurrencySeconds}
 * lengthens the run.
 */
class SagaDispatchConcurrencyTest {

  private static final AggregateType PAYMENT = AggregateType.of("payment");
  private static final AggregateType ACCOUNT = AggregateType.of("account");

  @Test
  void aSagaLeaderUnderLoad_storesEveryCommandUnderItsOwnSaga_andCompensatesNoHealthyOrder()
      throws Exception {
    Duration run = Duration.ofSeconds(Long.getLong("streamrune.test.sagaConcurrencySeconds", 3L));
    var inbox = new InMemoryCommandInbox();
    var sagaStore = new InMemorySagaStore();
    var store = new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(sagaStore);
    var refundsFail = new AtomicBoolean(true);
    ThreadLocal<StreamRuneContext.RequestContext> request = new ThreadLocal<>();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .unboundRequestContext(request::get)
            .register(PAYMENT, Charge.class, c -> AggregateId.of(c.orderId()), new ChargeDecider())
            .register(
                PAYMENT,
                Refund.class,
                c -> AggregateId.of(c.orderId()),
                new RefundDecider(refundsFail))
            .register(ACCOUNT, Touch.class, c -> AggregateId.of(c.accountId()), new TouchDecider())
            .build();
    var runner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderSaga())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
    var sweeper =
        SagaCompensationRetrySweeper.<OrderState>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .leadership(SubscriptionLeadership.NOOP)
            .retryInterval(Duration.ofMillis(5))
            .giveUpAfter(Duration.ofHours(1))
            .build();
    var listener = runner.asEventListener();

    // Orders whose charge is declined: their refund fails while the run lasts, so the sweeper
    // re-drives their compensation over and over beside the deliveries.
    List<EventEnvelope> declined = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      declined.add(storeOrderPlaced(store, "declined-" + i));
    }
    listener.onEvents(declined);

    AtomicBoolean stop = new AtomicBoolean();
    ConcurrentLinkedQueue<Throwable> refusals = new ConcurrentLinkedQueue<>();
    List<String> healthy = java.util.Collections.synchronizedList(new ArrayList<>());
    AtomicLong requests = new AtomicLong();
    sweeper.start();
    Thread poll =
        Thread.ofVirtual()
            .name("streamrune-poll-order-saga")
            .start(
                () -> {
                  AtomicInteger next = new AtomicInteger();
                  while (!stop.get()) {
                    List<EventEnvelope> page = new ArrayList<>();
                    for (int i = 0; i < 25; i++) {
                      String orderId = "healthy-" + next.incrementAndGet();
                      page.add(storeOrderPlaced(store, orderId));
                      healthy.add(orderId);
                    }
                    try {
                      listener.onEvents(page);
                    } catch (ReservedCorrelationIdException refused) {
                      refusals.add(refused);
                      // the subscription delivers the page again
                      listener.onEvents(page);
                    }
                  }
                });
    List<Thread> routes = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      int route = r;
      routes.add(
          Thread.ofVirtual()
              .name("route-" + r)
              .start(
                  () -> {
                    while (!stop.get()) {
                      request.set(
                          StreamRuneContext.RequestContext.fromRequest(
                              null,
                              UserId.of("user-" + route),
                              CorrelationId.of("request-" + requests.incrementAndGet()),
                              Instant.now(),
                              Map.of()));
                      try {
                        bus.execute(new Touch("account-" + route));
                      } finally {
                        request.remove();
                      }
                    }
                  }));
    }
    Thread.sleep(run.toMillis());
    stop.set(true);
    poll.join(Duration.ofSeconds(30));
    for (Thread route : routes) {
      route.join(Duration.ofSeconds(30));
    }
    sweeper.close();
    refundsFail.set(false);

    assertThat(refusals)
        .as("forward commands refused for the bindings of their dispatch")
        .isEmpty();
    assertThat(healthy).as("the run delivered orders").isNotEmpty();
    for (String orderId : List.copyOf(healthy)) {
      LoadedSaga<OrderState> saga =
          sagaStore
              .load(sagaIdOf(orderId), SagaType.fromClass(OrderState.class), OrderState.class)
              .orElseThrow();
      assertThat(saga.status()).as("order %s", orderId).isEqualTo(SagaStatus.RUNNING);
      assertThat(saga.episodeVersion()).as("order %s is not compensated", orderId).isNull();
      List<EventEnvelope> payment =
          store.readStream(StreamId.of(PAYMENT, AggregateId.of(orderId)), Version.initial(), 10);
      assertThat(payment).as("order %s", orderId).hasSize(1);
      SagaOrigin own =
          SagaOrigin.dispatchedBy(SagaType.fromClass(OrderState.class), sagaIdOf(orderId));
      EventMetadata metadata = payment.getFirst().metadata();
      assertThat(metadata.sagaOrigin()).as("origin of %s's charge", orderId).isEqualTo(own);
      assertThat(metadata.correlationId())
          .as("correlation id of %s's charge", orderId)
          .isEqualTo(own.correlationId());
    }
    assertThat(requests.get()).as("the routes dispatched").isPositive();
  }

  private static EventEnvelope storeOrderPlaced(InMemoryEventStore store, String orderId) {
    StreamId orderStream = StreamId.of(AggregateType.of("order"), AggregateId.of(orderId));
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
                    EventId.of("evt-" + orderId),
                    CommandId.of("cmd-" + orderId),
                    null,
                    null,
                    CorrelationId.of("client-" + orderId),
                    null,
                    null,
                    Instant.now()))),
        Version.initial());
    return store.readStream(orderStream, Version.initial(), 1).getFirst();
  }

  static SagaId sagaIdOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  record OrderPlaced(String orderId) implements DomainEvent {}

  record Charge(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record Charged(String orderId) implements DomainEvent {}

  record Refunded(String orderId) implements DomainEvent {}

  record PaymentState(boolean charged) implements AggregateState {}

  /** Declines the charge of an order whose id starts with {@code declined}. */
  static final class ChargeDecider implements Decider<Charge, PaymentState, Charged> {
    @Override
    public PaymentState initialState() {
      return new PaymentState(false);
    }

    @Override
    public List<Charged> decide(Charge command, PaymentState state) {
      if (command.orderId().startsWith("declined")) {
        throw new DomainException("declined: " + command.orderId());
      }
      return List.of(new Charged(command.orderId()));
    }

    @Override
    public PaymentState evolve(PaymentState state, Charged event) {
      return new PaymentState(true);
    }
  }

  /** Fails transiently while {@code failing} is set, as a gateway that is down. */
  static final class RefundDecider implements Decider<Refund, PaymentState, Refunded> {
    private final AtomicBoolean failing;

    RefundDecider(AtomicBoolean failing) {
      this.failing = failing;
    }

    @Override
    public PaymentState initialState() {
      return new PaymentState(false);
    }

    @Override
    public List<Refunded> decide(Refund command, PaymentState state) {
      if (failing.get()) {
        throw new EventStoreException("the payment gateway is down");
      }
      return List.of(new Refunded(command.orderId()));
    }

    @Override
    public PaymentState evolve(PaymentState state, Refunded event) {
      return new PaymentState(false);
    }
  }

  record Touch(String accountId) implements Command {}

  record Touched(String accountId) implements DomainEvent {}

  record AccountState(int touches) implements AggregateState {}

  static final class TouchDecider implements Decider<Touch, AccountState, Touched> {
    @Override
    public AccountState initialState() {
      return new AccountState(0);
    }

    @Override
    public List<Touched> decide(Touch command, AccountState state) {
      return List.of(new Touched(command.accountId()));
    }

    @Override
    public AccountState evolve(AccountState state, Touched event) {
      return new AccountState(state.touches() + 1);
    }
  }

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  /** {@code OrderPlaced} charges the order; the undo refunds it. */
  static final class OrderSaga implements SagaOrchestrator<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED, null);
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
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return event.event() instanceof OrderPlaced placed
          ? new OrderState(SagaStatus.RUNNING, placed.orderId())
          : state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return event.event() instanceof OrderPlaced placed
          ? List.of(SagaCommand.of(new Charge(placed.orderId()), AggregateId.of(placed.orderId())))
          : List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new Refund(state.orderId()), AggregateId.of(state.orderId())));
    }
  }
}

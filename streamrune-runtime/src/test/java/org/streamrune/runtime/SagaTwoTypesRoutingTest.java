package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Two runners of different saga types read the same events. An event that carries an origin of one
 * type is, for the other type's runner, an event it did not cause: with the default routing that
 * runner loads no saga and writes nothing, even when the origin's saga id is the id of one of its
 * own sagas.
 */
class SagaTwoTypesRoutingTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("two_types"), AggregateId.of("order-1"));

  /** Both types derive their saga ids in one shape: {@code order-saga-<n>}. */
  private static final SagaId PAYMENT_SAGA = SagaId.of("order-saga-1");

  private static final SagaId SHIPPING_SAGA = SagaId.of("order-saga-2");

  record PaymentStarted(String sagaId) implements DomainEvent {}

  record ShippingStarted(String sagaId) implements DomainEvent {}

  record Replied() implements DomainEvent {}

  record PaymentState(SagaStatus status, int replies) implements SagaState {}

  record ShippingState(SagaStatus status, int replies) implements SagaState {}

  /** Starts on its own start event; counts every other event it is given. No routing hook. */
  abstract static class CountingSaga<S extends SagaState> implements SagaOrchestrator<S> {
    @Override
    public List<SagaCommand> handle(S state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final class PaymentSaga extends CountingSaga<PaymentState> {
    @Override
    public Class<PaymentState> stateType() {
      return PaymentState.class;
    }

    @Override
    public PaymentState initialState(SagaId sagaId) {
      return new PaymentState(SagaStatus.STARTED, 0);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof PaymentStarted;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of(((PaymentStarted) event.event()).sagaId());
    }

    @Override
    public PaymentState evolve(PaymentState state, EventEnvelope event) {
      return new PaymentState(
          SagaStatus.RUNNING,
          event.event() instanceof Replied ? state.replies() + 1 : state.replies());
    }
  }

  static final class ShippingSaga extends CountingSaga<ShippingState> {
    @Override
    public Class<ShippingState> stateType() {
      return ShippingState.class;
    }

    @Override
    public ShippingState initialState(SagaId sagaId) {
      return new ShippingState(SagaStatus.STARTED, 0);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof ShippingStarted;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of(((ShippingStarted) event.event()).sagaId());
    }

    @Override
    public ShippingState evolve(ShippingState state, EventEnvelope event) {
      return new ShippingState(
          SagaStatus.RUNNING,
          event.event() instanceof Replied ? state.replies() + 1 : state.replies());
    }
  }

  static final class NoopBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return ok();
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(List.of(), STREAM, Version.initial(), List.of());
    }
  }

  private InMemorySagaStore rows;
  private ForwardingSagaStore paymentStore;
  private ForwardingSagaStore shippingStore;
  private InMemorySagaDeadLetterStore deadLetters;
  private SagaRunner<PaymentState> paymentRunner;
  private SagaRunner<ShippingState> shippingRunner;

  @BeforeEach
  void setUp() {
    rows = new InMemorySagaStore();
    paymentStore = new ForwardingSagaStore(rows);
    shippingStore = new ForwardingSagaStore(rows);
    deadLetters = new InMemorySagaDeadLetterStore(rows);
    paymentRunner =
        SagaRunner.<PaymentState>builder()
            .orchestrator(new PaymentSaga())
            .sagaStore(paymentStore)
            .commandBus(new NoopBus())
            .sagaDeadLetterStore(deadLetters)
            .build();
    shippingRunner =
        SagaRunner.<ShippingState>builder()
            .orchestrator(new ShippingSaga())
            .sagaStore(shippingStore)
            .commandBus(new NoopBus())
            .sagaDeadLetterStore(deadLetters)
            .build();
    deliverToBoth(
        envelope(1L, new PaymentStarted(PAYMENT_SAGA.value()), null),
        envelope(2L, new ShippingStarted(SHIPPING_SAGA.value()), null));
  }

  private void deliverToBoth(EventEnvelope... events) {
    paymentRunner.asEventListener().onEvents(List.of(events));
    shippingRunner.asEventListener().onEvents(List.of(events));
  }

  private static EventEnvelope envelope(long offset, DomainEvent event, SagaOrigin origin) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        STREAM,
        Version.initial(),
        EventType.fromClass(event.getClass()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-" + offset),
            null,
            null,
            Instant.now(),
            Map.of(),
            origin));
  }

  private static SagaOrigin origin(Class<? extends SagaState> stateType, SagaId sagaId) {
    return SagaOrigin.dispatchedBy(SagaType.fromClass(stateType), sagaId);
  }

  private int paymentReplies() {
    return rows.load(PAYMENT_SAGA, SagaType.fromClass(PaymentState.class), PaymentState.class)
        .orElseThrow()
        .state()
        .replies();
  }

  private int shippingReplies() {
    return rows.load(SHIPPING_SAGA, SagaType.fromClass(ShippingState.class), ShippingState.class)
        .orElseThrow()
        .state()
        .replies();
  }

  @Test
  void neitherRunnerLoadsTheOtherTypesSaga() {
    int paymentCalls = paymentStore.calls().size();
    int shippingCalls = shippingStore.calls().size();

    deliverToBoth(envelope(3L, new Replied(), origin(PaymentState.class, PAYMENT_SAGA)));

    assertThat(paymentReplies()).isEqualTo(1);
    assertThat(shippingStore.calls())
        .as("the shipping runner neither loads nor writes for an event a payment saga caused")
        .hasSize(shippingCalls);

    paymentCalls = paymentStore.calls().size();
    deliverToBoth(envelope(4L, new Replied(), origin(ShippingState.class, SHIPPING_SAGA)));

    assertThat(shippingReplies()).isEqualTo(1);
    assertThat(paymentStore.calls())
        .as("the payment runner neither loads nor writes for an event a shipping saga caused")
        .hasSize(paymentCalls);
    assertThat(deadLetters.all()).isEmpty();
  }

  /**
   * An origin of the other type that names the id of this type's saga: the id alone addresses
   * nothing. The other type's runner finds no saga of its own under that id and drops the event.
   */
  @Test
  void anOriginOfTheOtherTypeNamingThisTypesSagaId_doesNotReachThatSaga() {
    int shippingCalls = shippingStore.calls().size();

    deliverToBoth(envelope(3L, new Replied(), origin(PaymentState.class, SHIPPING_SAGA)));

    assertThat(shippingReplies()).isZero();
    assertThat(shippingStore.calls()).hasSize(shippingCalls);
    assertThat(paymentReplies()).isZero();
    assertThat(deadLetters.all()).isEmpty();
  }
}

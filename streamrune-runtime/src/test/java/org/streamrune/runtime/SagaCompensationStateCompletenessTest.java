package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
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
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Compensation completeness contract.
 *
 * <p>A single correlated-event step emits two forward commands: the first (ChargePayment) executes,
 * the second (ReserveStock) fails. {@code compensate()} is then invoked from the saga
 * <em>state</em>, which does NOT reflect that ChargePayment already ran — its confirmation event
 * (PaymentCharged) is still in flight and has not been {@code evolve}d into state. The framework
 * hands {@code compensate} exactly {@code (state, failure, failedCommand)}; it deliberately does
 * <b>not</b> pass the list of forward commands already dispatched in this loop, because the
 * crash-resume, timeout, and sweeper re-drive paths recompute compensation from persisted {@code
 * state} alone under the shared episode-scoped idempotency key — so {@code compensate} must stay a
 * pure function of {@code state}.
 *
 * <p>These tests pin the contract the javadoc/saga.md now state loudly:
 *
 * <ul>
 *   <li>{@link #cancelVoidCompensation_coversTheAlreadyExecutedForwardCommand()} — a correctly
 *       written cancel/void compensation (unconditional, idempotent at the target aggregate) DOES
 *       undo the already-executed ChargePayment. The machinery gives the app everything it needs.
 *   <li>{@link #stateConditionedCompensation_missesTheAlreadyExecutedForwardCommand()} — a
 *       compensation that instead <em>conditions on state</em> ("refund only if state.charged")
 *       silently MISSES the undo, because state does not yet record the charge. This is the money
 *       inconsistency the orchestrator contract warns about, and why compensation must use
 *       cancel/void semantics.
 * </ul>
 */
class SagaCompensationStateCompletenessTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  record ChargePayment(String orderId) implements Command {}

  record ReserveStock(String orderId) implements Command {}

  record CancelPayment(String orderId) implements Command {}

  record ReleaseStock(String orderId) implements Command {}

  record OrderReady(String orderId) implements DomainEvent {}

  record OrderCreated(String orderId) implements DomainEvent {}

  record PaymentSagaState(SagaStatus status, String orderId, boolean charged)
      implements SagaState {}

  /** Records the arguments the framework passed to the most recent {@code compensate} call. */
  abstract static class RecordingSaga implements SagaOrchestrator<PaymentSagaState> {
    PaymentSagaState lastCompensateState;
    SagaCommand lastFailedCommand;
    int compensateCalls;

    @Override
    public Class<PaymentSagaState> stateType() {
      return PaymentSagaState.class;
    }

    @Override
    public PaymentSagaState initialState(SagaId sagaId) {
      return new PaymentSagaState(SagaStatus.STARTED, null, false);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderCreated;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((OrderCreated) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      return corrId.startsWith("saga-") ? Optional.of(SagaId.of(corrId)) : Optional.empty();
    }

    @Override
    public PaymentSagaState evolve(PaymentSagaState state, EventEnvelope event) {
      // OrderReady advances no persisted flag; the ChargePayment confirmation (PaymentCharged) is
      // in-flight and is never evolved into state within this single delivery.
      return state;
    }

    @Override
    public List<SagaCommand> handle(PaymentSagaState state, EventEnvelope event) {
      if (event.event() instanceof OrderReady) {
        return List.of(
            SagaCommand.of(new ChargePayment(state.orderId()), AggregateId.of(state.orderId())),
            SagaCommand.of(new ReserveStock(state.orderId()), AggregateId.of(state.orderId())));
      }
      return List.of();
    }

    final void record(PaymentSagaState state, SagaCommand failedCmd) {
      this.lastCompensateState = state;
      this.lastFailedCommand = failedCmd;
      this.compensateCalls++;
    }
  }

  /** Correct model: cancel/void compensation, unconditional, idempotent at the target aggregate. */
  static class CancelVoidSaga extends RecordingSaga {
    @Override
    public List<SagaCommand> compensate(
        PaymentSagaState state, Throwable failure, SagaCommand failedCmd) {
      record(state, failedCmd);
      // Undo every forward step with cancel/void commands the aggregate treats idempotently — a
      // CancelPayment is a no-op if the charge never landed and a refund if it did. Never condition
      // on state that may not yet record an in-flight forward step.
      return List.of(
          SagaCommand.of(new CancelPayment(state.orderId()), AggregateId.of(state.orderId())),
          SagaCommand.of(new ReleaseStock(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  /** Buggy model: compensation conditions the refund on state.charged (which is false here). */
  static class StateConditioningSaga extends RecordingSaga {
    @Override
    public List<SagaCommand> compensate(
        PaymentSagaState state, Throwable failure, SagaCommand failedCmd) {
      record(state, failedCmd);
      List<SagaCommand> comp = new ArrayList<>();
      if (state.charged()) { // state.charged is false — the charge confirmation never arrived.
        comp.add(
            SagaCommand.of(new CancelPayment(state.orderId()), AggregateId.of(state.orderId())));
      }
      comp.add(SagaCommand.of(new ReleaseStock(state.orderId()), AggregateId.of(state.orderId())));
      return comp;
    }
  }

  static class SelectiveFailBus implements CommandBus {
    final List<Object> dispatched = new ArrayList<>();
    private final Class<?> failOn;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    SelectiveFailBus(Class<?> failOn) {
      this.failOn = failOn;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (failOn.isInstance(command)) {
        throw new RuntimeException("simulated failure: " + command.getClass().getSimpleName());
      }
      dispatched.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  private EventEnvelope envelope(DomainEvent event, String correlationId) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of(correlationId),
            null,
            null,
            Instant.now()));
  }

  private SagaRunner<PaymentSagaState> runnerFor(
      RecordingSaga saga, InMemorySagaStore store, SelectiveFailBus bus) {
    store.create(
        SagaId.of("saga-order-1"),
        SagaType.fromClass(PaymentSagaState.class),
        new PaymentSagaState(SagaStatus.RUNNING, "order-1", false),
        SagaStatus.RUNNING);
    return SagaRunner.<PaymentSagaState>builder()
        .orchestrator(saga)
        .sagaStore(store)
        .commandBus(bus)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        .build();
  }

  @Test
  void cancelVoidCompensation_coversTheAlreadyExecutedForwardCommand() {
    var saga = new CancelVoidSaga();
    var store = new InMemorySagaStore();
    var bus = new SelectiveFailBus(ReserveStock.class);
    var runner = runnerFor(saga, store, bus);

    runner.asEventListener().onEvents(List.of(envelope(new OrderReady("order-1"), "saga-order-1")));

    // The first forward command ran; the second failed; compensation then ran.
    assertThat(bus.dispatched)
        .containsExactly(
            new ChargePayment("order-1"),
            new CancelPayment("order-1"),
            new ReleaseStock("order-1"));
    // A cancel/void compensation undoes the already-executed ChargePayment even though state never
    // recorded the charge.
    assertThat(bus.dispatched).contains(new CancelPayment("order-1"));

    // Contract: the framework hands compensate() the pre-dispatch state and the FAILED command
    // (ReserveStock) — never a "these forward commands already succeeded" list. compensate() must
    // stay a pure function of state, so the resume/timeout/sweep re-drivers reproduce it
    // identically.
    assertThat(saga.compensateCalls).isEqualTo(1);
    assertThat(saga.lastCompensateState.charged())
        .as("state does not reflect the in-flight charge confirmation")
        .isFalse();
    assertThat(saga.lastFailedCommand.command()).isInstanceOf(ReserveStock.class);

    assertThat(
            store
                .load(
                    SagaId.of("saga-order-1"),
                    SagaType.fromClass(PaymentSagaState.class),
                    PaymentSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void stateConditionedCompensation_missesTheAlreadyExecutedForwardCommand() {
    var saga = new StateConditioningSaga();
    var store = new InMemorySagaStore();
    var bus = new SelectiveFailBus(ReserveStock.class);
    var runner = runnerFor(saga, store, bus);

    runner.asEventListener().onEvents(List.of(envelope(new OrderReady("order-1"), "saga-order-1")));

    // The hazard: conditioning the refund on state.charged (false, because the confirmation
    // is in-flight) silently DROPS the CancelPayment — the customer stays charged for a cancelled
    // order. This is why the contract forbids conditioning compensation on state completeness and
    // mandates cancel/void semantics.
    assertThat(bus.dispatched).contains(new ChargePayment("order-1"));
    assertThat(bus.dispatched)
        .as("state-conditioned compensation misses the already-executed forward command")
        .doesNotContain(new CancelPayment("order-1"));
  }
}

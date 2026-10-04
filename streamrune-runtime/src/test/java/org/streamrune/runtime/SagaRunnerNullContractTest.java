package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
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
 * A {@code null} RETURN from an orchestrator pure-logic SPI method is the same deterministic
 * user-logic contract violation as a throw, and must be quarantined as a poison event — never
 * allowed to flow onward and NPE inside the executor's own frame, OUTSIDE the quarantine.
 *
 * <p>Pre-fix, {@code SagaStepExecutor.poison(...)} converted only THROWN {@code RuntimeException}s
 * and returned {@code step.get()} unchecked, so a null return from {@code initialState}/{@code
 * evolve}/{@code handle}/{@code compensate} (or a null {@code SagaState.status()}, or a null
 * element inside a returned command list) escaped the wrapper. The resulting {@code
 * NullPointerException} was not a {@code SagaPoisonException}, so {@code
 * SagaRunner.asEventListener}'s catch never saw it: it propagated out of {@code onEvents}, the
 * subscription checkpoint never advanced, and the identical batch was retried forever — permanent
 * head-of-line blocking for every saga sharing the subscription, with no dead-letter entry and no
 * FAULTED saga. This is the same defect, on the executor's SPI calls; these tests are its
 * regression pin.
 *
 * <p>Every test asserts the same three properties the fix must deliver: (a) {@code onEvents}
 * returns normally (the subscription is not wedged), (b) exactly one dead-letter entry exists at
 * the offending event's global offset, and (c) the saga row is {@code FAULTED}.
 */
class SagaRunnerNullContractTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ========== Domain ==========

  record OrderCreated(String orderId) implements DomainEvent {}

  record OrderPaid(String orderId) implements DomainEvent {}

  record FulfillOrder(String orderId) implements Command {}

  record RefundOrder(String orderId) implements Command {}

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  /**
   * Orchestrator whose SPI methods can each be switched to the audited contract violation: a null
   * return (or a null-bearing return) instead of a throw.
   */
  static class NullContractSaga implements SagaOrchestrator<OrderState> {

    volatile boolean initialStateReturnsNull = false;
    volatile boolean evolveReturnsNull = false;
    volatile boolean evolveReturnsNullStatus = false;
    volatile boolean handleReturnsNull = false;
    volatile boolean handleReturnsNullElement = false;
    volatile boolean compensateReturnsNull = false;
    volatile boolean compensateReturnsNullElement = false;

    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return initialStateReturnsNull ? null : new OrderState(SagaStatus.STARTED, null);
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

    /**
     * Deliberately does NOT dereference {@code state}: a null prior state (the {@code initialState}
     * violation) must be caught by the framework's SPI guard, not by an incidental NPE in user code
     * that would already be quarantined by the existing throw-conversion.
     */
    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      if (evolveReturnsNull) {
        return null;
      }
      String orderId =
          switch (event.event()) {
            case OrderCreated oc -> oc.orderId();
            case OrderPaid op -> op.orderId();
            default -> null;
          };
      return new OrderState(evolveReturnsNullStatus ? null : SagaStatus.RUNNING, orderId);
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      if (handleReturnsNull) {
        return null; // e.g. a switch arm that forgot to return List.of()
      }
      if (handleReturnsNullElement) {
        // Arrays.asList permits nulls (List.of does not) — the shape a user's ArrayList build-up
        // produces when one branch appends a null.
        return Arrays.asList(
            SagaCommand.of(new FulfillOrder("order-x"), AggregateId.of("order-x")), null);
      }
      // Every event dispatches one forward command, so a failing bus reaches the claim-first
      // compensation path from a correlated event as well as from a start event.
      return List.of(
          SagaCommand.of(new FulfillOrder(state.orderId()), AggregateId.of(state.orderId())));
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      if (compensateReturnsNull) {
        return null;
      }
      if (compensateReturnsNullElement) {
        return Arrays.asList(
            SagaCommand.of(new RefundOrder("order-x"), AggregateId.of("order-x")), null);
      }
      return List.of(
          SagaCommand.of(new RefundOrder(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  // ========== Fixtures ==========

  static class CapturingCommandBus implements CommandBus {
    final List<Object> dispatched = new ArrayList<>();
    private RuntimeException failWith = null;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    void failNext(RuntimeException e) {
      this.failWith = e;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (failWith != null) {
        RuntimeException ex = failWith;
        failWith = null;
        throw ex;
      }
      dispatched.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore dlq;
  CapturingCommandBus commandBus;
  NullContractSaga orchestrator;
  SagaRunner<OrderState> runner;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    dlq = new InMemorySagaDeadLetterStore(sagaStore);
    commandBus = new CapturingCommandBus();
    orchestrator = new NullContractSaga();
    runner =
        SagaRunner.<OrderState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .build();
  }

  private EventEnvelope envelope(DomainEvent event, String correlationId, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
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

  /** Seeds a live RUNNING saga so a correlated event has a row to advance. */
  private SagaId seedRunningSaga(String orderId) {
    SagaId sagaId = SagaId.of("saga-" + orderId);
    sagaStore.create(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.RUNNING, orderId),
        SagaStatus.RUNNING);
    return sagaId;
  }

  /** The three properties every SPI-contract violation must produce. */
  private void assertQuarantinedNotWedged(SagaId sagaId, EventEnvelope poisonEvent) {
    var entries = dlq.findBySaga(sagaId);
    assertThat(entries)
        .as("exactly one dead-letter entry for the offending event")
        .hasSize(1)
        .allSatisfy(e -> assertThat(e.eventOffset()).isEqualTo(poisonEvent.globalOffset()));
    assertThat(entries.get(0).errorType())
        .as("the contract violation is recorded as the cause")
        .contains("NullPointerException");
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .status())
        .as("the saga is quarantined FAULTED")
        .isEqualTo(SagaStatus.FAULTED);
  }

  // ========== evolve ==========

  @Test
  void evolveReturnsNull_isQuarantined_batchContinues_sagaFaulted() {
    SagaId sagaId = seedRunningSaga("order-evolve");
    orchestrator.evolveReturnsNull = true;
    var poison = envelope(new OrderPaid("order-evolve"), "saga-order-evolve", 100L);
    var healthyFollowUp = envelope(new OrderPaid("order-unrelated"), "unrelated", 101L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison, healthyFollowUp)))
        .as("a null evolve() return must not escape onEvents and wedge the subscription")
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
  }

  @Test
  void evolveReturnsStateWithNullStatus_isQuarantined_batchContinues_sagaFaulted() {
    SagaId sagaId = seedRunningSaga("order-status");
    orchestrator.evolveReturnsNullStatus = true;
    var poison = envelope(new OrderPaid("order-status"), "saga-order-status", 110L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .as("a null SagaState.status() must not escape onEvents and wedge the subscription")
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
  }

  // ========== handle ==========

  @Test
  void handleReturnsNull_isQuarantined_batchContinues_sagaFaulted() {
    SagaId sagaId = seedRunningSaga("order-handle");
    orchestrator.handleReturnsNull = true;
    var poison = envelope(new OrderPaid("order-handle"), "saga-order-handle", 120L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .as("a null handle() return must not escape onEvents and wedge the subscription")
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
    assertThat(commandBus.dispatched).isEmpty();
  }

  @Test
  void handleReturnsListWithNullElement_isQuarantined_notMisclassifiedAsCommandFailure() {
    SagaId sagaId = seedRunningSaga("order-elem");
    orchestrator.handleReturnsNullElement = true;
    var poison = envelope(new OrderPaid("order-elem"), "saga-order-elem", 130L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
    // The audited misclassification: pre-fix the NPE on the null element fired INSIDE the
    // dispatch try-block, was caught as `catch (Exception primary)`, and drove a full
    // claim-first COMPENSATION (a real RefundOrder) for a command that never failed — and the
    // first, valid command had already been dispatched.
    assertThat(commandBus.dispatched)
        .as("a malformed command list is a quarantine, never a forward-command failure")
        .isEmpty();
  }

  // ========== compensate ==========

  @Test
  void compensateReturnsNull_isQuarantined_batchContinues_sagaFaulted() {
    SagaId sagaId = seedRunningSaga("order-comp");
    orchestrator.compensateReturnsNull = true;
    // Fail the forward command so the claim-first compensation path runs.
    commandBus.failNext(new RuntimeException("fulfillment service down"));
    var poison = envelope(new OrderPaid("order-comp"), "saga-order-comp", 140L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .as("a null compensate() return must not escape onEvents and wedge the subscription")
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
  }

  @Test
  void compensateReturnsListWithNullElement_isQuarantined_notLeftCompensatingForever() {
    SagaId sagaId = seedRunningSaga("order-comp-elem");
    orchestrator.compensateReturnsNullElement = true;
    commandBus.failNext(new RuntimeException("fulfillment service down"));
    var poison = envelope(new OrderPaid("order-comp-elem"), "saga-order-comp-elem", 150L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .doesNotThrowAnyException();

    assertQuarantinedNotWedged(sagaId, poison);
    // Pre-fix the NPE on the null element was swallowed by compensateAndClassify's per-command
    // catch and classified TRANSIENT -> RETRY, leaving the saga COMPENSATING forever behind a
    // deterministic user bug that no re-drive can ever resolve.
    assertThat(commandBus.dispatched)
        .as("no compensation command is dispatched from a malformed list")
        .isEmpty();
  }

  // ========== initialState ==========

  @Test
  void initialStateReturnsNull_isQuarantined_batchContinues() {
    orchestrator.initialStateReturnsNull = true;
    var poison = envelope(new OrderCreated("order-init"), "some-correlation", 160L);

    assertThatCode(() -> runner.asEventListener().onEvents(List.of(poison)))
        .doesNotThrowAnyException();

    // A start-event poison whose initialState() is itself the violation leaves no saga row
    // (markFaulted skips the FAULTED-row create when genesis cannot be derived), so the
    // dead-letter entry is the sole record — it must exist.
    SagaId sagaId = SagaId.of("saga-order-init");
    var entries = dlq.findBySaga(sagaId);
    assertThat(entries)
        .as("a null initialState() return must be quarantined, not silently accepted")
        .hasSize(1);
    assertThat(entries.get(0).eventOffset()).isEqualTo(poison.globalOffset());
    assertThat(entries.get(0).errorType()).contains("NullPointerException");
    assertThat(commandBus.dispatched).isEmpty();
  }
}

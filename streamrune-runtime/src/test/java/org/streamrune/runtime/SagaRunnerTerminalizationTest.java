package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Tests that SagaRunner persists a terminal status (COMPENSATED/FAILED) after the compensation path
 * runs — fixing a bug where the saga was left RUNNING after compensation.
 */
class SagaRunnerTerminalizationTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ======================================================================
  // Minimal domain model
  // ======================================================================

  /** Start event — triggers saga creation. */
  record Start(String id) implements DomainEvent {}

  /** Command emitted by handle() when saga is RUNNING. */
  record DoWork(String id) implements Command {}

  /** Compensation command emitted by compensate(). */
  record Undo(String id) implements Command {}

  /** Generic correlated event used to probe the skip-on-terminal path. */
  record SomeOtherEvent(String id) implements DomainEvent {}

  // ======================================================================
  // Minimal saga state
  // ======================================================================

  record FakeSagaState(SagaStatus status, String id) implements SagaState {
    FakeSagaState withStatus(SagaStatus s) {
      return new FakeSagaState(s, id);
    }
  }

  // ======================================================================
  // Minimal orchestrator
  // ======================================================================

  /**
   * Orchestrator: start on Start event → RUNNING; handle in RUNNING emits DoWork; compensate emits
   * Undo.
   */
  static class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {

    final AtomicInteger handleCallCount = new AtomicInteger(0);

    @Override
    public Class<FakeSagaState> stateType() {
      return FakeSagaState.class;
    }

    @Override
    public FakeSagaState initialState(SagaId sagaId) {
      return new FakeSagaState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Start;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((Start) event.event()).id());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      if (corrId.startsWith("saga-")) return Optional.of(SagaId.of(corrId));
      return Optional.empty();
    }

    @Override
    public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
      if (event.event() instanceof Start s) {
        return new FakeSagaState(SagaStatus.RUNNING, s.id());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
      handleCallCount.incrementAndGet();
      if (state.status() == SagaStatus.RUNNING) {
        return List.of(SagaCommand.of(new DoWork(state.id()), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new Undo(state.id()), AggregateId.of(state.id())));
    }
  }

  // ======================================================================
  // Configurable recording CommandBus
  // ======================================================================

  static class RecordingCommandBus implements CommandBus {
    final List<Command> executed = new ArrayList<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    /** If set, throw on the next execute() call for this command type; clear after first throw. */
    Class<?> failCommandType = null;

    /** All future calls to execute will always throw (for testing total failure). */
    boolean alwaysFail = false;

    void failNextOfType(Class<?> type) {
      this.failCommandType = type;
    }

    void failAll() {
      this.alwaysFail = true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (alwaysFail) {
        // Deterministic (business) failure: per DEFAULT_DLQ_ELIGIBLE an IllegalArgumentException is
        // a permanent rejection, so a compensation command failing this way terminalizes the saga
        // FAILED. A transient RuntimeException would instead leave it
        // COMPENSATING for retry — covered separately by the transient-resume tests.
        throw new IllegalArgumentException(
            "Simulated deterministic failure for " + command.getClass().getSimpleName());
      }
      if (failCommandType != null && failCommandType.isInstance(command)) {
        failCommandType = null;
        throw new RuntimeException("Simulated failure for " + command.getClass().getSimpleName());
      }
      executed.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  // ======================================================================
  // Helpers
  // ======================================================================

  private EventEnvelope startEnvelope(String id) {
    Start event = new Start(id);
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
            CorrelationId.of("some-correlation"),
            null,
            null,
            Instant.now()));
  }

  // ======================================================================
  // Test setup
  // ======================================================================

  InMemorySagaStore sagaStore;
  RecordingCommandBus bus;
  FakeOrchestrator orchestrator;
  SagaRunner<FakeSagaState> runner;
  SagaId sagaId;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    bus = new RecordingCommandBus();
    orchestrator = new FakeOrchestrator();
    runner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
    sagaId = SagaId.of("saga-test");
  }

  // ======================================================================
  // Tests
  // ======================================================================

  /**
   * Primary command (DoWork) fails; compensation (Undo) succeeds → saga must be saved as
   * COMPENSATED.
   */
  @Test
  void primaryCommandFails_compensationSucceeds_sagaIsCOMPENSATED() {
    // bus: throw on DoWork, succeed on Undo
    bus.failNextOfType(DoWork.class);

    runner.asEventListener().onEvents(List.of(startEnvelope("test")));

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status()).isEqualTo(SagaStatus.COMPENSATED);
    // Undo must have been dispatched
    assertThat(bus.executed).hasSize(1).allMatch(c -> c instanceof Undo);
  }

  /**
   * Primary command (DoWork) fails; compensation (Undo) also throws → saga must be saved as FAILED.
   */
  @Test
  void primaryFails_compensationThrows_sagaIsFAILED() {
    // bus: throw on everything
    bus.failAll();

    runner.asEventListener().onEvents(List.of(startEnvelope("test")));

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAILED);
  }

  /**
   * After a saga is terminal (driven to COMPENSATED), further correlated events must be silently
   * skipped and handle() must not be invoked again.
   */
  @Test
  void terminalSaga_isSkipped_byCorrelatedEvents() {
    // Drive saga to terminal by failing DoWork
    bus.failNextOfType(DoWork.class);
    runner.asEventListener().onEvents(List.of(startEnvelope("test")));
    // Verify terminal
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);

    // Reset handle count
    orchestrator.handleCallCount.set(0);

    // Deliver a correlated event (simulate a late event with the saga's correlation id)
    var correlatedEnvelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            StreamId.of(TYPE, AggregateId.of("test-stream")),
            Version.initial(),
            new EventType("SomeOtherEvent"),
            new SomeOtherEvent("test"),
            new EventMetadata(
                EventId.of("evt-" + UUID.randomUUID()),
                CommandId.of("cmd-2"),
                null,
                null,
                CorrelationId.of("saga-test"), // correlates to our saga
                null,
                null,
                Instant.now()));

    bus.executed.clear();
    runner.asEventListener().onEvents(List.of(correlatedEnvelope));

    // handle must NOT have been called; no new commands dispatched
    assertThat(orchestrator.handleCallCount.get())
        .as("handle must not be called for terminal saga")
        .isZero();
    assertThat(bus.executed).as("no commands dispatched for terminal saga").isEmpty();
  }

  /**
   * Forward success path: primary command succeeds → persisted status must equal the domain status
   * returned by state.status() (RUNNING in this orchestrator).
   */
  @Test
  void forwardSuccess_persistsDomainStatus() {
    // bus: succeed on DoWork
    runner.asEventListener().onEvents(List.of(startEnvelope("test")));

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    // The orchestrator's evolve sets RUNNING after Start; handle emits DoWork which succeeds.
    // Persisted status must match state.status() == RUNNING.
    assertThat(loaded.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(bus.executed).hasSize(1).allMatch(c -> c instanceof DoWork);
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
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
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Verifies that {@link SagaRunner} and {@link SagaTimeoutRunner} emit {@link StreamRuneMetrics}
 * hooks at the right points, without changing any existing propagation/exception semantics.
 */
class SagaMetricsEmissionTest {

  private static final AggregateType TYPE = AggregateType.of("inventory");

  // ======================================================================
  // RecordingMetrics test fake
  // ======================================================================

  /** Records only the saga/inbox metric hooks under test; all other methods are no-ops. */
  static class RecordingMetrics implements StreamRuneMetrics {
    final List<String> sagaQuarantined = new ArrayList<>();
    final List<String> sagaFaulted = new ArrayList<>();
    final List<String> sagaCompensationTypes = new ArrayList<>();
    final List<String> sagaCompensationOutcomes = new ArrayList<>();
    final List<String> sagaCasConflict = new ArrayList<>();
    final List<String> inboxReplayHit = new ArrayList<>();
    final List<Long> inboxSwept = new ArrayList<>();
    final List<Long> outboxSwept = new ArrayList<>();

    @Override
    public void recordSagaQuarantined(String sagaType) {
      sagaQuarantined.add(sagaType);
    }

    @Override
    public void recordSagaFaulted(String sagaType) {
      sagaFaulted.add(sagaType);
    }

    @Override
    public void recordSagaCompensation(String sagaType, String outcome) {
      sagaCompensationTypes.add(sagaType);
      sagaCompensationOutcomes.add(outcome);
    }

    @Override
    public void recordSagaCasConflict(String sagaType) {
      sagaCasConflict.add(sagaType);
    }

    @Override
    public void recordInboxReplayHit(String commandType) {
      inboxReplayHit.add(commandType);
    }

    @Override
    public void recordInboxSwept(long rows) {
      inboxSwept.add(rows);
    }

    @Override
    public void recordOutboxSwept(long rows) {
      outboxSwept.add(rows);
    }
  }

  /**
   * A {@link StreamRuneMetrics} fake whose overridden record* methods all throw, simulating a
   * broken metrics backend. Used to verify that {@link SagaRunner} and {@link SagaTimeoutRunner}
   * never let a throwing metrics call corrupt CAS-conflict propagation or poison isolation.
   */
  static class ThrowingMetrics implements StreamRuneMetrics {
    @Override
    public void recordSagaQuarantined(String sagaType) {
      throw new RuntimeException("metrics down");
    }

    @Override
    public void recordSagaFaulted(String sagaType) {
      throw new RuntimeException("metrics down");
    }

    @Override
    public void recordSagaCompensation(String sagaType, String outcome) {
      throw new RuntimeException("metrics down");
    }

    @Override
    public void recordSagaCasConflict(String sagaType) {
      throw new RuntimeException("metrics down");
    }
  }

  // ======================================================================
  // Minimal domain model (mirrors SagaRunnerPoisonIsolationTest's FakeOrchestrator)
  // ======================================================================

  record Start(String id) implements DomainEvent {}

  record DoWork(String id) implements Command {}

  record FakeState(SagaStatus status, String id) implements SagaState {}

  /**
   * Configurable orchestrator: {@code handle()} throws for a specific saga id (poison path);
   * otherwise emits one {@link DoWork} command.
   */
  static class FakeOrchestrator implements SagaOrchestrator<FakeState> {

    SagaId handleThrowsFor = null;

    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
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
      return Optional.empty();
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      if (event.event() instanceof Start s) {
        return new FakeState(SagaStatus.RUNNING, s.id());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      if (handleThrowsFor != null) {
        SagaId sagaId = SagaId.of("saga-" + state.id());
        if (sagaId.equals(handleThrowsFor)) {
          throw new RuntimeException("simulated handle failure for saga " + sagaId.value());
        }
      }
      return List.of(SagaCommand.of(new DoWork(state.id()), AggregateId.of(state.id())));
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  // ======================================================================
  // Recording CommandBus (keyed execute only, mirrors existing test doubles)
  // ======================================================================

  static class RecordingCommandBus implements CommandBus {
    final List<Command> executed = new ArrayList<>();
    boolean primaryFails = false;
    boolean compensationFails = false;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      executed.add(command);
      if (command instanceof DoWork && primaryFails) {
        throw new RuntimeException("simulated primary command failure");
      }
      if (command instanceof DoWork && compensationFails) {
        throw new RuntimeException("simulated compensation command failure");
      }
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  // ======================================================================
  // Helpers
  // ======================================================================

  private static long offsetCounter = 1;

  private EventEnvelope startEnvelope(String id) {
    Start event = new Start(id);
    return new EventEnvelope(
        GlobalOffset.of(offsetCounter++),
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

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore sagaDlq;
  RecordingCommandBus commandBus;
  FakeOrchestrator orchestrator;
  RecordingMetrics metrics;

  @BeforeEach
  void setUp() {
    offsetCounter = 1;
    sagaStore = new InMemorySagaStore();
    sagaDlq = new InMemorySagaDeadLetterStore(sagaStore);
    commandBus = new RecordingCommandBus();
    orchestrator = new FakeOrchestrator();
    metrics = new RecordingMetrics();
  }

  private SagaRunner<FakeState> buildRunner() {
    return SagaRunner.<FakeState>builder()
        .orchestrator(orchestrator)
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        .sagaDeadLetterStore(sagaDlq)
        .metrics(metrics)
        .build();
  }

  // ======================================================================
  // Test case 1: poison event -> quarantined + faulted
  // ======================================================================

  @Test
  void poisonEvent_recordsQuarantinedAndFaulted() {
    SagaId sagaId = SagaId.of("saga-A");
    orchestrator.handleThrowsFor = sagaId;

    var runner = buildRunner();
    runner.asEventListener().onEvents(List.of(startEnvelope("A")));

    assertThat(metrics.sagaQuarantined)
        .containsExactly(SagaType.fromClass(FakeState.class).value());
    assertThat(metrics.sagaFaulted).containsExactly(SagaType.fromClass(FakeState.class).value());
  }

  @Test
  void throwingMetrics_doesNotBreakPoisonIsolation() {
    SagaId sagaIdA = SagaId.of("saga-A");
    SagaId sagaIdB = SagaId.of("saga-B");
    orchestrator.handleThrowsFor = sagaIdA;

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(sagaDlq)
            .metrics(new ThrowingMetrics())
            .build();

    // Batch: saga-A's poison event first, then saga-B's healthy event. A throwing metrics backend
    // must not escape the poison-isolation catch handler (which calls quarantine()/markFaulted(),
    // both of which record metrics) and abort the rest of the batch.
    runner.asEventListener().onEvents(List.of(startEnvelope("A"), startEnvelope("B")));

    // onEvents must return normally (implicit: no exception above).

    // A's event is quarantined
    assertThat(sagaDlq.findBySaga(sagaIdA)).hasSize(1);

    // A is FAULTED
    assertThat(
            sagaStore
                .load(sagaIdA, SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAULTED);

    // B was processed (no head-of-line block)
    assertThat(sagaStore.load(sagaIdB, SagaType.fromClass(FakeState.class), FakeState.class))
        .isPresent();
    assertThat(
            sagaStore
                .load(sagaIdB, SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  // ======================================================================
  // Test case 2: compensation outcome recorded
  // ======================================================================

  @Test
  void compensation_recordsOutcome_compensated() {
    commandBus.primaryFails = true;
    // orchestrator.compensate() returns [] by default -> compensateAndClassify would be FAILED.
    // Use an orchestrator whose compensate() returns one command that dispatches OK.
    var compensatingOrchestrator =
        new FakeOrchestrator() {
          @Override
          public List<SagaCommand> compensate(
              FakeState state, Throwable failure, SagaCommand failedCommand) {
            return List.of(
                SagaCommand.of(new DoWork("undo-" + state.id()), AggregateId.of(state.id())));
          }
        };
    var bus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            if (command instanceof DoWork dw && dw.id().equals("A")) {
              throw new RuntimeException("primary command failed");
            }
            return new CommandResult(
                List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
          }
        };

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(compensatingOrchestrator)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(sagaDlq)
            .metrics(metrics)
            .build();

    runner.asEventListener().onEvents(List.of(startEnvelope("A")));

    assertThat(metrics.sagaCompensationTypes)
        .containsExactly(SagaType.fromClass(FakeState.class).value());
    assertThat(metrics.sagaCompensationOutcomes).containsExactly(SagaStatus.COMPENSATED.name());
  }

  @Test
  void compensation_recordsOutcome_failed() {
    // Primary fails, and the orchestrator's compensation command ALSO fails DETERMINISTICALLY ->
    // the
    // terminal FAILED outcome. An IllegalArgumentException is a permanent
    // rejection per DEFAULT_DLQ_ELIGIBLE; a transient failure would record the RETRY outcome
    // instead.
    var throwingCompensationBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            throw new IllegalArgumentException("everything fails");
          }
        };
    var compensatingOrchestrator =
        new FakeOrchestrator() {
          @Override
          public List<SagaCommand> compensate(
              FakeState state, Throwable failure, SagaCommand failedCommand) {
            return List.of(
                SagaCommand.of(new DoWork("undo-" + state.id()), AggregateId.of(state.id())));
          }
        };

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(compensatingOrchestrator)
            .sagaStore(sagaStore)
            .commandBus(throwingCompensationBus)
            .sagaDeadLetterStore(sagaDlq)
            .metrics(metrics)
            .build();

    runner.asEventListener().onEvents(List.of(startEnvelope("A")));

    assertThat(metrics.sagaCompensationTypes)
        .containsExactly(SagaType.fromClass(FakeState.class).value());
    assertThat(metrics.sagaCompensationOutcomes).containsExactly(SagaStatus.FAILED.name());
  }

  // ======================================================================
  // Test case 3: correlated CAS conflict recorded AND still propagates
  // ======================================================================

  /** A SagaStore whose update() always throws OptimisticLockException, for the correlated path. */
  static class CasConflictingSagaStore implements SagaStore {
    private final InMemorySagaStore delegate;

    CasConflictingSagaStore(InMemorySagaStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
      delegate.createFaulted(sagaId, sagaType, initialState);
    }

    /** The forward CAS is {@code applyEvent} now — the seam moves with the executor. */
    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      throw new OptimisticLockException("simulated correlated-event CAS conflict");
    }

    @Override
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return delegate.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      delegate.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public void update(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      throw new OptimisticLockException("simulated correlated-event CAS conflict");
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
      return delegate.findTimedOut(sagaType, updatedBefore, limit);
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return delegate.findByStatus(sagaType, status, limit);
    }

    @Override
    public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
        SagaId sagaId, SagaType sagaType, Class<S> stateType) {
      return delegate.load(sagaId, sagaType, stateType);
    }

    @Override
    public void delete(SagaId sagaId, SagaType sagaType) {
      delegate.delete(sagaId, sagaType);
    }

    @Override
    public void claimCompensating(
        SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
      update(sagaId, sagaType, state, SagaStatus.COMPENSATING, expectedVersion);
    }
  }

  /** Orchestrator that correlates every event to a fixed saga id (no start events). */
  static class CorrelatingOrchestrator implements SagaOrchestrator<FakeState> {
    private final SagaId fixedSagaId;

    CorrelatingOrchestrator(SagaId fixedSagaId) {
      this.fixedSagaId = fixedSagaId;
    }

    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.RUNNING, sagaId.value());
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
      return Optional.of(fixedSagaId);
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  @Test
  void correlatedCasConflict_recordsAndStillPropagates() {
    SagaId sagaId = SagaId.of("saga-correlated-1");
    var underlying = new InMemorySagaStore();
    underlying.create(
        sagaId,
        SagaType.fromClass(FakeState.class),
        new FakeState(SagaStatus.RUNNING, "1"),
        SagaStatus.RUNNING);
    var conflictingStore = new CasConflictingSagaStore(underlying);

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(new CorrelatingOrchestrator(sagaId))
            .sagaStore(conflictingStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .metrics(metrics)
            .build();

    var event = startEnvelope("whatever"); // correlate() ignores content, always matches sagaId

    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(event)))
        .isInstanceOf(OptimisticLockException.class);

    assertThat(metrics.sagaCasConflict)
        .containsExactly(SagaType.fromClass(FakeState.class).value());
  }

  @Test
  void throwingMetrics_doesNotCorruptCasConflictPropagation() {
    SagaId sagaId = SagaId.of("saga-correlated-throwing-metrics");
    var underlying = new InMemorySagaStore();
    underlying.create(
        sagaId,
        SagaType.fromClass(FakeState.class),
        new FakeState(SagaStatus.RUNNING, "1"),
        SagaStatus.RUNNING);
    var conflictingStore = new CasConflictingSagaStore(underlying);

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(new CorrelatingOrchestrator(sagaId))
            .sagaStore(conflictingStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .metrics(new ThrowingMetrics())
            .build();

    var event = startEnvelope("whatever"); // correlate() ignores content, always matches sagaId

    // A throwing metrics backend must NOT replace the OptimisticLockException that the batch-retry
    // contract depends on — the CAS conflict must still be the exception observed by the caller.
    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(event)))
        .isInstanceOf(OptimisticLockException.class);
  }

  // ======================================================================
  // Test case 4: timeout claim-loss records CAS conflict, silently skips
  // ======================================================================

  static class StaleClaimSagaStore implements SagaStore {
    private final InMemorySagaStore delegate;

    StaleClaimSagaStore(InMemorySagaStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
      delegate.createFaulted(sagaId, sagaType, initialState);
    }

    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      delegate.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied);
    }

    @Override
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return delegate.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      delegate.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public void update(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      // Simulates: the event path won the claim-write race after our load.
      throw new OptimisticLockException("simulated claim-write conflict");
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
      return delegate.findTimedOut(sagaType, updatedBefore, limit);
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return delegate.findByStatus(sagaType, status, limit);
    }

    @Override
    public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
        SagaId sagaId, SagaType sagaType, Class<S> stateType) {
      return delegate.load(sagaId, sagaType, stateType);
    }

    @Override
    public void delete(SagaId sagaId, SagaType sagaType) {
      delegate.delete(sagaId, sagaType);
    }

    @Override
    public void claimCompensating(
        SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
      update(sagaId, sagaType, state, SagaStatus.COMPENSATING, expectedVersion);
    }
  }

  static class NoTimeoutOrchestrator implements org.streamrune.core.saga.SagaDecider<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.RUNNING, sagaId.value());
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new DoWork(state.id()), AggregateId.of(state.id())));
    }

    @Override
    public Optional<java.time.Duration> timeout() {
      return Optional.of(java.time.Duration.ofMinutes(5));
    }
  }

  @Test
  void timeoutClaimLost_recordsCasConflict() {
    SagaId sagaId = SagaId.of("saga-timeout-claim-loss");
    SagaType sagaType = SagaType.fromClass(FakeState.class);
    var underlying = new InMemorySagaStore();
    underlying.create(sagaId, sagaType, new FakeState(SagaStatus.RUNNING, "1"), SagaStatus.RUNNING);
    var staleStore = new StaleClaimSagaStore(underlying);

    var timeoutRunner =
        SagaTimeoutRunner.<FakeState>builder()
            .decider(new NoTimeoutOrchestrator())
            .sagaStore(staleStore)
            .commandBus(commandBus)
            .sagaType(sagaType)
            .metrics(metrics)
            .build();

    // Must not throw (timeout runner catches its own OLEs) and must skip silently.
    timeoutRunner.processTimedOutSaga(sagaId);

    assertThat(commandBus.executed)
        .as("losing the claim CAS must prevent any compensation dispatch")
        .isEmpty();
    assertThat(metrics.sagaCasConflict)
        .containsExactly(SagaType.fromClass(FakeState.class).value());
  }

  // ======================================================================
  // Test case 5: inbox replay hit recorded only on the second (deduplicated) call
  // ======================================================================

  record ReserveStock(String itemId) implements Command {}

  sealed interface StockEvent extends DomainEvent {
    record StockReserved(String itemId) implements StockEvent {}
  }

  record StockState(int reserved) implements AggregateState {}

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

  @Test
  void inboxReplayHit_recordedOnPreCheckHit() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .metrics(metrics)
            .register(
                TYPE, ReserveStock.class, cmd -> AggregateId.of(cmd.itemId()), new StockDecider())
            .build();

    var key = IdempotencyKey.of("test-key-1");
    var command = new ReserveStock("item-1");

    bus.execute(command, key);
    assertThat(metrics.inboxReplayHit).as("first execution must not record a replay hit").isEmpty();

    bus.execute(command, key);
    assertThat(metrics.inboxReplayHit)
        .as("second (deduplicated) execution must record a replay hit")
        .containsExactly("ReserveStock");
  }

  // ======================================================================
  // Test case 6: sweeper rows recorded only when > 0
  // ======================================================================

  @Test
  void inboxSweeper_recordsRowsOnlyWhenGreaterThanZero() {
    var deleteCount = new java.util.concurrent.atomic.AtomicInteger(0);
    var inbox =
        new org.streamrune.core.CommandInbox() {
          @Override
          public Optional<InboxResult> find(IdempotencyKey key) {
            return Optional.empty();
          }

          @Override
          public int deleteProcessedBefore(Instant cutoff) {
            return deleteCount.get();
          }
        };

    var sweeper =
        new InboxRetentionSweeper(
            inbox,
            java.time.Duration.ofDays(7),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics);

    deleteCount.set(0);
    sweeper.sweepOnce();
    assertThat(metrics.inboxSwept).as("zero rows must not be recorded").isEmpty();

    deleteCount.set(4);
    sweeper.sweepOnce();
    assertThat(metrics.inboxSwept).containsExactly(4L);
  }

  @Test
  void outboxSweeper_recordsRowsOnlyWhenGreaterThanZero() {
    var deleteCount = new java.util.concurrent.atomic.AtomicInteger(0);
    var outboxStore =
        new org.streamrune.core.outbox.OutboxStore() {
          @Override
          public void save(org.streamrune.core.outbox.OutboxEntry e) {}

          @Override
          public List<org.streamrune.core.outbox.OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          @Override
          public String claimedBy() {
            return "test";
          }

          @Override
          public boolean markDelivered(
              org.streamrune.core.outbox.OutboxEntryId id, String claimedBy) {
            return true;
          }

          @Override
          public boolean markFailed(
              org.streamrune.core.outbox.OutboxEntryId id,
              int attempts,
              String error,
              String claimedBy) {
            return true;
          }

          @Override
          public boolean markRetry(
              org.streamrune.core.outbox.OutboxEntryId id,
              int attempts,
              String error,
              java.time.Duration backoff,
              String claimedBy) {
            return true;
          }

          @Override
          public void delete(org.streamrune.core.outbox.OutboxEntryId id) {}

          @Override
          public List<org.streamrune.core.outbox.OutboxEntry> findByStatus(
              org.streamrune.core.outbox.OutboxStatus status, int limit) {
            return List.of();
          }

          @Override
          public boolean resetFailedToPending(org.streamrune.core.outbox.OutboxEntryId id) {
            return false;
          }

          @Override
          public java.util.Optional<org.streamrune.core.outbox.OutboxEntry> findById(
              org.streamrune.core.outbox.OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          @Override
          public boolean skipFailed(
              org.streamrune.core.outbox.OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          @Override
          public int deleteDelivered(Instant olderThan) {
            return deleteCount.get();
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };

    var sweeper =
        new OutboxRetentionSweeper(
            outboxStore,
            java.time.Duration.ofDays(7),
            java.time.Duration.ZERO,
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics);

    deleteCount.set(0);
    sweeper.sweepOnce();
    assertThat(metrics.outboxSwept).as("zero rows must not be recorded").isEmpty();

    deleteCount.set(6);
    sweeper.sweepOnce();
    assertThat(metrics.outboxSwept).containsExactly(6L);
  }
}

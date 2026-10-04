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
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
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
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Verifies the per-event poison guard in {@link SagaRunner}: a poison event (orchestrator
 * pure-logic exception) is quarantined and its saga is marked FAULTED, while other sagas in the
 * same batch continue to be processed normally. Infrastructure failures (SagaStore) propagate out —
 * they are NOT quarantined.
 */
class SagaRunnerPoisonIsolationTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ======================================================================
  // Minimal domain model
  // ======================================================================

  /** Start event — triggers saga creation. */
  record Start(String id) implements DomainEvent {}

  /** A non-start correlated event used to test correlate-poison. */
  record Stray(String correlationId) implements DomainEvent {}

  /** Command emitted by handle(). */
  record DoWork(String id) implements Command {}

  // ======================================================================
  // Minimal saga state
  // ======================================================================

  record FakeState(SagaStatus status, String id) implements SagaState {}

  // ======================================================================
  // Configurable fake orchestrator
  // ======================================================================

  /**
   * Orchestrator whose {@code handle()} can be configured to throw for a specific saga id. All
   * other methods are well-behaved. {@code correlate()} throws for {@link Stray} events (to test
   * correlate-poison path).
   */
  static class FakeOrchestrator implements SagaOrchestrator<FakeState> {

    /** If non-null, handle() throws for this saga id. */
    SagaId handleThrowsFor = null;

    /** If non-null, initialState() throws for this saga id. */
    SagaId initialStateThrowsFor = null;

    /** If true, correlate() throws for any Stray event. */
    boolean correlateThrows = false;

    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      if (initialStateThrowsFor != null && initialStateThrowsFor.equals(sagaId)) {
        throw new RuntimeException("simulated initialState failure for saga " + sagaId.value());
      }
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
      if (correlateThrows && event.event() instanceof Stray) {
        throw new RuntimeException("simulated correlate failure");
      }
      // Correlate via the event's correlationId metadata
      String corrId = event.metadata().correlationId().value();
      if (corrId.startsWith("saga-")) return Optional.of(SagaId.of(corrId));
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
  // Recording CommandBus
  // ======================================================================

  static class RecordingCommandBus implements CommandBus {
    final List<Command> executed = new ArrayList<>();

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

  private EventEnvelope strayEnvelope() {
    Stray event = new Stray("no-saga");
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
            CorrelationId.of("no-saga-correlation"),
            null,
            null,
            Instant.now()));
  }

  // ======================================================================
  // Test fixtures
  // ======================================================================

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore sagaDlq;
  RecordingCommandBus commandBus;
  FakeOrchestrator orchestrator;

  SagaId sagaIdA;
  SagaId sagaIdB;

  EventEnvelope startA;
  EventEnvelope startB;

  @BeforeEach
  void setUp() {
    offsetCounter = 1;
    sagaStore = new InMemorySagaStore();
    sagaDlq = new InMemorySagaDeadLetterStore(sagaStore);
    commandBus = new RecordingCommandBus();
    orchestrator = new FakeOrchestrator();

    sagaIdA = SagaId.of("saga-A");
    sagaIdB = SagaId.of("saga-B");
    startA = startEnvelope("A");
    startB = startEnvelope("B");
  }

  private SagaRunner<FakeState> buildRunner() {
    return SagaRunner.<FakeState>builder()
        .orchestrator(orchestrator)
        .sagaStore(sagaStore)
        .commandBus(commandBus)
        .sagaDeadLetterStore(sagaDlq)
        .build();
  }

  // ======================================================================
  // Tests
  // ======================================================================

  /**
   * A batch containing a poison event (handle throws for A) and a healthy event (B). A must be
   * quarantined and faulted; B must still be processed; onEvents must not throw.
   */
  @Test
  void poisonEvent_isQuarantined_sagaFaulted_otherSagasStillProcessed() {
    orchestrator.handleThrowsFor = sagaIdA;

    var runner = buildRunner();
    runner.asEventListener().onEvents(List.of(startA, startB));

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

    // onEvents did not throw (implicit: the call above returned without exception)
  }

  /**
   * {@code saga_dead_letters.error_message} is a persisted-text sink. The fake orchestrator embeds
   * the saga id — extracted from event data, bounded in length but not in charset — in its message,
   * exactly the shape a real orchestrator's message takes; the recorded entry carries the sanitized
   * text. Crash/rerun: the text is a pure function of the cause, so a re-quarantine of the same
   * event writes the same message and the one-record-per-event upsert still converges.
   */
  @Test
  void quarantinedErrorMessage_isPersistedSanitized() {
    String rawId = "X\n2026-10-02 INFO saga - completed\u0000";
    SagaId poisoned = SagaId.of("saga-" + rawId);
    orchestrator.handleThrowsFor = poisoned;

    buildRunner().asEventListener().onEvents(List.of(startEnvelope(rawId)));

    var entries = sagaDlq.findBySaga(poisoned);
    assertThat(entries).hasSize(1);
    assertThat(entries.getFirst().errorMessage())
        .isEqualTo(
            LogSanitizer.sanitizeFreeText("simulated handle failure for saga " + poisoned.value()))
        .doesNotContain("\n")
        .doesNotContain("\u0000");
  }

  /**
   * A deterministic throw from {@code initialState()} on a start event must be treated as a poison
   * event — quarantined, batch continues — not escape as an infra failure and wedge the whole
   * subscription (permanent head-of-line blocking). A must be quarantined; B must still be
   * processed; onEvents must not throw.
   */
  @Test
  void initialStatePoison_isQuarantined_otherSagasStillProcessed() {
    orchestrator.initialStateThrowsFor = sagaIdA;

    var runner = buildRunner();
    // Must NOT throw: a deterministic initialState() throw is a poison event, not an infra failure.
    runner.asEventListener().onEvents(List.of(startA, startB));

    // A's event is quarantined (dead-letter written before markFaulted runs)
    assertThat(sagaDlq.findBySaga(sagaIdA)).hasSize(1);

    // A has no persisted row: initialState() throws again on the FAULTED-row create path, which is
    // guarded to skip the create (the poison is already recorded) rather than wedge.
    assertThat(sagaStore.load(sagaIdA, SagaType.fromClass(FakeState.class), FakeState.class))
        .isEmpty();

    // B was processed (no head-of-line block)
    assertThat(
            sagaStore
                .load(sagaIdB, SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  /**
   * An infrastructure failure (SagaStore.create/update throws) must propagate out of onEvents so
   * the subscription retries. It must NOT be quarantined.
   */
  @Test
  void infraFailure_propagates_andIsNotQuarantined() {
    // Delegating store that throws on create() to simulate an infra failure
    var failingStore =
        new SagaStore() {
          private final InMemorySagaStore delegate = new InMemorySagaStore();

          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new RuntimeException("simulated infra failure");
          }

          /** The start path's first durable write is the genesis-pending create. */
          @Override
          public void createGenesisPending(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new RuntimeException("simulated infra failure");
          }

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {
            throw new RuntimeException("simulated infra failure");
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

          @Override
          public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
            throw new UnsupportedOperationException("createFaulted");
          }

          @Override
          public void applyEvent(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion,
              AppliedEvent applied) {
            throw new UnsupportedOperationException("applyEvent");
          }

          @Override
          public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
            throw new UnsupportedOperationException("markFaulted");
          }

          @Override
          public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
            throw new UnsupportedOperationException("setDeadLetterPending");
          }
        };

    // Bound to the runner's own saga store, so the dead-letter store sees the rows under test.
    var dlq = new InMemorySagaDeadLetterStore(failingStore);

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(orchestrator)
            .sagaStore(failingStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .build();

    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(startA)))
        .isInstanceOf(RuntimeException.class);

    // NOT quarantined — infra failures propagate, not quarantine
    assertThat(dlq.findAll(10)).isEmpty();
  }

  /**
   * When {@code correlate()} throws for a non-start event, the entry is quarantined with a null
   * sagaId (saga could not be identified). onEvents must not throw.
   */
  @Test
  void correlatePoison_quarantinesWithNullSaga() {
    orchestrator.correlateThrows = true;

    var runner = buildRunner();
    var strayEvent = strayEnvelope();
    runner.asEventListener().onEvents(List.of(strayEvent));

    assertThat(sagaDlq.findAll(10)).hasSize(1);
    assertThat(sagaDlq.findAll(10).get(0).sagaId()).isNull();
  }

  /**
   * A throwing {@code compensate()} is a deterministic orchestrator bug — it must FAULT the saga
   * and quarantine the triggering event (Slice-4 compensate-guard), instead of wedging the
   * subscription. The healthy saga in the same batch must still be processed.
   *
   * <p>Setup: command bus throws for the primary command of saga-A (to trigger the compensate
   * path), AND orchestrator.compensate() throws an IllegalStateException for saga-A. Saga-B's event
   * is in the same batch and must be processed normally.
   */
  @Test
  void throwingCompensator_faultsSaga_insteadOfWedging() {
    // Bus that throws for saga-A's primary command only
    var throwingBus =
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
              throw new RuntimeException("primary command failed for A");
            }
            // B succeeds
            return new CommandResult(
                List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
          }
        };

    // Orchestrator whose compensate() throws for saga-A
    var throwingCompensateOrchestrator =
        new FakeOrchestrator() {
          @Override
          public List<SagaCommand> compensate(
              FakeState state, Throwable failure, SagaCommand failedCommand) {
            if ("A".equals(state.id())) {
              throw new IllegalStateException("compensator bug");
            }
            return List.of();
          }
        };

    var runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(throwingCompensateOrchestrator)
            .sagaStore(sagaStore)
            .commandBus(throwingBus)
            .sagaDeadLetterStore(sagaDlq)
            .build();

    // Batch: saga-A's poison event first, then saga-B's healthy event
    runner.asEventListener().onEvents(List.of(startA, startB));

    // Triggering event for saga-A must be quarantined
    assertThat(sagaDlq.findBySaga(sagaIdA))
        .as("triggering event for saga-A must be quarantined")
        .hasSize(1);

    // Saga-A must be FAULTED
    assertThat(
            sagaStore
                .load(sagaIdA, SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .as("saga-A must be FAULTED after throwing compensate()")
        .isEqualTo(SagaStatus.FAULTED);

    // Saga-B must still have been processed (no head-of-line block)
    assertThat(sagaStore.load(sagaIdB, SagaType.fromClass(FakeState.class), FakeState.class))
        .as("saga-B must be present (processed despite saga-A's poison)")
        .isPresent();
    assertThat(
            sagaStore
                .load(sagaIdB, SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .as("saga-B must be in RUNNING state (healthy processing)")
        .isEqualTo(SagaStatus.RUNNING);
  }
}

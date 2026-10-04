package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;
import static org.streamrune.runtime.SagaStartFixtures.startEvent;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaStartFixtures.ScriptedBus;
import org.streamrune.runtime.SagaStartFixtures.StartOrchestrator;
import org.streamrune.runtime.SagaStartFixtures.StartState;
import org.streamrune.test.InMemorySagaStore;

/** Every crash point on the genesis path converges on rerun. */
class SagaGenesisCrashPointTest {
  private final SagaId sagaId = SagaId.of("saga-1");

  private SagaStepExecutor<StartState> executor(ForwardingSagaStore store, ScriptedBus bus) {
    return new SagaStepExecutor<>(new StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
  }

  @Test
  void crashAfterCreateGenesisPending_rerunRedrivesFromInitialState_andCommitsOnce() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec = executor(store, bus);
    store.crashOnceAfter("createGenesisPending");
    var event = startEvent(3L);

    Throwable crash =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    var pending = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(pending.genesisApplied()).as("the intent row exists before any dispatch").isFalse();
    assertThat(pending.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(bus.count("step-1")).isZero();

    StepOutcome rerun =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(rerun).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
    var applied = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(applied.genesisApplied()).isTrue();
    assertThat(applied.version()).isEqualTo(2L);
    assertThat(applied.lastAppliedOffset()).isEqualTo(3L);
  }

  @Test
  void retryLaterMidDispatch_leavesTheGenesisPendingRow_rerunDedupsTheCommittedIndex() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec = executor(store, bus);
    bus.failOnce("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var event = startEvent(3L);

    Throwable first =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(first).isInstanceOf(CircuitBreakerOpenException.class);
    var pending = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(pending.genesisApplied()).isFalse();
    assertThat(pending.version())
        .as("no escape write, no CAS: the intent row is untouched")
        .isEqualTo(1L);

    StepOutcome rerun =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(rerun).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).as("index 0 dedups on its offset-stable key").isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
    assertThat(store.load(sagaId, TYPE, StartState.class).orElseThrow().genesisApplied()).isTrue();
  }

  @Test
  void crashAfterApplyEvent_rerunIsSkippedDedup_nothingDispatchedTwice() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec = executor(store, bus);
    store.crashOnceAfter("applyEvent");
    var event = startEvent(3L);

    Throwable crash =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);

    StepOutcome rerun =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(rerun).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
  }

  @Test
  void concurrentCreate_reloadsAndRoutesOnTheRecordedRow() {
    var inner = new InMemorySagaStore();
    var store = new ForwardingSagaStore(inner);
    var bus = new ScriptedBus();
    var exec = executor(store, bus);
    var event = startEvent(3L);
    // A competing deliverer already created the intent row and committed the genesis.
    inner.createGenesisPending(
        sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    inner.applyEvent(
        sagaId,
        TYPE,
        new StartState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING,
        1L,
        AppliedEvent.liveStart(event.globalOffset()));

    StepOutcome outcome =
        exec.execute(sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false));

    assertThat(outcome)
        .as("createGenesisPending conflicted; the reload sees an applied genesis")
        .isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.count("step-1")).isZero();
  }

  /**
   * The concurrent-create race's other half: the conflicting row vanished before the reload
   * (created and deleted underneath us — an operator delete racing the delivery). Nothing recorded
   * can route the event, so the conflict propagates and the subscription redelivers; nothing was
   * dispatched.
   */
  @Test
  void conflictWithNoRowOnReload_propagatesTheConflict_nothingDispatched() {
    var store =
        new ForwardingSagaStore(new InMemorySagaStore()) {
          @Override
          public void createGenesisPending(
              SagaId id, SagaType t, SagaState s, SagaStatus st, boolean deadLetterPending) {
            throw new OptimisticLockException("simulated concurrent create, then delete");
          }
        };
    var bus = new ScriptedBus();
    var exec = executor(store, bus);

    Throwable thrown =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    Optional.empty(),
                    new SagaTrigger.ForwardStep(startEvent(3L), true, false)));

    assertThat(thrown).isInstanceOf(OptimisticLockException.class);
    assertThat(bus.count("step-1")).as("the intent row precedes every dispatch").isZero();
    assertThat(store.load(sagaId, TYPE, StartState.class)).isEmpty();
  }

  @Test
  void faultedGenesisPendingRow_liveStartIsSkipped_replayRedriveReEvolvesAndClearsTheFault() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec = executor(store, bus);
    store.createFaulted(sagaId, TYPE, new StartState(SagaStatus.STARTED, null));
    var event = startEvent(3L);

    StepOutcome live =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));
    assertThat(live).isEqualTo(StepOutcome.SKIPPED_HALTED);
    assertThat(bus.count("step-1")).isZero();

    StepOutcome replay =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, true));

    assertThat(replay).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    var row = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.lastReplayedOffset()).isEqualTo(3L);
    assertThat(bus.count("step-1")).isEqualTo(1);
  }

  // =========================================================================
  // DONE at genesis — a start event whose evolve is already terminal
  // =========================================================================

  /** {@link StartOrchestrator} whose evolve of the start event is already {@code COMPLETED}. */
  static final class DoneAtGenesisOrchestrator implements SagaOrchestrator<StartState> {
    private final StartOrchestrator start = new StartOrchestrator();

    @Override
    public Class<StartState> stateType() {
      return StartState.class;
    }

    @Override
    public StartState initialState(SagaId sagaId) {
      return start.initialState(sagaId);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return start.isStartEvent(event);
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return start.extractSagaId(event);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return start.correlate(event);
    }

    @Override
    public StartState evolve(StartState state, EventEnvelope event) {
      return new StartState(SagaStatus.COMPLETED, "order-1");
    }

    @Override
    public List<SagaCommand> handle(StartState state, EventEnvelope event) {
      return start.handle(state, event);
    }

    @Override
    public List<SagaCommand> compensate(
        StartState state, Throwable failure, SagaCommand failedCommand) {
      return start.compensate(state, failure, failedCommand);
    }
  }

  @Test
  void terminalEvolveOnAnAbsentRow_isCreatedCompleteAtGenesis_nothingDispatched() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new DoneAtGenesisOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var event = startEvent(3L);

    StepOutcome first =
        exec.execute(sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false));

    assertThat(first).isEqualTo(StepOutcome.COMPLETED);
    var row = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.version())
        .as(
            "create(state, COMPLETED): the row is complete from the start — no genesis-pending"
                + " insert, no CAS (a CAS would refuse the terminal row)")
        .isEqualTo(1L);
    assertThat(row.status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(row.genesisApplied()).isTrue();
    assertThat(bus.count("step-1")).as("nothing to dispatch at a terminal genesis").isZero();

    StepOutcome redelivered =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(redelivered).isEqualTo(StepOutcome.SKIPPED_HALTED);
    assertThat(bus.count("step-1")).isZero();
  }

  /** The DONE-at-genesis create is a durable write: a crash right after it converges on rerun. */
  @Test
  void crashAfterTheCompleteCreate_rerunIsSkippedHalted() {
    var store = new ForwardingSagaStore(new InMemorySagaStore());
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new DoneAtGenesisOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    store.crashOnceAfter("create");
    var event = startEvent(3L);

    Throwable crash =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);

    StepOutcome rerun =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(rerun).isEqualTo(StepOutcome.SKIPPED_HALTED);
    var row = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(bus.count("step-1")).isZero();
  }

  /** A concurrent deliverer won the DONE-at-genesis create: the conflict is dedup, not an error. */
  @Test
  void concurrentCompleteCreate_isSwallowedAsDedup() {
    var inner = new InMemorySagaStore();
    var store = new ForwardingSagaStore(inner);
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new DoneAtGenesisOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    inner.create(
        sagaId, TYPE, new StartState(SagaStatus.COMPLETED, "order-1"), SagaStatus.COMPLETED);

    StepOutcome outcome =
        exec.execute(
            sagaId, Optional.empty(), new SagaTrigger.ForwardStep(startEvent(3L), true, false));

    assertThat(outcome).isEqualTo(StepOutcome.COMPLETED);
    assertThat(store.load(sagaId, TYPE, StartState.class).orElseThrow().version()).isEqualTo(1L);
    assertThat(bus.count("step-1")).isZero();
  }
}

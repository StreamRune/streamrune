package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTimeoutException;
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
import org.streamrune.test.InMemorySagaStore;

class SagaStepExecutorTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  @Test
  void triggerRecords_carryTheirPathSpecificData() {
    var forward = new SagaTrigger.ForwardStep(null, true, false);
    assertThat(forward.startPath()).isTrue();
    assertThat(forward.replayRedrive()).isFalse();

    var cause = new RuntimeException("boom");
    assertThat(new SagaTrigger.ResumeCompensation(cause).resumeCause()).isSameAs(cause);
    assertThat(new SagaTrigger.ClaimTimeout(cause).timeoutCause()).isSameAs(cause);
  }

  @Test
  void stepOutcome_hasEveryCaseTheFiveCallersNeed() {
    assertThat(StepOutcome.values())
        .containsExactlyInAnyOrder(
            StepOutcome.FORWARD_PROGRESSED,
            StepOutcome.COMPLETED,
            StepOutcome.COMPENSATING_LEFT,
            StepOutcome.COMPENSATING_REFUSED, // refused-admission re-drive, not an attempt
            StepOutcome.COMPENSATED,
            StepOutcome.FAILED,
            StepOutcome.FAULTED,
            StepOutcome.CLAIM_LOST,
            StepOutcome.SKIPPED_HALTED,
            StepOutcome.SKIPPED_DEDUP,
            StepOutcome.SKIPPED_NO_ROW, // a correlated event on an absent row
            StepOutcome.NOT_COMPENSATING);
  }

  // ---- ForwardStep forward-only path ------------------------------------------------

  private SagaStepExecutor<FakeSagaState> newExecutor(SagaStore store, DedupingCommandBus bus) {
    return new SagaStepExecutor<>(new FakeOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
  }

  @Test
  void forwardStep_freshStart_dispatchesForwardCommands_andCreatesRunningSaga() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    var start = startEnvelope("x", 0L);

    StepOutcome outcome =
        exec.execute(sagaId, Optional.empty(), new SagaTrigger.ForwardStep(start, true, false));

    assertThat(outcome).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.RUNNING);
    // createGenesisPending at v1 BEFORE the first dispatch, applyEvent commits the
    // genesis at v2.
    assertThat(saved.version()).isEqualTo(2L);
    assertThat(saved.genesisApplied()).isTrue();
    assertThat(bus.execCount("do-1")).isEqualTo(1); // Charge ran
  }

  @Test
  void forwardStep_startPath_existingNonReplayRow_isDedupSkipped() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    var loaded = store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class);

    StepOutcome outcome =
        exec.execute(
            sagaId, loaded, new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, false));

    assertThat(outcome).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.execCount("do-1")).isZero();
  }

  @Test
  void forwardStep_startPath_replayRedrive_evolvesFromInitialState_notLoadedState() {
    // Regression pin: a replay-redrive of a start event against an existing RUNNING row must
    // evolve from initialState (a start event is the saga's genesis event), NOT from the
    // partially-progressed loaded state — the recorded state on a genesis-pending row exists for
    // the timeout runner's compensation, never as a resume point. The row's version is used ONLY
    // for the CAS. a genesis-pending row is the only row a replay re-drives — an
    // APPLIED genesis (store.create) correctly dedups instead — so the seed is
    // createGenesisPending.
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    // Seed a genesis-pending RUNNING row whose state has advanced past initialState (counter=5).
    store.createGenesisPending(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value(), 5),
        SagaStatus.RUNNING,
        false);
    var loaded = store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class);

    StepOutcome outcome =
        exec.execute(
            sagaId, loaded, new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, true));

    assertThat(outcome).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.RUNNING);
    // Correct (evolve from initialState counter=0 -> 1); the pre-fix bug evolves from the loaded
    // state counter=5 -> 6.
    assertThat(saved.state().counter()).isEqualTo(1);
  }

  @Test
  void forwardStep_haltedRow_isSkipped() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.COMPLETED, sagaId.value()),
        SagaStatus.COMPLETED);
    var loaded = store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class);

    StepOutcome outcome =
        exec.execute(
            sagaId, loaded, new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 1L), false, false));

    assertThat(outcome).isEqualTo(StepOutcome.SKIPPED_HALTED);
  }

  // ---- claim-first compensation + COMPENSATING resume routing ------------------------

  @Test
  void forwardStep_correlatedFailure_claimsFirst_thenCompensates_toCOMPENSATED() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus(); // undo-1 succeeds
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    // seed a RUNNING saga at v1 so this is the correlated path (loaded present, startPath=false)
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    bus.failTransientlyNTimes("do-2", 1); // Reserve fails -> compensate

    // A correlated Start-shaped event that re-emits [Charge, Reserve]; Reserve fails once.
    var evt = startEnvelope("x", 5L);
    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ForwardStep(evt, false, false));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-1")).isEqualTo(1);
  }

  @Test
  void forwardStep_onCompensatingRow_resumesInsteadOfReRunningForward() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING); // claimed at v1

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, false));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED); // resumed to terminal
    assertThat(bus.execCount("do-2")).isZero(); // forward NEVER re-ran
    assertThat(bus.execCount("undo-1")).isEqualTo(1);
  }

  @Test
  void forwardStep_correlatedLostClaim_propagatesOptimisticLock() {
    // correlated path: a stale loaded version -> claim update() throws -> executor rethrows (batch
    // retry)
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    var stale =
        store.load(
            sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class); // version 1
    // advance the row so the claim CAS at v1 will lose
    store.update(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING,
        1L); // now v2
    bus.failTransientlyNTimes("do-2", 1);

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                exec.execute(
                    sagaId,
                    stale,
                    new SagaTrigger.ForwardStep(startEnvelope("x", 5L), false, false)))
        .isInstanceOf(org.streamrune.core.OptimisticLockException.class);
    assertThat(bus.execCount("undo-1")).as("lost claim dispatches ZERO compensation").isZero();
  }

  // ---- explicit ResumeCompensation trigger -------------------------------------------

  @Test
  void resumeCompensation_reDrivesAtCurrentVersion_toTerminal() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.execCount("undo-1")).isEqualTo(1);
  }

  @Test
  void resumeCompensation_nonCompensatingRow_isNotCompensating() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(outcome).isEqualTo(StepOutcome.NOT_COMPENSATING);
  }

  @Test
  void resumeCompensation_throwingCompensate_faultsTheSaga_noThrow() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec =
        new SagaStepExecutor<>(
            new ThrowingCompensateOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(outcome).isEqualTo(StepOutcome.FAULTED);
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAULTED);
  }

  // ---- explicit ClaimTimeout trigger -------------------------------------------------

  @Test
  void claimTimeout_claimsThenCompensates_atEpisodeVersionPlusOne() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING); // v1

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ClaimTimeout(
                new SagaTimeoutException(sagaId, java.time.Duration.ofSeconds(1))));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(saved.version()).isEqualTo(3L); // claim (v2) + terminal (v3)
    assertThat(bus.execCount("undo-1")).isEqualTo(1);
  }

  @Test
  void claimTimeout_lostClaim_dispatchesNothing_returnsClaimLost() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    var stale =
        store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class); // v1
    store.update(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING,
        1L); // now v2

    StepOutcome outcome =
        exec.execute(
            sagaId,
            stale,
            new SagaTrigger.ClaimTimeout(
                new SagaTimeoutException(sagaId, java.time.Duration.ofSeconds(1))));

    assertThat(outcome).isEqualTo(StepOutcome.CLAIM_LOST);
    assertThat(bus.execCount("undo-1")).isZero();
  }

  // ---- shared harness (modeled on SagaStartEventClaimFirstCrashTest) -------------------------
  // Start emits [Charge(do-1), Reserve(do-2)]; Reserve fails transiently; compensate -> [Refund].

  record Start(String id) implements DomainEvent {}

  record Poke(String id) implements DomainEvent {}

  interface TestCommand extends Command {
    String cmdId();
  }

  /** Forward: charge payment. */
  record Charge(String cmdId) implements TestCommand {}

  /** Forward: reserve stock (fails transiently on the first attempt). */
  record Reserve(String cmdId) implements TestCommand {}

  /** Compensation: refund the charge. */
  record Refund(String cmdId) implements TestCommand {}

  /**
   * {@code counter} makes {@code evolve}'s prior-state input observable: evolving a start event
   * from {@code initialState} (counter=0) lands at a different counter than evolving it on top of a
   * partially-progressed loaded state — the pin for the start-path prior-state fix. The 2-arg
   * constructor keeps the pre-existing call sites (counter defaults to 0).
   */
  record FakeSagaState(SagaStatus status, String id, int counter) implements SagaState {
    FakeSagaState(SagaStatus status, String id) {
      this(status, id, 0);
    }
  }

  static class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {
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
      String corr = event.metadata().correlationId().value();
      return corr.startsWith("saga-") ? Optional.of(SagaId.of(corr)) : Optional.empty();
    }

    @Override
    public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
      if (event.event() instanceof Start s) {
        // Increment the prior state's counter so the evolve prior-state is observable:
        // from initialState(counter=0) -> 1; from a loaded state(counter=5) -> 6.
        return new FakeSagaState(SagaStatus.RUNNING, "saga-" + s.id(), state.counter() + 1);
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
      if (state.status() == SagaStatus.RUNNING && event.event() instanceof Start) {
        return List.of(
            SagaCommand.of(new Charge("do-1"), AggregateId.of(state.id())),
            SagaCommand.of(new Reserve("do-2"), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new Refund("undo-1"), AggregateId.of(state.id())));
    }
  }

  /** A {@link FakeOrchestrator} whose {@code compensate} throws — drives the FAULT branch. */
  static final class ThrowingCompensateOrchestrator extends FakeOrchestrator {
    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      throw new RuntimeException("bug");
    }
  }

  /**
   * Models the real command inbox: a committed key short-circuits; a thrown command is not keyed.
   */
  static final class DedupingCommandBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> execCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();

    void failTransientlyNTimes(String cmdId, int n) {
      failuresRemaining.put(cmdId, new AtomicInteger(n));
    }

    int execCount(String cmdId) {
      return execCounts.getOrDefault(cmdId, new AtomicInteger(0)).get();
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (committed.contains(key)) {
        return ok();
      }
      String id = ((TestCommand) command).cmdId();
      AtomicInteger remaining = failuresRemaining.get(id);
      if (remaining != null && remaining.get() > 0) {
        remaining.decrementAndGet();
        throw new RuntimeException("transient failure for " + id);
      }
      execCounts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
      committed.add(key);
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  private EventEnvelope startEnvelope(String id, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Start"),
        new Start(id),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("unrelated-correlation"),
            null,
            null,
            Instant.now()));
  }

  private EventEnvelope pokeEnvelope(String sagaCorrelation, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Poke"),
        new Poke("poke"),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-2"),
            null,
            null,
            CorrelationId.of(sagaCorrelation),
            null,
            null,
            Instant.now()));
  }
}

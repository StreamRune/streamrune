package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
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
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Regression (critical): the start-event path must CLAIM the compensation episode (write {@code
 * COMPENSATING} to the saga row) <em>before</em> dispatching any compensation command, mirroring
 * the correlated-event path — both now route through the same {@code SagaStepExecutor} claim-first
 * logic — and {@code SagaTimeoutRunner}.
 *
 * <p><b>The bug:</b> {@code processStartEvent} used to dispatch every compensation command
 * (executing e.g. a refund) and only afterwards {@code create()} the saga row. A crash between the
 * compensation-execute and the persist orphans the refund: on start-event redelivery the empty
 * store re-runs the FULL forward path (the failed forward command now succeeds transiently), the
 * saga proceeds to a running/complete state, and the already-executed refund is never reconciled —
 * customer charged AND refunded AND the order ships (silent money loss).
 *
 * <p><b>Deterministic crash seam:</b> {@link CrashOnTerminalWriteStore} throws {@link
 * EventStoreException} exactly once on the first store write that persists a terminal status
 * ({@code COMPENSATED}/{@code FAILED}/{@code COMPLETED}) — i.e. AFTER the compensation command has
 * executed, BEFORE the saga is durably terminalized. With the claim-first fix, a non-terminal
 * {@code COMPENSATING} claim row was already durably written before the refund ran, so redelivery
 * of the start event is a no-op dedup (the saga stays {@code COMPENSATING}, later resumed) and the
 * forward path never re-runs to completion behind an executed refund.
 */
class SagaStartEventClaimFirstCrashTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ---- domain: Start emits [Charge(do-1), Reserve(do-2)]; Reserve fails transiently once. ----
  record Start(String id) implements DomainEvent {}

  record Poke(String id) implements DomainEvent {}

  interface TestCommand extends Command {
    String cmdId();
  }

  /** Forward: charge payment. */
  record Charge(String cmdId) implements TestCommand {}

  /** Forward: reserve stock (fails transiently on the first attempt). */
  record Reserve(String cmdId) implements TestCommand {}

  /** Compensation: refund the charge. Must never orphan against a running/complete saga. */
  record Refund(String cmdId) implements TestCommand {}

  record FakeSagaState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {
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
        return new FakeSagaState(SagaStatus.RUNNING, "saga-" + s.id());
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
      // Deterministic (pure function of state): the charge must be refunded.
      return List.of(SagaCommand.of(new Refund("undo-1"), AggregateId.of(state.id())));
    }
  }

  /**
   * Models the real command inbox: a committed key short-circuits; a thrown command is not keyed.
   */
  static final class DedupingCommandBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> execCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

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

  /**
   * A {@link SagaStore} decorator that simulates a crash: the first {@code create}/{@code update}
   * that would persist a TERMINAL status throws {@link EventStoreException} instead of writing. A
   * non-terminal status ({@code STARTED}/{@code RUNNING}/{@code COMPENSATING}) is written normally,
   * so the claim-first {@code COMPENSATING} row survives while the subsequent terminal write
   * crashes.
   */
  static final class CrashOnTerminalWriteStore implements SagaStore {
    private final SagaStore delegate;
    private final AtomicBoolean armed = new AtomicBoolean(true);

    CrashOnTerminalWriteStore(SagaStore delegate) {
      this.delegate = delegate;
    }

    private void crashIfTerminal(SagaStatus status) {
      if (status.isTerminal() && armed.compareAndSet(true, false)) {
        throw new EventStoreException("simulated crash before durable terminal write");
      }
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      crashIfTerminal(status);
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void update(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      crashIfTerminal(status);
      delegate.update(sagaId, sagaType, state, status, expectedVersion);
    }

    /**
     * The forward CAS is {@code applyEvent}; it carries the same terminal-status seam as {@code
     * update}, so the fixture keeps meaning "any durable terminal write".
     */
    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      crashIfTerminal(status);
      delegate.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied);
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
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return delegate.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      delegate.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
      return delegate.findTimedOut(sagaType, cutoff, limit);
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return delegate.findByStatus(sagaType, status, limit);
    }

    @Override
    public long countByStatus(SagaType sagaType, SagaStatus status) {
      return delegate.countByStatus(sagaType, status);
    }

    @Override
    public List<CompensatingSaga> findCompensating(
        SagaType sagaType, Instant updatedBefore, int limit) {
      return delegate.findCompensating(sagaType, updatedBefore, limit);
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

  InMemorySagaStore backing;
  CrashOnTerminalWriteStore sagaStore;
  DedupingCommandBus bus;
  SagaRunner<FakeSagaState> runner;
  SagaId sagaId;

  @BeforeEach
  void setUp() {
    backing = new InMemorySagaStore();
    sagaStore = new CrashOnTerminalWriteStore(backing);
    bus = new DedupingCommandBus();
    runner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(backing))
            .build();
    sagaId = SagaId.of("saga-x");
  }

  @Test
  void startEventCompensation_isClaimedBeforeDispatch_soACrashCannotOrphanTheRefund() {
    // Reserve (do-2) fails transiently on the first delivery, then succeeds on redelivery.
    bus.failTransientlyNTimes("do-2", 1);

    // 1) First delivery: Charge ok, Reserve fails -> compensate -> Refund executes. The terminal
    // write (COMPENSATED) then crashes. With claim-first, a COMPENSATING row was durably written
    // BEFORE the refund ran; the crash only loses the terminal write.
    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L))))
        .as("the simulated crash on the terminal write propagates like an infra failure")
        .isInstanceOf(EventStoreException.class);

    assertThat(bus.execCount("undo-1"))
        .as("the refund executed exactly once during the crashed attempt")
        .isEqualTo(1);

    var afterCrash =
        backing.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class);
    assertThat(afterCrash)
        .as(
            "claim-first must have durably written a COMPENSATING row BEFORE dispatching the refund"
                + " — without it the refund is orphaned against a non-existent saga")
        .isPresent();
    assertThat(afterCrash.orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
    // Genesis-pending row at v1 (create-first), the claim-first CAS at v2.
    assertThat(afterCrash.orElseThrow().version()).isEqualTo(2L);

    // 2) Redelivery of the START event (crash seam now disarmed). The saga row already exists
    // (COMPENSATING), so processStartEvent RESUMES the claimed compensation episode
    // instead of skipping it — it must NEVER re-run the forward path. The refund dedups under the
    // shared episode-scoped key (execCount stays 1) and the resume drives the episode to terminal
    // COMPENSATED. (This is the ONLY recovery for a start-path claim crash with no timeout runner
    // and
    // no correlated event to arrive.)
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    var afterRedelivery =
        backing
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(afterRedelivery.status())
        .as("start redelivery RESUMES compensation to terminal COMPENSATED — never re-runs forward")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("do-2"))
        .as("Reserve (the shipped step) must NEVER execute after the charge was refunded")
        .isZero();
    assertThat(bus.execCount("undo-1")).as("the refund must not double-execute").isEqualTo(1);

    // 3) A later correlated event is now a no-op — the saga already reached terminal COMPENSATED
    // via
    // the start-redelivery resume. It must not resurrect the saga or re-dispatch anything.
    runner.asEventListener().onEvents(List.of(pokeEnvelope("saga-x", 1L)));

    assertThat(
            backing
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("the saga stays terminal COMPENSATED")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("the refund still ran exactly once across crash + resume")
        .isEqualTo(1);
    assertThat(bus.execCount("do-2")).as("the shipped step never ran").isZero();
  }

  @Test
  void startRedeliveryResumesAClaimedCompensatingEpisode_theOnlyRecoveryWithNoTimeoutOrSweeper() {
    // A prior delivery's forward command failed, claim-first wrote COMPENSATING at v1,
    // then the process crashed BEFORE the refund was dispatched (onEvents did not complete, so the
    // offset was not committed → the START event is redelivered). With no timeout() and the sweeper
    // disabled, NO correlated event will ever arrive (the failed forward command produced no
    // events),
    // so the redelivered START is the ONLY thing that can resume the episode. Before the fix,
    // processStartEvent's fast-path dedup returned unconditionally and the refund was never issued
    // —
    // the saga wedged COMPENSATING forever.
    var plainRunner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(backing) // no crash decorator — we want the terminal write to succeed
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(backing))
            .build();

    // Seed the crashed state directly: COMPENSATING at v1, refund NOT yet executed.
    backing.create(
        sagaId,
        SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);
    assertThat(bus.execCount("undo-1")).as("no refund has run yet").isZero();

    // Redeliver the START event. processStartEvent must RESUME compensation, not skip it.
    plainRunner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    var after =
        backing
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(after.status())
        .as("start redelivery drives the wedged COMPENSATING episode to terminal COMPENSATED")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("the refund is finally issued on start redelivery")
        .isEqualTo(1);
    assertThat(bus.execCount("do-2")).as("the forward path must never run").isZero();
  }
}

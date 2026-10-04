package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
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
import org.streamrune.core.subscription.SubscriptionLeadership;
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
import org.streamrune.test.MutableClock;

/**
 * Durable, automatic recovery of a stuck {@code COMPENSATING} saga that has <b>no</b> {@link
 * SagaTimeoutRunner} (empty {@code timeout()}) and receives <b>no</b> further correlated event —
 * exactly the topology where a transient compensation failure previously wedged the saga forever
 * with the undo abandoned.
 *
 * <p>The headline test asserts the pre-fix behavior as a fail-first checkpoint (the saga is stuck
 * {@code COMPENSATING} with no path re-driving it), then drives {@link
 * SagaCompensationRetrySweeper#sweepOnce()} and asserts the undo eventually completes and the saga
 * reaches a terminal status — with no timeout runner and no new event.
 */
class SagaCompensationRetrySweeperTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ---- domain ----
  record Start(String id) implements DomainEvent {}

  interface TestCommand extends Command {
    String cmdId();
  }

  record DoWork(String cmdId) implements TestCommand {}

  record Undo(String cmdId) implements TestCommand {}

  record FakeSagaState(SagaStatus status, String id) implements SagaState {}

  /**
   * handle() emits one forward command (do-1) that fails, triggering compensate() → [undo-1,
   * undo-2]. compensate() is a pure function of state (both resume paths require this) and never
   * throws. A {@code throwOnCompensate} flag makes compensate() itself throw, to exercise the
   * poison-during-sweep path.
   */
  static final class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {
    volatile boolean throwOnCompensate = false;

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
      return Optional.empty();
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
        return List.of(SagaCommand.of(new DoWork("do-1"), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      if (throwOnCompensate) {
        throw new IllegalStateException("compensate() bug");
      }
      return List.of(
          SagaCommand.of(new Undo("undo-1"), AggregateId.of(state.id())),
          SagaCommand.of(new Undo("undo-2"), AggregateId.of(state.id())));
    }
  }

  /**
   * A CommandBus modeling the real command inbox: a committed idempotency key short-circuits
   * without re-executing; a handler that throws does NOT commit its key (so a retry re-runs it).
   * Transient failures are configured per command id, either a fixed count or forever.
   */
  static final class DedupingCommandBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> execCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();
    private final java.util.Set<String> failForever = ConcurrentHashMap.newKeySet();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    void failTransientlyNTimes(String cmdId, int n) {
      failuresRemaining.put(cmdId, new AtomicInteger(n));
    }

    void failTransientlyForever(String cmdId) {
      failForever.add(cmdId);
    }

    private final java.util.Set<String> refuseAdmission = ConcurrentHashMap.newKeySet();

    /** The bus REFUSES the command at admission (shutting down) — nothing is attempted. */
    void refuseAdmissionForever(String cmdId) {
      refuseAdmission.add(cmdId);
    }

    void admitAgain(String cmdId) {
      refuseAdmission.remove(cmdId);
    }

    /**
     * Models the {@code InboxRetentionSweeper} pruning every processed inbox row past the retention
     * window: committed idempotency keys are forgotten, so a later re-dispatch under the same key
     * MISSES the inbox and re-executes the handler (the double-refund hazard).
     */
    void pruneCommittedKeys() {
      committed.clear();
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
      if (refuseAdmission.contains(id)) {
        throw new org.streamrune.core.CommandBusClosedException(
            "Command bus is closed, cannot accept new commands");
      }
      if (failForever.contains(id)) {
        throw new RuntimeException("permanent-but-transient-looking failure for " + id);
      }
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

  /** A leadership stub whose leader status is flipped by the test. */
  static final class ToggleLeadership implements SubscriptionLeadership {
    private static final Optional<Lease> LEASE = Optional.of(new Lease(1L));
    volatile boolean leader;
    volatile int resigns = 0;

    ToggleLeadership(boolean leader) {
      this.leader = leader;
    }

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return leader ? LEASE : Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return leader ? LEASE : Optional.empty();
    }

    @Override
    public void resign(String consumerName) {
      resigns++;
    }

    @Override
    public void close() {}
  }

  /** Records saga metric calls for assertions. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final AtomicInteger compensationRetries = new AtomicInteger();
    final AtomicInteger faulted = new AtomicInteger();
    final AtomicInteger casConflicts = new AtomicInteger();
    final Map<String, AtomicInteger> compensationOutcomes = new ConcurrentHashMap<>();
    final Map<String, Long> compensatingBacklog = new ConcurrentHashMap<>();
    final Map<String, Long> faultedRows = new ConcurrentHashMap<>();
    final java.util.concurrent.atomic.AtomicLong faultedBacklog =
        new java.util.concurrent.atomic.AtomicLong(-1);

    @Override
    public void recordSagaCompensationRetry(String sagaType) {
      compensationRetries.incrementAndGet();
    }

    @Override
    public void recordSagaFaulted(String sagaType) {
      faulted.incrementAndGet();
    }

    @Override
    public void recordSagaCasConflict(String sagaType) {
      casConflicts.incrementAndGet();
    }

    @Override
    public void recordSagaCompensation(String sagaType, String outcome) {
      compensationOutcomes.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
    }

    @Override
    public void recordSagaCompensatingBacklog(String sagaType, long count) {
      compensatingBacklog.put(sagaType, count);
    }

    @Override
    public void recordSagaFaultedRows(String sagaType, long count) {
      faultedRows.put(sagaType, count);
    }

    @Override
    public void recordSagaFaultedBacklog(long count) {
      faultedBacklog.set(count);
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

  MutableClock clock;
  InMemorySagaStore sagaStore;
  DedupingCommandBus bus;
  FakeOrchestrator orchestrator;
  SagaRunner<FakeSagaState> runner;
  RecordingMetrics metrics;
  SagaId sagaId;
  InMemorySagaDeadLetterStore deadLetterStore;

  @BeforeEach
  void setUp() {
    clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    sagaStore = new InMemorySagaStore(clock);
    bus = new DedupingCommandBus();
    orchestrator = new FakeOrchestrator();
    metrics = new RecordingMetrics();
    deadLetterStore = new InMemorySagaDeadLetterStore(sagaStore);
    runner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetterStore)
            .metrics(metrics)
            .build();
    sagaId = SagaId.of("saga-x");
  }

  private SagaCompensationRetrySweeper<FakeSagaState> sweeper(
      SubscriptionLeadership leadership, Duration retryInterval, Duration giveUpAfter) {
    return SagaCompensationRetrySweeper.<FakeSagaState>builder()
        .sagaRunner(runner)
        .sagaStore(sagaStore)
        .leadership(leadership)
        .retryInterval(retryInterval)
        .giveUpAfter(giveUpAfter)
        .clock(clock)
        .metrics(metrics)
        .build();
  }

  private SagaCompensationRetrySweeper<FakeSagaState> sweeperWithInboxRetention(
      SubscriptionLeadership leadership,
      Duration retryInterval,
      Duration giveUpAfter,
      Duration inboxRetentionMaxAge) {
    return SagaCompensationRetrySweeper.<FakeSagaState>builder()
        .sagaRunner(runner)
        .sagaStore(sagaStore)
        .leadership(leadership)
        .retryInterval(retryInterval)
        .giveUpAfter(giveUpAfter)
        .inboxRetentionMaxAge(inboxRetentionMaxAge)
        .clock(clock)
        .metrics(metrics)
        .build();
  }

  /** Drives the saga into COMPENSATING with undo-1 already succeeded and undo-2 pending. */
  private void driveIntoStuckCompensating(int undo2Failures) {
    // Forward do-1 fails once → compensate([undo-1, undo-2]); undo-1 succeeds, undo-2 fails
    // transiently `undo2Failures` times → the start-event path persists COMPENSATING at version 2
    // (create-first genesis v1 + the compensation claim v2).
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyNTimes("undo-2", undo2Failures);
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
  }

  @Test
  void liveness_accessorsReflectStartAndClose() {
    // The sweeper exposes isStarted()/isAlive()/consecutiveFailures() so the health
    // contributor can surface a dead/wedged compensation sweeper as DOWN.
    var sweeper = sweeper(new ToggleLeadership(true), Duration.ofMillis(50), Duration.ofHours(1));
    assertThat(sweeper.isStarted()).isFalse();
    assertThat(sweeper.isAlive()).isFalse();
    assertThat(sweeper.consecutiveFailures()).isZero();
    assertThat(sweeper.sagaTypeName()).isEqualTo(SagaType.fromClass(FakeSagaState.class).value());

    sweeper.start();
    assertThat(sweeper.isStarted()).isTrue();
    assertThat(sweeper.isAlive()).isTrue();

    sweeper.close();
    assertThat(sweeper.isStarted()).isFalse();
  }

  @Test
  void close_throwingResign_stillCompletesAndStaysRestartable() {
    // Sweep: close() calls leadership.resign() mid-shutdown relying on its no-throw
    // contract. A resign that throws unchecked (a wrapped DataSource dying at exactly shutdown
    // time) previously escaped close() — the started guard was never reset (sweeper permanently
    // not-restartable), the re-drive evidence was never cleared, and the throw propagated into
    // whatever lifecycle adapter was shutting the context down.
    var leadership =
        new SubscriptionLeadership() {
          @Override
          public Optional<Lease> tryAcquire(String consumerName) {
            return Optional.of(new Lease(1L));
          }

          @Override
          public Optional<Lease> current(String consumerName) {
            return Optional.of(new Lease(1L));
          }

          @Override
          public void resign(String consumerName) {
            throw new IllegalStateException("wrapped DataSource torn down");
          }

          @Override
          public void close() {}
        };
    var sweeper = sweeper(leadership, Duration.ofSeconds(60), Duration.ofHours(1));
    sweeper.start();

    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        sweeper::close, "close() must survive a throwing resign");
    assertThat(sweeper.isStarted())
        .as("the started guard must be reset so the sweeper stays restartable")
        .isFalse();

    // Restartable for real: a second lifecycle cycle works.
    sweeper.start();
    assertThat(sweeper.isAlive()).isTrue();
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(sweeper::close);
  }

  @Test
  void compensatingBacklogGauge_isSampledEachCycle() {
    // The COMPENSATING backlog gauge is sampled every sweep cycle so a stalled-but-alive
    // sweeper (or a genuinely growing backlog of un-issued refunds) is visible on the metrics
    // endpoint — the exact operator-blindness the health/gauge coverage was added to remove.
    var type = org.streamrune.core.types.SagaType.fromClass(FakeSagaState.class);
    for (int i = 0; i < 2; i++) {
      sagaStore.create(
          SagaId.of("comp-" + i),
          type,
          new FakeSagaState(SagaStatus.COMPENSATING, "comp-" + i),
          SagaStatus.COMPENSATING);
    }

    var sweeper = sweeper(new ToggleLeadership(true), Duration.ofSeconds(60), Duration.ofHours(1));
    sweeper.sweepOnce();

    assertThat(metrics.compensatingBacklog)
        .containsEntry(SagaType.fromClass(FakeSagaState.class).value(), 2L);
  }

  /**
   * A saga quarantined {@code FAULTED} by an AUTOMATIC path writes NO dead-letter entry, so it is
   * invisible to {@code streamrune.saga.faulted_backlog} (computed purely from {@code
   * saga_dead_letters}) and to {@code streamrune.saga.compensating} (the status moved off {@code
   * COMPENSATING}). Money is stuck with every standing signal green; the only trace was a one-shot
   * counter and a WARN. The per-type FAULTED-ROW gauge is what makes it observable.
   *
   * <p>The episode here is driven past {@code inboxRetentionMaxAge}, so the sweeper computes {@code
   * staleKeys} and takes the give-up route — {@code SagaRunner.faultStuckCompensation}, which
   * CAS-writes {@code FAULTED} with no dispatch and no dead-letter entry.
   */
  @Test
  void faultedRowsGauge_makesAnEntrylessAutomaticQuarantineObservable() {
    // Drive the saga into a stuck COMPENSATING episode whose undo never completes.
    driveIntoStuckCompensating(Integer.MAX_VALUE);
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    // Age the episode past the command-inbox retention window: the dedup keys of any
    // already-succeeded compensation may have been pruned, so the sweeper must give up rather
    // than risk a double refund.
    clock.advance(Duration.ofDays(8));

    var sweeper = guardedSweeper(Duration.ofHours(1), Duration.ofDays(7));
    sweeper.sweepOnce();

    var saga =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saga.status())
        .as("the give-up route quarantines the saga FAULTED")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(deadLetterStore.findBySaga(sagaId))
        .as("this quarantine path deliberately writes no dead-letter entry")
        .isEmpty();

    // A second cycle samples the gauges over the now-FAULTED row.
    sweeper.sweepOnce();

    assertThat(metrics.faultedRows)
        .as("the stranded saga must be visible on a STANDING per-type series")
        .containsEntry(SagaType.fromClass(FakeSagaState.class).value(), 1L);
    assertThat(metrics.compensatingBacklog.get(SagaType.fromClass(FakeSagaState.class).value()))
        .as("it is no longer COMPENSATING, so that gauge correctly reports 0")
        .isZero();
    // faulted_backlog is computed purely from dead-letter ENTRIES over a FAULTED saga. Sampled
    // over the very same saga store, it correctly — and uselessly, for this saga — reports 0.
    assertThat(new InMemorySagaDeadLetterStore(sagaStore).countFaultedBacklog())
        .as("faulted_backlog keeps its dead-letter-entry semantics and cannot see this saga")
        .isZero();
  }

  @Test
  void noTimeoutSaga_transientCompensationFailure_isRecoveredBySweeper_withoutTimeoutOrNewEvent() {
    // undo-2 fails on the start path + on the first 2 sweep re-drives, then succeeds on the 3rd.
    driveIntoStuckCompensating(3);

    // --- fail-first checkpoint: with no timeout runner and no redelivered event, the saga is stuck
    // COMPENSATING and the undo (undo-2) has NOT completed. Nothing in the current runner re-drives
    // it. This is precisely the wedged state a crash leaves behind without the sweeper.
    var stuck =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(stuck.status())
        .as("transient compensation failure leaves a no-timeout saga COMPENSATING")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(stuck.version())
        .as("create-first genesis v1 + the compensation claim v2")
        .isEqualTo(2L);
    assertThat(bus.execCount("undo-1")).as("undo-1 ran once on the start path").isEqualTo(1);
    assertThat(bus.execCount("undo-2"))
        .as("undo-2 never completed — the undo is abandoned")
        .isZero();

    // --- the sweeper is the missing driver. No SagaTimeoutRunner exists and no new event arrives.
    var sweeper = sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(60), Duration.ofHours(1));
    // Make the stuck saga "due": age it past the retry interval (updated_at is frozen at claim time
    // because a transient re-drive never writes the row).
    clock.advance(Duration.ofSeconds(61));

    sweeper.sweepOnce(); // undo-2 fails (2 left) — still COMPENSATING
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);
    sweeper.sweepOnce(); // undo-2 fails (1 left) — still COMPENSATING
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);
    sweeper.sweepOnce(); // undo-2 succeeds → COMPENSATED

    var recovered =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(recovered.status())
        .as("the sweeper drives the undo to completion → terminal COMPENSATED")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2"))
        .as("the abandoned undo finally ran, exactly once")
        .isEqualTo(1);
    // Idempotency: undo-1 already succeeded on the start path — the shared episode key must dedup
    // it
    // on every sweep re-drive, so it never runs a second time.
    assertThat(bus.execCount("undo-1"))
        .as("already-succeeded compensation is not re-executed")
        .isEqualTo(1);
    assertThat(metrics.compensationRetries.get())
        .as("each re-drive emits the retry metric")
        .isEqualTo(3);
  }

  @Test
  void persistentlyTransientCompensation_isBounded_faultsAfterGiveUpHorizon() {
    // undo-2 fails forever (a permanently broken downstream that never rejects deterministically).
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    var sweeper =
        sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(1), Duration.ofSeconds(5));

    clock.advance(Duration.ofSeconds(2)); // age 2s < 5s → re-drive (fails)
    sweeper.sweepOnce();
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);
    clock.advance(Duration.ofSeconds(2)); // age 4s < 5s → re-drive (fails)
    sweeper.sweepOnce();
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);
    clock.advance(Duration.ofSeconds(2)); // age 6s > 5s → give up → FAULTED

    sweeper.sweepOnce();

    var terminal =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(terminal.status())
        .as("a compensation stuck past the give-up horizon is FAULTED, not looped forever")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(terminal.preFaultStatus())
        .as("a give-up fault records what it replaced")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(bus.execCount("undo-2")).as("the poison undo never completed").isZero();
    assertThat(metrics.compensationRetries.get())
        .as("re-drove twice before giving up")
        .isEqualTo(2);
    assertThat(metrics.faulted.get()).as("the give-up records a fault").isEqualTo(1);
  }

  @Test
  void refusedAdmissionReDrives_areNotAttempts_neverFaultPastTheGiveUpHorizon() {
    // Repro. The undo is REFUSED at admission (the bus is closing — the same shape as an
    // open breaker or an interceptor veto): nothing is attempted, nothing can be learned about the
    // compensation. Pre-fix the sweeper recorded the refusal as a real re-drive and, one cycle
    // past giveUpAfter, FAULTED the saga with ZERO actual dispatches — the class
    // DeadLetterRetryRunner
    // already guards against. The saga must stay COMPENSATING until the gate lifts.
    bus.failTransientlyNTimes("do-1", 1);
    bus.refuseAdmissionForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    var sweeper =
        sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(1), Duration.ofSeconds(5));

    for (int cycle = 0; cycle < 4; cycle++) {
      clock.advance(Duration.ofSeconds(2)); // ages 2s, 4s, 6s, 8s — past the 5s horizon twice
      sweeper.sweepOnce();
      assertThat(
              sagaStore
                  .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                  .orElseThrow()
                  .status())
          .as("cycle %d: a refused admission is not an attempt — no give-up FAULT", cycle)
          .isEqualTo(SagaStatus.COMPENSATING);
    }
    assertThat(bus.execCount("undo-2")).as("nothing was ever attempted").isZero();
    assertThat(metrics.faulted.get()).as("no zero-dispatch FAULT").isZero();
    // streamrune.saga.compensation_retries counts re-driven episodes; a refused admission
    // re-drove nothing, so four refused cycles must not read as four re-drives on a dashboard.
    assertThat(metrics.compensationRetries.get())
        .as("a refused admission is not a re-drive — the retry counter stays at zero")
        .isZero();

    // The gate lifts: the next re-drive completes the undo.
    bus.admitAgain("undo-2");
    clock.advance(Duration.ofSeconds(2));
    sweeper.sweepOnce();
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2")).isEqualTo(1);
    assertThat(metrics.compensationRetries.get())
        .as("exactly the one real re-drive is counted")
        .isEqualTo(1);
  }

  /**
   * The give-up fault races a concurrent writer — {@code markFaulted} conflicts (a resumer
   * terminalized the episode, a replay advanced it). The row is already in the winner's hands, so
   * the conflict is recorded and swallowed: one cas_conflict sample, no faulted sample, nothing
   * thrown, the sweep completes and the row is left to the winner.
   */
  @Test
  void giveUpFault_thatConflictsWithAConcurrentWriter_isRecordedAsCasConflict_notThrown() {
    var conflicting =
        new ForwardingSagaStore(sagaStore) {
          @Override
          public boolean markFaulted(SagaId id, org.streamrune.core.types.SagaType t, long v) {
            throw new org.streamrune.core.OptimisticLockException(
                "a concurrent writer advanced the row");
          }
        };
    var racingRunner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(orchestrator)
            .sagaStore(conflicting)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetterStore)
            .metrics(metrics)
            .build();
    bus.failTransientlyNTimes("do-1", 1);
    bus.refuseAdmissionForever("undo-2");
    racingRunner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    var stuck =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(stuck.status()).isEqualTo(SagaStatus.COMPENSATING);

    var sweeper =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(racingRunner)
            .sagaStore(conflicting)
            .leadership(SubscriptionLeadership.NOOP)
            .retryInterval(Duration.ofSeconds(1))
            .giveUpAfter(Duration.ofSeconds(5))
            .inboxRetentionMaxAge(Duration.ofSeconds(30))
            .clock(clock)
            .metrics(metrics)
            .build();
    clock.advance(Duration.ofSeconds(31)); // past the inbox window: the key-age give-up fires

    sweeper.sweepOnce(); // must complete: the conflict is swallowed inside the give-up

    var row =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(row.status())
        .as("the row is left to the concurrent winner")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version()).isEqualTo(stuck.version());
    assertThat(metrics.casConflicts.get()).as("one cas_conflict sample").isEqualTo(1);
    assertThat(metrics.faulted.get()).as("no faulted sample").isZero();
    assertThat(bus.execCount("undo-2")).as("nothing dispatched").isZero();
  }

  @Test
  void refusedAdmission_doesNotDisarmTheKeyAgeGiveUp() {
    // The staleKeys route stays untouched: an episode past the inbox retention window gives up
    // on the key-age bound regardless of how its re-drives fared — a refused admission must not
    // launder it into an unbounded dwell.
    bus.failTransientlyNTimes("do-1", 1);
    bus.refuseAdmissionForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    var sweeper =
        sweeperWithInboxRetention(
            SubscriptionLeadership.NOOP,
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Duration.ofSeconds(30));
    clock.advance(Duration.ofSeconds(31)); // past the inbox retention window
    sweeper.sweepOnce();

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("the key-age bound still FAULTs without dispatching")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(bus.execCount("undo-2")).isZero();
  }

  @Test
  void giveUpHorizon_anchorsOnEpisodeClaim_faultReplayCycleCannotResetIt() {
    // Repro. The give-up horizon exists so a stuck COMPENSATING episode is always
    // terminalized before its succeeded-compensation inbox keys can be swept (the boot validator
    // proves giveUpAfter < inboxRetention on that assumption). It was measured from updated_at —
    // sound only while updated_at equals the claim instant. But every CAS write refreshes
    // updated_at: a fault->replay cycle (markFaulted + the replay feed's CAS both write the
    // row) RESETS the give-up clock, so an episode claimed at T0 can be re-driven at T0+2h+
    // against a 1h horizon — and, iterated, dwell past the inbox window, after which a re-drive
    // MISSES the pruned key of an already-succeeded compensation and re-executes it (double
    // refund). The fix anchors the horizon on the fault-cycle-immutable episode_claimed_at,
    // surfaced through SagaStore.findCompensating; every write that enters
    // COMPENSATING stamps it and a row surfaced without it is refused, never measured from
    // updated_at, which still spaces re-drives (the due cutoff is unchanged).
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING); // episode claimed at T0 (episode_claimed_at = T0)

    // A fault->replay cycle two hours later refreshes updated_at; the episode identity
    // (episode_version, episode_claimed_at) is deliberately preserved by generic updates.
    clock.advance(Duration.ofHours(2));
    sagaStore.update(
        sagaId,
        org.streamrune.core.types.SagaType.fromClass(FakeSagaState.class),
        new FakeSagaState(SagaStatus.COMPENSATING, "x"),
        SagaStatus.COMPENSATING,
        2L); // create-first genesis v1 + the compensation claim v2

    var sweeper = sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(60), Duration.ofHours(1));
    clock.advance(Duration.ofSeconds(61)); // due again (updated_at aged past the retry interval)
    sweeper.sweepOnce(); // guaranteed first attempt: re-drive, undo-2 still fails
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    clock.advance(Duration.ofSeconds(61));
    sweeper.sweepOnce(); // episode age = claim anchor (T0) -> ~2h3m > 1h -> give up

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as(
            "the give-up horizon must anchor on the immutable episode claim — a fault->replay"
                + " cycle refreshing updated_at must not reset it, or the episode dwells past the"
                + " inbox retention window and a re-drive double-executes a succeeded compensation")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(metrics.faulted.get()).isEqualTo(1);
    assertThat(bus.execCount("undo-2")).as("the poison undo never completed").isZero();
  }

  @Test
  void downtimeLongerThanGiveUpAfter_getsAtLeastOneReDrive_notImmediateFault() {
    // If the process is DOWN (or paused) longer than giveUpAfter, then on restart
    // now − updated_at already exceeds giveUpAfter on the very FIRST sweep — so the pure wall-clock
    // bound would CAS-FAULT a recoverable COMPENSATING saga with ZERO re-drive attempts. A saga
    // stuck only because of downtime must instead get at least one real recovery attempt before the
    // give-up horizon is honored.
    //
    // undo-2 fails once (on the start path), then succeeds on its first re-drive. Fail-first:
    // before
    // the fix the first sweep gives up immediately (FAULTED, undo-2 never runs).
    driveIntoStuckCompensating(1);
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    var sweeper =
        sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(60), Duration.ofMinutes(30));
    // Downtime: jump the clock far past giveUpAfter BEFORE this sweeper's first cycle runs.
    clock.advance(Duration.ofHours(2));

    sweeper.sweepOnce(); // must RE-DRIVE (not give up on the first sight) → undo-2 succeeds

    var recovered =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(recovered.status())
        .as("a saga stuck only because of downtime gets a real re-drive, not an immediate FAULT")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2")).as("the abandoned undo finally ran").isEqualTo(1);
    assertThat(metrics.faulted.get()).as("no fault — it recovered").isZero();
    assertThat(metrics.compensationRetries.get()).as("exactly one re-drive").isEqualTo(1);
  }

  @Test
  void downtimePastGiveUpAfter_stillFaultsOnTheSecondCycleIfCompensationKeepsFailing() {
    // The one-re-drive-before-give-up rule must not disable the poison bound: a compensation that
    // keeps failing after downtime is still FAULTED — just on the cycle AFTER its guaranteed first
    // attempt, not on sight.
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    var sweeper =
        sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(60), Duration.ofMinutes(30));
    clock.advance(Duration.ofHours(2)); // downtime past giveUpAfter

    sweeper.sweepOnce(); // guaranteed first attempt (re-drive, fails) — NOT a give-up
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("first post-downtime cycle re-drives rather than faulting")
        .isEqualTo(SagaStatus.COMPENSATING);

    clock.advance(Duration.ofSeconds(61)); // stay past the horizon
    sweeper.sweepOnce(); // already attempted last cycle → honor give-up → FAULTED

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("a compensation that keeps failing is still bounded, one cycle later")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(metrics.compensationRetries.get()).isEqualTo(1);
    assertThat(metrics.faulted.get()).isEqualTo(1);
  }

  @Test
  void throwingCompensate_duringSweep_faultsTheSaga() {
    driveIntoStuckCompensating(Integer.MAX_VALUE); // undo-2 will never succeed anyway
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    // Now make compensate() itself throw: a deterministic orchestrator bug with no event to
    // quarantine on the sweep path (mirrors SagaTimeoutRunner) → FAULTED.
    orchestrator.throwOnCompensate = true;
    var sweeper = sweeper(SubscriptionLeadership.NOOP, Duration.ofSeconds(1), Duration.ofHours(1));
    clock.advance(Duration.ofSeconds(2));

    sweeper.sweepOnce();

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("a throwing compensate() during sweep faults the saga")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(metrics.faulted.get()).isEqualTo(1);
    assertThat(deadLetterStore.findBySaga(sagaId))
        .as(
            "no DLQ entry — no triggering event to quarantine on the sweep-resume path"
                + " (catchPoison=true)")
        .isEmpty();
  }

  @Test
  void sweep_isLeadershipGated_noOpOnStandby() {
    driveIntoStuckCompensating(1); // one transient failure, then would succeed on re-drive
    clock.advance(Duration.ofSeconds(61));

    var standby = new ToggleLeadership(false);
    var sweeper = sweeper(standby, Duration.ofSeconds(60), Duration.ofHours(1));

    sweeper.sweepOnce();

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("a standby (non-leader) must not re-drive anything")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(bus.execCount("undo-2")).as("no compensation runs on standby").isZero();
    assertThat(metrics.compensationRetries.get()).isZero();

    // Becoming leader unblocks the exact same re-drive.
    standby.leader = true;
    sweeper.sweepOnce();
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("once leader, the same saga is recovered")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2")).isEqualTo(1);
  }

  @Test
  void close_resignsLeadership() {
    var leadership = new ToggleLeadership(true);
    var sweeper = sweeper(leadership, Duration.ofSeconds(60), Duration.ofHours(1));
    sweeper.start();
    sweeper.close();
    assertThat(leadership.resigns)
        .as("close() resigns so a standby can take over")
        .isGreaterThanOrEqualTo(1);
  }

  // ── The automatic re-drive path needs the replayer's key-age guard ────────────────

  private SagaCompensationRetrySweeper<FakeSagaState> guardedSweeper(
      Duration giveUpAfter, Duration inboxRetentionMaxAge) {
    return SagaCompensationRetrySweeper.<FakeSagaState>builder()
        .sagaRunner(runner)
        .sagaStore(sagaStore)
        .leadership(SubscriptionLeadership.NOOP)
        .retryInterval(Duration.ofSeconds(60))
        .giveUpAfter(giveUpAfter)
        .inboxRetentionMaxAge(inboxRetentionMaxAge)
        .clock(clock)
        .metrics(metrics)
        .build();
  }

  @Test
  void episodeDwelledPastInboxRetention_isFaultedWithoutDispatch_noDoubleRefund() {
    // Repro. undo-1 succeeded at claim time (T0) and wrote its inbox dedup row; undo-2
    // failed once. The whole deployment then goes DOWN for longer than the inbox retention window
    // (7d): the InboxRetentionSweeper (surviving replica, or its first post-restart cycle racing
    // this sweeper's) prunes undo-1's row. On the saga sweeper's first post-restart cycle,
    // previouslyReDriven is empty (the one-free-re-drive grant), so pre-fix the sweeper
    // re-drove: undo-1's key MISSED the pruned inbox and the refund EXECUTED A SECOND TIME (double
    // refund), then undo-2 succeeded and the episode terminalized COMPENSATED — silently. The
    // key-age guard must instead route to the existing give-up handling: FAULTED, operator-visible,
    // with NO dispatch — the episode's dedup keys are unprovably fresh, so quarantining is the only
    // safe automatic action (the same remedy the replayer's STALE_COMPENSATION_BLOCKED prescribes).
    driveIntoStuckCompensating(1); // claim at T0; undo-1 committed; undo-2 would succeed on retry
    clock.advance(Duration.ofDays(8)); // outage dwell past the 7d inbox window
    bus.pruneCommittedKeys(); // the inbox retention sweeper pruned the dedup rows

    var sweeper = guardedSweeper(Duration.ofHours(1), Duration.ofDays(7));
    sweeper.sweepOnce(); // first post-restart cycle: previouslyReDriven empty

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as(
            "an episode whose keys outlived inbox retention must be FAULTED for operator"
                + " reconciliation, never re-driven")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(bus.execCount("undo-1"))
        .as("the already-succeeded refund must NOT re-execute (double refund)")
        .isEqualTo(1);
    assertThat(bus.execCount("undo-2")).as("nothing was dispatched on the stale episode").isZero();
    assertThat(metrics.faulted.get()).isEqualTo(1);
    assertThat(metrics.compensationRetries.get()).as("no re-drive happened").isZero();
  }

  @Test
  void misconfiguredGiveUpAboveInboxRetention_keyAgeGuardIsTheCompensatingControl() {
    // The boot validator (SagaRetentionValidator.validate) deliberately only WARNs when
    // giveUpAfter >= inboxRetentionMaxAge (by design: failing fast would reject shipped
    // configs). In exactly that gap — give-up 14d, inbox retention 7d, both horizons honored —
    // an episode ages past the inbox window while still under the give-up horizon, and pre-fix the
    // sweeper kept re-driving it: pruned undo-1 key -> double refund on every cycle. The runtime
    // key-age guard is the compensating control: staleness FAULTs regardless of the horizon.
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    var sweeper = guardedSweeper(Duration.ofDays(14), Duration.ofDays(7));
    clock.advance(Duration.ofDays(8)); // past inbox retention, still under the give-up horizon
    bus.pruneCommittedKeys();

    sweeper.sweepOnce();

    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("stale keys must FAULT even while the (misconfigured) give-up horizon is not reached")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(bus.execCount("undo-1"))
        .as("the already-succeeded refund must NOT re-execute")
        .isEqualTo(1);
    assertThat(metrics.compensationRetries.get()).isZero();
  }

  @Test
  void freshEpisode_withKeyAgeGuardConfigured_isReDrivenNormally() {
    // Regression pin: the guard must not touch a fresh episode — with the knob configured, a saga
    // due for its normal retry re-drives exactly as before (undo-1 dedups, undo-2 completes).
    driveIntoStuckCompensating(1);
    var sweeper = guardedSweeper(Duration.ofHours(1), Duration.ofDays(7));
    clock.advance(Duration.ofSeconds(61)); // due, and well within the inbox window

    sweeper.sweepOnce();

    var recovered =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(recovered.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2")).as("the pending undo completed").isEqualTo(1);
    assertThat(bus.execCount("undo-1")).as("the succeeded undo deduped, not re-run").isEqualTo(1);
    assertThat(metrics.faulted.get()).isZero();
  }

  @Test
  void downtimePastGiveUpButWithinInboxRetention_stillGetsItsGuaranteedFirstReDrive() {
    // Interplay pin: the guard must override the one-free-re-drive grant ONLY when the
    // keys are unprovably fresh. Downtime past giveUpAfter but well within the inbox window keeps
    // the dedup keys intact, so the guaranteed first post-restart re-drive still happens (and
    // recovers the saga) exactly as intended.
    driveIntoStuckCompensating(1);
    var sweeper = guardedSweeper(Duration.ofMinutes(30), Duration.ofDays(7));
    clock.advance(Duration.ofHours(2)); // > giveUpAfter, << inbox retention

    sweeper.sweepOnce();

    var recovered =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(recovered.status())
        .as("provably-fresh keys → the guaranteed first re-drive is preserved")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-2")).isEqualTo(1);
    assertThat(bus.execCount("undo-1")).as("deduped against the intact inbox row").isEqualTo(1);
    assertThat(metrics.faulted.get()).isZero();
  }

  // close() must JOIN the sweep thread before resetting the started guard, so a stale
  // finally { running = false } cannot clobber a subsequent start()'s running = true (which would
  // silently kill the restarted sweeper).
  @Test
  void closeJoinsSweepThreadSoRestartCannotSilentlyKillTheSweeper() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    SagaStore blockingStore = mock(SagaStore.class);
    when(blockingStore.findCompensating(any(), any(), anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));

    var sweeper =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(blockingStore)
            .clock(clock)
            .retryInterval(Duration.ofSeconds(1))
            .giveUpAfter(Duration.ofHours(1))
            .build();
    sweeper.start();
    assertThat(entered.await(5, TimeUnit.SECONDS))
        .as("sweep thread should have entered findCompensating")
        .isTrue();

    sweeper.close();

    assertThat(pollThread.get().isAlive())
        .as("close() must join the sweep thread before returning")
        .isFalse();
  }

  // ── Sampling-only mode keeps the gauges live without re-driving ──────────────────────

  @Test
  void compensationRetryDisabled_samplesTheBacklogGauges_butNeverTakesTheLeaseOrReDrives() {
    // streamrune.saga.compensating / faulted_rows are emitted by this sweeper's per-cycle
    // sample and by nothing else. The integrations used to withhold the sweeper entirely under
    // compensation-retry-enabled=false, so both gauges went silent. In sampling-only mode the
    // sample
    // still runs every cycle, and the cycle ends BEFORE the leadership gate — no lease is taken and
    // nothing is re-driven.
    var type = org.streamrune.core.types.SagaType.fromClass(FakeSagaState.class);
    for (int i = 0; i < 2; i++) {
      sagaStore.create(
          SagaId.of("comp-" + i),
          type,
          new FakeSagaState(SagaStatus.COMPENSATING, "comp-" + i),
          SagaStatus.COMPENSATING);
    }
    SubscriptionLeadership leadership = mock(SubscriptionLeadership.class);
    var samplingOnly =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .leadership(leadership)
            .retryInterval(Duration.ofSeconds(60))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(metrics)
            .compensationRetryEnabled(false)
            .build();
    assertThat(samplingOnly.isCompensationRetryEnabled()).isFalse();

    samplingOnly.sweepOnce();

    assertThat(metrics.compensatingBacklog)
        .containsEntry(SagaType.fromClass(FakeSagaState.class).value(), 2L);
    assertThat(metrics.faultedRows.get(SagaType.fromClass(FakeSagaState.class).value())).isZero();
    verifyNoInteractions(leadership);
    assertThat(
            sagaStore
                .load(
                    SagaId.of("comp-0"),
                    SagaType.fromClass(FakeSagaState.class),
                    FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    // close() must not resign a lease this sampling-only sweeper never took — a resign()
    // call here would be a no-op UPDATE per saga type per replica on shutdown, and — if the DB is
    // already gone — a misleading "resign(...) failed; lease will expire naturally" WARN.
    samplingOnly.close();
    verifyNoInteractions(leadership);

    // Control: the default (re-drive enabled) sweeper DOES take the lease on the same cycle.
    when(leadership.tryAcquire(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(java.util.Optional.empty());
    var reDriving =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .leadership(leadership)
            .retryInterval(Duration.ofSeconds(60))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(metrics)
            .build();
    assertThat(reDriving.isCompensationRetryEnabled()).isTrue();
    reDriving.sweepOnce();
    verify(leadership).tryAcquire(org.mockito.ArgumentMatchers.anyString());
  }

  @Test
  void samplingOnlyMode_backlogSampleFailure_degradesConsecutiveFailures_insteadOfSwallowed()
      throws InterruptedException {
    // sampleBacklog() swallowed every RuntimeException from countByStatus — correct in
    // re-drive mode, where a saga-store outage is ALSO signaled by findCompensating failing and
    // propagating through sweepOnce(). But sampling-only mode never calls findCompensating at
    // all: sampleBacklog is the ONLY store interaction each cycle, so swallowing here left
    // consecutiveFailures() stuck at 0 (health UP) through a sustained saga-store outage, even
    // though this sweeper is registered on relay health under a name that implies an active
    // driver (now suffixed ":sampling-only" — see BackgroundRelayHealthContributor). Drives the
    // sweeper through its REAL background poll loop (not a direct sweepOnce() call) so the
    // ResilientPollLoop backoff/consecutiveFailures wiring is genuinely exercised end to end.
    SagaStore throwingStore = mock(SagaStore.class);
    when(throwingStore.countByStatus(any(), any()))
        .thenThrow(new RuntimeException("saga store unreachable"));
    var samplingOnly =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(throwingStore)
            .retryInterval(Duration.ofMillis(20))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(metrics)
            .compensationRetryEnabled(false)
            .build();

    samplingOnly.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (samplingOnly.consecutiveFailures() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertThat(samplingOnly.consecutiveFailures())
          .as(
              "a sustained saga-store outage must be visible even in sampling-only mode, not"
                  + " silently swallowed")
          .isGreaterThan(0);
      assertThat(
              BackgroundRelayHealthContributor.statusFor(
                  samplingOnly.isStarted(),
                  samplingOnly.isAlive(),
                  samplingOnly.consecutiveFailures()))
          .as(
              "the relay-health status for this component must read DEGRADED, not UP, through"
                  + " the outage")
          .isEqualTo(BackgroundRelayHealthContributor.Status.DEGRADED);
    } finally {
      samplingOnly.close();
    }
  }

  @Test
  void samplingOnlyMode_throwingMetricsSink_doesNotDegradeConsecutiveFailures_butWarns()
      throws InterruptedException {
    // sampleBacklog() used to wrap BOTH the countByStatus reads AND the metrics.record*
    // writes in one try; the "propagate any RuntimeException in sampling-only mode" rule
    // (correct for a REAL saga-store outage — see the test above) then ALSO caught a throwing
    // METRICS sink over a perfectly healthy store (a Prometheus meter-id collision, a closed
    // registry — the same hazard class guarded at every other metrics call site) and
    // read it as a store outage: ERROR+trace per cycle, consecutiveFailures climbing, health
    // DEGRADED — while re-drive mode swallowed the identical failure with a WARN. Store reads and
    // metric writes must be guarded independently. Drives the sweeper through its REAL background
    // poll loop, like the test above, so consecutiveFailures is genuinely exercised.
    var type = org.streamrune.core.types.SagaType.fromClass(FakeSagaState.class);
    sagaStore.create(
        SagaId.of("comp-throw"),
        type,
        new FakeSagaState(SagaStatus.COMPENSATING, "comp-throw"),
        SagaStatus.COMPENSATING);
    StreamRuneMetrics throwingSink =
        new StreamRuneMetrics() {
          @Override
          public void recordSagaCompensatingBacklog(String sagaType, long count) {
            throw new IllegalStateException(
                "simulated metrics backend failure (compensating backlog)");
          }
        };
    var samplingOnly =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .retryInterval(Duration.ofMillis(20))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(throwingSink)
            .compensationRetryEnabled(false)
            .build();

    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(SagaCompensationRetrySweeper.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    samplingOnly.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (appender.list.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
    } finally {
      samplingOnly.close();
      logger.detachAppender(appender);
      appender.stop();
    }

    assertThat(appender.list).as("the metrics sink failure must still be surfaced").isNotEmpty();
    assertThat(
            appender.list.stream()
                .anyMatch(
                    e ->
                        e.getLevel() == ch.qos.logback.classic.Level.WARN
                            && e.getFormattedMessage()
                                .contains(
                                    "Metrics recording failed for saga compensation-retry"
                                        + " sweeper")))
        .as("the metrics failure must be logged as a WARN (recordMetric's guard), not an ERROR")
        .isTrue();
    assertThat(
            appender.list.stream()
                .noneMatch(e -> e.getLevel() == ch.qos.logback.classic.Level.ERROR))
        .as("a throwing metrics sink over a HEALTHY saga store must never log at ERROR")
        .isTrue();
    assertThat(samplingOnly.consecutiveFailures())
        .as("a throwing metrics sink over a HEALTHY saga store must not read as a store outage")
        .isZero();
  }

  @Test
  void samplingOnlyMode_throwingMetricsSink_warnsOnceAcrossManyRetryCycles()
      throws InterruptedException {
    // recordMetric's guard used to WARN-with-stack-trace on EVERY call, and
    // sampleBacklog() calls it every single sweep cycle regardless of mode — a persistently
    // throwing sink therefore flooded the log at retry-interval cadence, forever, the same class
    // of defect latched for SubscriptionHealthContributor's lag-metric sink. This
    // drives the sweeper through many real cycles (not just until the first WARN, like the test
    // above) to prove the latch actually holds rather than merely permitting one WARN.
    var type = org.streamrune.core.types.SagaType.fromClass(FakeSagaState.class);
    sagaStore.create(
        SagaId.of("comp-throw-repeat"),
        type,
        new FakeSagaState(SagaStatus.COMPENSATING, "comp-throw-repeat"),
        SagaStatus.COMPENSATING);
    StreamRuneMetrics throwingSink =
        new StreamRuneMetrics() {
          @Override
          public void recordSagaCompensatingBacklog(String sagaType, long count) {
            throw new IllegalStateException(
                "simulated metrics backend failure (compensating backlog)");
          }
        };
    var samplingOnly =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .retryInterval(Duration.ofMillis(20))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(throwingSink)
            .compensationRetryEnabled(false)
            .build();

    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(SagaCompensationRetrySweeper.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    samplingOnly.start();
    try {
      // ~20 retry-interval cycles (20ms each) — long enough that a per-cycle WARN would show up
      // as many entries if the latch did not hold.
      Thread.sleep(400);
    } finally {
      samplingOnly.close();
      logger.detachAppender(appender);
      appender.stop();
    }

    long warnings =
        appender.list.stream()
            .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
            .filter(
                e ->
                    e.getFormattedMessage()
                        .contains("Metrics recording failed for saga compensation-retry sweeper"))
            .count();
    assertThat(warnings)
        .as(
            "a persistently throwing metrics sink must be logged exactly ONCE across many retry"
                + " cycles, not once per cycle")
        .isEqualTo(1L);
  }

  /**
   * The give-up horizon and the key-age guard anchor on {@code episode_claimed_at} and on nothing
   * else. A COMPENSATING row surfaced WITHOUT it violates the SagaStore contract — every write that
   * enters COMPENSATING stamps it — and is REFUSED: not re-driven (the deleted {@code updated_at}
   * fallback plus the first-re-drive grant re-drove it on first sight), not given up on, nothing
   * written; it is left for the next cycle to refuse again until the store is fixed. Modelled with
   * a decorator that strips the claim instant from the enumeration.
   */
  @Test
  void anUnstampedCompensatingRow_isRefusedBySweep_neitherReDrivenNorFaulted() {
    bus.failTransientlyNTimes("do-1", 1);
    bus.failTransientlyForever("undo-2");
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    var type = SagaType.fromClass(FakeSagaState.class);
    var entered = sagaStore.load(sagaId, type, FakeSagaState.class).orElseThrow();
    assertThat(entered.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(entered.episodeClaimedAt()).as("the real store stamped the claim").isNotNull();
    int undo2Before = bus.execCount("undo-2");

    var unstamping =
        new ForwardingSagaStore(sagaStore) {
          @Override
          public List<SagaStore.CompensatingSaga> findCompensating(
              SagaType t, Instant before, int limit) {
            return super.findCompensating(t, before, limit).stream()
                .map(cs -> new SagaStore.CompensatingSaga(cs.sagaId(), cs.updatedAt(), null))
                .toList();
          }
        };
    var sweeper =
        SagaCompensationRetrySweeper.<FakeSagaState>builder()
            .sagaRunner(runner)
            .sagaStore(unstamping)
            .leadership(SubscriptionLeadership.NOOP)
            .retryInterval(Duration.ofSeconds(60))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .metrics(metrics)
            .build();
    clock.advance(Duration.ofHours(2)); // due, and past the give-up horizon
    sweeper.sweepOnce();
    clock.advance(Duration.ofHours(2));
    sweeper.sweepOnce(); // the next cycle refuses again

    var after = sagaStore.load(sagaId, type, FakeSagaState.class).orElseThrow();
    assertThat(after.status())
        .as("neither re-driven to a terminal status nor given up on")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(after.version()).as("nothing written").isEqualTo(entered.version());
    assertThat(metrics.compensationRetries.get()).as("no re-drive attempt").isZero();
    assertThat(metrics.faulted.get()).as("no give-up fault").isZero();
    assertThat(bus.execCount("undo-2")).as("nothing dispatched").isEqualTo(undo2Before);
  }
}

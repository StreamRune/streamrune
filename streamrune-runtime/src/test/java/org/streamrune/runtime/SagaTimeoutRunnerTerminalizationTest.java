package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTimeoutException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * Tests that SagaTimeoutRunner persists a terminal status (COMPENSATED/FAILED) after
 * timeout-compensation — fixing a bug where the saga was left RUNNING and re-selected forever.
 */
class SagaTimeoutRunnerTerminalizationTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ======================================================================
  // Minimal domain model
  // ======================================================================

  /** Compensation command emitted on timeout. */
  record Undo(String id) implements Command {}

  // ======================================================================
  // Minimal saga state
  // ======================================================================

  record FakeSagaState(String id, SagaStatus status) implements SagaState {}

  // ======================================================================
  // SagaDecider fixture: timeout is set; compensate emits one Undo
  // ======================================================================

  static class TimeoutDecider implements SagaDecider<FakeSagaState> {

    @Override
    public Class<FakeSagaState> stateType() {
      return FakeSagaState.class;
    }

    @Override
    public FakeSagaState initialState(SagaId sagaId) {
      return new FakeSagaState(sagaId.value(), SagaStatus.STARTED);
    }

    @Override
    public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      if (failure instanceof SagaTimeoutException) {
        return List.of(SagaCommand.of(new Undo(state.id()), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  // ======================================================================
  // Recording CommandBus
  // ======================================================================

  static class RecordingCommandBus implements CommandBus {
    final List<Command> executed = new ArrayList<>();
    final List<IdempotencyKey> keys = new ArrayList<>();
    boolean alwaysFail = false;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
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
        // a permanent rejection, so a failing compensation command terminalizes the saga FAILED.
        // A transient failure would instead keep it COMPENSATING for re-pick.
        throw new IllegalArgumentException(
            "Simulated deterministic failure for " + command.getClass().getSimpleName());
      }
      executed.add(command);
      keys.add(key);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  // ======================================================================
  // Test setup
  // ======================================================================

  // The stored saga_type must equal SagaType.fromClass(stateType): the SagaStepExecutor (which the
  // timeout runner now routes through) derives its CAS-write saga_type from the state class, and
  // InMemorySagaStore.update type-scopes the CAS. A human label like "TimeoutDecider"
  // would make the executor's claim/terminal writes miss the seeded row. Mirrors
  // SagaStepExecutorTest
  // and SagaConcurrencyRaceTest.FULFILL_TYPE.
  static final SagaType SAGA_TYPE = SagaType.fromClass(FakeSagaState.class);

  /** Far-future cutoff — every saga saved "now" is treated as timed out. */
  static final Instant FAR_FUTURE_CUTOFF = Instant.parse("2099-01-01T00:00:00Z");

  MutableClock clock;
  InMemorySagaStore sagaStore;
  RecordingCommandBus bus;
  SagaId sagaId;
  SagaTimeoutRunner<FakeSagaState> runner;

  @BeforeEach
  void setUp() {
    clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    sagaStore = new InMemorySagaStore(clock);
    bus = new RecordingCommandBus();
    sagaId = SagaId.of("saga-timeout-1");

    // Seed a RUNNING saga with an old updatedAt (clock is in the past)
    sagaStore.create(
        sagaId,
        SAGA_TYPE,
        new FakeSagaState(sagaId.value(), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    runner =
        SagaTimeoutRunner.<FakeSagaState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaType(SAGA_TYPE)
            .build();
  }

  // ======================================================================
  // Tests
  // ======================================================================

  /**
   * Primary assertion: after processBatch the saga must have a terminal status in the store, and
   * findTimedOut must not return it again (no compensation storm).
   */
  @Test
  void timeoutCompensation_persistsTerminalStatus_andSagaIsNotRePicked() {
    runner.processBatch(FAR_FUTURE_CUTOFF);

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status().isTerminal())
        .as("saga status must be terminal after timeout compensation (was RUNNING — the old bug)")
        .isTrue();
    assertThat(sagaStore.findTimedOut(SAGA_TYPE, FAR_FUTURE_CUTOFF, 50))
        .as("timed-out query must not return a terminal saga (no compensation storm)")
        .doesNotContain(sagaId);
  }

  /**
   * When compensation dispatches successfully, the persisted status must be COMPENSATED (not
   * FAILED).
   */
  @Test
  void timeoutCompensation_allSucceed_isCOMPENSATED() {
    runner.processBatch(FAR_FUTURE_CUTOFF);

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status())
        .as("successful compensation must yield COMPENSATED status")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.executed)
        .as("Undo command must have been dispatched")
        .hasSize(1)
        .allMatch(c -> c instanceof Undo);
  }

  /**
   * When all compensation commands fail, the persisted status must be FAILED (still terminal — no
   * re-pick).
   */
  @Test
  void timeoutCompensation_compensationFails_isFAILED_andNotRePicked() {
    bus.failAll();

    runner.processBatch(FAR_FUTURE_CUTOFF);

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status())
        .as("failed compensation must yield FAILED status")
        .isEqualTo(SagaStatus.FAILED);
    assertThat(sagaStore.findTimedOut(SAGA_TYPE, FAR_FUTURE_CUTOFF, 50))
        .as("FAILED saga must not be re-picked")
        .doesNotContain(sagaId);
  }

  /**
   * Defensive guard: the terminal-guard branch inside {@code processTimedOutSaga} short-circuits
   * before invoking {@code decider.compensate} when the saga is already terminal at load time.
   *
   * <p>We seed the store with a COMPENSATED saga and call {@code runner.processTimedOutSaga}
   * directly (bypassing {@code findTimedOut}, which would filter it out). This is the only way to
   * reach and cover the {@code if (loaded.status().isTerminal()) return;} guard.
   */
  @Test
  void alreadyTerminalSaga_isSkipped_noCommandDispatched() {
    // Overwrite the RUNNING saga (seeded in setUp) with a terminal COMPENSATED state.
    // setUp created it at version=1, so expectedVersion=1.
    sagaStore.update(
        sagaId,
        SAGA_TYPE,
        new FakeSagaState(sagaId.value(), SagaStatus.COMPENSATED),
        SagaStatus.COMPENSATED,
        1L);

    // Call processTimedOutSaga directly — this is the method that contains the guard.
    // findTimedOut would filter this saga out, so processBatch would never reach the guard.
    bus.executed.clear();
    runner.processTimedOutSaga(sagaId);

    assertThat(bus.executed)
        .as("terminal-guard must short-circuit before compensation: no command dispatched")
        .isEmpty();
  }

  /**
   * Defensive guard covers FAULTED: a saga that is FAULTED (quarantined) slips through the narrow
   * window between {@code findTimedOut} and {@code load}, so the guard must also skip it. FAULTED
   * is not UPSERT-guarded — if compensation fires it would overwrite the FAULTED status.
   *
   * <p>The guard must use {@link SagaStatus#isHalted()} (= {@code isTerminal() || FAULTED}), not
   * just {@code isTerminal()}.
   */
  @Test
  void faultedSaga_isSkipped_noCommandDispatchedAndStatusUnchanged() {
    // Overwrite the RUNNING saga (seeded in setUp) with FAULTED (quarantined — not terminal, but
    // halted). setUp created it at version=1, so expectedVersion=1.
    sagaStore.update(
        sagaId,
        SAGA_TYPE,
        new FakeSagaState(sagaId.value(), SagaStatus.FAULTED),
        SagaStatus.FAULTED,
        1L);

    // Call processTimedOutSaga directly — simulates the race where findTimedOut saw the saga
    // before it was quarantined, but load now sees FAULTED.
    bus.executed.clear();
    runner.processTimedOutSaga(sagaId);

    assertThat(bus.executed)
        .as("isHalted guard must short-circuit for FAULTED saga: no command dispatched")
        .isEmpty();
    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status())
        .as("FAULTED status must not be overwritten by timeout compensation")
        .isEqualTo(SagaStatus.FAULTED);
  }

  /**
   * When {@code decider.compensate()} throws on the timeout path, there is no triggering event to
   * quarantine. The runner must mark the saga FAULTED (halted → not re-selected) instead of logging
   * a WARN loop forever.
   *
   * <p>After {@code processTimedOutSaga} completes, the saga must be FAULTED in the store AND must
   * not appear in a subsequent {@code findTimedOut} query (log-loop is gone).
   */
  @Test
  void throwingCompensator_onTimeout_marksFaulted_noDlqEntry() {
    // Decider whose compensate() always throws
    var throwingDecider =
        new SagaDecider<FakeSagaState>() {
          @Override
          public Class<FakeSagaState> stateType() {
            return FakeSagaState.class;
          }

          @Override
          public FakeSagaState initialState(SagaId sagaId) {
            return new FakeSagaState(sagaId.value(), SagaStatus.STARTED);
          }

          @Override
          public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
            return state;
          }

          @Override
          public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
            return List.of();
          }

          @Override
          public List<SagaCommand> compensate(
              FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
            throw new IllegalStateException("compensator bug on timeout");
          }

          @Override
          public Optional<Duration> timeout() {
            return Optional.of(Duration.ofMinutes(5));
          }
        };

    var throwingRunner =
        SagaTimeoutRunner.<FakeSagaState>builder()
            .decider(throwingDecider)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaType(SAGA_TYPE)
            .build();

    // Must not throw
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> throwingRunner.processTimedOutSaga(sagaId));

    // Saga must be FAULTED in the store
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .as("saga must be FAULTED after throwing compensate() on timeout path")
        .isEqualTo(SagaStatus.FAULTED);

    // FAULTED is halted — findTimedOut must not return it (no log-loop)
    assertThat(sagaStore.findTimedOut(SAGA_TYPE, FAR_FUTURE_CUTOFF, 50))
        .as("FAULTED saga must not be re-selected by findTimedOut")
        .doesNotContain(sagaId);
  }

  /**
   * CAS-update conflict during timeout terminalization: if the event path advances the saga while
   * we are compensating, our {@code update} sees a stale version and throws {@link
   * OptimisticLockException}. The runner must swallow it silently (DEBUG log) — no exception must
   * escape {@code processTimedOutSaga} and the saga in the store must remain at its pre-timeout
   * status (unchanged by us).
   */
  @Test
  void timeoutTerminalization_onStaleVersion_skipsQuietly() {
    // Delegating store: load returns the RUNNING saga; update always throws OLE.
    var staleStore =
        new SagaStore() {
          private final InMemorySagaStore delegate = new InMemorySagaStore(clock);

          {
            // Seed the delegate with the same RUNNING saga that setUp created in sagaStore
            delegate.create(
                sagaId,
                SAGA_TYPE,
                new FakeSagaState(sagaId.value(), SagaStatus.RUNNING),
                SagaStatus.RUNNING);
          }

          @Override
          public void create(
              SagaId id,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            delegate.create(id, sagaType, state, status, deadLetterPending);
          }

          @Override
          public void update(
              SagaId id,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {
            throw new OptimisticLockException("simulated concurrent advance");
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
          public void createGenesisPending(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new UnsupportedOperationException("createGenesisPending");
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

    var staleRunner =
        SagaTimeoutRunner.<FakeSagaState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(staleStore)
            .commandBus(bus)
            .sagaType(SAGA_TYPE)
            .build();

    // Must not throw — stale-version conflict is silently skipped
    assertThat(staleRunner).satisfies(r -> assertThat(r).isNotNull()); // runner built OK
    // processTimedOutSaga must not propagate the OLE
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> staleRunner.processTimedOutSaga(sagaId));

    // Store still holds the pre-timeout status (our update did not land)
    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status())
        .as("saga status must be unchanged when update is stale (our write did not land)")
        .isEqualTo(SagaStatus.RUNNING);
  }

  // ======================================================================
  // Claim-first ownership tests
  // ======================================================================

  /**
   * Fresh claim: a RUNNING saga at v1 processed by the timeout runner must first CAS to
   * COMPENSATING (an intermediate write bumping the version to 2) before dispatching any
   * compensation command, then persist the terminal status at that same episode version (2),
   * landing at a final version of 3. The dispatched compensation command's idempotency key must be
   * scoped by episodeVersion=2.
   */
  @Test
  void freshClaim_writesIntermediateCompensating_thenTerminalAtEpisodeVersion() {
    // setUp seeded a RUNNING saga at version=1.
    runner.processTimedOutSaga(sagaId);

    var loaded =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.status())
        .as("successful compensation must yield a terminal COMPENSATED status")
        .isEqualTo(SagaStatus.COMPENSATED);
    // Claim (v1->v2) + terminal write (v2->v3) = final version 3.
    assertThat(loaded.version())
        .as("final version must be 3: claim write (v1->v2) + terminal write (v2->v3)")
        .isEqualTo(3L);

    assertThat(bus.executed).as("Undo command must have been dispatched").hasSize(1);
    assertThat(bus.keys)
        .as("compensation key must be scoped by episodeVersion=2 (the claim's resulting version)")
        .containsExactly(IdempotencyKey.of("saga:" + sagaId.value() + ":episode:2:comp:0"));
  }

  /**
   * Crash-resume episode: a saga already COMPENSATING (ownership previously claimed, e.g. by a run
   * that crashed after the claim but before the terminal write) must NOT be re-claimed. The runner
   * reuses the saga's current version as the episode id, so the compensation key is stable across
   * the retry, and the terminal write CAS targets that same (unbumped) version.
   */
  @Test
  void crashResumeEpisode_reusesStableEpisodeVersion_noReClaim() {
    // Move the seeded RUNNING saga (v1) to COMPENSATING at v1->v2, simulating a prior run that
    // claimed ownership but crashed before completing the episode.
    sagaStore.update(
        sagaId,
        SAGA_TYPE,
        new FakeSagaState(sagaId.value(), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING,
        1L);
    var claimed =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(claimed.version()).isEqualTo(2L);
    assertThat(claimed.status()).isEqualTo(SagaStatus.COMPENSATING);

    // Resume: processTimedOutSaga must recognize COMPENSATING and reuse version 2 as the episode
    // id — no re-claim write, so the pre-terminal version stays 2.
    runner.processTimedOutSaga(sagaId);

    assertThat(bus.keys)
        .as("resumed episode must reuse episodeVersion=2 (the version at which it was claimed)")
        .containsExactly(IdempotencyKey.of("saga:" + sagaId.value() + ":episode:2:comp:0"));

    var after =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(after.status())
        .as("resumed episode must reach a terminal status")
        .isEqualTo(SagaStatus.COMPENSATED);
    // No re-claim write: only the terminal write happens (2 -> 3), not (2 -> 3 -> 4).
    assertThat(after.version())
        .as("resuming must not re-claim: only the terminal write bumps the version once (2->3)")
        .isEqualTo(3L);
  }
}

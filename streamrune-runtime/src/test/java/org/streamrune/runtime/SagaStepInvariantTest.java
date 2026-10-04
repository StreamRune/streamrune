package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
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
import org.streamrune.test.MutableClock;

/**
 * Structural safety net: a single crash/interleave matrix asserting the SAME episode-integrity
 * invariants for EVERY {@link SagaTrigger} type. Because all five entry paths ({@code SagaRunner}
 * start/correlated/resume + {@code SagaTimeoutRunner} claim/resume) route through {@link
 * SagaStepExecutor#execute}, and a future entry path would too, proving the invariant is
 * <em>trigger-agnostic</em> here closes the "sibling-path asymmetry" defect class by construction:
 * a new caller cannot re-introduce a divergent forward/compensation behaviour without failing this
 * matrix.
 *
 * <p>The four trigger drivers of a COMPENSATING episode mirror the real adapters exactly:
 *
 * <ul>
 *   <li>{@code ForwardStep(startPath=true)} — {@code SagaRunner.processStartEvent} redelivery
 *   <li>{@code ForwardStep(startPath=false)} — {@code SagaRunner.processCorrelatedEvent}
 *   <li>{@code ResumeCompensation(SagaCompensationResumeException)} — the sweeper / no-event resume
 *   <li>{@code ResumeCompensation(SagaTimeoutException)} — {@code SagaTimeoutRunner}'s
 *       <em>resume</em> of an already-{@code COMPENSATING} row (it deliberately does NOT route a
 *       COMPENSATING row to {@code ClaimTimeout}, which would re-claim at a new version and shift
 *       the episode key — see {@code SagaTimeoutRunner.processTimedOutSaga})
 * </ul>
 *
 * <p>The genuinely fresh-claim {@code ClaimTimeout} (RUNNING → COMPENSATING) and the crash-seam
 * recoverability cases are focused non-parameterized companions.
 *
 * <p>Every assertion is non-vacuous: exact dispatch counts ({@code undo-1}==1, {@code do-2}==0),
 * exact final status, and (where relevant) exact persisted version — so a test would FAIL if the
 * executor re-ran forward, double-dispatched, or re-claimed. Harness copied from {@code
 * SagaStepExecutorTest} + {@code SagaStartEventClaimFirstCrashTest} so this file is self-contained.
 */
class SagaStepInvariantTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  private static final SagaType FAKE_TYPE = SagaType.fromClass(FakeSagaState.class);

  /** A trigger case that drives the SAME (seeded) episode from a given starting row. */
  record TriggerCase(String name, SagaStatus seedStatus, Supplier<SagaTrigger> build) {
    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * The four triggers that can drive a COMPENSATING episode, each modelling a real adapter path.
   */
  static Stream<TriggerCase> triggers() {
    var timeoutCause = new SagaTimeoutException(SagaId.of("saga-x"), Duration.ofSeconds(1));
    return Stream.of(
        new TriggerCase(
            "ForwardStep(start)",
            SagaStatus.COMPENSATING,
            () -> new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, false)),
        new TriggerCase(
            "ForwardStep(correlated)",
            SagaStatus.COMPENSATING,
            () -> new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 3L), false, false)),
        new TriggerCase(
            "ResumeCompensation(resume-marker)",
            SagaStatus.COMPENSATING,
            () -> new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException())),
        new TriggerCase(
            "ResumeCompensation(timeout-resume)",
            SagaStatus.COMPENSATING,
            () -> new SagaTrigger.ResumeCompensation(timeoutCause)));
  }

  // ==== Invariant 1: resume-a-claimed-episode exactly once, never re-opening forward ============

  @ParameterizedTest(name = "{0}")
  @MethodSource("triggers")
  void everyTrigger_resumesAClaimedEpisodeExactlyOnce_neverReOpeningForward(TriggerCase tc) {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    // Seed a claimed COMPENSATING episode at v1 (refund not yet run).
    store.create(
        sagaId, FAKE_TYPE, new FakeSagaState(SagaStatus.RUNNING, sagaId.value()), tc.seedStatus());

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            tc.build().get());

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.execCount("undo-1")).as("compensation runs exactly once").isEqualTo(1);
    assertThat(bus.execCount("do-2"))
        .as("forward step NEVER re-runs on a COMPENSATING episode")
        .isZero();
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    // One terminal CAS-write only (v1 -> v2). No re-claim (that would be v3): pins "no version
    // bump".
    assertThat(saved.version()).as("resumed at the claimed version — no re-claim").isEqualTo(2L);
  }

  // ==== Invariant 2: cross-path episode-key dedup (no double refund) ============================

  @ParameterizedTest(name = "{0}")
  @MethodSource("triggers")
  void twoDriversResumeSameEpisode_dedupViaEpisodeKey_noDoubleCompensation(TriggerCase tc) {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus(); // shared inbox across both drivers
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);
    var snapshot =
        store.load(
            sagaId,
            SagaType.fromClass(FakeSagaState.class),
            FakeSagaState.class); // both drivers hold the same v1 view

    exec.execute(sagaId, snapshot, tc.build().get()); // driver A wins the terminal CAS
    StepOutcome b = exec.execute(sagaId, snapshot, tc.build().get()); // driver B loses (stale)

    assertThat(bus.execCount("undo-1")).as("episode key dedups the second drive").isEqualTo(1);
    assertThat(b)
        .isIn(StepOutcome.COMPENSATED, StepOutcome.NOT_COMPENSATING); // benign CAS-conflict skip
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    // Exactly one terminal write across both drivers (v1 -> v2). A second write would be v3.
    assertThat(saved.version()).as("driver B's terminal CAS lost benignly").isEqualTo(2L);
  }

  // ==== Invariant 2b: cross-TRIGGER-TYPE episode-key dedup (no double refund) ====================

  @Test
  void
      forwardStepAndResumeCompensation_raceSameClaimedEpisode_dedupViaEpisodeKey_noDoubleCompensation() {
    // Invariant 2 (above) proves dedup between two drivers of the SAME trigger kind. This proves
    // the SAME guarantee head-to-head across TWO DIFFERENT recovery paths racing the SAME claimed
    // episode from the SAME loaded view: a redelivered correlated event (ForwardStep) and a
    // sweeper/no-event resume (ResumeCompensation). The episode-scoped key is shared across every
    // trigger kind by construction, so a genuine cross-path race must dedup exactly like a
    // same-path race — the double-refund bug this matrix guards against does not care which two
    // adapters collided.
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus(); // shared inbox across both drivers
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);
    var snapshot =
        store.load(
            sagaId,
            SagaType.fromClass(FakeSagaState.class),
            FakeSagaState.class); // both drivers hold the same v1 view

    // Driver A: a redelivered correlated event resumes the claimed episode.
    StepOutcome a =
        exec.execute(
            sagaId,
            snapshot,
            new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 9L), false, false));
    // Driver B: a DIFFERENT trigger type (sweeper / no-event resume) races the SAME loaded view.
    StepOutcome b =
        exec.execute(
            sagaId,
            snapshot,
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(bus.execCount("undo-1"))
        .as("episode key dedups across DIFFERENT recovery paths — no double refund")
        .isEqualTo(1);
    assertThat(a)
        .as("first driver's outcome is benign")
        .isIn(StepOutcome.COMPENSATED, StepOutcome.NOT_COMPENSATING);
    assertThat(b)
        .as("second (racing) driver's outcome is benign")
        .isIn(StepOutcome.COMPENSATED, StepOutcome.NOT_COMPENSATING);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    // Exactly one terminal write across both drivers (v1 -> v2). A second write would be v3.
    assertThat(saved.version()).as("the losing driver's terminal CAS lost benignly").isEqualTo(2L);
  }

  // ==== Invariant 5: transient COMPENSATING_LEFT, then resume-to-terminal (undo total == 1) =====

  @ParameterizedTest(name = "{0}")
  @MethodSource("triggers")
  void transientCompensationLeavesCompensating_thenAnyTriggerResumesToTerminalOnce(TriggerCase tc) {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);
    bus.failTransientlyNTimes(
        "undo-1", 1); // the compensation fails transiently on the first attempt

    // First attempt: undo-1 throws transiently -> RETRY -> saga LEFT COMPENSATING (not
    // terminalized).
    StepOutcome first =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));
    assertThat(first)
        .as("transient compensation failure leaves COMPENSATING")
        .isEqualTo(StepOutcome.COMPENSATING_LEFT);
    assertThat(bus.execCount("undo-1"))
        .as("the throwing attempt did not commit the refund")
        .isZero();
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    // Second attempt via ANY trigger: undo-1 now succeeds -> terminal COMPENSATED, total refunds ==
    // 1.
    StepOutcome resumed =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            tc.build().get());

    assertThat(resumed).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("refund totals exactly once across both attempts")
        .isEqualTo(1);
    assertThat(bus.execCount("do-2")).as("forward never re-runs while resuming").isZero();
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  // ==== Invariant 3: lost-claim dispatches nothing =============================================

  @Test
  void claimTimeout_freshClaim_claimsThenCompensates_atEpisodeVersionPlusOne() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING); // v1

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ClaimTimeout(new SagaTimeoutException(sagaId, Duration.ofSeconds(1))));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(saved.version()).as("claim (v2) + terminal (v3)").isEqualTo(3L);
    assertThat(bus.execCount("undo-1")).isEqualTo(1);
  }

  @Test
  void claimTimeout_freshClaimLoses_dispatchesNothing_returnsClaimLost() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    var stale =
        store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class); // v1
    store.update(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING,
        1L); // v2

    StepOutcome outcome =
        exec.execute(
            sagaId,
            stale,
            new SagaTrigger.ClaimTimeout(new SagaTimeoutException(sagaId, Duration.ofSeconds(1))));

    assertThat(outcome).isEqualTo(StepOutcome.CLAIM_LOST);
    assertThat(bus.execCount("undo-1")).as("a lost claim dispatches ZERO compensation").isZero();
  }

  @Test
  void forwardStartPath_freshCreateLoses_dispatchesNothingAtAll_returnsSkippedDedup() {
    // Coverage gap (create-first): the start path loads
    // NOTHING (Optional.empty) while a concurrent writer already owns the row with an APPLIED
    // genesis. The OLD shape — "the forward charge ran, then the fresh claim-first create() lost"
    // — is impossible now: the genesis-pending create precedes the first dispatch, so the losing
    // driver has dispatched NOTHING (no charge, no refund) and routes on the reload: the
    // recorded genesis is applied, the delivery is a dedup.
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    bus.failTransientlyNTimes("do-2", 1); // would take the compensate path — never reached
    // A concurrent owner already created the row (so the genesis-pending create will lose).
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            Optional.empty(),
            new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, false));

    assertThat(outcome).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.execCount("undo-1"))
        .as("a lost genesis-pending create dispatches ZERO compensation")
        .isZero();
    assertThat(bus.execCount("do-1"))
        .as("the intent row precedes the first dispatch — no charge either")
        .isZero();
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .version())
        .as("the owner's row is untouched")
        .isEqualTo(1L);
  }

  @Test
  void
      forwardStartPath_staleGenesisPendingView_claimLoses_dispatchesNoCompensation_returnsClaimLost() {
    // The start-path CLAIM_LOST branch survives create-first in this shape — this
    // deliverer's view of the genesis-pending row (v1) is STALE: between its load and its claim a
    // concurrent writer advanced the row (here a timeout claim, v1 -> COMPENSATING v2). The forward
    // charge ran (it dedups on its offset-stable key on redelivery), Reserve failed, and the
    // claim-first CAS at v1 loses. Claim-first means the loser dispatches ZERO compensation and
    // SWALLOWS the conflict (no orphaned refund; the winner's episode owns the undo).
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    bus.failTransientlyNTimes("do-2", 1); // Reserve fails -> compensate path
    store.createGenesisPending(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING,
        false); // v1, genesis pending
    var stale = store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class);
    store.claimCompensating(
        sagaId, FAKE_TYPE, new FakeSagaState(SagaStatus.RUNNING, sagaId.value()), 1L); // v2

    StepOutcome outcome =
        exec.execute(
            sagaId, stale, new SagaTrigger.ForwardStep(startEnvelope("x", 0L), true, false));

    assertThat(outcome).isEqualTo(StepOutcome.CLAIM_LOST);
    assertThat(bus.execCount("undo-1"))
        .as("a lost start-path claim dispatches ZERO compensation")
        .isZero();
    assertThat(bus.execCount("do-1"))
        .as("the forward charge ran before the failure, as expected")
        .isEqualTo(1);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status())
        .as("the concurrent claim survives untouched")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(saved.version()).isEqualTo(2L);
  }

  @Test
  void forwardCorrelatedPath_lostClaim_propagatesOptimisticLock_zeroCompensation() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    var stale =
        store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class); // v1
    store.update(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING,
        1L); // v2
    bus.failTransientlyNTimes("do-2", 1);

    assertThatThrownBy(
            () ->
                exec.execute(
                    sagaId,
                    stale,
                    new SagaTrigger.ForwardStep(startEnvelope("x", 5L), false, false)))
        .as("correlated path: a lost claim propagates for batch retry")
        .isInstanceOf(OptimisticLockException.class);
    assertThat(bus.execCount("undo-1")).as("a lost claim dispatches ZERO compensation").isZero();
  }

  // ==== Invariant 4: crash-before-terminal is recoverable ======================================

  @Test
  void crashAfterClaimBeforeDispatch_leavesRecoverableCompensating_thenAnyTriggerCompletesOnce() {
    // Seam AFTER_CLAIM: the COMPENSATING claim is durably written, then the process crashes BEFORE
    // any compensation command is dispatched. Recovery: the row is COMPENSATING at the claimed
    // version, so a subsequent drive (here: a correlated ForwardStep) resumes it and dispatches the
    // refund exactly once.
    var backing = new InMemorySagaStore();
    var crashStore = new CrashPointStore(backing, CrashSeam.AFTER_CLAIM);
    var bus = new DedupingCommandBus();
    var exec = newExecutor(crashStore, bus);
    var sagaId = SagaId.of("saga-x");
    backing.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING); // v1

    // Drive a fresh ClaimTimeout: it CAS-claims COMPENSATING (durable), then the seam crashes.
    assertThatThrownBy(
            () ->
                exec.execute(
                    sagaId,
                    backing.load(
                        sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
                    new SagaTrigger.ClaimTimeout(
                        new SagaTimeoutException(sagaId, Duration.ofSeconds(1)))))
        .isInstanceOf(EventStoreException.class);

    var afterCrash =
        backing
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(afterCrash.status())
        .as("claim durably written before the crash")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(afterCrash.version()).as("claimed at v2 (v1 + claim)").isEqualTo(2L);
    assertThat(bus.execCount("undo-1")).as("no compensation dispatched before the crash").isZero();

    // Redrive with a DIFFERENT trigger (correlated ForwardStep). Seam is disarmed.
    StepOutcome resumed =
        exec.execute(
            sagaId,
            backing.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 7L), false, false));

    assertThat(resumed).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(
            backing
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("refund runs exactly once across crash + redrive")
        .isEqualTo(1);
    assertThat(bus.execCount("do-2")).as("forward never runs during recovery").isZero();
  }

  @Test
  void crashAfterDispatchBeforeTerminal_leavesRecoverableCompensating_thenRedriveDedupsToOnce() {
    // Seam BEFORE_TERMINAL: compensation is dispatched (refund runs), then the process crashes
    // BEFORE the durable terminal write. Recovery: the row is still COMPENSATING at the claimed
    // version; a resume re-dispatches under the SAME episode key (dedups in the inbox), so the
    // refund still totals exactly once across the crash + redrive.
    var backing = new InMemorySagaStore();
    var crashStore = new CrashPointStore(backing, CrashSeam.BEFORE_TERMINAL);
    var bus = new DedupingCommandBus();
    var exec = newExecutor(crashStore, bus);
    var sagaId = SagaId.of("saga-x");
    backing.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING); // v1

    assertThatThrownBy(
            () ->
                exec.execute(
                    sagaId,
                    backing.load(
                        sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
                    new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException())))
        .isInstanceOf(EventStoreException.class);

    var afterCrash =
        backing
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(afterCrash.status())
        .as("terminal write lost — still recoverable COMPENSATING")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(afterCrash.version()).as("still at the claimed version v1").isEqualTo(1L);
    assertThat(bus.execCount("undo-1"))
        .as("the refund already ran during the crashed attempt")
        .isEqualTo(1);

    // Redrive (seam disarmed): the refund dedups under the shared episode key; drive to terminal.
    StepOutcome resumed =
        exec.execute(
            sagaId,
            backing.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(resumed).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(
            backing
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("no double refund across the crash + redrive")
        .isEqualTo(1);
    assertThat(bus.execCount("do-2")).as("forward never runs during recovery").isZero();
  }

  // ==== shared harness (copied from SagaStepExecutorTest + SagaStartEventClaimFirstCrashTest) ===

  // ==== FAULTED rows in the matrix ==================================================

  /**
   * A FAULTED row records the status it replaced ({@code pre_fault_status}, written by {@code
   * markFaulted}). A LIVE forward step never revives it ({@code SKIPPED_HALTED}); a replay re-drive
   * routes on {@code effectiveStatus()} — here COMPENSATING — and resumes the claimed episode
   * exactly once, clearing the fault with the terminal CAS.
   */
  @Test
  void faultedRow_liveForwardStepIsSkipped_replayRedriveResumesFromTheRecordedPreFaultStatus() {
    var store = new InMemorySagaStore();
    var bus = new DedupingCommandBus();
    var exec = newExecutor(store, bus);
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.COMPENSATING);
    assertThat(store.markFaulted(sagaId, FAKE_TYPE, 1L)).isTrue();

    StepOutcome live =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 3L), false, false));
    assertThat(live).isEqualTo(StepOutcome.SKIPPED_HALTED);
    assertThat(bus.execCount("undo-1")).isZero();

    StepOutcome replay =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ForwardStep(pokeEnvelope("saga-x", 3L), false, true));
    assertThat(replay).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.execCount("undo-1"))
        .as("the recorded pre-fault status COMPENSATING resumes the episode exactly once")
        .isEqualTo(1);
    var saved =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saved.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(saved.preFaultStatus()).isNull();
  }

  private SagaStepExecutor<FakeSagaState> newExecutor(SagaStore store, DedupingCommandBus bus) {
    return new SagaStepExecutor<>(new FakeOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
  }

  record Start(String id) implements DomainEvent {}

  record Poke(String id) implements DomainEvent {}

  interface TestCommand extends Command {
    String cmdId();
  }

  /** Forward: charge payment. */
  record Charge(String cmdId) implements TestCommand {}

  /** Forward: reserve stock (fails transiently on the first attempt when configured). */
  record Reserve(String cmdId) implements TestCommand {}

  /** Compensation: refund the charge — must run exactly once per episode. */
  record Refund(String cmdId) implements TestCommand {}

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

  /** Which crash seam a {@link CrashPointStore} injects. */
  enum CrashSeam {
    /** Crash AFTER the COMPENSATING claim is durably written, BEFORE any compensation dispatch. */
    AFTER_CLAIM,
    /** Crash BEFORE the durable terminal write (AFTER compensation has already dispatched). */
    BEFORE_TERMINAL
  }

  /**
   * A {@link SagaStore} decorator that injects a single, arm-once crash at a chosen seam — modelled
   * on {@code SagaStartEventClaimFirstCrashTest.CrashOnTerminalWriteStore}, generalized to the two
   * recoverable seams the executor exposes between claim and terminal.
   *
   * <ul>
   *   <li>{@link CrashSeam#AFTER_CLAIM}: a write persisting {@code COMPENSATING} is delegated (the
   *       claim survives) and THEN throws — the process "dies" after claiming, before dispatch.
   *   <li>{@link CrashSeam#BEFORE_TERMINAL}: a write persisting a terminal status throws BEFORE
   *       delegating — the terminal write is lost, so the row stays {@code COMPENSATING}.
   * </ul>
   *
   * Both leave a recoverable {@code COMPENSATING} row at the claimed version.
   */
  static final class CrashPointStore implements SagaStore {
    private final SagaStore delegate;
    private final CrashSeam seam;
    private final AtomicBoolean armed = new AtomicBoolean(true);

    CrashPointStore(SagaStore delegate, CrashSeam seam) {
      this.delegate = delegate;
      this.seam = seam;
    }

    private void crashBeforeIfTerminal(SagaStatus status) {
      if (seam == CrashSeam.BEFORE_TERMINAL
          && status.isTerminal()
          && armed.compareAndSet(true, false)) {
        throw new EventStoreException("simulated crash before durable terminal write");
      }
    }

    private void crashAfterIfClaim(SagaStatus status) {
      if (seam == CrashSeam.AFTER_CLAIM
          && status == SagaStatus.COMPENSATING
          && armed.compareAndSet(true, false)) {
        throw new EventStoreException("simulated crash after durable COMPENSATING claim");
      }
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      crashBeforeIfTerminal(status);
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
      crashAfterIfClaim(status);
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
      crashBeforeIfTerminal(status);
      delegate.update(sagaId, sagaType, state, status, expectedVersion);
      crashAfterIfClaim(status);
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

  private static EventEnvelope startEnvelope(String id, long offset) {
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

  private static EventEnvelope pokeEnvelope(String sagaCorrelation, long offset) {
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

  // ==== The compensation key is scoped to the DURABLE episode, on the resume paths ====
  //
  // SagaStepExecutor derives the compensation IdempotencyKey from requireStampedEpisode(...) — the
  // stamped episode_version — NOT from loaded.version(), which shifts on every unrelated row write.
  // This matters because after a fault->replay cycle the row version moves while the episode
  // identity does not, so the two arguments DIVERGE and only the stamped one keeps the key stable.
  //
  // No test pinned that on the ResumeCompensation branch (the sweeper and the timeout runner's
  // COMPENSATING-resume). The matrix above seeds every case with store.create(..., COMPENSATING)
  // at version 1, whose stamp (episode_version = 1, the version the row is born at) is
  // equal to loaded.version(), so the two expressions are indistinguishable throughout. Replacing
  // the stamped value with loaded.get().version() on the ResumeCompensation branch left the whole
  // suite green — and in
  // production would re-issue an already-executed refund, because the recomputed key misses the
  // command-inbox row written under the original episode.
  //
  // These tests are non-vacuous BY CONSTRUCTION: the seed makes episode_version == 2 while the row
  // version is 4, and the assertion names episode:2. Under the mutation the executor would derive
  // episode:4 — a different, asserted-against value — so the test cannot pass either way round.

  /** A bus that records the exact IdempotencyKey of every dispatch and dedups on it. */
  static final class KeyRecordingBus implements CommandBus {
    final java.util.List<String> keys = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.Set<IdempotencyKey> committed =
        java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicInteger> execCounts =
        new java.util.concurrent.ConcurrentHashMap<>();

    int execCount(String cmdId) {
      var counter = execCounts.get(cmdId);
      return counter == null ? 0 : counter.get();
    }

    /** Pre-commits a key, modelling a command-inbox row written by an earlier episode run. */
    void preCommit(String key) {
      committed.add(IdempotencyKey.of(key));
    }

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
      keys.add(key.value());
      if (!committed.add(key)) {
        return ok(); // already executed under this exact key — the inbox dedup
      }
      execCounts
          .computeIfAbsent(
              ((TestCommand) command).cmdId(), k -> new java.util.concurrent.atomic.AtomicInteger())
          .incrementAndGet();
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  /**
   * Seeds a COMPENSATING episode whose stamped episode_version (2) has DIVERGED from the row
   * version (4) — claimCompensating at v1->v2 stamps the episode, then markFaulted + un-fault move
   * the row to v4 while deliberately leaving episode_version alone.
   */
  private static SagaId seedDivergedEpisode(InMemorySagaStore store) {
    var sagaId = SagaId.of("saga-x");
    store.create(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.RUNNING, sagaId.value()),
        SagaStatus.RUNNING);
    store.claimCompensating(
        sagaId, FAKE_TYPE, new FakeSagaState(SagaStatus.COMPENSATING, sagaId.value()), 1L);
    store.update(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.COMPENSATING, sagaId.value()),
        SagaStatus.COMPENSATING,
        2L);
    store.update(
        sagaId,
        FAKE_TYPE,
        new FakeSagaState(SagaStatus.COMPENSATING, sagaId.value()),
        SagaStatus.COMPENSATING,
        3L);
    var loaded =
        store
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(loaded.version())
        .as("the row version moved with the fault/replay cycle")
        .isEqualTo(4L);
    assertThat(loaded.episodeVersion())
        .as("the durable episode identity did NOT move")
        .isEqualTo(2L);
    return sagaId;
  }

  @Test
  void resumeCompensation_keysCompensationOnTheDurableEpisode_notTheShiftedRowVersion() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new KeyRecordingBus();
    var exec = newExecutorWith(store, bus);
    var sagaId = seedDivergedEpisode(store);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
            new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.keys)
        .as("the compensation key must be scoped to episode 2 (stamped), never row version 4")
        .containsExactly("saga:saga-x:episode:2:comp:0");
  }

  @Test
  void resumeCompensation_dedupsAgainstAnEarlierRunOfTheSAMEEpisode() {
    // The production consequence, stated as a test: comp#0 already committed under episode 2 in an
    // earlier run of this episode. After the fault->replay cycle shifted the row version to 4, the
    // resume must recompute the SAME key and dedup — a version-scoped key would miss it and issue
    // the refund a second time.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new KeyRecordingBus();
    bus.preCommit("saga:saga-x:episode:2:comp:0");
    var exec = newExecutorWith(store, bus);
    var sagaId = seedDivergedEpisode(store);

    exec.execute(
        sagaId,
        store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
        new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException()));

    assertThat(bus.execCount("undo-1"))
        .as("the already-committed compensation must NOT be issued a second time")
        .isZero();
  }

  @Test
  void timeoutResume_keysCompensationOnTheDurableEpisode_notTheShiftedRowVersion() {
    // The sibling resume path: SagaTimeoutRunner routes an already-COMPENSATING row through
    // ResumeCompensation rather than re-claiming it, so it must derive the same episode-scoped key.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new KeyRecordingBus();
    var exec = newExecutorWith(store, bus);
    var sagaId = seedDivergedEpisode(store);

    exec.execute(
        sagaId,
        store.load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class),
        new SagaTrigger.ResumeCompensation(
            new SagaTimeoutException(sagaId, Duration.ofSeconds(1))));

    assertThat(bus.keys).containsExactly("saga:saga-x:episode:2:comp:0");
  }

  private SagaStepExecutor<FakeSagaState> newExecutorWith(SagaStore store, CommandBus bus) {
    return new SagaStepExecutor<>(new FakeOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayerTest.Harness;
import org.streamrune.runtime.SagaStartFixtures.StartState;

/**
 * Recovery transitions of the saga state machine reachable through {@code SagaRunner} + {@code
 * SagaDeadLetterReplayer} alone, with the reused {@link Harness} and {@link
 * SagaDeadLetterReplayerTest.PerOffsetPoisonOrchestrator}. Each method below is tagged with the
 * transition(s) it pins. Two transitions are pinned elsewhere, not here: the FAULTED still-poison
 * refresh (FAULTED -> FAULTED) lives in {@code SagaDeadLetterReplayerTest} next to its
 * poison-refresh setup — see the pointer comment there. The genesis-pending timeout ->
 * claimCompensating transition is NOT pinned anywhere with this Harness: {@code SagaTimeoutRunner}
 * requires a {@code SagaDecider}, a distinct interface from the {@code SagaOrchestrator} this
 * Harness's orchestrator implements (no start/correlate routing), and there is no adapter from
 * {@code SagaOrchestrator} to {@code SagaDecider} — only the reverse ({@code SagaDeciderAdapter},
 * used internally by {@code SagaTimeoutRunner}'s own crash-resume executor). Exercising it
 * faithfully needs a dedicated {@code SagaDecider} implementation, its own {@code
 * SagaTimeoutRunner} instance, and a deterministic single-poll trigger — a new integration seam
 * this test does not build; see {@code SagaTimeoutRunnerTest} / {@code SagaConcurrencyRaceTest} for
 * the general timeout -> claimCompensating mechanism (not the genesis-pending-specific nuance).
 *
 * <p><b>Three transitions are pinned only PARTIALLY — named here so the list is not misread as
 * complete coverage.</b> ACTIVE forward step -> ACTIVE / COMPENSATING / DONE (per outcome) is
 * pinned only for the ACTIVE -> ACTIVE sub-case, exercised incidentally by the live
 * correlated-event delivery in {@link
 * #shield_isSetByEveryHold_andClearedOnlyByAnObservedEmptyBacklog}; the COMPENSATING and DONE
 * outcomes of an ACTIVE forward step are not exercised by any method here. COMPENSATING -> DONE
 * (compensation completes / gives up) is pinned only for the COMPENSATED sub-case ({@link
 * #genesisPending_deterministicDispatchFailure_claimsCompensation_genesisStaysUnapplied}, {@link
 * #compensating_giveUp_recordsCompensating_replayResumesAndTerminalizes}, {@link
 * #compensatingGenesisPending_giveUp_recordsCompensating_replayResumesNeverRedrivesForward}); the
 * FAILED sub-case is not exercised here. COMPENSATING -> FAULTED (poison in {@code compensate} or
 * give-up horizon) is pinned only for the sweeper's give-up-horizon sub-case ({@code
 * sweepCompensation}, in the same two give-up tests that pin the COMPENSATED case above); a poison
 * thrown from {@code compensate()} is not exercised here.
 */
class SagaRecoveryStateMachineTest {
  private static final SagaId SAGA = SagaDeadLetterReplayerTest.SAGA;

  // ABSENT -> ABSENT (initialState throws; the one row-less residual).
  @Test
  void absent_initialStatePoison_staysAbsent_entryOnly() {
    var h = new Harness();
    var start = h.start();
    h.orchestrator.initialStatePoison = true;
    h.deliver(start);
    assertThat(h.sagaStore.load(SAGA, TYPE, StartState.class))
        .as("initialState threw before any row write — the saga stays row-less")
        .isEmpty();
    assertThat(h.entries()).hasSize(1);
    assertThat(h.bus.count("step-1")).isZero();
  }

  // ABSENT -> GENESIS_PENDING (create v1), GENESIS_PENDING -> ACTIVE (applyEvent),
  // GENESIS_PENDING -> GENESIS_PENDING (retry-later re-drive).
  @Test
  void absent_toGenesisPending_toActive() {
    var h = new Harness();
    var start = h.start();
    h.bus.failOnce("step-2", () -> new CircuitBreakerOpenException("open"));
    try {
      h.deliver(start);
    } catch (RuntimeException _) {
      // the listener propagates retry-later so the subscription redelivers
    }
    var pending = h.row();
    assertThat(pending.genesisApplied()).isFalse();
    assertThat(pending.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(pending.version()).isEqualTo(1L);
    assertThat(h.sagaStore.findTimedOut(TYPE, h.clock.instant().plus(Duration.ofDays(1)), 10))
        .as("the timeout runner can see a genesis-pending saga")
        .contains(SAGA);

    h.deliver(start); // redelivery
    var active = h.row();
    assertThat(active.genesisApplied()).isTrue();
    assertThat(active.version()).isEqualTo(2L);
    assertThat(active.lastAppliedOffset()).isEqualTo(h.offset(start));
    assertThat(h.bus.count("step-1")).isEqualTo(1);
    assertThat(h.bus.count("step-2")).isEqualTo(1);
  }

  // ABSENT -> FAULTED via createFaulted (pre NULL, shield on — SagaRunner.recordPoison's
  // row-absent branch), FAULTED(pre NULL) -> ACTIVE by replayed START.
  @Test
  void absent_evolvePoison_createsFaultedRow_thenStartReplayReopensForward() {
    var h = new Harness();
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    var faulted = h.row();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus()).isNull();
    assertThat(faulted.genesisApplied()).isFalse();
    assertThat(faulted.deadLetterPending()).isTrue();
    assertThat(faulted.version())
        .as(
            "createFaulted's v1 — discriminates this ABSENT->FAULTED path from markFaulted's v+1"
                + " on an existing row")
        .isEqualTo(1L);

    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    var active = h.row();
    assertThat(active.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(active.genesisApplied()).isTrue();
    assertThat(active.preFaultStatus()).isNull();
    assertThat(active.deadLetterPending()).isFalse();
    assertThat(active.lastReplayedOffset()).isEqualTo(h.offset(start));
  }

  // GENESIS_PENDING + poison -> markFaulted (pre NULL because genesis_applied=false, row
  // kept, version bumped — not createFaulted). The RUNNING-row twin of the COMPENSATING case
  // pinned by compensatingGenesisPending_... below.
  @Test
  void genesisPending_evolvePoison_marksFaulted_withNoPreFaultStatus_rowKept() {
    var h = new Harness();
    var start = h.start();
    h.bus.failOnce("step-2", () -> new CircuitBreakerOpenException("open"));
    try {
      h.deliver(start);
    } catch (RuntimeException _) {
      // the listener propagates retry-later so the subscription redelivers
    }
    var pending = h.row();
    assertThat(pending.genesisApplied()).isFalse();
    assertThat(pending.version()).isEqualTo(1L);

    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start); // redelivery: the re-drive's re-evolve is now poison
    var faulted = h.row();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus())
        .as("nothing to resume on a genesis-pending fault — a replay re-evolves from initialState")
        .isNull();
    assertThat(faulted.genesisApplied()).isFalse();
    assertThat(faulted.version())
        .as(
            "markFaulted's v+1 on the existing GENESIS_PENDING row — discriminates this path from"
                + " the earlier createFaulted (v1)")
        .isEqualTo(2L);
    assertThat(faulted.deadLetterPending()).isTrue();
  }

  // ACTIVE -> FAULTED (pre RUNNING), FAULTED(pre RUNNING) -> ACTIVE by replayed
  // correlated.
  @Test
  void active_toFaulted_recordsRunning_toActiveByReplay() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().preFaultStatus()).isNull();
  }

  // GENESIS_PENDING -> COMPENSATING via claimCompensating (genesis stays false), then
  // COMPENSATING -> DONE (the COMPENSATED branch).
  @Test
  void genesisPending_deterministicDispatchFailure_claimsCompensation_genesisStaysUnapplied() {
    var h = new Harness();
    var start = h.start();
    h.bus.failAlways("step-2", () -> new RuntimeException("business rejection")); // not retry-later
    h.deliver(start);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.episodeVersion()).isEqualTo(2L);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    h.deliver(start); // redelivery over a terminal row: skipped, nothing re-run
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
  }

  // COMPENSATING -> FAULTED (pre COMPENSATING, via the sweeper's give-up), then
  // FAULTED(pre COMPENSATING) -> DONE by replay resuming the episode.
  @Test
  void compensating_giveUp_recordsCompensating_replayResumesAndTerminalizes() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    h.deliver(corr); // held while faulted
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
  }

  // The same transitions on a genesis-pending row: a claimed episode is recorded even on a
  // genesis-pending row, and a replay resume never re-opens forward dispatch.
  @Test
  void compensatingGenesisPending_giveUp_recordsCompensating_replayResumesNeverRedrivesForward() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.bus.failAlways(
        "step-2", () -> new RuntimeException("business rejection")); // deterministic → claim-first
    h.bus.failOnce(
        "undo-1", () -> new CircuitBreakerOpenException("open")); // compensation left COMPENSATING
    h.deliver(start);
    var claimed = h.row();
    assertThat(claimed.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(claimed.genesisApplied()).isFalse();
    assertThat(claimed.episodeVersion()).isEqualTo(2L);

    assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
    assertThat(h.row().preFaultStatus())
        .as("a claimed episode is recorded even on a genesis-pending row")
        .isEqualTo(SagaStatus.COMPENSATING);
    h.deliver(corr); // held while faulted

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    // count("step-2") is zero by construction (failAlways never cleared, and
    // ScriptedBus counts only a SUCCESSFUL execute) — it holds regardless of whether forward
    // re-opens, so it cannot discriminate. This assertion does: consumed already holds
    // offset(start)
    // once from the original genesis evolve; a forward re-drive would append a second copy, and a
    // forward consumption of corr would append it too. containsExactly(offset(start)) rules out
    // both — it subsumes the old doesNotContain(corr) check.
    assertThat(h.orchestrator.consumed)
        .as("resume never re-drives forward: no re-evolve of the start, corr never consumed")
        .containsExactly(h.offset(start));
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  // any -> live event held -> shield on; any -> drain observes empty backlog ->
  // shield off; the shield set before every publish is implied by the state observed throughout.
  @Test
  void shield_isSetByEveryHold_andClearedOnlyByAnObservedEmptyBacklog() {
    var h = new Harness();
    var start = h.start();
    var a = h.advance();
    var b = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(a));
    h.deliver(a, b);
    assertThat(h.row().deadLetterPending()).isTrue();
    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replay(SAGA, a.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.row().deadLetterPending()).as("b is still pending").isTrue();
    assertThat(h.replayer.replay(SAGA, b.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.row().deadLetterPending()).isFalse();
    var c = h.advance();
    h.deliver(c);
    assertThat(h.orchestrator.consumed).contains(h.offset(c));
  }
}

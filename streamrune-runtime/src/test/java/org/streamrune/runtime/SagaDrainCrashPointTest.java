package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ResumeOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayerTest.Harness;
import org.streamrune.runtime.SagaStartFixtures.SimulatedProcessCrash;

/**
 * Drain crash points: anchor before feed, discard before shield clear, the crash point a forced
 * stale-compensation resume introduces, and the crash point of the operator's {@code
 * resumeFaulted}.
 */
class SagaDrainCrashPointTest {
  private static final SagaId SAGA = SagaDeadLetterReplayerTest.SAGA;

  @Test
  void crashAfterTheAnchor_beforeTheFeed_neverForgesALicense() {
    var h = new Harness();
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    h.orchestrator.evolvePoison.clear();
    h.dlq.crashOnceAfterAnchor();
    Instant attempt = h.clock.instant();

    Throwable crash = catchThrowable(() -> h.replayer.replayAll(SAGA));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.bus.count("step-1")).isZero();
    assertThat(h.entries().get(0).firstReplayStartedAt()).isEqualTo(attempt);

    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.STALE_REDRIVE_BLOCKED);
    assertThat(h.replayer.replayAll(SAGA, true)).containsExactly(ReplayOutcome.REPLAYED);
    // The convergence half was previously inferred from REPLAYED alone.
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().genesisApplied()).isTrue();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  // Moved verbatim from SagaDeadLetterReplayerTest: the
  // implementer's original version here hand-staged the post-crash state with discard() followed
  // by feedReplay() — the reverse of the drain's real feed-then-discard order — and never crossed
  // the discard -> clearShieldIfDrained seam. This is the sibling that actually crashes there, via
  // crashOnceAfter("discard"); both drain crash points now live together in this file.
  @Test
  void crashAfterTheDiscardBeforeTheShieldClear_rerunClearsTheShield() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.dlq.crashOnceAfter("discard");

    assertThat(catchThrowable(() -> h.replayer.replayAll(SAGA)))
        .isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().deadLetterPending())
        .as("the shield stays TRUE with no entries — errs toward recording")
        .isTrue();

    var live = h.advance();
    h.deliver(live);
    assertThat(h.orchestrator.consumed).doesNotContain(h.offset(live));
    assertThat(h.entries()).as("the next live event is held (recorded)").hasSize(1);

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .containsExactly(h.offset(start), h.offset(corr), h.offset(live));
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * The crash point the forced pass-through introduces: a forced resume of a stale compensation
   * episode dies after {@code crashAfter} executed and BEFORE the terminal write. The whole story
   * runs through the real paths:
   *
   * <ol>
   *   <li>the start's second forward command is rejected, so the start path claims the episode;
   *       undo-1 executes, undo-2 fails transiently, and the episode is left COMPENSATING;
   *   <li>an outage longer than the inbox window: the inbox sweeper prunes undo-1's dedup key;
   *   <li>a live correlated event meets the executor's key-age guard — quarantined, saga FAULTED,
   *       nothing dispatched — and a plain drain is {@code STALE_COMPENSATION_BLOCKED};
   *   <li>the operator reconciles and forces; the process dies mid-resume.
   * </ol>
   *
   * <p>The crashed attempt wrote nothing durable but the entry's first-replay anchor and the inbox
   * keys of the commands it executed: the row is still FAULTED over the SAME episode, whose {@code
   * episode_claimed_at} did not move, so a plain rerun is still refused — the crash forged no
   * license. The forced rerun converges: it re-dispatches the episode under the SAME episode keys,
   * every command the crashed attempt committed dedups on the key it wrote moments ago, and only
   * the rest runs. What re-executes is exactly the at-least-once the operator acknowledged: undo-1,
   * whose ORIGINAL key was pruned, runs a second time in total and never a third; undo-2 runs once.
   */
  @ParameterizedTest
  @ValueSource(strings = {"undo-1", "undo-2"})
  void forcedStaleResume_crashBeforeTheTerminalWrite_forcedRerunConverges(String crashAfter) {
    var meter = new SagaDeadLetterReplayerTest.ForcedResumeMeter();
    var h = Harness.autoConfigured(meter);
    h.orchestrator.undos = List.of("undo-1", "undo-2");
    var start = h.start();
    var corr = h.advance();
    h.bus.failAlways("step-2", () -> new IllegalArgumentException("payment declined"));
    h.bus.failOnce("undo-2", () -> new IllegalStateException("connection reset"));
    h.deliver(start);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isZero();

    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
    h.bus.pruneCommittedKeys();
    h.deliver(corr);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.bus.count("undo-1")).as("the live path dispatched nothing").isEqualTo(1);
    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);
    long faultedVersion = h.row().version();

    h.bus.crashOnceAfterCommit(crashAfter);
    assertThat(catchThrowable(() -> h.replayer.replayAll(SAGA, true)))
        .isInstanceOf(SimulatedProcessCrash.class);

    assertThat(h.bus.count("undo-1"))
        .as("its original key was pruned: the forced resume re-executed it (acknowledged)")
        .isEqualTo(2);
    assertThat(h.bus.count("undo-2")).isEqualTo(crashAfter.equals("undo-2") ? 1 : 0);
    var crashed = h.row();
    assertThat(crashed.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(crashed.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(crashed.version())
        .as("no saga-row write in the crashed attempt")
        .isEqualTo(faultedVersion);
    assertThat(crashed.deadLetterPending()).isTrue();
    assertThat(h.entries()).hasSize(1);
    assertThat(meter.forcedStaleResumes).as("the crashed attempt overrode the guard").hasSize(1);
    assertThat(h.replayer.replayAll(SAGA))
        .as("the crash forged no license: a plain rerun is still refused")
        .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);

    assertThat(h.replayer.replayAll(SAGA, true)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1"))
        .as("dedups on the key the crashed attempt wrote — never a third execution")
        .isEqualTo(2);
    assertThat(h.bus.count("undo-2")).as("exactly once across the crash").isEqualTo(1);
    assertThat(meter.forcedStaleResumes)
        .as("each forced resume that overrode the guard is metered; the refused plain rerun is not")
        .hasSize(2);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.preFaultStatus()).isNull();
    assertThat(h.entries()).isEmpty();
    assertThat(row.deadLetterPending()).isFalse();
  }

  /**
   * {@link SagaDeadLetterReplayer#resumeFaulted(SagaId, boolean)} dies after {@code crashAfter}
   * committed and BEFORE the terminal write. The give-up shape comes from the real paths: the start
   * claims the episode (step-2 rejected); undo-1 executes, undo-2 and undo-3 fail transiently; the
   * compensation-retry sweeper gives up — FAULTED, {@code pre_fault_status = COMPENSATING}, no
   * dead-letter entry. With {@code stale}, an outage longer than the inbox window then pruned
   * undo-1's dedup key, and the operator reconciles and forces.
   *
   * <p>The crashed attempt writes nothing durable on the saga row (no claim, no version bump — the
   * resume runs at the durable {@code episode_version}) and no dead-letter entry: only the inbox
   * keys of the commands it executed. So the row is still FAULTED over the SAME episode with the
   * SAME {@code episode_claimed_at} — for a stale episode a plain rerun is still refused (the crash
   * forged no license) — and a rerun re-dispatches under the SAME keys: every command the crashed
   * attempt committed dedups, only the rest runs, and the terminal write clears the fault. What
   * re-executes is exactly the at-least-once a forced resume acknowledged — undo-1, whose ORIGINAL
   * key was pruned, runs a second time in total and never a third — and nothing re-executes without
   * force.
   */
  @ParameterizedTest
  @CsvSource({"undo-2, false", "undo-3, false", "undo-2, true", "undo-3, true"})
  void resumeFaulted_crashBeforeTheTerminalWrite_rerunConverges(String crashAfter, boolean stale) {
    var meter = new SagaResumeFaultedTest.ResumeMeter();
    var h = Harness.autoConfigured(meter);
    h.orchestrator.undos = List.of("undo-1", "undo-2", "undo-3");
    h.bus.failAlways("step-2", () -> new IllegalArgumentException("payment declined"));
    h.bus.failOnce("undo-2", () -> new IllegalStateException("connection reset"));
    h.bus.failOnce("undo-3", () -> new IllegalStateException("connection reset"));
    h.deliver(h.start());
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
    assertThat(h.entries()).isEmpty();
    if (stale) {
      h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
      h.bus.pruneCommittedKeys();
      assertThat(h.replayer.resumeFaulted(SAGA))
          .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
    }
    var faulted = h.row();

    h.bus.crashOnceAfterCommit(crashAfter);
    assertThat(catchThrowable(() -> h.replayer.resumeFaulted(SAGA, stale)))
        .isInstanceOf(SimulatedProcessCrash.class);

    int undo1 = stale ? 2 : 1; // stale: its original key was pruned — the acknowledged re-run
    assertThat(h.bus.count("undo-1")).isEqualTo(undo1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.bus.count("undo-3")).isEqualTo(crashAfter.equals("undo-3") ? 1 : 0);
    var crashed = h.row();
    assertThat(crashed.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(crashed.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(crashed.version())
        .as("no saga-row write in the crashed attempt")
        .isEqualTo(faulted.version());
    assertThat(crashed.episodeVersion()).isEqualTo(faulted.episodeVersion());
    assertThat(crashed.episodeClaimedAt()).isEqualTo(faulted.episodeClaimedAt());
    assertThat(h.entries()).as("no event, no entry").isEmpty();
    if (stale) {
      assertThat(h.replayer.resumeFaulted(SAGA))
          .as("the crash forged no license: a plain rerun is still refused")
          .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
    }

    assertThat(h.replayer.resumeFaulted(SAGA, stale)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1"))
        .as("dedups on the key the crashed attempt wrote — never a third execution")
        .isEqualTo(undo1);
    assertThat(h.bus.count("undo-2")).as("exactly once across the crash").isEqualTo(1);
    assertThat(h.bus.count("undo-3")).as("exactly once across the crash").isEqualTo(1);
    assertThat(meter.forcedStaleResumes).hasSize(stale ? 2 : 0);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(h.replayer.resumeFaulted(SAGA, stale))
        .as("a rerun after the terminal write changes nothing")
        .isEqualTo(ResumeOutcome.NOT_FAULTED);
    assertThat(h.bus.count("undo-3")).isEqualTo(1);
  }
}

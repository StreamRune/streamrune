package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ResumeOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayerTest.Harness;
import org.streamrune.runtime.SagaStartFixtures.SimulatedProcessCrash;

/**
 * {@link SagaDeadLetterReplayer#compensateFaulted(SagaId)} — the operator path for a saga a forward
 * step faulted once nothing is left to drain. Discarding the poison entry of a forward fault (its
 * event can no longer be read, or the business process was cancelled out-of-band) leaves the row
 * {@code FAULTED} with a forward {@code pre_fault_status} and no entry: {@code replayAll} has
 * nothing to feed, {@code resumeFaulted} refuses a forward fault, the live path holds every later
 * correlated event behind the fault, and neither the timeout runner nor the compensation-retry
 * sweeper ever selects a {@code FAULTED} row. {@code compensateFaulted} claims a fresh compensation
 * episode at the faulted row's version and compensates from the persisted state — what the saga's
 * timeout would have done — and {@code discard} names it when it leaves such a row behind.
 */
class SagaCompensateFaultedTest {
  private static final SagaId SAGA = SagaDeadLetterReplayerTest.SAGA;

  /** Records the replayer's compensateFaulted outcomes. */
  static final class CompensateMeter implements StreamRuneMetrics {
    final List<String> outcomes = new CopyOnWriteArrayList<>();
    final List<String> forcedStaleResumes = new CopyOnWriteArrayList<>();

    @Override
    public void recordSagaCompensateFaulted(String sagaType, String outcome) {
      assertThat(sagaType).isEqualTo(TYPE.value());
      outcomes.add(outcome);
    }

    @Override
    public void recordSagaForcedStaleResume(String sagaType) {
      forcedStaleResumes.add(sagaType);
    }
  }

  /**
   * The forward-fault shape, produced by the real paths: the start commits both forward commands
   * (the money moved), then a correlated event poisons {@code evolve} — the row is FAULTED with
   * {@code pre_fault_status = RUNNING} and owns the event's dead-letter entry.
   */
  static Harness forwardFaulted(StreamRuneMetrics meter) {
    var h = Harness.autoConfigured(meter);
    h.deliver(h.start());
    var corr = h.advance();
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.episodeVersion()).as("a forward fault belongs to no episode").isNull();
    assertThat(h.entries()).hasSize(1);
    assertThat(h.bus.count("step-1")).isEqualTo(1);
    assertThat(h.bus.count("step-2")).isEqualTo(1);
    return h;
  }

  /** {@link #forwardFaulted}, then the operator discards the fault's only entry. */
  static Harness stranded(StreamRuneMetrics meter) {
    var h = forwardFaulted(meter);
    assertThat(h.replayer.discard(SAGA, h.entries().get(0).eventOffset())).isTrue();
    assertThat(h.entries()).isEmpty();
    return h;
  }

  private static List<String> warnings(Runnable body) {
    return SagaDeadLetterReplayerTest.warningsDuring(SagaDeadLetterReplayer.class, body);
  }

  @Test
  void discardingTheLastEntryOfAForwardFault_namesTheRemedy_andCompensateFaultedTerminalizesIt() {
    var meter = new CompensateMeter();
    var h = forwardFaulted(meter);
    var entry = h.entries().get(0);

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, entry.eventOffset())).isTrue());

    assertThat(warnings)
        .filteredOn(w -> w.contains("no dead-letter entry left"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains(SAGA.value(), "compensateFaulted(sagaId)", "replayAll(sagaId)"));
    var stranded = h.row();
    assertThat(stranded.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(stranded.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(stranded.deadLetterPending()).as("the backlog is empty: shield cleared").isFalse();
    assertThat(h.replayer.replayAll(SAGA)).as("nothing left to drain").isEmpty();
    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.FORWARD_FAULT);
    assertThat(h.runner.sweepCompensation(SAGA, false))
        .as("the compensation-retry sweeper never re-drives a FAULTED row")
        .isEqualTo(StepOutcome.NOT_COMPENSATING);
    assertThat(h.row().version()).as("none of them wrote").isEqualTo(stranded.version());

    // Longer than the inbox window: irrelevant here — the episode is fresh, its keys are new.
    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
    var claimedAt = h.clock.instant();
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.TERMINAL);

    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.episodeVersion())
        .as("a fresh episode, claimed at the faulted row's version")
        .isEqualTo(stranded.version() + 1);
    assertThat(row.episodeClaimedAt()).isEqualTo(claimedAt);
    assertThat(h.bus.count("undo-1")).as("compensated under the fresh episode key").isEqualTo(1);
    assertThat(h.bus.count("step-1")).as("no forward command re-dispatched").isEqualTo(1);
    assertThat(h.bus.count("step-2")).isEqualTo(1);
    assertThat(h.entries()).isEmpty();
    assertThat(meter.forcedStaleResumes).isEmpty();
    assertThat(meter.outcomes).containsExactly("TERMINAL");

    assertThat(h.replayer.compensateFaulted(SAGA))
        .as("a rerun over the terminal row changes nothing")
        .isEqualTo(CompensateOutcome.NOT_FAULTED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("TERMINAL", "NOT_FAULTED");
  }

  /**
   * A genesis-pending forward fault (the start event's {@code evolve} threw) whose start entry is
   * discarded can never land its genesis: a live start over a FAULTED row is skipped, and every
   * later correlated entry would be deferred. {@code compensateFaulted} is its only remedy, and the
   * discard says so.
   */
  @Test
  void aGenesisPendingForwardFault_whoseStartEntryWasDiscarded_isCompensatedFromItsRecordedState() {
    var meter = new CompensateMeter();
    var h = Harness.autoConfigured(meter);
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    assertThat(h.row().preFaultStatus()).isNull();
    assertThat(h.row().genesisApplied()).isFalse();

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, start.globalOffset())).isTrue());

    assertThat(warnings)
        .filteredOn(w -> w.contains("no dead-letter entry left"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains(SAGA.value(), "genesis", "compensateFaulted(sagaId)")
                    .doesNotContain("replayAll"));
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.TERMINAL);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().genesisApplied()).isFalse();
    assertThat(h.bus.count("step-1")).isZero();
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("TERMINAL");
  }

  @Test
  void aForwardFaultThatStillOwnsItsEntry_isEntriesPending_andNothingIsWritten() {
    var meter = new CompensateMeter();
    var h = forwardFaulted(meter);
    long version = h.row().version();

    List<String> warnings =
        warnings(
            () ->
                assertThat(h.replayer.compensateFaulted(SAGA))
                    .isEqualTo(CompensateOutcome.ENTRIES_PENDING));

    assertThat(warnings)
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "replayAll(sagaId)", "discard"));
    assertThat(h.row().version()).isEqualTo(version);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.entries()).hasSize(1);
    assertThat(h.bus.count("undo-1")).isZero();
    assertThat(meter.outcomes).containsExactly("ENTRIES_PENDING");
  }

  /**
   * A row that faulted inside a compensation episode keeps that episode's stamp. A fresh claim
   * there would mint new compensation keys and re-execute every compensation the episode already
   * committed, so it is refused and {@code resumeFaulted} — which resumes under the episode's own
   * keys — is named instead.
   */
  @Test
  void aCompensationFault_isRefused_andResumeFaultedFinishesItWithoutReExecutingAnUndo() {
    var meter = new CompensateMeter();
    var h = SagaResumeFaultedTest.gaveUp(meter);
    var faulted = h.row();

    List<String> warnings =
        warnings(
            () ->
                assertThat(h.replayer.compensateFaulted(SAGA))
                    .isEqualTo(CompensateOutcome.COMPENSATION_FAULT));

    assertThat(warnings)
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "resumeFaulted(sagaId)"));
    assertThat(h.row().version()).isEqualTo(faulted.version());
    assertThat(h.row().episodeVersion()).isEqualTo(faulted.episodeVersion());
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isZero();

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1")).as("dedups on the episode's own key").isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("COMPENSATION_FAULT");
  }

  @Test
  void liveCompensatingTerminalAndAbsentRows_areRefused_andLeftAlone() {
    var meter = new CompensateMeter();
    var live = Harness.autoConfigured(meter);
    live.deliver(live.start());
    long version = live.row().version();
    assertThat(live.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.NOT_FAULTED);
    assertThat(live.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(live.row().version()).isEqualTo(version);

    var compensating = Harness.autoConfigured(meter);
    compensating.bus.failAlways("step-2", () -> new IllegalArgumentException("declined"));
    compensating.bus.failOnce("undo-1", () -> new IllegalStateException("connection reset"));
    compensating.deliver(compensating.start());
    assertThat(compensating.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(compensating.replayer.compensateFaulted(SAGA))
        .as("a COMPENSATING row is its re-drivers' — never re-claimed")
        .isEqualTo(CompensateOutcome.NOT_FAULTED);
    assertThat(compensating.bus.count("undo-1")).isZero();

    SagaId forged = SagaId.of("saga-9\nFORGED log line");
    List<String> warnings =
        warnings(
            () ->
                assertThat(live.replayer.compensateFaulted(forged))
                    .isEqualTo(CompensateOutcome.SAGA_NOT_FOUND));
    assertThat(warnings).singleElement().satisfies(w -> assertThat(w).doesNotContain("\n"));
    assertThat(catchThrowable(() -> live.replayer.compensateFaulted(null)))
        .isInstanceOf(NullPointerException.class);

    assertThat(meter.outcomes).containsExactly("NOT_FAULTED", "NOT_FAULTED", "SAGA_NOT_FOUND");
  }

  /**
   * {@code compensate()} throws on the fresh episode: the executor faults the row inside it ({@code
   * pre_fault_status = COMPENSATING}, the new stamp kept) — the give-up shape, which {@code
   * resumeFaulted} owns once the orchestrator is fixed. {@code compensateFaulted} now refuses it.
   */
  @Test
  void compensateThrows_isPoison_theRowFaultsInsideTheNewEpisode_andResumeFaultedFinishesIt() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    long strandedVersion = h.row().version();
    h.orchestrator.compensatePoison = true;

    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.POISON);

    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.episodeVersion()).isEqualTo(strandedVersion + 1);
    assertThat(h.entries()).as("no triggering event, no entry").isEmpty();
    assertThat(h.bus.count("undo-1")).isZero();

    h.orchestrator.compensatePoison = false;
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.COMPENSATION_FAULT);
    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().episodeVersion()).isEqualTo(strandedVersion + 1);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("POISON", "COMPENSATION_FAULT");
  }

  /**
   * A transient compensation failure leaves the claimed episode {@code COMPENSATING}: from there it
   * is an ordinary claimed episode, re-driven by the compensation-retry sweeper (or the timeout
   * runner, or a correlated event) under the same episode keys.
   */
  @Test
  void aTransientCompensationFailure_leavesTheEpisodeCompensating_forItsAutomaticReDrivers() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    long strandedVersion = h.row().version();
    h.bus.failOnce("undo-1", () -> new IllegalStateException("connection reset"));

    List<String> warnings =
        warnings(
            () ->
                assertThat(h.replayer.compensateFaulted(SAGA))
                    .isEqualTo(CompensateOutcome.COMPENSATING));

    assertThat(warnings)
        .filteredOn(w -> w.contains("COMPENSATING"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "sweeper"));
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.episodeVersion()).isEqualTo(strandedVersion + 1);
    assertThat(row.preFaultStatus()).isNull();

    assertThat(h.runner.sweepCompensation(SAGA, false)).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("COMPENSATING");
  }

  /**
   * Two operators, or an operator and a concurrent drain: the claim is a compare-and-swap at the
   * loaded version, so a writer that moved the row first wins and the loser dispatches nothing.
   */
  @Test
  void aClaimLostToAConcurrentWriter_dispatchesNothing_andTheRerunCompensates() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    var racing =
        new BeforeClaim(
            h.sagaStore,
            () ->
                assertThat(h.sagaStore.markFaulted(SAGA, TYPE, h.row().version()))
                    .as("a concurrent refresh of the fault bumps the version")
                    .isFalse());
    var replayer = SagaResumeFaultedTest.rig(h, racing, h.orchestrator, meter).replayer();

    assertThat(replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.CLAIM_LOST);

    assertThat(h.bus.count("undo-1")).isZero();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.TERMINAL);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(meter.outcomes).containsExactly("CLAIM_LOST", "TERMINAL");
  }

  /**
   * A live correlated event that loads the row while it is still FAULTED is held behind it (entry +
   * shield, no version bump), so the claim still commits. Over the terminal row the entry is moot
   * but keeps the shield: the replayer re-reads the backlog afterwards and names {@code replayAll},
   * which converges it.
   */
  @Test
  void anEntryHeldBeforeTheClaim_isReportedAfterTheTerminalWrite_andReplayAllConvergesIt() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    var corr = h.advance();
    var replayer =
        SagaResumeFaultedTest.rig(
                h, new BeforeClaim(h.sagaStore, () -> h.deliver(corr)), h.orchestrator, meter)
            .replayer();

    List<String> warnings =
        warnings(
            () ->
                assertThat(replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.TERMINAL));

    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.entries()).as("held behind the FAULTED row").hasSize(1);
    assertThat(h.row().deadLetterPending()).isTrue();
    assertThat(warnings)
        .filteredOn(w -> w.contains("arrived while the compensation ran"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "1 dead-letter entry", "replayAll"));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * Crash right after the claim committed, before any compensation was dispatched: the row is a
   * stamped {@code COMPENSATING} episode — no longer FAULTED, so a rerun of the same call refuses
   * it as {@code NOT_FAULTED} and dispatches nothing, and the episode's automatic re-drivers own
   * it.
   */
  @Test
  void crashAfterTheClaim_leavesAStampedEpisode_whichTheSweeperCompletesUnderItsOwnKeys() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    long strandedVersion = h.row().version();
    var crashing = new ForwardingSagaStore(h.sagaStore);
    crashing.crashOnceAfter("claimCompensating");
    var replayer = SagaResumeFaultedTest.rig(h, crashing, h.orchestrator, meter).replayer();

    assertThat(catchThrowable(() -> replayer.compensateFaulted(SAGA)))
        .isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);

    var crashed = h.row();
    assertThat(crashed.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(crashed.episodeVersion()).isEqualTo(strandedVersion + 1);
    assertThat(h.bus.count("undo-1")).isZero();
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.NOT_FAULTED);
    assertThat(h.bus.count("undo-1")).isZero();

    assertThat(h.runner.sweepCompensation(SAGA, false)).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().episodeVersion()).isEqualTo(strandedVersion + 1);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
  }

  /**
   * Crash after a compensation committed, before the terminal write: the re-driver resumes the same
   * episode under the same keys, so the committed compensation dedups and only the rest runs.
   */
  @Test
  void crashAfterACompensationCommitted_theReDriveDedupsIt_andRunsOnlyTheRest() {
    var meter = new CompensateMeter();
    var h = stranded(meter);
    h.orchestrator.undos = List.of("undo-1", "undo-2");
    h.bus.crashOnceAfterCommit("undo-1");

    assertThat(catchThrowable(() -> h.replayer.compensateFaulted(SAGA)))
        .isInstanceOf(SimulatedProcessCrash.class);

    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isZero();
    assertThat(h.replayer.compensateFaulted(SAGA)).isEqualTo(CompensateOutcome.NOT_FAULTED);

    assertThat(h.runner.sweepCompensation(SAGA, false)).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(h.bus.count("undo-1")).as("dedups on the episode key").isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * A forward fault with a later correlated event held behind it: discarding the poison entry
   * leaves an entry, so nothing is stranded and no remedy is named — the drain resumes the saga
   * forward, past the discarded event.
   */
  @Test
  void discardingAForwardFaultsEntry_whileAHeldEventRemains_namesNothing_andTheDrainResumesIt() {
    var h = forwardFaulted(StreamRuneMetrics.NOOP);
    var poison = h.entries().get(0);
    EventEnvelope held = h.advance();
    h.deliver(held);
    assertThat(h.entries()).hasSize(2);

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, poison.eventOffset())).isTrue());

    assertThat(warnings).filteredOn(w -> w.contains("no dead-letter entry left")).isEmpty();
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.orchestrator.consumed).contains(h.offset(held));
  }

  /** A give-up-shaped row whose held entry is discarded: the discard names resumeFaulted. */
  @Test
  void discardingTheLastEntryOfACompensationFault_namesResumeFaulted() {
    var h = SagaResumeFaultedTest.gaveUp(StreamRuneMetrics.NOOP);
    var corr = h.advance();
    h.deliver(corr);
    assertThat(h.entries()).as("held behind the FAULTED row").hasSize(1);

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, corr.globalOffset())).isTrue());

    assertThat(warnings)
        .filteredOn(w -> w.contains("no dead-letter entry left"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains(SAGA.value(), "resumeFaulted(sagaId)")
                    .doesNotContain("compensateFaulted"));
  }

  /** A live row is not stranded by a discard: no remedy is named. */
  @Test
  void discardingAnEntryOverALiveRow_namesNothing() {
    var h = Harness.autoConfigured(StreamRuneMetrics.NOOP);
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    h.orchestrator.evolvePoison.clear();
    var held = h.advance();
    h.deliver(held); // held behind the genesis-pending fault
    assertThat(h.replayer.replay(SAGA, start.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, held.globalOffset())).isTrue());

    assertThat(warnings).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * The strand check runs after the entry is gone and the shield cleared, so a re-read that fails
   * must not turn a discard that succeeded into an exception.
   */
  @Test
  void aStrandCheckThatCannotReadTheBacklog_stillReportsTheDiscard() {
    var h = forwardFaulted(StreamRuneMetrics.NOOP);
    var entry = h.entries().get(0);
    h.dlq.findBySagaFailure = new IllegalStateException("connection reset");

    List<String> warnings =
        warnings(() -> assertThat(h.replayer.discard(SAGA, entry.eventOffset())).isTrue());

    h.dlq.findBySagaFailure = null;
    assertThat(h.entries()).isEmpty();
    assertThat(warnings)
        .filteredOn(w -> w.contains("could not be checked"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "compensateFaulted"));
  }

  /** Runs {@code beforeClaim} once, right before the claim reaches the store. */
  static final class BeforeClaim extends ForwardingSagaStore {
    private Runnable pending;

    BeforeClaim(SagaStore delegate, Runnable beforeClaim) {
      super(delegate);
      this.pending = beforeClaim;
    }

    @Override
    public void claimCompensating(SagaId id, SagaType t, SagaState s, long v) {
      if (pending != null) {
        Runnable once = pending;
        pending = null;
        once.run();
      }
      super.claimCompensating(id, t, s, v);
    }
  }
}

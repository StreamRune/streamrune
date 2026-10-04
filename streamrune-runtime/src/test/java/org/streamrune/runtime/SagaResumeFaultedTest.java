package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayer.ResumeOutcome;
import org.streamrune.runtime.SagaDeadLetterReplayerTest.Harness;
import org.streamrune.runtime.SagaStartFixtures.Advance;
import org.streamrune.runtime.SagaStartFixtures.StartState;

/**
 * {@link SagaDeadLetterReplayer#resumeFaulted(SagaId, boolean)} — the operator repair for a
 * compensation episode the compensation-retry sweeper or the timeout runner gave up on. The give-up
 * writes {@code FAULTED} with {@code pre_fault_status = COMPENSATING} and NO dead-letter entry, so
 * {@code replayAll} has nothing to feed; before this operation the only remedy was a manual
 * database repair. Every outcome is pinned here; the crash point lives in {@link
 * SagaDrainCrashPointTest}.
 */
class SagaResumeFaultedTest {
  private static final SagaId SAGA = SagaDeadLetterReplayerTest.SAGA;

  /** Records the replayer's resume outcomes and the executor's forced stale resumes. */
  static final class ResumeMeter implements StreamRuneMetrics {
    final List<String> resumeOutcomes = new CopyOnWriteArrayList<>();
    final List<String> forcedStaleResumes = new CopyOnWriteArrayList<>();

    @Override
    public void recordSagaResumeFaulted(String sagaType, String outcome) {
      assertThat(sagaType).isEqualTo(TYPE.value());
      resumeOutcomes.add(outcome);
    }

    @Override
    public void recordSagaForcedStaleResume(String sagaType) {
      forcedStaleResumes.add(sagaType);
    }
  }

  /**
   * The give-up shape, produced by the real paths: the start's second forward command is rejected,
   * so the start claims the episode; undo-1 executes, undo-2 fails transiently and the episode is
   * left COMPENSATING; the compensation-retry sweeper then gives up ({@code
   * sweepCompensation(giveUp = true)}, exactly what the sweeper calls past {@code giveUpAfter} or
   * over a stale episode — and the timeout runner's give-ups write the same {@code markFaulted}).
   */
  static Harness gaveUp(StreamRuneMetrics meter) {
    var h = Harness.autoConfigured(meter);
    h.orchestrator.undos = List.of("undo-1", "undo-2");
    h.bus.failAlways("step-2", () -> new IllegalArgumentException("payment declined"));
    h.bus.failOnce("undo-2", () -> new IllegalStateException("connection reset"));
    h.deliver(h.start());
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isZero();

    assertThat(h.runner.sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(h.entries()).as("a give-up writes no dead-letter entry").isEmpty();
    return h;
  }

  @Test
  void giveUpFault_hasNothingForTheDrain_butResumeFaultedCompensates_dedupingTheCommittedUndo() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    assertThat(h.replayer.replayAll(SAGA)).as("the drain has nothing to feed").isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

    assertThat(h.bus.count("undo-1"))
        .as("committed by the original attempt: dedups on its episode key")
        .isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.preFaultStatus()).as("the success CAS cleared the fault").isNull();
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(h.entries()).isEmpty();
    assertThat(meter.resumeOutcomes).containsExactly("RESUMED");
    assertThat(meter.forcedStaleResumes).isEmpty();

    assertThat(h.replayer.resumeFaulted(SAGA))
        .as("a second call meets a terminal row and changes nothing")
        .isEqualTo(ResumeOutcome.NOT_FAULTED);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
  }

  /**
   * A shield with no entry is a legal residue (the shield errs toward recording — a crash after the
   * last discard, before the clear). It is not a pending entry, so it does not refuse the resume,
   * and the store's conditional clear releases it once the saga is terminal.
   */
  @Test
  void aShieldWithNoEntry_doesNotRefuse_andIsReleasedByTheTerminalResume() {
    var h = gaveUp(StreamRuneMetrics.NOOP);
    h.sagaStore.setDeadLetterPending(SAGA, TYPE, true);

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  @Test
  void aPermanentCompensationFailure_isAlsoATerminalResume_theRowIsFailed() {
    var h = gaveUp(StreamRuneMetrics.NOOP);
    h.bus.failOnce("undo-2", () -> new IllegalArgumentException("refund rejected"));

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);

    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAILED);
    assertThat(row.preFaultStatus()).isNull();
  }

  @Test
  void staleEpisode_isRefusedWithoutForce_rowUntouched_thenAForcedResumeIsLoudAndAtLeastOnce() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    long faultedVersion = h.row().version();
    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
    h.bus.pruneCommittedKeys(); // the outage pruned undo-1's dedup key

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> {
              assertThat(h.replayer.resumeFaulted(SAGA))
                  .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
              assertThat(h.replayer.resumeFaulted(SAGA, false))
                  .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
            });
    assertThat(warnings)
        .filteredOn(w -> w.contains("resumeFaulted(sagaId, true)"))
        .as("each refusal names the remedy")
        .hasSize(2);
    assertThat(h.bus.count("undo-1")).as("nothing dispatched").isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isZero();
    assertThat(h.row().version()).as("no row write").isEqualTo(faultedVersion);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(meter.forcedStaleResumes).isEmpty();

    List<String> executorWarnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaStepExecutor.class,
            () ->
                assertThat(h.replayer.resumeFaulted(SAGA, true)).isEqualTo(ResumeOutcome.RESUMED));

    assertThat(h.bus.count("undo-1"))
        .as("its original key was pruned: the forced resume re-executed it (acknowledged)")
        .isEqualTo(2);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().preFaultStatus()).isNull();
    assertThat(meter.forcedStaleResumes)
        .as("force reached the executor's key-age guard")
        .containsExactly(TYPE.value());
    assertThat(executorWarnings)
        .filteredOn(w -> w.contains("Operator forced an at-least-once resume"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "resumeFaulted(sagaId, true)"));
    assertThat(meter.resumeOutcomes)
        .containsExactly("STALE_COMPENSATION_BLOCKED", "STALE_COMPENSATION_BLOCKED", "RESUMED");
  }

  /**
   * The replayer's own refusal needs its {@code inboxRetentionMaxAge}; a replayer built without one
   * still cannot resume a stale episode silently, because the executor's key-age guard (armed on
   * the runner) is the second line — and with no triggering event, its refusal records nothing: no
   * entry, no re-fault, no dispatch.
   */
  @Test
  void aWindowlessReplayer_isStillRefusedByTheExecutorsGuard_andNothingIsRecorded() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var windowless =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(h.dlq)
            .sagaStore(h.sagaStore)
            .eventStore(h.eventStore)
            .sagaRunner(h.runner)
            .metrics(meter)
            .clock(h.clock)
            .build();
    long faultedVersion = h.row().version();
    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));

    List<String> executorWarnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaStepExecutor.class,
            () ->
                assertThat(windowless.resumeFaulted(SAGA))
                    .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED));

    assertThat(executorWarnings)
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains("resumeFaulted(sagaId, true)")
                    .doesNotContain("Quarantining the triggering event"));
    assertThat(h.bus.count("undo-2")).isZero();
    assertThat(h.row().version()).isEqualTo(faultedVersion);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.entries()).as("no event to quarantine: nothing recorded").isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();

    assertThat(windowless.resumeFaulted(SAGA, true)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(meter.forcedStaleResumes).hasSize(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("STALE_COMPENSATION_BLOCKED", "RESUMED");
  }

  @Test
  void compensateStillThrows_isStillPoison_rowReFaultedInPlace_noEntry_thenResumesAfterTheFix() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    long faultedVersion = h.row().version();
    h.orchestrator.compensatePoison = true;

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.STILL_POISON);

    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version()).as("markFaulted refreshed the fault").isGreaterThan(faultedVersion);
    assertThat(h.entries()).as("no triggering event, no entry").isEmpty();
    assertThat(h.bus.count("undo-2")).isZero();

    h.orchestrator.compensatePoison = false;
    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("STILL_POISON", "RESUMED");
  }

  @Test
  void aTransientFailure_makesNoDurableProgress_theRowStaysFaulted_andALaterResumeCompletes() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    long faultedVersion = h.row().version();
    h.bus.failOnce("undo-2", () -> new IllegalStateException("connection reset"));

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.NO_PROGRESS));

    assertThat(warnings)
        .filteredOn(w -> w.contains("no durable progress"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(StepOutcome.COMPENSATING_LEFT.name()));
    var row = h.row();
    assertThat(row.status())
        .as("only a successful CAS clears a fault; the sweeper never sees a FAULTED row")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version()).isEqualTo(faultedVersion);

    h.bus.failOnce("undo-2", () -> new CircuitBreakerOpenException("breaker open"));
    assertThat(h.replayer.resumeFaulted(SAGA))
        .as("a refused admission attempted nothing")
        .isEqualTo(ResumeOutcome.NO_PROGRESS);
    assertThat(h.row().version()).isEqualTo(faultedVersion);

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("NO_PROGRESS", "NO_PROGRESS", "RESUMED");
  }

  @Test
  void noRow_isSagaNotFound_andTheForgedIdIsSanitizedInTheWarning() {
    var meter = new ResumeMeter();
    var h = Harness.autoConfigured(meter);
    SagaId forged = SagaId.of("saga-9\nFORGED log line");

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () ->
                assertThat(h.replayer.resumeFaulted(forged))
                    .isEqualTo(ResumeOutcome.SAGA_NOT_FOUND));

    assertThat(warnings).singleElement().satisfies(w -> assertThat(w).doesNotContain("\n"));
    assertThat(meter.resumeOutcomes).containsExactly("SAGA_NOT_FOUND");
    assertThat(catchThrowable(() -> h.replayer.resumeFaulted(null)))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void liveCompensatingAndTerminalRows_areNotFaulted_andAreLeftAlone() {
    var meter = new ResumeMeter();
    var h = Harness.autoConfigured(meter);
    h.deliver(h.start());
    long version = h.row().version();
    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.NOT_FAULTED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().version()).isEqualTo(version);

    var compensating = Harness.autoConfigured(meter);
    compensating.bus.failAlways("step-2", () -> new IllegalArgumentException("declined"));
    compensating.bus.failOnce("undo-1", () -> new IllegalStateException("connection reset"));
    compensating.deliver(compensating.start());
    assertThat(compensating.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(compensating.replayer.resumeFaulted(SAGA, true))
        .as("a COMPENSATING row is the sweeper's to re-drive, force or not")
        .isEqualTo(ResumeOutcome.NOT_FAULTED);
    assertThat(compensating.bus.count("undo-1")).isZero();

    var done = gaveUp(meter);
    assertThat(done.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(done.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.NOT_FAULTED);

    assertThat(meter.resumeOutcomes)
        .containsExactly("NOT_FAULTED", "NOT_FAULTED", "RESUMED", "NOT_FAULTED");
  }

  @Test
  void aForwardFault_isRefused_itOwnsItsEntry_andReplayAllIsTheRemedy() {
    var meter = new ResumeMeter();
    var h = Harness.autoConfigured(meter);
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    var faulted = h.row();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () ->
                assertThat(h.replayer.resumeFaulted(SAGA, true))
                    .isEqualTo(ResumeOutcome.FORWARD_FAULT));

    assertThat(warnings).singleElement().satisfies(w -> assertThat(w).contains("replayAll"));
    assertThat(h.row().version()).isEqualTo(faulted.version());
    assertThat(h.entries()).hasSize(1);

    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(meter.resumeOutcomes).containsExactly("FORWARD_FAULT");
  }

  @Test
  void aGenesisPendingForwardFault_isAForwardFaultToo() {
    var meter = new ResumeMeter();
    var h = Harness.autoConfigured(meter);
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().preFaultStatus()).isNull();

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.FORWARD_FAULT);
    assertThat(h.bus.count("step-1")).isZero();
    assertThat(meter.resumeOutcomes).containsExactly("FORWARD_FAULT");
  }

  /**
   * A live correlated event that arrives after the give-up is held behind the FAULTED row (entry +
   * shield). Ordering belongs to the drain: {@code resumeFaulted} refuses and names {@code
   * replayAll}, whose feed resumes the very same episode under the very same keys.
   */
  @Test
  void aHeldEntry_isEntriesPending_andTheDrainResumesTheEpisodeInOrder() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var corr = h.advance();
    h.deliver(corr);
    assertThat(h.entries()).as("held behind the FAULTED row").hasSize(1);
    long version = h.row().version();

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () ->
                assertThat(h.replayer.resumeFaulted(SAGA, true))
                    .isEqualTo(ResumeOutcome.ENTRIES_PENDING));

    assertThat(warnings).singleElement().satisfies(w -> assertThat(w).contains("replayAll"));
    assertThat(h.bus.count("undo-2")).isZero();
    assertThat(h.row().version()).isEqualTo(version);

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().deadLetterPending()).isFalse();
    assertThat(meter.resumeOutcomes).containsExactly("ENTRIES_PENDING");
  }

  @Test
  void aNullSagaEntryResolvedToTheSaga_isEntriesPendingToo() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var unroutable = h.advance();
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.deliver(unroutable);
    h.orchestrator.routerPoison.clear();
    assertThat(h.replayer.replay(null, unroutable.globalOffset()))
        .isEqualTo(ReplayOutcome.TARGET_PENDING);
    assertThat(h.entries()).as("no named entry: only the resolved null-saga one").isEmpty();

    assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.ENTRIES_PENDING);
    assertThat(h.bus.count("undo-2")).isZero();

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.dlq.findNullSagaEntry(TYPE, unroutable.globalOffset())).isEmpty();
  }

  // ===== Entries landing mid-resume, the unstamped anchor, two store-reached outcomes =====

  /** A runner and a replayer over {@code store}, sharing everything else with {@code h}. */
  record Rig(SagaRunner<StartState> runner, SagaDeadLetterReplayer replayer) {}

  /**
   * The harness's collaborators (bus, dead-letter store, event store, clock), with {@code store} as
   * the saga store of BOTH the runner and the replayer, and the runner's key-age window armed the
   * way the auto-configurations arm it — the decorator seam the replayer tests use.
   */
  static Rig rig(
      Harness h,
      SagaStore store,
      SagaOrchestrator<StartState> orchestrator,
      StreamRuneMetrics metrics) {
    SagaRunner<StartState> runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(store)
            .commandBus(h.bus)
            .sagaDeadLetterStore(h.dlq)
            .metrics(metrics)
            .clock(h.clock)
            .build();
    SagaEventPathRetention.applyDefault(List.of(runner), SagaDeadLetterReplayerTest.WINDOW);
    SagaDeadLetterReplayer replayer =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(h.dlq)
            .sagaStore(store)
            .eventStore(h.eventStore)
            .sagaRunner(runner)
            .metrics(metrics)
            .inboxRetentionMaxAge(SagaDeadLetterReplayerTest.WINDOW)
            .clock(h.clock)
            .build();
    return new Rig(runner, replayer);
  }

  /**
   * Runs {@code beforeTerminalWrite} once, right before the resume's terminal CAS reaches the store
   * — the point at which a concurrent actor can still see the row FAULTED.
   */
  static final class BeforeTerminalWrite extends ForwardingSagaStore {
    private Runnable pending;

    BeforeTerminalWrite(SagaStore delegate, Runnable beforeTerminalWrite) {
      super(delegate);
      this.pending = beforeTerminalWrite;
    }

    @Override
    public void update(SagaId id, SagaType t, SagaState s, SagaStatus st, long v) {
      if (pending != null && st.isTerminal()) {
        Runnable once = pending;
        pending = null;
        once.run();
      }
      super.update(id, t, s, st, v);
    }
  }

  /**
   * {@code refuseResume} saw no entry, but one can land while the resume runs: a live correlated
   * event that loads the row while it is still FAULTED is held behind it (entry + shield), and the
   * terminal write then commits (the hold bumps no version). Over the terminal row the entry is
   * moot, yet it keeps the shield until a drain discards it — so the replayer re-reads the backlog
   * after a RESUMED terminal write and names replayAll, which converges it.
   */
  @Test
  void anEntryHeldWhileTheResumeRuns_isReportedAfterTheTerminalWrite_andReplayAllConvergesIt() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var corr = h.advance();
    var replayer =
        rig(h, new BeforeTerminalWrite(h.sagaStore, () -> h.deliver(corr)), h.orchestrator, meter)
            .replayer();

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED));

    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.entries()).as("held behind the FAULTED row mid-resume").hasSize(1);
    assertThat(row.deadLetterPending()).as("the hold's shield outlives the fault").isTrue();
    assertThat(warnings)
        .filteredOn(w -> w.contains("arrived while the resume ran"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "1 dead-letter entry", "replayAll"));
    assertThat(meter.resumeOutcomes).containsExactly("RESUMED");

    assertThat(h.replayer.replayAll(SAGA))
        .as("moot over the terminal row: discarded")
        .containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.bus.count("undo-2")).isEqualTo(1);
  }

  /**
   * Follow-up. The backlog re-read runs after the terminal CAS has committed, so a read that fails
   * (a database blip) must not turn a resume that succeeded into an exception: the operator would
   * see a failure, the metric would never count RESUMED, and a rerun would answer NOT_FAULTED. The
   * resume is reported RESUMED and a WARNING says the backlog could not be re-read and names
   * replayAll.
   */
  @Test
  void aBacklogReReadThatFailsAfterTheTerminalWrite_stillReportsResumed() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var replayer =
        rig(
                h,
                new BeforeTerminalWrite(
                    h.sagaStore,
                    () -> h.dlq.findBySagaFailure = new IllegalStateException("connection reset")),
                h.orchestrator,
                meter)
            .replayer();

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED));

    h.dlq.findBySagaFailure = null;
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("RESUMED");
    assertThat(warnings)
        .filteredOn(w -> w.contains("could not be re-read"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value(), "replayAll"));
  }

  /** A clean resume has nothing to report: the re-read finds the backlog empty. */
  @Test
  void aResumeWithNoEntryArriving_reportsNoBacklog() {
    var h = gaveUp(StreamRuneMetrics.NOOP);

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(h.replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED));

    assertThat(warnings).isEmpty();
  }

  /**
   * The terminal CAS loses to a concurrent writer that re-faulted the same episode (a concurrent
   * resume whose {@code compensate()} threw refreshes the fault: version + 1, still FAULTED, {@code
   * pre_fault_status} and the episode identity kept). The executor swallows the lost CAS as a
   * benign race; only the row tells the truth, so the replayer reports NO_PROGRESS — and the rerun
   * converges with every undo deduplicated on the durable episode keys.
   */
  @Test
  void aTerminalWriteThatLosesItsCas_isNoProgress_andTheRerunDedupsEveryUndo() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    var faulted = h.row();
    var racing =
        new BeforeTerminalWrite(
            h.sagaStore,
            () ->
                assertThat(h.sagaStore.markFaulted(SAGA, TYPE, faulted.version()))
                    .as("a still-poison refresh, not a transition")
                    .isFalse());
    var replayer = rig(h, racing, h.orchestrator, meter).replayer();

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.NO_PROGRESS));

    assertThat(warnings)
        .filteredOn(w -> w.contains("no durable progress"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .as("the WARNING names the lost CAS, not a transient compensation failure")
                    .contains(SAGA.value(), StepOutcome.COMPENSATED.name(), "lost a race"));
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version())
        .as("only the concurrent refresh wrote")
        .isEqualTo(faulted.version() + 1);
    assertThat(row.episodeVersion()).isEqualTo(faulted.episodeVersion());
    assertThat(h.bus.count("undo-2")).as("dispatched before the lost CAS").isEqualTo(1);
    assertThat(h.entries()).isEmpty();

    assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).as("dedups on the durable episode key").isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("NO_PROGRESS", "RESUMED");
  }

  /**
   * The terminal write cannot convert the saga state, deterministically: the store raises {@link
   * SagaStateSerializationException} over a mapping {@link IOException}, the executor classifies it
   * as {@link SagaPoisonException}, and the replayer reports STILL_POISON — nothing written, no
   * entry (no event to quarantine). Once the conversion is fixed the rerun converges.
   */
  @Test
  void aTerminalWriteThatCannotConvertTheState_isStillPoison_nothingWritten_andTheRerunConverges() {
    var meter = new ResumeMeter();
    var h = gaveUp(meter);
    long faultedVersion = h.row().version();
    var unconvertible =
        new BeforeTerminalWrite(
            h.sagaStore,
            () -> {
              throw new SagaStateSerializationException(
                  "saga state cannot be serialized", SAGA, new IOException("unmappable shape"));
            });
    var replayer = rig(h, unconvertible, h.orchestrator, meter).replayer();

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaDeadLetterReplayer.class,
            () -> assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.STILL_POISON));

    assertThat(warnings)
        .filteredOn(w -> w.contains("cannot convert the saga state"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value()));
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.version()).as("no saga-row write").isEqualTo(faultedVersion);
    assertThat(h.entries()).as("no triggering event, no entry").isEmpty();
    assertThat(h.bus.count("undo-2")).as("dispatched before the terminal write").isEqualTo(1);

    assertThat(replayer.resumeFaulted(SAGA)).isEqualTo(ResumeOutcome.RESUMED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.bus.count("undo-2")).as("dedups on the durable episode key").isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.resumeOutcomes).containsExactly("STILL_POISON", "RESUMED");
  }

  /**
   * {@code h.orchestrator}, except that an {@link Advance} evolves the state to {@code
   * COMPENSATING}: the saga enters a compensation episode through the forward path's own write
   * ({@code applyEvent}), which stamps the episode exactly as a claim does.
   */
  static SagaOrchestrator<StartState> evolvingIntoCompensation(
      SagaDeadLetterReplayerTest.PerOffsetPoisonOrchestrator inner) {
    return new SagaOrchestrator<>() {
      @Override
      public Class<StartState> stateType() {
        return inner.stateType();
      }

      @Override
      public StartState initialState(SagaId sagaId) {
        return inner.initialState(sagaId);
      }

      @Override
      public boolean isStartEvent(EventEnvelope event) {
        return inner.isStartEvent(event);
      }

      @Override
      public SagaId extractSagaId(EventEnvelope event) {
        return inner.extractSagaId(event);
      }

      @Override
      public Optional<SagaId> correlate(EventEnvelope event) {
        return inner.correlate(event);
      }

      @Override
      public StartState evolve(StartState state, EventEnvelope event) {
        return event.event() instanceof Advance
            ? new StartState(SagaStatus.COMPENSATING, state.orderId())
            : inner.evolve(state, event);
      }

      @Override
      public List<SagaCommand> handle(StartState state, EventEnvelope event) {
        return inner.handle(state, event);
      }

      @Override
      public List<SagaCommand> compensate(
          StartState state, Throwable failure, SagaCommand failedCommand) {
        return inner.compensate(state, failure, failedCommand);
      }
    };
  }

  /**
   * A compensation episode entered through an evolved {@code SagaState.status()} of COMPENSATING is
   * persisted by the forward path's own write ({@code applyEvent}), and that write stamps the
   * episode exactly as a claim does — so EVERY COMPENSATING row carries its claim, and the faults
   * of such an episode keep it. Both replayer paths anchor on that stamp and on nothing else: the
   * key age is measured from the instant the episode was entered (T0), never from the row's last
   * write (the fault at T1), which is what the deleted {@code updated_at} fallback did.
   *
   * <p>The episode is entered at T0 and faulted at T1 = T0 + window + 1h, either through the event
   * path (the executor's own guard, anchored on T0, refuses the live resume and quarantines the
   * event: the drain's shape) or through the sweeper's give-up (resumeFaulted's shape). At T1 the
   * episode is fresh from the fault but old from the claim: both paths REFUSE it ({@code
   * STALE_COMPENSATION_BLOCKED}) — the fallback admitted it here — and {@code force} is the
   * acknowledged at-least-once resume under the stamped episode's keys.
   */
  @ParameterizedTest(name = "viaDrain={0}")
  @ValueSource(booleans = {true, false})
  void anEvolvedCompensatingEpisode_isStampedAtEntry_andBothPathsAnchorOnThatStamp(
      boolean viaDrain) {
    var h = Harness.autoConfigured(StreamRuneMetrics.NOOP);
    var rig = rig(h, h.sagaStore, evolvingIntoCompensation(h.orchestrator), StreamRuneMetrics.NOOP);
    rig.runner().asEventListener().onEvents(List.of(h.start()));
    Instant entered = h.clock.instant();
    rig.runner().asEventListener().onEvents(List.of(h.advance()));
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.episodeVersion())
        .as("stamped by the forward write that landed COMPENSATING, at the version it landed at")
        .isEqualTo(row.version());
    assertThat(row.episodeClaimedAt())
        .as("claimed at the instant the episode was entered")
        .isEqualTo(entered);

    h.clock.advance(SagaDeadLetterReplayerTest.WINDOW.plusHours(1));
    if (viaDrain) {
      rig.runner().asEventListener().onEvents(List.of(h.advance()));
      assertThat(h.entries()).as("the refused live resume quarantined its event").hasSize(1);
    } else {
      assertThat(rig.runner().sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
      assertThat(h.entries()).isEmpty();
    }
    var faulted = h.row();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(faulted.episodeVersion()).as("a fault keeps the stamp").isEqualTo(row.version());
    assertThat(faulted.episodeClaimedAt()).as("a fault keeps the stamp").isEqualTo(entered);
    assertThat(faulted.updatedAt())
        .as("the fault refreshed the last write")
        .isEqualTo(h.clock.instant());

    // Fresh from the fault, old from the claim: refused — the anchor is the claim, not the write.
    if (viaDrain) {
      assertThat(rig.replayer().replayAll(SAGA))
          .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);
    } else {
      assertThat(rig.replayer().resumeFaulted(SAGA))
          .isEqualTo(ResumeOutcome.STALE_COMPENSATION_BLOCKED);
    }
    assertThat(h.bus.count("undo-1")).isZero();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);

    // force: the acknowledged at-least-once resume, under the stamped episode's own keys.
    if (viaDrain) {
      assertThat(rig.replayer().replayAll(SAGA, true)).containsExactly(ReplayOutcome.REPLAYED);
    } else {
      assertThat(rig.replayer().resumeFaulted(SAGA, true)).isEqualTo(ResumeOutcome.RESUMED);
    }
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  /**
   * The store side: a {@link SagaStore} that leaves a COMPENSATING row unstamped violates its
   * contract, and every consumer refuses such an episode outright instead of guessing its keys
   * (from a row version a fault has moved) or its age (from a last write the fault refreshed) — the
   * event path as poison (the event is quarantined and the saga FAULTED, nothing dispatched), the
   * drain and {@code resumeFaulted} by throwing {@link SagaUnstampedCompensationEpisodeException}
   * before any anchor, feed or write, whatever {@code force} says. Modelled with a decorator that
   * strips the stamp on load: no shipped store can produce the row ({@code PostgresSagaStore}
   * refuses it at the schema).
   */
  @ParameterizedTest(name = "viaDrain={0}, force={1}")
  @CsvSource({"true, false", "true, true", "false, false", "false, true"})
  void anUnstampedCompensationEpisode_isRefusedByEveryPath_nothingDispatchedNothingWritten(
      boolean viaDrain, boolean force) {
    var h = Harness.autoConfigured(StreamRuneMetrics.NOOP);
    var unstamping =
        new ForwardingSagaStore(h.sagaStore) {
          @Override
          public <S extends SagaState> Optional<LoadedSaga<S>> load(
              SagaId id, SagaType t, Class<S> type) {
            return super.load(id, t, type)
                .map(
                    r ->
                        new LoadedSaga<>(
                            r.state(),
                            r.status(),
                            r.version(),
                            r.updatedAt(),
                            /* episodeVersion= */ null,
                            /* episodeClaimedAt= */ null,
                            r.genesisApplied(),
                            r.preFaultStatus(),
                            r.deadLetterPending(),
                            r.lastAppliedOffset(),
                            r.lastReplayedOffset()));
          }
        };
    var rig = rig(h, unstamping, evolvingIntoCompensation(h.orchestrator), StreamRuneMetrics.NOOP);
    rig.runner().asEventListener().onEvents(List.of(h.start()));
    rig.runner().asEventListener().onEvents(List.of(h.advance()));
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.row().episodeVersion()).as("the real store stamped the episode").isNotNull();
    assertThat(unstamping.load(SAGA, TYPE, StartState.class).orElseThrow().episodeVersion())
        .as("the decorator hides the stamp: the contract violation under test")
        .isNull();

    if (viaDrain) {
      rig.runner().asEventListener().onEvents(List.of(h.advance()));
      assertThat(h.entries())
          .as("the event path refused the resume as poison and quarantined the event")
          .hasSize(1);
    } else {
      assertThat(rig.runner().sweepCompensation(SAGA, true)).isEqualTo(StepOutcome.FAULTED);
    }
    var faulted = h.row();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.count("undo-1")).as("nothing dispatched").isZero();

    assertThatThrownBy(
            () -> {
              if (viaDrain) {
                rig.replayer().replayAll(SAGA, force);
              } else {
                rig.replayer().resumeFaulted(SAGA, force);
              }
            })
        .isInstanceOf(SagaUnstampedCompensationEpisodeException.class)
        .hasMessageContaining(SAGA.value())
        .hasMessageContaining("episode_version=null, episode_claimed_at=null");
    assertThat(h.bus.count("undo-1")).as("nothing dispatched").isZero();
    assertThat(h.row().version()).as("nothing written").isEqualTo(faulted.version());
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.entries()).as("the entry stays quarantined").hasSize(viaDrain ? 1 : 0);
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaStartFixtures.Advance;
import org.streamrune.runtime.SagaStartFixtures.ScriptedBus;
import org.streamrune.runtime.SagaStartFixtures.StartEvent;
import org.streamrune.runtime.SagaStartFixtures.StartState;
import org.streamrune.runtime.SagaStartFixtures.Step;
import org.streamrune.runtime.SagaStartFixtures.Undo;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/** Drain rules and drain crash points, pinned as schedules. */
class SagaDeadLetterReplayerTest {
  static final SagaId SAGA = SagaId.of("saga-1");
  static final Duration WINDOW = Duration.ofDays(7);

  /** Start = StartEvent, correlated = Advance; poison per offset for evolve and for the router. */
  static final class PerOffsetPoisonOrchestrator implements SagaOrchestrator<StartState> {
    final Set<Long> evolvePoison = ConcurrentHashMap.newKeySet();
    final Set<Long> routerPoison = ConcurrentHashMap.newKeySet();
    final List<Long> consumed = new ArrayList<>();

    /** When set, {@code correlate} ignores {@link Advance} events (the router "moved on"). */
    volatile boolean ignoreAdvance;

    /**
     * When set, {@code initialState} throws — the one row-less residual: the event is quarantined
     * but no row is ever written, since {@code createFaultedRow} re-throws on the same
     * deterministic failure.
     */
    volatile boolean initialStatePoison;

    @Override
    public Class<StartState> stateType() {
      return StartState.class;
    }

    @Override
    public StartState initialState(SagaId sagaId) {
      if (initialStatePoison) {
        throw new RuntimeException("initialState poison");
      }
      return new StartState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      if (routerPoison.contains(event.globalOffset().value())) {
        throw new RuntimeException("router poison at " + event.globalOffset().value());
      }
      return event.event() instanceof StartEvent;
    }

    /** Per-offset start routing overrides (a router hotfix moved a start). */
    final java.util.Map<Long, SagaId> startRoutesTo = new ConcurrentHashMap<>();

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return startRoutesTo.getOrDefault(event.globalOffset().value(), SAGA);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (ignoreAdvance) {
        return Optional.empty();
      }
      return event.event() instanceof Advance ? Optional.of(SAGA) : Optional.empty();
    }

    @Override
    public StartState evolve(StartState state, EventEnvelope event) {
      if (evolvePoison.contains(event.globalOffset().value())) {
        throw new RuntimeException("evolve poison at " + event.globalOffset().value());
      }
      consumed.add(event.globalOffset().value());
      return event.event() instanceof StartEvent
          ? new StartState(SagaStatus.RUNNING, "order-1")
          : state;
    }

    @Override
    public List<SagaCommand> handle(StartState state, EventEnvelope event) {
      return event.event() instanceof StartEvent
          ? List.of(
              SagaCommand.of(new Step("step-1"), AggregateId.of("a")),
              SagaCommand.of(new Step("step-2"), AggregateId.of("b")))
          : List.of();
    }

    /** The compensation commands' ids, in dispatch order ({@code undo-1} alone by default). */
    volatile List<String> undos = List.of("undo-1");

    /** When set, {@code compensate} throws — a poison compensation (no command is dispatched). */
    volatile boolean compensatePoison;

    @Override
    public List<SagaCommand> compensate(
        StartState state, Throwable failure, SagaCommand failedCommand) {
      if (compensatePoison) {
        throw new IllegalStateException("compensate poison");
      }
      return undos.stream().map(id -> SagaCommand.of(new Undo(id), AggregateId.of("a"))).toList();
    }
  }

  /** Records the saga type of every forced stale resume the executor meters. */
  static final class ForcedResumeMeter implements StreamRuneMetrics {
    final List<String> forcedStaleResumes = new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public void recordSagaForcedStaleResume(String sagaType) {
      forcedStaleResumes.add(sagaType);
    }
  }

  static final class Harness {
    // ONE clock for the store (created_at / episode_claimed_at), the runner (faulted_at) and the
    // replayer (anchors, windows): every time-based assertion in this file relies on
    // it.
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-09-10T00:00:00Z"));
    final InMemoryEventStore eventStore = new InMemoryEventStore();
    final InMemorySagaStore sagaStore = new InMemorySagaStore(clock);
    final ForwardingSagaDeadLetterStore dlq =
        new ForwardingSagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore));
    final ScriptedBus bus = new ScriptedBus();
    final PerOffsetPoisonOrchestrator orchestrator = new PerOffsetPoisonOrchestrator();
    final SagaRunner<StartState> runner;
    final SagaDeadLetterReplayer replayer;

    Harness() {
      this(WINDOW);
    }

    /** {@code window} is the inbox retention window of BOTH the runner and the replayer. */
    Harness(Duration window) {
      this(window, false, StreamRuneMetrics.NOOP);
    }

    /**
     * The runner's event-path key-age guard armed exactly as the Spring, Quarkus and Micronaut
     * auto-configurations arm it: the application's builder names no window, and {@link
     * SagaEventPathRetention#applyDefault} supplies {@code streamrune.inbox.retention-max-age}
     * afterwards. {@code metrics} is the runner's and the replayer's.
     */
    static Harness autoConfigured(StreamRuneMetrics metrics) {
      return new Harness(WINDOW, true, metrics);
    }

    private Harness(Duration window, boolean autoConfigured, StreamRuneMetrics metrics) {
      var builder =
          SagaRunner.<StartState>builder()
              .orchestrator(orchestrator)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(dlq)
              .metrics(metrics)
              .clock(clock);
      if (!autoConfigured) {
        builder.inboxRetentionMaxAge(window);
      }
      runner = builder.build();
      if (autoConfigured) {
        SagaEventPathRetention.applyDefault(List.of(runner), window);
      }
      replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(dlq)
              .sagaStore(sagaStore)
              .eventStore(eventStore)
              .sagaRunner(runner)
              .metrics(metrics)
              .inboxRetentionMaxAge(window)
              .clock(clock)
              .build();
    }

    EventEnvelope start() {
      return SagaStartFixtures.append(eventStore, new StartEvent("order-1"));
    }

    EventEnvelope advance() {
      return SagaStartFixtures.append(eventStore, new Advance("order-1"));
    }

    void deliver(EventEnvelope... events) {
      runner.asEventListener().onEvents(List.of(events));
    }

    LoadedSaga<StartState> row() {
      return sagaStore.load(SAGA, TYPE, StartState.class).orElseThrow();
    }

    List<SagaDeadLetterEntry> entries() {
      return dlq.findBySaga(SAGA);
    }

    long offset(EventEnvelope e) {
      return e.globalOffset().value();
    }
  }

  @Test
  void genesisStillPoisonOnce_thenDrain_neverFeedsTheLaterEntryFirst() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start, corr);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().genesisApplied()).isFalse();
    assertThat(h.entries()).hasSize(2);

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.STILL_POISON);
    assertThat(h.orchestrator.consumed).as("nothing fed ahead of a poison genesis").isEmpty();
    assertThat(h.bus.count("step-1")).isZero();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);

    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.bus.count("step-1")).isEqualTo(1);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.deadLetterPending()).as("backlog drained: shield cleared").isFalse();
    assertThat(h.entries()).isEmpty();
  }

  @Test
  void heldCorrelatedEntryBeforeTheGenesis_isDeferredThenFedAfterIt() {
    var h = new Harness();
    var early = h.advance();
    var start = h.start();
    h.sagaStore.createGenesisPending(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    h.deliver(early); // held: GENESIS_PENDING
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start); // genesis poison: FAULTED, entry
    h.orchestrator.evolvePoison.clear();

    List<ReplayOutcome> outcomes = h.replayer.replayAll(SAGA);

    assertThat(outcomes)
        .containsExactly(
            ReplayOutcome.SAGA_ROW_PENDING, ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(early));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  @Test
  void a102_nullSagaEntryResolvedToAPendingTarget_isStampedThenFoldedAtCausalPosition() {
    var h = new Harness();
    var start = h.start();
    var unroutable = h.advance();
    var later = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.orchestrator.evolvePoison.add(h.offset(later));
    h.deliver(unroutable, later); // null-saga entry, then a poison that faults the saga
    h.orchestrator.routerPoison.clear();
    h.orchestrator.evolvePoison.clear();

    ReplayOutcome single = h.replayer.replay(null, unroutable.globalOffset());
    assertThat(single).isEqualTo(ReplayOutcome.TARGET_PENDING);
    assertThat(
            h.dlq.findNullSagaEntry(TYPE, unroutable.globalOffset()).orElseThrow().targetSagaId())
        .isEqualTo(SAGA);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .containsExactly(h.offset(start), h.offset(unroutable), h.offset(later));
    assertThat(h.dlq.findNullSagaEntry(TYPE, unroutable.globalOffset())).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  @Test
  void nullSagaEntry_whoseTargetIsHealthy_isFedAndDiscarded() {
    var h = new Harness();
    var start = h.start();
    var unroutable = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.deliver(unroutable);
    h.orchestrator.routerPoison.clear();

    assertThat(h.replayer.replay(null, unroutable.globalOffset()))
        .isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(unroutable));
    assertThat(h.dlq.findNullSagaEntry(TYPE, unroutable.globalOffset())).isEmpty();
  }

  /**
   * Nothing holds a saga's later events behind a null-saga entry (its saga was unknown when the
   * event was quarantined), so a later correlated event may already be applied when the entry is
   * replayed. The feed still happens — {@code evolve} must tolerate a late event — but it is not
   * silent: a WARNING names the saga, the entry's offset and the saga's last applied offset, so an
   * operator can tell an out-of-order REPLAYED from a causally ordered one.
   */
  @Test
  void nullSagaEntry_fedAfterALaterEventWasApplied_isFedWithAnOutOfOrderWarning() {
    var h = new Harness();
    var start = h.start();
    var unroutable = h.advance();
    var later = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.deliver(unroutable, later);
    h.orchestrator.routerPoison.clear();
    assertThat(h.row().lastAppliedOffset()).isEqualTo(h.offset(later));

    List<String> warnings =
        warningsDuring(
            SagaDeadLetterReplayer.class,
            () ->
                assertThat(h.replayer.replay(null, unroutable.globalOffset()))
                    .isEqualTo(ReplayOutcome.REPLAYED));

    assertThat(h.orchestrator.consumed)
        .containsExactly(h.offset(start), h.offset(later), h.offset(unroutable));
    assertThat(warnings)
        .filteredOn(w -> w.contains("out of order"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains(
                        SAGA.value(),
                        String.valueOf(h.offset(unroutable)),
                        String.valueOf(h.offset(later))));
  }

  /** The in-order feed of the same entry says nothing about ordering. */
  @Test
  void nullSagaEntry_fedBeforeAnyLaterEvent_hasNoOutOfOrderWarning() {
    var h = new Harness();
    var start = h.start();
    var unroutable = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.deliver(unroutable);
    h.orchestrator.routerPoison.clear();

    List<String> warnings =
        warningsDuring(
            SagaDeadLetterReplayer.class,
            () ->
                assertThat(h.replayer.replay(null, unroutable.globalOffset()))
                    .isEqualTo(ReplayOutcome.REPLAYED));

    assertThat(warnings).filteredOn(w -> w.contains("out of order")).isEmpty();
  }

  @Test
  void a82_anchorIsEstablishedBeforeTheFeed_andBlocksAfterTheWindow_unlessForced() {
    var h = new Harness();
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    Instant firstAttempt = h.clock.instant();

    assertThat(h.replayer.replay(SAGA, start.globalOffset())).isEqualTo(ReplayOutcome.STILL_POISON);
    assertThat(h.entries().get(0).firstReplayStartedAt()).isEqualTo(firstAttempt);

    h.clock.advance(WINDOW.plusHours(1));
    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replay(SAGA, start.globalOffset()))
        .isEqualTo(ReplayOutcome.STALE_REDRIVE_BLOCKED);
    assertThat(h.bus.count("step-1")).isZero();
    assertThat(h.entries().get(0).firstReplayStartedAt())
        .as("immutable anchor")
        .isEqualTo(firstAttempt);

    assertThat(h.replayer.replay(SAGA, start.globalOffset(), true))
        .isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("step-1")).isEqualTo(1);
  }

  @Test
  void w3fd4_eventNotFoundStop_leavesTheLiveRowLive_shieldOn_discardIsTheRemedy() {
    var h = new Harness();
    var start = h.start();
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start);
    var ghost = GlobalOffset.of(9_999L); // never appended
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA, TYPE, ghost, EventType.of("Ghost"), "x", "y", h.clock.instant()));
    h.sagaStore.setDeadLetterPending(SAGA, TYPE, true);
    h.orchestrator.evolvePoison.clear();

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.EVENT_NOT_FOUND);
    var row = h.row();
    assertThat(row.status())
        .as("a legitimately live row is never re-faulted")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();

    var live = h.advance();
    h.deliver(live);
    assertThat(h.orchestrator.consumed).doesNotContain(h.offset(live));
    assertThat(h.entries()).hasSize(2); // ghost + the held live event

    assertThat(h.replayer.discard(SAGA, ghost)).isTrue();
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).contains(h.offset(live));
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  @Test
  void w3fr1_healthyRowWithAResolvedNullEntry_isNeverFaultedByAStop() {
    var h = new Harness();
    var start = h.start();
    h.deliver(start);
    var ghost = GlobalOffset.of(9_999L);
    h.dlq.publish(
        new SagaDeadLetterEntry(
            null, TYPE, ghost, EventType.of("Ghost"), "x", "y", h.clock.instant()));
    h.dlq.setResolvedTarget(TYPE, ghost, SAGA);
    h.sagaStore.setDeadLetterPending(SAGA, TYPE, true);

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.EVENT_NOT_FOUND);
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().version()).isEqualTo(2L);
  }

  @Test
  void w3fr4_sameOffsetTwin_theNullTwinIsDiscardedBeforeTheNamedEntryIsFed() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr); // named entry
    h.dlq.publish(
        new SagaDeadLetterEntry(
            null, TYPE, corr.globalOffset(), corr.eventType(), "x", "y", h.clock.instant()));
    h.dlq.setResolvedTarget(TYPE, corr.globalOffset(), SAGA); // its null twin
    h.orchestrator.evolvePoison.clear();

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.dlq.findNullSagaEntry(TYPE, corr.globalOffset())).isEmpty();
    assertThat(h.entries()).isEmpty();
  }

  @Test
  void singleReplay_refusesToFeedAheadOfAnOlderPendingEntry_evenWhenForced() {
    var h = new Harness();
    var start = h.start();
    var a = h.advance();
    var b = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(a));
    h.deliver(a, b);
    h.orchestrator.evolvePoison.clear();

    assertThat(h.replayer.replay(SAGA, b.globalOffset()))
        .isEqualTo(ReplayOutcome.OLDER_ENTRY_PENDING);
    assertThat(h.replayer.replay(SAGA, b.globalOffset(), true))
        .isEqualTo(ReplayOutcome.OLDER_ENTRY_PENDING);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
    assertThat(h.replayer.replay(SAGA, a.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.replayer.replay(SAGA, b.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
  }

  // FAULTED -> FAULTED (still poison, keeps pre_fault_status, bumps version) — pinned
  // here rather than in SagaRecoveryStateMachineTest because it needs this file's poison-refresh
  // setup.
  @Test
  void stillPoison_leavesTheRowFaulted_entryRefreshed_noArtifactToUnfault() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    Instant before = h.entries().get(0).faultedAt();
    long versionBefore = h.row().version();
    h.clock.advance(Duration.ofMinutes(1));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.STILL_POISON);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version())
        .as("the refresh fault bumps the version; nothing was un-faulted")
        .isEqualTo(versionBefore + 1);
    assertThat(h.entries().get(0).faultedAt()).isAfter(before);
  }

  @Test
  void entryOverATerminalSaga_isDiscardedAsMoot() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.sagaStore.update(
        SAGA,
        TYPE,
        new StartState(SagaStatus.COMPLETED, "order-1"),
        SagaStatus.COMPLETED,
        h.row().version());
    h.orchestrator.evolvePoison.clear();

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
    assertThat(h.entries()).isEmpty();
  }

  @Test
  void faultedMidCompensation_replayResumesTheEpisode_andClearsTheFault_orBlocksWhenStale() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    h.sagaStore.markFaulted(SAGA, TYPE, h.row().version());
    h.deliver(corr); // held: SKIPPED_WHILE_FAULTED
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().preFaultStatus()).isNull();

    var stale = new Harness();
    var s2 = stale.start();
    var c2 = stale.advance();
    stale.deliver(s2);
    stale.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), stale.row().version());
    stale.sagaStore.markFaulted(SAGA, TYPE, stale.row().version());
    stale.deliver(c2);
    stale.clock.advance(WINDOW.plusHours(1));
    assertThat(stale.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);
    assertThat(stale.bus.count("undo-1")).isZero();
  }

  /**
   * The batch {@code [first, second]} — the first applies, the second poisons — is redelivered
   * whole (an infrastructure failure later in the batch, or a crash before the checkpoint). The
   * first event's redelivery is at or below the recorded {@code last_applied_offset}: it is
   * dedup-skipped by the runner, never held, so the drain after the fix feeds only the second event
   * and the first evolves exactly once. The replay feed's exact-match rule cannot catch this (a
   * deferred older entry must still feed), so the live guard is the only place the redelivery check
   * can hold ahead of the FAULTED hold.
   */
  @Test
  void
      batchRedeliveryAfterAPoison_theAlreadyAppliedEarlierEvent_isNotHeld_andTheDrainEvolvesItOnce() {
    var h = new Harness();
    var start = h.start();
    var c1 = h.advance();
    var c2 = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(c2));
    h.deliver(c1, c2);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().lastAppliedOffset()).isEqualTo(h.offset(c1));

    h.deliver(c1, c2); // the subscription redelivers the whole batch

    assertThat(h.entries())
        .as("the first event was applied: its redelivery is moot and records nothing")
        .extracting(e -> e.eventOffset().value())
        .containsExactly(h.offset(c2));
    h.orchestrator.evolvePoison.clear(); // the operator ships the fix

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .as(
            "the first event evolved exactly once across the live batch, its redelivery and the drain")
        .containsExactly(h.offset(start), h.offset(c1), h.offset(c2));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * The row-less residual — a start whose {@code initialState} threw — owns entries with no row
   * (E_S, then E_5 held BEFORE_START). When the drain lands the genesis the fresh row must be born
   * SHIELDED: a crash right after the start entry's discard leaves E_5 on a shielded row, where a
   * live event is held behind it and retention never prunes it — not on an unshielded live row
   * where the next live event is applied ahead of it and {@code deleteOlderThan} silently loses it.
   */
  @Test
  void drainOfARowlessResidual_keepsTheHeldEntriesShielded_acrossACrashAfterTheStartDiscard() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.orchestrator.initialStatePoison = true;
    h.deliver(start); // entry E_S, no row
    h.deliver(corr); // held BEFORE_START: entry E_5, still no row
    assertThat(h.sagaStore.load(SAGA, TYPE, StartState.class)).isEmpty();
    assertThat(h.entries()).hasSize(2);
    h.orchestrator.initialStatePoison = false; // the operator ships the fix
    h.dlq.crashOnceAfter("discard"); // the start entry's discard commits, then the process dies

    Throwable crash = catchThrowable(() -> h.replayer.replayAll(SAGA));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);

    var row = h.row();
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending())
        .as("the row is born shielded because the saga already owned entries")
        .isTrue();
    assertThat(h.entries())
        .extracting(e -> e.eventOffset().value())
        .containsExactly(h.offset(corr));

    h.clock.advance(Duration.ofDays(30));
    assertThat(h.dlq.deleteOlderThan(h.clock.instant()))
        .as("retention follows the shield: E_5 survives the sweep")
        .isZero();

    var live = h.advance();
    h.deliver(live); // in the window: held behind E_5, never applied ahead of it
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
    assertThat(h.entries()).hasSize(2);

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .containsExactly(h.offset(start), h.offset(corr), h.offset(live));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * A stale START entry over an APPLIED genesis on a row that a LATER event faulted is moot — the
   * feed is {@code SKIPPED_DEDUP} and can never make progress — so the drain discards it and moves
   * on to the entry that faulted the row, instead of classifying it STILL_POISON (the row IS still
   * FAULTED, but by a different event) and wedging forever with a WARN that promises a retry no
   * drain can honour. Pre-state as left by a crashed discard (on a moot feed) or by an older
   * unshielded row: E_S present, shield off, genesis applied.
   */
  @Test
  void
      staleStartEntry_overAnAppliedGenesis_whenALaterEventFaultedTheRow_isDiscardedAsMoot_notWedged() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start); // genesis applied live
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA,
            TYPE,
            start.globalOffset(),
            EventType.of("StartEvent"),
            "java.lang.RuntimeException",
            "stale start entry",
            h.clock.instant())); // E_S: a stale start entry the row outlived
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr); // a later forward poison faults the row (pre RUNNING), E5
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().genesisApplied()).isTrue();
    assertThat(h.entries()).hasSize(2);

    assertThat(h.replayer.replayAll(SAGA))
        .as("E_S is moot and discarded; the drain reaches E5, which is still poison")
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.STILL_POISON);
    assertThat(h.entries())
        .extracting(e -> e.eventOffset().value())
        .containsExactly(h.offset(corr));
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));

    h.orchestrator.evolvePoison.clear(); // the operator ships the fix
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * A resolved null-saga item whose router now points at a DIFFERENT saga's start lands no genesis
   * for THIS drain — the deferred entries are not retried for nothing (one {@code SAGA_ROW_PENDING}
   * per deferred entry, not two) — and the other saga's shield, set by the item's resolution, is
   * released by this drain instead of lingering until its own next drain.
   */
  @Test
  void
      resolvedNullItem_reRoutedToAnotherSagasStart_landsNoGenesisHere_andReleasesThatSagasShield() {
    var h = new Harness();
    var other = SagaId.of("saga-2");
    var unroutable = h.start();
    var corr = h.advance();
    h.orchestrator.routerPoison.add(h.offset(unroutable));
    h.deliver(unroutable); // null-saga entry: the router threw
    h.orchestrator.routerPoison.clear();
    h.dlq.setResolvedTarget(TYPE, unroutable.globalOffset(), SAGA); // an earlier TARGET_PENDING
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA,
            TYPE,
            corr.globalOffset(),
            EventType.of("Advance"),
            "java.lang.RuntimeException",
            "held before start",
            h.clock.instant())); // a named correlated entry of the row-less saga: deferred
    h.orchestrator.startRoutesTo.put(h.offset(unroutable), other); // the router hotfix moved it

    assertThat(h.replayer.replayAll(SAGA))
        .as("the other saga's start replayed; our correlated entry deferred ONCE, no retry pass")
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.SAGA_ROW_PENDING);
    assertThat(h.sagaStore.load(other, TYPE, StartState.class).orElseThrow().genesisApplied())
        .isTrue();
    assertThat(h.sagaStore.load(other, TYPE, StartState.class).orElseThrow().deadLetterPending())
        .as("the other saga's shield (set by the resolution) is released by this drain")
        .isFalse();
    assertThat(h.sagaStore.load(SAGA, TYPE, StartState.class))
        .as("no genesis landed here")
        .isEmpty();
    assertThat(h.dlq.findNullSagaEntry(TYPE, unroutable.globalOffset())).isEmpty();
    assertThat(h.entries()).hasSize(1);
  }

  @Test
  void crashAfterTheFeedBeforeTheDiscard_rerunDedupsAndDiscards() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.dlq.crashOnceOnDiscard();

    Throwable crash = catchThrowable(() -> h.replayer.replayAll(SAGA));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.entries()).hasSize(1);
    assertThat(h.row().lastReplayedOffset()).isEqualTo(h.offset(corr));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .as("the event evolved the state exactly once")
        .containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  /**
   * The rerun after a crash between the feed and the discard meets its own event as the saga's last
   * applied one. That is a duplicate the runner dedups, not a late event: no out-of-order warning.
   */
  @Test
  void crashAfterTheFeedBeforeTheDiscard_rerunHasNoOutOfOrderWarning() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.dlq.crashOnceOnDiscard();
    Throwable crash = catchThrowable(() -> h.replayer.replayAll(SAGA));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.row().lastAppliedOffset())
        .as("the crashed drain applied the entry's own event")
        .isEqualTo(h.offset(corr));

    List<String> warnings =
        replayerWarningsDuring(
            () -> assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED));

    assertThat(warnings).filteredOn(w -> w.contains("out of order")).isEmpty();
    assertThat(h.entries()).isEmpty();
  }

  @Test
  void routerStillThrowing_nullSagaEntryIsStillPoison_andRefreshed() {
    var h = new Harness();
    var start = h.start();
    var bad = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad);
    Instant before = h.dlq.findNullSagaEntry(TYPE, bad.globalOffset()).orElseThrow().faultedAt();
    h.clock.advance(Duration.ofMinutes(1));

    assertThat(h.replayer.replay(null, bad.globalOffset())).isEqualTo(ReplayOutcome.STILL_POISON);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset()).orElseThrow().faultedAt())
        .isAfter(before);
  }

  @Test
  void namedEntry_isFedToItsRecordedSaga_evenWhenTheRouterNowIgnoresTheEvent() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.orchestrator.ignoreAdvance = true; // correlate() now returns empty for Advance

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .as("the recorded saga is the authority")
        .containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().preFaultStatus()).isNull();
  }

  @Test
  void nullSagaEntry_isStampedAndShieldsItsTarget_beforeARefusal() {
    var h = new Harness();
    var start = h.start();
    var bad = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad); // null-saga entry
    h.orchestrator.routerPoison.clear();
    h.dlq.establishFirstReplayAnchor(
        null, TYPE, bad.globalOffset(), h.clock.instant().minus(WINDOW).minusSeconds(1));

    assertThat(h.replayer.replay(null, bad.globalOffset()))
        .isEqualTo(ReplayOutcome.STALE_REDRIVE_BLOCKED);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset()).orElseThrow().targetSagaId())
        .isEqualTo(SAGA);
    assertThat(h.row().deadLetterPending())
        .as("the refused entry keeps its retention shield")
        .isTrue();
    assertThat(h.dlq.deleteOlderThan(h.clock.instant().plus(WINDOW))).isZero();
  }

  @Test
  void deferredEntryWithoutAGenesis_staysPending_andIsReported() {
    var h = new Harness();
    var corr = h.advance();
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA, TYPE, corr.globalOffset(), corr.eventType(), "x", "y", h.clock.instant()));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.SAGA_ROW_PENDING);
    assertThat(h.entries()).hasSize(1);
    assertThat(h.orchestrator.consumed).isEmpty();
  }

  /**
   * A replay resume of a FAULTED mid-compensation row whose compensation is REFUSED admission makes
   * no durable write, so the row stays FAULTED — the replayer classifies on the ROW after the feed,
   * keeps the entry and the shield, and the next drain retries; the executor's success CAS is what
   * clears the fault.
   */
  @Test
  void
      compensationResume_thatMakesNoDurableProgress_keepsTheEntryAndTheRowFaulted_thenALaterDrainReplays() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    h.sagaStore.markFaulted(SAGA, TYPE, h.row().version());
    h.deliver(corr); // held: SKIPPED_WHILE_FAULTED — one owned entry, shield on
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    h.bus.failOnce("undo-1", () -> new CircuitBreakerOpenException("breaker open"));

    List<String> warnings =
        replayerWarningsDuring(
            () ->
                assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.STILL_POISON));

    assertThat(h.entries()).as("the entry stays for the next drain").hasSize(1);
    var row = h.row();
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.bus.count("undo-1")).as("nothing was executed").isZero();
    assertThat(warnings)
        .as("one WARN names the outcome")
        .filteredOn(w -> w.contains("no durable progress"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(StepOutcome.COMPENSATING_REFUSED.name()));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(h.row().preFaultStatus()).isNull();
    assertThat(h.dlq.findBySaga(SAGA)).isEmpty();
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  // ===== Step 5 pins: lookups, refusals, fallbacks, crash points, metrics =====

  @Test
  void replay_unknownEntry_isEntryNotFound_andAnEmptyBacklogDrainsToNothing() {
    var h = new Harness();
    assertThat(h.replayer.replay(SAGA, GlobalOffset.of(42L)))
        .isEqualTo(ReplayOutcome.ENTRY_NOT_FOUND);
    assertThat(h.replayer.replay(null, GlobalOffset.of(42L)))
        .isEqualTo(ReplayOutcome.ENTRY_NOT_FOUND);
    assertThat(h.replayer.replayAll(SAGA)).isEmpty();
  }

  @Test
  void foreignTypeEntries_areInvisible() {
    var h = new Harness();
    var start = h.start();
    var foreign = h.advance();
    h.deliver(start);
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA,
            SagaType.of("OtherSaga"),
            foreign.globalOffset(),
            foreign.eventType(),
            "x",
            "y",
            h.clock.instant()));

    assertThat(h.replayer.replay(SAGA, foreign.globalOffset()))
        .as("another type's record is not ours")
        .isEqualTo(ReplayOutcome.ENTRY_NOT_FOUND);
    assertThat(h.replayer.replayAll(SAGA))
        .as("the drain never feeds another type's record")
        .isEmpty();
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
    assertThat(h.replayer.discard(SAGA, foreign.globalOffset()))
        .as("the typed discard never touches another type's record")
        .isFalse();
    assertThat(h.dlq.findBySaga(SAGA)).hasSize(1);
  }

  @Test
  void
      storeThatCanNeitherListResolvedEntriesNorClearConditionally_drainsNamedOnly_andNeverClearsTheShield() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.dlq.resolvedLookupUnsupported = true;

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().deadLetterPending())
        .as("the backlog was never OBSERVED empty, so the shield stays")
        .isTrue();
    assertThat(h.replayer.discard(SAGA, corr.globalOffset())).isFalse();
    assertThat(h.row().deadLetterPending()).isTrue();
  }

  @Test
  void firstPassStop_countsTheDeferredEntriesAmongTheQuarantined() {
    var h = new Harness();
    var early = h.advance();
    var start = h.start();
    h.sagaStore.createGenesisPending(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    h.deliver(early); // held: GENESIS_PENDING
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start); // genesis poison: FAULTED, entry

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.SAGA_ROW_PENDING, ReplayOutcome.STILL_POISON);
    assertThat(h.orchestrator.consumed).isEmpty();
    assertThat(h.entries()).hasSize(2);
    assertThat(h.row().deadLetterPending()).isTrue();
  }

  @Test
  void secondPassStop_leavesTheLaterDeferredEntriesQuarantined_untilTheNextDrain() {
    var h = new Harness();
    var early1 = h.advance();
    var early2 = h.advance();
    var start = h.start();
    h.sagaStore.createGenesisPending(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    h.deliver(early1, early2); // both held: GENESIS_PENDING
    h.orchestrator.evolvePoison.add(h.offset(start));
    h.deliver(start); // genesis poison: FAULTED, entry
    h.orchestrator.evolvePoison.clear();
    h.orchestrator.evolvePoison.add(h.offset(early1)); // the first deferred entry is poison now

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(
            ReplayOutcome.SAGA_ROW_PENDING,
            ReplayOutcome.SAGA_ROW_PENDING,
            ReplayOutcome.REPLAYED,
            ReplayOutcome.STILL_POISON);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
    assertThat(h.entries()).as("early1 (still poison) + early2 (never fed)").hasSize(2);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().genesisApplied()).isTrue();
    assertThat(h.row().deadLetterPending()).isTrue();

    h.orchestrator.evolvePoison.clear();
    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed)
        .containsExactly(h.offset(start), h.offset(early1), h.offset(early2));
    assertThat(h.entries()).isEmpty();
    assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(h.row().deadLetterPending()).isFalse();
  }

  @Test
  void discardOfANullSagaEntry_clearsItsResolvedTargetsShield_whenTheBacklogIsEmpty() {
    var h = new Harness();
    var start = h.start();
    var bad = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad); // null-saga entry
    h.orchestrator.routerPoison.clear();
    h.dlq.setResolvedTarget(TYPE, bad.globalOffset(), SAGA);
    h.sagaStore.setDeadLetterPending(SAGA, TYPE, true);

    assertThat(h.replayer.discard(null, GlobalOffset.of(9_999L)))
        .as("nothing to discard, nothing to clear")
        .isFalse();
    assertThat(h.row().deadLetterPending()).isTrue();

    assertThat(h.replayer.discard(null, bad.globalOffset())).isTrue();
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset())).isEmpty();
    assertThat(h.row().deadLetterPending()).as("the resolved target's shield is cleared").isFalse();
  }

  @Test
  void nullSagaCorrelatedEntry_whoseRowlessTargetOwnsARecord_isTargetPending() {
    var h = new Harness();
    var bad = h.advance();
    var later = h.advance();
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad); // null-saga entry; the router poison is row-independent
    h.orchestrator.routerPoison.clear();
    // the row-less residual at a LATER offset: the target is FAULTED without a row
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA, TYPE, later.globalOffset(), later.eventType(), "x", "y", h.clock.instant()));

    assertThat(h.replayer.replay(null, bad.globalOffset())).isEqualTo(ReplayOutcome.TARGET_PENDING);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset()).orElseThrow().targetSagaId())
        .isEqualTo(SAGA);
    assertThat(h.orchestrator.consumed).isEmpty();
    assertThat(h.sagaStore.load(SAGA, TYPE, StartState.class)).isEmpty();
  }

  @Test
  void nullSagaCorrelatedEntry_whoseTargetNeverStarted_isDeferred_notFed() {
    var h = new Harness();
    var bad = h.advance();
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad);
    h.orchestrator.routerPoison.clear();

    assertThat(h.replayer.replay(null, bad.globalOffset()))
        .isEqualTo(ReplayOutcome.SAGA_ROW_PENDING);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset())).isPresent();
    assertThat(h.orchestrator.consumed).isEmpty();
  }

  @Test
  void nullSagaEntry_thatRoutesToNoSagaAnyMore_isDiscardedAsReplayed() {
    var h = new Harness();
    var start = h.start();
    var bad = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad);
    h.orchestrator.routerPoison.clear();
    h.orchestrator.ignoreAdvance = true; // correlate() now says "not mine"

    assertThat(h.replayer.replay(null, bad.globalOffset())).isEqualTo(ReplayOutcome.REPLAYED);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset())).isEmpty();
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
  }

  @Test
  void namedEntry_whoseRouterNowThrows_isStillPoison_theRowRefaulted() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.orchestrator.routerPoison.add(h.offset(corr));
    long before = h.row().version();

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.STILL_POISON);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().version()).as("the still-poison refresh fault").isEqualTo(before + 1);
    assertThat(h.entries()).hasSize(1);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
  }

  /**
   * {@code force} is the operator's explicit, per-saga acknowledgement of at-least-once, and the
   * executor honours it. Before, the replayer let the forced entry past its own {@code
   * STALE_COMPENSATION_BLOCKED}, and the executor's event-path key-age guard (armed on every runner
   * by the integrations) refused the SAME stale episode: the refusal was recorded as poison, the
   * drain reported {@code STILL_POISON} with nothing dispatched, and the documented remedy could
   * never resume the saga. The forced feed now carries the acknowledgement to the executor, which
   * skips its guard loudly and drives the episode to its terminal status.
   */
  @Test
  void
      staleCompensation_forcedReplay_carriesTheAcknowledgementPastTheExecutorsGuard_andCompensates() {
    var meter = new ForcedResumeMeter();
    var h = Harness.autoConfigured(meter);
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    h.sagaStore.markFaulted(SAGA, TYPE, h.row().version());
    h.deliver(corr); // held: SKIPPED_WHILE_FAULTED — one owned entry, shield on
    h.clock.advance(WINDOW.plusHours(1));
    assertThat(SagaEventPathRetention.effectiveWindowOf(h.runner))
        .as("the executor's own key-age guard is armed, the way the integrations arm it")
        .isEqualTo(WINDOW);
    assertThat(h.replayer.replay(SAGA, corr.globalOffset()))
        .isEqualTo(ReplayOutcome.STALE_COMPENSATION_BLOCKED);

    List<String> executorWarnings =
        warningsDuring(
            SagaStepExecutor.class,
            () ->
                assertThat(h.replayer.replayAll(SAGA, true))
                    .containsExactly(ReplayOutcome.REPLAYED));

    assertThat(h.bus.count("undo-1")).as("the compensation was dispatched").isEqualTo(1);
    var row = h.row();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(row.preFaultStatus()).isNull();
    assertThat(h.entries()).as("the entry is discarded").isEmpty();
    assertThat(row.deadLetterPending()).as("the shield is cleared").isFalse();
    assertThat(executorWarnings)
        .as("the skipped guard is loud and names the saga")
        .filteredOn(w -> w.contains("forced"))
        .singleElement()
        .satisfies(w -> assertThat(w).contains(SAGA.value()).contains("at-least-once"));
    assertThat(meter.forcedStaleResumes)
        .as("and metered: one forced stale resume of this saga type")
        .containsExactly(SagaType.fromClass(StartState.class).value());
  }

  /**
   * The negative half: only an operator's {@code force} carries the acknowledgement. A LIVE event
   * over the stale episode still meets the executor's guard (the live path cannot set it); a
   * non-forced drain is still {@code STALE_COMPENSATION_BLOCKED}; and a non-forced feed that does
   * reach the executor — from a replayer built without a window, so its own refusal is inert — is
   * still refused there and re-records the fault. Nothing is dispatched until the operator forces.
   */
  @Test
  void staleCompensation_withoutForce_theLivePath_theReplayer_andTheExecutor_allStillRefuse() {
    var meter = new ForcedResumeMeter();
    var h = Harness.autoConfigured(meter);
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    h.clock.advance(WINDOW.plusHours(1));

    h.deliver(corr); // LIVE, over a COMPENSATING row: routed to the executor's resume and its guard
    assertThat(h.bus.count("undo-1")).as("the live path dispatched nothing").isZero();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.entries())
        .singleElement()
        .satisfies(
            e ->
                assertThat(e.errorType())
                    .isEqualTo(SagaStaleCompensationEpisodeException.class.getName()));

    assertThat(h.replayer.replayAll(SAGA))
        .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);
    assertThat(h.replayer.replayAll(SAGA, false))
        .containsExactly(ReplayOutcome.STALE_COMPENSATION_BLOCKED);
    assertThat(h.bus.count("undo-1")).isZero();

    var windowless =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(h.dlq)
            .sagaStore(h.sagaStore)
            .eventStore(h.eventStore)
            .sagaRunner(h.runner)
            .clock(h.clock)
            .build();
    assertThat(windowless.replayAll(SAGA)).containsExactly(ReplayOutcome.STILL_POISON);
    assertThat(h.bus.count("undo-1")).as("the executor refused the non-forced feed").isZero();
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(h.row().preFaultStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(h.entries()).hasSize(1);
    assertThat(h.row().deadLetterPending()).isTrue();
    assertThat(meter.forcedStaleResumes).as("no override was recorded").isEmpty();

    assertThat(h.replayer.replayAll(SAGA, true))
        .as("the acknowledgement is the one key: the same saga resumes once forced")
        .containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(meter.forcedStaleResumes).hasSize(1);
  }

  @Test
  void disabledRetentionWindow_makesBothKeyAgeGuardsInert() {
    var h = new Harness(Duration.ZERO); // zero = inbox pruning disabled = no guard
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.sagaStore.claimCompensating(
        SAGA, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), h.row().version());
    h.sagaStore.markFaulted(SAGA, TYPE, h.row().version());
    h.deliver(corr);
    h.dlq.establishFirstReplayAnchor(
        SAGA, TYPE, corr.globalOffset(), h.clock.instant().minus(Duration.ofDays(30)));
    h.clock.advance(WINDOW.plusHours(1));

    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.bus.count("undo-1")).isEqualTo(1);
    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void crashAfterTheAnchorBeforeTheFeed_rerunFeeds_andTheAnchorNeverMoves() {
    var h = new Harness();
    var start = h.start();
    var corr = h.advance();
    h.deliver(start);
    h.orchestrator.evolvePoison.add(h.offset(corr));
    h.deliver(corr);
    h.orchestrator.evolvePoison.clear();
    h.dlq.crashOnceAfter("establishFirstReplayAnchor");
    Instant anchor = h.clock.instant();

    assertThat(catchThrowable(() -> h.replayer.replayAll(SAGA)))
        .isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.orchestrator.consumed).as("nothing was fed").containsExactly(h.offset(start));
    assertThat(h.entries().get(0).firstReplayStartedAt()).isEqualTo(anchor);
    assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);

    h.clock.advance(Duration.ofHours(1));
    assertThat(h.replayer.replayAll(SAGA)).containsExactly(ReplayOutcome.REPLAYED);
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start), h.offset(corr));
    assertThat(h.entries()).isEmpty();
  }

  // The discard-before-shield-clear crash-point test moved verbatim to SagaDrainCrashPointTest:
  // both drain crash points now live together in the dedicated file.

  @Test
  void crashAfterTheResolutionStamp_beforeTheShield_rerunReStampsAndShields() {
    var h = new Harness();
    var start = h.start();
    var bad = h.advance();
    h.deliver(start);
    h.orchestrator.routerPoison.add(h.offset(bad));
    h.deliver(bad); // null-saga entry
    h.orchestrator.routerPoison.clear();
    h.sagaStore.markFaulted(SAGA, TYPE, h.row().version()); // a pending target
    h.dlq.crashOnceAfter("setResolvedTarget");

    assertThat(catchThrowable(() -> h.replayer.replay(null, bad.globalOffset())))
        .isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(h.dlq.findNullSagaEntry(TYPE, bad.globalOffset()).orElseThrow().targetSagaId())
        .isEqualTo(SAGA);
    assertThat(h.row().deadLetterPending()).as("crashed before the shield").isFalse();

    assertThat(h.replayer.replay(null, bad.globalOffset())).isEqualTo(ReplayOutcome.TARGET_PENDING);
    assertThat(h.row().deadLetterPending()).isTrue();
    assertThat(h.orchestrator.consumed).containsExactly(h.offset(start));
  }

  @Test
  void metricsFailures_neverDisruptTheDrain() {
    var h = new Harness();
    var meterDown =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(h.dlq)
            .sagaStore(h.sagaStore)
            .eventStore(h.eventStore)
            .sagaRunner(h.runner)
            .inboxRetentionMaxAge(WINDOW)
            .clock(h.clock)
            .metrics(
                new StreamRuneMetrics() {
                  @Override
                  public void recordSagaReplayed(String sagaType, String outcome) {
                    throw new IllegalStateException("meter down");
                  }

                  @Override
                  public void recordSagaReplayDeferred(String sagaType) {
                    throw new IllegalStateException("meter down");
                  }
                })
            .build();
    var corr = h.advance();
    h.dlq.publish(
        new SagaDeadLetterEntry(
            SAGA, TYPE, corr.globalOffset(), corr.eventType(), "x", "y", h.clock.instant()));

    assertThat(meterDown.replayAll(SAGA)).containsExactly(ReplayOutcome.SAGA_ROW_PENDING);
    assertThat(meterDown.replay(SAGA, GlobalOffset.of(42L)))
        .isEqualTo(ReplayOutcome.ENTRY_NOT_FOUND);
  }

  @Test
  void builder_defaultsAndRequiredParts() {
    var h = new Harness();
    assertThat(
            SagaDeadLetterReplayer.builder()
                .sagaDeadLetterStore(h.dlq)
                .sagaStore(h.sagaStore)
                .eventStore(h.eventStore)
                .sagaRunner(h.runner)
                .metrics(null)
                .clock(null)
                .build())
        .isNotNull();
    assertThat(catchThrowable(() -> SagaDeadLetterReplayer.builder().build()))
        .isInstanceOf(NullPointerException.class);
    assertThat(
            catchThrowable(
                () -> SagaDeadLetterReplayer.builder().sagaDeadLetterStore(h.dlq).build()))
        .isInstanceOf(NullPointerException.class);
    assertThat(
            catchThrowable(
                () ->
                    SagaDeadLetterReplayer.builder()
                        .sagaDeadLetterStore(h.dlq)
                        .sagaStore(h.sagaStore)
                        .build()))
        .isInstanceOf(NullPointerException.class);
    assertThat(
            catchThrowable(
                () ->
                    SagaDeadLetterReplayer.builder()
                        .sagaDeadLetterStore(h.dlq)
                        .sagaStore(h.sagaStore)
                        .eventStore(h.eventStore)
                        .build()))
        .isInstanceOf(NullPointerException.class);
    assertThat(catchThrowable(() -> h.replayer.replay(SAGA, null)))
        .isInstanceOf(NullPointerException.class);
    assertThat(catchThrowable(() -> h.replayer.replayAll(null)))
        .isInstanceOf(NullPointerException.class);
    assertThat(catchThrowable(() -> h.replayer.discard(SAGA, null)))
        .isInstanceOf(NullPointerException.class);
  }

  /** Captures the replayer's WARNING records raised while {@code body} runs, formatted. */
  private static List<String> replayerWarningsDuring(Runnable body) {
    return warningsDuring(SagaDeadLetterReplayer.class, body);
  }

  /** Captures {@code source}'s WARN-and-above events raised while {@code body} runs, formatted. */
  static List<String> warningsDuring(Class<?> source, Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(source);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }
}

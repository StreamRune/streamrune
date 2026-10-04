package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;
import static org.streamrune.runtime.SagaStartFixtures.correlatedEvent;
import static org.streamrune.runtime.SagaStartFixtures.startEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaStartFixtures.CorrelatingStartOrchestrator;
import org.streamrune.runtime.SagaStartFixtures.ScriptedBus;
import org.streamrune.runtime.SagaStartFixtures.StartState;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/** Live routing and the single poison handler. */
class SagaRunnerHoldTest {

  private static final AggregateType STREAM_TYPE = AggregateType.of("other");
  private final SagaId sagaId = SagaId.of("saga-1");
  private final InMemorySagaStore inner = new InMemorySagaStore();
  private final ForwardingSagaStore store = new ForwardingSagaStore(inner);
  private final InMemorySagaDeadLetterStore dlq = new InMemorySagaDeadLetterStore(inner);
  private final ScriptedBus bus = new ScriptedBus();
  private final CorrelatingStartOrchestrator orchestrator = new CorrelatingStartOrchestrator();
  private final Map<String, AtomicInteger> held = new ConcurrentHashMap<>();
  private final AtomicInteger skippedWhileFaulted = new AtomicInteger();
  private final AtomicInteger faulted = new AtomicInteger();
  private final AtomicInteger quarantined = new AtomicInteger();
  private final AtomicInteger casConflicts = new AtomicInteger();

  private SagaRunner<StartState> runner() {
    return runner(store, orchestrator);
  }

  private SagaRunner<StartState> runner(
      ForwardingSagaStore sagaStore, SagaOrchestrator<StartState> sagaOrchestrator) {
    return SagaRunner.<StartState>builder()
        .orchestrator(sagaOrchestrator)
        .sagaStore(sagaStore)
        .commandBus(bus)
        .sagaDeadLetterStore(dlq)
        .metrics(
            new StreamRuneMetrics() {
              @Override
              public void recordSagaEventHeld(String sagaType, String reason) {
                held.computeIfAbsent(reason, r -> new AtomicInteger()).incrementAndGet();
              }

              @Override
              public void recordSagaSkippedWhileFaulted(String sagaType) {
                skippedWhileFaulted.incrementAndGet();
              }

              @Override
              public void recordSagaFaulted(String sagaType) {
                faulted.incrementAndGet();
              }

              @Override
              public void recordSagaQuarantined(String sagaType) {
                quarantined.incrementAndGet();
              }

              @Override
              public void recordSagaCasConflict(String sagaType) {
                casConflicts.incrementAndGet();
              }
            })
        .build();
  }

  private void deliver(SagaRunner<StartState> runner, EventEnvelope event) {
    runner.asEventListener().onEvents(List.of(event));
  }

  private List<SagaDeadLetterEntry> entries() {
    return dlq.findBySaga(sagaId);
  }

  @Test
  void correlatedEventOverAGenesisPendingRow_isHeld_recorded_shielded_rowUntouched() {
    inner.createGenesisPending(
        sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    var runner = runner();

    deliver(runner, correlatedEvent(5L));

    assertThat(orchestrator.consumedCorrelatedOffsets).isEmpty();
    assertThat(entries())
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.errorType()).isEqualTo(SagaEventHeldException.class.getName());
              assertThat(e.errorMessage()).contains("GENESIS_PENDING");
            });
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(held.get("GENESIS_PENDING").get()).isEqualTo(1);
  }

  @Test
  void correlatedEventOverAShieldedRow_isHeldBehindTheBacklog() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    inner.setDeadLetterPending(sagaId, TYPE, true);
    var runner = runner();

    deliver(runner, correlatedEvent(9L));

    assertThat(orchestrator.consumedCorrelatedOffsets).isEmpty();
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorMessage()).contains("BACKLOG_PENDING"));
    assertThat(held.get("BACKLOG_PENDING").get()).isEqualTo(1);
  }

  @Test
  void correlatedEventOnAnAbsentRow_isHeldOnlyWhenTheSagaOwnsARecord() {
    var runner = runner();
    deliver(runner, correlatedEvent(5L));
    assertThat(entries()).as("never started: silent drop").isEmpty();
    assertThat(held).isEmpty();

    dlq.publish(
        new SagaDeadLetterEntry(
            sagaId,
            TYPE,
            correlatedEvent(1L).globalOffset(),
            correlatedEvent(1L).eventType(),
            "x",
            "y",
            Instant.now()));
    deliver(runner, correlatedEvent(6L));
    assertThat(entries()).hasSize(2);
    assertThat(
            entries().stream()
                .anyMatch(
                    e -> e.errorMessage() != null && e.errorMessage().contains("BEFORE_START")))
        .isTrue();
    assertThat(held.get("BEFORE_START").get()).isEqualTo(1);
  }

  @Test
  void correlatedEventOverACompensatingGenesisPendingRow_resumesInsteadOfHolding() {
    inner.createGenesisPending(
        sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    inner.claimCompensating(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), 1L);
    var runner = runner();

    deliver(runner, correlatedEvent(5L));

    assertThat(entries()).isEmpty();
    assertThat(bus.count("undo-1")).as("the executor resumed the claimed episode").isEqualTo(1);
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void correlatedEventOverAFaultedRow_isRecordedAsSkippedWhileFaulted_andShielded() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    inner.markFaulted(sagaId, TYPE, 1L);
    var runner = runner();

    deliver(runner, correlatedEvent(5L));

    assertThat(entries())
        .singleElement()
        .satisfies(
            e ->
                assertThat(e.errorType())
                    .isEqualTo(SagaSkippedWhileFaultedException.class.getName()));
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().deadLetterPending())
        .isTrue();
    assertThat(skippedWhileFaulted.get()).isEqualTo(1);
  }

  @Test
  void poisonOnAnExistingRow_shieldThenEntryThenFault_recordsThePreFaultStatus() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var runner = runner();

    deliver(runner, correlatedEvent(5L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(entries()).hasSize(1);
    assertThat(faulted.get()).isEqualTo(1);

    deliver(runner, correlatedEvent(6L)); // still faulted: held, never a second faulted sample
    assertThat(faulted.get()).isEqualTo(1);
  }

  /**
   * The redelivery check precedes the holds. A redelivered correlated event at or below the
   * recorded {@code last_applied_offset} was delivered before — applied, held (its entry already
   * exists) or dropped — so over a FAULTED row it is dedup-skipped, never recorded as a NEW entry
   * that the drain would then feed a second time (the double-evolve this guard exists to close). A
   * genuinely new event over the same row is still held.
   */
  @Test
  void alreadyAppliedRedelivery_overAFaultedRow_isDedupSkipped_notHeld() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    var runner = runner();
    deliver(runner, correlatedEvent(5L)); // event 5 applies
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
    orchestrator.poisonCorrelated = true;
    deliver(runner, correlatedEvent(6L)); // event 6 poisons: shield, entry, FAULTED
    var faultedRow = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(faultedRow.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faultedRow.lastAppliedOffset()).isEqualTo(5L);

    deliver(runner, correlatedEvent(5L)); // the batch is redelivered: event 5 arrives again

    assertThat(entries())
        .as("no second entry: event 5 was applied, so its redelivery is moot")
        .extracting(e -> e.eventOffset().value())
        .containsExactly(6L);
    assertThat(skippedWhileFaulted.get()).isZero();
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().version())
        .isEqualTo(faultedRow.version());

    deliver(runner, correlatedEvent(7L)); // a genuinely new event over the same FAULTED row: held
    assertThat(entries())
        .extracting(e -> e.eventOffset().value())
        .containsExactlyInAnyOrder(6L, 7L);
    assertThat(skippedWhileFaulted.get()).isEqualTo(1);
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
  }

  /**
   * The shielded-ACTIVE twin of the redelivery case: the BACKLOG_PENDING hold never records an
   * applied redelivery.
   */
  @Test
  void alreadyAppliedRedelivery_overAShieldedActiveRow_isDedupSkipped_notHeld() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    var runner = runner();
    deliver(runner, correlatedEvent(5L)); // applied
    inner.setDeadLetterPending(
        sagaId, TYPE, true); // a shield (the crashed-drain shape, or an older hold)

    deliver(runner, correlatedEvent(5L)); // redelivered

    assertThat(entries()).as("the applied redelivery is dropped, not held").isEmpty();
    assertThat(held).isEmpty();
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);

    deliver(runner, correlatedEvent(7L)); // a genuinely new event: held behind the backlog
    assertThat(entries())
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.eventOffset().value()).isEqualTo(7L);
              assertThat(e.errorMessage()).contains("BACKLOG_PENDING");
            });
    assertThat(held.get("BACKLOG_PENDING").get()).isEqualTo(1);
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
  }

  /**
   * Live channel: after the first half of a fresh-start poison (entry only, row absent) and a
   * correlated event held BEFORE_START, the start's redelivery after a hot fix creates the row —
   * which must be born SHIELDED because the saga already owns entries, so the next live event is
   * held behind them instead of applied ahead of them.
   */
  @Test
  void liveStartOverARowlessResidual_createsTheRowShielded_andHoldsTheNextLiveEvent() {
    orchestrator.poisonStart = true;
    var crashBeforeCreate =
        new ForwardingSagaStore(inner) {
          private boolean crashed;

          @Override
          public void createFaulted(SagaId id, SagaType t, SagaState initial) {
            if (!crashed) {
              crashed = true;
              throw new ForwardingSagaStore.SimulatedCrash("publish, before createFaulted");
            }
            super.createFaulted(id, t, initial);
          }
        };
    var runner = runner(crashBeforeCreate, orchestrator);
    assertThat(catchThrowable(() -> deliver(runner, startEvent(3L))))
        .isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    deliver(runner, correlatedEvent(5L)); // held BEFORE_START: the saga owns E_S and E_5, no row
    assertThat(held.get("BEFORE_START").get()).isEqualTo(1);
    orchestrator.poisonStart = false; // the operator ships the fix

    deliver(runner, startEvent(3L)); // the start's redelivery creates the row

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.deadLetterPending())
        .as("born shielded — the saga already owns E_S and E_5")
        .isTrue();
    assertThat(entries()).hasSize(2);

    deliver(runner, correlatedEvent(7L));
    assertThat(orchestrator.consumedCorrelatedOffsets)
        .as("held behind the backlog, never applied ahead of E_5")
        .isEmpty();
    assertThat(held.get("BACKLOG_PENDING").get()).isEqualTo(1);
    assertThat(entries()).hasSize(3);
  }

  /**
   * Between the hold's shield write (T) and its publish, a concurrent drain's conditional clear (C)
   * may legitimately observe an empty backlog and write FALSE. The publish then lands the entry AND
   * the shield in one write, so the entry is never left unshielded and prunable. C is simulated
   * right before the publish delegates.
   */
  @Test
  void
      hold_publishesTheEntryAndTheShieldTogether_soAClearBetweenTheShieldAndThePublishCannotStrandTheEntry() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    inner.setDeadLetterPending(sagaId, TYPE, true); // an older backlog: the next live event is held
    var clearBetweenShieldAndPublish =
        new ForwardingSagaDeadLetterStore(dlq) {
          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            inner.setDeadLetterPending(
                sagaId, TYPE, false); // C: the drain observed nothing pending
            super.publishShielded(entry); // the entry + the shield, one write
          }
        };
    var runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(store)
            .commandBus(bus)
            .sagaDeadLetterStore(clearBetweenShieldAndPublish)
            .build();

    deliver(runner, correlatedEvent(5L)); // T -> [C] -> entry+shield

    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorMessage()).contains("BACKLOG_PENDING"));
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().deadLetterPending())
        .as("the shield lands with the entry")
        .isTrue();
    assertThat(dlq.deleteOlderThan(Instant.now().plusSeconds(3600)))
        .as("retention follows the shield: the held entry is not prunable")
        .isZero();
  }

  /**
   * A concurrent drain's conditional clear C lands between the hold's shield write T and its
   * publish P, and the process dies RIGHT AFTER the publish committed. The entry must not be left
   * on an unshielded row: the redelivery would then be APPLIED live (nothing blocks it any more —
   * the shield C cleared WAS the hold's condition, and {@code 9 > last_applied}) while the entry
   * survives as the record of an already-applied event, which the next drain evolves a second time
   * — the double-evolve by another route. With the publish and the shield in ONE durable write
   * there is no such window: after the crash the entry and the shield exist together, the
   * redelivery is held (entry refreshed), and the drain feeds it exactly once.
   */
  @Test
  void
      hold_crashRightAfterThePublish_afterAClearBetweenTheShieldAndThePublish_leavesTheEntryShielded() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    inner.setDeadLetterPending(sagaId, TYPE, true); // an older backlog entry: live events are held
    var clearBetweenShieldAndPublish =
        new ForwardingSagaDeadLetterStore(dlq) {
          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            inner.setDeadLetterPending(
                sagaId, TYPE, false); // C: the drain discarded that entry, cleared
            super.publishShielded(entry); // the entry + the shield — then the crash seam fires
          }
        };
    clearBetweenShieldAndPublish.crashOnceAfter("publish");
    var runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(store)
            .commandBus(bus)
            .sagaDeadLetterStore(clearBetweenShieldAndPublish)
            .build();

    Throwable crash = catchThrowable(() -> deliver(runner, correlatedEvent(9L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);

    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.eventOffset().value()).isEqualTo(9L));
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().deadLetterPending())
        .as("across the crash: the entry and the shield land together")
        .isTrue();

    deliver(runner, correlatedEvent(9L)); // the redelivery: held behind its own entry, not applied
    assertThat(orchestrator.consumedCorrelatedOffsets).as("never applied live").isEmpty();
    assertThat(entries()).hasSize(1);

    assertThat(runner.feedReplay(sagaId, correlatedEvent(9L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(orchestrator.consumedCorrelatedOffsets)
        .as("evolved exactly once")
        .containsExactly(9L);
  }

  /**
   * The correlated-path analogue of the start-handle poison on an ACTIVE row (the start-handle
   * poison proper — a START-path {@code handle} poison over the genesis-pending row — is {@code
   * startHandlePoison_crashAfterTheShield_beforeTheEntry_rerunConverges} below).
   */
  @Test
  void correlatedPoison_crashAfterTheShield_beforeTheEntry_rerunConverges() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    store.crashOnceAfter("setDeadLetterPending");
    var runner = runner();

    Throwable crash = catchThrowable(() -> deliver(runner, correlatedEvent(5L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().deadLetterPending())
        .isTrue();
    assertThat(entries()).as("shield without entry: harmless").isEmpty();

    deliver(runner, correlatedEvent(5L)); // redelivery: the shield holds it, nothing is re-run
    var heldRow = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(heldRow.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorMessage()).contains("BACKLOG_PENDING"));

    // the drain is the convergence path: the replay feed meets the poison and records
    // it
    assertThat(runner.feedReplay(sagaId, correlatedEvent(5L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FAULTED);
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(entries()).as("the upsert refreshed the held entry; no second record").hasSize(1);
  }

  @Test
  void concurrentCreateBetweenThePoisonLoadAndCreateFaulted_leavesTheRowFaultedAndShielded() {
    orchestrator.poisonStart = true;
    var racing =
        new ForwardingSagaStore(inner) {
          @Override
          public void createFaulted(SagaId id, SagaType t, SagaState initial) {
            inner.create(id, t, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
            throw new OptimisticLockException("a concurrent deliverer created the row first");
          }
        };
    var runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(racing)
            .commandBus(bus)
            .sagaDeadLetterStore(dlq)
            .build();

    deliver(runner, startEvent(3L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.deadLetterPending()).as("shield on the concurrent-create race").isTrue();
    assertThat(entries()).hasSize(1);
  }

  @Test
  void evolvePoisonOnAFreshStart_publishesTheEntryBeforeTheFaultedRow() {
    orchestrator.poisonStart = true;
    store.crashOnceAfter("createFaulted");
    var runner = runner();

    Throwable crash = catchThrowable(() -> deliver(runner, startEvent(3L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(entries()).as("the entry lands first: no FAULTED row without its entry").hasSize(1);
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.preFaultStatus()).isNull();

    // a live redelivery over the artifact is SKIPPED_HALTED (FAULTED and not a replay re-drive):
    // nothing re-runs, the row stays at v1
    deliver(runner, startEvent(3L));
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().version()).isEqualTo(1L);
    assertThat(entries()).hasSize(1);
  }

  @Test
  void faultWriteLosesTwice_rowStaysLive_entryAndShieldRecorded() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var alwaysConflicting =
        new ForwardingSagaStore(inner) {
          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            throw new OptimisticLockException("concurrent step keeps winning");
          }
        };
    var runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(alwaysConflicting)
            .commandBus(bus)
            .sagaDeadLetterStore(dlq)
            .build();

    deliver(runner, correlatedEvent(5L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(entries()).hasSize(1);
    deliver(runner, correlatedEvent(6L)); // the shield holds the next live event
    assertThat(entries()).hasSize(2);
  }

  // ===== The handler's remaining crash points (a)-(c), the GENESIS_PENDING window closure
  // end-to-end, the terminal short-circuit on the fault
  // ===== reload, and the still-poison refresh of an already-FAULTED row.

  /**
   * Crash point (a): crash after {@code publish}, before {@code markFaulted} (row present). The
   * rerun's live redelivery is held BACKLOG_PENDING behind the shield — and the upsert OVERWRITES
   * the poison's error details with the hold's until the drain re-discovers the poison (pinned
   * deliberately: an accepted end state); the drain then converges on the fault.
   */
  @Test
  void poisonOnAnExistingRow_crashAfterTheEntry_beforeTheFault_rerunConverges() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var crashBeforeFault =
        new ForwardingSagaStore(inner) {
          private boolean crashed;

          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            if (!crashed) {
              crashed = true;
              throw new ForwardingSagaStore.SimulatedCrash("publish, before markFaulted");
            }
            return super.markFaulted(id, t, v);
          }
        };
    var runner = runner(crashBeforeFault, orchestrator);

    Throwable crash = catchThrowable(() -> deliver(runner, correlatedEvent(5L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    var afterCrash = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(afterCrash.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(afterCrash.version()).isEqualTo(1L);
    assertThat(afterCrash.deadLetterPending()).isTrue();
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorType()).isEqualTo(RuntimeException.class.getName()));

    deliver(runner, correlatedEvent(5L)); // rerun: the shield holds the redelivery
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(entries())
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.errorType())
                  .as("the hold's upsert overwrites the poison's details (accepted end state)")
                  .isEqualTo(SagaEventHeldException.class.getName());
              assertThat(e.errorMessage()).contains("BACKLOG_PENDING");
            });

    assertThat(runner.feedReplay(sagaId, correlatedEvent(5L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FAULTED);
    var drained = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(drained.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(drained.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorType()).isEqualTo(RuntimeException.class.getName()));
    assertThat(faulted.get()).isEqualTo(1);
  }

  /**
   * Crash point (b) — first half of a fresh-start poison: crash after {@code publish}, before
   * {@code createFaulted}. Entry only, row absent; a correlated event in the window is held
   * BEFORE_START (the entry is the ownership signal); the start's redelivery re-drives, meets the
   * poison again and creates the FAULTED row.
   */
  @Test
  void evolvePoisonOnAFreshStart_crashAfterTheEntry_beforeTheFaultedRow_rerunConverges() {
    orchestrator.poisonStart = true;
    var crashBeforeCreate =
        new ForwardingSagaStore(inner) {
          private boolean crashed;

          @Override
          public void createFaulted(SagaId id, SagaType t, SagaState initial) {
            if (!crashed) {
              crashed = true;
              throw new ForwardingSagaStore.SimulatedCrash("publish, before createFaulted");
            }
            super.createFaulted(id, t, initial);
          }
        };
    var runner = runner(crashBeforeCreate, orchestrator);

    Throwable crash = catchThrowable(() -> deliver(runner, startEvent(3L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    assertThat(inner.load(sagaId, TYPE, StartState.class)).as("entry only, row absent").isEmpty();
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.eventOffset().value()).isEqualTo(3L));

    deliver(runner, correlatedEvent(5L)); // in the window: the entry makes the saga a known one
    assertThat(held.get("BEFORE_START").get()).isEqualTo(1);
    assertThat(inner.load(sagaId, TYPE, StartState.class)).isEmpty();

    deliver(runner, startEvent(3L)); // the start's redelivery
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(entries()).as("the refreshed start entry + the held correlated one").hasSize(2);
    assertThat(entries().stream().filter(e -> e.eventOffset().value() == 3L).toList())
        .as("the redelivery refreshed the start entry; no duplicate")
        .singleElement()
        .satisfies(e -> assertThat(e.errorType()).isEqualTo(RuntimeException.class.getName()));
    assertThat(faulted.get()).isEqualTo(1);
  }

  /**
   * Start-handle poison: a START-path {@code handle} poison over the genesis-pending row, crashing
   * after the shield and before the entry. The row is genesis-pending and shielded with no entry: a
   * correlated event delivered now is held GENESIS_PENDING (the genesis rule precedes the backlog
   * rule); the start's redelivery re-drives the genesis through the executor, meets the poison
   * again and faults the row with {@code pre_fault_status = NULL} — a genesis-pending forward fault
   * has nothing to resume (a replay re-evolves from {@code initialState}); both stores record NULL
   * for it.
   */
  @Test
  void startHandlePoison_crashAfterTheShield_beforeTheEntry_rerunConverges() {
    orchestrator.poisonStartHandle = true;
    store.crashOnceAfter("setDeadLetterPending");
    var runner = runner();

    Throwable crash = catchThrowable(() -> deliver(runner, startEvent(3L)));
    assertThat(crash).isInstanceOf(ForwardingSagaStore.SimulatedCrash.class);
    var afterCrash = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(afterCrash.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(afterCrash.version()).isEqualTo(1L);
    assertThat(afterCrash.genesisApplied()).isFalse();
    assertThat(afterCrash.deadLetterPending()).isTrue();
    assertThat(entries()).as("shield without entry").isEmpty();
    assertThat(bus.count("step-1")).as("handle threw before any dispatch").isZero();

    deliver(runner, correlatedEvent(5L)); // in the window
    assertThat(held.get("GENESIS_PENDING").get())
        .as("the genesis rule precedes the backlog rule")
        .isEqualTo(1);
    assertThat(held.containsKey("BACKLOG_PENDING")).isFalse();
    assertThat(orchestrator.consumedCorrelatedOffsets).isEmpty();

    deliver(runner, startEvent(3L)); // the start's redelivery re-drives the genesis
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus())
        .as("genesis-pending forward fault: nothing to resume")
        .isNull();
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(row.version()).isEqualTo(2L);
    assertThat(entries()).hasSize(2);
    assertThat(entries().stream().filter(e -> e.eventOffset().value() == 3L).toList())
        .singleElement()
        .satisfies(e -> assertThat(e.errorType()).isEqualTo(RuntimeException.class.getName()));
    assertThat(entries().stream().filter(e -> e.eventOffset().value() == 5L).toList())
        .singleElement()
        .satisfies(e -> assertThat(e.errorMessage()).contains("GENESIS_PENDING"));
    assertThat(faulted.get()).isEqualTo(1);
    assertThat(bus.count("step-1")).isZero();
  }

  /**
   * The genesis-pending window, closed end-to-end: a live correlated event over a genesis-pending
   * row is held (not applied, so the start's redelivery has nothing to clobber), the redelivery
   * commits the genesis, and the drain applies the held event in order.
   */
  @Test
  void genesisPendingWindow_heldEvent_isNotClobberedByTheStartRedelivery_andDrainsInOrder() {
    inner.createGenesisPending(
        sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    var runner = runner();

    deliver(runner, correlatedEvent(5L));
    assertThat(held.get("GENESIS_PENDING").get()).isEqualTo(1);
    assertThat(orchestrator.consumedCorrelatedOffsets).isEmpty();

    deliver(runner, startEvent(3L)); // the start's redelivery re-drives and commits the genesis
    var applied = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(applied.version()).isEqualTo(2L);
    assertThat(applied.genesisApplied()).isTrue();
    assertThat(applied.lastAppliedOffset()).isEqualTo(3L);
    assertThat(applied.deadLetterPending())
        .as("the shield outlives the genesis commit: only the drain clears it")
        .isTrue();
    assertThat(orchestrator.consumedCorrelatedOffsets)
        .as("nothing was applied out of order, so the redelivery clobbered nothing")
        .isEmpty();
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);

    assertThat(runner.feedReplay(sagaId, correlatedEvent(5L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
    var drained = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(drained.lastReplayedOffset()).isEqualTo(5L);
    assertThat(drained.lastAppliedOffset()).isEqualTo(5L);
    assertThat(drained.version()).isEqualTo(3L);
    assertThat(entries()).as("the runner never discards; that is the replayer's write").hasSize(1);
  }

  /**
   * The fault write conflicts because a concurrent path TERMINALIZED the saga (a sweeper, a timeout
   * runner, another replica). The reload sees the terminal row and stops: no second {@code
   * markFaulted} (both stores would refuse it), no spurious cas-conflict sample, no misleading
   * WARN. The entry is kept for the drain's discard.
   */
  @Test
  void faultWriteConflictsOnce_reloadShowsAConcurrentTerminalRow_noSecondWrite_noSecondConflict() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var terminalizedUnderneath =
        new ForwardingSagaStore(inner) {
          private boolean first = true;

          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            if (first) {
              first = false;
              inner.update(
                  id, t, new StartState(SagaStatus.COMPLETED, "order-1"), SagaStatus.COMPLETED, v);
              throw new OptimisticLockException("lost to a concurrent terminalization");
            }
            throw new AssertionError("a terminal row must not be written again");
          }
        };
    var runner = runner(terminalizedUnderneath, orchestrator);

    Throwable thrown = catchThrowable(() -> deliver(runner, correlatedEvent(5L)));

    assertThat(thrown).isNull();
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(row.version()).isEqualTo(2L);
    assertThat(row.deadLetterPending())
        .as("the shield was set before the write; the drain's discard clears it")
        .isTrue();
    assertThat(entries()).hasSize(1);
    assertThat(casConflicts.get())
        .as("exactly the first conflict; no refused second write")
        .isEqualTo(1);
    assertThat(faulted.get()).isZero();
  }

  /**
   * A still-poison {@code feedReplay} onto an ALREADY-FAULTED row refreshes the entry and bumps the
   * version, but {@code markFaulted} returns {@code false} — no transition, so no second {@code
   * saga.faulted} sample.
   */
  @Test
  void stillPoisonFeedReplay_ontoAnAlreadyFaultedRow_refreshesTheEntry_noSecondFaultedSample() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var runner = runner();
    deliver(runner, correlatedEvent(5L)); // the live poison: FAULTED v2, one entry, one sample
    assertThat(faulted.get()).isEqualTo(1);

    assertThat(runner.feedReplay(sagaId, correlatedEvent(5L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FAULTED);

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.version())
        .as("the idempotent markFaulted still bumps the version")
        .isEqualTo(3L);
    assertThat(row.preFaultStatus())
        .as("a still-poison refresh keeps the recorded status")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(faulted.get())
        .as("markFaulted returned false: no transition, no sample")
        .isEqualTo(1);
    assertThat(entries()).hasSize(1);
    assertThat(quarantined.get())
        .as("the entry was refreshed (upsert), and counted as such")
        .isEqualTo(2);
  }

  // ===== The remaining durable-write branches of the single poison handler, the
  // ===== replay feed's two paths, the router contract, and the metric contract.

  /**
   * Row-less residual, second spelling: {@code initialState} RETURNS null (the SPI violation the
   * executor already quarantines) instead of throwing. The old {@code markFaulted} guarded this
   * with {@code if (genesis != null)}; without the guard {@code createFaulted(null)} throws IAE out
   * of the poison handler and wedges the subscription. Entry only, no row, no throw.
   */
  @Test
  void nullInitialStateOnAStartPoison_recordsTheEntryOnly_noRow_noThrow() {
    var nullGenesis =
        new DelegatingOrchestrator(orchestrator) {
          @Override
          public StartState initialState(SagaId id) {
            return null;
          }
        };
    var runner = runner(store, nullGenesis);

    Throwable thrown = catchThrowable(() -> deliver(runner, startEvent(3L)));

    assertThat(thrown).isNull();
    assertThat(inner.load(sagaId, TYPE, StartState.class)).isEmpty();
    assertThat(entries())
        .singleElement()
        .satisfies(e -> assertThat(e.errorType()).isEqualTo(NullPointerException.class.getName()));
    assertThat(faulted.get()).isZero();
    assertThat(held).isEmpty();
  }

  /** First half: the fault write conflicts once and the reload shows a concurrent fault. */
  @Test
  void faultWriteConflictsOnce_reloadShowsAConcurrentFault_noSecondWrite_noOwnFaultedSample() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var concurrentFaultWins =
        new ForwardingSagaStore(inner) {
          private boolean first = true;

          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            if (first) {
              first = false;
              inner.markFaulted(id, t, v); // the concurrent path's fault lands first
              throw new OptimisticLockException("lost to the concurrent fault");
            }
            throw new AssertionError("a row already FAULTED must not be written again");
          }
        };
    var runner = runner(concurrentFaultWins, orchestrator);

    deliver(runner, correlatedEvent(5L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.version()).as("exactly one fault write landed").isEqualTo(2L);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(entries()).hasSize(1);
    assertThat(casConflicts.get()).isEqualTo(1);
    assertThat(faulted.get()).as("the transition was the concurrent path's, not ours").isZero();
  }

  /** Second half: the fault write conflicts once, then wins at the reloaded version. */
  @Test
  void faultWriteConflictsOnce_thenWinsAtTheReloadedVersion() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var advancedOnce =
        new ForwardingSagaStore(inner) {
          private boolean first = true;

          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            if (first) {
              first = false;
              // a concurrent forward step advanced the row (v1 -> v2, still RUNNING)
              inner.update(
                  id, t, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, v);
              throw new OptimisticLockException("stale version");
            }
            return super.markFaulted(id, t, v);
          }
        };
    var runner = runner(advancedOnce, orchestrator);

    deliver(runner, correlatedEvent(5L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.version()).as("v1 -> v2 (concurrent) -> v3 (our fault)").isEqualTo(3L);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(entries()).hasSize(1);
    assertThat(casConflicts.get()).isEqualTo(1);
    assertThat(faulted.get()).isEqualTo(1);
  }

  /**
   * The fault write conflicts and the reload cannot read the row back (deterministic): logged,
   * never re-raised as poison from inside the handler; the entry and the shield stand.
   */
  @Test
  void faultWriteConflicts_andTheReloadIsUnreadable_isLoggedNotThrown() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var conflictThenUnreadable =
        new ForwardingSagaStore(inner) {
          private int loads;

          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            throw new OptimisticLockException("stale version");
          }

          @Override
          public <T extends SagaState> Optional<LoadedSaga<T>> load(
              SagaId id, SagaType t, Class<T> type) {
            if (++loads
                == 3) { // 1: the live delivery, 2: recordPoison, 3: the post-conflict reload
              throw new SagaStateSerializationException(
                  "Failed to load saga state: " + id,
                  id,
                  new UncheckedIOException(new IOException("unmappable stored state")));
            }
            return super.load(id, t, type);
          }
        };
    var runner = runner(conflictThenUnreadable, orchestrator);

    Throwable thrown = catchThrowable(() -> deliver(runner, correlatedEvent(5L)));

    assertThat(thrown).isNull();
    assertThat(entries()).hasSize(1);
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
    assertThat(faulted.get()).isZero();
    assertThat(casConflicts.get()).isEqualTo(1);
  }

  /**
   * The poison handler's OWN load cannot read the row back (deterministic conversion failure): the
   * shield and the entry are recorded, the fault write is skipped loudly, nothing is thrown (thrown
   * from inside the handler it would escape the listener's catch).
   */
  @Test
  void poisonHandlerLoadUnreadable_recordsShieldAndEntry_skipsTheFaultWrite() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var unreadableOnPoison =
        new ForwardingSagaStore(inner) {
          private int loads;

          @Override
          public <T extends SagaState> Optional<LoadedSaga<T>> load(
              SagaId id, SagaType t, Class<T> type) {
            if (++loads == 2) { // 1: the live delivery, 2: recordPoison's own load
              throw new SagaStateSerializationException(
                  "Failed to load saga state: " + id,
                  id,
                  new UncheckedIOException(new IOException("unmappable stored state")));
            }
            return super.load(id, t, type);
          }
        };
    var runner = runner(unreadableOnPoison, orchestrator);

    Throwable thrown = catchThrowable(() -> deliver(runner, correlatedEvent(5L)));

    assertThat(thrown).isNull();
    assertThat(entries()).singleElement();
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.deadLetterPending()).as("the shield is set before the fault write").isTrue();
    assertThat(row.status())
        .as("the fault write was skipped, loudly")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(faulted.get()).isZero();
    assertThat(casConflicts.get()).isZero();
  }

  /** The row vanished between the poison load and the fault write: the FAULTED row is created. */
  @Test
  void faultWriteConflicts_andTheRowVanishedOnReload_createsTheFaultedRow() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    orchestrator.poisonCorrelated = true;
    var deletedUnderneath =
        new ForwardingSagaStore(inner) {
          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            inner.delete(id, t);
            throw new OptimisticLockException("row deleted underneath the fault write");
          }
        };
    var runner = runner(deletedUnderneath, orchestrator);

    deliver(runner, correlatedEvent(5L));

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.preFaultStatus()).isNull();
    assertThat(row.deadLetterPending()).as("createFaulted carries the shield").isTrue();
    assertThat(entries()).hasSize(1);
    assertThat(faulted.get()).isEqualTo(1);
  }

  /**
   * The post-conflict reload inside the poison handler (a concurrent deliverer won the FAULTED-row
   * create) must not turn a deterministic conversion failure into poison — thrown from INSIDE the
   * handler it would escape the listener's catch and wedge the subscription. The shield is already
   * set; the fault write is skipped, loudly.
   */
  @Test
  void concurrentCreateRace_unreadableRowOnTheReload_isLoggedNotThrown() {
    orchestrator.poisonStart = true;
    var racing =
        new ForwardingSagaStore(inner) {
          private int loads;

          @Override
          public void createFaulted(SagaId id, SagaType t, SagaState initial) {
            inner.create(id, t, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
            throw new OptimisticLockException("a concurrent deliverer created the row first");
          }

          @Override
          public <T extends SagaState> Optional<LoadedSaga<T>> load(
              SagaId id, SagaType t, Class<T> type) {
            if (++loads
                == 3) { // 1: the live delivery, 2: recordPoison, 3: the post-conflict reload
              throw new SagaStateSerializationException(
                  "Failed to load saga state: " + id,
                  id,
                  new UncheckedIOException(new IOException("unmappable stored state")));
            }
            return super.load(id, t, type);
          }
        };
    var runner = runner(racing, orchestrator);

    Throwable thrown = catchThrowable(() -> deliver(runner, startEvent(3L)));

    assertThat(thrown).isNull();
    assertThat(entries()).hasSize(1);
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.deadLetterPending()).as("the shield was set before the reload").isTrue();
    assertThat(row.status())
        .as("the fault write was skipped, loudly")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(faulted.get()).isZero();
    assertThat(casConflicts.get()).isEqualTo(1);
  }

  /** The replay feed IS the recovery channel: it never applies the live hold rules. */
  @Test
  void feedReplay_neverAppliesTheLiveHoldRules_appliesOverAShieldedRow() {
    inner.create(sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    inner.setDeadLetterPending(sagaId, TYPE, true);
    var runner = runner();

    assertThat(runner.feedReplay(sagaId, correlatedEvent(7L), false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FORWARD_PROGRESSED);

    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(7L);
    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.lastReplayedOffset()).isEqualTo(7L);
    assertThat(row.lastAppliedOffset()).isEqualTo(7L);
    assertThat(row.deadLetterPending()).as("only the drain clears the shield").isTrue();
    assertThat(entries()).isEmpty();
    assertThat(held).isEmpty();
  }

  /** The start-path contract: a replayed START entry re-drives a FAULTED genesis-pending row. */
  @Test
  void feedReplay_startPath_reDrivesAFaultedGenesisPendingRow_onceTheOrchestratorIsFixed() {
    orchestrator.poisonStart = true;
    var runner = runner();
    deliver(
        runner,
        startEvent(3L)); // fresh-start poison end state: FAULTED row, genesis pending, one entry
    assertThat(inner.load(sagaId, TYPE, StartState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.FAULTED);
    orchestrator.poisonStart = false; // the operator shipped the fix

    assertThat(runner.feedReplay(sagaId, startEvent(3L), true, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.FORWARD_PROGRESSED);

    var row = inner.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.preFaultStatus()).as("the success CAS clears the fault").isNull();
    assertThat(row.version()).isEqualTo(2L);
    assertThat(row.lastReplayedOffset()).isEqualTo(3L);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
    assertThat(entries()).as("the runner never discards; that is the replayer's write").hasSize(1);
  }

  /** The router contract: live routing, and a throwing router is null-saga poison. */
  @Test
  void resolveTarget_routesLikeLiveDelivery_andAThrowingRouterIsNullSagaPoison() {
    var runner = runner();

    assertThat(runner.resolveTarget(startEvent(3L)))
        .contains(new SagaRunner.ReplayTarget(sagaId, true));
    assertThat(runner.resolveTarget(correlatedEvent(5L)))
        .contains(new SagaRunner.ReplayTarget(sagaId, false));
    assertThat(runner.resolveTarget(unrelatedEvent(8L))).isEmpty();

    var brokenRouter =
        new DelegatingOrchestrator(orchestrator) {
          @Override
          public Optional<SagaId> correlate(EventEnvelope event) {
            throw new IllegalStateException("router broken");
          }
        };
    var broken = runner(store, brokenRouter);
    Throwable poison = catchThrowable(() -> broken.resolveTarget(correlatedEvent(5L)));
    assertThat(poison).isInstanceOf(SagaPoisonException.class);
    assertThat(((SagaPoisonException) poison).sagaId()).isNull();

    deliver(broken, correlatedEvent(5L)); // through the listener: a null-saga entry, nothing else
    assertThat(dlq.findAll(10))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.sagaId()).isNull();
              assertThat(e.sagaType()).isEqualTo(TYPE);
              assertThat(e.errorType()).isEqualTo(IllegalStateException.class.getName());
            });
    assertThat(inner.load(sagaId, TYPE, StartState.class)).isEmpty();
    assertThat(faulted.get()).isZero();
  }

  /** Metric contract: {@code saga.event_held} is a strict subset of {@code saga.quarantined}. */
  @Test
  void everyHoldAlsoCountsAsAQuarantine() {
    inner.createGenesisPending(
        sagaId, TYPE, new StartState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING, false);
    var runner = runner();

    deliver(runner, correlatedEvent(5L));
    deliver(runner, correlatedEvent(6L));

    assertThat(held.get("GENESIS_PENDING").get()).isEqualTo(2);
    assertThat(quarantined.get()).isEqualTo(2);
    assertThat(entries()).hasSize(2);
  }

  // ===== helpers

  /** Forwards every SPI call to {@code inner}; subclasses override one method at a time. */
  private static class DelegatingOrchestrator implements SagaOrchestrator<StartState> {
    private final SagaOrchestrator<StartState> inner;

    DelegatingOrchestrator(SagaOrchestrator<StartState> inner) {
      this.inner = inner;
    }

    @Override
    public Class<StartState> stateType() {
      return inner.stateType();
    }

    @Override
    public StartState initialState(SagaId id) {
      return inner.initialState(id);
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
      return inner.evolve(state, event);
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
  }

  private record Unrelated() implements DomainEvent {}

  /** An event neither router claims. */
  private static EventEnvelope unrelatedEvent(long globalOffset) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(STREAM_TYPE, AggregateId.of("other-stream")),
        Version.initial(),
        new EventType("Unrelated"),
        new Unrelated(),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("saga-1"),
            null,
            null,
            Instant.now()));
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
 * Correlated events delivered while a saga is FAULTED must leave a durable record, because the saga
 * is quarantined pending an operator fix — not finished.
 *
 * <p>Pre-fix, {@code SagaRunner.processCorrelatedEvent}'s guard {@code if (existing.isEmpty() ||
 * existing.get().status().isHalted()) return;} dropped them silently: no quarantine entry, no
 * metric, no log — and the batch completed, so the subscription checkpoint advanced past them. When
 * the operator later shipped the fix and replayed the original poison entry, the saga was re-driven
 * from that ONE event only; every event received during the FAULTED window was gone — never
 * quarantined, and already behind the checkpoint, so nothing would ever redeliver them. The saga
 * then waited forever for a payment it had already received, and the timeout runner eventually
 * compensated it, refunding a genuinely captured payment.
 *
 * <p>The framework already recognised this hazard for the REPLAY path ({@code TARGET_PENDING} —
 * formerly {@code TARGET_FAULTED} — exists because "the runner's halted guard silently skipped the
 * feed, so the event was never processed and this entry is its only surviving record"). These tests
 * pin the same discipline for LIVE delivery, and pin the split by intent: a TERMINAL saga is
 * legitimately done and its drop stays silent.
 */
class SagaFaultedWindowSkipTest {

  private static final AggregateType STREAM_TYPE = AggregateType.of("test");

  // ========== Domain ==========

  record OrderPlaced(String orderId) implements DomainEvent {}

  record PaymentCaptured(String orderId) implements DomainEvent {}

  /** Trips {@code evolve} — an ordinary orchestrator poison, the complement of the FAULTED skip. */
  record PoisonEvent(String orderId) implements DomainEvent {}

  record FulfillOrder(String orderId) implements Command {}

  record RefundOrder(String orderId) implements Command {}

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  static class OrderSaga implements SagaOrchestrator<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderPlaced;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((OrderPlaced) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      return corrId.startsWith("saga-") ? Optional.of(SagaId.of(corrId)) : Optional.empty();
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      if (event.event() instanceof PoisonEvent) {
        throw new IllegalStateException("simulated deterministic orchestrator defect");
      }
      return new OrderState(SagaStatus.RUNNING, "order-1");
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return List.of(
          SagaCommand.of(new FulfillOrder(state.orderId()), AggregateId.of(state.orderId())));
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new RefundOrder(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  // ========== Fixtures ==========

  static class CapturingCommandBus implements CommandBus {
    final List<Object> dispatched = new ArrayList<>();

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
      dispatched.add(command);
      return new CommandResult(
          List.of(),
          StreamId.of(STREAM_TYPE, AggregateId.of("test")),
          Version.initial(),
          List.of());
    }
  }

  static final class CountingMetrics implements StreamRuneMetrics {
    int quarantined = 0;
    int skippedWhileFaulted = 0;
    int faulted = 0;
    int eventHeld = 0;
    final List<String> skippedTags = new ArrayList<>();
    final List<String> holdReasons = new ArrayList<>();

    @Override
    public void recordSagaQuarantined(String sagaType) {
      quarantined++;
    }

    @Override
    public void recordSagaSkippedWhileFaulted(String sagaType) {
      skippedWhileFaulted++;
      skippedTags.add(sagaType);
    }

    @Override
    public void recordSagaFaulted(String sagaType) {
      faulted++;
    }

    @Override
    public void recordSagaEventHeld(String sagaType, String reason) {
      eventHeld++;
      holdReasons.add(reason);
    }
  }

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore dlq;
  CapturingCommandBus commandBus;
  CountingMetrics metrics;
  SagaRunner<OrderState> runner;

  static final SagaType TYPE = SagaType.fromClass(OrderState.class);
  static final SagaId SAGA = SagaId.of("saga-order-1");

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    dlq = new InMemorySagaDeadLetterStore(sagaStore);
    commandBus = new CapturingCommandBus();
    metrics = new CountingMetrics();
    runner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderSaga())
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .metrics(metrics)
            .build();
  }

  private EventEnvelope correlated(DomainEvent event, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(STREAM_TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of(SAGA.value()),
            null,
            null,
            Instant.now()));
  }

  // ========== Tests ==========

  @Test
  void correlatedEventsDuringTheFaultedWindow_areQuarantined_inOffsetOrder_withoutTouchingTheRow() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.FAULTED);
    long versionBefore =
        sagaStore
            .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .version();

    var payment = correlated(new PaymentCaptured("order-1"), 140L);
    var stock = correlated(new PaymentCaptured("order-1"), 175L);
    runner.asEventListener().onEvents(List.of(payment, stock));

    assertThat(dlq.findBySaga(SAGA))
        .as("both events skipped during the FAULTED window leave a durable record")
        .hasSize(2)
        .extracting(e -> e.eventOffset().value())
        .containsExactlyInAnyOrder(140L, 175L);
    assertThat(commandBus.dispatched).as("a FAULTED saga still dispatches nothing").isEmpty();

    // The skip path is record-only: it must NOT re-write the saga row (a version bump here would
    // invalidate the CAS token every concurrent re-driver — the replayer's un-fault, the sweeper,
    // the timeout runner — is holding, and re-FAULTing an already-FAULTED row buys nothing).
    var after =
        sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class).orElseThrow();
    assertThat(after.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(after.version()).isEqualTo(versionBefore);

    assertThat(metrics.quarantined).as("the skip is observable as a quarantine").isEqualTo(2);
  }

  /**
   * The skip needs a series of its own. Folding it into {@code streamrune.saga.quarantined} makes
   * an already-known fault look like a stream of NEW orchestrator defects, so the counter an
   * operator alerts on can no longer distinguish "a new bug appeared" from "events are piling up
   * behind the bug I already know about". Both fire — the entry genuinely is a quarantine — so
   * {@code quarantined - skipped_while_faulted - event_held} is the genuine-poison count (apart
   * from event-path {@code SagaStaleCompensationEpisodeException} refusals — see {@code
   * MetricNames#SAGA_SKIPPED_WHILE_FAULTED}).
   */
  @Test
  void skippedEvents_getTheirOwnCounter_notJustTheQuarantineOne() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.FAULTED);

    runner
        .asEventListener()
        .onEvents(
            List.of(
                correlated(new PaymentCaptured("order-1"), 140L),
                correlated(new PaymentCaptured("order-1"), 175L)));

    assertThat(metrics.skippedWhileFaulted)
        .as("a dedicated series for the skip, so its rate is separable from genuine poison")
        .isEqualTo(2);
    assertThat(metrics.skippedTags)
        .as("tagged by saga type, like every other saga counter")
        .containsExactly(
            SagaType.fromClass(OrderState.class).value(),
            SagaType.fromClass(OrderState.class).value());
    assertThat(metrics.quarantined)
        .as("still counted as a quarantine — an entry really was written")
        .isEqualTo(2);
    assertThat(metrics.faulted)
        .as("the saga was ALREADY faulted; the skip must not inflate the new-fault series")
        .isZero();
  }

  @Test
  void genuinePoison_doesNotTouchTheSkipCounter() {
    // The complement of the test above: a real orchestrator poison quarantines and faults, and the
    // skip series stays at zero — otherwise subtracting the two would not isolate genuine poison.
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);

    runner.asEventListener().onEvents(List.of(correlated(new PoisonEvent("order-1"), 200L)));

    assertThat(metrics.quarantined).isEqualTo(1);
    assertThat(metrics.faulted).isEqualTo(1);
    assertThat(metrics.skippedWhileFaulted).isZero();
  }

  @Test
  void skippedEventEntry_recordsTheCauseAndTheEventType_forOperatorTriage() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.FAULTED);

    var payment = correlated(new PaymentCaptured("order-1"), 140L);
    runner.asEventListener().onEvents(List.of(payment));

    var entry = dlq.findBySaga(SAGA).get(0);
    assertThat(entry.errorType()).isEqualTo(SagaSkippedWhileFaultedException.class.getName());
    assertThat(entry.errorMessage()).contains("FAULTED");
    assertThat(entry.eventType()).isEqualTo(payment.eventType());
    assertThat(entry.sagaType()).isEqualTo(TYPE);
  }

  @Test
  void redeliveryOfTheSameSkippedEvent_upsertsOneEntry_notADuplicatePerDelivery() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.FAULTED);
    var payment = correlated(new PaymentCaptured("order-1"), 140L);

    runner.asEventListener().onEvents(List.of(payment));
    runner.asEventListener().onEvents(List.of(payment));
    runner.asEventListener().onEvents(List.of(payment));

    assertThat(dlq.findBySaga(SAGA))
        .as(
            "publish is an idempotent upsert on (sagaId, eventOffset) — a redelivery storm or a"
                + " second replica cannot multiply the record")
        .hasSize(1);
  }

  @Test
  void terminalSaga_keepsTheSilentDrop_noDeadLetterEntry() {
    sagaStore.create(
        SAGA, TYPE, new OrderState(SagaStatus.COMPLETED, "order-1"), SagaStatus.COMPLETED);

    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(dlq.findBySaga(SAGA))
        .as("a COMPLETED saga is legitimately done — keeping the entry would strand it forever")
        .isEmpty();
    assertThat(metrics.quarantined).isZero();
  }

  @Test
  void noSagaRow_keepsTheSilentDrop_noDeadLetterEntry() {
    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(dlq.findAll(10))
        .as("a correlated event for a saga that does not exist is simply not ours")
        .isEmpty();
  }

  /**
   * Interaction: the ROW's recorded {@code pre_fault_status} decides how a replay re-drive routes —
   * {@code SagaDeadLetterReplayer} never un-faults; the executor routes a FAULTED row on {@code
   * LoadedSaga.effectiveStatus()}. A saga that faulted OUT OF a compensation episode must RESUME
   * the compensation on the replayed event instead of re-opening the forward phase behind an
   * already-executed refund. The row is FAULTED by the time the skip is recorded, so the phase is
   * read from the row's recorded {@code pre_fault_status} ({@code markFaulted} is the only fault
   * write and records the status it replaced — COMPENSATING for a claimed episode, whatever the
   * genesis flag says). The entry carries no copy of it.
   */
  @Test
  void
      skipEntryOfACompensationFaultedSaga_recordsCOMPENSATING_soAReplayResumesRatherThanReRunsForward() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    // A forward command failed: the episode was CAS-claimed COMPENSATING (stamping episodeVersion +
    // episodeClaimedAt), then compensate() poisoned and the runner marked the row FAULTED.
    sagaStore.claimCompensating(SAGA, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
    var claimed =
        sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class).orElseThrow();
    assertThat(claimed.episodeVersion()).isNotNull();
    sagaStore.markFaulted(SAGA, TYPE, claimed.version()); // the only fault write

    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(dlq.findBySaga(SAGA)).hasSize(1);
    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .preFaultStatus())
        .as("the row records what the fault replaced")
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void skipEntryOfAForwardFaultedSaga_recordsNoCompensationPhase() {
    // Never compensated: no episode stamp, so the fault happened in the forward phase and a replay
    // correctly un-faults to RUNNING.
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.FAULTED);

    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .preFaultStatus())
        .as("the row records what the fault replaced")
        .isNotEqualTo(SagaStatus.COMPENSATING);
  }

  // ========== The ROW-LESS residual ==========
  //
  // A saga can own a dead-letter entry with no row at all: the single poison handler publishes
  // the entry BEFORE createFaulted and skips the create only when orchestrator.initialState()
  // itself fails — the one row-less case. A correlated event arriving
  // during that window used to hit the empty-row guard and was silently dropped while the
  // checkpoint advanced: the exact money-losing hazard, on the sibling row-less path
  // the FAULTED-row branch never covered. The distinguishing signal is the saga's own dead-letter
  // record: row-less + own-type entry present == held with BEFORE_START (SagaEventHeldException,
  // the saga.event_held series); row-less with no record == genuinely never started (the drop
  // stays silent, exactly as before).

  /** OrderSaga whose {@code initialState()} throws — the row-less genesis fault. */
  static class GenesisPoisonOrderSaga extends OrderSaga {
    @Override
    public OrderState initialState(SagaId sagaId) {
      throw new IllegalStateException("simulated deterministic initialState defect");
    }
  }

  /** Feeds a poison START event so SAGA ends FAULTED with a dead-letter entry but NO row. */
  private SagaRunner<OrderState> rowlessFaultedRunner() {
    SagaRunner<OrderState> genesisPoisonRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new GenesisPoisonOrderSaga())
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .metrics(metrics)
            .build();
    genesisPoisonRunner
        .asEventListener()
        .onEvents(List.of(correlated(new OrderPlaced("order-1"), 100L)));
    assertThat(sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class))
        .as("precondition: initialState-throwing poison leaves NO saga row")
        .isEmpty();
    assertThat(dlq.findBySaga(SAGA))
        .as("precondition: the poison event itself IS quarantined under the saga id")
        .hasSize(1);
    return genesisPoisonRunner;
  }

  @Test
  void correlatedEventDuringTheRowlessFaultedWindow_isQuarantined_notSilentlyDropped() {
    var runner = rowlessFaultedRunner();
    int quarantinedBefore = metrics.quarantined;

    var payment = correlated(new PaymentCaptured("order-1"), 140L);
    runner.asEventListener().onEvents(List.of(payment));

    assertThat(dlq.findBySaga(SAGA))
        .as("the skipped event leaves a durable record next to the genesis poison entry")
        .hasSize(2)
        .extracting(e -> e.eventOffset().value())
        .containsExactlyInAnyOrder(100L, 140L);
    var skipEntry =
        dlq.findBySaga(SAGA).stream()
            .filter(e -> e.eventOffset().value() == 140L)
            .findFirst()
            .orElseThrow();
    assertThat(skipEntry.errorType()).isEqualTo(SagaEventHeldException.class.getName());
    assertThat(skipEntry.errorMessage()).contains("BEFORE_START");
    assertThat(metrics.eventHeld).isEqualTo(1);
    assertThat(metrics.holdReasons).containsExactly("BEFORE_START");
    assertThat(metrics.skippedWhileFaulted).as("a hold, not a FAULTED skip").isZero();
    assertThat(metrics.quarantined - quarantinedBefore)
        .as("still counted as a quarantine — an entry really was written")
        .isEqualTo(1);
    assertThat(sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class))
        .as("the skip path is record-only: it must not invent a saga row")
        .isEmpty();
    assertThat(commandBus.dispatched).as("a FAULTED saga still dispatches nothing").isEmpty();
  }

  @Test
  void neverStartedSaga_keepsTheSilentDrop_noEntryNoMetric() {
    // The negative that bounds the fix: a correlated event whose saga has no row AND no
    // dead-letter record genuinely never started — quarantining those would flood the store.
    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(dlq.findAll(10)).isEmpty();
    assertThat(metrics.skippedWhileFaulted).isZero();
    assertThat(metrics.eventHeld).isZero();
    assertThat(metrics.quarantined).isZero();
  }

  @Test
  void rowlessFaultedWindow_replayFeed_isNotReQuarantined() {
    // The replay feed never applies the live hold rules (feedReplay is the only replay
    // channel): a replayed event is already durably recorded by the entry being replayed, so the
    // row-less path must not mint a second record — the executor reports the absent row instead.
    var runner = rowlessFaultedRunner();
    var payment = correlated(new PaymentCaptured("order-1"), 140L);

    assertThat(runner.feedReplay(SAGA, payment, false, /* operatorForced= */ false))
        .isEqualTo(StepOutcome.SKIPPED_NO_ROW);

    assertThat(dlq.findBySaga(SAGA))
        .as("only the genesis poison entry — the replay feed must not re-quarantine")
        .hasSize(1);
    assertThat(metrics.skippedWhileFaulted).isZero();
    assertThat(metrics.eventHeld).isZero();
  }

  @Test
  void redeliveryOfARowlessSkippedEvent_upsertsOneEntry_notADuplicatePerDelivery() {
    var runner = rowlessFaultedRunner();
    var payment = correlated(new PaymentCaptured("order-1"), 140L);

    runner.asEventListener().onEvents(List.of(payment));
    runner.asEventListener().onEvents(List.of(payment));
    runner.asEventListener().onEvents(List.of(payment));

    assertThat(dlq.findBySaga(SAGA))
        .as("publish upserts on (sagaId, eventOffset): a crash-redelivery converges on one row")
        .hasSize(2);
  }

  @Test
  void foreignTypeEntriesOnly_keepTheSilentDrop() {
    // SagaIds are business ids shared across saga types: another type's fault under the same id
    // must not turn THIS type's never-started drop into a quarantine.
    dlq.publish(
        new org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry(
            SAGA,
            SagaType.of("OtherState"),
            GlobalOffset.of(90L),
            new EventType("SomethingElse"),
            "com.example.SomeError",
            "foreign type's fault",
            Instant.now()));

    runner.asEventListener().onEvents(List.of(correlated(new PaymentCaptured("order-1"), 140L)));

    assertThat(dlq.findBySaga(SAGA))
        .as("only the foreign entry — no quarantine minted for this type's never-started saga")
        .hasSize(1);
    assertThat(metrics.eventHeld).isZero();
    assertThat(metrics.skippedWhileFaulted).isZero();
  }
}

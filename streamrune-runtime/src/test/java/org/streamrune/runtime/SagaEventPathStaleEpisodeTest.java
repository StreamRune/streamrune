package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
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
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaUnstampedCompensationEpisodeException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * The event-path COMPENSATING resume was the ONLY re-drive path with no command-inbox key-age
 * guard.
 *
 * <p>Every path that can re-drive a claimed compensation episode feeds the SAME episode version
 * into the shared {@code episodeCompensationKey}, and the whole effectively-once argument rests on
 * those inbox rows still existing. The inbox never refreshes {@code processed_at} on a dedup hit,
 * so once {@code InboxRetentionSweeper} prunes an episode's keys a re-dispatch MISSES the inbox and
 * re-executes an already-succeeded compensation — a real double refund. Three of the four
 * re-drivers refuse that dispatch on the durable {@code episode_claimed_at} anchor:
 *
 * <ul>
 *   <li>{@code SagaCompensationRetrySweeper} — routes to CAS-FAULT, no dispatch;
 *   <li>{@code SagaTimeoutRunner.faultStaleEpisode} — CAS-FAULT, no dispatch;
 *   <li>{@code SagaDeadLetterReplayer} — {@code STALE_COMPENSATION_BLOCKED}.
 * </ul>
 *
 * <p>The fourth — {@code SagaStepExecutor.forwardStep}'s COMPENSATING branch, reached from every
 * correlated and start delivery — dispatched unconditionally. It is also the path that runs on
 * EVERY replica with no leadership gate, so on a non-leader replica (where the sweeper never runs)
 * a late or redelivered correlated event re-executed the pruned episode's RefundPayment a second
 * time.
 */
class SagaEventPathStaleEpisodeTest {

  private static final AggregateType STREAM_TYPE = AggregateType.of("test");

  // ========== Domain ==========

  record OrderPlaced(String orderId) implements DomainEvent {}

  record ShipmentCancelled(String orderId) implements DomainEvent {}

  record FulfillOrder(String orderId) implements Command {}

  record RefundPayment(String orderId) implements Command {}

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  static class OrderSaga implements SagaOrchestrator<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED, "order-1");
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderPlaced;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SAGA;
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      return corrId.startsWith("saga-") ? Optional.of(SagaId.of(corrId)) : Optional.empty();
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
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
          SagaCommand.of(new RefundPayment(state.orderId()), AggregateId.of(state.orderId())));
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

  static final SagaType TYPE = SagaType.fromClass(OrderState.class);
  static final SagaId SAGA = SagaId.of("saga-order-1");
  static final Duration INBOX_RETENTION = Duration.ofDays(7);

  MutableClock clock;
  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore dlq;
  CapturingCommandBus commandBus;

  @BeforeEach
  void setUp() {
    clock = MutableClock.startingAt(Instant.parse("2026-04-01T00:00:00Z"));
    sagaStore = new InMemorySagaStore(clock);
    dlq = new InMemorySagaDeadLetterStore(sagaStore);
    commandBus = new CapturingCommandBus();
  }

  private SagaRunner<OrderState> runner(Duration inboxRetentionMaxAge) {
    var builder =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderSaga())
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .clock(clock);
    if (inboxRetentionMaxAge != null) {
      builder.inboxRetentionMaxAge(inboxRetentionMaxAge);
    }
    return builder.build();
  }

  /** RUNNING at v1, then CAS-claimed COMPENSATING at v2 with episode_claimed_at = now. */
  private void claimEpisodeNow() {
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    sagaStore.claimCompensating(SAGA, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
  }

  private EventEnvelope event(DomainEvent payload, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(STREAM_TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(payload.getClass().getSimpleName()),
        payload,
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
  void correlatedResumeOfAnEpisodePastTheInboxWindow_dispatchesNothing_andQuarantines() {
    claimEpisodeNow(); // T0: episode claimed, RefundPayment's inbox key written around now

    // An outage longer than the inbox retention window; the InboxRetentionSweeper has pruned the
    // episode's succeeded-compensation keys.
    clock.advance(INBOX_RETENTION.plusDays(1));

    runner(INBOX_RETENTION)
        .asEventListener()
        .onEvents(List.of(event(new ShipmentCancelled("order-1"), 140L)));

    assertThat(commandBus.dispatched)
        .as(
            "the episode's dedup keys cannot be proven to exist, so a resume could re-execute an"
                + " already-succeeded RefundPayment — dispatch NOTHING, exactly as the sweeper, the"
                + " timeout runner and the replayer already do on the same anchor")
        .isEmpty();
    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .status())
        .as("the stale episode is quarantined for operator reconciliation")
        .isEqualTo(SagaStatus.FAULTED);
  }

  @Test
  void staleResumeRefusal_quarantinesTheTriggeringEvent_soItIsNotLost() {
    claimEpisodeNow();
    clock.advance(INBOX_RETENTION.plusDays(1));

    runner(INBOX_RETENTION)
        .asEventListener()
        .onEvents(List.of(event(new ShipmentCancelled("order-1"), 140L)));

    var entries = dlq.findBySaga(SAGA);
    assertThat(entries)
        .as(
            "the refusal has a triggering event, unlike the sweeper/timeout siblings — record it"
                + " rather than dropping it (event path)")
        .hasSize(1);
    assertThat(entries.get(0).eventOffset().value()).isEqualTo(140L);
    assertThat(entries.get(0).errorType())
        .isEqualTo(SagaStaleCompensationEpisodeException.class.getName());
    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .preFaultStatus())
        .as("the row records what the fault replaced; the entry copy mirrors it")
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void resumeInsideTheInboxWindow_stillDispatchesNormally() {
    claimEpisodeNow();
    clock.advance(INBOX_RETENTION.minusDays(1)); // still provably fresh

    runner(INBOX_RETENTION)
        .asEventListener()
        .onEvents(List.of(event(new ShipmentCancelled("order-1"), 140L)));

    assertThat(commandBus.dispatched)
        .as("a fresh episode resumes exactly as before — the guard must not break recovery")
        .hasSize(1)
        .allMatch(RefundPayment.class::isInstance);
    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void noInboxRetentionConfigured_keepsTheUnguardedStatusQuo() {
    claimEpisodeNow();
    clock.advance(Duration.ofDays(400));

    runner(null).asEventListener().onEvents(List.of(event(new ShipmentCancelled("order-1"), 140L)));

    assertThat(commandBus.dispatched)
        .as("with no configured window there is nothing to measure against — behaviour unchanged")
        .hasSize(1);
  }

  @Test
  void startEventRedeliveryToAStaleEpisode_isRefusedTheSameWay() {
    // A redelivered START event also resumes a claimed episode via the executor's
    // ForwardStep COMPENSATING branch, so it must honour the same key-age precondition.
    claimEpisodeNow();
    clock.advance(INBOX_RETENTION.plusDays(1));

    runner(INBOX_RETENTION)
        .asEventListener()
        .onEvents(List.of(event(new OrderPlaced("order-1"), 100L)));

    assertThat(commandBus.dispatched).isEmpty();
    assertThat(
            sagaStore
                .load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(dlq.findBySaga(SAGA)).hasSize(1);
  }

  /**
   * The anchor has no fallback, and "no anchor" is not a case. A row in a compensation episode that
   * carries no {@code episode_claimed_at} violates the {@code SagaStore} contract (every write that
   * enters {@code COMPENSATING} stamps it; no shipped store can produce this row), and the resume
   * is REFUSED before the key-age guard and before any dispatch — as poison, so the adapter
   * quarantines the triggering event and marks the saga FAULTED. Until this decision the executor
   * let such a resume proceed unguarded ("staleness is unknowable"); guessing is no longer an
   * option on either axis.
   */
  @Test
  void anEpisodeWithNoClaimInstantAtAll_isRefusedAsPoison_notResumedUnguarded() {
    var executor =
        new SagaStepExecutor<>(
            new OrderSaga(), sagaStore, commandBus, StreamRuneMetrics.NOOP, INBOX_RETENTION, clock);
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    sagaStore.claimCompensating(SAGA, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
    clock.advance(Duration.ofDays(400));

    var anchorless =
        Optional.of(
            new LoadedSaga<>(
                new OrderState(SagaStatus.COMPENSATING, "order-1"),
                SagaStatus.COMPENSATING,
                2L,
                /* updatedAt= */ null,
                /* episodeVersion= */ 2L,
                /* episodeClaimedAt= */ null,
                /* genesisApplied= */ true,
                /* preFaultStatus= */ null,
                /* deadLetterPending= */ false,
                /* lastAppliedOffset= */ null,
                /* lastReplayedOffset= */ null));

    assertThatThrownBy(
            () ->
                executor.execute(
                    SAGA,
                    anchorless,
                    new SagaTrigger.ForwardStep(
                        event(new ShipmentCancelled("order-1"), 140L), false, false)))
        .isInstanceOf(SagaPoisonException.class)
        .hasCauseInstanceOf(SagaUnstampedCompensationEpisodeException.class)
        .cause()
        .hasMessageContaining(SAGA.value())
        .hasMessageContaining("episode_claimed_at=null");
    assertThat(commandBus.dispatched).as("nothing dispatched").isEmpty();
  }

  /**
   * The same row WITH an {@code updatedAt}: the deleted fallback measured the age from it (stale
   * here, so the old refusal LOOKED right — but for the wrong reason: the stamp is what is
   * missing). Now it is refused for the missing stamp, before the age is even computed, and the
   * cause names the contract violation rather than staleness.
   */
  @Test
  void anEpisodeWithoutTheClaimInstant_isRefusedForTheMissingStamp_notForItsAge() {
    var executor =
        new SagaStepExecutor<>(
            new OrderSaga(), sagaStore, commandBus, StreamRuneMetrics.NOOP, INBOX_RETENTION, clock);
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    sagaStore.claimCompensating(SAGA, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
    Instant claimedAt = clock.instant();
    clock.advance(INBOX_RETENTION.plusDays(1));

    var unstampedRow =
        Optional.of(
            new LoadedSaga<>(
                new OrderState(SagaStatus.COMPENSATING, "order-1"),
                SagaStatus.COMPENSATING,
                2L,
                /* updatedAt= */ claimedAt,
                /* episodeVersion= */ 2L,
                /* episodeClaimedAt= */ null,
                /* genesisApplied= */ true,
                /* preFaultStatus= */ null,
                /* deadLetterPending= */ false,
                /* lastAppliedOffset= */ null,
                /* lastReplayedOffset= */ null));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                executor.execute(
                    SAGA,
                    unstampedRow,
                    new SagaTrigger.ForwardStep(
                        event(new ShipmentCancelled("order-1"), 140L), false, false)))
        .as("the ForwardStep refusal propagates as poison so the adapter quarantines the event")
        .isInstanceOf(SagaPoisonException.class)
        .hasCauseInstanceOf(SagaUnstampedCompensationEpisodeException.class);
    assertThat(commandBus.dispatched).isEmpty();
  }

  /**
   * The other half of the stamp, and the event-less triggers: a row without {@code episodeVersion}
   * is refused the same way (the key scope cannot be guessed from the row version — a fault→replay
   * cycle moves it), and on {@code ResumeCompensation} / {@code ResumeFaultedEpisode} — no event to
   * quarantine — the refusal is the bare {@link SagaUnstampedCompensationEpisodeException}. The
   * key-age guard being DISABLED changes nothing, and neither does {@code force}: the stamp is
   * required for the KEY, not only for the age.
   */
  @Test
  void anEpisodeWithoutItsVersion_isRefusedOnEveryResumeTrigger_evenWithTheGuardDisabled() {
    var guardless =
        new SagaStepExecutor<>(new OrderSaga(), sagaStore, commandBus, StreamRuneMetrics.NOOP);
    sagaStore.create(SAGA, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    sagaStore.claimCompensating(SAGA, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
    Instant claimedAt = clock.instant();
    var compensating =
        Optional.of(
            new LoadedSaga<>(
                new OrderState(SagaStatus.COMPENSATING, "order-1"),
                SagaStatus.COMPENSATING,
                2L,
                /* updatedAt= */ claimedAt,
                /* episodeVersion= */ null,
                /* episodeClaimedAt= */ claimedAt,
                /* genesisApplied= */ true,
                /* preFaultStatus= */ null,
                /* deadLetterPending= */ false,
                /* lastAppliedOffset= */ null,
                /* lastReplayedOffset= */ null));
    var faultedOutOfIt =
        Optional.of(
            new LoadedSaga<>(
                new OrderState(SagaStatus.COMPENSATING, "order-1"),
                SagaStatus.FAULTED,
                3L,
                /* updatedAt= */ claimedAt,
                /* episodeVersion= */ null,
                /* episodeClaimedAt= */ claimedAt,
                /* genesisApplied= */ true,
                /* preFaultStatus= */ SagaStatus.COMPENSATING,
                /* deadLetterPending= */ false,
                /* lastAppliedOffset= */ null,
                /* lastReplayedOffset= */ null));

    assertThatThrownBy(
            () ->
                guardless.execute(
                    SAGA,
                    compensating,
                    new SagaTrigger.ResumeCompensation(new SagaCompensationResumeException())))
        .isInstanceOf(SagaUnstampedCompensationEpisodeException.class)
        .hasMessageContaining("episode_version=null");
    assertThatThrownBy(
            () ->
                guardless.execute(
                    SAGA,
                    faultedOutOfIt,
                    new SagaTrigger.ResumeFaultedEpisode(/* operatorForced= */ true)))
        .as("force acknowledges at-least-once over KNOWN keys; it never licenses a guessed key")
        .isInstanceOf(SagaUnstampedCompensationEpisodeException.class);
    assertThatThrownBy(
            () ->
                guardless.execute(
                    SAGA,
                    compensating,
                    new SagaTrigger.ForwardStep(
                        event(new ShipmentCancelled("order-1"), 140L), false, false)))
        .isInstanceOf(SagaPoisonException.class)
        .hasCauseInstanceOf(SagaUnstampedCompensationEpisodeException.class);
    assertThat(commandBus.dispatched).as("nothing dispatched on any trigger").isEmpty();
    assertThat(sagaStore.load(SAGA, TYPE, OrderState.class).orElseThrow().version())
        .as("nothing written")
        .isEqualTo(2L);
  }

  // ========== An operator's force passes this guard ==========

  /** RUNNING at v1, COMPENSATING at v2 (claimed now), then FAULTED mid-compensation at v3. */
  private void faultMidEpisode(SagaId sagaId) {
    sagaStore.create(
        sagaId, TYPE, new OrderState(SagaStatus.RUNNING, "order-1"), SagaStatus.RUNNING);
    sagaStore.claimCompensating(
        sagaId, TYPE, new OrderState(SagaStatus.COMPENSATING, "order-1"), 1L);
    sagaStore.markFaulted(sagaId, TYPE, 2L);
  }

  private SagaStepExecutor<OrderState> guardedExecutor(StreamRuneMetrics metrics) {
    return new SagaStepExecutor<>(
        new OrderSaga(), sagaStore, commandBus, metrics, INBOX_RETENTION, clock);
  }

  /**
   * The live path cannot set the acknowledgement: a step that is not a replay feed refuses to carry
   * it, and every constructor the live paths use leaves it off. It can only enter through {@code
   * SagaRunner.feedReplay}, from a replayer an operator called with {@code force}.
   */
  @Test
  void aLiveStepCannotCarryTheOperatorsAcknowledgement() {
    var event = event(new ShipmentCancelled("order-1"), 140L);

    assertThatThrownBy(
            () ->
                new SagaTrigger.ForwardStep(
                    event, false, /* replayRedrive= */ false, false, /* operatorForced= */ true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("operatorForced");
    assertThat(new SagaTrigger.ForwardStep(event, false, false).operatorForced()).isFalse();
    assertThat(new SagaTrigger.ForwardStep(event, true, false, true).operatorForced()).isFalse();
    assertThat(new SagaTrigger.ForwardStep(event, false, true).operatorForced()).isFalse();
    assertThat(new SagaTrigger.ForwardStep(event, false, true, true, true).operatorForced())
        .isTrue();
  }

  @Test
  void forcedReplayStep_overAStaleEpisode_resumes_loudly_andIsMetered() {
    List<String> forced = new ArrayList<>();
    var executor =
        guardedExecutor(
            new StreamRuneMetrics() {
              @Override
              public void recordSagaForcedStaleResume(String sagaType) {
                forced.add(sagaType);
              }
            });
    SagaId forgedId = SagaId.of("saga-order-1\nFORGED log line");
    faultMidEpisode(forgedId);
    clock.advance(INBOX_RETENTION.plusDays(1));

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaStepExecutor.class,
            () ->
                assertThat(
                        executor.execute(
                            forgedId,
                            sagaStore.load(
                                forgedId, SagaType.fromClass(OrderState.class), OrderState.class),
                            new SagaTrigger.ForwardStep(
                                event(new ShipmentCancelled("order-1"), 140L),
                                false,
                                true,
                                true,
                                /* operatorForced= */ true)))
                    .isEqualTo(StepOutcome.COMPENSATED));

    assertThat(commandBus.dispatched).singleElement().isInstanceOf(RefundPayment.class);
    assertThat(
            sagaStore
                .load(forgedId, SagaType.fromClass(OrderState.class), OrderState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(forced).containsExactly(SagaType.fromClass(OrderState.class).value());
    assertThat(warnings)
        .filteredOn(w -> w.contains("forced"))
        .singleElement()
        .satisfies(
            w ->
                assertThat(w)
                    .contains("at-least-once")
                    .contains(LogSanitizer.sanitizeForLog(forgedId.value()))
                    .doesNotContain("\nFORGED"));
  }

  /** Non-forced replays keep the guard exactly as it is — only the acknowledgement lifts it. */
  @Test
  void nonForcedReplayStep_overAStaleEpisode_isStillRefused() {
    var executor = guardedExecutor(StreamRuneMetrics.NOOP);
    faultMidEpisode(SAGA);
    clock.advance(INBOX_RETENTION.plusDays(1));

    assertThatThrownBy(
            () ->
                executor.execute(
                    SAGA,
                    sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class),
                    new SagaTrigger.ForwardStep(
                        event(new ShipmentCancelled("order-1"), 140L), false, true)))
        .isInstanceOf(SagaPoisonException.class)
        .hasCauseInstanceOf(SagaStaleCompensationEpisodeException.class);
    assertThat(commandBus.dispatched).isEmpty();
  }

  /** A forced feed over a FRESH episode overrode nothing, so it is neither logged nor metered. */
  @Test
  void forcedReplayStep_overAFreshEpisode_overridesNothing_andIsNotMetered() {
    List<String> forced = new ArrayList<>();
    var executor =
        guardedExecutor(
            new StreamRuneMetrics() {
              @Override
              public void recordSagaForcedStaleResume(String sagaType) {
                forced.add(sagaType);
              }
            });
    faultMidEpisode(SAGA);
    clock.advance(INBOX_RETENTION.minusDays(1));

    List<String> warnings =
        SagaDeadLetterReplayerTest.warningsDuring(
            SagaStepExecutor.class,
            () ->
                assertThat(
                        executor.execute(
                            SAGA,
                            sagaStore.load(
                                SAGA, SagaType.fromClass(OrderState.class), OrderState.class),
                            new SagaTrigger.ForwardStep(
                                event(new ShipmentCancelled("order-1"), 140L),
                                false,
                                true,
                                true,
                                true)))
                    .isEqualTo(StepOutcome.COMPENSATED));

    assertThat(forced).isEmpty();
    assertThat(warnings).noneMatch(w -> w.contains("forced"));
  }

  /** A meter that throws never turns an acknowledged resume back into a refusal. */
  @Test
  void forcedResume_survivesAFailingMeter() {
    var executor =
        guardedExecutor(
            new StreamRuneMetrics() {
              @Override
              public void recordSagaForcedStaleResume(String sagaType) {
                throw new IllegalStateException("meter down");
              }
            });
    faultMidEpisode(SAGA);
    clock.advance(INBOX_RETENTION.plusDays(1));

    assertThat(
            executor.execute(
                SAGA,
                sagaStore.load(SAGA, SagaType.fromClass(OrderState.class), OrderState.class),
                new SagaTrigger.ForwardStep(
                    event(new ShipmentCancelled("order-1"), 140L), false, true, true, true)))
        .isEqualTo(StepOutcome.COMPENSATED);
    assertThat(commandBus.dispatched).hasSize(1);
  }
}

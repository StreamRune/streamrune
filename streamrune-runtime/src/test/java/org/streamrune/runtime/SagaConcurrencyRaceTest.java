package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
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
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * Deterministic concurrency race tests for saga CAS semantics.
 *
 * <p>Each test pins a specific race (load → interleave → write) using {@link
 * InterleavingSagaStore}, which executes a one-shot hook after the first {@link SagaStore#load}
 * returns, before the writer performs its update. The hook simulates "the other thread wins first".
 *
 * <p>These are pinning tests: they must pass against the current code (Tasks 1-2 shipped the
 * behaviour). Each test carries a comment noting what the pre-CAS (blind-upsert) code would have
 * done wrong.
 */
class SagaConcurrencyRaceTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  // ===========================================================================
  // Shared domain fixtures
  // ===========================================================================

  /** State for the fulfillment saga used across tests. */
  record FulfillState(SagaStatus status, String orderId, String step) implements SagaState {}

  /** Command dispatched by the saga. */
  record AdvanceOrder(String orderId) implements Command {}

  /** Event emitted by the AdvanceOrder decider. */
  record OrderAdvanced(String orderId) implements DomainEvent {}

  /** Correlated event triggering a saga step. */
  record OrderShipped(String orderId) implements DomainEvent {}

  /** Start event for the saga. */
  record OrderCreated(String orderId) implements DomainEvent {}

  /** Aggregate state (single-event sourced). */
  record OrderAggrState(int count) implements AggregateState {}

  /** Decider that emits one {@link OrderAdvanced} per {@link AdvanceOrder} command. */
  static class AdvanceDecider implements Decider<AdvanceOrder, OrderAggrState, OrderAdvanced> {
    @Override
    public OrderAggrState initialState() {
      return new OrderAggrState(0);
    }

    @Override
    public List<OrderAdvanced> decide(AdvanceOrder command, OrderAggrState state) {
      return List.of(new OrderAdvanced(command.orderId()));
    }

    @Override
    public OrderAggrState evolve(OrderAggrState state, OrderAdvanced event) {
      return new OrderAggrState(state.count() + 1);
    }
  }

  /**
   * Timeout-capable {@link SagaDecider}: has a 5-minute timeout, compensates with {@link
   * AdvanceOrder} on timeout, no-ops otherwise.
   */
  static class TimeoutOrchestrator implements SagaDecider<FulfillState> {
    @Override
    public Class<FulfillState> stateType() {
      return FulfillState.class;
    }

    @Override
    public FulfillState initialState(SagaId sagaId) {
      return new FulfillState(SagaStatus.RUNNING, sagaId.value(), "initial");
    }

    @Override
    public FulfillState evolve(FulfillState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FulfillState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillState state, Throwable failure, SagaCommand failedCmd) {
      if (failure instanceof SagaTimeoutException) {
        return List.of(
            SagaCommand.of(new AdvanceOrder(state.orderId()), AggregateId.of(state.orderId())));
      }
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  /**
   * Start+correlate orchestrator: {@link OrderCreated} starts a saga; {@link OrderShipped}
   * correlates by orderId prefix in correlationId. {@link AdvanceOrder} is dispatched on start.
   */
  static class FulfillOrchestrator implements SagaOrchestrator<FulfillState> {
    @Override
    public Class<FulfillState> stateType() {
      return FulfillState.class;
    }

    @Override
    public FulfillState initialState(SagaId sagaId) {
      return new FulfillState(SagaStatus.RUNNING, sagaId.value(), "initial");
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderCreated;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      String orderId = ((OrderCreated) event.event()).orderId();
      return SagaId.of("fulfill-" + orderId);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (event.event() instanceof OrderShipped os) {
        return Optional.of(SagaId.of("fulfill-" + os.orderId()));
      }
      return Optional.empty();
    }

    @Override
    public FulfillState evolve(FulfillState state, EventEnvelope event) {
      if (event.event() instanceof OrderCreated oc) {
        return new FulfillState(SagaStatus.RUNNING, oc.orderId(), "created");
      }
      if (event.event() instanceof OrderShipped os) {
        return new FulfillState(SagaStatus.RUNNING, os.orderId(), "shipped");
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FulfillState state, EventEnvelope event) {
      if (event.event() instanceof OrderCreated oc) {
        return List.of(
            SagaCommand.of(new AdvanceOrder(oc.orderId()), AggregateId.of(oc.orderId())));
      }
      if (event.event() instanceof OrderShipped os) {
        return List.of(
            SagaCommand.of(new AdvanceOrder(os.orderId()), AggregateId.of(os.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillState state, Throwable failure, SagaCommand failedCmd) {
      return List.of();
    }
  }

  // ===========================================================================
  // InterleavingSagaStore fixture
  // ===========================================================================

  /**
   * Delegating {@link SagaStore} that executes a one-shot {@code afterLoadHook} after the <em>first
   * </em> {@link #load} returns — then clears the hook so subsequent loads are unaffected.
   *
   * <p>This simulates "the other thread wins between my load and my write" without needing real
   * thread scheduling. Set the hook to perform whatever the concurrent writer would do on the
   * underlying store before the writer under test calls {@code update}.
   */
  static final class InterleavingSagaStore implements SagaStore {

    final InMemorySagaStore underlying;
    private final AtomicReference<Runnable> hook = new AtomicReference<>(null);

    InterleavingSagaStore(InMemorySagaStore underlying) {
      this.underlying = underlying;
    }

    /** Sets a one-shot hook to run after the next {@link #load} call. */
    void afterLoadHook(Runnable action) {
      hook.set(action);
    }

    /** Clears any pending hook without running it. */
    void clearHook() {
      hook.set(null);
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      underlying.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      underlying.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
      underlying.createFaulted(sagaId, sagaType, initialState);
    }

    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      underlying.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied);
    }

    @Override
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return underlying.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      underlying.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public void update(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      underlying.update(sagaId, sagaType, state, status, expectedVersion);
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
      return underlying.findTimedOut(sagaType, updatedBefore, limit);
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return underlying.findByStatus(sagaType, status, limit);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId sagaId, SagaType sagaType, Class<S> stateType) {
      Optional<LoadedSaga<S>> result = underlying.load(sagaId, sagaType, stateType);
      // Fire the hook exactly once after the first load — then clear it.
      Runnable action = hook.getAndSet(null);
      if (action != null) {
        action.run();
      }
      return result;
    }

    @Override
    public void delete(SagaId sagaId, SagaType sagaType) {
      underlying.delete(sagaId, sagaType);
    }

    @Override
    public void claimCompensating(
        SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
      update(sagaId, sagaType, state, SagaStatus.COMPENSATING, expectedVersion);
    }
  }

  // ===========================================================================
  // Helper: build a stable event envelope
  // ===========================================================================

  private EventEnvelope makeEnvelope(DomainEvent event, long globalOffset, String correlationId) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of(correlationId),
            null,
            null,
            Instant.now()));
  }

  // ===========================================================================
  // Test-level wiring
  // ===========================================================================

  // The SagaRunner derives the stored saga_type from the STATE class
  // (SagaType.fromClass(stateType)), and both its type-scoped load and its CAS-update now enforce
  // it, so a pre-seeded row must carry the SAME type the FulfillOrchestrator
  // (stateType=FulfillState)
  // uses — a human label like "FulfillOrchestrator" would make the runner's load/update miss the
  // row.
  static final SagaType FULFILL_TYPE = SagaType.fromClass(FulfillState.class);
  // Same rule for the timeout path: SagaStepExecutor claims/terminalizes with
  // SagaType.fromClass(orchestrator.stateType()) and TimeoutOrchestrator.stateType() ==
  // FulfillState,
  // so the seed row MUST carry SagaType.fromClass(FulfillState.class). A human label like
  // "TimeoutOrchestrator" would make the claim CAS miss on TYPE (label != fromClass) and throw an
  // OptimisticLockException regardless of the version race — disarming the pin (it would go green
  // even if the version-CAS logic broke). With the fromClass type, the claim CAS races on VERSION,
  // which is what timeoutRunner_losesClaim_noCompensationExecuted is meant to prove.
  static final SagaType TIMEOUT_TYPE = SagaType.fromClass(FulfillState.class);

  MutableClock clock;
  InMemorySagaStore underlying;
  InterleavingSagaStore interleaving;
  SagaId sagaId;
  FulfillState advancedState;

  @BeforeEach
  void setUp() {
    clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    underlying = new InMemorySagaStore(clock);
    interleaving = new InterleavingSagaStore(underlying);
    sagaId = SagaId.of("fulfill-race-order-1");
    advancedState = new FulfillState(SagaStatus.RUNNING, "race-order-1", "advanced");
  }

  // ===========================================================================
  // Test 1 — headline: event×timeout lost-update
  // ===========================================================================

  /**
   * Claim-first pin: with claim-first ownership, the timeout runner's <em>first</em> write is the
   * COMPENSATING claim — before any compensation command is dispatched. If the event path wins the
   * race on that claim CAS, the timeout runner must skip immediately, having dispatched NOTHING.
   *
   * <p>Pre-fix (orphaned compensation bug): the timeout runner dispatched compensation FIRST, then
   * attempted the terminal-status CAS. Losing that CAS only skipped the status write — the
   * already-executed compensation command (a real refund/cancel against a live aggregate) was
   * orphaned: never reconciled, never visible in the saga record.
   *
   * <p>With claim-first (this fix): the timeout runner loads v1, the hook races in and advances the
   * saga to v2 (event path wins), then the timeout runner's CLAIM {@code update(...,
   * expectedVersion=1)} throws {@link OptimisticLockException} — before {@code decider.compensate}
   * is even invoked. The runner swallows it (DEBUG log, skip). Zero compensation commands are
   * dispatched, and the newer event-path write at v2 SURVIVES untouched.
   */
  @Test
  void timeoutRunner_losesClaim_noCompensationExecuted() {
    // Seed a RUNNING saga at v1 with an old updatedAt (clock in the past).
    underlying.create(
        sagaId,
        TIMEOUT_TYPE,
        new FulfillState(SagaStatus.RUNNING, "race-order-1", "initial"),
        SagaStatus.RUNNING);

    // Build a timeout runner backed by the interleaving store.
    var commandBus = new SagaTimeoutRunnerTerminalizationTest.RecordingCommandBus();
    var timeoutRunner =
        SagaTimeoutRunner.<FulfillState>builder()
            .decider(new TimeoutOrchestrator())
            .sagaStore(interleaving)
            .commandBus(commandBus)
            .sagaType(TIMEOUT_TYPE)
            .build();

    // Hook: after timeoutRunner loads v1, the event path advances the saga to v2 — this races
    // against the timeout runner's COMPENSATING claim (also targeting expectedVersion=1).
    interleaving.afterLoadHook(
        () ->
            underlying.update(
                sagaId, TIMEOUT_TYPE, advancedState, SagaStatus.RUNNING, 1L)); // event path → v2

    // The timeout runner must not throw and must silently skip — before dispatching anything.
    timeoutRunner.processTimedOutSaga(sagaId);

    // ZERO commands dispatched: losing the claim CAS means compensation was never invoked.
    assertThat(commandBus.executed)
        .as(
            "losing the COMPENSATING claim must prevent ANY compensation dispatch"
                + " — no orphaned side effects")
        .isEmpty();

    // The newer write (event path, v2) must survive — not overwritten by the timeout runner.
    var after =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(after.status())
        .as(
            "newer event-path write must survive: RUNNING (not COMPENSATING/COMPENSATED from"
                + " timeout runner)")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(after.state())
        .as("event-path state (advancedState) must not be overwritten")
        .isEqualTo(advancedState);
    assertThat(after.version())
        .as("version must be 2 (event path write landed, timeout claim rejected)")
        .isEqualTo(2L);
  }

  // ===========================================================================
  // Test 2 — start-event atomic dedup: concurrent creator wins, no duplicate row
  // ===========================================================================

  /**
   * Pre-CAS: the runner used load-then-create without an atomic guard. Two concurrent writers could
   * both pass the load-returns-empty check and create duplicate state — or one could silently
   * overwrite the other. The race was documented as "use a single-threaded subscription" with no
   * enforcement.
   *
   * <p>With CAS: create is atomic (putIfAbsent). If the hook creates the saga between the runner's
   * load and create, the runner's create throws OLE which it swallows (DEBUG log, skip). Exactly
   * one saga row survives, version=1.
   */
  @Test
  void startEvent_concurrentCreator_atomicDedup_singleSagaRow() {
    var inbox = new InMemoryCommandInbox();
    var eventStore = new InMemoryEventStore().withCommandInbox(inbox);

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(inbox)
            .register(
                TYPE,
                AdvanceOrder.class,
                cmd -> AggregateId.of(cmd.orderId()),
                new AdvanceDecider())
            .build();

    var runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(new FulfillOrchestrator())
            .sagaStore(interleaving)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .build();

    var startSagaId = SagaId.of("fulfill-concurrent-order");

    // Hook: a concurrent creator inserts the saga between the runner's load (empty) and create.
    interleaving.afterLoadHook(
        () ->
            underlying.create(
                startSagaId,
                SagaType.fromClass(FulfillState.class),
                new FulfillState(SagaStatus.RUNNING, "concurrent-order", "created-by-other"),
                SagaStatus.RUNNING));

    var event = makeEnvelope(new OrderCreated("concurrent-order"), 42L, "saga-start-correlation");

    // Must not throw — concurrent create OLE is swallowed as a dedup signal.
    runner.asEventListener().onEvents(List.of(event));

    // Exactly one saga row — no duplicate creation.
    assertThat(underlying.all())
        .as("exactly one saga row must exist after concurrent-create dedup")
        .contains(startSagaId)
        .hasSize(1);

    // The surviving row is the one created by the hook (version=1).
    var loaded =
        underlying
            .load(startSagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(loaded.version())
        .as("surviving saga must be at version 1 (created by concurrent winner)")
        .isEqualTo(1L);
  }

  // ===========================================================================
  // Test 3 — correlated conflict is retry-safe end-to-end (inbox dedup)
  // ===========================================================================

  /**
   * Pre-CAS: the runner used a blind upsert for correlated events. A concurrent advance would be
   * silently overwritten. No OLE was thrown, so the subscription never knew to retry — no retry
   * path existed.
   *
   * <p>With CAS: the correlated-event update is a CAS. If the hook advances the saga (v1→v2)
   * between load and update, the runner's update throws OLE which propagates out of {@code
   * onEvents} — the subscription retries. On retry (hook cleared, fresh load of v2), the runner
   * processes cleanly. The command inbox deduplicates by (event offset, command index), so the
   * {@link AdvanceOrder} command's events are appended exactly once across both deliveries.
   */
  @Test
  void correlatedConflict_thenRedelivery_isRetrySafe_endToEnd() {
    var inbox = new InMemoryCommandInbox();
    var eventStore = new InMemoryEventStore().withCommandInbox(inbox);

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(inbox)
            .register(
                TYPE,
                AdvanceOrder.class,
                cmd -> AggregateId.of(cmd.orderId()),
                new AdvanceDecider())
            .build();

    // Seed a RUNNING saga at v1.
    underlying.create(
        sagaId,
        FULFILL_TYPE,
        new FulfillState(SagaStatus.RUNNING, "race-order-1", "initial"),
        SagaStatus.RUNNING);

    var runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(new FulfillOrchestrator())
            .sagaStore(interleaving)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .build();

    // Correlated event with a stable globalOffset (same on redelivery).
    var shippedEvent = makeEnvelope(new OrderShipped("race-order-1"), 77L, "fulfill-race-order-1");

    // ---- First delivery ----
    // Hook: between the runner's load (v1) and its update, bump the saga to v2.
    interleaving.afterLoadHook(
        () ->
            underlying.update(
                sagaId,
                FULFILL_TYPE,
                new FulfillState(SagaStatus.RUNNING, "race-order-1", "bumped-by-other"),
                SagaStatus.RUNNING,
                1L)); // concurrent advance → v2

    // Must throw OLE — correlated-event conflict propagates so the subscription can retry.
    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(shippedEvent)))
        .as(
            "correlated-event CAS conflict must propagate as OptimisticLockException"
                + " so the subscription retries on fresh state")
        .isInstanceOf(OptimisticLockException.class);

    // The (successful) forward command dispatch happens BEFORE the saga-store CAS update in the
    // runner's pipeline: evolve → handle → dispatch forward command → sagaStore.update (throws
    // OLE).
    // So the command event IS appended on the first delivery, then OLE propagates.
    // On retry the inbox dedup finds the key already recorded and short-circuits.
    var aggregateStream = StreamId.of(TYPE, AggregateId.of("race-order-1"));
    var afterFirst = eventStore.readStream(aggregateStream, Version.initial(), 100);
    assertThat(afterFirst)
        .as("first delivery dispatched the command (event appended) before the saga-store OLE")
        .hasSize(1);

    // ---- Second delivery (subscription retry — same event, same globalOffset) ----
    // Hook is already cleared (was one-shot). The runner now loads v2 cleanly.
    runner.asEventListener().onEvents(List.of(shippedEvent));

    // Exactly one OrderAdvanced event in the aggregate stream — no duplicate from redelivery.
    var afterSecond = eventStore.readStream(aggregateStream, Version.initial(), 100);
    assertThat(afterSecond)
        .as(
            "exactly one OrderAdvanced event must exist after both deliveries"
                + " (inbox dedup on same offset+index key)")
        .hasSize(1);
    assertThat(afterSecond.getFirst().event())
        .isInstanceOf(OrderAdvanced.class)
        .extracting(e -> ((OrderAdvanced) e).orderId())
        .isEqualTo("race-order-1");

    // Saga is in a non-failed terminal-or-running state (shipped → RUNNING with shipped step).
    var finalSaga =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(finalSaga.status())
        .as("saga must not be spuriously compensated after the retry")
        .isNotIn(SagaStatus.COMPENSATED, SagaStatus.FAILED);
  }

  // ===========================================================================
  // Test 4 & 5 — event-path compensation is claim-first (no double refund)
  // ===========================================================================

  /** Compensation command emitted when the forward command fails. */
  record Refund(String orderId) implements Command {}

  /**
   * Orchestrator whose correlated {@link OrderShipped} handling dispatches a forward {@link
   * AdvanceOrder} (which {@link FailForwardRecordCompensationBus} is wired to FAIL), driving the
   * event-path compensation branch, and whose {@link #compensate} returns a distinct {@link Refund}
   * so forward vs. compensation dispatch can be told apart.
   */
  static class CompensatingOrchestrator implements SagaOrchestrator<FulfillState> {
    @Override
    public Class<FulfillState> stateType() {
      return FulfillState.class;
    }

    @Override
    public FulfillState initialState(SagaId sagaId) {
      return new FulfillState(SagaStatus.RUNNING, sagaId.value(), "initial");
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderCreated;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("fulfill-" + ((OrderCreated) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (event.event() instanceof OrderShipped os) {
        return Optional.of(SagaId.of("fulfill-" + os.orderId()));
      }
      return Optional.empty();
    }

    @Override
    public FulfillState evolve(FulfillState state, EventEnvelope event) {
      if (event.event() instanceof OrderShipped os) {
        return new FulfillState(SagaStatus.RUNNING, os.orderId(), "shipped");
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FulfillState state, EventEnvelope event) {
      // Forward command — the bus fails it, triggering event-path compensation.
      return List.of(
          SagaCommand.of(new AdvanceOrder(state.orderId()), AggregateId.of(state.orderId())));
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillState state, Throwable failure, SagaCommand failedCmd) {
      return List.of(SagaCommand.of(new Refund(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  /** Bus that FAILS any {@link AdvanceOrder} (forward) and RECORDS anything else (compensation). */
  static final class FailForwardRecordCompensationBus implements CommandBus {
    final List<Command> compensationsDispatched = new java.util.ArrayList<>();
    final List<IdempotencyKey> compensationKeys = new java.util.ArrayList<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("keyed execute only");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (command instanceof AdvanceOrder) {
        throw new RuntimeException("simulated forward command failure");
      }
      compensationsDispatched.add(command);
      compensationKeys.add(key);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  /**
   * Headline (fail-first): a timeout claim wins the compensation-episode CAS after the event path
   * has decided to compensate. The event path must claim-first — CAS to COMPENSATING at its loaded
   * version BEFORE dispatching any compensation — so losing that CAS means it dispatches ZERO
   * compensation commands and abandons (OLE → batch retry), leaving compensation to the sole owner
   * (the timeout runner).
   *
   * <p>Pre-fix (double-compensation bug): the event path dispatched compensation ({@link Refund})
   * BEFORE its terminal CAS, so this interleave dispatched Refund once under the disjoint
   * offset-based keyspace while the timeout path would compensate again under its own keyspace —
   * silent double refund. Pre-fix this assertion sees exactly one dispatched Refund and FAILS.
   */
  @Test
  void eventPath_losesClaimToTimeout_dispatchesZeroCompensation() {
    underlying.create(
        sagaId,
        FULFILL_TYPE,
        new FulfillState(SagaStatus.RUNNING, "race-order-1", "initial"),
        SagaStatus.RUNNING);

    var bus = new FailForwardRecordCompensationBus();
    var runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(new CompensatingOrchestrator())
            .sagaStore(interleaving)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .build();

    // Hook: after the event path loads v1 (RUNNING) but before its claim CAS, a timeout runner
    // claims the compensation episode (CAS v1 → COMPENSATING v2). The event path's forward command
    // then fails and its claim CAS at expectedVersion=1 loses to v2.
    interleaving.afterLoadHook(
        () ->
            underlying.update(
                sagaId,
                FULFILL_TYPE,
                new FulfillState(SagaStatus.COMPENSATING, "race-order-1", "claimed-by-timeout"),
                SagaStatus.COMPENSATING,
                1L));

    var shipped = makeEnvelope(new OrderShipped("race-order-1"), 77L, "fulfill-race-order-1");

    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(shipped)))
        .as("losing the COMPENSATING claim must propagate OLE (batch retry)")
        .isInstanceOf(OptimisticLockException.class);

    assertThat(bus.compensationsDispatched)
        .as(
            "event path must dispatch ZERO compensation after losing the claim — otherwise it"
                + " double-compensates with the timeout path under a disjoint key")
        .isEmpty();

    var after =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(after.status())
        .as("the timeout runner's COMPENSATING claim must survive untouched")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(after.version()).isEqualTo(2L);
  }

  /**
   * Shared-key (fail-first): when the event path DOES win the claim and drives compensation, it
   * must key the compensation with the SHARED, episode-scoped key that the timeout path also uses,
   * so a genuine double-delivery across the two paths dedups in the command inbox rather than
   * executing the side effect twice.
   *
   * <p>Pre-fix the event path used the disjoint offset-based compensation key ({@code
   * saga:…:77:comp:0}) and did a single blind CAS (final version 2). Post-fix it claims (v1→v2),
   * uses the episode-scoped key ({@code saga:…:episode:2:comp:0}), then terminalizes (v2→v3). Both
   * the key and the version assertions fail pre-fix.
   */
  @Test
  void eventPathCompensation_usesSharedEpisodeScopedKey_notDisjointOffsetKey() {
    underlying.create(
        sagaId,
        FULFILL_TYPE,
        new FulfillState(SagaStatus.RUNNING, "race-order-1", "initial"),
        SagaStatus.RUNNING);

    var bus = new FailForwardRecordCompensationBus();
    var runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(new CompensatingOrchestrator())
            .sagaStore(underlying) // no interleave: the event path wins its own claim
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .build();

    var shipped = makeEnvelope(new OrderShipped("race-order-1"), 77L, "fulfill-race-order-1");
    runner.asEventListener().onEvents(List.of(shipped));

    var corr = CorrelationId.of(sagaId.value());
    assertThat(bus.compensationKeys)
        .as(
            "event-path compensation must use the shared episode-scoped key (episodeVersion=2) that"
                + " the timeout path also uses — not the disjoint offset-based key")
        .containsExactly(SagaCommandDispatch.episodeCompensationKey(corr, 2L, 0));

    var after =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(after.status())
        .as("event-path compensation must terminalize")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(after.version())
        .as("claim (v1→v2) + terminal write (v2→v3) = final version 3")
        .isEqualTo(3L);
  }

  // ===========================================================================
  // Test 6 — no-timeout saga resumes a crashed compensation episode on redelivery
  // ===========================================================================

  /** Error used to simulate a JVM crash between the COMPENSATING claim and the terminal write. */
  static final class SimulatedCrashError extends Error {
    SimulatedCrashError() {
      super("simulated crash between COMPENSATING claim and terminal write");
    }
  }

  /**
   * Bus that FAILS any {@link AdvanceOrder} (forward → triggers compensation) and, for compensation
   * commands ({@link Refund}), <em>crashes exactly once</em> before recording — then records on
   * every later dispatch. The crash is an {@link Error}, so it escapes {@code
   * compensateAndClassify}'s {@code catch (Exception)} and propagates out of {@code onEvents},
   * modelling a process death between the claim and the terminal write: the claim is already
   * committed (saga COMPENSATING at the episode version), but no compensation was recorded and no
   * terminal status was persisted.
   */
  static final class CrashOnceThenRecordCompensationBus implements CommandBus {
    final List<Command> compensationsDispatched = new java.util.ArrayList<>();
    final List<IdempotencyKey> compensationKeys = new java.util.ArrayList<>();
    private boolean crashed = false;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("keyed execute only");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (command instanceof AdvanceOrder) {
        throw new RuntimeException("simulated forward command failure");
      }
      if (!crashed) {
        crashed = true;
        throw new SimulatedCrashError();
      }
      compensationsDispatched.add(command);
      compensationKeys.add(key);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  /**
   * A saga with <b>no timeout configured</b> — a {@link SagaOrchestrator} has no {@code
   * SagaDecider.timeout()}, so no {@link SagaTimeoutRunner} can ever recover it — that crashes
   * between claiming {@code COMPENSATING} and writing the terminal status must still reach a
   * terminal state when the correlated event is redelivered. The event path is the <em>sole</em>
   * crash-recovery for such a saga.
   *
   * <p>First delivery: the forward {@link AdvanceOrder} fails, the event path claims the episode
   * (CAS v1 → COMPENSATING v2), then the first {@link Refund} dispatch throws a {@link
   * SimulatedCrashError} that propagates out of {@code onEvents} before the terminal write — the
   * saga is left COMPENSATING at v2 with zero compensation recorded.
   *
   * <p>Redelivery: the event path <em>resumes</em> the episode (no re-claim, no version bump) at v2
   * — re-driving compensation under the shared episode-scoped key {@code saga:…:episode:2:comp:0}
   * and terminal-CAS'ing v2 → v3. Compensation is dispatched exactly once and the saga reaches
   * {@code COMPENSATED}.
   *
   * <p><b>Fail-first evidence:</b> pre-fix, {@code processCorrelatedEvent} unconditionally skips a
   * {@code COMPENSATING} saga on redelivery, so no compensation is ever dispatched (list stays
   * empty) and the saga is wedged at {@code COMPENSATING} v2 forever — both the "exactly one
   * compensation" and "reaches COMPENSATED" assertions fail.
   */
  @Test
  void noTimeoutSaga_crashDuringCompensation_resumesOnRedelivery_compensatesExactlyOnce() {
    underlying.create(
        sagaId,
        FULFILL_TYPE,
        new FulfillState(SagaStatus.RUNNING, "race-order-1", "initial"),
        SagaStatus.RUNNING);

    var bus = new CrashOnceThenRecordCompensationBus();
    var runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(new CompensatingOrchestrator()) // SagaOrchestrator → no timeout runner
            .sagaStore(underlying)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(underlying))
            .build();

    var shipped = makeEnvelope(new OrderShipped("race-order-1"), 77L, "fulfill-race-order-1");

    // ---- First delivery: claim COMPENSATING (v2), then crash before the terminal write. ----
    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(shipped)))
        .as("the simulated crash (Error) propagates out of onEvents")
        .isInstanceOf(SimulatedCrashError.class);

    var afterCrash =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(afterCrash.status())
        .as("crash left the saga COMPENSATING (claim committed, terminal write never ran)")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(afterCrash.version()).as("claim bumped v1 → v2").isEqualTo(2L);
    assertThat(bus.compensationsDispatched)
        .as("crash aborted before any compensation was recorded")
        .isEmpty();

    // ---- Redelivery: the event path must RESUME and complete compensation. ----
    runner.asEventListener().onEvents(List.of(shipped));

    var corr = CorrelationId.of(sagaId.value());
    assertThat(bus.compensationsDispatched)
        .as("compensation must be dispatched exactly once across crash + resume")
        .hasSize(1);
    assertThat(bus.compensationsDispatched.getFirst()).isInstanceOf(Refund.class);
    assertThat(bus.compensationKeys)
        .as(
            "resume must re-drive compensation under the SHARED episode-scoped key at the existing"
                + " version (episodeVersion=2) — the same key a timeout resume would use")
        .containsExactly(SagaCommandDispatch.episodeCompensationKey(corr, 2L, 0));

    var resumed =
        underlying
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(resumed.status())
        .as("resume must terminalize the crashed episode — a no-timeout saga is not wedged forever")
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(resumed.version())
        .as("terminal write resumes at the episode version (v2 → v3), without a re-claim bump")
        .isEqualTo(3L);
  }
}

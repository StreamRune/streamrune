package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
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
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

class SagaRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ========== Test domain types ==========

  record OrderCreated(String orderId) implements DomainEvent {}

  record OrderPaid(String orderId) implements DomainEvent {}

  record FulfillOrder(String orderId) implements Command {}

  record RefundOrder(String orderId) implements Command {}

  // Simple saga state (records work fine with InMemorySagaStore — no JSON needed)
  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  // ========== Test saga orchestrator ==========

  static class OrderFulfillmentSaga implements SagaOrchestrator<OrderState> {
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
      return event.event() instanceof OrderCreated;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((OrderCreated) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      if (corrId.startsWith("saga-")) return Optional.of(SagaId.of(corrId));
      return Optional.empty();
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      if (event.event() instanceof OrderCreated oc) {
        return new OrderState(SagaStatus.RUNNING, oc.orderId());
      }
      if (event.event() instanceof OrderPaid) {
        return new OrderState(SagaStatus.COMPLETED, state.orderId());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      if (event.event() instanceof OrderCreated) {
        return List.of(
            SagaCommand.of(new FulfillOrder(state.orderId()), AggregateId.of(state.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCmd) {
      // failedCmd is null when compensation is resumed after a crash (SagaRunner) or driven by a
      // timeout (SagaTimeoutRunner): recompute the undo from state, not from the unavailable failed
      // command.
      if (failedCmd == null || failedCmd.command() instanceof FulfillOrder) {
        return List.of(
            SagaCommand.of(new RefundOrder(state.orderId()), AggregateId.of(state.orderId())));
      }
      return List.of();
    }
  }

  // ========== Capturing CommandBus ==========

  static class CapturingCommandBus implements CommandBus {
    final List<Object> dispatched = new ArrayList<>();
    private RuntimeException failWith = null;

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    void failNext(RuntimeException e) {
      this.failWith = e;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (failWith != null) {
        RuntimeException ex = failWith;
        failWith = null;
        throw ex;
      }
      dispatched.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  // ========== Context-capturing CommandBus ==========

  /**
   * Captures the {@link StreamRuneContext} bound at the moment {@code execute} runs, so a test can
   * assert what request context the saga runner bound around the dispatch. Optionally fails the
   * first command to exercise the compensation path.
   */
  static class ContextCapturingCommandBus implements CommandBus {
    final List<RequestContext> contexts = new ArrayList<>();
    final List<Object> dispatched = new ArrayList<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    private RuntimeException failWith = null;

    void failNext(RuntimeException e) {
      this.failWith = e;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      contexts.add(StreamRuneContext.capture());
      if (failWith != null) {
        RuntimeException ex = failWith;
        failWith = null;
        throw ex;
      }
      dispatched.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  // ========== Helpers ==========

  private EventEnvelope envelope(DomainEvent event, String correlationId) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of(correlationId),
            null,
            null,
            Instant.now()));
  }

  InMemorySagaStore sagaStore;
  CapturingCommandBus commandBus;
  InMemorySagaDeadLetterStore dlq;
  SagaRunner<OrderState> runner;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    commandBus = new CapturingCommandBus();
    dlq = new InMemorySagaDeadLetterStore(sagaStore);
    runner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(dlq)
            .build();
  }

  // ========== Tests ==========

  @Test
  void processes_start_event_creates_saga_and_dispatches_command() {
    var event = envelope(new OrderCreated("order-1"), "some-correlation");

    runner.asEventListener().onEvents(List.of(event));

    assertEquals(1, commandBus.dispatched.size());
    assertInstanceOf(FulfillOrder.class, commandBus.dispatched.get(0));
    assertEquals("order-1", ((FulfillOrder) commandBus.dispatched.get(0)).orderId());

    var loaded =
        sagaStore.load(
            SagaId.of("saga-order-1"), SagaType.fromClass(OrderState.class), OrderState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.RUNNING, loaded.get().status());
    assertEquals("order-1", loaded.get().state().orderId());
  }

  @Test
  void correlate_event_advances_existing_saga() {
    sagaStore.create(
        SagaId.of("saga-order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);

    var event = envelope(new OrderPaid("order-1"), "saga-order-1");
    runner.asEventListener().onEvents(List.of(event));

    assertEquals(0, commandBus.dispatched.size());

    var loaded =
        sagaStore.load(
            SagaId.of("saga-order-1"), SagaType.fromClass(OrderState.class), OrderState.class);
    assertTrue(loaded.isPresent());
    assertEquals(SagaStatus.COMPLETED, loaded.get().status());
  }

  /**
   * Wiring pin: {@code processCorrelatedEvent} is now a thin adapter that routes a live correlated
   * event through {@code SagaStepExecutor} (a {@code ForwardStep} trigger). This asserts the
   * executor path is wired to production — a correlated event evolves the loaded saga and
   * CAS-persists its advance (RUNNING -&gt; COMPLETED at a bumped version). This orchestrator's
   * {@code handle()} emits a forward command only for the START event {@code OrderCreated}, so a
   * correlated {@code OrderPaid} dispatches no forward command; the persisted advance IS the
   * forward path's observable effect. Command dispatch through the same executor (compensation) is
   * pinned by {@link
   * #compensatingSaga_resumesCompensationOnRedelivery_withoutForwardEvolveOrHandle()}.
   */
  @Test
  void correlatedEvent_routesThroughExecutor_advancesSaga() {
    sagaStore.create(
        SagaId.of("saga-order-wired"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.RUNNING, "order-wired"),
        SagaStatus.RUNNING);

    var event = envelope(new OrderPaid("order-wired"), "saga-order-wired");
    runner.asEventListener().onEvents(List.of(event));

    var loaded =
        sagaStore.load(
            SagaId.of("saga-order-wired"), SagaType.fromClass(OrderState.class), OrderState.class);
    assertTrue(loaded.isPresent());
    // The executor's forward step evolved the saga to a terminal COMPLETED and CAS-persisted it.
    assertEquals(SagaStatus.COMPLETED, loaded.get().status());
    assertEquals("order-wired", loaded.get().state().orderId());
    // create() -> v1, the executor's forward CAS update -> v2: proof a store write went through the
    // executor path rather than a hand-rolled inline update.
    assertEquals(2L, loaded.get().version());
  }

  @Test
  void command_failure_triggers_compensation() {
    commandBus.failNext(new RuntimeException("fulfillment service down"));
    var event = envelope(new OrderCreated("order-2"), "some-correlation");

    runner.asEventListener().onEvents(List.of(event));

    assertEquals(1, commandBus.dispatched.size());
    assertInstanceOf(RefundOrder.class, commandBus.dispatched.get(0));
    assertEquals("order-2", ((RefundOrder) commandBus.dispatched.get(0)).orderId());
  }

  @Test
  void terminal_saga_ignores_follow_up_events() {
    sagaStore.create(
        SagaId.of("saga-order-3"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.COMPLETED, "order-3"),
        SagaStatus.COMPLETED);

    var event = envelope(new OrderPaid("order-3"), "saga-order-3");
    runner.asEventListener().onEvents(List.of(event));

    assertEquals(0, commandBus.dispatched.size());
    assertEquals(
        SagaStatus.COMPLETED,
        sagaStore
            .load(SagaId.of("saga-order-3"), SagaType.fromClass(OrderState.class), OrderState.class)
            .get()
            .status());
    // A TERMINAL saga is legitimately done and can never consume the event: the drop is correct and
    // deliberately leaves NO record (the halted guard is split by intent).
    assertTrue(dlq.findBySaga(SagaId.of("saga-order-3")).isEmpty());
  }

  /**
   * A correlated event delivered while the saga is FAULTED must leave a durable record. The saga is
   * quarantined pending an operator fix, not finished — dropping the event silently (the pre-fix
   * behaviour) meant the subscription checkpoint advanced past an event nothing would ever
   * redeliver, so the eventual dead-letter replay of the original poison event resurrected a saga
   * that could never learn about the payment/stock events received in the FAULTED window.
   *
   * <p>The pre-fix assertion set — 0 commands dispatched, status unchanged — is exactly what let
   * the loss through, so it is kept and the missing durable-record assertion is added.
   */
  @Test
  void faulted_saga_ignores_follow_up_events() {
    var sagaId = SagaId.of("saga-order-faulted");
    sagaStore.create(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.RUNNING, "order-faulted"),
        SagaStatus.FAULTED);

    var event = envelope(new OrderPaid("order-faulted"), "saga-order-faulted");
    runner.asEventListener().onEvents(List.of(event));

    // FAULTED is halted — no commands dispatched, status unchanged (the runner must NOT re-write
    // the saga row from the skip path).
    assertEquals(0, commandBus.dispatched.size());
    assertEquals(
        SagaStatus.FAULTED,
        sagaStore
            .load(sagaId, SagaType.fromClass(OrderState.class), OrderState.class)
            .get()
            .status());

    // ...but the skipped event is now recorded, so replayAll can re-deliver it after the fix ships.
    var entries = dlq.findBySaga(sagaId);
    assertEquals(1, entries.size(), "the skipped event must leave a dead-letter record");
    assertEquals(event.globalOffset(), entries.get(0).eventOffset());
  }

  /**
   * A correlated event redelivered to a saga already {@code COMPENSATING} must NOT re-run forward
   * evolve/handle (it must never dispatch the forward {@code FulfillOrder}), but it MUST
   * <em>resume</em> the compensation episode a crashed owner left behind — re-driving compensation
   * at the existing version so a saga with no timeout runner still reaches a terminal status. The
   * earlier behaviour (defer entirely, store untouched) permanently wedged such a saga.
   */
  @Test
  void compensatingSaga_resumesCompensationOnRedelivery_withoutForwardEvolveOrHandle() {
    sagaStore.create(
        SagaId.of("saga-order-compensating"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.RUNNING, "order-compensating"),
        SagaStatus.RUNNING);
    // A prior owner claimed the episode: CAS RUNNING (v1) -> COMPENSATING (v2), then crashed before
    // the terminal write.
    sagaStore.update(
        SagaId.of("saga-order-compensating"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaStatus.COMPENSATING, "order-compensating"),
        SagaStatus.COMPENSATING,
        1L);
    var claimed =
        sagaStore
            .load(
                SagaId.of("saga-order-compensating"),
                SagaType.fromClass(OrderState.class),
                OrderState.class)
            .get();
    assertEquals(SagaStatus.COMPENSATING, claimed.status());
    assertEquals(2L, claimed.version());

    var event = envelope(new OrderPaid("order-compensating"), "saga-order-compensating");
    runner.asEventListener().onEvents(List.of(event));

    // Compensation was resumed: exactly the compensation command (RefundOrder) was dispatched —
    // never the forward FulfillOrder (no forward evolve/handle on a COMPENSATING saga).
    assertEquals(1, commandBus.dispatched.size());
    assertInstanceOf(RefundOrder.class, commandBus.dispatched.get(0));
    assertEquals("order-compensating", ((RefundOrder) commandBus.dispatched.get(0)).orderId());

    // The episode reached a terminal status at the resumed episode version (v2 -> v3), no re-claim.
    var after =
        sagaStore
            .load(
                SagaId.of("saga-order-compensating"),
                SagaType.fromClass(OrderState.class),
                OrderState.class)
            .get();
    assertEquals(SagaStatus.COMPENSATED, after.status());
    assertEquals(3L, after.version());
  }

  @Test
  void compensation_failure_is_swallowed_and_saga_state_is_still_saved() {
    // Both the primary command and its compensation will fail
    commandBus.failNext(new RuntimeException("primary failure"));
    // Set up a second failure for the compensation command
    var failingBus =
        new CommandBus() {
          int callCount = 0;

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
            callCount++;
            throw new RuntimeException("always fails: call " + callCount);
          }
        };
    var failingRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(failingBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var event = envelope(new OrderCreated("order-5"), "some-correlation");

    // Must not throw even though both primary and compensation fail
    assertDoesNotThrow(() -> failingRunner.asEventListener().onEvents(List.of(event)));
    // Saga state must still be persisted
    assertTrue(
        sagaStore
            .load(SagaId.of("saga-order-5"), SagaType.fromClass(OrderState.class), OrderState.class)
            .isPresent(),
        "Saga state must be saved even when compensation fails");
  }

  @Test
  void duplicate_start_event_is_idempotent() {
    var event = envelope(new OrderCreated("order-4"), "some-correlation");

    runner.asEventListener().onEvents(List.of(event));
    assertEquals(1, commandBus.dispatched.size());

    // Replay the same start event — must be a no-op
    runner.asEventListener().onEvents(List.of(event));
    assertEquals(
        1,
        commandBus.dispatched.size(),
        "Replay of start event must not dispatch additional commands");
    assertEquals(
        SagaStatus.RUNNING,
        sagaStore
            .load(SagaId.of("saga-order-4"), SagaType.fromClass(OrderState.class), OrderState.class)
            .get()
            .status(),
        "Saga state must not be overwritten by replay");
  }

  @Test
  void non_matching_event_is_ignored() {
    var event = envelope(new OrderPaid("order-99"), "unrelated-correlation");
    runner.asEventListener().onEvents(List.of(event));
    assertEquals(0, commandBus.dispatched.size());
  }

  // ========== Null RETURN from the routers is poison, not a wedge ==========

  /**
   * Orchestrator with a user bug: {@code extractSagaId} does a map lookup that returns {@code null}
   * for a malformed producer event instead of throwing — the exact deterministic user-logic failure
   * class the poison system exists for, except it surfaces as a null RETURN rather than a thrown
   * exception.
   */
  static class NullReturningRouterSaga extends OrderFulfillmentSaga {
    volatile boolean extractReturnsNull = false;
    volatile boolean correlateReturnsNull = false;

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      if (extractReturnsNull) {
        return null; // e.g. event.metadata().get("orderId") on a malformed event
      }
      return super.extractSagaId(event);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      if (correlateReturnsNull) {
        return null; // contract violation: must be Optional.empty(), never a bare null
      }
      return super.correlate(event);
    }
  }

  /**
   * A {@code null} return from {@code extractSagaId} must be classified exactly like a thrown
   * exception — a poison event, quarantined as a null-saga entry — never allowed to flow onward as
   * a {@code null} sagaId. Pre-fix, {@code processStartEvent} passed the {@code null} to {@code
   * sagaStore.load(null, ...)}, whose NPE (wrapped as an infra exception by real stores) escaped
   * {@code asEventListener}'s poison handling: the subscription offset never advanced and the batch
   * was retried forever — a permanently wedged saga subscription (head-of-line blocking) on a
   * deterministic user bug.
   */
  @Test
  void startEvent_extractSagaIdReturnsNull_isQuarantinedAsNullSagaPoison_batchContinues() {
    var sagaDlq = new InMemorySagaDeadLetterStore(sagaStore);
    var nullSaga = new NullReturningRouterSaga();
    nullSaga.extractReturnsNull = true;
    var nullRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(nullSaga)
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(sagaDlq)
            .build();

    var poisonEvent = envelope(new OrderCreated("order-null-id"), "some-correlation");
    var healthyFollowUp = envelope(new OrderPaid("order-unrelated"), "unrelated-correlation");

    // The audited harm: this must NOT throw (a throw here means the subscription never advances
    // and retries the same batch forever — the deterministic wedge).
    assertDoesNotThrow(
        () -> nullRunner.asEventListener().onEvents(List.of(poisonEvent, healthyFollowUp)),
        "a null extractSagaId return must be quarantined like a throw, not wedge the subscription");

    // Classified as poison: quarantined by metadata as a NULL-SAGA entry (no saga id could be
    // resolved — the same entry class as a throwing extractSagaId).
    var entries = sagaDlq.findAll(10);
    assertEquals(1, entries.size(), "the null-return event must be quarantined");
    assertNull(entries.get(0).sagaId(), "no saga id was resolvable — a null-saga entry");
    assertEquals(poisonEvent.globalOffset(), entries.get(0).eventOffset());
    assertTrue(
        entries.get(0).errorType().contains("NullPointerException"),
        "the deterministic contract violation is the recorded cause");
    // No saga row was created or faulted (there is no saga to fault), and the batch continued.
    assertEquals(0, commandBus.dispatched.size());
  }

  /**
   * A bare {@code null} returned from {@code correlate} (instead of {@code Optional.empty()}) must
   * likewise be quarantined as a null-saga poison event. Pre-fix the NPE fired at {@code
   * corr.ifPresent(...)} — OUTSIDE the poison wrapper — and escaped {@code asEventListener}
   * entirely, wedging the subscription on the batch forever.
   */
  @Test
  void correlatedEvent_correlateReturnsNull_isQuarantinedAsNullSagaPoison_batchContinues() {
    var sagaDlq = new InMemorySagaDeadLetterStore(sagaStore);
    var nullSaga = new NullReturningRouterSaga();
    nullSaga.correlateReturnsNull = true;
    var nullRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(nullSaga)
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(sagaDlq)
            .build();

    // OrderPaid is not a start event, so it routes through correlate().
    var poisonEvent = envelope(new OrderPaid("order-null-corr"), "saga-order-null-corr");

    assertDoesNotThrow(
        () -> nullRunner.asEventListener().onEvents(List.of(poisonEvent)),
        "a null correlate return must be quarantined like a throw, not wedge the subscription");

    var entries = sagaDlq.findAll(10);
    assertEquals(1, entries.size(), "the null-return event must be quarantined");
    assertNull(entries.get(0).sagaId(), "no saga id was resolvable — a null-saga entry");
    assertTrue(entries.get(0).errorType().contains("NullPointerException"));
    assertEquals(0, commandBus.dispatched.size());
  }

  /**
   * Contract-conforming null-signal pin: {@code correlate} returning {@code Optional.empty()} is
   * the documented "not mine" signal — ignored, never quarantined. The fix must only catch the
   * contract VIOLATION (a bare null), not the legitimate empty.
   */
  @Test
  void correlatedEvent_correlateReturnsEmptyOptional_isIgnored_neverQuarantined() {
    var sagaDlq = new InMemorySagaDeadLetterStore(sagaStore);
    var conformingRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(sagaDlq)
            .build();

    var event = envelope(new OrderPaid("order-empty"), "unrelated-correlation");
    assertDoesNotThrow(() -> conformingRunner.asEventListener().onEvents(List.of(event)));

    assertEquals(0, sagaDlq.findAll(10).size(), "Optional.empty() is 'not mine', never poison");
    assertEquals(0, commandBus.dispatched.size());
  }

  // ========== Conflict-semantics tests ==========

  /**
   * Concurrent-create dedup: two writers race on the same start event; the other's genesis-pending
   * create has already won and committed its genesis. This writer's {@code createGenesisPending}
   * sees {@link OptimisticLockException}, the executor RELOADS and routes on the recorded row — an
   * applied genesis — so the delivery is {@code SKIPPED_DEDUP}: {@code onEvents} must not throw and
   * must dispatch nothing (the create precedes every dispatch).
   */
  @Test
  void startEvent_whenSagaConcurrentlyCreated_skipsSilently() {
    // Delegating store modelling the race: the runner's pre-create load is STALE (empty), every
    // create conflicts with the winner's row, and the executor's reload after the conflict sees
    // the winner's applied genesis.
    var concurrentlyCreatedStore =
        new SagaStore() {
          private final InMemorySagaStore delegate = new InMemorySagaStore();
          private final AtomicBoolean staleFirstLoad = new AtomicBoolean(true);

          {
            delegate.create(
                SagaId.of("saga-order-concurrent"),
                SagaType.fromClass(OrderState.class),
                new OrderState(SagaStatus.RUNNING, "order-concurrent"),
                SagaStatus.RUNNING); // the winner: genesis applied
          }

          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new OptimisticLockException("simulated concurrent create");
          }

          @Override
          public void createGenesisPending(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new OptimisticLockException("simulated concurrent create");
          }

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {
            delegate.update(sagaId, sagaType, state, status, expectedVersion);
          }

          @Override
          public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
            return delegate.findTimedOut(sagaType, updatedBefore, limit);
          }

          @Override
          public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
            return delegate.findByStatus(sagaType, status, limit);
          }

          @Override
          public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
              SagaId sagaId, SagaType sagaType, Class<S> stateType) {
            // The runner's first load is stale (empty); the executor's reload sees the winner.
            return staleFirstLoad.compareAndSet(true, false)
                ? Optional.empty()
                : delegate.load(sagaId, sagaType, stateType);
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

          @Override
          public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
            throw new UnsupportedOperationException("createFaulted");
          }

          @Override
          public void applyEvent(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion,
              AppliedEvent applied) {
            throw new UnsupportedOperationException("applyEvent");
          }

          @Override
          public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
            throw new UnsupportedOperationException("markFaulted");
          }

          @Override
          public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
            throw new UnsupportedOperationException("setDeadLetterPending");
          }
        };

    var concurrentRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(concurrentlyCreatedStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(concurrentlyCreatedStore))
            .build();

    var event = envelope(new OrderCreated("order-concurrent"), "some-correlation");

    // Primary assertion: must not throw — the conflict is a dedup signal, routed on the reload.
    assertDoesNotThrow(() -> concurrentRunner.asEventListener().onEvents(List.of(event)));
    // The intent row precedes the first dispatch, so a losing creator has
    // dispatched NOTHING — no command reaches the bus from this replica.
    assertThat(commandBus.dispatched).isEmpty();
  }

  /**
   * Correlated-event stale-version conflict propagates: the CAS {@code update} throws {@link
   * OptimisticLockException} (another writer advanced the saga first), which must propagate out of
   * {@code onEvents} so the subscription can retry. It must NOT be quarantined in the saga DLQ.
   */
  @Test
  void correlatedEvent_onStaleVersion_propagatesOptimisticLock() {
    // Delegating store: load returns a saga, but update throws OLE (stale view).
    var staleStore =
        new SagaStore() {
          private final InMemorySagaStore delegate = new InMemorySagaStore();

          {
            delegate.create(
                SagaId.of("saga-order-stale"),
                SagaType.fromClass(OrderState.class),
                new OrderState(SagaStatus.RUNNING, "order-stale"),
                SagaStatus.RUNNING);
          }

          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            delegate.create(sagaId, sagaType, state, status, deadLetterPending);
          }

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {
            throw new OptimisticLockException("simulated stale update");
          }

          /** The correlated forward CAS is {@code applyEvent} — same stale conflict. */
          @Override
          public void applyEvent(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion,
              AppliedEvent applied) {
            throw new OptimisticLockException("simulated stale update");
          }

          @Override
          public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
            return delegate.findTimedOut(sagaType, updatedBefore, limit);
          }

          @Override
          public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
            return delegate.findByStatus(sagaType, status, limit);
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

          @Override
          public void createGenesisPending(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {
            throw new UnsupportedOperationException("createGenesisPending");
          }

          @Override
          public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
            throw new UnsupportedOperationException("createFaulted");
          }

          @Override
          public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
            throw new UnsupportedOperationException("markFaulted");
          }

          @Override
          public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
            throw new UnsupportedOperationException("setDeadLetterPending");
          }
        };

    var sagaDlq = new InMemorySagaDeadLetterStore(staleStore);
    var staleRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(staleStore)
            .commandBus(commandBus)
            .sagaDeadLetterStore(sagaDlq)
            .build();

    // Correlated event for the existing running saga
    var event = envelope(new OrderPaid("order-stale"), "saga-order-stale");

    // Must propagate — subscription retries on fresh state
    assertThatThrownBy(() -> staleRunner.asEventListener().onEvents(List.of(event)))
        .isInstanceOf(OptimisticLockException.class);

    // NOT quarantined — this is an infra conflict, not a poison event
    assertThat(sagaDlq.findAll(10)).isEmpty();
  }

  @Test
  void builder_rejects_missing_orchestrator() {
    assertThrows(
        IllegalStateException.class,
        () -> SagaRunner.<OrderState>builder().sagaStore(sagaStore).commandBus(commandBus).build());
  }

  @Test
  void builder_rejects_missing_saga_store() {
    assertThrows(
        IllegalStateException.class,
        () ->
            SagaRunner.<OrderState>builder()
                .orchestrator(new OrderFulfillmentSaga())
                .commandBus(commandBus)
                .build());
  }

  @Test
  void builder_rejects_missing_command_bus() {
    assertThrows(
        IllegalStateException.class,
        () ->
            SagaRunner.<OrderState>builder()
                .orchestrator(new OrderFulfillmentSaga())
                .sagaStore(sagaStore)
                .build());
  }

  /**
   * Every saga command dispatch goes through the keyed {@code execute(command, key)} and has no
   * unkeyed fallback, so a bus without a CommandInbox (supportsIdempotentExecution() == false)
   * makes every saga silently wedge COMPENSATING then FAULTED at runtime. The builder must reject
   * such a bus at build time, since the misconfiguration is 100% detectable there.
   */
  @Test
  void builder_rejects_bus_without_idempotent_execution() {
    // A bus that implements keyed execute() but reports no idempotency support (default false).
    CommandBus noInboxBus =
        new CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            return new CommandResult(
                List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
          }
        };
    assertThatThrownBy(
            () ->
                SagaRunner.<OrderState>builder()
                    .orchestrator(new OrderFulfillmentSaga())
                    .sagaStore(sagaStore)
                    .commandBus(noInboxBus)
                    .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("idempotent");
  }

  /**
   * A saga state class whose fully-qualified name the {@code saga_type} column cannot hold is
   * refused when the runner is built, not on the first delivered event.
   */
  @Test
  void builder_rejects_a_state_class_name_the_saga_type_column_cannot_hold() throws Exception {
    byte[] bytes =
        ClassFile.of().build(ClassDesc.of("org.streamrune.runtime." + "L".repeat(240)), cb -> {});
    Class<?> longNamed = MethodHandles.lookup().defineHiddenClass(bytes, false).lookupClass();
    assertTrue(longNamed.getName().length() > 255);
    @SuppressWarnings("unchecked")
    Class<OrderState> longStateType = (Class<OrderState>) (Class<?>) longNamed;
    var longNamedSaga =
        new OrderFulfillmentSaga() {
          @Override
          public Class<OrderState> stateType() {
            return longStateType;
          }
        };

    assertThatThrownBy(
            () ->
                SagaRunner.<OrderState>builder()
                    .orchestrator(longNamedSaga)
                    .sagaStore(sagaStore)
                    .commandBus(commandBus)
                    .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at most 255");
  }

  // ========== Correlation propagation ==========

  @Test
  void dispatched_command_runs_under_context_with_correlation_id_equal_to_saga_id() {
    var capturingBus = new ContextCapturingCommandBus();
    var correlatingRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(capturingBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var event = envelope(new OrderCreated("order-1"), "some-correlation");
    correlatingRunner.asEventListener().onEvents(List.of(event));

    assertEquals(1, capturingBus.contexts.size());
    RequestContext ctx = capturingBus.contexts.get(0);
    assertNotNull(ctx, "the saga runner must bind a request context around the command bus");
    // correlationId == sagaId.value(); the saga id is "saga-" + orderId (see extractSagaId).
    assertEquals("saga-order-1", ctx.correlationId().value());
  }

  @Test
  void dispatch_preserves_ambient_trace_and_user_while_overriding_correlation_id() {
    var capturingBus = new ContextCapturingCommandBus();
    var correlatingRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(capturingBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var ambient =
        new RequestContext(
            TraceId.of("trace-ambient"),
            UserId.of("user-ambient"),
            CorrelationId.of("ambient-correlation"),
            Instant.now(),
            Map.of("tenant", "acme"));

    var event = envelope(new OrderCreated("order-7"), "some-correlation");
    ScopedValue.where(StreamRuneContext.CURRENT, ambient)
        .run(() -> correlatingRunner.asEventListener().onEvents(List.of(event)));

    assertEquals(1, capturingBus.contexts.size());
    RequestContext ctx = capturingBus.contexts.get(0);
    assertNotNull(ctx);
    // correlation id is overridden to the saga id...
    assertEquals("saga-order-7", ctx.correlationId().value());
    // ...while ambient trace / user / baggage are preserved.
    assertNotNull(ctx.traceId());
    assertEquals("trace-ambient", ctx.traceId().value());
    assertNotNull(ctx.userId());
    assertEquals("user-ambient", ctx.userId().value());
    assertEquals("acme", ctx.baggage().get("tenant"));
  }

  @Test
  void compensation_command_also_runs_under_correlated_context() {
    var capturingBus = new ContextCapturingCommandBus();
    // First command (FulfillOrder) fails, triggering the compensation (RefundOrder) dispatch.
    capturingBus.failNext(new RuntimeException("fulfillment service down"));
    var correlatingRunner =
        SagaRunner.<OrderState>builder()
            .orchestrator(new OrderFulfillmentSaga())
            .sagaStore(sagaStore)
            .commandBus(capturingBus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    var event = envelope(new OrderCreated("order-8"), "some-correlation");
    correlatingRunner.asEventListener().onEvents(List.of(event));

    // Two execute() calls: the failing primary and the compensation.
    assertEquals(2, capturingBus.contexts.size());
    // The compensation command was the one that actually dispatched (primary threw).
    assertEquals(1, capturingBus.dispatched.size());
    assertInstanceOf(RefundOrder.class, capturingBus.dispatched.get(0));
    // Both the primary and the compensation ran under the saga-correlated context.
    for (RequestContext ctx : capturingBus.contexts) {
      assertNotNull(ctx, "every dispatch (primary + compensation) must bind a context");
      assertEquals("saga-order-8", ctx.correlationId().value());
    }
  }
}

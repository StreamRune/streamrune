package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
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
import org.streamrune.test.InMemorySagaStore;

/**
 * Chain B — a saga whose stored id carries characters the ingress bound refuses must keep running,
 * and must keep being able to undo itself.
 *
 * <p>{@code SagaStepExecutor} derives the dispatch correlation id from the SAGA ID ({@code
 * CorrelationId.of(sagaId.value())}), and every background saga entry point rebuilds that {@link
 * SagaId} <em>from storage</em>: {@code PostgresSagaStore.findTimedOut / findByStatus /
 * findCompensating} all do {@code SagaId.of(rs.getString("saga_id"))}. {@link SagaId} bounds length
 * (VARCHAR(255)) but deliberately not charset, and user {@code correlate()} implementations derive
 * the id from event data — a business field, or an event's metadata correlation id, which a context
 * built with the plain {@code RequestContext} constructor or application-built {@code
 * EventMetadata} carries without ever crossing the ingress door.
 *
 * <p>So if the ingress bound runs inside the {@code RequestContext} constructor, {@code
 * executeCorrelated} throws {@link IllegalArgumentException} <b>before the bus is ever called</b>.
 * On the forward path that is permanent evidence ({@code isRetryLater == false}) → claim-first
 * compensation; every compensation dispatch then throws the identical exception at the identical
 * point → {@code hadPermanentFailure} → terminal {@code FAILED} with the undo never dispatched. An
 * in-flight saga whose id the bound refuses is terminally failed, on data the read path handles
 * fine.
 */
class SagaStoredControlCharacterCorrelationIdTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  /**
   * A saga id carrying CR/LF, as a {@code correlate()} implementation can derive it from event data
   * the ingress bound never saw. Written as an escape so this source file stays control-character
   * free.
   */
  private static final String CONTROL_CHAR_SAGA_ID =
      "fulfil-order" + (char) 0x0d + (char) 0x0a + "1";

  // =========================================================================
  // 1. End to end through the real step executor
  // =========================================================================

  @Test
  void aRunningSagaWithAStoredControlCharacterIdStillDispatchesItsForwardCommands() {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new OneStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of(CONTROL_CHAR_SAGA_ID);
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
            new SagaTrigger.ForwardStep(correlated(9L), false, false));

    assertThat(outcome).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).as("the forward command must reach the bus").isEqualTo(1);
    assertThat(bus.count("undo-1")).as("nothing failed, so nothing is undone").isZero();
    var row =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void aStoredControlCharacterIdIsNotItselfAPermanentFailureThatTerminalizesTheSaga() {
    // The failure shape stated as originally reported: pre-fix this saga ends FAILED with
    // bus.count("undo-1") == 0 — terminal, halted, and the refund never issued.
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new OneStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of(CONTROL_CHAR_SAGA_ID);
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);
    // A genuine business rejection of the forward command: the saga SHOULD compensate here, and
    // the compensation must actually be dispatched.
    bus.failAlways("step-1", () -> new org.streamrune.core.DomainException("insufficient funds"));

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
            new SagaTrigger.ForwardStep(correlated(11L), false, false));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.count("undo-1"))
        .as("the undo must be DISPATCHED, not swallowed by a correlation-id constraint")
        .isEqualTo(1);
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  // =========================================================================
  // 2. Dispatch-level pins
  // =========================================================================

  @Test
  void executeCorrelatedAcceptsAControlCharacterCorrelationIdAndBindsItVerbatim() {
    var captured = new java.util.concurrent.atomic.AtomicReference<CorrelationId>();
    CommandBus bus =
        new CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            StreamRuneContext.RequestContext ctx = StreamRuneContext.capture();
            captured.set(ctx != null ? ctx.correlationId() : null);
            return null;
          }
        };

    assertThatCode(
            () ->
                SagaCommandDispatch.executeCorrelated(
                    bus,
                    CorrelationId.of(CONTROL_CHAR_SAGA_ID),
                    new ReserveStock("step-1"),
                    IdempotencyKey.of("saga:control-char:1:0")))
        .doesNotThrowAnyException();
    assertThat(captured.get())
        .as("the stored id is bound verbatim — a rewritten id is a different id")
        .isEqualTo(CorrelationId.of(CONTROL_CHAR_SAGA_ID));
  }

  @Test
  void compensateAndClassifyRunsTheUndoForAControlCharacterCorrelationId() {
    var bus = new ScriptedBus();
    var outcome =
        SagaCommandDispatch.compensateAndClassify(
            List.of(SagaCommand.of(new ReleaseStock("undo-1"), AggregateId.of("a"))),
            bus,
            CorrelationId.of(CONTROL_CHAR_SAGA_ID),
            i -> IdempotencyKey.of("k:" + i),
            org.slf4j.LoggerFactory.getLogger("test"));

    assertThat(outcome).isEqualTo(SagaCommandDispatch.CompensationOutcome.COMPENSATED);
    assertThat(bus.count("undo-1")).isEqualTo(1);
  }

  @Test
  void aControlCharacterCorrelationIdSurvivesAnAmbientContextCarryingControlCharacterIdsToo() {
    // The saga's ambient context can itself be a reconstructed one (SagaDeadLetterReplayer drives
    // a saga from an operator request thread; a DLQ replay drives one under a context rebound from
    // stored columns). Copying trace/user/baggage out of it must not re-run an ingress check
    // either.
    var ambient =
        new StreamRuneContext.RequestContext(
            org.streamrune.core.types.TraceId.of("trace" + (char) 0x01),
            org.streamrune.core.types.UserId.of("user-1"),
            CorrelationId.of("outer" + (char) 0x01),
            Instant.now(),
            Map.of());
    var bus = new ScriptedBus();

    assertThatCode(
            () ->
                ScopedValue.where(StreamRuneContext.CURRENT, ambient)
                    .run(
                        () ->
                            SagaCommandDispatch.executeCorrelated(
                                bus,
                                CorrelationId.of(CONTROL_CHAR_SAGA_ID),
                                new ReserveStock("step-1"),
                                IdempotencyKey.of("saga:control-char:1:0"))))
        .doesNotThrowAnyException();
    assertThat(bus.count("step-1")).isEqualTo(1);
  }

  // =========================================================================
  // Fixtures
  // =========================================================================

  record OrderConfirmed(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record ReleaseStock(String orderId) implements Command {}

  record FulfilState(SagaStatus status, String orderId) implements SagaState {}

  /** One forward command ("step-1") and one undo ("undo-1"). */
  static final class OneStepOrchestrator implements SagaOrchestrator<FulfilState> {
    @Override
    public Class<FulfilState> stateType() {
      return FulfilState.class;
    }

    @Override
    public FulfilState initialState(SagaId sagaId) {
      return new FulfilState(SagaStatus.STARTED, "order-1");
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of(CONTROL_CHAR_SAGA_ID);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.of(SagaId.of(CONTROL_CHAR_SAGA_ID));
    }

    @Override
    public FulfilState evolve(FulfilState state, EventEnvelope event) {
      return new FulfilState(SagaStatus.RUNNING, "order-1");
    }

    @Override
    public List<SagaCommand> handle(FulfilState state, EventEnvelope event) {
      return List.of(SagaCommand.of(new ReserveStock("step-1"), AggregateId.of("a")));
    }

    @Override
    public List<SagaCommand> compensate(
        FulfilState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new ReleaseStock("undo-1"), AggregateId.of("a")));
    }
  }

  /** Counts executions per command id and can be scripted to fail one of them. */
  static final class ScriptedBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
    private final Map<String, java.util.function.Supplier<RuntimeException>> always =
        new ConcurrentHashMap<>();

    void failAlways(String cmdId, java.util.function.Supplier<RuntimeException> failure) {
      always.put(cmdId, failure);
    }

    int count(String cmdId) {
      return counts.getOrDefault(cmdId, new AtomicInteger()).get();
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (committed.contains(key)) {
        return ok();
      }
      String id =
          switch (command) {
            case ReserveStock r -> r.orderId();
            case ReleaseStock r -> r.orderId();
            default -> command.getClass().getSimpleName();
          };
      var repeated = always.get(id);
      if (repeated != null) {
        throw repeated.get();
      }
      counts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
      committed.add(key);
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  private static EventEnvelope correlated(long globalOffset) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("order-stream")),
        Version.initial(),
        new EventType("OrderConfirmed"),
        new OrderConfirmed("order-1"),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of(CONTROL_CHAR_SAGA_ID),
            null,
            null,
            Instant.now()));
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.RetryPolicy;
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
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * End-to-end: an interceptor-vetoed saga command must NOT be indistinguishable from success. A
 * veto-style {@link CommandInterceptor} whose {@code before()} returns {@code false} (maintenance
 * mode / kill switch / rate limiter — a documented first-class bus feature) makes the bus RETURN a
 * {@code VETOED} {@code CommandResult} without throwing. {@code
 * SagaCommandDispatch.executeCorrelated} used to discard that result, so:
 *
 * <ul>
 *   <li><b>compensation path</b> — a vetoed refund returned "normally", {@code
 *       compensateAndClassify} saw zero failures, and the saga terminal-wrote {@code COMPENSATED}
 *       while the refund handler NEVER RAN: no inbox row, no DLQ entry, and the terminal guard
 *       forever prevents a re-drive — silent money loss;
 *   <li><b>forward path</b> — a vetoed forward command returned from the dispatch loop as success
 *       and the saga persisted {@code FORWARD_PROGRESSED} with the side effect never executed.
 * </ul>
 *
 * <p>The fix (mirroring the dead-letter replay path): {@code executeCorrelated} inspects the result
 * and throws {@link SagaCommandVetoedException}. A veto is deliberately NOT poison: the triggering
 * event is fine — the COMMAND was refused — so nothing is quarantined.
 *
 * <p><b>What the veto then MEANS.</b> it was originally binned as a permanent business rejection (a
 * {@code DomainException}), which contradicted the circuit breaker's ruling that a VETOED callback
 * "carries no evidence" for the circuit breaker, and contradicted the retry-later classification of
 * {@code CircuitBreakerOpenException} — the framework's identical admission gate, expressed by
 * throwing — as retry-later. The cost was concrete: one load-shedding window terminally FAILED
 * every saga that received an event during it, with forward side effects applied and the undo
 * (vetoed by the same gate) never run. A veto asserts nothing, so every consumer abstains: the
 * forward path leaves the saga untouched and propagates for redelivery, and the compensation path
 * keeps the episode {@code COMPENSATING} for the resume paths. The original protection is intact —
 * a veto is still never {@code COMPENSATED} and never forward progress.
 *
 * <p>SAGA_OWNED semantics are unchanged: user interceptors still run on saga commands (only the two
 * authorization interceptors honor the saga-system principal); the fix makes the veto VISIBLE, it
 * does not bypass interceptors.
 */
class SagaCommandVetoTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  // ---- domain ----
  record Start(String id) implements DomainEvent {}

  record ShipOrder(String orderId) implements Command {}

  record RefundPayment(String orderId) implements Command {}

  record OrderShipped(String orderId) implements DomainEvent {}

  record PaymentRefunded(String orderId) implements DomainEvent {}

  record ShipState() implements AggregateState {}

  record RefundState() implements AggregateState {}

  static final class ShipDecider implements Decider<ShipOrder, ShipState, OrderShipped> {
    volatile boolean reject; // deterministic forward rejection → enters compensation
    final AtomicInteger decideCalls = new AtomicInteger();

    @Override
    public ShipState initialState() {
      return new ShipState();
    }

    @Override
    public List<OrderShipped> decide(ShipOrder command, ShipState state) {
      if (reject) {
        throw new DomainException("out of stock — deterministic rejection");
      }
      decideCalls.incrementAndGet();
      return List.of(new OrderShipped(command.orderId()));
    }

    @Override
    public ShipState evolve(ShipState state, OrderShipped event) {
      return state;
    }
  }

  static final class RefundDecider implements Decider<RefundPayment, RefundState, PaymentRefunded> {
    final AtomicInteger decideCalls = new AtomicInteger();

    @Override
    public RefundState initialState() {
      return new RefundState();
    }

    @Override
    public List<PaymentRefunded> decide(RefundPayment command, RefundState state) {
      decideCalls.incrementAndGet();
      return List.of(new PaymentRefunded(command.orderId()));
    }

    @Override
    public RefundState evolve(RefundState state, PaymentRefunded event) {
      return state;
    }
  }

  /**
   * A maintenance-mode-style veto interceptor: {@code before()} returns {@code false} (no throw)
   * for the configured command types, which makes the bus return a {@code VETOED} result.
   */
  static final class MaintenanceModeInterceptor implements CommandInterceptor {
    final Set<Class<?>> vetoedTypes = ConcurrentHashMap.newKeySet();

    @Override
    public boolean before(CommandContext ctx) {
      return !vetoedTypes.contains(ctx.command().getClass());
    }
  }

  record FakeSagaState(SagaStatus status, String id) implements SagaState {}

  /** Forward: ship the order. Compensation (pure function of state): refund the payment. */
  static final class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {
    @Override
    public Class<FakeSagaState> stateType() {
      return FakeSagaState.class;
    }

    @Override
    public FakeSagaState initialState(SagaId sagaId) {
      return new FakeSagaState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Start;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((Start) event.event()).id());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
      if (event.event() instanceof Start s) {
        return new FakeSagaState(SagaStatus.RUNNING, "saga-" + s.id());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
      if (state.status() == SagaStatus.RUNNING && event.event() instanceof Start) {
        return List.of(SagaCommand.of(new ShipOrder(state.id()), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new RefundPayment(state.id()), AggregateId.of(state.id())));
    }
  }

  /** Records compensation outcomes so the operator-visible signal can be asserted. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final Map<String, AtomicInteger> compensationOutcomes = new ConcurrentHashMap<>();

    @Override
    public void recordSagaCompensation(String sagaType, String outcome) {
      compensationOutcomes.computeIfAbsent(outcome, k -> new AtomicInteger()).incrementAndGet();
    }
  }

  private EventEnvelope startEnvelope(String id, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Start"),
        new Start(id),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("unrelated-correlation"),
            null,
            null,
            Instant.now()));
  }

  InMemorySagaStore sagaStore;
  InMemorySagaDeadLetterStore deadLetterStore;
  MaintenanceModeInterceptor interceptor;
  ShipDecider shipDecider;
  RefundDecider refundDecider;
  RecordingMetrics metrics;
  SagaRunner<FakeSagaState> runner;
  SagaId sagaId;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    deadLetterStore = new InMemorySagaDeadLetterStore(sagaStore);
    interceptor = new MaintenanceModeInterceptor();
    shipDecider = new ShipDecider();
    refundDecider = new RefundDecider();
    metrics = new RecordingMetrics();
    // A REAL bus with a REAL veto interceptor: before() → false produces the exact VETOED
    // CommandResult shape the production bus returns (no stub drift).
    var inbox = new InMemoryCommandInbox();
    EventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .interceptors(interceptor)
            .register(TYPE, ShipOrder.class, cmd -> AggregateId.of(cmd.orderId()), shipDecider)
            .register(
                TYPE, RefundPayment.class, cmd -> AggregateId.of(cmd.orderId()), refundDecider)
            .build();
    runner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetterStore)
            .metrics(metrics)
            .build();
    sagaId = SagaId.of("saga-x");
  }

  @Test
  void vetoedCompensation_staysCompensatingForRedrive_neverReportsCompensated() {
    // The audited money-loss shape: the forward command is rejected deterministically (business
    // rejection → claim-first compensation), and maintenance mode vetoes the refund. The veto
    // returns a normal VETOED CommandResult — before the fix, compensateAndClassify counted zero
    // failures and the saga terminal-wrote COMPENSATED while the refund handler never ran.
    //
    // The veto is still never a success, but it is not a deterministic rejection
    // either — it is EVIDENCE-FREE. Terminalizing FAILED on it abandoned the undo permanently for a
    // maintenance window that lifts in ten minutes. The episode must stay COMPENSATING so the
    // resume paths re-drive it (bounded by SagaCompensationRetrySweeper's age-based give-up →
    // FAULTED, exactly like every other transient compensation failure).
    shipDecider.reject = true;
    interceptor.vetoedTypes.add(RefundPayment.class);

    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    assertThat(refundDecider.decideCalls.get())
        .as("the vetoed refund handler must never have run")
        .isZero();
    var saga =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saga.status())
        .as(
            "a vetoed compensation must leave the episode COMPENSATING for the resume paths — NOT"
                + " COMPENSATED (silent money loss) and NOT terminal FAILED (the undo abandoned for"
                + " a gate that lifts)")
        .isEqualTo(SagaStatus.COMPENSATING);
    // Operator-visible: the RETRY-class compensation outcome metric fires (vs a lying
    // COMPENSATED). a veto is an ADMISSION refusal, so the outcome is RETRY_REFUSED —
    // the episode stays COMPENSATING exactly like RETRY, but the sweeper does not count the
    // refused re-drive toward its give-up horizon.
    assertThat(
            metrics.compensationOutcomes.getOrDefault("RETRY_REFUSED", new AtomicInteger()).get())
        .isEqualTo(1);
    assertThat(metrics.compensationOutcomes).doesNotContainKey("COMPENSATED");
    assertThat(metrics.compensationOutcomes).doesNotContainKey("FAILED");
    // A veto is NOT poison: the triggering event is fine (the COMMAND was refused), so nothing is
    // quarantined.
    assertThat(deadLetterStore.findBySaga(sagaId))
        .as("a veto must take the rejection path, not quarantine")
        .isEmpty();
  }

  @Test
  void vetoedCompensation_redrivenAfterTheGateLifts_completesTheUndo() {
    // Convergence proof for the branch above: the episode left COMPENSATING is re-driven under the
    // SAME episode-scoped key once the window closes, and the refund actually runs.
    shipDecider.reject = true;
    interceptor.vetoedTypes.add(RefundPayment.class);
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    interceptor.vetoedTypes.clear(); // maintenance window over
    runner.sweepCompensation(sagaId, false);

    assertThat(refundDecider.decideCalls.get())
        .as("the undo runs as soon as the gate lifts — nothing was abandoned")
        .isEqualTo(1);
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void vetoedForwardCommand_leavesTheSagaUntouchedAndPropagatesForRedelivery() {
    // Mirror defect on the forward path: before the fix, a vetoed forward command returned from the
    // dispatch loop as success and the saga persisted RUNNING (FORWARD_PROGRESSED) with the ship
    // side effect never executed.
    //
    // It must not be read as a business rejection either. The classification already routes
    // the framework's OWN admission gate (CircuitBreakerOpenException, an interceptor before() that
    // refuses by throwing) to retry-later; a user interceptor that refuses by returning false is
    // the same event and gets the same answer. Otherwise one load-shedding window terminally FAILs
    // every in-flight saga, with forward side effects applied and the undo — vetoed by the same
    // gate — never run.
    interceptor.vetoedTypes.add(ShipOrder.class);

    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L))))
        .as("the veto propagates so the subscription redelivers instead of checkpointing past it")
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("VETOED");

    assertThat(shipDecider.decideCalls.get())
        .as("the vetoed forward handler must never have run")
        .isZero();
    // A retry-later on a FRESH start event is
    // durably anchored because the executor wrote the genesis-pending row (RUNNING, version 1,
    // genesis_applied = FALSE) BEFORE the first dispatch, so the saga is visible to
    // SagaTimeoutRunner's bounded escape while the gate stays closed; the failure leaves it
    // untouched. No compensation claim, no quarantine — and the redelivery re-drives the row (see
    // the convergence test below).
    var escapeRow =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(escapeRow.status())
        .as("the genesis-pending row is RUNNING — never a compensation claim")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(escapeRow.version()).isEqualTo(1L);
    assertThat(escapeRow.genesisApplied())
        .as("the recorded genesis-pending flag marks the dispatch as incomplete")
        .isFalse();
    assertThat(refundDecider.decideCalls.get())
        .as("a healthy business flow must not be undone by a gate that refused to admit it")
        .isZero();
    assertThat(metrics.compensationOutcomes).isEmpty();
    assertThat(deadLetterStore.findBySaga(sagaId)).isEmpty();
  }

  @Test
  void vetoedForwardCommand_redeliveredAfterTheGateLifts_progressesNormally() {
    // Convergence proof: the redelivery the propagate branch buys actually converges.
    interceptor.vetoedTypes.add(ShipOrder.class);
    assertThatThrownBy(() -> runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L))))
        .isInstanceOf(RuntimeException.class);

    interceptor.vetoedTypes.clear(); // maintenance window over
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    assertThat(shipDecider.decideCalls.get()).isEqualTo(1);
    var saga =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saga.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(refundDecider.decideCalls.get()).isZero();
  }

  @Test
  void noVeto_forwardPathUnchanged() {
    // Regression pin: with the interceptor present but not vetoing, the forward path is untouched.
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    assertThat(shipDecider.decideCalls.get()).isEqualTo(1);
    var saga =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(saga.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(refundDecider.decideCalls.get()).isZero();
  }
}

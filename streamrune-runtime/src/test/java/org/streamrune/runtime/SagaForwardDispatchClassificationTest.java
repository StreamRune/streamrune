package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonMappingException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandBusClosedException;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
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
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A saga's FORWARD command dispatch must classify its failure transient-vs-permanent before
 * deciding to compensate, exactly as the sibling COMPENSATION dispatch one method away already does
 * ({@link SagaCommandDispatch#compensateAndClassify}).
 *
 * <p>Pre-fix, {@code SagaStepExecutor.forwardStep}'s dispatch loop caught bare {@code Exception}
 * and went straight to claim-first compensation. The reliable trigger is the framework's OWN
 * default {@link CircuitBreakerCommandInterceptor} bean (Spring/Quarkus/Micronaut all register one
 * unless the application replaces it): a {@link CircuitBreakerOpenException} means the command was
 * never even attempted — a pure "retry later" signal — yet it reached the blanket catch and made a
 * healthy business flow issue its undo (a refund) and terminalize, silently: no command-DLQ entry
 * (the breaker throws from {@code before()}, before the bus's DLQ publish, and saga-owned dispatch
 * suppresses it anyway), no saga dead-letter entry, and a {@code recordSagaCompensation(...,
 * "COMPENSATED")} indistinguishable from a legitimate business compensation.
 */
class SagaForwardDispatchClassificationTest {

  private static final AggregateType TYPE = AggregateType.of("inventory");

  // =========================================================================
  // 1. The reliable trigger, end to end: real bus, real breaker, real inbox
  // =========================================================================

  /**
   * The repro. A real {@link VirtualThreadCommandBus} carrying the framework's default {@link
   * CircuitBreakerCommandInterceptor}; the breaker is tripped by a genuine infrastructure failure,
   * then a live correlated event drives the saga's forward command straight into the OPEN circuit.
   *
   * <p>The compensation dispatch that follows is deliberately allowed to SUCCEED: the orchestrator
   * advances the breaker's monotonic clock past its cooldown from inside {@code compensate()},
   * modelling the overwhelmingly likely interleaving — the outage ends between the forward
   * rejection and the undo, so the probe closes the circuit and the refund lands. That is what
   * makes the pre-fix outcome IRREVERSIBLE: {@code COMPENSATED} is both terminal and halted, so no
   * later correlated event, timeout sweep or compensation sweep ever revisits the saga.
   */
  @Test
  void forwardCommandRejectedByAnOpenCircuitBreaker_neverCompensatesTheSaga() {
    var nanos = new AtomicLong(0L);
    var breaker =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            nanos::get);

    var inbox = new InMemoryCommandInbox();
    var eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    var sagaStore = new InMemorySagaStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(inbox)
            .interceptors(breaker)
            .register(
                TYPE, ReserveStock.class, c -> AggregateId.of(c.orderId()), new StockDecider())
            .register(
                TYPE, ReleaseStock.class, c -> AggregateId.of(c.orderId()), new ReleaseDecider())
            .build();

    // Trip the breaker for real, through the bus, with a genuine infra failure on an unrelated
    // aggregate (INFRASTRUCTURE_FAILURES counts it; threshold is 1 → OPEN).
    Throwable trip =
        catchThrowable(() -> bus.execute(new ReserveStock("trip-me"), IdempotencyKey.of("trip:0")));
    assertThat(trip).isInstanceOf(EventStoreException.class);
    assertThat(breaker.circuitState()).isEqualTo("OPEN");

    // 1s into a 10s cooldown: the circuit is still OPEN and rejects everything.
    nanos.addAndGet(Duration.ofSeconds(1).toNanos());

    var sagaId = SagaId.of("fulfil-order-1");
    sagaStore.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);

    var runner =
        SagaRunner.<FulfilState>builder()
            .orchestrator(
                new FulfilOrchestrator(() -> nanos.addAndGet(Duration.ofSeconds(30).toNanos())))
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    Throwable propagated =
        catchThrowable(
            () -> runner.asEventListener().onEvents(List.of(correlated("order-1", 42L))));

    assertThat(propagated)
        .as(
            "An OPEN circuit breaker means the forward command was NEVER attempted — a"
                + " pure retry-later signal. It must propagate so the subscription redelivers the"
                + " event (the already-dispatched forward keys dedup), NOT drive a claim-first"
                + " compensation.")
        .isInstanceOf(CircuitBreakerOpenException.class);

    var row =
        sagaStore
            .load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class)
            .orElseThrow();
    assertThat(row.status())
        .as("the saga must still be RUNNING — nothing durable was written")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).as("no version bump: no claim, no terminal write").isEqualTo(1L);
    assertThat(
            eventStore.readStream(
                StreamId.of(TYPE, AggregateId.of("order-1")), Version.initial(), 100))
        .as("neither the forward command nor the undo may have produced events")
        .isEmpty();
  }

  // =========================================================================
  // 2. Executor-level classification pins
  // =========================================================================

  /** Retry-later: the breaker rejected the command outright. */
  @Test
  void openCircuitBreaker_propagates_leavingTheSagaRunningAtItsLoadedVersion() {
    assertForwardFailurePropagates(() -> new CircuitBreakerOpenException("circuit is OPEN"));
  }

  /**
   * The finding's original scenario: the connection pool is momentarily exhausted, so the append
   * fails with an {@code EventStoreException} wrapping a {@code SQLException}. JDBC evidence is the
   * same fail-safe transient marker {@link SagaStateConversion} and {@code ReadPoisonClassifier}
   * already key on.
   */
  @Test
  void poolExhaustion_eventStoreExceptionOverSqlException_propagates() {
    assertForwardFailurePropagates(
        () ->
            new EventStoreException(
                "append failed", new SQLTransientConnectionException("pool exhausted")));
  }

  /**
   * A lock that could not be acquired: the command never ran. The bus itself calls it transient.
   */
  @Test
  void lockNotAcquired_propagates() {
    assertForwardFailurePropagates(
        () ->
            new LockAcquisitionException(StreamId.of(TYPE, AggregateId.of("agg")), "lock timeout"));
  }

  /**
   * The bus is shutting down — the command was refused ADMISSION, exactly like an open breaker.
   * Nothing was attempted, so undoing the business flow on it turned every graceful node shutdown
   * into a compensation storm for perfectly healthy sagas. Propagate for redelivery; the restarted
   * node (or a peer) re-drives under the same deterministic forward keys.
   */
  @Test
  void closedBus_forwardStep_propagatesForRedelivery_neverCompensates() {
    assertForwardFailurePropagates(
        () -> new CommandBusClosedException("Command bus is closed, cannot accept new commands"));
  }

  /**
   * A business rejection keeps the documented contract: claim COMPENSATING first, then compensate.
   * This is the ONE meaning of "a forward command failed" for which undoing the flow is correct.
   */
  @Test
  void domainRejection_stillClaimsFirstAndCompensates() {
    assertForwardFailureCompensates(() -> new DomainException("insufficient funds"));
  }

  /**
   * The trap the poison handling exists to prevent, pinned from the other side. {@code
   * DEFAULT_DLQ_ELIGIBLE} calls a bare {@code RuntimeException} (a buggy command handler NPE'ing,
   * say) transient, but it is deterministic: propagating it would retry the identical batch forever
   * and wedge every listener on the subscription, with nothing dead-lettered. Unprovable failures
   * therefore keep the pre-fix outcome — compensate and terminalize — which is halted, durable and
   * operator-visible.
   */
  @Test
  void unprovableRuntimeFailure_stillCompensates_soTheSubscriptionIsNeverWedged() {
    assertForwardFailureCompensates(() -> new IllegalStateException("handler bug"));
  }

  /**
   * Same trap, its most concrete instance: an {@code EventStoreException} wrapping a Jackson
   * mapping defect. Deterministic, so it must NOT be routed back as a retry.
   */
  @Test
  void jacksonMappingDefect_stillCompensates_soTheSubscriptionIsNeverWedged() {
    assertForwardFailureCompensates(
        () ->
            new EventStoreException(
                "serialize failed",
                JsonMappingException.from(
                    (com.fasterxml.jackson.core.JsonParser) null, "no serializer")));
  }

  /**
   * A forward target longer than the {@code VARCHAR(255)} stream columns. {@code AggregateId.of} in
   * the bus's id extractor refuses it with an {@code IllegalArgumentException}, a permanent
   * rejection, so the saga compensates. Without that bound the same target reached the PostgreSQL
   * append and failed with SQLState 22001 (value too long), which is JDBC evidence and therefore
   * retry-later: the triggering event was redelivered without end and never compensated. The
   * refusal is deterministic and nothing is written before it, so every redelivery reaches the same
   * verdict and converges on the one claim-first compensation.
   */
  @Test
  void overLengthForwardTarget_refusedAtTheIngressFactory_compensates() {
    assertForwardFailureCompensates(() -> ingressRefusal("x".repeat(256)));

    assertThat(
            SagaCommandDispatch.isRetryLater(
                new EventStoreException(
                    "append failed",
                    new java.sql.SQLException(
                        "value too long for type character varying(255)", "22001"))))
        .as("the column overflow the ingress bound pre-empts was retry-later")
        .isTrue();
  }

  private static IllegalArgumentException ingressRefusal(String value) {
    try {
      AggregateId.of(value);
    } catch (IllegalArgumentException e) {
      return e;
    }
    throw new AssertionError("AggregateId.of accepted a " + value.length() + "-unit id");
  }

  // =========================================================================
  // 3. Rerun convergence after a propagated transient failure
  // =========================================================================

  /**
   * The propagate branch is only correct because redelivery converges: {@code forwardKey} is {@code
   * saga:<id>:<triggerOffset>:<index>} and nothing durable was written, so the rerun re-evolves the
   * SAME row to the SAME command list, index 0 dedups in the command inbox, and only the failed
   * index re-runs.
   */
  @Test
  void afterAPropagatedTransientFailure_redeliveryDedupsIndexZeroAndConverges() {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new TwoStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);
    bus.failOnce("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));

    var event = correlated("order-1", 7L);
    Throwable first =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
                    new SagaTrigger.ForwardStep(event, false, false)));
    assertThat(first).isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isZero();
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class)
                .orElseThrow()
                .version())
        .isEqualTo(1L);

    // Redelivery of the SAME event (same globalOffset ⇒ same forward keys).
    StepOutcome second =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
            new SagaTrigger.ForwardStep(event, false, false));

    assertThat(second).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).as("index 0 dedups on its stable forward key").isEqualTo(1);
    assertThat(bus.count("step-2")).as("only the failed index re-runs").isEqualTo(1);
    var row =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).isEqualTo(2L);
  }

  /**
   * Crash-point convergence, end to end on a REAL closed bus. Crash points of a shutdown
   * mid-forward-dispatch: (a) before any dispatch — nothing durable exists, redelivery re-runs
   * everything; (b) after index 0 committed, shutdown rejects index 1 — this test. In both, the
   * closed-bus rejection happens at ADMISSION (the drain lets already-admitted commands finish), so
   * no partial side effect exists for the rejected index, nothing durable was written for this
   * delivery (no claim, no version bump), and the rerun re-derives the SAME forward keys: committed
   * indices dedup in the command inbox, only the rejected one re-runs. A compensation claim is
   * never written — the exact opposite of the pre-fix behaviour, where shutdown triggered
   * claim-first compensation.
   */
  @Test
  void shutdownMidForwardDispatch_redeliveryAfterRestartConverges_noCompensationClaim() {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new TwoStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);
    bus.failOnce(
        "step-2",
        () -> new CommandBusClosedException("Command bus is closed, cannot accept new commands"));

    var event = correlated("order-1", 7L);
    Throwable duringShutdown =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
                    new SagaTrigger.ForwardStep(event, false, false)));
    assertThat(duringShutdown).isInstanceOf(CommandBusClosedException.class);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isZero();
    assertThat(bus.count("undo-1")).as("shutdown must never claim a compensation").isZero();
    var rowAfterShutdown =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(rowAfterShutdown.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(rowAfterShutdown.version()).as("nothing durable was written").isEqualTo(1L);

    // "Restart": the same event is redelivered (same globalOffset ⇒ same forward keys).
    StepOutcome afterRestart =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
            new SagaTrigger.ForwardStep(event, false, false));

    assertThat(afterRestart).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).as("index 0 dedups on its stable forward key").isEqualTo(1);
    assertThat(bus.count("step-2")).as("only the rejected index re-runs").isEqualTo(1);
    var row =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).isEqualTo(2L);
  }

  /**
   * Start path: the transient failure still never claims a compensation (the retry-later half), but
   * it is no longer allowed to leave NOTHING durable — without a row, {@code
   * SagaTimeoutRunner.SELECT_TIMED_OUT} could never see the saga, so the documented "the outage is
   * bounded by the saga's own timeout()" escape was false on exactly the start path. the executor
   * writes the genesis-pending row (RUNNING, version 1, {@code genesis_applied = FALSE}) BEFORE the
   * first dispatch, and the retry-later failure leaves it untouched — see {@code
   * SagaStartEventRetryLaterDurabilityTest} for the full redelivery-convergence suite.
   */
  @Test
  void startPath_transientForwardFailure_writesTheDurableEscapeRow_neverACompensationClaim() {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new TwoStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failOnce("step-1", () -> new CircuitBreakerOpenException("circuit is OPEN"));

    Throwable thrown =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    Optional.empty(),
                    new SagaTrigger.ForwardStep(correlated("order-1", 3L), true, false)));

    assertThat(thrown).isInstanceOf(CircuitBreakerOpenException.class);
    var row =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(row.status())
        .as("the genesis-pending row is RUNNING — never a compensation claim, never a quarantine")
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.genesisApplied())
        .as("the recorded genesis-pending flag licenses the redelivery re-drive")
        .isFalse();
    assertThat(bus.count("undo-1")).as("retry-later never compensates").isZero();
  }

  // =========================================================================
  // 4. Sibling-path symmetry
  // =========================================================================

  /**
   * The two dispatch paths now reach the same verdict for the same signal: an OPEN breaker is
   * retry-later on the forward path and RETRY (episode kept {@code COMPENSATING}) on the
   * compensation path.
   */
  @Test
  void compensationPath_classifiesTheOpenBreakerAsRetryToo() {
    var bus = new ScriptedBus();
    bus.failAlways("undo-1", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var outcome =
        SagaCommandDispatch.compensateAndClassify(
            List.of(SagaCommand.of(new ReleaseStock("undo-1"), AggregateId.of("a"))),
            bus,
            CorrelationId.of("saga-1"),
            i -> IdempotencyKey.of("k:" + i),
            org.slf4j.LoggerFactory.getLogger("test"));
    // An open breaker REFUSED ADMISSION — RETRY-class (the episode stays COMPENSATING),
    // but surfaced distinctly so the sweeper's give-up bookkeeping does not count it as an attempt.
    assertThat(outcome).isEqualTo(SagaCommandDispatch.CompensationOutcome.RETRY_REFUSED);
    assertThat(outcome.isTerminal()).isFalse();
    assertThat(SagaCommandDispatch.isRetryLater(new CircuitBreakerOpenException("open"))).isTrue();
  }

  /**
   * The split itself: the SAME closed-bus rejection used to be NOT-retry-later on the forward path
   * (→ claim-first compensation — shutdown triggered undo) yet RETRY on the compensation path. Both
   * paths must read a shutdown as "later": the forward path propagates for redelivery, and the
   * compensation path keeps the episode {@code COMPENSATING} for the resume paths (a node going
   * away must never terminalize an undo).
   */
  @Test
  void closedBus_bothSagaPathsClassifyShutdownAsRetryLater() {
    var bus = new ScriptedBus();
    bus.failAlways(
        "undo-1",
        () -> new CommandBusClosedException("Command bus is closed, cannot accept new commands"));
    var outcome =
        SagaCommandDispatch.compensateAndClassify(
            List.of(SagaCommand.of(new ReleaseStock("undo-1"), AggregateId.of("a"))),
            bus,
            CorrelationId.of("saga-1"),
            i -> IdempotencyKey.of("k:" + i),
            org.slf4j.LoggerFactory.getLogger("test"));
    // A closing bus REFUSED ADMISSION — RETRY-class, surfaced distinctly (see above).
    assertThat(outcome).isEqualTo(SagaCommandDispatch.CompensationOutcome.RETRY_REFUSED);
    assertThat(outcome.isTerminal()).isFalse();

    assertThat(SagaCommandDispatch.isRetryLater(new CommandBusClosedException("closed"))).isTrue();
    assertThat(
            SagaCommandDispatch.isRetryLater(
                new RuntimeException("wrapped", new CommandBusClosedException("closed"))))
        .as("the marker is recognized anywhere in the chain, like every sibling marker")
        .isTrue();
    // The OTHER IllegalStateException the bus throws — a missing CommandInbox — is genuine
    // misconfiguration and deliberately keeps the compensate contract.
    assertThat(SagaCommandDispatch.isRetryLater(new IllegalStateException("no CommandInbox")))
        .isFalse();
  }

  /** Every marker the forward classifier keys on, and the unprovable default. */
  @Test
  void isRetryLater_provesOnlyTheDirectionThatChangesBehaviour() {
    assertThat(SagaCommandDispatch.isRetryLater(new CircuitBreakerOpenException("open"))).isTrue();
    assertThat(SagaCommandDispatch.isRetryLater(new LockException("busy"))).isTrue();
    assertThat(SagaCommandDispatch.isRetryLater(new OptimisticLockException("s", 1, 2))).isTrue();
    assertThat(
            SagaCommandDispatch.isRetryLater(
                new EventStoreException("x", new SQLTransientConnectionException("pool"))))
        .isTrue();
    assertThat(SagaCommandDispatch.isRetryLater(new CryptoOperationException("vault down")))
        .isTrue();

    // Permanent / unprovable ⇒ the documented compensate contract, unchanged.
    assertThat(SagaCommandDispatch.isRetryLater(new IllegalStateException("handler bug")))
        .isFalse();
    assertThat(SagaCommandDispatch.isRetryLater(new IllegalArgumentException("bad"))).isFalse();
    assertThat(SagaCommandDispatch.isRetryLater(new DomainException("nope"))).isFalse();
    assertThat(SagaCommandDispatch.isRetryLater(new CryptoMappingException("shape"))).isFalse();
    assertThat(
            SagaCommandDispatch.isRetryLater(
                new EventStoreException("x", new SubjectForgottenException("subject erased"))))
        .as(
            "a crypto-shredded subject is permanent even though the chain looks like"
                + " infrastructure")
        .isFalse();
    assertThat(
            SagaCommandDispatch.isRetryLater(
                new EventStoreException("x", new DomainException("rejected"))))
        .as("permanent evidence anywhere in the chain outranks every retry-later marker")
        .isFalse();
  }

  /**
   * One meaning for "an interceptor refused to admit this command", whichever way the interceptor
   * expresses it.
   *
   * <p>A VETO "carries no evidence": nothing executed, no infrastructure was touched, so the
   * circuit breaker neither closes nor resets on it. The retry-later classification then pinned the
   * same signal as a PERMANENT business rejection on the saga forward path — while classifying
   * {@link CircuitBreakerOpenException}, which is the framework's own interceptor refusing in
   * {@code before()} by throwing, as retry-later. Two spellings of one event, opposite verdicts:
   * one load-shedding window terminally FAILED every in-flight saga, forward side effects applied
   * and the undo (vetoed by the same gate) never run.
   *
   * <p>An interceptor veto asserts nothing about the business or the infrastructure. Every consumer
   * must decline to conclude from it: the breaker abstains (unchanged), and the saga's abstain is
   * "leave the saga exactly as it was and do not advance" — i.e. retry-later.
   */
  @Test
  void isRetryLater_treatsAnInterceptorVetoLikeTheFrameworksOwnAdmissionGate() {
    assertThat(SagaCommandDispatch.isRetryLater(new SagaCommandVetoedException("gate", "Cmd")))
        .as("a veto refuses to ADMIT the command; it is not a decision about the business")
        .isTrue();
    assertThat(SagaCommandDispatch.isRetryLater(new CircuitBreakerOpenException("open")))
        .as("the framework's own admission gate — the same event, expressed by throwing")
        .isTrue();
  }

  /**
   * The taxonomy must hold in {@code CommandFailureClassification} too, not only in the saga's own
   * classifier: leaving the veto inside the permanent-rejection set would re-introduce this defect
   * in the next consumer that asks the shared question.
   */
  @Test
  void aVetoIsNotAPermanentRejection() {
    assertThat(
            org.streamrune.core.CommandFailureClassification.isPermanentRejection(
                new SagaCommandVetoedException("gate", "Cmd")))
        .as("nothing was executed, so the domain gave no final answer to record")
        .isFalse();
    assertThat(
            VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE.test(
                new SagaCommandVetoedException("gate", "Cmd")))
        .as("which makes compensateAndClassify bin it RETRY, not terminal FAILED")
        .isTrue();
  }

  /** A cyclic cause chain must not spin the classifier. */
  @Test
  void isRetryLater_toleratesACyclicCauseChain() {
    var a = new RuntimeException("a");
    var b = new RuntimeException("b", a);
    a.initCause(b);
    assertThatCode(() -> SagaCommandDispatch.isRetryLater(a)).doesNotThrowAnyException();
  }

  // =========================================================================
  // Shared assertions
  // =========================================================================

  private void assertForwardFailurePropagates(Supplier<RuntimeException> failure) {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new TwoStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);
    bus.failAlways("step-2", failure);

    Throwable thrown =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
                    new SagaTrigger.ForwardStep(correlated("order-1", 9L), false, false)));

    assertThat(thrown).isSameAs(bus.lastThrown());
    var row =
        store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version()).isEqualTo(1L);
    assertThat(bus.count("undo-1")).as("no compensation may be dispatched").isZero();
  }

  private void assertForwardFailureCompensates(Supplier<RuntimeException> failure) {
    var store = new InMemorySagaStore();
    var bus = new ScriptedBus();
    var exec =
        new SagaStepExecutor<>(new TwoStepOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    store.create(
        sagaId,
        SagaType.fromClass(FulfilState.class),
        new FulfilState(SagaStatus.RUNNING, "order-1"),
        SagaStatus.RUNNING);
    bus.failAlways("step-2", failure);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            store.load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class),
            new SagaTrigger.ForwardStep(correlated("order-1", 9L), false, false));

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(
            store
                .load(sagaId, SagaType.fromClass(FulfilState.class), FulfilState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
    assertThat(bus.count("undo-1")).isEqualTo(1);
  }

  // =========================================================================
  // Fixtures
  // =========================================================================

  record OrderConfirmed(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record ReleaseStock(String orderId) implements Command {}

  record StockReserved(String orderId) implements DomainEvent {}

  record StockReleased(String orderId) implements DomainEvent {}

  record StockState(int reserved) implements AggregateState {}

  /** Fails the FIRST command for "trip-me" so the breaker opens through a genuine bus failure. */
  static final class StockDecider implements Decider<ReserveStock, StockState, StockReserved> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<StockReserved> decide(ReserveStock command, StockState state) {
      if ("trip-me".equals(command.orderId())) {
        throw new EventStoreException("connection reset by peer");
      }
      return List.of(new StockReserved(command.orderId()));
    }

    @Override
    public StockState evolve(StockState state, StockReserved event) {
      return new StockState(state.reserved() + 1);
    }
  }

  static final class ReleaseDecider implements Decider<ReleaseStock, StockState, StockReleased> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<StockReleased> decide(ReleaseStock command, StockState state) {
      return List.of(new StockReleased(command.orderId()));
    }

    @Override
    public StockState evolve(StockState state, StockReleased event) {
      return new StockState(state.reserved() - 1);
    }
  }

  record FulfilState(SagaStatus status, String orderId) implements SagaState {}

  /** One forward command, one undo. {@code compensate} runs {@code onCompensate} first. */
  static final class FulfilOrchestrator implements SagaOrchestrator<FulfilState> {
    private final Runnable onCompensate;

    FulfilOrchestrator(Runnable onCompensate) {
      this.onCompensate = onCompensate;
    }

    @Override
    public Class<FulfilState> stateType() {
      return FulfilState.class;
    }

    @Override
    public FulfilState initialState(SagaId sagaId) {
      return new FulfilState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return event.event() instanceof OrderConfirmed oc
          ? Optional.of(SagaId.of("fulfil-" + oc.orderId()))
          : Optional.empty();
    }

    @Override
    public FulfilState evolve(FulfilState state, EventEnvelope event) {
      return event.event() instanceof OrderConfirmed oc
          ? new FulfilState(SagaStatus.RUNNING, oc.orderId())
          : state;
    }

    @Override
    public List<SagaCommand> handle(FulfilState state, EventEnvelope event) {
      return event.event() instanceof OrderConfirmed oc
          ? List.of(SagaCommand.of(new ReserveStock(oc.orderId()), AggregateId.of(oc.orderId())))
          : List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FulfilState state, Throwable failure, SagaCommand failedCommand) {
      onCompensate.run();
      return List.of(
          SagaCommand.of(new ReleaseStock(state.orderId()), AggregateId.of(state.orderId())));
    }
  }

  /** Two forward commands ("step-1", "step-2") and one undo ("undo-1"). */
  static final class TwoStepOrchestrator implements SagaOrchestrator<FulfilState> {
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
      return SagaId.of("saga-1");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.of(SagaId.of("saga-1"));
    }

    @Override
    public FulfilState evolve(FulfilState state, EventEnvelope event) {
      return new FulfilState(SagaStatus.RUNNING, "order-1");
    }

    @Override
    public List<SagaCommand> handle(FulfilState state, EventEnvelope event) {
      return List.of(
          SagaCommand.of(new ReserveStock("step-1"), AggregateId.of("a")),
          SagaCommand.of(new ReserveStock("step-2"), AggregateId.of("b")));
    }

    @Override
    public List<SagaCommand> compensate(
        FulfilState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new ReleaseStock("undo-1"), AggregateId.of("a")));
    }
  }

  /**
   * Models the real command inbox: a committed key short-circuits; a thrown command is never keyed,
   * so it re-runs on redelivery.
   */
  static final class ScriptedBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
    private final Map<String, Supplier<RuntimeException>> always = new ConcurrentHashMap<>();
    private final Map<String, Supplier<RuntimeException>> once = new ConcurrentHashMap<>();
    private volatile RuntimeException lastThrown;

    void failAlways(String cmdId, Supplier<RuntimeException> failure) {
      always.put(cmdId, failure);
    }

    void failOnce(String cmdId, Supplier<RuntimeException> failure) {
      once.put(cmdId, failure);
    }

    int count(String cmdId) {
      return counts.getOrDefault(cmdId, new AtomicInteger()).get();
    }

    RuntimeException lastThrown() {
      return lastThrown;
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
      Supplier<RuntimeException> single = once.remove(id);
      if (single != null) {
        lastThrown = single.get();
        throw lastThrown;
      }
      Supplier<RuntimeException> repeated = always.get(id);
      if (repeated != null) {
        lastThrown = repeated.get();
        throw lastThrown;
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

  private static EventEnvelope correlated(String orderId, long globalOffset) {
    var event = new OrderConfirmed(orderId);
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("order-stream")),
        Version.initial(),
        new EventType("OrderConfirmed"),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("fulfil-" + orderId),
            null,
            null,
            Instant.now()));
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.streamrune.runtime.SagaStartFixtures.*;

import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.InMemorySagaStore;

/**
 * A retry-later classified forward failure on a FRESH START event must leave a durable saga row, so
 * the documented bounded escape ("if the outage outlives the saga's configured {@code timeout()},
 * {@code SagaTimeoutRunner} compensates it on the operator's own deadline") actually exists on the
 * start path.
 *
 * <p>Pre-fix, the forward dispatch loop preceded {@code persistForwardStatus}, so a retry-later
 * failure on a fresh start event propagated with NO {@code saga_state} row anywhere — and {@code
 * SagaTimeoutRunner}'s {@code SELECT_TIMED_OUT} (which needs a row) could never recover it. An
 * earlier same-delivery dispatch (a charge at index 0) was then held hostage for the whole outage,
 * unbounded, while the {@code forwardRetryLater} javadoc claimed a bound that was false.
 *
 * <p>The create-first genesis supersedes the old escape row: the executor writes the
 * genesis-pending row ({@code SagaStore.createGenesisPending} — evolved state, evolved non-terminal
 * status, {@code version = 1}, {@code genesis_applied = FALSE}) BEFORE the first forward command is
 * dispatched, so the durable anchor exists unconditionally — not only on the retry-later branch —
 * and a retry-later failure leaves it untouched (no escape write, no CAS). The recorded {@code
 * genesis_applied} flag is what lets the start-path dedup tell a re-drivable genesis-pending row
 * ("dispatch incomplete — re-drive me") apart from an applied genesis, which keeps dedup-skipping;
 * the completing {@code applyEvent} CAS stamps it TRUE (version 1 → 2).
 */
class SagaStartEventRetryLaterDurabilityTest {

  // =========================================================================
  // 1. The defect: the escape must be durable and timeout-runner-visible
  // =========================================================================

  /**
   * The repro. Index 0 dispatches (a real charge, committed in the command inbox), index 1 is
   * refused by an OPEN breaker. The failure must propagate for redelivery AND leave a durable
   * RUNNING row that {@code SagaStore.findTimedOut} can select — otherwise the outage holds the
   * index-0 charge hostage with no bound.
   */
  @Test
  void freshStartRetryLater_persistsARunningRowTheTimeoutRunnerCanSee() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failAlways("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));

    Throwable thrown =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId,
                    Optional.empty(),
                    new SagaTrigger.ForwardStep(startEvent(3L), true, false)));

    assertThat(thrown)
        .as("retry-later still propagates so the subscription redelivers the start event")
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isZero();
    assertThat(bus.count("undo-1")).as("retry-later never compensates").isZero();

    var row =
        store.load(
            sagaId,
            SagaType.fromClass(SagaStartFixtures.StartState.class),
            SagaStartFixtures.StartState.class);
    assertThat(row)
        .as(
            "The escape must be DURABLE — without a row, SagaTimeoutRunner's"
                + " SELECT_TIMED_OUT can never see this saga and the documented bounded escape is"
                + " false on the start path")
        .isPresent();
    assertThat(row.orElseThrow().status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.orElseThrow().version()).isEqualTo(1L);
    assertThat(row.orElseThrow().genesisApplied())
        .as("The intent row is recorded genesis-pending — the redelivery's re-drive license")
        .isFalse();
    assertThat(store.findTimedOut(TYPE, Instant.now().plusSeconds(3600), 10))
        .as("the row is what makes the saga visible to the timeout runner's bounded escape")
        .contains(sagaId);
  }

  // =========================================================================
  // 2. Redelivery convergence over the genesis-pending row
  // =========================================================================

  /**
   * The redelivery must NOT be swallowed by the start-path dedup fast-path: the genesis-pending row
   * is the executor's own intent record holding the genesis-evolved state with dispatch incomplete,
   * so the re-drive re-evolves the SAME state, index 0 dedups on its stable forward key, only the
   * failed index re-runs, and the completing {@code applyEvent} commits the genesis (version 1 → 2,
   * {@code genesis_applied = TRUE}).
   */
  @Test
  void redeliveryOverTheEscapeRow_reDrives_dedupsCommittedIndex_andConverges() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failOnce("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var event = startEvent(3L);

    Throwable first =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(first).isInstanceOf(CircuitBreakerOpenException.class);

    // Redelivery of the SAME start event (same globalOffset ⇒ same forward keys), loaded fresh.
    StepOutcome second =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(second)
        .as("the escape row must be re-driven, not dedup-skipped")
        .isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(bus.count("step-1")).as("index 0 dedups on its stable forward key").isEqualTo(1);
    assertThat(bus.count("step-2")).as("only the failed index re-runs").isEqualTo(1);
    var row =
        store
            .load(
                sagaId,
                SagaType.fromClass(SagaStartFixtures.StartState.class),
                SagaStartFixtures.StartState.class)
            .orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.version())
        .as("the completing applyEvent CAS-advances the genesis-pending row")
        .isEqualTo(2L);
  }

  /** After convergence the row is an ordinary completed-start row: dedup resumes. */
  @Test
  void redeliveryAfterConvergence_dedupSkips() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failOnce("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var event = startEvent(3L);

    catchThrowable(
        () ->
            exec.execute(
                sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    exec.execute(
        sagaId,
        store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
        new SagaTrigger.ForwardStep(event, true, false));

    StepOutcome third =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(third).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
    assertThat(
            store
                .load(
                    sagaId,
                    SagaType.fromClass(SagaStartFixtures.StartState.class),
                    SagaStartFixtures.StartState.class)
                .orElseThrow()
                .version())
        .as("a dedup skip writes nothing")
        .isEqualTo(2L);
  }

  /**
   * The genesis-applied guard: a row built by a COMPLETED start delivery keeps dedup-skipping
   * exactly as before — the re-drive license exists ONLY for a genesis-pending row, never for a row
   * whose genesis is recorded as applied (whose state a genesis re-evolve + CAS would clobber).
   */
  @Test
  void completedStart_redelivery_stillDedupSkips_neverReDrives() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    var event = startEvent(3L);

    StepOutcome first =
        exec.execute(sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false));
    assertThat(first).isEqualTo(StepOutcome.FORWARD_PROGRESSED);

    StepOutcome redelivered =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(redelivered).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(bus.count("step-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).isEqualTo(1);
    // Create-first at v1 (genesis pending), the completing applyEvent commits the
    // genesis at v2; the dedup skip writes nothing.
    assertThat(
            store
                .load(
                    sagaId,
                    SagaType.fromClass(SagaStartFixtures.StartState.class),
                    SagaStartFixtures.StartState.class)
                .orElseThrow()
                .version())
        .isEqualTo(2L);
  }

  // =========================================================================
  // 3. The bounded escape end-to-end: timeout claim, then start redelivery
  // =========================================================================

  /**
   * The outage outlives {@code timeout()}: the timeout runner (modelled by the executor's own
   * {@code ClaimTimeout} trigger) claims the escape row COMPENSATING — the operator's configured
   * deadline, exactly the correlated path's bounded escape. The still-redelivering start event then
   * RESUMES the claimed episode (never re-opens the forward phase), terminalizes it, and the
   * subscription finally advances.
   */
  @Test
  void escapeRowClaimedByTimeout_startRedeliveryResumesCompensation() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failAlways("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var event = startEvent(3L);

    catchThrowable(
        () ->
            exec.execute(
                sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));

    // The undo is refused once too (the outage is still on) — the claim leaves COMPENSATING.
    bus.failOnce("undo-1", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    StepOutcome claimed =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
            new SagaTrigger.ClaimTimeout(new RuntimeException("timed out")));
    // The undo was REFUSED ADMISSION by the open breaker — COMPENSATING_REFUSED, the
    // RETRY-class outcome that leaves the episode COMPENSATING without counting an attempt.
    assertThat(claimed).isEqualTo(StepOutcome.COMPENSATING_REFUSED);
    assertThat(
            store
                .load(
                    sagaId,
                    SagaType.fromClass(SagaStartFixtures.StartState.class),
                    SagaStartFixtures.StartState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATING);

    // The start event is still being redelivered; over a COMPENSATING row it must RESUME the
    // claimed episode (undo-1 re-drives under the episode key), never re-run the forward phase.
    StepOutcome resumed =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, SagaStartFixtures.StartState.class),
            new SagaTrigger.ForwardStep(event, true, false));

    assertThat(resumed).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(bus.count("undo-1")).isEqualTo(1);
    assertThat(bus.count("step-2")).as("the forward phase is never re-opened").isZero();
    assertThat(
            store
                .load(
                    sagaId,
                    SagaType.fromClass(SagaStartFixtures.StartState.class),
                    SagaStartFixtures.StartState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.COMPENSATED);
  }

  // =========================================================================
  // 4. Genesis-pending create: conflicts and conversion failures
  // =========================================================================

  /**
   * A concurrent deliverer won the genesis-pending create between this replica's (stale) empty load
   * and its own create: the {@link org.streamrune.core.OptimisticLockException} is a dedup signal —
   * the executor reloads and routes on the recorded row. The genesis is still pending there, so it
   * re-drives (index 0 dedups on its forward key), hits the same outage, and the retry-later
   * failure still propagates; the row stays at version 1.
   */
  @Test
  void escapeWriteRacesAConcurrentCreator_swallowsTheConflict_andStillPropagatesRetryLater() {
    var store = new InMemorySagaStore();
    var bus = new SagaStartFixtures.ScriptedBus();
    var exec =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(), store, bus, StreamRuneMetrics.NOOP);
    var sagaId = SagaId.of("saga-1");
    bus.failAlways("step-2", () -> new CircuitBreakerOpenException("circuit is OPEN"));
    var event = startEvent(3L);

    // Replica A: escape row created.
    catchThrowable(
        () ->
            exec.execute(
                sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));
    assertThat(
            store.load(
                sagaId,
                SagaType.fromClass(SagaStartFixtures.StartState.class),
                SagaStartFixtures.StartState.class))
        .isPresent();

    // Replica B raced: loaded BEFORE A's write (stale empty view), dispatches (index 0 dedups),
    // hits the same outage, and its escape write conflicts with A's row.
    Throwable fromB =
        catchThrowable(
            () ->
                exec.execute(
                    sagaId, Optional.empty(), new SagaTrigger.ForwardStep(event, true, false)));

    assertThat(fromB)
        .as("the conflict is dedup, not an error — the retry-later verdict stands")
        .isInstanceOf(CircuitBreakerOpenException.class);
    assertThat(bus.count("step-1")).as("index 0 dedups across the replicas").isEqualTo(1);
    assertThat(
            store
                .load(
                    sagaId,
                    SagaType.fromClass(SagaStartFixtures.StartState.class),
                    SagaStartFixtures.StartState.class)
                .orElseThrow()
                .version())
        .isEqualTo(1L);
  }

  /**
   * The genesis-pending create is the start path's first durable write, and its conversion failures
   * are classified exactly like every other store write. A DETERMINISTIC {@link
   * SagaStateSerializationException} is poison NOW — the same state would fail the completing
   * {@code applyEvent} too, so the delivery can never complete, and quarantining with a durable
   * record beats an endless retry-later loop with no trace; a TRANSIENT one propagates as itself so
   * the batch retries. Nothing is dispatched either way: the create precedes the first command.
   */
  @Test
  void conversionFailureOnTheGenesisPendingCreate_deterministicIsPoison_transientPropagates() {
    var bus = new SagaStartFixtures.ScriptedBus();
    var deterministic =
        new SagaStateSerializationException(
            "cannot serialize",
            SagaId.of("saga-1"),
            new org.streamrune.core.crypto.CryptoMappingException("bad shape"));
    var poisoned =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(),
            failingGenesisPendingCreate(deterministic),
            bus,
            StreamRuneMetrics.NOOP);

    Throwable poison =
        catchThrowable(
            () ->
                poisoned.execute(
                    SagaId.of("saga-1"),
                    Optional.empty(),
                    new SagaTrigger.ForwardStep(startEvent(3L), true, false)));

    assertThat(poison).isInstanceOf(SagaPoisonException.class);
    assertThat(bus.count("step-1")).as("the create precedes the first dispatch").isZero();

    var transientFailure =
        new SagaStateSerializationException(
            "cannot serialize",
            SagaId.of("saga-1"),
            new org.streamrune.core.EventStoreException(
                "create failed", new java.sql.SQLTransientConnectionException("pool")));
    var retried =
        new SagaStepExecutor<>(
            new SagaStartFixtures.StartOrchestrator(),
            failingGenesisPendingCreate(transientFailure),
            bus,
            StreamRuneMetrics.NOOP);

    Throwable propagated =
        catchThrowable(
            () ->
                retried.execute(
                    SagaId.of("saga-1"),
                    Optional.empty(),
                    new SagaTrigger.ForwardStep(startEvent(3L), true, false)));

    assertThat(propagated)
        .as("transient: propagates as itself for the batch retry")
        .isSameAs(transientFailure);
    assertThat(bus.count("step-1")).isZero();
  }

  private static ForwardingSagaStore failingGenesisPendingCreate(RuntimeException failure) {
    return new ForwardingSagaStore(new InMemorySagaStore()) {
      @Override
      public void createGenesisPending(
          SagaId id, SagaType t, SagaState s, SagaStatus st, boolean deadLetterPending) {
        throw failure;
      }
    };
  }
}

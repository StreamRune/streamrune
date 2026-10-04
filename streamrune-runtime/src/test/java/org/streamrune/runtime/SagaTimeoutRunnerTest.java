package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.*;
import org.streamrune.core.types.*;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

class SagaTimeoutRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  record OrderState(SagaId sagaId, SagaStatus status) implements SagaState {
    @JsonCreator
    OrderState(@JsonProperty("sagaId") SagaId sagaId, @JsonProperty("status") SagaStatus status) {
      this.sagaId = sagaId;
      this.status = status;
    }
  }

  record CancelOrder(String orderId) implements Command {}

  static class TimeoutDecider implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(sagaId, SagaStatus.STARTED);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      if (failure instanceof SagaTimeoutException) {
        return List.of(
            SagaCommand.of(
                new CancelOrder(state.sagaId().value()), AggregateId.of(state.sagaId().value())));
      }
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

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
    public <C extends Command> CommandResult execute(
        C command, org.streamrune.core.types.IdempotencyKey key) {
      dispatched.add(command);
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("s")), new Version(1), List.of(), List.of());
    }
  }

  /**
   * Counts {@code recordSagaFaulted} calls: the shared {@code RecordingStreamRuneMetrics} does not
   * track this meter, so a small local double stands in for the give-up metric-once-per-transition
   * pin.
   */
  static class FaultCountingMetrics implements org.streamrune.core.StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger faulted =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void recordSagaFaulted(String sagaType) {
      faulted.incrementAndGet();
    }
  }

  @Test
  void processBatchCompensatesTimedOutSaga() {
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();

    // Save saga in RUNNING state
    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    // Process batch — saga was saved "just now" so cutoff must be in future
    runner.processBatch(Instant.now().plusSeconds(600));

    // Compensation command dispatched
    assertEquals(1, bus.dispatched.size());
    assertInstanceOf(CancelOrder.class, bus.dispatched.getFirst());

    // Saga state updated to FAILED (no compensation evolve, so stays as-is in store)
    var loaded =
        store.load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class);
    assertTrue(loaded.isPresent());
  }

  @Test
  void processTimedOutSaga_foreignTypeRow_isNotProcessed() {
    // A row keyed by the SAME sagaId but owned by a DIFFERENT saga type must be
    // invisible to this runner's timeout load. Before the fix processTimedOutSaga used the
    // un-scoped 2-arg load(sagaId, stateType). For an already-COMPENSATING foreign row that routes
    // to ResumeCompensation, which DISPATCHES compensation from the foreign state BEFORE any
    // type-guarded CAS can 0-row. The type-scoped 3-arg load returns empty for a cross-type row, so
    // nothing is loaded, routed, or dispatched.
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();

    // A foreign saga type owns this sagaId, already COMPENSATING (the un-contained resume path).
    store.create(
        SagaId.of("order-1"),
        SagaType.of("ForeignSaga"),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    runner.processTimedOutSaga(SagaId.of("order-1"));

    // Cross-type row must not be loaded, compensated, or mutated.
    assertTrue(bus.dispatched.isEmpty());
    var foreign = store.load(SagaId.of("order-1"), SagaType.of("ForeignSaga"), OrderState.class);
    assertTrue(foreign.isPresent());
    assertEquals(SagaStatus.COMPENSATING, foreign.orElseThrow().status());
  }

  @Test
  void processBatchSkipsTerminalSagas() {
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPLETED),
        SagaStatus.COMPLETED);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    runner.processBatch(Instant.now().plusSeconds(600));

    // No compensation for terminal saga
    assertTrue(bus.dispatched.isEmpty());
  }

  @Test
  void processBatchHandlesEmptyStore() {
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    runner.processBatch(Instant.now().plusSeconds(600));
    assertTrue(bus.dispatched.isEmpty());
  }

  @Test
  void compensationFailureIsSwallowed() {
    var store = new InMemorySagaStore();
    var decider = new TimeoutDecider();

    var failingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new RuntimeException("compensation failed");
          }
        };

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(failingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    // Should not throw
    assertDoesNotThrow(() -> runner.processBatch(Instant.now().plusSeconds(600)));
  }

  /**
   * SagaTimeoutRunner dispatches compensation commands through the keyed {@code execute(command,
   * key)} with no unkeyed fallback, so a bus without a CommandInbox (supportsIdempotentExecution()
   * == false) makes every timed-out saga's compensation fail. The builder must reject such a bus at
   * build time.
   */
  @Test
  void builderRejectsBusWithoutIdempotentExecution() {
    var store = new InMemorySagaStore();
    var decider = new TimeoutDecider();
    // A bus that implements keyed execute() but reports no idempotency support (default false).
    var noInboxBus =
        new CommandBus() {
          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            return new CommandResult(
                List.of(),
                StreamId.of(TYPE, AggregateId.of("s")),
                new Version(1),
                List.of(),
                List.of());
          }
        };

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                SagaTimeoutRunner.<OrderState>builder()
                    .decider(decider)
                    .sagaStore(store)
                    .commandBus(noInboxBus)
                    .sagaType(SagaType.fromClass(OrderState.class))
                    .build());
    assertTrue(ex.getMessage().contains("idempotent"));
  }

  @Test
  void transientCompensationFailure_leavesCompensatingThenResumesToCompensated() {
    // A compensation command that fails for a TRANSIENT/infra reason must NOT
    // force-terminalize the saga FAILED (silently losing the undo with no durable retry). It must
    // stay COMPENSATING so the next timeout re-pick resumes the SAME episode under the SAME key:
    // already-succeeded compensations dedup, only the failed one re-runs, until the undo durably
    // succeeds. Before the fix the first transient failure classified FAILED (terminal) and the
    // refund/cancel was abandoned forever.
    var store = new InMemorySagaStore();
    var decider = new TimeoutDecider();

    // Compensation dispatch fails transiently (EventStoreException — an infra fault, not a business
    // rejection) on the first attempt, then succeeds on the second (a momentary DB blip that
    // clears).
    var dispatched = new ArrayList<Object>();
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var flakyBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            if (calls.incrementAndGet() == 1) {
              throw new org.streamrune.core.EventStoreException("transient DB blip");
            }
            dispatched.add(command);
            return new CommandResult(
                List.of(),
                StreamId.of(TYPE, AggregateId.of("s")),
                new Version(1),
                List.of(),
                List.of());
          }
        };

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(flakyBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    // First attempt: claim COMPENSATING, dispatch CancelOrder → transient failure. The saga must be
    // left COMPENSATING (NOT force-FAILED), and the undo not yet delivered.
    runner.processTimedOutSaga(SagaId.of("order-1"));
    assertEquals(
        SagaStatus.COMPENSATING,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "a transient compensation failure must NOT force-terminalize the saga FAILED");
    assertTrue(dispatched.isEmpty(), "the undo did not succeed on the first (transient) attempt");

    // Resume: the next timeout re-pick re-drives the SAME episode; this time the dispatch succeeds.
    runner.processTimedOutSaga(SagaId.of("order-1"));
    assertEquals(
        SagaStatus.COMPENSATED,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "the undo must be delivered on resume, ending COMPENSATED — not abandoned as FAILED");
    assertEquals(1, dispatched.size(), "the undo command executed exactly once");
    assertInstanceOf(CancelOrder.class, dispatched.getFirst());
    assertEquals(2, calls.get(), "two dispatch attempts: one transient failure, then success");
  }

  @Test
  void compensatingEpisode_pastMaxDwell_isFaulted_notReDrivenForever() {
    // When the compensation-retry sweeper is disabled, the timeout runner is the only
    // driver re-picking a stuck COMPENSATING saga — and with no dwell bound it re-drives forever,
    // so
    // the episode can outlive the command-inbox retention window and a pruned
    // succeeded-compensation
    // key would re-execute (double refund). An independent max-COMPENSATING dwell bound
    // terminalizes
    // FAULTED once the episode has been stuck longer than the bound (mirroring the sweeper's
    // give-up), instead of re-driving indefinitely.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var decider = new TimeoutDecider();
    var alwaysFailingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new org.streamrune.core.EventStoreException("downstream permanently unreachable");
          }
        };

    // A COMPENSATING episode claimed at T0 (updated_at = claim instant).
    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);
    clock.advance(Duration.ofHours(2)); // stuck COMPENSATING for 2h

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(alwaysFailingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .build();

    runner.processTimedOutSaga(SagaId.of("order-1"));

    assertEquals(
        SagaStatus.FAULTED,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "a COMPENSATING episode past the dwell bound with a still-failing compensation must be"
            + " FAULTED, not re-driven forever");
    assertEquals(
        SagaStatus.COMPENSATING,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .preFaultStatus(),
        "a give-up fault records what it replaced");
  }

  /**
   * The give-up fault races a concurrent writer — {@code markFaulted} conflicts (a resumer
   * terminalized the episode, a replay advanced it). The runner skips: row unchanged, no faulted
   * sample, nothing thrown.
   */
  @Test
  void giveUpFault_thatConflictsWithAConcurrentWriter_leavesTheRowAlone_noFaultedSample() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var inner = new InMemorySagaStore(clock);
    var conflicting =
        new ForwardingSagaStore(inner) {
          @Override
          public boolean markFaulted(SagaId id, SagaType t, long v) {
            throw new org.streamrune.core.OptimisticLockException(
                "a concurrent writer advanced the row");
          }
        };
    var metrics = new FaultCountingMetrics();
    var alwaysFailingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new org.streamrune.core.EventStoreException("downstream permanently unreachable");
          }
        };
    inner.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);
    clock.advance(Duration.ofHours(2)); // stuck COMPENSATING past the 1h dwell bound

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(conflicting)
            .commandBus(alwaysFailingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .metrics(metrics)
            .build();

    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> runner.processTimedOutSaga(SagaId.of("order-1")));

    var row =
        inner
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow();
    assertEquals(SagaStatus.COMPENSATING, row.status(), "the row is left to the concurrent winner");
    assertEquals(1L, row.version(), "no write landed");
    org.junit.jupiter.api.Assertions.assertNull(row.preFaultStatus());
    assertEquals(0, metrics.faulted.get(), "no faulted sample on a conflict");
  }

  @Test
  void giveUpFault_recordsMetricOncePerTransition_notOnRepeatedGiveUpAttempts() {
    // The fault metric counts the TRANSITION only: a repeated give-up attempt on an
    // already-FAULTED row must not double count it. processTimedOutSaga's halted guard skips a
    // FAULTED row entirely on any later poll, so running the give-up path twice on the same row
    // must record the metric exactly once.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var decider = new TimeoutDecider();
    var metrics = new FaultCountingMetrics();
    var alwaysFailingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new org.streamrune.core.EventStoreException("downstream permanently unreachable");
          }
        };

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);
    clock.advance(Duration.ofHours(2)); // stuck COMPENSATING for 2h

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(alwaysFailingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .metrics(metrics)
            .build();

    runner.processTimedOutSaga(SagaId.of("order-1")); // give-up #1: FAULTED, metric = 1
    assertEquals(
        SagaStatus.FAULTED,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status());

    runner.processTimedOutSaga(SagaId.of("order-1")); // give-up #2: halted guard skips it

    assertEquals(
        1,
        metrics.faulted.get(),
        "the faulted metric counts the TRANSITION only -- a repeated give-up on an"
            + " already-FAULTED row must not double count");
  }

  @Test
  void compensatingEpisode_withinMaxDwell_staysCompensating() {
    // The dwell bound must NOT fire early: within the bound a transient failure still leaves the
    // episode COMPENSATING so a recoverable compensation gets more attempts.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var decider = new TimeoutDecider();
    var failingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new org.streamrune.core.EventStoreException("transient blip");
          }
        };

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING);
    clock.advance(Duration.ofMinutes(10)); // well within the 1h dwell bound

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(failingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .build();

    runner.processTimedOutSaga(SagaId.of("order-1"));

    assertEquals(
        SagaStatus.COMPENSATING,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "within the dwell bound a transient failure must leave the episode COMPENSATING");
  }

  @Test
  void compensatingDwell_anchorsOnEpisodeClaim_faultReplayCycleCannotResetIt() {
    // Repro. The dwell bound exists so a poison compensation cannot outlive the
    // command-inbox retention window (a pruned succeeded-compensation key re-executes on the next
    // re-drive — double refund). But the bound was measured from updated_at, which EVERY CAS write
    // refreshes: a fault->replay cycle (markFaulted + the replayed step's write that clears the
    // fault both write the row) resets the clock, so an episode claimed at T0 could still be
    // re-driven at T0+2h against a 1h bound — and, iterated, dwell indefinitely. The immutable
    // episode_claimed_at — the same anchor discipline as the replayer's stale guard — is
    // the durable claim instant the bound must measure from; every write that enters
    // COMPENSATING stamps it, and a row without it is refused rather than measured from updated_at.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var decider = new TimeoutDecider();
    var alwaysFailingBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            throw new org.streamrune.core.EventStoreException("still failing transiently");
          }
        };

    // Episode claimed at T0: the durable claim stamps episode_claimed_at = T0.
    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    store.claimCompensating(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        1L);

    // A fault->replay cycle two hours later: the un-fault CAS refreshes updated_at while the
    // episode identity (episode_version, episode_claimed_at) is deliberately preserved.
    clock.advance(Duration.ofHours(2));
    store.update(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.COMPENSATING),
        SagaStatus.COMPENSATING,
        2L);

    clock.advance(Duration.ofMinutes(1));
    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(alwaysFailingBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .build();

    runner.processTimedOutSaga(SagaId.of("order-1"));

    assertEquals(
        SagaStatus.FAULTED,
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "the dwell bound must anchor on the immutable episode claim (T0, 2h1m ago > 1h) — a"
            + " fault->replay cycle refreshing updated_at must not reset the give-up clock");
  }

  @Test
  void liveness_accessorsReflectStartAndStop() throws InterruptedException {
    // The driver exposes isStarted()/isAlive()/consecutiveFailures() so the health
    // contributor
    // can surface a dead/wedged timeout runner as DOWN instead of leaving timed-out sagas stranded.
    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(new InMemorySagaStore())
            .commandBus(new CapturingCommandBus())
            .sagaType(SagaType.fromClass(OrderState.class))
            .pollInterval(Duration.ofMillis(50))
            .build();

    assertFalse(runner.isStarted(), "not started yet");
    assertFalse(runner.isAlive(), "no thread yet");
    assertEquals(0, runner.consecutiveFailures());
    assertEquals(SagaType.fromClass(OrderState.class).value(), runner.sagaTypeName());

    runner.start();
    assertTrue(runner.isStarted(), "started");
    assertTrue(runner.isAlive(), "poll thread alive after start");

    runner.stop();
    assertFalse(runner.isStarted(), "stop() resets the started guard");
  }

  @Test
  void timedOutBacklogGauge_isSampledEachCycle() {
    // The timed-out backlog gauge is sampled every poll cycle so a stalled/dead runner (or a
    // growing timeout backlog) is visible on the metrics endpoint.
    var store = new InMemorySagaStore();
    var metrics = new RecordingStreamRuneMetrics();
    for (int i = 0; i < 3; i++) {
      store.create(
          SagaId.of("order-" + i),
          SagaType.fromClass(OrderState.class),
          new OrderState(SagaId.of("order-" + i), SagaStatus.RUNNING),
          SagaStatus.RUNNING);
    }

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(store)
            .commandBus(new CapturingCommandBus())
            .sagaType(SagaType.fromClass(OrderState.class))
            .metrics(metrics)
            .build();

    runner.processBatch(Instant.now().plusSeconds(600)); // all three are timed out

    assertEquals(
        3L,
        metrics.sagaTimedOutBacklog(SagaType.fromClass(OrderState.class).value()),
        "the timed-out backlog gauge must reflect the sagas picked up this cycle");
  }

  @Test
  void builderRejectsMissingDecider() {
    assertThrows(
        NullPointerException.class,
        () ->
            SagaTimeoutRunner.builder()
                .sagaStore(new InMemorySagaStore())
                .commandBus(new CapturingCommandBus())
                .sagaType(SagaType.of("T"))
                .build());
  }

  @Test
  void builderRejectsANonPositiveBatchSize() {
    // A zero batch would never time anything out; a negative one fails every poll in the store.
    for (int batchSize : new int[] {0, -1}) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  SagaTimeoutRunner.<OrderState>builder()
                      .decider(new TimeoutDecider())
                      .sagaStore(new InMemorySagaStore())
                      .commandBus(new CapturingCommandBus())
                      .sagaType(SagaType.fromClass(OrderState.class))
                      .batchSize(batchSize)
                      .build());
      assertTrue(ex.getMessage().contains("batchSize"), ex.getMessage());
    }
  }

  @Test
  void builderRejectsSagaTypeMismatchedWithDeciderStateType() {
    // Saga rows are created with SagaType.fromClass(stateType); a mismatched injected sagaType
    // makes findTimedOut() match nothing, leaving the runner silently inert (forward timeouts
    // never fire, no alert). The builder must fail fast instead of shipping a dead runner.
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SagaTimeoutRunner.<OrderState>builder()
                    .decider(new TimeoutDecider())
                    .sagaStore(new InMemorySagaStore())
                    .commandBus(new CapturingCommandBus())
                    .sagaType(SagaType.of("WrongLabel"))
                    .build());
    assertTrue(ex.getMessage().contains("WrongLabel"));
    assertTrue(ex.getMessage().contains("OrderState"));
  }

  @Test
  void pollOnceComputesCutoffFromInjectedClock() {
    // Capture the cutoff the runner derives so we can assert it deterministically. A fixed clock
    // makes the cutoff exactly now - timeout; against the old Instant.now() code the captured
    // cutoff would track wall-clock time and never equal this fixed value (non-vacuous).
    var capturedCutoff = new AtomicReference<Instant>();
    SagaStore capturingStore =
        new SagaStore() {
          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {}

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {}

          @Override
          public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
            capturedCutoff.set(updatedBefore);
            return List.of();
          }

          @Override
          public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
            return List.of();
          }

          @Override
          public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
              SagaId sagaId, SagaType sagaType, Class<S> stateType) {
            return Optional.empty();
          }

          @Override
          public void delete(SagaId sagaId, SagaType sagaType) {}

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

    Instant fixedNow = Instant.parse("2026-06-17T12:00:00Z");
    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(capturingStore)
            .commandBus(new CapturingCommandBus())
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(Clock.fixed(fixedNow, ZoneOffset.UTC))
            .build();

    runner.pollOnce();

    // TimeoutDecider.timeout() == 5 minutes, so cutoff must be exactly fixedNow - 5m.
    assertEquals(fixedNow.minus(Duration.ofMinutes(5)), capturedCutoff.get());
  }

  @Test
  void builderRejectsNullClock() {
    assertThrows(
        NullPointerException.class,
        () ->
            SagaTimeoutRunner.<OrderState>builder()
                .decider(new TimeoutDecider())
                .sagaStore(new InMemorySagaStore())
                .commandBus(new CapturingCommandBus())
                .sagaType(SagaType.fromClass(OrderState.class))
                .clock(null)
                .build());
  }

  @Test
  void startAndStopLifecycle() throws InterruptedException {
    var store = new InMemorySagaStore();
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .pollInterval(Duration.ofMillis(50))
            .build();

    runner.start();
    Thread.sleep(100);
    runner.stop();
    // Should stop without hanging and leave the runner restartable. isStarted() is the proof the
    // poll thread died: stop() resets the started guard only when the join did not time out.
    assertFalse(runner.isAlive(), "stop() releases the poll-thread handle");
    assertFalse(runner.isStarted(), "stop() resets the started guard");
  }

  @Test
  void stopAndRestartAllowsSecondStart() throws Exception {
    var pollCount = new java.util.concurrent.atomic.AtomicInteger(0);
    SagaStore countingStore =
        new SagaStore() {
          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {}

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {}

          @Override
          public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
            pollCount.incrementAndGet();
            return List.of();
          }

          @Override
          public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
            return List.of();
          }

          @Override
          public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
              SagaId sagaId, SagaType sagaType, Class<S> stateType) {
            return Optional.empty();
          }

          @Override
          public void delete(SagaId sagaId, SagaType sagaType) {}

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

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(countingStore)
            .commandBus(new CapturingCommandBus())
            .sagaType(SagaType.fromClass(OrderState.class))
            .pollInterval(Duration.ofMillis(20))
            .build();

    runner.start();
    // Wait for at least one poll
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (pollCount.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(pollCount.get() > 0, "runner must have polled at least once");
    runner.stop();

    // After stop, a second start() must not throw and must poll again
    int countBeforeRestart = pollCount.get();
    assertDoesNotThrow(runner::start, "start() after stop() must not throw");
    deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (pollCount.get() <= countBeforeRestart && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertTrue(pollCount.get() > countBeforeRestart, "restarted runner must poll again");
    runner.stop();
  }

  @Test
  void doubleStartWithoutStopPreventsSecondThread() throws Exception {
    var pollCount = new java.util.concurrent.atomic.AtomicInteger(0);
    SagaStore countingStore =
        new SagaStore() {
          @Override
          public void create(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              boolean deadLetterPending) {}

          @Override
          public void update(
              SagaId sagaId,
              SagaType sagaType,
              SagaState state,
              SagaStatus status,
              long expectedVersion) {}

          @Override
          public List<SagaId> findTimedOut(SagaType sagaType, Instant updatedBefore, int limit) {
            pollCount.incrementAndGet();
            return List.of();
          }

          @Override
          public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
            return List.of();
          }

          @Override
          public <S extends SagaState> java.util.Optional<LoadedSaga<S>> load(
              SagaId sagaId, SagaType sagaType, Class<S> stateType) {
            return Optional.empty();
          }

          @Override
          public void delete(SagaId sagaId, SagaType sagaType) {}

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

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(countingStore)
            .commandBus(new CapturingCommandBus())
            .sagaType(SagaType.fromClass(OrderState.class))
            .pollInterval(Duration.ofMillis(20))
            .build();

    runner.start();
    // A second start without stop must throw (not spawn a second thread)
    assertThrows(
        IllegalStateException.class,
        runner::start,
        "double start without stop must throw IllegalStateException");
    runner.stop();
  }

  // stop() must JOIN the poll thread before resetting the started guard, so a
  // stop()->start() restart cannot leave the old poll thread running concurrently with the new one.
  @Test
  void stopJoinsPollThreadSoRestartDoesNotLeakADuplicatePoller() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    SagaStore blockingStore = mock(SagaStore.class);
    when(blockingStore.findTimedOut(any(), any(), anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));
    CommandBus mockBus = mock(CommandBus.class);
    when(mockBus.supportsIdempotentExecution()).thenReturn(true);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(blockingStore)
            .commandBus(mockBus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .pollInterval(Duration.ofSeconds(1))
            .build();
    runner.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered findTimedOut");

    runner.stop();

    assertFalse(
        pollThread.get().isAlive(),
        "stop() must join the poll thread before returning so a restart cannot leak a duplicate"
            + " poller");
  }

  // ── A vetoed timeout compensation is a rejection, not a silent success ────────────

  /** A bus whose keyed execute RETURNS a VETOED result (a veto interceptor's before() == false). */
  static class VetoingCommandBus extends CapturingCommandBus {
    @Override
    public <C extends Command> CommandResult execute(
        C command, org.streamrune.core.types.IdempotencyKey key) {
      dispatched.add(command);
      return new CommandResult(
          List.of(),
          StreamId.of(TYPE, AggregateId.of("s")),
          new Version(1),
          List.of(),
          List.of(),
          "MaintenanceModeInterceptor",
          CommandBus.ShortCircuitReason.VETOED);
    }
  }

  @Test
  void vetoedTimeoutCompensation_staysCompensating_notCompensatedNotFailed() {
    // ClaimTimeout path inheritance: the timeout runner funnels through
    // the same SagaCommandDispatch as the event path, so a vetoed compensation must not terminal-
    // write COMPENSATED while the compensation handler never ran — and must not terminalize FAILED
    // either, since a veto refuses to admit the command rather than rejecting the business. The
    // episode stays COMPENSATING for the resume paths.
    var store = new InMemorySagaStore();
    var bus = new VetoingCommandBus();
    var decider = new TimeoutDecider();

    store.create(
        SagaId.of("order-1"),
        SagaType.fromClass(OrderState.class),
        new OrderState(SagaId.of("order-1"), SagaStatus.RUNNING),
        SagaStatus.RUNNING);

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .build();

    runner.processBatch(Instant.now().plusSeconds(600));

    assertEquals(1, bus.dispatched.size(), "the compensation was dispatched (and vetoed)");
    var loaded =
        store
            .load(SagaId.of("order-1"), SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow();
    assertEquals(
        SagaStatus.COMPENSATING,
        loaded.status(),
        "a vetoed timeout compensation must stay COMPENSATING for the resume paths — not"
            + " COMPENSATED with the undo never executed, and not terminal FAILED with the undo"
            + " abandoned for a gate that lifts");
  }

  // ── The COMPENSATING resume needs the replayer's key-age guard, BEFORE dispatch ───

  @Test
  void compensatingResume_pastInboxRetention_isFaultedWithoutDispatch() {
    // An episode claimed at T0 dwells through an outage past the inbox retention window (7d): its
    // succeeded-compensation dedup keys may already be pruned. processTimedOutSaga's resume path
    // ran the ResumeCompensation dispatch FIRST and evaluated the maxCompensatingDwell force-FAULT
    // only AFTER (on COMPENSATING_LEFT) — so the first post-outage resume re-dispatched under
    // possibly-pruned keys and could RE-EXECUTE an already-succeeded compensation (double refund)
    // before any bound applied. With inboxRetentionMaxAge configured, the resume must pre-check the
    // episode's age (same episode_claimed_at anchor as the dwell bound and the replayer's
    // guard) and CAS-FAULT WITHOUT dispatching anything.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();
    var sagaId = SagaId.of("order-1");

    // Claimed COMPENSATING at T0 (episode_claimed_at = T0), then an outage of 8 days.
    // Claim-first on an existing row, v2/episode 2
    store.create(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(sagaId, SagaStatus.COMPENSATING),
        SagaStatus.RUNNING);
    store.claimCompensating(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(sagaId, SagaStatus.COMPENSATING),
        1L);
    clock.advance(Duration.ofDays(8));

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .inboxRetentionMaxAge(Duration.ofDays(7))
            .build();

    runner.processTimedOutSaga(sagaId);

    assertTrue(
        bus.dispatched.isEmpty(),
        "a resume of an episode older than the inbox retention window must NOT dispatch — its"
            + " dedup keys are unprovably fresh and a re-dispatch could re-execute an"
            + " already-succeeded compensation (double refund)");
    assertEquals(
        SagaStatus.FAULTED,
        store
            .load(sagaId, SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status(),
        "the stale episode is quarantined FAULTED (operator-visible), the same remedy as the"
            + " replayer's STALE_COMPENSATION_BLOCKED");
    assertEquals(
        SagaStatus.COMPENSATING,
        store
            .load(sagaId, SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .preFaultStatus(),
        "a give-up fault records what it replaced");
  }

  @Test
  void compensatingResume_withinInboxRetention_resumesNormally() {
    // Regression pin: with the knob configured, a fresh COMPENSATING episode resumes exactly as
    // before — the compensation dispatches and the episode terminalizes COMPENSATED.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var bus = new CapturingCommandBus();
    var decider = new TimeoutDecider();
    var sagaId = SagaId.of("order-1");

    // Claim-first on an existing row, v2/episode 2
    store.create(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(sagaId, SagaStatus.COMPENSATING),
        SagaStatus.RUNNING);
    store.claimCompensating(
        sagaId,
        SagaType.fromClass(OrderState.class),
        new OrderState(sagaId, SagaStatus.COMPENSATING),
        1L);
    clock.advance(Duration.ofHours(1)); // well within the 7d window

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(decider)
            .sagaStore(store)
            .commandBus(bus)
            .sagaType(SagaType.fromClass(OrderState.class))
            .clock(clock)
            .inboxRetentionMaxAge(Duration.ofDays(7))
            .build();

    runner.processTimedOutSaga(sagaId);

    assertEquals(1, bus.dispatched.size(), "a fresh episode resumes normally");
    assertInstanceOf(CancelOrder.class, bus.dispatched.getFirst());
    assertEquals(
        SagaStatus.COMPENSATED,
        store
            .load(sagaId, SagaType.fromClass(OrderState.class), OrderState.class)
            .orElseThrow()
            .status());
  }

  /**
   * The key-age pre-check and the dwell bound anchor on {@code episode_claimed_at} and on nothing
   * else. A COMPENSATING row WITHOUT the stamp violates the SagaStore contract — every write that
   * enters COMPENSATING stamps it; no shipped store produces such a row — and is REFUSED: nothing
   * dispatched, nothing written, not even the dwell force-FAULT the deleted {@code updatedAt}
   * fallback would have applied to this row, and the row is left for the next cycle to refuse again
   * until the store is fixed. Modelled with a decorator that strips the stamp on load.
   */
  @Test
  void anUnstampedCompensatingRow_isRefused_nothingDispatchedNothingWritten() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var unstamping =
        new ForwardingSagaStore(store) {
          @Override
          public <S extends SagaState> Optional<LoadedSaga<S>> load(
              SagaId id, SagaType t, Class<S> type) {
            return super.load(id, t, type)
                .map(
                    r ->
                        new LoadedSaga<>(
                            r.state(),
                            r.status(),
                            r.version(),
                            r.updatedAt(),
                            /* episodeVersion= */ null,
                            /* episodeClaimedAt= */ null,
                            r.genesisApplied(),
                            r.preFaultStatus(),
                            r.deadLetterPending(),
                            r.lastAppliedOffset(),
                            r.lastReplayedOffset()));
          }
        };
    var bus = new CapturingCommandBus();
    var sagaId = SagaId.of("order-1");
    var type = SagaType.fromClass(OrderState.class);
    store.create(sagaId, type, new OrderState(sagaId, SagaStatus.RUNNING), SagaStatus.RUNNING);
    store.claimCompensating(sagaId, type, new OrderState(sagaId, SagaStatus.COMPENSATING), 1L);
    assertNotNull(
        store.load(sagaId, type, OrderState.class).orElseThrow().episodeClaimedAt(),
        "the real store stamped the claim; the decorator hides it");
    clock.advance(Duration.ofHours(2)); // past the 1h dwell bound: the fallback would force-FAULT

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(unstamping)
            .commandBus(bus)
            .sagaType(type)
            .clock(clock)
            .maxCompensatingDwell(Duration.ofHours(1))
            .inboxRetentionMaxAge(Duration.ofDays(7))
            .build();

    runner.processTimedOutSaga(sagaId);
    runner.processTimedOutSaga(sagaId); // the next cycle refuses again; nothing accumulates

    assertTrue(bus.dispatched.isEmpty(), "nothing dispatched");
    var row = store.load(sagaId, type, OrderState.class).orElseThrow();
    assertEquals(SagaStatus.COMPENSATING, row.status(), "no force-FAULT, no terminal write");
    assertEquals(2L, row.version(), "nothing written");
  }

  /**
   * A downstream outage keeps a full batch of early sagas stuck {@code COMPENSATING}: every
   * re-drive fails transiently and writes nothing, so those rows stay the oldest by last write on
   * every poll. A saga started later that overruns its deadline must still be claimed by the very
   * next poll — otherwise it completes forward after the outage, hours past the deadline its
   * timeout promised, while the timed-out gauge reads a steady full batch.
   */
  @Test
  void aFullBatchOfStuckCompensations_doesNotStarveAForwardTimeout() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var store = new InMemorySagaStore(clock);
    var type = SagaType.fromClass(OrderState.class);
    List<Object> attempted = new ArrayList<>();
    var outageBus =
        new CommandBus() {
          @Override
          public boolean supportsIdempotentExecution() {
            return true;
          }

          @Override
          public <C extends Command> CommandResult execute(C command) {
            throw new UnsupportedOperationException("use keyed execute");
          }

          @Override
          public <C extends Command> CommandResult execute(
              C command, org.streamrune.core.types.IdempotencyKey key) {
            attempted.add(command);
            throw new org.streamrune.core.EventStoreException("payment gateway unreachable");
          }
        };
    for (int i = 0; i < 50; i++) {
      var stuck = SagaId.of("stuck-" + i);
      store.create(
          stuck, type, new OrderState(stuck, SagaStatus.COMPENSATING), SagaStatus.COMPENSATING);
    }
    clock.advance(Duration.ofMinutes(1));
    var overdue = SagaId.of("order-overdue");
    store.create(overdue, type, new OrderState(overdue, SagaStatus.RUNNING), SagaStatus.RUNNING);
    clock.advance(Duration.ofMinutes(10)); // every row is past the 5-minute timeout

    var runner =
        SagaTimeoutRunner.<OrderState>builder()
            .decider(new TimeoutDecider())
            .sagaStore(store)
            .commandBus(outageBus)
            .sagaType(type)
            .clock(clock)
            .build(); // default batch size: 50

    runner.pollOnce();

    var row = store.load(overdue, type, OrderState.class).orElseThrow();
    assertEquals(
        SagaStatus.COMPENSATING,
        row.status(),
        "the overdue saga must be claimed for compensation in the first poll");
    assertNotNull(row.episodeClaimedAt(), "claimed by the timeout runner");
    assertTrue(
        attempted.contains(new CancelOrder("order-overdue")),
        "its compensation was attempted in the same poll");
  }
}

package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The compensation-retry sweeper lifecycle is wired in Micronaut and builds one sweeper per {@link
 * SagaRunner} bean at startup. {@code streamrune.saga.compensation-retry-enabled=false} no longer
 * suppresses it — the knob switches its sweepers to sampling-only mode, so the saga backlog gauges
 * they alone emit stay live.
 */
class SagaCompensationRetryLifecycleWiringTest {

  record FakeState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final CommandBus NOOP_BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  private static SagaRunner<FakeState> sagaRunner(SagaStore store) {
    return SagaRunner.<FakeState>builder()
        .orchestrator(new FakeOrchestrator())
        .sagaStore(store)
        .commandBus(NOOP_BUS)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
        .build();
  }

  @Test
  void sweeperLifecycle_beanPresent_byDefault() {
    var store = new InMemorySagaStore();
    try (var ctx = ApplicationContext.builder().singletons(store).start()) {
      assertTrue(ctx.containsBean(SagaCompensationRetryLifecycle.class));
    }
  }

  @Test
  void startSweepers_buildsOnePerRunner_thenStopClearsThem() {
    // Unit-drive the lifecycle's build-per-runner logic directly (a real app registers SagaRunner
    // as compile-time beans, reliably collected into the injected List; this asserts the logic
    // without depending on runtime-singleton collection timing).
    var store = new InMemorySagaStore();
    var lifecycle =
        new SagaCompensationRetryLifecycle(
            List.of(sagaRunner(store)),
            store,
            null,
            null,
            null,
            java.time.Duration.ofSeconds(60),
            java.time.Duration.ofHours(1),
            java.time.Duration.ofDays(7),
            java.time.Duration.ofDays(7),
            true);

    lifecycle.startSweepers();
    assertEquals(1, lifecycle.sweepers().size());

    lifecycle.close();
    assertEquals(0, lifecycle.sweepers().size());
  }

  @Test
  void startSweepers_noOp_whenNoRunner() {
    var store = new InMemorySagaStore();
    var lifecycle =
        new SagaCompensationRetryLifecycle(
            List.of(),
            store,
            null,
            null,
            null,
            java.time.Duration.ofSeconds(60),
            java.time.Duration.ofHours(1),
            java.time.Duration.ofDays(7),
            java.time.Duration.ofDays(7),
            true);

    lifecycle.startSweepers();

    assertEquals(0, lifecycle.sweepers().size());
  }

  @Test
  void startSweepers_buildsNoSweepers_whenKnobOffAndNoMetricsWired() {
    // With compensation-retry-enabled=false AND no StreamRuneMetrics wired (metrics=null
    // here resolves to NOOP, same as the constructor's own null-coalescing), a sweeper would be a
    // pure idle vthread plus a content-free ":sampling-only" health component — it re-drives
    // nothing (the knob is off) and samples nothing (NOOP short-circuits sampleBacklog before it
    // ever touches the saga store). Unlike
    // sagaBacklogGauges_areSampledEvenWhenCompensationRetryIsOff below (which DOES wire metrics,
    // and correctly still builds a sampling sweeper), this combination must build zero sweepers.
    var store = new InMemorySagaStore();
    var lifecycle =
        new SagaCompensationRetryLifecycle(
            List.of(sagaRunner(store)),
            store,
            null,
            null,
            null,
            java.time.Duration.ofSeconds(60),
            java.time.Duration.ofHours(1),
            java.time.Duration.ofDays(7),
            java.time.Duration.ofDays(7),
            false);

    lifecycle.startSweepers();

    assertEquals(0, lifecycle.sweepers().size());
  }

  @Test
  void sweeper_lifecycle_present_whenKnobOff() {
    // streamrune.saga.compensation-retry-enabled=false used to remove this bean
    // (@Requires(notEquals = "false")) — and with it the ONLY sampler of
    // streamrune.saga.compensating / streamrune.saga.faulted_rows. The knob now switches off the
    // RE-DRIVE only, so the bean must still exist; only the saga kill switch
    // (streamrune.saga.enabled=false) removes it.
    var store = new InMemorySagaStore();
    try (var ctx =
        ApplicationContext.builder()
            .properties(java.util.Map.of("streamrune.saga.compensation-retry-enabled", "false"))
            .singletons(store)
            .start()) {
      assertTrue(ctx.containsBean(SagaCompensationRetryLifecycle.class));
    }
  }

  /** Counts the saga backlog gauge samples. */
  static final class SagaGaugeRecorder implements org.streamrune.core.StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger compensating =
        new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.atomic.AtomicInteger faultedRows =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void recordSagaCompensatingBacklog(String sagaType, long count) {
      compensating.incrementAndGet();
    }

    @Override
    public void recordSagaFaultedRows(String sagaType, long count) {
      faultedRows.incrementAndGet();
    }
  }

  @Test
  void sagaBacklogGauges_areSampledEvenWhenCompensationRetryIsOff() throws Exception {
    // streamrune.saga.compensating and streamrune.saga.faulted_rows are emitted ONLY by
    // SagaCompensationRetrySweeper.sampleBacklog. With compensation-retry-enabled=false this bean
    // was never created, so no sweeper was ever built and BOTH gauges went silent — while
    // MetricNames and production.md promised whole-population coverage with no caveat. The knob
    // must switch off the RE-DRIVE, not the observability: the sweeper still runs, sampling only.
    // Unit-driven like startSweepers_buildsOnePerRunner_thenStopClearsThem (a runtime-registered
    // SagaRunner singleton is not reliably collected into the injected List).
    var store = new InMemorySagaStore();
    var metrics = new SagaGaugeRecorder();
    var lifecycle =
        new SagaCompensationRetryLifecycle(
            List.of(sagaRunner(store)),
            store,
            null,
            metrics,
            null,
            java.time.Duration.ofSeconds(60),
            java.time.Duration.ofHours(1),
            java.time.Duration.ofDays(7),
            java.time.Duration.ofDays(7),
            false);

    lifecycle.startSweepers();
    try {
      assertEquals(1, lifecycle.sweepers().size());
      assertFalse(
          lifecycle.sweepers().getFirst().isCompensationRetryEnabled(),
          "the sweeper runs, but sampling-only");
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while ((metrics.compensating.get() == 0 || metrics.faultedRows.get() == 0)
          && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(
          metrics.compensating.get() > 0,
          "streamrune.saga.compensating must be sampled with compensation retry OFF — the knob"
              + " stops the re-drive, not the observability");
      assertTrue(metrics.faultedRows.get() > 0, "streamrune.saga.faulted_rows likewise");
    } finally {
      lifecycle.close();
    }
  }

  @Test
  void sweeper_managesNothing_whenNoSagaRunnerBean() {
    var store = new InMemorySagaStore();
    try (var ctx = ApplicationContext.builder().singletons(store).start()) {
      assertTrue(ctx.containsBean(SagaCompensationRetryLifecycle.class));
      assertEquals(0, ctx.getBean(SagaCompensationRetryLifecycle.class).sweepers().size());
    }
  }
}

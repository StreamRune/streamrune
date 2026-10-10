package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The compensation-retry sweeper is auto-wired (one sweeper per {@link SagaRunner} bean) when a
 * {@link SagaStore} + at least one {@link SagaRunner} bean are present, and suppressed when the
 * knob is off or no saga runner exists.
 */
class SagaCompensationRetryLifecycleAutoConfigurationTest {

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

  static final class FakeDecider implements SagaDecider<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
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
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofHours(1));
    }
  }

  private static SagaTimeoutRunner<FakeState> timeoutRunner() {
    return SagaTimeoutRunner.<FakeState>builder()
        .decider(new FakeDecider())
        .sagaStore(new InMemorySagaStore())
        .commandBus(NOOP_BUS)
        .sagaType(SagaType.fromClass(FakeState.class))
        .build();
  }

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(SagaStore.class, InMemorySagaStore::new);

  @Test
  void sweeperLifecycle_autoWired_perSagaRunnerBean_byDefault() {
    runner
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SagaCompensationRetryLifecycle.class);
              // SmartLifecycle auto-starts on refresh → one sweeper per SagaRunner bean.
              assertThat(ctx.getBean(SagaCompensationRetryLifecycle.class).sweepers()).hasSize(1);
            });
  }

  @Test
  void sweeperLifecycle_samplingOnly_whenKnobOff() {
    // The knob switches off the RE-DRIVE only. The lifecycle (and its per-runner sweeper)
    // must still exist so streamrune.saga.compensating / faulted_rows — emitted nowhere else — keep
    // being sampled; the sweeper itself reports that re-drive is off. A metrics bean must be
    // wired for this — with compensation-retry-enabled=false AND no StreamRuneMetrics wired the
    // lifecycle now builds ZERO sweepers instead (nothing would be sampled anyway; see
    // sweeperLifecycle_buildsZeroSweepers_whenKnobOffAndNoMetricsWired below).
    runner
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .withBean(org.streamrune.core.StreamRuneMetrics.class, SagaGaugeRecorder::new)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SagaCompensationRetryLifecycle.class);
              var sweepers = ctx.getBean(SagaCompensationRetryLifecycle.class).sweepers();
              assertThat(sweepers).hasSize(1);
              assertThat(sweepers.getFirst().isCompensationRetryEnabled())
                  .as("the sweeper runs, but sampling-only")
                  .isFalse();
            });
  }

  @Test
  void sweeperLifecycle_buildsZeroSweepers_whenKnobOffAndNoMetricsWired() {
    // With compensation-retry-enabled=false AND no StreamRuneMetrics bean wired (NOOP
    // default), a sweeper would be a pure idle vthread plus a content-free ":sampling-only"
    // health component — it re-drives nothing (the knob is off) and samples nothing (NOOP
    // short-circuits sampleBacklog before it ever touches the saga store). Unlike
    // sagaBacklogGauges_areSampledEvenWhenCompensationRetryIsOff below (which DOES wire metrics,
    // and correctly still builds a sampling sweeper), this combination must build no sweeper —
    // though the lifecycle BEAN itself still exists (its own start() simply decides not to build
    // anything), unlike the saga.enabled=false kill switch which removes the bean entirely.
    runner
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SagaCompensationRetryLifecycle.class);
              assertThat(ctx.getBean(SagaCompensationRetryLifecycle.class).sweepers()).isEmpty();
            });
  }

  /**
   * {@code streamrune.saga.enabled=false} is the documented saga kill switch, and Quarkus ({@code
   * startSweepers()} returns early) and Micronaut (class-level {@code @Requires}) both stop the
   * compensation-retry sweepers on it. Spring gated the lifecycle only on
   * {@code @ConditionalOnBean({SagaRunner, SagaStore})} + {@code compensation-retry-enabled}, and
   * this runner supplies its own {@link SagaStore} bean — exactly the deployment in the finding —
   * so the bean condition stayed satisfied, the sweepers were built and started, and they kept
   * re-driving COMPENSATING sagas (issuing real compensation commands) after the operator believed
   * sagas were off. A silent kill-switch no-op on the one framework where an operator is most
   * likely to reach for it.
   */
  @Test
  void sweeperLifecycle_disabled_whenSagaKillSwitchOff_evenWithUserSuppliedSagaStore() {
    runner
        .withPropertyValues("streamrune.saga.enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .run(ctx -> assertThat(ctx).doesNotHaveBean(SagaCompensationRetryLifecycle.class));
  }

  /**
   * Defence in depth, matching the Quarkus implementation: even if an application hand-declares the
   * lifecycle bean (bypassing the {@code @ConditionalOnProperty}), {@code start()} must honour the
   * kill switch rather than half-configuring the sweepers.
   */
  @Test
  void sweeperLifecycle_startIsNoOp_whenSagaKillSwitchOff() {
    runner
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .run(
            ctx -> {
              var properties = ctx.getBean(StreamRuneProperties.class);
              var sagaDisabled =
                  new StreamRuneProperties(
                      properties.snapshotEveryNEvents(),
                      properties.retryMaxAttempts(),
                      properties.retryInitialDelayMs(),
                      properties.retryBackoffMultiplier(),
                      properties.pollingIntervalMs(),
                      properties.pollingJitterMs(),
                      properties.lockTimeout(),
                      properties.stripeCount(),
                      properties.maxInFlightAsyncCommands(),
                      properties.circuitBreakerFailureThreshold(),
                      properties.circuitBreakerCooldown(),
                      properties.eventAuditEnabled(),
                      properties.projectionDlqEnabled(),
                      properties.queryCache(),
                      new StreamRuneProperties.Saga(
                          false,
                          properties.saga().deadLetterRetentionMaxAge(),
                          properties.saga().compensationRetryEnabled(),
                          properties.saga().compensationRetryInterval(),
                          properties.saga().compensationRetryGiveUpAfter()),
                      properties.outbox(),
                      properties.inbox(),
                      properties.deadLetter(),
                      properties.subscription(),
                      properties.metrics(),
                      properties.validationEnabled(),
                      properties.startupLog(),
                      properties.eventStore(),
                      properties.metadata(),
                      properties.security(),
                      properties.sse());
              var lifecycle =
                  new SagaCompensationRetryLifecycle(
                      ctx.getBeanProvider(
                          org.springframework.core.ResolvableType.forClassWithGenerics(
                              SagaRunner.class, Object.class)),
                      new InMemorySagaStore(),
                      ctx.getBeanProvider(
                          org.streamrune.core.subscription.SubscriptionLeadership.class),
                      ctx.getBeanProvider(org.streamrune.core.StreamRuneMetrics.class),
                      ctx.getBeanProvider(BackgroundRelayHealthContributor.class),
                      sagaDisabled);
              lifecycle.start();
              assertThat(lifecycle.sweepers())
                  .as("the saga kill switch must skip the sweepers, not half-configure them")
                  .isEmpty();
              assertThat(lifecycle.isRunning())
                  .as("a kill-switched lifecycle must not report itself running")
                  .isFalse();
            });
  }

  @Test
  void sweeperLifecycle_absent_whenNoSagaRunnerBean() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(SagaCompensationRetryLifecycle.class));
  }

  @Test
  void timeoutRunnerHealth_registered_evenWhenSweeperDisabled() {
    // With the compensation re-drive switched OFF (the documented "rely on a
    // SagaTimeoutRunner" config) and no SagaRunner bean, the sweeper lifecycle bean is absent
    // (the knob alone no longer removes it; the missing SagaRunner does) — but the
    // always-present
    // SagaTimeoutRunnerHealthRegistrar must still register the timeout runner's liveness, so a dead
    // timeout-runner thread surfaces DOWN instead of leaving /health UP while sagas strand.
    runner
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .withBean(
            "relayHealth",
            BackgroundRelayHealthContributor.class,
            BackgroundRelayHealthContributor::new)
        .withBean("orderTimeoutRunner", SagaTimeoutRunner.class, () -> timeoutRunner())
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean(SagaCompensationRetryLifecycle.class);
              assertThat(ctx).hasSingleBean(SagaTimeoutRunnerHealthRegistrar.class);
              var health = ctx.getBean(BackgroundRelayHealthContributor.class);
              assertThat(health.components())
                  .anyMatch(
                      c ->
                          c.name()
                              .equals(
                                  "saga-timeout:" + SagaType.fromClass(FakeState.class).value()));
            });
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
  void sagaBacklogGauges_areSampledEvenWhenCompensationRetryIsOff() {
    // streamrune.saga.compensating and streamrune.saga.faulted_rows are emitted ONLY by
    // SagaCompensationRetrySweeper.sampleBacklog. With compensation-retry-enabled=false the
    // lifecycle bean was never created, so no sweeper was ever built and BOTH gauges went silent —
    // while MetricNames and production.md promised whole-population coverage with no caveat. The
    // knob must switch off the RE-DRIVE, not the observability: the sweeper still runs, sampling
    // only.
    var metrics = new SagaGaugeRecorder();
    runner
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .withBean("orderSagaRunner", SagaRunner.class, () -> sagaRunner(new InMemorySagaStore()))
        .withBean(org.streamrune.core.StreamRuneMetrics.class, () -> metrics)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              // The sweeper reports the two gauges with two separate recorder calls on its own
              // thread, so the wait covers both: seeing the first sample says nothing about
              // whether the second call has happened yet.
              await()
                  .atMost(Duration.ofSeconds(10))
                  .untilAsserted(
                      () -> {
                        assertThat(metrics.compensating.get())
                            .as(
                                "streamrune.saga.compensating must be sampled with compensation"
                                    + " retry OFF — the knob stops the re-drive, not the"
                                    + " observability")
                            .isPositive();
                        assertThat(metrics.faultedRows.get())
                            .as("streamrune.saga.faulted_rows likewise")
                            .isPositive();
                      });
              var sweepers = ctx.getBean(SagaCompensationRetryLifecycle.class).sweepers();
              assertThat(sweepers).hasSize(1);
              assertThat(sweepers.getFirst().isCompensationRetryEnabled()).isFalse();
            });
  }
}

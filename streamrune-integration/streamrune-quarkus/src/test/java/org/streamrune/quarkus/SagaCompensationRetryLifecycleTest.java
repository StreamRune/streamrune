package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The Quarkus compensation-retry lifecycle builds one sweeper per discovered {@link SagaRunner},
 * threads the {@code streamrune.saga.compensation-retry-enabled} knob (bound through the real
 * {@code @ConfigMapping}) into each sweeper as sampling-only mode, and stops them all on shutdown.
 */
class SagaCompensationRetryLifecycleTest {

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

  private static StreamRuneQuarkusProperties bind(Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  private static SagaCompensationRetryLifecycle lifecycle(
      List<SagaRunner<?>> runners, SagaStore store, StreamRuneQuarkusProperties props) {
    return lifecycle(runners, store, props, null);
  }

  @SuppressWarnings("unchecked")
  private static SagaCompensationRetryLifecycle lifecycle(
      List<SagaRunner<?>> runners,
      SagaStore store,
      StreamRuneQuarkusProperties props,
      StreamRuneMetrics metrics) {
    Instance<SagaStore> storeInstance = mock(Instance.class);
    when(storeInstance.isUnsatisfied()).thenReturn(store == null);
    when(storeInstance.get()).thenReturn(store);
    Instance<SubscriptionLeadership> leadershipInstance = mock(Instance.class);
    when(leadershipInstance.isUnsatisfied()).thenReturn(true);
    Instance<StreamRuneMetrics> metricsInstance = mock(Instance.class);
    when(metricsInstance.isUnsatisfied()).thenReturn(metrics == null);
    when(metricsInstance.get()).thenReturn(metrics);
    Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance =
        mock(Instance.class);
    when(relayHealthInstance.isUnsatisfied()).thenReturn(true);
    return new SagaCompensationRetryLifecycle(
        runners, storeInstance, leadershipInstance, metricsInstance, relayHealthInstance, props);
  }

  @Test
  void startsOneSweeperPerRunner_thenStopsThemAll() {
    var store = new InMemorySagaStore();
    var lifecycle = lifecycle(List.of(sagaRunner(store)), store, bind(Map.of()));

    lifecycle.startSweepers();
    assertEquals(1, lifecycle.sweepers().size());

    lifecycle.stopSweepers();
    assertTrue(lifecycle.sweepers().isEmpty());
  }

  /** Counts the saga backlog gauge samples. */
  static final class SagaGaugeRecorder implements StreamRuneMetrics {
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
  void samplingOnlySweeper_whenKnobDisabled() throws Exception {
    // streamrune.saga.compensating and streamrune.saga.faulted_rows are emitted ONLY by
    // SagaCompensationRetrySweeper.sampleBacklog. With compensation-retry-enabled=false
    // startSweepers() returned before building a single sweeper, so BOTH gauges went silent — while
    // MetricNames and production.md promised whole-population coverage with no caveat. The knob
    // must switch off the RE-DRIVE, not the observability: a sweeper is still built and started,
    // sampling only.
    var store = new InMemorySagaStore();
    var metrics = new SagaGaugeRecorder();
    var lifecycle =
        lifecycle(
            List.of(sagaRunner(store)),
            store,
            bind(Map.of("streamrune.saga.compensation-retry-enabled", "false")),
            metrics);

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
      lifecycle.stopSweepers();
    }
  }

  @Test
  void noSweepers_whenKnobDisabledAndNoMetricsWired() {
    // With compensation-retry-enabled=false AND no StreamRuneMetrics wired (NOOP), a
    // sweeper would be a pure idle vthread plus a content-free ":sampling-only" health component
    // — it re-drives nothing (the knob is off) and samples nothing (NOOP short-circuits
    // sampleBacklog before it ever touches the saga store). Unlike
    // samplingOnlySweeper_whenKnobDisabled above (which DOES have metrics wired, and correctly
    // still builds a sampling sweeper), this combination must build no sweeper at all.
    var store = new InMemorySagaStore();
    var lifecycle =
        lifecycle(
            List.of(sagaRunner(store)),
            store,
            bind(Map.of("streamrune.saga.compensation-retry-enabled", "false")));

    lifecycle.startSweepers();

    assertTrue(
        lifecycle.sweepers().isEmpty(),
        "compensation-retry-enabled=false + no metrics wired must build zero sweepers");
  }

  @Test
  void noSweepers_whenNoRunner() {
    var store = new InMemorySagaStore();
    var lifecycle = lifecycle(List.of(), store, bind(Map.of()));

    lifecycle.startSweepers();

    assertTrue(lifecycle.sweepers().isEmpty());
  }

  // ── saga.enabled=false leaves a SATISFIED SagaStore bean with a NULL product ───────

  /**
   * The two beans a real Quarkus application supplies that raw Arc cannot (the Agroal {@code
   * DataSource} and the SmallRye-bound {@code @ConfigMapping}), with the documented saga kill
   * switch flipped off. Everything else comes from the <strong>real</strong> {@link
   * StreamRuneProducers}.
   */
  @ApplicationScoped
  public static class SagaDisabledInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of("streamrune.saga.enabled", "false"));
    }
  }

  @Test
  void sagaDisabled_realContainerYieldsSatisfiedButNullSagaStore_andSweepersAreSkipped()
      throws Exception {
    // StreamRuneProducers.sagaStore is a dependent-scoped @DefaultBean producer that
    // returns NULL when streamrune.saga.enabled=false. The bean DEFINITION still exists, so
    // isUnsatisfied() is false and get() hands back null — exactly the null-product contract the
    // producers' class javadoc warns consumers about. startSweepers() must therefore null-check the
    // resolved store instead of feeding null into the sweeper builder (which throws
    // NullPointerException "sagaStore is required" inside the @Observes StartupEvent observer and
    // aborts Quarkus startup).
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, SagaDisabledInfrastructure.class))) {
      Instance<SagaStore> realStoreInstance = arc.container().select(SagaStore.class);
      assertFalse(
          realStoreInstance.isUnsatisfied(),
          "the @DefaultBean SagaStore producer exists even with saga.enabled=false — the"
              + " isUnsatisfied() guard cannot see the null product");
      assertNull(
          realStoreInstance.get(),
          "the real producer returns null when sagas are disabled (dependent scope permits it)");

      var props = bind(Map.of("streamrune.saga.enabled", "false"));
      var lifecycle =
          new SagaCompensationRetryLifecycle(
              List.of(sagaRunner(new InMemorySagaStore())),
              realStoreInstance,
              arc.container().select(SubscriptionLeadership.class),
              arc.container().select(StreamRuneMetrics.class),
              arc.container().select(org.streamrune.runtime.BackgroundRelayHealthContributor.class),
              props);

      assertDoesNotThrow(
          lifecycle::startSweepers,
          "startSweepers() must not NPE when the resolved SagaStore is null — that aborts boot");
      assertTrue(
          lifecycle.sweepers().isEmpty(),
          "no compensation-retry sweeper can run without a saga store");
    }
  }
}

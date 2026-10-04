package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.BackgroundRelayHealthContributor.ComponentHealth;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Two saga-state classes that share a simple name ({@code Fulfillment.OrderState} and {@code
 * Refund.OrderState} here; {@code com.shop.fulfillment.OrderState} and {@code
 * com.shop.refund.OrderState} collide the same way) are two distinct saga types — {@link
 * SagaType#fromClass} derives the fully-qualified name, and the saga stores keep their rows apart.
 * Their background drivers must therefore report as two health components and two metric series.
 *
 * <p>Keyed by the simple name, both timeout runners were {@code saga-timeout:OrderState}: the
 * framework health indicators put each component's details under {@code "relay." + name}, so one
 * saga's dead driver hid behind the other's healthy entry; and both sweepers fed ONE {@code
 * streamrune.saga.compensating}/{@code streamrune.saga.timed_out} gauge per name, last write wins,
 * so the gauge flipped between the two sagas' backlogs each cycle. Health components and metric
 * tags are keyed by {@link SagaType#value()} — the same identity the stores persist.
 */
class SagaSameSimpleNameObservabilityTest {

  /** Holder for the first of two saga-state classes whose simple name is {@code OrderState}. */
  static final class Fulfillment {
    private Fulfillment() {}

    record OrderState(SagaStatus status) implements SagaState {}
  }

  /** Holder for the second saga-state class whose simple name is {@code OrderState}. */
  static final class Refund {
    private Refund() {}

    record OrderState(SagaStatus status) implements SagaState {}
  }

  private static final String FULFILLMENT_TYPE =
      SagaType.fromClass(Fulfillment.OrderState.class).value();
  private static final String REFUND_TYPE = SagaType.fromClass(Refund.OrderState.class).value();

  @Test
  void theTwoStateClassesShareASimpleNameButNotASagaType() {
    // Guards the fixture: without the shared simple name this test would prove nothing.
    assertThat(Fulfillment.OrderState.class.getSimpleName())
        .isEqualTo(Refund.OrderState.class.getSimpleName());
    assertThat(FULFILLMENT_TYPE).isNotEqualTo(REFUND_TYPE);
  }

  @Test
  void timeoutRunnersAreTwoHealthComponentsKeyedByTheSagaType() {
    var store = new InMemorySagaStore();
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerSagaTimeoutRunner(
        timeoutRunner(Fulfillment.OrderState.class, store, StreamRuneMetrics.NOOP));
    contributor.registerSagaTimeoutRunner(
        timeoutRunner(Refund.OrderState.class, store, StreamRuneMetrics.NOOP));

    assertThat(contributor.components())
        .extracting(ComponentHealth::name)
        .containsExactly("saga-timeout:" + FULFILLMENT_TYPE, "saga-timeout:" + REFUND_TYPE);
  }

  @Test
  void compensationRetrySweepersAreTwoHealthComponentsKeyedByTheSagaType() {
    var store = new InMemorySagaStore();
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerSagaCompensationRetrySweeper(
        sweeper(Fulfillment.OrderState.class, store, StreamRuneMetrics.NOOP, true));
    contributor.registerSagaCompensationRetrySweeper(
        sweeper(Refund.OrderState.class, store, StreamRuneMetrics.NOOP, true));
    contributor.registerSagaCompensationRetrySweeper(
        sweeper(Refund.OrderState.class, store, StreamRuneMetrics.NOOP, false));

    assertThat(contributor.components())
        .extracting(ComponentHealth::name)
        .containsExactly(
            "saga-compensation-retry:" + FULFILLMENT_TYPE,
            "saga-compensation-retry:" + REFUND_TYPE,
            "saga-compensation-retry:" + REFUND_TYPE + ":sampling-only");
  }

  @Test
  void compensatingAndFaultedBacklogGaugesAreTwoSeriesKeyedByTheSagaType() {
    var store = new InMemorySagaStore();
    createRows(store, Fulfillment.OrderState.class, SagaStatus.COMPENSATING, 2);
    createRows(store, Refund.OrderState.class, SagaStatus.COMPENSATING, 1);
    createRows(store, Refund.OrderState.class, SagaStatus.FAULTED, 3);
    var metrics = new GaugeRecordingMetrics();

    // Sampling-only: one cycle samples the two backlog gauges and re-drives nothing.
    sweeper(Fulfillment.OrderState.class, store, metrics, false).sweepOnce();
    sweeper(Refund.OrderState.class, store, metrics, false).sweepOnce();

    assertThat(metrics.compensating)
        .containsExactlyInAnyOrderEntriesOf(Map.of(FULFILLMENT_TYPE, 2L, REFUND_TYPE, 1L));
    assertThat(metrics.faultedRows)
        .containsExactlyInAnyOrderEntriesOf(Map.of(FULFILLMENT_TYPE, 0L, REFUND_TYPE, 3L));
  }

  @Test
  void timedOutBacklogGaugesAreTwoSeriesKeyedByTheSagaType() {
    var store = new InMemorySagaStore();
    var metrics = new GaugeRecordingMetrics();

    // An empty poll still samples the gauge (0), so this pins the series key, not a count.
    timeoutRunner(Fulfillment.OrderState.class, store, metrics).processBatch(Instant.EPOCH);
    timeoutRunner(Refund.OrderState.class, store, metrics).processBatch(Instant.EPOCH);

    assertThat(metrics.timedOut)
        .containsExactlyInAnyOrderEntriesOf(Map.of(FULFILLMENT_TYPE, 0L, REFUND_TYPE, 0L));
  }

  // ---- fixtures ----

  private static <S extends SagaState> void createRows(
      InMemorySagaStore store, Class<S> stateType, SagaStatus status, int count) {
    for (int i = 0; i < count; i++) {
      store.create(
          SagaId.of(stateType.getName() + "-" + status + "-" + i),
          SagaType.fromClass(stateType),
          newState(stateType, status),
          status);
    }
  }

  private static <S extends SagaState> S newState(Class<S> stateType, SagaStatus status) {
    try {
      return stateType.getDeclaredConstructor(SagaStatus.class).newInstance(status);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <S extends SagaState> SagaTimeoutRunner<S> timeoutRunner(
      Class<S> stateType, InMemorySagaStore store, StreamRuneMetrics metrics) {
    return SagaTimeoutRunner.<S>builder()
        .decider(new MinimalDecider<>(stateType))
        .sagaStore(store)
        .commandBus(new UnusedCommandBus())
        .sagaType(SagaType.fromClass(stateType))
        .metrics(metrics)
        .build();
  }

  private static <S extends SagaState> SagaCompensationRetrySweeper<S> sweeper(
      Class<S> stateType,
      InMemorySagaStore store,
      StreamRuneMetrics metrics,
      boolean compensationRetryEnabled) {
    SagaRunner<S> runner =
        SagaRunner.<S>builder()
            .decider(new MinimalDecider<>(stateType))
            .startWhen(event -> false)
            .extractSagaId(event -> SagaId.of("unused"))
            .correlateBy(event -> Optional.empty())
            .sagaStore(store)
            .commandBus(new UnusedCommandBus())
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
            .build();
    return SagaCompensationRetrySweeper.<S>builder()
        .sagaRunner(runner)
        .sagaStore(store)
        .compensationRetryEnabled(compensationRetryEnabled)
        .metrics(metrics)
        .build();
  }

  /** A decider no test here feeds an event to; only its state type and timeout matter. */
  private record MinimalDecider<S extends SagaState>(Class<S> stateType) implements SagaDecider<S> {
    @Override
    public S initialState(SagaId sagaId) {
      return newState(stateType, SagaStatus.STARTED);
    }

    @Override
    public S evolve(S state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(S state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  /** Idempotent-capable so the runners build; never dispatched to by these tests. */
  private static final class UnusedCommandBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends org.streamrune.core.Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("no command is dispatched by these tests");
    }

    @Override
    public <C extends org.streamrune.core.Command> CommandResult execute(
        C command, org.streamrune.core.types.IdempotencyKey key) {
      throw new UnsupportedOperationException("no command is dispatched by these tests");
    }
  }

  /** Records the latest sample of each saga backlog gauge per {@code saga.type} tag value. */
  private static final class GaugeRecordingMetrics implements StreamRuneMetrics {
    final Map<String, Long> compensating = new ConcurrentHashMap<>();
    final Map<String, Long> faultedRows = new ConcurrentHashMap<>();
    final Map<String, Long> timedOut = new ConcurrentHashMap<>();

    @Override
    public void recordSagaCompensatingBacklog(String sagaType, long count) {
      compensating.put(sagaType, count);
    }

    @Override
    public void recordSagaFaultedRows(String sagaType, long count) {
      faultedRows.put(sagaType, count);
    }

    @Override
    public void recordSagaTimedOutBacklog(String sagaType, long count) {
      timedOut.put(sagaType, count);
    }
  }
}

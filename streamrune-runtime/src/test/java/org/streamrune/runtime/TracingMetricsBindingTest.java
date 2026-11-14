package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.ThrowingTracing.Fault;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderPlaced;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderSaga;
import org.streamrune.runtime.TracingFailureIsolationTest.OrderState;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.RecordingStreamRuneMetrics;
import org.streamrune.test.SagaFixture;

/**
 * Every builder that is given a tracing and metrics counts the failures of that tracing in those
 * metrics. The tracing here throws from {@code enabled}, which each component asks when it is built
 * or when it starts its work, so every failure is the component's own and must be counted.
 */
class TracingMetricsBindingTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final StreamId LAMP_1 = StreamId.of(LAMP, AggregateId.of("l-1"));

  private final ThrowingTracing broken = ThrowingTracing.on(Fault.ENABLED);
  private final RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(condition.getAsBoolean()).as("condition within 5s").isTrue();
  }

  private void assertEveryFailureCounted() {
    assertThat(broken.thrown.get()).as("the component asked its tracing").isPositive();
    assertThat(metrics.total(MetricNames.TRACING_FAILURES, MetricNames.TAG_OPERATION, "enabled"))
        .as("every failure of the tracing is counted in the component's metrics")
        .isEqualTo(broken.thrown.get());
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(2));
  }

  @Test
  void theOutboxPoller() {
    OutboxPoller.builder()
        .outboxStore(new InMemoryOutboxStore())
        .publisher(entry -> {})
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .pollInterval(Duration.ofSeconds(60))
        .tracing(broken)
        .metrics(metrics)
        .build();

    assertEveryFailureCounted();
  }

  @Test
  void theSagaRunnerItsExecutorAndTheTimeoutRunner() {
    SagaFixture.of(
            OrderState.class,
            builder ->
                builder
                    .decider(new OrderSaga())
                    .startWhen(event -> event.event() instanceof OrderPlaced)
                    .extractSagaId(
                        event -> SagaId.of("order-" + ((OrderPlaced) event.event()).orderId()))
                    .tracing(broken)
                    .metrics(metrics))
        .withTimeouts(builder -> builder.decider(new OrderSaga()).tracing(broken).metrics(metrics));

    assertEveryFailureCounted();
  }

  @Test
  void thePollingSubscription() {
    PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of("lights"))
        .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
        .offsetStore(new InMemoryOffsetStore())
        .listener(events -> {})
        .config(fastConfig())
        .tracing(broken)
        .metrics(metrics)
        .build();

    assertEveryFailureCounted();
  }

  @Test
  void theStreamSubscription() {
    StreamEventSubscription.builder()
        .subscriptionName(SubscriptionName.of("lamp-1"))
        .streamId(LAMP_1)
        .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
        .offsetStore(new InMemoryOffsetStore())
        .listener(events -> {})
        .config(fastConfig())
        .tracing(broken)
        .metrics(metrics)
        .build();

    assertEveryFailureCounted();
  }

  @Test
  void theScheduledProjectionRunner_withOneGuardForAllItsProjections() {
    Projection projection = batch -> {};
    ScheduledProjectionRunner.builder()
        .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
        .offsetStore(new InMemoryOffsetStore())
        .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
        .register(ProjectionName.of("nightly"), projection, "0 0 3 * * *", AT_LEAST_ONCE_IDEMPOTENT)
        .register(ProjectionName.of("weekly"), projection, "0 0 4 * * *", AT_LEAST_ONCE_IDEMPOTENT)
        .tracing(broken)
        .metrics(metrics)
        .build();

    assertEveryFailureCounted();
    assertThat(broken.thrown.get()).as("asked once per projection").isEqualTo(2);
  }

  @Test
  void theContinuousProjectionRunner() throws InterruptedException {
    ContinuousProjectionRunner runner =
        ContinuousProjectionRunner.builder()
            .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
            .offsetStore(new InMemoryOffsetStore())
            .subscriptionConfig(fastConfig())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .tracing(broken)
            .metrics(metrics)
            .build();
    Thread thread =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(ProjectionName.of("lamps"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      await(() -> broken.thrown.get() > 0);
    } finally {
      runner.close();
      thread.join(TimeUnit.SECONDS.toMillis(5));
    }

    assertEveryFailureCounted();
  }

  @Test
  void theMultiProjectionRunner() throws InterruptedException {
    MultiProjectionRunner runner =
        MultiProjectionRunner.builder()
            .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
            .offsetStore(new InMemoryOffsetStore())
            .subscriptionConfig(fastConfig())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .register(ProjectionName.of("multi"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .tracing(broken)
            .metrics(metrics)
            .build();
    runner.start();
    try {
      await(() -> broken.thrown.get() > 0);
    } finally {
      runner.stop();
    }

    assertEveryFailureCounted();
  }

  @Test
  void theStreamRuneFacade_forItsBusItsLockerAndItsProjections() {
    try (StreamRune streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore(TracingFailureIsolationTest.TYPES))
            .register(
                LAMP,
                TracingFailureIsolationTest.Switch.class,
                c -> AggregateId.of(c.lampId()),
                new TracingFailureIsolationTest.LampDecider())
            .registerProjection(ProjectionName.of("facade"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .projectionRunner(new TracingBuildersTest.RecordingRunner())
            .tracing(broken)
            .metrics(metrics)
            .build()) {
      assertThat(streamRune).isNotNull();
    }

    assertEveryFailureCounted();
  }

  // ── inline projections ─────────────────────────────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

  /**
   * An inline projection that lets a checked exception out without declaring it is one failed
   * projection: the remaining inline projections still run.
   */
  @Test
  void anInlineProjectionsUndeclaredCheckedExceptionDoesNotStopTheOthers() {
    AtomicInteger later = new AtomicInteger();
    InMemoryEventStore store = new InMemoryEventStore(TracingFailureIsolationTest.TYPES);
    InlineProjectionInterceptor inline =
        InlineProjectionInterceptor.builder()
            .register(
                ProjectionName.of("failing"),
                (Projection)
                    batch -> sneakyThrow(new java.io.IOException("the read model is down")))
            .register(ProjectionName.of("later"), (Projection) batch -> later.incrementAndGet())
            .build();
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(
                LAMP,
                TracingFailureIsolationTest.Switch.class,
                c -> AggregateId.of(c.lampId()),
                new TracingFailureIsolationTest.LampDecider())
            .interceptors(inline)
            .build()) {
      // The events are committed; a failed inline projection is logged and fails no command.
      assertThat(bus.execute(new TracingFailureIsolationTest.Switch("l-1")).events()).hasSize(1);
    }
    assertThat(later.get()).as("the projection registered after the failing one ran").isEqualTo(1);
  }
}

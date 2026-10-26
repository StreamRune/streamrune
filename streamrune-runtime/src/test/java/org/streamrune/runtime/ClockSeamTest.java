package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;
import org.streamrune.test.MutableClock;
import org.streamrune.test.RecordingStreamRuneMetrics;

/**
 * Every builder that reads the time takes a {@link java.time.Clock}: a {@link MutableClock} then
 * decides the timestamps its product records and the durations it measures, one assertion per
 * builder.
 */
class ClockSeamTest {

  record OrderPlaced(String orderId) implements DomainEvent {}

  record PlaceOrder(String orderId) implements Command {}

  record Order(boolean placed) implements AggregateState {}

  private static final Instant START = Instant.parse("2026-01-01T00:00:30Z");
  private static final StreamId ORDER_1 =
      StreamId.of(AggregateType.of("order"), AggregateId.of("1"));

  private static InMemoryEventStore storeWithOneOrder() {
    var store = new InMemoryEventStore();
    store.append(
        ORDER_1,
        List.of(
            EventStoreFixture.event(
                ORDER_1,
                new Version(1),
                EventType.fromClass(OrderPlaced.class),
                new OrderPlaced("1"))),
        Version.initial());
    return store;
  }

  /** A listener that takes 250 ms on the clock to deliver a page. */
  private static EventListener taking250ms(MutableClock clock) {
    return events -> clock.advance(Duration.ofMillis(250));
  }

  @Test
  void pollingEventSubscription_timesItsDeliveriesOnTheClock() {
    var clock = MutableClock.startingAt(START);
    var metrics = new RecordingStreamRuneMetrics();
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName("orders")
            .eventStore(storeWithOneOrder())
            .offsetStore(new InMemoryOffsetStore())
            .listener(taking250ms(clock))
            .metrics(metrics)
            .clock(clock)
            .build();

    subscription.pollOnce();

    assertThat(metrics.total(MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY))
        .isEqualTo(Duration.ofMillis(250).toNanos());
  }

  @Test
  void streamEventSubscription_timesItsDeliveriesOnTheClock() {
    var clock = MutableClock.startingAt(START);
    var metrics = new RecordingStreamRuneMetrics();
    var subscription =
        StreamEventSubscription.builder()
            .subscriptionName("order-1")
            .streamId(ORDER_1)
            .eventStore(storeWithOneOrder())
            .offsetStore(new InMemoryOffsetStore())
            .listener(taking250ms(clock))
            .metrics(metrics)
            .clock(clock)
            .build();

    subscription.pollOnce();

    assertThat(metrics.total(MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY))
        .isEqualTo(Duration.ofMillis(250).toNanos());
  }

  @Test
  void continuousProjectionRunner_stampsADeadLetterEntryWithTheClock() throws Exception {
    var clock = MutableClock.startingAt(START);
    var deadLetters = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(storeWithOneOrder())
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(deadLetters)
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .clock(clock)
            .build();
    Thread run =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("orders"),
                        batch -> {
                          throw new IllegalArgumentException("poison");
                        },
                        AT_LEAST_ONCE_IDEMPOTENT));
    try {
      await().atMost(Duration.ofSeconds(10)).until(() -> !deadLetters.all().isEmpty());
    } finally {
      runner.close();
      run.join(10_000);
    }

    assertThat(deadLetters.all().getFirst().failedAt()).isEqualTo(START);
  }

  @Test
  void multiProjectionRunner_handsItsClockToItsRunners() {
    var clock = MutableClock.startingAt(START);
    var deadLetters = new InMemoryProjectionDeadLetterStore();
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(storeWithOneOrder())
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .deadLetterStore(deadLetters)
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .register(
                "orders",
                batch -> {
                  throw new IllegalArgumentException("poison");
                },
                ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .clock(clock)
            .build();
    runner.start();
    try {
      await().atMost(Duration.ofSeconds(10)).until(() -> !deadLetters.all().isEmpty());
    } finally {
      runner.stop();
    }

    assertThat(deadLetters.all().getFirst().failedAt()).isEqualTo(START);
  }

  @Test
  void scheduledProjectionRunner_computesTheNextFireFromTheClock() {
    var clock = MutableClock.startingAt(START);
    var runner =
        ScheduledProjectionRunner.builder()
            .eventStore(new InMemoryEventStore())
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .register("orders", batch -> {}, "0 * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .clock(clock)
            .build();

    assertThat(runner.status().get("orders").nextFireAt())
        .isEqualTo(Instant.parse("2026-01-01T00:01:00Z"));
  }

  @Test
  void outboxPoller_measuresTheBlockageAgeOnTheClock() {
    var clock = MutableClock.startingAt(START);
    var outbox = new InMemoryOutboxStore(clock);
    outbox.save(OutboxEntry.pending(OutboxEntryId.of("entry-1"), "{}", "OrderPlaced", ORDER_1));
    outbox.loadPending(1);
    outbox.markFailed(OutboxEntryId.of("entry-1"), 3, "broker refused", outbox.claimedBy());
    clock.advance(Duration.ofSeconds(90));
    var metrics = new RecordingStreamRuneMetrics();
    OutboxPublisher publisher = entry -> {};
    var poller =
        OutboxPoller.builder()
            .outboxStore(outbox)
            .publisher(publisher)
            .metrics(metrics)
            .clock(clock)
            .build();

    poller.processBatch();

    assertThat(metrics.lastGauge(MetricNames.OUTBOX_BLOCKAGE_AGE_SECONDS))
        .isEqualTo(OptionalLong.of(90));
  }

  @Test
  void streamRune_handsItsClockToTheCommandBus() {
    var clock = MutableClock.startingAt(START);
    var store = new InMemoryEventStore();
    Decider<PlaceOrder, Order, OrderPlaced> decider =
        new Decider<>() {
          @Override
          public Order initialState() {
            return new Order(false);
          }

          @Override
          public List<OrderPlaced> decide(PlaceOrder command, Order state) {
            return List.of(new OrderPlaced(command.orderId()));
          }

          @Override
          public Order evolve(Order state, OrderPlaced event) {
            return new Order(true);
          }
        };
    try (var app =
        StreamRune.builder()
            .eventStore(store)
            .register(
                AggregateType.of("order"),
                PlaceOrder.class,
                command -> AggregateId.of(command.orderId()),
                decider)
            .clock(clock)
            .build()) {

      app.execute(new PlaceOrder("1"));
    }

    EventEnvelope placed = store.load(ORDER_1).events().getFirst();
    assertThat(placed.metadata().timestamp()).isEqualTo(START);
  }

  /**
   * A system clock reads the wall clock, which an NTP step moves: with one, in any zone, a duration
   * is measured on {@code System.nanoTime()}, as without a clock.
   */
  @Test
  void aSystemClock_timesDurationsOnSystemNanoTime() {
    for (Clock system :
        List.of(
            Clock.systemUTC(),
            Clock.systemDefaultZone(),
            Clock.system(ZoneId.of("Europe/Prague")))) {
      TimeSource time = TimeSource.of(system);

      long before = System.nanoTime();
      long reading = time.nanoTime();
      long after = System.nanoTime();

      assertThat(reading).as("%s times on System.nanoTime()", system).isBetween(before, after);
    }
  }
}

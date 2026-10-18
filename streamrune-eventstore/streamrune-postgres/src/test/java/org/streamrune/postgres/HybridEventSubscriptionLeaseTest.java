package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
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
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SubscriptionLease;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A {@link HybridEventSubscription} hands its lease to the polling subscription inside it, so a
 * notification reaches the same gate as a poll.
 */
class HybridEventSubscriptionLeaseTest {

  private static final String NAME = "hybrid-leased";

  record Opened(String orderId) implements DomainEvent {}

  record OrderState(SagaStatus status) implements SagaState {}

  /** A saga that starts on {@link Opened} and issues no command. */
  static final class QuietSaga implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return new OrderState(SagaStatus.RUNNING);
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return List.of();
    }
  }

  private final RecordingLeadership leadership = new RecordingLeadership();

  @Test
  void notificationsOnAStandbyAskForTheLeaseOncePerInterval() {
    EventStore eventStore = mock(EventStore.class);
    OffsetStore offsetStore = mock(OffsetStore.class);
    var sub =
        HybridEventSubscription.builder()
            .subscriptionName(NAME)
            .dataSource(mock(DataSource.class))
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
            .leadership(leadership)
            .build();
    sub.start();
    try {
      await().atMost(5, TimeUnit.SECONDS).until(() -> leadership.tryAcquires(NAME) == 1);

      for (int i = 0; i < 20; i++) {
        sub.onNotificationReceived();
      }

      assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
      verifyNoInteractions(eventStore, offsetStore);
    } finally {
      sub.close();
    }
  }

  @Test
  void sagaRunnerReachesTheInnerSubscription() {
    InMemorySagaStore rows = new InMemorySagaStore();
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    InMemoryEventStore eventStore =
        new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(rows);
    SagaRunner<OrderState> runner =
        SagaRunner.<OrderState>builder()
            .decider(new QuietSaga())
            .startWhen(e -> e.event() instanceof Opened)
            .extractSagaId(e -> SagaId.of("order-" + ((Opened) e.event()).orderId()))
            .correlateByEventData((streamId, e) -> Optional.empty())
            .sagaStore(rows)
            .commandBus(
                VirtualThreadCommandBus.builder()
                    .eventStore(eventStore)
                    .commandInbox(inbox)
                    .build())
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(rows))
            .leadership(leadership)
            .build();
    leadership.grant(runner.lease().name());
    var sub =
        HybridEventSubscription.builder()
            .subscriptionName(NAME)
            .dataSource(mock(DataSource.class))
            .eventStore(eventStore)
            .offsetStore(new InMemoryOffsetStore())
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
            .sagaRunner(runner)
            .build();
    sub.start();
    try {
      assertThat(runner.lease().everAnchored()).isTrue();
      append(eventStore, new Opened("1"));

      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () ->
                  rows.load(
                          SagaId.of("order-1"),
                          SagaType.fromClass(OrderState.class),
                          OrderState.class)
                      .isPresent());
      assertThat(runner.lease().isLeadingHere()).isTrue();
    } finally {
      sub.close();
    }
    assertThat(runner.lease().isLeadingHere()).isFalse();
  }

  @Test
  void leaseTogetherWithLeadershipIsRefused() {
    assertThatThrownBy(
            () ->
                HybridEventSubscription.builder()
                    .subscriptionName(NAME)
                    .lease(SubscriptionLease.of(leadership, NAME))
                    .leadership(leadership)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "HybridEventSubscription.Builder: lease(...) and leadership(...) are both set —"
                + " leadership(l) is lease(SubscriptionLease.of(l, subscriptionName)); name one"
                + " of them");
  }

  @Test
  void sagaRunnerTogetherWithAnotherSourceIsRefused() {
    SagaRunner<?> runner = mock(SagaRunner.class);
    assertThatThrownBy(
            () ->
                HybridEventSubscription.builder()
                    .subscriptionName(NAME)
                    .sagaRunner(runner)
                    .listener(events -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "HybridEventSubscription.Builder: sagaRunner(...) and listener(...) are both set");
    assertThatThrownBy(
            () ->
                HybridEventSubscription.builder()
                    .subscriptionName(NAME)
                    .sagaRunner(runner)
                    .lease(SubscriptionLease.ungated())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "HybridEventSubscription.Builder: sagaRunner(...) and lease(...) are both set");
    assertThatThrownBy(
            () ->
                HybridEventSubscription.builder()
                    .subscriptionName(NAME)
                    .sagaRunner(runner)
                    .leadership(leadership)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "HybridEventSubscription.Builder: sagaRunner(...) and leadership(...) are both set");
  }

  private static void append(InMemoryEventStore eventStore, DomainEvent event) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    eventStore.append(
        streamId,
        List.of(
            new EventEnvelope(
                GlobalOffset.initial(),
                streamId,
                Version.initial(),
                EventType.fromClass(event.getClass()),
                event,
                new EventMetadata(
                    EventId.of("evt-" + UUID.randomUUID()),
                    CommandId.of("cmd-" + UUID.randomUUID()),
                    null,
                    null,
                    CorrelationId.of("c"),
                    null,
                    null,
                    Instant.now()))),
        Version.initial());
  }
}

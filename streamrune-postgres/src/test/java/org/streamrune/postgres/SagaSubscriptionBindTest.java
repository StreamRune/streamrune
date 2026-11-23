package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaParallelismDefault;
import org.streamrune.runtime.SagaParallelismSettings;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/** A subscription built with {@code sagaRunner(runner)} binds its name to the runner. */
class SagaSubscriptionBindTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder().registerEvent(Opened.class).build();

  private static final SubscriptionName NAME = SubscriptionName.of("order-saga");

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

  private final InMemorySagaStore rows = new InMemorySagaStore();
  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore eventStore =
      new InMemoryEventStore(TYPES).withCommandInbox(inbox).withSagaStore(rows);

  @Test
  void pollingSubscriptionBuiltWithSagaRunnerBindsItsName() {
    SagaRunner<OrderState> runner = runner();

    PollingEventSubscription.builder()
        .subscriptionName(NAME)
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .sagaRunner(runner)
        .build();

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("order-saga"));
  }

  @Test
  void hybridSubscriptionBuiltWithSagaRunnerBindsItsName() {
    SagaRunner<OrderState> runner = runner();

    HybridEventSubscription.builder()
        .subscriptionName(NAME)
        .dataSource(mock(DataSource.class))
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
        .sagaRunner(runner)
        .build();

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("order-saga"));
  }

  @Test
  void notificationSubscriptionBuiltWithSagaRunnerBindsItsName() {
    SagaRunner<OrderState> runner = runner();

    notification().sagaRunner(runner).build();

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("order-saga"));
  }

  @Test
  void aClosedHybridSubscriptionReleasesItsBind_soARebuiltOneUnderAnotherNameCountsOnce() {
    SagaRunner<OrderState> runner = runner();
    HybridEventSubscription first = hybrid(NAME).sagaRunner(runner).build();

    first.close();
    HybridEventSubscription second =
        hybrid(SubscriptionName.of("order-saga.v2")).sagaRunner(runner).build();

    assertThat(runner.boundSubscriptionNames())
        .containsExactly(SubscriptionName.of("order-saga.v2"));
    first.close();
    assertThat(runner.boundSubscriptionNames())
        .containsExactly(SubscriptionName.of("order-saga.v2"));
    second.close();
    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aClosedNotificationSubscriptionReleasesItsBind_soARebuiltOneUnderAnotherNameCountsOnce() {
    SagaRunner<OrderState> runner = runner();
    PostgresNotificationSubscription first = notification().sagaRunner(runner).build();

    first.close();
    PostgresNotificationSubscription second =
        notification(SubscriptionName.of("order-saga.v2")).sagaRunner(runner).build();

    assertThat(runner.boundSubscriptionNames())
        .containsExactly(SubscriptionName.of("order-saga.v2"));
    first.close();
    assertThat(runner.boundSubscriptionNames())
        .containsExactly(SubscriptionName.of("order-saga.v2"));
    second.close();
    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aSubscriptionBuiltWithTheRunnersListenerBindsNothing_andTheSettingsAreToldHowToBindIt() {
    SagaRunner<OrderState> runner = runner();

    PollingEventSubscription.builder()
        .subscriptionName(NAME)
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .listener(runner.asEventListener())
        .build();
    HybridEventSubscription.builder()
        .subscriptionName(NAME)
        .dataSource(mock(DataSource.class))
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
        .listener(runner.asEventListener())
        .build();
    notification().listener(runner.asEventListener()).build();

    assertThat(runner.boundSubscriptionNames()).isEmpty();
    SagaParallelismSettings settings = new SagaParallelismSettings(4, Map.of("order-saga", 2));
    assertThatThrownBy(
            () -> SagaParallelismDefault.requireKnownSubscriptions(List.of(runner), settings))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Subscriptions bound to a saga runner: []")
        .hasMessageContaining("is bound when it is built with .sagaRunner(runner)");
  }

  @Test
  void aPollingBuildThatFailsInTheConstructorBindsNothing() {
    SagaRunner<OrderState> runner = runner();

    // passes the two-sources check, fails in the constructor: no offset store
    assertThatThrownBy(
            () ->
                PollingEventSubscription.builder()
                    .subscriptionName(NAME)
                    .eventStore(eventStore)
                    .sagaRunner(runner)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("offsetStore is required");

    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aHybridBuildThatFailsInTheConstructorBindsNothing() {
    SagaRunner<OrderState> runner = runner();

    // passes the two-sources check, fails in the constructor: no data source
    assertThatThrownBy(
            () ->
                HybridEventSubscription.builder()
                    .subscriptionName(NAME)
                    .eventStore(eventStore)
                    .offsetStore(new InMemoryOffsetStore())
                    .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
                    .sagaRunner(runner)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("dataSource is required");

    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aNotificationBuildThatFailsInTheConstructorBindsNothing() {
    SagaRunner<OrderState> runner = runner();

    assertThatThrownBy(
            () ->
                PostgresNotificationSubscription.builder(NAME)
                    .eventStore(eventStore)
                    .offsetStore(new InMemoryOffsetStore())
                    .sagaRunner(runner)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("listenDataSource is required");

    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aBuildThatNamesTwoSourcesBindsNothing() {
    SagaRunner<OrderState> runner = runner();

    assertThatThrownBy(
            () ->
                PollingEventSubscription.builder()
                    .subscriptionName(NAME)
                    .eventStore(eventStore)
                    .offsetStore(new InMemoryOffsetStore())
                    .sagaRunner(runner)
                    .listener(events -> {})
                    .build())
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  private HybridEventSubscription.Builder hybrid(SubscriptionName name) {
    return HybridEventSubscription.builder()
        .subscriptionName(name)
        .dataSource(mock(DataSource.class))
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)));
  }

  private PostgresNotificationSubscription.Builder notification() {
    return notification(NAME);
  }

  private PostgresNotificationSubscription.Builder notification(SubscriptionName name) {
    return PostgresNotificationSubscription.builder(name)
        .listenDataSource(mock(DataSource.class))
        .eventStore(eventStore)
        .offsetStore(new InMemoryOffsetStore())
        .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
        .fetchSize(10);
  }

  private SagaRunner<OrderState> runner() {
    return SagaRunner.<OrderState>builder()
        .decider(new QuietSaga())
        .startWhen(e -> e.event() instanceof Opened)
        .extractSagaId(e -> SagaId.of("order-" + ((Opened) e.event()).orderId()))
        .correlateByEventData((streamId, e) -> Optional.empty())
        .sagaStore(rows)
        .commandBus(
            VirtualThreadCommandBus.builder().eventStore(eventStore).commandInbox(inbox).build())
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(rows))
        .build();
  }
}

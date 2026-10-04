package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * Bean presence/absence for the single-active-consumer wiring: the {@code LeaseBasedLeadership}
 * bean and the {@code OutboxFailedReplayer}. The leadership bean opens no connection eagerly (its
 * renew heartbeat idles while no lease is held), so a mock {@link DataSource} suffices to assert
 * the conditions.
 */
class SubscriptionLeadershipAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  @Test
  void leadershipBeanRegisteredWhenDataSourcePresent() {
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SubscriptionLeadership.class);
              assertThat(ctx.getBean(SubscriptionLeadership.class))
                  .isInstanceOf(org.streamrune.postgres.LeaseBasedLeadership.class);
              // Paired lifecycle bean is registered so close() sequences after the runners.
              assertThat(ctx).hasBean("streamRuneSubscriptionLeadershipLifecycle");
            });
  }

  @Test
  void leadershipBeanAbsentWithoutDataSource() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(SubscriptionLeadership.class));
  }

  @Test
  void leadershipBeanAbsentWhenDisabled() {
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=false")
        .run(ctx -> assertThat(ctx).doesNotHaveBean(SubscriptionLeadership.class));
  }

  @Test
  void leadershipBeanYieldsToUserDefinedBean() {
    var userLeadership = SubscriptionLeadership.NOOP;
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(SubscriptionLeadership.class, () -> userLeadership)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SubscriptionLeadership.class);
              assertThat(ctx.getBean(SubscriptionLeadership.class)).isSameAs(userLeadership);
              assertThat(ctx).doesNotHaveBean(org.streamrune.postgres.LeaseBasedLeadership.class);
            });
  }

  @Test
  void outboxFailedReplayerRegisteredWhenOutboxEnabledAndStorePresent() {
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withPropertyValues("streamrune.outbox.enabled=true")
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            () -> mock(org.streamrune.core.outbox.OutboxStore.class))
        .run(
            ctx ->
                assertThat(ctx).hasSingleBean(org.streamrune.runtime.OutboxFailedReplayer.class));
  }

  @Test
  void outboxFailedReplayerAbsentWhenOutboxDisabled() {
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            () -> mock(org.streamrune.core.outbox.OutboxStore.class))
        .run(
            ctx ->
                assertThat(ctx).doesNotHaveBean(org.streamrune.runtime.OutboxFailedReplayer.class));
  }
}

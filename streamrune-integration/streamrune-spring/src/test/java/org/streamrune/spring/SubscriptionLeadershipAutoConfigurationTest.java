package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Optional;
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

  /**
   * Two leadership beans and none {@code @Primary}: the context does not start, and the message
   * names both and says how to choose. Quarkus refuses an ambiguous leadership the same way, and
   * Micronaut refuses the non-unique bean; treating it as absent would make every replica lead.
   */
  @Test
  void twoLeadershipBeansWithoutAPrimaryFailStartupNamingBoth() {
    runner
        .withBean("leadershipA", SubscriptionLeadership.class, LeadershipA::new)
        .withBean("leadershipB", SubscriptionLeadership.class, LeadershipB::new)
        .run(
            ctx ->
                assertThat(ctx)
                    .hasFailed()
                    .getFailure()
                    .hasStackTraceContaining(
                        "2 SubscriptionLeadership beans and none is the default ("
                            + LeadershipA.class.getName()
                            + ", "
                            + LeadershipB.class.getName()
                            + ")")
                    .hasStackTraceContaining("mark the one to use @Primary"));
  }

  /** With one of the two marked {@code @Primary}, that one is used. */
  @Test
  void twoLeadershipBeansWithAPrimaryUseThePrimary() {
    runner
        .withBean(
            "leadershipA",
            SubscriptionLeadership.class,
            LeadershipA::new,
            definition -> definition.setPrimary(true))
        .withBean("leadershipB", SubscriptionLeadership.class, LeadershipB::new)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(SubscriptionLeadership.class)).isInstanceOf(LeadershipA.class);
            });
  }

  /** A leadership that holds nothing and grants nothing. */
  static class LeadershipA implements SubscriptionLeadership {
    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.empty();
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  /** A second leadership class, so the message can name both. */
  static class LeadershipB extends LeadershipA {}

  @Test
  void outboxFailedReplayerRegisteredWhenOutboxEnabledAndStorePresent() {
    runner
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withPropertyValues("streamrune.outbox.enabled=true")
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            SpringTestMocks::outboxStoreStatingOrderingMode)
        // An enabled outbox needs something that relays it, or the boot is refused.
        .withBean(
            org.streamrune.core.outbox.OutboxPublisher.class,
            () -> mock(org.streamrune.core.outbox.OutboxPublisher.class))
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

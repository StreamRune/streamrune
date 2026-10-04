package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.types.EventType;
import org.streamrune.runtime.BeanValidationInterceptor;

class NewAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  @Test
  void micrometerMetricsRegisteredWhenMeterRegistryPresent() {
    runner
        .withBean(
            io.micrometer.core.instrument.MeterRegistry.class,
            () -> new io.micrometer.core.instrument.simple.SimpleMeterRegistry())
        .run(
            ctx ->
                assertThat(ctx)
                    .hasSingleBean(org.streamrune.integration.MicrometerStreamRuneMetrics.class));
  }

  @Test
  void micrometerMetricsNotRegisteredWhenDisabled() {
    runner
        .withPropertyValues("streamrune.metrics.enabled=false")
        .withBean(
            io.micrometer.core.instrument.MeterRegistry.class,
            () -> new io.micrometer.core.instrument.simple.SimpleMeterRegistry())
        .run(
            ctx ->
                assertThat(ctx)
                    .doesNotHaveBean(org.streamrune.integration.MicrometerStreamRuneMetrics.class));
  }

  @Test
  void beanValidationInterceptorRegisteredWhenValidatorOnClasspath() {
    runner
        .withPropertyValues("streamrune.validation-enabled=true")
        .run(ctx -> assertThat(ctx).hasSingleBean(BeanValidationInterceptor.class));
  }

  @Test
  void beanValidationInterceptorNotRegisteredWhenDisabled() {
    runner
        .withPropertyValues("streamrune.validation-enabled=false")
        .run(ctx -> assertThat(ctx).doesNotHaveBean(BeanValidationInterceptor.class));
  }

  @Test
  void sagaStoreRegisteredWhenDataSourcePresent() {
    runner.run(ctx -> assertThat(ctx).hasSingleBean(org.streamrune.core.saga.SagaStore.class));
  }

  @Test
  void sagaStoreNotRegisteredWhenDisabled() {
    runner
        .withPropertyValues("streamrune.saga.enabled=false")
        .run(ctx -> assertThat(ctx).doesNotHaveBean(org.streamrune.core.saga.SagaStore.class));
  }

  @Test
  void outboxPollerNotRegisteredByDefault() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(org.streamrune.runtime.OutboxPoller.class));
  }

  /**
   * A DB-less app that supplies its own EventStore and has no DataSource must boot.
   * postgresEventStoreFactory is now @ConditionalOnBean(DataSource.class) so it backs off, and
   * eventStore() is @ConditionalOnBean(EventStoreFactory.class) so it backs off too. Before the fix
   * the factory bean was created unconditionally and failed context refresh on the missing
   * DataSource, even though the user's EventStore already satisfied the app.
   */
  @Test
  void dbLessAppWithUserEventStoreBoots() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(EventStore.class);
              assertThat(ctx).doesNotHaveBean(EventStoreFactory.class);
            });
  }

  @Test
  void outboxPollerRegisteredWhenEnabledAndStorePresent() {
    runner
        .withPropertyValues("streamrune.outbox.enabled=true")
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            SpringTestMocks::outboxStoreStatingOrderingMode)
        .withBean(
            org.streamrune.core.outbox.OutboxPublisher.class,
            () -> mock(org.streamrune.core.outbox.OutboxPublisher.class))
        .run(ctx -> assertThat(ctx).hasSingleBean(org.streamrune.runtime.OutboxPoller.class));
  }

  @Test
  void deadLetterRetryRunnerRegisteredWhenDlqPresent() {
    runner
        .withBean(
            org.streamrune.core.DeadLetterQueue.class,
            () -> mock(org.streamrune.core.DeadLetterQueue.class))
        .withBean(
            com.fasterxml.jackson.databind.ObjectMapper.class,
            () -> new com.fasterxml.jackson.databind.ObjectMapper())
        .run(
            ctx ->
                assertThat(ctx).hasSingleBean(org.streamrune.runtime.DeadLetterRetryRunner.class));
  }

  @Test
  void deadLetterRetryRunnerNotRegisteredWhenDisabled() {
    runner
        .withPropertyValues("streamrune.dead-letter.enabled=false")
        .withBean(
            org.streamrune.core.DeadLetterQueue.class,
            () -> mock(org.streamrune.core.DeadLetterQueue.class))
        .run(
            ctx ->
                assertThat(ctx)
                    .doesNotHaveBean(org.streamrune.runtime.DeadLetterRetryRunner.class));
  }

  @Test
  void offsetStoreRegisteredWhenDataSourcePresent() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(org.streamrune.core.projection.OffsetStore.class);
          assertThat(ctx)
              .getBean(org.streamrune.core.projection.OffsetStore.class)
              .isInstanceOf(org.streamrune.postgres.PostgresOffsetStore.class);
        });
  }

  @Test
  void offsetStoreYieldsToUserDefinedBean() {
    var userOffsetStore = mock(org.streamrune.core.projection.OffsetStore.class);
    runner
        .withBean(org.streamrune.core.projection.OffsetStore.class, () -> userOffsetStore)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(org.streamrune.core.projection.OffsetStore.class);
              assertThat(ctx).doesNotHaveBean(org.streamrune.postgres.PostgresOffsetStore.class);
              assertThat(ctx.getBean(org.streamrune.core.projection.OffsetStore.class))
                  .isSameAs(userOffsetStore);
            });
  }

  @Test
  void eventUpcastersRegisteredWithEventStore() {
    runner
        .withBean(
            org.streamrune.core.upcasting.EventUpcaster.class,
            () -> {
              var upcaster = mock(org.streamrune.core.upcasting.EventUpcaster.class);
              when(upcaster.eventType()).thenReturn(new EventType("TestEvent"));
              when(upcaster.currentVersion()).thenReturn(2);
              return upcaster;
            })
        .run(
            ctx -> {
              assertThat(ctx).hasBean("upcasterChain");
            });
  }
}

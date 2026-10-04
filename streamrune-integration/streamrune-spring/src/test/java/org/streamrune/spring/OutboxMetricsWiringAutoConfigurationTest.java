package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * Proves the auto-configured {@link OutboxPoller} carries the real {@link StreamRuneMetrics} bean,
 * not {@link StreamRuneMetrics#NOOP}. Without the {@code .metrics(...)} wiring the documented alert
 * metric {@code streamrune.outbox.delivery_failed} and the {@code streamrune.outbox.pending}
 * backlog gauge would never be emitted in production, and the gap would be invisible — the poller's
 * own unit tests inject the metrics double directly into the builder. This boots the real
 * auto-config graph and inspects the wired collaborator.
 */
class OutboxMetricsWiringAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(OutboxStore.class, InMemoryOutboxStore::new)
          .withBean(OutboxPublisher.class, () -> (OutboxPublisher) entry -> {})
          // Keep the auto-started poller idle for the test's lifetime.
          .withPropertyValues(
              "streamrune.outbox.enabled=true", "streamrune.outbox.flush-interval-ms=3600000");

  @Test
  void autoConfiguredPollerCarriesTheMicrometerMetricsBean() {
    runner
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .run(
            ctx -> {
              StreamRuneMetrics wired = metricsOf(ctx.getBean(OutboxPoller.class));
              assertThat(wired).isInstanceOf(MicrometerStreamRuneMetrics.class);
              assertThat(wired).isSameAs(ctx.getBean(StreamRuneMetrics.class));
            });
  }

  @Test
  void autoConfiguredPollerFallsBackToNoopWithoutMetricsBean() {
    runner.run(
        ctx -> {
          StreamRuneMetrics wired = metricsOf(ctx.getBean(OutboxPoller.class));
          assertThat(wired).isSameAs(StreamRuneMetrics.NOOP);
        });
  }

  @Test
  void autoConfiguredPollerReflectsOutboxRetryKnobs() {
    // The retry ladder must be configurable so an operator can widen it for a broker with
    // longer restarts. These properties must reach the poller's RetryPolicy; without the wiring the
    // poller silently used the built-in default (the only escape hatch was replacing the whole
    // bean).
    runner
        .withPropertyValues(
            "streamrune.outbox.retry-max-attempts=7",
            "streamrune.outbox.retry-initial-delay-ms=250",
            "streamrune.outbox.retry-multiplier=3.0")
        .run(
            ctx -> {
              RetryPolicy rp = retryPolicyOf(ctx.getBean(OutboxPoller.class));
              assertThat(rp.maxAttempts()).isEqualTo(7);
              assertThat(rp.initialDelay()).isEqualTo(Duration.ofMillis(250));
              assertThat(rp.backoffMultiplier()).isEqualTo(3.0);
            });
  }

  @Test
  void autoConfiguredSweeperReceivesTheSkippedWindow_andMicrometerExposesTheFourSeries() {
    // Not the shared runner: it registers an InMemoryOutboxStore as the OutboxStore bean, and a
    // second (PostgresOutboxStore) bean would make the poller's OutboxStore injection ambiguous.
    // The one PostgresOutboxStore bean satisfies both the poller's OutboxStore and the sweeper's
    // @ConditionalOnBean(PostgresOutboxStore). Its lifecycle may attempt a sweep against the mock
    // DataSource and log a failure — ResilientPollLoop absorbs it; the assertions read only the
    // constructor argument and the registered meters.
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(OutboxPublisher.class, () -> (OutboxPublisher) entry -> {})
        .withPropertyValues(
            "streamrune.outbox.enabled=true", "streamrune.outbox.flush-interval-ms=3600000")
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withBean(PostgresOutboxStore.class, () -> new PostgresOutboxStore(mock(DataSource.class)))
        .withPropertyValues("streamrune.outbox.skipped-retention-max-age=45d")
        .run(
            ctx -> {
              var sweeper = ctx.getBean(OutboxRetentionSweeper.class);
              assertThat(durationFieldOf(sweeper, "skippedMaxAge")).isEqualTo(Duration.ofDays(45));
              var registry = ctx.getBean(MeterRegistry.class);
              assertThat(registry.find("streamrune.outbox.blocked_aggregates").gauge()).isNotNull();
              assertThat(registry.find("streamrune.outbox.blockage_age_seconds").gauge())
                  .isNotNull();
              assertThat(registry.find("streamrune.outbox.skipped").counter()).isNotNull();
              assertThat(registry.find("streamrune.outbox.skipped_swept").counter()).isNotNull();
              assertThat(registry.find("streamrune.outbox.failed_swept").counter()).isNull();
            });
  }

  private static Duration durationFieldOf(Object target, String name) throws Exception {
    Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return (Duration) f.get(target);
  }

  private static StreamRuneMetrics metricsOf(OutboxPoller poller) throws Exception {
    Field f = OutboxPoller.class.getDeclaredField("metrics");
    f.setAccessible(true);
    return (StreamRuneMetrics) f.get(poller);
  }

  private static RetryPolicy retryPolicyOf(OutboxPoller poller) throws Exception {
    Field f = OutboxPoller.class.getDeclaredField("retryPolicy");
    f.setAccessible(true);
    return (RetryPolicy) f.get(poller);
  }
}

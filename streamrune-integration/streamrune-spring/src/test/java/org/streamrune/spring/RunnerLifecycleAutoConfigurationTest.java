package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.ScheduledProjectionRunner;

/**
 * Proves that the auto-configured background runners are started on context refresh and stopped on
 * context close via {@link RunnerLifecycle} — and that user-defined runner beans are left alone.
 */
class RunnerLifecycleAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  private final ApplicationContextRunner projectionRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
          .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(OffsetStore.class, () -> mock(OffsetStore.class));

  @Test
  void outboxPollerIsStartedOnRefreshAndStoppedOnClose() {
    var lifecycleRef = new AtomicReference<RunnerLifecycle>();
    runner
        .withPropertyValues("streamrune.outbox.enabled=true")
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            SpringTestMocks::outboxStoreStatingOrderingMode)
        .withBean(
            org.streamrune.core.outbox.OutboxPublisher.class,
            () -> mock(org.streamrune.core.outbox.OutboxPublisher.class))
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(OutboxPoller.class);
              var lifecycle = ctx.getBean("streamRuneOutboxPollerLifecycle", RunnerLifecycle.class);
              assertThat(lifecycle.isRunning()).isTrue();
              lifecycleRef.set(lifecycle);
            });
    assertThat(lifecycleRef.get().isRunning()).isFalse();
  }

  @Test
  void userDefinedOutboxPollerIsNeverStartedByTheFramework() {
    OutboxPoller userPoller = mock(OutboxPoller.class);
    runner
        .withPropertyValues("streamrune.outbox.enabled=true")
        .withBean(
            org.streamrune.core.outbox.OutboxStore.class,
            () -> mock(org.streamrune.core.outbox.OutboxStore.class))
        .withBean(
            org.streamrune.core.outbox.OutboxPublisher.class,
            () -> mock(org.streamrune.core.outbox.OutboxPublisher.class))
        .withBean(OutboxPoller.class, () -> userPoller)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean("streamRuneOutboxPollerLifecycle");
              verify(userPoller, never()).start();
            });
  }

  @Test
  void deadLetterRetryRunnerIsStartedOnRefreshAndStoppedOnClose() {
    // Deliberately no com.fasterxml ObjectMapper bean: Spring Boot 4 auto-configures Jackson 3,
    // so the auto-configuration must fall back to its own Jackson 2 mapper.
    var runnerRef = new AtomicReference<DeadLetterRetryRunner>();
    runner
        .withBean(
            org.streamrune.core.DeadLetterQueue.class,
            () -> mock(org.streamrune.core.DeadLetterQueue.class))
        .run(
            ctx -> {
              var retryRunner = ctx.getBean(DeadLetterRetryRunner.class);
              assertThat(retryRunner.isRunning()).isTrue();
              assertThat(ctx).hasBean("streamRuneDeadLetterRetryRunnerLifecycle");
              runnerRef.set(retryRunner);
            });
    assertThat(runnerRef.get().isRunning()).isFalse();
  }

  @Test
  void userDefinedDeadLetterRetryRunnerIsNeverStartedByTheFramework() {
    DeadLetterRetryRunner userRunner = mock(DeadLetterRetryRunner.class);
    runner
        .withBean(
            org.streamrune.core.DeadLetterQueue.class,
            () -> mock(org.streamrune.core.DeadLetterQueue.class))
        .withBean(DeadLetterRetryRunner.class, () -> userRunner)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean("streamRuneDeadLetterRetryRunnerLifecycle");
              verify(userRunner, never()).start();
            });
  }

  @Test
  void multiProjectionRunnerIsStartedOnRefreshAndStoppedOnClose() {
    var lifecycleRef = new AtomicReference<RunnerLifecycle>();
    projectionRunner
        .withBean("continuousProjection", Projection.class, ContinuousProjection::new)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              var lifecycle =
                  ctx.getBean("streamRuneMultiProjectionRunnerLifecycle", RunnerLifecycle.class);
              assertThat(lifecycle.isRunning()).isTrue();
              lifecycleRef.set(lifecycle);
            });
    assertThat(lifecycleRef.get().isRunning()).isFalse();
  }

  @Test
  void scheduledProjectionRunnerIsStartedOnRefreshAndStoppedOnClose() {
    var lifecycleRef = new AtomicReference<RunnerLifecycle>();
    projectionRunner
        .withBean("scheduledProjection", Projection.class, ScheduledProjection::new)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              var lifecycle =
                  ctx.getBean(
                      "streamRuneScheduledProjectionRunnerLifecycle", RunnerLifecycle.class);
              assertThat(lifecycle.isRunning()).isTrue();
              lifecycleRef.set(lifecycle);
            });
    assertThat(lifecycleRef.get().isRunning()).isFalse();
  }

  @Test
  void userDefinedMultiProjectionRunnerIsNeverStartedByTheFramework() {
    MultiProjectionRunner userRunner = mock(MultiProjectionRunner.class);
    projectionRunner
        .withBean("continuousProjection", Projection.class, ContinuousProjection::new)
        .withBean(MultiProjectionRunner.class, () -> userRunner)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean("streamRuneMultiProjectionRunnerLifecycle");
              verify(userRunner, never()).start();
            });
  }

  @ProjectionConfig(
      name = "lifecycle_continuous",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class ContinuousProjection implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "lifecycle-scheduled",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 0 1 1 *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class ScheduledProjection implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }
}

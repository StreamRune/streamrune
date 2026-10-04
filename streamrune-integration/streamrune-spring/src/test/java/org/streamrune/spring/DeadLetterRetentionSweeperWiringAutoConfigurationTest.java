package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeadLetterRetryRunner;

/**
 * The command dead-letter queue's retention sweeper is a storage-limitation control, so it follows
 * the {@link DeadLetterQueue} bean and {@code streamrune.dead-letter.retention-max-age} — never the
 * {@code streamrune.dead-letter.enabled} switch, which governs only the automatic retry runner. The
 * command bus keeps dead-lettering whatever that switch says, so a sweeper that vanished with it
 * would leave every exhausted command's payload in {@code dead_letter_queue} for ever.
 */
class DeadLetterRetentionSweeperWiringAutoConfigurationTest {

  private final DeadLetterQueue deadLetterQueue = mock(DeadLetterQueue.class);

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  @Test
  void sweeperIsRegisteredAndStartedByDefaultWhenAQueueBeanExists() {
    runner
        .withBean(DeadLetterQueue.class, () -> deadLetterQueue)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(DeadLetterRetentionSweeper.class);
              assertThat(ctx.containsBean("streamRuneDeadLetterRetentionSweeperLifecycle"))
                  .as("the sweeper must be started by a RunnerLifecycle on context refresh")
                  .isTrue();
              assertThat(ctx.getBean(DeadLetterRetentionSweeper.class).isStarted())
                  .as("the default 30-day retention starts the sweep thread")
                  .isTrue();
            });
  }

  @Test
  void sweeperSurvivesDisablingTheAutomaticRetryRunner() {
    runner
        .withPropertyValues("streamrune.dead-letter.enabled=false")
        .withBean(DeadLetterQueue.class, () -> deadLetterQueue)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx)
                  .as("dead-letter.enabled=false turns automatic replays off, nothing else")
                  .doesNotHaveBean(DeadLetterRetryRunner.class);
              assertThat(ctx)
                  .as("the bus still dead-letters, so the retention sweeper must still exist")
                  .hasSingleBean(DeadLetterRetentionSweeper.class);
              assertThat(ctx.containsBean("streamRuneDeadLetterRetentionSweeperLifecycle"))
                  .as("and it must still be started on context refresh")
                  .isTrue();
              assertThat(ctx.getBean(DeadLetterRetentionSweeper.class).isStarted()).isTrue();
              assertThat(ctx.getBean(BackgroundRelayHealthContributor.class).components())
                  .as("and its liveness must still be reported through the relay health")
                  .anyMatch(c -> c.name().equals("dead-letter-retention-sweeper"))
                  .noneMatch(c -> c.name().equals("dead-letter-retry"));
            });
  }

  @Test
  void aZeroRetentionMaxAgeIsTheSwitchThatTurnsPruningOff() {
    runner
        .withPropertyValues("streamrune.dead-letter.retention-max-age=0")
        .withBean(DeadLetterQueue.class, () -> deadLetterQueue)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(DeadLetterRetentionSweeper.class);
              assertThat(ctx.getBean(DeadLetterRetentionSweeper.class).isStarted())
                  .as("zero retention starts no thread — pruning is off, by an explicit choice")
                  .isFalse();
            });
  }

  @Test
  void noSweeperWithoutAQueueBean() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx).doesNotHaveBean(DeadLetterRetentionSweeper.class);
        });
  }

  @Test
  void aQueueTheApplicationSuppliesItselfIsSweptToo() {
    // The framework registers no DeadLetterQueue of its own, so every queue is an application
    // bean; the sweeper must follow whichever one the command bus dead-letters into.
    runner
        .withUserConfiguration(UserQueueConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(DeadLetterRetentionSweeper.class);
            });
  }

  @Test
  void anApplicationSweeperReplacesTheFrameworkOne() {
    runner
        .withUserConfiguration(UserQueueConfig.class, UserSweeperConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(DeadLetterRetentionSweeper.class);
              assertThat(ctx.getBean(DeadLetterRetentionSweeper.class))
                  .isSameAs(UserSweeperConfig.USER_SWEEPER);
              assertThat(ctx.containsBean("streamRuneDeadLetterRetentionSweeper")).isFalse();
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class UserQueueConfig {
    @Bean
    DeadLetterQueue applicationDeadLetterQueue() {
      return mock(DeadLetterQueue.class);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class UserSweeperConfig {
    static final DeadLetterRetentionSweeper USER_SWEEPER = mock(DeadLetterRetentionSweeper.class);

    @Bean
    DeadLetterRetentionSweeper applicationSweeper() {
      return USER_SWEEPER;
    }
  }
}

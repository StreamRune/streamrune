package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;

/**
 * Container-level wiring coverage for the saga dead-letter retention sweeper.
 *
 * <p>The sweeper is gated on the <em>concrete</em> {@link PostgresSagaDeadLetterStore} type while
 * the framework's own default store bean used to declare the {@link SagaDeadLetterStore}
 * <em>interface</em> as its factory-method return type. Spring's {@code @ConditionalOnBean}
 * predicts bean types from the declared return type before instantiation, so the gate never matched
 * the framework's own bean: the sweeper was silently absent in <strong>every</strong> default
 * deployment and {@code saga_dead_letters} grew without bound (a GDPR storage-limitation violation
 * the sweeper exists to bound). The boot validators only check the <em>store</em> by interface
 * type, so they passed while the sweeper was missing, and every other test hand-feeds the store,
 * which is exactly how real container type-resolution went unexercised.
 *
 * <p>These tests therefore run the <em>real</em> auto-configuration through an {@link
 * ApplicationContextRunner} with nothing but a mock {@code DataSource} — the default deployment
 * shape — and assert on the sweeper, not the store.
 */
class SagaDeadLetterSweeperWiringAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(DataSource.class, () -> mock(DataSource.class));

  @Test
  void sweeperIsRegisteredAndStartedInTheDefaultDataSourceConfiguration() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx)
              .as(
                  "the saga dead-letter retention sweeper must exist in the default configuration —"
                      + " without it saga_dead_letters is never pruned")
              .hasSingleBean(SagaDeadLetterRetentionSweeper.class);
          // The lifecycle adapter is what actually calls start()/close(); its presence (plus a
          // successfully refreshed context, which drives SmartLifecycle.start()) proves the sweeper
          // is started, not merely constructed.
          assertThat(ctx.containsBean("streamRuneSagaDeadLetterRetentionSweeperLifecycle"))
              .as("the sweeper must be started by a RunnerLifecycle on context refresh")
              .isTrue();
        });
  }

  @Test
  void defaultStoreIsResolvableByItsConcreteTypeAndStillSatisfiesTheInterface() {
    runner.run(
        ctx -> {
          assertThat(ctx)
              .as(
                  "the default store must be resolvable by its concrete type — that is what the"
                      + " sweeper's @ConditionalOnBean gate and constructor parameter ask for")
              .hasSingleBean(PostgresSagaDeadLetterStore.class);
          assertThat(ctx)
              .as("the concrete store must still satisfy interface-typed consumers")
              .hasSingleBean(SagaDeadLetterStore.class);
          assertThat(ctx.getBean(SagaDeadLetterStore.class))
              .isSameAs(ctx.getBean(PostgresSagaDeadLetterStore.class));
        });
  }

  @Test
  void userSuppliedStoreOverridesTheDefaultAndOwnsItsOwnRetention() {
    // Override semantics are deliberately declared on the INTERFACE
    // (@ConditionalOnMissingBean(SagaDeadLetterStore.class)), so a user-supplied interface-typed
    // bean still replaces the concrete-typed default. The framework then registers no sweeper: it
    // sweeps only the Postgres store it created itself, never a store it does not own (the same
    // rule the inbox sweeper follows via @ConditionalOnBean(PostgresCommandInbox.class)). A user
    // who brings their own store owns its retention.
    runner
        .withUserConfiguration(UserStoreConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx)
                  .as("a user-supplied interface-typed store must suppress the framework default")
                  .doesNotHaveBean(PostgresSagaDeadLetterStore.class);
              assertThat(ctx.getBean(SagaDeadLetterStore.class))
                  .isSameAs(UserStoreConfig.USER_STORE);
              assertThat(ctx)
                  .as("the framework must not sweep a store it did not create")
                  .doesNotHaveBean(SagaDeadLetterRetentionSweeper.class);
            });
  }

  @Test
  void neitherStoreNorSweeperExistWhenSagasAreDisabled() {
    runner
        .withPropertyValues("streamrune.saga.enabled=false")
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean(PostgresSagaDeadLetterStore.class);
              assertThat(ctx).doesNotHaveBean(SagaDeadLetterRetentionSweeper.class);
            });
  }

  @Test
  void noSweeperWithoutADataSource() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean(PostgresSagaDeadLetterStore.class);
              assertThat(ctx).doesNotHaveBean(SagaDeadLetterRetentionSweeper.class);
            });
  }

  @Configuration
  static class UserStoreConfig {

    static final SagaDeadLetterStore USER_STORE = mock(SagaDeadLetterStore.class);

    @Bean
    SagaDeadLetterStore userSagaDeadLetterStore() {
      return USER_STORE;
    }
  }

  @Test
  void sweeperIsRegisteredWithTheRelayHealthContributor() {
    // The auto-configured sweeper must be registered with the
    // BackgroundRelayHealthContributor
    // so a sweep thread killed by an Error reports DOWN instead of silently freezing the
    // streamrune.saga.faulted_backlog gauge it is the sole sampler of.
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var relayHealth =
              ctx.getBean(org.streamrune.runtime.BackgroundRelayHealthContributor.class);
          assertThat(relayHealth.components())
              .as("the started saga dead-letter retention sweeper is a registered relay component")
              .anySatisfy(
                  c -> {
                    assertThat(c.name()).isEqualTo("saga-dead-letter-retention-sweeper");
                    assertThat(c.started()).isTrue();
                  });
        });
  }
}

package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * Proves that the auto-configured {@link PostgresEventStoreFactory} receives the {@link
 * CryptoEngine} and {@link EventUpcaster} beans, and that a {@link JdbcProjectionRepository} bean
 * is wired into the projection runner as its atomic batch processor when a registration declares
 * TRANSACTIONAL_LOCAL — and only then.
 */
class EventStoreWiringAutoConfigurationTest {

  /**
   * No EventStoreFactory override here: the real {@link PostgresEventStoreFactory} bean must be
   * built so its wiring can be inspected. EventStore IS overridden so {@code factory.create()}
   * never runs against the mock DataSource.
   */
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

  @Test
  void cryptoEngineBeanIsWiredIntoEventStoreFactory() {
    runner
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              assertThat(factory).isInstanceOf(PostgresEventStoreFactory.class);
              assertThat(ReflectionTestUtils.getField(factory, "cryptoEngine"))
                  .isSameAs(ctx.getBean(CryptoEngine.class));
            });
  }

  @Test
  void noCryptoEngineBeanLeavesFactoryUnencrypted() {
    runner.run(
        ctx -> {
          var factory = ctx.getBean(EventStoreFactory.class);
          assertThat(ReflectionTestUtils.getField(factory, "cryptoEngine")).isNull();
        });
  }

  @Test
  void ambiguousCryptoEnginesFailStartupInsteadOfPersistingPlaintext() {
    runner
        .withUserConfiguration(TwoCryptoEnginesConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("PLAINTEXT")
                  .hasStackTraceContaining("@Primary");
            });
  }

  @Test
  void primaryCryptoEngineResolvesAmbiguity() {
    runner
        .withUserConfiguration(PrimaryCryptoEngineConfig.class)
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              assertThat(ReflectionTestUtils.getField(factory, "cryptoEngine"))
                  .isSameAs(ctx.getBean("primaryEngine"));
            });
  }

  @Test
  void eventUpcasterBeansAreWiredIntoEventStoreFactory() {
    EventUpcaster upcaster = upcaster("TestEvent");
    runner
        .withBean(EventUpcaster.class, () -> upcaster)
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              @SuppressWarnings("unchecked")
              var upcasters =
                  (List<EventUpcaster>) ReflectionTestUtils.getField(factory, "upcasters");
              assertThat(upcasters).containsExactly(upcaster);
            });
  }

  @Test
  void snapshotMigrationBeansAreWiredIntoEventStoreFactory() {
    SnapshotMigration migration = mock(SnapshotMigration.class);
    runner
        .withBean(SnapshotMigration.class, () -> migration)
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              @SuppressWarnings("unchecked")
              var migrations =
                  (List<SnapshotMigration>)
                      ReflectionTestUtils.getField(factory, "snapshotMigrations");
              assertThat(migrations).containsExactly(migration);
            });
  }

  @Test
  void jdbcProjectionRepositoryIsAutoConfiguredWithDataSource() {
    runner.run(ctx -> assertThat(ctx).hasSingleBean(JdbcProjectionRepository.class));
  }

  // ── the advertised CommandInbox override must not become a runtime trap ─────────────

  /**
   * A user-supplied {@link org.streamrune.core.CommandInbox} suppresses the framework's {@code
   * PostgresCommandInbox} default (the bean javadoc advertises exactly that), so the event-store
   * factory's concrete-typed {@code ObjectProvider<PostgresCommandInbox>} resolves nothing and the
   * store is built WITHOUT an inbox — while the command bus, which injects the INTERFACE, happily
   * takes the user's. The app booted green and then every keyed execution, including the saga
   * executor's idempotency-keyed dispatch, failed at runtime inside {@code appendWithKey} with
   * UnsupportedOperationException, dead-lettering healthy commands. It must fail fast at wiring
   * time instead.
   */
  @Test
  void customCommandInboxWithTheFrameworkEventStoreFactoryFailsFast() {
    runner
        .withBean(
            org.streamrune.core.CommandInbox.class, org.streamrune.test.InMemoryCommandInbox::new)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("command-inbox wiring")
                  .hasStackTraceContaining("appendWithKey")
                  .hasStackTraceContaining("EventStoreFactory");
            });
  }

  /**
   * The legitimate fully-custom composition: the application owns BOTH the inbox and the event
   * store, so its {@code appendWithKey} can honour the inbox atomically. The framework factory is
   * never created, so the check never runs and the app must still boot.
   */
  @Test
  void customCommandInboxWithACustomEventStoreFactoryStillBoots() {
    runner
        .withBean(
            org.streamrune.core.CommandInbox.class, org.streamrune.test.InMemoryCommandInbox::new)
        .withBean(EventStoreFactory.class, () -> mock(EventStoreFactory.class))
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  /** The default configuration: bus and store share the framework's own inbox. */
  @Test
  void defaultCommandInboxIsSharedByTheBusAndTheEventStore() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var factory = ctx.getBean(EventStoreFactory.class);
          assertThat(ReflectionTestUtils.getField(factory, "commandInbox"))
              .as("the framework store must be built with the same inbox bean the bus resolves")
              .isSameAs(ctx.getBean(org.streamrune.core.CommandInbox.class));
        });
  }

  @Test
  void userDefinedProjectionRepositoryBacksOffAutoConfiguredOne() {
    runner
        .withBean(ProjectionRepository.class, () -> mock(ProjectionRepository.class))
        .run(ctx -> assertThat(ctx).doesNotHaveBean(JdbcProjectionRepository.class));
  }

  @Test
  void jdbcProjectionRepositoryIsWiredAsAtomicBatchProcessorOnProjectionRunner() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    projectionRunnerOver(repository)
        .withBean(
            "wiringTransactionalProjection", Projection.class, TransactionalWiringProjection::new)
        .run(
            ctx -> {
              var projectionRunner = ctx.getBean(MultiProjectionRunner.class);
              assertThat(ReflectionTestUtils.getField(projectionRunner, "atomicProcessor"))
                  .isSameAs(repository);
            });
  }

  /**
   * The bean's presence selects nothing on its own: a runner whose registrations are all
   * AT_LEAST_ONCE_IDEMPOTENT, without leadership, runs on the nonatomic processor even when a
   * JdbcProjectionRepository bean exists.
   */
  @Test
  void jdbcProjectionRepositoryIsNotWiredIntoAnAtLeastOnceOnlyRunner() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    projectionRunnerOver(repository)
        .withBean(
            "wiringContinuousProjection",
            Projection.class,
            RunnerLifecycleAutoConfigurationTest.ContinuousProjection::new)
        .run(
            ctx -> {
              var projectionRunner = ctx.getBean(MultiProjectionRunner.class);
              assertThat(ReflectionTestUtils.getField(projectionRunner, "atomicProcessor"))
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  private static ApplicationContextRunner projectionRunnerOver(
      JdbcProjectionRepository repository) {
    return new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(OffsetStore.class, () -> mock(OffsetStore.class))
        .withBean(JdbcProjectionRepository.class, () -> repository);
  }

  /** Writes through the handed repository (a no-op here), so TRANSACTIONAL_LOCAL is accepted. */
  @ProjectionConfig(
      name = "wiring_transactional",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class TransactionalWiringProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}
  }

  private static EventUpcaster upcaster(String eventType) {
    EventUpcaster upcaster = mock(EventUpcaster.class);
    when(upcaster.eventType()).thenReturn(new EventType(eventType));
    when(upcaster.currentVersion()).thenReturn(2);
    return upcaster;
  }

  @Configuration
  static class TwoCryptoEnginesConfig {
    @Bean
    CryptoEngine engineA() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine engineB() {
      return new InMemoryCryptoEngine();
    }
  }

  @Configuration
  static class PrimaryCryptoEngineConfig {
    @Bean
    @Primary
    CryptoEngine primaryEngine() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine secondaryEngine() {
      return new InMemoryCryptoEngine();
    }
  }
}

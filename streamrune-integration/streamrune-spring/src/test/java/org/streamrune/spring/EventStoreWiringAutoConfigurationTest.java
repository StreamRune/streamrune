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
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * Proves that the auto-configured {@link PostgresEventStoreFactory} receives the {@link
 * CryptoEngine} and {@link EventUpcaster} beans, that a {@code DataSource} brings the default
 * read-model store and the Postgres subscription checkpoint, and that the projection runner routes
 * a TRANSACTIONAL_LOCAL registration to the store and an at-least-once one with no write target to
 * the subscription checkpoint.
 */
class EventStoreWiringAutoConfigurationTest {

  /**
   * No EventStoreFactory override here: the real {@link PostgresEventStoreFactory} bean must be
   * built so its wiring can be inspected. EventStore IS overridden so {@code factory.create()}
   * never runs against the mock DataSource.
   */
  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
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

  /**
   * The default store is declared, lazily — creating it reads the server version, which the mock
   * DataSource cannot answer — and the subscription checkpoint pairs the auto-configured {@code
   * PostgresOffsetStore} with the fencing Postgres processor over the same table.
   */
  @Test
  void aDataSourceBringsTheLazyDefaultStoreAndThePostgresSubscriptionCheckpoint() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var store = ctx.getBeanFactory().getBeanDefinition("postgres");
          assertThat(store.isLazyInit()).as("the default store is created on first use").isTrue();
          assertThat(ctx.getBeanFactory().getType("postgres"))
              .isEqualTo(PostgresReadModelStore.class);
          var checkpoint =
              ctx.getBean(
                  StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT, CheckpointTarget.class);
          assertThat(checkpoint.name()).isEqualTo("subscription_checkpoint");
          assertThat(checkpoint.checkpoints()).isSameAs(ctx.getBean(PostgresOffsetStore.class));
          assertThat(checkpoint.processor())
              .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
          assertThat(checkpoint.processor().fencesCheckpoint()).isTrue();
          assertThat(ReflectionTestUtils.getField(checkpoint.processor(), "dataSource"))
              .as("the processor is built on the DataSource its offset store reads")
              .isSameAs(ReflectionTestUtils.getField(checkpoint.checkpoints(), "dataSource"));
        });
  }

  /**
   * An offset store the application defines is never paired with the Postgres processor, which
   * saves into its own table: the subscription checkpoint saves into that offset store instead.
   */
  @Test
  void anApplicationOffsetStore_isPairedWithTheNonatomicProcessor() {
    var offsetStore = mock(OffsetStore.class);
    runner
        .withBean(OffsetStore.class, () -> offsetStore)
        .run(
            ctx -> {
              var checkpoint =
                  ctx.getBean(
                      StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT, CheckpointTarget.class);
              assertThat(checkpoint.checkpoints()).isSameAs(offsetStore);
              assertThat(checkpoint.processor())
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  /**
   * A PostgresOffsetStore the application defines on its own DataSource — here with no {@code
   * DataSource} bean at all — gets the Postgres processor on that same DataSource: the bean looks
   * up no DataSource of its own.
   */
  @Test
  void anApplicationPostgresOffsetStore_getsTheProcessorOnItsOwnDataSource() {
    DataSource own = mock(DataSource.class);
    var offsetStore = new PostgresOffsetStore(own);
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(OffsetStore.class, () -> offsetStore)
        .run(
            ctx -> {
              var checkpoint =
                  ctx.getBean(
                      StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT, CheckpointTarget.class);
              assertThat(checkpoint.checkpoints()).isSameAs(offsetStore);
              assertThat(checkpoint.processor())
                  .isInstanceOf(PostgresSubscriptionCheckpointProcessor.class);
              assertThat(ReflectionTestUtils.getField(checkpoint.processor(), "dataSource"))
                  .isSameAs(own);
            });
  }

  @Test
  void withoutADataSource_theSubscriptionCheckpointIsTheOffsetStoreBeanAndTheNonatomicProcessor() {
    var offsetStore = mock(OffsetStore.class);
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(OffsetStore.class, () -> offsetStore)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean(ReadModelStore.class);
              assertThat(ctx).doesNotHaveBean(ProjectionRepository.class);
              var checkpoint =
                  ctx.getBean(
                      StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT, CheckpointTarget.class);
              assertThat(checkpoint.checkpoints()).isSameAs(offsetStore);
              assertThat(checkpoint.processor())
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  @Test
  void withNeitherADataSourceNorAnOffsetStore_thereIsNoSubscriptionCheckpoint() {
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .run(
            ctx ->
                assertThat(ctx.getBeanProvider(CheckpointTarget.class).getIfAvailable()).isNull());
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
    var repository = mock(ProjectionRepository.class);
    runner
        .withBean(ProjectionRepository.class, () -> repository)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean("streamRuneProjectionRepository");
              assertThat(ctx.getBean(ProjectionRepository.class)).isSameAs(repository);
            });
  }

  /**
   * Any store the application defines replaces the default one. Without a store named {@code
   * postgres} there is no default repository to inject, and asking for one says so.
   */
  @Test
  void userDefinedReadModelStoreBacksOffTheDefaultStore() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    runner
        .withBean(
            ReadModelStore.class,
            () -> ReadModelStore.of("search", repository, mock(OffsetStore.class), repository))
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean("postgres");
              assertThat(ctx.getBean(ReadModelStore.class).name()).isEqualTo("search");
              org.assertj.core.api.Assertions.assertThatThrownBy(
                      () -> ctx.getBean(ProjectionRepository.class))
                  .hasStackTraceContaining("No read-model store is named 'postgres'")
                  .hasStackTraceContaining("'search'");
            });
  }

  /** The default store's repository is the injectable ProjectionRepository. */
  @Test
  void theProjectionRepositoryBeanIsTheDefaultStoresRepository() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    runner
        .withBean(
            "postgres",
            ReadModelStore.class,
            () -> ReadModelStore.of("postgres", repository, mock(OffsetStore.class), repository))
        .run(ctx -> assertThat(ctx.getBean(ProjectionRepository.class)).isSameAs(repository));
  }

  /**
   * A TRANSACTIONAL_LOCAL registration with no write target goes to the one transactional store.
   */
  @Test
  void aTransactionalRegistrationIsRoutedToTheTransactionalStore() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    projectionRunnerOver(repository)
        .withBean(
            "wiringTransactionalProjection", Projection.class, TransactionalWiringProjection::new)
        .run(
            ctx -> {
              var projectionRunner = ctx.getBean(MultiProjectionRunner.class);
              assertThat(processorOf(projectionRunner, "wiring_transactional"))
                  .isSameAs(repository);
            });
  }

  /**
   * The store's presence routes nothing on its own: an AT_LEAST_ONCE_IDEMPOTENT registration with
   * no write target goes to the subscription checkpoint even when a store exists.
   */
  @Test
  void anAtLeastOnceRegistrationWithNoWriteTargetIsNotRoutedToTheStore() {
    var repository = new JdbcProjectionRepository(mock(DataSource.class));
    projectionRunnerOver(repository)
        .withBean(
            "wiringContinuousProjection",
            Projection.class,
            RunnerLifecycleAutoConfigurationTest.ContinuousProjection::new)
        .run(
            ctx -> {
              var projectionRunner = ctx.getBean(MultiProjectionRunner.class);
              assertThat(processorOf(projectionRunner, "lifecycle_continuous"))
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  private static Object processorOf(MultiProjectionRunner projectionRunner, String projection) {
    var pairs = (java.util.Map<?, ?>) ReflectionTestUtils.getField(projectionRunner, "checkpoints");
    return ReflectionTestUtils.invokeMethod(
        pairs.get(org.streamrune.core.types.ProjectionName.of(projection)), "processor");
  }

  private static ApplicationContextRunner projectionRunnerOver(
      JdbcProjectionRepository repository) {
    return new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(
            StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT,
            CheckpointTarget.class,
            SpringTestMocks::subscriptionCheckpoint)
        .withBean(
            ReadModelStore.class,
            () -> ReadModelStore.of("postgres", repository, mock(OffsetStore.class), repository));
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

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
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

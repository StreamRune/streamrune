package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.aws.AwsKmsCryptoEngine;
import org.streamrune.crypto.postgres.JdbcForgottenSubjectStore;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.PostgresBackend;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * The Spring integration builds every store through the backend SPI: one {@link StreamRuneBackend}
 * chosen by {@code streamrune.backend}, one {@link BackendStores} bundle opened on the {@code
 * DataSource} and closed with the context, and a {@link CryptoBackend} for the crypto engines. The
 * beans keep their names and are declared with their SPI interface types.
 */
class SpringBackendWiringTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues(
              SpringTestMocks.NO_SNAPSHOTS,
              "streamrune.event-audit-enabled=true",
              "streamrune.projection-dlq-enabled=true")
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

  /**
   * Each formerly Postgres-typed bean, by its unchanged name, and the interface it now declares.
   */
  private static final Map<String, Class<?>> DECLARED_TYPES =
      Map.ofEntries(
          Map.entry("postgresEventStoreFactory", EventStoreFactory.class),
          Map.entry("sagaStore", SagaStore.class),
          Map.entry("sagaDeadLetterStore", SagaDeadLetterStore.class),
          Map.entry("commandInbox", CommandInbox.class),
          Map.entry("postgresAuditStore", AuditStore.class),
          Map.entry("complianceReportQuery", ComplianceReportQuery.class),
          Map.entry("commandAuditQuery", CommandAuditQuery.class),
          Map.entry("postgresEventAuditStore", EventAuditStore.class),
          Map.entry("postgresEventAuditQuery", EventAuditQuery.class),
          Map.entry("postgresProjectionDeadLetterStore", ProjectionDeadLetterStore.class),
          Map.entry("streamRuneSubscriptionLeadership", SubscriptionLeadership.class),
          Map.entry(
              StreamRuneAutoConfiguration.DEFAULT_READ_MODEL_STORE_BEAN, ReadModelStore.class),
          Map.entry("postgresOffsetStore", OffsetStore.class));

  @Test
  void theBackendStoresBeanIsThePostgresBundle() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx.getBean("streamRuneBackend", StreamRuneBackend.class))
              .isInstanceOf(PostgresBackend.class);
          assertThat(ctx.getBean("streamRuneBackendStores", BackendStores.class).backendName())
              .isEqualTo("postgres");
        });
  }

  @Test
  void theCommandInboxBeanIsDeclaredAsTheInterface() {
    runner.run(ctx -> assertThat(declaredType(ctx, "commandInbox")).isEqualTo(CommandInbox.class));
  }

  @Test
  void everyFormerPostgresBeanKeepsItsNameAndDeclaresItsInterface() {
    runner.run(
        ctx ->
            DECLARED_TYPES.forEach(
                (name, type) ->
                    assertThat(declaredType(ctx, name))
                        .as("declared type of %s", name)
                        .isEqualTo(type)));
  }

  @Test
  void theStoresAreTheOnesTheBundleBuilds() {
    runner.run(
        ctx -> {
          BackendStores stores = ctx.getBean(BackendStores.class);
          assertThat(ctx.getBean("commandInbox")).isSameAs(stores.commandInbox());
          assertThat(ctx.getBean("sagaStore")).isSameAs(stores.sagaStore());
          assertThat(ctx.getBean("sagaDeadLetterStore")).isSameAs(stores.sagaDeadLetterStore());
          assertThat(ctx.getBean("postgresAuditStore")).isSameAs(stores.auditStore());
          assertThat(ctx.getBean("complianceReportQuery")).isSameAs(stores.complianceReportQuery());
          assertThat(ctx.getBean("commandAuditQuery")).isSameAs(stores.commandAuditQuery());
          assertThat(ctx.getBean("postgresEventAuditStore")).isSameAs(stores.eventAuditStore());
          assertThat(ctx.getBean("postgresEventAuditQuery")).isSameAs(stores.eventAuditQuery());
          assertThat(ctx.getBean("postgresProjectionDeadLetterStore"))
              .isSameAs(stores.projectionDeadLetterStore());
          assertThat(ctx.getBean("postgresOffsetStore")).isSameAs(stores.offsetStore());
          assertThat(ctx.getBean("streamRuneSubscriptionLeadership"))
              .isSameAs(
                  stores.leadership(
                      ctx.getBean(StreamRuneProperties.class)
                          .subscription()
                          .singleActiveConsumer()
                          .leaseTtl()));
          // The event store factory is the bundle's, built with the stores it hands out.
          var factory = ctx.getBean(EventStoreFactory.class);
          assertThat(ReflectionTestUtils.getField(factory, "commandInbox"))
              .isSameAs(stores.commandInbox());
          assertThat(ReflectionTestUtils.getField(factory, "eventAuditStore"))
              .isSameAs(stores.eventAuditStore());
        });
  }

  @Test
  void theDefaultReadModelStoreIsNamedAfterTheBackend() {
    runner.run(
        ctx -> {
          BackendStores stores = ctx.getBean(BackendStores.class);
          assertThat(ctx.getBean(ReadModelStores.class).defaultStoreName())
              .isEqualTo(stores.backendName());
        });
  }

  @Test
  void anUnknownBackendFailsTheStartWithTheSelectionMessage() {
    runner
        .withPropertyValues("streamrune.backend=oracle")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining(
                      "streamrune.backend='oracle' is not a backend; valid values: postgres, sqlite");
            });
  }

  @Test
  void theBackendStoresAreClosedOnceWhenTheContextCloses() {
    // Spring calls a destroy method on the bean as its factory method returned it, so the spy is
    // made where the bundle is born: the backend's open().
    AtomicReference<BackendStores> opened = new AtomicReference<>();
    runner
        .withBean(
            "spyOpenedBackendStores",
            BeanPostProcessor.class,
            () ->
                new BeanPostProcessor() {
                  @Override
                  public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!"streamRuneBackend".equals(beanName)) {
                      return bean;
                    }
                    StreamRuneBackend real = (StreamRuneBackend) bean;
                    return new StreamRuneBackend() {
                      @Override
                      public String name() {
                        return real.name();
                      }

                      @Override
                      public void verify(DataSource dataSource) {
                        real.verify(dataSource);
                      }

                      @Override
                      public BackendStores open(BackendContext context) {
                        BackendStores stores = spy(real.open(context));
                        opened.set(stores);
                        return stores;
                      }
                    };
                  }
                })
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(BackendStores.class)).isSameAs(opened.get());
              verify(opened.get(), never()).close();
            });
    verify(opened.get(), times(1)).close();
  }

  // ── the retention sweepers sweep only the framework's own stores ──────────────────────

  @Test
  void theDefaultContextHasBothRetentionSweepers() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx).hasBean("streamRuneInboxRetentionSweeper");
          assertThat(ctx).hasBean("streamRuneSagaDeadLetterRetentionSweeper");
          assertThat(ctx.getBean(InboxRetentionSweeper.class)).isNotNull();
          assertThat(ctx.getBean(SagaDeadLetterRetentionSweeper.class)).isNotNull();
        });
  }

  /**
   * An application's plain {@code @Bean SagaDeadLetterStore sagaDeadLetterStore()} carries the
   * framework bean's name, but it is the application's store: the framework must not prune it.
   */
  @Test
  void anApplicationSagaDeadLetterStoreUnderTheFrameworkNameGetsNoSweeper() {
    runner
        .withUserConfiguration(ApplicationSagaDeadLetterStore.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(SagaDeadLetterStore.class))
                  .isSameAs(ApplicationSagaDeadLetterStore.STORE);
              assertThat(ctx).doesNotHaveBean(SagaDeadLetterRetentionSweeper.class);
            });
  }

  /**
   * The same for an application's plain {@code @Bean CommandInbox commandInbox()}; it brings its
   * own event store factory, so the backend's foreign-store refusal does not stop the start first.
   */
  @Test
  void anApplicationCommandInboxUnderTheFrameworkNameGetsNoSweeper() {
    runner
        .withUserConfiguration(ApplicationCommandInbox.class)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(CommandInbox.class)).isSameAs(ApplicationCommandInbox.INBOX);
              assertThat(ctx).doesNotHaveBean(InboxRetentionSweeper.class);
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationSagaDeadLetterStore {
    static final SagaDeadLetterStore STORE = mock(SagaDeadLetterStore.class);

    @Bean
    SagaDeadLetterStore sagaDeadLetterStore() {
      return STORE;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ApplicationCommandInbox {
    static final CommandInbox INBOX = new InMemoryCommandInbox();

    @Bean
    CommandInbox commandInbox() {
      return INBOX;
    }
  }

  // ── an application store of a foreign type fails the start ────────────────────────────

  @Test
  void aForeignOutboxStoreWithAMapperFailsTheStartWithTheR1Refusal() {
    runner
        .withBean(OutboxStore.class, InMemoryOutboxStore::new)
        .withBean(OutboxEventMapper.class, () -> event -> List.of())
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasRootCauseInstanceOf(IllegalArgumentException.class)
                  .hasStackTraceContaining(
                      "outbox store of backend 'InMemoryOutboxStore' cannot share the 'postgres'"
                          + " event store's transaction; build it from this backend's"
                          + " BackendStores");
            });
  }

  @Test
  void anApplicationDeclaredPostgresOutboxStoreIsStillWired() {
    var outboxStore = new PostgresOutboxStore(mock(DataSource.class));
    OutboxEventMapper mapper = event -> List.of();
    runner
        .withBean(PostgresOutboxStore.class, () -> outboxStore)
        .withBean(OutboxEventMapper.class, () -> mapper)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var factory = ctx.getBean(EventStoreFactory.class);
              assertThat(ReflectionTestUtils.getField(factory, "outboxStore"))
                  .isSameAs(outboxStore);
              assertThat(ReflectionTestUtils.getField(factory, "outboxEventMapper"))
                  .isSameAs(mapper);
            });
  }

  // ── crypto: the engines take their stores and signals from the CryptoBackend ──────────

  private final ApplicationContextRunner kmsRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  CryptoPropertiesConfiguration.class, AwsKmsCryptoEngineConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withPropertyValues(
              "streamrune.crypto.aws.enabled=true",
              "streamrune.crypto.aws.region=us-west-2",
              "streamrune.crypto.aws.kms-key-id=arn:aws:kms:us-west-2:123456789012:key/test-key-id");

  @Test
  void theCryptoBackendBeanIsThePostgresOneAndExistsOnlyWithAnEngine() {
    kmsRunner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx.getBean("streamRuneCryptoBackend", CryptoBackend.class))
              .isInstanceOf(PostgresCryptoBackend.class);
          var store = forgottenSubjectsOf(ctx.getBean(CryptoEngine.class));
          assertThat(store).isInstanceOf(JdbcForgottenSubjectStore.class);
          assertThat(ReflectionTestUtils.getField(store, "dataSource"))
              .isSameAs(ctx.getBean(DataSource.class));
        });
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                CryptoPropertiesConfiguration.class, AwsKmsCryptoEngineConfiguration.class))
        .run(ctx -> assertThat(ctx).doesNotHaveBean(CryptoBackend.class));
  }

  @Test
  void theKmsEngineTakesItsForgottenSubjectStoreFromTheCryptoBackend() {
    ForgottenSubjectStore fromBackend = mock(ForgottenSubjectStore.class);
    AtomicReference<DataSource> askedWith = new AtomicReference<>();
    kmsRunner
        .withBean(
            "stubCryptoBackend",
            BeanPostProcessor.class,
            () ->
                new BeanPostProcessor() {
                  @Override
                  public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (!"streamRuneCryptoBackend".equals(beanName)) {
                      return bean;
                    }
                    CryptoBackend real = (CryptoBackend) bean;
                    return new ForwardingCryptoBackend(real) {
                      @Override
                      public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
                        askedWith.set(dataSource);
                        return fromBackend;
                      }
                    };
                  }
                })
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(forgottenSubjectsOf(ctx.getBean(CryptoEngine.class)))
                  .isSameAs(fromBackend);
              assertThat(askedWith.get()).isSameAs(ctx.getBean(DataSource.class));
            });
  }

  private static Object forgottenSubjectsOf(CryptoEngine engine) {
    CryptoEngine delegate =
        engine instanceof CachedCryptoEngine cached ? cached.delegate() : engine;
    assertThat(delegate).isInstanceOf(AwsKmsCryptoEngine.class);
    return ReflectionTestUtils.getField(delegate, "forgottenSubjects");
  }

  /** The return type the bean's factory method declares, read without creating the bean. */
  private static Class<?> declaredType(ConfigurableApplicationContext ctx, String beanName) {
    var definition = (AnnotatedBeanDefinition) ctx.getBeanFactory().getBeanDefinition(beanName);
    try {
      return Class.forName(definition.getFactoryMethodMetadata().getReturnTypeName());
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  /** A {@link CryptoBackend} that forwards every call; a test overrides the one it observes. */
  private static class ForwardingCryptoBackend implements CryptoBackend {
    private final CryptoBackend delegate;

    ForwardingCryptoBackend(CryptoBackend delegate) {
      this.delegate = delegate;
    }

    @Override
    public String name() {
      return delegate.name();
    }

    @Override
    public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
      return delegate.forgottenSubjects(dataSource);
    }

    @Override
    public CryptoForgetSignal forgetSignal(DataSource dataSource) {
      return delegate.forgetSignal(dataSource);
    }

    @Override
    public CryptoForgetSignal forgetSignal(
        DataSource dataSource,
        java.util.function.Supplier<? extends DataSource> listenDataSourceFactory) {
      return delegate.forgetSignal(dataSource, listenDataSourceFactory);
    }

    @Override
    public CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings) {
      return delegate.keyStoreEngine(dataSource, settings);
    }
  }
}

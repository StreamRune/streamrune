package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
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
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The Micronaut integration builds every store through the backend SPI: one {@link BackendStores}
 * bundle of the backend {@code streamrune.backend} names, opened on the {@code DataSource} and
 * closed when the context shuts down. Booted in a real Micronaut context, so the bean types
 * Micronaut derives from each factory method's declared return type are the ones under test.
 */
class MicronautBackendWiringTest {

  private static final String SPEC = "spec.name";
  private static final String WIRING = "backend-wiring";

  /** The values of an ordinary boot: every optional store switched on. */
  private static Map<String, Object> properties(String spec, Object... more) {
    Map<String, Object> values = new HashMap<>();
    values.put(SPEC, spec);
    values.put("streamrune.event-audit-enabled", "true");
    values.put("streamrune.projection-dlq-enabled", "true");
    for (int i = 0; i < more.length; i += 2) {
      values.put((String) more[i], more[i + 1]);
    }
    return values;
  }

  // ── infrastructure ────────────────────────────────────────────────────────────────────────

  /** The DataSource every boot below takes, and an event store that never queries it. */
  @Factory
  @Requires(property = SPEC, pattern = "backend-wiring.*")
  static class Infrastructure {
    static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Singleton
    DataSource dataSource() {
      return DATA_SOURCE;
    }

    // The startup validators resolve the command bus; an event store of its own keeps the bus off
    // the mock DataSource. The framework's EventStoreFactory, which these tests inspect, remains.
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }
  }

  // ── the bundle and the backend ────────────────────────────────────────────────────────────

  @Test
  void theBackendStoresBeanIsThePostgresBundle() {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      assertThat(ctx.getBean(StreamRuneBackend.class)).isInstanceOf(PostgresBackend.class);
      assertThat(ctx.getBean(BackendStores.class).backendName()).isEqualTo("postgres");
    }
  }

  @Test
  void theCommandInboxBeanIsDeclaredAsTheInterface() {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      assertThat(ctx.containsBean(CommandInbox.class)).isTrue();
      assertThat(ctx.containsBean(PostgresCommandInbox.class))
          .as("no framework bean carries the PostgresCommandInbox type any more")
          .isFalse();
      assertThat(frameworkDefinitions(ctx, CommandInbox.class))
          .extracting(BeanDefinition::getBeanType)
          .containsExactly(CommandInbox.class);
    }
  }

  /** Micronaut exposes a factory bean under its declared return type: none is a Postgres type. */
  @Test
  void noFrameworkBeanCarriesAPostgresType() {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      List<String> postgresTyped = new ArrayList<>();
      for (BeanDefinition<?> definition : ctx.getAllBeanDefinitions()) {
        if (!isFramework(definition)) {
          continue;
        }
        List<Class<?>> types = new ArrayList<>(definition.getExposedTypes());
        types.add(definition.getBeanType());
        for (Class<?> type : types) {
          if (type.getName().startsWith("org.streamrune.postgres.")) {
            postgresTyped.add(definition + " as " + type.getName());
          }
        }
      }
      assertThat(postgresTyped).isEmpty();
    }
  }

  @Test
  void theStoresAreTheOnesTheBundleBuilds() throws Exception {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      BackendStores stores = ctx.getBean(BackendStores.class);
      assertThat(ctx.getBean(CommandInbox.class)).isSameAs(stores.commandInbox());
      assertThat(ctx.getBean(SagaStore.class)).isSameAs(stores.sagaStore());
      assertThat(ctx.getBean(SagaDeadLetterStore.class)).isSameAs(stores.sagaDeadLetterStore());
      assertThat(ctx.getBean(AuditStore.class)).isSameAs(stores.auditStore());
      assertThat(ctx.getBean(ComplianceReportQuery.class)).isSameAs(stores.complianceReportQuery());
      assertThat(ctx.getBean(CommandAuditQuery.class)).isSameAs(stores.commandAuditQuery());
      assertThat(ctx.getBean(EventAuditStore.class)).isSameAs(stores.eventAuditStore());
      assertThat(ctx.getBean(EventAuditQuery.class)).isSameAs(stores.eventAuditQuery());
      assertThat(ctx.getBean(ProjectionDeadLetterStore.class))
          .isSameAs(stores.projectionDeadLetterStore());
      assertThat(ctx.getBean(OffsetStore.class)).isSameAs(stores.offsetStore());
      Duration ttl = ctx.getBean(StreamRuneMicronautProperties.class).subscriptionLeaseTtl();
      assertThat(ctx.getBean(SubscriptionLeadership.class)).isSameAs(stores.leadership(ttl));
      // The event store factory is the bundle's, built with the stores it hands out: the bus and
      // the store share the one inbox bean.
      Object factory = ctx.getBean(EventStoreFactory.class);
      assertThat(field(factory, "commandInbox")).isSameAs(stores.commandInbox());
      assertThat(field(factory, "eventAuditStore")).isSameAs(stores.eventAuditStore());
    }
  }

  @Test
  void theDefaultReadModelStoreIsNamedAfterTheBackend() {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      BackendStores stores = ctx.getBean(BackendStores.class);
      assertThat(ctx.getBean(ReadModelStores.class).defaultStoreName())
          .isEqualTo(stores.backendName());
      ReadModelStore defaultStore =
          ctx.getBean(
              ReadModelStore.class,
              Qualifiers.byName(StreamRuneMicronautModule.DEFAULT_READ_MODEL_STORE_BEAN));
      assertThat(defaultStore.name()).isEqualTo(stores.backendName());
    }
  }

  @Test
  void anUnknownBackendFailsTheStartWithTheSelectionMessage() {
    assertThatThrownBy(
            () ->
                ApplicationContext.run(properties(WIRING, "streamrune.backend", "oracle")).close())
        .hasStackTraceContaining(
            "streamrune.backend='oracle' is not a backend; valid values: postgres, sqlite");
  }

  // ── the bundle is closed once, when the context shuts down ────────────────────────────────

  /** A backend whose {@code open()} hands out a spy, so the test sees what the bundle is asked. */
  @Factory
  @Requires(property = SPEC, pattern = "backend-wiring-(spy|replaced)")
  static class SpyingBackend {
    static final AtomicReference<BackendStores> OPENED = new AtomicReference<>();

    @Singleton
    @Replaces(StreamRuneBackend.class)
    StreamRuneBackend spyingBackend() {
      PostgresBackend real = new PostgresBackend();
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
          OPENED.set(stores);
          return stores;
        }
      };
    }
  }

  @Test
  void theBackendStoresAreClosedOnceWhenTheContextShutsDown() {
    SpyingBackend.OPENED.set(null);
    try (var ctx = ApplicationContext.run(properties("backend-wiring-spy"))) {
      assertThat(ctx.getBean(BackendStores.class)).isSameAs(SpyingBackend.OPENED.get());
      verify(SpyingBackend.OPENED.get(), never()).close();
    }
    verify(SpyingBackend.OPENED.get(), times(1)).close();
  }

  // ── a replaced default read-model store is never created, even with eager singletons ──────

  /** The application's own store named {@code postgres}: it replaces the default one. */
  @Factory
  @Requires(property = SPEC, value = "backend-wiring-replaced")
  static class ReplacementStore {
    static final InMemoryProjectionRepository REPOSITORY = new InMemoryProjectionRepository();

    @Singleton
    @jakarta.inject.Named("appPostgres")
    ReadModelStore appPostgres() {
      return ReadModelStore.of("postgres", REPOSITORY, REPOSITORY, REPOSITORY);
    }
  }

  @Test
  void aReplacedDefaultStoreIsNeverAskedOfTheBundle_evenWithEagerSingletons() {
    SpyingBackend.OPENED.set(null);
    try (var ctx =
        ApplicationContext.builder()
            .properties(
                properties(
                    "backend-wiring-replaced",
                    "streamrune.runner-lifecycle-enabled",
                    "false",
                    "streamrune.subscription.single-active-consumer.enabled",
                    "false"))
            .eagerInitSingletons(true)
            .start()) {
      assertThat(ctx.getBean(ReadModelStores.class).stores())
          .extracting(ReadModelStore::name)
          .containsExactly("postgres");
      assertThat(SpyingBackend.OPENED.get()).as("the bundle is opened eagerly").isNotNull();
      verify(SpyingBackend.OPENED.get(), never()).defaultReadModelStore();
    }
  }

  // ── the retention sweepers sweep only stores the framework built, or Postgres-typed ones ──

  @Test
  void theDefaultContextHasBothRetentionSweepersOverTheBundlesStores() throws Exception {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      BackendStores stores = ctx.getBean(BackendStores.class);
      assertThat(field(inboxSweeper(ctx), "inbox")).isSameAs(stores.commandInbox());
      assertThat(field(sagaSweeper(ctx), "store")).isSameAs(stores.sagaDeadLetterStore());
    }
  }

  /** An application's own saga dead-letter store and command inbox, declared on the interfaces. */
  @Factory
  @Requires(property = SPEC, pattern = "backend-(wiring|nodatasource)-own-stores")
  static class ApplicationStores {
    static final SagaDeadLetterStore SAGA_STORE = mock(SagaDeadLetterStore.class);
    static final CommandInbox INBOX = new InMemoryCommandInbox();

    // Under the framework's own bean names, the way an application most often declares them.
    @Singleton
    SagaDeadLetterStore sagaDeadLetterStore() {
      return SAGA_STORE;
    }

    @Singleton
    CommandInbox commandInbox() {
      return INBOX;
    }

    // An inbox of another type is honoured only by an event store that claims keys in it.
    @Singleton
    EventStoreFactory eventStoreFactory() {
      return mock(EventStoreFactory.class);
    }
  }

  /**
   * The application's interface-typed stores win every injection point, and the sweepers keep
   * sweeping the framework's own stores (as before: they swept the framework's Postgres bean, which
   * {@code @Secondary} leaves registered), never the application's.
   */
  @Test
  void anApplicationsOwnStoresAreNeverSwept() throws Exception {
    try (var ctx = ApplicationContext.run(properties("backend-wiring-own-stores"))) {
      BackendStores stores = ctx.getBean(BackendStores.class);
      assertThat(ctx.getBean(SagaDeadLetterStore.class)).isSameAs(ApplicationStores.SAGA_STORE);
      assertThat(ctx.getBean(CommandInbox.class)).isSameAs(ApplicationStores.INBOX);
      assertThat(field(sagaSweeper(ctx), "store"))
          .isNotSameAs(ApplicationStores.SAGA_STORE)
          .isSameAs(stores.sagaDeadLetterStore());
      assertThat(field(inboxSweeper(ctx), "inbox"))
          .isNotSameAs(ApplicationStores.INBOX)
          .isSameAs(stores.commandInbox());
    }
  }

  /** The event store of the boot without a DataSource. */
  @Factory
  @Requires(property = SPEC, value = "backend-nodatasource-own-stores")
  static class EventStoreWithoutDataSource {
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }
  }

  /** Without a DataSource the framework builds no store, so nothing is swept at all. */
  @Test
  void withoutADataSourceAnApplicationsOwnStoresGetNoSweeper() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                SPEC,
                "backend-nodatasource-own-stores",
                "streamrune.subscription.single-active-consumer.enabled",
                "false"))) {
      assertThat(ctx.containsBean(BackendStores.class)).isFalse();
      assertThat(ctx.containsBean(InboxRetentionSweeper.class)).isFalse();
      assertThat(ctx.containsBean(SagaDeadLetterRetentionSweeper.class)).isFalse();
    }
  }

  /** Application stores declared with their Postgres types, which the sweepers sweep. */
  @Factory
  @Requires(property = SPEC, value = "backend-wiring-postgres-stores")
  static class ApplicationPostgresStores {
    static final PostgresSagaDeadLetterStore SAGA_STORE =
        new PostgresSagaDeadLetterStore(mock(DataSource.class));
    static final PostgresCommandInbox INBOX = new PostgresCommandInbox(mock(DataSource.class));

    @Singleton
    PostgresSagaDeadLetterStore applicationSagaDeadLetterStore() {
      return SAGA_STORE;
    }

    @Singleton
    PostgresCommandInbox applicationCommandInbox() {
      return INBOX;
    }
  }

  @Test
  void applicationDeclaredPostgresStoresAreSweptAsBefore() throws Exception {
    try (var ctx = ApplicationContext.run(properties("backend-wiring-postgres-stores"))) {
      assertThat(field(sagaSweeper(ctx), "store")).isSameAs(ApplicationPostgresStores.SAGA_STORE);
      assertThat(field(inboxSweeper(ctx), "inbox")).isSameAs(ApplicationPostgresStores.INBOX);
      // ... and the event store shares the application's inbox with the bus.
      Object factory = ctx.getBean(EventStoreFactory.class);
      assertThat(field(factory, "commandInbox")).isSameAs(ApplicationPostgresStores.INBOX);
    }
  }

  private static InboxRetentionSweeper inboxSweeper(ApplicationContext ctx) {
    return ctx.getBean(
        InboxRetentionSweeper.class,
        Qualifiers.byName(StreamRuneMicronautModule.FRAMEWORK_INBOX_RETENTION_SWEEPER));
  }

  private static SagaDeadLetterRetentionSweeper sagaSweeper(ApplicationContext ctx) {
    return ctx.getBean(
        SagaDeadLetterRetentionSweeper.class,
        Qualifiers.byName(StreamRuneMicronautModule.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER));
  }

  // ── an application store of a foreign type fails the start ──────────────────────────────

  /** An outbox store of no backend, with a mapper. */
  @Factory
  @Requires(property = SPEC, value = "backend-wiring-foreign-outbox")
  static class ForeignOutbox {
    @Singleton
    OutboxStore outboxStore() {
      return new InMemoryOutboxStore();
    }

    @Singleton
    OutboxEventMapper outboxEventMapper() {
      return event -> List.of();
    }
  }

  /** The startup validator builds the event store factory, so the refusal fails the start. */
  @Test
  void aForeignOutboxStoreWithAMapperFailsTheStartWithTheR1Refusal() {
    assertThatThrownBy(
            () -> ApplicationContext.run(properties("backend-wiring-foreign-outbox")).close())
        .hasStackTraceContaining(
            "outbox store of backend 'InMemoryOutboxStore' cannot share the 'postgres'"
                + " event store's transaction; build it from this backend's BackendStores");
  }

  // ── crypto: the engines take their stores and signals from the CryptoBackend ──────────────

  private static Map<String, Object> awsEnabled(String spec) {
    return properties(
        spec,
        "streamrune.gdpr.snapshot-purge",
        "none",
        "streamrune.crypto.aws.enabled",
        "true",
        "streamrune.crypto.aws.region",
        "eu-central-1",
        "streamrune.crypto.aws.kms-key-id",
        "alias/test");
  }

  @Test
  void theKmsEnginesForgottenSubjectStoreIsTheCryptoBackendsOne() throws Exception {
    try (var ctx = ApplicationContext.run(awsEnabled(WIRING))) {
      assertThat(ctx.getBean(CryptoBackend.class)).isInstanceOf(PostgresCryptoBackend.class);
      Object store = forgottenSubjectsOf(ctx.getBean(CryptoEngine.class));
      assertThat(store).isInstanceOf(JdbcForgottenSubjectStore.class);
      assertThat(field(store, "dataSource")).isSameAs(Infrastructure.DATA_SOURCE);
    }
  }

  /** A crypto backend that records the forgotten-subject store it hands out. */
  @Factory
  @Requires(property = SPEC, value = "backend-wiring-recording-crypto")
  static class RecordingCryptoBackend {
    static final ForgottenSubjectStore STORE = mock(ForgottenSubjectStore.class);
    static final AtomicReference<DataSource> ASKED_WITH = new AtomicReference<>();

    @Singleton
    @Replaces(CryptoBackend.class)
    CryptoBackend recordingCryptoBackend() {
      CryptoBackend real = new PostgresCryptoBackend();
      return new CryptoBackend() {
        @Override
        public String name() {
          return real.name();
        }

        @Override
        public ForgottenSubjectStore forgottenSubjects(DataSource dataSource) {
          ASKED_WITH.set(dataSource);
          return STORE;
        }

        @Override
        public CryptoForgetSignal forgetSignal(DataSource dataSource) {
          return real.forgetSignal(dataSource);
        }

        @Override
        public CryptoForgetSignal forgetSignal(
            DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory) {
          return real.forgetSignal(dataSource, listenDataSourceFactory);
        }

        @Override
        public CryptoEngine keyStoreEngine(DataSource dataSource, KeyStoreEngineSettings settings) {
          return real.keyStoreEngine(dataSource, settings);
        }
      };
    }
  }

  @Test
  void theKmsEngineAsksTheCryptoBackendBeanForItsForgottenSubjectStore() throws Exception {
    RecordingCryptoBackend.ASKED_WITH.set(null);
    try (var ctx = ApplicationContext.run(awsEnabled("backend-wiring-recording-crypto"))) {
      assertThat(forgottenSubjectsOf(ctx.getBean(CryptoEngine.class)))
          .isSameAs(RecordingCryptoBackend.STORE);
      assertThat(RecordingCryptoBackend.ASKED_WITH.get()).isSameAs(Infrastructure.DATA_SOURCE);
    }
  }

  /** The crypto backend is a bean only when a crypto engine is enabled. */
  @Test
  void theCryptoBackendBeanExistsOnlyWithAnEnabledEngine() {
    try (var ctx = ApplicationContext.run(properties(WIRING))) {
      assertThat(ctx.containsBean(CryptoBackend.class)).isFalse();
    }
    try (var ctx = ApplicationContext.run(awsEnabled(WIRING))) {
      assertThat(ctx.containsBean(CryptoBackend.class)).isTrue();
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────────────────────────

  private static boolean isFramework(BeanDefinition<?> definition) {
    Class<?> declaring = definition.getDeclaringType().orElse(null);
    return StreamRuneMicronautModule.class.equals(declaring)
        || StreamRuneCryptoFactory.class.equals(declaring);
  }

  private static <T> List<BeanDefinition<T>> frameworkDefinitions(
      ApplicationContext ctx, Class<T> type) {
    return ctx.getBeanDefinitions(type).stream()
        .filter(MicronautBackendWiringTest::isFramework)
        .toList();
  }

  private static Object forgottenSubjectsOf(CryptoEngine engine) throws Exception {
    CryptoEngine delegate =
        engine instanceof CachedCryptoEngine cached ? cached.delegate() : engine;
    assertThat(delegate).isInstanceOf(AwsKmsCryptoEngine.class);
    return field(delegate, "forgottenSubjects");
  }

  /** A private field of {@code target}, searched up its class hierarchy. */
  static Object field(Object target, String name) throws Exception {
    for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
      } catch (NoSuchFieldException e) {
        // up the hierarchy
      }
    }
    throw new NoSuchFieldException(target.getClass().getName() + "." + name);
  }
}

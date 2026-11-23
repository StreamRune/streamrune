package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
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
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * The Quarkus integration builds every store through the backend SPI: one {@link BackendStores}
 * bundle of the backend {@code streamrune.backend} names, opened on the {@code DataSource} and
 * closed when the container shuts down. Booted in a real Arc container ({@link
 * RealArcTestContainer}), so the bean types Arc fixes from each producer's declared return type are
 * the ones under test.
 */
class QuarkusBackendWiringTest {

  // ── infrastructure ────────────────────────────────────────────────────────────────────────

  /** The DataSource and the properties every boot below takes. */
  @ApplicationScoped
  public static class Infrastructure {

    static final DataSource DATA_SOURCE = mock(DataSource.class);

    /** The {@code streamrune.*} values of the next boot. */
    static volatile Map<String, String> values =
        Map.of(
            "streamrune.event-audit-enabled", "true", "streamrune.projection-dlq-enabled", "true");

    @Produces
    @Singleton
    public DataSource dataSource() {
      return DATA_SOURCE;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.of(values);
    }
  }

  private static RealArcTestContainer bootFramework(Class<?>... application) throws Exception {
    List<Class<?>> classes =
        new ArrayList<>(List.of(StreamRuneProducers.class, Infrastructure.class));
    classes.addAll(List.of(application));
    return RealArcTestContainer.boot(classes);
  }

  // ── the bundle and the backend ────────────────────────────────────────────────────────────

  @Test
  void theBackendStoresBeanIsThePostgresBundle() throws Exception {
    try (var arc = bootFramework()) {
      assertThat(arc.container().instance(StreamRuneBackend.class).get())
          .isInstanceOf(PostgresBackend.class);
      assertThat(arc.container().instance(BackendStores.class).get().backendName())
          .isEqualTo("postgres");
    }
  }

  @Test
  void theCommandInboxBeanIsDeclaredAsTheInterface() throws Exception {
    try (var arc = bootFramework()) {
      assertThat(arc.container().instance(CommandInbox.class).isAvailable()).isTrue();
      assertThat(arc.container().instance(PostgresCommandInbox.class).isAvailable())
          .as("no framework bean carries the PostgresCommandInbox type any more")
          .isFalse();
    }
  }

  /** Arc fixes a producer bean's types from its declared return type: none is a Postgres type. */
  @Test
  void noFrameworkBeanCarriesAPostgresType() throws Exception {
    try (var arc = bootFramework()) {
      List<String> postgresTyped = new ArrayList<>();
      for (Bean<?> bean :
          arc.container().beanManager().getBeans(Object.class, Any.Literal.INSTANCE)) {
        if (!StreamRuneProducers.class.equals(bean.getBeanClass())) {
          continue;
        }
        for (Type type : bean.getTypes()) {
          if (type.getTypeName().startsWith("org.streamrune.postgres.")) {
            postgresTyped.add(type.getTypeName());
          }
        }
      }
      assertThat(postgresTyped).isEmpty();
    }
  }

  @Test
  void theStoresAreTheOnesTheBundleBuilds() throws Exception {
    try (var arc = bootFramework()) {
      var container = arc.container();
      BackendStores stores = container.instance(BackendStores.class).get();
      assertThat(container.instance(CommandInbox.class).get()).isSameAs(stores.commandInbox());
      assertThat(container.instance(SagaStore.class).get()).isSameAs(stores.sagaStore());
      assertThat(container.instance(SagaDeadLetterStore.class).get())
          .isSameAs(stores.sagaDeadLetterStore());
      assertThat(container.instance(AuditStore.class).get()).isSameAs(stores.auditStore());
      assertThat(container.instance(ComplianceReportQuery.class).get())
          .isSameAs(stores.complianceReportQuery());
      assertThat(container.instance(CommandAuditQuery.class).get())
          .isSameAs(stores.commandAuditQuery());
      assertThat(container.instance(EventAuditStore.class).get())
          .isSameAs(stores.eventAuditStore());
      assertThat(container.instance(EventAuditQuery.class).get())
          .isSameAs(stores.eventAuditQuery());
      assertThat(container.instance(ProjectionDeadLetterStore.class).get())
          .isSameAs(stores.projectionDeadLetterStore());
      assertThat(container.instance(OffsetStore.class).get()).isSameAs(stores.offsetStore());
      Duration ttl =
          container
              .instance(StreamRuneQuarkusProperties.class)
              .get()
              .subscription()
              .singleActiveConsumer()
              .leaseTtl();
      assertThat(container.instance(SubscriptionLeadership.class).get())
          .isSameAs(stores.leadership(ttl));
      // The event store factory is the bundle's, built with the stores it hands out: the bus and
      // the store share the one inbox bean.
      Object factory = container.instance(EventStoreFactory.class).get();
      assertThat(field(factory, "commandInbox")).isSameAs(stores.commandInbox());
      assertThat(field(factory, "eventAuditStore")).isSameAs(stores.eventAuditStore());
    }
  }

  @Test
  void theDefaultReadModelStoreIsNamedAfterTheBackend() throws Exception {
    try (var arc = bootFramework()) {
      BackendStores stores = arc.container().instance(BackendStores.class).get();
      assertThat(arc.container().instance(ReadModelStores.class).get().defaultStoreName())
          .isEqualTo(stores.backendName());
    }
  }

  @Test
  void anUnknownBackendFailsWithTheSelectionMessage() throws Exception {
    Map<String, String> previous = Infrastructure.values;
    Infrastructure.values = Map.of("streamrune.backend", "oracle");
    try (var arc = bootFramework()) {
      assertThatThrownBy(() -> arc.container().instance(EventStoreFactory.class).get())
          .hasStackTraceContaining(
              "streamrune.backend='oracle' is not a backend; valid values: postgres, sqlite");
    } finally {
      Infrastructure.values = previous;
    }
  }

  // ── the bundle is closed once, when the container shuts down ──────────────────────────────

  /** A backend whose {@code open()} hands out a spy, so the test sees the bundle's close. */
  @ApplicationScoped
  public static class SpyingBackend {

    static final AtomicReference<BackendStores> OPENED = new AtomicReference<>();

    @Produces
    @Singleton
    @Alternative
    @Priority(1)
    public StreamRuneBackend spyingBackend() {
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
  void theBackendStoresAreClosedOnceWhenTheContainerShutsDown() throws Exception {
    SpyingBackend.OPENED.set(null);
    try (var arc = bootFramework(SpyingBackend.class)) {
      assertThat(arc.container().instance(BackendStores.class).get())
          .isSameAs(SpyingBackend.OPENED.get());
      verify(SpyingBackend.OPENED.get(), never()).close();
    }
    verify(SpyingBackend.OPENED.get(), times(1)).close();
  }

  // ── the retention sweepers sweep only stores the framework built, or Postgres-typed ones ──

  @Test
  void theDefaultContextHasBothRetentionSweepersOverTheBundlesStores() throws Exception {
    try (var arc = bootFramework()) {
      BackendStores stores = arc.container().instance(BackendStores.class).get();
      InboxRetentionSweeper inbox = inboxSweeper(arc);
      SagaDeadLetterRetentionSweeper saga = sagaSweeper(arc);
      assertThat(inbox).isNotNull();
      assertThat(saga).isNotNull();
      assertThat(field(inbox, "inbox")).isSameAs(stores.commandInbox());
      assertThat(field(saga, "store")).isSameAs(stores.sagaDeadLetterStore());
    }
  }

  /** An application's own saga dead-letter store, declared on the interface. */
  @ApplicationScoped
  public static class ApplicationSagaDeadLetterStore {
    static final SagaDeadLetterStore STORE = mock(SagaDeadLetterStore.class);

    @Produces
    @Singleton
    public SagaDeadLetterStore sagaDeadLetterStore() {
      return STORE;
    }
  }

  /** An application's own command inbox, declared on the interface. */
  @ApplicationScoped
  public static class ApplicationCommandInbox {
    static final CommandInbox INBOX = new InMemoryCommandInbox();

    @Produces
    @Singleton
    public CommandInbox commandInbox() {
      return INBOX;
    }
  }

  /**
   * The application's interface-typed stores win every injection point, and the sweepers keep
   * sweeping the framework's own stores (as before: they swept the framework's Postgres bean),
   * never the application's.
   */
  @Test
  void anApplicationsOwnStoresAreNeverSwept() throws Exception {
    try (var arc =
        bootFramework(ApplicationSagaDeadLetterStore.class, ApplicationCommandInbox.class)) {
      BackendStores stores = arc.container().instance(BackendStores.class).get();
      assertThat(arc.container().instance(SagaDeadLetterStore.class).get())
          .isSameAs(ApplicationSagaDeadLetterStore.STORE);
      assertThat(arc.container().instance(CommandInbox.class).get())
          .isSameAs(ApplicationCommandInbox.INBOX);
      assertThat(field(sagaSweeper(arc), "store"))
          .isNotSameAs(ApplicationSagaDeadLetterStore.STORE)
          .isSameAs(stores.sagaDeadLetterStore());
      assertThat(field(inboxSweeper(arc), "inbox"))
          .isNotSameAs(ApplicationCommandInbox.INBOX)
          .isSameAs(stores.commandInbox());
    }
  }

  /** Application stores declared with their Postgres types, which the sweepers sweep. */
  @ApplicationScoped
  public static class ApplicationPostgresStores {
    static final PostgresSagaDeadLetterStore SAGA_STORE =
        new PostgresSagaDeadLetterStore(mock(DataSource.class));
    static final PostgresCommandInbox INBOX = new PostgresCommandInbox(mock(DataSource.class));

    @Produces
    @Singleton
    public PostgresSagaDeadLetterStore sagaDeadLetterStore() {
      return SAGA_STORE;
    }

    @Produces
    @Singleton
    public PostgresCommandInbox commandInbox() {
      return INBOX;
    }
  }

  @Test
  void applicationDeclaredPostgresStoresAreSweptAsBefore() throws Exception {
    try (var arc = bootFramework(ApplicationPostgresStores.class)) {
      assertThat(field(sagaSweeper(arc), "store")).isSameAs(ApplicationPostgresStores.SAGA_STORE);
      assertThat(field(inboxSweeper(arc), "inbox")).isSameAs(ApplicationPostgresStores.INBOX);
      // ... and the event store shares the application's inbox with the bus.
      Object factory = arc.container().instance(EventStoreFactory.class).get();
      assertThat(field(factory, "commandInbox")).isSameAs(ApplicationPostgresStores.INBOX);
    }
  }

  private static InboxRetentionSweeper inboxSweeper(RealArcTestContainer arc) {
    return arc.container()
        .instance(
            InboxRetentionSweeper.class,
            NamedLiteral.of(StreamRuneProducers.FRAMEWORK_INBOX_RETENTION_SWEEPER))
        .get();
  }

  private static SagaDeadLetterRetentionSweeper sagaSweeper(RealArcTestContainer arc) {
    return arc.container()
        .instance(
            SagaDeadLetterRetentionSweeper.class,
            NamedLiteral.of(StreamRuneProducers.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER))
        .get();
  }

  // ── an application store of a foreign type fails the start ──────────────────────────────

  /** An outbox store of no backend, with a mapper. */
  @ApplicationScoped
  public static class ForeignOutbox {
    @Produces
    @Singleton
    public OutboxStore outboxStore() {
      return new InMemoryOutboxStore();
    }

    @Produces
    @Singleton
    public OutboxEventMapper outboxEventMapper() {
      return event -> List.of();
    }
  }

  @Test
  void aForeignOutboxStoreWithAMapperFailsWithTheR1Refusal() throws Exception {
    try (var arc = bootFramework(ForeignOutbox.class)) {
      assertThatThrownBy(() -> arc.container().instance(EventStoreFactory.class).get())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              "outbox store of backend 'InMemoryOutboxStore' cannot share the 'postgres'"
                  + " event store's transaction; build it from this backend's BackendStores");
    }
  }

  /** A Postgres outbox store the application declares, with a mapper. */
  @ApplicationScoped
  public static class ApplicationPostgresOutbox {
    static final PostgresOutboxStore STORE = new PostgresOutboxStore(mock(DataSource.class));
    static final OutboxEventMapper MAPPER = event -> List.of();

    @Produces
    @Singleton
    public PostgresOutboxStore outboxStore() {
      return STORE;
    }

    @Produces
    @Singleton
    public OutboxEventMapper outboxEventMapper() {
      return MAPPER;
    }
  }

  @Test
  void anApplicationDeclaredPostgresOutboxStoreIsStillWired() throws Exception {
    try (var arc = bootFramework(ApplicationPostgresOutbox.class)) {
      Object factory = arc.container().instance(EventStoreFactory.class).get();
      assertThat(field(factory, "outboxStore")).isSameAs(ApplicationPostgresOutbox.STORE);
      assertThat(field(factory, "outboxEventMapper")).isSameAs(ApplicationPostgresOutbox.MAPPER);
    }
  }

  // ── crypto: the engines take their stores and signals from the CryptoBackend ──────────────

  private static final String AWS = "streamrune.crypto.aws.enabled";

  /** The DataSource, the crypto properties and the backend a crypto deployment takes. */
  @ApplicationScoped
  public static class CryptoInfrastructure {

    static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Produces
    @Singleton
    public DataSource dataSource() {
      return DATA_SOURCE;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties properties() {
      return new SmallRyeConfigBuilder()
          .withMapping(StreamRuneQuarkusCryptoProperties.class)
          .withConverter(
              Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
          .withDefaultValues(
              Map.of(
                  AWS,
                  "true",
                  "streamrune.crypto.aws.region",
                  "us-west-2",
                  "streamrune.crypto.aws.kms-key-id",
                  "arn:aws:kms:us-west-2:123456789012:key/test-key-id"))
          .build()
          .getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    }

    @Produces
    @Singleton
    public StreamRuneBackend backend() {
      return new PostgresBackend();
    }
  }

  /** What a real deployment indexes of the crypto producers, plus the infrastructure. */
  private static List<Class<?>> cryptoDeployment(Class<?>... application) {
    List<Class<?>> classes = new ArrayList<>();
    classes.add(StreamRuneCryptoProducers.class);
    classes.addAll(List.of(StreamRuneCryptoProducers.class.getDeclaredClasses()));
    classes.add(CryptoInfrastructure.class);
    classes.addAll(List.of(application));
    return classes;
  }

  @Test
  void theKmsEnginesForgottenSubjectStoreIsTheCryptoBackendsOne() throws Exception {
    try (var arc = RealArcTestContainer.boot(cryptoDeployment(), Map.of(AWS, "true"))) {
      assertThat(arc.container().instance(CryptoBackend.class).get())
          .isInstanceOf(PostgresCryptoBackend.class);
      Object store = forgottenSubjectsOf(arc.container().instance(CryptoEngine.class).get());
      assertThat(store).isInstanceOf(JdbcForgottenSubjectStore.class);
      assertThat(field(store, "dataSource")).isSameAs(CryptoInfrastructure.DATA_SOURCE);
    }
  }

  /** A crypto backend that records the forgotten-subject store it hands out. */
  @ApplicationScoped
  public static class RecordingCryptoBackend {
    static final ForgottenSubjectStore STORE = mock(ForgottenSubjectStore.class);
    static final AtomicReference<DataSource> ASKED_WITH = new AtomicReference<>();

    @Produces
    @Singleton
    @Alternative
    @Priority(1)
    public CryptoBackend recordingCryptoBackend() {
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
    try (var arc =
        RealArcTestContainer.boot(
            cryptoDeployment(RecordingCryptoBackend.class), Map.of(AWS, "true"))) {
      assertThat(forgottenSubjectsOf(arc.container().instance(CryptoEngine.class).get()))
          .isSameAs(RecordingCryptoBackend.STORE);
      assertThat(RecordingCryptoBackend.ASKED_WITH.get())
          .isSameAs(CryptoInfrastructure.DATA_SOURCE);
    }
  }

  /**
   * The crypto backend is a bean only where an engine needs it: a real Quarkus deployment removes
   * unused beans, so without an enabled engine it is gone.
   */
  @Test
  void theCryptoBackendBeanExistsOnlyWithAnEnabledEngine() throws Exception {
    try (var arc =
        RealArcTestContainer.bootRemovingUnusedBeans(
            cryptoDeployment(), Map.of(), Set.of(EngineConsumer.class))) {
      assertThat(arc.container().instance(CryptoBackend.class).isAvailable()).isFalse();
    }
    try (var arc =
        RealArcTestContainer.bootRemovingUnusedBeans(
            cryptoDeployment(), Map.of(AWS, "true"), Set.of(EngineConsumer.class))) {
      assertThat(arc.container().instance(CryptoBackend.class).isAvailable()).isTrue();
    }
  }

  /** An application class that injects the engine when there is one, as an entry point. */
  @ApplicationScoped
  public static class EngineConsumer {
    @jakarta.inject.Inject jakarta.enterprise.inject.Instance<CryptoEngine> engine;
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

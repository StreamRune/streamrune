package org.streamrune.quarkus;

import io.opentelemetry.api.OpenTelemetry;
import io.quarkus.arc.All;
import io.quarkus.arc.DefaultBean;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.Typed;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.QueryBus;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendOptionException;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.EventStoreOptions;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.postgres.PostgresBackend;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.AuditingQueryBus;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.BeanValidationInterceptor;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.OpenTelemetryTracing;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.TracingSettings;
import org.streamrune.runtime.UnboundRequestContextQueryBus;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * Quarkus CDI producers for StreamRune components.
 *
 * <p>Scope convention: producers that always yield a bean are {@code @Singleton}; producers that
 * may yield <em>no</em> bean (optional features, missing collaborators) are {@code @Dependent} (the
 * default producer scope) and return {@code null}, which CDI only permits for dependent beans.
 * Consumers of optional beans must therefore null-check the value obtained from {@link
 * Instance#get()} — see {@link StreamRuneLifecycle} and {@link #virtualThreadCommandBus}. A product
 * that every consumer must SHARE (the metrics collector, the subscription health contributor, the
 * leadership) is {@code @Singleton} even though its feature is optional, with an inert non-null
 * fallback ({@code NOOP}, an inert contributor) instead of {@code null}: a {@code @Dependent}
 * producer would hand each injection point its own copy.
 *
 * <p>The {@link SagaStore} is shared too, and has no inert fallback to be {@code @Singleton} with:
 * a saga store that keeps nothing would let a runner lose its sagas, and {@code null} is the
 * documented answer of the saga kill switch ({@code streamrune.saga.enabled=false}) that consumers
 * null-check. Its producer therefore stays dependent-scoped and hands every injection point the one
 * instance the backend's bundle memoizes — see {@link #sagaStore}.
 */
@ApplicationScoped
public class StreamRuneProducers {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(StreamRuneProducers.class);

  // Framework bean names for the background runners. StreamRuneLifecycle injects each BY
  // NAME so it starts/stops only the framework-assembled runner, and it does not even resolve that
  // name when the application supplies its own (unqualified) runner of the type — Arc would still
  // satisfy the named injection point from the @DefaultBean producer, which it suppresses only
  // where it is ambiguous. The application starts and stops its own runner.
  static final String FRAMEWORK_OUTBOX_POLLER = "streamRuneOutboxPoller";
  static final String FRAMEWORK_DEAD_LETTER_RETRY_RUNNER = "streamRuneDeadLetterRetryRunner";
  static final String FRAMEWORK_OUTBOX_RETENTION_SWEEPER = "streamRuneOutboxRetentionSweeper";
  static final String FRAMEWORK_INBOX_RETENTION_SWEEPER = "streamRuneInboxRetentionSweeper";
  static final String FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER =
      "streamRuneSagaDeadLetterRetentionSweeper";
  static final String FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER =
      "streamRuneDeadLetterRetentionSweeper";

  /** Provides a default empty EventTypeRegistry if the application does not define one. */
  @Produces
  @Singleton
  @DefaultBean
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder().build();
  }

  /**
   * The storage backend {@code streamrune.backend} names, through {@link BackendSelection}: {@link
   * PostgresBackend} unless it is set. Choosing a backend builds nothing and touches no database;
   * an unknown value, or {@code sqlite} without the module {@code org.streamrune:streamrune-sqlite}
   * on the classpath, fails with {@link BackendSelection}'s message when the backend is first
   * needed: Arc creates beans lazily, and the framework's startup observers ({@link
   * StreamRuneConfigValidator}, {@link StreamRuneLifecycle}) resolve the stores on the startup
   * event.
   *
   * @param properties the bound {@code streamrune.*} properties
   * @return the backend
   */
  @Produces
  @Singleton
  public StreamRuneBackend streamRuneBackend(StreamRuneQuarkusProperties properties) {
    return BackendSelection.resolve(
        BackendSelection.parse(properties.backend()), PostgresBackend::new);
  }

  /**
   * The stores of the {@link #streamRuneBackend backend}, opened once on the application's {@code
   * DataSource}: every store these producers build is taken from it, so they all share one backend.
   * Opening builds no store: each store is built on first use and memoized by the bundle. On
   * Postgres opening touches no database; on SQLite it verifies the DataSource, checks the
   * connection setup and takes the file's instance guard (see below). {@link #closeBackendStores}
   * closes the bundle when the container shuts down, which closes the leadership it built.
   *
   * <p>The bundle is opened through {@link org.streamrune.integration.BackendStartup#open}: a
   * backend other than Postgres is verified against the {@code DataSource} first, and SQLite's
   * {@code open} takes the instance guard of the database file.
   *
   * <p><b>Shutdown order.</b> A SQLite store keeps writing the file after its bundle released the
   * instance guard, so the bundle must close after every user of its stores stopped. The
   * framework's own users (runners, sweepers, pollers) stop on the shutdown event ({@link
   * StreamRuneLifecycle}), before Arc destroys any bean, and a retention sweeper's close waits for
   * a sweep in flight. Arc then destroys the application context before the singleton context the
   * bundle lives in, so an {@code @ApplicationScoped} application bean's {@code @PreDestroy} runs
   * before the bundle closes. The order in which Arc destroys the beans of the singleton context is
   * unspecified: an application {@code @Singleton} bean must not write through a store in its
   * {@code @PreDestroy}, because the bundle may already be closed; it observes the shutdown event
   * instead.
   *
   * <p>The bundle's context carries the {@link CryptoEngine} bean (if present), which the event
   * store, the saga store and the default read-model store wire into their ObjectMappers through
   * {@code CryptoShreddingModule} so {@code @Encrypted} fields are persisted as ciphertext instead
   * of plaintext PII, and fall within GDPR forget scope; the {@link
   * org.streamrune.core.StreamRuneMetrics} bean (or {@code NOOP}), which a redaction on a decrypt
   * path emits {@code subject_redacted} to; the {@link StreamRuneTracing} bean, which the backend
   * guards with those metrics; and the schema switches: {@code
   * streamrune.event-store.schema.auto-initialize}, {@code
   * streamrune.read-model.schema.auto-initialize} (unset, the former) and schema validation, which
   * is always on.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} fields as PLAINTEXT
   */
  @Produces
  @Singleton
  public BackendStores streamRuneBackendStores(
      StreamRuneBackend backend,
      DataSource dataSource,
      StreamRuneQuarkusProperties properties,
      Instance<CryptoEngine> cryptoEngineInstance,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance,
      StreamRuneTracing tracing) {
    if (cryptoEngineInstance.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Priority — StreamRune cannot "
              + "decide which engine to wire into the event store, the saga store and the default "
              + "read-model store, and skipping encryption would persist @Encrypted fields as "
              + "PLAINTEXT. Ensure exactly one CryptoEngine bean is resolvable (remove duplicates "
              + "or qualify one with @Default).");
    }
    CryptoEngine cryptoEngine =
        cryptoEngineInstance.isUnsatisfied() ? null : cryptoEngineInstance.get();
    // Verified first unless it is the Postgres backend, which opens nothing (BackendStartup); on
    // SQLite, open() takes the instance guard, so another process holding the file fails here.
    return org.streamrune.integration.BackendStartup.open(
        backend,
        new BackendContext(
            dataSource,
            cryptoEngine,
            java.time.Clock.systemUTC(),
            resolveMetricsOrDefault(metricsInstance),
            tracing,
            properties.eventStore().schema().autoInitialize(),
            readModelAutoInitialize(properties),
            true));
  }

  /**
   * Opens the {@link #streamRuneBackendStores stores} when the application starts, whenever a
   * {@code DataSource} bean is resolvable, so their startup checks never wait for the first command
   * of an application that runs no projection or saga. On SQLite, opening verifies the DataSource
   * (a SQLite database with the required connection setup) and takes the instance guard of the
   * file, so another process on the same file fails the start; on Postgres it touches no database.
   * Observed at {@code LIBRARY_BEFORE + 50}: the explicit opener runs after the startup validators
   * ({@code LIBRARY_BEFORE}) and before {@link StreamRuneLifecycle} starts the runners ({@code
   * LIBRARY_BEFORE + 100}). It does not keep every database access behind the validators: an
   * observer at {@code LIBRARY_BEFORE} that resolves a store (the request-identity validator builds
   * the command buses, the projection index declarer writes through the repositories) opens the
   * stores itself, and CDI leaves the order within {@code LIBRARY_BEFORE} unspecified.
   *
   * @param event the startup event
   * @param stores the stores, resolvable when a {@code DataSource} is
   */
  // public: the observer invoker Arc generates may live in another classloader.
  public void openBackendStoresAtStartup(
      @jakarta.enterprise.event.Observes
          @jakarta.annotation.Priority(jakarta.interceptor.Interceptor.Priority.LIBRARY_BEFORE + 50)
          io.quarkus.runtime.StartupEvent event,
      Instance<BackendStores> stores) {
    if (stores.isResolvable()) {
      stores.get().backendName();
    }
  }

  /**
   * Closes the {@link #streamRuneBackendStores stores} when the container shuts down, which closes
   * every leadership they built. CDI destroys a producer-method product only through a matching
   * {@code @Disposes} method.
   *
   * @param stores the stores
   */
  public void closeBackendStores(@Disposes BackendStores stores) {
    stores.close();
  }

  /**
   * Creates the backend's EventStoreFactory: {@link BackendStores#eventStoreFactory} with the event
   * store's options. The factory is configured now and touches the database only in {@code
   * create()}.
   *
   * <p>Passes all {@link org.streamrune.core.upcasting.EventUpcaster} beans into the store so
   * stored events are upcast on read, and all {@link org.streamrune.core.SnapshotMigration} beans
   * so stored snapshots are upgraded on read; the {@link org.streamrune.core.audit.EventAuditStore}
   * and {@link CommandInbox} beans, which the store writes in the transaction of the append; and
   * the {@link org.streamrune.core.outbox.OutboxStore} bean together with the {@link
   * org.streamrune.core.outbox.OutboxEventMapper} bean, only when both are present (if exactly one
   * is present a warning is logged). The crypto engine, metrics and tracing come with the {@link
   * #streamRuneBackendStores} bundle.
   *
   * <p>The command bus ({@link #virtualThreadCommandBus}) resolves the same {@code CommandInbox}
   * bean, so the bus and the store share one inbox: the half-override the framework once had to
   * detect (the bus on the application's inbox, the store on the framework's) cannot happen, and an
   * inbox the backend cannot write in the append's transaction fails here.
   *
   * <p>On SQLite, which has no statement timeout, a {@code
   * streamrune.event-store.statement-timeout} the application set is ignored and logged once as a
   * WARN.
   *
   * @throws IllegalArgumentException if one of those store beans is not the backend's own type: the
   *     store could not share the event store's transaction
   * @throws IllegalStateException if {@code streamrune.event-store.statement-timeout} is a value
   *     the backend cannot honour
   */
  @Produces
  @Singleton
  @DefaultBean
  public EventStoreFactory postgresEventStoreFactory(
      BackendStores stores,
      EventTypeRegistry typeRegistry,
      StreamRuneQuarkusProperties properties,
      @All List<org.streamrune.core.upcasting.EventUpcaster> upcasters,
      @All List<org.streamrune.core.SnapshotMigration> snapshotMigrations,
      Instance<org.streamrune.core.audit.EventAuditStore> eventAuditStoreInstance,
      Instance<org.streamrune.core.outbox.OutboxEventMapper> outboxEventMapperInstance,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStoreInstance,
      Instance<CommandInbox> commandInboxInstance,
      Instance<org.eclipse.microprofile.config.Config> config) {
    // SQLite has no statement timeout: a value the application set is ignored there, with a WARN.
    org.streamrune.integration.BackendStartup.statementTimeoutIgnored(
        stores.backendName(),
        explicitlyConfigured(
            config, org.streamrune.integration.BackendStartup.STATEMENT_TIMEOUT_PROPERTY));
    boolean hasMapper = !outboxEventMapperInstance.isUnsatisfied();
    boolean hasStore = !outboxStoreInstance.isUnsatisfied();
    if (hasMapper != hasStore) {
      LOG.warn(
          "Transactional outbox emission is DISABLED: found {} but not the other. "
              + "Define BOTH an OutboxEventMapper and an OutboxStore bean to enable it.",
          hasMapper ? "an OutboxEventMapper" : "an OutboxStore");
    }
    boolean outboxPaired = hasMapper && hasStore;
    var options =
        new EventStoreOptions(
            typeRegistry,
            upcasters,
            // Mirrors the EventUpcaster collection so a user's standard SnapshotMigration bean is
            // threaded into the store's snapshot-upgrade pipeline. Empty list = no migrations.
            snapshotMigrations,
            outboxPaired ? outboxStoreInstance.get() : null,
            outboxPaired ? outboxEventMapperInstance.get() : null,
            // The bus resolves this same CommandInbox bean, so the bus and the store share one
            // inbox; one the backend cannot write in the append's transaction fails here.
            commandInboxInstance.isResolvable() ? commandInboxInstance.get() : null,
            // null when event audit is off: the framework's producer then declines.
            eventAuditStoreInstance.isUnsatisfied() ? null : eventAuditStoreInstance.get(),
            // The store borrows from the Agroal DataSource as given and bounds its own statements
            // per transaction or per statement; the bound is the one knob it needs.
            properties.eventStore().statementTimeout());
    try {
      return stores.eventStoreFactory(options);
    } catch (BackendOptionException e) {
      if (!"statementTimeout".equals(e.option())) {
        throw e;
      }
      throw new IllegalStateException(
          "streamrune.event-store.statement-timeout: "
              + LogSanitizer.sanitizeFreeText(e.getMessage()),
          e);
    }
  }

  /**
   * The raw value of {@code key} when a configuration source of the application sets it, or {@code
   * null} when only a default applies: the default values of a configuration mapping live in
   * sources of negative ordinal, below every source an application writes (properties files, the
   * environment, system properties). Without a {@code Config} bean, the configuration of this
   * thread's class loader.
   */
  static String explicitlyConfigured(
      Instance<org.eclipse.microprofile.config.Config> config, String key) {
    org.eclipse.microprofile.config.Config source =
        config.isResolvable()
            ? config.get()
            : org.eclipse.microprofile.config.ConfigProvider.getConfig();
    org.eclipse.microprofile.config.ConfigValue value = source.getConfigValue(key);
    return value.getRawValue() != null && value.getSourceOrdinal() > 0 ? value.getRawValue() : null;
  }

  /** Creates an EventStore from the factory. */
  @Produces
  @Singleton
  @DefaultBean
  public EventStore eventStore(EventStoreFactory factory) {
    return factory.create();
  }

  /**
   * Creates a default LocalStripedLocker with configured stripe count, tracing its acquisitions
   * through the {@link StreamRuneTracing} bean; a failure of that tracing counts in the {@link
   * org.streamrune.core.StreamRuneMetrics} bean.
   */
  @Produces
  @Singleton
  @DefaultBean
  public org.streamrune.core.AggregateLocker aggregateLocker(
      StreamRuneQuarkusProperties properties,
      StreamRuneTracing tracing,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance) {
    return new LocalStripedLocker(properties.stripeCount())
        .traced(
            org.streamrune.runtime.GuardedTracing.of(
                tracing, resolveMetricsOrDefault(metricsInstance)));
  }

  /**
   * The bean name ({@code @Named}) of the default read-model store, {@value}: a framework name, so
   * an application store {@code @Named("postgres")} never collides with it.
   */
  static final String DEFAULT_READ_MODEL_STORE_BEAN =
      org.streamrune.integration.ProjectionStoreRouting.DEFAULT_STORE_BEAN;

  /**
   * The bean name of the event store's subscription checkpoint, {@value}: the {@link
   * CheckpointTarget} the framework's projection runners route an {@code AT_LEAST_ONCE_IDEMPOTENT}
   * registration to when the registration matches no read-model store. Produce a {@code
   * CheckpointTarget} under that name to replace it.
   */
  static final String SUBSCRIPTION_CHECKPOINT = "streamRuneSubscriptionCheckpoint";

  /**
   * Produces the default read-model store, {@link BackendStores#defaultReadModelStore()}, named
   * after the backend ({@code postgres} by default) on the application's {@code DataSource}: on
   * PostgreSQL its read models live in {@code <name>_view} tables and its checkpoints live beside
   * them in {@code readmodel_checkpoint}, so a routed {@code TRANSACTIONAL_LOCAL} / {@code
   * EXTERNAL_EFFECT} registration's read-model writes and its checkpoint commit in one transaction.
   * The framework's projection runners route to it every registration that names it, writes to its
   * repository, or (transactional, with no write target of its own) has no other transactional
   * store to go to.
   *
   * <p>Creating the store checks the server version (on Postgres; any other backend verified the
   * {@code DataSource} when its stores were opened), applies the backend's read-model migration
   * series when {@code streamrune.read-model.schema.auto-initialize} — unset, {@code
   * streamrune.event-store.schema.auto-initialize} — is on, and validates the read-model schema.
   * Arc creates a {@code @Singleton} bean when it is first injected or looked up, and only {@link
   * #readModelStores} looks it up — when the stores are first selected — so the database is touched
   * when the first bean that needs the stores is created: in an application with projection
   * runners, the runners' producers on the startup event (or the {@link ProjectionIndexDeclarer},
   * earlier on the same event, when index beans exist).
   *
   * <p>Quarkus always provides a {@code DataSource} bean, so this store is produced even in an
   * application that uses no database: when the Postgres server version cannot be read (no usable
   * database), the failure says that such an application produces its own {@code ReadModelStore}
   * named after the backend. A Postgres server whose version is read and too old fails with {@link
   * org.streamrune.postgres.UnsupportedServerVersionException}, unwrapped.
   *
   * <p>Its repository's ObjectMapper wires the {@link CryptoEngine} bean (if present) through
   * {@code CryptoShreddingModule}, which the {@link #streamRuneBackendStores} bundle carries — so
   * {@code @Encrypted} read-model fields are persisted as ciphertext instead of plaintext PII, and
   * fall within GDPR forget scope.
   *
   * <p>Replaced only by name: an application {@link ReadModelStore} bean whose {@code name()} is
   * the backend's name takes its place, and this bean is then never created; a store of any other
   * name is added beside it ({@link #readModelStores}). {@code @DefaultBean} only keeps an
   * unqualified {@code ReadModelStore} injection point of the application resolving to the
   * application's store, as before.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} read-model fields as PLAINTEXT — or the
   *     Postgres server version cannot be read (no usable database)
   * @throws org.streamrune.postgres.UnsupportedServerVersionException if the server is older than
   *     StreamRune supports
   */
  @Produces
  @Singleton
  @DefaultBean
  @jakarta.inject.Named(DEFAULT_READ_MODEL_STORE_BEAN)
  public ReadModelStore postgresReadModelStore(DataSource dataSource, BackendStores stores) {
    if (!"postgres".equals(stores.backendName())) {
      // Any other backend verified the DataSource when the bundle was opened, at startup.
      return stores.defaultReadModelStore();
    }
    // The version check runs once more inside create; run first here, its "cannot read the server
    // version" failure — no usable database behind the DataSource Quarkus always provides — gets
    // the hint below. A readable but unsupported version is a real, old server: rethrown as is.
    try {
      org.streamrune.postgres.PostgresServerVersion.requireSupported(dataSource);
    } catch (org.streamrune.postgres.UnsupportedServerVersionException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "The default read-model store '"
              + stores.backendName()
              + "' cannot be created on the application's DataSource, which is not a usable"
              + " PostgreSQL database ("
              + e.getMessage()
              + "). Quarkus always provides a DataSource bean, so this store is produced even when"
              + " the application uses no database: an application without a database produces"
              + " its own ReadModelStore bean named '"
              + stores.backendName()
              + "', which replaces this default store.",
          e);
    }
    return stores.defaultReadModelStore();
  }

  /**
   * Whether the default read-model store applies its migration series: {@code
   * streamrune.read-model.schema.auto-initialize} when set, otherwise {@code
   * streamrune.event-store.schema.auto-initialize}.
   */
  static boolean readModelAutoInitialize(StreamRuneQuarkusProperties properties) {
    return properties
        .readModel()
        .schema()
        .autoInitialize()
        .orElse(properties.eventStore().schema().autoInitialize());
  }

  /**
   * Produces the application's read-model stores in one bean: every {@link ReadModelStore} bean the
   * application produces, whatever its qualifiers, and the default store ({@link
   * #postgresReadModelStore}) unless one of them is named after the backend ({@link
   * StreamRuneBackend#name()}, {@code postgres} by default; {@link
   * org.streamrune.integration.ProjectionStoreRouting#selectStores}). Selected and validated once,
   * when first asked for: producing this bean creates no store.
   *
   * <p>Replacing the default store is by name, which no bean condition can see: a store's name is
   * known only once the store exists. The application's stores are read through the handles of
   * {@code @Any Instance<ReadModelStore>}, leaving out the default store by its bean name, {@value
   * #DEFAULT_READ_MODEL_STORE_BEAN}, without creating it (a handle's bean is metadata; only {@code
   * get()} creates); the default store is looked up by that name only when no application store
   * took its name, so a replaced default store never touches its database. A {@code null} product
   * (a dependent-scoped producer that declined) is no store.
   *
   * @param storeInstance every read-model store bean
   * @param backend the backend, which names the default store
   * @return the stores
   * @hidden
   */
  @Produces
  @Singleton
  @org.streamrune.core.Internal
  public org.streamrune.integration.ReadModelStores readModelStores(
      @Any Instance<ReadModelStore> storeInstance, StreamRuneBackend backend) {
    return new org.streamrune.integration.ReadModelStores(
        backend.name(),
        () -> {
          Instance<ReadModelStore> named =
              storeInstance.select(
                  jakarta.enterprise.inject.literal.NamedLiteral.of(DEFAULT_READ_MODEL_STORE_BEAN));
          return named.isResolvable()
              ? java.util.Optional.of(named.get())
              : java.util.Optional.empty();
        },
        () -> {
          List<ReadModelStore> stores = new ArrayList<>();
          for (var handle : storeInstance.handles()) {
            if (DEFAULT_READ_MODEL_STORE_BEAN.equals(handle.getBean().getName())) {
              continue;
            }
            ReadModelStore store = handle.get();
            if (store != null) {
              stores.add(store);
            }
          }
          return stores;
        });
  }

  /**
   * Produces the repository of the store named after the backend — the default store's, or the
   * application store's that replaced it — as the injectable {@link ProjectionRepository}, so query
   * handlers and {@code BaseProjection} constructors that take a {@code ProjectionRepository} read
   * and write it; other stores' repositories are reached through their {@code ReadModelStore}
   * beans. {@code @DefaultBean}: it backs off when the application produces a {@code
   * ProjectionRepository} bean.
   *
   * <p>Finding the store named after the backend creates the application's store beans (a store's
   * name is known only once it exists) and the default store only when none of them replaced it.
   *
   * @throws IllegalStateException when the stores are invalid, or none is named after the backend
   * @hidden
   */
  @Produces
  @Singleton
  @DefaultBean
  @org.streamrune.core.Internal
  public ProjectionRepository projectionRepository(
      org.streamrune.integration.ReadModelStores stores) {
    return stores.defaultRepository();
  }

  /**
   * Produces the event store's subscription checkpoint, {@value #SUBSCRIPTION_CHECKPOINT}: the pair
   * the framework's projection runners read and commit an {@code AT_LEAST_ONCE_IDEMPOTENT}
   * registration through when the registration matches no read-model store: the {@link OffsetStore}
   * bean's {@link OffsetStore#subscriptionCheckpoint()}.
   *
   * <ul>
   *   <li>A {@link org.streamrune.postgres.PostgresOffsetStore} (the default one, or one the
   *       application produces on any {@code DataSource}) pairs itself with a {@link
   *       org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor} on the {@code
   *       DataSource} it reads — it fences the checkpoint under its row lock and hands the
   *       projection no repository, so the projection writes where it writes (at least once).
   *   <li>Any other offset store keeps the default: itself and {@link
   *       AtomicBatchProcessor#nonAtomicAtLeastOnce()}, which saves each checkpoint into it. It
   *       does not fence, so routing refuses an at-least-once registration onto it under real
   *       leadership.
   * </ul>
   *
   * <p>No {@code DataSource} is looked up here: both halves of the pair come from the offset store,
   * so the processor commits where the offset store reads.
   */
  @Produces
  @Singleton
  @DefaultBean
  @jakarta.inject.Named(SUBSCRIPTION_CHECKPOINT)
  public CheckpointTarget subscriptionCheckpoint(OffsetStore offsetStore) {
    // A PostgresOffsetStore overrides it with the fencing processor built on the DataSource it
    // reads; any other offset store keeps the nonatomic default.
    return offsetStore.subscriptionCheckpoint();
  }

  /**
   * Produces the backend's {@link BackendStores#offsetStore()} as the default {@link OffsetStore}.
   */
  @Produces
  @Singleton
  @DefaultBean
  public OffsetStore offsetStore(BackendStores stores) {
    return stores.offsetStore();
  }

  /**
   * Creates a default {@link SimpleQueryBus} for query dispatching on the read side. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean (e.g. the auto-produced {@link
   * MicrometerStreamRuneMetrics}) is wired in when present so {@code queries.dispatched} and {@code
   * queries.duration} are recorded. Metrics are attached to this innermost bus only — the
   * decorating {@link AuditingQueryBus} in {@link #queryBus} is left without metrics so
   * dispatched/duration are counted exactly once. Cache hits short-circuit before reaching this bus
   * and are tracked separately via the {@link CachingQueryBus} cache meters.
   *
   * <p>Exposed as {@code SimpleQueryBus} ONLY ({@code @Typed}): it is a building block of {@link
   * #queryBus}, and {@code SimpleQueryBus implements QueryBus} means that without {@code @Typed} it
   * would ALSO carry the {@link QueryBus} bean type — making it a second {@code @DefaultBean}
   * candidate for {@code QueryBus} injection points alongside {@link #queryBus} itself. Arc's
   * {@code Beans.resolveAmbiguity} strips every {@code @DefaultBean} candidate first; with two (or
   * three, once {@link #cachingQueryBus} is enabled) un-prioritised survivors it falls back to
   * comparing {@code @DefaultBean} priorities, finds them all equal (default 0), and reports the
   * injection point ambiguous — a real Quarkus application's BUILD fails the moment it injects
   * {@code QueryBus}. This mirrors Micronaut's {@code @Bean(typed = SimpleQueryBus.class)} and
   * Spring's {@code @ConditionalOnMissingBean(value = QueryBus.class, ignored =
   * {SimpleQueryBus.class, ...})} ignore list — on each framework the building blocks must not
   * themselves satisfy plain {@code QueryBus} resolution.
   */
  @Produces
  @Singleton
  @DefaultBean
  @Typed(SimpleQueryBus.class)
  public SimpleQueryBus simpleQueryBus(Instance<org.streamrune.core.StreamRuneMetrics> metrics) {
    return new SimpleQueryBus(resolveMetricsOrDefault(metrics));
  }

  /**
   * Resolves the optional {@link org.streamrune.core.StreamRuneMetrics} bean. The framework's own
   * {@link #micrometerStreamRuneMetrics} producer is {@code @Singleton} and never yields {@code
   * null} (it hands out {@code StreamRuneMetrics.NOOP} when metrics are disabled or no registry
   * exists), but an application-supplied dependent-scoped producer may still hand back a null
   * product, so the value obtained from {@link Instance#get()} is null-checked regardless.
   */
  private static org.streamrune.core.StreamRuneMetrics resolveMetricsOrNull(
      Instance<org.streamrune.core.StreamRuneMetrics> metrics) {
    return metrics.isResolvable() ? metrics.get() : null;
  }

  /**
   * The effective {@link SnapshotPolicy} for the command bus: an application-supplied bean when one
   * resolves, otherwise the shipped default derived from {@code streamrune.snapshot-every-n-events}
   * at snapshot schema version 1. Resolve-then-null-check rather than trusting {@code
   * isResolvable()} alone, because a dependent-scoped producer in this class may hand back a null
   * product.
   */
  /**
   * The policy the dead-letter retry runner replays under, built from {@code
   * streamrune.dead-letter.*}: the runner and the command bus take the same one, so the bus answers
   * a keyed command that fails again under its key by the rules the runner replays by.
   */
  static org.streamrune.core.DeadLetterRetryPolicy deadLetterRetryPolicy(
      StreamRuneQuarkusProperties properties) {
    return new org.streamrune.core.DeadLetterRetryPolicy(
        properties.deadLetter().maxRetries(),
        properties.deadLetter().retryInterval(),
        2.0,
        false,
        org.streamrune.integration.DeadLetterUnkeyedReplaySetting.parse(
            properties.deadLetter().unkeyedReplay()));
  }

  private static SnapshotPolicy resolveSnapshotPolicy(
      Instance<SnapshotPolicy> snapshotPolicyInstance, StreamRuneQuarkusProperties properties) {
    SnapshotPolicy supplied =
        snapshotPolicyInstance.isResolvable() ? snapshotPolicyInstance.get() : null;
    return supplied != null
        ? supplied
        : SnapshotPolicy.everyNEvents(properties.snapshotEveryNEvents());
  }

  /**
   * Creates the {@link SseEventPublisher} the Server-Sent Events endpoint subscribes its clients
   * to. When the endpoint is enabled, {@link SseEventFeedLifecycle} publishes the stored events to
   * it.
   */
  @Produces
  @Singleton
  @DefaultBean
  public SseEventPublisher sseEventPublisher() {
    return new SseEventPublisher();
  }

  /**
   * Disposer for {@link #sseEventPublisher()}. CDI/Arc destroys a producer-method product only
   * through a matching {@code @Disposes} method — mirroring {@link
   * StreamRuneCryptoProducers.VaultCryptoProducer#closeVaultHttpClient}. Without this, every
   * still-registered SSE subscription's dedicated virtual-thread delivery worker, and its bounded
   * queue of up to 256 decrypted {@code EventEnvelope}s, survives every context teardown (dev-mode
   * live reload, a {@code @QuarkusTest} suite standing up several contexts) — accumulating one such
   * fleet per cycle.
   */
  public void closeSsePublisher(@Disposes SseEventPublisher publisher) {
    publisher.close();
  }

  /**
   * Produces a {@link BeanValidationInterceptor} when Jakarta Validation is available and
   * validation is enabled. Dependent-scoped optional bean: returns {@code null} when disabled, no
   * Validator bean exists, and no validation provider (e.g. Hibernate Validator) is on the
   * classpath — command validation is an optional feature and must not fail application startup.
   * The no-provider case logs a WARN naming the remedy, exactly as on Spring: silently disabled
   * command validation deserves a visible signal on every framework.
   */
  @Produces
  @DefaultBean
  public BeanValidationInterceptor beanValidationInterceptor(
      StreamRuneQuarkusProperties properties,
      Instance<jakarta.validation.Validator> validatorInstance) {
    if (!properties.validationEnabled()) {
      return null;
    }
    if (!validatorInstance.isUnsatisfied()) {
      return new BeanValidationInterceptor(validatorInstance.get());
    }
    try {
      return new BeanValidationInterceptor(
          jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator());
    } catch (jakarta.validation.ValidationException e) {
      // No provider on the classpath (NoProviderFoundException extends ValidationException).
      LOG.warn(
          "StreamRune: jakarta.validation-api is on the classpath but no Bean Validation provider"
              + " could be bootstrapped — command validation is DISABLED. Add hibernate-validator"
              + " (or another provider) to enable it, or set streamrune.validation-enabled=false"
              + " to silence this warning. Cause: {}",
          e.getMessage());
      return null;
    }
  }

  /**
   * Produces the single shared {@link org.streamrune.core.StreamRuneMetrics} bean: a {@link
   * MicrometerStreamRuneMetrics} when metrics are enabled and a {@code MeterRegistry} bean exists,
   * otherwise the inert {@link org.streamrune.core.StreamRuneMetrics#NOOP}. Meter names start with
   * the {@code streamrune.metrics.prefix} property (default {@code streamrune}).
   *
   * <p><strong>{@code @Singleton}, not {@code @Dependent}.</strong> This producer carried no scope,
   * so every one of the {@code Instance<StreamRuneMetrics>} injection points in this class and its
   * siblings (command bus, query buses, DLQ retry runner, outbox poller, projection producers,
   * health contributor, retention sweepers, saga lifecycle, …) received its OWN {@code
   * MicrometerStreamRuneMetrics}. Each instance registers the same gauge ids against the shared
   * registry and Micrometer's {@code register()} dedups by id, so only the FIRST instance's {@code
   * AtomicLong}s backed the gauges — every other consumer's backlog/degradation samples went to an
   * orphaned field and never reached a scrape. Spring ({@code @Bean}) and Micronaut
   * ({@code @Singleton}) were already single-instance; {@link #subscriptionHealthContributor} is
   * the precedent for the same class of defect in this file.
   *
   * <p>CDI forbids a null {@code @Singleton} product, so the disabled / no-registry path returns
   * {@code StreamRuneMetrics.NOOP} instead of {@code null} — exactly what every consumer already
   * substituted for an absent bean (see {@link #resolveMetricsOrNull}), mirroring {@link
   * #subscriptionLeadership}. The declared type is the {@code StreamRuneMetrics} interface so the
   * NOOP constant is a valid product; {@code @DefaultBean} is kept, so an application-supplied
   * {@code StreamRuneMetrics} bean still wins every injection point.
   */
  @Produces
  @Singleton
  @DefaultBean
  public org.streamrune.core.StreamRuneMetrics micrometerStreamRuneMetrics(
      Instance<io.micrometer.core.instrument.MeterRegistry> registry,
      StreamRuneQuarkusProperties properties) {
    if (!properties.metrics().enabled() || !registry.isResolvable()) {
      return org.streamrune.core.StreamRuneMetrics.NOOP;
    }
    return MicrometerStreamRuneMetrics.builder()
        .registry(registry.get())
        .prefix(properties.metrics().prefix())
        .build();
  }

  /** Produces a circuit breaker interceptor with configured failure threshold and cooldown. */
  @Produces
  @Singleton
  @DefaultBean
  public CircuitBreakerCommandInterceptor circuitBreakerCommandInterceptor(
      StreamRuneQuarkusProperties properties) {
    return new CircuitBreakerCommandInterceptor(
        properties.circuitBreakerFailureThreshold(), properties.circuitBreakerCooldown());
  }

  /**
   * The tracing every component these producers build starts its spans through, under the switches
   * of {@code streamrune.tracing.*} (runtime configuration). It traces through the application's
   * {@code OpenTelemetry} bean (the one the Quarkus OpenTelemetry extension produces), or, without
   * one, through the global OpenTelemetry ({@code GlobalOpenTelemetry.get()}), which the
   * OpenTelemetry Java agent registers before the application starts. It is {@link
   * StreamRuneTracing#NOOP} when the master switch is off, and when that OpenTelemetry has no
   * tracer provider of its own (nothing registered), so an application without OpenTelemetry pays
   * nothing. It is {@code NOOP} too when the configuration holds one of the {@link
   * org.streamrune.integration.OpenTelemetrySwitches#QUARKUS switches} that turn OpenTelemetry
   * tracing off ({@code quarkus.otel.sdk.disabled=true}, {@code otel.sdk.disabled=true}, and more).
   * An application {@code StreamRuneTracing} bean replaces it.
   *
   * <p>The global OpenTelemetry is read once, here: OpenTelemetry fixes it on its first read, so an
   * SDK the application registers globally is registered before the application starts, or is an
   * {@code OpenTelemetry} bean.
   */
  @Produces
  @Singleton
  @DefaultBean
  public StreamRuneTracing streamRuneTracing(
      Instance<OpenTelemetry> openTelemetry,
      StreamRuneQuarkusProperties properties,
      Instance<org.eclipse.microprofile.config.Config> config,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance) {
    // The application's configuration; without a Config bean, the one of this thread's class
    // loader, which reads system properties and environment variables too.
    org.eclipse.microprofile.config.Config source =
        config.isResolvable()
            ? config.get()
            : org.eclipse.microprofile.config.ConfigProvider.getConfig();
    // Made counting: a failure of the tracing counts in the metrics bean wherever the bean is
    // handed to, the application's own components included.
    return org.streamrune.runtime.GuardedTracing.of(
        tracingOf(
            openTelemetry.isResolvable() ? openTelemetry.get() : null,
            tracingSettings(properties.tracing()),
            key -> source.getOptionalValue(key, String.class).orElse(null)),
        resolveMetricsOrDefault(metricsInstance));
  }

  /**
   * The tracing over {@code openTelemetry}, or over the global OpenTelemetry when it is {@code
   * null}, under {@code settings}; {@link StreamRuneTracing#NOOP} when {@code property} holds one
   * of the {@link org.streamrune.integration.OpenTelemetrySwitches#QUARKUS switches} that turn
   * OpenTelemetry tracing off, {@code quarkus.otel.sdk.disabled=true} among them.
   */
  static StreamRuneTracing tracingOf(
      OpenTelemetry openTelemetry,
      TracingSettings settings,
      java.util.function.Function<String, String> property) {
    if (!settings.enabled()) {
      return StreamRuneTracing.NOOP;
    }
    var switchedOff =
        org.streamrune.integration.OpenTelemetrySwitches.switchedOffBy(
            org.streamrune.integration.OpenTelemetrySwitches.QUARKUS, property);
    if (switchedOff.isPresent()) {
      LOG.info(
          "StreamRune starts no span: OpenTelemetry tracing is switched off ({})",
          switchedOff.get());
      return StreamRuneTracing.NOOP;
    }
    OpenTelemetry source =
        openTelemetry != null ? openTelemetry : io.opentelemetry.api.GlobalOpenTelemetry.get();
    if (source.getTracerProvider() == io.opentelemetry.api.trace.TracerProvider.noop()) {
      return StreamRuneTracing.NOOP;
    }
    return OpenTelemetryTracing.of(source, settings);
  }

  /** {@code streamrune.tracing.*} as the {@link TracingSettings} the tracing takes. */
  static TracingSettings tracingSettings(StreamRuneQuarkusProperties.TracingConfig tracing) {
    return TracingSettings.builder()
        .enabled(tracing.enabled())
        .area(TracingArea.COMMAND, tracing.command().enabled())
        .area(TracingArea.SUBSCRIPTION, tracing.subscription().enabled())
        .area(TracingArea.PROJECTION, tracing.projection().enabled())
        .area(TracingArea.EVENT_STORE, tracing.eventStore().enabled())
        .area(TracingArea.LOCK, tracing.lock().enabled())
        .area(TracingArea.OUTBOX, tracing.outbox().enabled())
        .area(TracingArea.SAGA, tracing.saga().enabled())
        .build();
  }

  /**
   * The locker the command bus takes, tracing through {@code tracing} when it is one of the
   * framework's lockers: {@link LocalStripedLocker} and {@link
   * org.streamrune.postgres.PgAdvisoryLocker} return a locker on the same locks that starts {@code
   * lock/acquire}. Any other locker is returned as it is.
   */
  static org.streamrune.core.AggregateLocker traced(
      org.streamrune.core.AggregateLocker locker, StreamRuneTracing tracing) {
    // Asked through the guard, like every other use of the tracing: one that throws here traces
    // no lock and fails no startup.
    if (!org.streamrune.runtime.GuardedTracing.of(tracing).enabled(TracingArea.LOCK)) {
      return locker;
    }
    if (locker instanceof LocalStripedLocker local) {
      return local.traced(tracing);
    }
    if (locker instanceof org.streamrune.postgres.PgAdvisoryLocker advisory) {
      return advisory.traced(tracing);
    }
    return locker;
  }

  /**
   * Produces the single shared {@link org.streamrune.runtime.SubscriptionHealthContributor}.
   *
   * <p><strong>{@code @Singleton}, not {@code @Dependent}.</strong> Every injection point — {@link
   * ProjectionProducer#multiProjectionRunner}, {@link
   * ProjectionProducer#scheduledProjectionRunner}, and {@link StreamRuneHealthCheck} — must resolve
   * the <em>same</em> instance. A {@code @Dependent} producer (the prior bug) hands the runner and
   * the health check <em>different</em> contributors, each with its own registration map, so a
   * projection halt (the runner calling {@code markTerminalError} on ITS instance) never reaches
   * {@code /q/health/ready}, which consults its own empty instance and reports UP while the read
   * model is frozen. Making the producer {@code @Singleton} — mirroring {@link
   * #subscriptionLeadership} — shares one instance across all three.
   *
   * <p>CDI forbids a null {@code @Singleton} product, so the OffsetStore-absent path returns an
   * inert contributor backed by {@link #INERT_OFFSET_STORE} (no projections register without a real
   * OffsetStore, so its empty registration map yields {@code overallStatus()} UP — behavior
   * unchanged) instead of {@code null}, again mirroring the {@code subscriptionLeadership} NOOP
   * pattern. The {@link org.streamrune.core.StreamRuneMetrics} bean (e.g. the auto-produced {@link
   * MicrometerStreamRuneMetrics}) is wired in when present so each health sample emits the {@code
   * streamrune.subscriptions.lag} gauge; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when absent (behavior unchanged). The lag at which
   * a running subscription reads {@code DEGRADED} is {@code
   * streamrune.subscription.health.lag-threshold} (default 1000 events); lag never reads {@code
   * DOWN}, so it never takes the instance out of {@code /q/health/ready}.
   */
  @Produces
  @Singleton
  @DefaultBean
  public org.streamrune.runtime.SubscriptionHealthContributor subscriptionHealthContributor(
      EventStore eventStore,
      Instance<OffsetStore> offsetStoreInstance,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance,
      StreamRuneQuarkusProperties properties) {
    long lagThreshold = properties.subscription().health().lagThreshold();
    if (lagThreshold < 1) {
      throw new IllegalStateException(
          "streamrune.subscription.health.lag-threshold must be at least 1, got: " + lagThreshold);
    }
    OffsetStore offsetStore =
        offsetStoreInstance.isUnsatisfied() ? null : offsetStoreInstance.get();
    if (offsetStore == null) {
      offsetStore = INERT_OFFSET_STORE;
    }
    org.streamrune.core.StreamRuneMetrics metrics = resolveMetricsOrDefault(metricsInstance);
    return new org.streamrune.runtime.SubscriptionHealthContributor(
        eventStore, offsetStore, lagThreshold, metrics);
  }

  /**
   * Inert {@link OffsetStore} used only to keep the {@code @Singleton} {@link
   * #subscriptionHealthContributor} product non-null when no real OffsetStore bean exists (CDI
   * forbids a null singleton). Reports offset 0 and swallows saves; it is never registered against
   * a live projection (there are no projection runners without a real OffsetStore), so its
   * lag/checkpoint behavior is never exercised. Analogous to {@link
   * org.streamrune.core.subscription.SubscriptionLeadership#NOOP}.
   */
  private static final OffsetStore INERT_OFFSET_STORE =
      new OffsetStore() {
        @Override
        public org.streamrune.core.types.GlobalOffset getLastOffset(
            org.streamrune.core.types.ProjectionName projectionName) {
          return org.streamrune.core.types.GlobalOffset.initial();
        }

        @Override
        public void saveOffset(
            org.streamrune.core.types.ProjectionName projectionName,
            org.streamrune.core.types.GlobalOffset offset) {
          // inert: no live projection is ever registered against this store
        }
      };

  /**
   * Produces an {@link AuthorizationCommandInterceptor} when a {@link CommandAuthorizationPolicy}
   * bean is available. Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public AuthorizationCommandInterceptor authorizationCommandInterceptor(
      Instance<CommandAuthorizationPolicy> policy) {
    if (policy.isUnsatisfied()) {
      return null;
    }
    return new AuthorizationCommandInterceptor(policy.get());
  }

  /**
   * Produces a {@link QuarkusSecurityUserRoleResolver} when Quarkus Security is available.
   * Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public QuarkusSecurityUserRoleResolver quarkusSecurityUserRoleResolver(
      Instance<SecurityIdentity> identity) {
    if (identity.isUnsatisfied()) {
      return null;
    }
    return new QuarkusSecurityUserRoleResolver(identity.get());
  }

  /**
   * Produces an {@link AuthenticatedUserResolver} backed by the Quarkus {@link SecurityIdentity}
   * when Quarkus Security is available, so the request filter can bind the authorization identity
   * from the authenticated principal rather than the client-supplied {@code X-User-Id} header.
   * Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public AuthenticatedUserResolver quarkusAuthenticatedUserResolver(
      Instance<SecurityIdentity> identity) {
    if (identity.isUnsatisfied()) {
      return null;
    }
    return new QuarkusAuthenticatedUserResolver(identity.get());
  }

  /**
   * The one rule by which every request-facing component ({@link StreamRuneRequestFilter}, {@link
   * SseController}) derives a request's identity, so they can never disagree — the same {@link
   * RequestIdentityPolicy} the Spring integration uses. {@code
   * streamrune.security.trust-user-id-header=true} selects the trusted-gateway mode (the {@code
   * X-User-Id} header is the identity); otherwise an {@link AuthenticatedUserResolver} (Quarkus
   * Security's {@link QuarkusAuthenticatedUserResolver}, or an application bean) selects the
   * authenticated principal, and without one every request is anonymous and the header is ignored.
   * Fail-closed: the header is never an identity unless the flag says so.
   *
   * <p>{@code @Singleton}: one decision for the whole application, made once. The resolver producer
   * above is dependent-scoped and yields {@code null} when Quarkus Security is absent, so {@code
   * isResolvable()} alone is not a presence test — the product itself is checked.
   *
   * <p>More than one {@link AuthenticatedUserResolver} bean is refused: CDI reports an ambiguous
   * {@code Instance} as not resolvable, exactly like an unsatisfied one, so without this check the
   * application would silently run every request anonymously — where Spring and Micronaut fail on
   * the same configuration.
   *
   * @throws IllegalStateException if more than one {@link AuthenticatedUserResolver} bean exists
   */
  @Produces
  @Singleton
  @DefaultBean
  public RequestIdentityPolicy requestIdentityPolicy(
      StreamRuneQuarkusProperties properties,
      Instance<AuthenticatedUserResolver> authenticatedUserResolver) {
    if (authenticatedUserResolver.isAmbiguous()) {
      List<String> beans = new ArrayList<>();
      authenticatedUserResolver
          .handles()
          .forEach(handle -> beans.add(handle.getBean().getBeanClass().getName()));
      throw new IllegalStateException(
          "There is more than one AuthenticatedUserResolver bean ("
              + String.join(", ", beans)
              + "), so the request identity is ambiguous. Keep exactly one.");
    }
    AuthenticatedUserResolver resolver =
        authenticatedUserResolver.isResolvable() ? authenticatedUserResolver.get() : null;
    return RequestIdentityPolicy.of(resolver, properties.security().trustUserIdHeader());
  }

  /**
   * Fail-closed {@link SseAuthorizer} used when SSE is enabled ({@code
   * streamrune.sse.enabled=true}) but the application provides no authorizer of its own. Denies
   * every stream, so enabling SSE never exposes an aggregate's events without an explicit access
   * decision. Applications override it by producing their own {@link SseAuthorizer} bean (this is a
   * {@link DefaultBean}).
   */
  @Produces
  @DefaultBean
  @IfBuildProperty(name = "streamrune.sse.enabled", stringValue = "true")
  public SseAuthorizer sseDenyAllAuthorizer() {
    return SseAuthorizer.DENY_ALL;
  }

  /**
   * Produces an {@link AnnotationAuthorizationInterceptor} when a {@link UserRoleResolver} is
   * available. Dependent-scoped optional bean.
   *
   * <p>The resolver is used as-is — it is deliberately <em>not</em> wrapped in {@link
   * org.streamrune.runtime.CachingUserRoleResolver}. Caching by {@link
   * org.streamrune.core.types.UserId} is unsound for resolvers that derive authorities from ambient
   * per-request state (e.g. {@link QuarkusSecurityUserRoleResolver} reading the request {@code
   * SecurityIdentity}): a cache hit would bypass the resolver's principal-match check and serve one
   * principal's authorities to a different caller presenting the same user ID. Applications whose
   * resolver is a pure function of the user ID (e.g. a database lookup) can opt in explicitly by
   * producing a {@code CachingUserRoleResolver} bean wrapping their resolver.
   */
  @Produces
  @DefaultBean
  public AnnotationAuthorizationInterceptor annotationAuthorizationInterceptor(
      Instance<UserRoleResolver> resolver) {
    if (resolver.isUnsatisfied()) {
      return null;
    }
    UserRoleResolver raw = resolver.get();
    if (raw == null) {
      return null;
    }
    return new AnnotationAuthorizationInterceptor(raw);
  }

  /**
   * Produces the backend's {@link BackendStores#eventAuditStore()} when event audit is enabled
   * ({@code streamrune.event-audit-enabled=true}). Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public org.streamrune.core.audit.EventAuditStore postgresEventAuditStore(
      BackendStores stores, StreamRuneQuarkusProperties properties) {
    if (!properties.eventAuditEnabled()) {
      return null;
    }
    return stores.eventAuditStore();
  }

  /**
   * Produces the backend's {@link BackendStores#eventAuditQuery()} when event audit is enabled.
   * Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public org.streamrune.core.audit.EventAuditQuery postgresEventAuditQuery(
      BackendStores stores, StreamRuneQuarkusProperties properties) {
    if (!properties.eventAuditEnabled()) {
      return null;
    }
    return stores.eventAuditQuery();
  }

  /**
   * Produces the backend's {@link BackendStores#projectionDeadLetterStore()} when projection DLQ is
   * enabled ({@code streamrune.projection-dlq-enabled=true}). Dependent-scoped optional bean.
   */
  @Produces
  @DefaultBean
  public org.streamrune.core.projection.ProjectionDeadLetterStore postgresProjectionDeadLetterStore(
      BackendStores stores, StreamRuneQuarkusProperties properties) {
    if (!properties.projectionDlqEnabled()) {
      return null;
    }
    return stores.projectionDeadLetterStore();
  }

  /**
   * Produces the backend's {@link BackendStores#sagaStore()} as the default {@link SagaStore} when
   * sagas are enabled. Dependent-scoped optional bean: {@code null} when {@code
   * streamrune.saga.enabled=false}.
   *
   * <p><strong>One store per application.</strong> A dependent-scoped producer is called once per
   * injection point; the bundle memoizes its saga store, so every injection point receives the same
   * object: each {@code SagaRunner} producer of the application that takes a {@code SagaStore}
   * parameter, the compensation-retry sweepers and the saga dead-letter wiring work on it. Runners
   * that share their rows so share their store, which is the first thing the startup check that
   * refuses two saga runners over one saga state class and one store compares ({@code
   * SagaTypeOwnership.requireOneRunnerPerSagaType}). The product cannot be {@code @Singleton} like
   * the other shared products of this class, because the disabled case answers {@code null}, which
   * CDI permits for a dependent bean only.
   *
   * <p>The saga store's ObjectMapper wires the {@link CryptoEngine} bean (if present) through
   * {@code CryptoShreddingModule}, which the {@link #streamRuneBackendStores} bundle carries — so
   * {@code @Encrypted} saga-state fields are persisted as ciphertext instead of plaintext PII, and
   * fall within GDPR forget scope.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} saga-state fields as PLAINTEXT
   */
  @Produces
  @DefaultBean
  public SagaStore sagaStore(BackendStores stores, StreamRuneQuarkusProperties properties) {
    if (!properties.saga().enabled()) {
      return null;
    }
    return stores.sagaStore();
  }

  /**
   * Produces the backend's {@link BackendStores#sagaDeadLetterStore()} as the default {@link
   * SagaDeadLetterStore} when sagas are enabled. Dependent-scoped optional bean.
   *
   * <p>Declared as the interface: Arc fixes a producer's bean types from its declared return type,
   * so no injection point may ask for a backend's store type to find this bean. {@link
   * #sagaDeadLetterRetentionSweeper} therefore takes the framework's store from the bundle itself,
   * not through this bean, and {@code @DefaultBean} lets an application's {@code
   * SagaDeadLetterStore} win every injection point.
   */
  @Produces
  @DefaultBean
  public SagaDeadLetterStore sagaDeadLetterStore(
      BackendStores stores, StreamRuneQuarkusProperties properties) {
    if (!properties.saga().enabled()) {
      return null;
    }
    return stores.sagaDeadLetterStore();
  }

  /**
   * Produces the backend's {@link BackendStores#commandInbox()} as the default {@link
   * CommandInbox}. Dependent-scoped bean: the inbox is used by {@link VirtualThreadCommandBus} and
   * by the event store ({@link #postgresEventStoreFactory}) for idempotent saga command dispatch;
   * the bundle memoizes it, so both receive the same object.
   *
   * <p>Declared as the interface: Arc fixes a producer's bean types from its declared return type,
   * so no injection point may ask for a backend's store type to find this bean. {@link
   * #inboxRetentionSweeper} therefore takes the framework's inbox from the bundle itself.
   *
   * <p><strong>What overriding actually requires.</strong> Because the store must claim the
   * idempotency key in the SAME transaction as the append, a custom inbox is only honoured
   * end-to-end if the event store can write it there. The bus and the event store read the same
   * {@code CommandInbox} bean, so a custom inbox that is not the backend's own type fails startup
   * when the event store factory is built — unless the application also supplies its own {@code
   * EventStoreFactory}/{@code EventStore} whose {@code appendWithKey} claims the key atomically.
   */
  @Produces
  @DefaultBean
  public CommandInbox commandInbox(BackendStores stores) {
    return stores.commandInbox();
  }

  /**
   * Produces the backend's {@link BackendStores#auditStore()} as the default {@link AuditStore}.
   */
  @Produces
  @Singleton
  @DefaultBean
  public AuditStore auditStore(BackendStores stores) {
    return stores.auditStore();
  }

  /**
   * Produces the backend's {@link BackendStores#complianceReportQuery()} as the default {@link
   * ComplianceReportQuery}.
   */
  @Produces
  @Singleton
  @DefaultBean
  public ComplianceReportQuery complianceReportQuery(BackendStores stores) {
    return stores.complianceReportQuery();
  }

  /**
   * Produces the backend's {@link BackendStores#commandAuditQuery()} as the default {@link
   * org.streamrune.core.audit.CommandAuditQuery}.
   */
  @Produces
  @Singleton
  @DefaultBean
  public org.streamrune.core.audit.CommandAuditQuery commandAuditQuery(BackendStores stores) {
    return stores.commandAuditQuery();
  }

  /**
   * Produces an {@link AuditCommandInterceptor} when an {@link AuditStore} bean is available.
   * Dependent-scoped optional bean: returns {@code null} when no audit store exists.
   */
  @Produces
  @DefaultBean
  public AuditCommandInterceptor auditCommandInterceptor(Instance<AuditStore> store) {
    if (store.isUnsatisfied()) {
      return null;
    }
    AuditStore auditStore = store.get();
    if (auditStore == null) {
      return null;
    }
    return new AuditCommandInterceptor(auditStore);
  }

  /**
   * Produces a {@link ForgetSubjectService} when a {@link CryptoEngine} bean is available.
   * Dependent-scoped optional bean: returns {@code null} when no {@code CryptoEngine} is
   * registered, so the application sees a missing-bean error only if it actually injects the
   * service.
   *
   * <p>All registered {@link SubjectDataPurger} beans are collected via {@code @All} and wired in,
   * mirroring how {@link #exportSubjectDataService} collects its {@link SubjectDataCollector}
   * beans. Without this the auto-produced forget would crypto-shred the key but never purge
   * read-model projections, leaving derived PII behind after a GDPR Article-17 erasure.
   *
   * <p>The {@link CachingQueryBus}, when {@code streamrune.query-cache.enabled=true} produces one,
   * is wired in as the forget's query cache: every forget evicts it once its purgers have run, so a
   * cached answer holding the subject's data is not served after the erasure.
   *
   * <p>One of the purger beans must be a {@link org.streamrune.core.gdpr.SnapshotPurger} — for
   * {@code PostgresEventStore}, a {@code PostgresSnapshotStorePurger} the application produces,
   * since only its domain knows which streams' snapshots hold a subject's data — unless {@code
   * streamrune.gdpr.snapshot-purge=none} states that the application has never stored a snapshot.
   * Otherwise the producer throws ({@link
   * org.streamrune.integration.SnapshotPurgeSetting#withoutSnapshotPurge}); {@link
   * StreamRuneConfigValidator} resolves this bean on the startup event, so that is a startup
   * failure, not a failure of the first request that injects the service.
   */
  @Produces
  @DefaultBean
  public ForgetSubjectService forgetSubjectService(
      Instance<CryptoEngine> cryptoEngine,
      Instance<AuditStore> auditStore,
      @All List<SubjectDataPurger> purgers,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance,
      Instance<CachingQueryBus> queryCache,
      StreamRuneQuarkusProperties properties) {
    if (cryptoEngine.isUnsatisfied()) {
      return null;
    }
    // A normal-scoped snapshot purger arrives as a client proxy without the SnapshotPurger marker;
    // expose it, so the snapshot-purge rule sees the purger the application declared.
    List<SubjectDataPurger> exposed = SnapshotPurgerReferences.exposeSnapshotPurgers(purgers);
    var b = ForgetSubjectService.builder().cryptoEngine(cryptoEngine.get()).purgers(exposed);
    if (org.streamrune.integration.SnapshotPurgeSetting.parse(properties.gdpr().snapshotPurge())
        .withoutSnapshotPurge(exposed)) {
      b.withoutSnapshotPurge();
    }
    if (!auditStore.isUnsatisfied()) {
      b.auditStore(auditStore.get());
    }
    // Wire metrics so a failed read-model purge increments streamrune.gdpr.purge_failed.
    b.metrics(resolveMetricsOrDefault(metricsInstance));
    // A forget evicts the query cache after its last purger, so no cached answer taken before the
    // erasure is served after it. An ambiguous CachingQueryBus fails loud rather than leave one
    // un-evicted.
    if (queryCache.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CachingQueryBus beans exist but none is resolvable as default — StreamRune"
              + " will not build a ForgetSubjectService that evicts only one of them; mark one"
              + " @Default/@Priority or remove the extra bean");
    }
    if (queryCache.isResolvable()) {
      b.queryCache(queryCache.get());
    }
    return b.build();
  }

  /**
   * Produces an {@link ExportSubjectDataService} aggregating all registered {@link
   * SubjectDataCollector} beans. Quarkus quirk: {@code @Produces} cannot easily condition on a
   * collection being non-empty, so the service is always constructed; if no collectors are
   * registered, {@link ExportSubjectDataService#export(org.streamrune.core.types.SubjectId,
   * org.streamrune.core.types.UserId) export} throws {@link IllegalStateException} at call time.
   */
  @Produces
  @Singleton
  @DefaultBean
  public ExportSubjectDataService exportSubjectDataService(
      @All List<SubjectDataCollector> collectors, Instance<AuditStore> auditStore) {
    var b = ExportSubjectDataService.builder().collectors(collectors);
    if (!auditStore.isUnsatisfied()) {
      b.auditStore(auditStore.get());
    }
    return b.build();
  }

  /**
   * Produces a {@link CachingQueryBus} decorating {@link SimpleQueryBus} with per-query-type
   * caching. Gated at build time on {@code streamrune.query-cache.enabled=true}: when caching is
   * disabled (the default) this bean does not exist at all, and {@link #queryBus} falls back to the
   * plain {@link SimpleQueryBus}. The {@link org.streamrune.core.StreamRuneMetrics} bean is wired
   * in when present so {@code queries.cache.hits} and {@code queries.cache.misses} are recorded;
   * the caching bus does not record dispatched/duration, so this does not double-count with {@link
   * #simpleQueryBus}.
   *
   * <p>Exposed as {@code CachingQueryBus} ONLY, for the same reason as {@link #simpleQueryBus}:
   * {@code CachingQueryBus implements QueryBus}, so without {@code @Typed} it would be a third
   * {@code @DefaultBean} candidate for plain {@code QueryBus} injection points.
   *
   * <p>An application-supplied {@link org.streamrune.core.QueryAuthorizer} bean, if resolvable, is
   * wired in too — consulted on every dispatch of a {@code @Cacheable} query, hit or miss, before
   * the cache is touched. Absent, no authorization check runs here and behavior is unchanged; see
   * {@link org.streamrune.core.QueryAuthorizer}'s javadoc for why one is usually needed once
   * caching is enabled for a query whose results depend on the caller.
   *
   * <p>{@link Instance#isResolvable()} is {@code false} both when no bean exists AND when the bean
   * is AMBIGUOUS (two unqualified {@code QueryAuthorizer} beans) — the ambiguous case must fail
   * loud, not silently build the caching bus with no authorizer and serve every cache hit
   * unauthorized. Fails closed the same way this module's {@code SagaStateCryptoValidator} already
   * does for an ambiguous {@code CryptoEngine}.
   */
  @Produces
  @Singleton
  @DefaultBean
  @Typed(CachingQueryBus.class)
  @IfBuildProperty(name = "streamrune.query-cache.enabled", stringValue = "true")
  public CachingQueryBus cachingQueryBus(
      SimpleQueryBus delegate,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.core.QueryAuthorizer> authorizer) {
    var builder = CachingQueryBus.builder().delegate(delegate);
    builder.metrics(resolveMetricsOrDefault(metrics));
    if (authorizer.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple QueryAuthorizer beans exist but none is resolvable as default — StreamRune"
              + " will not build a caching query bus with an undecidable authorizer; mark one"
              + " @Default/@Priority or remove the extra bean");
    }
    if (authorizer.isResolvable()) {
      builder.authorizer(authorizer.get());
    }
    return builder.build();
  }

  /**
   * Produces the {@link CacheInvalidator} backed by the auto-configured {@link CachingQueryBus}.
   * Exists only when {@code streamrune.query-cache.enabled=true}, alongside the bus itself.
   */
  @Produces
  @Singleton
  @DefaultBean
  @IfBuildProperty(name = "streamrune.query-cache.enabled", stringValue = "true")
  public CacheInvalidator cacheInvalidator(CachingQueryBus bus) {
    return bus.cacheInvalidator();
  }

  /**
   * Produces the primary {@link QueryBus} composing the full chain: SimpleQueryBus →
   * CachingQueryBus (if {@code streamrune.query-cache.enabled=true}) → AuditingQueryBus (if an
   * AuditStore bean is available), inside an {@link UnboundRequestContextQueryBus} over {@link
   * StreamRuneRequestContextHolder#ofCurrentRequest()}. Always yields a working bus — when caching
   * is disabled the {@link CachingQueryBus} bean simply does not exist and the simple bus is used
   * directly.
   *
   * <p><b>The request's caller in every resource method.</b> A JAX-RS filter cannot wrap the
   * resource method, so {@link StreamRuneRequestFilter} stores the request's context in the
   * request-scoped {@link StreamRuneRequestContextHolder} and leaves {@link
   * org.streamrune.core.StreamRuneContext#CURRENT} unbound while the method runs. A query
   * dispatched there runs under the holder's context, so the audit row and the handler see the
   * request's caller; a query with no request current is dispatched without a context.
   */
  @Produces
  @Singleton
  @DefaultBean
  public QueryBus queryBus(
      SimpleQueryBus simpleBus,
      Instance<CachingQueryBus> cachingBus,
      Instance<AuditStore> auditStore) {
    QueryBus bus = cachingBus.isResolvable() ? cachingBus.get() : simpleBus;
    if (!auditStore.isUnsatisfied()) {
      AuditStore store = auditStore.get();
      if (store != null) {
        bus = new AuditingQueryBus(bus, store);
      }
    }
    return new UnboundRequestContextQueryBus(bus, StreamRuneRequestContextHolder::ofCurrentRequest);
  }

  /**
   * Creates a default VirtualThreadCommandBus.
   *
   * <p><b>Interceptor chain:</b> the union of the application's {@link CommandInterceptor} beans
   * and the framework's, collected through {@link CommandInterceptorBeans} rather than an {@code
   * Instance} — Arc's resolution of an {@code Instance} (or {@code @All List}) drops every
   * {@code @DefaultBean} framework interceptor as soon as the application declares one {@code
   * CommandInterceptor} bean of its own. An application bean of a framework interceptor's type
   * replaces that framework interceptor, as on Spring and Micronaut; an optional framework producer
   * that declines (dependent-scoped, returns {@code null}) contributes nothing. The chain is then
   * sorted into the canonical order by {@link org.streamrune.runtime.CommandInterceptorOrdering}.
   * The {@code interceptorBeans} injection point is never read: it is what keeps every interceptor
   * bean in the application through Quarkus's build-time removal of unused beans, which cannot see
   * a {@code BeanManager} lookup (see {@link CommandInterceptorBeans}).
   *
   * <p><b>Fail-fast on {@code @Encrypted} command fields:</b> command types are validated at wiring
   * time via {@code CryptoConfigValidator}; if a registered command carries an {@code @Encrypted}
   * field but no {@link CryptoEngine} bean is configured, startup fails instead of silently
   * dead-lettering that PII as plaintext.
   *
   * <p><b>Dead-letter queue &amp; metrics (parity with Spring):</b> when an optional {@link
   * org.streamrune.core.DeadLetterQueue} bean is present it is wired into the bus so commands that
   * exhaust retries on an infrastructure failure are published to it (mirroring {@code
   * StreamRuneAutoConfiguration}); the DLQ publish mapper is ALWAYS the crypto-aware default (via
   * {@link org.streamrune.runtime.DeadLetterRetryRunner#createObjectMapper(CryptoEngine)}), never
   * an application ObjectMapper bean, so {@code @Encrypted} command fields are dead-lettered as
   * ciphertext. When an optional {@link org.streamrune.core.StreamRuneMetrics} bean is present it
   * is wired in too, so command-bus meters
   * (dispatched/succeeded/failed/duration/retries/DLQ-published) are recorded instead of NOOPed.
   * Both default to today's behavior (no DLQ / NOOP metrics) when absent.
   *
   * <p><b>The request's caller in every resource method.</b> The bus is built with {@link
   * StreamRuneRequestContextHolder#ofCurrentRequest()} as its {@link
   * VirtualThreadCommandBus.Builder#unboundRequestContext unbound-request-context source}. A JAX-RS
   * filter cannot wrap the resource method, so {@link StreamRuneRequestFilter} stores the request's
   * context in the request-scoped {@link StreamRuneRequestContextHolder} and {@link
   * org.streamrune.core.StreamRuneContext#CURRENT} is unbound while the method runs: a command the
   * method dispatches is authorized, audited and recorded as the request's caller, and so are a
   * command sent through {@code executeAsync} and the commands of a saga replay run from it. A
   * dispatch with {@code CURRENT} bound (a {@link StreamRuneRequestFilter#withContext} block, a
   * saga, a replay) keeps its binding, and a dispatch with no request current runs without a
   * context.
   *
   * <p><b>Snapshot policy:</b> the default is {@code
   * SnapshotPolicy.everyNEvents(streamrune.snapshot-every-n-events)} at snapshot schema version 1.
   * An application {@link SnapshotPolicy} bean replaces it wholesale — that is the ONLY way to bump
   * the snapshot schema version or to switch snapshots off, and until this hook existed both were
   * unreachable through these producers: the version was pinned to 1, which also made every
   * registered {@code SnapshotMigration} bean dead code (the store's migrate/discard branch is only
   * entered on a version mismatch). It is deliberately a bean rather than a property — the version
   * must track the application's state-class schema, so it belongs next to that code and is
   * released with it.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist (none resolvable) or if a
   *     registered command type has an {@code @Encrypted} field with no CryptoEngine configured
   */
  @Produces
  @Singleton
  @DefaultBean
  @SuppressWarnings("unchecked")
  public VirtualThreadCommandBus virtualThreadCommandBus(
      EventStore eventStore,
      org.streamrune.core.AggregateLocker locker,
      StreamRuneQuarkusProperties properties,
      Instance<SnapshotPolicy> snapshotPolicyInstance,
      BeanManager beanManager,
      // Never read — keeps every CommandInterceptor bean through unused-bean removal.
      @Any Instance<CommandInterceptor> interceptorBeans,
      @All List<DeciderRegistration<?, ?, ?>> registrations,
      Instance<CryptoEngine> cryptoEngineInstance,
      Instance<CommandInbox> commandInboxInstance,
      Instance<org.streamrune.core.DeadLetterQueue> deadLetterQueueInstance,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance,
      StreamRuneTracing tracing) {

    // Fail fast when a registered command type carries an @Encrypted field but no
    // CryptoEngine is configured — otherwise that PII would be dead-lettered as PLAINTEXT.
    if (cryptoEngineInstance.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Priority — StreamRune cannot "
              + "decide which engine to validate command types against, and skipping encryption "
              + "would persist @Encrypted fields as PLAINTEXT. Ensure exactly one CryptoEngine "
              + "bean is resolvable (remove duplicates or qualify one with @Default).");
    }
    CryptoEngine cryptoEngine =
        cryptoEngineInstance.isUnsatisfied() ? null : cryptoEngineInstance.get();
    java.util.List<Class<?>> commandTypes =
        registrations.stream()
            .map(org.streamrune.runtime.DeciderRegistration::commandType)
            .collect(java.util.stream.Collectors.toList());
    org.streamrune.crypto.CryptoConfigValidator.validate(commandTypes, cryptoEngine);

    var retryPolicy =
        new RetryPolicy(
            properties.retryMaxAttempts(),
            properties.retryInitialDelay(),
            properties.retryBackoffMultiplier());

    // The container yields the interceptor beans in an unspecified order. Sort them into the
    // canonical, security-relevant chain (audit outside authorization so denials are audited,
    // validation after authorization, breaker innermost) so Quarkus matches Spring's @Order chain.
    // Collected through CommandInterceptorBeans, never by iterating an Instance: see that class for
    // why an Instance would hand this producer only the application's interceptors.
    List<CommandInterceptor> interceptors =
        org.streamrune.runtime.CommandInterceptorOrdering.sorted(
            CommandInterceptorBeans.collect(beanManager));

    // The tracing meets the metrics bean here: its guard counts its failures there, and it is
    // this guard the locker and the bus are handed.
    StreamRuneTracing boundTracing =
        org.streamrune.runtime.GuardedTracing.of(tracing, resolveMetricsOrDefault(metricsInstance));
    var builder =
        VirtualThreadCommandBus.builder()
            .allowGraalJit(properties.jvm().allowGraalJit())
            .eventStore(eventStore)
            .locker(traced(locker, boundTracing))
            // Adds the command interceptor in its slot, unless the chain holds the application's.
            .tracing(boundTracing)
            .retryPolicy(retryPolicy)
            .snapshotPolicy(resolveSnapshotPolicy(snapshotPolicyInstance, properties))
            .lockTimeout(properties.lockTimeout())
            .stripeCount(properties.stripeCount())
            .maxInFlightAsyncCommands(properties.maxInFlightAsyncCommands())
            .interceptors(interceptors)
            .unboundRequestContext(StreamRuneRequestContextHolder::ofCurrentRequest);

    // Wire the optional StreamRuneMetrics bean so command-bus meters are recorded (NOOP if absent),
    // mirroring Spring (StreamRuneAutoConfiguration) and the query bus on this framework.
    org.streamrune.core.StreamRuneMetrics metrics = resolveMetricsOrDefault(metricsInstance);
    builder.metrics(metrics);

    // Wire the optional DeadLetterQueue bean so commands that exhaust retries are dead-lettered,
    // mirroring Spring. The DLQ publish mapper is ALWAYS the crypto-aware default — never an
    // application ObjectMapper bean — so @Encrypted command fields are persisted as ciphertext.
    if (deadLetterQueueInstance.isResolvable()) {
      org.streamrune.core.DeadLetterQueue deadLetterQueue = deadLetterQueueInstance.get();
      if (deadLetterQueue != null) {
        // The retry runner's policy and inbox window tell a keyed command that fails again under
        // its key whether the entry stored for it is still replayed on the schedule.
        builder
            .deadLetterQueue(deadLetterQueue)
            .objectMapper(
                org.streamrune.runtime.DeadLetterRetryRunner.createObjectMapper(
                    cryptoEngine, metrics))
            .deadLetterRetryPolicy(deadLetterRetryPolicy(properties))
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge());
      }
    }

    if (commandInboxInstance.isResolvable()) {
      CommandInbox inbox = commandInboxInstance.get();
      if (inbox != null) {
        builder.commandInbox(inbox);
      }
    }

    for (DeciderRegistration<?, ?, ?> reg : registrations) {
      register(builder, reg);
    }

    return builder.build();
  }

  /** Captures the registration's type parameters once so the builder call type-checks. */
  private static <
          C extends org.streamrune.core.Command,
          S extends org.streamrune.core.AggregateState,
          E extends org.streamrune.core.DomainEvent>
      void register(VirtualThreadCommandBus.Builder builder, DeciderRegistration<C, S, E> reg) {
    builder.register(reg.aggregateType(), reg.commandType(), reg.idExtractor(), reg.decider());
  }

  /**
   * Produces a {@link DeadLetterRetryRunner} when dead letter retry is enabled and both {@link
   * org.streamrune.core.DeadLetterQueue} and {@link org.streamrune.core.CommandBus} are available.
   * Dependent-scoped optional bean: returns {@code null} when disabled or dependencies are absent.
   * Started and stopped by {@link StreamRuneLifecycle}. {@code streamrune.dead-letter.enabled}
   * governs only this automatic retry: the command bus keeps dead-lettering into the queue and
   * {@link #deadLetterRetentionSweeper} keeps pruning it.
   *
   * <p>Dead-letter payloads are ALWAYS deserialized with a DEDICATED mapper built via {@link
   * org.streamrune.runtime.DeadLetterRetryRunner#createObjectMapper(CryptoEngine)} — never the
   * application's shared {@code com.fasterxml.jackson.databind.ObjectMapper} bean — wired with the
   * {@link CryptoEngine} bean (if present) the same way {@link #postgresEventStoreFactory} and
   * {@link #sagaStore} wire their mappers, so {@code @Encrypted} command fields round-trip as
   * ciphertext instead of plaintext PII regardless of any application ObjectMapper bean.
   *
   * <p><b>Command-type registry:</b> the runner resolves a stored DLQ entry's command class by its
   * fully-qualified class name (see {@code VirtualThreadCommandBus.publishToDeadLetterQueue}), and
   * the registry is keyed by that name. It is seeded here from the same {@link DeciderRegistration}
   * beans {@link #virtualThreadCommandBus} already consumes, so every command type registered with
   * the bus is also resolvable by the runner — without this, the registry would be empty and every
   * retry would be permanently unresolvable. {@link
   * org.streamrune.runtime.DeadLetterRetryRunner.Builder#registerCommand(Class)} is available for
   * command types published to the DLQ without a decider registration bean. A registration whose
   * command type is a sealed root (the usual {@code OrderCommand.class}) makes every permitted
   * command resolvable, since {@code registerCommand} expands a sealed hierarchy and the bus
   * persists the concrete class; one registered by a non-sealed supertype cannot be expanded, so
   * its concrete commands replay only from a runner the application builds and registers them on.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} DLQ payloads as PLAINTEXT
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
  public DeadLetterRetryRunner deadLetterRetryRunner(
      Instance<org.streamrune.core.DeadLetterQueue> dlq,
      Instance<org.streamrune.core.CommandBus> commandBus,
      Instance<CryptoEngine> cryptoEngineInstance,
      Instance<org.streamrune.core.subscription.SubscriptionLeadership> leadershipInstance,
      Instance<org.streamrune.core.StreamRuneMetrics> metricsInstance,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance,
      @All List<DeciderRegistration<?, ?, ?>> registrations,
      StreamRuneQuarkusProperties properties) {
    if (!properties.deadLetter().enabled() || !dlq.isResolvable() || !commandBus.isResolvable()) {
      return null;
    }
    if (cryptoEngineInstance.isAmbiguous()) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Priority — StreamRune cannot "
              + "decide which engine to wire into the dead-letter queue, and skipping encryption "
              + "would persist @Encrypted fields as PLAINTEXT. Ensure exactly one CryptoEngine "
              + "bean is resolvable (remove duplicates or qualify one with @Default).");
    }
    // The DLQ replay mapper is ALWAYS the crypto-aware default — never the application's own
    // ObjectMapper bean — so @Encrypted command fields round-trip as ciphertext, mirroring the
    // event store, saga store, and Micronaut.
    CryptoEngine cryptoEngine =
        cryptoEngineInstance.isUnsatisfied() ? null : cryptoEngineInstance.get();
    // Resolve the metrics bean once: it feeds BOTH the DLQ decrypt mapper's CryptoShreddingModule
    // (subject_redacted) and retry-exhaustion counters below; NOOP when absent.
    org.streamrune.core.StreamRuneMetrics metrics = resolveMetricsOrDefault(metricsInstance);
    com.fasterxml.jackson.databind.ObjectMapper resolvedMapper =
        org.streamrune.runtime.DeadLetterRetryRunner.createObjectMapper(cryptoEngine, metrics);
    var builder =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq.get())
            .commandBus(commandBus.get())
            .objectMapper(resolvedMapper)
            .policy(deadLetterRetryPolicy(properties))
            // The window the inbox retention sweeper prunes with: a keyed entry older than it is
            // refused (STALE_REPLAY_BLOCKED) instead of replayed past its dedup row.
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .pollInterval(properties.deadLetter().retryInterval());

    for (DeciderRegistration<?, ?, ?> reg : registrations) {
      // registerCommand keys by fully-qualified class name, so replay resolves by FQN and
      // two commands sharing a simple name across bounded contexts can never collide in the map.
      builder.registerCommand(reg.commandType());
    }

    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on) into the DLQ retry runner so only the leader
    // replica polls/retries; NOOP (always leader) when absent. Without it, two replicas' staggered
    // polls both re-execute the same DLQ command (duplicate domain events) with no CommandInbox to
    // de-dup.
    // Two leadership beans with none the default fail here, as the projection runners and the
    // saga sweepers do, instead of leaving this runner on NOOP.
    builder.leadership(LeadershipInstances.resolve(leadershipInstance));

    // Wire the optional StreamRuneMetrics bean (resolved above) so retry-exhaustion emits
    // streamrune.dlq.exhausted + a terminal ERROR log under the default policy; NOOP when absent.
    builder.metrics(metrics);

    DeadLetterRetryRunner runner = builder.build();
    // Register for relay liveness health so /q/health/ready degrades if the thread dies.
    if (relayHealthInstance.isResolvable()) {
      relayHealthInstance.get().registerDlqRetryRunner(runner);
    }
    return runner;
  }

  /**
   * Aggregates outbox-relay and DLQ-retry-runner liveness for the readiness health check. The
   * runners register themselves into it when produced, so {@code /q/health/ready} reports DOWN when
   * a started relay's poll thread has died instead of silently reporting UP.
   */
  @Produces
  @jakarta.inject.Singleton
  @DefaultBean
  public org.streamrune.runtime.BackgroundRelayHealthContributor
      backgroundRelayHealthContributor() {
    return new org.streamrune.runtime.BackgroundRelayHealthContributor();
  }

  /**
   * Produces an {@link OutboxPoller} when outbox is enabled and both {@link
   * org.streamrune.core.outbox.OutboxStore} and {@link org.streamrune.core.outbox.OutboxPublisher}
   * are available. Dependent-scoped optional bean: returns {@code null} (opt-in) when outbox is
   * disabled or dependencies are absent. Started and stopped by {@link StreamRuneLifecycle}.
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_OUTBOX_POLLER)
  public OutboxPoller outboxPoller(
      Instance<org.streamrune.core.outbox.OutboxStore> store,
      Instance<org.streamrune.core.outbox.OutboxPublisher> publisher,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance,
      StreamRuneTracing tracing) {
    // Diagnostics only — the null-return control flow is unchanged. A dead poller that never starts
    // is otherwise invisible to operators: StreamRuneLifecycle resolves this to null and logs
    // nothing, so an operator who enabled the outbox but forgot the store/publisher bean has no
    // signal.
    if (!properties.outbox().enabled()) {
      LOG.info("OutboxPoller not started: streamrune.outbox.enabled=false");
      return null;
    }
    if (!store.isResolvable() || !publisher.isResolvable()) {
      LOG.warn(
          "OutboxPoller enabled (streamrune.outbox.enabled=true) but {} bean missing — poller will"
              + " not run",
          store.isResolvable() ? "OutboxPublisher" : "OutboxStore");
      return null;
    }
    // Wire the metrics bean so streamrune.outbox.delivery_failed fires on a terminal FAILED entry
    // (the documented alert metric) and streamrune.outbox.pending tracks the backlog; NOOP when no
    // metrics bean exists (behavior unchanged), mirroring Spring/Micronaut.
    OutboxPoller poller =
        OutboxPoller.builder()
            .outboxStore(store.get())
            .publisher(publisher.get())
            .batchSize(properties.outbox().batchSize())
            .pollInterval(properties.outbox().flushInterval())
            // Expose the retry ladder; transport failures are non-counting regardless.
            .retryPolicy(
                new org.streamrune.core.RetryPolicy(
                    properties.outbox().retryMaxAttempts(),
                    properties.outbox().retryInitialDelay(),
                    properties.outbox().retryBackoffMultiplier()))
            .metrics(resolveMetricsOrDefault(metrics))
            .tracing(tracing)
            .build();
    // Register for relay liveness health so /q/health/ready degrades if the thread dies.
    if (relayHealthInstance.isResolvable()) {
      relayHealthInstance.get().registerOutboxPoller(poller);
    }
    return poller;
  }

  /**
   * Produces an {@link OutboxRetentionSweeper} with two independent windows: it prunes {@code
   * DELIVERED} outbox entries older than {@code streamrune.outbox.retention-max-age} (default 7
   * days) and operator-skipped {@code SKIPPED} entries older than {@code
   * streamrune.outbox.skipped-retention-max-age} (default 30 days — the audit of the skip
   * decision). Zero or negative disables either window. {@code FAILED} entries are never pruned: an
   * operator resolves them through {@link org.streamrune.runtime.OutboxFailedReplayer}.
   * Dependent-scoped optional bean: returns {@code null} when outbox is disabled or no {@link
   * org.streamrune.core.outbox.OutboxStore} bean exists. The framework produces none, so the store
   * is always the application's, typically built with {@link BackendStores#outboxStore}. Started
   * and stopped by {@link StreamRuneLifecycle}. The {@link org.streamrune.core.StreamRuneMetrics}
   * bean is wired in when present so {@code streamrune.outbox.swept_rows} and {@code
   * streamrune.outbox.skipped_swept} are recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * <p>Intentionally dependent-scoped (no {@code @Singleton}): this producer may return {@code
   * null} when the outbox is disabled or no {@link org.streamrune.core.outbox.OutboxStore} is
   * present; {@code @Singleton} + null-return would NPE at injection. {@link StreamRuneLifecycle}
   * owns the single instance via {@link Instance#get()}.
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_OUTBOX_RETENTION_SWEEPER)
  public OutboxRetentionSweeper outboxRetentionSweeper(
      Instance<org.streamrune.core.outbox.OutboxStore> store,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance) {
    if (!properties.outbox().enabled() || store.isUnsatisfied()) {
      return null;
    }
    return registerRetentionSweeperHealth(
        relayHealthInstance,
        new OutboxRetentionSweeper(
            store.get(),
            properties.outbox().retentionMaxAge(),
            properties.outbox().skippedRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            resolveMetricsOrDefault(metrics)));
  }

  /**
   * Produces an {@link org.streamrune.runtime.OutboxFailedReplayer} beside the outbox poller for
   * operator-driven recovery of terminal {@code FAILED} outbox entries: {@code replay(id)} resets
   * one entry to {@code PENDING} in its original position (same {@code seq}), {@code
   * replayFailed(int)} does so oldest-first in bulk, {@code skip(id, by, reason)} moves one entry
   * to the audited {@code SKIPPED} state (releasing its aggregate on a strict channel), and {@code
   * skipFailed(max, by, reason)} is the bulk mirror of {@code replayFailed}. Dependent-scoped
   * optional bean: returns {@code null} when the outbox is disabled or no {@link
   * org.streamrune.core.outbox.OutboxStore} bean exists. It is <em>not</em> a runner (no
   * lifecycle); the application invokes it from an admin endpoint. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.outbox.replayed} and {@code streamrune.outbox.skipped} are recorded; falls back to
   * {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Produces
  @DefaultBean
  public org.streamrune.runtime.OutboxFailedReplayer outboxFailedReplayer(
      Instance<org.streamrune.core.outbox.OutboxStore> store,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics) {
    if (!properties.outbox().enabled() || store.isUnsatisfied()) {
      return null;
    }
    return new org.streamrune.runtime.OutboxFailedReplayer(
        store.get(), resolveMetricsOrDefault(metrics));
  }

  /**
   * Produces the single shared {@link org.streamrune.core.subscription.SubscriptionLeadership}, the
   * backend's {@link BackendStores#leadership(java.time.Duration)} — on PostgreSQL a {@link
   * org.streamrune.postgres.LeaseBasedLeadership}, a DB-clocked persisted lease with a monotonic
   * fencing {@code epoch} — so that, with more than one replica of the same projection/subscription
   * running, at most one replica is the active consumer and the others sit in standby. Leadership
   * is a single atomic upsert over {@code subscription_leases} decided by the database clock (no
   * session state, no dedicated connection held across calls), so it is correct under any
   * connection pooling; the leader's monotonic epoch fences a superseded leader out at the
   * projection commit. The lease TTL is {@code
   * streamrune.subscription.single-active-consumer.lease-ttl} (default 15s) — the failover bound;
   * the renew heartbeat runs at {@code ttl/3}.
   *
   * <p><strong>{@code @Singleton}, not {@code @Dependent}.</strong> Every injection point ({@link
   * ProjectionProducer#multiProjectionRunner}, {@link
   * ProjectionProducer#scheduledProjectionRunner}, and {@link StreamRuneLifecycle#onStart}) must
   * resolve the <em>same</em> instance: a {@code LeaseBasedLeadership} runs a single renew
   * heartbeat thread, so a per-injection-point {@code @Dependent} product would spin up N
   * heartbeats and the lifecycle would close only one of them. To keep {@code @Singleton} legal
   * (CDI forbids a null singleton product) the disabled/no-{@link DataSource} path returns the
   * shared non-null {@link org.streamrune.core.subscription.SubscriptionLeadership#NOOP} (always
   * leader — single-instance behavior unchanged) instead of {@code null}. {@link
   * StreamRuneLifecycle} owns this single instance and calls {@code close()} on shutdown, after the
   * runners (which stops the heartbeat and resigns held leases); {@code NOOP.close()} is a no-op.
   * The bundle memoizes the leadership and closes it once more, idempotently, when it is closed
   * itself ({@link #closeBackendStores}); no consumer closes it on its own.
   */
  @Produces
  @Singleton
  @DefaultBean
  public org.streamrune.core.subscription.SubscriptionLeadership subscriptionLeadership(
      Instance<BackendStores> stores, StreamRuneQuarkusProperties properties) {
    boolean enabled =
        org.streamrune.integration.SingleActiveConsumerSetting.enabled(
            properties.subscription().singleActiveConsumer().enabled().orElse(null),
            BackendSelection.parse(properties.backend()));
    if (!enabled || stores.isUnsatisfied()) {
      return org.streamrune.core.subscription.SubscriptionLeadership.NOOP;
    }
    return stores.get().leadership(properties.subscription().singleActiveConsumer().leaseTtl());
  }

  /**
   * Produces an {@link InboxRetentionSweeper} that prunes command-inbox rows older than {@code
   * streamrune.inbox.retention-max-age} (default 7 days). It sweeps an application's {@link
   * org.streamrune.postgres.PostgresCommandInbox} bean when there is one, and otherwise the
   * framework's own inbox, {@link BackendStores#commandInbox()} — never an application inbox of
   * another type: the framework prunes only what it built or what is declared as its Postgres
   * store. Dependent-scoped optional bean: returns {@code null} without either. The retention
   * window exceeds typical broker redelivery and subscription replay horizons so a pruned key
   * cannot re-admit a duplicate. Started and stopped by {@link StreamRuneLifecycle}. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.inbox.swept_rows} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_INBOX_RETENTION_SWEEPER)
  public InboxRetentionSweeper inboxRetentionSweeper(
      Instance<org.streamrune.postgres.PostgresCommandInbox> applicationInboxInstance,
      Instance<BackendStores> storesInstance,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance) {
    // No framework bean carries the Postgres type, so this point sees only an application's own
    // Postgres inbox; without one, the framework's own inbox comes from the bundle.
    CommandInbox inbox =
        applicationInboxInstance.isUnsatisfied() ? null : applicationInboxInstance.get();
    if (inbox == null && !storesInstance.isUnsatisfied()) {
      inbox = storesInstance.get().commandInbox();
    }
    if (inbox == null) {
      return null;
    }
    return registerRetentionSweeperHealth(
        relayHealthInstance,
        new InboxRetentionSweeper(
            inbox,
            properties.inbox().retentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            resolveMetricsOrDefault(metrics)));
  }

  /**
   * Produces a {@link org.streamrune.runtime.SagaDeadLetterRetentionSweeper} that prunes
   * quarantined saga dead-letter entries older than {@code
   * streamrune.saga.dead-letter-retention-max-age} (default 7 days, and must not exceed {@code
   * streamrune.inbox.retention-max-age} — see {@link
   * org.streamrune.integration.SagaRetentionValidator}). It sweeps an application's {@link
   * org.streamrune.postgres.PostgresSagaDeadLetterStore} bean when there is one, and otherwise the
   * framework's own store, {@link BackendStores#sagaDeadLetterStore()} — never an application store
   * of another type. Dependent-scoped optional bean: returns {@code null} when sagas are disabled
   * or there is neither. Started and stopped by {@link StreamRuneLifecycle}. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.saga.dead_letters_swept} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * <p><strong>Why not the {@link SagaDeadLetterStore} bean:</strong> an application's own store of
   * another type wins that bean, and the framework must not prune a store it did not build. So the
   * framework's store is taken from the bundle, and an application that supplied its own {@link
   * SagaDeadLetterStore} owns that store's retention. Without a bundle (no {@code DataSource}) this
   * logs a WARN before returning {@code null}, so a missing sweeper is never silent.
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER)
  public org.streamrune.runtime.SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper(
      Instance<org.streamrune.postgres.PostgresSagaDeadLetterStore> applicationStoreInstance,
      Instance<BackendStores> storesInstance,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance) {
    if (!properties.saga().enabled()) {
      return null;
    }
    // No framework bean carries the Postgres type, so this point sees only an application's own
    // Postgres store; without one, the framework's own store comes from the bundle.
    SagaDeadLetterStore store =
        applicationStoreInstance.isUnsatisfied() ? null : applicationStoreInstance.get();
    if (store == null && !storesInstance.isUnsatisfied()) {
      store = storesInstance.get().sagaDeadLetterStore();
    }
    if (store == null) {
      LOG.warn(
          "Sagas are enabled but neither an application PostgresSagaDeadLetterStore bean nor the"
              + " framework's own saga dead-letter store (no DataSource) is resolvable, so no"
              + " SagaDeadLetterRetentionSweeper was created: quarantined saga dead letters will"
              + " NEVER be pruned (streamrune.saga.dead-letter-retention-max-age has no effect).");
      return null;
    }
    return registerRetentionSweeperHealth(
        relayHealthInstance,
        new org.streamrune.runtime.SagaDeadLetterRetentionSweeper(
            store,
            properties.saga().deadLetterRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            resolveMetricsOrDefault(metrics)));
  }

  /**
   * Produces a {@link org.streamrune.runtime.DeadLetterRetentionSweeper} that prunes command
   * dead-letter-queue entries older than {@code streamrune.dead-letter.retention-max-age} (default
   * 7 days). A DLQ entry holds the full command payload (encrypted where {@code @Encrypted} fields
   * apply per) — this bounds how long that payload remains queryable, a GDPR storage-limitation
   * concern. Dependent-scoped optional bean: returns {@code null} when no {@link
   * org.streamrune.core.DeadLetterQueue} bean exists, mirroring how {@link #deadLetterRetryRunner}
   * is gated on the generic interface rather than a Postgres-specific type — an implementation that
   * does not support retention pruning throws {@code UnsupportedOperationException} from {@code
   * deleteOlderThan}, so the sweeper fails loudly (not silently) on its first cycle.
   *
   * <p>Produced for every {@link org.streamrune.core.DeadLetterQueue} bean — the framework
   * registers none itself, so that is always one the application supplied — because the command bus
   * dead-letters into whichever queue bean exists. Unlike {@link #deadLetterRetryRunner} it is
   * therefore <em>not</em> gated on {@code streamrune.dead-letter.enabled}, which switches only the
   * automatic retry: an operator who turns replays off and relies on the admin retry would
   * otherwise keep every exhausted command's payload for ever. A zero or negative {@code
   * retention-max-age} is the switch that turns pruning off (the sweeper then logs that at start).
   *
   * <p>Started and stopped by {@link StreamRuneLifecycle}. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.dlq.swept_rows} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Produces
  @DefaultBean
  @jakarta.inject.Named(FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER)
  public org.streamrune.runtime.DeadLetterRetentionSweeper deadLetterRetentionSweeper(
      Instance<org.streamrune.core.DeadLetterQueue> dlqInstance,
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.StreamRuneMetrics> metrics,
      Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance) {
    if (dlqInstance.isUnsatisfied()) {
      return null;
    }
    org.streamrune.core.DeadLetterQueue dlq = dlqInstance.get();
    if (dlq == null) {
      return null;
    }
    return registerRetentionSweeperHealth(
        relayHealthInstance,
        new org.streamrune.runtime.DeadLetterRetentionSweeper(
            dlq,
            properties.deadLetter().retentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            resolveMetricsOrDefault(metrics)));
  }

  /**
   * Registers a freshly produced retention sweeper with the relay-health contributor (when one is
   * resolvable) so {@code /q/health/ready} reports DOWN if its sweep thread dies, exactly like the
   * outbox poller and the DLQ retry runner. Returns the sweeper for chaining. Each of the four
   * sweeper producers is {@code @Dependent} and resolved once by {@link StreamRuneLifecycle}, so
   * the registered instance is the one that gets started.
   */
  private static <S extends org.streamrune.runtime.RetentionSweeper>
      S registerRetentionSweeperHealth(
          Instance<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthInstance,
          S sweeper) {
    if (relayHealthInstance.isResolvable()) {
      relayHealthInstance.get().registerRetentionSweeper(sweeper);
    }
    return sweeper;
  }

  /**
   * Resolves the optional {@link org.streamrune.core.StreamRuneMetrics} bean, falling back to
   * {@link org.streamrune.core.StreamRuneMetrics#NOOP}. Mirrors {@link #resolveMetricsOrNull} but
   * never returns {@code null} — used by collaborators (the retention sweepers) whose constructor
   * requires a non-null metrics instance.
   */
  private static org.streamrune.core.StreamRuneMetrics resolveMetricsOrDefault(
      Instance<org.streamrune.core.StreamRuneMetrics> metrics) {
    org.streamrune.core.StreamRuneMetrics resolved = resolveMetricsOrNull(metrics);
    return resolved != null ? resolved : org.streamrune.core.StreamRuneMetrics.NOOP;
  }
}

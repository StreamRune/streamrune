package org.streamrune.spring;

import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.streamrune.core.AsyncCommandBus;
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
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SingleActiveConsumerSetting;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.AuditingQueryBus;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.BeanValidationInterceptor;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.CommandInterceptorOrdering;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.OpenTelemetryTracing;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.TracingCommandInterceptor;
import org.streamrune.runtime.TracingSettings;
import org.streamrune.runtime.UnboundRequestContextQueryBus;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * Spring Boot auto-configuration for StreamRune framework. Registers default beans when not already
 * provided by the application.
 *
 * <p><strong>Ordering:</strong> this class is explicitly ordered AFTER all four crypto engine
 * configurations. Absent that declaration, auto-configurations are processed in alphabetical order,
 * which puts {@code VaultCryptoEngineConfiguration} (V &gt; S) <em>after</em> this class: {@link
 * #forgetSubjectService}'s {@code @ConditionalOnBean(CryptoEngine.class)} was then evaluated before
 * the Vault engine's bean definition existed, so a correctly configured Vault deployment silently
 * got no GDPR Article-17 erasure service (and an app injecting {@code ForgetSubjectService} failed
 * startup with a confusing {@code NoSuchBeanDefinitionException}). The filesystem, Postgres, and
 * AWS-KMS backends happened to sort before "StreamRune" and worked. Any new crypto backend must be
 * added to this list — {@code SpringGdprAutoConfigTest} enforces that against the
 * auto-configuration imports file.
 */
@AutoConfiguration(
    after = {
      AwsKmsCryptoEngineConfiguration.class,
      FileSystemCryptoEngineConfiguration.class,
      PostgresCryptoEngineConfiguration.class,
      SqliteCryptoEngineConfiguration.class,
      VaultCryptoEngineConfiguration.class
    })
@EnableConfigurationProperties(StreamRuneProperties.class)
@ConditionalOnClass(VirtualThreadCommandBus.class)
@ImportRuntimeHints(StreamRuneRuntimeHints.class)
@org.springframework.context.annotation.Import({
  BackendConfiguration.class,
  ReadModelStoresConfiguration.class
})
public class StreamRuneAutoConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneAutoConfiguration.class);

  // Interceptor chain order: the interceptor beans below carry
  // @Order(CommandInterceptorOrdering.*),
  // the single source of truth shared with the Quarkus and Micronaut integrations (which sort their
  // container-assembled chain by it), so all three frameworks run the identical,
  // security-relevant order. The command bus calls before() in ascending order and
  // after()/onError() in reverse (see CommandInterceptor's lifecycle contract). Values leave gaps
  // so user interceptors can slot between framework ones with @Order; user interceptors without an
  // explicit order default to Ordered.LOWEST_PRECEDENCE and run innermost.

  /**
   * Servlet filter-chain order for {@link ScopedValueFilter}: <em>after</em> Spring Security's
   * filter chain (default order {@code -100}) so the {@code SecurityContextHolder} is already
   * populated when the filter derives the authorization identity ({@code RequestContext.userId})
   * from the authenticated principal — binding it before authentication would leave the identity
   * unresolved and force a fall-back to the spoofable {@code X-User-Id} header. Still runs after
   * OpenTelemetry's servlet instrumentation filter ({@code Ordered.HIGHEST_PRECEDENCE}) so OTel
   * baggage is populated, and before the {@code DispatcherServlet} so the context is bound for
   * controllers and the command bus they invoke.
   */
  public static final int SCOPED_VALUE_FILTER_ORDER = -90;

  /**
   * The servlet-only request-context plumbing, isolated in a nested {@code @Configuration} gated by
   * {@code @ConditionalOnClass} on {@code jakarta.servlet.Filter} — evaluated by class NAME through
   * configuration metadata, so Spring never introspects a servlet-typed {@code @Bean} signature on
   * the enclosing class when {@code jakarta.servlet-api} (a {@code compileOnly}, classpath-optional
   * dependency) is absent.
   *
   * <p><strong>Why nesting, when {@code @ConditionalOnWebApplication(SERVLET)} is already
   * present.</strong> A condition only suppresses bean REGISTRATION. The failure happens earlier
   * and unconditionally: {@code Class#getDeclaredMethods} on the configuration class resolves every
   * declared method's return and parameter types, so creating the {@code Method} object for {@code
   * scopedValueFilter()} linked {@link ScopedValueFilter}, whose direct superinterface {@code
   * jakarta.servlet.Filter} could not be resolved — {@code NoClassDefFoundError} → {@code
   * IllegalStateException: Failed to introspect Class [StreamRuneAutoConfiguration]} → context
   * refresh failed before any condition ran. A WebFlux app (spring-boot-starter-webflux ships no
   * servlet API) or a headless HA worker therefore could not boot at all. Same mechanism as the
   * OpenTelemetry nesting documented elsewhere in this class, same remedy. The earlier regression
   * test could not catch it because {@code FilteredClassLoader} delegates class DEFINITION to a
   * parent whose classpath still has servlet-api; {@code OptionalDependencyAbsentTest} runs on a
   * genuinely stripped classpath instead.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "jakarta.servlet.Filter")
  // Also a servlet-web-only concern behaviourally — a headless worker
  // (WebApplicationType.NONE) that happens to have servlet-api on its classpath must still not get
  // a filter it can never invoke.
  @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
  static class ServletRequestContextConfiguration {

    /**
     * Registers a {@link ScopedValueFilter} that propagates HTTP request metadata into {@link
     * org.streamrune.core.StreamRuneContext} via ScopedValue. Registered into the servlet filter
     * chain by {@link #scopedValueFilterRegistration(ScopedValueFilter)}. The filter's OTel baggage
     * allowlist is {@code streamrune.metadata.baggage-allowlist}; the framework adds no key of its
     * own ({@code BaggageAllowlist.DEFAULT_ALLOWED_KEYS} is empty).
     */
    @Bean
    @ConditionalOnMissingBean
    public ScopedValueFilter scopedValueFilter(
        StreamRuneProperties properties,
        RequestIdentityPolicy requestIdentityPolicy,
        ObjectProvider<UserRoleResolver> userRoleResolver) {
      return new ScopedValueFilter(
          Set.copyOf(properties.metadata().baggageAllowlist()),
          requestIdentityPolicy,
          // The SAME resolver instance the
          // AnnotationAuthorizationInterceptor bean gets, so the authority captured at the request
          // edge is byte-identical to the one a synchronous execute would have computed.
          userRoleResolver.getIfAvailable());
    }

    /**
     * The source the command bus and the query bus take the caller's request context from for a
     * dispatch that starts with {@code StreamRuneContext.CURRENT} unbound: the context {@link
     * ScopedValueFilter} stored on the request the calling thread serves, which Spring MVC makes
     * current on the thread that runs a {@code Callable} a controller returns. Servlet web
     * applications only, beside the filter.
     */
    @Bean
    @ConditionalOnMissingBean
    public RequestAttributeContextSource requestAttributeContextSource() {
      return new RequestAttributeContextSource();
    }

    /**
     * Refuses startup when a command bus authorizes against the request identity but HTTP requests
     * have no identity source (no {@link AuthenticatedUserResolver}, {@code
     * streamrune.security.trust-user-id-header=false}), and logs the identity mode once. Servlet
     * web applications only — that is where {@link ScopedValueFilter} derives identities from
     * requests. See {@link StreamRuneRequestIdentityValidator} for the precise rule.
     */
    @Bean
    @ConditionalOnMissingBean
    public StreamRuneRequestIdentityValidator streamRuneRequestIdentityValidator(
        RequestIdentityPolicy requestIdentityPolicy,
        ObjectProvider<VirtualThreadCommandBus> commandBuses) {
      return new StreamRuneRequestIdentityValidator(requestIdentityPolicy, commandBuses);
    }

    /**
     * Registers the {@link ScopedValueFilter} in the servlet filter chain at the explicit position
     * {@link StreamRuneAutoConfiguration#SCOPED_VALUE_FILTER_ORDER}. Without a registration bean
     * Spring Boot would auto-register the bare {@code Filter} bean at {@code
     * Ordered.LOWEST_PRECEDENCE}, leaving its position relative to the security filter chain and
     * OTel instrumentation undefined. To change the order or URL patterns, define a bean named
     * {@code scopedValueFilterRegistration} wrapping the filter — this one then backs off, and
     * Spring Boot skips auto-registering the plain filter bean a registration bean already wraps.
     */
    @Bean
    @ConditionalOnMissingBean(name = "scopedValueFilterRegistration")
    public FilterRegistrationBean<ScopedValueFilter> scopedValueFilterRegistration(
        ScopedValueFilter filter) {
      var registration = new FilterRegistrationBean<>(filter);
      registration.setOrder(SCOPED_VALUE_FILTER_ORDER);
      registration.addUrlPatterns("/*");
      return registration;
    }
  }

  /**
   * Resolves the authenticated principal from Spring Security's {@code SecurityContextHolder} so
   * the request filter can bind the authorization identity from the verified principal rather than
   * the client-supplied {@code X-User-Id} header. Registered only when Spring Security is on the
   * classpath.
   *
   * <p>Safe to declare here despite spring-security being {@code compileOnly}: the returned type
   * implements only the framework's own {@link AuthenticatedUserResolver}, so linking it never
   * touches a Spring Security type — those appear solely inside method bodies, which are resolved
   * lazily.
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.security.core.context.SecurityContextHolder")
  @ConditionalOnMissingBean(AuthenticatedUserResolver.class)
  public SpringSecurityAuthenticatedUserResolver springSecurityAuthenticatedUserResolver() {
    return new SpringSecurityAuthenticatedUserResolver();
  }

  /**
   * The one rule by which every request-facing component ({@link ScopedValueFilter}, {@link
   * SseController}) derives a request's identity — so they can never disagree. {@code
   * streamrune.security.trust-user-id-header=true} selects the trusted-gateway mode (the {@code
   * X-User-Id} header is the identity); otherwise an {@link AuthenticatedUserResolver} bean selects
   * the authenticated principal, and without one every request is anonymous and the header is
   * ignored. Fail-closed: the header is never an identity unless the flag says so.
   */
  @Bean
  @ConditionalOnMissingBean
  public RequestIdentityPolicy requestIdentityPolicy(
      StreamRuneProperties properties,
      ObjectProvider<AuthenticatedUserResolver> authenticatedUserResolver) {
    return RequestIdentityPolicy.of(
        authenticatedUserResolver.getIfAvailable(), properties.security().trustUserIdHeader());
  }

  /** Provides a default empty EventTypeRegistry if the application does not define one. */
  @Bean
  @ConditionalOnMissingBean
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder().build();
  }

  /**
   * The stores of the backend ({@link BackendConfiguration#BACKEND}), opened once on the {@code
   * DataSource}: every store bean below that the framework builds is taken from it, so they all
   * share one backend. Opening builds no store: each store is built on first use and memoized by
   * the bundle. On Postgres opening touches no database; on SQLite it verifies the DataSource,
   * checks the connection setup and takes the file's instance guard (see below). The bundle is
   * closed when the context closes (its destroy method), which closes the leadership it built.
   *
   * <p>The bundle is opened through {@link org.streamrune.integration.BackendStartup#open}: a
   * backend other than Postgres is verified against the {@code DataSource} first (a SQLite backend
   * on a PostgreSQL database fails naming both, a SQLite connection setup that is wrong names the
   * setting), and SQLite's {@code open} takes the instance guard of the database file, so a second
   * process on the same file fails the start.
   *
   * <p><b>Shutdown order.</b> Every framework store bean is built from this bean, so Spring records
   * it as their dependency and destroys them, and every bean that injects them, before it; the
   * bundle is closed last, after every user of its stores. On SQLite this matters: a store keeps
   * writing the file after its bundle released the instance guard.
   *
   * <p>The bundle's context carries the {@link CryptoEngine} bean (if present), which the event
   * store, the saga store and the default read-model store wire into their ObjectMappers through
   * {@code CryptoShreddingModule} so {@code @Encrypted} fields are persisted as ciphertext; the
   * {@link org.streamrune.core.StreamRuneMetrics} and {@link StreamRuneTracing} beans (or their
   * no-op stand-ins); and the schema switches: {@code
   * streamrune.event-store.schema.auto-initialize}, {@code
   * streamrune.read-model.schema.auto-initialize} (unset, the former), and schema validation, which
   * is always on.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist PII as plaintext
   */
  @Bean(destroyMethod = "close")
  @ConditionalOnBean(DataSource.class)
  public org.streamrune.core.backend.BackendStores streamRuneBackendStores(
      org.streamrune.core.backend.StreamRuneBackend backend,
      DataSource dataSource,
      StreamRuneProperties properties,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {
    CryptoEngine cryptoEngine =
        resolveCryptoEngine(
            cryptoEngineProvider,
            "the event store, the saga store and the default read-model store");
    // Verified first unless it is the Postgres backend, which opens nothing (BackendStartup); on
    // SQLite, open() takes the instance guard, so another process holding the file fails here.
    return org.streamrune.integration.BackendStartup.open(
        backend,
        new org.streamrune.core.backend.BackendContext(
            dataSource,
            cryptoEngine,
            java.time.Clock.systemUTC(),
            // A mass redaction on a decrypt path emits subject_redacted here; NOOP when no
            // metrics bean exists. The backend guards the tracing with these metrics.
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP),
            tracingOf(tracingProvider),
            properties.eventStore().schema().autoInitialize(),
            properties.readModel().autoInitialize(properties.eventStore()),
            true));
  }

  /**
   * Creates the backend's EventStoreFactory when a DataSource is available: {@link
   * org.streamrune.core.backend.BackendStores#eventStoreFactory} with the event store's options.
   * The factory is configured now and touches the database only in {@code create()}.
   *
   * <p>Passes all {@link org.streamrune.core.upcasting.EventUpcaster} beans into the store so
   * stored events are upcast on read, and all {@link org.streamrune.core.SnapshotMigration} beans
   * so stored snapshots are upgraded on read; the {@link org.streamrune.core.audit.EventAuditStore}
   * and {@link CommandInbox} beans, which the store writes in the transaction of the append; and
   * the {@link org.streamrune.core.outbox.OutboxStore} bean together with the {@link
   * org.streamrune.core.outbox.OutboxEventMapper} bean, only when both exist. The crypto engine,
   * metrics and tracing come with the {@link #streamRuneBackendStores} bundle.
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
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(EventStoreFactory.class)
  public EventStoreFactory postgresEventStoreFactory(
      org.streamrune.core.backend.BackendStores stores,
      EventTypeRegistry typeRegistry,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.upcasting.EventUpcaster> upcasterProvider,
      ObjectProvider<org.streamrune.core.SnapshotMigration> snapshotMigrationProvider,
      ObjectProvider<org.streamrune.core.audit.EventAuditStore> eventAuditStoreProvider,
      ObjectProvider<org.streamrune.core.outbox.OutboxEventMapper> outboxEventMapperProvider,
      ObjectProvider<org.streamrune.core.outbox.OutboxStore> outboxStoreProvider,
      ObjectProvider<CommandInbox> commandInboxProvider,
      org.springframework.core.env.Environment environment) {
    // SQLite has no statement timeout: a value the application set is ignored there, with a WARN.
    org.streamrune.integration.BackendStartup.statementTimeoutIgnored(
        stores.backendName(),
        environment.getProperty(
            org.streamrune.integration.BackendStartup.STATEMENT_TIMEOUT_PROPERTY));
    org.streamrune.core.outbox.OutboxEventMapper outboxMapper =
        outboxEventMapperProvider.getIfAvailable();
    org.streamrune.core.outbox.OutboxStore outboxStore = outboxStoreProvider.getIfAvailable();
    if ((outboxMapper == null) != (outboxStore == null)) {
      LOG.warn(
          "Transactional outbox emission is DISABLED: found {} but not the other. "
              + "Define BOTH an OutboxEventMapper and an OutboxStore bean to enable it.",
          outboxMapper != null ? "an OutboxEventMapper" : "an OutboxStore");
    }
    boolean outboxPaired = outboxMapper != null && outboxStore != null;
    var options =
        new org.streamrune.core.backend.EventStoreOptions(
            typeRegistry,
            upcasterProvider.orderedStream().toList(),
            // Mirrors the EventUpcaster collection so a user's standard @Bean SnapshotMigration is
            // threaded into the store's snapshot-upgrade pipeline. Empty list = no migrations.
            snapshotMigrationProvider.orderedStream().toList(),
            outboxPaired ? outboxStore : null,
            outboxPaired ? outboxMapper : null,
            // The bus resolves this same CommandInbox bean, so the bus and the store share one
            // inbox; one the backend cannot write in the append's transaction fails here.
            commandInboxProvider.getIfAvailable(),
            eventAuditStoreProvider.getIfAvailable(),
            // The store borrows from the application DataSource as given and bounds its own
            // statements per transaction or per statement; the bound is the one knob it needs.
            properties.eventStore().statementTimeout());
    try {
      return stores.eventStoreFactory(options);
    } catch (org.streamrune.core.backend.BackendOptionException e) {
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
   * Creates an EventStore from the factory.
   *
   * <p><b>Transaction semantics:</b> the PostgreSQL event store acquires its own JDBC connections
   * from the {@link DataSource} and manages commit/rollback itself — event appends do <em>not</em>
   * participate in Spring-managed {@code @Transactional} transactions. A command handler that
   * writes to a JPA/JDBC read model inside {@code @Transactional} and also appends events performs
   * a non-atomic dual write. The event store is the single source of truth: derive read models from
   * events via projections (inline, continuous, or scheduled), and use the outbox for external side
   * effects, instead of writing both stores in one handler.
   */
  @Bean
  @ConditionalOnBean(EventStoreFactory.class)
  @ConditionalOnMissingBean
  public EventStore eventStore(EventStoreFactory factory) {
    return factory.create();
  }

  /**
   * Creates a default LocalStripedLocker with configured stripe count, tracing its acquisitions
   * through the {@link StreamRuneTracing} bean; a failure of that tracing counts in the {@link
   * org.streamrune.core.StreamRuneMetrics} bean.
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.core.AggregateLocker.class)
  public org.streamrune.core.AggregateLocker aggregateLocker(
      StreamRuneProperties properties,
      ObjectProvider<StreamRuneTracing> tracingProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    return new LocalStripedLocker(properties.stripeCount())
        .traced(
            org.streamrune.runtime.GuardedTracing.of(
                tracingOf(tracingProvider),
                metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP)));
  }

  /** The {@link StreamRuneTracing} bean, or {@link StreamRuneTracing#NOOP} without one. */
  private static StreamRuneTracing tracingOf(ObjectProvider<StreamRuneTracing> tracingProvider) {
    return tracingProvider.getIfAvailable(() -> StreamRuneTracing.NOOP);
  }

  /**
   * Creates a default {@link SimpleQueryBus} for query dispatching on the read side. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean (e.g. the auto-configured {@link
   * MicrometerStreamRuneMetrics}) is wired in when present so {@code queries.dispatched} and {@code
   * queries.duration} are recorded. Metrics are attached to this innermost bus only — the
   * decorating {@link AuditingQueryBus} is left without metrics so dispatched/duration are counted
   * exactly once. Cache hits short-circuit before reaching this bus and are tracked separately via
   * the {@link CachingQueryBus} cache meters.
   */
  @Bean
  @ConditionalOnMissingBean(SimpleQueryBus.class)
  public SimpleQueryBus simpleQueryBus(
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    return new SimpleQueryBus(
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Creates a {@link CachingQueryBus} that decorates {@link SimpleQueryBus} with per-query-type
   * caching when {@code streamrune.query-cache.enabled=true}. Registered as {@code @Primary} so it
   * wins over {@link SimpleQueryBus} for {@link QueryBus} injection points. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * queries.cache.hits} and {@code queries.cache.misses} are recorded; the caching bus does not
   * record dispatched/duration, so this does not double-count with {@link #simpleQueryBus}.
   *
   * <p>An application-supplied {@link org.streamrune.core.QueryAuthorizer} bean, if present, is
   * wired in too — consulted on every dispatch of a {@code @Cacheable} query, hit or miss, before
   * the cache is touched. Absent, no authorization check runs here and behavior is unchanged; see
   * {@link org.streamrune.core.QueryAuthorizer}'s javadoc for why one is usually needed once
   * caching is enabled for a query whose results depend on the caller.
   */
  @Bean
  @ConditionalOnProperty(prefix = "streamrune.query-cache", name = "enabled", havingValue = "true")
  @ConditionalOnMissingBean(name = "cachingQueryBus")
  public CachingQueryBus cachingQueryBus(
      SimpleQueryBus delegate,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.core.QueryAuthorizer> authorizerProvider) {
    var builder = CachingQueryBus.builder().delegate(delegate);
    metricsProvider.ifAvailable(builder::metrics);
    authorizerProvider.ifAvailable(builder::authorizer);
    return builder.build();
  }

  /**
   * Creates a {@link CacheInvalidator} backed by the auto-configured {@link CachingQueryBus}. Only
   * registered when a {@link CachingQueryBus} bean is present.
   */
  @Bean
  @ConditionalOnBean(CachingQueryBus.class)
  @ConditionalOnMissingBean
  public CacheInvalidator cacheInvalidator(CachingQueryBus bus) {
    return bus.cacheInvalidator();
  }

  /**
   * Creates the primary {@link QueryBus} bean composing the full chain: SimpleQueryBus →
   * CachingQueryBus (if enabled) → AuditingQueryBus (if AuditStore available), inside an {@link
   * UnboundRequestContextQueryBus} over the {@link RequestAttributeContextSource} bean in a servlet
   * web application.
   */
  // Back off on any application-defined QueryBus by TYPE, not just a bean literally named
  // "queryBus". A user QueryBus under any other name (e.g. a tenant/authorization-filtering
  // decorator) must suppress the framework's @Primary composite — otherwise @Primary silently
  // wins every injection point and the user's bus is instantiated but never used. The framework's
  // own building blocks (SimpleQueryBus, CachingQueryBus) are ignored so the composite does not
  // self-suppress. Matches the Quarkus @DefaultBean / Micronaut override semantics.
  @Bean
  @Primary
  @ConditionalOnMissingBean(
      value = QueryBus.class,
      ignored = {SimpleQueryBus.class, CachingQueryBus.class})
  public QueryBus queryBus(
      SimpleQueryBus simpleBus,
      ObjectProvider<CachingQueryBus> cachingBusProvider,
      ObjectProvider<AuditStore> auditStoreProvider,
      ObjectProvider<RequestAttributeContextSource> requestContextSourceProvider) {
    CachingQueryBus caching = cachingBusProvider.getIfAvailable();
    QueryBus bus = caching != null ? caching : simpleBus;
    AuditStore store = auditStoreProvider.getIfAvailable();
    QueryBus composed = store != null ? new AuditingQueryBus(bus, store) : bus;
    // In a servlet web application a query dispatched off the servlet thread (a Callable a
    // controller returns) is audited, cached and handled as the request's caller.
    RequestAttributeContextSource source = requestContextSourceProvider.getIfAvailable();
    return source != null ? new UnboundRequestContextQueryBus(composed, source) : composed;
  }

  /**
   * Creates a default VirtualThreadCommandBus. The {@link org.streamrune.core.StreamRuneMetrics}
   * bean (e.g. the auto-configured {@link MicrometerStreamRuneMetrics}) is wired in when present so
   * command and event metrics are actually recorded.
   *
   * <p><b>Interceptor ordering:</b> auto-registered interceptors carry explicit {@link Order}
   * values (the {@code ORDER_*} constants of {@link CommandInterceptorOrdering}), so the chain is
   * deterministic: OpenTelemetry → Audit → Authorization → AnnotationAuthorization → BeanValidation
   * → CircuitBreaker → InlineProjection. {@code before()} runs in that order; {@code after()} and
   * {@code onError()} in reverse. User interceptors without {@code @Order} run innermost (after all
   * framework interceptors); declare {@code @Order} on the bean to slot in between.
   *
   * <p><b>Tracing:</b> the bus traces through the {@link StreamRuneTracing} bean ({@link
   * VirtualThreadCommandBus.Builder#tracing}): it adds the {@link TracingCommandInterceptor} in its
   * slot ahead of every framework interceptor, unless the chain already holds one the application
   * declared. The {@link org.streamrune.core.AggregateLocker} bean traces its acquisitions through
   * it too when it is a {@link LocalStripedLocker} or a {@code PgAdvisoryLocker}.
   *
   * <p><b>Dead letter queue:</b> when the application defines a {@link
   * org.streamrune.core.DeadLetterQueue} bean, it is wired into the bus so commands that exhaust
   * retries are recorded. Payloads are ALWAYS serialized with {@link
   * #defaultDeadLetterObjectMapper(CryptoEngine, org.streamrune.core.StreamRuneMetrics)} — the same
   * crypto-aware mapper the auto-configured {@link DeadLetterRetryRunner} uses for deserialization,
   * so payloads round-trip — regardless of any application-defined {@code
   * com.fasterxml.jackson.databind.ObjectMapper} bean. That mapper is wired with the {@link
   * CryptoEngine} bean (if present), the same way {@link #postgresEventStoreFactory} wires the
   * event store, so {@code @Encrypted} command fields are persisted as ciphertext instead of
   * plaintext PII in the DLQ.
   *
   * <p><b>Fail-fast on {@code @Encrypted} command fields:</b> command types are validated at wiring
   * time via {@code CryptoConfigValidator}; if a registered command carries an {@code @Encrypted}
   * field but no {@link CryptoEngine} bean is configured, startup fails instead of silently
   * dead-lettering that PII as plaintext.
   *
   * <p><b>Snapshot policy:</b> the default is {@code
   * SnapshotPolicy.everyNEvents(streamrune.snapshot-every-n-events)} at snapshot schema version 1.
   * An application {@link SnapshotPolicy} bean replaces it wholesale — that is the ONLY way to bump
   * the snapshot schema version or to switch snapshots off, and until this hook existed both were
   * unreachable through the auto-configuration: the version was pinned to 1, which also made every
   * registered {@code SnapshotMigration} bean dead code (the store's migrate/discard branch is only
   * entered on a version mismatch). It is deliberately a bean rather than a property — the version
   * must track the application's state-class schema, so it belongs next to that code and is
   * released with it, exactly as {@code docs/guide/advanced/snapshot-versioning.md} specifies.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist @Encrypted DLQ payloads as
   *     plaintext; or if a registered command type has an {@code @Encrypted} field with no
   *     CryptoEngine configured
   */
  @Bean
  @ConditionalOnMissingBean(VirtualThreadCommandBus.class)
  public VirtualThreadCommandBus virtualThreadCommandBus(
      EventStore eventStore,
      org.streamrune.core.AggregateLocker locker,
      StreamRuneProperties properties,
      ObjectProvider<SnapshotPolicy> snapshotPolicyProvider,
      ObjectProvider<CommandInterceptor> interceptorProvider,
      ObjectProvider<DeciderRegistration<?, ?, ?>> registrationProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.core.DeadLetterQueue> deadLetterQueueProvider,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<CommandInbox> commandInboxProvider,
      ObjectProvider<RequestAttributeContextSource> requestContextSourceProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {

    // Resolve the CryptoEngine once, up front: it both fails fast on @Encrypted command fields
    // (below) and makes the DLQ publish mapper crypto-aware (below).
    CryptoEngine cryptoEngine = resolveCryptoEngine(cryptoEngineProvider, "a command type");

    // Fail fast when a registered command type carries an @Encrypted field but no
    // CryptoEngine is configured — otherwise that PII would be dead-lettered as PLAINTEXT. Mirrors
    // the event/state-type validation already done over EventTypeRegistry.registeredTypes().
    java.util.List<Class<?>> commandTypes =
        registrationProvider
            .orderedStream()
            .map(org.streamrune.runtime.DeciderRegistration::commandType)
            .collect(java.util.stream.Collectors.toList());
    org.streamrune.crypto.CryptoConfigValidator.validate(commandTypes, cryptoEngine);

    var retryPolicy =
        new RetryPolicy(
            properties.retryMaxAttempts(),
            properties.retryInitialDelay(),
            properties.retryBackoffMultiplier());

    // Normalize the chain the same way Quarkus and Micronaut do. Spring's own
    // framework interceptors already carry @Order and so were unaffected by this, but an
    // application bean that OVERRIDES a framework interceptor TYPE (e.g. a custom
    // AuditCommandInterceptor, back off via @ConditionalOnMissingBean below) carries no @Order and
    // sorted at Ordered.LOWEST_PRECEDENCE — innermost, after AuthorizationCommandInterceptor —
    // instead of its canonical, security-relevant slot. CommandInterceptorOrdering.sorted() keys by
    // exact class, so it restores the slot regardless of annotations, matching Quarkus/Micronaut.
    List<CommandInterceptor> interceptors =
        CommandInterceptorOrdering.sorted(interceptorProvider.orderedStream().toList());

    // The tracing meets the metrics bean here: its guard counts its failures there, and it is
    // this guard the locker and the bus are handed.
    StreamRuneTracing tracing =
        org.streamrune.runtime.GuardedTracing.of(
            tracingOf(tracingProvider),
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    var builder =
        VirtualThreadCommandBus.builder()
            .allowGraalJit(properties.jvm().allowGraalJit())
            .eventStore(eventStore)
            .locker(traced(locker, tracing))
            .tracing(tracing)
            .retryPolicy(retryPolicy)
            .snapshotPolicy(
                snapshotPolicyProvider.getIfAvailable(
                    () -> SnapshotPolicy.everyNEvents(properties.snapshotEveryNEvents())))
            .lockTimeout(properties.lockTimeout())
            .stripeCount(properties.stripeCount())
            .maxInFlightAsyncCommands(properties.maxInFlightAsyncCommands())
            .interceptors(interceptors);
    // In a servlet web application a command dispatched off the servlet thread (a Callable a
    // controller returns) is authorized, audited and recorded as the request's caller.
    requestContextSourceProvider.ifAvailable(builder::unboundRequestContext);

    org.streamrune.core.StreamRuneMetrics metrics =
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP);
    builder.metrics(metrics);

    org.streamrune.core.DeadLetterQueue deadLetterQueue = deadLetterQueueProvider.getIfAvailable();
    if (deadLetterQueue != null) {
      // The DLQ publish mapper is ALWAYS the crypto-aware default — never the application's
      // own ObjectMapper bean — so @Encrypted command fields are persisted as ciphertext, mirroring
      // the event store, saga store, and Micronaut. The metrics bean is threaded in so the shared
      // DLQ decrypt mapper's CryptoShreddingModule reports subject_redacted. The retry runner's
      // policy and inbox window tell a keyed command that fails again under its key whether the
      // entry stored for it is still replayed on the schedule.
      builder
          .deadLetterQueue(deadLetterQueue)
          .objectMapper(defaultDeadLetterObjectMapper(cryptoEngine, metrics))
          .deadLetterRetryPolicy(deadLetterRetryPolicy(properties))
          .inboxRetentionMaxAge(properties.inbox().retentionMaxAge());
    }

    commandInboxProvider.ifAvailable(builder::commandInbox);

    registrationProvider.orderedStream().forEach(reg -> registerDecider(builder, reg));

    return builder.build();
  }

  /**
   * Resolves the optional {@link CryptoEngine} bean for wiring into a crypto-aware {@link
   * com.fasterxml.jackson.databind.ObjectMapper}, failing fast if multiple candidates exist with
   * none marked {@code @Primary} — silently picking one (or skipping encryption) would risk
   * persisting {@code @Encrypted} fields as plaintext PII. Shared by {@link
   * #virtualThreadCommandBus} and {@link #streamRuneDeadLetterRetryRunner} so both the DLQ publish
   * and replay mappers resolve the engine identically.
   *
   * @param cryptoEngineProvider the provider to resolve
   * @param target a short description of what the engine would be wired into, for the exception
   *     message (e.g. {@code "the dead-letter queue"})
   */
  private static CryptoEngine resolveCryptoEngine(
      ObjectProvider<CryptoEngine> cryptoEngineProvider, String target) {
    try {
      return cryptoEngineProvider.getIfAvailable();
    } catch (org.springframework.beans.factory.NoUniqueBeanDefinitionException e) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Primary — StreamRune cannot "
              + "decide which engine to wire into "
              + target
              + ", and skipping encryption would persist @Encrypted fields as PLAINTEXT. Mark "
              + "exactly one CryptoEngine bean as @Primary.",
          e);
    }
  }

  /**
   * The policy the dead-letter retry runner replays under, built from {@code
   * streamrune.dead-letter.*}: the runner and the command bus take the same one, so the bus answers
   * a keyed command that fails again under its key by the rules the runner replays by.
   */
  static org.streamrune.core.DeadLetterRetryPolicy deadLetterRetryPolicy(
      StreamRuneProperties properties) {
    return new org.streamrune.core.DeadLetterRetryPolicy(
        properties.deadLetter().maxRetries(),
        properties.deadLetter().retryInterval(),
        2.0,
        false,
        properties.deadLetter().unkeyedReplayPolicy());
  }

  /** Captures the registration's type parameters once so the builder call type-checks. */
  private static <
          C extends org.streamrune.core.Command,
          S extends org.streamrune.core.AggregateState,
          E extends org.streamrune.core.DomainEvent>
      void registerDecider(
          VirtualThreadCommandBus.Builder builder, DeciderRegistration<C, S, E> reg) {
    builder.register(reg.aggregateType(), reg.commandType(), reg.idExtractor(), reg.decider());
  }

  /** Exposes the VirtualThreadCommandBus as AsyncCommandBus for async injection. */
  @Bean
  @ConditionalOnMissingBean(AsyncCommandBus.class)
  public AsyncCommandBus asyncCommandBus(VirtualThreadCommandBus commandBus) {
    return commandBus;
  }

  /**
   * Creates a default {@link SseEventPublisher} for Server-Sent Events, matching the Quarkus and
   * Micronaut integrations. When the endpoint is enabled, the nested SSE configuration feeds the
   * publisher from the event store ({@code streamRuneSseEventFeed(...)}) and subscribes its clients
   * to it ({@code streamRuneSseController(...)}).
   */
  @Bean
  @ConditionalOnMissingBean(SseEventPublisher.class)
  public SseEventPublisher sseEventPublisher() {
    return new SseEventPublisher();
  }

  /**
   * The Server-Sent Events endpoint, isolated in a nested {@code @Configuration} — one nested
   * configuration per optional surface, matching {@link ServletRequestContextConfiguration} and
   * {@link HealthIndicatorConfiguration}.
   *
   * <p><strong>Servlet + MVC only:</strong> {@link SseController#stream(String,
   * jakarta.servlet.http.HttpServletRequest)} returns Spring MVC's {@code SseEmitter}, so the
   * controller can only exist on a servlet + Spring-MVC stack. Gated only on the property and on
   * the (unconditionally auto-configured) {@link SseEventPublisher}, the bean was created on stacks
   * that have neither — an app on {@code spring-boot-starter-jersey}, or a worker replica sharing
   * the web app's {@code application.yml} with {@code streamrune.sse.enabled=true} — and Spring's
   * introspection of the controller class for lifecycle/autowire metadata and the inferred {@code
   * close()} destroy method failed with {@code NoClassDefFoundError} → {@code Failed to introspect
   * Class [org.streamrune.spring.SseController]} → boot failure. The framework's own
   * headless-worker guidance broke the moment shared config enabled SSE. Quarkus and Micronaut
   * cannot hit this: their SSE controller can only exist with its HTTP stack present.
   *
   * <p>The class-name gate also keeps the ENCLOSING configuration free of the MVC-bound type: a
   * top-level {@code @Bean} signature returning {@link SseController} would be resolved by {@code
   * Class#getDeclaredMethods} on every classpath. {@link #sseEventPublisher()} stays outside — the
   * publisher is stack-independent.
   *
   * <p><strong>The feed belongs to the endpoint.</strong> The {@link SseEventFeed} that publishes
   * the stored events is registered here, under the same conditions as the controller: an
   * application without the endpoint (a headless worker sharing the web application's
   * configuration) does not read the global stream for it.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
  @ConditionalOnClass(name = "org.springframework.web.servlet.mvc.method.annotation.SseEmitter")
  @ConditionalOnProperty(name = "streamrune.sse.enabled", havingValue = "true")
  static class SseEndpointConfiguration {

    /**
     * Fail-closed {@link SseAuthorizer} used when the SSE endpoint is enabled but the application
     * provides no authorizer of its own. Denies every stream, so enabling SSE never exposes an
     * aggregate's events without an explicit access decision. Applications enable SSE by defining
     * their own {@link SseAuthorizer} bean (and setting {@code streamrune.sse.enabled=true}).
     */
    @Bean
    @ConditionalOnMissingBean(SseAuthorizer.class)
    public SseAuthorizer sseDenyAllAuthorizer() {
      return SseAuthorizer.DENY_ALL;
    }

    /**
     * Registers the shipped {@link SseController} (mapped at {@code
     * /api/sse/{aggregateType}/{aggregateId}}) only when {@code streamrune.sse.enabled=true}. The
     * endpoint streams an aggregate's decrypted domain events by stream, so it is disabled by
     * default and, when enabled, gated by the {@link SseAuthorizer} bean (a fail-closed deny-all
     * authorizer when the application supplies none). The caller is resolved by the same {@link
     * RequestIdentityPolicy} as the request filter.
     */
    // Deliberately NOT @ConditionalOnBean(SseEventPublisher.class). Spring registers a
    // configuration class's member classes BEFORE the enclosing class's own @Bean definitions, and
    // REGISTER_BEAN-phase conditions are evaluated against the registry as it goes — so a condition
    // on the enclosing sseEventPublisher could never see it and would silently drop the controller.
    // The publisher is unconditional anyway, and the parameter below already requires it.
    @Bean
    @ConditionalOnMissingBean(SseController.class)
    public SseController streamRuneSseController(
        SseEventPublisher publisher,
        SseAuthorizer authorizer,
        RequestIdentityPolicy requestIdentityPolicy,
        StreamRuneProperties properties) {
      return new SseController(
          publisher,
          authorizer,
          requestIdentityPolicy,
          properties.sse().timeout(),
          properties.sse().keepAliveInterval());
    }

    /**
     * The exception resolver the dispatcher servlet consults before every other: Spring MVC's own
     * list and the application's resolvers, whatever their order (see {@link
     * SseHandoverFailureResolver}). It releases the stream of a request whose handling failed while
     * Spring MVC was taking the emitter over (a client that reset its connection before the
     * response was written) and resolves nothing itself: every exception goes on to the resolvers
     * after it. Not an autowire candidate, so it never competes for an injection point of the
     * application that asks for a {@link HandlerExceptionResolver}.
     *
     * @return the resolver
     */
    @Bean(autowireCandidate = false)
    public HandlerExceptionResolver streamRuneSseHandoverFailureResolver() {
      return new SseHandoverFailureResolver();
    }

    /**
     * The feed of the endpoint: a polling subscription that starts at the head of the global stream
     * and publishes every event stored from then on to the subscribers of the event's own stream,
     * every {@code streamrune.sse.polling-interval}. Live, best-effort and at-most-once — see
     * {@link SseEventFeed}. Started and stopped by {@link #streamRuneSseEventFeedLifecycle}, and
     * reported by the {@link org.streamrune.runtime.BackgroundRelayHealthContributor} as {@code
     * sse-event-feed}: {@code DOWN} once its polling thread has died.
     *
     * <p>An application that declares its own {@link SseEventFeed} bean replaces this one and
     * starts and stops its feed itself: the lifecycle below belongs to the auto-configured bean
     * only. A {@code streamrune.sse.polling-interval} below one millisecond fails the start-up
     * here.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(SseEventFeed.class)
    public SseEventFeed streamRuneSseEventFeed(
        EventStore eventStore,
        SseEventPublisher publisher,
        StreamRuneProperties properties,
        ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor>
            relayHealthProvider) {
      SseEventFeed feed =
          new SseEventFeed(eventStore, publisher, properties.sse().pollingInterval());
      relayHealthProvider.ifAvailable(c -> c.registerSseEventFeed(feed));
      return feed;
    }

    /**
     * Starts the auto-configured {@link SseEventFeed} on context refresh, before the web server
     * accepts requests, and stops it on context close, after the web server has drained and the
     * controller has ended its open streams. Conditional on the auto-configured bean name, so a
     * feed the application declares (and starts and stops itself) is never started twice.
     *
     * <p>The feed and Spring Boot's web server start in the same lifecycle phase ({@link
     * RunnerLifecycle#PHASE}); see there for why the feed comes first.
     */
    @Bean
    @ConditionalOnBean(name = "streamRuneSseEventFeed")
    public RunnerLifecycle streamRuneSseEventFeedLifecycle(
        @Qualifier("streamRuneSseEventFeed") SseEventFeed feed) {
      return new RunnerLifecycle("SseEventFeed", feed::start, feed::close);
    }
  }

  /**
   * The actuator health indicator, isolated in a nested {@code @Configuration} for the same reason
   * as {@link ServletRequestContextConfiguration}: {@link StreamRuneHealthIndicator} implements
   * {@code org.springframework.boot.health.contributor.HealthIndicator}, which in Boot 4 lives in
   * the {@code spring-boot-health} module contributed only by the actuator starter (a {@code
   * compileOnly} dependency here). With actuator absent, resolving this method's return type during
   * {@code Class#getDeclaredMethods} threw {@code NoClassDefFoundError} and failed context refresh
   * — the {@code @ConditionalOnClass} gate on the method could not help, because it is evaluated
   * only after the enclosing class has been introspected.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
  static class HealthIndicatorConfiguration {

    /** Registers a health indicator that pings the PostgreSQL DataSource. */
    @Bean
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean(StreamRuneHealthIndicator.class)
    public StreamRuneHealthIndicator streamRuneHealthIndicator(
        DataSource dataSource,
        ObjectProvider<org.streamrune.runtime.SubscriptionHealthContributor>
            healthContributorProvider,
        ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor>
            relayHealthContributorProvider) {
      return new StreamRuneHealthIndicator(
          dataSource,
          healthContributorProvider.getIfAvailable(),
          relayHealthContributorProvider.getIfAvailable());
    }
  }

  /**
   * Aggregates outbox-relay and DLQ-retry-runner liveness for the health endpoint. The runners
   * register themselves into it when they are created (below), so {@code /health} reports DOWN when
   * a started relay's poll thread has died instead of silently reporting UP.
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
  public org.streamrune.runtime.BackgroundRelayHealthContributor
      backgroundRelayHealthContributor() {
    return new org.streamrune.runtime.BackgroundRelayHealthContributor();
  }

  /**
   * The tracing of an application without {@code opentelemetry-api} on its classpath: {@link
   * StreamRuneTracing#NOOP}, which traces nothing and loads no OpenTelemetry class. An application
   * {@code StreamRuneTracing} bean replaces it.
   */
  @Bean
  @ConditionalOnMissingClass("io.opentelemetry.api.OpenTelemetry")
  @ConditionalOnMissingBean(StreamRuneTracing.class)
  public StreamRuneTracing noopStreamRuneTracing() {
    return StreamRuneTracing.NOOP;
  }

  /**
   * The tracing of an application with {@code opentelemetry-api} on its classpath, isolated in a
   * nested {@code @Configuration} gated by {@code @ConditionalOnClass} on the OTel API — evaluated
   * by class NAME via configuration metadata, so Spring never introspects this class's OTel-typed
   * {@code @Bean} method signature when {@code opentelemetry-api} (a {@code compileOnly},
   * classpath-optional dependency) is absent. Without this isolation an application that omits
   * {@code opentelemetry-api} would fail context init with a NoClassDefFoundError as Spring
   * reflected on the {@code OpenTelemetry}-typed bean method.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "io.opentelemetry.api.OpenTelemetry")
  static class OpenTelemetryConfiguration {

    /**
     * The tracing every component the integration builds starts its spans through, under the
     * switches of {@code streamrune.tracing.*}. It traces through the application's {@code
     * OpenTelemetry} bean, or, without one, through the global OpenTelemetry ({@code
     * GlobalOpenTelemetry.get()}), which the OpenTelemetry Java agent registers before the
     * application starts. It is {@link StreamRuneTracing#NOOP} when the master switch is off, and
     * when that OpenTelemetry has no tracer provider of its own (nothing registered), so an
     * application without OpenTelemetry pays nothing. It is {@code NOOP} too when the environment
     * holds one of the {@link org.streamrune.integration.OpenTelemetrySwitches#SPRING switches}
     * that turn OpenTelemetry tracing off ({@code management.opentelemetry.enabled=false}, {@code
     * otel.sdk.disabled=true}). An application {@code StreamRuneTracing} bean replaces it. A
     * failure of the tracing counts in the {@link org.streamrune.core.StreamRuneMetrics} bean
     * ({@code streamrune.tracing.failures}).
     *
     * <p>The global OpenTelemetry is read once, here: OpenTelemetry fixes it on its first read, so
     * an SDK the application registers globally is registered before the context starts, or is an
     * {@code OpenTelemetry} bean.
     */
    @Bean
    @ConditionalOnMissingBean(StreamRuneTracing.class)
    public StreamRuneTracing streamRuneTracing(
        ObjectProvider<io.opentelemetry.api.OpenTelemetry> openTelemetry,
        StreamRuneProperties properties,
        org.springframework.core.env.Environment environment,
        ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
      // Made counting: a failure of the tracing counts in the metrics bean wherever the bean is
      // handed to, the application's own components included.
      return org.streamrune.runtime.GuardedTracing.of(
          tracingOf(
              openTelemetry.getIfAvailable(),
              properties.tracing().settings(),
              environment::getProperty),
          metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    }

    /**
     * The tracing over {@code openTelemetry}, or over the global OpenTelemetry when it is {@code
     * null}, under {@code settings}; {@link StreamRuneTracing#NOOP} when {@code property} holds one
     * of the {@link org.streamrune.integration.OpenTelemetrySwitches#SPRING switches} that turn
     * OpenTelemetry tracing off.
     */
    static StreamRuneTracing tracingOf(
        io.opentelemetry.api.OpenTelemetry openTelemetry,
        TracingSettings settings,
        java.util.function.Function<String, String> property) {
      if (!settings.enabled()) {
        return StreamRuneTracing.NOOP;
      }
      var switchedOff =
          org.streamrune.integration.OpenTelemetrySwitches.switchedOffBy(
              org.streamrune.integration.OpenTelemetrySwitches.SPRING, property);
      if (switchedOff.isPresent()) {
        LOG.info(
            "StreamRune starts no span: OpenTelemetry tracing is switched off ({})",
            switchedOff.get());
        return StreamRuneTracing.NOOP;
      }
      io.opentelemetry.api.OpenTelemetry source =
          openTelemetry != null ? openTelemetry : io.opentelemetry.api.GlobalOpenTelemetry.get();
      if (source.getTracerProvider() == io.opentelemetry.api.trace.TracerProvider.noop()) {
        return StreamRuneTracing.NOOP;
      }
      return OpenTelemetryTracing.of(source, settings);
    }
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
    if (!org.streamrune.runtime.GuardedTracing.of(tracing)
        .enabled(org.streamrune.core.tracing.TracingArea.LOCK)) {
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
   * Registers a SubscriptionHealthContributor when EventStore and OffsetStore are available. The
   * auto-configured {@link org.streamrune.core.StreamRuneMetrics} bean (e.g. {@link
   * MicrometerStreamRuneMetrics}) is wired in when present so each health sample also emits the
   * {@code streamrune.subscriptions.lag} gauge; absent metrics fall back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} (behavior unchanged). The lag at which a running
   * subscription reads {@code DEGRADED} is {@code streamrune.subscription.health.lag-threshold}
   * (default 1000 events); lag never reads {@code DOWN}.
   *
   * <p><strong>Offset store resolution:</strong> the OffsetStore is resolved through an {@link
   * ObjectProvider} at instantiation time and must NOT be added back to {@code @ConditionalOnBean}.
   * Spring registers a configuration class's {@code @Bean} definitions in declaration order and
   * evaluates REGISTER_BEAN-phase conditions against the registry as it goes; the auto-configured
   * {@link #postgresOffsetStore} is declared much later in this same class, so a condition on
   * {@code OffsetStore} could never see it and the contributor was silently absent from every
   * default deployment — {@code /health} reported UP with a terminally halted continuous projection
   * (frozen read model) and the documented {@code streamrune.subscriptions.lag} gauge was never
   * emitted. Returning {@code null} when no OffsetStore exists mirrors {@link
   * ProjectionAutoConfig}, whose {@code healthProvider.ifAvailable(...)} consumer filters the
   * resulting NullBean.
   */
  @Bean
  @ConditionalOnBean(EventStore.class)
  @ConditionalOnMissingBean(org.streamrune.runtime.SubscriptionHealthContributor.class)
  public org.streamrune.runtime.SubscriptionHealthContributor subscriptionHealthContributor(
      EventStore eventStore,
      ObjectProvider<org.streamrune.core.projection.OffsetStore> offsetStoreProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      StreamRuneProperties properties) {
    org.streamrune.core.projection.OffsetStore offsetStore = offsetStoreProvider.getIfAvailable();
    if (offsetStore == null) {
      // No offset store anywhere (no DataSource and no user-defined store) — there is nothing to
      // measure subscription lag against. Spring skips a null @Bean product.
      return null;
    }
    long lagThreshold = properties.subscription().health().lagThreshold();
    if (lagThreshold < 1) {
      throw new IllegalStateException(
          "streamrune.subscription.health.lag-threshold must be at least 1, got: " + lagThreshold);
    }
    return new org.streamrune.runtime.SubscriptionHealthContributor(
        eventStore,
        offsetStore,
        lagThreshold,
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Command validation, isolated in a nested {@code @Configuration} gated by class NAME on {@code
   * jakarta.validation.Validator}.
   *
   * <p><strong>Nesting:</strong> the nesting is not cosmetic. The interceptor's {@code
   * getIfAvailable} fallback lambda compiles to a synthetic {@code private static
   * jakarta.validation.Validator lambda$beanValidationInterceptor$0()} <em>method of the enclosing
   * class</em> — a real method descriptor naming an optional type, invisible in the source. {@code
   * Class#getDeclaredMethods} resolves synthetic methods too, so on a classpath without
   * jakarta.validation-api (a {@code compileOnly} dependency) the whole configuration class failed
   * to introspect and the application did not boot. The {@code @ConditionalOnClass} on the bean
   * method never got a chance to run. Moving the method — and with it its lambda — into this nested
   * class keeps the enclosing class free of the type.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "jakarta.validation.Validator")
  static class BeanValidationConfiguration {

    /**
     * Registers a {@link BeanValidationInterceptor} when Jakarta Validation is on the classpath and
     * validation is enabled.
     *
     * <p><strong>Validation provider:</strong> {@code @ConditionalOnClass} can only see the API
     * jar, not a provider, and jakarta.validation-api is a common <em>transitive</em> dependency.
     * With the API present, no {@code Validator} bean, and no provider (no hibernate-validator /
     * spring-boot-starter-validation), the {@code buildDefaultValidatorFactory()} fallback below
     * throws {@code NoProviderFoundException}; without the catch that would <em>fail context
     * initialization</em> — the application would not boot, from a dependency graph that boots fine
     * without StreamRune. That case degrades to validation-disabled (returning {@code null}, which
     * Spring skips) with a WARN naming the remedy, exactly as on Quarkus: silently disabled command
     * validation deserves a visible signal on every framework.
     */
    @Bean
    @Order(CommandInterceptorOrdering.ORDER_VALIDATION)
    @ConditionalOnMissingBean(BeanValidationInterceptor.class)
    @ConditionalOnProperty(
        name = "streamrune.validation-enabled",
        havingValue = "true",
        matchIfMissing = true)
    public BeanValidationInterceptor beanValidationInterceptor(
        ObjectProvider<jakarta.validation.Validator> validatorProvider) {
      try {
        jakarta.validation.Validator validator =
            validatorProvider.getIfAvailable(
                () -> jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator());
        return new BeanValidationInterceptor(validator);
      } catch (jakarta.validation.ValidationException e) {
        // No usable provider on the classpath (NoProviderFoundException extends
        // ValidationException).
        LOG.warn(
            "StreamRune: jakarta.validation-api is on the classpath but no Bean Validation provider"
                + " could be bootstrapped — command validation is DISABLED. Add hibernate-validator"
                + " (or spring-boot-starter-validation) to enable it, or set"
                + " streamrune.validation-enabled=false to silence this warning. Cause: {}",
            e.getMessage());
        return null;
      }
    }
  }

  /**
   * Registers a MicrometerStreamRuneMetrics bean when a MeterRegistry is available. Meter names use
   * the {@code streamrune.metrics.prefix} property (default {@code streamrune}).
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.core.StreamRuneMetrics.class)
  @ConditionalOnBean(io.micrometer.core.instrument.MeterRegistry.class)
  @ConditionalOnProperty(
      name = "streamrune.metrics.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public MicrometerStreamRuneMetrics micrometerStreamRuneMetrics(
      io.micrometer.core.instrument.MeterRegistry meterRegistry, StreamRuneProperties properties) {
    return MicrometerStreamRuneMetrics.builder()
        .registry(meterRegistry)
        .prefix(properties.metrics().prefix())
        .build();
  }

  /** Registers a circuit breaker interceptor with configured failure threshold and cooldown. */
  @Bean
  @Order(CommandInterceptorOrdering.ORDER_CIRCUIT_BREAKER)
  @ConditionalOnMissingBean(CircuitBreakerCommandInterceptor.class)
  public CircuitBreakerCommandInterceptor circuitBreakerCommandInterceptor(
      StreamRuneProperties properties) {
    return new CircuitBreakerCommandInterceptor(
        properties.circuitBreakerFailureThreshold(), properties.circuitBreakerCooldown());
  }

  /**
   * Registers an authorization interceptor when the application provides a {@link
   * CommandAuthorizationPolicy} bean.
   */
  @Bean
  @Order(CommandInterceptorOrdering.ORDER_AUTHORIZATION)
  @ConditionalOnBean(CommandAuthorizationPolicy.class)
  @ConditionalOnMissingBean(AuthorizationCommandInterceptor.class)
  public AuthorizationCommandInterceptor authorizationCommandInterceptor(
      CommandAuthorizationPolicy policy) {
    return new AuthorizationCommandInterceptor(policy);
  }

  /**
   * Registers a {@link SpringSecurityUserRoleResolver} when Spring Security is on the classpath.
   */
  @Bean
  @ConditionalOnClass(name = "org.springframework.security.core.context.SecurityContextHolder")
  @ConditionalOnMissingBean(UserRoleResolver.class)
  public SpringSecurityUserRoleResolver springSecurityUserRoleResolver() {
    return new SpringSecurityUserRoleResolver();
  }

  /**
   * Registers an {@link AnnotationAuthorizationInterceptor} when a {@link UserRoleResolver} bean is
   * available.
   *
   * <p>The resolver is used as-is — it is deliberately <em>not</em> wrapped in {@link
   * org.streamrune.runtime.CachingUserRoleResolver}. Caching by {@link
   * org.streamrune.core.types.UserId} is unsound for resolvers that derive authorities from ambient
   * per-request state (e.g. {@link SpringSecurityUserRoleResolver} reading {@code
   * SecurityContextHolder}): a cache hit would bypass the resolver's principal-match check and
   * serve one principal's authorities to a different caller presenting the same user ID.
   * Applications whose resolver is a pure function of the user ID (e.g. a database lookup) can opt
   * in explicitly by defining a {@code CachingUserRoleResolver} bean wrapping their resolver.
   */
  @Bean
  @Order(CommandInterceptorOrdering.ORDER_ANNOTATION_AUTHORIZATION)
  @ConditionalOnBean(UserRoleResolver.class)
  @ConditionalOnMissingBean(AnnotationAuthorizationInterceptor.class)
  public AnnotationAuthorizationInterceptor annotationAuthorizationInterceptor(
      UserRoleResolver resolver) {
    return new AnnotationAuthorizationInterceptor(resolver);
  }

  /**
   * Fails startup when a command bus registers commands that carry
   * {@code @RequireRole}/{@code @RequirePermission} but its interceptor chain has no {@link
   * AnnotationAuthorizationInterceptor} — fail-closed instead of silently executing annotated
   * commands unguarded. Checks every {@link VirtualThreadCommandBus} bean, the auto-configured one
   * or the application's own.
   */
  @Bean
  @ConditionalOnMissingBean(StreamRuneAuthorizationValidator.class)
  public StreamRuneAuthorizationValidator streamRuneAuthorizationValidator(
      ObjectProvider<VirtualThreadCommandBus> commandBuses) {
    return new StreamRuneAuthorizationValidator(commandBuses);
  }

  /**
   * Fails startup when a registered saga carries an {@code @Encrypted} saga-state field but no
   * {@link CryptoEngine} bean is configured — fail-closed instead of silently persisting that PII
   * as plaintext in {@code saga_state}.
   */
  @Bean
  @ConditionalOnMissingBean(SagaStateCryptoValidator.class)
  public SagaStateCryptoValidator sagaStateCryptoValidator(
      ObjectProvider<org.streamrune.runtime.SagaRunner<?>> sagaRunnerProvider,
      ObjectProvider<CryptoEngine> cryptoEngineProvider) {
    return new SagaStateCryptoValidator(sagaRunnerProvider, cryptoEngineProvider);
  }

  /**
   * Supplies {@code streamrune.inbox.retention-max-age} to every application {@code SagaRunner}
   * bean's event-path compensation key-age guard. Deliberately unconditional — the event path is
   * the one compensation re-driver with no leadership gate and no enable knob, so its guard must
   * not depend on the sweeper being switched on.
   */
  @Bean
  @ConditionalOnMissingBean(SagaEventPathRetentionConfigurer.class)
  public SagaEventPathRetentionConfigurer sagaEventPathRetentionConfigurer(
      ObjectProvider<org.streamrune.runtime.SagaRunner<?>> sagaRunnerProvider,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.subscription.SubscriptionLeadership> leadershipProvider,
      org.springframework.core.env.Environment environment) {
    return new SagaEventPathRetentionConfigurer(
        sagaRunnerProvider, properties, leadershipProvider, environment);
  }

  /**
   * After every {@code SmartLifecycle} has started: unknown {@code
   * streamrune.saga.subscriptions.<name>.parallelism} names, one line per saga runner, the refusal
   * of a pool the aggregate locks can take whole, and the pool warning.
   */
  @Bean
  @ConditionalOnMissingBean(SagaParallelismStartupCheck.class)
  public SagaParallelismStartupCheck sagaParallelismStartupCheck(
      ObjectProvider<org.streamrune.runtime.SagaRunner<?>> sagaRunners,
      StreamRuneProperties properties,
      org.springframework.core.env.Environment environment,
      ObjectProvider<javax.sql.DataSource> dataSource,
      ObjectProvider<org.streamrune.core.AggregateLocker> locker) {
    return new SagaParallelismStartupCheck(
        sagaRunners, properties, environment, dataSource, locker);
  }

  /**
   * Registers the backend's saga store ({@link
   * org.streamrune.core.backend.BackendStores#sagaStore}) when DataSource is available.
   *
   * <p>The bundle wires the {@link CryptoEngine} bean (if present) into the saga store's
   * ObjectMapper via {@code CryptoShreddingModule}, the same way it wires the event store — so
   * {@code @Encrypted} saga-state fields are persisted as ciphertext instead of plaintext PII, and
   * fall within GDPR forget scope.
   */
  @Bean
  @ConditionalOnMissingBean(SagaStore.class)
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public SagaStore sagaStore(org.streamrune.core.backend.BackendStores stores) {
    return stores.sagaStore();
  }

  /**
   * Registers the backend's saga dead-letter store ({@link
   * org.streamrune.core.backend.BackendStores#sagaDeadLetterStore}) when DataSource is available.
   * Applications that supply their own {@link SagaDeadLetterStore} bean will suppress this default
   * (guarded on the interface type via {@link ConditionalOnMissingBean#value()} so a user-supplied
   * interface-typed bean still overrides it).
   *
   * <p>The bean is declared with the interface type. {@link
   * #streamRuneSagaDeadLetterRetentionSweeper} does not look this bean up: it sweeps the bundle's
   * own store ({@link org.streamrune.core.backend.BackendStores#sagaDeadLetterStore}), the instance
   * this method returns, or an application-declared {@code PostgresSagaDeadLetterStore}, so an
   * application's store of another type under this bean's name is never swept.
   */
  @Bean
  @ConditionalOnMissingBean(SagaDeadLetterStore.class)
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public SagaDeadLetterStore sagaDeadLetterStore(org.streamrune.core.backend.BackendStores stores) {
    return stores.sagaDeadLetterStore();
  }

  /**
   * Registers the backend's {@link CommandInbox} ({@link
   * org.streamrune.core.backend.BackendStores#commandInbox}) when DataSource is available.
   * Applications that supply their own {@link CommandInbox} bean will suppress this default
   * (guarded on the interface type via {@link ConditionalOnMissingBean#value()} so a user-supplied
   * interface-typed bean still overrides it).
   *
   * <p>The bean is declared with the interface type. {@link #streamRuneInboxRetentionSweeper}
   * sweeps the bundle's own inbox ({@link org.streamrune.core.backend.BackendStores#commandInbox}),
   * the instance this method returns, or an application-declared {@code PostgresCommandInbox}; an
   * application's inbox of another type is never swept.
   *
   * <p><strong>What overriding actually requires.</strong> Because the store must claim the
   * idempotency key in the SAME transaction as the append, the event store is built with this same
   * {@code CommandInbox} bean ({@link #postgresEventStoreFactory}), which the command bus resolves
   * too. A {@code CommandInbox} that is not the backend's own type therefore fails startup there
   * (the backend cannot write it in the append's transaction), unless you also supply your own
   * {@code EventStoreFactory}/{@code EventStore} whose {@code appendWithKey} claims the key
   * atomically.
   */
  @Bean
  @ConditionalOnMissingBean(CommandInbox.class)
  @ConditionalOnBean(DataSource.class)
  public CommandInbox commandInbox(org.streamrune.core.backend.BackendStores stores) {
    return stores.commandInbox();
  }

  /** Registers the backend's audit store when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(AuditStore.class)
  public AuditStore postgresAuditStore(org.streamrune.core.backend.BackendStores stores) {
    return stores.auditStore();
  }

  /** Registers the backend's compliance report query when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(ComplianceReportQuery.class)
  public ComplianceReportQuery complianceReportQuery(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.complianceReportQuery();
  }

  /** Registers the backend's command audit query when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.CommandAuditQuery.class)
  public org.streamrune.core.audit.CommandAuditQuery commandAuditQuery(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.commandAuditQuery();
  }

  /** Registers an audit interceptor when an AuditStore bean is available. */
  @Bean
  @Order(CommandInterceptorOrdering.ORDER_AUDIT)
  @ConditionalOnBean(AuditStore.class)
  @ConditionalOnMissingBean(AuditCommandInterceptor.class)
  public AuditCommandInterceptor auditCommandInterceptor(AuditStore auditStore) {
    return new AuditCommandInterceptor(auditStore);
  }

  /**
   * Registers the backend's event audit store when event audit is enabled; the event store writes
   * it in the transaction of the append ({@link #postgresEventStoreFactory}).
   */
  @Bean
  @ConditionalOnProperty(name = "streamrune.event-audit-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.EventAuditStore.class)
  public org.streamrune.core.audit.EventAuditStore postgresEventAuditStore(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.eventAuditStore();
  }

  /** Registers the backend's event audit query when event audit is enabled. */
  @Bean
  @ConditionalOnProperty(name = "streamrune.event-audit-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.EventAuditQuery.class)
  public org.streamrune.core.audit.EventAuditQuery postgresEventAuditQuery(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.eventAuditQuery();
  }

  /** Registers the backend's projection dead letter store when projection DLQ is enabled. */
  @Bean
  @ConditionalOnProperty(name = "streamrune.projection-dlq-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.ProjectionDeadLetterStore.class)
  public org.streamrune.core.projection.ProjectionDeadLetterStore postgresProjectionDeadLetterStore(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.projectionDeadLetterStore();
  }

  /**
   * Registers a DeadLetterRetryRunner when DeadLetterQueue and CommandBus are available and {@code
   * streamrune.dead-letter.enabled} is not {@code false}. That switch governs only this automatic
   * retry: the command bus keeps dead-lettering into the queue and {@link
   * #streamRuneDeadLetterRetentionSweeper} keeps pruning it, so a deployment that replays through
   * an admin endpoint instead stays bounded.
   *
   * <p>Dead-letter payloads are ALWAYS deserialized with {@link
   * #defaultDeadLetterObjectMapper(CryptoEngine, org.streamrune.core.StreamRuneMetrics)}, never an
   * application-defined {@code com.fasterxml.jackson.databind.ObjectMapper} bean — matching the
   * mapper the auto-configured command bus serializes dead-letter payloads with (the {@link
   * CryptoEngine} bean, if present, is resolved identically in both places — see {@link
   * #resolveCryptoEngine} — so the two mappers stay crypto-aware in lockstep and payloads
   * round-trip).
   *
   * <p><b>Command-type registry:</b> the runner resolves a stored DLQ entry's command class by its
   * fully-qualified class name (see {@code VirtualThreadCommandBus.publishToDeadLetterQueue}), and
   * the registry is keyed by that name. It is seeded here from the same {@link DeciderRegistration}
   * beans the auto-configured {@link #virtualThreadCommandBus} already consumes, so every command
   * type registered with the bus is also resolvable by the runner — without this, the registry
   * would be empty and every retry would be permanently unresolvable. {@link
   * org.streamrune.runtime.DeadLetterRetryRunner.Builder#registerCommand(Class)} is available for
   * command types published to the DLQ without a decider registration bean. A registration whose
   * command type is a sealed root (the usual {@code OrderCommand.class}) makes every permitted
   * command resolvable, since {@code registerCommand} expands a sealed hierarchy and the bus
   * persists the concrete class; one registered by a non-sealed supertype cannot be expanded, so
   * its concrete commands replay only from a runner the application builds and registers them on.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist @Encrypted DLQ payloads as
   *     plaintext
   */
  @Bean
  @ConditionalOnMissingBean(DeadLetterRetryRunner.class)
  @ConditionalOnBean({
    org.streamrune.core.DeadLetterQueue.class,
    org.streamrune.core.CommandBus.class
  })
  @ConditionalOnProperty(
      name = "streamrune.dead-letter.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public DeadLetterRetryRunner streamRuneDeadLetterRetryRunner(
      org.streamrune.core.DeadLetterQueue deadLetterQueue,
      VirtualThreadCommandBus commandBus,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<DeciderRegistration<?, ?, ?>> registrationProvider,
      ObjectProvider<org.streamrune.core.subscription.SubscriptionLeadership> leadershipProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider,
      StreamRuneProperties properties) {
    CryptoEngine cryptoEngine = resolveCryptoEngine(cryptoEngineProvider, "the dead-letter queue");
    // Resolve the metrics bean once: it feeds BOTH retry-exhaustion counters AND the DLQ decrypt
    // mapper's CryptoShreddingModule (subject_redacted); NOOP when no metrics bean exists.
    org.streamrune.core.StreamRuneMetrics metrics =
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP);
    var builder =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(deadLetterQueue)
            .commandBus(commandBus)
            .objectMapper(defaultDeadLetterObjectMapper(cryptoEngine, metrics))
            // Metrics bean (MicrometerStreamRuneMetrics when present) so retry-exhaustion emits the
            // streamrune.dlq.exhausted counter + terminal ERROR log under the default policy;
            // defaults to NOOP when no metrics bean exists (behavior unchanged).
            .metrics(metrics)
            .policy(deadLetterRetryPolicy(properties))
            // The window the inbox retention sweeper prunes with: a keyed entry older than it is
            // refused (STALE_REPLAY_BLOCKED) instead of replayed past its dedup row.
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .pollInterval(properties.deadLetter().retryInterval());

    registrationProvider
        .orderedStream()
        // registerCommand keys by fully-qualified class name, so replay resolves by FQN and
        // two commands sharing a simple name across bounded contexts can never collide in the map.
        .forEach(reg -> builder.registerCommand(reg.commandType()));

    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on) into the DLQ retry runner so only the leader
    // replica polls/retries; SubscriptionLeadership.NOOP (always leader) when no bean exists.
    // Without this, two replicas' staggered polls both re-execute the same DLQ command (duplicate
    // domain events) whenever no CommandInbox is present to de-dup them. Two leadership beans with
    // none @Primary fail here, as on Quarkus and Micronaut.
    builder.leadership(LeadershipBeans.resolve(leadershipProvider));

    DeadLetterRetryRunner runner = builder.build();
    // Register for relay liveness health so /health degrades if this runner's thread
    // dies.
    relayHealthProvider.ifAvailable(c -> c.registerDlqRetryRunner(runner));
    return runner;
  }

  /**
   * The mapper for dead-letter command payloads, used by both the auto-configured command bus
   * (serialization) and {@link DeadLetterRetryRunner} (deserialization). Delegates to {@link
   * DeadLetterRetryRunner#createObjectMapper(CryptoEngine)}: {@code JavaTimeModule} always
   * registered so {@code java.time} fields round-trip, plus {@code CryptoShreddingModule} when
   * {@code cryptoEngine} is non-null so {@code @Encrypted} command fields are persisted as
   * ciphertext instead of plaintext PII, and fall within GDPR forget scope.
   *
   * <p>This mapper is ALWAYS used for the DLQ, mirroring the event store, saga store, and the
   * Micronaut integration — it is never replaced by an application-defined {@code
   * com.fasterxml.jackson.databind.ObjectMapper} bean. That guarantee is what keeps
   * {@code @Encrypted} command payloads from ever being persisted to the DLQ as plaintext PII,
   * regardless of any Jackson mapper the application registers for its own use.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} to build a crypto-blind
   *     mapper
   * @param metrics the metrics collector threaded into {@code CryptoShreddingModule} so a redaction
   *     on the DLQ decrypt path emits subject_redacted; NOOP disables the signal
   */
  static com.fasterxml.jackson.databind.ObjectMapper defaultDeadLetterObjectMapper(
      CryptoEngine cryptoEngine, org.streamrune.core.StreamRuneMetrics metrics) {
    return DeadLetterRetryRunner.createObjectMapper(cryptoEngine, metrics);
  }

  /**
   * Starts the auto-configured {@link DeadLetterRetryRunner} on context refresh and stops it on
   * context close. Conditional on the auto-configured bean name so user-defined runners (which the
   * user starts and stops themselves) are never started twice.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneDeadLetterRetryRunner")
  public RunnerLifecycle streamRuneDeadLetterRetryRunnerLifecycle(DeadLetterRetryRunner runner) {
    return new RunnerLifecycle("DeadLetterRetryRunner", runner::start, runner::close);
  }

  /**
   * Registers a {@link DeadLetterRetentionSweeper} that prunes command dead-letter-queue entries
   * older than {@code streamrune.dead-letter.retention-max-age} (default 7 days). A DLQ entry holds
   * the full command payload (encrypted where {@code @Encrypted} fields apply per) — this bounds
   * how long that payload remains queryable, a GDPR storage-limitation concern. Created for every
   * {@link org.streamrune.core.DeadLetterQueue} bean — the framework registers none itself, so that
   * is always one the application supplied — because the command bus dead-letters into whichever
   * queue bean exists. Unlike {@link #streamRuneDeadLetterRetryRunner} it is therefore <em>not</em>
   * gated on {@code streamrune.dead-letter.enabled}, which switches only the automatic retry: an
   * operator who turns replays off and relies on the admin retry would otherwise keep every
   * exhausted command's payload for ever. A zero or negative {@code retention-max-age} is the
   * switch that turns pruning off (the sweeper then logs that at start). Gated on the generic
   * interface rather than a Postgres-specific type — an implementation that does not support
   * retention pruning throws {@code UnsupportedOperationException} from {@code deleteOlderThan}, so
   * the sweeper fails loudly (not silently) on its first cycle. An application {@link
   * DeadLetterRetentionSweeper} bean replaces it. The {@link org.streamrune.core.StreamRuneMetrics}
   * bean is wired in when present so {@code streamrune.dlq.swept_rows} is recorded; falls back to
   * {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(DeadLetterRetentionSweeper.class)
  @ConditionalOnBean(org.streamrune.core.DeadLetterQueue.class)
  public DeadLetterRetentionSweeper streamRuneDeadLetterRetentionSweeper(
      org.streamrune.core.DeadLetterQueue deadLetterQueue,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider) {
    var sweeper =
        new DeadLetterRetentionSweeper(
            deadLetterQueue,
            properties.deadLetter().retentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // Register for relay liveness health so /health reports DOWN if the sweep thread
    // dies, exactly like the outbox poller and the DLQ retry runner.
    relayHealthProvider.ifAvailable(c -> c.registerRetentionSweeper(sweeper));
    return sweeper;
  }

  /**
   * Starts the auto-configured {@link DeadLetterRetentionSweeper} on context refresh and stops it
   * on context close. Mirrors the saga dead-letter retention sweeper lifecycle pattern.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneDeadLetterRetentionSweeper")
  public RunnerLifecycle streamRuneDeadLetterRetentionSweeperLifecycle(
      DeadLetterRetentionSweeper sweeper) {
    return new RunnerLifecycle("DeadLetterRetentionSweeper", sweeper::start, sweeper::close);
  }

  /**
   * Registers an OutboxPoller when outbox is enabled and both OutboxStore and OutboxPublisher are
   * available.
   */
  @Bean
  @ConditionalOnMissingBean(OutboxPoller.class)
  @ConditionalOnBean({
    org.streamrune.core.outbox.OutboxStore.class,
    org.streamrune.core.outbox.OutboxPublisher.class
  })
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public OutboxPoller streamRuneOutboxPoller(
      org.streamrune.core.outbox.OutboxStore outboxStore,
      org.streamrune.core.outbox.OutboxPublisher publisher,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {
    // Wire the metrics bean so streamrune.outbox.delivery_failed fires on a terminal FAILED entry
    // (the documented alert metric) and streamrune.outbox.pending tracks the backlog; falls back to
    // NOOP when no metrics bean exists (behavior unchanged).
    OutboxPoller poller =
        OutboxPoller.builder()
            .outboxStore(outboxStore)
            .publisher(publisher)
            .batchSize(properties.outbox().batchSize())
            .pollInterval(properties.outbox().flushInterval())
            // Expose the retry ladder so an operator can widen it; transport failures are
            // non-counting regardless (see OutboxPoller.recordFailure).
            .retryPolicy(
                new RetryPolicy(
                    properties.outbox().retryMaxAttempts(),
                    properties.outbox().retryInitialDelay(),
                    properties.outbox().retryBackoffMultiplier()))
            .metrics(
                metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP))
            .tracing(tracingOf(tracingProvider))
            .build();
    // Register for relay liveness health so /health degrades if the relay thread dies.
    relayHealthProvider.ifAvailable(c -> c.registerOutboxPoller(poller));
    return poller;
  }

  /**
   * Starts the auto-configured {@link OutboxPoller} on context refresh and stops it on context
   * close. Conditional on the auto-configured bean name so user-defined pollers (which the user
   * starts and stops themselves) are never started twice.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneOutboxPoller")
  public RunnerLifecycle streamRuneOutboxPollerLifecycle(OutboxPoller poller) {
    return new RunnerLifecycle("OutboxPoller", poller::start, poller::close);
  }

  /**
   * Registers an {@link OutboxRetentionSweeper} with two independent windows: it prunes {@code
   * DELIVERED} outbox entries older than {@code streamrune.outbox.retention-max-age} (default 7
   * days) and operator-skipped {@code SKIPPED} entries older than {@code
   * streamrune.outbox.skipped-retention-max-age} (default 30 days — the audit of the skip
   * decision). Zero or negative disables either window. {@code FAILED} entries are never pruned: an
   * operator resolves them through {@link org.streamrune.runtime.OutboxFailedReplayer}. Only
   * created when the outbox is enabled and an {@link org.streamrune.core.outbox.OutboxStore} bean
   * is present: the framework declares none, so it is always the application's, typically built
   * with {@link org.streamrune.core.backend.BackendStores#outboxStore}. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.outbox.swept_rows} and {@code streamrune.outbox.skipped_swept} are recorded; falls
   * back to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(OutboxRetentionSweeper.class)
  @ConditionalOnBean(org.streamrune.core.outbox.OutboxStore.class)
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public OutboxRetentionSweeper streamRuneOutboxRetentionSweeper(
      org.streamrune.core.outbox.OutboxStore outboxStore,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider) {
    var sweeper =
        new OutboxRetentionSweeper(
            outboxStore,
            properties.outbox().retentionMaxAge(),
            properties.outbox().skippedRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // Register for relay liveness health so /health reports DOWN if the sweep thread
    // dies, exactly like the outbox poller and the DLQ retry runner.
    relayHealthProvider.ifAvailable(c -> c.registerRetentionSweeper(sweeper));
    return sweeper;
  }

  /**
   * Starts the auto-configured {@link OutboxRetentionSweeper} on context refresh and stops it on
   * context close. Mirrors the outbox poller lifecycle pattern.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneOutboxRetentionSweeper")
  public RunnerLifecycle streamRuneOutboxRetentionSweeperLifecycle(OutboxRetentionSweeper sweeper) {
    return new RunnerLifecycle("OutboxRetentionSweeper", sweeper::start, sweeper::close);
  }

  /**
   * Registers an {@link org.streamrune.runtime.OutboxFailedReplayer} beside the outbox poller for
   * operator-driven recovery of terminal {@code FAILED} outbox entries: {@code replay(id)} resets
   * one entry to {@code PENDING} in its original position (same {@code seq}), {@code
   * replayFailed(int)} does so oldest-first in bulk, {@code skip(id, by, reason)} moves one entry
   * to the audited {@code SKIPPED} state (releasing its aggregate on a strict channel), and {@code
   * skipFailed(max, by, reason)} is the bulk mirror of {@code replayFailed}. Produced when the
   * outbox is enabled and an {@link org.streamrune.core.outbox.OutboxStore} bean is present; it is
   * <em>not</em> a runner (no {@link RunnerLifecycle}) — the application invokes it from an admin
   * endpoint. The {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when present so
   * {@code streamrune.outbox.replayed} and {@code streamrune.outbox.skipped} are recorded; falls
   * back to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.runtime.OutboxFailedReplayer.class)
  @ConditionalOnBean(org.streamrune.core.outbox.OutboxStore.class)
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public org.streamrune.runtime.OutboxFailedReplayer streamRuneOutboxFailedReplayer(
      org.streamrune.core.outbox.OutboxStore outboxStore,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    return new org.streamrune.runtime.OutboxFailedReplayer(
        outboxStore,
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Registers the backend's per-instance leadership ({@link
   * org.streamrune.core.backend.BackendStores#leadership}; on Postgres a {@link
   * org.streamrune.postgres.LeaseBasedLeadership}) — a DB-clocked, persisted lease with a monotonic
   * fencing {@code epoch} — so that, with more than one replica of the same projection/subscription
   * running, at most one replica is the active consumer and the others sit in standby. Leadership
   * is a single atomic upsert over {@code subscription_leases} decided by the database clock, so it
   * is correct under any connection pooling (no session state, no dedicated connection held across
   * calls); the leader's monotonic epoch is threaded into the projection commit and rejected
   * downstream if a superseded leader ever tries to write. Produced when a {@link DataSource} is
   * present and {@code streamrune.subscription.single-active-consumer.enabled} reads as on (default
   * on) — through {@link SingleActiveConsumerEnabledCondition}, the same reading the saga forward
   * fence's {@code auto} follows. The lease TTL is {@code
   * streamrune.subscription.single-active-consumer.lease-ttl} (default 15s) — the failover bound;
   * the renew heartbeat runs at {@code ttl/3}. When absent or disabled the projection runners fall
   * back to {@link org.streamrune.core.subscription.SubscriptionLeadership#NOOP} (always leader —
   * single-instance behavior unchanged); see {@link ProjectionAutoConfig}, which resolves this bean
   * via {@code ObjectProvider} and threads it into the runner builders.
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.subscription.SubscriptionLeadership.class)
  @org.springframework.context.annotation.Conditional(SingleActiveConsumerEnabledCondition.class)
  public org.streamrune.core.subscription.SubscriptionLeadership streamRuneSubscriptionLeadership(
      org.streamrune.core.backend.BackendStores stores, StreamRuneProperties properties) {
    // Memoized by the bundle, which closes it when it is closed; the lifecycle below closes it
    // earlier, in the runners' shutdown phase. Closing twice is harmless (idempotent).
    return stores.leadership(properties.subscription().singleActiveConsumer().leaseTtl());
  }

  /**
   * Matches when {@code streamrune.subscription.single-active-consumer.enabled} reads as on through
   * {@link SingleActiveConsumerSetting#enabled}, the parser {@link
   * SagaEventPathRetentionConfigurer} reads it through for the forward fence's {@code auto}. The
   * value is bound with the same relaxed binding as {@link StreamRuneProperties}, so the condition
   * and the bound value see the same text. {@code @ConditionalOnProperty(havingValue = "true")}
   * matched the literal {@code true} only: {@code on}, {@code yes} or {@code 1} left this bean out
   * while the fence read them as on. An unknown spelling throws, so the context does not start.
   * Unset, the property follows {@code streamrune.backend}: on for {@code postgres}, off for {@code
   * sqlite}, where an explicit on fails the start (SQLite is single-instance).
   */
  static final class SingleActiveConsumerEnabledCondition
      implements org.springframework.context.annotation.Condition {
    @Override
    public boolean matches(
        org.springframework.context.annotation.ConditionContext context,
        org.springframework.core.type.AnnotatedTypeMetadata metadata) {
      var binder =
          org.springframework.boot.context.properties.bind.Binder.get(context.getEnvironment());
      String value = binder.bind(SingleActiveConsumerSetting.PROPERTY, String.class).orElse(null);
      String backend =
          org.streamrune.integration.BackendSelection.parse(
              binder
                  .bind(org.streamrune.integration.BackendSelection.PROPERTY, String.class)
                  .orElse(null));
      return SingleActiveConsumerSetting.enabled(value, backend);
    }
  }

  /**
   * Stops the {@link #streamRuneSubscriptionLeadership} renew heartbeat and resigns its held leases
   * on context close. Uses a {@link RunnerLifecycle} at the same {@link RunnerLifecycle#PHASE}
   * ({@code DEFAULT_PHASE - 2048}) as the projection runners — the same shutdown phase as the
   * runner lifecycles, not a dependency-enforced ordering after them. A runner that observes lost
   * leadership while stopping simply stands by, which is benign (all {@code SmartLifecycle} beans
   * still stop before singleton destruction, so this runs before the {@link DataSource} closes
   * regardless). The start action is a no-op — leadership is acquired lazily by the runners; only
   * close needs sequencing. Conditional on the auto-configured bean so a user-supplied leadership
   * bean (which the user owns) is never closed by the framework.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneSubscriptionLeadership")
  public RunnerLifecycle streamRuneSubscriptionLeadershipLifecycle(
      org.streamrune.core.subscription.SubscriptionLeadership leadership) {
    return new RunnerLifecycle("SubscriptionLeadership", () -> {}, leadership::close);
  }

  /**
   * Registers an {@link InboxRetentionSweeper} that prunes command-inbox rows older than {@code
   * streamrune.inbox.retention-max-age} (default 7 days). It sweeps an application-declared {@code
   * PostgresCommandInbox} bean when there is one, and otherwise the bundle's own inbox, {@link
   * org.streamrune.core.backend.BackendStores#commandInbox()} — never an application inbox of
   * another type: the framework prunes only what it built or what is declared as its Postgres
   * store. The {@link CommandInbox} bean is not consulted, since an application's inbox of another
   * type wins that bean. The retention window exceeds typical broker redelivery and subscription
   * replay horizons so a pruned key cannot re-admit a duplicate. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.inbox.swept_rows} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(InboxRetentionSweeper.class)
  @org.springframework.context.annotation.Conditional(BackendStoresOrPostgresCommandInbox.class)
  public InboxRetentionSweeper streamRuneInboxRetentionSweeper(
      ObjectProvider<org.streamrune.postgres.PostgresCommandInbox> applicationInboxProvider,
      ObjectProvider<org.streamrune.core.backend.BackendStores> storesProvider,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider) {
    // No framework bean is declared with the Postgres type, so this provider finds an application's
    // own Postgres inbox (or, once created, the bundle's same instance); otherwise the bundle's.
    CommandInbox commandInbox = applicationInboxProvider.getIfAvailable();
    if (commandInbox == null) {
      commandInbox = storesProvider.getObject().commandInbox();
    }
    var sweeper =
        new InboxRetentionSweeper(
            commandInbox,
            properties.inbox().retentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // Register for relay liveness health so /health reports DOWN if the sweep thread
    // dies, exactly like the outbox poller and the DLQ retry runner.
    relayHealthProvider.ifAvailable(c -> c.registerRetentionSweeper(sweeper));
    return sweeper;
  }

  /**
   * Matches when the backend's stores exist (with a {@code DataSource}) or the application declares
   * a {@code PostgresCommandInbox} bean: the inboxes {@link #streamRuneInboxRetentionSweeper}
   * sweeps.
   */
  static final class BackendStoresOrPostgresCommandInbox
      extends org.springframework.boot.autoconfigure.condition.AnyNestedCondition {
    BackendStoresOrPostgresCommandInbox() {
      super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnBean(org.streamrune.core.backend.BackendStores.class)
    static final class Backend {}

    @ConditionalOnBean(org.streamrune.postgres.PostgresCommandInbox.class)
    static final class ApplicationDeclared {}
  }

  /**
   * Matches when the backend's stores exist (with a {@code DataSource}) or the application declares
   * a {@code PostgresSagaDeadLetterStore} bean: the stores {@link
   * #streamRuneSagaDeadLetterRetentionSweeper} sweeps.
   */
  static final class BackendStoresOrPostgresSagaDeadLetterStore
      extends org.springframework.boot.autoconfigure.condition.AnyNestedCondition {
    BackendStoresOrPostgresSagaDeadLetterStore() {
      super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnBean(org.streamrune.core.backend.BackendStores.class)
    static final class Backend {}

    @ConditionalOnBean(org.streamrune.postgres.PostgresSagaDeadLetterStore.class)
    static final class ApplicationDeclared {}
  }

  /**
   * Starts the auto-configured {@link InboxRetentionSweeper} on context refresh and stops it on
   * context close. Mirrors the outbox retention sweeper lifecycle pattern.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneInboxRetentionSweeper")
  public RunnerLifecycle streamRuneInboxRetentionSweeperLifecycle(InboxRetentionSweeper sweeper) {
    return new RunnerLifecycle("InboxRetentionSweeper", sweeper::start, sweeper::close);
  }

  /**
   * Registers a {@link org.streamrune.runtime.SagaDeadLetterRetentionSweeper} that prunes
   * quarantined saga dead-letter entries older than {@code
   * streamrune.saga.dead-letter-retention-max-age} (default 7 days, and must not exceed {@code
   * streamrune.inbox.retention-max-age} — see {@link
   * org.streamrune.integration.SagaRetentionValidator}). Only created when sagas are enabled. It
   * sweeps an application-declared {@code PostgresSagaDeadLetterStore} bean when there is one, and
   * otherwise the bundle's own store, {@link
   * org.streamrune.core.backend.BackendStores#sagaDeadLetterStore()} — never an application store
   * of another type, which wins the {@link SagaDeadLetterStore} bean and so is not consulted. The
   * {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.saga.dead_letters_swept} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.runtime.SagaDeadLetterRetentionSweeper.class)
  @org.springframework.context.annotation.Conditional(
      BackendStoresOrPostgresSagaDeadLetterStore.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public org.streamrune.runtime.SagaDeadLetterRetentionSweeper
      streamRuneSagaDeadLetterRetentionSweeper(
          ObjectProvider<org.streamrune.postgres.PostgresSagaDeadLetterStore>
              applicationStoreProvider,
          ObjectProvider<org.streamrune.core.backend.BackendStores> storesProvider,
          StreamRuneProperties properties,
          ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
          ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor>
              relayHealthProvider) {
    // As for the inbox sweeper: an application's own Postgres store, otherwise the bundle's.
    SagaDeadLetterStore sagaDeadLetterStore = applicationStoreProvider.getIfAvailable();
    if (sagaDeadLetterStore == null) {
      sagaDeadLetterStore = storesProvider.getObject().sagaDeadLetterStore();
    }
    var sweeper =
        new org.streamrune.runtime.SagaDeadLetterRetentionSweeper(
            sagaDeadLetterStore,
            properties.saga().deadLetterRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // Register for relay liveness health so /health reports DOWN if the sweep thread
    // dies, exactly like the outbox poller and the DLQ retry runner.
    relayHealthProvider.ifAvailable(c -> c.registerRetentionSweeper(sweeper));
    return sweeper;
  }

  /**
   * Starts the auto-configured {@link org.streamrune.runtime.SagaDeadLetterRetentionSweeper} on
   * context refresh and stops it on context close. Mirrors the inbox retention sweeper lifecycle
   * pattern.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneSagaDeadLetterRetentionSweeper")
  public RunnerLifecycle streamRuneSagaDeadLetterRetentionSweeperLifecycle(
      org.streamrune.runtime.SagaDeadLetterRetentionSweeper sweeper) {
    return new RunnerLifecycle("SagaDeadLetterRetentionSweeper", sweeper::start, sweeper::close);
  }

  /**
   * Auto-wires a {@link org.streamrune.runtime.SagaCompensationRetrySweeper} for every application
   * {@link org.streamrune.runtime.SagaRunner} bean. This is the durable, automatic recovery path
   * for a stuck {@code COMPENSATING} saga that has no {@link
   * org.streamrune.runtime.SagaTimeoutRunner} (empty {@code timeout()}) and receives no further
   * correlated event: without it, a transient compensation failure wedges the saga {@code
   * COMPENSATING} forever with the undo abandoned. Gated on the presence of at least one {@code
   * SagaRunner} bean plus a {@link SagaStore} — mirroring how the dead-letter retry runner is gated
   * on a {@code DeadLetterQueue} bean. {@code streamrune.saga.compensation-retry-enabled} (default
   * on) does not withhold this bean: the sweepers are the sole emitters of {@code
   * streamrune.saga.compensating} / {@code streamrune.saga.faulted_rows}, so the knob is threaded
   * into each sweeper as sampling-only mode instead — the re-drive stops, the gauges stay live. The
   * shared {@link org.streamrune.core.subscription.SubscriptionLeadership} bean is threaded in so
   * each per-saga-type sweeper runs single-active across replicas; falls back to always-leader
   * (single instance) when no leadership bean exists.
   *
   * <p><strong>Kill switch:</strong> also gated on the saga kill switch {@code
   * streamrune.saga.enabled}, matching Quarkus ({@code
   * SagaCompensationRetryLifecycle.startSweepers()} returns early on it) and Micronaut (class-level
   * {@code @Requires}). The {@code @ConditionalOnBean(SagaStore.class)} gate implemented the kill
   * switch only INDIRECTLY — by suppressing the framework's own {@link #sagaStore} bean — so an
   * application that supplies its own {@code SagaStore} (custom schema/store) satisfied the
   * condition anyway and the sweepers kept re-driving {@code COMPENSATING} sagas, issuing real
   * compensation commands, after the operator flipped the documented kill switch during an
   * incident.
   */
  @Bean
  @ConditionalOnMissingBean(SagaCompensationRetryLifecycle.class)
  @ConditionalOnBean({org.streamrune.runtime.SagaRunner.class, SagaStore.class})
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public SagaCompensationRetryLifecycle streamRuneSagaCompensationRetryLifecycle(
      ObjectProvider<org.streamrune.runtime.SagaRunner<?>> sagaRunners,
      SagaStore sagaStore,
      ObjectProvider<org.streamrune.core.subscription.SubscriptionLeadership> leadershipProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider,
      StreamRuneProperties properties) {
    return new SagaCompensationRetryLifecycle(
        sagaRunners,
        sagaStore,
        leadershipProvider,
        metricsProvider,
        relayHealthProvider,
        properties);
  }

  /**
   * Registers the forget channel of the {@link org.streamrune.core.crypto.CryptoEngine} bean, when
   * it listens for peers' forgets, with the {@link
   * org.streamrune.runtime.BackgroundRelayHealthContributor} as {@code crypto-forget-listen}. An
   * application without such an engine registers nothing.
   */
  @Bean
  @ConditionalOnMissingBean(CryptoForgetListenHealthRegistrar.class)
  @ConditionalOnBean(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
  public CryptoForgetListenHealthRegistrar streamRuneCryptoForgetListenHealthRegistrar(
      ObjectProvider<org.streamrune.core.crypto.CryptoEngine> cryptoEngines,
      org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return new CryptoForgetListenHealthRegistrar(cryptoEngines, relayHealth);
  }

  /**
   * Registers every application-declared {@link org.streamrune.runtime.SagaTimeoutRunner} bean with
   * the {@link org.streamrune.runtime.BackgroundRelayHealthContributor} for liveness, from a path
   * that is NOT gated on {@code streamrune.saga.compensation-retry-enabled}. When the compensation
   * re-drive is switched off (sampling-only sweeper) a {@code SagaTimeoutRunner} is the sole
   * compensation driver, so its dead-thread signal must survive independently of the sweeper.
   * Present whenever a {@code SagaTimeoutRunner} and the health contributor both exist.
   */
  @Bean
  @ConditionalOnMissingBean(SagaTimeoutRunnerHealthRegistrar.class)
  @ConditionalOnBean({
    org.streamrune.runtime.SagaTimeoutRunner.class,
    org.streamrune.runtime.BackgroundRelayHealthContributor.class
  })
  public SagaTimeoutRunnerHealthRegistrar streamRuneSagaTimeoutRunnerHealthRegistrar(
      ObjectProvider<org.streamrune.runtime.SagaTimeoutRunner<?>> sagaTimeoutRunners,
      org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return new SagaTimeoutRunnerHealthRegistrar(sagaTimeoutRunners, relayHealth);
  }

  /**
   * The bean name of the default read-model store, {@value}: a framework name, so an application
   * bean named {@code postgres} never collides with it.
   */
  static final String DEFAULT_READ_MODEL_STORE_BEAN =
      org.streamrune.integration.ProjectionStoreRouting.DEFAULT_STORE_BEAN;

  /**
   * The bean name of the event store's subscription checkpoint, {@value}: the {@link
   * org.streamrune.core.projection.CheckpointTarget} an auto-configured projection runner routes an
   * {@code AT_LEAST_ONCE_IDEMPOTENT} registration to when the registration matches no read-model
   * store. Define a bean of that name to replace it.
   */
  static final String SUBSCRIPTION_CHECKPOINT = "streamRuneSubscriptionCheckpoint";

  /**
   * Registers the default read-model store on the application's {@code DataSource}: the backend's
   * ({@link org.streamrune.core.backend.BackendStores#defaultReadModelStore}), named after the
   * backend ({@code postgres} by default; on Postgres a {@link
   * org.streamrune.postgres.PostgresReadModelStore}), whose read models live in {@code <name>_view}
   * tables and whose checkpoints live beside them in {@code readmodel_checkpoint}, so a routed
   * {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} registration's read-model writes and its
   * checkpoint commit in one transaction. The auto-configured projection runners route to it every
   * registration that names it, writes to its repository, or (transactional, with no write target
   * of its own) has no other transactional store to go to.
   *
   * <p>Creating the store (the bundle's first {@code defaultReadModelStore()} call) checks the
   * server version, applies the read-model migration series ({@code db/readmodel-migration}) when
   * {@code streamrune.read-model.schema.auto-initialize} — unset, {@code
   * streamrune.event-store.schema.auto-initialize} — is on, and validates the read-model schema, as
   * the event-store factory does for its own series. The bean is lazy: it is created, and the
   * database touched, when the stores are first selected ({@code ReadModelStoresConfiguration}) —
   * for a projection runner with a registration to route, the {@code ProjectionRepository} bean, or
   * the index declarer.
   *
   * <p>Its repository's ObjectMapper wires the {@link CryptoEngine} bean (if present) through
   * {@code CryptoShreddingModule}, the same way the bundle wires the saga store — so
   * {@code @Encrypted} read-model fields are persisted as ciphertext instead of plaintext PII, and
   * fall within GDPR forget scope.
   *
   * <p>Replaced only by name: an application {@link org.streamrune.core.projection.ReadModelStore}
   * bean whose {@code name()} is the backend's name takes its place, and this bean is then never
   * created; a store of any other name is added beside it. The bean stays defined either way — a
   * store's name is known only once the store exists, so no bean condition can decide — and the
   * stores are selected by {@code ReadModelStoresConfiguration}, which leaves it out by its bean
   * name, {@value #DEFAULT_READ_MODEL_STORE_BEAN}, until it is needed. It is a fallback bean
   * ({@link org.springframework.context.annotation.Fallback}): an unqualified {@code
   * ReadModelStore} injection point of the application resolves to the application's store, as on
   * Quarkus (a default bean) and Micronaut (a secondary bean).
   */
  @Bean(DEFAULT_READ_MODEL_STORE_BEAN)
  @org.springframework.context.annotation.Lazy
  @org.springframework.context.annotation.Fallback
  @ConditionalOnBean(DataSource.class)
  public org.streamrune.core.projection.ReadModelStore postgresReadModelStore(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.defaultReadModelStore();
  }

  /**
   * Registers the repository of the store named after the backend — the default store's, or the
   * application store's that replaced it — as the injectable {@link
   * org.streamrune.core.projection.ProjectionRepository}, so query handlers and {@code
   * BaseProjection} constructors that take a {@code ProjectionRepository} read and write it; other
   * stores' repositories are reached through their {@code ReadModelStore} beans. Lazy, like the
   * store. Backs off when the application defines a {@code ProjectionRepository} bean, and is
   * absent when no read-model store bean exists.
   *
   * <p>Finding the store named after the backend creates the application's store beans (a store's
   * name is known only once it exists) and the default store only when none of them replaced it.
   *
   * @throws IllegalStateException when the stores are invalid, or the application's read-model
   *     stores name none after the backend and there is no default store (no {@code DataSource})
   * @hidden
   */
  @Bean
  @org.springframework.context.annotation.Lazy
  @ConditionalOnBean(org.streamrune.core.projection.ReadModelStore.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.ProjectionRepository.class)
  @org.streamrune.core.Internal
  public org.streamrune.core.projection.ProjectionRepository streamRuneProjectionRepository(
      org.streamrune.integration.ReadModelStores stores) {
    return stores.defaultRepository();
  }

  /**
   * Registers the event store's subscription checkpoint, {@value #SUBSCRIPTION_CHECKPOINT}: the
   * pair an auto-configured projection runner reads and commits an {@code AT_LEAST_ONCE_IDEMPOTENT}
   * registration through when the registration matches no read-model store: the {@code OffsetStore}
   * bean's {@link org.streamrune.core.projection.OffsetStore#subscriptionCheckpoint()}.
   *
   * <ul>
   *   <li>A {@link org.streamrune.postgres.PostgresOffsetStore} (the one auto-configured on the
   *       {@code DataSource}, or one the application defines on any DataSource) pairs itself with a
   *       {@link org.streamrune.postgres.PostgresSubscriptionCheckpointProcessor} on its own
   *       DataSource, over the same {@code subscription_checkpoint} table — it fences the
   *       checkpoint under its row lock and hands the projection no repository, so the projection
   *       writes where it writes (at least once).
   *   <li>Any other offset store keeps the default: itself and {@link
   *       org.streamrune.core.projection.AtomicBatchProcessor#nonAtomicAtLeastOnce()}, which saves
   *       each checkpoint into it. It does not fence, so routing refuses an at-least-once
   *       registration onto it under real leadership.
   * </ul>
   *
   * <p>The {@code OffsetStore} is resolved at instantiation, for the reason {@link
   * #subscriptionHealthContributor} gives. Without one there is no subscription checkpoint, and no
   * bean.
   */
  @Bean(SUBSCRIPTION_CHECKPOINT)
  @ConditionalOnMissingBean(name = SUBSCRIPTION_CHECKPOINT)
  public org.streamrune.core.projection.CheckpointTarget streamRuneSubscriptionCheckpoint(
      ObjectProvider<org.streamrune.core.projection.OffsetStore> offsetStoreProvider) {
    org.streamrune.core.projection.OffsetStore offsetStore = offsetStoreProvider.getIfAvailable();
    if (offsetStore == null) {
      // Spring skips a null @Bean product.
      return null;
    }
    // A PostgresOffsetStore overrides it with the fencing processor built on the DataSource it
    // reads; any other offset store keeps the nonatomic default.
    return offsetStore.subscriptionCheckpoint();
  }

  /**
   * Declares every {@link org.streamrune.core.projection.ProjectionIndex} bean, in bean order, on
   * the store its projection is routed to — the routes the projection runners use ({@link
   * org.streamrune.integration.ProjectionRoutes}, computed once by {@link ProjectionAutoConfig}) —
   * once every singleton exists and before any lifecycle bean (the projection runners) starts. An
   * index whose projection is routed to no store (the subscription checkpoint, an INLINE
   * projection, no registration, or projection discovery off) goes to the application's {@link
   * org.streamrune.core.projection.ProjectionRepository} bean — the repository of the store named
   * named after the backend ({@link #streamRuneProjectionRepository}) or one the application
   * defines. A refused key, an index that cannot be created, or such an index without a repository
   * bean fail the start.
   *
   * @param repositories the projection repository bean, resolved only for an index routed to no
   *     store
   * @param projectionIndexes the index beans
   * @param routes the routes of the projection runners' registrations, resolved only when index
   *     beans exist; absent when projection discovery is off
   * @return the declaring callback
   * @hidden
   */
  @Bean
  @org.streamrune.core.Internal
  public org.springframework.beans.factory.SmartInitializingSingleton
      streamRuneProjectionIndexDeclarer(
          ObjectProvider<org.streamrune.core.projection.ProjectionRepository> repositories,
          ObjectProvider<org.streamrune.core.projection.ProjectionIndex> projectionIndexes,
          ObjectProvider<org.streamrune.integration.ProjectionRoutes> routes) {
    return () -> {
      var indexes = projectionIndexes.orderedStream().toList();
      if (indexes.isEmpty()) {
        return;
      }
      var routed = routes.getIfAvailable(org.streamrune.integration.ProjectionRoutes::none);
      for (var index : indexes) {
        routed
            .indexRepository(
                index,
                () -> {
                  var repository = repositories.getIfAvailable();
                  if (repository == null) {
                    throw new IllegalStateException(
                        "ProjectionIndex bean for projection '"
                            + LogSanitizer.sanitizeForLog(index.projection().value())
                            + "', routed to no read-model store, but no ProjectionRepository bean"
                            + " to declare it on: define one, or a DataSource for the default"
                            + " read-model store");
                  }
                  return repository;
                })
            .declareIndex(index);
      }
    };
  }

  /**
   * Registers the backend's {@link org.streamrune.core.projection.OffsetStore} ({@link
   * org.streamrune.core.backend.BackendStores#offsetStore}; on Postgres a {@link
   * org.streamrune.postgres.PostgresOffsetStore}) when a DataSource is available, so the
   * auto-configured projection runners have somewhere to checkpoint subscription offsets without
   * the application hand-defining one. Mirrors the Quarkus and Micronaut integrations.
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.OffsetStore.class)
  public org.streamrune.core.projection.OffsetStore postgresOffsetStore(
      org.streamrune.core.backend.BackendStores stores) {
    return stores.offsetStore();
  }

  /**
   * Registers a {@link ForgetSubjectService} when a {@link CryptoEngine} bean is available. The
   * audit store, if present, is wired automatically; otherwise audit writes are disabled.
   *
   * <p>All registered {@link org.streamrune.core.gdpr.SubjectDataPurger} beans are collected (in
   * declared order via {@code orderedStream()}) and wired in, mirroring how {@link
   * #exportSubjectDataService} collects its {@link SubjectDataCollector} beans. Without this the
   * auto-configured forget would crypto-shred the key but never purge read-model projections,
   * leaving derived PII behind after a GDPR Article-17 erasure.
   *
   * <p>The {@link CachingQueryBus}, when {@code streamrune.query-cache.enabled=true} provides one,
   * is wired in as the forget's query cache: every forget evicts it once its purgers have run, so a
   * cached answer holding the subject's data is not served after the erasure.
   *
   * <p>One of the purger beans must be a {@link org.streamrune.core.gdpr.SnapshotPurger} — for
   * {@code PostgresEventStore}, a {@code PostgresSnapshotStorePurger} the application declares,
   * since only its domain knows which streams' snapshots hold a subject's data — unless {@code
   * streamrune.gdpr.snapshot-purge=none} states that the application has never stored a snapshot.
   * Otherwise the context does not start ({@link
   * org.streamrune.integration.SnapshotPurgeSetting#withoutSnapshotPurge}).
   */
  @Bean
  @ConditionalOnBean(CryptoEngine.class)
  @ConditionalOnMissingBean
  public ForgetSubjectService forgetSubjectService(
      CryptoEngine cryptoEngine,
      ObjectProvider<AuditStore> auditStoreProvider,
      ObjectProvider<org.streamrune.core.gdpr.SubjectDataPurger> purgerProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<CachingQueryBus> queryCacheProvider,
      StreamRuneProperties properties) {
    var b = ForgetSubjectService.builder().cryptoEngine(cryptoEngine);
    auditStoreProvider.ifAvailable(b::auditStore);
    List<org.streamrune.core.gdpr.SubjectDataPurger> purgers =
        purgerProvider.orderedStream().toList();
    b.purgers(purgers);
    if (org.streamrune.integration.SnapshotPurgeSetting.parse(properties.gdpr().snapshotPurge())
        .withoutSnapshotPurge(purgers)) {
      b.withoutSnapshotPurge();
    }
    // Wire metrics so a failed read-model purge increments streamrune.gdpr.purge_failed.
    b.metrics(metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // A forget evicts the query cache after its last purger, so no cached answer taken before the
    // erasure is served after it. Two CachingQueryBus beans fail here rather than leave one
    // un-evicted.
    queryCacheProvider.ifAvailable(b::queryCache);
    return b.build();
  }

  /**
   * Registers an {@link ExportSubjectDataService} when at least one {@link SubjectDataCollector}
   * bean is available. All collectors are injected in declared order (Spring's default ordering for
   * {@code List<T>} injection).
   */
  @Bean
  @ConditionalOnBean(SubjectDataCollector.class)
  @ConditionalOnMissingBean
  public ExportSubjectDataService exportSubjectDataService(
      List<SubjectDataCollector> collectors, ObjectProvider<AuditStore> auditStoreProvider) {
    var b = ExportSubjectDataService.builder().collectors(collectors);
    auditStoreProvider.ifAvailable(b::auditStore);
    return b.build();
  }

  /** Logs a startup summary line after all StreamRune beans are initialized. */
  @Bean
  @ConditionalOnMissingBean(StreamRuneStartupLogger.class)
  @ConditionalOnProperty(
      name = "streamrune.startup-log",
      havingValue = "true",
      matchIfMissing = true)
  public StreamRuneStartupLogger streamRuneStartupLogger(
      org.springframework.context.ApplicationContext applicationContext) {
    return new StreamRuneStartupLogger(applicationContext);
  }

  /** Validates StreamRune configuration at startup and fails fast on misconfiguration. */
  @Bean
  @ConditionalOnMissingBean(StreamRuneConfigValidator.class)
  public StreamRuneConfigValidator streamRuneConfigValidator(
      StreamRuneProperties properties,
      org.springframework.context.ApplicationContext ctx,
      org.springframework.core.env.Environment env) {
    return new StreamRuneConfigValidator(properties, ctx, env);
  }
}

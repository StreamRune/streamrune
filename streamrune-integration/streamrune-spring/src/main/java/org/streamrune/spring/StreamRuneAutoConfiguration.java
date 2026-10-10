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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
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
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresComplianceReportQuery;
import org.streamrune.postgres.PostgresEventStoreFactory;
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
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.runtime.SseEventPublisher;
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
      VaultCryptoEngineConfiguration.class
    })
@EnableConfigurationProperties(StreamRuneProperties.class)
@ConditionalOnClass(VirtualThreadCommandBus.class)
@ImportRuntimeHints(StreamRuneRuntimeHints.class)
public class StreamRuneAutoConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRuneAutoConfiguration.class);

  // Interceptor chain order. The command bus calls before() in ascending order and
  // after()/onError() in reverse (see CommandInterceptor's lifecycle contract). Values leave
  // gaps so user interceptors can slot between framework ones with @Order; user interceptors
  // without an explicit order default to Ordered.LOWEST_PRECEDENCE and run innermost.
  //
  // These mirror org.streamrune.runtime.CommandInterceptorOrdering — the single source of truth
  // shared with the Quarkus and Micronaut integrations (which sort their container-assembled chain
  // by it), so all three frameworks run the identical, security-relevant order. Referencing the
  // shared constants (rather than restating literals) keeps them from drifting apart.

  /** Outermost interceptor: the OTel span covers the whole pipeline, including rejections. */
  public static final int ORDER_OPEN_TELEMETRY = CommandInterceptorOrdering.ORDER_OPEN_TELEMETRY;

  /** Audits every command attempt, including ones later rejected by authorization/validation. */
  public static final int ORDER_AUDIT = CommandInterceptorOrdering.ORDER_AUDIT;

  /** Policy-based authorization gate ({@link CommandAuthorizationPolicy}). */
  public static final int ORDER_AUTHORIZATION = CommandInterceptorOrdering.ORDER_AUTHORIZATION;

  /** Annotation-based ({@code @RequireRole}/{@code @RequirePermission}) authorization gate. */
  public static final int ORDER_ANNOTATION_AUTHORIZATION =
      CommandInterceptorOrdering.ORDER_ANNOTATION_AUTHORIZATION;

  /** Bean Validation runs after authorization so unauthorized callers cannot probe validation. */
  public static final int ORDER_VALIDATION = CommandInterceptorOrdering.ORDER_VALIDATION;

  /**
   * Innermost gate: registered after authorization/validation so the breaker counts only command
   * execution failures, never client rejections.
   */
  public static final int ORDER_CIRCUIT_BREAKER = CommandInterceptorOrdering.ORDER_CIRCUIT_BREAKER;

  /**
   * Innermost interceptor: {@code after()} runs in reverse order, so inline projections are applied
   * first once events are committed.
   */
  public static final int ORDER_INLINE_PROJECTION =
      CommandInterceptorOrdering.ORDER_INLINE_PROJECTION;

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
     * allowlist is seeded from {@code streamrune.metadata.baggage-allowlist}, on top of the
     * framework-meaningful defaults ({@link
     * org.streamrune.core.BaggageAllowlist#DEFAULT_ALLOWED_KEYS}).
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
   * Creates an EventStoreFactory for PostgreSQL when DataSource is available.
   *
   * <p>Wires the {@link CryptoEngine} bean (if present) into the event store's ObjectMapper via
   * {@code CryptoShreddingModule} so {@code @Encrypted} fields are persisted as ciphertext, and
   * passes all {@link org.streamrune.core.upcasting.EventUpcaster} beans into the store so stored
   * events are upcast on read, and all {@link org.streamrune.core.SnapshotMigration} beans so
   * stored snapshots are upgraded on read.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist PII as plaintext
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(EventStoreFactory.class)
  public EventStoreFactory postgresEventStoreFactory(
      DataSource dataSource,
      EventTypeRegistry typeRegistry,
      StreamRuneProperties properties,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<org.streamrune.core.upcasting.EventUpcaster> upcasterProvider,
      ObjectProvider<org.streamrune.core.SnapshotMigration> snapshotMigrationProvider,
      ObjectProvider<org.streamrune.postgres.PostgresEventAuditStore> eventAuditStoreProvider,
      ObjectProvider<org.streamrune.core.outbox.OutboxEventMapper> outboxEventMapperProvider,
      ObjectProvider<org.streamrune.postgres.PostgresOutboxStore> outboxStoreProvider,
      ObjectProvider<PostgresCommandInbox> commandInboxProvider,
      ObjectProvider<CommandInbox> busCommandInboxProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    var factory =
        new PostgresEventStoreFactory(dataSource, typeRegistry)
            .autoInitializeSchema(properties.eventStore().schema().autoInitialize())
            // Thread the metrics bean into the event store's CryptoShreddingModule so a
            // mass redaction on the replay/projection decrypt path emits subject_redacted; NOOP
            // when no metrics bean exists (behavior unchanged).
            .metrics(
                metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP));
    // The store borrows from the application DataSource as given and bounds its own statements
    // per transaction or per statement; the bound is the one knob it needs from configuration.
    try {
      factory.statementTimeout(properties.eventStore().statementTimeout());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "streamrune.event-store.statement-timeout: "
              + LogSanitizer.sanitizeFreeText(e.getMessage()),
          e);
    }

    CryptoEngine cryptoEngine;
    try {
      cryptoEngine = cryptoEngineProvider.getIfAvailable();
    } catch (org.springframework.beans.factory.NoUniqueBeanDefinitionException e) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Primary — StreamRune cannot "
              + "decide which engine to wire into the event store, and skipping encryption would "
              + "persist @Encrypted fields as PLAINTEXT. Mark exactly one CryptoEngine bean as "
              + "@Primary.",
          e);
    }
    if (cryptoEngine != null) {
      factory.cryptoEngine(cryptoEngine);
    }

    List<org.streamrune.core.upcasting.EventUpcaster> upcasters =
        upcasterProvider.orderedStream().toList();
    if (!upcasters.isEmpty()) {
      factory.upcasters(upcasters);
    }

    // Collect all SnapshotMigration beans (mirrors the EventUpcaster collection above) so a user's
    // standard @Bean SnapshotMigration is threaded into the store's snapshot-upgrade pipeline.
    // Empty list = no migrations (unchanged behavior).
    List<org.streamrune.core.SnapshotMigration> snapshotMigrations =
        snapshotMigrationProvider.orderedStream().toList();
    if (!snapshotMigrations.isEmpty()) {
      factory.snapshotMigrations(snapshotMigrations);
    }

    eventAuditStoreProvider.ifAvailable(factory::eventAuditStore);

    org.streamrune.core.outbox.OutboxEventMapper outboxMapper =
        outboxEventMapperProvider.getIfAvailable();
    org.streamrune.postgres.PostgresOutboxStore outboxStore = outboxStoreProvider.getIfAvailable();
    if (outboxMapper != null && outboxStore != null) {
      factory.outboxStore(outboxStore).outboxEventMapper(outboxMapper);
    } else if (outboxMapper != null || outboxStore != null) {
      LOG.warn(
          "Transactional outbox emission is DISABLED: found {} but not the other. "
              + "Define BOTH an OutboxEventMapper and a PostgresOutboxStore bean to enable it.",
          outboxMapper != null ? "an OutboxEventMapper" : "a PostgresOutboxStore");
    }

    PostgresCommandInbox storeCommandInbox = commandInboxProvider.getIfAvailable();
    if (storeCommandInbox != null) {
      factory.commandInbox(storeCommandInbox);
    }
    // The bus and the store are wired from different injection points and different types
    // (interface vs concrete, per), so a user-supplied CommandInbox can leave the bus
    // with an inbox while THIS factory has none — and every keyed execution then dies inside
    // appendWithKey. Fail fast here rather than on the first keyed command in production. This
    // bean only exists when the framework built the factory (@ConditionalOnMissingBean), so a
    // fully-custom EventStoreFactory + CommandInbox pairing is untouched.
    org.streamrune.integration.CommandInboxWiringValidator.validateFrameworkEventStoreWiring(
        busCommandInboxProvider.getIfAvailable(), storeCommandInbox);

    return factory;
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

  /** Creates a default LocalStripedLocker with configured stripe count. */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.core.AggregateLocker.class)
  public org.streamrune.core.AggregateLocker aggregateLocker(StreamRuneProperties properties) {
    return new LocalStripedLocker(properties.stripeCount());
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
    return new SimpleQueryBus(metricsProvider.getIfAvailable());
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
   * CachingQueryBus (if enabled) → AuditingQueryBus (if AuditStore available).
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
      ObjectProvider<AuditStore> auditStoreProvider) {
    CachingQueryBus caching = cachingBusProvider.getIfAvailable();
    QueryBus bus = caching != null ? caching : simpleBus;
    AuditStore store = auditStoreProvider.getIfAvailable();
    return store != null ? new AuditingQueryBus(bus, store) : bus;
  }

  /**
   * Creates a default VirtualThreadCommandBus. The {@link org.streamrune.core.StreamRuneMetrics}
   * bean (e.g. the auto-configured {@link MicrometerStreamRuneMetrics}) is wired in when present so
   * command and event metrics are actually recorded.
   *
   * <p><b>Interceptor ordering:</b> auto-registered interceptors carry explicit {@link Order}
   * values (the {@code ORDER_*} constants on this class), so the chain is deterministic:
   * OpenTelemetry → Audit → Authorization → AnnotationAuthorization → BeanValidation →
   * CircuitBreaker → InlineProjection. {@code before()} runs in that order; {@code after()} and
   * {@code onError()} in reverse. User interceptors without {@code @Order} run innermost (after all
   * framework interceptors); declare {@code @Order} on the bean to slot in between.
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
   * time via {@link org.streamrune.crypto.CryptoConfigValidator}; if a registered command carries
   * an {@code @Encrypted} field but no {@link CryptoEngine} bean is configured, startup fails
   * instead of silently dead-lettering that PII as plaintext.
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
      ObjectProvider<CommandInbox> commandInboxProvider) {

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
            java.time.Duration.ofMillis(properties.retryInitialDelayMs()),
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

    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(locker)
            .retryPolicy(retryPolicy)
            .snapshotPolicy(
                snapshotPolicyProvider.getIfAvailable(
                    () -> SnapshotPolicy.everyNEvents(properties.snapshotEveryNEvents())))
            .lockTimeout(properties.lockTimeout())
            .stripeCount(properties.stripeCount())
            .maxInFlightAsyncCommands(properties.maxInFlightAsyncCommands())
            .interceptors(interceptors);

    org.streamrune.core.StreamRuneMetrics metrics =
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP);
    builder.metrics(metrics);

    org.streamrune.core.DeadLetterQueue deadLetterQueue = deadLetterQueueProvider.getIfAvailable();
    if (deadLetterQueue != null) {
      // The DLQ publish mapper is ALWAYS the crypto-aware default — never the application's
      // own ObjectMapper bean — so @Encrypted command fields are persisted as ciphertext, mirroring
      // the event store, saga store, and Micronaut. The metrics bean is threaded in so the shared
      // DLQ decrypt mapper's CryptoShreddingModule reports subject_redacted.
      builder
          .deadLetterQueue(deadLetterQueue)
          .objectMapper(defaultDeadLetterObjectMapper(cryptoEngine, metrics));
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
     * Puts a {@link SseHandoverFailureResolver} ahead of Spring MVC's exception resolvers. It
     * releases the stream of a request whose handling failed while Spring MVC was taking the
     * emitter over (a client that reset its connection before the response was written) and
     * resolves nothing itself: every exception goes on to the resolvers after it.
     *
     * @return the Spring MVC configurer that adds the resolver
     */
    @Bean
    public WebMvcConfigurer streamRuneSseHandoverFailureResolver() {
      return SseHandoverFailureResolver.asFirstResolver();
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
   * OpenTelemetry command tracing, isolated in a nested {@code @Configuration} gated by
   * {@code @ConditionalOnClass} on the OTel API — evaluated by class NAME via configuration
   * metadata, so Spring never introspects this class's OTel-typed {@code @Bean} method signature
   * when {@code opentelemetry-api} (a {@code compileOnly}, classpath-optional dependency) is
   * absent. Without this isolation an application that omits {@code opentelemetry-api} would fail
   * context init with a NoClassDefFoundError as Spring reflected on the {@code OpenTelemetry}-typed
   * bean method.
   */
  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(name = "io.opentelemetry.api.OpenTelemetry")
  static class OpenTelemetryConfiguration {

    /** Registers the OTel command interceptor when an OpenTelemetry bean is available. */
    @Bean
    @Order(ORDER_OPEN_TELEMETRY)
    @ConditionalOnBean(io.opentelemetry.api.OpenTelemetry.class)
    @ConditionalOnMissingBean(OpenTelemetryCommandInterceptor.class)
    public OpenTelemetryCommandInterceptor openTelemetryCommandInterceptor(
        io.opentelemetry.api.OpenTelemetry openTelemetry) {
      return new OpenTelemetryCommandInterceptor(openTelemetry);
    }
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
    @Order(ORDER_VALIDATION)
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
  @Order(ORDER_CIRCUIT_BREAKER)
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
  @Order(ORDER_AUTHORIZATION)
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
  @Order(ORDER_ANNOTATION_AUTHORIZATION)
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
      StreamRuneProperties properties) {
    return new SagaEventPathRetentionConfigurer(sagaRunnerProvider, properties);
  }

  /**
   * Registers a PostgreSQL-backed saga store when DataSource is available.
   *
   * <p>Wires the {@link CryptoEngine} bean (if present) into the saga store's ObjectMapper via
   * {@code CryptoShreddingModule}, the same way {@link #postgresEventStoreFactory} wires the event
   * store — so {@code @Encrypted} saga-state fields are persisted as ciphertext instead of
   * plaintext PII, and fall within GDPR forget scope.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist PII as plaintext
   */
  @Bean
  @ConditionalOnMissingBean(SagaStore.class)
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public SagaStore sagaStore(
      DataSource dataSource,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    CryptoEngine cryptoEngine;
    try {
      cryptoEngine = cryptoEngineProvider.getIfAvailable();
    } catch (org.springframework.beans.factory.NoUniqueBeanDefinitionException e) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans exist but none is marked @Primary — StreamRune cannot "
              + "decide which engine to wire into the saga store, and skipping encryption would "
              + "persist @Encrypted saga-state fields as PLAINTEXT. Mark exactly one CryptoEngine "
              + "bean as @Primary.",
          e);
    }
    // Thread the metrics bean into the saga store's CryptoShreddingModule so a redaction
    // on the saga-state decrypt path emits subject_redacted; NOOP when no metrics bean exists.
    org.streamrune.core.StreamRuneMetrics metrics =
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP);
    return new org.streamrune.postgres.PostgresSagaStore(
        dataSource,
        org.streamrune.postgres.PostgresSagaStore.createObjectMapper(cryptoEngine, metrics));
  }

  /**
   * Registers a PostgreSQL-backed saga dead-letter store when DataSource is available. Applications
   * that supply their own {@link SagaDeadLetterStore} bean will suppress this default (guarded on
   * the interface type via {@link ConditionalOnMissingBean#value()} so a user-supplied
   * interface-typed bean still overrides it).
   *
   * <p><strong>Concrete return type:</strong> the declared return type is deliberately the concrete
   * {@link org.streamrune.postgres.PostgresSagaDeadLetterStore}, not the {@link
   * SagaDeadLetterStore} interface. Spring's {@code @ConditionalOnBean} predicts bean types from
   * the factory method's declared return type <em>before</em> the bean is instantiated, so an
   * interface-typed bean method leaves concrete-typed gates and injection points — {@link
   * #streamRuneSagaDeadLetterRetentionSweeper} asks for
   * {@code @ConditionalOnBean(PostgresSagaDeadLetterStore.class)} and takes a concrete parameter —
   * permanently unsatisfied even though a bean exists. That silently dropped the retention sweeper
   * from every default deployment and let {@code saga_dead_letters} grow without bound (a GDPR
   * storage-limitation violation), while the boot validators, which check this store by interface
   * type, still passed. Same defect class as {@link #commandInbox}. The concrete class still
   * implements {@link SagaDeadLetterStore}, so interface-typed consumers keep resolving this bean.
   */
  @Bean
  @ConditionalOnMissingBean(SagaDeadLetterStore.class)
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public org.streamrune.postgres.PostgresSagaDeadLetterStore sagaDeadLetterStore(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresSagaDeadLetterStore(dataSource);
  }

  /**
   * Registers a PostgreSQL-backed {@link CommandInbox} when DataSource is available. Applications
   * that supply their own {@link CommandInbox} bean will suppress this default (guarded on the
   * interface type via {@link ConditionalOnMissingBean#value()} so a user-supplied interface-typed
   * bean still overrides it).
   *
   * <p>The declared return type is deliberately the concrete {@link PostgresCommandInbox}, not the
   * {@link CommandInbox} interface: Spring's {@link ObjectProvider}/{@code @ConditionalOnBean}
   * resolution for concrete-typed injection points (e.g. {@link #postgresEventStoreFactory} and
   * {@link #streamRuneInboxRetentionSweeper}) predicts bean types from the factory method's
   * declared return type, so an interface-typed bean method leaves those concrete-typed points
   * permanently unsatisfied even though a bean exists. The concrete class still implements {@link
   * CommandInbox}, so interface-typed consumers (e.g. {@link VirtualThreadCommandBus}'s {@code
   * ObjectProvider<CommandInbox>} parameter) continue to resolve this bean too.
   *
   * <p><strong>What overriding actually requires.</strong> Because the store must claim the
   * idempotency key in the SAME transaction as the append, a custom inbox is only honoured
   * end-to-end if the event store knows about it. Supplying a {@code CommandInbox} that is not a
   * {@link PostgresCommandInbox} therefore also requires supplying your own {@code
   * EventStoreFactory}/{@code EventStore} whose {@code appendWithKey} claims the key atomically;
   * {@link org.streamrune.integration.CommandInboxWiringValidator} fails startup on the mismatched
   * half-override rather than letting every keyed execution break at runtime.
   */
  @Bean
  @ConditionalOnMissingBean(CommandInbox.class)
  @ConditionalOnBean(DataSource.class)
  public PostgresCommandInbox commandInbox(DataSource dataSource) {
    return new PostgresCommandInbox(dataSource);
  }

  /** Registers a PostgreSQL-backed audit store when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(AuditStore.class)
  public AuditStore postgresAuditStore(DataSource dataSource) {
    return new PostgresAuditStore(dataSource);
  }

  /** Registers a PostgreSQL-backed compliance report query when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(ComplianceReportQuery.class)
  public ComplianceReportQuery complianceReportQuery(DataSource dataSource) {
    return new PostgresComplianceReportQuery(dataSource);
  }

  /** Registers a PostgreSQL-backed command audit query when DataSource is available. */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.CommandAuditQuery.class)
  public org.streamrune.core.audit.CommandAuditQuery commandAuditQuery(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresCommandAuditQuery(dataSource);
  }

  /** Registers an audit interceptor when an AuditStore bean is available. */
  @Bean
  @Order(ORDER_AUDIT)
  @ConditionalOnBean(AuditStore.class)
  @ConditionalOnMissingBean(AuditCommandInterceptor.class)
  public AuditCommandInterceptor auditCommandInterceptor(AuditStore auditStore) {
    return new AuditCommandInterceptor(auditStore);
  }

  /** Registers a PostgreSQL-backed event audit store when event audit is enabled. */
  @Bean
  @ConditionalOnProperty(name = "streamrune.event-audit-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.EventAuditStore.class)
  public org.streamrune.postgres.PostgresEventAuditStore postgresEventAuditStore(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresEventAuditStore(dataSource);
  }

  /** Registers a PostgreSQL-backed event audit query when event audit is enabled. */
  @Bean
  @ConditionalOnProperty(name = "streamrune.event-audit-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.audit.EventAuditQuery.class)
  public org.streamrune.postgres.PostgresEventAuditQuery postgresEventAuditQuery(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresEventAuditQuery(dataSource);
  }

  /** Registers a PostgreSQL-backed projection dead letter store when projection DLQ is enabled. */
  @Bean
  @ConditionalOnProperty(name = "streamrune.projection-dlq-enabled", havingValue = "true")
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.ProjectionDeadLetterStore.class)
  public org.streamrune.postgres.PostgresProjectionDeadLetterStore
      postgresProjectionDeadLetterStore(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresProjectionDeadLetterStore(dataSource);
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
            .policy(
                new org.streamrune.core.DeadLetterRetryPolicy(
                    properties.deadLetter().maxRetries(),
                    java.time.Duration.ofMillis(properties.deadLetter().retryIntervalMs()),
                    2.0,
                    false))
            .pollInterval(java.time.Duration.ofMillis(properties.deadLetter().retryIntervalMs()));

    registrationProvider
        .orderedStream()
        // registerCommand keys by fully-qualified class name, so replay resolves by FQN and
        // two commands sharing a simple name across bounded contexts can never collide in the map.
        .forEach(reg -> builder.registerCommand(reg.commandType()));

    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on) into the DLQ retry runner so only the leader
    // replica polls/retries; defaults to SubscriptionLeadership.NOOP (always leader) when no bean
    // exists. Without this, two replicas' staggered polls both re-execute the same DLQ command
    // (duplicate domain events) whenever no CommandInbox is present to de-dup them.
    leadershipProvider.ifAvailable(builder::leadership);

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
   * older than {@code streamrune.dead-letter.retention-max-age} (default 30 days). A DLQ entry
   * holds the full command payload (encrypted where {@code @Encrypted} fields apply per) — this
   * bounds how long that payload remains queryable, a GDPR storage-limitation concern. Created for
   * every {@link org.streamrune.core.DeadLetterQueue} bean — the framework registers none itself,
   * so that is always one the application supplied — because the command bus dead-letters into
   * whichever queue bean exists. Unlike {@link #streamRuneDeadLetterRetryRunner} it is therefore
   * <em>not</em> gated on {@code streamrune.dead-letter.enabled}, which switches only the automatic
   * retry: an operator who turns replays off and relies on the admin retry would otherwise keep
   * every exhausted command's payload for ever. A zero or negative {@code retention-max-age} is the
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
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider) {
    // Wire the metrics bean so streamrune.outbox.delivery_failed fires on a terminal FAILED entry
    // (the documented alert metric) and streamrune.outbox.pending tracks the backlog; falls back to
    // NOOP when no metrics bean exists (behavior unchanged).
    OutboxPoller poller =
        OutboxPoller.builder()
            .outboxStore(outboxStore)
            .publisher(publisher)
            .batchSize(properties.outbox().batchSize())
            .pollInterval(java.time.Duration.ofMillis(properties.outbox().flushIntervalMs()))
            // Expose the retry ladder so an operator can widen it; transport failures are
            // non-counting regardless (see OutboxPoller.recordFailure).
            .retryPolicy(
                new RetryPolicy(
                    properties.outbox().retryMaxAttempts(),
                    java.time.Duration.ofMillis(properties.outbox().retryInitialDelayMs()),
                    properties.outbox().retryMultiplier()))
            .metrics(
                metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP))
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
   * created when the outbox is enabled and a {@link org.streamrune.postgres.PostgresOutboxStore}
   * bean is present. The {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when
   * present so {@code streamrune.outbox.swept_rows} and {@code streamrune.outbox.skipped_swept} are
   * recorded; falls back to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean
   * exists.
   */
  @Bean
  @ConditionalOnMissingBean(OutboxRetentionSweeper.class)
  @ConditionalOnBean(org.streamrune.postgres.PostgresOutboxStore.class)
  @ConditionalOnProperty(name = "streamrune.outbox.enabled", havingValue = "true")
  public OutboxRetentionSweeper streamRuneOutboxRetentionSweeper(
      org.streamrune.postgres.PostgresOutboxStore outboxStore,
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
   * Registers a per-instance {@link org.streamrune.postgres.LeaseBasedLeadership} — a DB-clocked,
   * persisted lease with a monotonic fencing {@code epoch} — so that, with more than one replica of
   * the same projection/subscription running, at most one replica is the active consumer and the
   * others sit in standby. Leadership is a single atomic upsert over {@code subscription_leases}
   * decided by the database clock, so it is correct under any connection pooling (no session state,
   * no dedicated connection held across calls); the leader's monotonic epoch is threaded into the
   * projection commit and rejected downstream if a superseded leader ever tries to write. Produced
   * when a {@link DataSource} is present and {@code
   * streamrune.subscription.single-active-consumer.enabled} is not {@code false} (default on). The
   * lease TTL is {@code streamrune.subscription.single-active-consumer.lease-ttl} (default 15s) —
   * the failover bound; the renew heartbeat runs at {@code ttl/3}. When absent or disabled the
   * projection runners fall back to {@link
   * org.streamrune.core.subscription.SubscriptionLeadership#NOOP} (always leader — single-instance
   * behavior unchanged); see {@link ProjectionAutoConfig}, which resolves this bean via {@code
   * ObjectProvider} and threads it into the runner builders.
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.subscription.SubscriptionLeadership.class)
  @ConditionalOnProperty(
      name = "streamrune.subscription.single-active-consumer.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public org.streamrune.core.subscription.SubscriptionLeadership streamRuneSubscriptionLeadership(
      DataSource dataSource, StreamRuneProperties properties) {
    return new org.streamrune.postgres.LeaseBasedLeadership(
        dataSource, properties.subscription().singleActiveConsumer().leaseTtl());
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
   * streamrune.inbox.retention-max-age} (default 7 days). Only created when a {@link
   * PostgresCommandInbox} bean is present. The retention window exceeds typical broker redelivery
   * and subscription replay horizons so a pruned key cannot re-admit a duplicate. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.inbox.swept_rows} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(InboxRetentionSweeper.class)
  @ConditionalOnBean(PostgresCommandInbox.class)
  public InboxRetentionSweeper streamRuneInboxRetentionSweeper(
      PostgresCommandInbox commandInbox,
      StreamRuneProperties properties,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor> relayHealthProvider) {
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
   * org.streamrune.integration.SagaRetentionValidator}). Only created when sagas are enabled and a
   * {@link org.streamrune.postgres.PostgresSagaDeadLetterStore} bean is present. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.saga.dead_letters_swept} is recorded; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   */
  @Bean
  @ConditionalOnMissingBean(org.streamrune.runtime.SagaDeadLetterRetentionSweeper.class)
  @ConditionalOnBean(org.streamrune.postgres.PostgresSagaDeadLetterStore.class)
  @ConditionalOnProperty(
      name = "streamrune.saga.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public org.streamrune.runtime.SagaDeadLetterRetentionSweeper
      streamRuneSagaDeadLetterRetentionSweeper(
          org.streamrune.postgres.PostgresSagaDeadLetterStore sagaDeadLetterStore,
          StreamRuneProperties properties,
          ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
          ObjectProvider<org.streamrune.runtime.BackgroundRelayHealthContributor>
              relayHealthProvider) {
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
   * on) no longer withholds this bean: the sweepers are the sole emitters of {@code
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
   * Registers a JDBC-backed {@link org.streamrune.core.projection.ProjectionRepository} when a
   * DataSource is available. {@link org.streamrune.postgres.JdbcProjectionRepository} also
   * implements {@link org.streamrune.core.projection.AtomicBatchProcessor}: an auto-configured
   * projection runner takes it as its processor when one of its registrations declares {@code
   * TRANSACTIONAL_LOCAL} or {@code EXTERNAL_EFFECT}, or when single-active-consumer leadership
   * needs its epoch honoured — the {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT}
   * registrations' writes and the offset checkpoint then commit in one transaction. A runner of
   * {@code AT_LEAST_ONCE_IDEMPOTENT} registrations without leadership does not use it, whether or
   * not the bean exists.
   *
   * <p>Wires the {@link CryptoEngine} bean (if present) into the repository's ObjectMapper via
   * {@code CryptoShreddingModule}, the same way {@link #sagaStore} wires the saga store — so
   * {@code @Encrypted} read-model fields are persisted as ciphertext instead of plaintext PII, and
   * fall within GDPR forget scope. Without this the auto-configured read model was a silent
   * plaintext no-op even when a CryptoEngine was configured for events and saga state.
   *
   * @throws IllegalStateException if multiple CryptoEngine beans exist with none marked
   *     {@code @Primary} — silently skipping encryption would persist PII as plaintext
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.ProjectionRepository.class)
  public org.streamrune.postgres.JdbcProjectionRepository jdbcProjectionRepository(
      DataSource dataSource,
      ObjectProvider<CryptoEngine> cryptoEngineProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider) {
    CryptoEngine cryptoEngine =
        resolveCryptoEngine(cryptoEngineProvider, "the projection repository");
    // Thread the metrics bean into the repository's CryptoShreddingModule so a redaction
    // on the read-model decrypt path emits subject_redacted; NOOP when no metrics bean exists.
    org.streamrune.core.StreamRuneMetrics metrics =
        metricsProvider.getIfAvailable(() -> org.streamrune.core.StreamRuneMetrics.NOOP);
    return new org.streamrune.postgres.JdbcProjectionRepository(
        dataSource,
        org.streamrune.postgres.JdbcProjectionRepository.createObjectMapper(cryptoEngine, metrics));
  }

  /**
   * Registers a PostgreSQL-backed {@link org.streamrune.core.projection.OffsetStore} when a
   * DataSource is available, so the auto-configured projection runners have somewhere to checkpoint
   * subscription offsets without the application hand-defining one. Mirrors the Quarkus and
   * Micronaut integrations, which both default to {@link
   * org.streamrune.postgres.PostgresOffsetStore} when a DataSource is present.
   */
  @Bean
  @ConditionalOnBean(DataSource.class)
  @ConditionalOnMissingBean(org.streamrune.core.projection.OffsetStore.class)
  public org.streamrune.postgres.PostgresOffsetStore postgresOffsetStore(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresOffsetStore(dataSource);
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
   */
  @Bean
  @ConditionalOnBean(CryptoEngine.class)
  @ConditionalOnMissingBean
  public ForgetSubjectService forgetSubjectService(
      CryptoEngine cryptoEngine,
      ObjectProvider<AuditStore> auditStoreProvider,
      ObjectProvider<org.streamrune.core.gdpr.SubjectDataPurger> purgerProvider,
      ObjectProvider<org.streamrune.core.StreamRuneMetrics> metricsProvider,
      ObjectProvider<CachingQueryBus> queryCacheProvider) {
    var b = ForgetSubjectService.builder().cryptoEngine(cryptoEngine);
    auditStoreProvider.ifAvailable(b::auditStore);
    b.purgers(purgerProvider.orderedStream().toList());
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

  /**
   * Registers an {@link org.streamrune.core.upcasting.UpcasterChain} when at least one {@link
   * org.streamrune.core.upcasting.EventUpcaster} bean is available.
   */
  @Bean("upcasterChain")
  @ConditionalOnBean(org.streamrune.core.upcasting.EventUpcaster.class)
  @ConditionalOnMissingBean(org.streamrune.core.upcasting.UpcasterChain.class)
  public org.streamrune.core.upcasting.UpcasterChain upcasterChain(
      List<org.streamrune.core.upcasting.EventUpcaster> upcasters) {
    return new org.streamrune.core.upcasting.UpcasterChain(upcasters);
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

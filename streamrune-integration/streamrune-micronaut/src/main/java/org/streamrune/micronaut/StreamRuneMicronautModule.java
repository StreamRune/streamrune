package org.streamrune.micronaut;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.core.annotation.Nullable;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.inject.Singleton;
import java.time.Duration;
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
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.postgres.PostgresAuditStore;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresOffsetStore;
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
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.SseEventPublisher;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * Micronaut factory for StreamRune components. Registers StreamRune components as beans in the
 * Micronaut context.
 *
 * <p>Framework defaults are guarded with {@code @Requires(missingBeans = ...)} so that any bean of
 * the same type defined by the application replaces the default — the standard Micronaut pattern
 * for library-provided defaults. This is load-bearing for every bean that <em>does work</em> — the
 * background runners and sweepers, and the leadership coordinator — because {@code @Secondary}
 * alone only demotes a definition in ambiguous single-injection-point selection; it never removes
 * it, and {@link StreamRuneLifecycle} injects each runner BY NAME, so without {@code missingBeans}
 * a user's runner and the framework's would both be created and started (two pollers on the same
 * table, two sweepers under different retention windows). Passive component defaults (stores,
 * query/audit adapters, health contributors, the SSE publisher) rely on {@code @Secondary}
 * precedence alone: they perform no background work, so the demoted framework bean is inert and
 * every consumer resolves the application's. Two of those carry a deliberately documented
 * divergence — see {@link #commandInbox} and {@link #sagaDeadLetterStore}. {@link
 * StreamRuneMicronautProperties} is bound directly by Micronaut as a
 * {@code @ConfigurationProperties} bean and is intentionally not produced here.
 */
@Factory
public class StreamRuneMicronautModule {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(StreamRuneMicronautModule.class);

  /** The property that sets the subscription health lag threshold. */
  static final String SUBSCRIPTION_HEALTH_LAG_THRESHOLD_PROPERTY =
      "streamrune.subscription.health.lag-threshold";

  // Framework bean names for the background runners. StreamRuneLifecycle injects each BY
  // NAME so it starts/stops only the framework-assembled runner — a user-supplied runner bean
  // (which outranks the @Secondary framework bean) is never started twice (OutboxPoller.start()
  // deliberately throws on a second start). Extends the named-bean ownership already applied
  // to the two projection runners in ProjectionFactory.
  static final String FRAMEWORK_OUTBOX_POLLER = "streamRuneOutboxPoller";
  static final String FRAMEWORK_DEAD_LETTER_RETRY_RUNNER = "streamRuneDeadLetterRetryRunner";
  static final String FRAMEWORK_OUTBOX_RETENTION_SWEEPER = "streamRuneOutboxRetentionSweeper";
  static final String FRAMEWORK_INBOX_RETENTION_SWEEPER = "streamRuneInboxRetentionSweeper";
  static final String FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER =
      "streamRuneSagaDeadLetterRetentionSweeper";
  static final String FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER =
      "streamRuneDeadLetterRetentionSweeper";

  /** Creates a {@link StreamRuneMicronautModule}. */
  public StreamRuneMicronautModule() {}

  /**
   * Provides a default empty EventTypeRegistry if the application does not define one.
   *
   * @return a new empty {@link EventTypeRegistry}
   */
  @Singleton
  @Requires(missingBeans = EventTypeRegistry.class)
  public EventTypeRegistry eventTypeRegistry() {
    return SimpleEventTypeRegistry.builder().build();
  }

  /**
   * Creates an EventStoreFactory for PostgreSQL when DataSource is available and the application
   * does not define its own {@link EventStoreFactory}.
   *
   * <p>Wires the {@link CryptoEngine} bean (if exactly one is present) so {@code @Encrypted} fields
   * are persisted as ciphertext. Explicitly guards against ambiguity: if more than one {@link
   * CryptoEngine} bean is registered, an {@link IllegalStateException} is thrown at startup —
   * matching the behaviour of the Spring integration ({@code ObjectProvider.getIfAvailable}) and
   * the Quarkus integration ({@code Instance.isAmbiguous()}). Micronaut injects all beans of the
   * interface type as a {@link List}; an empty list means no crypto, a single-element list means
   * the engine is wired, and a list with more than one element triggers the guard.
   *
   * <p>Passes all {@link org.streamrune.core.upcasting.EventUpcaster} beans so stored events are
   * upcast on read, and all {@link org.streamrune.core.SnapshotMigration} beans so stored snapshots
   * are upgraded on read (Micronaut injects an empty {@link List} when none are registered). When
   * both an {@link org.streamrune.core.outbox.OutboxEventMapper} and an {@link
   * org.streamrune.core.outbox.OutboxStore} bean are present, outbox entries are written
   * transactionally with each event append; if exactly one is present a warning is logged. The
   * store is injected by the {@code OutboxStore} interface, like every other outbox consumer in
   * this module, so its factory method may declare either return type; paired with a mapper it must
   * be a {@link org.streamrune.postgres.PostgresOutboxStore} instance, which alone can write the
   * entries inside the append transaction.
   *
   * @param dataSource the JDBC data source
   * @param typeRegistry the event type registry
   * @param properties the StreamRune configuration properties
   * @param cryptoEngines all registered {@link CryptoEngine} beans (empty if none, one if present,
   *     more than one triggers an {@link IllegalStateException})
   * @param upcasters all registered event upcasters (empty list when none)
   * @param snapshotMigrations all registered snapshot migrations (empty list when none)
   * @param eventAuditStore optional audit store for event-level auditing
   * @param outboxEventMapper optional outbox event mapper for transactional outbox emit
   * @param outboxStore optional outbox store for transactional outbox emit
   * @return a new {@link PostgresEventStoreFactory}
   * @throws IllegalStateException when an {@code OutboxEventMapper} bean is paired with an {@code
   *     OutboxStore} that is not a {@code PostgresOutboxStore}
   */
  @Singleton
  @Requires(beans = DataSource.class)
  @Requires(missingBeans = EventStoreFactory.class)
  // The factory builds the store on the DataSource as given and owns nothing that needs closing,
  // so the bean declares no preDestroy. Micronaut still exposes it under EventStoreFactory for
  // every interface-typed injection point (see #eventStore below).
  public PostgresEventStoreFactory postgresEventStoreFactory(
      DataSource dataSource,
      EventTypeRegistry typeRegistry,
      StreamRuneMicronautProperties properties,
      List<CryptoEngine> cryptoEngines,
      List<org.streamrune.core.upcasting.EventUpcaster> upcasters,
      List<org.streamrune.core.SnapshotMigration> snapshotMigrations,
      @jakarta.annotation.Nullable org.streamrune.postgres.PostgresEventAuditStore eventAuditStore,
      @jakarta.annotation.Nullable org.streamrune.core.outbox.OutboxEventMapper outboxEventMapper,
      @jakarta.annotation.Nullable org.streamrune.core.outbox.OutboxStore outboxStore,
      @jakarta.annotation.Nullable PostgresCommandInbox commandInbox,
      @jakarta.annotation.Nullable CommandInbox busCommandInbox,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them. Found: "
              + cryptoEngines);
    }
    var factory =
        new PostgresEventStoreFactory(dataSource, typeRegistry)
            .autoInitializeSchema(properties.autoInitializeSchema())
            // Thread the metrics bean into the event store's CryptoShreddingModule so a
            // mass redaction on the replay/projection decrypt path emits subject_redacted; NOOP
            // when absent (metrics(null) → NOOP), behavior unchanged.
            .metrics(metrics);
    // The store borrows from the application DataSource as given and bounds its own statements
    // per transaction or per statement; the bound is the one knob it needs from configuration.
    // An explicit null keeps the factory's default.
    if (properties.eventStoreStatementTimeout() != null) {
      try {
        factory.statementTimeout(properties.eventStoreStatementTimeout());
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(
            "streamrune.event-store.statement-timeout: "
                + LogSanitizer.sanitizeFreeText(e.getMessage()),
            e);
      }
    }
    if (!cryptoEngines.isEmpty()) {
      factory.cryptoEngine(cryptoEngines.get(0));
    }
    if (!upcasters.isEmpty()) {
      factory.upcasters(upcasters);
    }
    // Collect all SnapshotMigration beans (mirrors the EventUpcaster List injection above) so a
    // user's standard SnapshotMigration bean is threaded into the store's snapshot-upgrade
    // pipeline. Micronaut injects an empty list when none are registered (unchanged behavior).
    if (!snapshotMigrations.isEmpty()) {
      factory.snapshotMigrations(snapshotMigrations);
    }
    if (eventAuditStore != null) {
      factory.eventAuditStore(eventAuditStore);
    }
    var emittingStore = transactionalOutboxStore(outboxEventMapper, outboxStore);
    if (emittingStore != null) {
      factory.outboxStore(emittingStore).outboxEventMapper(outboxEventMapper);
    } else if (outboxEventMapper != null || outboxStore != null) {
      LOG.warn(
          "Transactional outbox emission is DISABLED: found {} but not the other. "
              + "Define BOTH an OutboxEventMapper and a PostgresOutboxStore bean to enable it.",
          outboxEventMapper != null ? "an OutboxEventMapper" : "an OutboxStore");
    }
    if (commandInbox != null) {
      factory.commandInbox(commandInbox);
    }
    // The bus takes the CommandInbox INTERFACE while this factory takes the concrete
    // PostgresCommandInbox (the concrete type is deliberate), so a user-supplied CommandInbox wins
    // the bus's injection point while the framework's @Secondary default still satisfies this one:
    // the store would keep claiming keys in the framework's own command_inbox table while the bus
    // pre-checks the user's, silently ignoring the override on the authoritative dedup path. Fail
    // fast at wiring instead. This factory only exists when the application did not supply its own
    // EventStoreFactory (@Requires(missingBeans)), so a fully-custom factory + inbox pairing — the
    // one composition that can honour a custom inbox atomically — is untouched.
    org.streamrune.integration.CommandInboxWiringValidator.validateFrameworkEventStoreWiring(
        busCommandInbox, commandInbox);
    return factory;
  }

  /**
   * The outbox store the framework's event store writes into, or {@code null} when the mapper or
   * the store is absent (the caller then logs which one is missing).
   *
   * <p>The store arrives through the {@link org.streamrune.core.outbox.OutboxStore} interface, the
   * type the outbox poller, retention sweeper, failed-entry replayer and {@link
   * StreamRuneConfigValidator} also use: Micronaut exposes a factory bean only under its declared
   * return type and that type's supertypes, so a store declared as {@code OutboxStore} is invisible
   * to an injection point typed {@code PostgresOutboxStore}. The event store writes the outbox
   * entries on the append's own connection, inside its transaction, which only a {@link
   * org.streamrune.postgres.PostgresOutboxStore} can do; paired with a mapper, any other store is
   * refused, because the event store would append every event without its outbox entry while the
   * poller relayed an empty table.
   *
   * @param mapper the outbox event mapper bean, or {@code null}
   * @param store the outbox store bean, or {@code null}
   * @return the store to bind, or {@code null} when either bean is absent
   * @throws IllegalStateException when both are present and the store is not a {@code
   *     PostgresOutboxStore}
   */
  static org.streamrune.postgres.PostgresOutboxStore transactionalOutboxStore(
      @jakarta.annotation.Nullable org.streamrune.core.outbox.OutboxEventMapper mapper,
      @jakarta.annotation.Nullable org.streamrune.core.outbox.OutboxStore store) {
    if (mapper == null || store == null) {
      return null;
    }
    if (store instanceof org.streamrune.postgres.PostgresOutboxStore postgresStore) {
      return postgresStore;
    }
    throw new IllegalStateException(
        "An OutboxEventMapper bean is present, but the OutboxStore bean is a "
            + store.getClass().getName()
            + ", not a PostgresOutboxStore. The framework's event store writes outbox entries in"
            + " the append's own transaction, which only a PostgresOutboxStore supports, so every"
            + " event would be appended without its outbox entry. Declare a PostgresOutboxStore"
            + " bean, or supply your own EventStoreFactory bean that writes into this store.");
  }

  /**
   * Creates an EventStore from the factory when the application does not define its own {@link
   * EventStore}.
   *
   * @param factory the event store factory
   * @return a new {@link EventStore}
   */
  @Singleton
  @Requires(beans = EventStoreFactory.class)
  @Requires(missingBeans = EventStore.class)
  public EventStore eventStore(EventStoreFactory factory) {
    return factory.create();
  }

  /**
   * Creates a default LocalStripedLocker with configured stripe count.
   *
   * @param properties the StreamRune configuration properties
   * @return a new {@link LocalStripedLocker}
   */
  @Singleton
  @Requires(missingBeans = org.streamrune.core.AggregateLocker.class)
  public org.streamrune.core.AggregateLocker aggregateLocker(
      StreamRuneMicronautProperties properties) {
    return new LocalStripedLocker(properties.stripeCount());
  }

  /**
   * Creates a default {@link SimpleQueryBus} for query dispatching on the read side. The {@link
   * org.streamrune.core.StreamRuneMetrics} bean (e.g. the auto-configured {@link
   * MicrometerStreamRuneMetrics}) is wired in when present so {@code queries.dispatched} and {@code
   * queries.duration} are recorded. Metrics are attached to this innermost bus only — the
   * decorating {@link AuditingQueryBus} in {@link #queryBus} is left without metrics so
   * dispatched/duration are counted exactly once. Cache hits short-circuit before reaching this bus
   * and are tracked separately via the {@link CachingQueryBus} cache meters.
   *
   * <p>Exposed as {@code SimpleQueryBus} ONLY ({@code @Bean(typed = ...)}): it is a building block
   * of {@link #queryBus}, and if it also carried the {@link QueryBus} bean type it would satisfy
   * that method's {@code @Requires(missingBeans = QueryBus.class)} and make the composite
   * self-suppress. {@code @Secondary} is not enough for that — the requirement inspects bean
   * <em>definitions</em>, not resolution order. This is Micronaut's equivalent of Spring's
   * {@code @ConditionalOnMissingBean(value = QueryBus.class, ignored = {SimpleQueryBus.class,
   * ...})} ignore list.
   *
   * @param metrics the optional metrics collector
   * @return a new {@link SimpleQueryBus}
   */
  @Singleton
  @Secondary
  @Bean(typed = SimpleQueryBus.class)
  public SimpleQueryBus simpleQueryBus(
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {
    return new SimpleQueryBus(metrics);
  }

  /**
   * Creates a {@link CachingQueryBus} that decorates {@link SimpleQueryBus} with per-query-type
   * caching when {@code streamrune.query-cache.enabled=true}. Marked {@code @Secondary} — it is a
   * building block of {@link #queryBus}, not the bus applications should receive for plain {@link
   * QueryBus} injection points. The {@link org.streamrune.core.StreamRuneMetrics} bean is wired in
   * when present so {@code queries.cache.hits} and {@code queries.cache.misses} are recorded; the
   * caching bus does not record dispatched/duration, so this does not double-count with {@link
   * #simpleQueryBus}.
   *
   * <p>Exposed as {@code CachingQueryBus} ONLY, for the same reason as {@link #simpleQueryBus}.
   *
   * <p>An application-supplied {@link org.streamrune.core.QueryAuthorizer} bean, if present, is
   * wired in too — consulted on every dispatch of a {@code @Cacheable} query, hit or miss, before
   * the cache is touched. Absent, no authorization check runs here and behavior is unchanged; see
   * {@link org.streamrune.core.QueryAuthorizer}'s javadoc for why one is usually needed once
   * caching is enabled for a query whose results depend on the caller.
   *
   * @param delegate the underlying {@link SimpleQueryBus}
   * @param metrics the optional metrics collector
   * @param authorizer the optional query authorizer
   * @return a new {@link CachingQueryBus}
   */
  @Singleton
  @Secondary
  @Bean(typed = CachingQueryBus.class)
  @Requires(property = "streamrune.query-cache.enabled", value = "true")
  @Requires(missingBeans = CachingQueryBus.class)
  public CachingQueryBus cachingQueryBus(
      SimpleQueryBus delegate,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable org.streamrune.core.QueryAuthorizer authorizer) {
    var builder = CachingQueryBus.builder().delegate(delegate);
    if (metrics != null) {
      builder.metrics(metrics);
    }
    if (authorizer != null) {
      builder.authorizer(authorizer);
    }
    return builder.build();
  }

  /**
   * Creates a {@link CacheInvalidator} backed by the auto-configured {@link CachingQueryBus}.
   *
   * @param bus the caching query bus
   * @return a new {@link CacheInvalidator}
   */
  @Singleton
  @Secondary
  @Requires(beans = CachingQueryBus.class)
  public CacheInvalidator cacheInvalidator(CachingQueryBus bus) {
    return bus.cacheInvalidator();
  }

  /**
   * Creates the {@link QueryBus} composing the full chain: SimpleQueryBus → CachingQueryBus (if
   * enabled) → AuditingQueryBus (if AuditStore available). The building blocks ({@link
   * #simpleQueryBus}, {@link #cachingQueryBus}) do not carry the {@link QueryBus} bean type, so
   * this composed bus is the only framework candidate for plain {@code QueryBus} injection points.
   *
   * <p><b>It backs off entirely for any application-defined {@code QueryBus}.</b> Without
   * {@code @Requires(missingBeans = QueryBus.class)} this method won every injection point even
   * when the application supplied its own bus: Micronaut's {@code
   * DefaultBeanContext.lastChanceResolve} ends in {@code filterExactMatch}, which keeps only
   * definitions whose bean type is <em>exactly</em> the requested type — so a user
   * {@code @Singleton class TenantFilteringQueryBus implements QueryBus} (bean type {@code
   * TenantFilteringQueryBus}) was silently discarded and every query was served by the framework's
   * unfiltered chain. On an authorization/tenant-filtering decorator — the documented reason to
   * replace this bean — that is a silent read-side data leak. Spring suppresses the framework
   * composite with {@code @ConditionalOnMissingBean(QueryBus.class)} and Quarkus with
   * {@code @DefaultBean}; this is the Micronaut equivalent.
   *
   * <p>Matching Quarkus's {@code @DefaultBean} semantics, an application bean whose type is one of
   * the framework's own building blocks ({@code SimpleQueryBus}/{@code CachingQueryBus}) also
   * suppresses this composite — such an application receives its own bus <em>unwrapped</em>, i.e.
   * without the {@link AuditingQueryBus} decoration. Spring's {@code ignored = {...}} list keeps
   * the wrapper in that one case; Micronaut's {@code missingBeans} has no ignore list. Wrap
   * explicitly if you replace a building block and still want query auditing.
   *
   * @param simpleBus the base query bus
   * @param cachingBus the optional caching layer
   * @param auditStore the optional audit store
   * @return the fully composed query bus
   */
  @Singleton
  @Requires(missingBeans = QueryBus.class)
  public QueryBus queryBus(
      SimpleQueryBus simpleBus,
      @jakarta.annotation.Nullable CachingQueryBus cachingBus,
      @jakarta.annotation.Nullable AuditStore auditStore) {
    QueryBus bus = cachingBus != null ? cachingBus : simpleBus;
    return auditStore != null ? new AuditingQueryBus(bus, auditStore) : bus;
  }

  /**
   * Creates an {@link SseEventPublisher} for Server-Sent Events.
   *
   * <p>{@code @Bean(preDestroy = "close")} is required — Micronaut does not auto-close a
   * {@code @Factory}-produced {@link AutoCloseable} singleton without it (see this class's other
   * factory methods and {@code FactoryBeanDisposalTest}). Without it, every still-registered SSE
   * subscription's dedicated virtual-thread delivery worker and its bounded queue of up to 256
   * decrypted {@code EventEnvelope}s outlive context teardown (dev-mode live reload, a
   * {@code @MicronautTest} suite standing up several contexts, or {@code
   * ApplicationContext.stop()}), accumulating one such fleet per cycle.
   *
   * @return a new {@link SseEventPublisher}
   */
  @Singleton
  @Secondary
  @Bean(preDestroy = "close")
  public SseEventPublisher sseEventPublisher() {
    return new SseEventPublisher();
  }

  /**
   * Creates a {@link BeanValidationInterceptor}. Prefers a container-provided {@link
   * jakarta.validation.Validator} bean (Micronaut Validation registers one); when absent, builds
   * the default JSR-380 validator so validation never silently no-ops. The validator is wrapped in
   * {@link IntrospectionSafeValidator} because Micronaut's compile-time validator throws for
   * commands that are not {@code @Introspected}.
   *
   * @param validator the JSR-380 validator, or {@code null} when no bean is available
   * @return a new {@link BeanValidationInterceptor}
   */
  @Singleton
  @Secondary
  // Back off entirely when the user defines their own BeanValidationInterceptor. @Secondary
  // only demotes single-candidate resolution; the command bus injects List<CommandInterceptor>, and
  // collection injection includes @Secondary beans, so without missingBeans a user override would
  // JOIN the framework default in the chain (both run) instead of replacing it.
  @Requires(missingBeans = BeanValidationInterceptor.class)
  @Requires(property = "streamrune.validation-enabled", value = "true", defaultValue = "true")
  public BeanValidationInterceptor beanValidationInterceptor(
      @jakarta.annotation.Nullable jakarta.validation.Validator validator) {
    if (validator == null) {
      validator = jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator();
    }
    return new BeanValidationInterceptor(new IntrospectionSafeValidator(validator));
  }

  /**
   * Creates a {@link MicrometerStreamRuneMetrics} when a {@link
   * io.micrometer.core.instrument.MeterRegistry} is available. Meter names start with the {@code
   * streamrune.metrics.prefix} property (default {@code streamrune}).
   *
   * @param meterRegistry the Micrometer registry
   * @param properties the StreamRune configuration properties
   * @return a new {@link MicrometerStreamRuneMetrics}
   */
  @Singleton
  @Secondary
  @Requires(beans = io.micrometer.core.instrument.MeterRegistry.class)
  @Requires(property = "streamrune.metrics.enabled", value = "true", defaultValue = "true")
  public MicrometerStreamRuneMetrics micrometerStreamRuneMetrics(
      io.micrometer.core.instrument.MeterRegistry meterRegistry,
      StreamRuneMicronautProperties properties) {
    return MicrometerStreamRuneMetrics.builder()
        .registry(meterRegistry)
        .prefix(properties.metricsPrefix())
        .build();
  }

  /**
   * Creates a circuit breaker interceptor with configured failure threshold and cooldown.
   *
   * @param properties the StreamRune configuration properties
   * @return a new {@link CircuitBreakerCommandInterceptor}
   */
  @Singleton
  @Secondary
  // Back off when the user defines their own CircuitBreakerCommandInterceptor (e.g. with a
  // different threshold). Without missingBeans, collection injection keeps the framework breaker in
  // the chain alongside the user's — commands keep tripping at the framework threshold, a silent
  // wrong answer.
  @Requires(missingBeans = CircuitBreakerCommandInterceptor.class)
  public CircuitBreakerCommandInterceptor circuitBreakerCommandInterceptor(
      StreamRuneMicronautProperties properties) {
    return new CircuitBreakerCommandInterceptor(
        properties.circuitBreakerFailureThreshold(), properties.circuitBreakerCooldown());
  }

  /**
   * Creates a default VirtualThreadCommandBus when the application does not define its own.
   *
   * <p><b>Fail-fast on {@code @Encrypted} command fields:</b> command types are validated at wiring
   * time via {@link org.streamrune.crypto.CryptoConfigValidator}; if a registered command carries
   * an {@code @Encrypted} field but no {@link CryptoEngine} bean is configured, startup fails
   * instead of silently dead-lettering that PII as plaintext. Micronaut injects all {@link
   * CryptoEngine} beans as a {@link List}; an empty list means no engine, more than one triggers
   * the same ambiguity guard used by {@link #postgresEventStoreFactory}/{@link #sagaStore}/{@link
   * #deadLetterRetryRunner}.
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
   * <p><b>Snapshot policy:</b> the default is {@code
   * SnapshotPolicy.everyNEvents(streamrune.snapshot-every-n-events)} at snapshot schema version 1.
   * An application {@link SnapshotPolicy} bean replaces it wholesale — that is the ONLY way to bump
   * the snapshot schema version or to switch snapshots off, and until this hook existed both were
   * unreachable through this module: the version was pinned to 1, which also made every registered
   * {@code SnapshotMigration} bean dead code (the store's migrate/discard branch is only entered on
   * a version mismatch). It is deliberately a bean rather than a property — the version must track
   * the application's state-class schema, so it belongs next to that code and is released with it.
   *
   * @param eventStore the event store
   * @param locker the aggregate locker
   * @param properties the StreamRune configuration properties
   * @param snapshotPolicy optional application-supplied snapshot policy; when absent the default
   *     {@code everyNEvents(properties.snapshotEveryNEvents())} at schema version 1 is used
   * @param interceptors the list of command interceptors
   * @param registrations the list of decider registrations
   * @param cryptoEngines all registered {@link CryptoEngine} beans (empty if none, one if present,
   *     more than one triggers an {@link IllegalStateException})
   * @param commandInbox optional command inbox
   * @param deadLetterQueue optional dead-letter queue; when present, failed commands are published
   *     to it through a crypto-aware mapper
   * @param metrics optional metrics collector; when present, command-bus meters are recorded
   * @return a new {@link VirtualThreadCommandBus}
   * @throws IllegalStateException if multiple CryptoEngine beans exist, or if a registered command
   *     type has an {@code @Encrypted} field with no CryptoEngine configured
   */
  @Singleton
  @Requires(beans = EventStore.class)
  @Requires(missingBeans = VirtualThreadCommandBus.class)
  @Bean(preDestroy = "close")
  public VirtualThreadCommandBus virtualThreadCommandBus(
      EventStore eventStore,
      org.streamrune.core.AggregateLocker locker,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable SnapshotPolicy snapshotPolicy,
      List<CommandInterceptor> interceptors,
      List<DeciderRegistration<?, ?, ?>> registrations,
      List<CryptoEngine> cryptoEngines,
      @jakarta.annotation.Nullable CommandInbox commandInbox,
      @jakarta.annotation.Nullable org.streamrune.core.DeadLetterQueue deadLetterQueue,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {

    // Fail fast when a registered command type carries an @Encrypted field but no
    // CryptoEngine is configured — otherwise that PII would be dead-lettered as PLAINTEXT.
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them. Found: "
              + cryptoEngines);
    }
    CryptoEngine cryptoEngine = cryptoEngines.isEmpty() ? null : cryptoEngines.get(0);
    java.util.List<Class<?>> commandTypes =
        registrations.stream()
            .map(org.streamrune.runtime.DeciderRegistration::commandType)
            .collect(java.util.stream.Collectors.toList());
    org.streamrune.crypto.CryptoConfigValidator.validate(commandTypes, cryptoEngine);

    var retryPolicy =
        new RetryPolicy(
            properties.retryMaxAttempts(),
            java.time.Duration.ofMillis(properties.retryInitialDelayMs()),
            properties.retryBackoffMultiplier());

    // Micronaut injects the interceptor List in an unspecified order. Sort it into the
    // canonical, security-relevant chain (audit outside authorization so denials are audited,
    // validation after authorization, breaker innermost) so Micronaut matches Spring's @Order
    // chain.
    List<CommandInterceptor> orderedInterceptors =
        org.streamrune.runtime.CommandInterceptorOrdering.sorted(interceptors);

    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(locker)
            .retryPolicy(retryPolicy)
            .snapshotPolicy(
                snapshotPolicy != null
                    ? snapshotPolicy
                    : SnapshotPolicy.everyNEvents(properties.snapshotEveryNEvents()))
            .lockTimeout(properties.lockTimeout())
            .stripeCount(properties.stripeCount())
            .maxInFlightAsyncCommands(properties.maxInFlightAsyncCommands())
            .interceptors(orderedInterceptors);

    // Wire the optional StreamRuneMetrics bean so command-bus meters are recorded (NOOP if absent),
    // mirroring Spring (StreamRuneAutoConfiguration) and the query bus on this framework.
    if (metrics != null) {
      builder.metrics(metrics);
    }

    // Wire the optional DeadLetterQueue bean so commands that exhaust retries are dead-lettered,
    // mirroring Spring. The DLQ publish mapper is ALWAYS the crypto-aware default — never an
    // application ObjectMapper bean — so @Encrypted command fields are persisted as ciphertext.
    if (deadLetterQueue != null) {
      builder
          .deadLetterQueue(deadLetterQueue)
          .objectMapper(
              org.streamrune.runtime.DeadLetterRetryRunner.createObjectMapper(
                  cryptoEngine, metrics));
    }

    if (commandInbox != null) {
      builder.commandInbox(commandInbox);
    }

    for (DeciderRegistration<?, ?, ?> reg : registrations) {
      registerDecider(builder, reg);
    }

    return builder.build();
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

  /**
   * Creates an OTel command interceptor when {@code OpenTelemetry} is available.
   *
   * @param openTelemetry the OpenTelemetry instance
   * @return a new {@link OpenTelemetryCommandInterceptor}
   */
  @Singleton
  @Secondary
  // Back off when the user defines their own OpenTelemetryCommandInterceptor (collection
  // injection would otherwise run both — duplicate command spans).
  @Requires(missingBeans = OpenTelemetryCommandInterceptor.class)
  @Requires(beans = OpenTelemetry.class)
  public OpenTelemetryCommandInterceptor openTelemetryCommandInterceptor(
      OpenTelemetry openTelemetry) {
    return new OpenTelemetryCommandInterceptor(openTelemetry);
  }

  /**
   * Creates a {@link org.streamrune.runtime.SubscriptionHealthContributor} when EventStore and
   * OffsetStore are available. The {@link org.streamrune.core.StreamRuneMetrics} bean (e.g. the
   * auto-configured {@link MicrometerStreamRuneMetrics}) is wired in when present so each health
   * sample emits the {@code streamrune.subscriptions.lag} gauge; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when absent (behavior unchanged).
   *
   * @param eventStore the event store
   * @param offsetStore the offset store
   * @param metrics optional metrics collector
   * @param lagThreshold {@code streamrune.subscription.health.lag-threshold}: the lag, in events,
   *     at or above which a running subscription reads {@code DEGRADED} (lag never reads {@code
   *     DOWN}); at least 1, default 1000
   * @return a new {@link org.streamrune.runtime.SubscriptionHealthContributor}
   */
  @Singleton
  @Secondary
  @Requires(beans = {EventStore.class, org.streamrune.core.projection.OffsetStore.class})
  public org.streamrune.runtime.SubscriptionHealthContributor subscriptionHealthContributor(
      EventStore eventStore,
      org.streamrune.core.projection.OffsetStore offsetStore,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @Property(name = SUBSCRIPTION_HEALTH_LAG_THRESHOLD_PROPERTY, defaultValue = "1000")
          long lagThreshold) {
    if (lagThreshold < 1) {
      throw new IllegalStateException(
          SUBSCRIPTION_HEALTH_LAG_THRESHOLD_PROPERTY + " must be at least 1, got: " + lagThreshold);
    }
    return new org.streamrune.runtime.SubscriptionHealthContributor(
        eventStore,
        offsetStore,
        lagThreshold,
        metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP);
  }

  /**
   * Creates an {@link AuthorizationCommandInterceptor} when the application registers a {@link
   * CommandAuthorizationPolicy} bean.
   *
   * @param policy the command authorization policy
   * @return a new {@link AuthorizationCommandInterceptor}
   */
  @Singleton
  @Secondary
  // Back off when the user defines their own AuthorizationCommandInterceptor (collection
  // injection would otherwise run both — a duplicated / conflicting authorization pass).
  @Requires(missingBeans = AuthorizationCommandInterceptor.class)
  @Requires(beans = CommandAuthorizationPolicy.class)
  public AuthorizationCommandInterceptor authorizationCommandInterceptor(
      CommandAuthorizationPolicy policy) {
    return new AuthorizationCommandInterceptor(policy);
  }

  /**
   * Creates a {@link MicronautSecurityUserRoleResolver} when Micronaut Security is on the classpath
   * and its {@link io.micronaut.security.utils.SecurityService} bean is available. The service
   * resolves the {@code Authentication} of the current request on every call — an {@code
   * Authentication} itself is per-request state and must never be captured in a singleton.
   *
   * @param securityService the Micronaut Security service for the current authentication
   * @return a new {@link MicronautSecurityUserRoleResolver}
   */
  @Singleton
  @Requires(classes = io.micronaut.security.utils.SecurityService.class)
  @Requires(beans = io.micronaut.security.utils.SecurityService.class)
  @Requires(missingBeans = UserRoleResolver.class)
  public MicronautSecurityUserRoleResolver micronautSecurityUserRoleResolver(
      io.micronaut.security.utils.SecurityService securityService) {
    return new MicronautSecurityUserRoleResolver(securityService);
  }

  /**
   * Creates an {@link AuthenticatedUserResolver} backed by Micronaut Security's {@link
   * io.micronaut.security.utils.SecurityService} when it is on the classpath and enabled, so the
   * {@link #requestIdentityPolicy request identity policy} takes the authorization identity from
   * the authenticated principal rather than the client-supplied {@code X-User-Id} header.
   *
   * @param securityService the Micronaut Security service for the current authentication
   * @return a new {@link MicronautAuthenticatedUserResolver}
   */
  @Singleton
  @Requires(classes = io.micronaut.security.utils.SecurityService.class)
  @Requires(beans = io.micronaut.security.utils.SecurityService.class)
  @Requires(missingBeans = AuthenticatedUserResolver.class)
  public MicronautAuthenticatedUserResolver micronautAuthenticatedUserResolver(
      io.micronaut.security.utils.SecurityService securityService) {
    return new MicronautAuthenticatedUserResolver(securityService);
  }

  /**
   * The one rule by which every request-facing component ({@link StreamRuneContextFilter}, {@link
   * SseController}) derives a request's identity, so they can never disagree — the same {@link
   * RequestIdentityPolicy} the Spring and Quarkus integrations use. {@code
   * streamrune.security.trust-user-id-header=true} selects the trusted-gateway mode (the {@code
   * X-User-Id} header is the identity); otherwise an {@link AuthenticatedUserResolver} (Micronaut
   * Security's {@link MicronautAuthenticatedUserResolver}, or an application bean) selects the
   * authenticated principal, and without one every request is anonymous and the header is ignored.
   * Fail-closed: the header is never an identity unless the flag says so. Micronaut Security's
   * beans exist only while {@code micronaut.security.enabled} is not {@code false}, so a disabled
   * security module counts as absent.
   *
   * @param authenticatedUserResolver the authenticated-principal resolver, or {@code null} when
   *     none is available
   * @param trustUserIdHeader the value of {@code streamrune.security.trust-user-id-header}
   * @return the application's request identity policy
   */
  @Singleton
  @Requires(missingBeans = RequestIdentityPolicy.class)
  public RequestIdentityPolicy requestIdentityPolicy(
      @Nullable AuthenticatedUserResolver authenticatedUserResolver,
      @Property(name = RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY, defaultValue = "false")
          boolean trustUserIdHeader) {
    return RequestIdentityPolicy.of(authenticatedUserResolver, trustUserIdHeader);
  }

  /**
   * Fail-closed {@link org.streamrune.integration.SseAuthorizer} used when SSE is enabled ({@code
   * streamrune.sse.enabled=true}) but the application provides no authorizer of its own. Denies
   * every stream, so enabling SSE never exposes an aggregate's events without an explicit access
   * decision. Applications override it by defining their own {@code SseAuthorizer} bean.
   *
   * @return a deny-all authorizer
   */
  @Singleton
  @Secondary
  @Requires(property = "streamrune.sse.enabled", value = "true")
  @Requires(missingBeans = org.streamrune.integration.SseAuthorizer.class)
  public org.streamrune.integration.SseAuthorizer sseDenyAllAuthorizer() {
    return org.streamrune.integration.SseAuthorizer.DENY_ALL;
  }

  /**
   * Creates an {@link AnnotationAuthorizationInterceptor} when a {@link UserRoleResolver} bean is
   * available.
   *
   * <p>The resolver is used as-is — it is deliberately <em>not</em> wrapped in {@link
   * org.streamrune.runtime.CachingUserRoleResolver}. Caching by {@link
   * org.streamrune.core.types.UserId} is unsound for resolvers that derive authorities from ambient
   * per-request state (e.g. {@link MicronautSecurityUserRoleResolver} reading the request {@code
   * SecurityService}): a cache hit would bypass the resolver's principal-match check and serve one
   * principal's authorities to a different caller presenting the same user ID. Applications whose
   * resolver is a pure function of the user ID (e.g. a database lookup) can opt in explicitly by
   * defining a {@code CachingUserRoleResolver} bean wrapping their resolver.
   *
   * @param resolver the user role resolver
   * @return a new {@link AnnotationAuthorizationInterceptor}
   */
  @Singleton
  @Secondary
  // Back off when the user defines their own AnnotationAuthorizationInterceptor (collection
  // injection would otherwise run both — a duplicated @RequireRole authorization pass).
  @Requires(missingBeans = AnnotationAuthorizationInterceptor.class)
  @Requires(beans = UserRoleResolver.class)
  public AnnotationAuthorizationInterceptor annotationAuthorizationInterceptor(
      UserRoleResolver resolver) {
    return new AnnotationAuthorizationInterceptor(resolver);
  }

  /**
   * Creates a PostgresEventAuditStore when event audit is enabled.
   *
   * @param dataSource the JDBC data source
   * @return a new PostgresEventAuditStore
   */
  @Singleton
  @Secondary
  @Requires(property = "streamrune.event-audit-enabled", value = "true")
  @Requires(beans = DataSource.class)
  public org.streamrune.postgres.PostgresEventAuditStore postgresEventAuditStore(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresEventAuditStore(dataSource);
  }

  /**
   * Creates a PostgresEventAuditQuery when event audit is enabled.
   *
   * @param dataSource the JDBC data source
   * @return a new PostgresEventAuditQuery
   */
  @Singleton
  @Secondary
  @Requires(property = "streamrune.event-audit-enabled", value = "true")
  @Requires(beans = DataSource.class)
  public org.streamrune.postgres.PostgresEventAuditQuery postgresEventAuditQuery(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresEventAuditQuery(dataSource);
  }

  /**
   * Creates a PostgresProjectionDeadLetterStore when projection DLQ is enabled.
   *
   * @param dataSource the JDBC data source
   * @return a new PostgresProjectionDeadLetterStore
   */
  @Singleton
  @Secondary
  @Requires(property = "streamrune.projection-dlq-enabled", value = "true")
  @Requires(beans = DataSource.class)
  // Back off entirely when the user supplies their own ProjectionDeadLetterStore, so
  // the user's bean cleanly REPLACES the default. @Secondary only de-prioritizes single-bean
  // injection; it does NOT filter the List<ProjectionDeadLetterStore> collection injection that
  // ProjectionFactory uses, so without this guard a user store + the default both register and
  // resolveSingleDlq hard-fails boot with "Multiple ProjectionDeadLetterStore" (its
  // @Primary remediation cannot reduce a collection). Parity with Spring's
  // @ConditionalOnMissingBean and Quarkus's @DefaultBean.
  @Requires(missingBeans = org.streamrune.core.projection.ProjectionDeadLetterStore.class)
  public org.streamrune.postgres.PostgresProjectionDeadLetterStore
      postgresProjectionDeadLetterStore(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresProjectionDeadLetterStore(dataSource);
  }

  /**
   * Creates a {@link org.streamrune.postgres.PostgresSagaStore} as the default {@link SagaStore}.
   *
   * <p>Wires the {@link CryptoEngine} bean (if exactly one is present) into the saga store's
   * ObjectMapper via {@code CryptoShreddingModule}, the same way {@link #postgresEventStoreFactory}
   * wires the event store — so {@code @Encrypted} saga-state fields are persisted as ciphertext
   * instead of plaintext PII, and fall within GDPR forget scope.
   *
   * @param dataSource the JDBC data source
   * @param cryptoEngines all registered {@link CryptoEngine} beans (empty if none, one if present,
   *     more than one triggers an {@link IllegalStateException})
   * @return a new {@link org.streamrune.postgres.PostgresSagaStore}
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} saga-state fields as PLAINTEXT
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  @Requires(property = "streamrune.saga.enabled", value = "true", defaultValue = "true")
  public SagaStore sagaStore(
      DataSource dataSource,
      List<CryptoEngine> cryptoEngines,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them. Found: "
              + cryptoEngines);
    }
    CryptoEngine cryptoEngine = cryptoEngines.isEmpty() ? null : cryptoEngines.get(0);
    // Thread the metrics bean into the saga store's CryptoShreddingModule so a redaction
    // on the saga-state decrypt path emits subject_redacted; NOOP when absent (behavior unchanged).
    return new org.streamrune.postgres.PostgresSagaStore(
        dataSource,
        org.streamrune.postgres.PostgresSagaStore.createObjectMapper(cryptoEngine, metrics));
  }

  /**
   * Creates a {@link org.streamrune.postgres.PostgresSagaDeadLetterStore} as the default {@link
   * SagaDeadLetterStore}. Applications that supply their own {@link SagaDeadLetterStore} bean take
   * precedence over this default via Micronaut's {@code @Secondary} ordering.
   *
   * <p><strong>Concrete return type:</strong> the declared return type is deliberately the concrete
   * {@link org.streamrune.postgres.PostgresSagaDeadLetterStore}, not the {@link
   * SagaDeadLetterStore} interface. Micronaut derives a factory bean's types from the method's
   * declared return type, so an interface-typed factory leaves concrete-typed gates and injection
   * points — {@link #sagaDeadLetterRetentionSweeper} declares {@code @Requires(beans =
   * PostgresSagaDeadLetterStore.class)} and takes a concrete parameter — permanently unsatisfied
   * even though a bean exists. That silently dropped the retention sweeper from every default
   * deployment and let {@code saga_dead_letters} grow without bound (a GDPR storage-limitation
   * violation), while the boot validators, which check this store by interface type, still passed.
   * Same defect class as {@link #commandInbox}. The concrete class still implements {@link
   * SagaDeadLetterStore}, so interface-typed consumers keep resolving this bean.
   *
   * @param dataSource the JDBC data source
   * @return a new {@link org.streamrune.postgres.PostgresSagaDeadLetterStore}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  @Requires(property = "streamrune.saga.enabled", value = "true", defaultValue = "true")
  public org.streamrune.postgres.PostgresSagaDeadLetterStore sagaDeadLetterStore(
      DataSource dataSource) {
    return new org.streamrune.postgres.PostgresSagaDeadLetterStore(dataSource);
  }

  /**
   * Creates a {@link PostgresCommandInbox} as the default {@link CommandInbox} when a DataSource is
   * available. Applications that supply their own {@link CommandInbox} bean will suppress this
   * default via Micronaut's {@code @Secondary} precedence.
   *
   * <p>The declared return type is deliberately the concrete {@link PostgresCommandInbox}, not the
   * {@link CommandInbox} interface: Micronaut resolves {@code @Nullable PostgresCommandInbox} /
   * {@code @Requires(beans = PostgresCommandInbox.class)} injection points (e.g. {@link
   * #postgresEventStoreFactory} and {@link #inboxRetentionSweeper}) from the factory method's
   * declared return type, so an interface-typed factory leaves those concrete-typed points
   * permanently unsatisfied even though a bean exists. The concrete class still implements {@link
   * CommandInbox}, so interface-typed consumers (e.g. {@link VirtualThreadCommandBus}'s
   * {@code @Nullable CommandInbox} parameter) continue to resolve this bean too.
   *
   * <p><strong>What overriding actually requires.</strong> {@code @Secondary} makes a user bean win
   * the INTERFACE-typed points (the bus) while this default still satisfies the CONCRETE-typed ones
   * (the event-store factory), so a half-override leaves the store claiming keys in the framework's
   * own {@code command_inbox} while the bus pre-checks the user's — the override silently ignored
   * on the authoritative dedup path. Because the store must claim the idempotency key in the SAME
   * transaction as the append, a custom inbox is only honoured end-to-end when the event store
   * knows about it: supplying a {@code CommandInbox} that is not a {@link PostgresCommandInbox}
   * therefore also requires supplying your own {@code EventStoreFactory}/{@code EventStore} whose
   * {@code appendWithKey} claims the key atomically. {@link
   * org.streamrune.integration.CommandInboxWiringValidator} fails startup on the mismatch rather
   * than letting the guarantee break silently in production.
   *
   * @param dataSource the JDBC data source
   * @return a new {@link PostgresCommandInbox}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  public PostgresCommandInbox commandInbox(DataSource dataSource) {
    return new PostgresCommandInbox(dataSource);
  }

  /**
   * Creates a {@link PostgresAuditStore} as the default {@link AuditStore}.
   *
   * @param dataSource the JDBC data source
   * @return a new {@link PostgresAuditStore}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  public AuditStore auditStore(DataSource dataSource) {
    return new PostgresAuditStore(dataSource);
  }

  /**
   * Creates a PostgresComplianceReportQuery when DataSource is available.
   *
   * @param dataSource the JDBC data source
   * @return a new PostgresComplianceReportQuery
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  public ComplianceReportQuery complianceReportQuery(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresComplianceReportQuery(dataSource);
  }

  /**
   * Creates a {@link org.streamrune.postgres.PostgresCommandAuditQuery} as the default {@link
   * org.streamrune.core.audit.CommandAuditQuery} when DataSource is available.
   *
   * @param dataSource the JDBC data source
   * @return a new PostgresCommandAuditQuery
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  public org.streamrune.core.audit.CommandAuditQuery commandAuditQuery(DataSource dataSource) {
    return new org.streamrune.postgres.PostgresCommandAuditQuery(dataSource);
  }

  /**
   * Creates a {@link PostgresOffsetStore} as the default {@link
   * org.streamrune.core.projection.OffsetStore} when a DataSource is present and the application
   * does not define its own. Brings Micronaut to parity with Quarkus, which defaults the same way —
   * without this a Micronaut application must hand-define an {@code OffsetStore} for projections to
   * run.
   *
   * @param dataSource the JDBC data source
   * @return a new {@link PostgresOffsetStore}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  @Requires(missingBeans = org.streamrune.core.projection.OffsetStore.class)
  public org.streamrune.core.projection.OffsetStore offsetStore(DataSource dataSource) {
    return new PostgresOffsetStore(dataSource);
  }

  /**
   * Creates a {@link org.streamrune.postgres.JdbcProjectionRepository} as the default {@link
   * org.streamrune.core.projection.ProjectionRepository} when a DataSource is present and the
   * application does not define its own. It also implements {@link
   * org.streamrune.core.projection.AtomicBatchProcessor}: a projection runner is given it when one
   * of its registrations declares {@code TRANSACTIONAL_LOCAL} or {@code EXTERNAL_EFFECT}, or when
   * single-active-consumer leadership must fence. The runner then commits each batch's offset
   * checkpoint in ONE transaction, under the checkpoint row's {@code FOR UPDATE} lock that runs the
   * epoch fence, the overlap guard and the monotonic guard, together with the read-model writes of
   * its {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} registrations. An {@code
   * AT_LEAST_ONCE_IDEMPOTENT} registration is never handed that transaction, so its writes are not
   * part of it. The bean's presence alone selects nothing: a runner of {@code
   * AT_LEAST_ONCE_IDEMPOTENT} registrations without leadership does not use it ({@link
   * org.streamrune.integration.ProjectionProcessorResolution}).
   *
   * <p>This producer was missing while {@link #subscriptionLeadership} was not. The module handed
   * the runners a real, epoch-issuing lease leadership and no processor that could honour an epoch,
   * so every projection ran on the nonatomic processor ({@link
   * org.streamrune.core.projection.AtomicBatchProcessor#nonAtomicAtLeastOnce()}): the takeover
   * stamp was a no-op, the fence and overlap guards never ran, and a superseded leader resuming
   * inside its local staleness window re-applied its in-flight batch to the read model — silently,
   * since the offset store's monotonic guard then swallowed the trailing checkpoint save. Spring
   * ({@code @ConditionalOnBean(DataSource.class)}) and Quarkus ({@code @DefaultBean}) have always
   * produced this bean; only Micronaut shipped the unfenced combination by default.
   *
   * @param dataSource the JDBC data source
   * @return a new {@link org.streamrune.postgres.JdbcProjectionRepository}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  @Requires(missingBeans = org.streamrune.core.projection.ProjectionRepository.class)
  public org.streamrune.postgres.JdbcProjectionRepository jdbcProjectionRepository(
      DataSource dataSource,
      List<CryptoEngine> cryptoEngines,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {
    // Wire the CryptoEngine bean (if exactly one is present) into the
    // repository's ObjectMapper via CryptoShreddingModule, the same way sagaStore wires the saga
    // store — so @Encrypted read-model fields are ciphertext at rest and within GDPR forget scope.
    // Without this the auto-configured read model was a silent plaintext no-op.
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them. Found: "
              + cryptoEngines);
    }
    CryptoEngine cryptoEngine = cryptoEngines.isEmpty() ? null : cryptoEngines.get(0);
    // Thread the metrics bean into the repository's CryptoShreddingModule so a redaction
    // on the read-model decrypt path emits subject_redacted; NOOP when absent (behavior unchanged).
    return new org.streamrune.postgres.JdbcProjectionRepository(
        dataSource,
        org.streamrune.postgres.JdbcProjectionRepository.createObjectMapper(cryptoEngine, metrics));
  }

  /**
   * Creates an {@link AuditCommandInterceptor} when an {@link AuditStore} bean is present.
   *
   * @param auditStore the audit store
   * @return a new {@link AuditCommandInterceptor}
   */
  @Singleton
  @Secondary
  // Back off when the user defines their own AuditCommandInterceptor (e.g. redirecting audit
  // writes to a custom store). Without missingBeans, collection injection would run both — every
  // command audited TWICE, polluting the compliance trail with duplicate rows.
  @Requires(missingBeans = AuditCommandInterceptor.class)
  @Requires(beans = AuditStore.class)
  public AuditCommandInterceptor auditCommandInterceptor(AuditStore auditStore) {
    return new AuditCommandInterceptor(auditStore);
  }

  /**
   * Creates a {@link ForgetSubjectService} when a {@link CryptoEngine} bean is available.
   *
   * <p>All registered {@link SubjectDataPurger} beans are injected as a {@link List} and wired in,
   * mirroring how {@link #exportSubjectDataService} collects its {@link SubjectDataCollector} beans
   * (Micronaut injects an empty list when none are registered). Without this the auto-configured
   * forget would crypto-shred the key but never purge read-model projections, leaving derived PII
   * behind after a GDPR Article-17 erasure.
   *
   * @param cryptoEngine the crypto engine
   * @param auditStore optional audit store
   * @param purgers all registered read-model purgers (empty list when none)
   * @param metrics optional metrics collector; a failed purge increments {@code
   *     streamrune.gdpr.purge_failed}
   * @param queryCache the {@link CachingQueryBus}, present when {@code
   *     streamrune.query-cache.enabled=true}; every forget evicts it once its purgers have run, so
   *     a cached answer holding the subject's data is not served after the erasure
   * @return a new {@link ForgetSubjectService}
   */
  @Singleton
  @Secondary
  @Requires(beans = CryptoEngine.class)
  public ForgetSubjectService forgetSubjectService(
      CryptoEngine cryptoEngine,
      @jakarta.annotation.Nullable AuditStore auditStore,
      List<SubjectDataPurger> purgers,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable CachingQueryBus queryCache) {
    var b = ForgetSubjectService.builder().cryptoEngine(cryptoEngine).purgers(purgers);
    if (auditStore != null) {
      b.auditStore(auditStore);
    }
    b.metrics(metrics);
    // A forget evicts the query cache after its last purger, so no cached answer taken before the
    // erasure is served after it.
    b.queryCache(queryCache);
    return b.build();
  }

  /**
   * Creates an {@link ExportSubjectDataService} when at least one {@link SubjectDataCollector} bean
   * is available.
   *
   * @param collectors all registered subject data collectors
   * @param auditStore optional audit store
   * @return a new {@link ExportSubjectDataService}
   */
  @Singleton
  @Secondary
  @Requires(beans = SubjectDataCollector.class)
  public ExportSubjectDataService exportSubjectDataService(
      List<SubjectDataCollector> collectors, @jakarta.annotation.Nullable AuditStore auditStore) {
    var b = ExportSubjectDataService.builder().collectors(collectors);
    if (auditStore != null) {
      b.auditStore(auditStore);
    }
    return b.build();
  }

  /**
   * Creates a {@link DeadLetterRetryRunner} when dead letter retry is enabled and both {@link
   * org.streamrune.core.DeadLetterQueue} and {@link org.streamrune.core.CommandBus} are available.
   * {@code streamrune.dead-letter.enabled} governs only this automatic retry: the command bus keeps
   * dead-lettering into the queue and {@link #deadLetterRetentionSweeper} keeps pruning it.
   *
   * <p>Dead-letter payloads are serialized/deserialized with a DEDICATED mapper built via {@link
   * org.streamrune.runtime.DeadLetterRetryRunner#createObjectMapper(CryptoEngine)} — wired with the
   * {@link CryptoEngine} bean (if exactly one is present), the same way {@link #sagaStore} wires
   * {@code PostgresSagaStore}'s mapper — so {@code @Encrypted} command fields round-trip as
   * ciphertext instead of plaintext PII. This mapper is deliberately NOT the application's shared
   * {@code com.fasterxml.jackson.databind.ObjectMapper} bean (Micronaut's own Jackson
   * auto-configuration): registering {@code CryptoShreddingModule} on that shared instance would
   * silently make crypto-shredding apply to unrelated app-wide JSON handling (e.g. REST responses)
   * for any record that happens to reuse a command type with an {@code @Encrypted} field.
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
   * @param deadLetterQueue the dead letter queue
   * @param commandBus the command bus
   * @param cryptoEngines all registered {@link CryptoEngine} beans (empty if none, one if present,
   *     more than one triggers an {@link IllegalStateException})
   * @param registrations the list of decider registrations
   * @param properties the StreamRune configuration properties
   * @return a new {@link DeadLetterRetryRunner}
   * @throws IllegalStateException if multiple CryptoEngine beans exist — silently skipping
   *     encryption would persist {@code @Encrypted} DLQ payloads as PLAINTEXT
   */
  @Singleton
  @Secondary
  @Requires(
      beans = {org.streamrune.core.DeadLetterQueue.class, org.streamrune.core.CommandBus.class})
  @Requires(property = "streamrune.dead-letter.enabled", value = "true", defaultValue = "true")
  // Back off entirely when the application defines its own runner.
  @Requires(missingBeans = DeadLetterRetryRunner.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
  public DeadLetterRetryRunner deadLetterRetryRunner(
      org.streamrune.core.DeadLetterQueue deadLetterQueue,
      org.streamrune.core.CommandBus commandBus,
      List<CryptoEngine> cryptoEngines,
      List<DeciderRegistration<?, ?, ?>> registrations,
      @jakarta.annotation.Nullable
          org.streamrune.core.subscription.SubscriptionLeadership leadership,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth,
      StreamRuneMicronautProperties properties) {
    if (cryptoEngines.size() > 1) {
      throw new IllegalStateException(
          "Multiple CryptoEngine beans found ("
              + cryptoEngines.size()
              + "); register exactly one or qualify them. Found: "
              + cryptoEngines);
    }
    CryptoEngine cryptoEngine = cryptoEngines.isEmpty() ? null : cryptoEngines.get(0);
    var builder =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(deadLetterQueue)
            .commandBus(commandBus)
            .objectMapper(
                org.streamrune.runtime.DeadLetterRetryRunner.createObjectMapper(
                    cryptoEngine, metrics))
            .policy(
                new org.streamrune.core.DeadLetterRetryPolicy(
                    properties.deadLetterMaxRetries(),
                    java.time.Duration.ofMillis(properties.deadLetterRetryIntervalMs()),
                    2.0,
                    false))
            .pollInterval(java.time.Duration.ofMillis(properties.deadLetterRetryIntervalMs()))
            // Metrics bean (MicrometerStreamRuneMetrics when present) so retry-exhaustion emits
            // streamrune.dlq.exhausted + a terminal ERROR log under the default policy; NOOP when
            // absent (behavior unchanged).
            .metrics(metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP);

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
    if (leadership != null) {
      builder.leadership(leadership);
    }

    DeadLetterRetryRunner runner = builder.build();
    // Register for relay liveness health so /health degrades if this runner's thread
    // dies.
    if (relayHealth != null) {
      relayHealth.registerDlqRetryRunner(runner);
    }
    return runner;
  }

  /**
   * Aggregates outbox-relay and DLQ-retry-runner liveness for the health endpoint. The runners
   * register themselves into it when created, so {@code /health} reports DOWN when a started
   * relay's poll thread has died instead of silently reporting UP.
   *
   * @return a new {@link org.streamrune.runtime.BackgroundRelayHealthContributor}
   */
  @Singleton
  @Secondary
  public org.streamrune.runtime.BackgroundRelayHealthContributor
      backgroundRelayHealthContributor() {
    return new org.streamrune.runtime.BackgroundRelayHealthContributor();
  }

  /**
   * Creates an {@link OutboxPoller} when outbox is enabled and both {@link
   * org.streamrune.core.outbox.OutboxStore} and {@link org.streamrune.core.outbox.OutboxPublisher}
   * are present. Opt-in: requires {@code streamrune.outbox.enabled=true}.
   *
   * <p>The {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.outbox.delivery_failed} fires on a terminal FAILED entry (the documented alert
   * metric) and {@code streamrune.outbox.pending} tracks the backlog; falls back to {@link
   * org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists (behavior unchanged).
   *
   * @param store the outbox store
   * @param publisher the outbox publisher
   * @param properties the StreamRune configuration properties
   * @param metrics the optional metrics collector
   * @return a new {@link OutboxPoller}
   */
  @Singleton
  @Secondary
  @Requires(
      beans = {
        org.streamrune.core.outbox.OutboxStore.class,
        org.streamrune.core.outbox.OutboxPublisher.class
      })
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  // Back off entirely when the application defines its own poller.
  @Requires(missingBeans = OutboxPoller.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_OUTBOX_POLLER)
  public OutboxPoller outboxPoller(
      org.streamrune.core.outbox.OutboxStore store,
      org.streamrune.core.outbox.OutboxPublisher publisher,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    OutboxPoller poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .batchSize(properties.outboxBatchSize())
            .pollInterval(java.time.Duration.ofMillis(properties.outboxFlushIntervalMs()))
            // Expose the retry ladder; transport failures are non-counting regardless.
            .retryPolicy(
                new org.streamrune.core.RetryPolicy(
                    properties.outboxRetryMaxAttempts(),
                    java.time.Duration.ofMillis(properties.outboxRetryInitialDelayMs()),
                    properties.outboxRetryMultiplier()))
            .metrics(metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP)
            .build();
    // Register for relay liveness health so /health degrades if the relay thread dies.
    if (relayHealth != null) {
      relayHealth.registerOutboxPoller(poller);
    }
    return poller;
  }

  /**
   * Creates an {@link org.streamrune.runtime.OutboxRetentionSweeper} with two independent windows:
   * it prunes {@code DELIVERED} outbox entries older than {@code
   * streamrune.outbox.retention-max-age} (default 7 days) and operator-skipped {@code SKIPPED}
   * entries older than {@code streamrune.outbox.skipped-retention-max-age} (default 30 days — the
   * audit of the skip decision). Zero or negative disables either window. {@code FAILED} entries
   * are never pruned: an operator resolves them through {@link
   * org.streamrune.runtime.OutboxFailedReplayer}. Opt-in: requires {@code
   * streamrune.outbox.enabled=true} and an {@link org.streamrune.core.outbox.OutboxStore} bean
   * (injected by the interface, as the poller and the event store take it, so a store declared as
   * {@code OutboxStore} is pruned too). Started and stopped by {@link StreamRuneLifecycle}. The
   * {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when present so {@code
   * streamrune.outbox.swept_rows} and {@code streamrune.outbox.skipped_swept} are recorded; falls
   * back to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * @param outboxStore the outbox store to prune
   * @param properties the StreamRune configuration properties
   * @param metrics the optional metrics collector
   * @return a new {@link org.streamrune.runtime.OutboxRetentionSweeper}
   */
  @Singleton
  @Secondary
  @Requires(beans = org.streamrune.core.outbox.OutboxStore.class)
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  // Back off entirely when the application defines its own sweeper —
  // otherwise the framework's 7d default keeps DELETING rows the user's window meant to keep.
  @Requires(missingBeans = org.streamrune.runtime.OutboxRetentionSweeper.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_OUTBOX_RETENTION_SWEEPER)
  public org.streamrune.runtime.OutboxRetentionSweeper outboxRetentionSweeper(
      org.streamrune.core.outbox.OutboxStore outboxStore,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return registerRetentionSweeperHealth(
        relayHealth,
        new org.streamrune.runtime.OutboxRetentionSweeper(
            outboxStore,
            properties.outboxRetentionMaxAge(),
            properties.outboxSkippedRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Creates an {@link org.streamrune.runtime.OutboxFailedReplayer} beside the outbox poller for
   * operator-driven recovery of terminal {@code FAILED} outbox entries: {@code replay(id)} resets
   * one entry to {@code PENDING} in its original position (same {@code seq}), {@code
   * replayFailed(int)} does so oldest-first in bulk, {@code skip(id, by, reason)} moves one entry
   * to the audited {@code SKIPPED} state (releasing its aggregate on a strict channel), and {@code
   * skipFailed(max, by, reason)} is the bulk mirror of {@code replayFailed}. Opt-in: requires
   * {@code streamrune.outbox.enabled=true} and an {@link org.streamrune.core.outbox.OutboxStore}
   * bean. It is <em>not</em> a runner (no lifecycle); the application invokes it from an admin
   * endpoint. The {@link org.streamrune.core.StreamRuneMetrics} bean is wired in when present so
   * {@code streamrune.outbox.replayed} and {@code streamrune.outbox.skipped} are recorded; falls
   * back to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * @param store the outbox store
   * @param metrics the optional metrics collector
   * @return a new {@link org.streamrune.runtime.OutboxFailedReplayer}
   */
  @Singleton
  @Secondary
  @Requires(beans = org.streamrune.core.outbox.OutboxStore.class)
  @Requires(property = "streamrune.outbox.enabled", value = "true")
  public org.streamrune.runtime.OutboxFailedReplayer outboxFailedReplayer(
      org.streamrune.core.outbox.OutboxStore store,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics) {
    return new org.streamrune.runtime.OutboxFailedReplayer(
        store, metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP);
  }

  /**
   * Creates a per-instance {@link org.streamrune.postgres.LeaseBasedLeadership} — a DB-clocked,
   * persisted lease with a monotonic fencing {@code epoch} — so that, with more than one replica of
   * the same projection/subscription running, at most one replica is the active consumer and the
   * others sit in standby. Leadership is a single atomic upsert over {@code subscription_leases}
   * decided by the database clock (no session state, no dedicated connection held across calls), so
   * it is correct under any connection pooling; the leader's monotonic epoch fences a superseded
   * leader out at the projection commit. Requires a {@link DataSource} and {@code
   * streamrune.subscription.single-active-consumer.enabled != false} (default on). The lease TTL is
   * {@code streamrune.subscription.single-active-consumer.lease-ttl} (default 15s) — the failover
   * bound; the renew heartbeat runs at {@code ttl/3}. When absent or disabled the projection
   * runners fall back to {@link org.streamrune.core.subscription.SubscriptionLeadership#NOOP}
   * (always leader — single-instance behavior unchanged). {@link StreamRuneLifecycle} closes it on
   * shutdown, after the runners; the {@code @Bean(preDestroy = "close")} is a safety net that stops
   * the renew heartbeat and resigns held leases on context shutdown even when the runner lifecycle
   * is disabled ({@code streamrune.runner-lifecycle-enabled=false}) and no {@link
   * StreamRuneLifecycle} bean exists — Micronaut does not auto-close {@code @Factory}-produced
   * {@link AutoCloseable} beans without it.
   *
   * @param dataSource the JDBC data source
   * @param properties the StreamRune configuration (supplies the lease TTL)
   * @return a new {@link org.streamrune.postgres.LeaseBasedLeadership}
   */
  @Singleton
  @Secondary
  @Requires(beans = DataSource.class)
  @Requires(
      property = "streamrune.subscription.single-active-consumer.enabled",
      notEquals = "false")
  // Back off entirely when the application supplies its
  // own leadership coordinator. Unlike the inert @Secondary component defaults,
  // LeaseBasedLeadership STARTS a renew-heartbeat thread in its constructor, so leaving the
  // definition registered means a stray heartbeat (and a second lease holder id) the moment
  // anything resolves it by type.
  @Requires(missingBeans = org.streamrune.core.subscription.SubscriptionLeadership.class)
  @Bean(preDestroy = "close")
  public org.streamrune.core.subscription.SubscriptionLeadership subscriptionLeadership(
      DataSource dataSource, StreamRuneMicronautProperties properties) {
    Duration leaseTtl = properties.subscriptionLeaseTtl();
    return new org.streamrune.postgres.LeaseBasedLeadership(
        dataSource, leaseTtl != null ? leaseTtl : Duration.ofSeconds(15));
  }

  /**
   * Creates an {@link InboxRetentionSweeper} that prunes command-inbox rows older than {@code
   * streamrune.inbox.retention-max-age} (default 7 days). Only created when a {@link
   * PostgresCommandInbox} bean is present. The retention window exceeds typical broker redelivery
   * and subscription replay horizons so a pruned key cannot re-admit a duplicate. Started and
   * stopped by {@link StreamRuneLifecycle}. The {@link org.streamrune.core.StreamRuneMetrics} bean
   * is wired in when present so {@code streamrune.inbox.swept_rows} is recorded; falls back to
   * {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * @param commandInbox the command inbox to prune
   * @param properties the StreamRune configuration properties
   * @param metrics the optional metrics collector
   * @return a new {@link InboxRetentionSweeper}
   */
  @Singleton
  @Secondary
  @Requires(beans = PostgresCommandInbox.class)
  // Back off entirely when the application defines its own sweeper.
  @Requires(missingBeans = InboxRetentionSweeper.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_INBOX_RETENTION_SWEEPER)
  public InboxRetentionSweeper inboxRetentionSweeper(
      PostgresCommandInbox commandInbox,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return registerRetentionSweeperHealth(
        relayHealth,
        new InboxRetentionSweeper(
            commandInbox,
            properties.inboxRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Creates a {@link org.streamrune.runtime.SagaDeadLetterRetentionSweeper} that prunes quarantined
   * saga dead-letter entries older than {@code streamrune.saga.dead-letter-retention-max-age}
   * (default 7 days, and must not exceed {@code streamrune.inbox.retention-max-age} — see {@link
   * org.streamrune.integration.SagaRetentionValidator}). Only created when sagas are enabled and a
   * {@link org.streamrune.postgres.PostgresSagaDeadLetterStore} bean is present. Started and
   * stopped by {@link StreamRuneLifecycle}. The {@link org.streamrune.core.StreamRuneMetrics} bean
   * is wired in when present so {@code streamrune.saga.dead_letters_swept} is recorded; falls back
   * to {@link org.streamrune.core.StreamRuneMetrics#NOOP} when no metrics bean exists.
   *
   * @param sagaDeadLetterStore the saga dead-letter store to prune
   * @param properties the StreamRune configuration properties
   * @param metrics the optional metrics collector
   * @return a new {@link org.streamrune.runtime.SagaDeadLetterRetentionSweeper}
   */
  @Singleton
  @Secondary
  @Requires(beans = org.streamrune.postgres.PostgresSagaDeadLetterStore.class)
  @Requires(property = "streamrune.saga.enabled", value = "true", defaultValue = "true")
  // Back off entirely when the application defines its own sweeper.
  @Requires(missingBeans = org.streamrune.runtime.SagaDeadLetterRetentionSweeper.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER)
  public org.streamrune.runtime.SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper(
      org.streamrune.postgres.PostgresSagaDeadLetterStore sagaDeadLetterStore,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return registerRetentionSweeperHealth(
        relayHealth,
        new org.streamrune.runtime.SagaDeadLetterRetentionSweeper(
            sagaDeadLetterStore,
            properties.sagaDeadLetterRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Creates a {@link org.streamrune.runtime.DeadLetterRetentionSweeper} that prunes command
   * dead-letter-queue entries older than {@code streamrune.dead-letter.retention-max-age} (default
   * 30 days). A DLQ entry holds the full command payload (encrypted where {@code @Encrypted} fields
   * apply per) — this bounds how long that payload remains queryable, a GDPR storage-limitation
   * concern. Only created when a {@link org.streamrune.core.DeadLetterQueue} bean is present,
   * mirroring how {@link #deadLetterRetryRunner} is gated on the generic interface rather than a
   * Postgres-specific type — an implementation that does not support retention pruning throws
   * {@code UnsupportedOperationException} from {@code deleteOlderThan}, so the sweeper fails loudly
   * (not silently) on its first cycle.
   *
   * <p>Created for every {@link org.streamrune.core.DeadLetterQueue} bean — the framework registers
   * none itself, so that is always one the application supplied — because the command bus
   * dead-letters into whichever queue bean exists. Unlike {@link #deadLetterRetryRunner} it is
   * therefore <em>not</em> gated on {@code streamrune.dead-letter.enabled}, which switches only the
   * automatic retry: an operator who turns replays off and relies on the admin retry would
   * otherwise keep every exhausted command's payload for ever. A zero or negative {@code
   * retention-max-age} is the switch that turns pruning off (the sweeper then logs that at start).
   * Started and stopped by {@link StreamRuneLifecycle}.
   *
   * @param deadLetterQueue the command dead-letter queue to prune
   * @param properties the StreamRune configuration properties
   * @param metrics the optional metrics collector
   * @return a new {@link org.streamrune.runtime.DeadLetterRetentionSweeper}
   */
  @Singleton
  @Secondary
  @Requires(beans = org.streamrune.core.DeadLetterQueue.class)
  // Back off entirely when the application defines its own sweeper.
  @Requires(missingBeans = org.streamrune.runtime.DeadLetterRetentionSweeper.class)
  @Bean(preDestroy = "close")
  @jakarta.inject.Named(FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER)
  public org.streamrune.runtime.DeadLetterRetentionSweeper deadLetterRetentionSweeper(
      org.streamrune.core.DeadLetterQueue deadLetterQueue,
      StreamRuneMicronautProperties properties,
      @jakarta.annotation.Nullable org.streamrune.core.StreamRuneMetrics metrics,
      @jakarta.annotation.Nullable
          org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth) {
    return registerRetentionSweeperHealth(
        relayHealth,
        new org.streamrune.runtime.DeadLetterRetentionSweeper(
            deadLetterQueue,
            properties.deadLetterRetentionMaxAge(),
            java.time.Duration.ofHours(1),
            java.time.Clock.systemUTC(),
            metrics != null ? metrics : org.streamrune.core.StreamRuneMetrics.NOOP));
  }

  /**
   * Registers a freshly created retention sweeper with the relay-health contributor (when one is
   * present) so {@code /health} reports DOWN if its sweep thread dies, exactly like the outbox
   * poller and the DLQ retry runner. Returns the sweeper for chaining.
   */
  private static <S extends org.streamrune.runtime.RetentionSweeper>
      S registerRetentionSweeperHealth(
          @jakarta.annotation.Nullable
              org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth,
          S sweeper) {
    if (relayHealth != null) {
      relayHealth.registerRetentionSweeper(sweeper);
    }
    return sweeper;
  }

  /**
   * Creates an {@link org.streamrune.core.upcasting.UpcasterChain} when at least one {@link
   * org.streamrune.core.upcasting.EventUpcaster} bean is available.
   *
   * @param upcasters all registered event upcasters
   * @return a new {@link org.streamrune.core.upcasting.UpcasterChain}
   */
  @Singleton
  @Secondary
  @Requires(beans = org.streamrune.core.upcasting.EventUpcaster.class)
  public org.streamrune.core.upcasting.UpcasterChain upcasterChain(
      List<org.streamrune.core.upcasting.EventUpcaster> upcasters) {
    return new org.streamrune.core.upcasting.UpcasterChain(upcasters);
  }
}

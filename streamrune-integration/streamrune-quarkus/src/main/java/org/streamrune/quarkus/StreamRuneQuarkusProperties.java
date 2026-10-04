package org.streamrune.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Configuration properties for StreamRune in Quarkus applications.
 *
 * <p>Maps to the {@code streamrune.} configuration prefix using SmallRye {@code @ConfigMapping}.
 * Property names are the kebab-case form of the accessor names, e.g. {@code
 * streamrune.snapshot-every-n-events} or {@code streamrune.retry-max-attempts}. The
 * saga/outbox/dead-letter/metrics/query-cache groups nest, e.g. {@code streamrune.outbox.enabled} —
 * the same keys as the Spring and Micronaut integrations, so configuration is portable across
 * runtimes.
 *
 * <p>Quarkus registers {@code @ConfigMapping} interfaces found in indexed archives — this library
 * ships both a {@code META-INF/beans.xml} marker and a Jandex index, so the mapping is picked up by
 * any application that depends on this module; no extension build step is required.
 */
@ConfigMapping(prefix = "streamrune")
public interface StreamRuneQuarkusProperties {

  /** Snapshot is taken every N events per aggregate. */
  @WithDefault("100")
  int snapshotEveryNEvents();

  /** Maximum number of command retry attempts on concurrency conflicts. */
  @WithDefault("3")
  int retryMaxAttempts();

  /** Initial delay in milliseconds before the first command retry. */
  @WithDefault("50")
  long retryInitialDelayMs();

  /** Multiplier applied to the retry delay after each failed attempt. */
  @WithDefault("2.0")
  double retryBackoffMultiplier();

  /**
   * Catch-up polling interval in milliseconds for the event subscriptions of auto-discovered
   * CONTINUOUS projections.
   */
  @WithDefault("5000")
  long pollingIntervalMs();

  /** Random jitter added to the polling interval, in milliseconds. */
  @WithDefault("1000")
  long pollingJitterMs();

  /** Maximum time to wait for an aggregate lock. */
  @WithDefault("PT5S")
  Duration lockTimeout();

  /** Number of stripes in the local aggregate locker. */
  @WithDefault("1024")
  int stripeCount();

  /**
   * Admission-control budget for {@code executeAsync} — commands admitted and not yet complete
   * beyond this many are refused immediately with {@code CommandBusOverloadedException}; the
   * synchronous {@code execute} path is unaffected.
   */
  @WithDefault("10000")
  int maxInFlightAsyncCommands();

  /** Consecutive failures before the circuit breaker opens. */
  @WithDefault("5")
  int circuitBreakerFailureThreshold();

  /** How long the circuit breaker stays open before allowing a probe. */
  @WithDefault("PT30S")
  Duration circuitBreakerCooldown();

  /** Whether per-event audit logging to the event audit store is enabled. */
  @WithDefault("false")
  boolean eventAuditEnabled();

  /** Whether the projection dead-letter store is enabled. */
  @WithDefault("false")
  boolean projectionDlqEnabled();

  /**
   * Query cache configuration, {@code streamrune.query-cache.*}. Note that {@code
   * streamrune.query-cache.enabled} is read at build time ({@code @IfBuildProperty}) to decide
   * whether the {@code CachingQueryBus} bean exists.
   */
  QueryCacheConfig queryCache();

  /** Event-store configuration, {@code streamrune.event-store.*}. */
  EventStoreConfig eventStore();

  /** Saga configuration, {@code streamrune.saga.*}. */
  SagaConfig saga();

  /** Outbox configuration, {@code streamrune.outbox.*}. */
  OutboxConfig outbox();

  /** Command-inbox configuration, {@code streamrune.inbox.*}. */
  InboxConfig inbox();

  /** Dead-letter retry configuration, {@code streamrune.dead-letter.*}. */
  DeadLetterConfig deadLetter();

  /**
   * Subscription (single-active-consumer leadership) configuration, {@code
   * streamrune.subscription.*}.
   */
  SubscriptionConfig subscription();

  /** Metrics configuration, {@code streamrune.metrics.*}. */
  MetricsConfig metrics();

  /** Whether the Bean Validation command interceptor is enabled. */
  @WithDefault("true")
  boolean validationEnabled();

  /** Whether the startup banner/log line is emitted. */
  @WithDefault("true")
  boolean startupLog();

  /** Event-metadata configuration, {@code streamrune.metadata.*} (baggage allowlisting). */
  MetadataConfig metadata();

  /** Security configuration, {@code streamrune.security.*} (authorization-identity source). */
  SecurityConfig security();

  /** Server-Sent Events configuration, {@code streamrune.sse.*}. */
  SseConfig sse();

  /** Event-store section, {@code streamrune.event-store.*}. */
  interface EventStoreConfig {

    /** Event-store schema management configuration, {@code streamrune.event-store.schema.*}. */
    SchemaConfig schema();

    /**
     * The bound on every statement the event store runs, {@code
     * streamrune.event-store.statement-timeout}. A statement past it is cancelled and the operation
     * fails. Applied per transaction or per statement, never on the pooled connection's session.
     * {@code PT0S} sets no bound of the store's own, so the session's {@code statement_timeout}
     * applies; a negative value or a positive one below one millisecond fails the boot.
     */
    @WithDefault("PT30S")
    Duration statementTimeout();

    /** Schema management section, {@code streamrune.event-store.schema.*}. */
    interface SchemaConfig {

      /**
       * Whether the event store auto-creates / migrates its schema on startup using the bundled
       * Flyway migrations.
       */
      @WithDefault("true")
      boolean autoInitialize();
    }
  }

  /** Saga section, {@code streamrune.saga.*}. */
  interface SagaConfig {

    /** Whether the PostgreSQL saga store bean is produced. */
    @WithDefault("true")
    boolean enabled();

    /**
     * How long to keep quarantined saga dead-letter entries before the {@link
     * org.streamrune.runtime.SagaDeadLetterRetentionSweeper} deletes them. Zero or negative
     * disables retention pruning. Use ISO-8601 duration syntax, e.g. {@code PT168H} (7 days).
     *
     * <p>Defaults to the inbox retention window (7d) and MUST NOT exceed {@code
     * streamrune.inbox.retention-max-age}: a mid-compensation dead-letter is replayable throughout
     * this window and its replay re-derives the original command-inbox dedup keys, so if it
     * outlives those keys a replay re-executes an already-succeeded compensation (double refund).
     * The saga lifecycle fails fast on a config that violates this.
     */
    @WithDefault("PT168H")
    Duration deadLetterRetentionMaxAge();

    /**
     * Whether the auto-wired {@link org.streamrune.runtime.SagaCompensationRetrySweeper} (one per
     * {@code SagaRunner} bean) re-drives stuck {@code COMPENSATING} sagas — the durable recovery
     * path for a saga with no timeout runner. {@code false} keeps the sweeper running in
     * sampling-only mode so {@code streamrune.saga.compensating} / {@code faulted_rows} stay live.
     */
    @WithDefault("true")
    boolean compensationRetryEnabled();

    /**
     * How often the compensation-retry sweeper re-drives, and the age below which a just-claimed
     * compensation episode is not yet re-driven (so it does not race the event path). ISO-8601,
     * e.g. {@code PT60S}.
     */
    @WithDefault("PT60S")
    Duration compensationRetryInterval();

    /**
     * How long a saga may stay {@code COMPENSATING} before the sweeper terminalizes it {@code
     * FAULTED} (poison bound). ISO-8601, e.g. {@code PT1H}.
     */
    @WithDefault("PT1H")
    Duration compensationRetryGiveUpAfter();
  }

  /** Outbox section, {@code streamrune.outbox.*}. */
  interface OutboxConfig {

    /** Whether the outbox poller is enabled (requires an OutboxStore and OutboxPublisher bean). */
    @WithDefault("false")
    boolean enabled();

    /** Number of outbox entries published per poll. */
    @WithDefault("100")
    int batchSize();

    /** Outbox poll interval, in milliseconds. */
    @WithDefault("5000")
    long flushIntervalMs();

    /**
     * How long to keep delivered outbox entries before the {@link
     * org.streamrune.runtime.OutboxRetentionSweeper} deletes them. Zero or negative disables
     * retention pruning. Use ISO-8601 duration syntax, e.g. {@code PT168H} (7 days).
     */
    @WithDefault("PT168H")
    Duration retentionMaxAge();

    /**
     * How long to keep {@code SKIPPED} outbox entries — the audit of an operator's decision that an
     * entry will never be delivered — before the {@link
     * org.streamrune.runtime.OutboxRetentionSweeper} deletes them. Zero or negative disables the
     * SKIPPED sweep. {@code FAILED} entries are never pruned. Use ISO-8601 duration syntax, e.g.
     * {@code PT720H} (30 days).
     */
    @WithDefault("PT720H")
    Duration skippedRetentionMaxAge();

    /**
     * Delivery attempts a per-entry rejection may accrue before the entry is marked terminal {@code
     * FAILED}. Broker-wide transport failures are non-counting and never terminal-fail regardless
     * of this. Default 10.
     */
    @WithDefault("10")
    int retryMaxAttempts();

    /**
     * Initial retry backoff in milliseconds; grows by {@link #retryMultiplier} per attempt, capped
     * at 60s. Default 1000.
     */
    @WithDefault("1000")
    long retryInitialDelayMs();

    /** Exponential backoff multiplier applied to {@link #retryInitialDelayMs} each attempt. */
    @WithDefault("2.0")
    double retryMultiplier();
  }

  /** Command-inbox section, {@code streamrune.inbox.*}. */
  interface InboxConfig {

    /**
     * How long to keep processed command-inbox rows before the {@link
     * org.streamrune.runtime.InboxRetentionSweeper} deletes them. Zero or negative disables
     * retention pruning. Must exceed the maximum at-least-once redelivery horizon so a pruned key
     * cannot re-admit a duplicate. Use ISO-8601 duration syntax, e.g. {@code PT168H} (7 days).
     */
    @WithDefault("PT168H")
    Duration retentionMaxAge();
  }

  /** Dead-letter retry section, {@code streamrune.dead-letter.*}. */
  interface DeadLetterConfig {

    /**
     * Whether the automatic dead-letter retry runner is enabled (requires a DeadLetterQueue bean).
     * This switches only the automatic replays: the command bus keeps dead-lettering into the queue
     * bean, entries can still be retried on demand, and the retention sweeper keeps pruning (see
     * {@link #retentionMaxAge()}).
     */
    @WithDefault("true")
    boolean enabled();

    /** Maximum dead-letter retry attempts per command. */
    @WithDefault("5")
    int maxRetries();

    /** Interval between dead-letter retry polls, in milliseconds. */
    @WithDefault("60000")
    long retryIntervalMs();

    /**
     * How long to keep command dead-letter-queue entries (which hold command payloads, encrypted
     * where {@code @Encrypted} fields apply) before the {@link
     * org.streamrune.runtime.DeadLetterRetentionSweeper} deletes them. Zero or negative disables
     * retention pruning. Independent of {@link #enabled()}. Use ISO-8601 duration syntax, e.g.
     * {@code PT720H} (30 days).
     */
    @WithDefault("PT720H")
    Duration retentionMaxAge();
  }

  /** Metrics section, {@code streamrune.metrics.*}. */
  interface MetricsConfig {

    /** Whether Micrometer metrics are enabled (requires a MeterRegistry bean). */
    @WithDefault("true")
    boolean enabled();

    /** Prefix for all StreamRune metric names. */
    @WithDefault("streamrune")
    String prefix();
  }

  /** Subscription section, {@code streamrune.subscription.*}. */
  interface SubscriptionConfig {

    /** Single-active-consumer section, {@code streamrune.subscription.single-active-consumer.*}. */
    SingleActiveConsumerConfig singleActiveConsumer();

    /** Subscription health section, {@code streamrune.subscription.health.*}. */
    HealthConfig health();

    /** Subscription health section, {@code streamrune.subscription.health.*}. */
    interface HealthConfig {

      /**
       * The lag, in events between the head of the event stream and a subscription's checkpoint, at
       * or above which a running subscription is reported {@code DEGRADED} in the health data. Lag
       * never turns the readiness check {@code DOWN}: only a stopped or terminally halted
       * subscription does. At least 1. Default 1000.
       */
      @WithDefault("1000")
      long lagThreshold();
    }

    /** Single-active-consumer section, {@code streamrune.subscription.single-active-consumer.*}. */
    interface SingleActiveConsumerConfig {

      /**
       * Whether a {@code LeaseBasedLeadership} bean is produced (when a {@code DataSource} is
       * present) so at most one replica of each projection/subscription is the active consumer.
       * When {@code false} or no {@code DataSource} is present, runners receive {@code
       * SubscriptionLeadership.NOOP} (always leader — single-instance behavior).
       */
      @WithDefault("true")
      boolean enabled();

      /**
       * How long a granted lease stays live before another replica may take it over — the upper
       * bound on failover latency (a standby wins the name at most {@code leaseTtl} after a leader
       * stops renewing). The renew heartbeat runs at {@code leaseTtl/3}. Default 15s.
       */
      @WithDefault("PT15S")
      Duration leaseTtl();
    }
  }

  /** Query cache section, {@code streamrune.query-cache.*}. */
  interface QueryCacheConfig {

    /**
     * Whether query caching is enabled. Read at build time: when {@code true} the {@code
     * CachingQueryBus} and {@code CacheInvalidator} beans are produced and the primary {@code
     * QueryBus} routes through the cache.
     */
    @WithDefault("false")
    boolean enabled();
  }

  /** Security section, {@code streamrune.security.*}. */
  interface SecurityConfig {

    /**
     * The explicit trusted-gateway mode ({@code RequestIdentityPolicy}). When {@code true}, the
     * request filter and the SSE endpoint take the authorization identity ({@code
     * RequestContext.userId}) from the {@code X-User-Id} header — even when Quarkus Security is
     * present. Enable it only when a gateway in front of the service authenticates every caller,
     * overwrites {@code X-User-Id} with the authenticated id and strips any client-supplied value,
     * and the service is reachable only through that gateway; otherwise any client can act as any
     * user. Defaults to {@code false}: the identity is the authenticated {@code SecurityIdentity}
     * when an {@code AuthenticatedUserResolver} is available (Quarkus Security present), and
     * without one every request is anonymous and the header is ignored. In that anonymous mode a
     * configured {@code CommandAuthorizationPolicy} or annotation authorization ({@code
     * UserRoleResolver}) refuses startup (Quarkus REST applications).
     */
    @WithDefault("false")
    boolean trustUserIdHeader();
  }

  /** Server-Sent Events section, {@code streamrune.sse.*}. */
  interface SseConfig {

    /**
     * Whether the {@code GET /api/sse/{aggregateType}/{aggregateId}} Server-Sent Events endpoint is
     * exposed. Disabled by default: it streams an aggregate's decrypted domain events by the stream
     * named in the path, so it must be enabled deliberately and guarded by an {@link
     * org.streamrune.integration.SseAuthorizer} bean (the framework installs a fail-closed deny-all
     * authorizer if none is provided). Read at build time ({@code @IfBuildProperty}) to decide
     * whether the SSE resource is registered.
     */
    @WithDefault("false")
    boolean enabled();

    /**
     * How long an SSE stream may stay open before the server completes it. Finite by default so a
     * silently-dead client (half-open TCP — a mobile/NAT drop with no FIN/RST) on an idle stream is
     * reaped: the stream completes, the {@code SseEventPublisher} subscription, its delivery
     * worker, and the socket FD are released, and the client auto-reconnects. An unbounded stream
     * would let dead subscriptions accumulate across reconnect churn until FD exhaustion. Zero or
     * negative disables the timeout, leaving the keepalive as the sole reaper — disabling both
     * knobs re-opens the dead-client leak. Matches {@code streamrune.sse.timeout} on Spring and
     * Micronaut: that advertised parity is real (Spring used to map a non-positive timeout back to
     * its 5-minute default, so the same key meant opposite things per framework).
     */
    @WithDefault("PT5M")
    Duration timeout();

    /**
     * How often a keepalive comment frame is written to every SSE subscriber. Detection requires a
     * write: an idle stream writes nothing on its own, so a half-open peer is invisible. The
     * comment frame ({@code : keepalive}) is ignored by SSE clients but forces a socket write,
     * which fails for a dead peer and evicts the subscription well before the {@link #timeout()}
     * backstop. Zero or negative disables the keepalive. Matches {@code
     * streamrune.sse.keep-alive-interval} on Spring and Micronaut.
     */
    @WithDefault("PT30S")
    Duration keepAliveInterval();
  }

  /** Event-metadata section, {@code streamrune.metadata.*}. */
  interface MetadataConfig {

    /**
     * Additional OpenTelemetry baggage keys (beyond the framework-meaningful default — the
     * causation-id key) permitted to flow from client-supplied, unauthenticated {@code baggage:}
     * header entries into {@code RequestContext}/{@code EventMetadata} baggage, and from there into
     * the immutable, plaintext {@code event_stream.metadata} column. Every other baggage key is
     * dropped at the request filter. Listing {@code role} has no effect: that key is the {@code
     * X-User-Role} header in the trusted-gateway mode, and nothing else. Comma-separated, e.g.
     * {@code streamrune.metadata.baggage-allowlist=tenant,region}. {@code Optional} (no
     * {@code @WithDefault}) rather than defaulting to an empty string: SmallRye's collection
     * converter treats an empty-string default for a {@code List} property as invalid, not as an
     * empty list.
     */
    Optional<List<String>> baggageAllowlist();
  }
}

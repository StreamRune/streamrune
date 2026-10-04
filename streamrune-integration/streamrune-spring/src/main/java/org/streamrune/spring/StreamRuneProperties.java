package org.streamrune.spring;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration properties for StreamRune framework under the {@code streamrune} prefix.
 *
 * <p>Bound via constructor binding: each record has exactly one (canonical) constructor and
 * declares its defaults with {@link DefaultValue}. Do not add extra constructors — Spring Boot
 * cannot deduce a bind constructor for a record with multiple constructors and would silently fall
 * back to JavaBean binding, which ignores every configured value.
 *
 * @param snapshotEveryNEvents number of events between automatic snapshots
 * @param retryMaxAttempts maximum retry attempts on optimistic lock failure
 * @param retryInitialDelayMs initial delay in milliseconds before first retry
 * @param retryBackoffMultiplier multiplier applied to delay between successive retries
 * @param pollingIntervalMs catch-up polling interval in milliseconds for auto-configured projection
 *     subscriptions
 * @param pollingJitterMs jitter added to polling interval in milliseconds
 * @param lockTimeout lock acquisition timeout
 * @param stripeCount number of stripes for striped locker
 * @param maxInFlightAsyncCommands admission-control budget for {@code executeAsync} — commands
 *     admitted and not yet complete beyond this many are refused immediately with {@code
 *     CommandBusOverloadedException}; the synchronous {@code execute} path is unaffected
 * @param circuitBreakerFailureThreshold number of consecutive failures before circuit opens
 * @param circuitBreakerCooldown how long to wait in OPEN state before probing
 * @param eventAuditEnabled whether event audit trail is enabled
 * @param projectionDlqEnabled whether projection dead letter queue is enabled
 * @param queryCache query-result cache configuration
 * @param saga saga configuration
 * @param outbox outbox configuration
 * @param inbox command-inbox configuration
 * @param deadLetter dead letter retry configuration
 * @param subscription subscription configuration (single-active-consumer leadership, health)
 * @param metrics metrics configuration
 * @param validationEnabled whether command validation interceptor is enabled
 * @param startupLog whether to log StreamRune banner at startup
 * @param eventStore event-store configuration
 * @param metadata event-metadata configuration (baggage allowlisting)
 * @param security security configuration (authorization-identity source)
 * @param sse Server-Sent Events endpoint configuration
 */
@ConfigurationProperties(prefix = "streamrune")
public record StreamRuneProperties(
    @DefaultValue("100") int snapshotEveryNEvents,
    @DefaultValue("3") int retryMaxAttempts,
    @DefaultValue("50") long retryInitialDelayMs,
    @DefaultValue("2.0") double retryBackoffMultiplier,
    @DefaultValue("5000") long pollingIntervalMs,
    @DefaultValue("1000") long pollingJitterMs,
    @DefaultValue("5s") Duration lockTimeout,
    @DefaultValue("1024") int stripeCount,
    @DefaultValue("10000") int maxInFlightAsyncCommands,
    @DefaultValue("5") int circuitBreakerFailureThreshold,
    @DefaultValue("30s") Duration circuitBreakerCooldown,
    @DefaultValue("false") boolean eventAuditEnabled,
    @DefaultValue("false") boolean projectionDlqEnabled,
    @DefaultValue QueryCache queryCache,
    @DefaultValue Saga saga,
    @DefaultValue Outbox outbox,
    @DefaultValue Inbox inbox,
    @DefaultValue DeadLetter deadLetter,
    @DefaultValue Subscription subscription,
    @DefaultValue Metrics metrics,
    @DefaultValue("true") boolean validationEnabled,
    @DefaultValue("true") boolean startupLog,
    @DefaultValue EventStore eventStore,
    @DefaultValue Metadata metadata,
    @DefaultValue Security security,
    @DefaultValue Sse sse) {

  /**
   * @param enabled whether {@link org.streamrune.runtime.CachingQueryBus} is auto-configured
   */
  public record QueryCache(@DefaultValue("false") boolean enabled) {}

  /**
   * @param enabled whether saga auto-configuration is enabled
   * @param deadLetterRetentionMaxAge how long to keep quarantined saga dead-letter entries before
   *     the retention sweeper prunes them; zero or negative disables pruning. Defaults to the inbox
   *     retention window (7d) and MUST NOT exceed {@code streamrune.inbox.retention-max-age}: a
   *     dead-letter that faulted mid-compensation is replayable throughout this window, and its
   *     replay re-derives the original command-inbox dedup keys, so if it outlives those keys a
   *     replay re-executes an already-succeeded compensation (double refund). The saga auto-config
   *     fails fast on a config that violates this (see {@code SagaRetentionValidator})
   * @param compensationRetryEnabled whether the auto-wired compensation-retry sweeper (one per
   *     {@code SagaRunner} bean) re-drives stuck {@code COMPENSATING} sagas (default on) — the
   *     durable recovery path for a saga with no timeout runner. {@code false} keeps the sweeper
   *     running in sampling-only mode so {@code streamrune.saga.compensating} / {@code
   *     faulted_rows} stay live
   * @param compensationRetryInterval how often the sweeper re-drives, and the age below which a
   *     just-claimed compensation episode is not yet re-driven (so it does not race the event path)
   * @param compensationRetryGiveUpAfter how long a saga may stay {@code COMPENSATING} before the
   *     sweeper terminalizes it {@code FAULTED} (poison bound)
   */
  public record Saga(
      @DefaultValue("true") boolean enabled,
      @DefaultValue("7d") Duration deadLetterRetentionMaxAge,
      @DefaultValue("true") boolean compensationRetryEnabled,
      @DefaultValue("60s") Duration compensationRetryInterval,
      @DefaultValue("1h") Duration compensationRetryGiveUpAfter) {}

  /**
   * @param enabled whether outbox polling is enabled (opt-in)
   * @param batchSize max events per poll cycle
   * @param flushIntervalMs polling interval in milliseconds
   * @param retentionMaxAge how long to keep delivered outbox entries before the retention sweeper
   *     prunes them; zero or negative disables pruning
   * @param skippedRetentionMaxAge how long to keep SKIPPED outbox entries — the audit of an
   *     operator's decision that an entry will never be delivered — before the retention sweeper
   *     prunes them; zero or negative disables the SKIPPED sweep. FAILED entries are never pruned
   *     (an operator resolves them with OutboxFailedReplayer.replay or .skip). Keep this at least
   *     as long as your audit requires; default 30d.
   * @param retryMaxAttempts delivery attempts a per-entry rejection may accrue before the entry is
   *     marked terminal {@code FAILED}. Broker-wide transport failures are non-counting and never
   *     terminal-fail regardless of this. Default 10.
   * @param retryInitialDelayMs initial retry backoff in milliseconds; grows by {@link
   *     #retryMultiplier} per attempt, capped at 60s. Default 1000.
   * @param retryMultiplier exponential backoff multiplier applied to {@link #retryInitialDelayMs}
   *     each attempt. Default 2.0.
   */
  public record Outbox(
      @DefaultValue("false") boolean enabled,
      @DefaultValue("100") int batchSize,
      @DefaultValue("5000") long flushIntervalMs,
      @DefaultValue("7d") Duration retentionMaxAge,
      @DefaultValue("30d") Duration skippedRetentionMaxAge,
      @DefaultValue("10") int retryMaxAttempts,
      @DefaultValue("1000") long retryInitialDelayMs,
      @DefaultValue("2.0") double retryMultiplier) {}

  /**
   * @param singleActiveConsumer single-active-consumer (subscription leadership) configuration
   * @param health subscription health configuration
   */
  public record Subscription(
      @DefaultValue SingleActiveConsumer singleActiveConsumer, @DefaultValue Health health) {

    /**
     * @param lagThreshold the lag, in events between the head of the event stream and a
     *     subscription's checkpoint, at or above which a running subscription is reported {@code
     *     DEGRADED} in the health details. Lag never turns the health check {@code DOWN}: only a
     *     stopped or terminally halted subscription does. At least 1. Default 1000.
     */
    public record Health(@DefaultValue("1000") long lagThreshold) {}

    /**
     * @param enabled whether a {@code LeaseBasedLeadership} bean is auto-configured (when a {@code
     *     DataSource} is present) so at most one replica of each projection/subscription is the
     *     active consumer. When {@code false} or no {@code DataSource} is present, runners receive
     *     {@code SubscriptionLeadership.NOOP} (always leader — single-instance behavior).
     * @param leaseTtl how long a granted lease stays live before another replica may take it over.
     *     This is the upper bound on failover latency: when a leader stops renewing (crash, pause,
     *     graceful close) a standby wins the name at most {@code leaseTtl} later. The renew
     *     heartbeat runs at {@code leaseTtl/3}. Default 15s.
     */
    public record SingleActiveConsumer(
        @DefaultValue("true") boolean enabled, @DefaultValue("15s") Duration leaseTtl) {}
  }

  /**
   * @param retentionMaxAge how long to keep processed command-inbox rows before the retention
   *     sweeper prunes them; zero or negative disables pruning. Must exceed the maximum
   *     at-least-once redelivery horizon so a pruned key cannot re-admit a duplicate.
   */
  public record Inbox(@DefaultValue("7d") Duration retentionMaxAge) {}

  /**
   * @param enabled whether the automatic dead letter retry runner is enabled. This switches only
   *     the automatic replays: the command bus keeps dead-lettering into any {@code
   *     DeadLetterQueue} bean, entries can still be retried on demand, and the retention sweeper
   *     keeps pruning
   * @param maxRetries max retry attempts before permanent failure
   * @param retryIntervalMs interval between retry attempts in milliseconds
   * @param retentionMaxAge how long to keep command dead-letter-queue entries (which hold command
   *     payloads, encrypted where {@code @Encrypted} fields apply) before the retention sweeper
   *     prunes them; zero or negative disables pruning. Independent of {@code enabled}
   */
  public record DeadLetter(
      @DefaultValue("true") boolean enabled,
      @DefaultValue("5") int maxRetries,
      @DefaultValue("60000") long retryIntervalMs,
      @DefaultValue("30d") Duration retentionMaxAge) {}

  /**
   * @param enabled whether Micrometer metrics publishing is enabled
   * @param prefix metric name prefix
   */
  public record Metrics(
      @DefaultValue("true") boolean enabled, @DefaultValue("streamrune") String prefix) {}

  /**
   * @param schema event-store schema management configuration
   * @param statementTimeout the bound on every statement the event store runs (default {@code
   *     30s}); a statement past it is cancelled and the operation fails. {@code 0} sets no bound of
   *     the store's own, so the session's {@code statement_timeout} applies. Applied per
   *     transaction or per statement, never on the pooled connection's session. A negative value or
   *     a positive one below {@code 1ms} fails the boot.
   */
  public record EventStore(
      @DefaultValue Schema schema, @DefaultValue("30s") Duration statementTimeout) {}

  /**
   * @param autoInitialize whether the event store auto-creates / migrates its schema on startup
   *     (Flyway)
   */
  public record Schema(@DefaultValue("true") boolean autoInitialize) {}

  /**
   * @param baggageAllowlist additional OpenTelemetry baggage keys (beyond the framework-meaningful
   *     default — the causation-id key) permitted to flow from client-supplied, unauthenticated
   *     {@code baggage:} header entries into {@code RequestContext}/{@code EventMetadata} baggage,
   *     and from there into the immutable, plaintext {@code event_stream.metadata} column. Every
   *     other baggage key is dropped at the request filter. Listing {@code role} has no effect:
   *     that key is the {@code X-User-Role} header in the trusted-gateway mode, and nothing else.
   */
  public record Metadata(@DefaultValue List<String> baggageAllowlist) {

    public Metadata {
      baggageAllowlist = baggageAllowlist == null ? List.of() : List.copyOf(baggageAllowlist);
    }
  }

  /**
   * @param trustUserIdHeader the explicit trusted-gateway mode. When {@code true}, the request
   *     filter and the SSE endpoint take the authorization identity ({@code RequestContext.userId})
   *     from the {@code X-User-Id} header — even when Spring Security is present. Enable it only
   *     when a gateway in front of the service authenticates every caller, overwrites {@code
   *     X-User-Id} with the authenticated id and strips any client-supplied value, and the service
   *     is reachable only through that gateway; otherwise any client can act as any user. Defaults
   *     to {@code false}: the identity is the authenticated principal when an {@code
   *     AuthenticatedUserResolver} is available (Spring Security on the classpath), and without one
   *     every request is anonymous and the header is ignored. In that anonymous mode a configured
   *     {@code CommandAuthorizationPolicy} or annotation authorization ({@code UserRoleResolver})
   *     refuses startup.
   */
  public record Security(@DefaultValue("false") boolean trustUserIdHeader) {}

  /**
   * @param enabled whether the auto-registered {@code GET /api/sse/{aggregateType}/{aggregateId}}
   *     Server-Sent Events endpoint is exposed. Disabled by default: the endpoint streams an
   *     aggregate's decrypted domain events by the stream named in the path, so it must be enabled
   *     deliberately and guarded by an {@link org.streamrune.integration.SseAuthorizer} bean (the
   *     framework installs a fail-closed deny-all authorizer if none is provided).
   * @param timeout the SSE emitter timeout. Finite by default so a silently-dead client (half-open
   *     TCP, e.g. mobile/NAT drop with no FIN/RST) on an idle stream is reaped — {@code onTimeout}
   *     fires, the subscription/worker/socket are released, and the client reconnects. An unbounded
   *     timeout would let dead subscriptions accumulate until FD exhaustion. Zero or negative
   *     disables the timeout, leaving the keepalive as the sole reaper — the same contract as
   *     {@code streamrune.sse.timeout} on Quarkus and Micronaut. Disabling both knobs re-opens the
   *     dead-client FD leak.
   * @param keepAliveInterval how often a keepalive comment frame is written to every subscriber,
   *     turning a dead connection into a failed send that promptly evicts it — detection well
   *     before the {@code timeout} backstop. {@link Duration#ZERO} (or negative) disables the
   *     keepalive, leaving the timeout as the sole reaper.
   */
  public record Sse(
      @DefaultValue("false") boolean enabled,
      @DefaultValue("5m") Duration timeout,
      @DefaultValue("30s") Duration keepAliveInterval) {}
}

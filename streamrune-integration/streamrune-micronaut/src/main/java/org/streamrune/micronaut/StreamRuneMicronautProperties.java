package org.streamrune.micronaut;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import java.time.Duration;
import java.util.List;

/**
 * Configuration properties for StreamRune in Micronaut applications.
 *
 * <p>Maps to {@code streamrune.} configuration prefix. This record is the single source of truth
 * for StreamRune configuration — Micronaut binds it directly from configuration via the canonical
 * constructor, with {@link Bindable#defaultValue() defaults} applied for any key that is not set.
 *
 * <p>Do not register a second bean of this type (e.g., from a {@code @Factory}); that would cause a
 * {@code NonUniqueBeanException} for every property-dependent bean.
 */
@ConfigurationProperties("streamrune")
public record StreamRuneMicronautProperties(
    // Explicit name: Micronaut would hyphenate the single-letter hump to "snapshot-every-nevents",
    // while the documented key (and the Spring/Quarkus integrations) use "snapshot-every-n-events".
    @Bindable(defaultValue = "100")
        @io.micronaut.context.annotation.Property(name = "streamrune.snapshot-every-n-events")
        int snapshotEveryNEvents,
    @Bindable(defaultValue = "3") int retryMaxAttempts,
    @Bindable(defaultValue = "50") long retryInitialDelayMs,
    @Bindable(defaultValue = "2.0") double retryBackoffMultiplier,
    @Bindable(defaultValue = "5000") long pollingIntervalMs,
    @Bindable(defaultValue = "1000") long pollingJitterMs,
    @Bindable(defaultValue = "5s") Duration lockTimeout,
    @Bindable(defaultValue = "1024") int stripeCount,
    // Admission-control budget for executeAsync — commands admitted and
    // not yet complete beyond this many are refused immediately with CommandBusOverloadedException;
    // the synchronous execute path is unaffected.
    @Bindable(defaultValue = "10000") int maxInFlightAsyncCommands,
    @Bindable(defaultValue = "5") int circuitBreakerFailureThreshold,
    @Bindable(defaultValue = "30s") Duration circuitBreakerCooldown,
    @Bindable(defaultValue = "false") boolean eventAuditEnabled,
    @Bindable(defaultValue = "false") boolean projectionDlqEnabled,
    // Explicit names below: the saga/outbox/dead-letter/metrics groups use the nested key scheme
    // (streamrune.saga.enabled, ...) shared with the Spring integration, so configuration is
    // portable across runtimes. Without @Property, Micronaut would derive the flattened
    // "streamrune.saga-enabled" form from the component name.
    @Bindable(defaultValue = "true")
        @io.micronaut.context.annotation.Property(name = "streamrune.saga.enabled")
        boolean sagaEnabled,
    // saga.dead-letter-retention-max-age: matches Spring's
    // streamrune.saga.dead-letter-retention-max-age. Defaults to the inbox retention window (7d)
    // and must not exceed streamrune.inbox.retention-max-age — a mid-compensation dead-letter that
    // outlives its inbox dedup keys re-executes an already-succeeded compensation on replay (double
    // refund); the saga lifecycle fails fast on a violating config.
    @Bindable(defaultValue = "7d")
        @io.micronaut.context.annotation.Property(
            name = "streamrune.saga.dead-letter-retention-max-age")
        Duration sagaDeadLetterRetentionMaxAge,
    @Bindable(defaultValue = "false")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.enabled")
        boolean outboxEnabled,
    @Bindable(defaultValue = "100")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.batch-size")
        int outboxBatchSize,
    @Bindable(defaultValue = "5000")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.flush-interval-ms")
        long outboxFlushIntervalMs,
    @Bindable(defaultValue = "true")
        @io.micronaut.context.annotation.Property(name = "streamrune.dead-letter.enabled")
        boolean deadLetterEnabled,
    @Bindable(defaultValue = "5")
        @io.micronaut.context.annotation.Property(name = "streamrune.dead-letter.max-retries")
        int deadLetterMaxRetries,
    @Bindable(defaultValue = "60000")
        @io.micronaut.context.annotation.Property(name = "streamrune.dead-letter.retry-interval-ms")
        long deadLetterRetryIntervalMs,
    // dead-letter.retention-max-age: matches Spring's streamrune.dead-letter.retention-max-age
    @Bindable(defaultValue = "30d")
        @io.micronaut.context.annotation.Property(name = "streamrune.dead-letter.retention-max-age")
        Duration deadLetterRetentionMaxAge,
    @Bindable(defaultValue = "true")
        @io.micronaut.context.annotation.Property(name = "streamrune.metrics.enabled")
        boolean metricsEnabled,
    @Bindable(defaultValue = "streamrune")
        @io.micronaut.context.annotation.Property(name = "streamrune.metrics.prefix")
        String metricsPrefix,
    @Bindable(defaultValue = "true") boolean validationEnabled,
    @Bindable(defaultValue = "true") boolean startupLog,
    // event-store.schema.auto-initialize: matches Spring's
    // streamrune.event-store.schema.auto-initialize
    @Bindable(defaultValue = "true")
        @io.micronaut.context.annotation.Property(
            name = "streamrune.event-store.schema.auto-initialize")
        boolean autoInitializeSchema,
    // outbox.retention-max-age: matches Spring's streamrune.outbox.retention-max-age
    @Bindable(defaultValue = "7d")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.retention-max-age")
        Duration outboxRetentionMaxAge,
    // inbox.retention-max-age: matches Spring's streamrune.inbox.retention-max-age
    @Bindable(defaultValue = "7d")
        @io.micronaut.context.annotation.Property(name = "streamrune.inbox.retention-max-age")
        Duration inboxRetentionMaxAge,
    // metadata.baggage-allowlist: matches Spring's streamrune.metadata.baggage-allowlist.
    // Additional OTel baggage keys permitted into RequestContext/EventMetadata baggage, on top
    // of the framework default (the causation-id key; `role` is the trusted-gateway X-User-Role
    // header and nothing else, so listing it has no effect) — see BaggageAllowlist. @Nullable
    // (rather than @Bindable(defaultValue = "")) so an unset property binds to null instead of a
    // single-element list containing ""; the compact constructor below normalizes null to
    // List.of().
    @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(name = "streamrune.metadata.baggage-allowlist")
        List<String> metadataBaggageAllowlist,
    // outbox.skipped-retention-max-age: matches Spring's
    // streamrune.outbox.skipped-retention-max-age. How long to keep SKIPPED outbox entries (the
    // audit of an operator's skip) before the OutboxRetentionSweeper prunes them; zero or negative
    // disables the SKIPPED sweep. FAILED entries are never pruned. Same position and
    // default as the FAILED window it replaces, so the positional withDefaults() stays aligned.
    @Bindable(defaultValue = "30d")
        @io.micronaut.context.annotation.Property(
            name = "streamrune.outbox.skipped-retention-max-age")
        Duration outboxSkippedRetentionMaxAge,
    // subscription.single-active-consumer.enabled: matches Spring's
    // streamrune.subscription.single-active-consumer.enabled. Whether a
    // LeaseBasedLeadership bean is produced (when a DataSource is present) so at most one
    // replica of each projection/subscription is the active consumer. When false or no DataSource
    // is present, runners receive SubscriptionLeadership.NOOP (always leader).
    @Bindable(defaultValue = "true")
        @io.micronaut.context.annotation.Property(
            name = "streamrune.subscription.single-active-consumer.enabled")
        boolean subscriptionSingleActiveConsumerEnabled,
    // Outbox retry knobs — appended at the end so the positional withDefaults() stays
    // aligned. Delivery attempts a per-entry rejection may accrue before terminal FAILED; transport
    // failures are non-counting regardless (see OutboxPoller.recordFailure).
    @Bindable(defaultValue = "10")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.retry-max-attempts")
        int outboxRetryMaxAttempts,
    // Initial retry backoff (ms); grows by the multiplier per attempt, capped at 60s.
    @Bindable(defaultValue = "1000")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.retry-initial-delay-ms")
        long outboxRetryInitialDelayMs,
    @Bindable(defaultValue = "2.0")
        @io.micronaut.context.annotation.Property(name = "streamrune.outbox.retry-multiplier")
        double outboxRetryMultiplier,
    // subscription.single-active-consumer.lease-ttl: matches Spring/Quarkus. How long a granted
    // lease stays live before another replica may take it over — the failover bound; the
    // LeaseBasedLeadership renew heartbeat runs at leaseTtl/3. @Nullable with a "15s" default so
    // binding fills it while the factory defends against an explicit null (defaults to PT15S).
    // Appended at the end so the positional withDefaults() stays aligned.
    @Bindable(defaultValue = "15s")
        @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(
            name = "streamrune.subscription.single-active-consumer.lease-ttl")
        Duration subscriptionLeaseTtl,
    // sse.timeout / sse.keep-alive-interval: the dead-client reaping knobs, with
    // the same keys and defaults as Spring and Quarkus. Detecting a half-open peer requires a
    // WRITE, so an idle stream needs the periodic keepalive comment frame; the finite timeout is
    // the backstop that completes the stream (SSE clients auto-reconnect). Zero or negative
    // disables either one — that timeout contract genuinely holds on all three
    // frameworks (Spring used to map a non-positive timeout back to its 5-minute default).
    // Disabling both knobs re-opens the dead-client FD leak. Appended at the end so the
    // positional withDefaults() stays aligned.
    @Bindable(defaultValue = "5m")
        @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(name = "streamrune.sse.timeout")
        Duration sseTimeout,
    @Bindable(defaultValue = "30s")
        @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(name = "streamrune.sse.keep-alive-interval")
        Duration sseKeepAliveInterval,
    // event-store.statement-timeout: matches Spring/Quarkus. The bound on every statement the
    // event store runs, applied per transaction or per statement and never on the pooled
    // connection's session; 0 sets no bound of the store's own. A negative value or a positive one
    // below 1ms fails the boot. @Nullable with a "30s" default so binding fills it while the
    // factory method keeps the factory's default for an explicit null. Appended at the end so the
    // positional withDefaults() stays aligned.
    @Bindable(defaultValue = "30s")
        @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(name = "streamrune.event-store.statement-timeout")
        Duration eventStoreStatementTimeout,
    // sse.polling-interval: matches Spring/Quarkus. How often the SSE endpoint's feed reads the
    // global stream for new events — the upper bound on the delay between a commit and its frame.
    // @Nullable with a "1s" default so binding fills it while the feed lifecycle keeps the default
    // for an explicit null; a non-positive value fails the boot. Appended at the end so the
    // positional withDefaults() stays aligned.
    @Bindable(defaultValue = "1s")
        @io.micronaut.core.annotation.Nullable
        @io.micronaut.context.annotation.Property(name = "streamrune.sse.polling-interval")
        Duration ssePollingInterval) {

  public StreamRuneMicronautProperties {
    metadataBaggageAllowlist =
        metadataBaggageAllowlist == null ? List.of() : List.copyOf(metadataBaggageAllowlist);
  }

  /**
   * Creates properties with all defaults — convenience for tests and manual wiring. A static
   * factory rather than a no-arg constructor: Micronaut must only see the canonical constructor,
   * otherwise configuration binding silently falls back to defaults.
   *
   * @return properties with all defaults
   */
  public static StreamRuneMicronautProperties withDefaults() {
    return new StreamRuneMicronautProperties(
        100,
        3,
        50,
        2.0,
        5000,
        1000,
        Duration.ofSeconds(5),
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        // sagaDeadLetterRetentionMaxAge: defaults to the inbox window (7d), must not exceed it.
        Duration.ofDays(7),
        false,
        100,
        5000L,
        true,
        5,
        60000L,
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7),
        Duration.ofDays(7),
        List.of(),
        // outbox.skipped-retention-max-age (the SKIPPED audit window; FAILED is never pruned)
        Duration.ofDays(30),
        true,
        // Outbox retry knobs: maxAttempts, initialDelayMs, multiplier.
        10,
        1000L,
        2.0,
        // subscription.single-active-consumer.lease-ttl
        Duration.ofSeconds(15),
        // sse.timeout / sse.keep-alive-interval
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1));
  }
}

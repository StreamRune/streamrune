package org.streamrune.spring;

import java.util.Set;

/**
 * The keys {@link ConfigurationSpellingCheck} holds to the spelling rules, written out beside the
 * properties types they come from: {@link StreamRuneProperties}, {@link StreamRuneCryptoProperties}
 * and the conditions of the integration's auto-configurations.
 *
 * <p>Constants, not a walk of the record components or of the auto-configuration class files at
 * startup: a native image keeps neither the record components of a constructor-bound properties
 * record nor the class files and the {@code AutoConfiguration.imports} file as resources, so such a
 * walk throws or finds nothing there. {@code StreamRunePropertyKeysTest} derives the same sets from
 * the record components and the class files on the JVM and fails when a constant drifts from them.
 */
final class StreamRunePropertyKeys {

  private StreamRunePropertyKeys() {}

  /** Every {@code Duration} component of the properties types, as Spring names its key. */
  static final Set<String> DURATIONS =
      Set.of(
          "streamrune.circuit-breaker-cooldown",
          "streamrune.crypto.aws.data-key-cache.expire-after-write",
          "streamrune.crypto.cache.expire-after-access",
          "streamrune.crypto.cache.expire-after-write",
          "streamrune.dead-letter.retention-max-age",
          "streamrune.dead-letter.retry-interval",
          "streamrune.event-store.statement-timeout",
          "streamrune.inbox.retention-max-age",
          "streamrune.lock-timeout",
          "streamrune.outbox.flush-interval",
          "streamrune.outbox.retention-max-age",
          "streamrune.outbox.retry-initial-delay",
          "streamrune.outbox.skipped-retention-max-age",
          "streamrune.polling-interval",
          "streamrune.polling-jitter",
          "streamrune.retry-initial-delay",
          "streamrune.saga.compensation-retry-give-up-after",
          "streamrune.saga.compensation-retry-interval",
          "streamrune.saga.dead-letter-retention-max-age",
          "streamrune.sse.keep-alive-interval",
          "streamrune.sse.polling-interval",
          "streamrune.sse.timeout",
          "streamrune.subscription.single-active-consumer.lease-ttl");

  /**
   * Every {@code boolean} component of the properties types, and every {@code streamrune.*} key an
   * auto-configuration's {@code ConditionalOnProperty} compares with {@code true} or {@code false}:
   * the crypto backend switches are read by conditions only.
   */
  static final Set<String> SWITCHES =
      Set.of(
          "streamrune.crypto.aws.enabled",
          "streamrune.crypto.cache.enabled",
          "streamrune.crypto.filesystem.enabled",
          "streamrune.crypto.postgres.enabled",
          "streamrune.crypto.vault.enabled",
          "streamrune.dead-letter.enabled",
          "streamrune.event-audit-enabled",
          "streamrune.event-store.schema.auto-initialize",
          "streamrune.jvm.allow-graal-jit",
          "streamrune.metrics.enabled",
          "streamrune.outbox.enabled",
          "streamrune.projection-dlq-enabled",
          "streamrune.query-cache.enabled",
          "streamrune.saga.compensation-retry-enabled",
          "streamrune.saga.enabled",
          "streamrune.security.trust-user-id-header",
          "streamrune.sse.enabled",
          "streamrune.startup-log",
          "streamrune.tracing.command.enabled",
          "streamrune.tracing.enabled",
          "streamrune.tracing.event-store.enabled",
          "streamrune.tracing.lock.enabled",
          "streamrune.tracing.outbox.enabled",
          "streamrune.tracing.projection.enabled",
          "streamrune.tracing.saga.enabled",
          "streamrune.tracing.subscription.enabled",
          "streamrune.validation-enabled");
}

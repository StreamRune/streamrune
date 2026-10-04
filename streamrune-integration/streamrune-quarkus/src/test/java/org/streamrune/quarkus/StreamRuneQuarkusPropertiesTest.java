package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StreamRuneQuarkusProperties}. Binds the {@code @ConfigMapping} through a real
 * SmallRyeConfig — the same registration path Quarkus uses for indexed library archives — so these
 * tests verify the mapping actually resolves without an extension build step.
 */
class StreamRuneQuarkusPropertiesTest {

  @Test
  void defaultsMatchDocumented() {
    var p = TestProperties.defaults();
    assertEquals(100, p.snapshotEveryNEvents());
    assertEquals(3, p.retryMaxAttempts());
    assertEquals(50L, p.retryInitialDelayMs());
    assertEquals(2.0, p.retryBackoffMultiplier());
    assertEquals(5000L, p.pollingIntervalMs());
    assertEquals(1000L, p.pollingJitterMs());
    assertEquals(Duration.ofSeconds(5), p.lockTimeout());
    assertEquals(1024, p.stripeCount());
    assertEquals(5, p.circuitBreakerFailureThreshold());
    assertEquals(Duration.ofSeconds(30), p.circuitBreakerCooldown());
    assertFalse(p.eventAuditEnabled());
    assertFalse(p.projectionDlqEnabled());
    assertTrue(p.saga().enabled());
    // Defaults to the inbox retention window (7d), not longer.
    assertEquals(Duration.ofDays(7), p.saga().deadLetterRetentionMaxAge());
    assertFalse(p.outbox().enabled());
    assertEquals(100, p.outbox().batchSize());
    assertEquals(5000L, p.outbox().flushIntervalMs());
    assertEquals(Duration.ofDays(7), p.outbox().retentionMaxAge());
    assertEquals(Duration.ofDays(7), p.inbox().retentionMaxAge());
    assertTrue(p.eventStore().schema().autoInitialize());
    assertTrue(p.deadLetter().enabled());
    assertEquals(5, p.deadLetter().maxRetries());
    assertEquals(60000L, p.deadLetter().retryIntervalMs());
    assertTrue(p.metrics().enabled());
    assertEquals("streamrune", p.metrics().prefix());
    assertTrue(p.validationEnabled());
    assertTrue(p.startupLog());
    assertTrue(p.metadata().baggageAllowlist().isEmpty());
  }

  @Test
  void baggageAllowlistBindsCommaSeparatedDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.metadata.baggage-allowlist", "tracking,tenant"));
    assertEquals(
        java.util.List.of("tracking", "tenant"), p.metadata().baggageAllowlist().orElseThrow());
  }

  @Test
  void eventStoreSchemaAutoInitializeBindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.event-store.schema.auto-initialize", "false"));
    assertFalse(p.eventStore().schema().autoInitialize());
  }

  @Test
  void eventStoreStatementTimeoutDefaultsTo30SecondsAndBindsTheDocumentedKey() {
    assertEquals(Duration.ofSeconds(30), TestProperties.defaults().eventStore().statementTimeout());
    var p = TestProperties.of(Map.of("streamrune.event-store.statement-timeout", "PT2M"));
    assertEquals(Duration.ofMinutes(2), p.eventStore().statementTimeout());
    var none = TestProperties.of(Map.of("streamrune.event-store.statement-timeout", "PT0S"));
    assertEquals(Duration.ZERO, none.eventStore().statementTimeout());
  }

  @Test
  void outboxRetentionMaxAgeBindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.outbox.retention-max-age", "PT336H"));
    assertEquals(Duration.ofDays(14), p.outbox().retentionMaxAge());
  }

  @Test
  void outboxRetentionMaxAgeCanBeDisabled() {
    var p = TestProperties.of(Map.of("streamrune.outbox.retention-max-age", "PT0S"));
    assertTrue(p.outbox().retentionMaxAge().isZero());
  }

  @Test
  void outboxSkippedRetentionMaxAgeDefaultsTo30Days() {
    assertEquals(Duration.ofDays(30), TestProperties.defaults().outbox().skippedRetentionMaxAge());
  }

  @Test
  void outboxSkippedRetentionMaxAgeBindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.outbox.skipped-retention-max-age", "PT1440H"));
    assertEquals(Duration.ofDays(60), p.outbox().skippedRetentionMaxAge());
  }

  @Test
  void outboxSkippedRetentionMaxAgeCanBeDisabled() {
    var p = TestProperties.of(Map.of("streamrune.outbox.skipped-retention-max-age", "PT0S"));
    assertTrue(p.outbox().skippedRetentionMaxAge().isZero());
  }

  @Test
  void singleActiveConsumerEnabledByDefault() {
    assertTrue(TestProperties.defaults().subscription().singleActiveConsumer().enabled());
  }

  @Test
  void leaseTtlDefaultsToFifteenSeconds() {
    assertEquals(
        Duration.ofSeconds(15),
        TestProperties.defaults().subscription().singleActiveConsumer().leaseTtl());
  }

  @Test
  void sseReapingKnobsDefaultToFiveMinutesAndThirtySeconds() {
    // Knob parity with Spring and Micronaut. Without these an operator could not
    // even mitigate leaking half-open SSE clients by configuration.
    assertEquals(Duration.ofMinutes(5), TestProperties.defaults().sse().timeout());
    assertEquals(Duration.ofSeconds(30), TestProperties.defaults().sse().keepAliveInterval());
  }

  @Test
  void sseReapingKnobsBindDocumentedKeys() {
    var p =
        TestProperties.of(
            Map.of(
                "streamrune.sse.timeout", "PT2M",
                "streamrune.sse.keep-alive-interval", "PT5S"));
    assertEquals(Duration.ofMinutes(2), p.sse().timeout());
    assertEquals(Duration.ofSeconds(5), p.sse().keepAliveInterval());
  }

  @Test
  void leaseTtlBindsDocumentedKey() {
    var p =
        TestProperties.of(
            Map.of("streamrune.subscription.single-active-consumer.lease-ttl", "PT30S"));
    assertEquals(Duration.ofSeconds(30), p.subscription().singleActiveConsumer().leaseTtl());
  }

  @Test
  void singleActiveConsumerCanBeDisabled() {
    var p =
        TestProperties.of(
            Map.of("streamrune.subscription.single-active-consumer.enabled", "false"));
    assertFalse(p.subscription().singleActiveConsumer().enabled());
  }

  @Test
  void inboxRetentionMaxAgeBindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.inbox.retention-max-age", "PT336H"));
    assertEquals(Duration.ofDays(14), p.inbox().retentionMaxAge());
  }

  @Test
  void inboxRetentionMaxAgeCanBeDisabled() {
    var p = TestProperties.of(Map.of("streamrune.inbox.retention-max-age", "PT0S"));
    assertTrue(p.inbox().retentionMaxAge().isZero());
  }

  @Test
  void sagaDeadLetterRetentionMaxAgeBindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.saga.dead-letter-retention-max-age", "PT1440H"));
    assertEquals(Duration.ofDays(60), p.saga().deadLetterRetentionMaxAge());
  }

  @Test
  void sagaDeadLetterRetentionMaxAgeCanBeDisabled() {
    var p = TestProperties.of(Map.of("streamrune.saga.dead-letter-retention-max-age", "PT0S"));
    assertTrue(p.saga().deadLetterRetentionMaxAge().isZero());
  }

  @Test
  void bindsKebabCasePropertiesFromConfig() {
    var p =
        TestProperties.of(
            Map.of(
                "streamrune.snapshot-every-n-events", "50",
                "streamrune.retry-max-attempts", "5",
                "streamrune.retry-initial-delay-ms", "10",
                "streamrune.retry-backoff-multiplier", "1.5",
                "streamrune.lock-timeout", "PT1S",
                "streamrune.stripe-count", "16",
                "streamrune.circuit-breaker-failure-threshold", "10",
                "streamrune.circuit-breaker-cooldown", "PT60S",
                "streamrune.event-audit-enabled", "true",
                "streamrune.metrics.prefix", "custom"));
    assertEquals(50, p.snapshotEveryNEvents());
    assertEquals(5, p.retryMaxAttempts());
    assertEquals(10L, p.retryInitialDelayMs());
    assertEquals(1.5, p.retryBackoffMultiplier());
    assertEquals(Duration.ofSeconds(1), p.lockTimeout());
    assertEquals(16, p.stripeCount());
    assertEquals(10, p.circuitBreakerFailureThreshold());
    assertEquals(Duration.ofSeconds(60), p.circuitBreakerCooldown());
    assertTrue(p.eventAuditEnabled());
    assertEquals("custom", p.metrics().prefix());
  }

  @Test
  void nestedGroupKeysBindFromConfig_matchingSpringScheme() {
    // The saga/outbox/dead-letter/metrics groups use the same nested keys as the Spring and
    // Micronaut integrations (streamrune.saga.enabled, ...) so configuration is portable.
    var p =
        TestProperties.of(
            Map.of(
                "streamrune.saga.enabled", "false",
                "streamrune.outbox.enabled", "true",
                "streamrune.outbox.batch-size", "42",
                "streamrune.outbox.flush-interval-ms", "1234",
                "streamrune.dead-letter.enabled", "false",
                "streamrune.dead-letter.retry-interval-ms", "9876",
                "streamrune.metrics.enabled", "false"));
    assertFalse(p.saga().enabled());
    assertTrue(p.outbox().enabled());
    assertEquals(42, p.outbox().batchSize());
    assertEquals(1234L, p.outbox().flushIntervalMs());
    assertFalse(p.deadLetter().enabled());
    assertEquals(9876L, p.deadLetter().retryIntervalMs());
    assertFalse(p.metrics().enabled());
    assertEquals(5, p.deadLetter().maxRetries(), "unset keys must keep defaults");
  }

  @Test
  void queryCacheEnabled_defaultFalse() {
    var p = TestProperties.defaults();
    assertFalse(p.queryCache().enabled());
  }

  @Test
  void queryCacheEnabled_bindsDocumentedKey() {
    var p = TestProperties.of(Map.of("streamrune.query-cache.enabled", "true"));
    assertTrue(p.queryCache().enabled());
  }

  @Test
  void cryptoMappingBindsWithDefaultsOnly_optionalSecretsAbsent() {
    // Regression: token/region/kms-key-id are Optional so the mapping must
    // validate without them — otherwise every app without Vault/AWS fails startup.
    var crypto = TestProperties.crypto(Map.of());
    assertFalse(crypto.filesystem().enabled());
    assertFalse(crypto.postgres().enabled());
    assertFalse(crypto.vault().enabled());
    assertTrue(crypto.vault().token().isEmpty());
    assertFalse(crypto.aws().enabled());
    assertTrue(crypto.aws().region().isEmpty());
    assertTrue(crypto.aws().kmsKeyId().isEmpty());
    assertTrue(crypto.cache().enabled());
    assertEquals(10000, crypto.cache().maximumSize());
    assertTrue(crypto.cache().expireAfterWriteRaw().isEmpty());
    assertTrue(crypto.cache().expireAfterAccessRaw().isEmpty());
  }

  @Test
  void cryptoMappingBindsDocumentedKeys() {
    var crypto =
        TestProperties.crypto(
            Map.of(
                "streamrune.crypto.filesystem.enabled", "true",
                "streamrune.crypto.filesystem.key-directory", "/tmp/keys",
                "streamrune.crypto.vault.token", "s.token",
                "streamrune.crypto.aws.region", "eu-central-1",
                "streamrune.crypto.aws.kms-key-id", "key-1",
                "streamrune.crypto.cache.expire-after-write", "PT5M"));
    assertTrue(crypto.filesystem().enabled());
    assertEquals("/tmp/keys", crypto.filesystem().keyDirectory());
    assertEquals("s.token", crypto.vault().token().orElseThrow());
    assertEquals("eu-central-1", crypto.aws().region().orElseThrow());
    assertEquals("key-1", crypto.aws().kmsKeyId().orElseThrow());
    assertEquals("PT5M", crypto.cache().expireAfterWriteRaw().orElseThrow());
  }
}

package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * Tests for {@link StreamRuneProperties}. Properties are constructor-bound, so all tests exercise
 * the real binding path via {@link Binder} — both defaults (no sources) and overrides.
 */
class StreamRunePropertiesTest {

  private static StreamRuneProperties bind(Map<String, String> source) {
    return new Binder(new MapConfigurationPropertySource(source))
        .bindOrCreate("streamrune", Bindable.of(StreamRuneProperties.class));
  }

  private static StreamRuneProperties defaults() {
    return bind(Map.of());
  }

  @Test
  void defaultsMatchDocumented() {
    var p = defaults();
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
  }

  @Test
  void configuredValuesOverrideDefaults() {
    var p =
        bind(
            Map.of(
                "streamrune.snapshot-every-n-events", "10",
                "streamrune.retry-max-attempts", "1",
                "streamrune.retry-initial-delay-ms", "1",
                "streamrune.retry-backoff-multiplier", "1.0",
                "streamrune.polling-interval-ms", "100",
                "streamrune.polling-jitter-ms", "10",
                "streamrune.lock-timeout", "500ms",
                "streamrune.stripe-count", "8",
                "streamrune.circuit-breaker-failure-threshold", "3",
                "streamrune.circuit-breaker-cooldown", "10s"));
    assertEquals(10, p.snapshotEveryNEvents());
    assertEquals(1, p.retryMaxAttempts());
    assertEquals(1L, p.retryInitialDelayMs());
    assertEquals(1.0, p.retryBackoffMultiplier());
    assertEquals(100L, p.pollingIntervalMs());
    assertEquals(10L, p.pollingJitterMs());
    assertEquals(Duration.ofMillis(500), p.lockTimeout());
    assertEquals(8, p.stripeCount());
    assertEquals(3, p.circuitBreakerFailureThreshold());
    assertEquals(Duration.ofSeconds(10), p.circuitBreakerCooldown());
  }

  @Test
  void queryCache_disabledByDefault() {
    assertFalse(defaults().queryCache().enabled());
  }

  @Test
  void queryCache_canBeEnabled() {
    var p = bind(Map.of("streamrune.query-cache.enabled", "true"));
    assertTrue(p.queryCache().enabled());
  }

  @Test
  void defaultPropertiesShouldHaveSagaEnabled() {
    assertThat(defaults().saga().enabled()).isTrue();
  }

  @Test
  void defaultPropertiesShouldHaveSagaDeadLetterRetentionMaxAge() {
    // Defaults to the inbox retention window (7d), not longer — a dead-letter must not
    // outlive the inbox dedup keys its replay re-derives.
    assertThat(defaults().saga().deadLetterRetentionMaxAge()).isEqualTo(Duration.ofDays(7));
  }

  @Test
  void sagaDeadLetterRetentionMaxAgeBindsConfiguredValueAndCanBeDisabled() {
    var configured = bind(Map.of("streamrune.saga.dead-letter-retention-max-age", "60d"));
    assertThat(configured.saga().deadLetterRetentionMaxAge()).isEqualTo(Duration.ofDays(60));

    var disabled = bind(Map.of("streamrune.saga.dead-letter-retention-max-age", "0d"));
    assertThat(disabled.saga().deadLetterRetentionMaxAge()).isZero();
  }

  @Test
  void defaultPropertiesShouldHaveOutboxDisabled() {
    var props = defaults();
    assertThat(props.outbox().enabled()).isFalse();
    assertThat(props.outbox().batchSize()).isEqualTo(100);
    assertThat(props.outbox().flushIntervalMs()).isEqualTo(5000L);
    assertThat(props.outbox().retentionMaxAge()).isEqualTo(Duration.ofDays(7));
  }

  @Test
  void outboxRetentionMaxAgeCanBeDisabled() {
    var props = bind(Map.of("streamrune.outbox.retention-max-age", "0d"));
    assertThat(props.outbox().retentionMaxAge()).isZero();
  }

  @Test
  void defaultPropertiesShouldHaveOutboxSkippedRetentionMaxAge() {
    assertThat(defaults().outbox().skippedRetentionMaxAge()).isEqualTo(Duration.ofDays(30));
  }

  @Test
  void outboxSkippedRetentionMaxAgeBindsConfiguredValueAndCanBeDisabled() {
    var configured = bind(Map.of("streamrune.outbox.skipped-retention-max-age", "60d"));
    assertThat(configured.outbox().skippedRetentionMaxAge()).isEqualTo(Duration.ofDays(60));
    var disabled = bind(Map.of("streamrune.outbox.skipped-retention-max-age", "0d"));
    assertThat(disabled.outbox().skippedRetentionMaxAge()).isZero();
  }

  @Test
  void defaultPropertiesShouldHaveSingleActiveConsumerEnabled() {
    assertThat(defaults().subscription().singleActiveConsumer().enabled()).isTrue();
  }

  @Test
  void defaultLeaseTtlIsFifteenSeconds() {
    assertThat(defaults().subscription().singleActiveConsumer().leaseTtl())
        .isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void leaseTtlIsConfigurable() {
    var props = bind(Map.of("streamrune.subscription.single-active-consumer.lease-ttl", "30s"));
    assertThat(props.subscription().singleActiveConsumer().leaseTtl())
        .isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void singleActiveConsumerCanBeDisabled() {
    var props = bind(Map.of("streamrune.subscription.single-active-consumer.enabled", "false"));
    assertThat(props.subscription().singleActiveConsumer().enabled()).isFalse();
  }

  @Test
  void defaultPropertiesShouldHaveInboxRetentionMaxAge() {
    assertThat(defaults().inbox().retentionMaxAge()).isEqualTo(Duration.ofDays(7));
  }

  @Test
  void inboxRetentionMaxAgeBindsConfiguredValueAndCanBeDisabled() {
    var configured = bind(Map.of("streamrune.inbox.retention-max-age", "14d"));
    assertThat(configured.inbox().retentionMaxAge()).isEqualTo(Duration.ofDays(14));

    var disabled = bind(Map.of("streamrune.inbox.retention-max-age", "0d"));
    assertThat(disabled.inbox().retentionMaxAge()).isZero();
  }

  @Test
  void defaultPropertiesShouldHaveDeadLetterEnabled() {
    var props = defaults();
    assertThat(props.deadLetter().enabled()).isTrue();
    assertThat(props.deadLetter().maxRetries()).isEqualTo(5);
    assertThat(props.deadLetter().retryIntervalMs()).isEqualTo(60000L);
  }

  @Test
  void defaultPropertiesShouldHaveDeadLetterRetentionMaxAge() {
    assertThat(defaults().deadLetter().retentionMaxAge()).isEqualTo(Duration.ofDays(30));
  }

  @Test
  void deadLetterRetentionMaxAgeBindsConfiguredValueAndCanBeDisabled() {
    var configured = bind(Map.of("streamrune.dead-letter.retention-max-age", "60d"));
    assertThat(configured.deadLetter().retentionMaxAge()).isEqualTo(Duration.ofDays(60));

    var disabled = bind(Map.of("streamrune.dead-letter.retention-max-age", "0d"));
    assertThat(disabled.deadLetter().retentionMaxAge()).isZero();
  }

  @Test
  void defaultPropertiesShouldHaveMetricsEnabled() {
    var props = defaults();
    assertThat(props.metrics().enabled()).isTrue();
    assertThat(props.metrics().prefix()).isEqualTo("streamrune");
  }

  @Test
  void nestedPropertiesBindConfiguredValues() {
    var p =
        bind(
            Map.of(
                "streamrune.metrics.prefix", "myapp.es",
                "streamrune.outbox.batch-size", "7",
                "streamrune.dead-letter.max-retries", "2",
                "streamrune.saga.enabled", "false"));
    assertThat(p.metrics().prefix()).isEqualTo("myapp.es");
    assertThat(p.outbox().batchSize()).isEqualTo(7);
    assertThat(p.deadLetter().maxRetries()).isEqualTo(2);
    assertThat(p.saga().enabled()).isFalse();
  }

  @Test
  void defaultPropertiesShouldHaveValidationEnabled() {
    assertThat(defaults().validationEnabled()).isTrue();
  }

  @Test
  void defaultPropertiesShouldHaveStartupLogEnabled() {
    assertThat(defaults().startupLog()).isTrue();
  }

  @Test
  void defaultPropertiesShouldHaveEmptyBaggageAllowlist() {
    assertThat(defaults().metadata().baggageAllowlist()).isEmpty();
  }

  @Test
  void baggageAllowlistBindsCommaSeparatedConfiguredValue() {
    var p = bind(Map.of("streamrune.metadata.baggage-allowlist", "tracking,tenant"));
    assertThat(p.metadata().baggageAllowlist()).containsExactly("tracking", "tenant");
  }
}

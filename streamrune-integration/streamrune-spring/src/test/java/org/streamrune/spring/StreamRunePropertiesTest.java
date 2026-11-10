package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertEquals(Duration.ofMillis(50), p.retryInitialDelay());
    assertEquals(2.0, p.retryBackoffMultiplier());
    assertEquals(Duration.ofSeconds(5), p.pollingInterval());
    assertEquals(Duration.ofSeconds(1), p.pollingJitter());
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
                "streamrune.retry-initial-delay", "1ms",
                "streamrune.retry-backoff-multiplier", "1.0",
                "streamrune.polling-interval", "100ms",
                "streamrune.polling-jitter", "10ms",
                "streamrune.lock-timeout", "500ms",
                "streamrune.stripe-count", "8",
                "streamrune.circuit-breaker-failure-threshold", "3",
                "streamrune.circuit-breaker-cooldown", "10s"));
    assertEquals(10, p.snapshotEveryNEvents());
    assertEquals(1, p.retryMaxAttempts());
    assertEquals(Duration.ofMillis(1), p.retryInitialDelay());
    assertEquals(1.0, p.retryBackoffMultiplier());
    assertEquals(Duration.ofMillis(100), p.pollingInterval());
    assertEquals(Duration.ofMillis(10), p.pollingJitter());
    assertEquals(Duration.ofMillis(500), p.lockTimeout());
    assertEquals(8, p.stripeCount());
    assertEquals(3, p.circuitBreakerFailureThreshold());
    assertEquals(Duration.ofSeconds(10), p.circuitBreakerCooldown());
  }

  /**
   * The six interval keys are durations: each binds from the short form the configuration reference
   * writes ({@code 5s}) and from ISO-8601 ({@code PT5S}) alike.
   */
  @Test
  void theIntervalKeysBindTheShortFormAndIso8601() {
    for (String value : new String[] {"5s", "PT5S"}) {
      var p =
          bind(
              Map.of(
                  "streamrune.retry-initial-delay", value,
                  "streamrune.polling-interval", value,
                  "streamrune.polling-jitter", value,
                  "streamrune.dead-letter.retry-interval", value,
                  "streamrune.outbox.flush-interval", value,
                  "streamrune.outbox.retry-initial-delay", value));
      Duration five = Duration.ofSeconds(5);
      assertEquals(five, p.retryInitialDelay(), value);
      assertEquals(five, p.pollingInterval(), value);
      assertEquals(five, p.pollingJitter(), value);
      assertEquals(five, p.deadLetter().retryInterval(), value);
      assertEquals(five, p.outbox().flushInterval(), value);
      assertEquals(five, p.outbox().retryInitialDelay(), value);
    }
    assertEquals(
        2.5,
        bind(Map.of("streamrune.outbox.retry-backoff-multiplier", "2.5"))
            .outbox()
            .retryBackoffMultiplier());
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
  void sagaForwardFenceDefaultsToAuto() {
    assertThat(defaults().saga().forwardFence()).isEqualTo("auto");
  }

  @Test
  void sagaForwardFenceBindsConfiguredValue() {
    assertThat(bind(Map.of("streamrune.saga.forward-fence", "off")).saga().forwardFence())
        .isEqualTo("off");
  }

  /**
   * YAML 1.1 reads an unquoted {@code on} as a boolean, so the binder hands over the text {@code
   * true}; the setting reads it as on.
   */
  @Test
  void sagaForwardFenceBindsAnUnquotedYamlOn() throws Exception {
    var yaml = new org.springframework.boot.env.YamlPropertySourceLoader();
    var loaded =
        yaml.load(
            "application",
            new org.springframework.core.io.ByteArrayResource(
                "streamrune:\n  saga:\n    forward-fence: on\n".getBytes()));
    var bound =
        new Binder(
                org.springframework.boot.context.properties.source.ConfigurationPropertySources
                    .from(loaded.getFirst()))
            .bindOrCreate("streamrune", Bindable.of(StreamRuneProperties.class));
    assertThat(bound.saga().forwardFence()).isEqualTo("true");
    assertThat(
            org.streamrune.integration.SagaForwardFenceSetting.parse(bound.saga().forwardFence()))
        .isEqualTo(org.streamrune.integration.SagaForwardFenceSetting.ON);
  }

  @Test
  void defaultPropertiesShouldHaveOutboxDisabled() {
    var props = defaults();
    assertThat(props.outbox().enabled()).isFalse();
    assertThat(props.outbox().batchSize()).isEqualTo(100);
    assertThat(props.outbox().flushInterval()).isEqualTo(Duration.ofSeconds(5));
    assertThat(props.outbox().retryInitialDelay()).isEqualTo(Duration.ofSeconds(1));
    assertThat(props.outbox().retryBackoffMultiplier()).isEqualTo(2.0);
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
    assertThat(
            org.streamrune.integration.SingleActiveConsumerSetting.enabled(
                defaults().subscription().singleActiveConsumer().enabled()))
        .isTrue();
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
    assertThat(
            org.streamrune.integration.SingleActiveConsumerSetting.enabled(
                props.subscription().singleActiveConsumer().enabled()))
        .isFalse();
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
    assertThat(props.deadLetter().retryInterval()).isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  void defaultPropertiesShouldHaveDeadLetterRetentionMaxAge() {
    // The inbox window: a keyed entry must not outlive the inbox row that deduplicates its replay.
    assertThat(defaults().deadLetter().retentionMaxAge())
        .isEqualTo(defaults().inbox().retentionMaxAge())
        .isEqualTo(Duration.ofDays(7));
  }

  @Test
  void deadLetterUnkeyedReplayDefaultsToOperatorOnlyAndBindsAutomatic() {
    assertThat(defaults().deadLetter().unkeyedReplay()).isEqualTo("operator-only");
    assertThat(defaults().deadLetter().unkeyedReplayPolicy())
        .isEqualTo(org.streamrune.core.DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
    assertThat(
            bind(Map.of("streamrune.dead-letter.unkeyed-replay", "automatic"))
                .deadLetter()
                .unkeyedReplayPolicy())
        .isEqualTo(org.streamrune.core.DeadLetterRetryPolicy.UnkeyedReplay.AUTOMATIC);
  }

  @Test
  void deadLetterUnkeyedReplayRefusesAnUnknownValue() {
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> bind(Map.of("streamrune.dead-letter.unkeyed-replay", "yes")))
        .hasStackTraceContaining("streamrune.dead-letter.unkeyed-replay");
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

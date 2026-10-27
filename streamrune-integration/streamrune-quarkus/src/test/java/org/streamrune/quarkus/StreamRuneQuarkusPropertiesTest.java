package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.runtime.configuration.DurationConverter;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
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
    // The inbox window: a keyed entry must not outlive the inbox row that deduplicates its replay.
    assertEquals(Duration.ofDays(7), p.deadLetter().retentionMaxAge());
    assertEquals("operator-only", p.deadLetter().unkeyedReplay());
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
    assertTrue(
        org.streamrune.integration.SingleActiveConsumerSetting.enabled(
            TestProperties.defaults().subscription().singleActiveConsumer().enabled()));
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
    assertFalse(
        org.streamrune.integration.SingleActiveConsumerSetting.enabled(
            p.subscription().singleActiveConsumer().enabled()));
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
  void sagaForwardFenceDefaultsToAuto() {
    assertEquals("auto", TestProperties.defaults().saga().forwardFence());
  }

  @Test
  void sagaForwardFenceBindsAnUnquotedYamlOn() {
    // YAML 1.1 reads an unquoted `on` as a boolean; the Quarkus YAML source hands SmallRye the
    // text "true". The mapping keeps the String, and the setting reads it as on.
    var p = TestProperties.of(Map.of("streamrune.saga.forward-fence", "true"));
    assertEquals("true", p.saga().forwardFence());
    assertEquals(
        org.streamrune.integration.SagaForwardFenceSetting.ON,
        org.streamrune.integration.SagaForwardFenceSetting.parse(p.saga().forwardFence()));
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
    assertEquals(Duration.ofMinutes(5), crypto.cache().expireAfterWrite());
    assertTrue(crypto.cache().expireAfterAccess().isEmpty());
    assertEquals(1000, crypto.aws().dataKeyCache().maximumSize());
    assertEquals(Duration.ofMinutes(5), crypto.aws().dataKeyCache().expireAfterWrite());
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
                "streamrune.crypto.aws.data-key-cache.maximum-size", "50",
                "streamrune.crypto.aws.data-key-cache.expire-after-write", "PT2M",
                "streamrune.crypto.cache.expire-after-write", "PT30M",
                "streamrune.crypto.cache.expire-after-access", "PT10M"));
    assertTrue(crypto.filesystem().enabled());
    assertEquals("/tmp/keys", crypto.filesystem().keyDirectory());
    assertEquals("s.token", crypto.vault().token().orElseThrow());
    assertEquals("eu-central-1", crypto.aws().region().orElseThrow());
    assertEquals("key-1", crypto.aws().kmsKeyId().orElseThrow());
    assertEquals(Duration.ofMinutes(30), crypto.cache().expireAfterWrite());
    assertEquals(Duration.ofMinutes(10), crypto.cache().expireAfterAccess().orElseThrow());
    assertEquals(50, crypto.aws().dataKeyCache().maximumSize());
    assertEquals(Duration.ofMinutes(2), crypto.aws().dataKeyCache().expireAfterWrite());
  }

  /**
   * In a Quarkus application, which converts durations with Quarkus' converter, the crypto cache
   * expiries take the {@code 5m} form as well as {@code PT5M}, like every other StreamRune
   * duration.
   */
  @Test
  void theCryptoExpiriesTakeTheDurationFormsOfAQuarkusApplication() {
    var crypto =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusCryptoProperties.class)
            .withConverter(Duration.class, 100, new DurationConverter())
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "streamrune.crypto.aws.data-key-cache.expire-after-write", "2m",
                        "streamrune.crypto.cache.expire-after-write", "30m",
                        "streamrune.crypto.cache.expire-after-access", "PT10M"),
                    "quarkus-application",
                    1000))
            .build()
            .getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    assertEquals(Duration.ofMinutes(2), crypto.aws().dataKeyCache().expireAfterWrite());
    assertEquals(Duration.ofMinutes(30), crypto.cache().expireAfterWrite());
    assertEquals(Duration.ofMinutes(10), crypto.cache().expireAfterAccess().orElseThrow());
  }

  /**
   * In a Quarkus application every duration key takes the short form the configuration reference
   * writes its default in ({@code 7d}, {@code 1m}, {@code 30s}, {@code 500ms}) as well as ISO-8601:
   * bound with each default written that way, the mappings read as they read with no source at all.
   * SmallRye Config without Quarkus' converter reads ISO-8601 only, which is why the README's
   * listing writes ISO-8601.
   */
  @Test
  void everyDurationTakesTheShortFormOfTheConfigurationReferenceInAQuarkusApplication() {
    var crypto = TestProperties.crypto(Map.of());
    Map<String, String> defaults =
        ConfigurationDocLintTest.oracleOf(TestProperties.defaults(), crypto).keys();
    Map<String, String> shortForms = new TreeMap<>();
    Map<String, String> cryptoShortForms = new TreeMap<>();
    defaults.forEach(
        (key, value) -> {
          if (value.matches("[0-9]+(?:ms|d|h|m|s)")) {
            (key.startsWith("streamrune.crypto.") ? cryptoShortForms : shortForms).put(key, value);
          }
        });
    assertTrue(shortForms.size() >= 10, "the duration keys must be found: " + shortForms);
    assertTrue(cryptoShortForms.size() >= 2, "the crypto duration keys: " + cryptoShortForms);

    var application =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withMapping(StreamRuneQuarkusCryptoProperties.class)
            .withConverter(Duration.class, 100, new DurationConverter())
            .withSources(
                new PropertiesConfigSource(
                    merged(shortForms, cryptoShortForms), "quarkus-application", 1000))
            .build();

    assertEquals(
        defaults,
        ConfigurationDocLintTest.oracleOf(
                application.getConfigMapping(StreamRuneQuarkusProperties.class),
                application.getConfigMapping(StreamRuneQuarkusCryptoProperties.class))
            .keys());
    String sample = shortForms.keySet().iterator().next();
    assertThrows(
        RuntimeException.class,
        () -> TestProperties.of(Map.of(sample, shortForms.get(sample))),
        "SmallRye Config alone reads ISO-8601 only");
  }

  private static Map<String, String> merged(Map<String, String> a, Map<String, String> b) {
    Map<String, String> merged = new TreeMap<>(a);
    merged.putAll(b);
    return merged;
  }

  /** A crypto expiry that is not a duration fails the binding, and the failure names the key. */
  @Test
  void aCryptoExpiryThatIsNotADurationFailsTheBindingNamingTheKey() {
    for (String key :
        new String[] {
          "streamrune.crypto.aws.data-key-cache.expire-after-write",
          "streamrune.crypto.cache.expire-after-write",
          "streamrune.crypto.cache.expire-after-access"
        }) {
      var failure =
          assertThrows(RuntimeException.class, () -> TestProperties.crypto(Map.of(key, "xyz")));
      assertTrue(failure.getMessage().contains(key), failure.getMessage());
    }
  }
}

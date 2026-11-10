package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link StreamRuneMicronautProperties}. */
class StreamRuneMicronautPropertiesTest {

  @Test
  void defaultsMatchDocumented() {
    var p = StreamRuneMicronautProperties.withDefaults();
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
    // Defaults to the inbox retention window (7d), not longer.
    assertEquals(Duration.ofDays(7), p.sagaDeadLetterRetentionMaxAge());
    assertEquals(Duration.ofDays(7), p.outboxRetentionMaxAge());
    assertEquals(Duration.ofDays(7), p.inboxRetentionMaxAge());
    assertEquals(List.of(), p.metadataBaggageAllowlist());
    assertEquals(Duration.ofDays(30), p.outboxSkippedRetentionMaxAge());
    assertEquals("true", p.subscriptionSingleActiveConsumerEnabled());
  }

  @Test
  void sagaForwardFenceDefaultsToAuto() {
    assertEquals("auto", StreamRuneMicronautProperties.withDefaults().sagaForwardFence());
    try (var ctx = io.micronaut.context.ApplicationContext.run()) {
      assertEquals("auto", ctx.getBean(StreamRuneMicronautProperties.class).sagaForwardFence());
    }
  }

  @Test
  void sagaForwardFenceBindsAnUnquotedYamlOn() {
    // YAML 1.1 reads an unquoted `on` as a boolean; the Micronaut YAML loader hands over a
    // Boolean, which binds to the String "true". The setting reads it as on.
    try (var ctx =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.saga.forward-fence", Boolean.TRUE))) {
      var bound = ctx.getBean(StreamRuneMicronautProperties.class).sagaForwardFence();
      assertEquals("true", bound);
      assertEquals(
          org.streamrune.integration.SagaForwardFenceSetting.ON,
          org.streamrune.integration.SagaForwardFenceSetting.parse(bound));
    }
  }

  @Test
  void outboxSkippedRetentionMaxAgeCanBeDisabled() {
    // Zero disables the SKIPPED sweep — the sweeper treats <= 0 as "do not delete SKIPPED". FAILED
    // entries are never pruned in either case.
    try (var p =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.outbox.skipped-retention-max-age", "0s"))) {
      assertEquals(
          Duration.ZERO,
          p.getBean(StreamRuneMicronautProperties.class).outboxSkippedRetentionMaxAge());
    }
  }

  @Test
  void outboxSkippedRetentionMaxAgeDefaultsTo30Days() {
    // Same default as the FAILED window it replaces: the audit of
    // an operator's skip decision is kept 30 days.
    assertEquals(
        Duration.ofDays(30),
        StreamRuneMicronautProperties.withDefaults().outboxSkippedRetentionMaxAge());
  }

  @Test
  void subscriptionSingleActiveConsumerCanBeDisabled() {
    try (var p =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.subscription.single-active-consumer.enabled", "false"))) {
      assertFalse(
          org.streamrune.integration.SingleActiveConsumerSetting.enabled(
              p.getBean(StreamRuneMicronautProperties.class)
                  .subscriptionSingleActiveConsumerEnabled()));
    }
  }

  @Test
  void customConstructor() {
    var p =
        new StreamRuneMicronautProperties(
            10,
            1,
            Duration.ofMillis(1),
            1.0,
            Duration.ofMillis(100),
            Duration.ofMillis(10),
            Duration.ofMillis(500),
            8,
            10_000,
            3,
            Duration.ofSeconds(10),
            false,
            false,
            true,
            Duration.ofDays(60),
            false,
            100,
            Duration.ofSeconds(5),
            true,
            5,
            Duration.ofMinutes(1),
            Duration.ofDays(45),
            true,
            "streamrune",
            true,
            true,
            false,
            Duration.ofDays(14),
            Duration.ofDays(3),
            List.of("tracking", "tenant"),
            Duration.ofDays(90),
            "false",
            15,
            Duration.ofSeconds(2),
            3.5,
            Duration.ofSeconds(20),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1),
            "auto",
            "required",
            "operator-only");
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
    assertEquals(Duration.ofDays(60), p.sagaDeadLetterRetentionMaxAge());
    assertFalse(p.autoInitializeSchema());
    assertEquals(Duration.ofDays(14), p.outboxRetentionMaxAge());
    assertEquals(Duration.ofDays(3), p.inboxRetentionMaxAge());
    assertEquals(Duration.ofDays(45), p.deadLetterRetentionMaxAge());
    assertEquals(List.of("tracking", "tenant"), p.metadataBaggageAllowlist());
    assertEquals(Duration.ofDays(90), p.outboxSkippedRetentionMaxAge());
    assertEquals("false", p.subscriptionSingleActiveConsumerEnabled());
    assertEquals(15, p.outboxRetryMaxAttempts());
    assertEquals(Duration.ofSeconds(2), p.outboxRetryInitialDelay());
    assertEquals(3.5, p.outboxRetryBackoffMultiplier());
    assertEquals(Duration.ofSeconds(20), p.subscriptionLeaseTtl());
  }

  /**
   * The six interval keys are durations: each binds from the short form the configuration reference
   * writes ({@code 5s}) and from ISO-8601 ({@code PT5S}) alike, through a started context.
   */
  @Test
  void theIntervalKeysBindTheShortFormAndIso8601() {
    for (String value : new String[] {"5s", "PT5S"}) {
      try (var ctx =
          io.micronaut.context.ApplicationContext.run(
              java.util.Map.of(
                  "streamrune.retry-initial-delay", value,
                  "streamrune.polling-interval", value,
                  "streamrune.polling-jitter", value,
                  "streamrune.dead-letter.retry-interval", value,
                  "streamrune.outbox.flush-interval", value,
                  "streamrune.outbox.retry-initial-delay", value,
                  "streamrune.outbox.retry-backoff-multiplier", "2.5"))) {
        var p = ctx.getBean(StreamRuneMicronautProperties.class);
        Duration five = Duration.ofSeconds(5);
        assertEquals(five, p.retryInitialDelay(), value);
        assertEquals(five, p.pollingInterval(), value);
        assertEquals(five, p.pollingJitter(), value);
        assertEquals(five, p.deadLetterRetryInterval(), value);
        assertEquals(five, p.outboxFlushInterval(), value);
        assertEquals(five, p.outboxRetryInitialDelay(), value);
        assertEquals(2.5, p.outboxRetryBackoffMultiplier(), value);
      }
    }
  }

  @Test
  void deadLetterRetentionMaxAgeDefaultsToTheInboxWindow() {
    var defaults = StreamRuneMicronautProperties.withDefaults();
    assertEquals(Duration.ofDays(7), defaults.deadLetterRetentionMaxAge());
    assertEquals(defaults.inboxRetentionMaxAge(), defaults.deadLetterRetentionMaxAge());
  }

  @Test
  void bindingDefaultsMatchWithDefaultsForTheDeadLetterKeys() {
    try (var ctx = io.micronaut.context.ApplicationContext.run()) {
      var bound = ctx.getBean(StreamRuneMicronautProperties.class);
      assertEquals(Duration.ofDays(7), bound.deadLetterRetentionMaxAge());
      assertEquals("operator-only", bound.deadLetterUnkeyedReplay());
    }
  }

  @Test
  void deadLetterUnkeyedReplayBindsTheDocumentedKey() {
    try (var ctx =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.dead-letter.unkeyed-replay", "automatic"))) {
      assertEquals(
          "automatic", ctx.getBean(StreamRuneMicronautProperties.class).deadLetterUnkeyedReplay());
    }
  }

  @Test
  void anUnknownDeadLetterUnkeyedReplayValueFailsBinding() {
    var ex =
        assertThrows(
            RuntimeException.class,
            () -> {
              try (var ctx =
                  io.micronaut.context.ApplicationContext.run(
                      java.util.Map.of("streamrune.dead-letter.unkeyed-replay", "maybe"))) {
                ctx.getBean(StreamRuneMicronautProperties.class);
              }
            });
    Throwable t = ex;
    boolean named = false;
    while (t != null) {
      named |= String.valueOf(t.getMessage()).contains("streamrune.dead-letter.unkeyed-replay");
      t = t.getCause();
    }
    assertTrue(named, "the failure names the property");
  }

  @Test
  void sseReapingKnobsDefaultToFiveMinutesAndThirtySeconds() {
    // Knob parity with Spring and Quarkus. Without these an operator could not
    // even mitigate leaking half-open SSE clients by configuration.
    var d = StreamRuneMicronautProperties.withDefaults();
    assertEquals(Duration.ofMinutes(5), d.sseTimeout());
    assertEquals(Duration.ofSeconds(30), d.sseKeepAliveInterval());
  }

  @Test
  void eventStoreStatementTimeoutDefaultsTo30SecondsAndBindsTheDocumentedKey() {
    assertEquals(
        Duration.ofSeconds(30),
        StreamRuneMicronautProperties.withDefaults().eventStoreStatementTimeout());
    try (var ctx =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.event-store.statement-timeout", "2m"))) {
      assertEquals(
          Duration.ofMinutes(2),
          ctx.getBean(StreamRuneMicronautProperties.class).eventStoreStatementTimeout());
    }
    try (var ctx =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.event-store.statement-timeout", "0s"))) {
      assertEquals(
          Duration.ZERO,
          ctx.getBean(StreamRuneMicronautProperties.class).eventStoreStatementTimeout());
    }
  }

  /**
   * The five durations an application rarely sets are bound with their documented defaults when the
   * configuration names none: the SSE endpoint reaps a silent client after the timeout and probes
   * it with the keepalive, as on Spring and Quarkus.
   */
  @Test
  void aContextWithNoConfigurationBindsTheDocumentedDurations() {
    try (var ctx = io.micronaut.context.ApplicationContext.run()) {
      var bound = ctx.getBean(StreamRuneMicronautProperties.class);
      assertEquals(Duration.ofMinutes(5), bound.sseTimeout());
      assertEquals(Duration.ofSeconds(30), bound.sseKeepAliveInterval());
      assertEquals(Duration.ofSeconds(1), bound.ssePollingInterval());
      assertEquals(Duration.ofSeconds(30), bound.eventStoreStatementTimeout());
      assertEquals(Duration.ofSeconds(15), bound.subscriptionLeaseTtl());
    }
  }

  @Test
  void subscriptionLeaseTtlDefaultsTo15Seconds() {
    assertEquals(
        Duration.ofSeconds(15),
        StreamRuneMicronautProperties.withDefaults().subscriptionLeaseTtl());
  }
}

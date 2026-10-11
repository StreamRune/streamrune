package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;

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
    assertEquals(50L, p.retryInitialDelayMs());
    assertEquals(2.0, p.retryBackoffMultiplier());
    assertEquals(5000L, p.pollingIntervalMs());
    assertEquals(1000L, p.pollingJitterMs());
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
    assertTrue(p.subscriptionSingleActiveConsumerEnabled());
  }

  @Test
  void outboxSkippedRetentionMaxAgeCanBeDisabled() {
    // Zero disables the SKIPPED sweep — the sweeper treats <= 0 as "do not delete SKIPPED". FAILED
    // entries are never pruned in either case.
    var p =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.outbox.skipped-retention-max-age", "0s"));
    try {
      assertEquals(
          Duration.ZERO,
          p.getBean(StreamRuneMicronautProperties.class).outboxSkippedRetentionMaxAge());
    } finally {
      p.close();
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
    var p =
        io.micronaut.context.ApplicationContext.run(
            java.util.Map.of("streamrune.subscription.single-active-consumer.enabled", "false"));
    try {
      assertFalse(
          p.getBean(StreamRuneMicronautProperties.class).subscriptionSingleActiveConsumerEnabled());
    } finally {
      p.close();
    }
  }

  @Test
  void customConstructor() {
    var p =
        new StreamRuneMicronautProperties(
            10,
            1,
            1L,
            1.0,
            100L,
            10L,
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
            5000L,
            true,
            5,
            60000L,
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
            false,
            15,
            2000L,
            3.5,
            Duration.ofSeconds(20),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1));
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
    assertEquals(Duration.ofDays(60), p.sagaDeadLetterRetentionMaxAge());
    assertFalse(p.autoInitializeSchema());
    assertEquals(Duration.ofDays(14), p.outboxRetentionMaxAge());
    assertEquals(Duration.ofDays(3), p.inboxRetentionMaxAge());
    assertEquals(Duration.ofDays(45), p.deadLetterRetentionMaxAge());
    assertEquals(List.of("tracking", "tenant"), p.metadataBaggageAllowlist());
    assertEquals(Duration.ofDays(90), p.outboxSkippedRetentionMaxAge());
    assertFalse(p.subscriptionSingleActiveConsumerEnabled());
    assertEquals(15, p.outboxRetryMaxAttempts());
    assertEquals(2000L, p.outboxRetryInitialDelayMs());
    assertEquals(3.5, p.outboxRetryMultiplier());
    assertEquals(Duration.ofSeconds(20), p.subscriptionLeaseTtl());
  }

  @Test
  void deadLetterRetentionMaxAgeDefaultsTo30Days() {
    assertEquals(
        Duration.ofDays(30),
        StreamRuneMicronautProperties.withDefaults().deadLetterRetentionMaxAge());
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

  @Test
  void subscriptionLeaseTtlDefaultsTo15Seconds() {
    assertEquals(
        Duration.ofSeconds(15),
        StreamRuneMicronautProperties.withDefaults().subscriptionLeaseTtl());
  }
}

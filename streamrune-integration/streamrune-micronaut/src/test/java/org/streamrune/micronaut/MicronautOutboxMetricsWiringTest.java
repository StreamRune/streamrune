package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * The {@code outboxPoller} factory must thread the {@link StreamRuneMetrics} bean into the poller
 * so {@code streamrune.outbox.delivery_failed} and the {@code streamrune.outbox.pending} backlog
 * gauge are actually emitted. Without the fix the poller carried {@link StreamRuneMetrics#NOOP} and
 * the documented alert metric was a dead signal.
 */
class MicronautOutboxMetricsWiringTest {

  private final StreamRuneMicronautModule module = new StreamRuneMicronautModule();
  private final InMemoryOutboxStore store = new InMemoryOutboxStore();
  private final OutboxPublisher publisher = entry -> {};

  @Test
  void outboxPollerWiresMetricsBean() throws Exception {
    // A sentinel non-NOOP metrics instance — asserting the poller carries THIS instance proves the
    // factory threaded the metrics bean through (the fix), independent of the Micrometer impl.
    StreamRuneMetrics sentinel = new StreamRuneMetrics() {};
    OutboxPoller poller =
        module.outboxPoller(
            store,
            publisher,
            StreamRuneMicronautProperties.withDefaults(),
            sentinel,
            null,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertSame(
        sentinel, metricsOf(poller), "the auto-configured poller must carry the metrics bean");
  }

  @Test
  void outboxPollerFallsBackToNoopWhenNoMetricsBean() throws Exception {
    OutboxPoller poller =
        module.outboxPoller(
            store,
            publisher,
            StreamRuneMicronautProperties.withDefaults(),
            null,
            null,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertSame(StreamRuneMetrics.NOOP, metricsOf(poller));
  }

  @Test
  void outboxPollerReflectsRetryKnobs() throws Exception {
    // streamrune.outbox.retry-* must reach the poller's RetryPolicy in Micronaut too. Uses
    // non-default values (7 / 250ms / 3.0) so this fails if the knobs are dropped and the poller
    // falls back to the built-in default ladder.
    OutboxPoller poller =
        module.outboxPoller(
            store,
            publisher,
            propsWithRetry(7, Duration.ofMillis(250), 3.0),
            null,
            null,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    RetryPolicy rp = retryPolicyOf(poller);
    assertEquals(7, rp.maxAttempts());
    assertEquals(Duration.ofMillis(250), rp.initialDelay());
    assertEquals(3.0, rp.backoffMultiplier());
  }

  @Test
  void outboxRetentionSweeperReceivesTheSkippedWindow() throws Exception {
    var sweeper =
        module.outboxRetentionSweeper(
            new org.streamrune.postgres.PostgresOutboxStore(mock(DataSource.class)),
            propsWithSkippedRetention(Duration.ofDays(45)),
            null,
            null);
    Field f = org.streamrune.runtime.OutboxRetentionSweeper.class.getDeclaredField("skippedMaxAge");
    f.setAccessible(true);
    assertEquals(Duration.ofDays(45), f.get(sweeper));
  }

  /** withDefaults() with only the 31st component (outbox.skipped-retention-max-age) changed. */
  private static StreamRuneMicronautProperties propsWithSkippedRetention(Duration skipped) {
    return new StreamRuneMicronautProperties(
        100,
        3,
        Duration.ofMillis(50),
        2.0,
        Duration.ofSeconds(5),
        Duration.ofSeconds(1),
        Duration.ofSeconds(5),
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        Duration.ofDays(7),
        false,
        100,
        Duration.ofSeconds(5),
        true,
        5,
        Duration.ofMinutes(1),
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7),
        Duration.ofDays(7),
        List.of(),
        skipped,
        "true",
        10,
        Duration.ofSeconds(1),
        2.0,
        Duration.ofSeconds(15),
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        java.time.Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1),
        "auto",
        "required",
        "operator-only",
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true);
  }

  @Test
  void factoryMetricsExposeTheFourBlockageAndSkipSeries_andFailedSweptIsGone() throws Exception {
    // The Micronaut wiring test pins the same thing as Spring's and Quarkus's — the
    // factory-built MicrometerStreamRuneMetrics carries the two gauges (eager) and the two
    // counters, and the poller records into that very instance; failed_swept has no series.
    var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    StreamRuneMetrics metrics =
        module.micrometerStreamRuneMetrics(registry, StreamRuneMicronautProperties.withDefaults());
    OutboxPoller poller =
        module.outboxPoller(
            store,
            publisher,
            StreamRuneMicronautProperties.withDefaults(),
            metrics,
            null,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertSame(
        metrics, metricsOf(poller), "the poller records into the factory's Micrometer adapter");
    assertNotNull(registry.find("streamrune.outbox.blocked_aggregates").gauge());
    assertNotNull(registry.find("streamrune.outbox.blockage_age_seconds").gauge());
    assertNotNull(registry.find("streamrune.outbox.skipped").counter());
    assertNotNull(registry.find("streamrune.outbox.skipped_swept").counter());
    assertNull(
        registry.find("streamrune.outbox.failed_swept").counter(),
        "the FAILED sweep no longer exists, so neither does its series");
  }

  /**
   * Defaults for every field except the three outbox retry knobs (mirrors {@code withDefaults}).
   */
  private static StreamRuneMicronautProperties propsWithRetry(
      int maxAttempts, Duration initialDelay, double multiplier) {
    return new StreamRuneMicronautProperties(
        100,
        3,
        Duration.ofMillis(50),
        2.0,
        Duration.ofSeconds(5),
        Duration.ofSeconds(1),
        Duration.ofSeconds(5),
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        Duration.ofDays(30),
        false,
        100,
        Duration.ofSeconds(5),
        true,
        5,
        Duration.ofMinutes(1),
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7),
        Duration.ofDays(7),
        List.of(),
        Duration.ofDays(30),
        "true",
        maxAttempts,
        initialDelay,
        multiplier,
        Duration.ofSeconds(15),
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        java.time.Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1),
        "auto",
        "required",
        "operator-only",
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true);
  }

  private static StreamRuneMetrics metricsOf(OutboxPoller poller) throws Exception {
    Field f = OutboxPoller.class.getDeclaredField("metrics");
    f.setAccessible(true);
    return (StreamRuneMetrics) f.get(poller);
  }

  private static RetryPolicy retryPolicyOf(OutboxPoller poller) throws Exception {
    Field f = OutboxPoller.class.getDeclaredField("retryPolicy");
    f.setAccessible(true);
    return (RetryPolicy) f.get(poller);
  }
}

package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.runtime.OutboxPoller;

/**
 * The {@code outboxPoller} producer must thread the {@link StreamRuneMetrics} bean into the poller
 * so {@code streamrune.outbox.delivery_failed} and the {@code streamrune.outbox.pending} backlog
 * gauge are emitted. Without the fix the producer built the poller with {@link
 * StreamRuneMetrics#NOOP} and the documented alert metric was a dead signal.
 */
class QuarkusOutboxMetricsWiringTest {

  private final StreamRuneProducers producers = new StreamRuneProducers();

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  /**
   * A mocked store that states its ordering mode and claim lease: an un-stubbed mock returns {@code
   * null} from {@code orderingMode()}, which {@link OutboxPoller.Builder#build()} refuses, and
   * {@code Duration.ZERO} from {@code claimLease()}, which opts the poller out of its lease guard
   * and publish deadline.
   */
  private static OutboxStore modeStatingStore() {
    OutboxStore store = mock(OutboxStore.class);
    when(store.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(store.claimLease()).thenReturn(Duration.ofMinutes(4));
    return store;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isResolvable()).thenReturn(false);
    when(inst.isUnsatisfied()).thenReturn(true);
    return inst;
  }

  @Test
  void outboxPollerWiresMetricsBean() throws Exception {
    // A sentinel non-NOOP metrics instance — asserting the poller carries THIS instance proves the
    // producer threaded the metrics bean through (the fix), independent of the Micrometer impl.
    StreamRuneMetrics sentinel = new StreamRuneMetrics() {};
    OutboxPoller poller =
        producers.outboxPoller(
            satisfied(modeStatingStore()),
            satisfied((OutboxPublisher) e -> {}),
            TestProperties.of(Map.of("streamrune.outbox.enabled", "true")),
            satisfied(sentinel),
            unsatisfied());
    assertSame(sentinel, metricsOf(poller), "the produced poller must carry the metrics bean");
  }

  @Test
  void outboxPollerFallsBackToNoopWhenNoMetricsBean() throws Exception {
    OutboxPoller poller =
        producers.outboxPoller(
            satisfied(modeStatingStore()),
            satisfied((OutboxPublisher) e -> {}),
            TestProperties.of(Map.of("streamrune.outbox.enabled", "true")),
            unsatisfied(),
            unsatisfied());
    assertSame(StreamRuneMetrics.NOOP, metricsOf(poller));
  }

  @Test
  void outboxPollerReflectsRetryKnobs() throws Exception {
    // streamrune.outbox.retry-* must reach the poller's RetryPolicy in Quarkus too.
    OutboxPoller poller =
        producers.outboxPoller(
            satisfied(modeStatingStore()),
            satisfied((OutboxPublisher) e -> {}),
            TestProperties.of(
                Map.of(
                    "streamrune.outbox.enabled", "true",
                    "streamrune.outbox.retry-max-attempts", "7",
                    "streamrune.outbox.retry-initial-delay-ms", "250",
                    "streamrune.outbox.retry-multiplier", "3.0")),
            unsatisfied(),
            unsatisfied());
    RetryPolicy rp = retryPolicyOf(poller);
    assertEquals(7, rp.maxAttempts());
    assertEquals(Duration.ofMillis(250), rp.initialDelay());
    assertEquals(3.0, rp.backoffMultiplier());
  }

  @Test
  void outboxRetentionSweeperReceivesTheSkippedWindow() throws Exception {
    var sweeper =
        producers.outboxRetentionSweeper(
            satisfied(
                new org.streamrune.postgres.PostgresOutboxStore(mock(javax.sql.DataSource.class))),
            TestProperties.of(
                Map.of(
                    "streamrune.outbox.enabled", "true",
                    "streamrune.outbox.skipped-retention-max-age", "PT1080H")),
            unsatisfied(),
            unsatisfied());
    Field f = org.streamrune.runtime.OutboxRetentionSweeper.class.getDeclaredField("skippedMaxAge");
    f.setAccessible(true);
    assertEquals(Duration.ofDays(45), f.get(sweeper));
  }

  @Test
  void producedMetricsExposeTheFourBlockageAndSkipSeries_andFailedSweptIsGone() throws Exception {
    // The producer-built MicrometerStreamRuneMetrics is what the poller, replayer and sweeper
    // record into; the two gauges are eager, so all four series exist before the first
    // poll.
    var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    StreamRuneMetrics metrics =
        producers.micrometerStreamRuneMetrics(satisfied(registry), TestProperties.defaults());
    OutboxPoller poller =
        producers.outboxPoller(
            satisfied(modeStatingStore()),
            satisfied((OutboxPublisher) e -> {}),
            TestProperties.of(Map.of("streamrune.outbox.enabled", "true")),
            satisfied(metrics),
            unsatisfied());
    assertSame(
        metrics, metricsOf(poller), "the poller records into the produced Micrometer adapter");
    org.junit.jupiter.api.Assertions.assertNotNull(
        registry.find("streamrune.outbox.blocked_aggregates").gauge());
    org.junit.jupiter.api.Assertions.assertNotNull(
        registry.find("streamrune.outbox.blockage_age_seconds").gauge());
    org.junit.jupiter.api.Assertions.assertNotNull(
        registry.find("streamrune.outbox.skipped").counter());
    org.junit.jupiter.api.Assertions.assertNotNull(
        registry.find("streamrune.outbox.skipped_swept").counter());
    org.junit.jupiter.api.Assertions.assertNull(
        registry.find("streamrune.outbox.failed_swept").counter(),
        "the FAILED sweep no longer exists, so neither does its series");
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

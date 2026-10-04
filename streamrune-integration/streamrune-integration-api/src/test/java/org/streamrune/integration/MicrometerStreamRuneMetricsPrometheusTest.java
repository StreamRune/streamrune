package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * The meters must work on the registry adopters actually run.
 *
 * <p><b>Why this class exists.</b> Every other test in this module uses {@link
 * SimpleMeterRegistry}, which dedups meters by their FULL id (name + tags): registering {@code
 * streamrune.projections.processed} untagged and then {@code
 * streamrune.projections.processed{projection.name=orders}} yields two peacefully coexisting series
 * there. A {@link PrometheusMeterRegistry} does not — the Prometheus data model requires every
 * series in a metric family to carry the same label keys, so the second registration is rejected. A
 * green suite on {@code SimpleMeterRegistry} therefore proves nothing about the standard production
 * stack (Spring Boot + actuator + {@code micrometer-registry-prometheus}, or {@code
 * quarkus-micrometer-registry-prometheus}), where the rejection is swallowed by the framework's
 * metric guards and the series reads zero forever while traffic flows. This codebase already
 * documented the hazard at {@code SubscriptionHealthContributor.java:105} without applying it.
 *
 * <p>The guard has two halves: a real Prometheus registry proves the emissions land, and a
 * registry-independent shape assertion pins the invariant that makes them land — <b>one meter name,
 * one tag-key set</b>.
 */
class MicrometerStreamRuneMetricsPrometheusTest {

  @Test
  void queryMetersAreEmittedOnAPrometheusRegistry() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordQueryDispatched("FindOrders");
    metrics.recordQueryDuration("FindOrders", 1_000_000L);
    metrics.recordQueryCacheHit("FindOrders");
    metrics.recordQueryCacheMiss("FindOrders");

    assertNotNull(
        registry
            .find(MetricNames.QUERIES_DISPATCHED)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrders")
            .counter(),
        "streamrune.queries.dispatched{query.type} must exist on a Prometheus registry");
    assertEquals(
        1.0,
        registry
            .find(MetricNames.QUERIES_DISPATCHED)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrders")
            .counter()
            .count());

    String scrape = registry.scrape();
    assertTrue(
        scrape.contains("streamrune_queries_dispatched_total"),
        "the scrape must expose the query counter, got:\n" + scrape);
    assertTrue(
        scrape.contains("query_type=\"FindOrders\""),
        "the scrape must carry the query_type label, got:\n" + scrape);
  }

  @Test
  void projectionMetersAreEmittedOnAPrometheusRegistry() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    var orders = ProjectionName.of("orders");

    metrics.recordProjectionProcessed(orders);
    metrics.recordProjectionFailed(orders);
    metrics.recordProjectionDeadLettered(orders);
    metrics.recordProjectionDuration(orders, 1_000_000L);

    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_PROCESSED)
            .tag(MetricNames.TAG_PROJECTION_NAME, "orders")
            .counter()
            .count(),
        "streamrune.projections.processed{projection.name} must exist on a Prometheus registry");

    String scrape = registry.scrape();
    assertTrue(
        scrape.contains("streamrune_projections_processed_total"),
        "the scrape must expose the projection counter, got:\n" + scrape);
    assertTrue(
        scrape.contains("projection_name=\"orders\""),
        "the scrape must carry the projection_name label, got:\n" + scrape);
  }

  /**
   * The declared-mode gauge carries {@code projection.name} and {@code delivery.mode} on every
   * series, so it lands in the scrape as one family with both labels.
   */
  @Test
  void deliveryModeGaugeIsEmittedOnAPrometheusRegistry() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("audit"), ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);

    String scrape = registry.scrape();
    var auditSeries =
        java.util.regex.Pattern.compile(
                "^streamrune_projections_delivery_mode\\{[^}]*\\} 1\\.0$",
                java.util.regex.Pattern.MULTILINE)
            .matcher(scrape)
            .results()
            .map(java.util.regex.MatchResult::group)
            .filter(line -> line.contains("projection_name=\"audit\""))
            .toList();
    assertEquals(
        1,
        auditSeries.size(),
        "the scrape must carry exactly one delivery_mode series for audit, got:\n" + scrape);
    assertTrue(
        auditSeries.get(0).contains("delivery_mode=\"AT_LEAST_ONCE_IDEMPOTENT\""),
        "the audit series must carry its declared mode, got: " + auditSeries.get(0));
    assertTrue(
        scrape.contains("streamrune_projections_delivery_mode{"),
        "the scrape must expose the delivery_mode gauge, got:\n" + scrape);
  }

  /**
   * The generalized form of the two tests above: no {@code record*} entry point on the SPI may
   * throw against a real Prometheus registry, whichever order they are called in. Driving the
   * untagged and the tagged entry point of the SAME meter name back to back is the exact production
   * interleaving (an eagerly registered bare meter, then the first tagged sample) that the
   * collision breaks.
   */
  @Test
  void everyRecordingEntryPointSurvivesAPrometheusRegistry() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    driveEveryEntryPointTaggedAndUntagged(metrics);

    // A scrape renders every registered collector; a half-registered family surfaces here too.
    assertTrue(
        registry.scrape().contains("streamrune_"), "the scrape must expose StreamRune series");
  }

  /**
   * Registry-independent statement of the invariant that makes the Prometheus tests above pass:
   * every meter NAME commits to exactly ONE tag-key set. This is the assertion a future meter
   * addition trips on without needing a Prometheus registry in scope, and it names the offenders
   * instead of failing with a registration exception from deep inside Micrometer.
   */
  @Test
  void everyMeterNameCommitsToExactlyOneTagKeySet() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    driveEveryEntryPointTaggedAndUntagged(metrics);

    Map<String, Set<Set<String>>> shapesByName = new HashMap<>();
    for (Meter meter : registry.getMeters()) {
      Set<String> tagKeys =
          meter.getId().getTags().stream()
              .map(Tag::getKey)
              .collect(Collectors.toCollection(TreeSet::new));
      shapesByName
          .computeIfAbsent(meter.getId().getName(), name -> new LinkedHashSet<>())
          .add(tagKeys);
    }

    var offenders =
        shapesByName.entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .map(entry -> entry.getKey() + " -> " + entry.getValue())
            .sorted()
            .toList();

    assertTrue(
        offenders.isEmpty(),
        "these meter names carry more than one tag-key set; a PrometheusMeterRegistry rejects the"
            + " second registration of each, and the guarded metric call sites swallow the"
            + " rejection so the series reads zero forever: "
            + offenders);
  }

  /**
   * The untagged projection entry points have no projection name to report, so they emit under the
   * documented {@link MicrometerStreamRuneMetrics#UNKNOWN_TAG_VALUE} series rather than under a
   * second, tag-less shape of the same meter name. No framework call site takes this path (all
   * three projection runners pass the {@code ProjectionName}); it exists for third-party callers of
   * the untagged SPI.
   */
  @Test
  void untaggedProjectionEntryPointsEmitUnderTheUnknownSentinel() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordProjectionProcessed();
    metrics.recordProjectionFailed();
    metrics.recordProjectionDeadLettered();
    metrics.recordProjectionDuration(1_000L);

    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_PROCESSED)
            .tag(MetricNames.TAG_PROJECTION_NAME, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_FAILED)
            .tag(MetricNames.TAG_PROJECTION_NAME, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_DEAD_LETTERED)
            .tag(MetricNames.TAG_PROJECTION_NAME, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count());
    assertEquals(
        1L,
        registry
            .find(MetricNames.PROJECTIONS_DURATION)
            .tag(MetricNames.TAG_PROJECTION_NAME, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .timer()
            .count());
  }

  /**
   * {@code commands.dispatched} and {@code events.appended} are emitted UNTAGGED by the framework's
   * own path ({@code VirtualThreadCommandBus} calls the no-arg variants) and TAGGED through the
   * public SPI overloads. Both shapes cannot coexist in one Prometheus family, so the no-arg path
   * reports the {@code unknown} sentinel instead of a tag-less series: one family, two label
   * values, every sample scrapeable — and a future bus that starts passing the real type just moves
   * traffic out of the {@code unknown} bucket rather than reshaping a 1.0-frozen metric.
   */
  @Test
  void untaggedEntryPointsShareTheTaggedFamilyUnderTheUnknownSentinel() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandDispatched();
    metrics.recordCommandDispatched("PlaceOrder");
    metrics.recordEventAppended();
    metrics.recordEventAppended("OrderPlaced");

    assertEquals(
        1.0,
        registry
            .find(MetricNames.COMMANDS_DISPATCHED)
            .tag(MetricNames.TAG_COMMAND_TYPE, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count(),
        "the no-arg entry point must land in the unknown bucket of the same family");
    assertEquals(
        1.0,
        registry
            .find(MetricNames.COMMANDS_DISPATCHED)
            .tag(MetricNames.TAG_COMMAND_TYPE, "PlaceOrder")
            .counter()
            .count(),
        "the SPI overload must keep its own command.type series (counted once, not" + " twice)");
    assertEquals(
        1.0,
        registry
            .find(MetricNames.EVENTS_APPENDED)
            .tag(MetricNames.TAG_EVENT_TYPE, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.EVENTS_APPENDED)
            .tag(MetricNames.TAG_EVENT_TYPE, "OrderPlaced")
            .counter()
            .count());
  }

  /**
   * Calls every {@code record*} entry point, deliberately exercising BOTH the untagged and the
   * type-carrying overload of every meter that has both.
   */
  private static void driveEveryEntryPointTaggedAndUntagged(MicrometerStreamRuneMetrics metrics) {
    metrics.recordCommandDispatched();
    metrics.recordCommandDispatched("PlaceOrder");
    metrics.recordCommandSucceeded();
    metrics.recordCommandSucceeded("PlaceOrder");
    metrics.recordCommandFailed();
    metrics.recordCommandFailed("PlaceOrder");
    metrics.recordCommandDuration(1_000L);
    metrics.recordCommandDuration("PlaceOrder", 1_000L);
    metrics.recordCommandRetried("PlaceOrder");
    metrics.recordCommandShortCircuited("PlaceOrder");
    metrics.recordCommandAsyncRejected();
    metrics.registerInFlightCommandsGauge(() -> 5L);
    metrics.recordDeadLetterPublished("PlaceOrder");
    metrics.recordDeadLetterExhausted("PlaceOrder");
    metrics.recordLockWait(1_000L);

    metrics.recordEventAppended();
    metrics.recordEventAppended("OrderPlaced");
    metrics.recordEventReplayed();
    metrics.recordEventAppendDuration(1_000L);

    metrics.recordProjectionProcessed();
    metrics.recordProjectionProcessed(ProjectionName.of("orders"));
    metrics.recordProjectionFailed();
    metrics.recordProjectionFailed(ProjectionName.of("orders"));
    metrics.recordProjectionDeadLettered();
    metrics.recordProjectionDeadLettered(ProjectionName.of("orders"));
    metrics.recordProjectionDuration(1_000L);
    metrics.recordProjectionDuration(ProjectionName.of("orders"), 1_000L);
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);

    metrics.recordSubscriptionEventReceived();
    metrics.recordSubscriptionDeliveryLatency(1_000L);
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 3);
    metrics.recordListenerReconnect("streamrune_events");

    metrics.recordSnapshotCreated();
    metrics.recordSnapshotLoaded();
    metrics.recordSnapshotDiscarded("deserialize_failure");
    metrics.recordSubjectRedacted();
    metrics.recordSystemicKeyStoreFailure();
    metrics.recordGdprPurgeFailed("orders-read-model");

    metrics.recordQueryDispatched("FindOrder");
    metrics.recordQueryDuration("FindOrder", 1_000L);
    metrics.recordQueryCacheHit("FindOrder");
    metrics.recordQueryCacheMiss("FindOrder");

    metrics.recordSagaQuarantined("OrderSaga");
    metrics.recordSagaFaulted("OrderSaga");
    metrics.recordSagaSkippedWhileFaulted("OrderSaga");
    metrics.recordSagaEventHeld("OrderSaga", "GENESIS_PENDING");
    metrics.recordSagaReplayDeferred("OrderSaga");
    metrics.recordSagaCompensation("OrderSaga", "SUCCESS");
    metrics.recordSagaCompensationRetry("OrderSaga");
    metrics.recordSagaCasConflict("OrderSaga");
    metrics.recordSagaCompensatingBacklog("OrderSaga", 2);
    metrics.recordSagaTimedOutBacklog("OrderSaga", 1);
    metrics.recordSagaFaultedRows("OrderSaga", 1);
    metrics.recordSagaFaultedBacklog(1);
    metrics.recordSagaReplayed("OrderSaga", "SKIPPED_DEDUP");
    metrics.recordSagaForcedStaleResume("OrderSaga");
    metrics.recordSagaResumeFaulted("OrderSaga", "RESUMED");
    metrics.recordSagaCompensateFaulted("OrderSaga", "TERMINAL");

    metrics.recordInboxReplayHit("PlaceOrder");
    metrics.recordInboxSwept(1);
    metrics.recordSagaDeadLetterSwept(1);
    metrics.recordOutboxSwept(1);
    metrics.recordOutboxDeliveryFailed();
    metrics.recordOutboxBacklog(1);
    metrics.recordOutboxInFlight(1);
    metrics.recordOutboxBlockedAggregates(1);
    metrics.recordOutboxBlockageAge(1);
    metrics.recordOutboxSkipped();
    metrics.recordOutboxSkippedSwept(1);
    metrics.recordOutboxReplayed(1);
    metrics.recordDeadLetterSwept(1);
    metrics.recordDlqBacklog(1);
    metrics.recordOutboxRelayDegradation(0);
    metrics.recordDlqRetryDegradation(0);
  }

  @Test
  void commandTypeFamilyScrapesWithTheCommandTypeLabel_soPublishedJoinsExhausted() {
    // On the production registry the published side used to be a BARE family while the
    // exhausted side carried command_type, so the per-type published↔exhausted join that
    // DeadLetterRetryRunner's documentation describes could not be written at all (and
    // commands.retried / commands.short.circuited discarded their type the same way). Every member
    // of the family must scrape with the label on the very series the sample landed in.
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordDeadLetterPublished("PlaceOrder");
    metrics.recordDeadLetterExhausted("PlaceOrder");
    metrics.recordCommandRetried("PlaceOrder");
    metrics.recordCommandShortCircuited("PlaceOrder");

    String scrape = registry.scrape();
    for (String family :
        java.util.List.of(
            "streamrune_dlq_published_total",
            "streamrune_dlq_exhausted_total",
            "streamrune_commands_retried_total",
            "streamrune_commands_short_circuited_total")) {
      var labelled =
          java.util.regex.Pattern.compile(
              "^" + family + "\\{[^}]*command_type=\"PlaceOrder\"[^}]*\\} 1\\.0",
              java.util.regex.Pattern.MULTILINE);
      assertTrue(
          labelled.matcher(scrape).find(),
          family + "{command_type=\"PlaceOrder\"} must scrape with count 1.0, got:\n" + scrape);
    }
  }
}

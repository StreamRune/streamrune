package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

class MicrometerStreamRuneMetricsTest {

  @Test
  void countersIncrementAndTimersRecord() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandDispatched();
    metrics.recordCommandDispatched();
    metrics.recordCommandSucceeded();
    metrics.recordCommandFailed();
    metrics.recordCommandDuration(1_000_000L);

    metrics.recordEventAppended();
    metrics.recordEventReplayed();
    metrics.recordEventAppendDuration(500_000L);

    metrics.recordProjectionProcessed();
    metrics.recordProjectionFailed();
    metrics.recordProjectionDeadLettered();
    metrics.recordProjectionDuration(250_000L);

    metrics.recordSubscriptionEventReceived();
    metrics.recordSubscriptionDeliveryLatency(100_000L);

    // These four names carry a tag key on EVERY series (command.type / event.type /
    // projection.name), so they are located with find(...) rather than the bare
    // registry.counter(name)/timer(name) lookup — which would AUTO-VIVIFY a second, tag-less shape
    // of the metric family and read 0.0. The bare lookup is exactly the shape mistake this fix is
    // about, so the assertions must not use it on a tagged family.
    assertEquals(2.0, registry.find("streamrune.commands.dispatched").counter().count());
    assertEquals(1.0, registry.counter("streamrune.commands.succeeded").count());
    assertEquals(1.0, registry.counter("streamrune.commands.failed").count());
    assertEquals(1L, registry.timer("streamrune.commands.duration").count());

    assertEquals(1.0, registry.find("streamrune.events.appended").counter().count());
    assertEquals(1.0, registry.counter("streamrune.events.replayed").count());
    // The emitted timer is the DOCUMENTED MetricNames.EVENTS_DURATION contract
    // ("streamrune.events.duration"), not the old "streamrune.events.append.duration" this test
    // used to pin — which is why nothing caught the drift.
    assertEquals(1L, registry.timer(MetricNames.EVENTS_DURATION).count());

    assertEquals(1.0, registry.find("streamrune.projections.processed").counter().count());
    assertEquals(1.0, registry.find("streamrune.projections.failed").counter().count());
    assertEquals(1.0, registry.find("streamrune.projections.dead_lettered").counter().count());
    assertEquals(1L, registry.find("streamrune.projections.duration").timer().count());

    assertEquals(1.0, registry.counter("streamrune.subscriptions.events.received").count());
    assertEquals(1L, registry.timer("streamrune.subscriptions.delivery.latency").count());
  }

  @Test
  void subscriptionLagGaugeReflectsLatestValuePerSubscription() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 30);
    metrics.recordSubscriptionLag(SubscriptionName.of("payments"), 5);

    var ordersGauge =
        registry.find("streamrune.subscriptions.lag").tag("subscription.name", "orders").gauge();
    var paymentsGauge =
        registry.find("streamrune.subscriptions.lag").tag("subscription.name", "payments").gauge();
    assertNotNull(ordersGauge);
    assertNotNull(paymentsGauge);
    assertEquals(30.0, ordersGauge.value());
    assertEquals(5.0, paymentsGauge.value());

    // Re-reporting must update the existing gauge, not register a second meter.
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 12);
    assertEquals(12.0, ordersGauge.value());
    assertEquals(
        1,
        registry
            .find("streamrune.subscriptions.lag")
            .tag("subscription.name", "orders")
            .gauges()
            .size());
  }

  @Test
  void liveLagGaugeRecomputesFromItsSupplierOnEveryScrape() {
    // A supplier-backed lag gauge reads current lag on each scrape, independent of any
    // health-endpoint push. Registering it once and mutating the supplier's source must be visible
    // without another register/record call.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    var live = new java.util.concurrent.atomic.AtomicLong(30);

    metrics.registerSubscriptionLagGauge(SubscriptionName.of("orders"), live::get);

    var gauge =
        registry.find("streamrune.subscriptions.lag").tag("subscription.name", "orders").gauge();
    assertNotNull(gauge);
    assertEquals(30.0, gauge.value());

    // Falling behind — the very next scrape reflects it, no re-registration.
    live.set(140);
    assertEquals(140.0, gauge.value());

    // Registering the same name again is a no-op (one meter), and the push variant must not add a
    // competing meter for a supplier-backed name.
    metrics.registerSubscriptionLagGauge(SubscriptionName.of("orders"), () -> 999L);
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 7);
    assertEquals(
        1,
        registry
            .find("streamrune.subscriptions.lag")
            .tag("subscription.name", "orders")
            .gauges()
            .size());
    assertEquals(140.0, gauge.value(), "the original supplier gauge stays authoritative");
  }

  @Test
  void pushGaugeRegisteredBeforeSupplier_supplierBecomesLiveSourceInsteadOfFreezing() {
    // A health scrape can push a lag value (recordSubscriptionLag) BEFORE the live
    // supplier
    // gauge is registered, creating an AtomicLong-backed push gauge under the
    // subscriptions.lag{subscription.name} id. registerSubscriptionLagGauge then registers a
    // supplier
    // gauge with the SAME id — Micrometer dedups by id, so without the fix it keeps the stale push
    // gauge and drops the supplier, while the push path disables itself (containsKey==true). The
    // gauge then freezes at the last pushed value. The fix removes the pre-existing push meter
    // first
    // so the supplier gauge becomes the live source of truth.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    // 1) Health scrape pushes a lag value first — registers the push gauge at 5.
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 5);

    // 2) The subscription's live supplier is registered afterwards.
    var live = new java.util.concurrent.atomic.AtomicLong(100);
    metrics.registerSubscriptionLagGauge(SubscriptionName.of("orders"), live::get);

    var gauge =
        registry.find("streamrune.subscriptions.lag").tag("subscription.name", "orders").gauge();
    assertNotNull(gauge);
    // Exactly one meter for this id — the stale push gauge must not linger beside the supplier.
    assertEquals(
        1,
        registry
            .find("streamrune.subscriptions.lag")
            .tag("subscription.name", "orders")
            .gauges()
            .size());
    // The gauge reflects the LIVE supplier value (100), not the frozen push value (5)...
    assertEquals(
        100.0, gauge.value(), "supplier gauge must be the live source, not the frozen push value");
    // ...and tracks the supplier as lag changes, instead of freezing.
    live.set(250);
    assertEquals(
        250.0,
        gauge.value(),
        "supplier-backed lag gauge must track live lag after a prior push, not freeze");
  }

  @Test
  void supplierBackedLagGaugeSurvivesGc_stillReadsLiveLagNotNaN() {
    // The live supplier gauge must keep reading current lag for the whole process
    // lifetime. Micrometer's 3-arg Gauge.builder holds its state object (here the supplier) via a
    // WeakReference by default, so if nothing strongly references the supplier it is collected on
    // the first GC and the gauge reads Double.NaN forever — defeating the point of a live gauge.
    // This test registers
    // a supplier, drops every local strong reference to it, forces a GC, and asserts the gauge
    // still
    // returns the live value. Fail-first: before the fix it reads NaN here.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    // Register in a helper so neither the supplier lambda nor the AtomicLong it closes over is
    // reachable from this method's frame afterwards — only the metrics object can keep them alive.
    registerLiveLagSupplierAndDropLocalRef(metrics);

    var gauge =
        registry.find("streamrune.subscriptions.lag").tag("subscription.name", "gc-orders").gauge();
    assertNotNull(gauge);

    forceGc();

    assertEquals(
        88.0,
        gauge.value(),
        "supplier-backed lag gauge must survive GC and still read the live lag, not NaN");
  }

  private static void registerLiveLagSupplierAndDropLocalRef(MicrometerStreamRuneMetrics metrics) {
    var live = new java.util.concurrent.atomic.AtomicLong(88);
    metrics.registerSubscriptionLagGauge(SubscriptionName.of("gc-orders"), live::get);
    // `live` and the `live::get` lambda go out of scope on return: unless the metrics object
    // retains
    // the supplier with a STRONG reference, the next GC collects them and the gauge reads NaN.
  }

  /**
   * Forces at least one real GC cycle by allocating pressure until a weakly-referenced canary is
   * cleared — so the fail-first assertion is deterministic (a bare {@code System.gc()} is only a
   * hint). After this returns, any object reachable only through a Micrometer {@code WeakReference}
   * has been collected.
   */
  private static void forceGc() {
    var canary = new java.lang.ref.WeakReference<>(new Object());
    while (canary.get() != null) {
      System.gc();
      byte[] pressure = new byte[4 * 1024 * 1024];
      pressure[0] = 1; // touch so the allocation is not optimized away
    }
  }

  @Test
  void relayObservabilityGaugesTrackLatestSampledValues() {
    // The command DLQ depth gauge and the two relay/runner degradation gauges are
    // eagerly registered (discoverable before the first sample) and update in place per sample.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    assertNotNull(registry.find("streamrune.dlq.pending").gauge());
    assertNotNull(registry.find("streamrune.outbox.relay.consecutive_failures").gauge());
    assertNotNull(registry.find("streamrune.dlq.retry.consecutive_failures").gauge());

    metrics.recordDlqBacklog(7);
    metrics.recordOutboxRelayDegradation(3);
    metrics.recordDlqRetryDegradation(2);

    assertEquals(7.0, registry.find("streamrune.dlq.pending").gauge().value());
    assertEquals(
        3.0, registry.find("streamrune.outbox.relay.consecutive_failures").gauge().value());
    assertEquals(2.0, registry.find("streamrune.dlq.retry.consecutive_failures").gauge().value());

    // Last write wins (gauge semantics).
    metrics.recordDlqBacklog(0);
    assertEquals(0.0, registry.find("streamrune.dlq.pending").gauge().value());
  }

  @Test
  void projectionDeadLetterBacklogGaugeTracksLatestSampledValue() {
    // The projection dead-letter backlog gauge is eagerly registered (discoverable
    // before the first sample) and updates in place per sample, like the command DLQ depth gauge.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    assertNotNull(registry.find("streamrune.projections.dead_letter_backlog").gauge());

    metrics.recordProjectionDeadLetterBacklog(4);
    assertEquals(4.0, registry.find("streamrune.projections.dead_letter_backlog").gauge().value());

    // Last write wins (gauge semantics).
    metrics.recordProjectionDeadLetterBacklog(0);
    assertEquals(0.0, registry.find("streamrune.projections.dead_letter_backlog").gauge().value());
  }

  @Test
  void deliveryModeGauge_isOnePerRegistration_andIdempotent() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("audit"), ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
    var orders =
        registry
            .find("streamrune.projections.delivery_mode")
            .tags("projection.name", "orders", "delivery.mode", "TRANSACTIONAL_LOCAL")
            .gauge();
    assertNotNull(orders);
    assertEquals(1.0, orders.value());
    assertEquals(2, registry.find("streamrune.projections.delivery_mode").gauges().size());
  }

  @Test
  void listenerReconnectIncrementsCounterPerChannel() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordListenerReconnect("streamrune_events");
    metrics.recordListenerReconnect("streamrune_events");
    metrics.recordListenerReconnect("other_channel");

    var events =
        registry
            .find("streamrune.subscriptions.listener.reconnects")
            .tag("channel", "streamrune_events")
            .counter();
    var other =
        registry
            .find("streamrune.subscriptions.listener.reconnects")
            .tag("channel", "other_channel")
            .counter();
    assertNotNull(events);
    assertNotNull(other);
    assertEquals(2.0, events.count());
    assertEquals(1.0, other.count());
  }

  @Test
  void outboxDeliveryFailedIncrementsCounter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxDeliveryFailed();
    metrics.recordOutboxDeliveryFailed();

    assertEquals(2.0, registry.counter("streamrune.outbox.delivery_failed").count());
  }

  @Test
  void outboxBacklogUpdatesPendingGauge() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    // Registered eagerly at construction (discoverable before the first sample), default 0.
    assertEquals(0.0, registry.get("streamrune.outbox.pending").gauge().value());

    metrics.recordOutboxBacklog(42);
    assertEquals(42.0, registry.get("streamrune.outbox.pending").gauge().value());

    // Gauge semantics: re-reporting updates the same meter in place (last write wins).
    metrics.recordOutboxBacklog(7);
    assertEquals(7.0, registry.get("streamrune.outbox.pending").gauge().value());
    assertEquals(1, registry.find("streamrune.outbox.pending").gauges().size());
  }

  @Test
  void outboxBlockageGauges_areEagerAndTrackTheLatestSample() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    assertEquals(0.0, registry.get("streamrune.outbox.blocked_aggregates").gauge().value());
    assertEquals(0.0, registry.get("streamrune.outbox.blockage_age_seconds").gauge().value());

    metrics.recordOutboxBlockedAggregates(3);
    metrics.recordOutboxBlockageAge(912);
    assertEquals(3.0, registry.get("streamrune.outbox.blocked_aggregates").gauge().value());
    assertEquals(912.0, registry.get("streamrune.outbox.blockage_age_seconds").gauge().value());

    metrics.recordOutboxBlockedAggregates(0);
    metrics.recordOutboxBlockageAge(0);
    assertEquals(0.0, registry.get("streamrune.outbox.blocked_aggregates").gauge().value());
    assertEquals(1, registry.find("streamrune.outbox.blocked_aggregates").gauges().size());
  }

  @Test
  void outboxSkippedCounters_incrementPerRow_andFailedSweptIsGone() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    metrics.recordOutboxSkipped();
    metrics.recordOutboxSkipped();
    metrics.recordOutboxSkippedSwept(7);
    assertEquals(2.0, registry.counter("streamrune.outbox.skipped").count());
    assertEquals(7.0, registry.counter("streamrune.outbox.skipped_swept").count());
    assertNull(
        registry.find("streamrune.outbox.failed_swept").counter(),
        "the FAILED sweep no longer exists, so neither does its series");
  }

  @Test
  void outboxReplayedIncrementsByRows() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordOutboxReplayed(4);
    metrics.recordOutboxReplayed(3);

    assertEquals(7.0, registry.counter("streamrune.outbox.replayed").count());
  }

  /**
   * Drift guard. {@link MetricNames} is the operator-facing contract — dashboards, PromQL alerts
   * and SLOs are written against those constants. Nothing used to link them to the meter names this
   * class actually registers (they were built by string concatenation), so {@code events.duration}
   * silently diverged into {@code events.append.duration}: the documented series never existed and
   * its own test pinned the wrong name.
   *
   * <p>This test closes the loop mechanically: it reflects over EVERY {@code streamrune.*} constant
   * in {@code MetricNames} and asserts a meter with exactly that name is registered after driving
   * every {@code record*} entry point once (lazily-registered tagged meters included). Add a
   * constant without emitting it — or rename an emitted meter away from its constant — and this
   * fails.
   */
  @Test
  void everyMetricNamesConstantHasAMatchingRegisteredMeter() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    driveEveryRecordingEntryPoint(metrics);

    Set<String> registered =
        registry.getMeters().stream()
            .map(meter -> meter.getId().getName())
            .collect(Collectors.toSet());

    List<String> missing = new ArrayList<>();
    for (String documented : documentedMetricNames()) {
      if (!registered.contains(documented)) {
        missing.add(documented);
      }
    }
    assertTrue(
        missing.isEmpty(),
        "MetricNames constants with no registered meter of the same name (the contract and the"
            + " emission have drifted): "
            + missing
            + "; registered names were "
            + registered.stream().sorted().toList());
  }

  /**
   * Every {@code public static final String} in {@link MetricNames} whose value is a meter name.
   * Tag-key constants are excluded structurally: only metric names carry the {@code streamrune.}
   * prefix (tag keys are {@code command.type}, {@code saga.type}, …).
   */
  private static List<String> documentedMetricNames() {
    List<String> names = new ArrayList<>();
    for (Field field : MetricNames.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.getType() != String.class) {
        continue;
      }
      try {
        String value = (String) field.get(null);
        if (value != null && value.startsWith(MicrometerStreamRuneMetrics.DEFAULT_PREFIX + ".")) {
          names.add(value);
        }
      } catch (IllegalAccessException e) {
        throw new AssertionError("MetricNames constant not readable: " + field.getName(), e);
      }
    }
    assertFalse(names.isEmpty(), "reflection found no MetricNames constants — the guard is inert");
    return names;
  }

  /** Calls every recording entry point once so lazily-registered tagged meters exist too. */
  private static void driveEveryRecordingEntryPoint(MicrometerStreamRuneMetrics metrics) {
    metrics.recordCommandDispatched("PlaceOrder");
    metrics.recordCommandSucceeded();
    metrics.recordCommandFailed();
    metrics.recordCommandDuration(1_000L);
    metrics.recordCommandRetried("PlaceOrder");
    metrics.recordCommandShortCircuited("PlaceOrder");
    metrics.recordCommandAsyncRejected();
    metrics.registerInFlightCommandsGauge(() -> 5L);
    metrics.recordDeadLetterPublished("PlaceOrder");
    metrics.recordDeadLetterExhausted("PlaceOrder");
    metrics.recordLockWait(1_000L);
    metrics.recordEventAppended("OrderPlaced");
    metrics.recordEventReplayed();
    metrics.recordEventAppendDuration(1_000L);
    metrics.recordSnapshotCreated();
    metrics.recordSnapshotLoaded();
    metrics.recordSnapshotDiscarded("deserialize_failure");
    metrics.recordSubjectRedacted();
    metrics.recordSystemicKeyStoreFailure();
    metrics.recordGdprPurgeFailed("orders-read-model");
    metrics.recordProjectionProcessed();
    metrics.recordProjectionFailed();
    metrics.recordProjectionDeadLettered();
    metrics.recordProjectionDeadLetterBacklog(2);
    metrics.recordProjectionDeliveryMode(
        ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    metrics.recordProjectionDuration(1_000L);
    metrics.recordSubscriptionEventReceived();
    metrics.recordSubscriptionDeliveryLatency(1_000L);
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 3);
    metrics.recordListenerReconnect("streamrune_events");
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
    metrics.recordOutboxInFlightHorizonViolation();
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
  void inFlightGaugeIsDiscoverableEagerlyAndIsSeparateFromThePendingGauge() {
    // A post-hand-off delivery outage holds the claim, so the backlog parks
    // in IN_PROGRESS while outbox.pending reads ~0. The gauge must exist before the first sample
    // (like outbox.pending) and must be its OWN series — folding IN_PROGRESS into
    // streamrune.outbox.pending would silently redefine every alert already written against it.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    var inFlight = registry.find(MetricNames.OUTBOX_IN_FLIGHT).gauge();
    assertNotNull(inFlight, "the gauge must exist before the first sample");
    assertEquals(0.0, inFlight.value());

    metrics.recordOutboxInFlight(7);
    assertEquals(7.0, inFlight.value());
    assertEquals(
        0.0,
        registry.find(MetricNames.OUTBOX_PENDING).gauge().value(),
        "the pending gauge must be untouched — the two series are independent");

    metrics.recordOutboxBacklog(3);
    assertEquals(7.0, inFlight.value(), "sampling pending must not disturb in_flight");
    assertEquals(3.0, registry.find(MetricNames.OUTBOX_PENDING).gauge().value());

    metrics.recordOutboxInFlight(2);
    assertEquals(2.0, inFlight.value(), "last write wins");
    assertEquals(1, registry.find(MetricNames.OUTBOX_IN_FLIGHT).gauges().size());
  }

  @Test
  void faultedBacklogGaugeIsDiscoverableEagerlyAndTracksTheLatestSample() {
    // A fleet-wide gauge of dead-letter entries whose saga is still
    // FAULTED — the population the retention sweep now deliberately protects. Registered eagerly
    // (like outbox.pending) so it is scrapeable before the first sweep cycle, and updated in place
    // afterwards (gauge semantics, last write wins).
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    var gauge = registry.find(MetricNames.SAGA_FAULTED_BACKLOG).gauge();
    assertNotNull(gauge, "the gauge must exist before the first sample");
    assertEquals(0.0, gauge.value());

    metrics.recordSagaFaultedBacklog(4);
    assertEquals(4.0, gauge.value());

    metrics.recordSagaFaultedBacklog(1);
    assertEquals(1.0, gauge.value(), "last write wins");
    assertEquals(1, registry.find(MetricNames.SAGA_FAULTED_BACKLOG).gauges().size());
  }

  @Test
  void faultedRowsGaugeIsRegisteredPerSagaTypeAndTracksTheLatestSample() {
    // SagaCompensationRetrySweeper.sampleBacklog now calls
    // metrics.recordSagaFaultedRows(sagaType, count) to make an entry-less
    // FAULTED saga observable, but MicrometerStreamRuneMetrics never overrode it — the SPI's
    // default no-op silently absorbed every sample, so the series never reached a real registry.
    // Mirrors recordSagaCompensatingBacklog/recordSagaTimedOutBacklog exactly: one gauge per saga
    // type, tagged with TAG_SAGA_TYPE, last-write-wins.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordSagaFaultedRows("OrderSaga", 3);
    metrics.recordSagaFaultedRows("ShipmentSaga", 1);

    var orderGauge =
        registry
            .find(MetricNames.SAGA_FAULTED_ROWS)
            .tag(MetricNames.TAG_SAGA_TYPE, "OrderSaga")
            .gauge();
    var shipmentGauge =
        registry
            .find(MetricNames.SAGA_FAULTED_ROWS)
            .tag(MetricNames.TAG_SAGA_TYPE, "ShipmentSaga")
            .gauge();
    assertNotNull(orderGauge, "streamrune.saga.faulted_rows must be registered per saga type");
    assertNotNull(shipmentGauge);
    assertEquals(3.0, orderGauge.value());
    assertEquals(1.0, shipmentGauge.value());

    // Last write wins (gauge semantics), same meter re-used rather than a second registration.
    metrics.recordSagaFaultedRows("OrderSaga", 0);
    assertEquals(0.0, orderGauge.value());
    assertEquals(
        1,
        registry
            .find(MetricNames.SAGA_FAULTED_ROWS)
            .tag(MetricNames.TAG_SAGA_TYPE, "OrderSaga")
            .gauges()
            .size());
  }

  @Test
  void projectionNameAndQueryTypeTagsAreEmittedNotDiscarded() {
    // projection.name / query.type are documented (docs/guide/production.md) as "key
    // tags" an operator can filter/group by, but MicrometerStreamRuneMetrics never overrode the
    // ProjectionName-tagged StreamRuneMetrics defaults (they fall through to the untagged
    // no-arg method) and discarded the queryType parameter entirely on the query methods — so
    // `sum by (projection_name) (...)` / `sum by (query.type) (...)` always returned nothing.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordProjectionProcessed(ProjectionName.of("orders"));
    metrics.recordProjectionFailed(ProjectionName.of("orders"));
    metrics.recordProjectionDeadLettered(ProjectionName.of("orders"));
    metrics.recordProjectionDuration(ProjectionName.of("orders"), 1_000L);

    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_PROCESSED)
            .tag(MetricNames.TAG_PROJECTION_NAME, "orders")
            .counter()
            .count(),
        "streamrune.projections.processed must carry a projection.name=orders series");
    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_FAILED)
            .tag(MetricNames.TAG_PROJECTION_NAME, "orders")
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.PROJECTIONS_DEAD_LETTERED)
            .tag(MetricNames.TAG_PROJECTION_NAME, "orders")
            .counter()
            .count());
    assertEquals(
        1L,
        registry
            .find(MetricNames.PROJECTIONS_DURATION)
            .tag(MetricNames.TAG_PROJECTION_NAME, "orders")
            .timer()
            .count());

    metrics.recordQueryDispatched("FindOrder");
    metrics.recordQueryDuration("FindOrder", 2_000L);
    metrics.recordQueryCacheHit("FindOrder");
    metrics.recordQueryCacheMiss("FindOrder");

    assertEquals(
        1.0,
        registry
            .find(MetricNames.QUERIES_DISPATCHED)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrder")
            .counter()
            .count(),
        "streamrune.queries.dispatched must carry a query.type=FindOrder series");
    assertEquals(
        1L,
        registry
            .find(MetricNames.QUERIES_DURATION)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrder")
            .timer()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.QUERIES_CACHE_HITS)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrder")
            .counter()
            .count());
    assertEquals(
        1.0,
        registry
            .find(MetricNames.QUERIES_CACHE_MISSES)
            .tag(MetricNames.TAG_QUERY_TYPE, "FindOrder")
            .counter()
            .count());
  }

  @Test
  void everyDocumentedTagKeyIsEmittedOnAtLeastOneRegisteredMeter() {
    // Drift guard, the tag-key analogue of everyMetricNamesConstantHasAMatchingMeter:
    // that guard only checks metric NAMES, so a documented tag key (MetricNames.TAG_*) could be
    // declared and never actually attached to any emitted series — exactly what happened to
    // TAG_PROJECTION_NAME and TAG_QUERY_TYPE before this fix. Reflects over every TAG_* constant
    // and asserts at least one meter registered after driving every entry point carries it.
    //
    // NOT_YET_WIRED_TAGS below are constants this reflection sweep also turned up that are
    // orphaned for a DIFFERENT reason: nothing in the codebase — not even a
    // record*(..., tagValue) parameter — carries them anywhere, so there is no discarded value to
    // wire up here (unlike projection.name/query.type, which WERE received as parameters and
    // silently dropped). Reported separately for routing rather than guessed at in this fix.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    driveEveryRecordingEntryPoint(metrics);
    metrics.recordProjectionProcessed(ProjectionName.of("orders"));
    metrics.recordProjectionFailed(ProjectionName.of("orders"));
    metrics.recordProjectionDeadLettered(ProjectionName.of("orders"));
    metrics.recordProjectionDuration(ProjectionName.of("orders"), 1_000L);

    Set<String> emittedTagKeys =
        registry.getMeters().stream()
            .flatMap(m -> m.getId().getTags().stream())
            .map(io.micrometer.core.instrument.Tag::getKey)
            .collect(Collectors.toSet());

    Set<String> notYetWiredTags = Set.of(MetricNames.TAG_STREAM_ID);

    List<String> missing = new ArrayList<>();
    for (String tagKey : documentedTagKeys()) {
      if (!emittedTagKeys.contains(tagKey) && !notYetWiredTags.contains(tagKey)) {
        missing.add(tagKey);
      }
    }
    assertTrue(
        missing.isEmpty(),
        "MetricNames TAG_* constants never attached to any registered meter: "
            + missing
            + "; emitted tag keys were "
            + emittedTagKeys.stream().sorted().toList());
  }

  /** Every {@code public static final String} in {@link MetricNames} whose value is a tag key. */
  private static List<String> documentedTagKeys() {
    List<String> keys = new ArrayList<>();
    for (Field field : MetricNames.class.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers()) || field.getType() != String.class) {
        continue;
      }
      try {
        String value = (String) field.get(null);
        if (value != null && !value.startsWith(MicrometerStreamRuneMetrics.DEFAULT_PREFIX + ".")) {
          keys.add(value);
        }
      } catch (IllegalAccessException e) {
        throw new AssertionError("MetricNames constant not readable: " + field.getName(), e);
      }
    }
    assertFalse(keys.isEmpty(), "reflection found no MetricNames tag-key constants");
    return keys;
  }

  @Test
  void taggedCommandAndEventOverloadsEmitExactlyOneSeriesNoDoubleCount() {
    // recordCommandDispatched(String)/recordEventAppended(String) used to call their
    // untagged sibling AND register a tagged counter under the SAME meter name, so
    // sum(streamrune_commands_dispatched_total) double-counted every call reachable through the
    // tagged SPI (framework call sites use only the untagged variants, so production traffic was
    // unaffected — this is reachable through the public SPI, e.g. a custom bus/interceptor).
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandDispatched("PlaceOrder");
    double totalDispatched =
        registry.find(MetricNames.COMMANDS_DISPATCHED).counters().stream()
            .mapToDouble(io.micrometer.core.instrument.Counter::count)
            .sum();
    assertEquals(
        1.0,
        totalDispatched,
        "one recordCommandDispatched(type) call must add up to exactly ONE across every series"
            + " sharing streamrune.commands.dispatched, tagged or not");

    metrics.recordEventAppended("OrderPlaced");
    double totalAppended =
        registry.find(MetricNames.EVENTS_APPENDED).counters().stream()
            .mapToDouble(io.micrometer.core.instrument.Counter::count)
            .sum();
    assertEquals(1.0, totalAppended, "one recordEventAppended(type) call must not double-count");
  }

  @Test
  void meterRebasesCanonicalNamesAndRejectsNonCanonicalOnes() {
    // The single derivation point. A MetricNames constant is rebased onto the
    // configured prefix; anything not carrying the canonical prefix is a programming error and is
    // rejected eagerly rather than emitting a malformed series that no dashboard would ever find.
    var registry = new SimpleMeterRegistry();
    var defaultPrefixed = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    var customPrefixed =
        MicrometerStreamRuneMetrics.builder()
            .registry(new SimpleMeterRegistry())
            .prefix("myapp.es")
            .build();

    assertEquals("streamrune.events.duration", defaultPrefixed.meter(MetricNames.EVENTS_DURATION));
    assertEquals("myapp.es.events.duration", customPrefixed.meter(MetricNames.EVENTS_DURATION));

    assertThrows(
        IllegalArgumentException.class,
        () -> defaultPrefixed.meter("events.duration"),
        "a bare suffix is not a MetricNames constant");
    assertThrows(
        IllegalArgumentException.class,
        () -> defaultPrefixed.meter("streamruneish.events"),
        "the canonical prefix must be followed by a dot, not merely be a string prefix");
    assertThrows(IllegalArgumentException.class, () -> defaultPrefixed.meter(null));
  }

  @Test
  void builderRequiresRegistry() {
    assertThrows(
        IllegalArgumentException.class, () -> MicrometerStreamRuneMetrics.builder().build());
  }

  @Test
  void customPrefixIsAppliedToAllMeterNames() {
    var registry = new SimpleMeterRegistry();
    var metrics =
        MicrometerStreamRuneMetrics.builder().registry(registry).prefix("myapp.es").build();

    metrics.recordCommandDispatched();
    metrics.recordEventAppended();
    metrics.recordProjectionProcessed();
    metrics.recordSubscriptionEventReceived();

    assertEquals(1.0, registry.find("myapp.es.commands.dispatched").counter().count());
    assertEquals(1.0, registry.find("myapp.es.events.appended").counter().count());
    assertEquals(1.0, registry.find("myapp.es.projections.processed").counter().count());
    assertEquals(1.0, registry.counter("myapp.es.subscriptions.events.received").count());
    assertNull(registry.find("streamrune.commands.dispatched").counter());
  }

  @Test
  void builderRejectsBlankPrefix() {
    var registry = new SimpleMeterRegistry();
    assertThrows(
        IllegalArgumentException.class,
        () -> MicrometerStreamRuneMetrics.builder().registry(registry).prefix(" ").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> MicrometerStreamRuneMetrics.builder().registry(registry).prefix(null).build());
  }

  // ── the command-type family (commands.retried / commands.short.circuited /
  // dlq.published / dlq.exhausted) is tagged per type, on every series ────────────────────────────

  @Test
  void commandTypeFamilyIsTaggedPerType_discoverableFromBoot_andJoinsPublishedToExhausted() {
    // recordCommandRetried/recordCommandShortCircuited/recordDeadLetterPublished(String
    // commandType) took the command type and DISCARDED it, bumping a bare counter — while
    // recordDeadLetterExhausted registered per type. The per-type published↔exhausted join that
    // DeadLetterRetryRunner's documentation promised was therefore impossible: the published side
    // carried no command.type label at all.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    // Discoverable from boot, in the family's REAL (tagged) shape under the UNKNOWN sentinel —
    // never bare, which on a Prometheus registry would win the family and drop every typed sample.
    for (String name :
        List.of(
            MetricNames.COMMANDS_RETRIED,
            MetricNames.COMMANDS_SHORT_CIRCUITED,
            MetricNames.DLQ_PUBLISHED)) {
      var eager =
          registry
              .find(name)
              .tag(MetricNames.TAG_COMMAND_TYPE, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE)
              .counter();
      assertNotNull(eager, name + " must be registered at construction under the UNKNOWN sentinel");
      assertEquals(0.0, eager.count());
      assertTrue(
          registry.find(name).counters().stream()
              .allMatch(c -> c.getId().getTag(MetricNames.TAG_COMMAND_TYPE) != null),
          name + " must carry command.type on EVERY series (one meter name, one tag-key set)");
    }

    metrics.recordCommandRetried("PlaceOrder");
    metrics.recordCommandRetried("PlaceOrder");
    metrics.recordCommandShortCircuited("PlaceOrder");
    metrics.recordDeadLetterPublished("PlaceOrder");
    metrics.recordDeadLetterPublished("PlaceOrder");
    metrics.recordDeadLetterPublished("CancelOrder");
    metrics.recordDeadLetterExhausted("PlaceOrder");

    assertEquals(2.0, typed(registry, MetricNames.COMMANDS_RETRIED, "PlaceOrder"));
    assertEquals(1.0, typed(registry, MetricNames.COMMANDS_SHORT_CIRCUITED, "PlaceOrder"));
    // The documented join: published and exhausted for the SAME command type, same label.
    assertEquals(2.0, typed(registry, MetricNames.DLQ_PUBLISHED, "PlaceOrder"));
    assertEquals(1.0, typed(registry, MetricNames.DLQ_PUBLISHED, "CancelOrder"));
    assertEquals(1.0, typed(registry, MetricNames.DLQ_EXHAUSTED, "PlaceOrder"));
    // A typed sample moves traffic OUT of the sentinel bucket; it never also bumps it.
    assertEquals(
        0.0,
        typed(registry, MetricNames.DLQ_PUBLISHED, MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE));
    for (String name :
        List.of(
            MetricNames.COMMANDS_RETRIED,
            MetricNames.COMMANDS_SHORT_CIRCUITED,
            MetricNames.DLQ_PUBLISHED,
            MetricNames.DLQ_EXHAUSTED)) {
      assertTrue(
          registry.find(name).counters().stream()
              .allMatch(c -> c.getId().getTag(MetricNames.TAG_COMMAND_TYPE) != null),
          name + " must carry command.type on EVERY series after typed samples too");
    }
  }

  @Test
  void commandTypeFamilyReportsTheUnknownSentinelForANullOrBlankType() {
    // A custom bus/interceptor calling the SPI without a type must neither NPE inside
    // Micrometer (Tag.of rejects a null value) nor open a second, tag-less shape of the family —
    // it lands in the documented UNKNOWN sentinel series, like every other untagged entry point.
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();

    metrics.recordCommandRetried(null);
    metrics.recordCommandShortCircuited("");
    metrics.recordDeadLetterPublished(null);
    metrics.recordDeadLetterExhausted("  ");

    String unknown = MicrometerStreamRuneMetrics.UNKNOWN_TAG_VALUE;
    assertEquals(1.0, typed(registry, MetricNames.COMMANDS_RETRIED, unknown));
    assertEquals(1.0, typed(registry, MetricNames.COMMANDS_SHORT_CIRCUITED, unknown));
    assertEquals(1.0, typed(registry, MetricNames.DLQ_PUBLISHED, unknown));
    assertEquals(1.0, typed(registry, MetricNames.DLQ_EXHAUSTED, unknown));
  }

  private static double typed(SimpleMeterRegistry registry, String name, String commandType) {
    var counter = registry.find(name).tag(MetricNames.TAG_COMMAND_TYPE, commandType).counter();
    assertNotNull(counter, name + "{command.type=" + commandType + "} must exist");
    return counter.count();
  }
}

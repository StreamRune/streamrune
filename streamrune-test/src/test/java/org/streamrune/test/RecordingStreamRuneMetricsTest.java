package org.streamrune.test;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

class RecordingStreamRuneMetricsTest {

  /**
   * The meter each method reports, as {@code MicrometerStreamRuneMetrics} registers it; the
   * overloads of one name report one meter.
   */
  private static final Map<String, String> METER_OF =
      Map.ofEntries(
          entry("recordCommandDispatched", MetricNames.COMMANDS_DISPATCHED),
          entry("recordCommandSucceeded", MetricNames.COMMANDS_SUCCEEDED),
          entry("recordCommandFailed", MetricNames.COMMANDS_FAILED),
          entry("recordCommandDuration", MetricNames.COMMANDS_DURATION),
          entry("recordCommandRetried", MetricNames.COMMANDS_RETRIED),
          entry("recordCommandShortCircuited", MetricNames.COMMANDS_SHORT_CIRCUITED),
          entry("recordCommandAsyncRejected", MetricNames.COMMANDS_ASYNC_REJECTED),
          entry("registerInFlightCommandsGauge", MetricNames.COMMANDS_IN_FLIGHT),
          entry("recordDeadLetterPublished", MetricNames.DLQ_PUBLISHED),
          entry("recordDeadLetterExhausted", MetricNames.DLQ_EXHAUSTED),
          entry("recordDeadLetterStaleReplayBlocked", MetricNames.DLQ_STALE_REPLAY_BLOCKED),
          entry("recordLockWait", MetricNames.LOCKS_WAIT),
          entry("recordEventAppended", MetricNames.EVENTS_APPENDED),
          entry("recordEventReplayed", MetricNames.EVENTS_REPLAYED),
          entry("recordEventAppendDuration", MetricNames.EVENTS_DURATION),
          entry("recordSnapshotCreated", MetricNames.SNAPSHOTS_CREATED),
          entry("recordSnapshotLoaded", MetricNames.SNAPSHOTS_LOADED),
          entry("recordSnapshotRefused", MetricNames.SNAPSHOTS_REFUSED),
          entry("recordSnapshotDiscarded", MetricNames.SNAPSHOTS_DISCARDED),
          entry("recordSubjectRedacted", MetricNames.CRYPTO_SUBJECT_REDACTED),
          entry("recordSystemicKeyStoreFailure", MetricNames.CRYPTO_KEYSTORE_SYSTEMIC_FAILURE),
          entry("recordGdprPurgeFailed", MetricNames.GDPR_PURGE_FAILED),
          entry("recordProjectionProcessed", MetricNames.PROJECTIONS_PROCESSED),
          entry("recordProjectionFailed", MetricNames.PROJECTIONS_FAILED),
          entry("recordProjectionDeadLettered", MetricNames.PROJECTIONS_DEAD_LETTERED),
          entry("recordProjectionDuration", MetricNames.PROJECTIONS_DURATION),
          entry("recordProjectionDeadLetterBacklog", MetricNames.PROJECTIONS_DEAD_LETTER_BACKLOG),
          entry("recordProjectionDeliveryMode", MetricNames.PROJECTIONS_DELIVERY_MODE),
          entry("recordSubscriptionEventReceived", MetricNames.SUBSCRIPTIONS_EVENTS_RECEIVED),
          entry("recordSubscriptionDeliveryLatency", MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY),
          entry("recordSubscriptionLag", MetricNames.SUBSCRIPTIONS_LAG),
          entry("registerSubscriptionLagGauge", MetricNames.SUBSCRIPTIONS_LAG),
          entry("recordListenerReconnect", MetricNames.SUBSCRIPTIONS_LISTENER_RECONNECTS),
          entry("recordQueryDispatched", MetricNames.QUERIES_DISPATCHED),
          entry("recordQueryDuration", MetricNames.QUERIES_DURATION),
          entry("recordQueryCacheHit", MetricNames.QUERIES_CACHE_HITS),
          entry("recordQueryCacheMiss", MetricNames.QUERIES_CACHE_MISSES),
          entry("recordSagaQuarantined", MetricNames.SAGA_QUARANTINED),
          entry("recordSagaFaulted", MetricNames.SAGA_FAULTED),
          entry("recordSagaSkippedWhileFaulted", MetricNames.SAGA_SKIPPED_WHILE_FAULTED),
          entry("recordSagaEventHeld", MetricNames.SAGA_EVENT_HELD),
          entry("recordSagaReplayDeferred", MetricNames.SAGA_REPLAY_DEFERRED),
          entry("recordSagaCompensation", MetricNames.SAGA_COMPENSATION),
          entry("recordSagaCompensationRetry", MetricNames.SAGA_COMPENSATION_RETRIES),
          entry("recordSagaCasConflict", MetricNames.SAGA_CAS_CONFLICTS),
          entry(
              "recordSagaCompensationUnderForwardStep",
              MetricNames.SAGA_COMPENSATION_UNDER_FORWARD_STEP),
          entry("recordSagaForwardCommandFenced", MetricNames.SAGA_FORWARD_COMMANDS_FENCED),
          entry("recordSagaDeliveryRetried", MetricNames.SAGA_DELIVERY_RETRIED),
          entry("recordSagaCompensatingBacklog", MetricNames.SAGA_COMPENSATING),
          entry("recordSagaTimedOutBacklog", MetricNames.SAGA_TIMED_OUT),
          entry("recordSagaTimeoutClaimDeferred", MetricNames.SAGA_TIMEOUT_CLAIMS_DEFERRED),
          entry("recordSagaFaultedBacklog", MetricNames.SAGA_FAULTED_BACKLOG),
          entry("recordSagaFaultedRows", MetricNames.SAGA_FAULTED_ROWS),
          entry("recordSagaReplayed", MetricNames.SAGA_REPLAYED),
          entry("recordSagaForcedStaleResume", MetricNames.SAGA_FORCED_STALE_RESUME),
          entry("recordSagaResumeFaulted", MetricNames.SAGA_RESUME_FAULTED),
          entry("recordSagaCompensateFaulted", MetricNames.SAGA_COMPENSATE_FAULTED),
          entry("recordInboxReplayHit", MetricNames.INBOX_REPLAY_HITS),
          entry("recordInboxSwept", MetricNames.INBOX_SWEPT_ROWS),
          entry("recordSagaDeadLetterSwept", MetricNames.SAGA_DEAD_LETTERS_SWEPT),
          entry("recordOutboxSwept", MetricNames.OUTBOX_SWEPT_ROWS),
          entry("recordOutboxDeliveryFailed", MetricNames.OUTBOX_DELIVERY_FAILED),
          entry("recordOutboxBacklog", MetricNames.OUTBOX_PENDING),
          entry("recordOutboxInFlight", MetricNames.OUTBOX_IN_FLIGHT),
          entry(
              "recordOutboxInFlightHorizonViolation",
              MetricNames.OUTBOX_IN_FLIGHT_HORIZON_VIOLATION),
          entry("recordOutboxReplayed", MetricNames.OUTBOX_REPLAYED),
          entry("recordOutboxBlockedAggregates", MetricNames.OUTBOX_BLOCKED_AGGREGATES),
          entry("recordOutboxBlockageAge", MetricNames.OUTBOX_BLOCKAGE_AGE_SECONDS),
          entry("recordOutboxSkipped", MetricNames.OUTBOX_SKIPPED),
          entry("recordOutboxSkippedSwept", MetricNames.OUTBOX_SKIPPED_SWEPT),
          entry("recordDeadLetterSwept", MetricNames.DLQ_SWEPT_ROWS),
          entry("recordDlqBacklog", MetricNames.DLQ_PENDING),
          entry("recordDlqAwaitingOperator", MetricNames.DLQ_AWAITING_OPERATOR),
          entry("recordOutboxRelayDegradation", MetricNames.OUTBOX_RELAY_DEGRADED),
          entry("recordDlqRetryDegradation", MetricNames.DLQ_RETRY_DEGRADED));

  private static Set<String> metricNamesConstants(boolean meters) {
    Set<String> values = new TreeSet<>();
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          String value = (String) field.get(null);
          if (value.startsWith("streamrune.") == meters) {
            values.add(value);
          }
        } catch (IllegalAccessException e) {
          throw new AssertionError(e);
        }
      }
    }
    return values;
  }

  /**
   * A meter is a constant named under {@code streamrune.}, a tag key one named {@code TAG_*} that
   * is not: {@link MetricNames} holds no constant of another kind, which the recording double reads
   * as neither.
   */
  @Test
  void everyStringConstantOfMetricNames_isAMeterOrATagKey() throws Exception {
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        String value = (String) field.get(null);
        assertEquals(
            !field.getName().startsWith("TAG_"),
            value.startsWith("streamrune."),
            () -> field.getName() + " = " + value + " is neither a meter nor a tag key");
      }
    }
  }

  /** Constants of every kind the double tells apart. */
  static final class Names {
    public static final String ORDERS_PLACED = "streamrune.orders.placed";
    public static final String TAG_REGION = "region";
    public static final String OUTCOME_SUCCESS = "success";
    public static final String TAG_PREFIX = "streamrune.";
    public static final int LIMIT = 3;
  }

  @Test
  void aConstantOfAnotherKind_isNeitherAMeterNorATagKey() {
    assertEquals(
        Set.of("streamrune.orders.placed"),
        RecordingStreamRuneMetrics.constants(Names.class, true));
    assertEquals(Set.of("region"), RecordingStreamRuneMetrics.constants(Names.class, false));
  }

  @Test
  void everyMethodOfTheInterfaceIsOverridden() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        StreamRuneMetrics.class, RecordingStreamRuneMetrics.class, 86);
  }

  @Test
  void everyMeterOfMetricNames_isReportedByAMethod_andTheTableNamesEveryMethod() {
    Set<String> methodNames = new TreeSet<>();
    ForwardingDecoratorChecks.methodsOf(StreamRuneMetrics.class)
        .forEach(method -> methodNames.add(method.getName()));

    assertEquals(methodNames, new TreeSet<>(METER_OF.keySet()));
    assertEquals(metricNamesConstants(true), new TreeSet<>(METER_OF.values()));
  }

  /**
   * Every call is recorded once, under its method's meter, with each name it carries as the value
   * of a {@link MetricNames} tag and its {@code long} argument as the value; a supplied gauge is
   * read through {@code lastGauge}.
   */
  @Test
  void everyCall_isRecordedUnderItsMeter_withItsNamesAsTags_andItsAmountAsTheValue()
      throws Exception {
    Set<String> tagKeys = metricNamesConstants(false);
    for (Method method : ForwardingDecoratorChecks.methodsOf(StreamRuneMetrics.class)) {
      var metrics = new RecordingStreamRuneMetrics();
      Class<?>[] types = method.getParameterTypes();
      Object[] args = new Object[types.length];
      for (int i = 0; i < types.length; i++) {
        args[i] = ForwardingDecoratorChecks.sample(types[i], i + 1, 0);
      }

      method.invoke(metrics, args);

      String meter = METER_OF.get(method.getName());
      List<String> names = new ArrayList<>();
      Long amount = null;
      LongSupplier supplier = null;
      for (Object arg : args) {
        switch (arg) {
          case String s -> names.add(s);
          case ProjectionName p -> names.add(p.value());
          case SubscriptionName s -> names.add(s.value());
          case ProjectionDeliveryMode mode -> names.add(mode.name());
          case Long l -> amount = l;
          case LongSupplier s -> supplier = s;
          default -> throw new AssertionError("no rule for " + arg + " of " + method);
        }
      }
      String[] tagPairs = tagPairsFor(metrics, meter, method);
      if (supplier != null) {
        assertTrue(metrics.samples().isEmpty(), () -> method + " recorded " + metrics.samples());
        assertEquals(
            OptionalLong.of(supplier.getAsLong()),
            metrics.lastGauge(meter, tagPairs),
            () -> method + " does not read its supplier: " + metrics);
        continue;
      }
      assertEquals(1, metrics.samples().size(), () -> method + " recorded " + metrics.samples());
      RecordingStreamRuneMetrics.Sample sample = metrics.samples().getFirst();
      assertEquals(meter, sample.meterName(), () -> method + " reports another meter");
      assertTrue(
          tagKeys.containsAll(sample.tags().keySet()),
          () -> method + " uses a tag key outside MetricNames: " + sample.tags());
      assertEquals(
          names,
          new ArrayList<>(sample.tags().values()),
          () -> method + " does not carry its names as tags: " + sample.tags());
      assertEquals(amount == null ? 1L : amount, sample.value(), () -> method + " value");
    }
  }

  /** The tag pairs the single sample or supplied gauge of {@code method} is reported under. */
  private static String[] tagPairsFor(
      RecordingStreamRuneMetrics metrics, String meter, Method method) {
    if (method.getName().equals("registerSubscriptionLagGauge")) {
      return new String[] {
        MetricNames.TAG_SUBSCRIPTION_NAME,
        ((SubscriptionName) ForwardingDecoratorChecks.sample(SubscriptionName.class, 1, 0)).value()
      };
    }
    return new String[0];
  }

  @Test
  void count_total_andSamples_readWhatWasRecorded() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordSagaCompensation("order", "success");
    metrics.recordSagaCompensation("order", "failure");
    metrics.recordSagaCompensation("payment", "success");
    metrics.recordInboxSwept(3);
    metrics.recordInboxSwept(4);

    assertEquals(3, metrics.count(MetricNames.SAGA_COMPENSATION));
    assertEquals(
        2, metrics.count(MetricNames.SAGA_COMPENSATION, MetricNames.TAG_SAGA_TYPE, "order"));
    assertEquals(
        1,
        metrics.count(
            MetricNames.SAGA_COMPENSATION,
            MetricNames.TAG_SAGA_TYPE,
            "order",
            MetricNames.TAG_OUTCOME,
            "failure"));
    assertEquals(0, metrics.count(MetricNames.SAGA_FAULTED));
    assertEquals(7, metrics.total(MetricNames.INBOX_SWEPT_ROWS));
    assertEquals(2, metrics.samples(MetricNames.INBOX_SWEPT_ROWS).size());
    assertEquals(5, metrics.samples().size());
  }

  @Test
  void anOverloadThatCarriesATag_recordsIt() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordCommandSucceeded("PlaceOrder");

    assertEquals(
        1,
        metrics.count(MetricNames.COMMANDS_SUCCEEDED, MetricNames.TAG_COMMAND_TYPE, "PlaceOrder"));
  }

  @Test
  void aNullName_isRecordedAsANullTagValue() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordProjectionProcessed((ProjectionName) null);
    metrics.recordSubscriptionEventReceived((SubscriptionName) null);
    metrics.recordProjectionDeliveryMode(null, null);

    assertEquals(
        1, metrics.count(MetricNames.PROJECTIONS_PROCESSED, MetricNames.TAG_PROJECTION_NAME, null));
    assertEquals(
        1,
        metrics.count(
            MetricNames.SUBSCRIPTIONS_EVENTS_RECEIVED, MetricNames.TAG_SUBSCRIPTION_NAME, null));
    assertEquals(
        1,
        metrics.count(MetricNames.PROJECTIONS_DELIVERY_MODE, MetricNames.TAG_DELIVERY_MODE, null));
  }

  @Test
  void lastGauge_readsARegisteredSupplierNow_elseTheLastValueSet() {
    var metrics = new RecordingStreamRuneMetrics();
    var inFlight = new AtomicLong(2);
    metrics.registerInFlightCommandsGauge(inFlight::get);
    metrics.recordOutboxBacklog(5);
    metrics.recordOutboxBacklog(3);
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 9);

    inFlight.set(4);

    assertEquals(OptionalLong.of(4), metrics.lastGauge(MetricNames.COMMANDS_IN_FLIGHT));
    assertEquals(OptionalLong.of(3), metrics.lastGauge(MetricNames.OUTBOX_PENDING));
    assertEquals(
        OptionalLong.of(9),
        metrics.lastGauge(
            MetricNames.SUBSCRIPTIONS_LAG, MetricNames.TAG_SUBSCRIPTION_NAME, "orders"));
    assertEquals(
        OptionalLong.empty(),
        metrics.lastGauge(
            MetricNames.SUBSCRIPTIONS_LAG, MetricNames.TAG_SUBSCRIPTION_NAME, "payments"));
    assertEquals(OptionalLong.empty(), metrics.lastGauge(MetricNames.DLQ_PENDING));
  }

  @Test
  void lastGauge_prefersTheSupplier_ofTheSameTags() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordSubscriptionLag(SubscriptionName.of("orders"), 9);
    metrics.registerSubscriptionLagGauge(SubscriptionName.of("orders"), () -> 1);

    assertEquals(
        OptionalLong.of(1),
        metrics.lastGauge(
            MetricNames.SUBSCRIPTIONS_LAG, MetricNames.TAG_SUBSCRIPTION_NAME, "orders"));
  }

  @Test
  void assertIncremented_passesForARecordedMeter_andFailsListingWhatWasRecorded() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordSagaFaulted("order");
    metrics.recordCommandDispatched("PlaceOrder");

    metrics.assertIncremented(MetricNames.SAGA_FAULTED);
    metrics.assertIncremented(MetricNames.SAGA_FAULTED, MetricNames.TAG_SAGA_TYPE, "order");
    var failure =
        assertThrows(
            AssertionError.class,
            () ->
                metrics.assertIncremented(
                    MetricNames.SAGA_FAULTED, MetricNames.TAG_SAGA_TYPE, "payment"));

    assertTrue(failure.getMessage().contains(MetricNames.SAGA_FAULTED), failure.getMessage());
    assertTrue(
        failure.getMessage().contains(MetricNames.COMMANDS_DISPATCHED), failure.getMessage());
    assertTrue(failure.getMessage().contains("PlaceOrder"), failure.getMessage());
  }

  @Test
  void assertIncremented_onARecorderThatRecordedNothing_saysSo() {
    var failure =
        assertThrows(
            AssertionError.class,
            () -> new RecordingStreamRuneMetrics().assertIncremented(MetricNames.SAGA_FAULTED));

    assertTrue(failure.getMessage().endsWith("recorded: nothing"), failure.getMessage());
  }

  @Test
  void aMisspelledMeterOrTagKey_andAnIncompletePair_areRefused() {
    var metrics = new RecordingStreamRuneMetrics();

    assertThrows(IllegalArgumentException.class, () -> metrics.count("streamrune.saga.faulteds"));
    assertThrows(IllegalArgumentException.class, () -> metrics.samples("saga.faulted"));
    assertThrows(
        IllegalArgumentException.class, () -> metrics.lastGauge("streamrune.outbox.pendings"));
    assertThrows(
        IllegalArgumentException.class,
        () -> metrics.count(MetricNames.SAGA_FAULTED, "sagaType", "order"));
    assertThrows(
        IllegalArgumentException.class,
        () -> metrics.count(MetricNames.SAGA_FAULTED, MetricNames.TAG_SAGA_TYPE));
    assertThrows(
        IllegalArgumentException.class, () -> metrics.assertIncremented("streamrune.nothing"));
  }

  @Test
  void reset_forgetsTheSamplesAndTheGauges() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordSagaFaulted("order");
    metrics.registerInFlightCommandsGauge(() -> 1);

    metrics.reset();

    assertTrue(metrics.samples().isEmpty());
    assertEquals(OptionalLong.empty(), metrics.lastGauge(MetricNames.COMMANDS_IN_FLIGHT));
  }

  @Test
  void toString_listsTheSamplesAndTheSuppliedGauges() {
    var metrics = new RecordingStreamRuneMetrics();
    metrics.recordSagaFaulted("order");
    metrics.recordOutboxBacklog(2);
    metrics.registerInFlightCommandsGauge(() -> 1);

    String text = metrics.toString();

    assertTrue(text.contains("streamrune.saga.faulted{saga.type=order}=1"), text);
    assertTrue(text.contains("streamrune.outbox.pending=2"), text);
    assertTrue(text.contains("streamrune.commands.in_flight=<supplied>"), text);
  }

  @Test
  void callsFromManyThreads_areAllRecorded() throws Exception {
    var metrics = new RecordingStreamRuneMetrics();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        futures.add(
            executor.submit(
                () -> {
                  for (int i = 0; i < 250; i++) {
                    metrics.recordEventAppended("OrderPlaced");
                  }
                }));
      }
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    }
    assertEquals(2000, metrics.count(MetricNames.EVENTS_APPENDED));
    assertEquals(new HashSet<>(List.of("OrderPlaced")), tagValues(metrics));
  }

  private static Set<String> tagValues(RecordingStreamRuneMetrics metrics) {
    Set<String> values = new HashSet<>();
    metrics.samples().forEach(sample -> values.addAll(sample.tags().values()));
    return values;
  }
}

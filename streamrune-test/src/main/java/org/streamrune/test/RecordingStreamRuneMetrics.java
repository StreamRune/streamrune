package org.streamrune.test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * A {@link StreamRuneMetrics} that records every call it receives, under the meter name of {@link
 * MetricNames} the call reports and the tags it carries, so a test asserts which meters the code
 * under test reports instead of writing a counting double of its own:
 *
 * <pre>{@code
 * var metrics = new RecordingStreamRuneMetrics();
 * // ... build the runner with .metrics(metrics) and drive it ...
 * metrics.assertIncremented(MetricNames.SAGA_COMPENSATION,
 *     MetricNames.TAG_SAGA_TYPE, sagaType, MetricNames.TAG_OUTCOME, "success");
 * assertEquals(3, metrics.total(MetricNames.INBOX_SWEPT_ROWS));
 * assertEquals(OptionalLong.of(0), metrics.lastGauge(MetricNames.OUTBOX_PENDING));
 * }</pre>
 *
 * <p>Every method of the interface is overridden: a call is never lost to a default method, and an
 * overload that carries a tag records it ({@code recordCommandSucceeded(commandType)} records
 * {@code streamrune.commands.succeeded} with {@code command.type}). Each call is one {@link
 * Sample}: a counter's increment (1, or the rows a sweep removed), a timer's nanoseconds, or the
 * value a gauge was set to. A gauge registered with a supplier ({@code
 * registerInFlightCommandsGauge}, {@code registerSubscriptionLagGauge}) is kept as that supplier
 * and read when {@link #lastGauge} asks for it, as a metrics scrape reads it; it is no sample.
 *
 * <p>The queries take a meter name and tag pairs ({@code key, value, key, value, …}), and refuse a
 * meter name or a tag key that is not a constant of {@link MetricNames} with {@link
 * IllegalArgumentException}, so a misspelled name fails the test instead of matching nothing. A
 * sample matches when it carries every given pair; it may carry more tags.
 *
 * <p>Thread-safe: the runners under test report from their own threads.
 */
public class RecordingStreamRuneMetrics implements StreamRuneMetrics {

  /**
   * One recorded call.
   *
   * @param meterName the meter, a constant of {@link MetricNames}
   * @param tags the tags the call carries, by {@link MetricNames} tag key, in the order of the
   *     call's parameters; a value is {@code null} when the call passed none
   * @param value the counter's increment, the timer's nanoseconds or the gauge's value
   */
  public record Sample(String meterName, Map<String, String> tags, long value) {

    /** Copies the tags, keeping their order. */
    public Sample {
      Objects.requireNonNull(meterName, "meterName is required");
      tags = Collections.unmodifiableMap(new LinkedHashMap<>(tags));
    }

    private boolean matches(String meter, Map<String, String> wanted) {
      if (!meterName.equals(meter)) {
        return false;
      }
      for (Map.Entry<String, String> tag : wanted.entrySet()) {
        if (!tags.containsKey(tag.getKey())
            || !Objects.equals(tags.get(tag.getKey()), tag.getValue())) {
          return false;
        }
      }
      return true;
    }

    private String describe() {
      return tags.isEmpty() ? meterName : meterName + tags;
    }
  }

  /** A gauge registered with a supplier: its meter, its tags and the supplier a scrape reads. */
  private record SuppliedGauge(String meterName, Map<String, String> tags, LongSupplier supplier) {}

  private static final Set<String> METERS = constants(true);
  private static final Set<String> TAG_KEYS = constants(false);

  private final List<Sample> samples = new CopyOnWriteArrayList<>();
  private final List<SuppliedGauge> suppliedGauges = new CopyOnWriteArrayList<>();

  /** Creates a recorder with nothing recorded. */
  public RecordingStreamRuneMetrics() {}

  // ==================== Queries ====================

  /**
   * Every call recorded so far, in the order the calls were made; supplier-registered gauges are
   * not among them.
   *
   * @return an immutable copy
   */
  public List<Sample> samples() {
    return List.copyOf(samples);
  }

  /**
   * The calls recorded for one meter, in the order they were made.
   *
   * @param meterName a meter constant of {@link MetricNames}
   * @return an immutable copy
   * @throws IllegalArgumentException if {@code meterName} is not a meter of {@link MetricNames}
   */
  public List<Sample> samples(String meterName) {
    requireMeter(meterName);
    return samples.stream().filter(sample -> sample.meterName().equals(meterName)).toList();
  }

  /**
   * How many calls recorded the meter with the given tags.
   *
   * @param meterName a meter constant of {@link MetricNames}
   * @param tags tag pairs: a key of {@link MetricNames}, then the value
   * @return the number of matching samples
   * @throws IllegalArgumentException if a name or a key is not a constant of {@link MetricNames},
   *     or the pairs are incomplete
   */
  public long count(String meterName, String... tags) {
    return matching(meterName, tags).size();
  }

  /**
   * The sum of the values the meter was recorded with under the given tags: the rows a sweeper
   * removed, the nanoseconds a timer measured.
   *
   * @param meterName a meter constant of {@link MetricNames}
   * @param tags tag pairs: a key of {@link MetricNames}, then the value
   * @return the sum of the matching samples' values
   * @throws IllegalArgumentException if a name or a key is not a constant of {@link MetricNames},
   *     or the pairs are incomplete
   */
  public long total(String meterName, String... tags) {
    return matching(meterName, tags).stream().mapToLong(Sample::value).sum();
  }

  /**
   * The current value of a gauge under the given tags: the supplier registered for it, read now, or
   * else the value the last matching call set.
   *
   * @param meterName a meter constant of {@link MetricNames}
   * @param tags tag pairs: a key of {@link MetricNames}, then the value
   * @return the value, or empty when nothing recorded the gauge
   * @throws IllegalArgumentException if a name or a key is not a constant of {@link MetricNames},
   *     or the pairs are incomplete
   */
  public OptionalLong lastGauge(String meterName, String... tags) {
    Map<String, String> wanted = tags(meterName, tags);
    for (int i = suppliedGauges.size() - 1; i >= 0; i--) {
      SuppliedGauge gauge = suppliedGauges.get(i);
      if (new Sample(gauge.meterName(), gauge.tags(), 0).matches(meterName, wanted)) {
        return OptionalLong.of(gauge.supplier().getAsLong());
      }
    }
    List<Sample> matching = matching(meterName, tags);
    return matching.isEmpty() ? OptionalLong.empty() : OptionalLong.of(matching.getLast().value());
  }

  /**
   * Fails unless at least one call recorded the meter with the given tags; the failure lists what
   * was recorded.
   *
   * @param meterName a meter constant of {@link MetricNames}
   * @param tags tag pairs: a key of {@link MetricNames}, then the value
   * @throws AssertionError if no call recorded the meter with those tags
   * @throws IllegalArgumentException if a name or a key is not a constant of {@link MetricNames},
   *     or the pairs are incomplete
   */
  public void assertIncremented(String meterName, String... tags) {
    if (!matching(meterName, tags).isEmpty()) {
      return;
    }
    Set<String> recorded =
        samples.stream().map(Sample::describe).collect(Collectors.toCollection(TreeSet::new));
    throw new AssertionError(
        "no call recorded "
            + meterName
            + (tags.length == 0 ? "" : " with " + tags(meterName, tags))
            + "; recorded: "
            + (recorded.isEmpty() ? "nothing" : recorded));
  }

  /** Forgets every sample and every registered gauge. */
  public void reset() {
    samples.clear();
    suppliedGauges.clear();
  }

  private List<Sample> matching(String meterName, String... tags) {
    Map<String, String> wanted = tags(meterName, tags);
    return samples.stream().filter(sample -> sample.matches(meterName, wanted)).toList();
  }

  private static Map<String, String> tags(String meterName, String... tags) {
    requireMeter(meterName);
    if (tags.length % 2 != 0) {
      throw new IllegalArgumentException(
          "tags come in pairs (key, value): " + Arrays.toString(tags));
    }
    Map<String, String> wanted = new LinkedHashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      if (!TAG_KEYS.contains(tags[i])) {
        throw new IllegalArgumentException(
            tags[i] + " is not a tag key of MetricNames; the tag keys are " + TAG_KEYS);
      }
      wanted.put(tags[i], tags[i + 1]);
    }
    return wanted;
  }

  private static void requireMeter(String meterName) {
    if (!METERS.contains(meterName)) {
      throw new IllegalArgumentException(
          meterName + " is not a meter of MetricNames; use one of its constants");
    }
  }

  /** The values of the {@code String} constants of {@link MetricNames}: meters or tag keys. */
  private static Set<String> constants(boolean meters) {
    Set<String> values = new LinkedHashSet<>();
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          String value = (String) field.get(null);
          if (value.startsWith("streamrune.") == meters) {
            values.add(value);
          }
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return Collections.unmodifiableSet(values);
  }

  // ==================== Recording ====================

  private void record(String meterName, long value, String... tags) {
    Map<String, String> recorded = new LinkedHashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      recorded.put(tags[i], tags[i + 1]);
    }
    samples.add(new Sample(meterName, recorded, value));
  }

  private void supply(String meterName, LongSupplier supplier, String... tags) {
    Map<String, String> recorded = new LinkedHashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      recorded.put(tags[i], tags[i + 1]);
    }
    suppliedGauges.add(
        new SuppliedGauge(
            meterName,
            Collections.unmodifiableMap(recorded),
            Objects.requireNonNull(supplier, "supplier is required")));
  }

  private static String name(ProjectionName projectionName) {
    return projectionName == null ? null : projectionName.value();
  }

  private static String name(SubscriptionName subscriptionName) {
    return subscriptionName == null ? null : subscriptionName.value();
  }

  // ==================== Commands ====================

  @Override
  public void recordCommandDispatched() {
    record(MetricNames.COMMANDS_DISPATCHED, 1);
  }

  @Override
  public void recordCommandDispatched(String commandType) {
    record(MetricNames.COMMANDS_DISPATCHED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandSucceeded() {
    record(MetricNames.COMMANDS_SUCCEEDED, 1);
  }

  @Override
  public void recordCommandSucceeded(String commandType) {
    record(MetricNames.COMMANDS_SUCCEEDED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandFailed() {
    record(MetricNames.COMMANDS_FAILED, 1);
  }

  @Override
  public void recordCommandFailed(String commandType) {
    record(MetricNames.COMMANDS_FAILED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandDuration(long durationNanos) {
    record(MetricNames.COMMANDS_DURATION, durationNanos);
  }

  @Override
  public void recordCommandDuration(String commandType, long durationNanos) {
    record(MetricNames.COMMANDS_DURATION, durationNanos, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandRetried(String commandType) {
    record(MetricNames.COMMANDS_RETRIED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandShortCircuited(String commandType) {
    record(MetricNames.COMMANDS_SHORT_CIRCUITED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordCommandAsyncRejected() {
    record(MetricNames.COMMANDS_ASYNC_REJECTED, 1);
  }

  @Override
  public void registerInFlightCommandsGauge(LongSupplier inFlightSupplier) {
    supply(MetricNames.COMMANDS_IN_FLIGHT, inFlightSupplier);
  }

  @Override
  public void recordDeadLetterPublished(String commandType) {
    record(MetricNames.DLQ_PUBLISHED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordDeadLetterExhausted(String commandType) {
    record(MetricNames.DLQ_EXHAUSTED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordDeadLetterStaleReplayBlocked(String commandType) {
    record(MetricNames.DLQ_STALE_REPLAY_BLOCKED, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordLockWait(long durationNanos) {
    record(MetricNames.LOCKS_WAIT, durationNanos);
  }

  // ==================== Event store ====================

  @Override
  public void recordEventAppended() {
    record(MetricNames.EVENTS_APPENDED, 1);
  }

  @Override
  public void recordEventAppended(String eventType) {
    record(MetricNames.EVENTS_APPENDED, 1, MetricNames.TAG_EVENT_TYPE, eventType);
  }

  @Override
  public void recordEventReplayed() {
    record(MetricNames.EVENTS_REPLAYED, 1);
  }

  @Override
  public void recordEventAppendDuration(long durationNanos) {
    record(MetricNames.EVENTS_DURATION, durationNanos);
  }

  @Override
  public void recordSnapshotCreated() {
    record(MetricNames.SNAPSHOTS_CREATED, 1);
  }

  @Override
  public void recordSnapshotLoaded() {
    record(MetricNames.SNAPSHOTS_LOADED, 1);
  }

  @Override
  public void recordSnapshotRefused() {
    record(MetricNames.SNAPSHOTS_REFUSED, 1);
  }

  @Override
  public void recordSnapshotDiscarded(String reason) {
    record(MetricNames.SNAPSHOTS_DISCARDED, 1, MetricNames.TAG_REASON, reason);
  }

  // ==================== Crypto and GDPR ====================

  @Override
  public void recordSubjectRedacted() {
    record(MetricNames.CRYPTO_SUBJECT_REDACTED, 1);
  }

  @Override
  public void recordSystemicKeyStoreFailure() {
    record(MetricNames.CRYPTO_KEYSTORE_SYSTEMIC_FAILURE, 1);
  }

  @Override
  public void recordGdprPurgeFailed(String purgerName) {
    record(MetricNames.GDPR_PURGE_FAILED, 1, MetricNames.TAG_PURGER, purgerName);
  }

  // ==================== Projections ====================

  @Override
  public void recordProjectionProcessed() {
    record(MetricNames.PROJECTIONS_PROCESSED, 1);
  }

  @Override
  public void recordProjectionProcessed(ProjectionName projectionName) {
    record(
        MetricNames.PROJECTIONS_PROCESSED,
        1,
        MetricNames.TAG_PROJECTION_NAME,
        name(projectionName));
  }

  @Override
  public void recordProjectionFailed() {
    record(MetricNames.PROJECTIONS_FAILED, 1);
  }

  @Override
  public void recordProjectionFailed(ProjectionName projectionName) {
    record(
        MetricNames.PROJECTIONS_FAILED, 1, MetricNames.TAG_PROJECTION_NAME, name(projectionName));
  }

  @Override
  public void recordProjectionDeadLettered() {
    record(MetricNames.PROJECTIONS_DEAD_LETTERED, 1);
  }

  @Override
  public void recordProjectionDeadLettered(ProjectionName projectionName) {
    record(
        MetricNames.PROJECTIONS_DEAD_LETTERED,
        1,
        MetricNames.TAG_PROJECTION_NAME,
        name(projectionName));
  }

  @Override
  public void recordProjectionDuration(long durationNanos) {
    record(MetricNames.PROJECTIONS_DURATION, durationNanos);
  }

  @Override
  public void recordProjectionDuration(ProjectionName projectionName, long durationNanos) {
    record(
        MetricNames.PROJECTIONS_DURATION,
        durationNanos,
        MetricNames.TAG_PROJECTION_NAME,
        name(projectionName));
  }

  @Override
  public void recordProjectionDeadLetterBacklog(long count) {
    record(MetricNames.PROJECTIONS_DEAD_LETTER_BACKLOG, count);
  }

  @Override
  public void recordProjectionDeliveryMode(
      ProjectionName projectionName, ProjectionDeliveryMode mode) {
    record(
        MetricNames.PROJECTIONS_DELIVERY_MODE,
        1,
        MetricNames.TAG_PROJECTION_NAME,
        name(projectionName),
        MetricNames.TAG_DELIVERY_MODE,
        mode == null ? null : mode.name());
  }

  // ==================== Subscriptions ====================

  @Override
  public void recordSubscriptionEventReceived() {
    record(MetricNames.SUBSCRIPTIONS_EVENTS_RECEIVED, 1);
  }

  @Override
  public void recordSubscriptionEventReceived(SubscriptionName subscriptionName) {
    record(
        MetricNames.SUBSCRIPTIONS_EVENTS_RECEIVED,
        1,
        MetricNames.TAG_SUBSCRIPTION_NAME,
        name(subscriptionName));
  }

  @Override
  public void recordSubscriptionDeliveryLatency(long durationNanos) {
    record(MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY, durationNanos);
  }

  @Override
  public void recordSubscriptionDeliveryLatency(
      SubscriptionName subscriptionName, long durationNanos) {
    record(
        MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY,
        durationNanos,
        MetricNames.TAG_SUBSCRIPTION_NAME,
        name(subscriptionName));
  }

  @Override
  public void recordSubscriptionLag(SubscriptionName subscriptionName, long lag) {
    record(
        MetricNames.SUBSCRIPTIONS_LAG,
        lag,
        MetricNames.TAG_SUBSCRIPTION_NAME,
        name(subscriptionName));
  }

  @Override
  public void registerSubscriptionLagGauge(
      SubscriptionName subscriptionName, LongSupplier lagSupplier) {
    supply(
        MetricNames.SUBSCRIPTIONS_LAG,
        lagSupplier,
        MetricNames.TAG_SUBSCRIPTION_NAME,
        name(subscriptionName));
  }

  @Override
  public void recordListenerReconnect(String channel) {
    record(MetricNames.SUBSCRIPTIONS_LISTENER_RECONNECTS, 1, MetricNames.TAG_CHANNEL, channel);
  }

  // ==================== Queries ====================

  @Override
  public void recordQueryDispatched(String queryType) {
    record(MetricNames.QUERIES_DISPATCHED, 1, MetricNames.TAG_QUERY_TYPE, queryType);
  }

  @Override
  public void recordQueryDuration(String queryType, long durationNanos) {
    record(MetricNames.QUERIES_DURATION, durationNanos, MetricNames.TAG_QUERY_TYPE, queryType);
  }

  @Override
  public void recordQueryCacheHit(String queryType) {
    record(MetricNames.QUERIES_CACHE_HITS, 1, MetricNames.TAG_QUERY_TYPE, queryType);
  }

  @Override
  public void recordQueryCacheMiss(String queryType) {
    record(MetricNames.QUERIES_CACHE_MISSES, 1, MetricNames.TAG_QUERY_TYPE, queryType);
  }

  // ==================== Sagas ====================

  @Override
  public void recordSagaQuarantined(String sagaType) {
    record(MetricNames.SAGA_QUARANTINED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaFaulted(String sagaType) {
    record(MetricNames.SAGA_FAULTED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaSkippedWhileFaulted(String sagaType) {
    record(MetricNames.SAGA_SKIPPED_WHILE_FAULTED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaEventHeld(String sagaType, String reason) {
    record(
        MetricNames.SAGA_EVENT_HELD,
        1,
        MetricNames.TAG_SAGA_TYPE,
        sagaType,
        MetricNames.TAG_HOLD_REASON,
        reason);
  }

  @Override
  public void recordSagaReplayDeferred(String sagaType) {
    record(MetricNames.SAGA_REPLAY_DEFERRED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaCompensation(String sagaType, String outcome) {
    record(
        MetricNames.SAGA_COMPENSATION,
        1,
        MetricNames.TAG_SAGA_TYPE,
        sagaType,
        MetricNames.TAG_OUTCOME,
        outcome);
  }

  @Override
  public void recordSagaCompensationRetry(String sagaType) {
    record(MetricNames.SAGA_COMPENSATION_RETRIES, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaCasConflict(String sagaType) {
    record(MetricNames.SAGA_CAS_CONFLICTS, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaCompensationUnderForwardStep(String sagaType) {
    record(
        MetricNames.SAGA_COMPENSATION_UNDER_FORWARD_STEP, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaForwardCommandFenced(String sagaType) {
    record(MetricNames.SAGA_FORWARD_COMMANDS_FENCED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaDeliveryRetried(String sagaType) {
    record(MetricNames.SAGA_DELIVERY_RETRIED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaCompensatingBacklog(String sagaType, long count) {
    record(MetricNames.SAGA_COMPENSATING, count, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaTimedOutBacklog(String sagaType, long count) {
    record(MetricNames.SAGA_TIMED_OUT, count, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaTimeoutClaimDeferred(String sagaType) {
    record(MetricNames.SAGA_TIMEOUT_CLAIMS_DEFERRED, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaFaultedBacklog(long count) {
    record(MetricNames.SAGA_FAULTED_BACKLOG, count);
  }

  @Override
  public void recordSagaFaultedRows(String sagaType, long count) {
    record(MetricNames.SAGA_FAULTED_ROWS, count, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaReplayed(String sagaType, String outcome) {
    record(
        MetricNames.SAGA_REPLAYED,
        1,
        MetricNames.TAG_SAGA_TYPE,
        sagaType,
        MetricNames.TAG_OUTCOME,
        outcome);
  }

  @Override
  public void recordSagaForcedStaleResume(String sagaType) {
    record(MetricNames.SAGA_FORCED_STALE_RESUME, 1, MetricNames.TAG_SAGA_TYPE, sagaType);
  }

  @Override
  public void recordSagaResumeFaulted(String sagaType, String outcome) {
    record(
        MetricNames.SAGA_RESUME_FAULTED,
        1,
        MetricNames.TAG_SAGA_TYPE,
        sagaType,
        MetricNames.TAG_OUTCOME,
        outcome);
  }

  @Override
  public void recordSagaCompensateFaulted(String sagaType, String outcome) {
    record(
        MetricNames.SAGA_COMPENSATE_FAULTED,
        1,
        MetricNames.TAG_SAGA_TYPE,
        sagaType,
        MetricNames.TAG_OUTCOME,
        outcome);
  }

  // ==================== Inbox, outbox and dead letters ====================

  @Override
  public void recordInboxReplayHit(String commandType) {
    record(MetricNames.INBOX_REPLAY_HITS, 1, MetricNames.TAG_COMMAND_TYPE, commandType);
  }

  @Override
  public void recordInboxSwept(long rows) {
    record(MetricNames.INBOX_SWEPT_ROWS, rows);
  }

  @Override
  public void recordSagaDeadLetterSwept(long rows) {
    record(MetricNames.SAGA_DEAD_LETTERS_SWEPT, rows);
  }

  @Override
  public void recordOutboxSwept(long rows) {
    record(MetricNames.OUTBOX_SWEPT_ROWS, rows);
  }

  @Override
  public void recordOutboxDeliveryFailed() {
    record(MetricNames.OUTBOX_DELIVERY_FAILED, 1);
  }

  @Override
  public void recordOutboxBacklog(long pending) {
    record(MetricNames.OUTBOX_PENDING, pending);
  }

  @Override
  public void recordOutboxInFlight(long inFlight) {
    record(MetricNames.OUTBOX_IN_FLIGHT, inFlight);
  }

  @Override
  public void recordOutboxInFlightHorizonViolation() {
    record(MetricNames.OUTBOX_IN_FLIGHT_HORIZON_VIOLATION, 1);
  }

  @Override
  public void recordOutboxReplayed(long rows) {
    record(MetricNames.OUTBOX_REPLAYED, rows);
  }

  @Override
  public void recordOutboxBlockedAggregates(long failedAggregates) {
    record(MetricNames.OUTBOX_BLOCKED_AGGREGATES, failedAggregates);
  }

  @Override
  public void recordOutboxBlockageAge(long seconds) {
    record(MetricNames.OUTBOX_BLOCKAGE_AGE_SECONDS, seconds);
  }

  @Override
  public void recordOutboxSkipped() {
    record(MetricNames.OUTBOX_SKIPPED, 1);
  }

  @Override
  public void recordOutboxSkippedSwept(long rows) {
    record(MetricNames.OUTBOX_SKIPPED_SWEPT, rows);
  }

  @Override
  public void recordDeadLetterSwept(long rows) {
    record(MetricNames.DLQ_SWEPT_ROWS, rows);
  }

  @Override
  public void recordDlqBacklog(long pending) {
    record(MetricNames.DLQ_PENDING, pending);
  }

  @Override
  public void recordDlqAwaitingOperator(long awaiting) {
    record(MetricNames.DLQ_AWAITING_OPERATOR, awaiting);
  }

  @Override
  public void recordOutboxRelayDegradation(long consecutiveFailures) {
    record(MetricNames.OUTBOX_RELAY_DEGRADED, consecutiveFailures);
  }

  @Override
  public void recordDlqRetryDegradation(long consecutiveFailures) {
    record(MetricNames.DLQ_RETRY_DEGRADED, consecutiveFailures);
  }

  /** Lists every sample, for a failure message of the test's own. */
  @Override
  public String toString() {
    List<String> lines = new ArrayList<>();
    samples.forEach(sample -> lines.add(sample.describe() + "=" + sample.value()));
    suppliedGauges.forEach(
        gauge ->
            lines.add(
                (gauge.tags().isEmpty() ? gauge.meterName() : gauge.meterName() + gauge.tags())
                    + "=<supplied>"));
    return "RecordingStreamRuneMetrics" + lines;
  }
}

package org.streamrune.runtime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * Test double that counts every {@link StreamRuneMetrics} call (untagged and tagged), so tests can
 * assert which meters fired. Counts are keyed by a synthetic meter name; tagged calls also bump a
 * per-tag-value counter so per-projection / per-subscription / per-query breakdowns are verifiable.
 */
final class RecordingStreamRuneMetrics implements StreamRuneMetrics {

  private final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>();

  /** Latest lag value reported per subscription name (gauge semantics, last write wins). */
  private final ConcurrentHashMap<String, Long> subscriptionLag = new ConcurrentHashMap<>();

  /** Live lag-gauge suppliers registered per subscription name. */
  private final ConcurrentHashMap<String, LongSupplier> lagSuppliers = new ConcurrentHashMap<>();

  /** Latest DLQ-depth / relay / DLQ-retry degradation gauge values reported. */
  private volatile long lastDlqBacklog = -1;

  private volatile long lastDlqRetryDegradation = -1;

  /** Latest projection dead-letter backlog gauge value reported. */
  private volatile long lastProjectionDeadLetterBacklog = -1;

  private void inc(String meter) {
    counts.computeIfAbsent(meter, k -> new AtomicLong()).incrementAndGet();
  }

  /** Total invocations of the given meter (any tag value). */
  long count(String meter) {
    var c = counts.get(meter);
    return c == null ? 0 : c.get();
  }

  /** Invocations of the meter tagged with the given value (e.g. a projection name). */
  long count(String meter, String tag) {
    return count(meter + ":" + tag);
  }

  /** Latest lag reported for the given subscription, or {@code null} if none was reported. */
  Long lag(String subscriptionName) {
    return subscriptionLag.get(subscriptionName);
  }

  // --- Command ---

  @Override
  public void recordCommandRetried(String commandType) {
    inc("command.retried");
    inc("command.retried:" + commandType);
  }

  @Override
  public void recordCommandShortCircuited(String commandType) {
    inc("command.shortCircuited");
    inc("command.shortCircuited:" + commandType);
  }

  @Override
  public void recordDeadLetterPublished(String commandType) {
    inc("command.dlqPublished");
    inc("command.dlqPublished:" + commandType);
  }

  @Override
  public void recordDeadLetterExhausted(String commandType) {
    inc("command.dlqExhausted");
    inc("command.dlqExhausted:" + commandType);
  }

  @Override
  public void recordLockWait(long durationNanos) {
    inc("command.lockWait");
  }

  // --- Event store ---

  @Override
  public void recordEventAppended() {
    inc("event.appended");
  }

  @Override
  public void recordEventReplayed() {
    inc("event.replayed");
  }

  @Override
  public void recordEventAppendDuration(long durationNanos) {
    inc("event.appendDuration");
  }

  @Override
  public void recordSnapshotCreated() {
    inc("snapshot.created");
  }

  @Override
  public void recordSnapshotLoaded() {
    inc("snapshot.loaded");
  }

  // --- Projections ---

  @Override
  public void recordProjectionProcessed(ProjectionName projectionName) {
    inc("projection.processed");
    inc("projection.processed:" + projectionName.value());
  }

  @Override
  public void recordProjectionFailed(ProjectionName projectionName) {
    inc("projection.failed");
    inc("projection.failed:" + projectionName.value());
  }

  @Override
  public void recordProjectionDeadLettered(ProjectionName projectionName) {
    inc("projection.deadLettered");
    inc("projection.deadLettered:" + projectionName.value());
  }

  @Override
  public void recordProjectionDuration(ProjectionName projectionName, long durationNanos) {
    inc("projection.duration");
    inc("projection.duration:" + projectionName.value());
  }

  /** Latest delivery mode recorded per projection name (gauge semantics, last write wins). */
  private final ConcurrentHashMap<String, ProjectionDeliveryMode> deliveryModes =
      new ConcurrentHashMap<>();

  @Override
  public void recordProjectionDeliveryMode(
      ProjectionName projectionName, ProjectionDeliveryMode mode) {
    inc("projection.delivery_mode");
    inc("projection.delivery_mode:" + projectionName.value());
    deliveryModes.put(projectionName.value(), mode);
  }

  /** The delivery mode last recorded for the projection, or {@code null} if none was recorded. */
  ProjectionDeliveryMode deliveryModeOf(String projectionName) {
    return deliveryModes.get(projectionName);
  }

  // --- Subscriptions ---

  @Override
  public void recordSubscriptionEventReceived(SubscriptionName subscriptionName) {
    inc("subscription.received");
    inc("subscription.received:" + subscriptionName.value());
  }

  @Override
  public void recordSubscriptionDeliveryLatency(
      SubscriptionName subscriptionName, long durationNanos) {
    inc("subscription.latency");
    inc("subscription.latency:" + subscriptionName.value());
  }

  @Override
  public void recordSubscriptionLag(SubscriptionName subscriptionName, long lag) {
    inc("subscription.lag");
    inc("subscription.lag:" + subscriptionName.value());
    subscriptionLag.put(subscriptionName.value(), lag);
  }

  @Override
  public void registerSubscriptionLagGauge(
      SubscriptionName subscriptionName, LongSupplier lagSupplier) {
    inc("subscription.lagGauge.registered");
    inc("subscription.lagGauge.registered:" + subscriptionName.value());
    lagSuppliers.put(subscriptionName.value(), lagSupplier);
  }

  /**
   * Invokes the live lag-gauge supplier registered for {@code subscriptionName} — simulates a
   * metrics-endpoint scrape reading the gauge, independent of any health scrape.
   */
  long sampleLagGauge(String subscriptionName) {
    var supplier = lagSuppliers.get(subscriptionName);
    if (supplier == null) {
      throw new IllegalStateException("no live lag gauge registered for " + subscriptionName);
    }
    return supplier.getAsLong();
  }

  // --- relay/DLQ observability ---

  @Override
  public void recordDlqBacklog(long pending) {
    inc("dlq.backlog");
    lastDlqBacklog = pending;
  }

  @Override
  public void recordOutboxRelayDegradation(long consecutiveFailures) {
    inc("outbox.relay.degradation");
  }

  @Override
  public void recordDlqRetryDegradation(long consecutiveFailures) {
    inc("dlq.retry.degradation");
    lastDlqRetryDegradation = consecutiveFailures;
  }

  /** Latest command-DLQ depth reported, or {@code -1} if none. */
  long lastDlqBacklog() {
    return lastDlqBacklog;
  }

  @Override
  public void recordProjectionDeadLetterBacklog(long count) {
    inc("projection.deadLetterBacklog");
    lastProjectionDeadLetterBacklog = count;
  }

  long lastProjectionDeadLetterBacklog() {
    return lastProjectionDeadLetterBacklog;
  }

  // --- saga backlog gauges ---

  private final ConcurrentHashMap<String, Long> sagaCompensatingBacklog = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> sagaTimedOutBacklog = new ConcurrentHashMap<>();

  @Override
  public void recordSagaCompensatingBacklog(String sagaType, long count) {
    inc("saga.compensating");
    inc("saga.compensating:" + sagaType);
    sagaCompensatingBacklog.put(sagaType, count);
  }

  @Override
  public void recordSagaTimedOutBacklog(String sagaType, long count) {
    inc("saga.timedOut");
    inc("saga.timedOut:" + sagaType);
    sagaTimedOutBacklog.put(sagaType, count);
  }

  /** Latest COMPENSATING backlog reported for the saga type, or {@code null} if none. */
  Long sagaCompensatingBacklog(String sagaType) {
    return sagaCompensatingBacklog.get(sagaType);
  }

  /** Latest timed-out backlog reported for the saga type, or {@code null} if none. */
  Long sagaTimedOutBacklog(String sagaType) {
    return sagaTimedOutBacklog.get(sagaType);
  }

  /** Latest DLQ retry runner degradation reported, or {@code -1} if none. */
  long lastDlqRetryDegradation() {
    return lastDlqRetryDegradation;
  }

  // --- Queries ---

  @Override
  public void recordQueryDispatched(String queryType) {
    inc("query.dispatched");
    inc("query.dispatched:" + queryType);
  }

  @Override
  public void recordQueryDuration(String queryType, long durationNanos) {
    inc("query.duration");
    inc("query.duration:" + queryType);
  }

  @Override
  public void recordQueryCacheHit(String queryType) {
    inc("query.cacheHit");
    inc("query.cacheHit:" + queryType);
  }

  @Override
  public void recordQueryCacheMiss(String queryType) {
    inc("query.cacheMiss");
    inc("query.cacheMiss:" + queryType);
  }
}

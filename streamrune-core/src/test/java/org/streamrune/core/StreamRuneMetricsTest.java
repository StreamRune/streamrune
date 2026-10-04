package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

class StreamRuneMetricsTest {

  @Test
  void noopMethodsDoNotThrow() {
    StreamRuneMetrics m = StreamRuneMetrics.NOOP;
    assertDoesNotThrow(
        () -> {
          m.recordCommandDispatched();
          m.recordCommandSucceeded();
          m.recordCommandFailed();
          m.recordCommandDuration(1000);
          m.recordCommandRetried("CreateOrder");
          m.recordCommandShortCircuited("CreateOrder");
          m.recordDeadLetterPublished("CreateOrder");
          m.recordLockWait(1000);
          m.recordEventAppended();
          m.recordEventReplayed();
          m.recordEventAppendDuration(1000);
          m.recordSnapshotCreated();
          m.recordSnapshotLoaded();
          m.recordProjectionProcessed();
          m.recordProjectionFailed();
          m.recordProjectionDuration(1000);
          m.recordSubscriptionEventReceived();
          m.recordSubscriptionDeliveryLatency(1000);
          m.recordSubscriptionLag(SubscriptionName.of("order-feed"), 42);
          m.recordQueryDispatched("GetOrder");
          m.recordQueryDuration("GetOrder", 1000);
          m.recordQueryCacheHit("GetOrder");
          m.recordQueryCacheMiss("GetOrder");
        });
  }

  /**
   * Implementations that only override the untagged methods must still see calls made through the
   * tagged overloads — the tagged defaults delegate to the untagged variants.
   */
  @Test
  void taggedDefaultsDelegateToUntaggedVariants() {
    var counter = new AtomicInteger();
    StreamRuneMetrics m =
        new StreamRuneMetrics() {
          @Override
          public void recordCommandDispatched() {
            counter.incrementAndGet();
          }

          @Override
          public void recordCommandSucceeded() {
            counter.incrementAndGet();
          }

          @Override
          public void recordCommandFailed() {
            counter.incrementAndGet();
          }

          @Override
          public void recordCommandDuration(long durationNanos) {
            counter.incrementAndGet();
          }

          @Override
          public void recordEventAppended() {
            counter.incrementAndGet();
          }

          @Override
          public void recordProjectionProcessed() {
            counter.incrementAndGet();
          }

          @Override
          public void recordProjectionFailed() {
            counter.incrementAndGet();
          }

          @Override
          public void recordProjectionDuration(long durationNanos) {
            counter.incrementAndGet();
          }

          @Override
          public void recordSubscriptionEventReceived() {
            counter.incrementAndGet();
          }

          @Override
          public void recordSubscriptionDeliveryLatency(long durationNanos) {
            counter.incrementAndGet();
          }
        };

    m.recordCommandDispatched("CreateOrder");
    m.recordCommandSucceeded("CreateOrder");
    m.recordCommandFailed("CreateOrder");
    m.recordCommandDuration("CreateOrder", 1000);
    m.recordEventAppended("OrderCreated");
    m.recordProjectionProcessed(ProjectionName.of("order-summary"));
    m.recordProjectionFailed(ProjectionName.of("order-summary"));
    m.recordProjectionDuration(ProjectionName.of("order-summary"), 1000);
    m.recordSubscriptionEventReceived(SubscriptionName.of("order-feed"));
    m.recordSubscriptionDeliveryLatency(SubscriptionName.of("order-feed"), 1000);

    assertEquals(10, counter.get());
  }

  @Test
  void recordProjectionDeliveryMode_defaultIsANoOp() {
    StreamRuneMetrics metrics = new StreamRuneMetrics() {};
    assertDoesNotThrow(
        () ->
            metrics.recordProjectionDeliveryMode(
                ProjectionName.of("orders"), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    assertEquals("streamrune.projections.delivery_mode", MetricNames.PROJECTIONS_DELIVERY_MODE);
    assertEquals("delivery.mode", MetricNames.TAG_DELIVERY_MODE);
  }
}

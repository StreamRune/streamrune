package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;

/**
 * The projection runner samples the fleet-wide dead-letter backlog gauge ({@code
 * streamrune.projections.dead_letter_backlog}) once per tick, so permanent read-model holes stay
 * visible after a failure burst even though the projection reports itself health-UP; and it
 * degrades gracefully (disables the gauge, does not retry) against a store that cannot count.
 */
class ScheduledProjectionDeadLetterBacklogMetricTest {

  private ScheduledProjectionRunner runner;

  @AfterEach
  void tearDown() {
    if (runner != null) {
      runner.close();
    }
  }

  /** Captures the latest projection dead-letter backlog sample. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final AtomicLong lastBacklog = new AtomicLong(-1);
    final CountDownLatch sampled = new CountDownLatch(1);

    @Override
    public void recordProjectionDeadLetterBacklog(long count) {
      lastBacklog.set(count);
      sampled.countDown();
    }
  }

  private static ProjectionDeadLetterEntry hole(String projection, long offset) {
    return new ProjectionDeadLetterEntry(
        ProjectionName.of(projection),
        GlobalOffset.of(offset),
        GlobalOffset.of(offset),
        1,
        "Boom",
        "boom",
        1,
        Instant.now());
  }

  @Test
  void tickSamplesFleetWideDeadLetterBacklog() throws Exception {
    var dlq = new InMemoryProjectionDeadLetterStore();
    // Two permanent read-model holes the runner has advanced its checkpoint past.
    dlq.save(hole("orders", 5));
    dlq.save(hole("orders", 9));
    var metrics = new RecordingMetrics();

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(SimpleTestEventStore.of(List.of()))
            .offsetStore(new InMemoryOffsetStore())
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .metrics(metrics)
            .register(
                "orders",
                batch -> {},
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    assertThat(metrics.sampled.await(5, TimeUnit.SECONDS))
        .as("the runner must sample the dead-letter backlog gauge on its first tick")
        .isTrue();
    assertThat(metrics.lastBacklog.get())
        .as("the sampled backlog must be the store's total dead-letter count")
        .isEqualTo(2L);
  }

  @Test
  void gaugeIsDisabledAndNotRetried_whenStoreCannotCount() throws Exception {
    var callCount = new AtomicInteger(0);
    // A store that does not support countPending() — its default would throw, but here we count how
    // often it is invoked to prove the runner disables the gauge after the first throw.
    ProjectionDeadLetterStore uncountable =
        new ProjectionDeadLetterStore() {
          @Override
          public void save(ProjectionDeadLetterEntry entry) {}

          @Override
          public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
            return List.of();
          }

          @Override
          public List<ProjectionDeadLetterEntry> readAll(int limit) {
            return List.of();
          }

          @Override
          public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {}

          @Override
          public long countPending() {
            callCount.incrementAndGet();
            throw new UnsupportedOperationException("cannot count");
          }
        };
    var metrics = new RecordingMetrics();

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(SimpleTestEventStore.of(List.of()))
            .offsetStore(new InMemoryOffsetStore())
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(uncountable)
            .metrics(metrics)
            .register(
                "orders",
                batch -> {},
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    // Span several ticks (cron fires every second). The gauge must be sampled ONCE, then disabled.
    Thread.sleep(3_200);

    assertThat(callCount.get())
        .as("countPending() must be called once and then disabled — never retried each cycle")
        .isEqualTo(1);
    assertThat(metrics.lastBacklog.get())
        .as("the backlog gauge must never be recorded for a store that cannot count")
        .isEqualTo(-1L);
  }
}

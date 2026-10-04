package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;

/**
 * Verifies both projection runners emit the dedicated {@code
 * StreamRuneMetrics#recordProjectionDeadLettered} counter exactly once each time they write a
 * projection dead-letter under the DLQ error strategy — the one quarantine path that permanently
 * advances the read model past the failed range.
 */
class ProjectionDeadLetteredMetricTest {

  private static final AggregateType TYPE = AggregateType.of("dlq");

  private record Fail(String payload) implements DomainEvent {}

  @Test
  void continuousRunner_firesDeadLetteredCounterExactlyOncePerDeadLetter() throws Exception {
    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var metrics = new RecordingStreamRuneMetrics();

    StreamId stream = StreamId.of(TYPE, AggregateId.of("dlq-stream"));
    eventStore.append(stream, List.of(poison(stream)), Version.initial());

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("permanent failure"); // POISON
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .metrics(metrics)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .build();

    var name = ProjectionName.of("dlq-proj");
    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      // The poison range is dead-lettered, the runner advances past it and continues; the counter
      // fires exactly once (offset advanced, so no re-drive of the same range).
      awaitDeadLetter(dlqStore, name);
      assertEquals(1, metrics.count("projection.deadLettered"), "dead-letter counter fires once");
      assertEquals(
          1, metrics.count("projection.deadLettered", name.value()), "tagged by projection");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  void scheduledRunner_firesDeadLetteredCounterExactlyOncePerDeadLetter() {
    var store = SimpleTestEventStore.of(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = new InMemoryOffsetStore();
    var dlq = new InMemoryProjectionDeadLetterStore();
    var metrics = new RecordingStreamRuneMetrics();

    Projection p =
        batch -> {
          throw new RuntimeException("boom"); // POISON
        };

    var runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .metrics(metrics)
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      var name = ProjectionName.of("p");
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> offsets.getLastOffset(name).value() == 1L);
      // The offset advanced past the dead-lettered range, so later ticks find no events to re-drive
      // — the counter must be at exactly one, tagged with the projection name.
      assertEquals(1, metrics.count("projection.deadLettered"), "dead-letter counter fires once");
      assertEquals(1, metrics.count("projection.deadLettered", "p"), "tagged by projection");
    } finally {
      runner.close();
    }
  }

  private static void awaitDeadLetter(InMemoryProjectionDeadLetterStore store, ProjectionName name)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (store.read(name, 10).isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
  }

  private static EventEnvelope poison(StreamId stream) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        stream,
        new Version(1),
        new EventType("Fail"),
        new Fail("fail"),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }
}

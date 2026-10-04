package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
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
 * Projection poison-vs-transient error classification under the DLQ strategy. A TRANSIENT failure
 * (infra/SQL/IO) is retried with backoff and only dead-lettered once the bound is exhausted; a
 * POISON failure is dead-lettered after the first attempt. Under DLQ both runners now
 * dead-letter-and-continue (Continuous no longer halts).
 */
class ProjectionErrorClassificationTest {

  private static final AggregateType TYPE = AggregateType.of("transient");

  private ScheduledProjectionRunner scheduled;

  @AfterEach
  void tearDown() {
    if (scheduled != null) {
      scheduled.close();
    }
  }

  record TestEvent(String payload) implements DomainEvent {}

  private static EventMetadata metadata(String correlationId) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of(correlationId),
        null,
        null,
        Instant.now());
  }

  private static EventEnvelope envelope(StreamId streamId, long version, String payload) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent(payload),
        metadata("corr-" + version));
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10));
  }

  // ==================== Continuous runner ====================

  @Test
  void continuous_transientRecovers_noDeadLetter_offsetAdvances() throws Exception {
    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("transient-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "one")), Version.initial());

    var calls = new AtomicInteger(0);
    Projection failsOnceThenOk =
        events -> {
          if (calls.incrementAndGet() == 1) {
            // A read-model DB blip surfaces as an EventStoreException => TRANSIENT.
            throw new EventStoreException(
                "read-model connection reset", new java.sql.SQLException());
          }
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("transient-proj"),
                        failsOnceThenOk,
                        AT_LEAST_ONCE_IDEMPOTENT));

    // The transient failure is retried from the checkpoint; the second attempt succeeds and the
    // offset advances. No dead-letter is written.
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> offsetStore.getLastOffset(ProjectionName.of("transient-proj")).value() == 1L);

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    assertThat(dlqStore.read(ProjectionName.of("transient-proj"), 10)).isEmpty();
    assertThat(calls.get()).isGreaterThanOrEqualTo(2);
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
  }

  @Test
  void continuous_poisonDeadLettersImmediately_noRetryStorm() throws Exception {
    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("poison-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "bad")), Version.initial());

    var calls = new AtomicInteger(0);
    Projection alwaysPoison =
        events -> {
          calls.incrementAndGet();
          throw new RuntimeException("deterministic mapping bug"); // POISON
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("poison-proj"), alwaysPoison, AT_LEAST_ONCE_IDEMPOTENT));

    // Exactly one dead-letter after the FIRST failure — no retry storm.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> dlqStore.read(ProjectionName.of("poison-proj"), 10).size() == 1);

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    var entries = dlqStore.read(ProjectionName.of("poison-proj"), 10);
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).errorType()).isEqualTo("java.lang.RuntimeException");
    // The projection was invoked once (poison => no retry before dead-lettering the range).
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void continuous_poisonDeadLettersAndContinues_pastTheRange() throws Exception {
    // Behavior: under DLQ a POISON chunk is dead-lettered and the runner CONTINUES
    // past it instead of halting. Offsets 1-2 commit, chunk 3-4 is poison (dead-lettered), and
    // chunk 5-6 — which the old halt would never reach — is now processed.
    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("continue-stream"));
    eventStore.append(
        s,
        List.of(
            envelope(s, 1, "ok"),
            envelope(s, 2, "ok"),
            envelope(s, 3, "fail"),
            envelope(s, 4, "fail"),
            envelope(s, 5, "after"),
            envelope(s, 6, "after")),
        Version.initial());

    var processed = new ConcurrentLinkedQueue<String>();
    Projection poisonMiddle =
        events -> {
          for (var e : events) {
            if (((TestEvent) e.event()).payload().equals("fail")) {
              throw new RuntimeException("poison chunk"); // POISON
            }
          }
          for (var e : events) {
            processed.add(((TestEvent) e.event()).payload());
          }
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(2)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("continue-proj"),
                        poisonMiddle,
                        AT_LEAST_ONCE_IDEMPOTENT));

    // The runner must advance past the poison range and process the trailing chunk.
    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> processed.contains("after"));

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    var entries = dlqStore.read(ProjectionName.of("continue-proj"), 10);
    assertThat(entries).hasSize(1);
    assertThat(entries.get(0).fromOffset().value()).isEqualTo(3);
    assertThat(entries.get(0).toOffset().value()).isEqualTo(4);
    // Committed offset advanced past the dead-lettered range to the trailing chunk.
    assertThat(offsetStore.getLastOffset(ProjectionName.of("continue-proj")).value()).isEqualTo(6);
    // The runner is NOT in ERROR (old behavior halted here).
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
    assertThat(processed).containsExactly("ok", "ok", "after", "after");
  }

  @Test
  void continuous_transientBurst_doesNotHalt_staysNonError() throws Exception {
    // A burst of transient failures is retried; the runner must NOT drop into ERROR while retrying.
    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("burst-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "one")), Version.initial());

    var calls = new AtomicInteger(0);
    Projection flaky =
        events -> {
          // Fail transiently 3 times, then succeed.
          if (calls.incrementAndGet() <= 3) {
            throw new EventStoreException("transient blip #" + calls.get());
          }
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("burst-proj"), flaky, AT_LEAST_ONCE_IDEMPOTENT));

    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> offsetStore.getLastOffset(ProjectionName.of("burst-proj")).value() == 1L);

    // While retrying the transient burst the runner never entered ERROR, and it recovered without
    // dead-lettering.
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
    assertThat(dlqStore.read(ProjectionName.of("burst-proj"), 10)).isEmpty();

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  // ==================== Scheduled runner ====================

  @Test
  void scheduled_transientDoesNotAdvanceOrDeadLetter_thenRecovers() {
    // A TRANSIENT failure must NOT dead-letter and must NOT advance the offset — drainWithRetry
    // retries the tick and the projection eventually succeeds.
    var store = SimpleTestEventStore.of(List.of(evt()));
    var offsets = new InMemoryOffsetStore();
    var dlq = new InMemoryProjectionDeadLetterStore();
    var calls = new AtomicInteger(0);
    Projection failsTwiceThenOk =
        batch -> {
          if (calls.incrementAndGet() <= 2) {
            throw new EventStoreException("read-model blip");
          }
        };

    scheduled =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .register(
                "sched-transient",
                failsTwiceThenOk,
                "* * * * * *",
                ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduled.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> offsets.getLastOffset(ProjectionName.of("sched-transient")).value() == 1L);

    // No dead-letter was written for the transient failures.
    assertThat(dlq.read(ProjectionName.of("sched-transient"), 10)).isEmpty();
    assertThat(calls.get()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void scheduled_poisonDeadLettersAndAdvances() {
    var store = SimpleTestEventStore.of(List.of(evt()));
    var offsets = new InMemoryOffsetStore();
    var dlq = new InMemoryProjectionDeadLetterStore();
    Projection poison =
        batch -> {
          throw new RuntimeException("poison"); // POISON
        };

    scheduled =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .register(
                "sched-poison",
                poison,
                "* * * * * *",
                ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduled.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(ProjectionName.of("sched-poison")).value() == 1L);

    // POISON: dead-lettered and advanced past (current behavior).
    assertThat(dlq.read(ProjectionName.of("sched-poison"), 10)).hasSize(1);
  }

  private static EventEnvelope evt() {
    StreamId s = StreamId.of(TYPE, AggregateId.of("sched-s"));
    return new EventEnvelope(
        GlobalOffset.initial(),
        s,
        new Version(1),
        new EventType("TestEvent"),
        new TestEvent("x"),
        metadata("corr-sched"));
  }
}

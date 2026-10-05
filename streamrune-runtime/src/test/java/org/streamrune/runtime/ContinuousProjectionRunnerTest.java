package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ContinuousProjectionRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  private EventStore eventStore;
  private OffsetStore offsetStore;
  private TestProjection projection;

  @BeforeEach
  void setUp() {
    eventStore = new InMemoryEventStore();
    offsetStore = new InMemoryOffsetStore();
    projection = new TestProjection();
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10));
  }

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

  // Simple test event record implementing DomainEvent
  record TestEvent(String payload) implements DomainEvent {}

  private static EventEnvelope envelope(StreamId streamId, long version, String payload) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent(payload),
        metadata("corr-" + version));
  }

  // ==================== Builder validation ====================

  @Test
  void shouldCatchUpFromStoredOffset() {
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("stream-1"));
    eventStore.append(
        streamId,
        List.of(envelope(streamId, 1, "event1"), envelope(streamId, 2, "event2")),
        Version.initial());
    offsetStore.saveOffset(ProjectionName.of("test-projection"), GlobalOffset.initial());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(100)
            .batchSize(50)
            .subscriptionConfig(SubscriptionConfig.DEFAULT)
            .build();

    assertNotNull(runner);
  }

  @Test
  void shouldRejectInvalidFetchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ContinuousProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .fetchSize(0)
                .batchSize(50)
                .build());
  }

  @Test
  void shouldRejectInvalidBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ContinuousProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .fetchSize(100)
                .batchSize(0)
                .build());
  }

  @Test
  void shouldRejectNullEventStore() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ContinuousProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(null)
                .offsetStore(offsetStore)
                .build());
  }

  @Test
  void shouldRejectNullOffsetStore() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ContinuousProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(null)
                .build());
  }

  @Test
  void shouldAcceptErrorHandler() {
    var errors = new ArrayList<Throwable>();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .errorHandler(errors::add)
            .build();
    assertNotNull(runner);
  }

  @Test
  void shouldResetOffset() {
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("stream-1"));
    eventStore.append(streamId, List.of(envelope(streamId, 1, "event1")), Version.initial());
    offsetStore.saveOffset(ProjectionName.of("test-projection"), GlobalOffset.of(100));

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .build();

    runner.reset(ProjectionName.of("test-projection"));

    assertEquals(
        GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("test-projection")));
  }

  @Test
  void resetThrowsWhenOffsetStoreResetDoesNotActuallyRewind() {
    // Defense-in-depth: a broken OffsetStore whose reset() overrides the interface (so the
    // throwing default never fires) but doesn't actually rewind the checkpoint must still be
    // caught loudly by the runner, not silently accepted.
    var brokenStore =
        new OffsetStore() {
          private final Map<String, GlobalOffset> offsets = new HashMap<>();

          @Override
          public GlobalOffset getLastOffset(ProjectionName projectionName) {
            return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
          }

          @Override
          public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
            offsets.put(projectionName.value(), offset);
          }

          @Override
          public void reset(ProjectionName projectionName) {
            // Deliberately broken: claims success (no exception) but does not rewind.
          }
        };
    var name = ProjectionName.of("broken-reset-proj");
    brokenStore.saveOffset(name, GlobalOffset.of(50));
    var brokenRunner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(brokenStore)
            .build();

    var ex = assertThrows(IllegalStateException.class, () -> brokenRunner.reset(name));
    assertTrue(ex.getMessage().contains("reset"), ex.getMessage());
  }

  // ==================== Delivery mode: refusal at run(), before the claim ====================

  /**
   * Claims fencing and hands {@code null} to the updater — a plain projection runs at-least-once.
   */
  private static final org.streamrune.core.projection.AtomicBatchProcessor TRANSACTIONAL =
      new org.streamrune.core.projection.AtomicBatchProcessor() {
        @Override
        public boolean supportsFencing() {
          return true;
        }

        @Override
        public void executeAtomically(
            ProjectionName pn,
            List<EventEnvelope> batch,
            GlobalOffset newOffset,
            long fencingEpoch,
            ProjectionUpdater updater,
            OffsetStore os) {
          updater.update(null);
          os.saveOffset(pn, newOffset);
        }
      };

  /**
   * A plain projection declared {@code TRANSACTIONAL_LOCAL} under a transactional repository is
   * refused by {@code run()} before the runner is claimed: the state stays PENDING, and the same
   * runner accepts the same projection once it is declared {@code AT_LEAST_ONCE_IDEMPOTENT}.
   */
  @Test
  void run_refusesATransactionalLocalPlainProjection_beforeClaimingTheRunner() throws Exception {
    var repository = new InMemoryProjectionRepository();
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(repository)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .atomicProcessor(repository)
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                runner.run(
                    ProjectionName.of("non-write-through"), projection, TRANSACTIONAL_LOCAL));
    assertTrue(ex.getMessage().contains("ContinuousProjectionRunner"), ex.getMessage());
    assertTrue(ex.getMessage().contains("'non-write-through'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("TRANSACTIONAL_LOCAL"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("does not write through the handed repository"), ex.getMessage());
    assertEquals(ProjectionState.PENDING, runner.state(), "refused before the claim");

    StreamId s = StreamId.of(TYPE, AggregateId.of("declared-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());
    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("non-write-through"),
                        projection,
                        AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertEquals(1, projection.processed.size());
    assertEquals(
        GlobalOffset.of(1), repository.committedOffset(ProjectionName.of("non-write-through")));
  }

  @Test
  void buildRefusesAMissingProcessor_namingBothWaysOut() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ContinuousProjectionRunner.builder()
                    .eventStore(eventStore)
                    .offsetStore(offsetStore)
                    .build());
    assertEquals(
        "atomicProcessor is required: pass the JdbcProjectionRepository your TRANSACTIONAL_LOCAL"
            + " projections write to, or AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
            + " AT_LEAST_ONCE_IDEMPOTENT projections",
        ex.getMessage());
  }

  /**
   * run() asks the processor about the name before it reads an event, so a name the processor
   * cannot store read models under fails at startup instead of on the first batch. (Bounded: were
   * the check missing, run() would block polling the empty store.)
   */
  @Test
  void run_refusesANameTheProcessorCannotStore_beforeReadingAnEvent() {
    var processor = new NameRuleProcessor();
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .atomicProcessor(processor)
            .build();

    var ex =
        assertTimeoutPreemptively(
            Duration.ofSeconds(10),
            () ->
                assertThrows(
                    IllegalArgumentException.class,
                    () ->
                        runner.run(
                            ProjectionName.of("order-summary"),
                            projection,
                            AT_LEAST_ONCE_IDEMPOTENT)));
    assertTrue(ex.getMessage().contains("order-summary"), ex.getMessage());
    assertEquals(List.of(ProjectionName.of("order-summary")), processor.checked);
    assertEquals(0, processor.commits.get());
  }

  @Test
  void run_acceptsAPlainProjectionDeclaredAtLeastOnce_underAFencingProcessor() throws Exception {
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .atomicProcessor(TRANSACTIONAL)
            .build();

    StreamId s = StreamId.of(TYPE, AggregateId.of("at-least-once-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("at-least-once"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertEquals(1, projection.processed.size());
  }

  // ==================== Run path ====================

  @Test
  void runCatchesUpExistingEventsAndDeliversToProjection() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    eventStore.append(s, List.of(envelope(s, 1, "a"), envelope(s, 2, "b")), Version.initial());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("cart-view"), projection, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertEquals(2, projection.processed.size());
  }

  @Test
  void runProcessesEventsInBatches() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("stream-batches"));
    eventStore.append(
        s,
        List.of(
            envelope(s, 1, "a"),
            envelope(s, 2, "b"),
            envelope(s, 3, "c"),
            envelope(s, 4, "d"),
            envelope(s, 5, "e")),
        Version.initial());

    var batchCounts = new CopyOnWriteArrayList<Integer>();
    Projection batchTracking =
        events -> {
          batchCounts.add(events.size());
          projection.processed.addAll(events);
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(2)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("batches"), batchTracking, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.size() < 5 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    assertEquals(5, projection.processed.size());
    // batchSize=2 → 3 batches (2,2,1)
    int total = batchCounts.stream().mapToInt(Integer::intValue).sum();
    assertEquals(5, total);
  }

  @Test
  void runDeliversLiveEventsAfterCatchUp() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "initial")), Version.initial());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("live"), projection, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.size() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(1, projection.processed.size());

    // Append a live event after catch-up.
    eventStore.append(s, List.of(envelope(s, 2, "live")), new Version(1));

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertTrue(projection.processed.size() >= 2);
  }

  @Test
  void errorHandlerCalledOnCatchUpError() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("err-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());

    var errors = new AtomicInteger();
    Projection failing =
        batch -> {
          throw new RuntimeException("boom");
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .errorHandler(t -> errors.incrementAndGet())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("failing-proj"), failing, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (errors.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertTrue(errors.get() > 0);
  }

  @Test
  void closeStopsRunner() throws Exception {
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("idle"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    Thread.sleep(50);
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertFalse(t.isAlive());
  }

  @Test
  void closeWaitsForRunToReturn() throws Exception {
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("close-wait"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    runner.close();

    // close() interrupts the run thread and waits for run() to return, so the lifecycle state is
    // final the moment it does — no extra join needed before tearing down shared resources.
    assertEquals(ProjectionState.STOPPED, runner.state());
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertFalse(t.isAlive());
  }

  @Test
  void requestStopSignalsWithoutWaiting() throws Exception {
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("request-stop"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    runner.requestStop();

    // The run thread observes the flag at its next live-loop tick (≤1s) and exits on its own.
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
    assertEquals(ProjectionState.STOPPED, runner.state());
  }

  @Test
  void runRejectsConcurrentExecution() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("only-one"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("first"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      // Wait until the first run is in progress (it won the `running` CAS and reached LIVE). A
      // fixed sleep could let run("second") below win the CAS instead, and that call never returns
      // (this module has no JUnit timeout), so the test would hang rather than fail.
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> runner.state() == ProjectionState.LIVE);
      assertThrows(
          IllegalStateException.class,
          () -> runner.run(ProjectionName.of("second"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  @Test
  void runRejectingConcurrentExecution_quotesTheProjectionNameSanitized() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("only-one-forged"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();
    String forged = "second\n2026-10-04 10:00:00 WARN forged\r\u202e";

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("first"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> runner.state() == ProjectionState.LIVE);
      var ex =
          assertThrows(
              IllegalStateException.class,
              () -> runner.run(ProjectionName.of(forged), projection, AT_LEAST_ONCE_IDEMPOTENT));
      assertTrue(
          ex.getMessage().contains(org.streamrune.core.types.LogSanitizer.sanitizeForLog(forged)),
          ex.getMessage());
      assertFalse(ex.getMessage().chars().anyMatch(Character::isISOControl), ex.getMessage());
      assertFalse(ex.getMessage().contains("\u202e"), ex.getMessage());
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  @Test
  void usesSubscriptionFactoryWhenProvided() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("factory-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var factoryCalled = new AtomicInteger();
    ContinuousProjectionRunner.SubscriptionFactory factory =
        (name, listener, readPoisonBound) -> {
          factoryCalled.incrementAndGet();
          return PollingEventSubscription.builder()
              .subscriptionName(name.value())
              .eventStore(eventStore)
              .offsetStore(offsetStore)
              .listener(listener)
              .config(fastConfig())
              .fetchSize(10)
              .build();
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .subscriptionFactory(factory)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("with-factory"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (factoryCalled.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertTrue(factoryCalled.get() > 0);
  }

  @Test
  void factoryBuiltSubscription_dlqPoisonLiveBatch_isDeadLetteredAndCheckpointAdvances()
      throws Exception {
    // Pairing the runner with a factory-built subscription (the recommended
    // HybridEventSubscription production path) must NOT drop the error strategy in LIVE mode.
    // Before the fix the factory received the RAW projection and a poison live batch spun forever
    // (offset frozen, nothing dead-lettered, health never fed). Now the factory receives the
    // runner's fully-wrapped listener, so processLiveBatch applies: a poison live batch is
    // dead-lettered under DLQ and the checkpoint advances past it.
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    ContinuousProjectionRunner.SubscriptionFactory factory =
        (name, listener, readPoisonBound) ->
            PollingEventSubscription.builder()
                .subscriptionName(name.value())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .listener(listener) // the runner's WRAPPED listener — the error strategy applies
                .config(fastConfig())
                .fetchSize(10)
                .readPoisonBound(readPoisonBound)
                .build();

    Projection poison =
        events -> {
          throw new RuntimeException("permanent poison"); // POISON → dead-lettered
        };
    var proj = ProjectionName.of("factory-dlq");
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
            .subscriptionFactory(factory)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(proj, poison, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException _) {
                    // a permanent ERROR exit would rethrow; DLQ should instead continue past
                  }
                });

    // Reach LIVE (empty catch-up), then append a poison event so it is delivered live.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());
    StreamId s = StreamId.of(TYPE, AggregateId.of("factory-dlq-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "poison")), Version.initial());

    // The poison live batch is dead-lettered and the checkpoint advances past it (not frozen).
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (dlqStore.read(proj, 10).isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(
        dlqStore.read(proj, 10).isEmpty(),
        "a factory-path poison live batch must be dead-lettered under DLQ, not spin forever");
    while (offsetStore.getLastOffset(proj).value() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(
        offsetStore.getLastOffset(proj).value() >= 1,
        "the checkpoint must advance past the dead-lettered batch");
    assertNotEquals(ProjectionState.ERROR, runner.state());
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  // ==================== Error strategy + state ====================

  @Test
  void skipStrategy_advancesOffsetPastFailingBatch() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("skip-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail"), envelope(s, 2, "ok")), Version.initial());

    var failOnce = new AtomicInteger(0);
    Projection skipProj =
        events -> {
          if (failOnce.incrementAndGet() == 1) {
            throw new RuntimeException("transient failure");
          }
          projection.processed.addAll(events);
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(ProjectionName.of("skip-proj"), skipProj, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    // Second batch (event 2) should have been processed after first was skipped
    assertFalse(projection.processed.isEmpty());
  }

  @Test
  void catchUpSkip_transientSaveOffsetFailureOnAdvance_retriesFromCheckpointNotKilled()
      throws Exception {
    // When the catch-up SKIP branch's checkpoint advance (offsetStore.saveOffset)
    // throws a TRANSIENT infra error (a DB failover blip), the OLD code let it propagate out of
    // handleBatchError → catchUp → run(), which set state=ERROR and terminally killed the
    // projection. The fix backs off and retries from the checkpoint (offset NOT advanced), re-reads
    // and re-skips idempotently, so a seconds-long blip no longer kills the projection.
    StreamId s = StreamId.of(TYPE, AggregateId.of("skip-save-fail"));
    eventStore.append(
        s, List.of(envelope(s, 1, "poison"), envelope(s, 2, "ok")), Version.initial());

    var failNextSave = new AtomicInteger(1); // throw on the FIRST saveOffset (the skip advance)
    OffsetStore flaky =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return offsetStore.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            if (failNextSave.getAndDecrement() > 0) {
              throw new RuntimeException("offset store blip");
            }
            offsetStore.saveOffset(n, o);
          }

          @Override
          public void reset(ProjectionName n) {
            offsetStore.reset(n);
          }
        };

    Projection p =
        events -> {
          if (events.getFirst().globalOffset().value() == 1L) {
            throw new RuntimeException("poison"); // event 1 always fails → SKIP applies
          }
          projection.processed.addAll(events);
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(flaky)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("skip-save-fail"), p, AT_LEAST_ONCE_IDEMPOTENT));
    // Event 2 is processed only after the skip advance recovers from the transient save failure.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(
        projection.processed.isEmpty(),
        "runner must recover past the skip and process event 2, not die on the transient save");
    assertNotEquals(ProjectionState.ERROR, runner.state());
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void catchUpDlq_transientSaveOffsetFailureOnAdvance_retriesFromCheckpointNotKilled()
      throws Exception {
    // The dead-letter WRITE succeeds but the subsequent checkpoint
    // advance (offsetStore.saveOffset) throws a TRANSIENT blip. The OLD code propagated it to run()
    // and killed the projection; the fix backs off and retries from the checkpoint. Re-processing
    // re-dead-letters idempotently (the store upserts on (projection_name, from_offset)), so the
    // range is eventually advanced past without the projection dying.
    StreamId s = StreamId.of(TYPE, AggregateId.of("dlq-save-fail"));
    eventStore.append(
        s, List.of(envelope(s, 1, "poison"), envelope(s, 2, "ok")), Version.initial());

    var failNextSave = new AtomicInteger(1); // throw on the FIRST saveOffset (the DLQ advance)
    OffsetStore flaky =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return offsetStore.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            if (failNextSave.getAndDecrement() > 0) {
              throw new RuntimeException("offset store blip");
            }
            offsetStore.saveOffset(n, o);
          }

          @Override
          public void reset(ProjectionName n) {
            offsetStore.reset(n);
          }
        };

    Projection p =
        events -> {
          if (events.getFirst().globalOffset().value() == 1L) {
            throw new RuntimeException("permanent poison"); // POISON → dead-lettered
          }
          projection.processed.addAll(events);
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(flaky)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("dlq-save-fail"), p, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(
        projection.processed.isEmpty(),
        "runner must recover past the dead-lettered range, not die on the transient save");
    assertNotEquals(ProjectionState.ERROR, runner.state());
    // The poison range is dead-lettered (idempotent upsert on retry — at least once entry present).
    assertFalse(
        dlqStore.read(ProjectionName.of("dlq-save-fail"), 10).isEmpty(),
        "the poison range must be dead-lettered");
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void catchUpSkip_epochFenceRejection_doesNotAdvanceCheckpoint_noDeadLetter() throws Exception {
    // An epoch-fence OptimisticLockException from executeAtomically is a LEADERSHIP signal,
    // not a projection error. Under errorStrategy=SKIP a stale leader (whose local
    // leadership.current is still present during the partition window) must NOT convert the fence
    // REJECTION into a forward checkpoint advance — that would silently, permanently skip the
    // fenced events. The batch is abandoned, the checkpoint is untouched, and nothing is
    // dead-lettered.
    var name = ProjectionName.of("fenced-skip");
    StreamId s = StreamId.of(TYPE, AggregateId.of("fenced-skip"));
    eventStore.append(s, List.of(envelope(s, 1, "e1"), envelope(s, 2, "e2")), Version.initial());

    // A stale leader: leadership.current()/tryAcquire() keep reporting "I am leader" (epoch 1)
    // throughout — exactly the stale local-map window the fence exists to cover downstream.
    var staleLeadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(1L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    // The transactional processor rejects the stale leader's commit with an OptimisticLockException
    // (epoch 1 < stored epoch 2) BEFORE writing anything, and never saves the offset.
    var atomicCalls = new java.util.concurrent.CountDownLatch(1);
    org.streamrune.core.projection.AtomicBatchProcessor fencingProcessor =
        (projectionName, batch, newOffset, fencingEpoch, updater, os) -> {
          atomicCalls.countDown();
          try {
            Thread.sleep(20); // throttle the abandon/re-check loop so the test doesn't busy-spin
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
          throw new org.streamrune.core.projection.ProjectionCommitFencedException(
              "commit fenced out: caller epoch 1 is below the stored epoch 2");
        };

    // Record every saveOffset the runner attempts, to prove the checkpoint is never advanced.
    var forwardSaves = new CopyOnWriteArrayList<Long>();
    OffsetStore recording =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return offsetStore.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            forwardSaves.add(o.value());
            offsetStore.saveOffset(n, o);
          }

          @Override
          public void reset(ProjectionName n) {
            offsetStore.reset(n);
          }
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(recording)
            .atomicProcessor(TestFencingProcessor.fencing(fencingProcessor))
            .leadership(staleLeadership)
            .fetchSize(2)
            .batchSize(2)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(
        atomicCalls.await(5, TimeUnit.SECONDS), "the runner must attempt the fenced atomic commit");
    Thread.sleep(300); // give a buggy SKIP path time to advance the checkpoint if it were going to

    assertEquals(
        0L,
        offsetStore.getLastOffset(name).value(),
        "the fence rejection must NOT advance the checkpoint past the un-applied events");
    assertTrue(
        forwardSaves.isEmpty(),
        () ->
            "no checkpoint advance may be written for a fence rejection, but saw: " + forwardSaves);
    assertTrue(dlqStore.read(name, 10).isEmpty(), "a fence rejection must never be dead-lettered");
    assertTrue(projection.processed.isEmpty(), "no events were applied by the fenced-out leader");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void skip_staleLeaderCheckpointAdvance_isEpochFenced_neverSkipsEvents() throws Exception {
    // A genuine PROJECTION error (NOT a fence) under errorStrategy=SKIP triggers the
    // SKIP checkpoint advance. Pre-fix that advance was a RAW offsetStore.saveOffset — a forward
    // move the monotonic guard cannot reject — so a stale leader whose local leadership.current()
    // still reports "leader" (the ttl/3 staleness window) would advance the checkpoint past events
    // a newer leader has not applied, SILENTLY and permanently skipping them from the read model
    // with NO dead-letter. The fix routes the checkpoint-only advance (catch-up AND live) through
    // the EPOCH-FENCED empty-batch executeAtomically, which a real Jdbc processor rejects for a
    // superseded epoch. Here the fencing processor RUNS the projection on the non-empty processing
    // batch (so its own error surfaces and drives the SKIP path) but FENCES the empty-batch advance
    // (caller epoch 1 < stored epoch 2). The checkpoint must never advance; nothing is
    // dead-lettered.
    var name = ProjectionName.of("a6-skip-fenced");
    StreamId s = StreamId.of(TYPE, AggregateId.of("a6-skip-fenced"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());

    // A stale leader: current()/tryAcquire() keep reporting "I am leader" (epoch 1) throughout —
    // exactly the stale local-map window the fence exists to cover downstream.
    var staleLeadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(1L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    // Fencing processor mirroring the real Jdbc one: the NON-EMPTY processing batch runs the
    // updater (so the projection's own error surfaces → SKIP), while the EMPTY-batch checkpoint
    // advance is fenced out (epoch 1 < stored epoch 2), as a superseded leader's advance would be.
    var emptyBatchAdvanceAttempted = new java.util.concurrent.CountDownLatch(1);
    org.streamrune.core.projection.AtomicBatchProcessor fencingProcessor =
        (pn, batch, newOffset, fencingEpoch, updater, os) -> {
          if (batch.isEmpty()) {
            emptyBatchAdvanceAttempted.countDown();
            try {
              Thread.sleep(20); // throttle the abandon/re-check loop so the test doesn't busy-spin
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
            throw new org.streamrune.core.projection.ProjectionCommitFencedException(
                "commit fenced out: caller epoch " + fencingEpoch + " is below the stored epoch 2");
          }
          updater.update(null); // runs the projection → it throws → drives the SKIP path
          os.saveOffset(pn, newOffset);
        };

    // Record every RAW saveOffset to prove no unfenced checkpoint advance ever lands.
    var forwardSaves = new CopyOnWriteArrayList<Long>();
    OffsetStore recording =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName n) {
            return offsetStore.getLastOffset(n);
          }

          @Override
          public void saveOffset(ProjectionName n, GlobalOffset o) {
            forwardSaves.add(o.value());
            offsetStore.saveOffset(n, o);
          }

          @Override
          public void reset(ProjectionName n) {
            offsetStore.reset(n);
          }
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    Projection poison =
        events -> {
          throw new RuntimeException("poison batch"); // a genuine processing error → SKIP
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(recording)
            .atomicProcessor(TestFencingProcessor.fencing(fencingProcessor))
            .leadership(staleLeadership)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t = Thread.ofVirtual().start(() -> runner.run(name, poison, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(
        emptyBatchAdvanceAttempted.await(5, TimeUnit.SECONDS),
        "the SKIP checkpoint advance must be routed through the fenced empty-batch commit");
    Thread.sleep(300); // give a buggy (unfenced) SKIP path time to advance if it were going to

    assertEquals(
        0L,
        offsetStore.getLastOffset(name).value(),
        "a fenced-out stale leader must NOT advance the checkpoint past the un-applied events");
    assertTrue(
        forwardSaves.isEmpty(),
        () -> "no raw checkpoint advance may land for a fenced SKIP, but saw: " + forwardSaves);
    assertTrue(dlqStore.read(name, 10).isEmpty(), "SKIP must never dead-letter");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void takeover_stampsEpochOnCheckpointBeforeAnyProcessing() throws Exception {
    // The epoch fence compares against the epoch STORED on the checkpoint row, which
    // only a committed advance used to stamp — so between a takeover and the new leader's first
    // committed advance the row still carried the superseded leader's epoch and ALL of its writes
    // passed the fence. The runner must stamp the just-acquired epoch on the checkpoint row
    // (stampFencingEpoch) BEFORE reading or processing a single event, closing that window.
    var name = ProjectionName.of("takeover-stamp");
    StreamId s = StreamId.of(TYPE, AggregateId.of("takeover-stamp"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());

    var leadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(7L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    // Records the exact order of stamp vs processing calls (anonymous class: a lambda could not
    // override the default stampFencingEpoch).
    var calls = new CopyOnWriteArrayList<String>();
    var firstExecute = new java.util.concurrent.CountDownLatch(1);
    var stampingProcessor =
        new org.streamrune.core.projection.AtomicBatchProcessor() {
          @Override
          public boolean supportsFencing() {
            return true;
          }

          @Override
          public void executeAtomically(
              ProjectionName pn,
              List<EventEnvelope> batch,
              GlobalOffset newOffset,
              long fencingEpoch,
              ProjectionUpdater updater,
              OffsetStore os) {
            calls.add("execute:" + fencingEpoch);
            firstExecute.countDown();
            updater.update(null);
            os.saveOffset(pn, newOffset);
          }

          @Override
          public void stampFencingEpoch(ProjectionName pn, long fencingEpoch) {
            calls.add("stamp:" + fencingEpoch);
          }
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(stampingProcessor)
            .leadership(leadership)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(firstExecute.await(5, TimeUnit.SECONDS), "the batch must be processed");

    assertEquals(
        "stamp:7",
        calls.get(0),
        () ->
            "the takeover stamp (with the acquired epoch) must precede ANY processing, but the"
                + " call order was: "
                + calls);
    assertTrue(
        calls.contains("execute:7"),
        () -> "the commit carries the same held epoch; calls: " + calls);

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void takeover_stampFailure_failsClosed_noProcessingUntilStampCommits() throws Exception {
    // Fail-closed discipline: processing WITHOUT the takeover stamp would re-open the
    // takeover window (the fence stays inert for the superseded leader), so a stamp failure must
    // NOT be shrugged off — the runner stands by and retries the acquire+stamp, and only
    // processes once a stamp has committed.
    var name = ProjectionName.of("takeover-stamp-fail");
    StreamId s = StreamId.of(TYPE, AggregateId.of("takeover-stamp-fail"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());

    var leadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(9L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    var stampAttempts = new java.util.concurrent.atomic.AtomicInteger();
    var processedBeforeStamp = new java.util.concurrent.atomic.AtomicBoolean(false);
    var stampSucceeded = new java.util.concurrent.atomic.AtomicBoolean(false);
    var firstExecute = new java.util.concurrent.CountDownLatch(1);
    var failingThenStampingProcessor =
        new org.streamrune.core.projection.AtomicBatchProcessor() {
          @Override
          public boolean supportsFencing() {
            return true;
          }

          @Override
          public void executeAtomically(
              ProjectionName pn,
              List<EventEnvelope> batch,
              GlobalOffset newOffset,
              long fencingEpoch,
              ProjectionUpdater updater,
              OffsetStore os) {
            if (!stampSucceeded.get()) {
              processedBeforeStamp.set(true); // the violation this test pins against
            }
            updater.update(null);
            os.saveOffset(pn, newOffset);
            // Signal only once the batch has actually been applied. Counting down on ENTRY let the
            // await below return before updater.update(null) had populated projection.processed, so
            // the size assertion sampled a half-finished execute and intermittently saw 0.
            firstExecute.countDown();
          }

          @Override
          public void stampFencingEpoch(ProjectionName pn, long fencingEpoch) {
            if (stampAttempts.incrementAndGet() <= 2) {
              throw new RuntimeException("simulated transient stamp failure " + stampAttempts);
            }
            stampSucceeded.set(true);
          }
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(failingThenStampingProcessor)
            .leadership(leadership)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(
        firstExecute.await(10, TimeUnit.SECONDS),
        "the runner must recover and process once the stamp finally commits");

    assertTrue(stampAttempts.get() >= 3, "the stamp is retried across standby cycles");
    assertFalse(
        processedBeforeStamp.get(),
        "FAIL-CLOSED: nothing may be processed before a takeover stamp has committed");
    assertEquals(1, projection.processed.size(), "the batch is applied after the stamp lands");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void catchUp_userOptimisticLockInsideProcess_reachesErrorStrategy_notTreatedAsFence()
      throws Exception {
    // A projection's OWN versioned-save conflict (findById(OPTIMISTIC) + save(id, model,
    // expectedVersion)) throws a plain OptimisticLockException from INSIDE process() — a user
    // version conflict, NOT a framework epoch-fence/CAS rejection. It must reach the configured
    // error strategy (here SKIP -> advance past the failed batch), not be swallowed as a
    // leadership/CAS signal, which would abandon-and-re-read forever and silently freeze the
    // projection. Only the framework subtype ProjectionCommitFencedException is the fence signal.
    var name = ProjectionName.of("user-ole");
    StreamId s = StreamId.of(TYPE, AggregateId.of("user-ole"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());

    // Mirror the real Jdbc processor: a non-empty processing batch RUNS the updater (so the
    // projection's user OptimisticLockException surfaces), while the empty-batch SKIP advance
    // commits the checkpoint.
    org.streamrune.core.projection.AtomicBatchProcessor userConflictProcessor =
        (pn, batch, newOffset, fencingEpoch, updater, os) -> {
          if (batch.isEmpty()) {
            os.saveOffset(pn, newOffset); // the SKIP checkpoint-only advance
            return;
          }
          updater.update(null); // runs the projection -> throws a plain (user) OLE, never fenced
          os.saveOffset(pn, newOffset);
        };

    Projection userConflict =
        events -> {
          throw new org.streamrune.core.OptimisticLockException(
              "Optimistic lock conflict on projection 'user-ole/x': expected version 3");
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(userConflictProcessor)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .deadLetterStore(new InMemoryProjectionDeadLetterStore())
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, userConflict, AT_LEAST_ONCE_IDEMPOTENT));

    // Post-fix: the user OLE reaches the SKIP error strategy -> the checkpoint advances past the
    // failed batch. Pre-fix: it was caught as a fence -> abandon-and-re-read -> the checkpoint
    // never advances (the projection wedges).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline && offsetStore.getLastOffset(name).value() < 1L) {
      Thread.sleep(20);
    }
    assertEquals(
        1L,
        offsetStore.getLastOffset(name).value(),
        "a user OptimisticLockException from process() must reach the SKIP error strategy and"
            + " advance the checkpoint, not be swallowed as a fence");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void run_afterTimedOutClose_refusesToReviveStuckLoop() throws Exception {
    // A run() thread stuck inside process() (uninterruptible JDBC) survives a timed-out
    // close(), which leaves running=false but the thread alive. A second run() must NOT flip
    // running back to true and revive the stuck loop — two concurrent loops double-apply under
    // nonAtomicAtLeastOnce(). run() refuses while the previous thread is alive (mirrors
    // the other runners' stop discipline).
    var name = ProjectionName.of("stuck-revive");
    StreamId s = StreamId.of(TYPE, AggregateId.of("stuck-revive"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());

    var inProcess = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    Projection blocking =
        events -> {
          inProcess.countDown();
          try {
            release.await();
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        };
    // Runs the (blocking) projection via the updater; never advances the offset.
    org.streamrune.core.projection.AtomicBatchProcessor proc =
        (pn, batch, off, epoch, updater, os) -> updater.update(null);

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(proc)
            .fetchSize(1)
            .batchSize(1)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t1 =
        Thread.ofVirtual().start(() -> runner.run(name, blocking, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(inProcess.await(5, TimeUnit.SECONDS), "the run thread must reach process()");

    // Simulate a timed-out close(): signal stop, but the thread stays stuck in process() (alive).
    runner.requestStop();

    // A second run() must be refused while t1 is alive. Run it on another thread so a buggy
    // (revived) run() that blocks in process() does not hang the test thread.
    var outcome = new java.util.concurrent.atomic.AtomicReference<String>();
    var secondDone = new java.util.concurrent.CountDownLatch(1);
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                runner.run(name, blocking, AT_LEAST_ONCE_IDEMPOTENT);
                outcome.set("STARTED");
              } catch (IllegalStateException _) {
                outcome.set("REFUSED");
              } finally {
                secondDone.countDown();
              }
            });

    assertTrue(
        secondDone.await(3, TimeUnit.SECONDS),
        "the second run() must return promptly (refused), not revive the stuck loop and block");
    assertEquals(
        "REFUSED",
        outcome.get(),
        "run() must refuse to revive while the previous (stuck) run thread is alive");

    // Cleanup: release the stuck thread so it exits.
    release.countDown();
    t1.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void skip_singleNodeEpochZeroCheckpointAdvance_isFencedButStillCommits() throws Exception {
    // No-regression: with NOOP leadership (epoch 0, unfenced) the SKIP checkpoint advance
    // now flows through the empty-batch executeAtomically. A guard-enforcing processor mirroring
    // the real Jdbc fence (rejects epoch != 0 && < stored, epoch 0 always passes) must STILL
    // advance the checkpoint past the skipped batch and let the next event process — the common
    // single-node path must not regress.
    var name = ProjectionName.of("a6-skip-single-node");
    StreamId s = StreamId.of(TYPE, AggregateId.of("a6-skip-single-node"));
    eventStore.append(
        s, List.of(envelope(s, 1, "poison"), envelope(s, 2, "ok")), Version.initial());

    var emptyBatchAdvances = new CopyOnWriteArrayList<Long>();
    org.streamrune.core.projection.AtomicBatchProcessor guarded =
        (pn, batch, newOffset, fencingEpoch, updater, os) -> {
          if (fencingEpoch != 0L && fencingEpoch < 2L) {
            throw new org.streamrune.core.projection.ProjectionCommitFencedException("fenced");
          }
          if (batch.isEmpty()) {
            emptyBatchAdvances.add(newOffset.value()); // the SKIP checkpoint-only advance
          }
          updater.update(null);
          os.saveOffset(pn, newOffset);
        };

    Projection p =
        events -> {
          if (events.getFirst().globalOffset().value() == 1L) {
            throw new RuntimeException("poison"); // event 1 → SKIP
          }
          projection.processed.addAll(events);
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .atomicProcessor(guarded)
            .fetchSize(1)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t = Thread.ofVirtual().start(() -> runner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(
        projection.processed.isEmpty(),
        "single-node SKIP must advance past the poison (epoch 0 is unfenced) and process event 2");
    assertTrue(
        emptyBatchAdvances.contains(1L),
        "the SKIP advance must route through the empty-batch fenced commit at epoch 0");
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  /**
   * {@code projection_dead_letters.error_message} is a persisted-text sink — a projection's message
   * can echo event data; the entry carries the sanitized text.
   */
  @Test
  void dlqStrategy_poison_persistsTheErrorMessageSanitized() throws Exception {
    String raw = "unknown order 'o-1\r\n2026-10-02 INFO proj - caught up'\u0000";
    StreamId s = StreamId.of(TYPE, AggregateId.of("dlq-text-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());
    Projection alwaysFail =
        events -> {
          throw new RuntimeException(raw); // POISON
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
                        ProjectionName.of("dlq-text"), alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (dlqStore.read(ProjectionName.of("dlq-text"), 10).isEmpty()
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    var entries = dlqStore.read(ProjectionName.of("dlq-text"), 10);
    assertEquals(1, entries.size());
    assertEquals(LogSanitizer.sanitizeFreeText(raw), entries.get(0).errorMessage());
    assertFalse(
        entries.get(0).errorMessage().chars().anyMatch(Character::isISOControl),
        entries.get(0).errorMessage());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void dlqStrategy_poison_writesToStoreAndContinues() throws Exception {
    // Behavior: a POISON batch under DLQ is dead-lettered and the runner CONTINUES
    // (no halt, no ERROR state) — it used to halt here. The single poison event is dead-lettered,
    // the catch-up advances past it, and the runner reaches STOPPED cleanly on close.
    StreamId s = StreamId.of(TYPE, AggregateId.of("dlq-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("permanent failure"); // POISON
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
    assertNull(runner.lastError(), "a runner that has not failed reports no error");

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("dlq-proj"), alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));

    // The poison range is dead-lettered exactly once, then the runner keeps going (does not halt).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (dlqStore.read(ProjectionName.of("dlq-proj"), 10).isEmpty()
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    var entries = dlqStore.read(ProjectionName.of("dlq-proj"), 10);
    assertEquals(1, entries.size());
    assertEquals(ProjectionName.of("dlq-proj"), entries.get(0).projectionName());
    assertEquals("java.lang.RuntimeException", entries.get(0).errorType());

    // The runner is NOT halted: it advanced past the dead-lettered range and never entered ERROR.
    assertNotEquals(ProjectionState.ERROR, runner.state());
    assertEquals(1, offsetStore.getLastOffset(ProjectionName.of("dlq-proj")).value());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void dlqStrategy_poison_entryCoversExactlyTheFailedChunk_andContinues() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("dlq-range-stream"));
    eventStore.append(
        s,
        List.of(
            envelope(s, 1, "ok"),
            envelope(s, 2, "ok"),
            envelope(s, 3, "fail"),
            envelope(s, 4, "fail"),
            envelope(s, 5, "ok-again"),
            envelope(s, 6, "ok-again")),
        Version.initial());

    // First chunk (offsets 1-2) commits; the second chunk (offsets 3-4) is POISON; under DLQ
    // the runner dead-letters that exact chunk and CONTINUES to the third chunk (offsets 5-6),
    // which now commits — the old halt would never have reached it.
    var calls = new AtomicInteger(0);
    Projection failsOnMiddleChunk =
        events -> {
          if (((TestEvent) events.get(0).event()).payload().equals("fail")) {
            calls.incrementAndGet();
            throw new RuntimeException("poison chunk"); // POISON
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
                        ProjectionName.of("dlq-range-proj"),
                        failsOnMiddleChunk,
                        AT_LEAST_ONCE_IDEMPOTENT));

    // Wait until the runner has advanced past the poison range to the trailing chunk.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (offsetStore.getLastOffset(ProjectionName.of("dlq-range-proj")).value() < 6
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    // The dead-letter entry scopes exactly the failed chunk: it starts at the first failed offset
    // (not the committed checkpoint), ends at the chunk's last offset, and counts only the chunk's
    // events. The poison chunk is dead-lettered exactly once — no retry storm.
    var entries = dlqStore.read(ProjectionName.of("dlq-range-proj"), 10);
    assertEquals(1, entries.size());
    var entry = entries.get(0);
    assertEquals(3, entry.fromOffset().value());
    assertEquals(4, entry.toOffset().value());
    assertEquals(2, entry.batchSize());
    assertEquals(1, calls.get(), "poison chunk dead-lettered exactly once (no retry storm)");

    // The committed checkpoint advanced PAST the dead-lettered range to the trailing chunk.
    assertEquals(6, offsetStore.getLastOffset(ProjectionName.of("dlq-range-proj")).value());
    assertNotEquals(ProjectionState.ERROR, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void dlqStrategy_poison_doesNotAdvanceAndRetriesWhenDeadLetterWriteFails() throws Exception {
    // When the dead-letter STORE WRITE itself fails (DLQ store down), a failed write
    // is a TRANSIENT infra failure — the runner must NOT advance past the (un-recorded) poison
    // range (which would silently, permanently lose it from the read model). It keeps the offset
    // put, stays RUNNING (never ERROR), and retries from the checkpoint; once the store recovers
    // the range is dead-lettered and the offset advances. This replaces the old test that asserted
    // the buggy advance-on-write-failure behavior.
    StreamId s = StreamId.of(TYPE, AggregateId.of("dlq-fail-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("processing failure"); // POISON
        };

    // Store stays DOWN until the test flips storeUp — so the "offset did not advance" assertion is
    // observed against a store that is reliably still failing, not one that already recovered.
    var storeUp = new java.util.concurrent.atomic.AtomicBoolean(false);
    var writeAttempts = new AtomicInteger(0);
    var written =
        new CopyOnWriteArrayList<org.streamrune.core.projection.ProjectionDeadLetterEntry>();
    var flakyDlqStore =
        new org.streamrune.core.projection.ProjectionDeadLetterStore() {
          @Override
          public void save(org.streamrune.core.projection.ProjectionDeadLetterEntry entry) {
            writeAttempts.incrementAndGet();
            if (!storeUp.get()) {
              throw new RuntimeException("DLQ store unavailable");
            }
            written.add(entry);
          }

          @Override
          public java.util.List<org.streamrune.core.projection.ProjectionDeadLetterEntry> read(
              org.streamrune.core.types.ProjectionName projectionName, int limit) {
            return java.util.List.copyOf(written);
          }

          @Override
          public java.util.List<org.streamrune.core.projection.ProjectionDeadLetterEntry> readAll(
              int limit) {
            return java.util.List.copyOf(written);
          }

          @Override
          public void discard(
              org.streamrune.core.types.ProjectionName projectionName,
              org.streamrune.core.types.GlobalOffset fromOffset) {}
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(flakyDlqStore)
            .subscriptionConfig(fastConfig())
            .build();

    var proj = ProjectionName.of("dlq-fail-proj");
    Thread t =
        Thread.ofVirtual().start(() -> runner.run(proj, alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));

    // First: the store rejects the write. The runner must NOT advance past the range and must NOT
    // go ERROR — it stays RUNNING and retries. Observe non-advancement while the store is still
    // down (storeUp == false), after the write has been attempted at least once.
    long attemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (writeAttempts.get() < 1 && System.nanoTime() < attemptDeadline) {
      Thread.sleep(10);
    }
    assertTrue(writeAttempts.get() >= 1, "the dead-letter write should have been attempted");
    assertEquals(
        0,
        offsetStore.getLastOffset(proj).value(),
        "offset must NOT advance while the dead-letter write is failing");
    assertNotEquals(ProjectionState.ERROR, runner.state());

    // Then: recover the store; the next retry dead-letters the range and advances past it.
    storeUp.set(true);
    long recoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (offsetStore.getLastOffset(proj).value() < 1 && System.nanoTime() < recoverDeadline) {
      Thread.sleep(20);
    }
    assertEquals(
        1,
        offsetStore.getLastOffset(proj).value(),
        "offset advances only after the dead-letter write succeeds");
    assertEquals(1, written.size(), "the poison range is dead-lettered exactly once, on recovery");
    assertNotEquals(ProjectionState.ERROR, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "Runner thread should have terminated after close");
  }

  @Test
  void skipStrategy_skipsOnlyTheFailedBatch_notUnattemptedOnes() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("skip-granular"));
    // One fetched page (fetchSize=10) split into 3 chunks of batchSize=1: "a", "fail", "c".
    eventStore.append(
        s,
        List.of(envelope(s, 1, "a"), envelope(s, 2, "fail"), envelope(s, 3, "c")),
        Version.initial());

    var processedPayloads = new CopyOnWriteArrayList<String>();
    Projection proj =
        events -> {
          for (var e : events) {
            if (((TestEvent) e.event()).payload().equals("fail")) {
              throw new RuntimeException("poison batch");
            }
          }
          events.forEach(e -> processedPayloads.add(((TestEvent) e.event()).payload()));
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("skip-granular-proj"), proj, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (processedPayloads.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    // Only the failed chunk ("fail") is skipped; "c" was never attempted when "fail" blew up
    // and MUST still be processed — skipping the whole page would lose it.
    assertEquals(List.of("a", "c"), List.copyOf(processedPayloads));
  }

  @Test
  void liveSkipStrategy_appliesErrorStrategyAndContinues() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-skip-stream"));

    var processedPayloads = new CopyOnWriteArrayList<String>();
    Projection proj =
        events -> {
          for (var e : events) {
            if (((TestEvent) e.event()).payload().equals("fail")) {
              throw new RuntimeException("poison live batch");
            }
          }
          events.forEach(e -> processedPayloads.add(((TestEvent) e.event()).payload()));
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("live-skip"), proj, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // Poisoned live batch — the configured SKIP strategy must apply (offset advances past it).
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (offsetStore.getLastOffset(ProjectionName.of("live-skip")).value() < 1
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(
        offsetStore.getLastOffset(ProjectionName.of("live-skip")).value() >= 1,
        "SKIP must advance the checkpoint past the poisoned live batch");

    // Subsequent live events keep flowing.
    eventStore.append(s, List.of(envelope(s, 2, "ok")), new Version(1));
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (processedPayloads.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(List.of("ok"), List.copyOf(processedPayloads));
    assertEquals(ProjectionState.LIVE, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void liveDlqStrategy_poison_writesToStoreAndContinuesLive() throws Exception {
    // Behavior: a POISON live batch under DLQ is dead-lettered and the subscription
    // ADVANCES past it — the runner stays LIVE (does not halt into ERROR as it used to).
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-dlq-stream"));

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("permanent live failure"); // POISON
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
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("live-dlq"), alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // Poisoned live batch with DLQ strategy: failed range written to the store and advanced past.
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());

    long dlqDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (dlqStore.read(ProjectionName.of("live-dlq"), 10).isEmpty()
        && System.nanoTime() < dlqDeadline) {
      Thread.sleep(20);
    }

    var entries = dlqStore.read(ProjectionName.of("live-dlq"), 10);
    assertEquals(1, entries.size());
    assertEquals(ProjectionName.of("live-dlq"), entries.get(0).projectionName());
    assertEquals("java.lang.RuntimeException", entries.get(0).errorType());

    // The runner is NOT halted: it stays LIVE and never entered ERROR.
    assertNotEquals(ProjectionState.ERROR, runner.state());
    assertTrue(t.isAlive(), "runner must keep running after the live DLQ write");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void liveDlqStrategy_poison_deadLetterWriteFails_doesNotAdvanceAndRetriesUntilStoreRecovers()
      throws Exception {
    // LIVE-mode analog of dlqStrategy_poison_doesNotAdvanceAndRetriesWhenDeadLetterWriteFails (line
    // 600), which drives the CATCH-UP path. The live advance mechanism — PollingEventSubscription
    // .pollOnce, which advances only when onEvents returns normally — is structurally different
    // from catch-up's RETRY_FROM_CHECKPOINT, so the live dead-letter-WRITE-failure branch
    // (ContinuousProjectionRunner.processLiveBatch: `if (!writeDeadLetter(...)) throw e;`) needs
    // its own coverage. When the dead-letter STORE WRITE itself fails on a LIVE poison batch, the
    // runner must NOT advance past the un-recorded range (which would silently, permanently drop it
    // from the read model), must stay LIVE (never ERROR), and must retry until the store recovers —
    // then dead-letter EXACTLY once and advance by EXACTLY one.
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-dlq-fail-stream"));

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("permanent live failure"); // POISON
        };

    // Store stays DOWN until the test flips storeUp, so "offset did not advance" is observed
    // against a store that is reliably still failing, not one that already recovered.
    var storeUp = new java.util.concurrent.atomic.AtomicBoolean(false);
    var writeAttempts = new AtomicInteger(0);
    var written =
        new CopyOnWriteArrayList<org.streamrune.core.projection.ProjectionDeadLetterEntry>();
    var flakyDlqStore =
        new org.streamrune.core.projection.ProjectionDeadLetterStore() {
          @Override
          public void save(org.streamrune.core.projection.ProjectionDeadLetterEntry entry) {
            writeAttempts.incrementAndGet();
            if (!storeUp.get()) {
              throw new RuntimeException("DLQ store unavailable");
            }
            written.add(entry);
          }

          @Override
          public java.util.List<org.streamrune.core.projection.ProjectionDeadLetterEntry> read(
              org.streamrune.core.types.ProjectionName projectionName, int limit) {
            return java.util.List.copyOf(written);
          }

          @Override
          public java.util.List<org.streamrune.core.projection.ProjectionDeadLetterEntry> readAll(
              int limit) {
            return java.util.List.copyOf(written);
          }

          @Override
          public void discard(
              org.streamrune.core.types.ProjectionName projectionName,
              org.streamrune.core.types.GlobalOffset fromOffset) {}
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(flakyDlqStore)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .build();

    var proj = ProjectionName.of("live-dlq-fail");
    Thread t =
        Thread.ofVirtual().start(() -> runner.run(proj, alwaysFail, AT_LEAST_ONCE_IDEMPOTENT));

    // Drive to LIVE first (empty store → catch-up completes immediately), reaching the live branch.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // A POISON live event: the DLQ store rejects the write, so the runner must NOT advance and must
    // NOT go ERROR while the store is down. Observe non-advancement after ≥1 failed write attempt.
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());
    long attemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (writeAttempts.get() < 1 && System.nanoTime() < attemptDeadline) {
      Thread.sleep(10);
    }
    assertTrue(writeAttempts.get() >= 1, "the live dead-letter write should have been attempted");
    assertEquals(
        0,
        offsetStore.getLastOffset(proj).value(),
        "offset must NOT advance while the live dead-letter write is failing");
    assertNotEquals(ProjectionState.ERROR, runner.state());
    assertTrue(t.isAlive(), "runner must keep retrying the live batch, not die");

    // Recover the store: the next retry dead-letters the range EXACTLY once and advances by one.
    storeUp.set(true);
    long recoverDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (offsetStore.getLastOffset(proj).value() < 1 && System.nanoTime() < recoverDeadline) {
      Thread.sleep(20);
    }
    assertEquals(
        1,
        offsetStore.getLastOffset(proj).value(),
        "offset advances by exactly one only after the live dead-letter write succeeds");
    assertEquals(1, written.size(), "the poison range is dead-lettered exactly once, on recovery");
    assertNotEquals(ProjectionState.ERROR, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "Runner thread should have terminated after close");
  }

  @Test
  void liveDlqStrategy_transient_retriesInPlaceNeverAdvancesUntilRecovery() throws Exception {
    // LIVE-mode transient path under DLQ: a TRANSIENT infra blip (EventStoreException, classified
    // TRANSIENT by ProjectionErrorClassifier.DEFAULT) under the retry bound is rethrown so the
    // subscription retries the SAME batch in place WITHOUT advancing and WITHOUT dead-lettering;
    // the checkpoint advances only once the batch finally succeeds. Mirrors the catch-up transient
    // behavior for the structurally-different live advance mechanism.
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-transient-stream"));

    var failuresLeft = new AtomicInteger(3);
    var processed = new CopyOnWriteArrayList<String>();
    Projection flaky =
        events -> {
          if (failuresLeft.getAndDecrement() > 0) {
            throw new org.streamrune.core.EventStoreException("read-model blip"); // TRANSIENT
          }
          events.forEach(e -> processed.add(((TestEvent) e.event()).payload()));
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
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .build();

    var proj = ProjectionName.of("live-transient");
    Thread t = Thread.ofVirtual().start(() -> runner.run(proj, flaky, AT_LEAST_ONCE_IDEMPOTENT));

    // Drive to LIVE (empty store).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // Append the live event: it throws TRANSIENT 3 times (well under the 50-error bound), is
    // retried in place with backoff, then succeeds — so the offset advances only after recovery.
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (offsetStore.getLastOffset(proj).value() < 1 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    assertEquals(
        1,
        offsetStore.getLastOffset(proj).value(),
        "offset advances only after the transient failures recover");
    assertEquals(
        List.of("ok"),
        List.copyOf(processed),
        "the batch is applied exactly once, after the transient retries — never skipped");
    assertTrue(failuresLeft.get() <= 0, "all transient failures were exercised before recovery");
    assertTrue(
        dlqStore.read(proj, 10).isEmpty(),
        "a TRANSIENT blip under the retry bound must NEVER be dead-lettered");
    assertNotEquals(ProjectionState.ERROR, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "Runner thread should have terminated after close");
  }

  @Test
  void liveSubscriptionDeath_surfacesAsErrorInsteadOfReportingLiveForever() throws Exception {
    var subscriptionRunning = new java.util.concurrent.atomic.AtomicBoolean(true);
    ContinuousProjectionRunner.SubscriptionFactory factory =
        (name, listener, readPoisonBound) ->
            new org.streamrune.core.subscription.EventSubscription() {
              @Override
              public void start() {}

              @Override
              public boolean isRunning() {
                return subscriptionRunning.get();
              }

              @Override
              public void close() {
                subscriptionRunning.set(false);
              }
            };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .subscriptionFactory(factory)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(ProjectionName.of("dead-sub"), projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException _) {
                    // dead subscription is surfaced as an exception
                  }
                });

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // The underlying subscription dies silently — the runner must notice and report ERROR.
    subscriptionRunning.set(false);

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.ERROR && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(ProjectionState.ERROR, runner.state());
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
  }

  @Test
  void factorySubscriptionReadPoison_surfacesAsReadPoisonHalt() throws Exception {
    // A live read-poison on the factory/Hybrid path must HALT with the distinct
    // read-poison ERROR. The runner must recognize ANY ReadPoisonAware live subscription — not only
    // a PollingEventSubscription — otherwise a HybridEventSubscription's live read-poison is
    // misreported as a generic "stopped unexpectedly" instead of the actionable read-poison HALT.
    var subscriptionRunning = new java.util.concurrent.atomic.AtomicBoolean(true);
    var poison =
        new ProjectionReadPoisonException("read-poison-proj", 5, new RuntimeException("bad event"));
    ContinuousProjectionRunner.SubscriptionFactory factory =
        (name, listener, readPoisonBound) ->
            new ReadPoisonAwareTestSubscription(subscriptionRunning, poison);

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .subscriptionFactory(factory)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(
                        ProjectionName.of("read-poison-proj"),
                        projection,
                        AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException _) {
                    // a read-poison HALT is surfaced as an exception
                  }
                });

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // The live subscription stops itself terminally on a deterministic read-poison.
    subscriptionRunning.set(false);

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.ERROR && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(ProjectionState.ERROR, runner.state());
    assertEquals(
        poison.getMessage(),
        runner.lastError(),
        "a factory/Hybrid live read-poison must surface as the distinct read-poison HALT, not a"
            + " generic subscription-died error");
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive());
  }

  /**
   * A factory live subscription that surfaces a terminal read-poison via {@link ReadPoisonAware}.
   */
  private static final class ReadPoisonAwareTestSubscription
      implements org.streamrune.core.subscription.EventSubscription, ReadPoisonAware {
    private final java.util.concurrent.atomic.AtomicBoolean running;
    private final ProjectionReadPoisonException poison;

    ReadPoisonAwareTestSubscription(
        java.util.concurrent.atomic.AtomicBoolean running, ProjectionReadPoisonException poison) {
      this.running = running;
      this.poison = poison;
    }

    @Override
    public void start() {}

    @Override
    public boolean isRunning() {
      return running.get();
    }

    @Override
    public void close() {
      running.set(false);
    }

    @Override
    public ProjectionReadPoisonException readPoisonError() {
      // Like PollingEventSubscription, the terminal poison is only surfaced once stopped.
      return running.get() ? null : poison;
    }
  }

  @Test
  void catchUp_retriesTransientStoreReadFailures() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("flaky-read"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var failuresLeft = new AtomicInteger(2);
    EventStore flakyStore =
        new EventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            if (failuresLeft.getAndDecrement() > 0) {
              throw new RuntimeException("connection refused");
            }
            return eventStore.readGlobalStream(afterOffset, maxCount);
          }

          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, org.streamrune.core.AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(flakyStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("flaky-read-proj"),
                        projection,
                        AT_LEAST_ONCE_IDEMPOTENT));

    // Catch-up must retry the two read failures (1s + ~2s backoff) and then deliver the event.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (projection.processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(1, projection.processed.size(), "event must be delivered after read recovery");
    assertNotEquals(ProjectionState.ERROR, runner.state());

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void catchUp_readPoison_haltsTerminallyInsteadOfRetryingForever() throws Exception {
    // A read/deserialization poison (here an unregistered event type) fails identically
    // on every read. The buggy runner classified it as a transient store error and retried FOREVER
    // with backoff, wedging the projection with no progress and never reaching the DLQ/error
    // strategy (the failure is before process() runs). The fixed runner bounds the poison reads and
    // HALTs terminally with a distinct signal. A plain transient read still retries forever —
    // proven by catchUp_retriesTransientStoreReadFailures.
    var poisonStore =
        new ReadThrowingEventStore(
            () ->
                new org.streamrune.core.UnknownEventTypeException(
                    "event type", "RemovedEvent", List.of("KnownEvent")));

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(poisonStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            // Fast, small bounds so the terminal HALT is reached in milliseconds, not seconds.
            .readRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .maxReadPoisonRetries(3)
            .build();

    var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(
                        ProjectionName.of("read-poison"), projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException e) {
                    thrown.set(e);
                  }
                });

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.ERROR && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(
        ProjectionState.ERROR,
        runner.state(),
        "a deterministic read poison must HALT, not retry forever");
    assertNotNull(runner.lastError());
    assertTrue(
        runner.lastError().contains("read/deserialization-level poison"),
        "lastError must carry the distinct read-poison signal: " + runner.lastError());

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "the run thread must terminate, not spin on the poison read");
    assertInstanceOf(ProjectionReadPoisonException.class, thrown.get());
    // Bounded: the poison read was retried a bounded number of times, not indefinitely.
    assertEquals(3, poisonStore.readAttempts());

    runner.close();
  }

  @Test
  void live_readPoison_haltsTerminallyAndHealthGoesDownInsteadOfSpinningForever() throws Exception {
    // A projection catches up, goes LIVE, then a poison event (here an
    // unregistered event type) is appended to the global stream. In LIVE mode the default reader is
    // PollingEventSubscription, whose ResilientPollLoop treated EVERY read failure as transient and
    // retried FOREVER — offset frozen, nothing dead-lettered, state LIVE, lastError null, health
    // UP. The fix applies the catch-up bound to the live read path too (parity with catchUp):
    // after maxReadPoisonRetries consecutive deterministic poison reads the projection HALTs
    // terminally (ERROR) with a read-poison lastError and health goes DOWN, within a bounded number
    // of reads.
    var reads = new AtomicInteger();
    var poison = new java.util.concurrent.atomic.AtomicBoolean(false);
    EventStore poisonStore =
        new EventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            if (poison.get()) {
              reads.incrementAndGet();
              throw new org.streamrune.core.UnknownEventTypeException(
                  "event type", "RemovedEvent", List.of("KnownEvent"));
            }
            return List.of(); // empty during catch-up so the runner reaches LIVE
          }

          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, org.streamrune.core.AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    var health = new SubscriptionHealthContributor(poisonStore, offsetStore);
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(poisonStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .maxReadPoisonRetries(3)
            .healthContributor(health)
            .build();

    var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(
                        ProjectionName.of("live-read-poison"),
                        projection,
                        AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException e) {
                    thrown.set(e);
                  }
                });

    // Drive to LIVE (empty store).
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());

    // Append a poison event: every subsequent LIVE read throws UnknownEventTypeException.
    poison.set(true);

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.ERROR && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(
        ProjectionState.ERROR,
        runner.state(),
        "a LIVE deterministic read poison must HALT terminally, not retry forever");
    assertNotNull(runner.lastError());
    assertTrue(
        runner.lastError().contains("read/deserialization-level poison"),
        "lastError must carry the distinct read-poison signal: " + runner.lastError());

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "the run thread must terminate, not spin on the poison read");
    assertInstanceOf(ProjectionReadPoisonException.class, thrown.get());
    assertEquals(
        org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN,
        health.overallStatus(),
        "a wedged LIVE projection must report health DOWN, not UP");
    // Bounded: the poison read was retried a bounded number of times, not indefinitely.
    assertEquals(3, reads.get(), "the live poison read must be bounded by maxReadPoisonRetries");

    runner.close();
  }

  @Test
  void dlqStrategy_rejectsNullStore() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ContinuousProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .errorStrategy(ProjectionErrorStrategy.DLQ)
                .build());
  }

  @Test
  void state_returnsPendingBeforeRun() {
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .build();

    assertEquals(ProjectionState.PENDING, runner.state());
  }

  @Test
  void state_returnsStoppedAfterClose() throws Exception {
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("state-test"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    // A close() that lands before run() has started returns early (no run thread yet), and run()
    // then starts and never stops. Wait until the runner is LIVE so close() has a run to stop.
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> runner.state() == ProjectionState.LIVE);
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }

    assertEquals(ProjectionState.STOPPED, runner.state());
  }

  // ── A stop that arrives before run() has claimed the runner is honoured ──────────────────────
  //
  // MultiProjectionRunner.start() and StreamRune.startProjections() hand run() to a fresh thread
  // and return; a stop() / close() issued right after can reach the runner before that thread has
  // won run()'s start CAS. Both stop calls only cleared the `running` flag, which the CAS then set
  // straight back to true, so the stop was lost: run() started, processed, went LIVE and never
  // returned, the owner's join timed out, and the projection kept reading through a DataSource its
  // owner was about to close. Ordering a stop strictly before run() is the deterministic extreme of
  // that race (the thread scheduling between the two calls cannot matter to the outcome).

  private void assertStopBeforeRunIsHonoured(
      java.util.function.Consumer<ContinuousProjectionRunner> stop, String name) throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of(name));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();
    var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();

    stop.accept(runner);
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(ProjectionName.of(name), projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    thrown.set(e);
                  }
                });
    try {
      t.join(TimeUnit.SECONDS.toMillis(3));
      assertFalse(t.isAlive(), "run() must return at once: it was told to stop before it started");
      assertNull(thrown.get(), "a cancelled run() returns normally");
      assertTrue(projection.processed.isEmpty(), "nothing may be processed after the stop");
      assertEquals(
          GlobalOffset.initial(),
          offsetStore.getLastOffset(ProjectionName.of(name)),
          "the checkpoint must not move after the stop");
      assertEquals(ProjectionState.STOPPED, runner.state());
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  @Test
  void close_beforeRunWinsItsStartCas_isHonoured() throws Exception {
    assertStopBeforeRunIsHonoured(ContinuousProjectionRunner::close, "close-before-run");
  }

  @Test
  void requestStop_beforeRunWinsItsStartCas_isHonoured() throws Exception {
    // MultiProjectionRunner.stop() signals its runners with requestStop(), not close().
    assertStopBeforeRunIsHonoured(
        ContinuousProjectionRunner::requestStop, "request-stop-before-run");
  }

  @Test
  void requestStop_duringStartup_isHonoured() throws Exception {
    // A stop that lands after the start CAS but before the processing loop (here: while run() is
    // registering its health view) must also hold. The registration is parked on a latch so the
    // stop deterministically lands inside that window.
    var inStartup = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var metrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void registerSubscriptionLagGauge(
              org.streamrune.core.types.SubscriptionName subscriptionName,
              java.util.function.LongSupplier lagSupplier) {
            inStartup.countDown();
            try {
              release.await();
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          }
        };
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, metrics);
    StreamId s = StreamId.of(TYPE, AggregateId.of("stop-during-startup"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .healthContributor(health)
            .build();
    var name = ProjectionName.of("stop_during_startup");

    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      assertTrue(inStartup.await(5, TimeUnit.SECONDS), "run() must reach its startup window");
      runner.requestStop();
      release.countDown();
      t.join(TimeUnit.SECONDS.toMillis(3));
      assertFalse(t.isAlive(), "run() must return: the stop landed during its startup");
      assertTrue(projection.processed.isEmpty(), "nothing may be processed after the stop");
      assertEquals(ProjectionState.STOPPED, runner.state());
    } finally {
      release.countDown();
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  @Test
  void aHeldStopsRun_keepsThePreviousRunsError() {
    // The run that consumes a held stop processes nothing, so it must not wipe the failure of the
    // run before it: state() still reports that run's ERROR, and lastError() must still say why.
    StreamId s = StreamId.of(TYPE, AggregateId.of("held-stop-keeps-error"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());
    Projection failing =
        events -> {
          throw new AssertionError("projection Error");
        };
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();
    var name = ProjectionName.of("held_stop_keeps_error");
    assertThrows(AssertionError.class, () -> runner.run(name, failing, AT_LEAST_ONCE_IDEMPOTENT));
    assertEquals(ProjectionState.ERROR, runner.state());
    String error = runner.lastError();
    assertNotNull(error);

    runner.requestStop(); // the runner is idle: the stop is held for the next run()
    runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT); // consumes it and returns at once

    assertTrue(projection.processed.isEmpty(), "the held stop's run processes nothing");
    assertEquals(ProjectionState.ERROR, runner.state());
    assertEquals(error, runner.lastError(), "the previous run's failure must stay visible");
  }

  @Test
  void discardHeldStop_letsTheNextRunProcess() throws Exception {
    // StreamRune.startProjections() drops a stop that a stopProjections() left held on an idle
    // runner (its run had ended on its own, or had never started), so the restart processes.
    StreamId s = StreamId.of(TYPE, AggregateId.of("discard-held-stop"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();
    var name = ProjectionName.of("discard_held_stop");

    runner.close(); // idle: held
    runner.discardHeldStop();
    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !projection.processed.isEmpty());
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
    assertFalse(t.isAlive());
    assertEquals(ProjectionState.STOPPED, runner.state());
  }

  @Test
  void aStopThatEndedARun_doesNotCancelTheNextRun() throws Exception {
    // The stop request is remembered only until a run consumes it: a close() that stopped a live
    // run must not linger and swallow a later, deliberate restart of the same runner.
    var name = ProjectionName.of("restart-after-stop");
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    Thread first =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> runner.state() == ProjectionState.LIVE);
    } finally {
      runner.close();
      first.join(TimeUnit.SECONDS.toMillis(3));
    }
    assertFalse(first.isAlive());
    assertEquals(ProjectionState.STOPPED, runner.state());

    StreamId s = StreamId.of(TYPE, AggregateId.of("restart-after-stop"));
    eventStore.append(s, List.of(envelope(s, 1, "after-restart")), Version.initial());
    Thread second =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      // The restarted run catches up on the new event and goes LIVE.
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> projection.processed.size() == 1 && runner.state() == ProjectionState.LIVE);
    } finally {
      runner.close();
      second.join(TimeUnit.SECONDS.toMillis(3));
    }
    assertFalse(second.isAlive());
  }

  @Test
  void aHeldStop_isConsumedByTheOneRunItCancels() throws Exception {
    // A stop held from before run() cancels exactly that run(): the next run() of the same runner
    // processes normally rather than also being swallowed.
    var name = ProjectionName.of("held-stop-consumed-once");
    StreamId s = StreamId.of(TYPE, AggregateId.of("held-stop-consumed-once"));
    eventStore.append(s, List.of(envelope(s, 1, "e1")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .build();

    runner.requestStop();
    Thread cancelled =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    cancelled.join(TimeUnit.SECONDS.toMillis(3));
    assertFalse(cancelled.isAlive(), "the held stop cancels this run()");
    assertTrue(projection.processed.isEmpty());

    Thread next =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> projection.processed.size() == 1 && runner.state() == ProjectionState.LIVE);
    } finally {
      runner.close();
      next.join(TimeUnit.SECONDS.toMillis(3));
    }
    assertFalse(next.isAlive());
  }

  /**
   * A metrics registry that throws while registering the subscription lag gauge must not be able to
   * kill the runner before its try/finally bookkeeping exists.
   *
   * <p>Before the fix, {@code registerStandbyHealthView} (the standby health view) ran AFTER {@code
   * running}/{@code runDone}/{@code runThread} were set but BEFORE run()'s {@code try}, and {@link
   * SubscriptionHealthContributor#register} called {@code metrics.registerSubscriptionLagGauge}
   * bare — the only unwrapped metrics call in the projection stack (a Prometheus registry rejects a
   * meter whose id collides under different label keys, a user MeterFilter can throw, a registry
   * mid-shutdown throws IllegalStateException). The throw escaped run() without touching the
   * finally: {@code running} stayed true (every later run() → "Runner already running"), the {@code
   * runDone} latch was never counted down (so close() blocked its full 30s CLOSE_JOIN_TIMEOUT), and
   * because the metrics call precedes {@code subscriptions.put}, NO health entry existed and
   * markTerminalError never ran — /health reported UP while the projection processed nothing.
   *
   * <p>After the fix the gauge failure is logged and swallowed, so the health entry is published
   * anyway (health tracking degrades to "no lag meter", never to "no entry") and the runner reaches
   * its normal loop; close() returns promptly and the runner ends STOPPED.
   */
  @Test
  void throwingLagGaugeRegistration_doesNotWedgeTheRunner() throws Exception {
    var gaugeAttempts = new AtomicInteger();
    var metrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void registerSubscriptionLagGauge(
              org.streamrune.core.types.SubscriptionName subscriptionName,
              java.util.function.LongSupplier lagSupplier) {
            gaugeAttempts.incrementAndGet();
            throw new IllegalStateException("meter registry rejected streamrune.subscriptions.lag");
          }
        };
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, metrics);
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .healthContributor(health)
            .build();

    var name = ProjectionName.of("gauge-throws");
    var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    thrown.set(e);
                  }
                });

    // The runner must still reach its processing loop despite the failing registry.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(
        ProjectionState.LIVE,
        runner.state(),
        "a throwing lag-gauge registration must not prevent the runner from starting");
    assertTrue(gaugeAttempts.get() > 0, "the gauge registration must actually have been attempted");

    // The health entry exists even though the gauge could not be registered — the whole point of
    // the standby view is that a frozen projection is never invisible to /health.
    assertTrue(
        health.health().stream().anyMatch(h -> h.name().equals(name.value())),
        "the health entry must be published even when the lag gauge cannot be registered");

    // close() must not block on a latch that was never counted down.
    long closeStart = System.nanoTime();
    runner.close();
    long closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart);
    assertTrue(
        closeMillis < 10_000,
        "close() must return promptly, not block the full CLOSE_JOIN_TIMEOUT; took "
            + closeMillis
            + "ms");

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "the run thread must exit");
    assertNull(thrown.get(), "run() must not die on a metrics-registry failure: " + thrown.get());
    assertEquals(ProjectionState.STOPPED, runner.state());
    assertTrue(
        health.health().stream().noneMatch(h -> h.name().equals(name.value())),
        "a clean stop still unregisters, so the finally bookkeeping ran");
  }

  /**
   * The {@code catch (RuntimeException)} added to {@link SubscriptionHealthContributor#register}
   * deliberately matches the projection stack's metrics convention and therefore does NOT swallow
   * an {@link Error}. Layer (b) — moving {@code registerStandbyHealthView} inside run()'s {@code
   * try} — is what makes that residual case safe: the throw still propagates to the caller, but it
   * now flows through run()'s catch(Throwable)/finally, so state is ERROR, the projection is marked
   * terminally DOWN, the runDone latch is counted down (close() returns promptly) and {@code
   * running} is reset (the runner is not permanently "already running"). Before the fix this path
   * skipped all of it.
   */
  @Test
  void erroringLagGaugeRegistration_stillRunsRunFinallyBookkeeping() throws Exception {
    var metrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void registerSubscriptionLagGauge(
              org.streamrune.core.types.SubscriptionName subscriptionName,
              java.util.function.LongSupplier lagSupplier) {
            throw new AssertionError("registry blew up with an Error, not a RuntimeException");
          }
        };
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, metrics);
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .healthContributor(health)
            .build();

    var name = ProjectionName.of("health-registration-throws");
    var thrown = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    thrown.set(e);
                  }
                });

    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "the run thread must exit");
    assertNotNull(thrown.get(), "the throw still propagates — it is not swallowed");

    // The finally ran: state is ERROR (not left at the initial value), the projection is marked
    // terminally DOWN, and close() does not block because the latch was counted down.
    assertEquals(ProjectionState.ERROR, runner.state());
    long closeStart = System.nanoTime();
    runner.close();
    assertTrue(
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart) < 10_000,
        "close() must not block on an uncounted runDone latch");

    // running was reset, so the runner is restartable rather than permanently "already running".
    var second = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t2 =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    second.set(e);
                  }
                });
    t2.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(
        second.get() instanceof IllegalStateException ise
            && ise.getMessage() != null
            && ise.getMessage().contains("Runner already running"),
        "running must have been reset by the finally, so the runner is not wedged");
  }

  // Test helper classes
  static class TestProjection implements Projection {
    final List<EventEnvelope> processed = new CopyOnWriteArrayList<>();

    @Override
    public void process(List<EventEnvelope> events) {
      processed.addAll(events);
    }
  }

  // InMemoryOffsetStore lives in its own top-level test class (shared by the runtime tests).

  // ── A dead-lettered or skipped batch must not read as health SUCCESS, and
  // the dead-letter backlog gauge must be live during catch-up ────────────────────────────────────

  private static int errorCountOf(SubscriptionHealthContributor health, ProjectionName name) {
    return health.health().stream()
        .filter(h -> h.name().equals(name.value()))
        .findFirst()
        .map(SubscriptionHealth::errorCount)
        .orElse(-1);
  }

  private static SubscriptionHealth.Status statusOf(
      SubscriptionHealthContributor health, ProjectionName name) {
    return health.health().stream()
        .filter(h -> h.name().equals(name.value()))
        .findFirst()
        .map(SubscriptionHealth::status)
        .orElse(null);
  }

  private static void awaitLive(ContinuousProjectionRunner runner) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (runner.state() != ProjectionState.LIVE && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(ProjectionState.LIVE, runner.state());
  }

  /** Polls {@code condition} until it holds or the deadline passes; returns whether it held. */
  private static boolean awaitCondition(java.util.function.BooleanSupplier condition, long seconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    return condition.getAsBoolean();
  }

  /** Fails only on the payload {@code "poison"}; every other batch is applied cleanly. */
  private static Projection poisonPayloadOnly() {
    return events -> {
      if (((TestEvent) events.get(0).event()).payload().equals("poison")) {
        throw new RuntimeException("permanent poison"); // POISON under DLQ; a failure under SKIP
      }
    };
  }

  @Test
  void liveDlq_deadLetteredBatchRecordsAHealthError_untilACleanBatchFollows() throws Exception {
    // processLiveBatch's DLQ arm returns normally so the subscription advances past the
    // dead-lettered range — and the HealthTrackingEventListener wrapper then recorded that normal
    // return as a SUCCESS, resetting the error count. The projection therefore stayed health-UP
    // while its checkpoint moved past events that were never applied; with the default NOOP
    // metrics the backlog gauge was silent too, so there was no signal at all. Sibling
    // ScheduledProjectionRunner records a health error BEFORE its strategy runs.
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, 1_000);
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var name = ProjectionName.of("live-dlq-health");
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
            .healthContributor(health)
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(name, poisonPayloadOnly(), AT_LEAST_ONCE_IDEMPOTENT));
    try {
      awaitLive(runner);
      StreamId poison = StreamId.of(TYPE, AggregateId.of("live-dlq-health-poison"));
      eventStore.append(poison, List.of(envelope(poison, 1, "poison")), Version.initial());
      assertTrue(
          awaitCondition(() -> !dlqStore.read(name, 10).isEmpty(), 5),
          "the poison live batch is dead-lettered");

      // No clean batch has followed, so the dead-letter's health error must still stand.
      assertTrue(
          awaitCondition(() -> errorCountOf(health, name) > 0, 3),
          "a dead-lettered live batch must be recorded as a health ERROR, not masked as a success");
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          statusOf(health, name),
          "one dead-lettered batch reads DEGRADED, not UP");

      // A genuinely clean batch resets the count: health-UP is kept for clean processing.
      StreamId clean = StreamId.of(TYPE, AggregateId.of("live-dlq-health-clean"));
      eventStore.append(clean, List.of(envelope(clean, 1, "clean")), Version.initial());
      assertTrue(
          awaitCondition(() -> errorCountOf(health, name) == 0, 5),
          "a clean live batch after the dead-letter resets the error count");
      assertEquals(SubscriptionHealth.Status.UP, statusOf(health, name));
      assertNotEquals(ProjectionState.ERROR, runner.state());
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  void liveSkip_skippedBatchRecordsAHealthError() throws Exception {
    // Identical masking — SKIP returned normally, the wrapper recorded a
    // success, and a projection silently skipping batches reported UP.
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, 1_000);
    var name = ProjectionName.of("live-skip-health");
    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(fastConfig())
            .healthContributor(health)
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(name, poisonPayloadOnly(), AT_LEAST_ONCE_IDEMPOTENT));
    try {
      awaitLive(runner);
      StreamId poison = StreamId.of(TYPE, AggregateId.of("live-skip-health-poison"));
      eventStore.append(poison, List.of(envelope(poison, 1, "poison")), Version.initial());
      assertTrue(
          awaitCondition(() -> offsetStore.getLastOffset(name).value() >= 1, 5),
          "the checkpoint advances past the skipped batch");
      assertTrue(
          awaitCondition(() -> errorCountOf(health, name) > 0, 3),
          "a skipped live batch must be recorded as a health ERROR, not masked as a success");
      assertEquals(SubscriptionHealth.Status.DEGRADED, statusOf(health, name));
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  /**
   * Catch-up harness: one page of two single-event chunks — the FIRST is poison (dead-lettered
   * under DLQ), the SECOND blocks on a latch — so the runner is held INSIDE catch-up right after
   * the dead-letter, with its health/metrics state observable, then released to finish cleanly.
   */
  private record CatchUpHarness(
      ContinuousProjectionRunner runner,
      Thread thread,
      InMemoryProjectionDeadLetterStore dlqStore,
      CountDownLatch release,
      ProjectionName name) {

    void finish() throws InterruptedException {
      release.countDown();
      runner.close();
      thread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  private CatchUpHarness startPoisonThenBlockingCatchUp(
      SubscriptionHealthContributor health, RecordingStreamRuneMetrics metrics)
      throws InterruptedException {
    StreamId s = StreamId.of(TYPE, AggregateId.of("catchup-harness"));
    eventStore.append(
        s, List.of(envelope(s, 1, "poison"), envelope(s, 2, "block")), Version.initial());
    var release = new CountDownLatch(1);
    Projection projection =
        events -> {
          String payload = ((TestEvent) events.get(0).event()).payload();
          if (payload.equals("poison")) {
            throw new RuntimeException("permanent poison"); // POISON → dead-lettered
          }
          if (payload.equals("block")) {
            try {
              // finish() always releases it; the bound only keeps a broken test from hanging, so
              // it sits far above every wait a test makes while the runner is held here.
              release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          }
        };
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var name = ProjectionName.of("catchup-harness");
    var builder =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(1)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig());
    if (health != null) {
      builder.healthContributor(health);
    }
    if (metrics != null) {
      builder.metrics(metrics);
    }
    var runner = builder.build();
    Thread t =
        Thread.ofVirtual().start(() -> runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(
        awaitCondition(() -> !dlqStore.read(name, 10).isEmpty(), 5),
        "the poison first chunk is dead-lettered during catch-up");
    assertEquals(
        ProjectionState.CATCHING_UP,
        runner.state(),
        "the runner is held inside catch-up by the blocking second chunk");
    return new CatchUpHarness(runner, t, dlqStore, release, name);
  }

  @Test
  void catchUpDlq_deadLetteredChunkRecordsAHealthError_untilACleanChunkFollows() throws Exception {
    // catchUp() never touched the health contributor at all — a
    // dead-lettered or skipped chunk left the standby health view's error count at 0 (health UP)
    // while the checkpoint advanced past it. ScheduledProjectionRunner records recordHealthError
    // before its strategy and recordHealthSuccess after a clean chunk; mirror it.
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, 1_000);
    var harness = startPoisonThenBlockingCatchUp(health, null);
    try {
      assertTrue(
          awaitCondition(() -> errorCountOf(health, harness.name()) > 0, 3),
          "a dead-lettered catch-up chunk must be recorded as a health ERROR");
      assertEquals(SubscriptionHealth.Status.DEGRADED, statusOf(health, harness.name()));
      harness.release().countDown(); // the blocked second chunk now completes cleanly
      assertTrue(
          awaitCondition(() -> errorCountOf(health, harness.name()) == 0, 5),
          "a clean chunk after the dead-letter resets the error count");
      assertNotEquals(ProjectionState.ERROR, harness.runner().state());
    } finally {
      harness.finish();
    }
  }

  @Test
  void catchUpDlq_deadLetteredFinalChunk_degradedPersistsIntoLive() throws Exception {
    // catchUpDlq_deadLetteredChunkRecordsAHealthError_untilACleanChunkFollows cannot see
    // this — its second (blocking) chunk completes CLEANLY before catch-up ends, which resets the
    // error count via recordHealthSuccess before the catch-up->live transition ever runs. Here the
    // dead-lettered chunk is the LAST thing catch-up ever processes: nothing clean follows it
    // before the runner goes LIVE. registerHealth's catch-up->live swap used to call
    // SubscriptionHealthContributor.register(), which always starts a fresh entry (errorCount=0,
    // UP) — silently discarding the catch-up error the instant the runner reports itself LIVE,
    // even though no clean batch ever ran. A poison in the very last catch-up page followed by a
    // quiet live stream then read UP over a read model with a hole (the original symptom
    // reopened through the catch-up->live health-view swap).
    var health = new SubscriptionHealthContributor(eventStore, offsetStore, 1_000);
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var name = ProjectionName.of("catchup-final-dlq-health");
    StreamId poison = StreamId.of(TYPE, AggregateId.of("catchup-final-dlq-health-poison"));
    eventStore.append(poison, List.of(envelope(poison, 1, "poison")), Version.initial());
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
            .healthContributor(health)
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(name, poisonPayloadOnly(), AT_LEAST_ONCE_IDEMPOTENT));
    try {
      assertTrue(
          awaitCondition(() -> !dlqStore.read(name, 10).isEmpty(), 5),
          "the poison chunk is dead-lettered during catch-up");
      awaitLive(runner);
      assertTrue(
          errorCountOf(health, name) > 0,
          "the catch-up dead-letter error must survive the catch-up->live health-view swap");
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          statusOf(health, name),
          "DEGRADED must persist once state()==LIVE — nothing clean ran between the dead-letter"
              + " and going live to legitimately earn UP");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  void catchUpDlq_samplesTheDeadLetterBacklogGauge_beforeGoingLive() throws Exception {
    // sampleDeadLetterBacklog() had a single call site — the top of the LIVE wait loop —
    // so through the whole catch-up phase (exactly where a mass dead-lettering happens after a bad
    // deploy) streamrune.projections.dead_letter_backlog was never sampled: it read 0/absent until
    // the runner went live. The runner is held inside catch-up here, so the sample must have been
    // taken by the catch-up path itself.
    //
    // Wait for the sample that reads the dead-lettered range, not for "any sample": the runner
    // also samples on entering CATCHING_UP, before the poison chunk, and that sample reads 0. The
    // dead-letter becomes visible in the store a moment before the runner samples it, so the
    // count of samples alone says nothing about whether the post-write sample has happened yet.
    var metrics = new RecordingStreamRuneMetrics();
    var harness = startPoisonThenBlockingCatchUp(null, metrics);
    try {
      assertTrue(
          awaitCondition(() -> metrics.lastProjectionDeadLetterBacklog() == 1, 5),
          "the dead-letter backlog gauge must be sampled during catch-up, right after a"
              + " dead-letter write, and reflect the one dead-lettered range — not only once the"
              + " runner is live; last sample: "
              + metrics.lastProjectionDeadLetterBacklog());
      // Read after the sample was seen: the runner only leaves CATCHING_UP forwards (to LIVE), so
      // still catching up now means the sample above was taken by the catch-up path.
      assertEquals(ProjectionState.CATCHING_UP, harness.runner().state(), "still catching up");
    } finally {
      harness.finish();
    }
  }
}

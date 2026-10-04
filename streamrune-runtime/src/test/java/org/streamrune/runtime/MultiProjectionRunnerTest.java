package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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
import org.streamrune.core.projection.ProjectionRepository;
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
import org.streamrune.test.InMemoryProjectionRepository;

class MultiProjectionRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("multi");

  private EventStore eventStore;
  private OffsetStore offsetStore;

  @BeforeEach
  void setUp() {
    eventStore = new InMemoryEventStore();
    offsetStore = new InMemoryOffsetStore();
  }

  record TestEvent(String payload) implements DomainEvent {}

  private static EventMetadata metadata() {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("corr"),
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
        metadata());
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10));
  }

  @Test
  void start_launches_all_projections() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("multi-1"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var processed1 = new CopyOnWriteArrayList<EventEnvelope>();
    var processed2 = new CopyOnWriteArrayList<EventEnvelope>();

    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("Proj1", events -> processed1.addAll(events), AT_LEAST_ONCE_IDEMPOTENT)
            .register("Proj2", events -> processed2.addAll(events), AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while ((processed1.isEmpty() || processed2.isEmpty()) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }

    runner.stop();

    assertFalse(processed1.isEmpty(), "Proj1 should have processed events");
    assertFalse(processed2.isEmpty(), "Proj2 should have processed events");
  }

  @Test
  void status_returns_state_per_projection() throws Exception {
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("A", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .register("B", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    Thread.sleep(100);

    var status = runner.status();
    assertEquals(2, status.size());
    assertNotNull(status.get("A"));
    assertNotNull(status.get("B"));

    runner.stop();
  }

  @Test
  void stop_shuts_down_gracefully() throws Exception {
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("StopTest", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    Thread.sleep(50);
    runner.stop();

    var status = runner.status();
    var state = status.get("StopTest").state();
    assertTrue(state == ProjectionState.STOPPED || state == ProjectionState.ERROR);
  }

  @Test
  void builder_rejects_duplicate_names() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MultiProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .register("Dup", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                .register("Dup", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                .build());
  }

  /**
   * build() asks the processor about every registered name, so a name it cannot store read models
   * under (the JDBC repository: outside {@code [a-z_][a-z0-9_]{0,57}}) fails at startup instead of
   * on the projection's first batch. With valid names, the exact-name duplicate check above is also
   * the check that no two registrations share a read-model table.
   */
  @Test
  void builder_refusesANameTheProcessorCannotStore_atBuild() {
    var processor = new NameRuleProcessor();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(eventStore)
                    .offsetStore(offsetStore)
                    .atomicProcessor(processor)
                    .register("orders", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                    .register("order-summary", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                    .build());
    assertTrue(ex.getMessage().contains("order-summary"), ex.getMessage());
    assertEquals(
        List.of(ProjectionName.of("orders"), ProjectionName.of("order-summary")),
        processor.checked);
    assertEquals(0, processor.commits.get());
  }

  @Test
  void builder_rejects_dlq_strategy_without_store() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MultiProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .register(
                    "DLQ", events -> {}, ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
                .build());
  }

  // A fencing-claiming processor that hands null to the updater: a plain projection declared
  // AT_LEAST_ONCE_IDEMPOTENT runs under it; a write-through projection declared TRANSACTIONAL_LOCAL
  // is accepted as declared (it exposes no write target). Every registration shares this runner's
  // single atomicProcessor, so build() checks each one against its own declared mode.
  private static AtomicBatchProcessor transactionalProcessor() {
    return new AtomicBatchProcessor() {
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
  }

  /**
   * {@code build()} refuses a {@code TRANSACTIONAL_LOCAL} lambda under an in-memory transactional
   * repository; the same lambda declared {@code AT_LEAST_ONCE_IDEMPOTENT} builds.
   */
  @Test
  void build_refusesATransactionalLocalLambda_andBuildsTheSameLambdaAtLeastOnce() {
    Projection lambda = events -> {};
    var repository = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(eventStore)
                    .offsetStore(repository)
                    .atomicProcessor(repository)
                    .register("non-write-through", lambda, TRANSACTIONAL_LOCAL)
                    .build());
    assertTrue(ex.getMessage().contains("MultiProjectionRunner"), ex.getMessage());
    assertTrue(ex.getMessage().contains("'non-write-through'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("TRANSACTIONAL_LOCAL"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("does not write through the handed repository"), ex.getMessage());

    try (var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(repository)
            .atomicProcessor(repository)
            .register("non-write-through", lambda, AT_LEAST_ONCE_IDEMPOTENT)
            .build()) {
      assertNotNull(runner);
    }
  }

  @Test
  void buildRefusesAMissingProcessor_namingBothWaysOut() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(eventStore)
                    .offsetStore(offsetStore)
                    .register("orders", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                    .build());
    assertEquals(
        "atomicProcessor is required: pass the JdbcProjectionRepository your TRANSACTIONAL_LOCAL"
            + " projections write to, or AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
            + " AT_LEAST_ONCE_IDEMPOTENT projections",
        ex.getMessage());
  }

  @Test
  void build_acceptsAPlainProjectionDeclaredAtLeastOnce_underAFencingProcessor() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("at-least-once-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var processed = new CopyOnWriteArrayList<EventEnvelope>();
    Projection nonWriteThrough = processed::addAll;

    var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .atomicProcessor(transactionalProcessor())
            .register("at-least-once", nonWriteThrough, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (processed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.stop();
    assertEquals(1, processed.size());
  }

  /** Overrides the 2-arg overload, so {@code writesThroughRepository()} reports {@code true}. */
  private static final class WriteThroughProjection implements Projection {
    final CopyOnWriteArrayList<EventEnvelope> processed = new CopyOnWriteArrayList<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      processed.addAll(batch);
    }

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
      process(batch);
    }
  }

  // The mode is PER REGISTRATION: a plain at-least-once lambda and a transactional write-through
  // projection share one runner, each checked against its own declaration.
  @Test
  void build_checksEachRegistrationAgainstItsOwnMode() throws Exception {
    // A single shared event stream: MultiProjectionRunner drives every registration over the SAME
    // global log, so both registrations below observe the one appended event — this test is about
    // per-registration modes, not event routing.
    StreamId s = StreamId.of(TYPE, AggregateId.of("per-reg-shared-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var atLeastOnceProcessed = new CopyOnWriteArrayList<EventEnvelope>();
    Projection atLeastOnceLambda = atLeastOnceProcessed::addAll;
    var writeThrough = new WriteThroughProjection();

    var runner =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .atomicProcessor(transactionalProcessor())
            .register(
                "at-least-once",
                atLeastOnceLambda,
                ProjectionErrorStrategy.HALT,
                AT_LEAST_ONCE_IDEMPOTENT)
            .register("write-through", writeThrough, TRANSACTIONAL_LOCAL)
            .build();

    runner.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while ((atLeastOnceProcessed.isEmpty() || writeThrough.processed.isEmpty())
        && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.stop();

    assertEquals(1, atLeastOnceProcessed.size());
    assertEquals(1, writeThrough.processed.size());
  }

  // The flip side: a sibling's AT_LEAST_ONCE_IDEMPOTENT never loosens the check for a registration
  // that declares TRANSACTIONAL_LOCAL on the same runner.
  @Test
  void build_aSiblingsAtLeastOnceNeverLoosensATransactionalRegistrationsCheck() {
    Projection atLeastOnce = events -> {};
    Projection transactional = events -> {};
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                MultiProjectionRunner.builder()
                    .eventStore(eventStore)
                    .offsetStore(offsetStore)
                    .atomicProcessor(transactionalProcessor())
                    .register(
                        "at-least-once",
                        atLeastOnce,
                        ProjectionErrorStrategy.HALT,
                        AT_LEAST_ONCE_IDEMPOTENT)
                    .register("transactional", transactional, TRANSACTIONAL_LOCAL)
                    .build());
    assertTrue(ex.getMessage().contains("'transactional'"), ex.getMessage());
    assertFalse(ex.getMessage().contains("'at-least-once'"), ex.getMessage());
  }

  @Test
  void builder_rejects_empty_registrations() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MultiProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .build());
  }

  @Test
  void start_twice_throws() throws Exception {
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("Once", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    try {
      assertThrows(IllegalStateException.class, runner::start);
      // Stop only once the run thread is LIVE: a stop that lands before run() wins its `running`
      // CAS is lost, and stop() then waits out the whole stop timeout.
      awaitLive(runner, "Once");
    } finally {
      runner.stop();
    }
  }

  private static void awaitLive(MultiProjectionRunner runner, String name) {
    org.awaitility.Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.status().get(name).state() == ProjectionState.LIVE);
  }

  @Test
  void dlqStrategy_poison_deadLettersAndContinues_noErrorState() throws Exception {
    // Behavior: under DLQ a POISON batch is dead-lettered and the per-registration
    // runner CONTINUES (no ERROR state) — MultiProjectionRunner.status() reflects the non-error
    // state and the dead-letter store records the failed range for later replay.
    StreamId s = StreamId.of(TYPE, AggregateId.of("error-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "fail")), Version.initial());

    Projection alwaysFail =
        events -> {
          throw new RuntimeException("boom"); // POISON
        };

    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .deadLetterStore(dlqStore)
            .register("FailProj", alwaysFail, ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();

    // The poison range is dead-lettered; the runner never enters ERROR.
    org.awaitility.Awaitility.await()
        .atMost(java.time.Duration.ofSeconds(5))
        .until(() -> !dlqStore.read(ProjectionName.of("FailProj"), 10).isEmpty());

    assertEquals(1, dlqStore.read(ProjectionName.of("FailProj"), 10).size());
    assertNotEquals(ProjectionState.ERROR, runner.status().get("FailProj").state());

    runner.stop();
  }

  @Test
  void restart_after_clean_stop_works() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("restart-1"));
    eventStore.append(s, List.of(envelope(s, 1, "first")), Version.initial());

    var processed = new CopyOnWriteArrayList<EventEnvelope>();
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("Restartable", events -> processed.addAll(events), AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    awaitSize(processed, 1);
    runner.stop();

    // All threads exited within the timeout, so a restart is allowed and picks up new events.
    eventStore.append(s, List.of(envelope(s, 2, "second")), new Version(1));
    runner.start();
    try {
      awaitSize(processed, 2);
      assertTrue(
          processed.stream().anyMatch(e -> ((TestEvent) e.event()).payload().equals("second")),
          "restarted runner must process events appended after stop");
    } finally {
      runner.stop();
    }
  }

  @Test
  void stop_timeout_blocks_restart_while_projection_thread_is_stuck() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("stuck-1"));
    eventStore.append(s, List.of(envelope(s, 1, "a")), Version.initial());

    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    Projection stuck =
        events -> {
          entered.countDown();
          // Simulates a wedged projection: blocks and swallows interrupts until released.
          while (release.getCount() > 0) {
            try {
              release.await();
            } catch (InterruptedException _) {
              // keep blocking — deliberately interrupt-proof
            }
          }
        };

    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .stopTimeout(Duration.ofMillis(100))
            .register("Stuck", stuck, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    try {
      assertTrue(entered.await(3, TimeUnit.SECONDS), "projection should start processing");

      runner.stop(); // times out: the thread is stuck inside the projection

      assertThrows(
          IllegalStateException.class,
          runner::start,
          "restart must be refused while the old projection thread is still alive");
    } finally {
      release.countDown();
      runner.stop(); // thread can now exit; this stop completes and releases the started flag
    }
  }

  @Test
  void builder_rejects_non_positive_stop_timeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MultiProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .stopTimeout(Duration.ZERO)
                .register("T", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                .build());
  }

  @Test
  void builder_rejects_sub_millisecond_stop_timeout() {
    // A positive sub-ms value passed the null/negative/zero validation, then
    // truncated to stopTimeoutMs == 0 — and Thread.join(0) waits FOREVER, so the tightest
    // configurable stop bound became an unbounded shutdown hang on a stuck projection thread.
    // Same sentinel-inversion class as the lock-timeout band fixed earlier.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MultiProjectionRunner.builder()
                .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                .eventStore(eventStore)
                .offsetStore(offsetStore)
                .stopTimeout(Duration.ofNanos(500_000)) // 0.5ms -> toMillis() == 0
                .register("T", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
                .build());
  }

  private static void awaitSize(CopyOnWriteArrayList<EventEnvelope> list, int size)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (list.size() < size && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
  }

  @Test
  void close_delegates_to_stop() throws Exception {
    var runner =
        MultiProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(fastConfig())
            .register("Closeable", events -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    awaitLive(runner, "Closeable");
    runner.close(); // AutoCloseable

    // close() must do what stop() does: the projection is no longer running, and the started flag
    // is released so the runner can start again. A no-op close() fails both checks.
    var state = runner.status().get("Closeable").state();
    assertTrue(
        state == ProjectionState.STOPPED || state == ProjectionState.ERROR,
        "close() must stop the projection, got " + state);
    try {
      assertDoesNotThrow(runner::start, "close() must release the started flag like stop() does");
      // As in start_twice_throws: stop the restarted runner only once its run thread is LIVE.
      awaitLive(runner, "Closeable");
    } finally {
      runner.stop();
    }
  }
}

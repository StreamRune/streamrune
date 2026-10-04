package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * Delivery-mode behaviour pins across the four runners: what the updater is handed, that a
 * checkpoint-save failure is retried from the checkpoint and never reaches the error strategy, the
 * one INFO line per registration, and the delivery-mode gauge.
 */
class ProjectionDeliveryModeRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  record Ping(int n) implements DomainEvent {}

  /** Records what the two-arg process was handed; writes through it when non-null. */
  static final class HandedRecorder implements Projection {
    final CopyOnWriteArrayList<ProjectionRepository> handed = new CopyOnWriteArrayList<>();
    final AtomicInteger applications = new AtomicInteger();
    private final ProjectionName name;

    HandedRecorder(String name) {
      this.name = ProjectionName.of(name);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      applications.addAndGet(batch.size());
    }

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
      handed.add(repository == null ? NULL_MARKER : repository);
      if (repository != null) {
        repository.save(name, "row", "written-in-tx");
      }
      process(batch);
    }
  }

  static final ProjectionRepository NULL_MARKER = new InMemoryProjectionRepository();

  private static InMemoryEventStore storeWith(int events) {
    var store = new InMemoryEventStore();
    for (int i = 1; i <= events; i++) {
      appendPings(store, i, i);
    }
    return store;
  }

  /**
   * Appends Pings {@code from..to} to stream "s" in ONE append (versions from..to, expecting
   * from-1). One append is not one batch: {@link InMemoryEventStore} publishes the events to its
   * global stream one at a time and reads that stream without the append lock, so a live poll can
   * see a prefix. A test that needs exactly one live batch appends a single event.
   */
  private static void appendPings(InMemoryEventStore store, int from, int to) {
    var events = new ArrayList<EventEnvelope>();
    for (int i = from; i <= to; i++) {
      events.add(
          EventStoreFixture.event(
              StreamId.of(TYPE, AggregateId.of("s")),
              new Version(i),
              new EventType("Ping"),
              new Ping(i)));
    }
    store.append(StreamId.of(TYPE, AggregateId.of("s")), events, new Version(from - 1));
  }

  /**
   * Fails the first {@code outages} saves (a store outage spanning several deliveries), then
   * forwards. Counts every attempt.
   */
  static final class OutageOffsetStore implements OffsetStore {
    private final OffsetStore delegate;
    private final int outages;
    final AtomicInteger saveAttempts = new AtomicInteger();

    OutageOffsetStore(OffsetStore delegate, int outages) {
      this.delegate = delegate;
      this.outages = outages;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return delegate.getLastOffset(projectionName);
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      if (saveAttempts.incrementAndGet() <= outages) {
        throw new IllegalStateException("offset store down");
      }
      delegate.saveOffset(projectionName, offset);
    }

    @Override
    public void reset(ProjectionName projectionName) {
      delegate.reset(projectionName);
    }
  }

  // ---------- what the updater is handed ----------

  @Test
  void multiRunner_handsTheRepositoryToTransactional_andNullToAtLeastOnce() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var tl = new HandedRecorder("tl");
    var alo = new HandedRecorder("alo");
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(storeWith(3))
            .offsetStore(repo)
            .atomicProcessor(repo)
            .batchSize(10)
            .register("tl", tl, TRANSACTIONAL_LOCAL)
            .register("alo", alo, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> tl.applications.get() == 3 && alo.applications.get() == 3);
    } finally {
      runner.close();
    }
    assertThat(tl.handed).hasSize(1);
    assertThat(tl.handed.get(0)).isNotSameAs(NULL_MARKER).isNotSameAs(repo);
    assertThat(repo.findById(ProjectionName.of("tl"), "row", String.class))
        .contains("written-in-tx");
    assertThat(alo.handed).containsExactly(NULL_MARKER);
    assertThat(repo.hasReadModels(ProjectionName.of("alo"))).isFalse();
    assertThat(repo.committedOffset(ProjectionName.of("alo"))).isEqualTo(GlobalOffset.of(3));
  }

  @Test
  void scheduledRunner_handsTheRepositoryByMode() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var tl = new HandedRecorder("tl");
    var alo = new HandedRecorder("alo");
    var runner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWith(2))
            .offsetStore(repo)
            .atomicProcessor(repo)
            .batchSize(10)
            .register("tl", tl, "* * * * * *", TRANSACTIONAL_LOCAL)
            .register("alo", alo, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> tl.applications.get() == 2 && alo.applications.get() == 2);
    } finally {
      runner.close();
    }
    assertThat(tl.handed.get(0)).isNotSameAs(NULL_MARKER);
    assertThat(alo.handed).containsExactly(NULL_MARKER);
  }

  @Test
  void pollingRunner_handsTheRepositoryByMode() {
    var repo = new InMemoryProjectionRepository();
    var runner = new PollingProjectionRunner(storeWith(2), repo, 100, 10, repo);
    var tl = new HandedRecorder("tl");
    var alo = new HandedRecorder("alo");
    runner.run(ProjectionName.of("tl"), tl, TRANSACTIONAL_LOCAL);
    runner.run(ProjectionName.of("alo"), alo, AT_LEAST_ONCE_IDEMPOTENT);
    assertThat(tl.handed.get(0)).isNotSameAs(NULL_MARKER);
    assertThat(alo.handed).containsExactly(NULL_MARKER);
  }

  @Test
  void continuousRunner_livePath_handsNullToAtLeastOnce() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var store = storeWith(1);
    var alo = new HandedRecorder("alo");
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(repo)
            .atomicProcessor(repo)
            .batchSize(10)
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("alo"), alo, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> alo.applications.get() == 1);
      // Only once the runner is LIVE is the next batch guaranteed to be delivered by the
      // subscription, not read by a still-running catch-up.
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> runner.state() == ProjectionState.LIVE);
      appendPings(store, 2, 2);
      Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> alo.applications.get() == 2);
    } finally {
      runner.close();
      t.join(5_000);
    }
    assertThat(alo.handed).containsExactly(NULL_MARKER, NULL_MARKER);
  }

  // ---------- a checkpoint-save failure is retried, never skipped or dead-lettered ----------

  @ParameterizedTest
  @EnumSource(ProjectionErrorStrategy.class)
  void continuousRunner_checkpointSaveFailure_isRetriedFromTheCheckpoint_underEveryStrategy(
      ProjectionErrorStrategy strategy) throws Exception {
    var offsets = new FailingOnceOffsetStore(new InMemoryOffsetStore());
    var dlq = new InMemoryProjectionDeadLetterStore();
    var counter = new AtomicInteger();
    Projection nonIdempotentCounter = batch -> counter.addAndGet(batch.size());
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(storeWith(2))
            .offsetStore(offsets)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .batchSize(10)
            .errorStrategy(strategy)
            .deadLetterStore(dlq)
            .readRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("ctr"), nonIdempotentCounter, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> offsets.getLastOffset(ProjectionName.of("ctr")).equals(GlobalOffset.of(2)));
    } finally {
      runner.close();
      t.join(5_000);
    }
    assertThat(offsets.saveAttempts.get()).isEqualTo(2);
    assertThat(counter.get())
        .as("the batch was applied twice (a counter double-counts): pinned, not blessed")
        .isEqualTo(4);
    assertThat(dlq.all()).as("never dead-lettered, under " + strategy).isEmpty();
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
  }

  /**
   * The LIVE path: the subscription delivers the batch, the processor applies it and the checkpoint
   * save fails. The subscription must not save its own offset, must back off and redeliver from the
   * un-advanced checkpoint, and the error strategy must never see the failure (SKIP would jump past
   * an applied batch, DLQ would dead-letter it, HALT would count it).
   */
  @ParameterizedTest
  @EnumSource(ProjectionErrorStrategy.class)
  void continuousRunner_livePath_checkpointSaveFailure_isRetried_underEveryStrategy(
      ProjectionErrorStrategy strategy) throws Exception {
    var store = new InMemoryEventStore();
    var offsets = new FailingOnceOffsetStore(new InMemoryOffsetStore());
    var dlq = new InMemoryProjectionDeadLetterStore();
    var counter = new AtomicInteger();
    Projection nonIdempotentCounter = batch -> counter.addAndGet(batch.size());
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .batchSize(10)
            .errorStrategy(strategy)
            .deadLetterStore(dlq)
            .readRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("ctr"), nonIdempotentCounter, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      // Nothing to catch up on: the first batch the projection ever sees is a live one.
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> runner.state() == ProjectionState.LIVE);
      // One event: a one-event batch cannot be split across polls.
      appendPings(store, 1, 1);
      // 1: the processor's save fails; 2: the processor's save on redelivery succeeds; 3: the
      // subscription's own save of the same offset (a monotonic no-op).
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> offsets.saveAttempts.get() == 3);
    } finally {
      runner.close();
      t.join(5_000);
    }
    assertThat(offsets.getLastOffset(ProjectionName.of("ctr"))).isEqualTo(GlobalOffset.of(1));
    assertThat(counter.get())
        .as("the live batch was applied twice (a counter double-counts): pinned, not blessed")
        .isEqualTo(2);
    assertThat(dlq.all()).as("never dead-lettered, under " + strategy).isEmpty();
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
  }

  /**
   * Two store outages precede one genuine projection failure under HALT on the live path. The HALT
   * arm's own attempt counter must then read 1: a checkpoint-store outage consumes nothing of the
   * HALT bound, so a long outage can never halt a healthy projection.
   */
  @Test
  void continuousRunner_livePath_checkpointSaveOutage_doesNotConsumeTheHaltBound()
      throws Exception {
    var continuousLog = captor(ContinuousProjectionRunner.class);
    var store = new InMemoryEventStore();
    var offsets = new OutageOffsetStore(new InMemoryOffsetStore(), 2);
    var counter = new AtomicInteger();
    var deliveries = new AtomicInteger();
    // Applies every delivery except the third, which fails the way a projection failure does.
    Projection counterFailingOnce =
        batch -> {
          if (deliveries.incrementAndGet() == 3) {
            throw new IllegalStateException("projection failure after the store recovered");
          }
          counter.addAndGet(batch.size());
        };
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.HALT)
            .readRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    Thread t =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        ProjectionName.of("ctr"), counterFailingOnce, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> runner.state() == ProjectionState.LIVE);
      // One event: a one-event batch cannot be split across polls.
      appendPings(store, 1, 1);
      // Deliveries 1 and 2: applied, save fails. Delivery 3: the projection fails before any save.
      // Delivery 4: applied, save succeeds (attempt 3), then the subscription's own save (4).
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> offsets.saveAttempts.get() == 4);
    } finally {
      runner.close();
      t.join(5_000);
    }
    assertThat(offsets.getLastOffset(ProjectionName.of("ctr"))).isEqualTo(GlobalOffset.of(1));
    assertThat(deliveries.get()).isEqualTo(4);
    assertThat(counter.get()).isEqualTo(3);
    assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
    var haltAttempts =
        continuousLog.list.stream()
            .map(ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains("Error processing live batch"))
            .toList();
    assertThat(haltAttempts).hasSize(1);
    assertThat(haltAttempts.getFirst())
        .as("two store outages consumed nothing of the HALT bound")
        .contains("(attempt 1/");
  }

  @Test
  void pollingRunner_checkpointSaveFailure_isRetriedInsideRun() {
    var offsets = new FailingOnceOffsetStore(new InMemoryOffsetStore());
    var counter = new AtomicInteger();
    var runner =
        new PollingProjectionRunner(
            storeWith(2), offsets, 100, 10, AtomicBatchProcessor.nonAtomicAtLeastOnce());
    runner.run(
        ProjectionName.of("ctr"),
        batch -> counter.addAndGet(batch.size()),
        AT_LEAST_ONCE_IDEMPOTENT);
    assertThat(offsets.getLastOffset(ProjectionName.of("ctr"))).isEqualTo(GlobalOffset.of(2));
    assertThat(offsets.saveAttempts.get()).isEqualTo(2);
    assertThat(counter.get()).isEqualTo(4);
  }

  @Test
  void scheduledRunner_checkpointSaveFailure_endsTheDrain_andTheNextTickReapplies()
      throws Exception {
    var offsets = new FailingOnceOffsetStore(new InMemoryOffsetStore());
    var dlq = new InMemoryProjectionDeadLetterStore();
    var counter = new AtomicInteger();
    var runner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWith(2))
            .offsetStore(offsets)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .deadLetterStore(dlq)
            .batchSize(10)
            .register(
                "ctr",
                batch -> counter.addAndGet(batch.size()),
                "* * * * * *",
                ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> offsets.getLastOffset(ProjectionName.of("ctr")).equals(GlobalOffset.of(2)));
    } finally {
      runner.close();
    }
    assertThat(counter.get()).isEqualTo(4);
    assertThat(dlq.all()).isEmpty();
  }

  // ---------- the INFO line and the gauge ----------

  @Test
  void infoLineIsLoggedOncePerRegistration_byTheExecutingRunner_andTheGaugeIsRecorded()
      throws Exception {
    var continuousLog = captor(ContinuousProjectionRunner.class);
    var multiLog = captor(MultiProjectionRunner.class);
    var metrics = new RecordingStreamRuneMetrics();
    var repo = new InMemoryProjectionRepository();
    var tl = new HandedRecorder("orders");
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(storeWith(1))
            .offsetStore(repo)
            .atomicProcessor(repo)
            .metrics(metrics)
            .register("orders", tl, TRANSACTIONAL_LOCAL)
            .register("audit", batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> tl.applications.get() == 1);
    } finally {
      runner.close();
    }
    var lines =
        continuousLog.list.stream()
            .filter(
                e ->
                    e.getLevel() == Level.INFO && e.getFormattedMessage().contains("delivery mode"))
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    assertThat(lines)
        .hasSize(2)
        .anySatisfy(
            l ->
                assertThat(l)
                    .isEqualTo(
                        "Projection 'orders' delivery mode TRANSACTIONAL_LOCAL on "
                            + InMemoryProjectionRepository.class.getName()
                            + " — transactional, write-through declared (write target not"
                            + " exposed)"))
        .anySatisfy(
            l ->
                assertThat(l)
                    .isEqualTo(
                        "Projection 'audit' delivery mode AT_LEAST_ONCE_IDEMPOTENT on "
                            + InMemoryProjectionRepository.class.getName()
                            + " — at-least-once: no transaction-scoped repository is handed;"
                            + " read-model writes are outside the checkpoint transaction"));
    assertThat(multiLog.list).noneMatch(e -> e.getFormattedMessage().contains("delivery mode"));
    assertThat(metrics.count("projection.delivery_mode", "orders")).isEqualTo(1);
    assertThat(metrics.count("projection.delivery_mode", "audit")).isEqualTo(1);
    assertThat(metrics.deliveryModeOf("orders")).isEqualTo(TRANSACTIONAL_LOCAL);
  }

  @Test
  void infoLineNamesTheNonatomicProcessorByName() {
    var pollingLog = captor(PollingProjectionRunner.class);
    var runner =
        new PollingProjectionRunner(
            storeWith(1),
            new InMemoryOffsetStore(),
            100,
            10,
            AtomicBatchProcessor.nonAtomicAtLeastOnce());
    runner.run(ProjectionName.of("audit"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT);
    assertThat(pollingLog.list.stream().map(ILoggingEvent::getFormattedMessage))
        .anySatisfy(
            l ->
                assertThat(l)
                    .startsWith(
                        "Projection 'audit' delivery mode AT_LEAST_ONCE_IDEMPOTENT on"
                            + " nonAtomicAtLeastOnce — at-least-once"));
  }

  // The test logback root sits at WARN, so a captor raises the class logger to INFO for the
  // duration of the test; the teardown puts the level back and detaches the appender.
  private record Captured(
      Logger logger, Level previousLevel, ListAppender<ILoggingEvent> appender) {}

  private final List<Captured> captured = new ArrayList<>();

  private ListAppender<ILoggingEvent> captor(Class<?> loggerClass) {
    var logger = (Logger) LoggerFactory.getLogger(loggerClass);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    captured.add(new Captured(logger, logger.getLevel(), appender));
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
    return appender;
  }

  @AfterEach
  void detachCaptors() {
    for (var c : captured) {
      c.logger().detachAppender(c.appender());
      c.logger().setLevel(c.previousLevel());
      c.appender().stop();
    }
    captured.clear();
  }
}

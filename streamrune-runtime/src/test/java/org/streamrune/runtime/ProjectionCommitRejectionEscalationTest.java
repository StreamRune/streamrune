package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionCommitFencedException.Guard;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLeadership;
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
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * A commit rejection that repeats at an unchanged checkpoint halts the projection with a
 * configuration error; one the re-read resolves, and an epoch-fence rejection, do not.
 *
 * <p>The wiring under test is the one that produces the repeat: the processor (an {@link
 * InMemoryProjectionRepository}, which keeps its own checkpoint) and the runner's {@link
 * OffsetStore} (a separate {@link InMemoryOffsetStore}) are two stores, as a {@code
 * JdbcProjectionRepository} and a {@code PostgresOffsetStore} over two {@code DataSource}s are. The
 * first batch commits in the processor's store; the runner reads its own store, still at the start,
 * reads the same batch again, and the processor rejects it as an overlap.
 */
class ProjectionCommitRejectionEscalationTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));

  record Placed(String id) implements DomainEvent {}

  record Applied(int events) {}

  /** Counts the events it is handed into one row, through the handed repository. */
  private static final class CountingProjection extends BaseProjection {
    final AtomicInteger batches = new AtomicInteger();

    CountingProjection(ProjectionRepository repository, String name) {
      super(repository, name);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      batches.incrementAndGet();
      int applied = findById("row", Applied.class).map(Applied::events).orElse(0);
      save("row", new Applied(applied + batch.size()));
    }
  }

  private static InMemoryEventStore storeWithTwoEvents() {
    var store = new InMemoryEventStore();
    store.append(
        STREAM,
        List.of(
            EventStoreFixture.event(
                STREAM, new Version(1), new EventType("Placed"), new Placed("a")),
            EventStoreFixture.event(
                STREAM, new Version(2), new EventType("Placed"), new Placed("b"))),
        new Version(0));
    return store;
  }

  private static SubscriptionConfig fastPolling() {
    return new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(10));
  }

  @Test
  void continuous_processorAndOffsetStoreAreTwoStores_haltsOnTheSecondRejection_healthDown()
      throws Exception {
    var name = ProjectionName.of("orders");
    var events = storeWithTwoEvents();
    var processor = new InMemoryProjectionRepository();
    var runnerOffsets = new InMemoryOffsetStore();
    var projection = new CountingProjection(processor, name.value());
    var health = new SubscriptionHealthContributor(events, runnerOffsets);
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(events)
            .offsetStore(runnerOffsets)
            .atomicProcessor(processor)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastPolling())
            .healthContributor(health)
            .build();

    var thrown = new AtomicReference<Throwable>();
    Thread run =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(name, projection, TRANSACTIONAL_LOCAL);
                  } catch (Throwable t) {
                    thrown.set(t);
                  }
                });
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> runner.state() == ProjectionState.ERROR);
      run.join(10_000);
    } finally {
      runner.close();
    }

    assertThat(run.isAlive()).as("the run ends instead of retrying forever").isFalse();
    assertThat(runner.lastError())
        .contains("rejected two commits in a row")
        .contains("stayed at 0")
        .contains("not over the same DataSource")
        .contains("two runners are registered under this projection name");
    assertThat(health.overallStatus()).isEqualTo(SubscriptionHealth.Status.DOWN);
    assertThat(projection.batches).as("the batch was applied once, never twice").hasValue(1);
    assertThat(processor.findById(name, "row", Applied.class)).contains(new Applied(2));
    assertThat(processor.getLastOffset(name)).isEqualTo(GlobalOffset.of(2));
    assertThat(runnerOffsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void continuous_oneRejectionTheReReadResolves_isTolerated_andTheProjectionKeepsRunning()
      throws Exception {
    var name = ProjectionName.of("orders");
    var events = storeWithTwoEvents();
    var store = new InMemoryProjectionRepository();
    var projection = new CountingProjection(store, name.value());
    var rejections = new AtomicInteger();
    // Another writer commits the first batch between this runner's read and its commit, so the
    // runner's own commit of that batch is rejected as an overlap. Its re-read starts after the
    // moved checkpoint.
    var racedOnce = new RacedByAnotherWriterOnce(store, rejections);
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(events)
            .offsetStore(store)
            .atomicProcessor(racedOnce)
            .fetchSize(1)
            .batchSize(1)
            .subscriptionConfig(fastPolling())
            .build();

    Thread run = Thread.ofVirtual().start(() -> runner.run(name, projection, TRANSACTIONAL_LOCAL));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> store.getLastOffset(name).equals(GlobalOffset.of(2)));
      assertThat(rejections).as("the retried batch was rejected once").hasValue(1);
      assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
      assertThat(runner.lastError()).isNull();
      assertThat(store.findById(name, "row", Applied.class)).contains(new Applied(2));
    } finally {
      runner.close();
      run.join(10_000);
    }
  }

  @Test
  void continuous_epochFenceRejectionsAtAnUnchangedCheckpoint_neverHalt() throws Exception {
    var name = ProjectionName.of("orders");
    var events = storeWithTwoEvents();
    var offsets = new InMemoryOffsetStore();
    var rejections = new AtomicInteger();
    // A superseded leader whose local lease view still says "leader": every commit is rejected by
    // the epoch fence, at a checkpoint that does not move. Standing by is the answer, for as long
    // as it lasts.
    AtomicBatchProcessor supersededEveryTime =
        TestFencingProcessor.fencing(
            (projectionName, batch, newOffset, fencingEpoch, updater, offsetStore) -> {
              rejections.incrementAndGet();
              throw new ProjectionCommitFencedException(
                  Guard.EPOCH_FENCE, "caller epoch 1 is below the stored epoch 2");
            });
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(events)
            .offsetStore(offsets)
            .atomicProcessor(supersededEveryTime)
            .leadership(alwaysLeaderAtEpoch(1L))
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastPolling())
            .build();

    Thread run =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(
                        name,
                        batch -> {},
                        org.streamrune.core.projection.ProjectionDeliveryMode
                            .AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> rejections.get() >= 4);
      assertThat(runner.state()).isNotEqualTo(ProjectionState.ERROR);
      assertThat(runner.lastError()).isNull();
      assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
    } finally {
      runner.close();
      run.join(10_000);
    }
  }

  @Test
  void scheduled_processorAndOffsetStoreAreTwoStores_haltsOnTheSecondRejection_healthDown() {
    var events = storeWithTwoEvents();
    var processor = new InMemoryProjectionRepository();
    var runnerOffsets = new InMemoryOffsetStore();
    var projection = new CountingProjection(processor, "orders");
    var health = new SubscriptionHealthContributor(events, runnerOffsets);
    var runner =
        ScheduledProjectionRunner.builder()
            .eventStore(events)
            .offsetStore(runnerOffsets)
            .atomicProcessor(processor)
            .healthContributor(health)
            .register("orders", projection, "* * * * * *", TRANSACTIONAL_LOCAL)
            .build();

    runner.start();
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> runner.status().get("orders").state() == ScheduledProjectionState.ERROR);

      assertThat(runner.status().get("orders").lastError())
          .contains("rejected two commits in a row")
          .contains("stayed at 0")
          .contains("not over the same DataSource")
          .contains("two runners are registered under this projection name");
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> health.overallStatus() == SubscriptionHealth.Status.DOWN);
      assertThat(projection.batches).as("the batch was applied once, never twice").hasValue(1);
      assertThat(processor.getLastOffset(ProjectionName.of("orders")))
          .isEqualTo(GlobalOffset.of(2));
    } finally {
      runner.stop();
    }
  }

  @Test
  void tracker_reportsOnlyASecondConsecutiveRejectionAtTheSameCheckpoint() {
    var tracker = new CommitRejectionTracker();
    var observed = new ArrayList<Boolean>();

    observed.add(tracker.repeatsAt(10)); // first at 10
    observed.add(tracker.repeatsAt(20)); // the checkpoint moved: a first one again
    tracker.reset(); // a commit, or an epoch-fence rejection
    observed.add(tracker.repeatsAt(20)); // first since the reset
    observed.add(tracker.repeatsAt(20)); // second at 20, nothing in between

    assertThat(observed).containsExactly(false, false, false, true);
  }

  /**
   * Commits the first batch it is handed twice: once as another writer of the same projection
   * would, and once for the runner, whose commit the store then rejects as an overlap. Forwards
   * everything after that, and counts the rejections it passes on.
   */
  private static final class RacedByAnotherWriterOnce implements AtomicBatchProcessor {
    private final InMemoryProjectionRepository store;
    private final AtomicInteger rejections;
    private final List<Long> raced = new CopyOnWriteArrayList<>();

    RacedByAnotherWriterOnce(InMemoryProjectionRepository store, AtomicInteger rejections) {
      this.store = store;
      this.rejections = rejections;
    }

    @Override
    public void executeAtomically(
        ProjectionName projectionName,
        List<EventEnvelope> batch,
        GlobalOffset newOffset,
        long fencingEpoch,
        ProjectionUpdater projectionUpdater,
        OffsetStore offsetStore) {
      if (raced.isEmpty()) {
        raced.add(newOffset.value());
        store.executeAtomically(
            projectionName, batch, newOffset, fencingEpoch, projectionUpdater, offsetStore);
      }
      try {
        store.executeAtomically(
            projectionName, batch, newOffset, fencingEpoch, projectionUpdater, offsetStore);
      } catch (ProjectionCommitFencedException rejected) {
        rejections.incrementAndGet();
        throw rejected;
      }
    }

    @Override
    public boolean supportsFencing() {
      return true;
    }

    @Override
    public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
      store.stampFencingEpoch(projectionName, fencingEpoch);
    }

    @Override
    public boolean writesTo(ProjectionRepository target) {
      return store.writesTo(target);
    }
  }

  private static SubscriptionLeadership alwaysLeaderAtEpoch(long epoch) {
    return new SubscriptionLeadership() {
      private final Optional<Lease> held = Optional.of(new Lease(epoch));

      @Override
      public Optional<Lease> tryAcquire(String consumerName) {
        return held;
      }

      @Override
      public Optional<Lease> current(String consumerName) {
        return held;
      }

      @Override
      public void resign(String consumerName) {}

      @Override
      public void close() {}
    };
  }
}

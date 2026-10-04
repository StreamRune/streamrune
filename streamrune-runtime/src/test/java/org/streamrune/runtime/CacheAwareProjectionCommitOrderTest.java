package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Query;
import org.streamrune.core.Versioned;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
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

/**
 * When does {@link CacheAwareProjection} invalidate the query cache, relative to the commit of the
 * read-model transaction it belongs to?
 *
 * <p>Under a transactional {@link AtomicBatchProcessor} the projection runs INSIDE the transaction
 * ({@code ContinuousProjectionRunner.processBatch} hands {@code projection.process(batch,
 * txRepository)} to {@code executeAtomically}). {@code CacheAwareProjection} invalidated right
 * after its delegate returned, i.e. before the commit. A query landing between that invalidation
 * and the commit missed the cache, read the read model as last committed (the old one: the batch's
 * writes are not visible to other connections yet) and cached it. Nothing evicted it after the
 * commit, so the cache served the pre-batch answer until the entry's TTL ran out.
 *
 * <p>The fix: {@code CacheAwareProjection} registers the invalidation on the transaction-scoped
 * repository ({@code ProjectionRepository.afterCommit}), and the transaction runs it once it has
 * committed. A rolled-back batch changed nothing, so its invalidation is dropped. With a
 * non-transactional processor (a {@code null} repository) the delegate's own writes have already
 * committed when it returns, so the invalidation still runs at once.
 *
 * <p>The processor below models a transactional one with READ COMMITTED visibility: writes made
 * through its repository stay pending until the commit, and other readers (the query handler) see
 * only committed state. Its {@code beforeCommit} hook is the window between the projection's writes
 * and the commit, where a concurrent query lands deterministically.
 *
 * <p>A query can also land in the window and still be loading when the after-commit invalidation
 * runs. Caffeine's {@code invalidateAll} does not discard a load in flight, so that query stored
 * its pre-commit answer after the invalidation; {@code CachingQueryBus} now removes an answer whose
 * load overlapped an eviction.
 */
class CacheAwareProjectionCommitOrderTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record OrderPlaced(String orderId) implements DomainEvent {}

  @Cacheable(ttlSeconds = 600, scope = Cacheable.Scope.GLOBAL, invalidateOn = OrderPlaced.class)
  record CountOrders() implements Query<Integer> {}

  private static final ProjectionName NAME = ProjectionName.of("order_count");

  /** The committed read model: the number of orders a reader on another connection can see. */
  private final AtomicInteger committedOrders = new AtomicInteger();

  /** Runs inside each transaction, after the projection wrote and before the commit. */
  private Runnable beforeCommit = () -> {};

  /** Fails the commit of the next transaction (the batch rolls back). */
  private boolean failNextCommit;

  /** A transaction-scoped repository: writes stay pending and after-commit actions wait. */
  private static final class TxRepository implements ProjectionRepository {
    int pendingOrders;
    final List<Runnable> afterCommitActions = new ArrayList<>();

    @Override
    public void afterCommit(Runnable action) {
      afterCommitActions.add(action);
    }

    @Override
    public <T> void save(ProjectionName projectionName, String id, T readModel) {
      pendingOrders++;
    }

    @Override
    public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void delete(ProjectionName projectionName, String id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Page<T> findAll(
        ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void save(
        ProjectionName projectionName, String id, T readModel, long expectedVersion) {
      throw new UnsupportedOperationException();
    }
  }

  /** Transactional processor with READ COMMITTED visibility (see the class javadoc). */
  private final AtomicBatchProcessor transactional =
      (projectionName, batch, newOffset, fencingEpoch, updater, offsetStore) -> {
        var tx = new TxRepository();
        updater.update(tx);
        beforeCommit.run();
        if (failNextCommit) {
          failNextCommit = false;
          // Rolled back: the pending writes and the after-commit actions are discarded.
          throw new IllegalStateException("commit failed: the batch is rolled back");
        }
        committedOrders.addAndGet(tx.pendingOrders); // COMMIT
        offsetStore.saveOffset(projectionName, newOffset);
        tx.afterCommitActions.forEach(Runnable::run);
      };

  /** Write-through projection: one read-model row per placed order. */
  private static final Projection ORDERS =
      new Projection() {
        @Override
        public void process(List<EventEnvelope> batch) {
          throw new UnsupportedOperationException("write-through only");
        }

        @Override
        public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
          for (EventEnvelope envelope : batch) {
            repository.save(NAME, ((OrderPlaced) envelope.event()).orderId(), envelope.event());
          }
        }
      };

  private static EventEnvelope orderPlaced(StreamId streamId, String orderId) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        streamId,
        new Version(1),
        new EventType("OrderPlaced"),
        new OrderPlaced(orderId),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-" + orderId),
            null,
            null,
            Instant.now()));
  }

  private CachingQueryBus cachingBus(AtomicInteger handlerCalls) {
    CachingQueryBus bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(
        CountOrders.class,
        query -> {
          handlerCalls.incrementAndGet();
          return committedOrders.get();
        });
    return bus;
  }

  @Test
  void aQueryBetweenTheProjectionsWritesAndTheCommit_doesNotPinThePreCommitReadModel()
      throws Exception {
    var handlerCalls = new AtomicInteger();
    CachingQueryBus bus = cachingBus(handlerCalls);
    assertEquals(0, bus.dispatch(new CountOrders()), "primes the cache with the empty model");

    // The concurrent query lands inside the window, after the projection wrote (and, before the
    // fix, after CacheAwareProjection had already invalidated) and before the commit.
    var seenInWindow = new AtomicReference<Integer>();
    beforeCommit = () -> seenInWindow.set(bus.dispatch(new CountOrders()));

    var eventStore = new InMemoryEventStore();
    var offsetStore = new InMemoryOffsetStore();
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("order-1"));
    eventStore.append(streamId, List.of(orderPlaced(streamId, "order-1")), Version.initial());
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            // The write-through projection below is handed the transaction-scoped repository only
            // under a transactional mode, which needs a processor that claims fencing.
            .atomicProcessor(TestFencingProcessor.fencing(transactional))
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .build();
    Projection cacheAware = new CacheAwareProjection(ORDERS, bus.cacheInvalidator());

    Thread t = Thread.ofVirtual().start(() -> runner.run(NAME, cacheAware, TRANSACTIONAL_LOCAL));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> committedOrders.get() == 1 && runner.state() == ProjectionState.LIVE);
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }

    assertEquals(0, seenInWindow.get(), "the in-window query saw the last committed model");
    assertEquals(
        1,
        bus.dispatch(new CountOrders()),
        "after the commit the cache must not keep serving the pre-commit read model");
  }

  @Test
  void aRolledBackBatch_leavesTheCacheAsItWas() {
    // A failed commit changed nothing in the read model, so whatever the cache holds still matches
    // it: the invalidation registered by the rolled-back batch is dropped, not run.
    var handlerCalls = new AtomicInteger();
    CachingQueryBus bus = cachingBus(handlerCalls);
    assertEquals(0, bus.dispatch(new CountOrders()));
    Projection cacheAware = new CacheAwareProjection(ORDERS, bus.cacheInvalidator());
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("order-2"));
    var batch = List.of(orderPlaced(streamId, "order-2"));
    failNextCommit = true;

    assertThrows(
        IllegalStateException.class,
        () ->
            transactional.executeAtomically(
                NAME,
                batch,
                GlobalOffset.of(1),
                0L,
                repository -> cacheAware.process(batch, repository),
                new InMemoryOffsetStore()));

    assertEquals(0, bus.dispatch(new CountOrders()));
    assertEquals(1, handlerCalls.get(), "the entry cached before the rolled-back batch survives");
  }

  @Test
  void aQueryWhoseLoadSpansTheCommit_doesNotPinThePreCommitReadModel() throws Exception {
    // Nothing is cached yet, so a query landing in the window misses and loads. It reads the read
    // model before the commit, but its answer reaches the cache only after the commit and the
    // after-commit invalidation have run. Caffeine's invalidateAll does not discard a load that is
    // still in flight, so without a guard that answer was stored afterwards and served until the
    // TTL ran out, exactly as if the invalidation had run before the commit.
    var handlerCalls = new AtomicInteger();
    var readTheModel = new CountDownLatch(1);
    var finishTheLoad = new CountDownLatch(1);
    CachingQueryBus bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(
        CountOrders.class,
        query -> {
          int seen = committedOrders.get();
          if (handlerCalls.incrementAndGet() == 1) {
            readTheModel.countDown();
            awaitQuietly(finishTheLoad);
          }
          return seen;
        });
    Projection cacheAware = new CacheAwareProjection(ORDERS, bus.cacheInvalidator());
    var batch = List.of(orderPlaced(StreamId.of(TYPE, AggregateId.of("order-4")), "order-4"));
    var seenInWindow = new AtomicReference<Integer>();
    var query = new AtomicReference<Thread>();
    beforeCommit =
        () -> {
          query.set(
              Thread.ofVirtual().start(() -> seenInWindow.set(bus.dispatch(new CountOrders()))));
          awaitQuietly(readTheModel);
        };

    transactional.executeAtomically(
        NAME,
        batch,
        GlobalOffset.of(1),
        0L,
        repository -> cacheAware.process(batch, repository),
        new InMemoryOffsetStore());
    finishTheLoad.countDown();
    query.get().join(TimeUnit.SECONDS.toMillis(5));

    assertEquals(0, seenInWindow.get(), "the in-window query read the last committed model");
    assertEquals(
        1,
        bus.dispatch(new CountOrders()),
        "a load that raced the invalidation must not pin the pre-commit read model");
    assertEquals(2, handlerCalls.get());
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS), "latch timed out");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void withoutATransaction_theInvalidationRunsAtOnce() {
    // AtomicBatchProcessor.nonAtomicAtLeastOnce() hands the projection a null repository: the
    // delegate wrote
    // through its own repository, whose writes are already committed when it returns.
    var handlerCalls = new AtomicInteger();
    CachingQueryBus bus = cachingBus(handlerCalls);
    assertEquals(0, bus.dispatch(new CountOrders()));
    var delegateCalls = new AtomicInteger();
    Projection plain =
        batch -> {
          delegateCalls.incrementAndGet();
          committedOrders.addAndGet(batch.size());
        };
    Projection cacheAware = new CacheAwareProjection(plain, bus.cacheInvalidator());

    cacheAware.process(
        List.of(orderPlaced(StreamId.of(TYPE, AggregateId.of("order-3")), "order-3")), null);

    assertEquals(1, delegateCalls.get());
    assertEquals(1, bus.dispatch(new CountOrders()), "the stale entry was evicted at once");
  }
}

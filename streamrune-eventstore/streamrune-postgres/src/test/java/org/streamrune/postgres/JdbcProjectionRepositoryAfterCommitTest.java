package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.Cacheable;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.Query;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.CacheAwareProjection;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The query cache and the read-model transaction of {@link JdbcProjectionRepository#
 * executeAtomically}, on a real PostgreSQL.
 *
 * <p>{@link CacheAwareProjection} runs inside that transaction and used to invalidate the cache
 * right after its delegate wrote, before the commit. A query landing in between read the read model
 * on another connection, where READ COMMITTED shows only the pre-batch rows, and cached that
 * answer. The commit evicted nothing, so the cache served the pre-batch read model until the TTL
 * expired.
 *
 * <p>The eviction is now an after-commit action of the transaction-scoped repository. The other
 * tests pin its failure and crash points: a rolled-back batch discards it; a {@code commit()} that
 * throws after the server committed still runs it (an unknown outcome counts as committed); a
 * failing action neither fails the committed batch nor skips the actions after it; an action
 * registered once the transaction is over runs at once after a commit and is discarded after a
 * rollback; an {@link Error} from an action propagates after a clean commit and rides, suppressed,
 * on the batch's exception when the commit threw. A process that dies between the commit and the
 * eviction loses the in-memory cache with it, so nothing has to be evicted at startup.
 */
@Testcontainers
class JdbcProjectionRepositoryAfterCommitTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_after_commit");

  static PGSimpleDataSource dataSource;

  record OrderView(String id) {}

  record OrderPlaced(String orderId) implements DomainEvent {}

  @Cacheable(ttlSeconds = 600, scope = Cacheable.Scope.GLOBAL, invalidateOn = OrderPlaced.class)
  record CountOrders(String projection) implements Query<Integer> {}

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  private static EventEnvelope orderPlaced(long offset, String orderId) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        TestStreams.stream(orderId),
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

  /** Write-through projection: one row per placed order. */
  private static Projection orders(ProjectionName name) {
    return new Projection() {
      @Override
      public void process(List<EventEnvelope> batch) {
        throw new UnsupportedOperationException("write-through only");
      }

      @Override
      public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
        for (EventEnvelope envelope : batch) {
          String id = ((OrderPlaced) envelope.event()).orderId();
          repository.save(name, id, new OrderView(id));
        }
      }
    };
  }

  @Test
  void aQueryBetweenTheProjectionsWritesAndTheCommit_doesNotPinThePreCommitReadModel() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("cache_commit_order");
    CachingQueryBus bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    // The handler reads through the pooled repository: its own connection, committed rows only.
    bus.register(
        CountOrders.class,
        query -> repo.findAll(ProjectionName.of(query.projection()), OrderView.class).size());
    var count = new CountOrders(name.value());
    assertEquals(0, bus.dispatch(count), "primes the cache with the empty read model");
    Projection cacheAware = new CacheAwareProjection(orders(name), bus.cacheInvalidator());
    var batch = List.of(orderPlaced(1, "order-1"));
    var seenInWindow = new AtomicReference<Integer>();

    repo.executeAtomically(
        name,
        batch,
        GlobalOffset.of(1),
        0L,
        tx -> {
          cacheAware.process(batch, tx);
          // A concurrent query lands here: after the projection's writes, before the commit.
          seenInWindow.set(bus.dispatch(count));
        },
        null);

    assertEquals(0, seenInWindow.get(), "inside the window the commit is not visible yet");
    assertEquals(1, repo.findAll(name, OrderView.class).size(), "the batch committed");
    assertEquals(
        1,
        bus.dispatch(count),
        "after the commit the cache must not keep serving the pre-commit read model");
  }

  @Test
  void aRolledBackBatch_leavesTheCacheAsItWas() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("cache_rolled_back");
    var handlerCalls = new AtomicInteger();
    CachingQueryBus bus = countingBus(repo, handlerCalls);
    var count = new CountOrders(name.value());
    assertEquals(0, bus.dispatch(count));
    Projection cacheAware = new CacheAwareProjection(orders(name), bus.cacheInvalidator());
    var batch = List.of(orderPlaced(1, "order-1"));

    assertThrows(
        IllegalStateException.class,
        () ->
            repo.executeAtomically(
                name,
                batch,
                GlobalOffset.of(1),
                0L,
                tx -> {
                  cacheAware.process(batch, tx);
                  throw new IllegalStateException("the batch fails after the projection wrote");
                },
                null));

    assertEquals(0, repo.findAll(name, OrderView.class).size(), "the batch rolled back");
    assertEquals(0, bus.dispatch(count));
    assertEquals(1, handlerCalls.get(), "the entry cached before the rolled-back batch survives");
  }

  @Test
  void aCommitWhoseOutcomeIsUnknown_stillEvictsTheCache() {
    // commit() reports a failure after the server committed (the connection dropped on the way
    // back). The batch IS committed, so the eviction must run; an unneeded eviction would only
    // cost one cache miss.
    var failNextCommit = new AtomicBoolean();
    var repo = new JdbcProjectionRepository(commitThenFail(dataSource, failNextCommit));
    var name = ProjectionName.of("cache_commit_in_doubt");
    var handlerCalls = new AtomicInteger();
    CachingQueryBus bus = countingBus(repo, handlerCalls);
    var count = new CountOrders(name.value());
    assertEquals(0, bus.dispatch(count));
    Projection cacheAware = new CacheAwareProjection(orders(name), bus.cacheInvalidator());
    var batch = List.of(orderPlaced(1, "order-1"));

    var thrown =
        assertThrows(
            EventStoreException.class,
            () ->
                repo.executeAtomically(
                    name,
                    batch,
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      cacheAware.process(batch, tx);
                      failNextCommit.set(true);
                    },
                    null));

    assertInstanceOf(SQLException.class, thrown.getCause());
    assertEquals(1, repo.findAll(name, OrderView.class).size(), "the server did commit");
    assertEquals(1, bus.dispatch(count), "the cache was evicted although commit() threw");
    assertEquals(2, handlerCalls.get());
  }

  @Test
  void aFailingAfterCommitAction_neitherFailsTheBatchNorSkipsTheOthers() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("after_commit_failing_action");
    var ran = new ArrayList<String>();
    var committedRowsSeen = new AtomicInteger(-1);

    repo.executeAtomically(
        name,
        List.of(orderPlaced(1, "order-1")),
        GlobalOffset.of(1),
        0L,
        tx -> {
          tx.save(name, "order-1", new OrderView("order-1"));
          tx.afterCommit(
              () -> {
                ran.add("first");
                throw new IllegalStateException("a broken after-commit action");
              });
          tx.afterCommit(
              () -> {
                ran.add("second");
                // Runs after the commit: another connection already sees the batch.
                committedRowsSeen.set(repo.findAll(name, OrderView.class).size());
              });
          assertEquals(List.of(), ran, "nothing runs before the commit");
        },
        null);

    assertEquals(List.of("first", "second"), ran);
    assertEquals(1, committedRowsSeen.get());
    // The checkpoint advanced with the batch: replaying offset 1 is rejected as already applied.
    assertThrows(
        ProjectionCommitFencedException.class,
        () ->
            repo.executeAtomically(
                name, List.of(orderPlaced(1, "order-1")), GlobalOffset.of(1), 0L, tx -> {}, null));
  }

  /**
   * The actions run while {@code executeAtomically} is already throwing when {@code commit()} threw
   * (or closing the connection failed after it). An {@link Error} from an action escaped the {@code
   * finally} that runs them and replaced the batch's {@link EventStoreException}: the runner saw a
   * {@code StackOverflowError} from cache-eviction code instead of the in-doubt commit, and the
   * commit failure was lost. The commit failure stays the exception, carrying the action's Error as
   * suppressed; the Error ends the run, as it does after a clean commit.
   */
  @Test
  void anErrorFromAnAfterCommitAction_whenTheCommitThrew_isSuppressedOnTheBatchFailure() {
    var failNextCommit = new AtomicBoolean();
    var repo = new JdbcProjectionRepository(commitThenFail(dataSource, failNextCommit));
    var name = ProjectionName.of("after_commit_error_in_flight");
    var actionError = new StackOverflowError("an after-commit action recursed too deep");
    var ran = new ArrayList<String>();

    var thrown =
        assertThrows(
            EventStoreException.class,
            () ->
                repo.executeAtomically(
                    name,
                    List.of(orderPlaced(1, "order-1")),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      tx.save(name, "order-1", new OrderView("order-1"));
                      tx.afterCommit(
                          () -> {
                            ran.add("first");
                            throw actionError;
                          });
                      tx.afterCommit(() -> ran.add("second"));
                      failNextCommit.set(true);
                    },
                    null));

    assertInstanceOf(SQLException.class, thrown.getCause(), "the commit failure is reported");
    assertArrayEquals(
        new Throwable[] {actionError},
        thrown.getSuppressed(),
        "the action's Error rides on the commit failure instead of replacing it");
    assertEquals(List.of("first"), ran, "an Error ends the after-commit run");
    assertEquals(1, repo.findAll(name, OrderView.class).size(), "the server did commit");
  }

  /**
   * The other side: after a clean commit there is no exception in flight, and an {@link Error} from
   * an action propagates out of {@code executeAtomically} as it always has. Only a {@link
   * RuntimeException} is logged and skipped. The batch and its checkpoint stay committed, and the
   * actions after the Error do not run.
   */
  @Test
  void anErrorFromAnAfterCommitAction_afterACleanCommit_propagatesAndTheBatchStaysCommitted() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("after_commit_error_committed");
    var actionError = new StackOverflowError("an after-commit action recursed too deep");
    var ran = new ArrayList<String>();

    var thrown =
        assertThrows(
            StackOverflowError.class,
            () ->
                repo.executeAtomically(
                    name,
                    List.of(orderPlaced(1, "order-1")),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      tx.save(name, "order-1", new OrderView("order-1"));
                      tx.afterCommit(
                          () -> {
                            ran.add("first");
                            throw actionError;
                          });
                      tx.afterCommit(() -> ran.add("second"));
                    },
                    null));

    assertSame(actionError, thrown);
    assertEquals(0, thrown.getSuppressed().length);
    assertEquals(List.of("first"), ran, "an Error ends the after-commit run");
    assertEquals(1, repo.findAll(name, OrderView.class).size(), "the batch committed");
    // The checkpoint advanced with the batch: replaying offset 1 is rejected as already applied.
    assertThrows(
        ProjectionCommitFencedException.class,
        () ->
            repo.executeAtomically(
                name, List.of(orderPlaced(1, "order-1")), GlobalOffset.of(1), 0L, tx -> {}, null));
  }

  /**
   * The JVM can throw one preallocated {@link Error} instance more than once, so an action can fail
   * with the very Error {@code commit()} threw. {@code addSuppressed} of an exception on itself
   * throws an {@link IllegalArgumentException}, which would replace the in-flight Error; the Error
   * is reported once, unchanged.
   */
  @Test
  void anActionFailingWithTheErrorTheCommitThrew_reportsThatErrorOnce() {
    var nextCommitFailure = new AtomicReference<Throwable>();
    var repo = new JdbcProjectionRepository(commitThenThrow(dataSource, nextCommitFailure));
    var name = ProjectionName.of("after_commit_same_error");
    var sharedError = new StackOverflowError("one Error instance thrown twice");

    var thrown =
        assertThrows(
            StackOverflowError.class,
            () ->
                repo.executeAtomically(
                    name,
                    List.of(orderPlaced(1, "order-1")),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      tx.save(name, "order-1", new OrderView("order-1"));
                      tx.afterCommit(
                          () -> {
                            throw sharedError;
                          });
                      nextCommitFailure.set(sharedError);
                    },
                    null));

    assertSame(sharedError, thrown);
    assertEquals(0, thrown.getSuppressed().length);
    assertEquals(1, repo.findAll(name, OrderView.class).size(), "the server did commit");
  }

  @Test
  void anActionRegisteredOnceTheTransactionIsOver_runsAfterACommitAndNotAfterARollback() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("after_commit_late_registration");
    var committedTx = new AtomicReference<ProjectionRepository>();
    var nested = new AtomicInteger();
    repo.executeAtomically(
        name,
        List.of(orderPlaced(1, "order-1")),
        GlobalOffset.of(1),
        0L,
        tx -> {
          committedTx.set(tx);
          // Registered from an action, i.e. after the commit: runs at once.
          tx.afterCommit(() -> tx.afterCommit(nested::incrementAndGet));
        },
        null);
    assertEquals(1, nested.get());
    var late = new AtomicInteger();
    committedTx.get().afterCommit(late::incrementAndGet);
    assertEquals(1, late.get(), "the writes are committed, so a late action runs at once");

    var rolledBackTx = new AtomicReference<ProjectionRepository>();
    assertThrows(
        IllegalStateException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(orderPlaced(2, "order-2")),
                GlobalOffset.of(2),
                0L,
                tx -> {
                  rolledBackTx.set(tx);
                  throw new IllegalStateException("rolled back");
                },
                null));
    var afterRollback = new AtomicInteger();
    rolledBackTx.get().afterCommit(afterRollback::incrementAndGet);
    assertEquals(0, afterRollback.get(), "nothing was committed, so the action is discarded");
  }

  @Test
  void aNullAfterCommitAction_isRejected() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("after_commit_null_action");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(orderPlaced(1, "order-1")),
                GlobalOffset.of(1),
                0L,
                tx -> tx.afterCommit(null),
                null));
  }

  private static CachingQueryBus countingBus(
      JdbcProjectionRepository repo, AtomicInteger handlerCalls) {
    CachingQueryBus bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(
        CountOrders.class,
        query -> {
          handlerCalls.incrementAndGet();
          return repo.findAll(ProjectionName.of(query.projection()), OrderView.class).size();
        });
    return bus;
  }

  /**
   * Wraps {@code real} so that, once {@code failNextCommit} is set, the next {@code commit()} on
   * any of its connections commits and then throws, as when the connection drops after the server
   * committed.
   */
  private static DataSource commitThenFail(DataSource real, AtomicBoolean failNextCommit) {
    return commitThenThrow(
        real,
        () ->
            failNextCommit.getAndSet(false)
                ? new SQLException("connection lost after COMMIT was sent", "08006")
                : null);
  }

  /**
   * Wraps {@code real} so that, once {@code nextCommitFailure} holds a throwable, the next {@code
   * commit()} on any of its connections commits and then throws it.
   */
  private static DataSource commitThenThrow(
      DataSource real, AtomicReference<Throwable> nextCommitFailure) {
    return commitThenThrow(real, () -> nextCommitFailure.getAndSet(null));
  }

  private static DataSource commitThenThrow(
      DataSource real, Supplier<Throwable> commitFailureOrNull) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result = invoke(method, real, args);
              if (!(result instanceof Connection connection)) {
                return result;
              }
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    Object r = invoke(m, connection, a);
                    if (m.getName().equals("commit")) {
                      Throwable failure = commitFailureOrNull.get();
                      if (failure != null) {
                        throw failure;
                      }
                    }
                    return r;
                  });
            });
  }

  private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}

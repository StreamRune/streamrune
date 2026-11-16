package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionCheckpointSaveException;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The fencing at-least-once processor over {@code subscription_checkpoint}: it hands the updater
 * {@code null}, rejects a superseded epoch or an overlapping batch before the updater runs, never
 * lets a stale leader move the checkpoint, keeps the checkpoint where it was when anything fails,
 * and serializes a dead-letter replay with a batch of the same projection. The database carries
 * both shipped series, so the test can show the checkpoint lands in the subscription table only.
 */
@Testcontainers
class PostgresSubscriptionCheckpointProcessorTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_subscription_processor_test");

  static PGSimpleDataSource dataSource;
  static PostgresSubscriptionCheckpointProcessor processor;

  @BeforeAll
  static void applyBothSeries() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    ShippedMigrationSeries.EVENT_STORE
        .configure(dataSource)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
    ShippedMigrationSeries.READ_MODEL
        .configure(dataSource)
        .table("flyway_schema_history_readmodel")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate();
    processor = new PostgresSubscriptionCheckpointProcessor(dataSource);
  }

  @Test
  void declaresAFencedNonTransactionalCheckpointThatSerializesReplay() {
    assertFalse(processor.transactional(), "it hands the updater no repository");
    assertTrue(processor.fencesCheckpoint());
    assertTrue(processor.serializesReplay());
    assertThrows(
        IllegalArgumentException.class, () -> new PostgresSubscriptionCheckpointProcessor(null));
  }

  @Test
  void handsTheUpdaterNull() {
    var name = freshName();
    var handed = new AtomicReference<ProjectionRepository>();
    var runs = new AtomicInteger();

    processor.executeAtomically(
        name,
        batch(1, 3),
        GlobalOffset.of(3),
        0L,
        repo -> {
          handed.set(repo);
          runs.incrementAndGet();
        },
        null);

    assertEquals(1, runs.get());
    assertNull(handed.get(), "the at-least-once updater writes through its own repository");
    assertEquals(GlobalOffset.of(3), offset(name));

    // A replay hands the same null.
    processor.executeReplay(
        name,
        batch(1, 1),
        repo -> {
          handed.set(repo);
          runs.incrementAndGet();
        });
    assertEquals(2, runs.get());
    assertNull(handed.get());
  }

  @Test
  void staleEpochIsRejectedBeforeTheUpdaterRuns() {
    var name = freshName();
    processor.stampFencingEpoch(name, 5L);
    var ran = new AtomicBoolean();

    var fenced =
        assertThrows(
            ProjectionCommitFencedException.class,
            () ->
                processor.executeAtomically(
                    name, batch(1, 2), GlobalOffset.of(2), 3L, repo -> ran.set(true), null));

    assertSame(ProjectionCommitFencedException.Guard.EPOCH_FENCE, fenced.guard());
    assertFalse(ran.get(), "a superseded leader's updater must never run");
    assertEquals(GlobalOffset.initial(), offset(name));
    assertEquals(5L, epoch(name));
  }

  /**
   * Review Focus 1: a superseded leader's SKIP or dead-letter advance (an empty batch, or a batch
   * whose updater does nothing, carrying a forward offset) is rejected by the epoch fence, so the
   * stale leader cannot jump the checkpoint past events the new leader has not applied.
   */
  @Test
  void staleLeaderDlqAdvanceIsRejected() {
    var name = freshName();
    processor.stampFencingEpoch(name, 2L);
    processor.executeAtomically(name, batch(1, 5), GlobalOffset.of(5), 2L, repo -> {}, null);

    for (List<EventEnvelope> advance : List.of(List.<EventEnvelope>of(), batch(6, 9))) {
      var ran = new AtomicBoolean();
      var fenced =
          assertThrows(
              ProjectionCommitFencedException.class,
              () ->
                  processor.executeAtomically(
                      name, advance, GlobalOffset.of(9), 1L, repo -> ran.set(true), null));
      assertSame(ProjectionCommitFencedException.Guard.EPOCH_FENCE, fenced.guard());
      assertFalse(ran.get());
      assertEquals(GlobalOffset.of(5), offset(name), "the stale advance must not move it");
      assertEquals(2L, epoch(name));
    }

    // The current leader advances from where the checkpoint stayed.
    processor.executeAtomically(name, batch(6, 9), GlobalOffset.of(9), 2L, repo -> {}, null);
    assertEquals(GlobalOffset.of(9), offset(name));
  }

  @Test
  void overlappingBatchIsRejectedBeforeTheUpdaterRuns() {
    var name = freshName();
    processor.executeAtomically(name, batch(1, 5), GlobalOffset.of(5), 0L, repo -> {}, null);
    var ran = new AtomicBoolean();

    var overlap =
        assertThrows(
            ProjectionCommitFencedException.class,
            () ->
                processor.executeAtomically(
                    name, batch(4, 7), GlobalOffset.of(7), 0L, repo -> ran.set(true), null));

    assertSame(ProjectionCommitFencedException.Guard.OVERLAP, overlap.guard());
    assertFalse(ran.get());
    assertEquals(GlobalOffset.of(5), offset(name));
  }

  /**
   * An empty advance that does not move the checkpoint forward is the monotonic guard's, after the
   * updater ran: it stays a fence rejection (the runners re-read), never a checkpoint-save failure.
   */
  @Test
  void aNonAdvancingCommitIsAMonotonicRejection() {
    var name = freshName();
    processor.executeAtomically(name, batch(1, 5), GlobalOffset.of(5), 4L, repo -> {}, null);

    var monotonic =
        assertThrows(
            ProjectionCommitFencedException.class,
            () ->
                processor.executeAtomically(
                    name, List.of(), GlobalOffset.of(5), 4L, repo -> {}, null));

    assertSame(ProjectionCommitFencedException.Guard.MONOTONIC, monotonic.guard());
    assertEquals(GlobalOffset.of(5), offset(name));
    assertEquals(4L, epoch(name));
  }

  @Test
  void updaterExceptionLeavesTheCheckpoint() {
    var name = freshName();
    processor.executeAtomically(name, batch(1, 2), GlobalOffset.of(2), 3L, repo -> {}, null);

    var failure = new IllegalStateException("projection failed");
    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                processor.executeAtomically(
                    name,
                    batch(3, 4),
                    GlobalOffset.of(4),
                    3L,
                    repo -> {
                      throw failure;
                    },
                    null));
    assertSame(failure, thrown, "the updater's own exception reaches the error strategy");

    var error = new AssertionError("projection Error");
    assertSame(
        error,
        assertThrows(
            AssertionError.class,
            () ->
                processor.executeAtomically(
                    name,
                    batch(3, 4),
                    GlobalOffset.of(4),
                    3L,
                    repo -> {
                      throw error;
                    },
                    null)));

    assertEquals(GlobalOffset.of(2), offset(name));
    assertEquals(3L, epoch(name));

    // The rolled-back transaction released the row lock: the retry commits.
    processor.executeAtomically(name, batch(3, 4), GlobalOffset.of(4), 3L, repo -> {}, null);
    assertEquals(GlobalOffset.of(4), offset(name));
  }

  /**
   * The checkpoint save fails after the updater returned: the batch is applied (its writes are the
   * projection's own) and the checkpoint did not move, which is a {@link
   * ProjectionCheckpointSaveException}, so the runners back off and re-read instead of skipping or
   * dead-lettering an applied batch.
   */
  @Test
  void aSaveFailureAfterTheUpdaterIsACheckpointSaveException() {
    var name = freshName();
    processor.executeAtomically(name, batch(1, 2), GlobalOffset.of(2), 0L, repo -> {}, null);
    var failing =
        new PostgresSubscriptionCheckpointProcessor(
            failingOn(
                (method, args) ->
                    "prepareStatement".equals(method)
                        && ((String) args[0]).contains("last_offset = EXCLUDED.last_offset")));
    var ran = new AtomicBoolean();

    var saveFailed =
        assertThrows(
            ProjectionCheckpointSaveException.class,
            () ->
                failing.executeAtomically(
                    name, batch(3, 4), GlobalOffset.of(4), 0L, repo -> ran.set(true), null));

    assertTrue(ran.get());
    assertEquals(name, saveFailed.projectionName());
    assertEquals(GlobalOffset.of(4), saveFailed.offset());
    var cause = assertInstanceOf(EventStoreException.class, saveFailed.getCause());
    assertEquals("Transaction failed during projection update", cause.getMessage());
    assertInstanceOf(SQLException.class, cause.getCause());
    assertEquals(GlobalOffset.of(2), offset(name));
  }

  /** A commit that fails after the updater returned is a checkpoint-save failure too. */
  @Test
  void aCommitFailureAfterTheUpdaterIsACheckpointSaveException() {
    var name = freshName();
    var failing =
        new PostgresSubscriptionCheckpointProcessor(
            failingOn((method, args) -> "commit".equals(method)));

    var saveFailed =
        assertThrows(
            ProjectionCheckpointSaveException.class,
            () ->
                failing.executeAtomically(
                    name, batch(1, 2), GlobalOffset.of(2), 0L, repo -> {}, null));

    var cause = assertInstanceOf(EventStoreException.class, saveFailed.getCause());
    assertEquals("Transaction failed during projection update", cause.getMessage());
    assertEquals(GlobalOffset.initial(), offset(name), "the failed commit rolled back");
  }

  /** A connection that cannot be had fails before the lock: nothing ran, nothing moved. */
  @Test
  void aFailedConnectionFailsBeforeTheUpdater() {
    var name = freshName();
    var down =
        new PostgresSubscriptionCheckpointProcessor(
            new FaultInjectingDataSource(dataSource).failFirst(Integer.MAX_VALUE));
    var ran = new AtomicBoolean();

    var batchFailure =
        assertThrows(
            EventStoreException.class,
            () ->
                down.executeAtomically(
                    name, batch(1, 1), GlobalOffset.of(1), 0L, repo -> ran.set(true), null));
    assertEquals("Failed to process projection batch atomically", batchFailure.getMessage());
    var replayFailure =
        assertThrows(
            EventStoreException.class,
            () -> down.executeReplay(name, batch(1, 1), repo -> ran.set(true)));
    assertEquals("Failed to replay a projection dead-letter range", replayFailure.getMessage());

    assertFalse(ran.get());
    assertEquals(GlobalOffset.initial(), offset(name));
  }

  /** A lock that fails runs no updater, and a lock failure is not a checkpoint-save failure. */
  @Test
  void aFailedLockFailsBeforeTheUpdater() {
    var name = freshName();
    var failing =
        new PostgresSubscriptionCheckpointProcessor(
            failingOn(
                (method, args) ->
                    "prepareStatement".equals(method)
                        && ((String) args[0]).contains("FOR UPDATE")));
    var ran = new AtomicBoolean();

    var batchFailure =
        assertThrows(
            EventStoreException.class,
            () ->
                failing.executeAtomically(
                    name, batch(1, 1), GlobalOffset.of(1), 0L, repo -> ran.set(true), null));
    assertEquals("Transaction failed during projection update", batchFailure.getMessage());
    var replayFailure =
        assertThrows(
            EventStoreException.class,
            () -> failing.executeReplay(name, batch(1, 1), repo -> ran.set(true)));
    assertEquals(
        "Transaction failed during projection dead-letter replay", replayFailure.getMessage());

    assertFalse(ran.get());
    assertEquals(GlobalOffset.initial(), offset(name));
    assertFalse(
        rowExists(CheckpointTable.SUBSCRIPTION, name), "the seed rolled back with the lock");
  }

  @Test
  void replayMovesNeitherTheOffsetNorTheEpoch() {
    var name = freshName();
    processor.stampFencingEpoch(name, 3L);
    processor.executeAtomically(name, batch(1, 5), GlobalOffset.of(5), 3L, repo -> {}, null);
    var ran = new AtomicBoolean();

    processor.executeReplay(name, batch(2, 2), repo -> ran.set(true));

    assertTrue(ran.get());
    assertEquals(GlobalOffset.of(5), offset(name));
    assertEquals(3L, epoch(name));

    // A replay whose updater throws propagates it and leaves the checkpoint.
    var failure = new IllegalStateException("replay failed");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                processor.executeReplay(
                    name,
                    batch(2, 2),
                    repo -> {
                      throw failure;
                    })));
    assertEquals(GlobalOffset.of(5), offset(name));
    assertEquals(3L, epoch(name));

    // A replay of a projection that never committed leaves its checkpoint at the start.
    var unseen = freshName();
    processor.executeReplay(unseen, batch(1, 1), repo -> {});
    assertEquals(GlobalOffset.initial(), offset(unseen));
    assertEquals(0L, epoch(unseen));
  }

  @Test
  void replayWaitsForAnOpenBatchOfTheSameProjection() throws InterruptedException {
    var name = freshName();
    assertSecondWaitsForFirst(
        updater ->
            processor.executeAtomically(name, batch(1, 1), GlobalOffset.of(1), 0L, updater, null),
        updater -> processor.executeReplay(name, batch(1, 1), updater));
  }

  @Test
  void batchWaitsForAnOpenReplayOfTheSameProjection() throws InterruptedException {
    var name = freshName();
    assertSecondWaitsForFirst(
        updater -> processor.executeReplay(name, batch(1, 1), updater),
        updater ->
            processor.executeAtomically(name, batch(1, 1), GlobalOffset.of(1), 0L, updater, null));
  }

  @Test
  void checkpointLandsInSubscriptionCheckpoint() throws SQLException {
    var name = freshName();

    processor.stampFencingEpoch(name, 7L);
    processor.executeAtomically(name, batch(1, 4), GlobalOffset.of(4), 7L, repo -> {}, null);

    assertTrue(rowExists(CheckpointTable.SUBSCRIPTION, name));
    assertFalse(rowExists(CheckpointTable.READ_MODEL, name));
    assertEquals(GlobalOffset.of(4), new PostgresOffsetStore(dataSource).getLastOffset(name));
    assertEquals(7L, epoch(name));
  }

  @Test
  void stampFencingEpochIsTheFencesTakeoverStamp() {
    var name = freshName();

    processor.stampFencingEpoch(name, 0L);
    assertFalse(rowExists(CheckpointTable.SUBSCRIPTION, name), "epoch 0 stamps nothing");

    processor.stampFencingEpoch(name, 4L);
    assertEquals(4L, epoch(name));
    assertEquals(GlobalOffset.initial(), offset(name));
    assertThrows(
        ProjectionEpochRegressionException.class, () -> processor.stampFencingEpoch(name, 2L));
    assertEquals(4L, epoch(name));
  }

  @Test
  void aMissingProjectionNameIsRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> processor.executeAtomically(null, List.of(), GlobalOffset.of(1), 0L, r -> {}, null));
    assertThrows(
        IllegalArgumentException.class, () -> processor.executeReplay(null, List.of(), r -> {}));
    assertThrows(IllegalArgumentException.class, () -> processor.stampFencingEpoch(null, 1L));
  }

  // --- helpers ---

  private static ProjectionName freshName() {
    return ProjectionName.of("sub_" + UUID.randomUUID().toString().replace("-", ""));
  }

  private static final PostgresCheckpointFence FENCE =
      new PostgresCheckpointFence(CheckpointTable.SUBSCRIPTION);

  private static GlobalOffset offset(ProjectionName name) {
    return FENCE.lastOffset(dataSource, name);
  }

  private static long epoch(ProjectionName name) {
    return FENCE.readEpoch(dataSource, name);
  }

  private static boolean rowExists(CheckpointTable table, ProjectionName name) {
    try (Connection conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT 1 FROM " + table.tableName() + " WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Which {@link Connection} call a {@link #failingOn} data source fails. */
  @FunctionalInterface
  private interface ConnectionFault {
    boolean fails(String method, Object[] args);
  }

  /**
   * {@link #dataSource}'s connections, with every call {@code fault} picks failing with a {@link
   * SQLException} before it reaches the real connection.
   */
  private static DataSource failingOn(ConnectionFault fault) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(dataSource, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (!"getConnection".equals(method.getName())) {
                return result;
              }
              Connection real = (Connection) result;
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if (fault.fails(m.getName(), a)) {
                      throw new SQLException("injected " + m.getName() + " failure", "08006");
                    }
                    try {
                      return m.invoke(real, a);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
            });
  }

  /**
   * Opens {@code first} and holds its updater, starts {@code second} on another thread, and asserts
   * that the second updater runs only after the first transaction is over.
   */
  private static void assertSecondWaitsForFirst(
      Consumer<AtomicBatchProcessor.ProjectionUpdater> first,
      Consumer<AtomicBatchProcessor.ProjectionUpdater> second)
      throws InterruptedException {
    var firstIsOpen = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var firstIsOver = new AtomicBoolean();
    var secondRanWhileFirstWasOpen = new AtomicBoolean();
    var secondRan = new CountDownLatch(1);
    var failure = new AtomicReference<Throwable>();
    Thread holder =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    first.accept(
                        repo -> {
                          firstIsOpen.countDown();
                          awaitRelease(release);
                          firstIsOver.set(true);
                        });
                  } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                  }
                });
    assertTrue(firstIsOpen.await(30, TimeUnit.SECONDS), "the first transaction opened");
    Thread waiter =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    second.accept(
                        repo -> {
                          secondRanWhileFirstWasOpen.set(!firstIsOver.get());
                          secondRan.countDown();
                        });
                  } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                    secondRan.countDown();
                  }
                });
    try {
      assertFalse(
          secondRan.await(500, TimeUnit.MILLISECONDS),
          "the second transaction must wait while the first holds the projection's row lock");
    } finally {
      release.countDown();
      holder.join(30_000);
      waiter.join(30_000);
    }
    assertNull(failure.get());
    assertTrue(secondRan.await(0, TimeUnit.MILLISECONDS), "the second transaction ran afterwards");
    assertFalse(secondRanWhileFirstWasOpen.get());
  }

  private static void awaitRelease(CountDownLatch release) {
    try {
      if (!release.await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("the open transaction was never released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static List<EventEnvelope> batch(long from, long to) {
    var events = new ArrayList<EventEnvelope>();
    for (long offset = from; offset <= to; offset++) {
      events.add(
          new EventEnvelope(
              GlobalOffset.of(offset),
              TestStreams.stream("stream-1"),
              new Version(offset),
              new EventType("TestEvent"),
              new TestEvent("v" + offset),
              new EventMetadata(
                  IdGenerator.generateEventId(),
                  IdGenerator.generateCommandId(),
                  null,
                  CorrelationId.of("corr"),
                  null,
                  null,
                  Instant.now())));
    }
    return List.copyOf(events);
  }

  private record TestEvent(String value) implements DomainEvent {}
}

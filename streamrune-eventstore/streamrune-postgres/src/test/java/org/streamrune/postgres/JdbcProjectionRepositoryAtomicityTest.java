package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Proves {@link JdbcProjectionRepository#executeAtomically} runs the projection update and the
 * offset save in one transaction: an updater failure rolls back both, an offset-save failure rolls
 * back the projection write, the happy path commits both, and the updater never needs a second
 * pooled connection.
 */
@Testcontainers
class JdbcProjectionRepositoryAtomicityTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_atomic_test");

  static PGSimpleDataSource dataSource;
  static PostgresOffsetStore offsetStore;

  record TestView(String id, String data) {}

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    createOffsetTable();
    offsetStore = new PostgresOffsetStore(dataSource);
  }

  static void createOffsetTable() {
    // The shipped event-store baseline, applied the way the factory applies it. This test only
    // uses projection_offset (with its epoch fencing column).
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  /** Reads the epoch fencing token stamped on the projection's checkpoint row (0 if no row). */
  private long storedEpoch(String projectionName) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, projectionName);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  @BeforeEach
  void cleanOffsets() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_offset");
    }
  }

  @Test
  void transactionScopedRepositoryHandlesEveryOperationAndValidatesInputs() {
    var repo = new JdbcProjectionRepository(dataSource);

    var pOps = ProjectionName.of("atomic_ops");
    repo.executeAtomically(
        pOps,
        List.of(),
        GlobalOffset.of(11),
        0L,
        tx -> {
          tx.save(pOps, "a", new TestView("a", "one"));
          tx.save(pOps, "b", new TestView("b", "two"));
          tx.save(pOps, "b", new TestView("b", "two-updated"), 0);

          assertEquals("one", tx.findById(pOps, "a", TestView.class).orElseThrow().data());
          var locked =
              tx.findById(pOps, "a", TestView.class, org.streamrune.core.LockMode.PESSIMISTIC);
          assertTrue(locked.isPresent());
          assertEquals(0, locked.orElseThrow().version());

          assertEquals(2, tx.findAll(pOps, TestView.class).size());
          var page = tx.findAll(pOps, TestView.class, org.streamrune.core.PageRequest.of(0, 1));
          assertEquals(1, page.content().size());
          assertEquals(2, page.totalElements());

          tx.delete(pOps, "a");
          assertTrue(tx.findById(pOps, "a", TestView.class).isEmpty());

          // Validation branches of the transaction-scoped view
          assertThrows(IllegalArgumentException.class, () -> tx.save(pOps, "x", null));
          assertThrows(
              IllegalArgumentException.class, () -> tx.save(pOps, "x", new TestView("x", "v"), -1));
          assertThrows(IllegalArgumentException.class, () -> tx.findById(pOps, "x", null));
          assertThrows(
              IllegalArgumentException.class, () -> tx.findById(pOps, "x", TestView.class, null));
          assertThrows(IllegalArgumentException.class, () -> tx.findAll(pOps, null));
          assertThrows(
              IllegalArgumentException.class, () -> tx.findAll(pOps, TestView.class, null));
          assertThrows(IllegalArgumentException.class, () -> tx.delete(pOps, " "));
          // A blank projection name never reaches the view: ProjectionName rejects it first.
          assertThrows(IllegalArgumentException.class, () -> ProjectionName.of(" "));
        },
        offsetStore);

    // Everything committed together
    assertEquals("two-updated", repo.findById(pOps, "b", TestView.class).orElseThrow().data());
    assertTrue(repo.findById(pOps, "a", TestView.class).isEmpty());
    assertEquals(GlobalOffset.of(11), offsetStore.getLastOffset(pOps));
  }

  @Test
  void happyPathCommitsProjectionWriteAndOffsetTogether() {
    var repo = new JdbcProjectionRepository(dataSource);

    var pHappy = ProjectionName.of("atomic_happy");
    repo.executeAtomically(
        pHappy,
        List.of(),
        GlobalOffset.of(7),
        0L,
        txRepository -> txRepository.save(pHappy, "id-1", new TestView("id-1", "data")),
        offsetStore);

    assertEquals("data", repo.findById(pHappy, "id-1", TestView.class).orElseThrow().data());
    assertEquals(GlobalOffset.of(7), offsetStore.getLastOffset(pHappy));
  }

  @Test
  void staleEpochCommitRejected_readModelAndOffsetUnchanged() throws Exception {
    // Epoch fence: a leader at epoch 5 commits the checkpoint; a superseded leader still
    // at
    // epoch 3 must be fenced OUT even though its offset advances forward (200 > 100) and its empty
    // batch does not overlap — only the epoch fence can reject it. The read-model write, the
    // offset,
    // AND the stored epoch must all remain at the epoch-5 state (rolled back before the updater
    // runs).
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("fence_stale");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(100),
        5L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch5")),
        offsetStore);
    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(name));
    assertEquals(5L, storedEpoch(name.value()));
    assertEquals("epoch5", repo.findById(name, "id-1", TestView.class).orElseThrow().data());

    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(200),
                3L,
                tx -> tx.save(name, "id-1", new TestView("id-1", "epoch3")),
                offsetStore));

    assertEquals(
        GlobalOffset.of(100),
        offsetStore.getLastOffset(name),
        "a fenced-out stale-epoch commit must not advance the offset");
    assertEquals(5L, storedEpoch(name.value()), "the stored epoch must not regress");
    assertEquals(
        "epoch5",
        repo.findById(name, "id-1", TestView.class).orElseThrow().data(),
        "a fenced-out stale-epoch commit must not overwrite the read model");
  }

  @Test
  void higherEpochAcceptedAndStamped() throws Exception {
    // A newer leader (higher epoch) is accepted and its epoch is stamped onto the checkpoint row so
    // it fences out the previous leader on any later stale commit.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("fence_higher");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(1),
        5L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch5")),
        offsetStore);
    assertEquals(5L, storedEpoch(name.value()));

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(2),
        6L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch6")),
        offsetStore);

    assertEquals(GlobalOffset.of(2), offsetStore.getLastOffset(name));
    assertEquals(6L, storedEpoch(name.value()), "the higher epoch must be stamped on commit");
    assertEquals("epoch6", repo.findById(name, "id-1", TestView.class).orElseThrow().data());
  }

  @Test
  void epochZeroIsUnfenced_singleNodeUnchanged() throws Exception {
    // Epoch 0 = unfenced (single-node / NOOP leadership): two epoch-0 commits both advance and the
    // stored epoch stays 0 — the fence never engages, so a single-node deployment is never fenced.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("fence_zero");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "a")),
        offsetStore);
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(2),
        0L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "b")),
        offsetStore);

    assertEquals(GlobalOffset.of(2), offsetStore.getLastOffset(name));
    assertEquals(0L, storedEpoch(name.value()), "unfenced epoch-0 commits keep the stored epoch 0");
    assertEquals("b", repo.findById(name, "id-1", TestView.class).orElseThrow().data());
  }

  @Test
  void epochZeroWriteDoesNotDowngradeStampedEpoch() throws Exception {
    // GREATEST pin: once a leader stamps epoch 6, an UNFENCED epoch-0 commit (the
    // single-node / NOOP path) with an ADVANCING offset still succeeds — epoch 0 is always unfenced
    // — but it must NOT downgrade the stored epoch back to 0. The stamp is GREATEST(stored,
    // caller), so the stored epoch REMAINS 6. Were it allowed to drop to 0, a stale epoch-1..5
    // leader could re-enter (0 < its epoch), defeating the fence.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("fence_greatest_pin");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(50),
        6L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch6")),
        offsetStore);
    assertEquals(6L, storedEpoch(name.value()));

    // Unfenced epoch-0 commit, offset advances 50 -> 51: commits (unfenced), but must not
    // downgrade.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(51),
        0L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch0")),
        offsetStore);

    assertEquals(
        GlobalOffset.of(51), offsetStore.getLastOffset(name), "the epoch-0 write advances");
    assertEquals(
        6L,
        storedEpoch(name.value()),
        "an unfenced epoch-0 write must NOT downgrade the stamped epoch (GREATEST pin)");
    assertEquals("epoch0", repo.findById(name, "id-1", TestView.class).orElseThrow().data());
  }

  @Test
  void higherEpochNonAdvancingOffset_rejectedAndNotStamped() throws Exception {
    // The stamp rides the monotonic WHERE: a HIGHER epoch does not license a NON-ADVANCING
    // offset. Commit at epoch 5 offset 10 (stamps 5); then epoch 7 at offset 10 (not > 10) updates
    // 0
    // rows -> OptimisticLockException, and because the epoch stamp is part of that same 0-row
    // UPDATE
    // the stored epoch must REMAIN 5 — an epoch must never be stamped by a rejected write (else a
    // rewound/redelivered batch from a newer leader would bump the fence without any real
    // progress).
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("fence_monotonic_stamp_pin");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(10),
        5L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch5")),
        offsetStore);
    assertEquals(5L, storedEpoch(name.value()));

    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(10),
                7L,
                tx -> tx.save(name, "id-1", new TestView("id-1", "epoch7")),
                offsetStore),
        "a non-advancing offset must be rejected even at a higher epoch (monotonic guard)");

    assertEquals(GlobalOffset.of(10), offsetStore.getLastOffset(name), "the offset must not move");
    assertEquals(
        5L,
        storedEpoch(name.value()),
        "a rejected (0-row) write must not stamp its epoch — the stamp rides the monotonic WHERE");
    assertEquals(
        "epoch5",
        repo.findById(name, "id-1", TestView.class).orElseThrow().data(),
        "a rejected write must not overwrite the read model");
  }

  @Test
  void takeoverStamp_closesPreFirstAdvanceFenceWindow() throws Exception {
    // The fence compares against the epoch STORED on the checkpoint row, stamped only
    // by a committed advance — so between a takeover (epoch 6 acquired) and the new leader's
    // first committed advance the row still carries epoch 5 and the superseded leader's writes
    // pass the fence. Phase 1 reproduces that inert window; phase 2 shows the takeover stamp
    // closing it BEFORE the new leader commits anything.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("takeover_stamp_window");

    // Leader A (epoch 5) commits the checkpoint at 100.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(100),
        5L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch5")),
        offsetStore);
    assertEquals(5L, storedEpoch(name.value()));

    // Phase 1 — the reproduced hole: B has acquired epoch 6, but WITHOUT a takeover stamp the
    // row still carries 5, so A's empty-batch SKIP/DLQ forward-jump to 150 passes every guard
    // (5 !< 5, 150 > 100, empty batch) — the fence is inert until B's first committed advance.
    repo.executeAtomically(name, List.of(), GlobalOffset.of(150), 5L, tx -> {}, offsetStore);
    assertEquals(
        GlobalOffset.of(150),
        offsetStore.getLastOffset(name),
        "without the takeover stamp the superseded leader's forward jump passes — the window"
            + " this fix closes at the runner level");

    // Phase 2 — the fix: B stamps its epoch AT TAKEOVER, before reading or processing anything.
    repo.stampFencingEpoch(name, 6L);
    assertEquals(6L, storedEpoch(name.value()), "the takeover stamp lands without any commit");
    assertEquals(
        GlobalOffset.of(150),
        offsetStore.getLastOffset(name),
        "the takeover stamp must never move the offset");

    // Every write the superseded leader STARTS after the stamp is now fenced — including the
    // empty-batch SKIP/DLQ forward-jump the overlap/monotonic guards cannot see.
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name, List.of(), GlobalOffset.of(200), 5L, tx -> {}, offsetStore),
        "after the takeover stamp the superseded leader's empty-batch advance is fenced");
    assertEquals(GlobalOffset.of(150), offsetStore.getLastOffset(name), "no stale advance landed");
    assertEquals(
        "epoch5",
        repo.findById(name, "id-1", TestView.class).orElseThrow().data(),
        "the fenced write never touched the read model");

    // The new leader's own first advance proceeds normally and re-stamps its epoch on commit.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(200),
        6L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch6")),
        offsetStore);
    assertEquals(GlobalOffset.of(200), offsetStore.getLastOffset(name));
    assertEquals(6L, storedEpoch(name.value()));
  }

  @Test
  void takeoverStamp_seedsZeroOffsetRowForNeverCommittedProjection() throws Exception {
    // A projection that has never committed has no checkpoint row. The takeover stamp must seed
    // the same 0 floor lockAndReadCheckpoint seeds — establishing the fence from birth — and the
    // new leader's genuine first advance must still pass the monotonic guard against that floor.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("takeover_stamp_seed");

    repo.stampFencingEpoch(name, 3L);
    assertEquals(3L, storedEpoch(name.value()), "the stamp seeds the row with its epoch");
    assertEquals(
        GlobalOffset.of(0),
        offsetStore.getLastOffset(name),
        "the seeded row sits at the 0 floor — the never-committed checkpoint");

    // A stale epoch-2 writer is fenced from birth.
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name, List.of(), GlobalOffset.of(10), 2L, tx -> {}, offsetStore));

    // The stamping leader's first real advance passes the monotonic guard (10 > 0) and commits.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(10),
        3L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "first")),
        offsetStore);
    assertEquals(GlobalOffset.of(10), offsetStore.getLastOffset(name));
  }

  @Test
  void takeoverStamp_isIdempotentMonotonic_neverRegressesOrChurns() throws Exception {
    // Idempotent re-stamps (a scheduled runner stamps on every leader tick) and out-of-order
    // stamps from an OLDER epoch must be 0-row no-ops: the stored epoch never regresses (or a
    // delayed stamp from a superseded leader could re-open the fence), the offset never moves,
    // and epoch 0 (unfenced/NOOP) is skipped entirely.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("takeover_stamp_monotonic");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(100),
        6L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "epoch6")),
        offsetStore);

    repo.stampFencingEpoch(name, 6L); // same-epoch re-stamp (every tick): no-op
    assertEquals(6L, storedEpoch(name.value()));
    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(name));

    // A stamp from BELOW the stored epoch is not a benign no-op. It means the caller
    // holds an epoch rejectStaleEpoch will reject on EVERY commit, and the runners classify that
    // rejection as benign — so swallowing it here froze the projection permanently at INFO level.
    // The stored epoch still never regresses; the difference is that the caller is now told.
    var regression =
        assertThrows(
            org.streamrune.core.projection.ProjectionEpochRegressionException.class,
            () -> repo.stampFencingEpoch(name, 4L),
            "a stamp below the stored epoch must be reported, not silently discarded");
    assertEquals(4L, regression.acquiredEpoch());
    assertEquals(6L, regression.storedEpoch());
    assertEquals(6L, storedEpoch(name.value()), "an older-epoch stamp never regresses the stored");
    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(name));

    repo.stampFencingEpoch(name, 0L); // unfenced: skipped
    assertEquals(6L, storedEpoch(name.value()));
    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(name));

    repo.stampFencingEpoch(name, 7L); // a genuine takeover advances the stamp
    assertEquals(7L, storedEpoch(name.value()));
    assertEquals(GlobalOffset.of(100), offsetStore.getLastOffset(name), "offset still untouched");

    // The runners refuse to pair a real leadership with a processor that cannot
    // fence, and this is the declaration they consult. The three tests above are what makes it
    // true — the stale-epoch rejection, the monotonic stamp, and the untouched offset.
    assertTrue(
        repo.supportsFencing(),
        "JdbcProjectionRepository honours the fencing epoch, so it must declare it — otherwise the"
            + " projection runners reject every multi-replica configuration that uses it");
  }

  @Test
  void updaterFailureRollsBackProjectionWriteAndOffset() {
    var repo = new JdbcProjectionRepository(dataSource);

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                repo.executeAtomically(
                    ProjectionName.of("atomic_updater_fail"),
                    List.of(),
                    GlobalOffset.of(7),
                    0L,
                    txRepository -> {
                      txRepository.save(
                          ProjectionName.of("atomic_updater_fail"),
                          "id-1",
                          new TestView("id-1", "data"));
                      // The write above ran on the transaction connection — the failure
                      // below must undo it together with the offset
                      throw new IllegalStateException("projection update failed");
                    },
                    offsetStore));

    assertEquals("projection update failed", thrown.getMessage());
    assertTrue(
        repo.findById(ProjectionName.of("atomic_updater_fail"), "id-1", TestView.class).isEmpty());
    assertEquals(
        GlobalOffset.initial(),
        offsetStore.getLastOffset(ProjectionName.of("atomic_updater_fail")));
  }

  @Test
  void updaterErrorRollsBackProjectionWriteAndOffset() {
    // An Error (StackOverflowError / OutOfMemoryError / AssertionError from user
    // mapping code in projectionUpdater.update()) escapes both inner catches (RuntimeException /
    // Exception). Before the fix the finally's setAutoCommit(true) COMMITTED the still-open
    // transaction, making the partial read-model write durable WITHOUT the offset advance — and the
    // retried batch re-applied it (the overlap guard passes because the checkpoint
    // never
    // moved), double-applying a non-idempotent projection. The single rollback-in-finally must undo
    // the write, exactly as on the event-store append path.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("atomic_updater_error");

    assertThrows(
        AssertionError.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(7),
                0L,
                txRepository -> {
                  txRepository.save(name, "id-1", new TestView("id-1", "data"));
                  // An Error, not a RuntimeException — slips past both inner catches so only the
                  // finally runs: exactly the partial-commit window.
                  throw new AssertionError(
                      "injected mid-transaction Error from projection updater");
                },
                offsetStore));

    assertTrue(
        repo.findById(name, "id-1", TestView.class).isEmpty(),
        "the partial read-model write must be rolled back, not committed by setAutoCommit(true)");
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }

  /**
   * On the projection batch: the updater fails after a read-model write and the rollback itself
   * fails (a {@code SQLException}) on a connection that still works. The finally used to log it as
   * benign and then call {@code setAutoCommit(true)}, which COMMITTED the open transaction: the
   * read-model write without its checkpoint advance, so the retried batch applied it twice. The
   * connection is now aborted instead and nothing commits; the retry applies the batch once.
   */
  @Test
  void aFailedRollbackOnALiveConnection_commitsNothing_andTheRetryAppliesTheBatchOnce() {
    var name = ProjectionName.of("atomic_rollback_fails_live");
    var failing =
        new JdbcProjectionRepository(
            rollbackFailing(new SQLException("injected rollback failure"), false));

    assertThrows(
        AssertionError.class,
        () ->
            failing.executeAtomically(
                name, List.of(), GlobalOffset.of(7), 0L, failingUpdater(name), offsetStore));

    var repo = new JdbcProjectionRepository(dataSource);
    assertTrue(
        repo.findById(name, "id-1", TestView.class).isEmpty(),
        "the aborted transaction must not commit the read-model write");
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(7),
        0L,
        tx -> tx.save(name, "id-1", new TestView("id-1", "data")),
        offsetStore);
    assertEquals("data", repo.findById(name, "id-1", TestView.class).orElseThrow().data());
    assertEquals(GlobalOffset.of(7), offsetStore.getLastOffset(name));
  }

  /**
   * The Agroal variant: an {@link Error} from the rollback skipped the autoCommit restore and
   * propagated, and the pool's reset on return ({@code setAutoCommit(true)} with no rollback first)
   * COMMITTED the open transaction. The Error still reaches the caller, after the abort, and
   * nothing commits.
   */
  @Test
  void aRollbackThatThrowsAnError_onAPoolThatResetsAutoCommitOnReturn_commitsNothing() {
    var name = ProjectionName.of("atomic_rollback_error_agroal");
    var failing =
        new JdbcProjectionRepository(
            rollbackFailing(new AssertionError("injected Error in the rollback"), true));

    var thrown =
        assertThrows(
            AssertionError.class,
            () ->
                failing.executeAtomically(
                    name, List.of(), GlobalOffset.of(7), 0L, failingUpdater(name), offsetStore));

    assertEquals("injected Error in the rollback", thrown.getMessage());
    var repo = new JdbcProjectionRepository(dataSource);
    assertTrue(
        repo.findById(name, "id-1", TestView.class).isEmpty(),
        "the pool's reset must not commit the read-model write");
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }

  /** Writes one read-model row through the transaction, then fails with an {@link Error}. */
  private static AtomicBatchProcessor.ProjectionUpdater failingUpdater(ProjectionName name) {
    return tx -> {
      tx.save(name, "id-1", new TestView("id-1", "data"));
      throw new AssertionError("injected mid-transaction Error from projection updater");
    };
  }

  /**
   * {@link #dataSource}'s connections, whose {@code rollback()} throws {@code rollbackFailure}
   * without rolling back, so the transaction stays open. With {@code agroalReturn}, {@code close()}
   * returns the connection the way Agroal 3.0 does: a changed autoCommit is reset with {@code
   * setAutoCommit(true)} and no rollback first (a {@code SQLException} there is only a pool
   * warning), then the real connection is closed. Every other call reaches the real pgjdbc
   * connection, whose {@code setAutoCommit(true)} COMMITS an open transaction.
   */
  private static DataSource rollbackFailing(Throwable rollbackFailure, boolean agroalReturn) {
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
              boolean[] autoCommitDirty = {false};
              return Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if ("rollback".equals(m.getName()) && (a == null || a.length == 0)) {
                      throw rollbackFailure;
                    }
                    if (agroalReturn
                        && "setAutoCommit".equals(m.getName())
                        && real.getAutoCommit() != (Boolean) a[0]) {
                      autoCommitDirty[0] = true;
                    }
                    if (agroalReturn && "close".equals(m.getName()) && autoCommitDirty[0]) {
                      autoCommitDirty[0] = false;
                      try {
                        real.setAutoCommit(true);
                      } catch (SQLException _) {
                        // Agroal logs a warning; a fatal SQLState makes it destroy the connection.
                      }
                    }
                    try {
                      return m.invoke(real, a);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
            });
  }

  @Test
  void offsetSaveFailureRollsBackProjectionWrite() throws Exception {
    var repo = new JdbcProjectionRepository(dataSource);

    // Sabotage the offset upsert: without the table, saveOffsetInternal fails after the
    // projection write succeeded — the write must be rolled back. The table is moved aside rather
    // than dropped, so the finally block can put the migrated table back as it was.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE projection_offset RENAME TO projection_offset_moved_aside");
    }
    try {
      var thrown =
          assertThrows(
              EventStoreException.class,
              () ->
                  repo.executeAtomically(
                      ProjectionName.of("atomic_offset_fail"),
                      List.of(),
                      GlobalOffset.of(7),
                      0L,
                      txRepository ->
                          txRepository.save(
                              ProjectionName.of("atomic_offset_fail"),
                              "id-1",
                              new TestView("id-1", "data")),
                      offsetStore));

      assertEquals("Transaction failed during projection update", thrown.getMessage());
      assertTrue(
          repo.findById(ProjectionName.of("atomic_offset_fail"), "id-1", TestView.class).isEmpty());
    } finally {
      try (var conn = dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute("ALTER TABLE projection_offset_moved_aside RENAME TO projection_offset");
      }
    }
  }

  @Test
  void saveOffsetInternalRejectsRegressionSilentNoOp() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("atomic_guard");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(300),
        0L,
        txRepository -> txRepository.save(name, "id-1", new TestView("id-1", "a")),
        offsetStore);
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));

    // Backward save → 0-row monotonic-guard no-op. The offset advance AND the read-model write must
    // roll back together: the guard rejects the advance with a conflict so the
    // 'b' write never commits. Offset stays 300 and the read model retains 'a' (no double-apply).
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(200),
                0L,
                txRepository -> txRepository.save(name, "id-1", new TestView("id-1", "b")),
                offsetStore));
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
    assertEquals(
        "a",
        repo.findById(name, "id-1", TestView.class).orElseThrow().data(),
        "a rejected backward offset must not commit its read-model write");

    // Forward save advances.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(301),
        0L,
        txRepository -> txRepository.save(name, "id-1", new TestView("id-1", "c")),
        offsetStore);
    assertEquals(GlobalOffset.of(301), offsetStore.getLastOffset(name));
    assertEquals("c", repo.findById(name, "id-1", TestView.class).orElseThrow().data());
  }

  @Test
  void reDeliveredAlreadyAppliedEventIsNotDoubleAppliedToReadModel() {
    // Regression: a stale/failed-over leader (or a redelivery) re-runs
    // executeAtomically for an event range another writer already applied and advanced past. The
    // monotonic offset guard no-ops the advance, but before the fix conn.commit() still committed
    // the read-model write — silently DOUBLE-APPLYING it for any non-idempotent projection. The
    // apply and the offset advance must be gated by the SAME guard in one transaction: if the guard
    // would no-op, the write must not commit.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("atomic_double_apply");

    // Applied once at offset 300 → applyCount 1 (non-idempotent count++ from the pre-batch value).
    int before =
        repo.findById(name, "id-1", AppliedView.class).map(AppliedView::applyCount).orElse(0);
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(300),
        0L,
        tx -> tx.save(name, "id-1", new AppliedView("id-1", before + 1)),
        offsetStore);
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
    assertEquals(1, repo.findById(name, "id-1", AppliedView.class).orElseThrow().applyCount());

    // Re-deliver an already-applied event (offset 200 <= 300): the guard no-ops the offset, so the
    // non-idempotent count++ write (which would make applyCount 2) must NOT commit. Expect a
    // conflict that rolls the whole batch back.
    int beforeReplay =
        repo.findById(name, "id-1", AppliedView.class).map(AppliedView::applyCount).orElse(0);
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(200),
                0L,
                tx -> tx.save(name, "id-1", new AppliedView("id-1", beforeReplay + 1)),
                offsetStore));

    // No double-apply (applyCount still 1) and no offset regression.
    assertEquals(
        1,
        repo.findById(name, "id-1", AppliedView.class).orElseThrow().applyCount(),
        "an already-applied re-delivery must not double-apply the read model");
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
  }

  record AppliedView(String id, int applyCount) {}

  @Test
  void overlappingBatchThatExtendsPastTheCheckpointIsRejectedNotDoubleApplied() {
    // The monotonic offset guard rejects only a NON-ADVANCING endpoint (offset <=
    // stored). An OVERLAPPING batch whose endpoint EXCEEDS the stored offset — the split-brain case
    // where a stale leader read at an older checkpoint and fetched a wider range — used to slip
    // through the guard (endpoint 110 > stored 105 → 1 row) and re-apply the [101..105] prefix a
    // second time. The overlap guard (batch.firstOffset must be strictly past the checkpoint) rolls
    // the whole transaction back instead.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("overlap_guard");

    // New leader L2 applied events [101..105] and advanced the checkpoint to 105 (applyCount 1).
    repo.executeAtomically(
        name,
        batch(101, 105),
        GlobalOffset.of(105),
        0L,
        tx -> tx.save(name, "id-1", new AppliedView("id-1", 1)),
        offsetStore);
    assertEquals(GlobalOffset.of(105), offsetStore.getLastOffset(name));
    assertEquals(1, repo.findById(name, "id-1", AppliedView.class).orElseThrow().applyCount());

    // Stale leader L1 read the checkpoint at 100 and fetched [101..110]; its endpoint 110 > 105
    // passes the monotonic guard, but its first offset 101 <= 105 overlaps already-applied events —
    // it must be rejected so the accumulating write is not applied twice.
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.executeAtomically(
                name,
                batch(101, 110),
                GlobalOffset.of(110),
                0L,
                tx -> tx.save(name, "id-1", new AppliedView("id-1", 2)),
                offsetStore));

    assertEquals(
        1,
        repo.findById(name, "id-1", AppliedView.class).orElseThrow().applyCount(),
        "an overlapping split-brain batch must not double-apply the read model");
    assertEquals(
        GlobalOffset.of(105),
        offsetStore.getLastOffset(name),
        "the checkpoint must not advance past the rejected overlapping batch");
  }

  @Test
  void concurrentDoubleWriterAtTheSameOffsetAppliesExactlyOnce() throws Exception {
    // The real two-leader race. Two writers concurrently process the SAME overlapping range
    // and target the SAME offset against the same read-model row. Exactly one commits; the other is
    // serialized on the projection_offset row lock and rejected. The non-idempotent increment is
    // applied exactly once and the checkpoint lands on the target offset.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("concurrent_same_row");

    Callable<Boolean> writer = concurrentWriter(repo, name, "id-1");
    var outcome = runConcurrently(writer, writer);

    assertEquals(1, outcome.successes, "exactly one writer may commit");
    assertEquals(1, outcome.conflicts, "the other writer must be rejected with OptimisticLock");
    assertEquals(
        1,
        repo.findById(name, "id-1", AppliedView.class).orElseThrow().applyCount(),
        "the non-idempotent increment must be applied exactly once");
    assertEquals(GlobalOffset.of(205), offsetStore.getLastOffset(name));
  }

  @Test
  void concurrentDoubleWriterOnDisjointRowsStillAppliesExactlyOnce() throws Exception {
    // Same race, but the two writers touch DISJOINT read-model rows — so there is no
    // read-model row contention and the serialization rests SOLELY on the projection_offset row
    // lock + rollback. Still exactly one commits; the loser's disjoint write is rolled back with
    // the
    // rejected offset advance, so only the winner's row exists.
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("concurrent_disjoint");

    var outcome =
        runConcurrently(
            concurrentWriter(repo, name, "row-a"), concurrentWriter(repo, name, "row-b"));

    assertEquals(1, outcome.successes, "exactly one writer may commit");
    assertEquals(1, outcome.conflicts, "the other writer must be rejected with OptimisticLock");
    int rowA =
        repo.findById(name, "row-a", AppliedView.class).map(AppliedView::applyCount).orElse(0);
    int rowB =
        repo.findById(name, "row-b", AppliedView.class).map(AppliedView::applyCount).orElse(0);
    assertEquals(
        1, rowA + rowB, "only the winning writer's disjoint row may be committed (exactly once)");
    assertEquals(GlobalOffset.of(205), offsetStore.getLastOffset(name));
  }

  /**
   * A writer that processes the overlapping range [201..205] and targets offset 205, applying a
   * non-idempotent set of applyCount=1 to {@code id}. Returns {@code true} if it committed, {@code
   * false} if it was rejected by the offset-row guard (OptimisticLockException).
   */
  private Callable<Boolean> concurrentWriter(
      JdbcProjectionRepository repo, ProjectionName name, String id) {
    return () -> {
      try {
        repo.executeAtomically(
            name,
            batch(201, 205),
            GlobalOffset.of(205),
            0L,
            tx -> tx.save(name, id, new AppliedView(id, 1)),
            offsetStore);
        return true;
      } catch (OptimisticLockException _) {
        return false;
      }
    };
  }

  private record ConcurrentOutcome(int successes, int conflicts) {}

  private ConcurrentOutcome runConcurrently(Callable<Boolean> a, Callable<Boolean> b)
      throws InterruptedException, ExecutionException {
    var barrier = new CyclicBarrier(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> fa = pool.submit(gated(barrier, a));
      Future<Boolean> fb = pool.submit(gated(barrier, b));
      int successes = 0;
      int conflicts = 0;
      for (boolean committed : List.of(fa.get(), fb.get())) {
        if (committed) {
          successes++;
        } else {
          conflicts++;
        }
      }
      return new ConcurrentOutcome(successes, conflicts);
    } finally {
      pool.shutdownNow();
    }
  }

  private static Callable<Boolean> gated(CyclicBarrier barrier, Callable<Boolean> task) {
    return () -> {
      try {
        barrier.await();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      return task.call();
    };
  }

  /**
   * Builds a contiguous batch of trivial envelopes spanning global offsets [from..to] inclusive.
   */
  private static List<EventEnvelope> batch(long from, long to) {
    var events = new java.util.ArrayList<EventEnvelope>();
    for (long offset = from; offset <= to; offset++) {
      events.add(envelope(offset));
    }
    return List.copyOf(events);
  }

  private record TestEvent(String value) implements DomainEvent {}

  private static EventEnvelope envelope(long offset) {
    var metadata =
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now());
    return new EventEnvelope(
        GlobalOffset.of(offset),
        TestStreams.stream("stream-1"),
        new Version(1),
        new EventType("TestEvent"),
        new TestEvent("v"),
        metadata);
  }

  @Test
  void resetOffsetRewindsBypassingGuard() {
    var repo = new JdbcProjectionRepository(dataSource);
    var name = ProjectionName.of("atomic_reset");

    // Advance the guarded offset to 400.
    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(400),
        0L,
        txRepository -> txRepository.save(name, "id-1", new TestView("id-1", "a")),
        offsetStore);
    assertEquals(GlobalOffset.of(400), offsetStore.getLastOffset(name));

    // resetOffset rewinds to 0 despite the monotonic guard — this is the reset/rebuild path.
    repo.resetOffset(name);
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }

  @Test
  void updaterRunsOnTransactionConnectionWithoutSecondPooledConnection() {
    var strictDataSource = new SingleConnectionDataSource(dataSource);
    var repo = new JdbcProjectionRepository(strictDataSource);

    var pSingle = ProjectionName.of("atomic_single_conn");
    var pSingleOther = ProjectionName.of("atomic_single_conn_other");
    repo.executeAtomically(
        pSingle,
        List.of(),
        GlobalOffset.of(3),
        0L,
        txRepository -> {
          txRepository.save(pSingle, "id-1", new TestView("id-1", "v1"));
          assertTrue(txRepository.findById(pSingle, "id-1", TestView.class).isPresent());
          // This table does not exist yet — its creation must also use the
          // transaction connection instead of requesting a pooled one
          txRepository.save(pSingleOther, "id-2", new TestView("id-2", "v2"));
        },
        offsetStore);

    var verifyRepo = new JdbcProjectionRepository(dataSource);
    assertEquals("v1", verifyRepo.findById(pSingle, "id-1", TestView.class).orElseThrow().data());
    assertEquals(
        "v2", verifyRepo.findById(pSingleOther, "id-2", TestView.class).orElseThrow().data());
    assertEquals(GlobalOffset.of(3), offsetStore.getLastOffset(pSingle));
  }

  /**
   * DataSource that fails when a second connection is requested while one is still open — simulates
   * an exhausted single-connection pool to prove the updater path never needs a second connection
   * (the pre-fix self-deadlock).
   */
  private static final class SingleConnectionDataSource implements DataSource {

    private final DataSource delegate;
    private final AtomicInteger active = new AtomicInteger();

    private SingleConnectionDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      if (active.incrementAndGet() > 1) {
        active.decrementAndGet();
        throw new SQLException("Second concurrent connection requested — pool exhausted");
      }
      Connection real;
      try {
        real = delegate.getConnection();
      } catch (SQLException | RuntimeException e) {
        active.decrementAndGet();
        throw e;
      }
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("close")) {
                  active.decrementAndGet();
                }
                try {
                  return method.invoke(real, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    @Override
    public Connection getConnection(String username, String password) {
      throw new UnsupportedOperationException("not needed for tests");
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException("not needed for tests");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}

package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.SagaForwardFenceContract;
import org.streamrune.test.SagaForwardFenceContract.FenceEvent;
import org.streamrune.test.SagaForwardFenceContract.FenceState;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * How a saga's forward append orders against a write that takes the saga out of its forward phase,
 * on a real PostgreSQL.
 *
 * <p>The {@code rawSql_…} methods test PostgreSQL itself with the statements the stores run: the
 * append's shared saga fence lock and its read of the saga row, against the exclusive lock and the
 * claim's stamping {@code UPDATE}. The other methods run the same orders through {@link
 * PostgresEventStore} and {@link PostgresSagaStore}; an append is frozen after its fence read and
 * before its commit by a helper transaction that holds the offset counter's row. Every wait is
 * observed on the server ({@code pg_blocking_pids}, {@code pg_stat_activity}) rather than inferred
 * from a sleep.
 */
@Testcontainers
class SagaForwardFenceLockingIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_forward_fence");

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  private static final SagaType TYPE = SagaType.of("fence.LockingSaga");
  private static final SagaId ID = SagaId.of("locking-saga-1");
  private static final StreamId TARGET =
      StreamId.of(AggregateType.of("fence_target"), AggregateId.of("target-1"));
  private static final EventType EVENT_TYPE = SagaForwardFenceContract.EVENT_TYPE;

  private static final String LOCK_SHARED =
      "SELECT pg_advisory_xact_lock_shared("
          + SagaFenceLock.CLASS_ID
          + ", "
          + SagaFenceLock.objectId(TYPE, ID)
          + ")";
  private static final String LOCK_EXCLUSIVE =
      "SELECT pg_advisory_xact_lock("
          + SagaFenceLock.CLASS_ID
          + ", "
          + SagaFenceLock.objectId(TYPE, ID)
          + ")";

  /** The fence read, as the event store runs it once the shared lock is granted. */
  private static final String FENCE_READ =
      "SELECT episode_version IS NULL AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')"
          + " AS forward FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  /** The claim: the compare-and-swap that lands the row at COMPENSATING and stamps the episode. */
  private static final String CLAIM =
      "UPDATE saga_state SET status = 'COMPENSATING', version = version + 1, updated_at = NOW(),"
          + " episode_version = CASE WHEN episode_version IS NULL THEN version + 1"
          + " ELSE episode_version END,"
          + " episode_claimed_at = CASE WHEN episode_version IS NULL THEN NOW()"
          + " ELSE episode_claimed_at END"
          + " WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";

  private final List<Connection> opened = new ArrayList<>();
  private ExecutorService pool;
  private PostgresEventStore events;
  private PostgresSagaStore sagas;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return FenceEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void forwardRow() throws SQLException {
    pool = Executors.newCachedThreadPool();
    sagas =
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
    events =
        PostgresEventStore.builder()
            .dataSource(dataSource)
            .typeRegistry(typeRegistry)
            .commandInbox(new PostgresCommandInbox(dataSource))
            .build();
    resetTables();
  }

  /** Empties the tables and writes the saga's row: RUNNING, version 1, started a day ago. */
  private static void resetTables() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE saga_state, event_stream, command_inbox");
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO saga_state (saga_id, saga_type, status, state_json, created_at)"
                  + " VALUES (?, ?, 'RUNNING', '{\"status\":\"RUNNING\"}'::jsonb,"
                  + " NOW() - INTERVAL '1 day')")) {
        ps.setString(1, ID.value());
        ps.setString(2, TYPE.value());
        ps.executeUpdate();
      }
    }
  }

  @AfterEach
  void closeAll() throws Exception {
    pool.shutdownNow();
    for (Connection conn : opened) {
      try {
        if (!conn.getAutoCommit()) {
          conn.rollback();
        }
      } catch (SQLException ignored) {
        // closing below is what matters
      }
      conn.close();
    }
    opened.clear();
    pool.awaitTermination(30, TimeUnit.SECONDS);
  }

  // ---------------------------------------------------------------------------------------------
  // PostgreSQL itself
  // ---------------------------------------------------------------------------------------------

  /** An append holding the shared fence lock makes the claim wait until the append commits. */
  @Test
  void rawSql_aClaimWaitsForAnAppendHoldingTheSharedLockThenWrites() throws Exception {
    Connection append = transaction();
    Connection claim = transaction();
    int appendPid = backendPid(append);
    int claimPid = backendPid(claim);

    assertTrue(lockedFenceRead(append), "the row is in its forward phase");

    Future<Integer> claimed =
        pool.submit(
            () -> {
              execute(claim, LOCK_EXCLUSIVE);
              return claimUpdate(claim, 1L);
            });
    awaitBlockedBy(claimPid, appendPid);
    assertEquals("Lock", waitEventType(claimPid), "the claim waits on a lock");
    assertEquals("advisory", waitEvent(claimPid), "on the saga fence lock");
    assertFalse(claimed.isDone(), "the claim cannot write while the append holds the fence lock");

    append.commit();
    assertEquals(1, claimed.get(30, TimeUnit.SECONDS), "the claim writes once the append ends");
    claim.commit();
    assertTrue(stamped(), "the row carries the episode stamp");
  }

  /**
   * An append that arrives behind a claim holding the exclusive fence lock waits, then reads the
   * claimed row in a snapshot taken after the lock is granted and answers {@code forward = false}.
   */
  @Test
  void rawSql_anAppendBehindAnOpenClaimReadsTheClaimedRow() throws Exception {
    Connection claim = transaction();
    Connection append = transaction();
    int claimPid = backendPid(claim);
    int appendPid = backendPid(append);

    execute(claim, LOCK_EXCLUSIVE);
    assertEquals(1, claimUpdate(claim, 1L));

    Future<Boolean> forward = pool.submit(() -> lockedFenceRead(append));
    awaitBlockedBy(appendPid, claimPid);
    assertEquals("advisory", waitEvent(appendPid), "the append waits on the saga fence lock");
    assertFalse(forward.isDone());

    claim.commit();
    assertFalse(
        forward.get(30, TimeUnit.SECONDS),
        "the read after the lock sees the claim: the saga has left its forward phase");
    append.commit();
  }

  /**
   * A steady stream of overlapping appends holding the shared fence lock of one saga does not
   * starve the claim: each claim's latency stays within max(20 × the mean locker loop, 1 s).
   */
  @Test
  void rawSql_sharedLockersDoNotStarveAClaim() throws Exception {
    int lockers = 8;
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong loops = new AtomicLong();
    AtomicLong loopNanos = new AtomicLong();
    List<Future<?>> running = new ArrayList<>();
    for (int i = 0; i < lockers; i++) {
      Connection conn = transaction();
      running.add(
          pool.submit(
              () -> {
                while (!stop.get()) {
                  long start = System.nanoTime();
                  lockedFenceRead(conn);
                  TimeUnit.MILLISECONDS.sleep(2);
                  conn.commit();
                  loopNanos.addAndGet(System.nanoTime() - start);
                  loops.incrementAndGet();
                }
                return null;
              }));
    }
    Connection claim = transaction();
    List<Long> latenciesMicros = new ArrayList<>();
    try {
      TimeUnit.MILLISECONDS.sleep(300); // warm-up: let the lockers overlap continuously
      for (int i = 0; i < 20; i++) {
        long start = System.nanoTime();
        Future<Integer> claimed =
            pool.submit(
                () -> {
                  execute(claim, LOCK_EXCLUSIVE);
                  return touchRow(claim);
                });
        assertEquals(1, claimed.get(60, TimeUnit.SECONDS));
        claim.commit();
        latenciesMicros.add((System.nanoTime() - start) / 1_000);
        TimeUnit.MILLISECONDS.sleep(10);
      }
    } finally {
      stop.set(true);
      for (Future<?> f : running) {
        f.get(30, TimeUnit.SECONDS);
      }
    }
    assertWithinStarvationBound(
        "shared lockers", lockers, loops.get(), loopNanos.get(), latenciesMicros);
  }

  /**
   * The timeout scan's {@code FOR UPDATE SKIP LOCKED} does not skip a saga whose forward append is
   * in flight (the append holds no row lock), and a claim made from that scan waits for the append
   * and then lands.
   */
  @Test
  void rawSql_theTimeoutScanSeesASagaWhoseAppendIsInFlightAndItsClaimWaits() throws Exception {
    Instant cutoff = Instant.now().plus(1, ChronoUnit.HOURS);
    Connection append = transaction();
    int appendPid = backendPid(append);
    assertTrue(lockedFenceRead(append));

    assertEquals(List.of(ID), sagas.findTimedOut(TYPE, cutoff, 10), "the scan sees the saga");
    Future<?> claimed =
        pool.submit(
            () -> {
              sagas.claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
              return null;
            });
    int claimPid = awaitBlockedOn(appendPid, claimed);
    assertEquals("advisory", waitEvent(claimPid));

    append.commit();
    claimed.get(30, TimeUnit.SECONDS);
    assertTrue(stamped());
  }

  // ---------------------------------------------------------------------------------------------
  // The stores: PostgresEventStore's fenced append against PostgresSagaStore's writes
  // ---------------------------------------------------------------------------------------------

  /**
   * A claim that arrives while a fenced append is frozen after its fence read, before its offset
   * reservation and commit, waits for the append; the forward command is stored before the episode
   * opens.
   */
  @Test
  void aClaimWaitsForAFencedAppendFrozenBeforeItsCommit() throws Exception {
    Connection freezer = transaction();
    int freezerPid = backendPid(freezer);
    freezeAppends(freezer);

    Future<IdempotentAppendResult> appended =
        pool.submit(() -> fencedAppend(TARGET, IdempotencyKey.of("fwd-frozen"), 0));
    int appendPid = awaitAnyBlockedBy(freezerPid);

    Future<?> claimed =
        pool.submit(
            () -> {
              sagas.claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
              return null;
            });
    int claimPid = awaitBlockedOn(appendPid, claimed);
    assertEquals("advisory", waitEvent(claimPid), "the claim waits on the saga fence lock");
    assertFalse(stamped(), "no episode is open while the forward command is in flight");

    freezer.commit();
    IdempotentAppendResult result = appended.get(30, TimeUnit.SECONDS);
    claimed.get(30, TimeUnit.SECONDS);

    assertFalse(result.alreadyApplied());
    assertEquals(1, result.globalOffsets().size(), "the forward command's event is stored");
    assertEquals(1L, count("SELECT count(*) FROM command_inbox"), "and its key");
    assertTrue(stamped(), "the claim stamped the episode after the forward command committed");
  }

  /**
   * The other fence-changing writes wait for a frozen fenced append like the claim does: a terminal
   * update, an applied event that completes the saga or opens its episode (the event path's write),
   * and a delete.
   */
  @Test
  void theOtherFenceChangingWritesWaitForAFrozenFencedAppend() throws Exception {
    for (String write :
        List.of("complete", "applyEventCompleted", "applyEventCompensating", "delete")) {
      resetTables();
      Connection freezer = transaction();
      int freezerPid = backendPid(freezer);
      freezeAppends(freezer);
      Future<IdempotentAppendResult> appended =
          pool.submit(() -> fencedAppend(TARGET, IdempotencyKey.of("fwd-" + write), 0));
      int appendPid = awaitAnyBlockedBy(freezerPid);

      Future<?> written =
          pool.submit(
              () -> {
                switch (write) {
                  case "complete" ->
                      sagas.update(
                          ID, TYPE, new FenceState(SagaStatus.COMPLETED), SagaStatus.COMPLETED, 1L);
                  case "applyEventCompleted" ->
                      sagas.applyEvent(
                          ID,
                          TYPE,
                          new FenceState(SagaStatus.COMPLETED),
                          SagaStatus.COMPLETED,
                          1L,
                          AppliedEvent.live(GlobalOffset.of(42L)));
                  case "applyEventCompensating" ->
                      sagas.applyEvent(
                          ID,
                          TYPE,
                          new FenceState(SagaStatus.COMPENSATING),
                          SagaStatus.COMPENSATING,
                          1L,
                          AppliedEvent.live(GlobalOffset.of(42L)));
                  default -> sagas.delete(ID, TYPE);
                }
                return null;
              });
      int writerPid = awaitBlockedOn(appendPid, written);
      assertEquals("advisory", waitEvent(writerPid), write + " waits on the saga fence lock");

      freezer.commit();
      assertFalse(appended.get(30, TimeUnit.SECONDS).alreadyApplied());
      written.get(30, TimeUnit.SECONDS);
    }
  }

  /**
   * A fence-changing write reads the row as it is once its lock is granted, whatever isolation the
   * pool's sessions default to: here the sessions default to SERIALIZABLE, and an unfenced write
   * (the dead-letter shield) commits while the claim waits for a frozen fenced append. Without the
   * transaction's READ COMMITTED pin the claim's snapshot predates that write and its update fails
   * with a serialization failure.
   */
  @Test
  void aFenceChangingWriteSeesTheRowAsItIsOnceTheLockIsGranted() throws Exception {
    PGSimpleDataSource serializableSessions = new PGSimpleDataSource();
    serializableSessions.setUrl(PG.getJdbcUrl());
    serializableSessions.setUser(PG.getUsername());
    serializableSessions.setPassword(PG.getPassword());
    serializableSessions.setOptions("-c default_transaction_isolation=serializable");
    var claimer =
        new PostgresSagaStore(
            serializableSessions, new ObjectMapper().registerModule(new JavaTimeModule()));

    Connection freezer = transaction();
    int freezerPid = backendPid(freezer);
    freezeAppends(freezer);
    Future<IdempotentAppendResult> appended =
        pool.submit(() -> fencedAppend(TARGET, IdempotencyKey.of("fwd-serializable"), 0));
    int appendPid = awaitAnyBlockedBy(freezerPid);
    Future<?> claimed =
        pool.submit(
            () -> {
              claimer.claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
              return null;
            });
    awaitBlockedOn(appendPid, claimed);

    sagas.setDeadLetterPending(ID, TYPE, true);
    freezer.commit();
    appended.get(30, TimeUnit.SECONDS);
    claimed.get(30, TimeUnit.SECONDS);

    assertTrue(stamped(), "the claim landed on the row the shield write left");
  }

  /** A write that cannot change the fence's verdict does not wait for a fenced append. */
  @Test
  void aWriteThatKeepsTheSagaForwardDoesNotWaitForAFencedAppend() throws Exception {
    Connection freezer = transaction();
    int freezerPid = backendPid(freezer);
    freezeAppends(freezer);
    Future<IdempotentAppendResult> appended =
        pool.submit(() -> fencedAppend(TARGET, IdempotencyKey.of("fwd-running"), 0));
    awaitAnyBlockedBy(freezerPid);

    sagas.update(ID, TYPE, new FenceState(SagaStatus.RUNNING), SagaStatus.RUNNING, 1L);
    sagas.markFaulted(ID, TYPE, 2L);
    sagas.setDeadLetterPending(ID, TYPE, true);
    assertFalse(appended.isDone(), "the append is still frozen; the writes did not wait for it");

    freezer.commit();
    assertFalse(appended.get(30, TimeUnit.SECONDS).alreadyApplied());
  }

  /**
   * A fenced append that arrives while a claim holds the exclusive fence lock waits, then reads the
   * claimed row and is refused: nothing of the command is stored and its key stays unclaimed.
   */
  @Test
  void aFencedAppendBehindAnOpenClaimIsRefused() throws Exception {
    Connection claim = transaction();
    int claimPid = backendPid(claim);
    execute(claim, LOCK_EXCLUSIVE);
    assertEquals(1, claimUpdate(claim, 1L));

    IdempotencyKey key = IdempotencyKey.of("fwd-behind-claim");
    Future<IdempotentAppendResult> appended = pool.submit(() -> fencedAppend(TARGET, key, 0));
    awaitAnyBlockedBy(claimPid);
    assertFalse(appended.isDone());

    claim.commit();
    var failure = assertThrows(ExecutionException.class, () -> appended.get(30, TimeUnit.SECONDS));
    assertInstanceOf(SagaForwardFencedException.class, failure.getCause());
    assertEquals(0L, count("SELECT count(*) FROM event_stream"), "no event is stored");
    assertEquals(0L, count("SELECT count(*) FROM command_inbox"), "the key is not claimed");
  }

  /**
   * A steady stream of fenced forward appends of one saga — each holding the shared fence lock
   * while it queues on the offset counter — does not starve a fence-changing write: each write's
   * latency stays within max(20 × the mean append, 1 s), and the store's own claim lands too.
   */
  @Test
  void forwardAppendsDoNotStarveAClaim() throws Exception {
    int appenders = 8;
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong appends = new AtomicLong();
    AtomicLong appendNanos = new AtomicLong();
    List<Future<?>> running = new ArrayList<>();
    for (int i = 0; i < appenders; i++) {
      StreamId target = StreamId.of(AggregateType.of("fence_target"), AggregateId.of("t-" + i));
      running.add(
          pool.submit(
              () -> {
                long version = 0;
                while (!stop.get()) {
                  long start = System.nanoTime();
                  fencedAppend(target, IdempotencyKey.of(target.value() + ":" + version), version);
                  version++;
                  appendNanos.addAndGet(System.nanoTime() - start);
                  appends.incrementAndGet();
                }
                return null;
              }));
    }
    Connection writer = transaction();
    List<Long> latenciesMicros = new ArrayList<>();
    try {
      TimeUnit.MILLISECONDS.sleep(300); // warm-up: let the appenders overlap continuously
      for (int i = 0; i < 20; i++) {
        long start = System.nanoTime();
        Future<Integer> written =
            pool.submit(
                () -> {
                  SagaFenceLock.lockExclusive(writer, TYPE, ID);
                  return touchRow(writer);
                });
        assertEquals(1, written.get(60, TimeUnit.SECONDS));
        writer.commit();
        latenciesMicros.add((System.nanoTime() - start) / 1_000);
        TimeUnit.MILLISECONDS.sleep(10);
      }
      long start = System.nanoTime();
      Future<?> claimed =
          pool.submit(
              () -> {
                long version = sagas.load(ID, TYPE, FenceState.class).orElseThrow().version();
                sagas.claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), version);
                return null;
              });
      claimed.get(60, TimeUnit.SECONDS);
      latenciesMicros.add((System.nanoTime() - start) / 1_000);
    } finally {
      stop.set(true);
      for (Future<?> f : running) {
        try {
          f.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          // after the claim the appenders are refused; that ends their loop
          assertInstanceOf(SagaForwardFencedException.class, e.getCause());
        }
      }
    }
    assertTrue(appends.get() > 0, "the appenders ran");
    assertTrue(stamped());
    assertWithinStarvationBound(
        "fenced appenders", appenders, appends.get(), appendNanos.get(), latenciesMicros);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private static void assertWithinStarvationBound(
      String what, int threads, long loops, long loopNanos, List<Long> latenciesMicros) {
    double meanLoopMicros = loopNanos / 1_000.0 / Math.max(1, loops);
    long boundMicros = Math.max((long) (20 * meanLoopMicros), 1_000_000L);
    List<Long> sorted = latenciesMicros.stream().sorted().toList();
    System.out.printf(
        "%s=%d loops=%d meanLoop=%.0fus bound=%dus write latencies (us) sorted=%s%n",
        what, threads, loops, meanLoopMicros, boundMicros, sorted);
    long worst = sorted.getLast();
    assertTrue(
        worst <= boundMicros,
        "a write waited " + worst + "us under " + what + ", bound " + boundMicros + "us");
  }

  private IdempotentAppendResult fencedAppend(StreamId target, IdempotencyKey key, long head) {
    return events.appendWithKey(
        target,
        List.of(
            EventStoreFixture.event(
                target, new Version(head + 1), EVENT_TYPE, new FenceEvent("v" + (head + 1)))),
        new Version(head),
        key,
        "FenceCommand",
        SagaOrigin.dispatchedBy(TYPE, ID));
  }

  /** Holds the offset counter's row: every append waits after its fence read, before its commit. */
  private static void freezeAppends(Connection freezer) throws SQLException {
    try (PreparedStatement ps =
            freezer.prepareStatement(
                "SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE");
        ResultSet rs = ps.executeQuery()) {
      assertTrue(rs.next());
    }
  }

  private Connection transaction() throws SQLException {
    Connection conn = dataSource.getConnection();
    opened.add(conn);
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    return conn;
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
    }
  }

  /** The append's fence: the shared lock, then the read in a statement of its own. */
  private static boolean lockedFenceRead(Connection conn) throws SQLException {
    execute(conn, LOCK_SHARED);
    try (PreparedStatement ps = conn.prepareStatement(FENCE_READ)) {
      ps.setString(1, ID.value());
      ps.setString(2, TYPE.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the fence read finds the saga row");
        return rs.getBoolean(1);
      }
    }
  }

  private static int claimUpdate(Connection conn, long expectedVersion) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(CLAIM)) {
      ps.setString(1, ID.value());
      ps.setString(2, TYPE.value());
      ps.setLong(3, expectedVersion);
      return ps.executeUpdate();
    }
  }

  /** A write to the saga row that can repeat; run after the exclusive lock, like a claim. */
  private static int touchRow(Connection conn) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(
            "UPDATE saga_state SET updated_at = NOW() WHERE saga_id = ? AND saga_type = ?")) {
      ps.setString(1, ID.value());
      ps.setString(2, TYPE.value());
      return ps.executeUpdate();
    }
  }

  private static boolean stamped() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT episode_version IS NOT NULL AND status = 'COMPENSATING'"
                    + " FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, ID.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private static long count(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static int backendPid(Connection conn) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_backend_pid()");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private static String waitEventType(int pid) throws SQLException {
    return activity("wait_event_type", pid);
  }

  private static String waitEvent(int pid) throws SQLException {
    return activity("wait_event", pid);
  }

  private static String activity(String column, int pid) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT " + column + " FROM pg_stat_activity WHERE pid = ?")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  /**
   * Returns once PostgreSQL reports {@code waiterPid} blocked by {@code holderPid}. The poll
   * interval only bounds how quickly the wait is noticed; the exit condition is the lock wait.
   */
  private static void awaitBlockedBy(int waiterPid, int holderPid) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT ? = ANY(pg_blocking_pids(?))")) {
      ps.setInt(1, holderPid);
      ps.setInt(2, waiterPid);
      while (System.nanoTime() < deadline) {
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next() && rs.getBoolean(1)) {
            return;
          }
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    throw new AssertionError("pid " + waiterPid + " never blocked on pid " + holderPid);
  }

  /** Returns the pid of a backend PostgreSQL reports blocked by {@code holderPid}. */
  private static int awaitAnyBlockedBy(int holderPid) throws Exception {
    return awaitBlockedOn(holderPid, null);
  }

  /**
   * Returns the pid of a backend blocked by {@code holderPid}; fails as soon as {@code waiter}
   * (when given) completes without having waited.
   */
  private static int awaitBlockedOn(int holderPid, Future<?> waiter) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT pid FROM pg_stat_activity"
                    + " WHERE pid <> pg_backend_pid() AND ? = ANY(pg_blocking_pids(pid))")) {
      ps.setInt(1, holderPid);
      while (System.nanoTime() < deadline) {
        if (waiter != null && waiter.isDone()) {
          throw new AssertionError("the write completed without waiting for pid " + holderPid);
        }
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1);
          }
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
    }
    throw new AssertionError("no backend blocked on pid " + holderPid);
  }
}

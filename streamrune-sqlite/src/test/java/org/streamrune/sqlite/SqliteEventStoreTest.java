package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;
import org.streamrune.test.EventStoreContract.ContractState;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * {@link SqliteEventStore} beyond the shared contract: the counter-row offsets under concurrent
 * appenders, conflicts and timeouts, the trigger that refuses offsets the counter did not hand out,
 * the classification of SQLite's failures, timestamps, and the factory.
 */
class SqliteEventStoreTest {

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId A = StreamId.of(ORDER, AggregateId.of("A"));
  private static final StreamId B = StreamId.of(ORDER, AggregateId.of("B"));

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();

  @AfterEach
  void close() throws Exception {
    for (AutoCloseable resource : resources.reversed()) {
      resource.close();
    }
  }

  // ── Offsets ─────────────────────────────────────────────────────────────────────────────────

  /**
   * Eight threads append fifty times each, on a pool whose connections are really concurrent, while
   * a reader pages through the global stream: every offset from 1 to 400 is taken exactly once,
   * every page the reader gets starts right after its checkpoint (nothing becomes visible above a
   * missing lower offset), and the counter ends at 400.
   */
  @Test
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentAppendersYieldGapFreeCommitOrderedOffsets() throws Exception {
    SqliteDatabase database = open(pool(dir.resolve("concurrent.db"), 10));
    EventStore store = factory(database).create();
    int threads = 8;
    int appends = 50;
    CyclicBarrier start = new CyclicBarrier(threads + 1);
    AtomicBoolean writing = new AtomicBoolean(true);
    List<GlobalOffset> read = new CopyOnWriteArrayList<>();
    List<String> readerFailures = new CopyOnWriteArrayList<>();
    ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
    try {
      List<Future<List<GlobalOffset>>> writers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        StreamId stream = StreamId.of(ORDER, AggregateId.of("writer-" + t));
        writers.add(
            pool.submit(
                () -> {
                  List<GlobalOffset> mine = new ArrayList<>();
                  start.await();
                  for (int i = 0; i < appends; i++) {
                    mine.addAll(
                        store
                            .append(stream, List.of(event(stream, "e" + i)), new Version(i))
                            .globalOffsets());
                  }
                  return mine;
                }));
      }
      Future<?> reader =
          pool.submit(
              () -> {
                start.await();
                GlobalOffset checkpoint = GlobalOffset.initial();
                boolean last = false;
                while (!last) {
                  last = !writing.get();
                  for (List<EventEnvelope> page = store.readGlobalStream(checkpoint, 7);
                      !page.isEmpty();
                      page = store.readGlobalStream(checkpoint, 7)) {
                    if (page.getFirst().globalOffset().value() != checkpoint.value() + 1) {
                      readerFailures.add(
                          "page after " + checkpoint + " starts at " + page.getFirst());
                    }
                    page.forEach(e -> read.add(e.globalOffset()));
                    checkpoint = page.getLast().globalOffset();
                  }
                }
                return null;
              });
      List<Long> taken = new ArrayList<>();
      for (Future<List<GlobalOffset>> writer : writers) {
        List<GlobalOffset> mine = writer.get(90, TimeUnit.SECONDS);
        assertThat(mine).isSortedAccordingTo((x, y) -> Long.compare(x.value(), y.value()));
        mine.forEach(offset -> taken.add(offset.value()));
      }
      writing.set(false);
      reader.get(90, TimeUnit.SECONDS);

      List<Long> all = LongStream.rangeClosed(1, threads * appends).boxed().toList();
      assertThat(taken).containsExactlyInAnyOrderElementsOf(all);
      assertThat(read).extracting(GlobalOffset::value).containsExactlyElementsOf(all);
      assertThat(readerFailures).isEmpty();
      assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(threads * appends));
      assertThat(counter(database)).isEqualTo(threads * appends);
      for (int t = 0; t < threads; t++) {
        assertThat(store.load(StreamId.of(ORDER, AggregateId.of("writer-" + t))).version())
            .isEqualTo(new Version(appends));
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * An append refused for its version, behind the head or beyond it, takes no offset: the next
   * successful append continues right after the last stored one, and the counter is the head.
   */
  @Test
  void conflictingAppendLeavesNoOffsetGap() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("conflict.db")));
    EventStore store = factory(database).create();
    assertThat(store.append(A, events(A, 2), Version.initial()).globalOffsets())
        .containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));

    assertThatThrownBy(() -> store.append(A, events(A, 3), new Version(1)))
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessageContaining("actual head is 2");
    assertThatThrownBy(() -> store.append(A, events(A, 3), new Version(5)))
        .isExactlyInstanceOf(OptimisticLockException.class);
    assertThat(counter(database)).isEqualTo(2);

    assertThat(store.append(B, events(B, 2), Version.initial()).globalOffsets())
        .containsExactly(GlobalOffset.of(3), GlobalOffset.of(4));
    assertThat(store.append(A, events(A, 1), new Version(2)).globalOffsets())
        .containsExactly(GlobalOffset.of(5));
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 100))
        .extracting(e -> e.globalOffset().value())
        .containsExactly(1L, 2L, 3L, 4L, 5L);
    assertThat(counter(database)).isEqualTo(5);
  }

  /**
   * Two appenders racing for one version of one stream, twenty rounds: one wins each round, the
   * other is refused as a version conflict, and the offsets stay gap-free.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void appendsRacingForOneVersionLeaveNoOffsetGap() throws Exception {
    SqliteDatabase database = open(pool(dir.resolve("race.db"), 4));
    EventStore store = factory(database).create();
    int rounds = 20;
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < rounds; round++) {
        Version expected = new Version(2L * round);
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<Future<Object>> racers = new ArrayList<>();
        for (int r = 0; r < 2; r++) {
          racers.add(
              pool.submit(
                  () -> {
                    barrier.await();
                    try {
                      return store.append(A, events(A, 2), expected);
                    } catch (OptimisticLockException conflict) {
                      return conflict;
                    }
                  }));
        }
        List<Object> outcomes = new ArrayList<>();
        for (Future<Object> racer : racers) {
          outcomes.add(racer.get(30, TimeUnit.SECONDS));
        }
        assertThat(outcomes)
            .filteredOn(o -> o instanceof OptimisticLockException)
            .as("round %d", round)
            .hasSize(1);
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 1_000))
        .extracting(e -> e.globalOffset().value())
        .containsExactlyElementsOf(LongStream.rangeClosed(1, 2L * rounds).boxed().toList());
    assertThat(counter(database)).isEqualTo(2L * rounds);
  }

  // ── The offset trigger ──────────────────────────────────────────────────────────────────────

  @Test
  void anEventRowWithoutAnOffsetIsRefused() throws Exception {
    SqliteDatabase database = migrated("no-offset.db");
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      assertThatThrownBy(() -> st.execute(insertRow(null, 1)))
          .isInstanceOfSatisfying(
              SQLiteException.class,
              e -> {
                assertThat(e.getResultCode()).isEqualTo(SQLiteErrorCode.SQLITE_CONSTRAINT_TRIGGER);
                assertThat(e.getMessage()).contains("was not reserved from global_offset_counter");
              });
      assertThatThrownBy(() -> st.execute(insertRow("NULL", 1)))
          .isInstanceOf(SQLiteException.class)
          .hasMessageContaining("was not reserved from global_offset_counter");
      assertThat(count(conn, "SELECT count(*) FROM event_stream")).isZero();
    }
  }

  @Test
  void anEventRowAboveTheCounterIsRefusedAndOneAtOrBelowItIsAccepted() throws Exception {
    SqliteDatabase database = migrated("above.db");
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("UPDATE global_offset_counter SET next_value = 5 WHERE id = 1");
      assertThatThrownBy(() -> st.execute(insertRow("6", 1)))
          .isInstanceOf(SQLiteException.class)
          .hasMessageContaining("was not reserved from global_offset_counter");
      st.execute(insertRow("5", 1));
      st.execute(insertRow("2", 2));
      assertThat(count(conn, "SELECT count(*) FROM event_stream")).isEqualTo(2);
    }
  }

  @Test
  void anUpdateThatMovesAnOffsetAboveTheCounterIsRefused() throws Exception {
    SqliteDatabase database = migrated("update.db");
    EventStore store = factory(database).create();
    store.append(A, events(A, 2), Version.initial());
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      assertThatThrownBy(
              () -> st.execute("UPDATE event_stream SET global_offset = 7 WHERE global_offset = 2"))
          .isInstanceOfSatisfying(
              SQLiteException.class,
              e -> {
                assertThat(e.getResultCode()).isEqualTo(SQLiteErrorCode.SQLITE_CONSTRAINT_TRIGGER);
                assertThat(e.getMessage()).contains("was not reserved from global_offset_counter");
              });
      st.execute("UPDATE event_stream SET version = 2 WHERE global_offset = 2");
      st.execute("DELETE FROM event_stream WHERE global_offset = 1");
      st.execute("UPDATE event_stream SET global_offset = 1 WHERE global_offset = 2");
    }
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10))
        .extracting(e -> e.globalOffset().value())
        .containsExactly(1L);
  }

  @Test
  void withoutTheCounterRowEveryEventRowIsRefused() throws Exception {
    SqliteDatabase database = migrated("no-counter.db");
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DELETE FROM global_offset_counter");
      assertThatThrownBy(() -> st.execute(insertRow("1", 1)))
          .hasMessageContaining("was not reserved from global_offset_counter");
    }
  }

  @Test
  void anAppendWithoutTheCounterRowFailsAndStoresNothing() throws Exception {
    SqliteDatabase database = migrated("missing-counter.db");
    EventStore store = factory(database).validateSchema(false).create();
    execute(database, "DELETE FROM global_offset_counter");

    assertThatThrownBy(() -> store.append(A, events(A, 1), Version.initial()))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("global_offset_counter row missing");
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10)).isEmpty();
  }

  /**
   * A counter at or below the stored head makes the append collide with a stored offset: not a
   * version conflict, so not retryable, and the message says how to re-seed the counter.
   */
  @Test
  void aCounterBehindTheHeadIsADesyncNotAConflict() throws Exception {
    SqliteDatabase database = migrated("desync.db");
    EventStore store = factory(database).create();
    store.append(A, events(A, 2), Version.initial());
    execute(database, "UPDATE global_offset_counter SET next_value = 1 WHERE id = 1");

    assertThatThrownBy(() -> store.append(B, events(B, 1), Version.initial()))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("global_offset collision")
        .hasMessageContaining("UPDATE global_offset_counter SET next_value");
    assertThat(counter(database)).isEqualTo(1);

    execute(
        database,
        "UPDATE global_offset_counter SET next_value = (SELECT COALESCE(max(global_offset), 0)"
            + " FROM event_stream) WHERE id = 1");
    assertThat(store.append(B, events(B, 1), Version.initial()).globalOffsets())
        .containsExactly(GlobalOffset.of(3));
  }

  // ── Busy timeout, the write lock and the pool ───────────────────────────────────────────────

  /**
   * An append that cannot get the write lock within busy_timeout fails as an infrastructure failure
   * naming busy_timeout, not as a version conflict, stores nothing and takes no offset.
   */
  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anAppendThatWaitsLongerThanBusyTimeoutFailsAndTakesNoOffset() throws Exception {
    Path file = dir.resolve("busy.db");
    SqliteDatabase database = open(TestDatabases.dataSource(file, "busy_timeout=200"));
    EventStore store = factory(database).create();
    store.append(A, events(A, 1), Version.initial());

    try (Connection holder = TestDatabases.dataSource(file).getConnection()) {
      holder.setAutoCommit(false);
      assertThatThrownBy(() -> store.append(A, events(A, 1), new Version(1)))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("busy_timeout")
          .hasRootCauseInstanceOf(SQLiteException.class);
      assertThatThrownBy(() -> store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 0))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("busy_timeout");
      holder.rollback();
      holder.setAutoCommit(true);
    }

    assertThat(store.append(B, events(B, 1), Version.initial()).globalOffsets())
        .containsExactly(GlobalOffset.of(2));
    assertThat(counter(database)).isEqualTo(2);
  }

  /** After every write path, refused or not, no connection keeps SQLite's write lock. */
  @Test
  void noWritePathKeepsTheWriteLock() throws Exception {
    Path file = dir.resolve("lock.db");
    HikariDataSource pool = pool(file, 1);
    SqliteDatabase database = open(pool);
    EventStore store = factory(database).create();

    store.append(A, events(A, 1), Version.initial());
    assertWriteLockFree(file);
    assertThatThrownBy(() -> store.append(A, events(A, 1), Version.initial()))
        .isInstanceOf(OptimisticLockException.class);
    assertWriteLockFree(file);
    store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 0);
    assertWriteLockFree(file);
    assertThat(store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 7)).isFalse();
    assertWriteLockFree(file);
    new SqliteSnapshotStorePurger(database, s -> List.of(A)).purge(SubjectId.of("s"));
    assertWriteLockFree(file);
    try (Connection conn = pool.getConnection()) {
      assertThat(conn.getAutoCommit()).as("the pooled connection is back in autocommit").isTrue();
    }
  }

  @Test
  void aConnectionHandedOutInManualCommitModeIsRefused() throws Exception {
    Path file = dir.resolve("manual.db");
    SqliteDatabase database = open(TestDatabases.dataSource(file));
    EventStore store = factory(database).create();
    AtomicBoolean manualCommit = new AtomicBoolean(false);
    SqliteDatabase manual = open(manualCommit(TestDatabases.dataSource(file), manualCommit));
    EventStore manualStore = factory(manual).autoInitializeSchema(false).create();
    manualCommit.set(true);

    assertThatThrownBy(() -> manualStore.append(A, events(A, 1), Version.initial()))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("autoCommit=false")
        .hasMessageContaining("application transaction");
    assertThatThrownBy(() -> manualStore.saveSnapshot(A, Version.initial(), new ContractState("s")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("autoCommit=false");
    assertThatThrownBy(
            () -> new SqliteSnapshotStorePurger(manual, s -> List.of(A)).purge(SubjectId.of("s")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("autoCommit=false");
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10)).isEmpty();
    assertThat(store.load(A).snapshotPurgeGeneration()).isZero();
  }

  // ── Classification ──────────────────────────────────────────────────────────────────────────

  @Test
  void aPrimaryKeyViolationOnTheGlobalOffsetIsACollision() {
    SQLException collision =
        new SQLiteException(
            "[SQLITE_CONSTRAINT_PRIMARYKEY] (UNIQUE constraint failed: event_stream.global_offset)",
            SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY);
    assertThat(SqliteEventStore.isGlobalOffsetCollision(collision)).isTrue();
    assertThat(SqliteEventStore.isGlobalOffsetCollision(new SQLException("x", collision))).isTrue();
    assertThat(
            SqliteEventStore.isGlobalOffsetCollision(
                new SQLiteException(
                    "[SQLITE_CONSTRAINT_PRIMARYKEY] (UNIQUE constraint failed: other.id)",
                    SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY)))
        .isFalse();
    assertThat(SqliteEventStore.isGlobalOffsetCollision(new SQLException("plain"))).isFalse();
  }

  @Test
  void aUniqueViolationAndAStaleSnapshotAreRetryableConflicts() {
    SQLException unique =
        new SQLiteException(
            "[SQLITE_CONSTRAINT_UNIQUE] (UNIQUE constraint failed: event_stream.aggregate_type,"
                + " event_stream.aggregate_id, event_stream.version)",
            SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE);
    assertThat(SqliteEventStore.isRetryableConflict(unique)).isTrue();
    assertThat(SqliteEventStore.isRetryableConflict(new SQLException("wrapped", unique))).isTrue();
    assertThat(
            SqliteEventStore.isRetryableConflict(
                new SQLiteException("stale", SQLiteErrorCode.SQLITE_BUSY_SNAPSHOT)))
        .isTrue();
    assertThat(
            SqliteEventStore.isRetryableConflict(
                new SQLiteException("busy", SQLiteErrorCode.SQLITE_BUSY)))
        .isFalse();
    assertThat(SqliteEventStore.isRetryableConflict(new SQLException("plain"))).isFalse();
  }

  @Test
  void busyIsRecognizedThroughTheCauseChain() {
    assertThat(
            SqliteEventStore.isBusy(
                new SQLException(
                    "wrapped", new SQLiteException("busy", SQLiteErrorCode.SQLITE_BUSY))))
        .isTrue();
    assertThat(
            SqliteEventStore.isBusy(
                new SQLiteException("timeout", SQLiteErrorCode.SQLITE_BUSY_TIMEOUT)))
        .isTrue();
    assertThat(
            SqliteEventStore.isBusy(new SQLiteException("locked", SQLiteErrorCode.SQLITE_LOCKED)))
        .as("SQLITE_LOCKED is not governed by busy_timeout")
        .isFalse();
    assertThat(SqliteEventStore.isBusy(new SQLException("plain"))).isFalse();
  }

  /** SQLITE_BUSY names busy_timeout; SQLITE_LOCKED, which no timeout governs, says so instead. */
  @Test
  void busyAndLockedFailuresHaveTheirOwnMessages() {
    assertThat(
            SqliteEventStore.lockFailure(
                new SQLiteException("busy", SQLiteErrorCode.SQLITE_BUSY), "append to stream 'x'"))
        .hasMessageContaining("busy_timeout")
        .hasMessageContaining("append to stream 'x'")
        .hasMessageNotContaining("SQLITE_LOCKED");
    for (SQLiteErrorCode code :
        List.of(
            SQLiteErrorCode.SQLITE_LOCKED,
            SQLiteErrorCode.SQLITE_LOCKED_SHAREDCACHE,
            SQLiteErrorCode.SQLITE_LOCKED_VTAB)) {
      assertThat(SqliteEventStore.lockFailure(new SQLiteException("locked", code), "purge"))
          .hasMessageContaining("SQLITE_LOCKED")
          .hasMessageContaining("busy_timeout does not apply");
    }
    assertThat(SqliteEventStore.lockFailure(new SQLException("plain"), "x")).isNull();
  }

  /** A real SQLITE_LOCKED: the same connection still reads the table a statement wants to drop. */
  @Test
  void aRealTableLockIsSqliteLocked() throws Exception {
    SqliteDatabase database = migrated("locked.db");
    try (Connection conn = database.dataSource().getConnection();
        Statement read = conn.createStatement();
        Statement drop = conn.createStatement()) {
      drop.execute("CREATE TABLE scratch (x INTEGER) STRICT");
      drop.execute("INSERT INTO scratch VALUES (1), (2)");
      try (var rs = read.executeQuery("SELECT x FROM scratch")) {
        rs.next();
        assertThatThrownBy(() -> drop.execute("DROP TABLE scratch"))
            .isInstanceOfSatisfying(
                SQLException.class, e -> assertThat(SqliteEventStore.isLocked(e)).isTrue());
      }
    }
  }

  @Test
  void aCyclicCauseChainEndsTheWalk() {
    SQLException first = new SQLException("first");
    SQLException second = new SQLException("second", first);
    first.initCause(second);
    assertThat(SqliteEventStore.isBusy(first)).isFalse();
    assertThat(SqliteEventStore.isRetryableConflict(first)).isFalse();
    assertThat(SqliteEventStore.isGlobalOffsetCollision(first)).isFalse();
  }

  // ── Timestamps ──────────────────────────────────────────────────────────────────────────────

  /**
   * An instant with a microsecond fraction, one before 1970 and one after 2038, read back equal: as
   * the metadata timestamp of an event on all three read paths, and as the created_at the store
   * stamps with its clock on the event row and the snapshot row.
   */
  @Test
  void timestampsRoundTripAtMicrosecondPrecision() throws Exception {
    List<Instant> instants =
        List.of(
            Instant.parse("1969-07-20T20:17:40.123456Z"),
            Instant.parse("2041-02-28T23:59:59.999999Z"),
            Instant.parse("2026-11-23T10:15:30.000001Z"));
    int n = 0;
    for (Instant instant : instants) {
      SqliteDatabase database = migrated("time-" + n++ + ".db");
      EventStore store = factory(database).clock(Clock.fixed(instant, ZoneOffset.UTC)).create();
      store.append(A, List.of(event(A, "at", instant)), Version.initial());
      store.saveSnapshot(A, new Version(1), new ContractState("s"));

      assertThat(store.load(A, EventStore.IGNORE_SNAPSHOT).events().getFirst())
          .extracting(e -> e.metadata().timestamp())
          .isEqualTo(instant);
      assertThat(store.readStream(A, Version.initial(), 1).getFirst().metadata().timestamp())
          .isEqualTo(instant);
      assertThat(
              store.readGlobalStream(GlobalOffset.initial(), 1).getFirst().metadata().timestamp())
          .isEqualTo(instant);
      long eventCreatedAt = longValue(database, "SELECT created_at FROM event_stream");
      long snapshotCreatedAt = longValue(database, "SELECT created_at FROM snapshot_store");
      assertThat(SqliteTimestamps.fromMicros(eventCreatedAt)).isEqualTo(instant);
      assertThat(SqliteTimestamps.fromMicros(snapshotCreatedAt)).isEqualTo(instant);
    }
  }

  // ── Reads and snapshots ─────────────────────────────────────────────────────────────────────

  @Test
  void aNegativeMaxCountIsRefused() throws Exception {
    EventStore store = factory(migrated("max.db")).create();
    assertThatThrownBy(() -> store.readGlobalStream(GlobalOffset.initial(), -1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxCount");
    assertThatThrownBy(() -> store.readStream(A, Version.initial(), -1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxCount");
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 0)).isEmpty();
  }

  /** SQLite stores JSON as text, so a NUL character, which Postgres' jsonb refuses, round-trips. */
  @Test
  void aNulCharacterInAPayloadRoundTrips() throws Exception {
    EventStore store = factory(migrated("nul.db")).create();
    store.append(A, List.of(event(A, "a\u0000b")), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("c\u0000d"));

    assertThat(((ContractEvent) store.readStream(A, Version.initial(), 1).getFirst().event()))
        .isEqualTo(new ContractEvent("a\u0000b"));
    assertThat(store.load(A).snapshotState()).isEqualTo(new ContractState("c\u0000d"));
  }

  /** A stream whose rows were deleted by hand loads what is left, in version order. */
  @Test
  void aStreamWithAVersionGapLoadsWhatIsStored() throws Exception {
    SqliteDatabase database = migrated("gap.db");
    EventStore store = factory(database).create();
    store.append(A, events(A, 3), Version.initial());
    execute(database, "DELETE FROM event_stream WHERE version = 2");

    assertThat(store.load(A).events()).extracting(e -> e.version().value()).containsExactly(1L, 3L);
    assertThat(store.load(A).events()).extracting(e -> e.version().value()).containsExactly(1L, 3L);
    assertThat(store.load(A, EventStore.IGNORE_SNAPSHOT).events()).hasSize(2);
  }

  /** The global read stops at a hole inside a page and pages over it on the next read. */
  @Test
  void theGlobalReadStopsAtAHoleAndPagesOverIt() throws Exception {
    SqliteDatabase database = migrated("hole.db");
    EventStore store = factory(database).create();
    store.append(A, events(A, 4), Version.initial());
    execute(database, "DELETE FROM event_stream WHERE global_offset = 2");

    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10))
        .extracting(e -> e.globalOffset().value())
        .containsExactly(1L);
    assertThat(store.readGlobalStream(GlobalOffset.of(1), 10))
        .extracting(e -> e.globalOffset().value())
        .containsExactly(3L, 4L);
  }

  /** Loads that ignore snapshots read every event, with and without a leftover snapshot row. */
  @Test
  void loadsIgnoringSnapshotsReadEveryEventWithOrWithoutALeftoverRow() throws Exception {
    EventStore store = factory(migrated("ignore.db")).create();
    store.append(A, events(A, 2), Version.initial());
    assertThat(store.load(A, EventStore.IGNORE_SNAPSHOT).events()).hasSize(2);

    EventStore other = factory(migrated("ignore-2.db")).create();
    other.append(A, events(A, 2), Version.initial());
    other.saveSnapshot(A, new Version(2), new ContractState("s"));
    for (int i = 0; i < 3; i++) {
      assertThat(other.load(A, EventStore.IGNORE_SNAPSHOT).events()).hasSize(2);
    }
  }

  @Test
  void aSnapshotStateTypeThatDoesNotResolveIsRefused() throws Exception {
    EventStore store = factory(migrated("unresolved.db")).create();
    record Unregistered(String x) implements org.streamrune.core.AggregateState {}

    assertThatThrownBy(() -> store.saveSnapshot(A, new Version(1), new Unregistered("x")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("does not resolve");
  }

  @Test
  void aFailingDatabaseFailsEveryOperationAsAnEventStoreException() throws Exception {
    SqliteDatabase database = migrated("dropped.db");
    EventStore store = factory(database).create();
    store.append(A, events(A, 1), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(database, "DROP TABLE event_stream");
    execute(database, "DROP TABLE snapshot_store");

    assertThatThrownBy(() -> store.append(A, events(A, 1), new Version(1)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to append");
    assertThatThrownBy(() -> store.load(A)).isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(() -> store.load(A, EventStore.IGNORE_SNAPSHOT))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(() -> store.readStream(A, Version.initial(), 1))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(() -> store.readGlobalStream(GlobalOffset.initial(), 1))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(store::lastGlobalOffset).isExactlyInstanceOf(EventStoreException.class);
    assertThatThrownBy(() -> store.saveSnapshot(A, new Version(1), new ContractState("s")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Failed to save snapshot");
  }

  // ── The factory ─────────────────────────────────────────────────────────────────────────────

  @Test
  void theFactoryRefusesMissingArguments() throws Exception {
    SqliteDatabase database = migrated("args.db");
    assertThatThrownBy(() -> new SqliteEventStoreFactory(null, registry()))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
    assertThatThrownBy(() -> new SqliteEventStoreFactory(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("typeRegistry");
    SqliteEventStoreFactory factory = factory(database);
    assertThatThrownBy(() -> factory.tracing(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> factory.metrics(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> factory.upcasters(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> factory.clock(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void createTwiceBuildsTwoStoresOverTheSameData() throws Exception {
    SqliteEventStoreFactory factory = factory(migrated("twice.db"));
    EventStore first = factory.create();
    EventStore second = factory.create();

    assertThat(second).isNotSameAs(first);
    first.append(A, events(A, 1), Version.initial());
    assertThat(second.load(A).events()).hasSize(1);
  }

  @Test
  void createMigratesAndValidatesAnEmptyDatabase() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("fresh.db")));
    EventStore store = factory(database).create();
    store.append(A, events(A, 1), Version.initial());
    assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void withoutAutoInitializeAnEmptyDatabaseFailsValidation() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("empty.db")));

    assertThatThrownBy(() -> factory(database).autoInitializeSchema(false).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("event_stream");
  }

  @Test
  void withBothSwitchesOffTheFactoryTouchesNoSchema() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("untouched.db")));

    EventStore store = factory(database).autoInitializeSchema(false).validateSchema(false).create();

    assertThatThrownBy(() -> store.append(A, events(A, 1), Version.initial()))
        .isExactlyInstanceOf(EventStoreException.class);
    try (Connection conn = database.dataSource().getConnection()) {
      assertThat(count(conn, "SELECT count(*) FROM sqlite_schema")).isZero();
    }
  }

  /** The @Encrypted check runs before the database is touched, as on Postgres. */
  @Test
  void anEncryptedTypeWithoutAnEngineIsRefusedBeforeTheDatabaseIsTouched() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("encrypted.db")));
    var registry =
        org.streamrune.core.SimpleEventTypeRegistry.builder()
            .registerEvent(EventStoreContract.EVENT_TYPE, ContractEvent.class)
            .registerState("Secret", Secret.class)
            .build();

    assertThatThrownBy(() -> new SqliteEventStoreFactory(database, registry).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("email");
    try (Connection conn = database.dataSource().getConnection()) {
      assertThat(count(conn, "SELECT count(*) FROM sqlite_schema")).isZero();
    }
    assertThat(
            new SqliteEventStoreFactory(database, registry)
                .cryptoEngine(new InMemoryCryptoEngine())
                .create())
        .isNotNull();
  }

  /** A state type carrying an encrypted field. */
  record Secret(String id, @org.streamrune.core.crypto.Encrypted(subjectId = "id") String email)
      implements org.streamrune.core.AggregateState {}

  /**
   * An engine that needs crypto tables makes the factory apply the crypto series, whose scripts
   * streamrune-sqlite-crypto ships (refused here, naming it, where that module is absent); with
   * auto-initialization off it refuses a database without the tables, naming them; once they exist,
   * the store is built. streamrune-sqlite-crypto checks the migrated case.
   */
  @Test
  void anEngineThatNeedsCryptoTablesRequiresThem() throws Exception {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve("crypto.db")));
    CryptoEngine needsTables = new NeedsTablesEngine();

    assertThatThrownBy(() -> factory(database).cryptoEngine(needsTables).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("db/sqlite-crypto-migration/V001__crypto.sql")
        .hasMessageContaining("streamrune-sqlite-crypto");
    assertThatThrownBy(
            () -> factory(database).cryptoEngine(needsTables).autoInitializeSchema(false).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("forgotten_subjects")
        .hasMessageContaining("CRYPTO");
    execute(database, "CREATE TABLE forgotten_subjects (subject_id TEXT PRIMARY KEY)");
    assertThat(factory(database).cryptoEngine(needsTables).autoInitializeSchema(false).create())
        .isNotNull();
    assertThat(
            factory(database)
                .cryptoEngine(needsTables)
                .autoInitializeSchema(false)
                .validateSchema(false)
                .create())
        .isNotNull();
  }

  /** An engine that keeps its tombstones in a table of the database. */
  private static final class NeedsTablesEngine implements CryptoEngine {
    private final CryptoEngine keys = new InMemoryCryptoEngine();

    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return keys.encrypt(subjectId, plaintext);
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      return keys.decrypt(subjectId, ciphertext);
    }

    @Override
    public void deleteKey(SubjectId subjectId) {
      keys.deleteKey(subjectId);
    }

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return keys.isKeyAvailable(subjectId);
    }

    @Override
    public java.util.Set<String> requiredCryptoTables() {
      return java.util.Set.of("forgotten_subjects");
    }
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    resources.add(database);
    return database;
  }

  private HikariDataSource pool(Path file, int size) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:sqlite:file:" + file + "?" + TestDatabases.REQUIRED_SETTINGS);
    config.setMaximumPoolSize(size);
    config.setPoolName("sqlite-event-store-test");
    HikariDataSource pool = new HikariDataSource(config);
    resources.add(pool);
    return pool;
  }

  private SqliteDatabase migrated(String name) {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve(name)));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    return database;
  }

  private static SqliteEventStoreFactory factory(SqliteDatabase database) {
    return new SqliteEventStoreFactory(database, registry());
  }

  private static org.streamrune.core.EventTypeRegistry registry() {
    return EventStoreContract.Setup.defaults().registry();
  }

  private static List<EventEnvelope> events(StreamId stream, int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(event(stream, "e" + i));
    }
    return events;
  }

  private static EventEnvelope event(StreamId stream, String value) {
    return event(stream, value, Instant.parse("2026-11-23T10:00:00Z"));
  }

  private static EventEnvelope event(StreamId stream, String value, Instant timestamp) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        EventStoreContract.EVENT_TYPE,
        new ContractEvent(value),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            timestamp));
  }

  private static String insertRow(String offset, long version) {
    String columns =
        "aggregate_type, aggregate_id, version, event_type, payload, metadata, created_at";
    String values = "'order', 'X', " + version + ", 'ContractEvent', '{}', '{}', 0";
    return offset == null
        ? "INSERT INTO event_stream (" + columns + ") VALUES (" + values + ")"
        : "INSERT INTO event_stream (global_offset, "
            + columns
            + ") VALUES ("
            + offset
            + ", "
            + values
            + ")";
  }

  private static long counter(SqliteDatabase database) throws SQLException {
    return longValue(database, "SELECT next_value FROM global_offset_counter WHERE id = 1");
  }

  private static long longValue(SqliteDatabase database, String sql) throws SQLException {
    try (Connection conn = database.dataSource().getConnection()) {
      return count(conn, sql);
    }
  }

  private static long count(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement();
        var rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void execute(SqliteDatabase database, String sql) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  /** Another connection gets the write lock at once: nothing holds it. */
  private static void assertWriteLockFree(Path file) throws SQLException {
    try (Connection probe = TestDatabases.dataSource(file, "busy_timeout=1").getConnection()) {
      probe.setAutoCommit(false);
      probe.rollback();
      probe.setAutoCommit(true);
    }
  }

  /**
   * {@code source}, its connections switched to manual commit while {@code manual} is set, as a
   * pool reconfigured or an application transaction enlisting the connection.
   */
  private static DataSource manualCommit(DataSource source, AtomicBoolean manual) {
    return new DataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        Connection conn = source.getConnection();
        if (manual.get()) {
          conn.setAutoCommit(false);
        }
        return conn;
      }

      @Override
      public Connection getConnection(String user, String password) throws SQLException {
        return getConnection();
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
      public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
      }

      @Override
      public <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLException("not a wrapper");
      }

      @Override
      public boolean isWrapperFor(Class<?> type) {
        return false;
      }
    };
  }
}

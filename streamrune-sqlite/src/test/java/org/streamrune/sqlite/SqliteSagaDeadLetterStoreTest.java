package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaDeadLetterStore.HoldResult;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.SagaStoreContract.ContractState;

/**
 * What {@link SqliteSagaDeadLetterStore} adds to the shared contract: the entry and its shield are
 * one transaction (a write killed between them leaves neither), concurrent holds of one event write
 * it once, a clear racing a shielded publish never leaves an entry unshielded, every write waits
 * for the write lock at most {@code busy_timeout}, timestamps keep their microseconds, and
 * retention drains in batches.
 */
class SqliteSagaDeadLetterStoreTest {

  private static final SagaType TYPE = SagaType.fromClass(ContractState.class);
  private static final SagaId A = SagaId.of("dl-sqlite-a");
  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");
  private static final ContractState RUNNING = new ContractState(SagaStatus.RUNNING, "r");
  private static final long BUSY_MILLIS = 200;
  private static final long SLACK_MILLIS = 1_500;

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();

  @AfterEach
  void close() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private SqliteDatabase migrated(String file, String override) {
    SqliteDatabase database =
        open(
            override.isEmpty()
                ? TestDatabases.dataSource(dir.resolve(file))
                : TestDatabases.dataSource(dir.resolve(file), override));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  private static SqliteSagaStore sagas(SqliteDatabase database) {
    return new SqliteSagaStore(database, new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  private static SagaDeadLetterEntry entry(SagaId sagaId, long offset, Instant faultedAt) {
    return new SagaDeadLetterEntry(
        sagaId,
        TYPE,
        GlobalOffset.of(offset),
        EventType.of("Evt"),
        "java.lang.RuntimeException",
        "boom",
        faultedAt);
  }

  private static SagaDeadLetterEntry held(SagaId sagaId, long offset, Instant faultedAt) {
    return new SagaDeadLetterEntry(
        sagaId,
        TYPE,
        GlobalOffset.of(offset),
        EventType.of("Evt"),
        "org.streamrune.runtime.SagaEventHeldException",
        "held",
        faultedAt);
  }

  @Test
  void rejectsANullDatabase() {
    assertThatThrownBy(() -> new SqliteSagaDeadLetterStore(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
  }

  /**
   * A shielded publish killed inside its shield write (the entry is already upserted in the same
   * transaction) leaves neither the entry nor the shield; killed right after its COMMIT, both. The
   * rerun converges to one entry and a set shield either way.
   */
  @Test
  void aShieldedPublishKilledBetweenEntryAndShieldLeavesNeither_andItsRerunConverges() {
    Path file = dir.resolve("crash-publish.db");
    SqliteDatabase plain = migrated("crash-publish.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    SqliteSagaDeadLetterStore store =
        new SqliteSagaDeadLetterStore(open(crash.connections(TestDatabases.dataSource(file))));
    SqliteSagaStore sagas = sagas(plain);
    sagas.create(A, TYPE, RUNNING, SagaStatus.RUNNING);

    crash.arm(new Point("saga_state", 1));
    assertThatThrownBy(() -> store.publishShielded(entry(A, 5, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(store.findBySaga(A)).as("no entry without its shield").isEmpty();
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isFalse();

    crash.arm(new Point("saga_dead_letters", 1));
    assertThatThrownBy(() -> store.publishShielded(entry(A, 5, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(store.findBySaga(A)).isEmpty();
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isFalse();

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.publishShielded(entry(A, 5, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(store.findBySaga(A)).hasSize(1);
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isTrue();

    store.publishShielded(entry(A, 5, T0.plusSeconds(1)));
    assertThat(store.findBySaga(A))
        .singleElement()
        .satisfies(
            e -> {
              assertThat(e.faultedAt()).isEqualTo(T0.plusSeconds(1));
              assertThat(e.firstFaultedAt()).isEqualTo(T0);
            });
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isTrue();
  }

  /**
   * A hold killed inside its shield write leaves nothing; killed right after its COMMIT it left the
   * entry and the shield, and its rerun keeps that entry: the event is held once either way.
   */
  @Test
  void aHoldKilledInsideItsShieldWriteLeavesNothing_afterItsCommitBoth_andItsRerunHoldsItOnce() {
    Path file = dir.resolve("crash-hold.db");
    SqliteDatabase plain = migrated("crash-hold.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    SqliteSagaDeadLetterStore store =
        new SqliteSagaDeadLetterStore(open(crash.connections(TestDatabases.dataSource(file))));
    SqliteSagaStore sagas = sagas(plain);
    sagas.create(A, TYPE, RUNNING, SagaStatus.RUNNING);

    crash.arm(new Point("saga_state", 1));
    assertThatThrownBy(() -> store.publishShieldedIfAbsent(held(A, 9, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(store.findBySaga(A)).isEmpty();
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isFalse();

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.publishShieldedIfAbsent(held(A, 9, T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(store.findBySaga(A)).as("killed after its COMMIT: the entry is durable").hasSize(1);
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending())
        .as("and so is its shield")
        .isTrue();

    assertThat(store.publishShieldedIfAbsent(held(A, 9, T0.plusSeconds(5))))
        .as("the rerun keeps the entry the committed hold wrote")
        .isInstanceOf(HoldResult.Kept.class);
    assertThat(store.findBySaga(A))
        .singleElement()
        .satisfies(e -> assertThat(e.faultedAt()).isEqualTo(T0));
  }

  /** "Two concurrent holds of one event write it once": exactly one is Written, the rest Kept. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentHoldsOfOneEventWriteItOnce() throws Exception {
    SqliteDatabase database = migrated("holds.db", "");
    SqliteSagaStore sagas = sagas(database);
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(database);
    sagas.create(A, TYPE, RUNNING, SagaStatus.RUNNING);
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < 5; round++) {
        long offset = 100 + round;
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Future<HoldResult>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          Instant at = T0.plusSeconds(t);
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    return store.publishShieldedIfAbsent(held(A, offset, at));
                  }));
        }
        List<HoldResult> results = new ArrayList<>();
        for (Future<HoldResult> future : futures) {
          results.add(future.get());
        }

        assertThat(results).filteredOn(r -> r instanceof HoldResult.Written).hasSize(1);
        assertThat(results).filteredOn(r -> r instanceof HoldResult.Kept).hasSize(threads - 1);
        assertThat(store.findBySaga(A))
            .filteredOn(e -> e.eventOffset().value() == offset)
            .hasSize(1);
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * A conditional clear racing a shielded publish: whichever order the write lock gives them, an
   * entry that exists afterwards has its owner shielded.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aClearRacingAShieldedPublishNeverLeavesAnEntryUnshielded() throws Exception {
    SqliteDatabase database = migrated("clear-race.db", "");
    SqliteSagaStore sagas = sagas(database);
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(database);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 20; round++) {
        SagaId sagaId = SagaId.of("race-" + round);
        sagas.create(sagaId, TYPE, RUNNING, SagaStatus.RUNNING, true);
        CyclicBarrier start = new CyclicBarrier(2);
        Future<?> publish =
            pool.submit(
                () -> {
                  start.await();
                  store.publishShielded(entry(sagaId, 1, T0));
                  return null;
                });
        Future<?> clear =
            pool.submit(
                () -> {
                  start.await();
                  store.clearShieldIfDrained(sagaId, TYPE);
                  return null;
                });
        publish.get();
        clear.get();

        assertThat(store.findBySaga(sagaId)).hasSize(1);
        assertThat(sagas.loadRowFacts(sagaId, TYPE).orElseThrow().deadLetterPending())
            .as("round " + round)
            .isTrue();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void everyWriteWaitsAtMostBusyTimeoutForTheWriteLock_andReadsDoNotWait() throws Exception {
    SqliteDatabase database = migrated("busy.db", "busy_timeout=" + BUSY_MILLIS);
    SqliteSagaStore sagas = sagas(database);
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(database);
    sagas.create(A, TYPE, RUNNING, SagaStatus.RUNNING);
    store.publish(entry(null, 3, T0));
    Map<String, Runnable> writes = new LinkedHashMap<>();
    writes.put("publish named", () -> store.publish(entry(A, 1, T0)));
    writes.put("publish null saga", () -> store.publish(entry(null, 2, T0)));
    writes.put("publishShielded", () -> store.publishShielded(entry(A, 1, T0)));
    writes.put("publishShieldedIfAbsent", () -> store.publishShieldedIfAbsent(held(A, 1, T0)));
    writes.put(
        "establishFirstReplayAnchor",
        () -> store.establishFirstReplayAnchor(null, TYPE, GlobalOffset.of(3), T0));
    writes.put("setResolvedTarget", () -> store.setResolvedTarget(TYPE, GlobalOffset.of(3), A));
    writes.put("discard", () -> store.discard(null, TYPE, GlobalOffset.of(3)));
    writes.put("clearShieldIfDrained", () -> store.clearShieldIfDrained(A, TYPE));
    writes.put("deleteOlderThan", () -> store.deleteOlderThan(T0.plusSeconds(60)));

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      for (var write : writes.entrySet()) {
        long started = System.nanoTime();
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("busy_timeout");
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
            .as(write.getKey() + " waited")
            .isLessThan(BUSY_MILLIS + SLACK_MILLIS);
      }
      // Reads never take the write lock the holder keeps: one that did would fail as SQLITE_BUSY.
      assertThat(store.findAll(10)).hasSize(1);
      assertThat(store.findBySaga(A)).isEmpty();
      assertThat(store.findNullSagaEntry(TYPE, GlobalOffset.of(3))).isPresent();
      assertThat(store.findNullSagaEntriesByResolvedTarget(TYPE, A)).isEmpty();
      assertThat(store.countFaultedBacklog()).isZero();
      st.execute("ROLLBACK");
    }

    assertThat(store.findAll(10)).as("nothing was written").hasSize(1);
    assertThat(store.findNullSagaEntry(TYPE, GlobalOffset.of(3)).orElseThrow().targetSagaId())
        .isNull();
    store.publishShielded(entry(A, 1, T0));
    assertThat(sagas.loadRowFacts(A, TYPE).orElseThrow().deadLetterPending()).isTrue();
  }

  /** Timestamps keep their microseconds, and the retention bound compares them exactly. */
  @Test
  void timestampsKeepMicroseconds_andRetentionComparesThemExactly() {
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(migrated("micros.db", ""));
    Instant anchor = Instant.parse("2026-11-23T10:00:01.654321Z");
    store.publish(entry(null, 4, T0));
    store.establishFirstReplayAnchor(null, TYPE, GlobalOffset.of(4), anchor);

    var stored = store.findNullSagaEntry(TYPE, GlobalOffset.of(4)).orElseThrow();
    assertThat(stored.faultedAt()).isEqualTo(T0);
    assertThat(stored.firstFaultedAt()).isEqualTo(T0);
    assertThat(stored.firstReplayStartedAt()).isEqualTo(anchor);

    assertThat(store.deleteOlderThan(Instant.parse("2026-11-23T10:00:00.123456Z")))
        .as("an entry exactly at the bound is not older than it")
        .isZero();
    assertThat(store.deleteOlderThan(Instant.parse("2026-11-23T10:00:00.123455Z"))).isZero();
    assertThat(store.deleteOlderThan(Instant.parse("2026-11-23T10:00:00.123457Z"))).isEqualTo(1);
  }

  /** Retention deletes in batches until a batch deletes nothing, keeping every protected entry. */
  @Test
  void retentionDrainsEveryPrunableEntryInBatches_andKeepsTheProtectedOnes() throws Exception {
    SqliteDatabase database = migrated("retention.db", "");
    SqliteSagaStore sagas = sagas(database);
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(database);
    int prunable = 2_500;
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        PreparedStatement ps =
            conn.prepareStatement(
                "INSERT INTO saga_dead_letters (saga_id, saga_type, event_offset, event_type,"
                    + " error_type, faulted_at, first_faulted_at) VALUES (NULL, ?, ?, 'Evt',"
                    + " 'E', ?, ?)")) {
      st.execute("BEGIN IMMEDIATE");
      for (int i = 0; i < prunable; i++) {
        ps.setString(1, TYPE.value());
        ps.setLong(2, i);
        ps.setLong(3, SqliteTimestamps.toMicros(T0));
        ps.setLong(4, SqliteTimestamps.toMicros(T0));
        ps.addBatch();
      }
      ps.executeBatch();
      st.execute("COMMIT");
    }
    sagas.create(A, TYPE, RUNNING, SagaStatus.RUNNING);
    store.publishShielded(entry(A, 1, T0));

    assertThat(store.deleteOlderThan(T0.plusSeconds(1))).isEqualTo(prunable);
    assertThat(store.findAll(10_000))
        .singleElement()
        .satisfies(e -> assertThat(e.sagaId()).isEqualTo(A));
    assertThat(store.countFaultedBacklog()).isEqualTo(1);
  }

  @Test
  void aMissingTableIsAStorageFailureForEveryReadAndWrite() throws SQLException {
    SqliteDatabase database = migrated("missing.db", "");
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(database);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE saga_dead_letters");
    }
    List<Runnable> calls =
        List.of(
            () -> store.publish(entry(A, 1, T0)),
            () -> store.publish(entry(null, 1, T0)),
            () -> store.publishShielded(entry(A, 1, T0)),
            () -> store.publishShieldedIfAbsent(held(A, 1, T0)),
            () -> store.establishFirstReplayAnchor(A, TYPE, GlobalOffset.of(1), T0),
            () -> store.setResolvedTarget(TYPE, GlobalOffset.of(1), A),
            () -> store.discard(A, TYPE, GlobalOffset.of(1)),
            () -> store.clearShieldIfDrained(A, TYPE),
            () -> store.deleteOlderThan(T0),
            () -> store.findAll(1),
            () -> store.findBySaga(A),
            () -> store.findNullSagaEntry(TYPE, GlobalOffset.of(1)),
            () -> store.findNullSagaEntriesByResolvedTarget(TYPE, A),
            () -> store.countFaultedBacklog());
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(SQLException.class);
    }
  }

  /**
   * The discard and the anchor write are index searches in both shapes, as on Postgres: the saga
   * id, or its absence, the saga type and the offset are the constraints of a search of the unique
   * key's index, never a scan of the table.
   */
  @Test
  void theDiscardAndTheAnchorWriteSearchTheUniqueKeysIndex_namedAndNullSaga() throws SQLException {
    SqliteDatabase database = migrated("plan.db", "");
    for (String sql :
        List.of(
            SqliteSagaDeadLetterStore.DISCARD_NAMED,
            SqliteSagaDeadLetterStore.DISCARD_NULL_SAGA,
            SqliteSagaDeadLetterStore.ESTABLISH_FIRST_REPLAY_ANCHOR_NAMED,
            SqliteSagaDeadLetterStore.ESTABLISH_FIRST_REPLAY_ANCHOR_NULL_SAGA)) {
      StringBuilder plan = new StringBuilder();
      try (Connection conn = database.dataSource().getConnection();
          PreparedStatement ps = conn.prepareStatement("EXPLAIN QUERY PLAN " + sql);
          var rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.append(rs.getString("detail")).append('\n');
        }
      }
      assertThat(plan.toString())
          .as(sql)
          .contains("SEARCH saga_dead_letters USING")
          .contains("(saga_id=? AND saga_type=? AND event_offset=?)")
          .doesNotContain("SCAN");
    }
  }

  @Test
  void aNegativeLimitIsRefused() {
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(migrated("limit.db", ""));

    assertThatThrownBy(() -> store.findAll(-1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    assertThat(store.findAll(0)).isEmpty();
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedBeforeAnythingIsWritten() {
    SqliteDatabase database = migrated("manual.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    SqliteSagaDeadLetterStore store = new SqliteSagaDeadLetterStore(open(source));
    source.manualCommit();

    assertThatThrownBy(() -> store.publishShielded(entry(A, 1, T0)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    assertThatThrownBy(() -> store.publishShieldedIfAbsent(held(A, 1, T0)))
        .isExactlyInstanceOf(IllegalStateException.class);
    assertThat(new SqliteSagaDeadLetterStore(database).findAll(10)).isEmpty();
  }
}

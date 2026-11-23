package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.MutableClock;

/**
 * What {@link SqliteSagaStore} adds to the shared contract: the application clock stamps every
 * timestamp, a write waits for the write lock at most {@code busy_timeout} and then fails as a
 * storage failure, a connection in another transaction or in manual-commit mode is refused, CAS
 * writes racing on one version let exactly one through, and a write killed half way leaves the row
 * as it was.
 */
class SqliteSagaStoreTest {

  record StepState(SagaStatus status, String step) implements SagaState {
    @JsonCreator
    StepState(@JsonProperty("status") SagaStatus status, @JsonProperty("step") String step) {
      this.status = status;
      this.step = step;
    }
  }

  private static final SagaType TYPE = SagaType.fromClass(StepState.class);
  private static final SagaId ID = SagaId.of("sqlite-saga-1");
  private static final StepState RUNNING = new StepState(SagaStatus.RUNNING, "running");
  private static final StepState COMPENSATING =
      new StepState(SagaStatus.COMPENSATING, "compensating");
  private static final long BUSY_MILLIS = 200;
  private static final long SLACK_MILLIS = 1_500;

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final List<SingleConnection> connections = new ArrayList<>();

  @AfterEach
  void close() throws SQLException {
    handles.forEach(SqliteDatabase::close);
    for (SingleConnection connection : connections) {
      connection.close();
    }
  }

  private static ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  /** A migrated database on {@code file} with the required settings plus {@code override}. */
  private SqliteDatabase migrated(String file, String override) {
    SqliteDatabase database =
        open(
            override.isEmpty()
                ? TestDatabases.dataSource(dir.resolve(file))
                : TestDatabases.dataSource(dir.resolve(file), override));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  @Test
  void rejectsNullArguments() {
    SqliteDatabase database = migrated("nulls.db", "");

    assertThatThrownBy(() -> new SqliteSagaStore(null, mapper()))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("database");
    assertThatThrownBy(() -> new SqliteSagaStore(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("objectMapper");
    assertThatThrownBy(() -> new SqliteSagaStore(database, mapper(), null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clock");
  }

  @Test
  void storageIdentityIsTheDatabaseFile_soEveryHandleOnTheFileIsOneStore() {
    SqliteDatabase database = migrated("identity.db", "");
    SqliteDatabase second = open(TestDatabases.dataSource(database.file()));
    SqliteDatabase other = migrated("other.db", "");

    Object identity = new SqliteSagaStore(database, mapper()).storageIdentity();

    assertThat(identity).isEqualTo(database.file());
    assertThat(new SqliteSagaStore(second, mapper()).storageIdentity()).isEqualTo(identity);
    assertThat(new SqliteSagaStore(other, mapper()).storageIdentity()).isNotEqualTo(identity);
  }

  @Test
  void everyTimestampIsTheApplicationClockInMicroseconds() throws Exception {
    SqliteDatabase database = migrated("clock.db", "");
    Instant created = Instant.parse("2026-11-23T10:00:00.123456Z");
    MutableClock clock = MutableClock.startingAt(created);
    SqliteSagaStore store = new SqliteSagaStore(database, mapper(), clock);

    store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING);
    assertThat(column(database, "created_at")).isEqualTo(created);
    assertThat(column(database, "updated_at")).isEqualTo(created);

    Instant updated = created.plus(Duration.ofMinutes(1));
    clock.setInstant(updated);
    store.update(ID, TYPE, RUNNING, SagaStatus.RUNNING, 1L);
    assertThat(column(database, "created_at")).as("the start instant is kept").isEqualTo(created);
    assertThat(column(database, "updated_at")).isEqualTo(updated);

    Instant claimed = updated.plus(Duration.ofMinutes(1));
    clock.setInstant(claimed);
    store.claimCompensating(ID, TYPE, COMPENSATING, 2L);
    assertThat(store.load(ID, TYPE, StepState.class).orElseThrow().episodeClaimedAt())
        .isEqualTo(claimed);
    assertThat(column(database, "updated_at")).isEqualTo(claimed);

    clock.setInstant(claimed.plus(Duration.ofMinutes(1)));
    store.setDeadLetterPending(ID, TYPE, true);
    assertThat(column(database, "updated_at")).as("the shield is no activity").isEqualTo(claimed);

    Instant faulted = claimed.plus(Duration.ofMinutes(2));
    clock.setInstant(faulted);
    store.markFaulted(ID, TYPE, 3L);
    assertThat(column(database, "updated_at")).isEqualTo(faulted);
    assertThat(store.load(ID, TYPE, StepState.class).orElseThrow().episodeClaimedAt())
        .as("the claim instant survives the fault")
        .isEqualTo(claimed);

    var compensating = store.findCompensating(TYPE, faulted.plusSeconds(1), 10);
    assertThat(compensating).isEmpty(); // FAULTED, not COMPENSATING
    SagaId born = SagaId.of("born-compensating");
    store.createGenesisPending(
        born, TYPE, COMPENSATING, SagaStatus.COMPENSATING, false, EventId.of("evt-1"));
    var found = store.findCompensating(TYPE, faulted.plusSeconds(1), 10);
    assertThat(found)
        .singleElement()
        .satisfies(
            c -> {
              assertThat(c.sagaId()).isEqualTo(born);
              assertThat(c.updatedAt()).isEqualTo(faulted);
              assertThat(c.episodeClaimedAt()).isEqualTo(faulted);
            });
  }

  /**
   * The row-write bound: while another connection holds SQLite's write lock, every write waits at
   * most {@code busy_timeout} and then fails as a storage failure ({@link EventStoreException}, the
   * type Postgres gives a write past its bound), writing nothing; reads do not wait at all.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void everyWriteWaitsAtMostBusyTimeoutForTheWriteLock_andReadsDoNotWait() throws Exception {
    SqliteDatabase database = migrated("busy.db", "busy_timeout=" + BUSY_MILLIS);
    SqliteSagaStore store = new SqliteSagaStore(database, mapper());
    store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING);
    SagaId other = SagaId.of("never-written");
    Map<String, Runnable> writes =
        Map.of(
            "create", () -> store.create(other, TYPE, RUNNING, SagaStatus.RUNNING),
            "createGenesisPending",
                () -> store.createGenesisPending(other, TYPE, RUNNING, SagaStatus.RUNNING, false),
            "createFaulted", () -> store.createFaulted(other, TYPE, RUNNING),
            "update", () -> store.update(ID, TYPE, RUNNING, SagaStatus.RUNNING, 1L),
            "applyEvent",
                () ->
                    store.applyEvent(
                        ID,
                        TYPE,
                        RUNNING,
                        SagaStatus.RUNNING,
                        1L,
                        AppliedEvent.live(GlobalOffset.of(1))),
            "claimCompensating", () -> store.claimCompensating(ID, TYPE, COMPENSATING, 1L),
            "markFaulted", () -> store.markFaulted(ID, TYPE, 1L),
            "setDeadLetterPending", () -> store.setDeadLetterPending(ID, TYPE, true),
            "delete", () -> store.delete(ID, TYPE));

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      for (var write : writes.entrySet()) {
        long started = System.nanoTime();
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("busy_timeout");
        long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertThat(waited).as(write.getKey() + " waited").isLessThan(BUSY_MILLIS + SLACK_MILLIS);
      }
      // Reads run in autocommit and, under WAL, never wait for the writer.
      long started = System.nanoTime();
      assertThat(store.load(ID, TYPE, StepState.class)).isPresent();
      assertThat(store.loadRowFacts(ID, TYPE)).isPresent();
      assertThat(store.findTimedOut(TYPE, Instant.now().plusSeconds(60), 10)).containsExactly(ID);
      assertThat(store.findByStatus(TYPE, SagaStatus.RUNNING, 10)).containsExactly(ID);
      assertThat(store.countByStatus(TYPE, SagaStatus.RUNNING)).isEqualTo(1);
      assertThat(store.findCompensating(TYPE, Instant.now().plusSeconds(60), 10)).isEmpty();
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))
          .isLessThan(BUSY_MILLIS);
      st.execute("ROLLBACK");
    }

    var row = store.load(ID, TYPE, StepState.class).orElseThrow();
    assertThat(row.version()).as("nothing was written").isEqualTo(1L);
    assertThat(row.deadLetterPending()).isFalse();
    assertThat(store.load(other, TYPE, StepState.class)).isEmpty();
    store.claimCompensating(ID, TYPE, COMPENSATING, 1L);
    assertThat(store.load(ID, TYPE, StepState.class).orElseThrow().status())
        .isEqualTo(SagaStatus.COMPENSATING);
  }

  /**
   * The application left a BEGIN open on the connection: a write would join that transaction and
   * not be durable when it returns. It is refused instead, writes nothing and leaves the
   * application's transaction alone.
   */
  @Test
  void aWriteOnAConnectionWithAnOpenTransactionIsRefusedAndLeavesThatTransactionAlone()
      throws Exception {
    SqliteDatabase database = migrated("foreign.db", "");
    new SqliteSagaStore(database, mapper()).create(ID, TYPE, RUNNING, SagaStatus.RUNNING);
    SingleConnection connection = new SingleConnection(TestDatabases.dataSource(database.file()));
    connections.add(connection);
    SqliteSagaStore store = new SqliteSagaStore(open(connection), mapper());
    try (Connection app = connection.getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      st.execute("CREATE TABLE app_marker (x INTEGER) STRICT");

      assertThatThrownBy(() -> store.create(SagaId.of("x"), TYPE, RUNNING, SagaStatus.RUNNING))
          .isExactlyInstanceOf(EventStoreException.class);
      assertThatThrownBy(() -> store.claimCompensating(ID, TYPE, COMPENSATING, 1L))
          .isExactlyInstanceOf(EventStoreException.class);
      assertThatThrownBy(() -> store.markFaulted(ID, TYPE, 1L))
          .isExactlyInstanceOf(EventStoreException.class);

      try (ResultSet rs = st.executeQuery("SELECT count(*) FROM app_marker")) {
        assertThat(rs.next()).as("the application's transaction is still open").isTrue();
      }
      st.execute("ROLLBACK");
    }
    var row = new SqliteSagaStore(database, mapper()).load(ID, TYPE, StepState.class);
    assertThat(row.orElseThrow().version()).isEqualTo(1L);
  }

  /**
   * A connection handed out in manual-commit mode would hold the write lock while idle and its
   * transaction is not the store's to commit: the write is refused as a storage failure whose cause
   * names the mode, and writes nothing.
   */
  @Test
  void aConnectionInManualCommitModeIsRefusedAsAStorageFailure() {
    SqliteDatabase database = migrated("manual.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    SqliteSagaStore store = new SqliteSagaStore(open(source), mapper());
    store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING);
    source.manualCommit();

    assertThatThrownBy(() -> store.update(ID, TYPE, RUNNING, SagaStatus.RUNNING, 1L))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .cause()
        .hasMessageContaining("autoCommit=false");
    assertThatThrownBy(() -> store.delete(ID, TYPE))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasCauseInstanceOf(IllegalStateException.class);

    var row = new SqliteSagaStore(database, mapper()).load(ID, TYPE, StepState.class);
    assertThat(row.orElseThrow().version()).isEqualTo(1L);
  }

  /** Concurrent claims of one version, on connections of their own: exactly one lands. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentClaimsOfOneVersion_exactlyOneLands_theOthersSeeAStaleView() throws Exception {
    SqliteDatabase database = migrated("claims.db", "");
    SqliteSagaStore store = new SqliteSagaStore(database, mapper());
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < 5; round++) {
        SagaId sagaId = SagaId.of("claimed-" + round);
        store.create(sagaId, TYPE, RUNNING, SagaStatus.RUNNING);
        CyclicBarrier start = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<Throwable> losers = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          String cause = "evt-" + t;
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    try {
                      store.claimCompensating(sagaId, TYPE, COMPENSATING, 1L, EventId.of(cause));
                    } catch (RuntimeException e) {
                      losers.add(e);
                    }
                    return null;
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }

        assertThat(losers).hasSize(threads - 1);
        assertThat(losers)
            .allSatisfy(e -> assertThat(e).isExactlyInstanceOf(OptimisticLockException.class));
        var row = store.load(sagaId, TYPE, StepState.class).orElseThrow();
        assertThat(row.version()).isEqualTo(2L);
        assertThat(row.episodeVersion()).isEqualTo(2L);
        assertThat(row.episodeCauseEventId()).isNotNull();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** Concurrent creates of one saga: exactly one inserts, the others see the benign dedup. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentCreatesOfOneSaga_exactlyOneInserts() throws Exception {
    SqliteDatabase database = migrated("creates.db", "");
    SqliteSagaStore store = new SqliteSagaStore(database, mapper());
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int round = 0; round < 5; round++) {
        SagaId sagaId = SagaId.of("created-" + round);
        CyclicBarrier start = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<Throwable> losers = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    try {
                      store.createGenesisPending(sagaId, TYPE, RUNNING, SagaStatus.RUNNING, false);
                    } catch (RuntimeException e) {
                      losers.add(e);
                    }
                    return null;
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }

        assertThat(losers).hasSize(threads - 1);
        assertThat(losers)
            .allSatisfy(e -> assertThat(e).isExactlyInstanceOf(OptimisticLockException.class));
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * A fault write killed inside its UPDATE (after its CAS read in the same transaction) leaves the
   * row as it was; the rerun converges. Killed right after its COMMIT, the fault is durable whole.
   */
  @Test
  void aFaultWriteKilledHalfWayLeavesTheRow_andItsRerunConverges() throws Exception {
    Path file = dir.resolve("crash-fault.db");
    migrated("crash-fault.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    SqliteSagaStore store =
        new SqliteSagaStore(open(crash.connections(TestDatabases.dataSource(file))), mapper());
    store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING);

    crash.arm(new Point("saga_state", 1));
    assertThatThrownBy(() -> store.markFaulted(ID, TYPE, 1L))
        .isExactlyInstanceOf(EventStoreException.class);
    var untouched = store.load(ID, TYPE, StepState.class).orElseThrow();
    assertThat(untouched.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(untouched.version()).isEqualTo(1L);

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.markFaulted(ID, TYPE, 1L))
        .isExactlyInstanceOf(EventStoreException.class);
    var faulted = store.load(ID, TYPE, StepState.class).orElseThrow();
    assertThat(faulted.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(faulted.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(faulted.version()).isEqualTo(2L);
    // The caller's rerun with its old version is a stale view; with the new one a refresh.
    assertThatThrownBy(() -> store.markFaulted(ID, TYPE, 1L))
        .isExactlyInstanceOf(OptimisticLockException.class);
    assertThat(store.markFaulted(ID, TYPE, 2L)).as("a still-poison refresh").isFalse();
  }

  /**
   * The forward step's CAS (state, status, offsets and the episode stamp in one statement) killed
   * inside its UPDATE leaves the row as it was: no stamp without its state, no offset without its
   * state. The redelivery reruns it and lands it once.
   */
  @Test
  void aForwardStepKilledInsideItsCasLeavesTheRow_andItsRedeliveryLandsItOnce() throws Exception {
    Path file = dir.resolve("crash-step.db");
    migrated("crash-step.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    SqliteSagaStore store =
        new SqliteSagaStore(open(crash.connections(TestDatabases.dataSource(file))), mapper());
    store.createGenesisPending(ID, TYPE, RUNNING, SagaStatus.RUNNING, false);
    AppliedEvent start =
        AppliedEvent.liveStart(GlobalOffset.of(7)).withEventId(EventId.of("evt-7"));

    crash.arm(new Point("saga_state", 1));
    assertThatThrownBy(
            () -> store.applyEvent(ID, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 1L, start))
        .isExactlyInstanceOf(EventStoreException.class);
    var untouched = store.load(ID, TYPE, StepState.class).orElseThrow();
    assertThat(untouched.version()).isEqualTo(1L);
    assertThat(untouched.genesisApplied()).isFalse();
    assertThat(untouched.lastAppliedOffset()).isNull();
    assertThat(untouched.episodeVersion()).isNull();

    store.applyEvent(ID, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 1L, start);
    var landed = store.load(ID, TYPE, StepState.class).orElseThrow();
    assertThat(landed.version()).isEqualTo(2L);
    assertThat(landed.genesisApplied()).isTrue();
    assertThat(landed.lastAppliedOffset()).isEqualTo(7L);
    assertThat(landed.episodeVersion()).isEqualTo(2L);
    assertThat(landed.episodeCauseEventId()).isEqualTo(EventId.of("evt-7"));
    assertThatThrownBy(
            () -> store.applyEvent(ID, TYPE, COMPENSATING, SagaStatus.COMPENSATING, 1L, start))
        .as("a second delivery of the same step is a stale view")
        .isExactlyInstanceOf(OptimisticLockException.class);
  }

  @Test
  void aNegativeLimitIsRefused_zeroFindsNothing() {
    SqliteSagaStore store = new SqliteSagaStore(migrated("limits.db", ""), mapper());
    store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING);
    Instant later = Instant.now().plusSeconds(60);

    assertThatThrownBy(() -> store.findTimedOut(TYPE, later, -1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    assertThatThrownBy(() -> store.findByStatus(TYPE, SagaStatus.RUNNING, -1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    assertThatThrownBy(() -> store.findCompensating(TYPE, later, -1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("limit");
    assertThat(store.findTimedOut(TYPE, later, 0)).isEmpty();
    assertThat(store.findByStatus(TYPE, SagaStatus.RUNNING, 0)).isEmpty();
    assertThat(store.findCompensating(TYPE, later, 0)).isEmpty();
  }

  @Test
  void aMissingTableIsAStorageFailureForEveryReadAndWrite() throws Exception {
    SqliteDatabase database = migrated("missing.db", "");
    SqliteSagaStore store = new SqliteSagaStore(database, mapper());
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE saga_state");
    }
    Instant later = Instant.now();
    List<Runnable> calls =
        List.of(
            () -> store.create(ID, TYPE, RUNNING, SagaStatus.RUNNING),
            () -> store.update(ID, TYPE, RUNNING, SagaStatus.RUNNING, 1L),
            () -> store.markFaulted(ID, TYPE, 1L),
            () -> store.setDeadLetterPending(ID, TYPE, true),
            () -> store.delete(ID, TYPE),
            () -> store.load(ID, TYPE, StepState.class),
            () -> store.loadRowFacts(ID, TYPE),
            () -> store.findTimedOut(TYPE, later, 1),
            () -> store.findByStatus(TYPE, SagaStatus.RUNNING, 1),
            () -> store.countByStatus(TYPE, SagaStatus.RUNNING),
            () -> store.findCompensating(TYPE, later, 1));
    for (Runnable call : calls) {
      assertThatThrownBy(call::run)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(SQLException.class);
    }
  }

  private static Instant column(SqliteDatabase database, String column) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT " + column + " FROM saga_state WHERE saga_id = '" + ID.value() + "'")) {
      assertThat(rs.next()).isTrue();
      return SqliteTimestamps.fromMicros(rs.getLong(1));
    }
  }
}

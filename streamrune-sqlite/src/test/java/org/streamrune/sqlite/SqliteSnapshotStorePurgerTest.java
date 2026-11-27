package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.jdbc.StreamKeys;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractState;

/** {@link SqliteSnapshotStorePurger}: what a purge writes, its races with a save, its crash. */
class SqliteSnapshotStorePurgerTest {

  private static final AggregateType ORDER = AggregateType.of("order");

  @TempDir Path dir;

  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private final List<SqliteDatabase> handles = new ArrayList<>();
  private SqliteDatabase database;
  private EventStore store;

  @BeforeEach
  void setUp() {
    database = open(dir.resolve("purger.db"));
    store =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .create();
  }

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase open(Path file) {
    SqliteDatabase handle =
        SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(file)));
    handles.add(handle);
    return handle;
  }

  private static StreamId stream(String id) {
    return StreamId.of(ORDER, AggregateId.of(id));
  }

  private SqliteSnapshotStorePurger purger(List<StreamId> streams) {
    return new SqliteSnapshotStorePurger(database, subject -> streams);
  }

  private void snapshot(StreamId stream) {
    store.saveSnapshot(stream, new Version(1), new ContractState("v1"));
  }

  /** The streams whose snapshot_store row still holds a state. */
  private Set<String> snapshotted() throws SQLException {
    Set<String> ids = new HashSet<>();
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT aggregate_type, aggregate_id FROM snapshot_store"
                    + " WHERE state_type IS NOT NULL")) {
      while (rs.next()) {
        ids.add(rs.getString(1) + ":" + rs.getString(2));
      }
    }
    return ids;
  }

  private long generation(StreamId stream) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT purge_generation FROM snapshot_store"
                    + " WHERE aggregate_type = ? AND aggregate_id = ?")) {
      StreamKeys.bind(ps, 1, stream);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).as("the purged stream keeps its row").isTrue();
        return rs.getLong(1);
      }
    }
  }

  @Test
  void purgeRemovesOnlyTheMappedStreamsSnapshots() throws Exception {
    snapshot(stream("subject-1"));
    snapshot(stream("other"));

    purger(List.of(stream("subject-1"))).purge(SubjectId.of("subject-1"));

    assertThat(snapshotted()).containsExactly("order:other");
  }

  @Test
  void purgeLeavesTheSnapshotOfAnotherTypeThatSharesTheIdValue() throws Exception {
    StreamId customer = StreamId.of(AggregateType.of("customer"), AggregateId.of("c-1"));
    StreamId order = stream("c-1");
    snapshot(customer);
    snapshot(order);

    purger(List.of(customer)).purge(SubjectId.of("c-1"));

    assertThat(snapshotted()).containsExactly("order:c-1");
  }

  @Test
  void purgeRemovesEveryMappedStream() throws Exception {
    snapshot(stream("a"));
    snapshot(stream("b"));
    snapshot(stream("untouched"));

    purger(List.of(stream("a"), stream("b"))).purge(SubjectId.of("multi"));

    assertThat(snapshotted()).containsExactly("order:untouched");
  }

  @Test
  void purgeWithNoMappedStreamsOrANullMappingWritesNothing() throws Exception {
    snapshot(stream("kept"));

    purger(List.of()).purge(SubjectId.of("none"));
    new SqliteSnapshotStorePurger(database, subject -> null).purge(SubjectId.of("null"));

    assertThat(snapshotted()).containsExactly("order:kept");
    assertThat(generation(stream("kept"))).isZero();
  }

  @Test
  void purgeRaisesTheGenerationEveryTimeAndIsIdempotentOtherwise() throws Exception {
    StreamId stream = stream("generation");
    snapshot(stream);
    SqliteSnapshotStorePurger purger = purger(List.of(stream));

    purger.purge(SubjectId.of("gen"));
    assertThat(generation(stream)).isEqualTo(1);
    assertThatCode(() -> purger.purge(SubjectId.of("gen"))).doesNotThrowAnyException();
    assertThat(generation(stream)).isEqualTo(2);
    assertThat(snapshotted()).isEmpty();
  }

  @Test
  void purgeOfAStreamWithoutASnapshotLeavesARowWithoutAState() throws Exception {
    StreamId stream = stream("never-snapshotted");

    purger(List.of(stream)).purge(SubjectId.of("none"));

    assertThat(generation(stream)).isEqualTo(1);
    assertThat(snapshotted()).isEmpty();
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT version, state_type, state_payload FROM snapshot_store")) {
      rs.next();
      assertThat(rs.getLong(1)).isZero();
      assertThat(rs.getString(2)).isNull();
      assertThat(rs.getString(3)).isNull();
    }
  }

  @Test
  void aStreamNamedTwiceByTheMappingIsPurgedOnce() throws Exception {
    StreamId stream = stream("twice");
    snapshot(stream);

    purger(List.of(stream, stream)).purge(SubjectId.of("twice"));

    assertThat(generation(stream)).isEqualTo(1);
  }

  /**
   * One statement purges every stream of the subject, whatever their number: twenty thousand
   * streams are more than SQLite's limit of bound parameters would allow as a VALUES list.
   */
  @Test
  void manyStreamsArePurgedInOneStatement() throws Exception {
    List<StreamId> streams = new ArrayList<>();
    for (int i = 0; i < 20_000; i++) {
      streams.add(stream("many-" + i));
    }

    purger(streams).purge(SubjectId.of("many"));

    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT count(*), min(purge_generation) FROM snapshot_store")) {
      rs.next();
      assertThat(rs.getInt(1)).isEqualTo(20_000);
      assertThat(rs.getInt(2)).isEqualTo(1);
    }
  }

  @Test
  void identifiersWithQuotesAndNonAsciiCharactersArePurgedLiterally() throws Exception {
    StreamId odd = stream("it's \"quoted\" ödd [0] 𝄞");
    snapshot(odd);
    snapshot(stream("plain"));

    purger(List.of(odd)).purge(SubjectId.of("odd"));

    assertThat(snapshotted()).containsExactly("order:plain");
    assertThat(generation(odd)).isEqualTo(1);
  }

  @Test
  void aSaveFromALoadBeforeThePurgeWritesNothingAndOneFromALoadAfterItWrites() throws Exception {
    StreamId stream = stream("save-after-purge");
    store.saveSnapshot(stream, new Version(1), new ContractState("before"));
    long seenBefore = store.load(stream).snapshotPurgeGeneration();
    purger(List.of(stream)).purge(SubjectId.of("save"));

    assertThat(
            store.saveSnapshot(stream, new Version(2), new ContractState("stale"), 1, seenBefore))
        .isFalse();
    assertThat(snapshotted()).isEmpty();

    var afterPurge = store.load(stream);
    assertThat(afterPurge.snapshotState()).as("a purged row is no snapshot").isNull();
    assertThat(afterPurge.snapshotPurgeGeneration()).isEqualTo(1);
    assertThat(
            store.saveSnapshot(
                stream,
                new Version(3),
                new ContractState("after"),
                1,
                afterPurge.snapshotPurgeGeneration()))
        .isTrue();
    assertThat(store.load(stream).snapshotState()).isEqualTo(new ContractState("after"));
    assertThat(generation(stream)).as("a save never changes the purge generation").isEqualTo(1);
  }

  /** Runs the purge statement in a write transaction the caller commits. */
  private static int purgeUncommitted(Connection conn, StreamId stream) throws SQLException {
    try (var ps = conn.prepareStatement(SqliteSnapshotStorePurger.PURGE_SNAPSHOTS)) {
      ps.setLong(1, SqliteTimestamps.toMicros(Instant.now()));
      ps.setString(2, SqliteSnapshotStorePurger.streamsJson(List.of(stream)));
      return ps.executeUpdate();
    }
  }

  /** Starts {@code save} and asserts it waits for the write lock the purge holds. */
  private static CompletableFuture<Boolean> saveBlockedBehindThePurge(
      java.util.function.Supplier<Boolean> save) {
    var future = CompletableFuture.supplyAsync(save);
    assertThatThrownBy(() -> future.get(700, TimeUnit.MILLISECONDS))
        .as("the save must wait for the uncommitted purge")
        .isInstanceOf(TimeoutException.class);
    return future;
  }

  /**
   * A save that meets a purge still in flight waits for SQLite's write lock, then sees the raised
   * generation and writes nothing.
   */
  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aSaveBehindAnUncommittedPurgeWritesNothingOnceThePurgeCommits() throws Exception {
    StreamId stream = stream("save-behind-purge");
    store.saveSnapshot(stream, new Version(1), new ContractState("old"));
    long seen = store.load(stream).snapshotPurgeGeneration();
    try (Connection purgeTx = database.dataSource().getConnection()) {
      purgeTx.setAutoCommit(false);
      assertThat(purgeUncommitted(purgeTx, stream)).isEqualTo(1);
      var save =
          saveBlockedBehindThePurge(
              () ->
                  store.saveSnapshot(stream, new Version(2), new ContractState("stale"), 1, seen));
      purgeTx.setAutoCommit(true);
      assertThat(save.get(10, TimeUnit.SECONDS)).isFalse();
    }
    assertThat(snapshotted()).isEmpty();
    assertThat(generation(stream)).isEqualTo(1);
  }

  /**
   * The first snapshot of a stream racing a purge that found no row: the purge inserted the row
   * first, and the save, which saw no row (generation 0), writes nothing.
   */
  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theFirstSnapshotOfAStreamLosesToAPurgeThatInsertedTheRowFirst() throws Exception {
    StreamId stream = stream("first-vs-purge");
    try (Connection purgeTx = database.dataSource().getConnection()) {
      purgeTx.setAutoCommit(false);
      assertThat(purgeUncommitted(purgeTx, stream)).isEqualTo(1);
      var save =
          saveBlockedBehindThePurge(
              () -> store.saveSnapshot(stream, new Version(1), new ContractState("first"), 1, 0));
      purgeTx.setAutoCommit(true);
      assertThat(save.get(10, TimeUnit.SECONDS)).isFalse();
    }
    assertThat(snapshotted()).isEmpty();
    assertThat(generation(stream)).isEqualTo(1);
  }

  @Test
  void aSaveWithoutALoadOnAPurgedStreamThrows() throws Exception {
    StreamId stream = stream("tool-save");
    purger(List.of(stream)).purge(SubjectId.of("tool"));

    assertThatThrownBy(() -> store.saveSnapshot(stream, new Version(1), new ContractState("v1")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("purged by a GDPR forget");
    assertThat(snapshotted()).isEmpty();
  }

  /**
   * A purge that dies half way, after it wrote the first of three rows, leaves all three as they
   * were: one statement in one transaction. Run again, it purges all three once.
   */
  @Test
  void aPurgeKilledHalfWayLeavesEveryStreamAsItWasAndItsRerunPurgesEachOnce() throws Exception {
    List<StreamId> streams = List.of(stream("k1"), stream("k2"), stream("k3"));
    streams.forEach(this::snapshot);

    crashPoints.arm(Point.SECOND_SNAPSHOT_ROW);
    assertThatThrownBy(() -> purger(streams).purge(SubjectId.of("killed")))
        .isExactlyInstanceOf(EventStoreException.class);

    database = open(database.file());
    assertThat(snapshotted()).containsExactlyInAnyOrder("order:k1", "order:k2", "order:k3");
    for (StreamId stream : streams) {
      assertThat(generation(stream)).isZero();
    }

    purger(streams).purge(SubjectId.of("killed"));
    assertThat(snapshotted()).isEmpty();
    for (StreamId stream : streams) {
      assertThat(generation(stream)).isEqualTo(1);
    }
  }

  /** A purge that dies after its commit is durable whole; its rerun raises each generation once. */
  @Test
  void aPurgeKilledAfterItsCommitIsDurable() throws Exception {
    List<StreamId> streams = List.of(stream("d1"), stream("d2"));
    streams.forEach(this::snapshot);

    crashPoints.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> purger(streams).purge(SubjectId.of("durable")))
        .isExactlyInstanceOf(EventStoreException.class);

    database = open(database.file());
    assertThat(snapshotted()).isEmpty();
    for (StreamId stream : streams) {
      assertThat(generation(stream)).isEqualTo(1);
    }
    purger(streams).purge(SubjectId.of("durable"));
    for (StreamId stream : streams) {
      assertThat(generation(stream)).isEqualTo(2);
    }
  }

  @Test
  void thePurgeStampsCreatedAtWithItsClock() throws Exception {
    Instant now = Instant.parse("1965-03-04T05:06:07.890123Z");
    StreamId stream = stream("stamped");
    new SqliteSnapshotStorePurger(
            database, subject -> List.of(stream), Clock.fixed(now, ZoneOffset.UTC))
        .purge(SubjectId.of("stamped"));

    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT created_at FROM snapshot_store")) {
      rs.next();
      assertThat(SqliteTimestamps.fromMicros(rs.getLong(1))).isEqualTo(now);
    }
  }

  @Test
  void aFailingDatabaseFailsThePurgeAsAnEventStoreException() throws Exception {
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("DROP TABLE snapshot_store");
    }

    assertThatThrownBy(() -> purger(List.of(stream("x"))).purge(SubjectId.of("x")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("snapshot_store");
  }

  @Test
  void isTheSnapshotPurgerTheForgetServiceRequiresWithAStableName() {
    SqliteSnapshotStorePurger purger = purger(List.of());
    assertThat(purger).isInstanceOf(SnapshotPurger.class);
    assertThat(purger.name()).isEqualTo("snapshot_store");
  }

  @Test
  void nullArgumentsAreRefused() {
    assertThatThrownBy(() -> new SqliteSnapshotStorePurger(null, subject -> List.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("database");
    assertThatThrownBy(() -> new SqliteSnapshotStorePurger(database, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("streamsForSubject");
    assertThatThrownBy(
            () -> new SqliteSnapshotStorePurger(database, subject -> List.of(), (Clock) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("clock");
    assertThatThrownBy(() -> purger(List.of()).purge(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("subjectId");
  }
}

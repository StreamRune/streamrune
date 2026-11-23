package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractState;

/**
 * The crash points of an append, a snapshot save and a purge, with a real process death: {@link
 * CrashChild} runs the write in a second JVM and halts it at the point, inside the statement or
 * right after the COMMIT, and this JVM then opens the file as the restarted service would, checks
 * what the write left, and runs it again.
 *
 * <p>SQLite in WAL mode: a transaction that has not committed never reaches the database, so a
 * death before COMMIT leaves no row and no counter advance; with {@code synchronous=FULL} a
 * committed transaction is on disk when COMMIT returns, so a death after it keeps the whole write.
 */
class SqliteEventStoreCrashIT {

  @TempDir Path dir;

  private void killAt(String mode, Path db, String point) throws Exception {
    try (ForkedJvm child = ForkedJvm.start(CrashChild.class, mode, db.toString(), point)) {
      assertThat(child.awaitExit())
          .as("the child died at " + point)
          .isEqualTo(SqliteCrashPoints.HALTED);
    }
  }

  private static EventStore restarted(SqliteDatabase database) {
    return new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
        .create();
  }

  private static long counter(SqliteDatabase database) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT next_value FROM global_offset_counter WHERE id = 1")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** Dead before COMMIT: none of the three events, the counter where it was, the rerun stores. */
  private void assertAppendLeftNothing(String point) throws Exception {
    Path db = dir.resolve(point + ".db");
    killAt("append", db, point);

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      assertThat(store.load(CrashChild.A).events()).hasSize(1);
      assertThat(counter(database)).isEqualTo(1);
      assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(1));

      assertThat(
              store
                  .append(
                      CrashChild.A, store.load(CrashChild.A).events().subList(0, 1), new Version(1))
                  .globalOffsets())
          .containsExactly(GlobalOffset.of(2));
      assertThat(counter(database)).isEqualTo(2);
    }
  }

  @Test
  void anAppendKilledAfterTakingItsOffsetsLeavesNoRowAndNoCounterAdvance() throws Exception {
    assertAppendLeftNothing("OFFSET_RESERVATION");
  }

  @Test
  void anAppendKilledAfterItsFirstEventRowLeavesNoRowAndNoCounterAdvance() throws Exception {
    assertAppendLeftNothing("SECOND_EVENT");
  }

  @Test
  void anAppendKilledRightAfterItsCommitIsStoredWhole() throws Exception {
    Path db = dir.resolve("after-commit.db");
    killAt("append", db, "AFTER_COMMIT");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      assertThat(store.load(CrashChild.A).events())
          .extracting(e -> e.globalOffset().value())
          .containsExactly(1L, 2L, 3L, 4L);
      assertThat(counter(database)).isEqualTo(4);
      assertThat(
              store
                  .append(
                      CrashChild.stream("next"),
                      store.readStream(CrashChild.A, new Version(3), 1),
                      Version.initial())
                  .globalOffsets())
          .containsExactly(GlobalOffset.of(5));
    }
  }

  @Test
  void aSnapshotSaveKilledInsideItsStatementKeepsTheEarlierSnapshot() throws Exception {
    Path db = dir.resolve("snapshot.db");
    killAt("snapshot", db, "FIRST_SNAPSHOT_ROW");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      AggregateHistory history = store.load(CrashChild.A);
      assertThat(history.snapshotState()).isEqualTo(new ContractState("at-1"));
      assertThat(history.events()).hasSize(1);
      store.saveSnapshot(CrashChild.A, new Version(2), new ContractState("at-2"));
      assertThat(store.load(CrashChild.A).snapshotState()).isEqualTo(new ContractState("at-2"));
    }
  }

  @Test
  void aSnapshotSaveKilledRightAfterItsCommitIsUsed() throws Exception {
    Path db = dir.resolve("snapshot-commit.db");
    killAt("snapshot", db, "AFTER_COMMIT");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      AggregateHistory history = store.load(CrashChild.A);
      assertThat(history.snapshotState()).isEqualTo(new ContractState("at-2"));
      assertThat(history.events()).isEmpty();
    }
  }

  @Test
  void aPurgeKilledHalfWayPurgesNothingAndItsRerunPurgesEachStreamOnce() throws Exception {
    Path db = dir.resolve("purge.db");
    killAt("purge", db, "SECOND_SNAPSHOT_ROW");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      for (StreamId stream : CrashChild.PURGED) {
        AggregateHistory history = store.load(stream);
        assertThat(history.snapshotState()).isEqualTo(new ContractState("v1"));
        assertThat(history.snapshotPurgeGeneration()).isZero();
      }

      new SqliteSnapshotStorePurger(database, subject -> CrashChild.PURGED)
          .purge(SubjectId.of("crashed"));
      for (StreamId stream : CrashChild.PURGED) {
        AggregateHistory history = store.load(stream);
        assertThat(history.snapshotState()).isNull();
        assertThat(history.snapshotPurgeGeneration()).isEqualTo(1);
      }
    }
  }

  @Test
  void aPurgeKilledRightAfterItsCommitIsDurableWhole() throws Exception {
    Path db = dir.resolve("purge-commit.db");
    killAt("purge", db, "AFTER_COMMIT");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      EventStore store = restarted(database);
      assertThat(CrashChild.PURGED)
          .allSatisfy(
              stream -> {
                assertThat(store.load(stream).snapshotState()).isNull();
                assertThat(store.load(stream).snapshotPurgeGeneration()).isEqualTo(1);
              });
    }
  }
}

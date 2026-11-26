package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.MutableClock;

/**
 * What the shared contract leaves to the SQLite inbox: the claim and the backfill on the caller's
 * connection, the processed time from the application clock, retention in chunks, and failures.
 */
class SqliteCommandInboxTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase migrated(String file) {
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve(file)));
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  @Test
  void refusesBadArguments() {
    SqliteDatabase database = migrated("args.db");
    var inbox = new SqliteCommandInbox(database);

    assertThatThrownBy(() -> new SqliteCommandInbox(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("database is required");
    assertThatThrownBy(() -> new SqliteCommandInbox(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("clock is required");
    assertThatThrownBy(() -> inbox.find(null)).isExactlyInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> inbox.deleteProcessedBefore(null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  /**
   * The claim and the backfill join the caller's transaction, and the row carries the processed
   * time of the inbox's clock: rolled back with the caller, it is gone; committed, it is found.
   */
  @Test
  void claimAndBackfillJoinTheCallersTransaction() throws Exception {
    SqliteDatabase database = migrated("claim.db");
    var inbox = new SqliteCommandInbox(database, MutableClock.startingAt(T0));
    IdempotencyKey key = IdempotencyKey.of("k1");

    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      assertThat(inbox.tryClaim(key, "Cmd", STREAM, new Version(2), conn)).isTrue();
      SqliteTransactions.rollback(conn, "test");
    }
    assertThat(inbox.find(key)).isEmpty();

    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      assertThat(inbox.tryClaim(key, "Cmd", STREAM, new Version(2), conn)).isTrue();
      assertThat(inbox.tryClaim(key, "Other", STREAM, new Version(9), conn))
          .as("a second claim of the key loses")
          .isFalse();
      inbox.backfillOffsets(key, List.of(GlobalOffset.of(7), GlobalOffset.of(8)), conn);
      SqliteTransactions.commit(conn);
    }

    var row = inbox.find(key).orElseThrow();
    assertThat(row.commandType()).isEqualTo("Cmd");
    assertThat(row.streamId()).isEqualTo(STREAM);
    assertThat(row.finalVersion()).isEqualTo(new Version(2));
    assertThat(row.globalOffsets()).containsExactly(GlobalOffset.of(7), GlobalOffset.of(8));
    assertThat(row.processedAt()).isEqualTo(T0);
  }

  /**
   * A backfill that finds no row of the key throws, so the append it belongs to rolls back instead
   * of committing a claim without its offsets.
   */
  @Test
  void aBackfillOfAKeyWithoutARowThrows() throws Exception {
    SqliteDatabase database = migrated("backfill.db");
    var inbox = new SqliteCommandInbox(database);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      assertThatThrownBy(
              () ->
                  inbox.backfillOffsets(
                      IdempotencyKey.of("never-claimed"), List.of(GlobalOffset.of(1)), conn))
          .isExactlyInstanceOf(SQLException.class)
          .hasMessageContaining("never-claimed");
      SqliteTransactions.rollback(conn, "test");
    }
  }

  /** Retention deletes in chunks, each in its own write transaction, until none is left. */
  @Test
  void retentionDeletesMoreRowsThanOneChunk() throws Exception {
    SqliteDatabase database = migrated("retention.db");
    MutableClock clock = MutableClock.startingAt(T0);
    var inbox = new SqliteCommandInbox(database, clock);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      for (int i = 0; i < 2_100; i++) {
        inbox.tryClaim(IdempotencyKey.of("old-" + i), "Cmd", STREAM, new Version(1), conn);
      }
      clock.advance(java.time.Duration.ofSeconds(10));
      inbox.tryClaim(IdempotencyKey.of("new"), "Cmd", STREAM, new Version(1), conn);
      SqliteTransactions.commit(conn);
    }

    assertThat(inbox.deleteProcessedBefore(T0)).as("strictly before").isZero();
    assertThat(inbox.deleteProcessedBefore(T0.plusSeconds(1))).isEqualTo(2_100);
    assertThat(inbox.find(IdempotencyKey.of("new"))).isPresent();
  }

  @Test
  void anUnreadableOffsetListIsAStorageFailure() throws Exception {
    SqliteDatabase database = migrated("corrupt.db");
    var inbox = new SqliteCommandInbox(database);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      inbox.tryClaim(IdempotencyKey.of("k"), "Cmd", STREAM, new Version(1), conn);
      SqliteTransactions.commit(conn);
      try (Statement st = conn.createStatement()) {
        st.execute("UPDATE command_inbox SET global_offsets = '[\"x\"]'");
      }
    }

    assertThatThrownBy(() -> inbox.find(IdempotencyKey.of("k")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to read command inbox for key: k");
  }

  @Test
  void storageFailuresAreEventStoreExceptions() throws SQLException {
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(dir.resolve("broken.db")));
    SqliteDatabase database = SqliteDatabase.open(source);
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    var inbox = new SqliteCommandInbox(database);
    source.failWith(() -> new IllegalStateException("pool closed"));

    assertThatThrownBy(() -> inbox.find(new IdempotencyKey("k\nnext")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith("Failed to read command inbox for key: k")
        .hasMessageNotContaining("\n");
    assertThatThrownBy(() -> inbox.deleteProcessedBefore(T0))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to prune command inbox");
  }

  @Test
  void retentionOnAManualCommitConnectionIsRefusedUnwrapped() {
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(dir.resolve("manual.db")));
    SqliteDatabase database = SqliteDatabase.open(source);
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    var inbox = new SqliteCommandInbox(database);
    source.manualCommit();

    assertThatThrownBy(() -> inbox.deleteProcessedBefore(T0))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
  }
}

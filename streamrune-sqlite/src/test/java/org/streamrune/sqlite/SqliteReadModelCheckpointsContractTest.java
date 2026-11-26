package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.test.OffsetStoreContract;

/**
 * The shared offset-store contract against {@code readmodel_checkpoint} of a database that holds
 * the read-model series alone (no event-store series), a fresh file for each store.
 */
class SqliteReadModelCheckpointsContractTest extends OffsetStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase readModelDatabase() {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("read-model-" + files++ + ".db")));
    handles.add(database);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    return database;
  }

  @Override
  protected OffsetStore newStore() {
    return new SqliteReadModelCheckpoints(readModelDatabase());
  }

  @Test
  void isNeverTheEventStoresSubscriptionCheckpoint() {
    // The inherited default would pair readmodel_checkpoint with the non-fencing at-least-once
    // processor and name it subscription_checkpoint: a target that lies about its table.
    var checkpoints = new SqliteReadModelCheckpoints(readModelDatabase());

    assertThatThrownBy(checkpoints::subscriptionCheckpoint)
        .isExactlyInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "SqliteReadModelCheckpoints are a read-model store's checkpoints"
                + " (readmodel_checkpoint), never the event store's subscription checkpoint: use"
                + " the store's checkpointTarget() to run a projection on them, or"
                + " SqliteOffsetStore's subscriptionCheckpoint() for the event store's"
                + " subscription_checkpoint");
  }

  @Test
  void rejectsNullDatabaseAndClock() {
    assertThatThrownBy(() -> new SqliteReadModelCheckpoints(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("database is required");
    assertThatThrownBy(() -> new SqliteReadModelCheckpoints(null, Clock.systemUTC()))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("database is required");
    SqliteDatabase database = readModelDatabase();
    assertThatThrownBy(() -> new SqliteReadModelCheckpoints(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("clock is required");
  }

  /** The file the checkpoints live in, which the read-model store's processor must share. */
  @Test
  void knowsTheFileItsCheckpointsLiveIn() {
    SqliteDatabase database = readModelDatabase();

    assertThat(new SqliteReadModelCheckpoints(database).file()).isEqualTo(database.file());
  }

  /** The fence the store's processor commits through is the one its reads and writes use. */
  @Test
  void theFenceIsBuiltOnTheStoresClock() {
    Clock clock = Clock.systemUTC();
    var checkpoints = new SqliteReadModelCheckpoints(readModelDatabase(), clock);

    assertThat(checkpoints.fence()).isSameAs(checkpoints.fence());
    assertThat(checkpoints.fence().clock()).isSameAs(clock);
    assertThat(checkpoints.fence().table()).isSameAs(CheckpointTable.READ_MODEL);
  }
}

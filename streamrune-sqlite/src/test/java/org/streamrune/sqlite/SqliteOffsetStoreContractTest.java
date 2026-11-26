package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.OffsetStoreContract;

/**
 * The shared offset-store contract against {@code subscription_checkpoint} of a database the event
 * store's factory migrated, a fresh file for each store.
 */
class SqliteOffsetStoreContractTest extends OffsetStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected OffsetStore newStore() {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("offsets-" + files++ + ".db")));
    handles.add(database);
    // The event store's factory applies the baseline that holds subscription_checkpoint.
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteOffsetStore(database);
  }

  @Test
  void rejectsNullDatabase() {
    assertThrows(NullPointerException.class, () -> new SqliteOffsetStore(null));
  }

  @Test
  void subscriptionCheckpointIsNonAtomicAtLeastOnce() {
    CheckpointTarget target = store.subscriptionCheckpoint();

    assertThat(target.name()).isEqualTo(OffsetStore.SUBSCRIPTION_CHECKPOINT);
    assertThat(target.checkpoints()).isSameAs(store);
    assertThat(target.processor()).isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }
}

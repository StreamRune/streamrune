package org.streamrune.sqlite;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.ProjectionDeadLetterStoreContract;

/**
 * The shared projection dead-letter contract against {@code projection_dead_letters} of a database
 * the event store's factory migrated, a fresh file for each store.
 */
class SqliteProjectionDeadLetterStoreContractTest extends ProjectionDeadLetterStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected ProjectionDeadLetterStore newStore() {
    SqliteDatabase database =
        SqliteDatabase.open(
            TestDatabases.dataSource(dir.resolve("projection-dlq-" + files++ + ".db")));
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteProjectionDeadLetterStore(database);
  }
}

package org.streamrune.sqlite;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.test.DeadLetterQueueContract;
import org.streamrune.test.EventStoreContract;

/**
 * The shared command dead-letter contract against {@code dead_letter_queue} of a database the event
 * store's factory migrated, a fresh file for each queue.
 */
class SqliteDeadLetterQueueContractTest extends DeadLetterQueueContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected DeadLetterQueue newQueue() {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("dlq-" + files++ + ".db")));
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteDeadLetterQueue(database);
  }
}

package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.test.DeadLetterQueueContract;
import org.streamrune.test.EventStoreContract;

/**
 * The shared command dead-letter contract once more, on a HikariCP pool of one connection: the
 * keyed publish that stores nothing, the refused duplicate {@code commandId} and the end of the
 * retention loop run on a reused physical connection, and the two concurrent keyed publishes queue
 * for it.
 */
class SqliteDeadLetterQueueContractOnAPoolTest extends DeadLetterQueueContract {

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private int files;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  @Override
  protected DeadLetterQueue newQueue() {
    HikariDataSource pool =
        TestDatabases.poolOfOne(dir.resolve("pooled-dlq-" + files++ + ".db"), "dlq-contract-pool");
    resources.add(pool);
    SqliteDatabase database = SqliteDatabase.open(pool);
    resources.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteDeadLetterQueue(database);
  }
}

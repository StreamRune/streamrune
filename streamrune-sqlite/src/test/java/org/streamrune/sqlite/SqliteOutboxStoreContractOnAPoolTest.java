package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.OutboxStoreContract;

/**
 * The shared ordering-mode contract once more, each relay on a HikariCP pool of one connection: the
 * claims, marks and reads run on a connection that has run earlier statements and transactions, and
 * none of them holds one connection while it borrows another.
 */
class SqliteOutboxStoreContractOnAPoolTest extends OutboxStoreContract {

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private Path file;
  private int files;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  private SqliteDatabase open(Path on) {
    HikariDataSource pool = TestDatabases.poolOfOne(on, "outbox-contract-pool-" + resources.size());
    resources.add(pool);
    SqliteDatabase handle = SqliteDatabase.open(pool);
    resources.add(handle);
    return handle;
  }

  @Override
  protected OutboxStore newStore(OutboxOrderingMode mode) {
    file = dir.resolve("pooled-outbox-" + files++ + ".db");
    SqliteDatabase database = open(file);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteOutboxStore(database, Duration.ofMinutes(4), mode);
  }

  /** A second relay with a pool of its own on the same file. */
  @Override
  protected OutboxStore sameDataWithMode(OutboxStore store, OutboxOrderingMode mode) {
    return new SqliteOutboxStore(open(file), Duration.ofMinutes(4), mode);
  }
}

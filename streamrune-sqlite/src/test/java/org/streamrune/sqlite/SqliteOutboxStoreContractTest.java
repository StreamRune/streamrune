package org.streamrune.sqlite;

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
 * The shared ordering-mode contract against {@link SqliteOutboxStore}, on {@code outbox_events} of
 * a database the event store's factory migrated, a fresh file for each store.
 */
class SqliteOutboxStoreContractTest extends OutboxStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected OutboxStore newStore(OutboxOrderingMode mode) {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("outbox-" + files++ + ".db")));
    handles.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteOutboxStore(database, Duration.ofMinutes(4), mode);
  }

  /** A second relay over the same file, through a handle of its own, with another mode. */
  @Override
  protected OutboxStore sameDataWithMode(OutboxStore store, OutboxOrderingMode mode) {
    SqliteDatabase database = handles.getLast();
    SqliteDatabase second = SqliteDatabase.open(TestDatabases.dataSource(database.file()));
    handles.add(second);
    return new SqliteOutboxStore(second, Duration.ofMinutes(4), mode);
  }
}

package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStore;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.test.EventStoreContract;

/**
 * The shared event-store contract once more, with the command inbox and the outbox, each store on a
 * HikariCP pool of one connection: every call reuses the same physical connection, as a production
 * pool does, and a call that held one connection while it borrowed another would wait for itself
 * until the pool's connection timeout and fail. That pins the keyed append's promise that a claim
 * loser returns its connection before it reads the recorded row. Crash injection is left to {@link
 * SqliteEventStoreContractTest}: a pooled connection is not closed when its write dies.
 */
class SqliteEventStoreContractOnAPoolTest extends EventStoreContract {

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private final Map<EventStore, SqliteDatabase> databases = new IdentityHashMap<>();
  private SqliteDatabase database;
  private int files;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  private SqliteDatabase open(Path file) {
    HikariDataSource pool =
        TestDatabases.poolOfOne(file, "event-contract-pool-" + resources.size());
    resources.add(pool);
    SqliteDatabase handle = SqliteDatabase.open(pool);
    resources.add(handle);
    return handle;
  }

  @Override
  protected EventStore newStore(Setup setup) {
    database = open(dir.resolve("pooled-events-" + files++ + ".db"));
    return build(database, setup);
  }

  /** A second pool of one and a handle of its own on the same file: another instance. */
  @Override
  protected EventStore reopen(EventStore store, Setup setup) {
    return build(open(database.file()), setup);
  }

  private EventStore build(SqliteDatabase handle, Setup setup) {
    EventStore store =
        new SqliteEventStoreFactory(handle, setup.registry())
            .cryptoEngine(setup.crypto())
            .upcasters(setup.upcasters())
            .snapshotMigrations(setup.migrations())
            .commandInbox(new SqliteCommandInbox(handle))
            .outboxStore(new SqliteOutboxStore(handle))
            .outboxEventMapper(OUTBOX_MAPPER)
            .create();
    databases.put(store, handle);
    return store;
  }

  @Override
  protected boolean supportsTransactionalOutbox() {
    return true;
  }

  @Override
  protected OutboxStore outboxOf(EventStore store) {
    return new SqliteOutboxStore(databases.get(store));
  }

  @Override
  protected boolean purgeSnapshot(StreamId streamId) {
    new SqliteSnapshotStorePurger(database, subject -> List.of(streamId))
        .purge(SubjectId.of("contract-subject"));
    return true;
  }
}

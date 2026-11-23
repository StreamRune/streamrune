package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.SagaStoreContract;

/**
 * The shared saga-store contract against {@code saga_state} of a database the event store's factory
 * migrated, a fresh file for each store.
 */
class SqliteSagaStoreContractTest extends SagaStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected SagaStore newStore() {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("sagas-" + files++ + ".db")));
    handles.add(database);
    // The event store's factory applies the baseline that holds saga_state.
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteSagaStore(database, new ObjectMapper().registerModule(new JavaTimeModule()));
  }
}

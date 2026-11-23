package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.SagaDeadLetterStoreContract;

/**
 * The shared saga dead-letter contract: a {@link SqliteSagaDeadLetterStore} and the {@link
 * SqliteSagaStore} whose rows it shields, on one database, a fresh file for each test.
 */
class SqliteSagaDeadLetterStoreContractTest extends SagaDeadLetterStoreContract {

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private SqliteDatabase current;
  private int files;

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  @Override
  protected SagaStore newSagaStore() {
    current = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("dead-" + files++ + ".db")));
    handles.add(current);
    new SqliteEventStoreFactory(current, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteSagaStore(current, new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  @Override
  protected SagaDeadLetterStore newStore(SagaStore sagaStore) {
    return new SqliteSagaDeadLetterStore(current);
  }
}

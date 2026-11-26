package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariDataSource;
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
 * The shared saga dead-letter contract once more, on a HikariCP pool of one connection that the
 * dead-letter store and the saga store share: the row counts behind {@code Written} and {@code
 * Kept}, the {@code discard} and shield-clear answers and the end of the retention loop are read on
 * a reused physical connection.
 */
class SqliteSagaDeadLetterStoreContractOnAPoolTest extends SagaDeadLetterStoreContract {

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private SqliteDatabase current;
  private int files;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  @Override
  protected SagaStore newSagaStore() {
    HikariDataSource pool =
        TestDatabases.poolOfOne(
            dir.resolve("pooled-dead-" + files++ + ".db"), "dead-letter-contract-pool");
    resources.add(pool);
    current = SqliteDatabase.open(pool);
    resources.add(current);
    new SqliteEventStoreFactory(current, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteSagaStore(current, new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  @Override
  protected SagaDeadLetterStore newStore(SagaStore sagaStore) {
    return new SqliteSagaDeadLetterStore(current);
  }
}

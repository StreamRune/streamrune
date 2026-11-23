package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.SagaStoreContract;

/**
 * The shared saga-store contract once more, on a HikariCP pool of one connection: every call reuses
 * the same physical connection, as a production pool does, so the row counts the store decides on
 * (a create that found its row, a stale compare-and-swap) are read on a connection that has run
 * earlier statements and transactions.
 */
class SqliteSagaStoreContractOnAPoolTest extends SagaStoreContract {

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private int files;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  /** A pool of one connection on {@code file}, with the required settings. */
  static HikariDataSource poolOfOne(Path file, String name) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:sqlite:file:" + file + "?" + TestDatabases.REQUIRED_SETTINGS);
    config.setMaximumPoolSize(1);
    config.setPoolName(name);
    return new HikariDataSource(config);
  }

  @Override
  protected SagaStore newStore() {
    HikariDataSource pool =
        poolOfOne(dir.resolve("pooled-sagas-" + files++ + ".db"), "saga-contract-pool");
    resources.add(pool);
    SqliteDatabase database = SqliteDatabase.open(pool);
    resources.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return new SqliteSagaStore(database, new ObjectMapper().registerModule(new JavaTimeModule()));
  }
}

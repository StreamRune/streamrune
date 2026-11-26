package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;

/**
 * The atomicity contract on a HikariCP pool of ONE connection: a batch, a replay and everything the
 * updater does through the handed repository run on the one connection the transaction holds, so
 * none of them waits for a second one. The second reader is a data source of its own.
 */
class SqliteProjectionRepositoryAtomicityOnAPoolTest
    extends SqliteProjectionRepositoryAtomicityTest {

  private HikariDataSource pool;

  @Override
  DataSource repositoryDataSource(Path file) {
    pool = TestDatabases.poolOfOne(file, "projection-repository-pool-of-one");
    return pool;
  }

  @AfterEach
  void closePool() {
    pool.close();
  }
}

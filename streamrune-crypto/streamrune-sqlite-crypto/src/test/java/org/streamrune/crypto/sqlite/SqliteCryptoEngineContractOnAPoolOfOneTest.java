package org.streamrune.crypto.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.test.CryptoKeyStoreContract;

/**
 * {@link SqliteCryptoEngine} against the shared key-store contract on a HikariCP pool of one
 * connection: no operation borrows a second connection while it holds one, so the concurrent cases
 * queue for the one connection instead of deadlocking.
 */
class SqliteCryptoEngineContractOnAPoolOfOneTest extends CryptoKeyStoreContract {

  @TempDir static Path dir;

  private static HikariDataSource pool;
  private static SqliteDatabase database;

  @BeforeAll
  static void open() {
    pool = SqliteFiles.pool(dir.resolve("keys.db"), 1);
    database = SqliteFiles.migrated(pool);
  }

  @AfterAll
  static void close() {
    database.close();
    pool.close();
  }

  @Override
  protected CryptoEngine newEngine() {
    return SqliteCryptoEngine.builder().database(database).build();
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    return SqliteCryptoEngine.builder().database(database).build();
  }
}

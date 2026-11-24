package org.streamrune.crypto.sqlite;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.test.CryptoKeyStoreContract;

/**
 * {@link SqliteCryptoEngine} against the shared key-store contract, on a SQLite file with a new
 * connection per operation. A restart is a new engine over the same file.
 */
class SqliteCryptoEngineContractTest extends CryptoKeyStoreContract {

  @TempDir static Path dir;

  private static SqliteDatabase database;

  @BeforeAll
  static void open() {
    database = SqliteFiles.migrated(SqliteFiles.dataSource(dir.resolve("keys.db")));
  }

  @AfterAll
  static void close() {
    database.close();
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

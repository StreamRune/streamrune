package org.streamrune.crypto.sqlite;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.test.CryptoKeyStoreContract;

/**
 * A {@link CachedCryptoEngine} over a {@link SqliteCryptoEngine}, as the integrations wire it,
 * against the shared key-store contract: a cached plaintext must not survive {@code deleteKey}. A
 * restart is a cold cache over a new engine on the same file.
 */
class CachedSqliteCryptoEngineContractTest extends CryptoKeyStoreContract {

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
    return CachedCryptoEngine.builder()
        .delegate(SqliteCryptoEngine.builder().database(database).build())
        .build();
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    return newEngine();
  }
}

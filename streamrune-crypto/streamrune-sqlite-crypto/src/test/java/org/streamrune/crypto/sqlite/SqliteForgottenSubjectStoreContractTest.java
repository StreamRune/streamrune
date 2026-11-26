package org.streamrune.crypto.sqlite;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.test.ForgottenSubjectStoreContract;

/** {@link SqliteForgottenSubjectStore} against the shared contract, as a durable store. */
class SqliteForgottenSubjectStoreContractTest extends ForgottenSubjectStoreContract {

  @TempDir static Path dir;

  private static SqliteDatabase database;

  @BeforeAll
  static void open() {
    database = SqliteFiles.migrated(SqliteFiles.dataSource(dir.resolve("tombstones.db")));
  }

  @AfterAll
  static void close() {
    database.close();
  }

  @Override
  protected ForgottenSubjectStore newStore() {
    return new SqliteForgottenSubjectStore(database);
  }

  @Override
  protected boolean durable() {
    return true;
  }

  @Override
  protected ForgottenSubjectStore sameStateNewInstance(ForgottenSubjectStore store) {
    return new SqliteForgottenSubjectStore(database);
  }
}

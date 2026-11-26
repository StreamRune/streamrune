package org.streamrune.crypto.postgres;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.test.CryptoKeyStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** {@link PostgresCryptoEngine} against the shared key-store contract, on real PostgreSQL. */
@Testcontainers
class PostgresCryptoEngineContractTest extends CryptoKeyStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          Files.readString(
              Path.of("src/main/resources/db/crypto-migration/V001__crypto_baseline.sql")));
    }
  }

  @Override
  protected CryptoEngine newEngine() {
    return PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    return PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }
}

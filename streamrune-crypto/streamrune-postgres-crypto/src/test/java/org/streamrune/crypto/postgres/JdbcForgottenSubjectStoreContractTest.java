package org.streamrune.crypto.postgres;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.test.ForgottenSubjectStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** {@link JdbcForgottenSubjectStore} against the shared contract, on real PostgreSQL. */
@Testcontainers
class JdbcForgottenSubjectStoreContractTest extends ForgottenSubjectStoreContract {

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
  protected ForgottenSubjectStore newStore() {
    return new JdbcForgottenSubjectStore(dataSource);
  }

  @Override
  protected boolean durable() {
    return true;
  }

  @Override
  protected ForgottenSubjectStore sameStateNewInstance(ForgottenSubjectStore store) {
    return new JdbcForgottenSubjectStore(dataSource);
  }
}

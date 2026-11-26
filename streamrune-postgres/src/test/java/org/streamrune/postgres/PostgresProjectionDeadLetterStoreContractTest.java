package org.streamrune.postgres;

import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.test.ProjectionDeadLetterStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The shared {@link ProjectionDeadLetterStore} contract against the real SQL on the shipped
 * baseline schema.
 */
@Testcontainers
class PostgresProjectionDeadLetterStoreContractTest extends ProjectionDeadLetterStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("proj_dlq_contract");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @Override
  protected ProjectionDeadLetterStore newStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_dead_letters");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new PostgresProjectionDeadLetterStore(dataSource);
  }
}

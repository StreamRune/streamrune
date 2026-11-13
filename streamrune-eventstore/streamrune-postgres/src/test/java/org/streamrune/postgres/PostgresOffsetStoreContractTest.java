package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.test.OffsetStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** The shared offset-store contract against {@code projection_offset} on the shipped baseline. */
@Testcontainers
class PostgresOffsetStoreContractTest extends OffsetStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("offset_store_contract");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    // The shipped event-store baseline, applied the way the factory applies it.
    Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @Override
  protected OffsetStore newStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM projection_offset");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return new PostgresOffsetStore(dataSource);
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresOffsetStore(null));
  }
}

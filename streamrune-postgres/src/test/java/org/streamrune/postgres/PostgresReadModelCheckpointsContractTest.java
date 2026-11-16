package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.test.OffsetStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The shared offset-store contract against {@code readmodel_checkpoint} on the shipped read-model
 * series, applied by {@link PostgresReadModelStore#create} alone: no event-store series.
 */
@Testcontainers
class PostgresReadModelCheckpointsContractTest extends OffsetStoreContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("readmodel_checkpoints_contract");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    PostgresReadModelStore.create(
        "postgres", dataSource, JdbcProjectionRepository.createObjectMapper(null), true, true);
  }

  @Override
  protected OffsetStore newStore() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM readmodel_checkpoint");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return new PostgresReadModelCheckpoints(dataSource);
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresReadModelCheckpoints(null));
  }
}

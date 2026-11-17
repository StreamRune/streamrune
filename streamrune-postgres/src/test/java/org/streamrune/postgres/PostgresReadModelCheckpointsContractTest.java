package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void isNeverTheEventStoresSubscriptionCheckpoint() {
    // The inherited default would pair readmodel_checkpoint with the non-fencing at-least-once
    // processor and name it subscription_checkpoint: a target that lies about its table.
    var checkpoints = new PostgresReadModelCheckpoints(dataSource);

    var refused =
        assertThrows(UnsupportedOperationException.class, checkpoints::subscriptionCheckpoint);
    assertTrue(
        refused.getMessage().contains("readmodel_checkpoint")
            && refused.getMessage().contains("checkpointTarget()")
            && refused.getMessage().contains("PostgresOffsetStore"),
        refused.getMessage());
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresReadModelCheckpoints(null));
  }
}

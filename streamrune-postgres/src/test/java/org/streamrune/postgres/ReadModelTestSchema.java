package org.streamrune.postgres;

import javax.sql.DataSource;

/**
 * Applies the read-model series ({@code db/readmodel-migration}) to a test database, exactly as
 * {@link PostgresReadModelStore#create} applies it: on its own history table, beside whatever
 * series the database already holds. A {@link JdbcProjectionRepository} checkpoints in that series'
 * {@code readmodel_checkpoint}, so every test that commits a batch through one needs it.
 */
final class ReadModelTestSchema {

  private ReadModelTestSchema() {}

  /**
   * Applies the read-model series to {@code dataSource}; a no-op when it is already applied.
   *
   * @param dataSource the test database, reached directly (not through a pooler)
   */
  static void apply(DataSource dataSource) {
    SeriesMigration.prepare(
            ShippedMigrationSeries.READ_MODEL,
            dataSource,
            PostgresReadModelStore.READ_MODEL_HISTORY_TABLE)
        .migrate();
  }
}

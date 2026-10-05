package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Replicas that start together on a new database all initialize the schema, and the series is
 * applied once.
 *
 * <p>Flyway decides how to treat a database without its history table before it takes its lock, so
 * a replica could read the table as absent, then find the schema made non-empty by a replica that
 * had just created the table, try to baseline it and fail with {@code Unable to baseline schema
 * history table ... as it already exists, and is empty}. Without {@link
 * PostgresEventStoreFactory#migrateConverging}, which then runs the migration again under Flyway's
 * lock, eight replicas on direct connections failed that way in about one start in five to eight,
 * so twenty starts all but always meet the race. Each repetition uses a database of its own, so
 * every one of them is a first start.
 */
@Testcontainers
class ConcurrentSchemaInitializationIT {

  private static final int REPLICAS = 8;

  private static final AtomicInteger DATABASES = new AtomicInteger();

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("concurrent_init");

  private static final EventTypeRegistry REGISTRY = SimpleEventTypeRegistry.builder().build();

  @RepeatedTest(20)
  @Timeout(120)
  void replicasStartingTogetherOnANewDatabaseAllSucceedAndApplyTheSeriesOnce() throws Exception {
    String db = "replicas_" + DATABASES.incrementAndGet();
    execute(dataSource(PG.getDatabaseName()), "CREATE DATABASE " + db);
    try {
      var start = new CyclicBarrier(REPLICAS);
      ExecutorService executor = Executors.newFixedThreadPool(REPLICAS);
      try {
        List<Future<?>> replicas = new ArrayList<>();
        for (int r = 0; r < REPLICAS; r++) {
          replicas.add(
              executor.submit(
                  () -> {
                    start.await(30, TimeUnit.SECONDS);
                    new PostgresEventStoreFactory(dataSource(db), REGISTRY).initializeSchema();
                    return null;
                  }));
        }
        for (Future<?> replica : replicas) {
          replica.get(90, TimeUnit.SECONDS);
        }
      } finally {
        executor.shutdownNow();
      }

      try (Connection conn = dataSource(db).getConnection();
          var st = conn.createStatement();
          var rs =
              st.executeQuery(
                  "SELECT count(*) FILTER (WHERE version = '001' AND success),"
                      + " count(*) FILTER (WHERE NOT success)"
                      + " FROM flyway_schema_history_streamrune")) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1), "the baseline migration must be recorded exactly once");
        assertEquals(0, rs.getInt(2), "no replica may leave a failed migration behind");
      }
      var validation = SchemaValidator.validate(dataSource(db));
      assertTrue(
          validation.isValid(), () -> "schema invalid after migrating: " + validation.issues());
    } finally {
      execute(dataSource(PG.getDatabaseName()), "DROP DATABASE " + db + " WITH (FORCE)");
    }
  }

  @Test
  void flywayStillRefusesToBaselineWithTheMessageTheSecondRunIsKeyedOn() throws Exception {
    // The second run is keyed on the start of Flyway's refusal. Provoke the refusal for real: a
    // history table that already holds a migration cannot be baselined.
    String db = "refusal_" + DATABASES.incrementAndGet();
    execute(dataSource(PG.getDatabaseName()), "CREATE DATABASE " + db);
    try {
      new PostgresEventStoreFactory(dataSource(db), REGISTRY).initializeSchema();
      var flyway =
          ShippedMigrationSeries.EVENT_STORE
              .configure(dataSource(db))
              .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
              .load();

      var refusal = assertThrows(FlywayException.class, flyway::baseline);
      assertTrue(
          refusal.getMessage().startsWith(PostgresEventStoreFactory.BASELINE_REFUSAL),
          refusal::getMessage);
    } finally {
      execute(dataSource(PG.getDatabaseName()), "DROP DATABASE " + db + " WITH (FORCE)");
    }
  }

  private static PGSimpleDataSource dataSource(String database) {
    var ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {PG.getHost()});
    ds.setPortNumbers(new int[] {PG.getMappedPort(5432)});
    ds.setDatabaseName(database);
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  private static void execute(PGSimpleDataSource dataSource, String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var st = conn.createStatement()) {
      st.execute(sql);
    }
  }
}

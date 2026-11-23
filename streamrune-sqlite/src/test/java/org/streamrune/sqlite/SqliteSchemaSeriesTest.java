package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.resource.LoadableResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;

/** The three schema series and the migration that applies them. */
class SqliteSchemaSeriesTest {

  /** Every table the event-store baseline creates. */
  static final List<String> BASELINE_TABLES =
      List.of(
          "audit_log",
          "command_inbox",
          "dead_letter_queue",
          "event_audit_log",
          "event_stream",
          "global_offset_counter",
          "outbox_events",
          "projection_dead_letters",
          "saga_dead_letters",
          "saga_state",
          "snapshot_store",
          "subscription_checkpoint");

  @TempDir Path dir;

  @Test
  void seriesHaveTheirDirectoriesHistoryTablesAndScripts() {
    assertThat(SqliteSchemaSeries.EVENT_STORE.directory()).isEqualTo("db/sqlite-migration");
    assertThat(SqliteSchemaSeries.EVENT_STORE.historyTable())
        .isEqualTo("flyway_schema_history_streamrune");
    assertThat(SqliteSchemaSeries.EVENT_STORE.scripts()).containsExactly("V001__baseline.sql");

    assertThat(SqliteSchemaSeries.READ_MODEL.directory())
        .isEqualTo("db/sqlite-readmodel-migration");
    assertThat(SqliteSchemaSeries.READ_MODEL.historyTable())
        .isEqualTo("flyway_schema_history_readmodel");
    assertThat(SqliteSchemaSeries.READ_MODEL.scripts())
        .containsExactly("V001__readmodel_baseline.sql");

    assertThat(SqliteSchemaSeries.CRYPTO.directory()).isEqualTo("db/sqlite-crypto-migration");
    assertThat(SqliteSchemaSeries.CRYPTO.historyTable()).isEqualTo("flyway_schema_history_crypto");
    assertThat(SqliteSchemaSeries.CRYPTO.scripts()).isEmpty();
  }

  @Test
  void expectedVersionIsTheHighestScriptVersion() {
    assertThat(SqliteSchemaSeries.EVENT_STORE.expectedVersion()).isEqualTo(1);
    assertThat(SqliteSchemaSeries.READ_MODEL.expectedVersion()).isEqualTo(1);
    assertThat(SqliteSchemaSeries.CRYPTO.expectedVersion()).isZero();
    assertThat(SqliteSchemaSeries.versionOf("V012__later.sql")).isEqualTo(12);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteSchemaSeries.versionOf("R__repeatable.sql"))
        .withMessageContaining("R__repeatable.sql");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteSchemaSeries.versionOf("V1_2__dotted.sql"))
        .withMessageContaining("V1_2__dotted.sql");
  }

  /** The listed scripts are exactly the {@code .sql} files of each directory of this module. */
  @Test
  void everySeriesOfThisModuleListsExactlyTheScriptsInItsDirectory() throws Exception {
    // The crypto series' scripts ship in streamrune-sqlite-crypto; its list is checked there.
    for (SqliteSchemaSeries series :
        List.of(SqliteSchemaSeries.EVENT_STORE, SqliteSchemaSeries.READ_MODEL)) {
      URL url = getClass().getClassLoader().getResource(series.directory());
      List<String> onDisk = new ArrayList<>();
      if (url != null) {
        try (Stream<Path> files = Files.list(Path.of(url.toURI()))) {
          files
              .map(file -> file.getFileName().toString())
              .filter(name -> name.endsWith(".sql"))
              .sorted()
              .forEach(onDisk::add);
        }
      }
      assertThat(series.scripts()).as(series.name()).isEqualTo(onDisk);
    }
  }

  @Test
  void answersFlywaysResourceQueriesFromTheListedScripts() throws Exception {
    ResourceProvider provider =
        SqliteSchemaMigration.configure(
                null, "db/sqlite-migration", "streamrune-sqlite", List.of("V001__baseline.sql"))
            .getResourceProvider();

    LoadableResource script = provider.getResource("V001__baseline.sql");
    assertThat(script).isNotNull();
    assertThat(provider.getResource("V001__baseline.sql.conf")).isNull();
    assertThat(script.getFilename()).isEqualTo("V001__baseline.sql");
    assertThat(script.getRelativePath()).isEqualTo("V001__baseline.sql");
    assertThat(script.getAbsolutePath()).isEqualTo("db/sqlite-migration/V001__baseline.sql");
    assertThat(script.getAbsolutePathOnDisk()).endsWith("V001__baseline.sql");
    assertThat(provider.getResources("V", new String[] {".sql"})).containsExactly(script);
    assertThat(provider.getResources("", new String[] {".txt", ".sql"})).containsExactly(script);
    assertThat(provider.getResources("R", new String[] {".sql"})).isEmpty();
    assertThat(provider.getResources("V", new String[] {".txt"})).isEmpty();

    String onClassPath;
    try (InputStream in =
        getClass().getClassLoader().getResourceAsStream("db/sqlite-migration/V001__baseline.sql")) {
      onClassPath = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertThat(readFully(script)).isEqualTo(onClassPath);
    assertThat(readFully(script)).as("every read starts from the beginning").isEqualTo(onClassPath);

    LoadableResource again =
        SqliteSchemaMigration.configure(
                null, "db/sqlite-migration", "streamrune-sqlite", List.of("V001__baseline.sql"))
            .getResourceProvider()
            .getResource("V001__baseline.sql");
    LoadableResource other =
        SqliteSchemaMigration.configure(
                null, "db/sqlite-test-crash", "streamrune-sqlite", List.of("V001__halfway.sql"))
            .getResourceProvider()
            .getResource("V001__halfway.sql");
    assertThat(again).isEqualTo(script).hasSameHashCodeAs(script).isNotEqualTo(other);
    assertThat((Object) script).isNotEqualTo("V001__baseline.sql");
  }

  @Test
  void tellsFlywayToLogThroughSlf4j() {
    assertThat(
            SqliteSchemaMigration.configure(null, "db/sqlite-migration", "a", List.of())
                .getLoggers())
        .containsExactly("slf4j");
  }

  @Test
  void aMissingScriptIsRefusedBeforeTheDatabaseIsTouched() throws Exception {
    try (SqliteDatabase db = open()) {
      assertThatThrownBy(
              () ->
                  SqliteSchemaMigration.migrate(
                      db.dataSource(),
                      "db/sqlite-nowhere",
                      "history_nowhere",
                      "streamrune-sqlite-crypto",
                      List.of("V001__missing.sql")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("db/sqlite-nowhere/V001__missing.sql")
          .hasMessageContaining("streamrune-sqlite-crypto")
          .hasMessageContaining("db/sqlite-nowhere/*.sql");
      assertThat(tables(db.dataSource())).isEmpty();
    }
  }

  @Test
  void migrateCreatesEveryBaselineTableAsStrict() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.EVENT_STORE);

      List<String> strict = new ArrayList<>();
      List<String> notStrict = new ArrayList<>();
      try (Connection connection = db.dataSource().getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs =
              statement.executeQuery(
                  "SELECT name, strict FROM pragma_table_list WHERE schema = 'main'"
                      + " AND type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
                      + " AND name <> 'flyway_schema_history_streamrune' ORDER BY name")) {
        while (rs.next()) {
          (rs.getInt(2) == 1 ? strict : notStrict).add(rs.getString(1));
        }
      }
      assertThat(notStrict).isEmpty();
      assertThat(strict).containsExactlyElementsOf(BASELINE_TABLES);
    }
  }

  /**
   * The read-model series creates {@code readmodel_checkpoint} alone, STRICT, with the Postgres
   * columns, timestamps as epoch microseconds and no default but the epoch's.
   */
  @Test
  void theReadModelSeriesCreatesItsCheckpointTableAlone() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.READ_MODEL);

      assertThat(
              query(
                  db.dataSource(),
                  "SELECT name, strict FROM pragma_table_list WHERE schema = 'main'"
                      + " AND type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
                      + " ORDER BY name"))
          .containsExactly(
              List.of("flyway_schema_history_readmodel", 0L), List.of("readmodel_checkpoint", 1L));
      assertThat(
              query(
                  db.dataSource(),
                  "SELECT name, type, \"notnull\", dflt_value, pk"
                      + " FROM pragma_table_info('readmodel_checkpoint') ORDER BY cid"))
          .containsExactly(
              Arrays.asList("projection_name", "TEXT", 1L, null, 1L),
              Arrays.asList("last_offset", "INTEGER", 1L, null, 0L),
              Arrays.asList("updated_at", "INTEGER", 0L, null, 0L),
              Arrays.asList("epoch", "INTEGER", 1L, "0", 0L));
      assertThat(history(db.dataSource(), "flyway_schema_history_readmodel"))
          .containsExactly(List.of("001", "V001__readmodel_baseline.sql", 1L));
      db.validate(SqliteSchemaSeries.READ_MODEL);
    }
  }

  @Test
  void globalOffsetCounterStartsAtZero() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.EVENT_STORE);

      assertThat(query(db.dataSource(), "SELECT id, next_value FROM global_offset_counter"))
          .containsExactly(List.of(1L, 0L));
    }
  }

  @Test
  void migrateTwiceIsIdempotent() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.EVENT_STORE);
      List<List<Object>> first = history(db.dataSource(), "flyway_schema_history_streamrune");
      List<String> tablesAfterFirst = tables(db.dataSource());

      db.migrate(SqliteSchemaSeries.EVENT_STORE);

      assertThat(history(db.dataSource(), "flyway_schema_history_streamrune")).isEqualTo(first);
      assertThat(first).containsExactly(List.of("001", "V001__baseline.sql", 1L));
      assertThat(tables(db.dataSource())).isEqualTo(tablesAfterFirst);
      assertThat(query(db.dataSource(), "SELECT next_value FROM global_offset_counter"))
          .containsExactly(List.of(0L));
    }
  }

  @Test
  void seriesInOneFileKeepSeparateHistoryTables() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.EVENT_STORE);
      db.migrate(SqliteSchemaSeries.READ_MODEL);
      db.migrate(SqliteSchemaSeries.CRYPTO);
      db.migrate(SqliteSchemaSeries.EVENT_STORE);

      assertThat(tables(db.dataSource()))
          .contains(
              "flyway_schema_history_streamrune",
              "flyway_schema_history_readmodel",
              "flyway_schema_history_crypto")
          .doesNotContain("flyway_schema_history");
      assertThat(history(db.dataSource(), "flyway_schema_history_streamrune"))
          .containsExactly(List.of("001", "V001__baseline.sql", 1L));
      // A series applied to a schema that already holds tables records Flyway's baseline (version
      // 0) in its own history table, then its own scripts; an empty series records the baseline
      // alone.
      assertThat(history(db.dataSource(), "flyway_schema_history_readmodel"))
          .containsExactly(
              List.of("0", "<< Flyway Baseline >>", 1L),
              List.of("001", "V001__readmodel_baseline.sql", 1L));
      assertThat(history(db.dataSource(), "flyway_schema_history_crypto"))
          .containsExactly(List.of("0", "<< Flyway Baseline >>", 1L));
      assertThat(tables(db.dataSource())).contains("readmodel_checkpoint");
      db.validate(SqliteSchemaSeries.EVENT_STORE);
      db.validate(SqliteSchemaSeries.READ_MODEL);
      db.validate(SqliteSchemaSeries.CRYPTO);
    }
  }

  @Test
  void anEmptySeriesMigratesAndValidatesCleanlyOnAnEmptyDatabase() throws Exception {
    try (SqliteDatabase db = open()) {
      db.migrate(SqliteSchemaSeries.CRYPTO);
      db.migrate(SqliteSchemaSeries.CRYPTO);

      assertThat(tables(db.dataSource())).containsExactly("flyway_schema_history_crypto");
      assertThat(history(db.dataSource(), "flyway_schema_history_crypto")).isEmpty();
      assertThat(SqliteSchemaValidator.validate(db.dataSource(), SqliteSchemaSeries.CRYPTO))
          .isEmpty();
      db.validate(SqliteSchemaSeries.CRYPTO);
    }
  }

  @Test
  void theEventStoreSeriesAppliesAfterAnotherSeriesOrTheApplicationsTables() throws Exception {
    try (SqliteDatabase db = open()) {
      execute(db.dataSource(), "CREATE TABLE application_table (id INTEGER PRIMARY KEY) STRICT");
      db.migrate(SqliteSchemaSeries.READ_MODEL);

      db.migrate(SqliteSchemaSeries.EVENT_STORE);

      // Baselined at 0, so the whole series still applies on a non-empty schema.
      assertThat(history(db.dataSource(), "flyway_schema_history_streamrune"))
          .containsExactly(
              List.of("0", "<< Flyway Baseline >>", 1L), List.of("001", "V001__baseline.sql", 1L));
      assertThat(tables(db.dataSource())).containsAll(BASELINE_TABLES);
      db.validate(SqliteSchemaSeries.EVENT_STORE);
    }
  }

  /**
   * Handles of one file in one process migrate one at a time, so concurrent first starts never race
   * through the creation of a history table: every migration succeeds and the script runs once.
   */
  @Test
  void concurrentMigrationsOfOneFileApplyEachScriptOnce() throws Exception {
    int threads = 6;
    List<SqliteDatabase> handles = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      handles.add(open());
    }
    java.util.concurrent.CyclicBarrier start = new java.util.concurrent.CyclicBarrier(threads);
    try (var executor = java.util.concurrent.Executors.newFixedThreadPool(threads)) {
      List<java.util.concurrent.Future<?>> runs = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        SqliteDatabase handle = handles.get(i);
        SqliteSchemaSeries series =
            i % 2 == 0 ? SqliteSchemaSeries.EVENT_STORE : SqliteSchemaSeries.READ_MODEL;
        runs.add(
            executor.submit(
                () -> {
                  start.await(30, java.util.concurrent.TimeUnit.SECONDS);
                  handle.migrate(series);
                  return null;
                }));
      }
      for (var run : runs) {
        run.get(60, java.util.concurrent.TimeUnit.SECONDS);
      }
    } finally {
      handles.forEach(SqliteDatabase::close);
    }
    try (SqliteDatabase db = open()) {
      assertThat(history(db.dataSource(), "flyway_schema_history_streamrune"))
          .filteredOn(row -> row.get(1).equals("V001__baseline.sql"))
          .containsExactly(List.of("001", "V001__baseline.sql", 1L));
      db.validate(SqliteSchemaSeries.EVENT_STORE);
      db.validate(SqliteSchemaSeries.READ_MODEL);
    }
  }

  /**
   * A migration that fails in the middle of its script is the same event, for the database, as a
   * crash in the middle of it: the script and its history row share one transaction, which SQLite
   * rolls back. Nothing of the script remains, the history records no success, and the next run of
   * the same script applies all of it.
   */
  @Test
  void aScriptThatFailsHalfwayLeavesNothingAndTheRerunConverges() throws Exception {
    try (SqliteDatabase db = open()) {
      // The script's last statement inserts 1, which this gate refuses.
      execute(db.dataSource(), "CREATE TABLE test_gate (open INTEGER CHECK (open = 0)) STRICT");

      assertThatThrownBy(() -> migrateHalfway(db))
          .isInstanceOf(EventStoreException.class)
          .hasMessageContaining("history_test");

      assertThat(tables(db.dataSource())).doesNotContain("first_half", "second_half");
      assertThat(
              query(
                  db.dataSource(),
                  "SELECT count(*) FROM history_test WHERE success = 1 AND version = '001'"))
          .containsExactly(List.of(0L));

      execute(
          db.dataSource(), "DROP TABLE test_gate", "CREATE TABLE test_gate (open INTEGER) STRICT");
      migrateHalfway(db);
      migrateHalfway(db);

      assertThat(tables(db.dataSource())).contains("first_half", "second_half");
      assertThat(query(db.dataSource(), "SELECT open FROM test_gate")).containsExactly(List.of(1L));
      assertThat(history(db.dataSource(), "history_test"))
          .containsExactly(
              List.of("0", "<< Flyway Baseline >>", 1L), List.of("001", "V001__halfway.sql", 1L));
    }
  }

  private static void migrateHalfway(SqliteDatabase db) {
    SqliteSchemaMigration.migrate(
        db.dataSource(),
        "db/sqlite-test-crash",
        "history_test",
        "streamrune-sqlite",
        List.of("V001__halfway.sql"));
  }

  @Test
  void migrateAndValidateRefuseNull() throws Exception {
    try (SqliteDatabase db = open()) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> db.migrate(null))
          .withMessage("series must not be null");
      assertThatIllegalArgumentException()
          .isThrownBy(() -> db.validate(null))
          .withMessage("series must not be null");
    }
  }

  @Test
  void migrateAndValidateRefuseAClosedHandle() throws Exception {
    SqliteDatabase db = open();
    db.close();

    assertThatThrownBy(() -> db.migrate(SqliteSchemaSeries.EVENT_STORE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed")
        .hasMessageContaining(db.file().toString());
    assertThatThrownBy(() -> db.validate(SqliteSchemaSeries.EVENT_STORE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
    assertThat(tables(db.dataSource())).isEmpty();
  }

  @Test
  void aFailingDataSourceFailsTheMigrationNamingTheSeries() throws Exception {
    try (SqliteDatabase db = open()) {
      DataSource broken = TestDatabases.url("jdbc:sqlite:" + dir.resolve("missing/dir/x.db"));
      assertThatThrownBy(
              () ->
                  SqliteSchemaMigration.migrate(
                      broken,
                      SqliteSchemaSeries.EVENT_STORE.directory(),
                      SqliteSchemaSeries.EVENT_STORE.historyTable(),
                      "streamrune-sqlite",
                      SqliteSchemaSeries.EVENT_STORE.scripts()))
          .isInstanceOf(EventStoreException.class)
          .hasMessageContaining("flyway_schema_history_streamrune");
    }
  }

  private SqliteDatabase open() {
    return SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("app.db")));
  }

  static List<String> tables(DataSource dataSource) throws SQLException {
    List<String> names = new ArrayList<>();
    for (List<Object> row :
        query(
            dataSource,
            "SELECT name FROM pragma_table_list WHERE schema = 'main' AND type = 'table'"
                + " AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name")) {
      names.add((String) row.get(0));
    }
    return names;
  }

  static List<List<Object>> history(DataSource dataSource, String table) throws SQLException {
    return query(
        dataSource, "SELECT version, script, success FROM " + table + " ORDER BY installed_rank");
  }

  static List<List<Object>> query(DataSource dataSource, String sql) throws SQLException {
    List<List<Object>> rows = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery()) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columns; i++) {
          Object value = rs.getObject(i);
          row.add(value instanceof Integer number ? Long.valueOf(number) : value);
        }
        rows.add(row);
      }
    }
    return rows;
  }

  static void execute(DataSource dataSource, String... statements) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    }
  }

  private static String readFully(LoadableResource resource) throws Exception {
    try (Reader reader = resource.read()) {
      StringBuilder content = new StringBuilder();
      char[] buffer = new char[4096];
      for (int n; (n = reader.read(buffer)) != -1; ) {
        content.append(buffer, 0, n);
      }
      return content.toString();
    }
  }
}

package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The published event-store series must not share a Flyway namespace with the consuming
 * application's own series.
 *
 * <p>The shipped jar used to carry {@code db/migration/V001__…}–{@code V038__…} at the DEFAULT
 * classpath location, migrated on the DEFAULT {@code flyway_schema_history} table. Spring Boot's
 * own documented default for an application's migrations is that same {@code
 * classpath:db/migration}, and {@code streamrune-postgres} declares {@code api(flyway-core)}, so
 * Boot's {@code FlywayAutoConfiguration} activates whether or not the application asked for it. Two
 * consequences, both reproduced or pinned here:
 *
 * <ol>
 *   <li>Flyway resolves the location across the WHOLE classpath, so the application's {@code V1__…}
 *       and the framework's {@code V001__…} are the same {@code MigrationVersion} 1 and the run
 *       aborts with {@code Found more than one migration with version 1} — the artifact is unusable
 *       in the default configuration.
 *   <li>An application that numbers around the collision has its OWN DDL executed by {@code
 *       PostgresEventStoreFactory.initializeSchema()}, under the framework's DataSource and the
 *       least-privilege credentials the production guide prescribes.
 * </ol>
 *
 * <p>The crypto series already solved this for itself ({@code db/crypto-migration} + {@code
 * flyway_schema_history_crypto}); the event-store series now does the same with {@code
 * db/streamrune-migration} + {@code flyway_schema_history_streamrune}.
 *
 * <p>The foreign migration is NOT placed in this module's own {@code src/test/resources} — that
 * would put a second {@code V1} on every other test's classpath. It is written to a temp directory
 * that is prepended to the thread-context classloader for the duration of the call, which is what
 * {@code Flyway.configure()} resolves {@code classpath:} against — a faithful stand-in for the
 * application jar that also ships {@code db/migration}.
 */
@Testcontainers
class SchemaNamespaceIsolationIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("namespace_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void setUpDataSource() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @BeforeEach
  void resetSchema() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP SCHEMA public CASCADE");
      stmt.execute("CREATE SCHEMA public");
    }
  }

  @Test
  void initializeSchema_withAForeignSeriesAtTheDefaultLocation_migratesOnlyTheFrameworkSeries(
      @TempDir Path applicationClasspath) throws Exception {
    writeForeignApplicationMigration(applicationClasspath);

    withApplicationOnTheClasspath(
        applicationClasspath,
        () ->
            new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
                .initializeSchema());

    assertTrue(tableExists("event_stream"), "the framework series must have been applied");
    assertFalse(
        tableExists("foreign_app_table"),
        "the framework's schema initialization must NEVER execute the application's own DDL —"
            + " it runs under the framework DataSource and the least-privilege credentials the"
            + " production guide prescribes");
    assertTrue(
        tableExists("flyway_schema_history_streamrune"),
        "the framework series must be recorded in its OWN history table");
    assertFalse(
        tableExists("flyway_schema_history"),
        "the application's default history table must be left entirely to the application");
    assertEquals(
        SchemaValidator.EXPECTED_VERSION,
        maxVersion("flyway_schema_history_streamrune"),
        "the whole framework series must be present in the namespaced history");
  }

  /**
   * The application's own Flyway — Spring Boot's auto-configuration defaults: {@code
   * classpath:db/migration}, default history table, {@code baseline-on-migrate=false} — must run
   * unaffected by the framework jar sitting on the same classpath. Before the relocation it aborted
   * with {@code Found more than one migration with version 1} (the framework's {@code V001} and the
   * application's {@code V1} are the same {@code MigrationVersion}), i.e. the published artifact
   * was unusable in the default configuration.
   *
   * <p>Ordering is the application's Flyway first, then the framework's schema initialization — the
   * order Spring Boot produces when the application's Flyway bean is initialized before the
   * event-store factory. The reverse order is covered by {@link
   * #applicationFlywayAfterFrameworkInitialization_needsBaselineOnMigrate()}.
   */
  @Test
  void applicationFlywayAtTheDefaultLocation_isUnaffectedByTheFrameworkJar(
      @TempDir Path applicationClasspath) throws Exception {
    writeForeignApplicationMigration(applicationClasspath);

    withApplicationOnTheClasspath(
        applicationClasspath,
        () ->
            org.flywaydb.core.Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(false)
                .load()
                .migrate());
    new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
        .initializeSchema();

    assertTrue(tableExists("foreign_app_table"), "the application's own migration must apply");
    assertTrue(tableExists("event_stream"), "the framework series must apply too");
    assertEquals(
        1,
        maxVersion("flyway_schema_history"),
        "the application's history must contain ONLY its own series");
    assertEquals(
        SchemaValidator.EXPECTED_VERSION,
        maxVersion("flyway_schema_history_streamrune"),
        "the framework's history must contain ONLY its own series");
    // Both series now start (and, for the framework, end) at version 1, so the versions alone
    // cannot tell the two histories apart — pin them by script.
    assertEquals(
        java.util.List.of("V1__create_orders.sql"),
        appliedScripts("flyway_schema_history"),
        "the application's history must contain ONLY its own migration");
    assertFalse(
        appliedScripts("flyway_schema_history_streamrune").contains("V1__create_orders.sql"),
        "the framework's history must never record the application's migration");
  }

  /**
   * The reverse order, and the one operator-visible cost of the relocation: when the framework
   * initializes the schema FIRST, the application's own Flyway then meets a non-empty schema with
   * no default history table and refuses unless {@code baseline-on-migrate=true} — Flyway's
   * standard "adopting an existing schema" rule, which {@code docs/guide/production.md} already
   * prescribes. Pinned so the upgrade note stays honest.
   */
  @Test
  void applicationFlywayAfterFrameworkInitialization_needsBaselineOnMigrate(
      @TempDir Path applicationClasspath) throws Exception {
    writeForeignApplicationMigration(applicationClasspath);
    new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
        .initializeSchema();

    withApplicationOnTheClasspath(
        applicationClasspath,
        () ->
            org.flywaydb.core.Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate());

    assertTrue(tableExists("foreign_app_table"), "the application's own migration must apply");
    assertEquals(
        SchemaValidator.EXPECTED_VERSION,
        maxVersion("flyway_schema_history_streamrune"),
        "the framework's history must be untouched by the application's baseline");
  }

  /**
   * Crash-point convergence: a run interrupted part-way through the framework series must resume,
   * not restart. Flyway records each applied migration in {@code flyway_schema_history_streamrune},
   * so a rerun sees the partial history and applies only the remainder — including when the
   * surrounding schema already carries the framework's tables.
   */
  @Test
  void initializeSchema_isIdempotentAcrossReruns() {
    var registry = SimpleEventTypeRegistry.builder().build();
    new PostgresEventStoreFactory(dataSource, registry).initializeSchema();
    int afterFirst = maxVersion("flyway_schema_history_streamrune");
    new PostgresEventStoreFactory(dataSource, registry).initializeSchema();
    assertEquals(afterFirst, maxVersion("flyway_schema_history_streamrune"));
    assertTrue(tableExists("event_stream"));
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  private static void writeForeignApplicationMigration(Path root) throws Exception {
    Path dir = root.resolve("db").resolve("migration");
    Files.createDirectories(dir);
    Files.writeString(
        dir.resolve("V1__create_orders.sql"),
        "CREATE TABLE foreign_app_table (id INT PRIMARY KEY);\n",
        StandardCharsets.UTF_8);
  }

  /** Runs {@code action} with {@code root} prepended to the thread-context classloader. */
  private static void withApplicationOnTheClasspath(Path root, Runnable action) throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {root.toUri().toURL()}, previous)) {
      Thread.currentThread().setContextClassLoader(loader);
      action.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static boolean tableExists(String table) {
    try (var conn = dataSource.getConnection();
        var rs = conn.getMetaData().getTables(null, "public", table, new String[] {"TABLE"})) {
      return rs.next();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static java.util.List<String> appliedScripts(String historyTable) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT script FROM "
                    + historyTable
                    + " WHERE type = 'SQL' AND success ORDER BY installed_rank")) {
      var scripts = new java.util.ArrayList<String>();
      while (rs.next()) {
        scripts.add(rs.getString(1));
      }
      return scripts;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static int maxVersion(String historyTable) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT MAX(CAST(version AS INTEGER)) FROM "
                    + historyTable
                    + " WHERE success = true AND version ~ '^[0-9]+$'")) {
      return rs.next() ? rs.getInt(1) : 0;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}

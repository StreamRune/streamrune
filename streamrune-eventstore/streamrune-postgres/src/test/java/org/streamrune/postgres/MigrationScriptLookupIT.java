package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Schema auto-initialization resolves each shipped migration script by its exact name, so it works
 * where the class path cannot be listed — a GraalVM native image — and refuses, naming the script,
 * when a script is absent.
 *
 * <p>Flyway lists a {@code classpath:} location through the URL the class loader returns for the
 * directory and understands only the {@code file} and {@code jar} protocols (plus a few
 * application-server ones). A native image serves its resources under the {@code resource}
 * protocol: Flyway logs "unsupported protocol" and applies nothing, and the schema validation that
 * follows refuses every framework table as missing. {@link NativeImageLikeClassLoader} answers
 * resource lookups the same way, and is installed as the thread context class loader, which is the
 * class loader Flyway resolves migrations through.
 */
@Testcontainers
class MigrationScriptLookupIT {

  private static final String EVENT_STORE_SCRIPT =
      "db/streamrune-migration/V001__streamrune_baseline.sql";
  private static final String CRYPTO_SCRIPT = "db/crypto-migration/V001__crypto_baseline.sql";

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("script_lookup_test");

  @Test
  void theNativeImageLikeClassLoaderDefeatsFlywaysOwnClassPathScan() throws Exception {
    // Positive control: with this class loader Flyway's own scan finds nothing, so the test below
    // exercises the failure a native image produces and cannot pass by scanning.
    var dataSource = dataSource("lookup_control");
    var loader = new NativeImageLikeClassLoader(Set.of());

    var resolved =
        Flyway.configure(loader)
            .dataSource(dataSource)
            .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
            .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
            .load()
            .info()
            .all();

    assertEquals(0, resolved.length, "Flyway's classpath scan must not list a resource: URL");
    assertNotNull(
        loader.getResource(EVENT_STORE_SCRIPT),
        "the script itself must still resolve by its exact name, as in a native image");
  }

  @Test
  void initializeSchema_appliesBothSeries_whenTheClassPathCannotBeListed() throws Exception {
    var dataSource = dataSource("lookup_unlisted");
    CryptoEngine engine = PostgresCryptoEngine.builder().dataSource(dataSource).build();

    withContextClassLoader(
        new NativeImageLikeClassLoader(Set.of()),
        () ->
            new PostgresEventStoreFactory(dataSource, SimpleEventTypeRegistry.builder().build())
                .cryptoEngine(engine)
                .initializeSchema());

    SchemaValidationResult eventStore = SchemaValidator.validate(dataSource);
    assertTrue(eventStore.isValid(), () -> "event-store schema: " + eventStore.issues());
    SchemaValidationResult crypto =
        SchemaValidator.validateCryptoTables(dataSource, engine.requiredCryptoTables());
    assertTrue(crypto.isValid(), () -> "crypto schema: " + crypto.issues());
    assertEquals(
        List.of("V001__streamrune_baseline.sql"),
        appliedScripts(dataSource, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE),
        "the history must record the script under the name Flyway's own scan gives it");
    assertEquals(
        List.of("V001__crypto_baseline.sql"),
        appliedScripts(dataSource, "flyway_schema_history_crypto"));
  }

  @Test
  void initializeSchema_refusesNamingTheScript_whenAnEventStoreScriptIsMissing() throws Exception {
    var dataSource = dataSource("lookup_missing_eventstore");

    var refused =
        assertThrows(
            IllegalStateException.class,
            () ->
                withContextClassLoader(
                    new NativeImageLikeClassLoader(Set.of(EVENT_STORE_SCRIPT)),
                    () ->
                        new PostgresEventStoreFactory(
                                dataSource, SimpleEventTypeRegistry.builder().build())
                            .initializeSchema()));

    assertTrue(refused.getMessage().contains(EVENT_STORE_SCRIPT), refused.getMessage());
    assertTrue(
        refused.getMessage().contains("db/streamrune-migration/*.sql"),
        () -> "the message must name the resource registration a native image needs: " + refused);
    assertFalse(
        tableExists(dataSource, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE),
        "nothing may be written before every script has been found");
  }

  @Test
  void initializeSchema_refusesBeforeAnyMigration_whenTheCryptoScriptIsMissing() throws Exception {
    var dataSource = dataSource("lookup_missing_crypto");
    CryptoEngine engine = PostgresCryptoEngine.builder().dataSource(dataSource).build();

    var refused =
        assertThrows(
            IllegalStateException.class,
            () ->
                withContextClassLoader(
                    new NativeImageLikeClassLoader(Set.of(CRYPTO_SCRIPT)),
                    () ->
                        new PostgresEventStoreFactory(
                                dataSource, SimpleEventTypeRegistry.builder().build())
                            .cryptoEngine(engine)
                            .initializeSchema()));

    assertTrue(refused.getMessage().contains(CRYPTO_SCRIPT), refused.getMessage());
    assertTrue(
        refused.getMessage().contains("db/crypto-migration/*.sql"),
        () -> "the message must name the resource registration a native image needs: " + refused);
    // Both series are resolved before either is applied: a missing crypto script leaves the
    // database untouched rather than half-provisioned.
    assertFalse(tableExists(dataSource, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE));
    assertFalse(tableExists(dataSource, "flyway_schema_history_crypto"));
  }

  /**
   * The script lists are maintained by hand, so each is compared with what Flyway's own classpath
   * scan resolves on the JVM: the same migrations, under the same script names and with the same
   * checksums. A script added to a series directory but not to its list fails here.
   */
  @Test
  void everyShippedSeriesResolvesExactlyWhatFlywaysClassPathScanResolves() throws Exception {
    var dataSource = dataSource("lookup_pin");
    for (ShippedMigrationSeries series :
        List.of(ShippedMigrationSeries.EVENT_STORE, ShippedMigrationSeries.CRYPTO)) {
      List<String> scanned =
          describe(
              Flyway.configure()
                  .dataSource(dataSource)
                  .locations(series.location())
                  .table("pin_history")
                  .load()
                  .info()
                  .all());
      List<String> listed =
          describe(series.configure(dataSource).table("pin_history").load().info().all());

      assertFalse(scanned.isEmpty(), () -> series.location() + " must hold migrations");
      assertEquals(scanned, listed, series.location());
    }
    assertEquals(
        "classpath:db/streamrune-migration",
        ShippedMigrationSeries.EVENT_STORE.location(),
        "the event-store series is the location the factory documents");
    assertEquals(List.of(EVENT_STORE_SCRIPT), ShippedMigrationSeries.EVENT_STORE.scriptPaths());
    assertEquals(List.of(CRYPTO_SCRIPT), ShippedMigrationSeries.CRYPTO.scriptPaths());
  }

  private static List<String> describe(org.flywaydb.core.api.MigrationInfo[] migrations) {
    return java.util.Arrays.stream(migrations)
        .map(
            m ->
                m.getVersion()
                    + " | "
                    + m.getDescription()
                    + " | "
                    + m.getScript()
                    + " | "
                    + m.getChecksum()
                    + " | "
                    + m.getType())
        .toList();
  }

  // ---------------------------------------------------------------------------------------------

  private static PGSimpleDataSource dataSource(String schema) throws SQLException {
    var admin = new PGSimpleDataSource();
    admin.setUrl(PG.getJdbcUrl());
    admin.setUser(PG.getUsername());
    admin.setPassword(PG.getPassword());
    try (var conn = admin.getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
    }
    var scoped = new PGSimpleDataSource();
    scoped.setUrl(PG.getJdbcUrl());
    scoped.setUser(PG.getUsername());
    scoped.setPassword(PG.getPassword());
    scoped.setCurrentSchema(schema);
    return scoped;
  }

  private static boolean tableExists(PGSimpleDataSource dataSource, String table)
      throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
      ps.setString(1, conn.getSchema());
      ps.setString(2, table);
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static List<String> appliedScripts(PGSimpleDataSource dataSource, String historyTable)
      throws SQLException {
    var scripts = new java.util.ArrayList<String>();
    try (var conn = dataSource.getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT script FROM "
                    + historyTable
                    + " WHERE type = 'SQL' ORDER BY installed_rank")) {
      while (rs.next()) {
        scripts.add(rs.getString(1));
      }
    }
    return scripts;
  }

  private static <T> T withContextClassLoader(ClassLoader loader, Supplier<T> body) {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(loader);
    try {
      return body.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  /**
   * Answers resource lookups for the two migration series the way a GraalVM native image does:
   * directories and scripts come back as {@code resource:} URLs, which Flyway's classpath scan
   * cannot list, a script still opens when looked up by its exact name, and a script named in
   * {@code unregistered} is absent, as one left out of the image's resource registration is.
   * Everything else is delegated to the test class path.
   */
  static final class NativeImageLikeClassLoader extends ClassLoader {

    private static final List<String> DIRECTORIES =
        List.of("db/streamrune-migration", "db/crypto-migration");

    private final Set<String> unregistered;

    NativeImageLikeClassLoader(Set<String> unregistered) {
      super(MigrationScriptLookupIT.class.getClassLoader());
      this.unregistered = unregistered;
    }

    @Override
    public URL getResource(String name) {
      String path = strip(name);
      if (DIRECTORIES.contains(path)) {
        return resourceUrl(path, null);
      }
      if (DIRECTORIES.stream().anyMatch(directory -> path.startsWith(directory + "/"))) {
        if (unregistered.contains(path)) {
          return null;
        }
        URL backing = getParent().getResource(path);
        return backing == null ? null : resourceUrl(path, backing);
      }
      return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      String path = strip(name);
      if (DIRECTORIES.stream()
          .anyMatch(directory -> path.equals(directory) || path.startsWith(directory + "/"))) {
        URL url = getResource(path);
        return url == null ? Collections.emptyEnumeration() : Collections.enumeration(List.of(url));
      }
      return super.getResources(name);
    }

    private static String strip(String name) {
      String path = name.startsWith("/") ? name.substring(1) : name;
      return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    private static URL resourceUrl(String path, URL backing) {
      URLStreamHandler handler =
          new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL url) throws IOException {
              if (backing == null) {
                throw new IOException("a directory has no content: " + url);
              }
              return backing.openConnection();
            }
          };
      try {
        return URL.of(URI.create("resource:/" + path), handler);
      } catch (java.net.MalformedURLException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}

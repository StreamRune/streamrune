package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.LogSanitizer;

/**
 * The PostgreSQL server versions StreamRune supports, and the startup check that enforces them.
 *
 * <p><b>PostgreSQL 17 or newer.</b> The project's CI runs every PostgreSQL-backed suite on the
 * newest supported server (18) and again on 17 (the {@code postgres-17} job); nothing older is
 * tested, so nothing older is supported. {@link PostgresEventStoreFactory#create()} calls {@link
 * #requireSupported(DataSource)} before it touches the schema, so the three framework integrations
 * (Spring, Quarkus, Micronaut — each builds its event store through that factory) refuse an older
 * server at boot, with a message naming both versions, instead of failing later on whichever
 * statement first needs something the older server does not have. The check cannot be switched off
 * with {@link PostgresEventStoreFactory#autoInitializeSchema(boolean)} or {@link
 * PostgresEventStoreFactory#validateSchema(boolean)}: those govern the schema, this governs the
 * product.
 *
 * <p>Applications that build {@code PostgresSagaStore}, {@code JdbcProjectionRepository} and the
 * other stores straight from a {@link DataSource}, without the factory, can call {@link
 * #requireSupported(DataSource)} themselves at startup.
 */
public final class PostgresServerVersion {

  /** The oldest PostgreSQL major version StreamRune supports. */
  public static final int MINIMUM_MAJOR_VERSION = 17;

  /** The product name the PostgreSQL JDBC driver reports. */
  private static final String PRODUCT_NAME = "PostgreSQL";

  private PostgresServerVersion() {}

  /**
   * Refuses a PostgreSQL server older than {@value #MINIMUM_MAJOR_VERSION}.
   *
   * <p>Reads the server version the driver already holds for the connection (the {@code
   * server_version} the server reports at connection start), so it costs one connection checkout
   * and no query, and it writes nothing: calling it again, or never, changes no state.
   *
   * @param dataSource the data source the event store will use
   * @throws UnsupportedServerVersionException if the server's major version is below {@value
   *     #MINIMUM_MAJOR_VERSION}
   * @throws EventStoreException if the server version cannot be read (the database is unreachable)
   */
  public static void requireSupported(DataSource dataSource) {
    requireSupportedVersion(read(dataSource, false));
  }

  /**
   * As {@link #requireSupported(DataSource)}, after first refusing a database that is not
   * PostgreSQL at all, in the same connection checkout: {@link PostgresBackend#verify} runs it when
   * the {@code postgres} backend is selected. Without the product check another database would be
   * judged by its own version number ("H2 2.3 is older than PostgreSQL 17").
   *
   * @param dataSource the application's data source
   * @throws IllegalStateException if the database is not PostgreSQL; the message names the {@code
   *     postgres} backend and the database's product and version
   * @throws UnsupportedServerVersionException if the server's major version is below {@value
   *     #MINIMUM_MAJOR_VERSION}
   * @throws EventStoreException if the server cannot be read (the database is unreachable)
   */
  static void requirePostgresSupported(DataSource dataSource) {
    ServerInfo server = read(dataSource, true);
    if (!PRODUCT_NAME.equalsIgnoreCase(server.product())) {
      throw new IllegalStateException(
          "The 'postgres' backend (streamrune.backend=postgres) needs a PostgreSQL database, but"
              + " the DataSource connects to "
              + LogSanitizer.sanitizeForLog(server.product())
              + " "
              + LogSanitizer.sanitizeForLog(server.version())
              + ". Point the DataSource at PostgreSQL "
              + MINIMUM_MAJOR_VERSION
              + " or newer, or select the backend of this database.");
    }
    requireSupportedVersion(server);
  }

  /**
   * What the driver reports about the server at connection start; {@code product} is {@code null}
   * when it was not asked for.
   */
  private record ServerInfo(String product, int major, String version) {}

  /**
   * Reads the server's version, and its product name when {@code withProduct}: {@link
   * #requireSupported(DataSource)} asks the driver for exactly what it always asked for.
   */
  private static ServerInfo read(DataSource dataSource, boolean withProduct) {
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      return new ServerInfo(
          withProduct ? meta.getDatabaseProductName() : null,
          meta.getDatabaseMajorVersion(),
          meta.getDatabaseProductVersion());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Cannot read the PostgreSQL server version (StreamRune requires PostgreSQL "
              + MINIMUM_MAJOR_VERSION
              + " or newer): "
              + LogSanitizer.sanitizeForLog(e.getMessage()),
          e);
    }
  }

  private static void requireSupportedVersion(ServerInfo server) {
    if (server.major() < MINIMUM_MAJOR_VERSION) {
      String shown = LogSanitizer.sanitizeForLog(server.version());
      throw new UnsupportedServerVersionException(
          "Unsupported PostgreSQL server: PostgreSQL "
              + shown
              + " is older than StreamRune supports. StreamRune requires PostgreSQL "
              + MINIMUM_MAJOR_VERSION
              + " or newer (its CI tests 17 and 18). Upgrade the database server; nothing was"
              + " migrated or written.",
          server.version(),
          MINIMUM_MAJOR_VERSION);
    }
  }
}

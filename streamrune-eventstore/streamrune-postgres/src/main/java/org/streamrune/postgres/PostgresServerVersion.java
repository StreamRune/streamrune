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
    int major;
    String version;
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      major = meta.getDatabaseMajorVersion();
      version = meta.getDatabaseProductVersion();
    } catch (SQLException e) {
      throw new EventStoreException(
          "Cannot read the PostgreSQL server version (StreamRune requires PostgreSQL "
              + MINIMUM_MAJOR_VERSION
              + " or newer): "
              + LogSanitizer.sanitizeForLog(e.getMessage()),
          e);
    }
    if (major < MINIMUM_MAJOR_VERSION) {
      String shown = LogSanitizer.sanitizeForLog(version);
      throw new UnsupportedServerVersionException(
          "Unsupported PostgreSQL server: PostgreSQL "
              + shown
              + " is older than StreamRune supports. StreamRune requires PostgreSQL "
              + MINIMUM_MAJOR_VERSION
              + " or newer (its CI tests 17 and 18). Upgrade the database server; nothing was"
              + " migrated or written.",
          version,
          MINIMUM_MAJOR_VERSION);
    }
  }
}

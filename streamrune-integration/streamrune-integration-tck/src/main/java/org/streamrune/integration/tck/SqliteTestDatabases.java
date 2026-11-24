package org.streamrune.integration.tck;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.streamrune.integration.BackendSelection;

/**
 * SQLite databases for the TCK's tests on the SQLite backend: each a HikariCP pool on a new
 * database file in one directory, with the connection settings the backend requires as JDBC URL
 * parameters ({@link #REQUIRED_SETTINGS}) and connections in autocommit mode, HikariCP's default,
 * as an application configures its pool.
 *
 * <p>The application a pool is given to closes it when it closes, as a container closes an
 * application's connection pool; {@link #close()} closes every pool this instance created that is
 * still open, so a test leaves no pool behind, whatever it started.
 */
public final class SqliteTestDatabases implements AutoCloseable {

  /**
   * The connection settings the SQLite backend requires, as JDBC URL parameters: write-ahead
   * logging, {@code synchronous=FULL}, foreign keys, a {@code busy_timeout} of 5000 ms and {@code
   * transaction_mode=IMMEDIATE}.
   */
  public static final String REQUIRED_SETTINGS =
      "journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000"
          + "&transaction_mode=IMMEDIATE";

  private final Path directory;
  private final List<HikariDataSource> pools = new ArrayList<>();
  private final Map<DataSource, Path> files = new IdentityHashMap<>();
  private int created;

  /**
   * Databases in {@code directory}.
   *
   * @param directory an existing directory the files are created in
   * @throws IllegalArgumentException if {@code directory} is {@code null}
   */
  public SqliteTestDatabases(Path directory) {
    if (directory == null) {
      throw new IllegalArgumentException("directory is required");
    }
    this.directory = directory;
  }

  /**
   * A pool on a new database file, {@code db-<n>.sqlite}, that no earlier call returned.
   *
   * @return the pool
   */
  public synchronized HikariDataSource create() {
    created++;
    return poolOn(directory.resolve("db-" + created + ".sqlite"));
  }

  /**
   * A new pool on the file of {@code pool}: what an application that starts again on the same
   * database opens, once the container closed the pool of its first start.
   *
   * @param pool a pool this instance created, open or closed
   * @return a new pool on the same file
   * @throws IllegalArgumentException if this instance did not create {@code pool}
   */
  public synchronized HikariDataSource reopen(DataSource pool) {
    return poolOn(fileOf(pool));
  }

  private HikariDataSource poolOn(Path file) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url(file));
    config.setPoolName("tck-sqlite-" + (pools.size() + 1));
    HikariDataSource pool = new HikariDataSource(config);
    pools.add(pool);
    files.put(pool, file);
    return pool;
  }

  /**
   * The database file a pool of this instance is on.
   *
   * @param pool a pool this instance created
   * @return its file
   * @throws IllegalArgumentException if this instance did not create {@code pool}
   */
  public synchronized Path fileOf(DataSource pool) {
    Path file = files.get(pool);
    if (file == null) {
      throw new IllegalArgumentException("pool was not created by these databases");
    }
    return file;
  }

  /**
   * The JDBC URL of {@code file} with {@link #REQUIRED_SETTINGS}.
   *
   * @param file the database file
   * @return the URL
   */
  public static String url(Path file) {
    return "jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS;
  }

  /**
   * The configuration of a start on the SQLite backend: {@code properties} with {@value
   * BackendSelection#PROPERTY}{@code =sqlite} when the application is given a database, and
   * unchanged without one, where the integration opens no backend.
   *
   * @param dataSource the application's {@code DataSource}, or {@code null}
   * @param properties the application's configuration
   * @return the configuration to start with
   */
  public static Map<String, String> onSqlite(
      DataSource dataSource, Map<String, String> properties) {
    if (dataSource == null) {
      return properties;
    }
    Map<String, String> selected = new LinkedHashMap<>(properties);
    selected.put(BackendSelection.PROPERTY, "sqlite");
    return selected;
  }

  /** Closes every pool this instance created that is still open. */
  @Override
  public synchronized void close() {
    for (HikariDataSource pool : pools) {
      pool.close();
    }
  }
}

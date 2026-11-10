package org.streamrune.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The PostgreSQL database a benchmark trial runs against: a schema of its own, created empty when
 * the trial starts and migrated by the framework's own Flyway series through {@link
 * PostgresEventStoreFactory}, behind a small HikariCP pool.
 *
 * <p><b>Which server.</b> When the environment variable {@value #URL_ENV} is set, the benchmarks
 * connect to that server as {@value #USER_ENV} with {@value #PASSWORD_ENV}; the nightly workflow
 * points them at the job's PostgreSQL service this way. Environment variables, because JMH runs
 * every benchmark in a JVM of its own, which inherits the environment and not the system
 * properties. Without the URL the first trial of a JVM starts a Testcontainers {@code
 * PostgreSQLContainer} on {@link PostgresTestImage#NAME}, which every later trial of the same JVM
 * reuses.
 *
 * <p><b>A fresh schema per trial.</b> {@link #open()} drops and creates the schema {@value #SCHEMA}
 * and nothing else, so a server shared with other data keeps it; the pool's connections use that
 * schema as their search path. A trial therefore never sees the rows of an earlier one, whatever
 * the order JMH runs them in.
 */
final class BenchmarkDatabase implements AutoCloseable {

  static final String URL_ENV = "STREAMRUNE_BENCH_JDBC_URL";
  static final String USER_ENV = "STREAMRUNE_BENCH_JDBC_USER";
  static final String PASSWORD_ENV = "STREAMRUNE_BENCH_JDBC_PASSWORD";

  /** The schema every trial recreates; the benchmarks write nowhere else. */
  static final String SCHEMA = "streamrune_bench";

  private static PostgreSQLContainer container;

  private final HikariDataSource dataSource;

  private BenchmarkDatabase(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  /** Recreates {@value #SCHEMA} empty and opens a pool on it. */
  static BenchmarkDatabase open() {
    Server server = server();
    try (var connection =
            DriverManager.getConnection(server.url(), server.user(), server.password());
        var statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
      statement.execute("CREATE SCHEMA " + SCHEMA);
    } catch (SQLException e) {
      throw new IllegalStateException("Cannot recreate the benchmark schema " + SCHEMA, e);
    }
    var config = new HikariConfig();
    config.setJdbcUrl(server.url());
    config.setUsername(server.user());
    config.setPassword(server.password());
    config.setSchema(SCHEMA);
    config.setMaximumPoolSize(2);
    config.setMinimumIdle(2);
    config.setPoolName("streamrune-bench");
    return new BenchmarkDatabase(new HikariDataSource(config));
  }

  /**
   * Builds an event store the way an application does, through {@link PostgresEventStoreFactory}:
   * the first call of a trial applies the migrations to the empty schema, every call validates it.
   */
  EventStore eventStore(EventTypeRegistry registry, List<EventUpcaster> upcasters) {
    return new PostgresEventStoreFactory(dataSource, registry).upcasters(upcasters).create();
  }

  /**
   * Empties the event stream and rewinds the global-offset counter, as an empty store starts, so an
   * iteration of an append benchmark starts on an empty table instead of on every row the
   * iterations before it wrote.
   */
  void clearEvents() {
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("TRUNCATE event_stream");
      statement.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    } catch (SQLException e) {
      throw new IllegalStateException("Cannot clear the benchmark event stream", e);
    }
  }

  @Override
  public void close() {
    dataSource.close();
  }

  private static synchronized Server server() {
    String url = setting(URL_ENV);
    if (url != null) {
      return new Server(url, setting(USER_ENV), setting(PASSWORD_ENV));
    }
    if (container == null) {
      var started = new PostgreSQLContainer(PostgresTestImage.NAME);
      started.start();
      container = started;
    }
    return new Server(container.getJdbcUrl(), container.getUsername(), container.getPassword());
  }

  private static String setting(String name) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? null : value;
  }

  private record Server(String url, String user, String password) {}
}

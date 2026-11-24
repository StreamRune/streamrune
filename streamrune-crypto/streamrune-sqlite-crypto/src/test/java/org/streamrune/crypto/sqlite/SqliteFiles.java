package org.streamrune.crypto.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteSchemaSeries;

/** Data sources and databases for the tests: SQLite files with the required settings. */
final class SqliteFiles {

  /** The connection settings StreamRune requires, as JDBC URL parameters. */
  static final String REQUIRED_SETTINGS =
      "journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000"
          + "&transaction_mode=IMMEDIATE";

  private SqliteFiles() {}

  /** A data source on {@code file} with every required setting: a new connection per request. */
  static DataSource dataSource(Path file) {
    return url("jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS);
  }

  /** As {@link #dataSource(Path)}, with {@code override} after the required settings. */
  static DataSource dataSource(Path file, String override) {
    return url("jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS + "&" + override);
  }

  /**
   * A HikariCP pool of {@code size} connections on {@code file}, whose borrow waits at most ten
   * seconds, so a call that waits for its own connection fails instead of hanging.
   */
  static HikariDataSource pool(Path file, int size) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS);
    config.setMaximumPoolSize(size);
    config.setConnectionTimeout(10_000);
    return new HikariDataSource(config);
  }

  /** Opens {@code dataSource} and applies the crypto series. */
  static SqliteDatabase migrated(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    database.migrate(SqliteSchemaSeries.CRYPTO);
    return database;
  }

  /** Runs each statement on its own autocommit connection. */
  static void execute(DataSource dataSource, String... statements) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement statement = conn.createStatement()) {
      for (String sql : statements) {
        statement.execute(sql);
      }
    }
  }

  /** The rows of {@code sql}, each as its columns' values. */
  static List<List<Object>> query(DataSource dataSource, String sql) throws SQLException {
    List<List<Object>> rows = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
        Statement statement = conn.createStatement();
        var rs = statement.executeQuery(sql)) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columns; i++) {
          row.add(rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /** A data source on exactly {@code url}: every connection comes from the driver. */
  static DataSource url(String url) {
    return new DataSource() {
      @Override
      public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
      }

      @Override
      public Connection getConnection(String user, String password) throws SQLException {
        return getConnection();
      }

      @Override
      public PrintWriter getLogWriter() {
        return null;
      }

      @Override
      public void setLogWriter(PrintWriter out) {}

      @Override
      public void setLoginTimeout(int seconds) {}

      @Override
      public int getLoginTimeout() {
        return 0;
      }

      @Override
      public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
      }

      @Override
      public <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLException("not a wrapper");
      }

      @Override
      public boolean isWrapperFor(Class<?> type) {
        return false;
      }

      @Override
      public String toString() {
        return url;
      }
    };
  }

  /**
   * A data source that passes connections through from {@code delegate} until {@link
   * Switchable#fail()}, after which every connection request throws a {@link SQLException}, as an
   * unreachable database does. A test opens the handle first and switches afterwards.
   */
  static final class Switchable implements DataSource {
    private final DataSource delegate;
    private volatile boolean failing;

    Switchable(DataSource delegate) {
      this.delegate = delegate;
    }

    void fail() {
      failing = true;
    }

    @Override
    public Connection getConnection() throws SQLException {
      if (failing) {
        throw new SQLException("connection unavailable");
      }
      return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String user, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }
}

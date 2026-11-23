package org.streamrune.sqlite;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/** Data sources for the tests: the required connection settings, or one of them changed. */
final class TestDatabases {

  /** The connection settings StreamRune requires, as JDBC URL parameters. */
  static final String REQUIRED_SETTINGS =
      "journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000"
          + "&transaction_mode=IMMEDIATE";

  private TestDatabases() {}

  /** A data source on {@code file} with every required setting. */
  static DataSource dataSource(Path file) {
    return url("jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS);
  }

  /** A data source on {@code file} with the required settings followed by {@code override}. */
  static DataSource dataSource(Path file, String override) {
    return url("jdbc:sqlite:file:" + file + "?" + REQUIRED_SETTINGS + "&" + override);
  }

  /**
   * A data source on exactly {@code url}: every connection comes from the driver with the URL
   * alone, as a pool such as HikariCP creates them, so the URL parameters are the connection
   * settings.
   */
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
}

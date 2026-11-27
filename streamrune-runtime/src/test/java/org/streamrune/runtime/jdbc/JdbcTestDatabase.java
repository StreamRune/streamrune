package org.streamrune.runtime.jdbc;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A temp-file SQLite database for the tests of the store helpers: real JDBC without Docker. The
 * driver is on the test classpath only and is reached through {@link DriverManager}, so no test
 * names a driver type.
 */
final class JdbcTestDatabase {

  private final String url;

  private JdbcTestDatabase(String url) {
    this.url = url;
  }

  /**
   * A database file in {@code dir} with {@code ddl} applied, one statement per string.
   *
   * @param dir a temporary directory the test owns
   * @param ddl the statements to run
   */
  static JdbcTestDatabase create(Path dir, String... ddl) throws SQLException {
    var db = new JdbcTestDatabase("jdbc:sqlite:" + dir.resolve("helpers.db"));
    try (Connection c = db.connection();
        Statement st = c.createStatement()) {
      for (String statement : ddl) {
        st.execute(statement);
      }
    }
    return db;
  }

  /** A new autocommit connection; the caller closes it. */
  Connection connection() throws SQLException {
    return DriverManager.getConnection(url);
  }
}

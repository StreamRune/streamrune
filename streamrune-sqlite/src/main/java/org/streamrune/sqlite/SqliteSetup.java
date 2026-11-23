package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Objects;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConnection;
import org.streamrune.core.types.LogSanitizer;

/**
 * What {@link SqliteDatabase#open} reads from a live connection: that it is SQLite, the database
 * file, the library version and the connection settings StreamRune requires.
 *
 * <p>Everything comes from the connection, never from configuration strings. The database file is
 * the path SQLite itself opened ({@code PRAGMA database_list}), so every spelling of a URL that
 * names one file ends at one path, and an in-memory or temporary database, which has no file, is
 * recognized whatever its URL looks like.
 */
final class SqliteSetup {

  /** {@code PRAGMA synchronous} reports FULL as 2 and EXTRA as 3. */
  private static final int SYNCHRONOUS_FULL = 2;

  private SqliteSetup() {}

  /**
   * The driver's connection behind {@code connection}.
   *
   * @throws IllegalStateException when the connection is not a SQLite connection
   */
  static SQLiteConnection sqliteConnection(Connection connection) throws SQLException {
    if (!connection.isWrapperFor(SQLiteConnection.class)) {
      throw new IllegalStateException(
          "SqliteDatabase needs a DataSource of SQLite connections (the org.xerial sqlite-jdbc"
              + " driver), but its connection is not a SQLite connection: "
              + LogSanitizer.sanitizeFreeText(connection.getClass().getName()));
    }
    return connection.unwrap(SQLiteConnection.class);
  }

  /**
   * The file of the {@code main} database, as SQLite resolved it.
   *
   * @throws IllegalStateException when the database is in-memory or temporary
   */
  static Path databaseFile(SQLiteConnection connection) throws SQLException {
    String file =
        Objects.requireNonNullElse(
            queryString(connection, "SELECT file FROM pragma_database_list WHERE name = 'main'"),
            "");
    if (file.isEmpty()) {
      throw new IllegalStateException(
          "The SQLite URL '"
              + LogSanitizer.sanitizeFreeText(connection.getMetaData().getURL())
              + "' opens an in-memory or temporary database, which has no file: each pooled"
              + " connection would get its own private database, and nothing would survive a"
              + " restart. Point the URL at a database file, for example jdbc:sqlite:/var/lib/app/"
              + "app.db.");
    }
    return Path.of(file);
  }

  /** The version of the SQLite library the driver loaded. */
  static String libraryVersion(SQLiteConnection connection) throws SQLException {
    return queryString(connection, "select sqlite_version()");
  }

  /**
   * Refuses a connection whose settings differ from what StreamRune requires: {@code
   * journal_mode=WAL}, {@code synchronous=FULL} (or the stricter {@code EXTRA}), {@code
   * foreign_keys=ON}, a {@code busy_timeout} above 0 and {@code transaction_mode=IMMEDIATE}.
   *
   * @throws IllegalStateException naming the first wrong setting, its value and the expected value
   */
  static void verify(SQLiteConnection connection) throws SQLException {
    String journalMode = queryString(connection, "PRAGMA journal_mode");
    if (!"wal".equalsIgnoreCase(journalMode)) {
      throw wrong("journal_mode", journalMode, "journal_mode=WAL", "journal_mode=WAL");
    }
    int synchronous = queryInt(connection, "PRAGMA synchronous");
    if (synchronous < SYNCHRONOUS_FULL) {
      throw wrong(
          "synchronous",
          Integer.toString(synchronous),
          "synchronous=FULL (2) or EXTRA (3)",
          "synchronous=FULL");
    }
    int foreignKeys = queryInt(connection, "PRAGMA foreign_keys");
    if (foreignKeys != 1) {
      throw wrong(
          "foreign_keys", Integer.toString(foreignKeys), "foreign_keys=ON", "foreign_keys=true");
    }
    int busyTimeout = queryInt(connection, "PRAGMA busy_timeout");
    if (busyTimeout <= 0) {
      throw wrong(
          "busy_timeout",
          busyTimeout + " ms",
          "busy_timeout greater than 0 ms (StreamRune documents 5000)",
          "busy_timeout=5000");
    }
    SQLiteConfig.TransactionMode transactionMode =
        connection.getConnectionConfig().getTransactionMode();
    if (transactionMode != SQLiteConfig.TransactionMode.IMMEDIATE) {
      throw wrong(
          "transaction_mode",
          transactionMode.name(),
          "transaction_mode=IMMEDIATE",
          "transaction_mode=IMMEDIATE");
    }
  }

  private static IllegalStateException wrong(
      String setting, String actual, String expected, String example) {
    return new IllegalStateException(
        "The SQLite connection setting "
            + setting
            + " is "
            + LogSanitizer.sanitizeForLog(actual.toUpperCase(Locale.ROOT))
            + ", but StreamRune requires "
            + expected
            + ". Set it in the connection properties of the DataSource (for example "
            + example
            + " as a JDBC URL parameter or a sqlite-jdbc SQLiteConfig property).");
  }

  private static String queryString(SQLiteConnection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      result.next();
      return result.getString(1);
    }
  }

  private static int queryInt(SQLiteConnection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      result.next();
      return result.getInt(1);
    }
  }
}

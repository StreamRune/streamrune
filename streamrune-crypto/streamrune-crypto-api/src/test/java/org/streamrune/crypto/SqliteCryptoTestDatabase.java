package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * A SQLite file created from the crypto DDL that {@code streamrune-sqlite-crypto} ships (its path
 * arrives in the system property {@code streamrune.crypto.sqlite-ddl}, set by this module's build),
 * and the test-owned write transaction the shared JDBC helpers run in.
 */
final class SqliteCryptoTestDatabase {

  /** The connection methods a shared JDBC helper must never call: the caller owns them. */
  private static final Set<String> CALLER_OWNED =
      Set.of("commit", "rollback", "close", "setAutoCommit");

  private final String url;

  private SqliteCryptoTestDatabase(String url) {
    this.url = url;
  }

  /** Creates the database file in {@code dir} and applies the shipped crypto DDL to it. */
  static SqliteCryptoTestDatabase create(Path dir) throws IOException, SQLException {
    String ddlPath = System.getProperty("streamrune.crypto.sqlite-ddl");
    if (ddlPath == null) {
      throw new IllegalStateException(
          "system property streamrune.crypto.sqlite-ddl is not set: run the test through Gradle");
    }
    String ddl = Files.readString(Path.of(ddlPath)).replaceAll("--[^\n]*", "");
    SqliteCryptoTestDatabase db =
        new SqliteCryptoTestDatabase("jdbc:sqlite:" + dir.resolve("crypto.db"));
    try (Connection conn = db.open();
        Statement st = conn.createStatement()) {
      for (String statement : ddl.split(";")) {
        if (!statement.isBlank()) {
          st.executeUpdate(statement);
        }
      }
    }
    return db;
  }

  /** A new connection to the database, in autoCommit mode. */
  Connection open() throws SQLException {
    return DriverManager.getConnection(url);
  }

  /** One unit of work on the test's connection. */
  @FunctionalInterface
  interface Work<T> {
    T run(Connection conn) throws Exception;
  }

  /**
   * Runs {@code work} inside a {@code BEGIN IMMEDIATE} ... {@code COMMIT} that the test owns, on a
   * connection that fails the test when the work commits, rolls back, closes it or changes its
   * autoCommit. A failure rolls the transaction back and propagates unchanged.
   */
  <T> T inWrite(Work<T> work) throws Exception {
    try (Connection conn = open()) {
      execute(conn, "BEGIN IMMEDIATE");
      T result;
      try {
        result = work.run(callerOwnedGuard(conn));
      } catch (Exception | Error e) {
        execute(conn, "ROLLBACK");
        throw e;
      }
      execute(conn, "COMMIT");
      return result;
    }
  }

  /** {@link #inWrite} of a work that answers a yes or no. */
  boolean inWriteBoolean(Work<Boolean> work) throws Exception {
    return inWrite(work);
  }

  /** {@link #inWrite} of a work that reads an optional instant. */
  java.util.Optional<java.time.Instant> inWriteInstant(
      Work<java.util.Optional<java.time.Instant>> work) throws Exception {
    return inWrite(work);
  }

  /** Runs one SQL statement on a connection of its own, in autoCommit mode. */
  void update(String sql, Object... params) throws SQLException {
    try (Connection conn = open();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      ps.executeUpdate();
    }
  }

  /** The number of rows {@code sql} (a {@code SELECT count(*)}) counts. */
  long count(String sql, Object... params) throws SQLException {
    try (Connection conn = open();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  /** A single long value {@code sql} selects, or {@code null} when it selects no row or NULL. */
  Long selectLong(String sql, Object... params) throws SQLException {
    try (Connection conn = open();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.length; i++) {
        ps.setObject(i + 1, params[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        long value = rs.getLong(1);
        return rs.wasNull() ? null : value;
      }
    }
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  /**
   * A view of {@code conn} that fails the test on {@link #CALLER_OWNED} methods and delegates every
   * other call.
   */
  static Connection callerOwnedGuard(Connection conn) {
    return proxy(
        Connection.class,
        conn,
        (target, method, args) -> {
          if (CALLER_OWNED.contains(method.getName())) {
            fail("a shared JDBC helper called Connection." + method.getName());
          }
          return invoke(target, method, args);
        });
  }

  /** Handles one call on a proxy around {@code target}. */
  @FunctionalInterface
  interface Handler<T> {
    Object handle(T target, java.lang.reflect.Method method, Object[] args) throws Throwable;
  }

  /** A proxy of {@code type} around {@code target}, each call going through {@code handler}. */
  static <T> T proxy(Class<T> type, T target, Handler<T> handler) {
    InvocationHandler h = (p, method, args) -> handler.handle(target, method, args);
    return type.cast(
        Proxy.newProxyInstance(
            SqliteCryptoTestDatabase.class.getClassLoader(), new Class<?>[] {type}, h));
  }

  /** Calls {@code method} on {@code target}, rethrowing what the method itself threw. */
  static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}

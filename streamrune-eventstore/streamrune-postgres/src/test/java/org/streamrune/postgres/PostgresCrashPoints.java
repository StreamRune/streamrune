package org.streamrune.postgres;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.streamrune.test.EventStoreContract.CrashPoint;

/**
 * Kills a write of a store over a PostgreSQL database at a {@link CrashPoint}, the way the death of
 * the process running it would: the database loses the write's session.
 *
 * <p>A point before the write is durable is a row trigger on the table the write reaches there,
 * installed by {@link #install()}, whose backend terminates itself with {@code
 * pg_terminate_backend(pg_backend_pid())} in the middle of the statement, so PostgreSQL rolls back
 * the transaction as it does for a dead client. Each trigger fires once per {@link #arm}: it is
 * armed by setting its sequence so that the next {@code nextval} returns {@code 2}, and a sequence
 * is not rolled back with the transaction that advanced it. A point after the write is durable
 * cannot be reached from the server: the connections of {@link #connections(DataSource)} commit, or
 * run an autocommit update, then terminate their backend and report the lost acknowledgement a dead
 * connection reports, SQLState {@code 08006}.
 *
 * <p>Arming applies to the database, not to one store: the crash cases run one write at a time.
 */
final class PostgresCrashPoints {

  private static final String CONNECTION_FAILURE = "08006";

  private static final Map<CrashPoint, String> TRIGGERS = new EnumMap<>(CrashPoint.class);

  static {
    TRIGGERS.put(CrashPoint.AFTER_KEY_CLAIM, "AFTER INSERT ON command_inbox");
    TRIGGERS.put(CrashPoint.AFTER_OFFSET_RESERVATION, "AFTER UPDATE ON global_offset_sequence");
    TRIGGERS.put(CrashPoint.AFTER_FIRST_EVENT_WRITE, "AFTER INSERT ON event_stream");
    TRIGGERS.put(CrashPoint.AFTER_FIRST_OUTBOX_WRITE, "AFTER INSERT ON outbox_events");
    TRIGGERS.put(CrashPoint.DURING_SNAPSHOT_SAVE, "AFTER INSERT OR UPDATE ON snapshot_store");
  }

  private static final Set<CrashPoint> AFTER_DURABLE =
      Set.of(CrashPoint.AFTER_COMMIT, CrashPoint.AFTER_SNAPSHOT_COMMIT);

  /** A sequence value no write reaches by accident: a disarmed trigger never sees {@code 2}. */
  private static final long DISARMED = 1_000_000;

  private final DataSource admin;
  private volatile boolean dieAfterDurable;

  PostgresCrashPoints(DataSource admin) {
    this.admin = admin;
  }

  /** Installs the function, a sequence and a trigger per point, all disarmed. */
  void install() {
    try (var conn = admin.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE OR REPLACE FUNCTION contract_crash() RETURNS trigger LANGUAGE plpgsql AS $$"
              + " BEGIN"
              + "   IF nextval(TG_ARGV[0]::regclass) = 2 THEN"
              + "     PERFORM pg_terminate_backend(pg_backend_pid());"
              + "   END IF;"
              + "   RETURN NULL;"
              + " END $$");
      for (var trigger : TRIGGERS.entrySet()) {
        String name = name(trigger.getKey());
        stmt.execute("CREATE SEQUENCE IF NOT EXISTS " + name + " START WITH " + DISARMED);
        stmt.execute(
            "CREATE OR REPLACE TRIGGER "
                + name
                + " "
                + trigger.getValue()
                + " FOR EACH ROW EXECUTE FUNCTION contract_crash('"
                + name
                + "')");
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    disarm();
  }

  /** Disarms every point: no write dies until the next {@link #arm}. */
  void disarm() {
    dieAfterDurable = false;
    try (var conn = admin.getConnection();
        var stmt = conn.createStatement()) {
      for (CrashPoint point : TRIGGERS.keySet()) {
        stmt.execute("SELECT setval('" + name(point) + "', " + DISARMED + ")");
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Arms {@code point}: the next write over the database that reaches it dies there. */
  void arm(CrashPoint point) {
    if (AFTER_DURABLE.contains(point)) {
      dieAfterDurable = true;
      return;
    }
    try (var conn = admin.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("SELECT setval('" + name(point) + "', 1)");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * {@code source} with connections that, armed for a point after the write is durable, let the
   * next commit or autocommit update take effect and then lose their backend.
   */
  DataSource connections(DataSource source) {
    return new Connections(source);
  }

  private static String name(CrashPoint point) {
    return "contract_crash_" + point.name().toLowerCase(Locale.ROOT);
  }

  /**
   * Terminates the backend of {@code conn} and returns the exception a client whose connection died
   * sees.
   */
  private SQLException die(Connection conn) {
    dieAfterDurable = false;
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SELECT pg_terminate_backend(pg_backend_pid())");
    } catch (SQLException expected) {
      // the backend is gone, which is the point
    }
    return new SQLException(
        "An I/O error occurred while sending to the backend.", CONNECTION_FAILURE);
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private final class Connections implements DataSource {
    private final DataSource source;

    Connections(DataSource source) {
      this.source = source;
    }

    @Override
    public Connection getConnection() throws SQLException {
      return wrap(source.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return wrap(source.getConnection(username, password));
    }

    private Connection wrap(Connection conn) {
      InvocationHandler handler =
          (proxy, method, args) -> {
            Object result = invoke(conn, method, args);
            if (dieAfterDurable && method.getName().equals("commit")) {
              throw die(conn);
            }
            if (result instanceof Statement stmt
                && (method.getName().equals("prepareStatement")
                    || method.getName().equals("createStatement"))) {
              return statement(conn, stmt, method.getReturnType());
            }
            return result;
          };
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    private Object statement(Connection conn, Statement stmt, Class<?> type) {
      InvocationHandler handler =
          (proxy, method, args) -> {
            Object result = invoke(stmt, method, args);
            if (dieAfterDurable
                && method.getName().startsWith("execute")
                && !method.getName().equals("executeQuery")
                && conn.getAutoCommit()) {
              throw die(conn);
            }
            return result;
          };
      return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return source.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      source.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      source.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return source.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return source.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return source.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return source.isWrapperFor(iface);
    }
  }
}

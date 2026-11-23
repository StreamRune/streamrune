package org.streamrune.sqlite;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.sqlite.Function;
import org.sqlite.SQLiteConnection;
import org.streamrune.test.EventStoreContract.CrashPoint;

/**
 * Kills a write of a store over a SQLite database at a point inside it, the way the death of the
 * process running it would.
 *
 * <p>A point before the write is durable is a temporary trigger on the table the write reaches
 * there, which calls the Java function {@code sr_crash_point} in the middle of the statement that
 * writes the row. A point after the write is durable is the statement that makes the write durable:
 * a {@code COMMIT}, or a write statement run in autocommit, which SQLite commits on its own; the
 * statement runs, then the connection dies. The triggers and the function are installed on a
 * connection handed out while a point is armed; a point fires once and disarms itself.
 *
 * <p>How the write dies depends on the {@link Kill}: {@link Kill#HALT_PROCESS} stops the JVM at
 * once with {@link Runtime#halt(int)}, a real process death that a forked JVM runs; {@link
 * Kill#CLOSE_CONNECTION} fails the statement and then closes the physical connection on the next
 * call the store makes on it, which makes SQLite roll back the open transaction as it does for a
 * process that died inside one, and makes every later call fail as it would on a dead connection.
 * The store under test must be built over {@link #connections(DataSource)} of a data source that
 * opens a new physical connection on every borrow, so a closed one is never handed out again.
 */
final class SqliteCrashPoints {

  /** The exit code of a process stopped by {@link Kill#HALT_PROCESS}. */
  static final int HALTED = 86;

  /** How a write dies at its point. */
  enum Kill {
    CLOSE_CONNECTION,
    HALT_PROCESS
  }

  /** A point a write can die at: a row write on a table, or the commit. */
  record Point(String table, int row) {

    /** After the counter row was updated: the offsets are taken, no event is written. */
    static final Point OFFSET_RESERVATION = new Point("global_offset_counter", 1);

    /** Inside the write of the second event row: the first is written, the others are not. */
    static final Point SECOND_EVENT = new Point("event_stream", 2);

    /** Inside the write of the first snapshot_store row of the statement. */
    static final Point FIRST_SNAPSHOT_ROW = new Point("snapshot_store", 1);

    /** Inside the write of the second snapshot_store row of the statement: a purge half done. */
    static final Point SECOND_SNAPSHOT_ROW = new Point("snapshot_store", 2);

    /** Right after the write's COMMIT returned. */
    static final Point AFTER_COMMIT = new Point(null, 0);

    /** The point that matches {@code point} of the contract. */
    static Point of(CrashPoint point) {
      return switch (point) {
        case AFTER_OFFSET_RESERVATION -> OFFSET_RESERVATION;
        case AFTER_FIRST_EVENT_WRITE -> SECOND_EVENT;
        case DURING_SNAPSHOT_SAVE -> FIRST_SNAPSHOT_ROW;
        case AFTER_COMMIT, AFTER_SNAPSHOT_COMMIT -> AFTER_COMMIT;
        case AFTER_KEY_CLAIM, AFTER_FIRST_OUTBOX_WRITE ->
            throw new IllegalArgumentException("the store has no " + point + " yet");
      };
    }
  }

  /**
   * The triggers that call {@code sr_crash_point} for each row a statement writes, by table; only
   * the armed point's table gets them, so a database that lacks another table can be armed too.
   */
  private static final Map<String, List<String>> TRIGGERS =
      Map.of(
          "global_offset_counter",
          List.of(
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_counter AFTER UPDATE ON"
                  + " main.global_offset_counter BEGIN SELECT"
                  + " sr_crash_point('global_offset_counter'); END"),
          "event_stream",
          List.of(
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_event AFTER INSERT ON main.event_stream"
                  + " BEGIN SELECT sr_crash_point('event_stream'); END"),
          "snapshot_store",
          List.of(
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_snapshot_insert AFTER INSERT ON"
                  + " main.snapshot_store BEGIN SELECT sr_crash_point('snapshot_store'); END",
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_snapshot_update AFTER UPDATE ON"
                  + " main.snapshot_store BEGIN SELECT sr_crash_point('snapshot_store'); END"),
          "saga_state",
          List.of(
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_saga_insert AFTER INSERT ON"
                  + " main.saga_state BEGIN SELECT sr_crash_point('saga_state'); END",
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_saga_update AFTER UPDATE ON"
                  + " main.saga_state BEGIN SELECT sr_crash_point('saga_state'); END"),
          "saga_dead_letters",
          List.of(
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_dead_letter_insert AFTER INSERT ON"
                  + " main.saga_dead_letters BEGIN SELECT sr_crash_point('saga_dead_letters');"
                  + " END",
              "CREATE TEMP TRIGGER IF NOT EXISTS sr_crash_dead_letter_update AFTER UPDATE ON"
                  + " main.saga_dead_letters BEGIN SELECT sr_crash_point('saga_dead_letters');"
                  + " END"));

  private final Kill kill;
  private volatile Point armed;
  private final AtomicInteger rowsSeen = new AtomicInteger();

  SqliteCrashPoints(Kill kill) {
    this.kill = kill;
  }

  /** Arms {@code point}: the next write that reaches it dies there. */
  void arm(Point point) {
    rowsSeen.set(0);
    armed = point;
  }

  /** Arms the point of the contract's {@code point}. */
  void arm(CrashPoint point) {
    arm(Point.of(point));
  }

  /** Disarms: no write dies until the next {@link #arm}. */
  void disarm() {
    armed = null;
  }

  /** {@code source}, its connections armed while a point is. */
  DataSource connections(DataSource source) {
    return new Connections(source);
  }

  /** One borrowed connection: whether its write died, so that every later call fails. */
  private final class Session {
    private final Connection physical;
    private volatile boolean dead;

    /** Whether a BEGIN ran on the connection and no COMMIT or ROLLBACK after it. */
    private boolean inTransaction;

    Session(Connection physical) {
      this.physical = physical;
    }

    /** Called by the triggers inside the statement that writes a row of {@code table}. */
    void rowWritten(String table) throws SQLException {
      Point point = armed;
      if (point == null || !table.equals(point.table())) {
        return;
      }
      if (rowsSeen.incrementAndGet() < point.row()) {
        return;
      }
      armed = null;
      die();
      throw new SQLException("killed inside the write of a " + table + " row");
    }

    /**
     * Whether {@code sql} makes a write durable when it returns: a {@code COMMIT}, or a write
     * statement run outside a transaction, which SQLite commits on its own.
     */
    boolean endsWrite(String sql) {
      String statement = sql.strip().toUpperCase(Locale.ROOT);
      return statement.startsWith("COMMIT")
          || (!inTransaction
              && (statement.startsWith("INSERT")
                  || statement.startsWith("UPDATE")
                  || statement.startsWith("DELETE")));
    }

    /** Follows the transaction statements the connection runs. */
    void ran(String sql) {
      String statement = sql.strip().toUpperCase(Locale.ROOT);
      if (statement.startsWith("BEGIN")) {
        inTransaction = true;
      } else if (statement.startsWith("COMMIT") || statement.startsWith("ROLLBACK")) {
        inTransaction = false;
      }
    }

    /** Called after a statement that made a write durable returned. */
    void committed() throws SQLException {
      if (armed != Point.AFTER_COMMIT) {
        return;
      }
      armed = null;
      die();
      closePhysical();
      throw new SQLException("the connection died right after its COMMIT");
    }

    private void die() {
      if (kill == Kill.HALT_PROCESS) {
        Runtime.getRuntime().halt(HALTED);
      }
      dead = true;
    }

    void closePhysical() {
      try {
        physical.close();
      } catch (SQLException ignored) {
        // the connection is gone, which is the point
      }
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

    private Connection wrap(Connection conn) throws SQLException {
      Session session = new Session(conn);
      Point point = armed;
      if (point != null && point != Point.AFTER_COMMIT) {
        install(conn, session, point.table());
      }
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                String name = method.getName();
                if (session.dead) {
                  session.closePhysical();
                  if (name.equals("close") || name.equals("isClosed") || name.equals("abort")) {
                    return name.equals("isClosed") ? Boolean.TRUE : null;
                  }
                  throw new SQLException("the connection died with its write");
                }
                Object result = invoke(conn, method, args);
                if (name.equals("createStatement")) {
                  return statement(result, Statement.class, null, session);
                }
                if (name.equals("prepareStatement")) {
                  return statement(result, PreparedStatement.class, (String) args[0], session);
                }
                return result;
              });
    }

    /**
     * {@code target}, a statement of a wrapped connection, that reports each statement it runs to
     * {@code session} and lets a write made durable die after it: the SQL of a prepared statement
     * is {@code preparedSql}, that of a plain statement the argument of its execute call.
     */
    private Object statement(Object target, Class<?> type, String preparedSql, Session session) {
      return Proxy.newProxyInstance(
          type.getClassLoader(),
          new Class<?>[] {type},
          (proxy, method, args) -> {
            if (session.dead) {
              throw new SQLException("the connection died with its write");
            }
            boolean executing = method.getName().startsWith("execute");
            String sql =
                preparedSql != null
                    ? preparedSql
                    : args != null && args.length > 0 && args[0] instanceof String text
                        ? text
                        : null;
            boolean endsWrite = executing && sql != null && session.endsWrite(sql);
            Object result = invoke(target, method, args);
            if (executing && sql != null) {
              session.ran(sql);
            }
            if (endsWrite) {
              session.committed();
            }
            return result;
          });
    }

    private void install(Connection conn, Session session, String table) throws SQLException {
      Function.create(
          conn.unwrap(SQLiteConnection.class),
          "sr_crash_point",
          new Function() {
            @Override
            protected void xFunc() throws SQLException {
              session.rowWritten(value_text(0));
              result();
            }
          });
      try (Statement statement = conn.createStatement()) {
        for (String trigger : TRIGGERS.get(table)) {
          statement.execute(trigger);
        }
      }
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

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}

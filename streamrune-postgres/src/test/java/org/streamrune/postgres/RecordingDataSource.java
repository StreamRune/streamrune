package org.streamrune.postgres;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.postgresql.jdbc.PgStatement;

/**
 * A recording decorator over a real {@link DataSource}: every connection it hands out, and every
 * statement those connections create, is a {@link Proxy} that delegates to the real object and
 * appends what the store did to one transcript, in order, across connections.
 *
 * <p>The event vocabulary ({@code n} is the 1-based number of the connection, in the order they
 * were opened):
 *
 * <ul>
 *   <li>{@code open #n} — {@code getConnection} returned;
 *   <li>{@code #n autoCommit=<true|false>} — {@code setAutoCommit} (reading the mode is not
 *       recorded);
 *   <li>{@code #n prepare <sql>} — {@code prepareStatement} or {@code prepareCall};
 *   <li>{@code #n execute <sql>} — a statement ran SQL text handed to it ({@code execute}, {@code
 *       executeQuery}, {@code executeUpdate} or {@code executeLargeUpdate} with a SQL argument, the
 *       {@code createStatement()} path); a prepared statement's own execution is not recorded, its
 *       {@code prepare} event carries the SQL;
 *   <li>{@code #n fetchSize=<k>} and {@code #n queryTimeout=<seconds>} — the statement setters;
 *   <li>{@code #n queryTimeout=driver} — the statement was unwrapped to {@link PgStatement}, the
 *       way {@code StatementTimeout.applyTo} does before setting its millisecond timeout on the
 *       real statement (which still applies);
 *   <li>{@code #n commit}, {@code #n rollback} (either form), {@code #n close}.
 * </ul>
 *
 * <p>Two fault and race hooks: {@link #beforePrepare} runs caller code on the real connection right
 * before a matching statement is prepared, and {@link #failNextCloseAfterCommit} makes a committed
 * connection's close fail after the real close.
 *
 * <p>Thread-safe: the transcript and the counters are guarded by this object.
 */
final class RecordingDataSource implements DataSource {

  /** Caller code a {@link #beforePrepare} hook runs. */
  @FunctionalInterface
  interface SqlHook {
    void run(Connection sameConnection) throws SQLException;
  }

  private record PendingHook(String sqlPrefix, SqlHook hook) {}

  private static final Set<String> SQL_EXECUTES =
      Set.of("execute", "executeQuery", "executeUpdate", "executeLargeUpdate");

  private final DataSource delegate;
  private final List<String> events = new ArrayList<>();
  private final List<PendingHook> hooks = new ArrayList<>();
  private int numbered;
  private int open;
  private boolean failNextCloseAfterCommit;

  RecordingDataSource(DataSource delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate is required");
    }
    this.delegate = delegate;
  }

  /** Every event so far, in order, across connections. */
  synchronized List<String> transcript() {
    return List.copyOf(events);
  }

  /** The SQL of the {@code prepare} and {@code execute} events only, in order. */
  synchronized List<String> sql() {
    List<String> sql = new ArrayList<>();
    for (String event : events) {
      int space = event.indexOf(' ');
      if (event.startsWith("#") && space > 0) {
        String rest = event.substring(space + 1);
        if (rest.startsWith("prepare ")) {
          sql.add(rest.substring("prepare ".length()));
        } else if (rest.startsWith("execute ")) {
          sql.add(rest.substring("execute ".length()));
        }
      }
    }
    return List.copyOf(sql);
  }

  /** The connections opened since construction or the last {@link #clear()}. */
  synchronized int connectionsOpened() {
    return numbered;
  }

  /** The connections opened and not yet closed, at the moment of the call. */
  synchronized int openConnections() {
    return open;
  }

  /**
   * Runs {@code hook} once, on the real (unrecorded) connection the statement belongs to, right
   * before the first statement whose SQL starts with {@code sqlPrefix} is prepared. Nothing the
   * hook does is recorded; the {@code prepare} event follows it.
   */
  synchronized void beforePrepare(String sqlPrefix, SqlHook hook) {
    if (sqlPrefix == null || hook == null) {
      throw new IllegalArgumentException("sqlPrefix and hook are required");
    }
    hooks.add(new PendingHook(sqlPrefix, hook));
  }

  /**
   * Makes the next {@code close()} of a connection that committed (a {@code commit()} returned on
   * it) throw {@code SQLException("injected close failure")} after the real close ran and the
   * {@code close} event was recorded. One-shot.
   */
  synchronized void failNextCloseAfterCommit() {
    failNextCloseAfterCommit = true;
  }

  /**
   * Forgets the transcript, restarts the connection numbering at {@code #1} and resets {@link
   * #connectionsOpened()}. {@link #openConnections()} keeps counting the connections still open;
   * pending hooks and the close-failure arm are kept.
   */
  synchronized void clear() {
    events.clear();
    numbered = 0;
  }

  private synchronized void record(String event) {
    events.add(event);
  }

  private synchronized int opened() {
    numbered++;
    open++;
    events.add("open #" + numbered);
    return numbered;
  }

  private synchronized void closed(int n) {
    open--;
    events.add("#" + n + " close");
  }

  private synchronized boolean consumeCloseFailure() {
    boolean fail = failNextCloseAfterCommit;
    failNextCloseAfterCommit = false;
    return fail;
  }

  private synchronized SqlHook takeHook(String sql) {
    for (int i = 0; i < hooks.size(); i++) {
      if (sql.startsWith(hooks.get(i).sqlPrefix())) {
        return hooks.remove(i).hook();
      }
    }
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return wrap(delegate.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return wrap(delegate.getConnection(username, password));
  }

  private Connection wrap(Connection real) {
    int n = opened();
    return (Connection)
        Proxy.newProxyInstance(
            RecordingDataSource.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            new ConnectionHandler(real, n));
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /** The connection proxy: records the connection events and wraps the statements it creates. */
  private final class ConnectionHandler implements InvocationHandler {

    private final Connection real;
    private final int n;
    private boolean committed;
    private boolean closed;

    ConnectionHandler(Connection real, int n) {
      this.real = real;
      this.n = n;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      switch (name) {
        case "setAutoCommit" -> record("#" + n + " autoCommit=" + args[0]);
        case "prepareStatement", "prepareCall" -> {
          String sql = (String) args[0];
          SqlHook hook = takeHook(sql);
          if (hook != null) {
            hook.run(real);
          }
          record("#" + n + " prepare " + sql);
        }
        case "close" -> {
          return close();
        }
        case "equals" -> {
          return proxy == args[0];
        }
        case "hashCode" -> {
          return System.identityHashCode(proxy);
        }
        case "toString" -> {
          return "RecordingConnection#" + n + "[" + real + "]";
        }
        default -> {
          // delegated below
        }
      }
      Object result = RecordingDataSource.invoke(real, method, args);
      switch (name) {
        case "commit" -> {
          committed = true;
          record("#" + n + " commit");
        }
        case "rollback" -> record("#" + n + " rollback");
        case "createStatement", "prepareStatement", "prepareCall" -> {
          return wrapStatement((Statement) result, (Connection) proxy, method.getReturnType());
        }
        default -> {
          // nothing to record
        }
      }
      return result;
    }

    private Object close() throws SQLException {
      if (closed) {
        real.close();
        return null;
      }
      closed = true;
      try {
        real.close();
      } finally {
        closed(n);
      }
      if (committed && consumeCloseFailure()) {
        throw new SQLException("injected close failure");
      }
      return null;
    }

    private Statement wrapStatement(Statement statement, Connection connection, Class<?> type) {
      Class<?> iface =
          CallableStatement.class.isAssignableFrom(type)
              ? CallableStatement.class
              : PreparedStatement.class.isAssignableFrom(type)
                  ? PreparedStatement.class
                  : Statement.class;
      return (Statement)
          Proxy.newProxyInstance(
              RecordingDataSource.class.getClassLoader(),
              new Class<?>[] {iface},
              new StatementHandler(statement, connection, n));
    }
  }

  /** The statement proxy: records the statement settings and the SQL a statement is handed. */
  private final class StatementHandler implements InvocationHandler {

    private final Statement real;
    private final Connection connection;
    private final int n;

    StatementHandler(Statement real, Connection connection, int n) {
      this.real = real;
      this.connection = connection;
      this.n = n;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      boolean hasArgs = args != null && args.length > 0;
      switch (name) {
        case "setFetchSize" -> record("#" + n + " fetchSize=" + args[0]);
        case "setQueryTimeout" -> record("#" + n + " queryTimeout=" + args[0]);
        case "unwrap" -> {
          if (args[0] == PgStatement.class) {
            record("#" + n + " queryTimeout=driver");
          }
        }
        case "getConnection" -> {
          return connection;
        }
        case "equals" -> {
          return proxy == args[0];
        }
        case "hashCode" -> {
          return System.identityHashCode(proxy);
        }
        case "toString" -> {
          return "RecordingStatement#" + n + "[" + real + "]";
        }
        default -> {
          if (hasArgs && SQL_EXECUTES.contains(name) && args[0] instanceof String sql) {
            record("#" + n + " execute " + sql);
          }
        }
      }
      return RecordingDataSource.invoke(real, method, args);
    }
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return delegate.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    delegate.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return delegate.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this) || delegate.isWrapperFor(iface);
  }
}

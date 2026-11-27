package org.streamrune.sqlite;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A data source that records what a store does with its connections: a {@link Proxy} decorator over
 * a real data source, its connections and their statements. Characterization tests compare the
 * {@link #transcript()} with today's, count the connections a call borrows, run a hook on the same
 * connection right before a chosen statement, and fail a close after a commit.
 *
 * <p>The events, in order across all connections, {@code n} being the 1-based number of the
 * connection in the order it was opened:
 *
 * <ul>
 *   <li>{@code open #n}: {@link #getConnection()} returned connection {@code n};
 *   <li>{@code #n autoCommit=<true|false>}: {@link Connection#setAutoCommit};
 *   <li>{@code #n prepare <sql>}: {@link Connection#prepareStatement};
 *   <li>{@code #n execute <sql>}: an {@code execute}, {@code executeQuery}, {@code executeUpdate}
 *       or {@code executeLargeUpdate} that takes its SQL, on a {@link Connection#createStatement}
 *       statement;
 *   <li>{@code #n fetchSize=<k>} and {@code #n queryTimeout=<seconds>}: the statement setters;
 *   <li>{@code #n commit} and {@code #n rollback}: {@link Connection#commit()} and {@link
 *       Connection#rollback()};
 *   <li>{@code #n close}: the first {@link Connection#close()} of the connection.
 * </ul>
 *
 * Each event is recorded before the call is delegated, so a call that fails is still in the
 * transcript. Everything else is passed through unrecorded.
 *
 * <p><b>SQLite.</b> {@link Connection#isWrapperFor} and {@link Connection#unwrap} are answered by
 * the real connection, so {@link SqliteTransactions} still finds the driver's {@code
 * SQLiteConnection} (for the in-process write mutex and the physical close after a failed
 * rollback). What a caller does on the unwrapped connection is not recorded. The SQLite stores
 * commit with a {@code COMMIT} statement, not with {@link Connection#commit()}: a connection counts
 * as committed for {@link #failNextCloseAfterCommit()} when either succeeded on it.
 */
final class RecordingDataSource implements DataSource {

  /** A hook that runs on the very connection a store is about to prepare a statement on. */
  @FunctionalInterface
  interface SqlHook {
    void run(Connection sameConnection) throws SQLException;
  }

  private record PendingHook(String sqlPrefix, SqlHook hook, AtomicBoolean ran) {}

  private final DataSource delegate;
  private final List<String> transcript = new ArrayList<>();
  private final AtomicInteger connectionsOpened = new AtomicInteger();
  private final AtomicInteger openConnections = new AtomicInteger();
  private final List<PendingHook> hooks = new CopyOnWriteArrayList<>();
  private final AtomicBoolean failNextCloseAfterCommit = new AtomicBoolean();

  RecordingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** Every event, in order, across connections. */
  List<String> transcript() {
    synchronized (transcript) {
      return List.copyOf(transcript);
    }
  }

  /** The SQL of the {@code prepare} and {@code execute} events only, in order. */
  List<String> sql() {
    List<String> sql = new ArrayList<>();
    for (String event : transcript()) {
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
    return sql;
  }

  /** The number of connections handed out since construction or the last {@link #clear()}. */
  int connectionsOpened() {
    return connectionsOpened.get();
  }

  /** The connections handed out and not closed yet, at the moment of the call. */
  int openConnections() {
    return openConnections.get();
  }

  /**
   * Runs {@code hook} once, on the same connection (the real one, so its work is not recorded),
   * right before the first statement whose SQL starts with {@code sqlPrefix} is prepared.
   */
  void beforePrepare(String sqlPrefix, SqlHook hook) {
    hooks.add(new PendingHook(sqlPrefix, hook, new AtomicBoolean()));
  }

  /**
   * The next {@link Connection#close()} of a connection that committed (a {@link
   * Connection#commit()} or a {@code COMMIT} statement succeeded on it) closes the real connection
   * and then throws {@code SQLException("injected close failure")}.
   */
  void failNextCloseAfterCommit() {
    failNextCloseAfterCommit.set(true);
  }

  /**
   * Forgets the transcript, restarts the connection numbering and {@link #connectionsOpened()} at
   * zero, and drops pending hooks and an armed close failure. {@link #openConnections()} keeps
   * counting the connections still open.
   */
  void clear() {
    synchronized (transcript) {
      transcript.clear();
    }
    connectionsOpened.set(0);
    hooks.clear();
    failNextCloseAfterCommit.set(false);
  }

  private void record(String event) {
    synchronized (transcript) {
      transcript.add(event);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return recorded(delegate.getConnection());
  }

  @Override
  public Connection getConnection(String user, String password) throws SQLException {
    return recorded(delegate.getConnection(user, password));
  }

  private Connection recorded(Connection real) {
    int n = connectionsOpened.incrementAndGet();
    openConnections.incrementAndGet();
    record("open #" + n);
    ConnectionHandler handler = new ConnectionHandler(n, real);
    handler.proxy =
        (Connection)
            Proxy.newProxyInstance(
                RecordingDataSource.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                handler);
    return handler.proxy;
  }

  /** Records the connection-level events and wraps the statements the connection hands out. */
  private final class ConnectionHandler implements InvocationHandler {

    private final int n;
    private final Connection real;
    private final AtomicBoolean committed = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    // The recorded connection itself, which a statement's getConnection() returns.
    private Connection proxy;

    private ConnectionHandler(int n, Connection real) {
      this.n = n;
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      int arity = args == null ? 0 : args.length;
      switch (name) {
        case "equals":
          if (arity == 1) {
            return proxy == args[0];
          }
          break;
        case "hashCode":
          if (arity == 0) {
            return System.identityHashCode(proxy);
          }
          break;
        case "toString":
          if (arity == 0) {
            return "recorded #" + n + " " + real;
          }
          break;
        case "setAutoCommit":
          record("#" + n + " autoCommit=" + args[0]);
          return delegate(method, args);
        case "prepareStatement":
          String sql = (String) args[0];
          runHooks(sql);
          record("#" + n + " prepare " + sql);
          return statement(PreparedStatement.class, delegate(method, args));
        case "createStatement":
          return statement(Statement.class, delegate(method, args));
        case "commit":
          record("#" + n + " commit");
          Object result = delegate(method, args);
          committed.set(true);
          return result;
        case "rollback":
          if (arity == 0) {
            record("#" + n + " rollback");
          }
          return delegate(method, args);
        case "close":
          return close(method, args);
        default:
          break;
      }
      return delegate(method, args);
    }

    private void runHooks(String sql) throws SQLException {
      for (PendingHook pending : hooks) {
        if (sql.startsWith(pending.sqlPrefix()) && pending.ran().compareAndSet(false, true)) {
          pending.hook().run(real);
        }
      }
    }

    private Object close(Method method, Object[] args) throws Throwable {
      if (!closed.compareAndSet(false, true)) {
        return delegate(method, args);
      }
      record("#" + n + " close");
      try {
        delegate(method, args);
      } finally {
        openConnections.decrementAndGet();
      }
      if (committed.get() && failNextCloseAfterCommit.compareAndSet(true, false)) {
        throw new SQLException("injected close failure");
      }
      return null;
    }

    private Object statement(Class<? extends Statement> type, Object real) {
      return Proxy.newProxyInstance(
          RecordingDataSource.class.getClassLoader(),
          new Class<?>[] {type},
          new StatementHandler(this, (Statement) real));
    }

    private Object delegate(Method method, Object[] args) throws Throwable {
      return invokeOn(real, method, args);
    }
  }

  /** Records the statement-level events of one statement of one connection. */
  private final class StatementHandler implements InvocationHandler {

    private final ConnectionHandler connection;
    private final Statement real;

    private StatementHandler(ConnectionHandler connection, Statement real) {
      this.connection = connection;
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      int arity = args == null ? 0 : args.length;
      int n = connection.n;
      switch (name) {
        case "equals":
          if (arity == 1) {
            return proxy == args[0];
          }
          break;
        case "hashCode":
          if (arity == 0) {
            return System.identityHashCode(proxy);
          }
          break;
        case "toString":
          if (arity == 0) {
            return "recorded statement of #" + n + " " + real;
          }
          break;
        case "execute", "executeQuery", "executeUpdate", "executeLargeUpdate":
          if (arity >= 1 && args[0] instanceof String sql) {
            record("#" + n + " execute " + sql);
            Object result = invokeOn(real, method, args);
            if (sql.strip().toUpperCase(Locale.ROOT).equals("COMMIT")) {
              connection.committed.set(true);
            }
            return result;
          }
          break;
        case "setFetchSize":
          record("#" + n + " fetchSize=" + args[0]);
          break;
        case "setQueryTimeout":
          record("#" + n + " queryTimeout=" + args[0]);
          break;
        case "getConnection":
          if (arity == 0) {
            return connection.proxy;
          }
          break;
        default:
          break;
      }
      return invokeOn(real, method, args);
    }
  }

  private static Object invokeOn(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
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
  public <T> T unwrap(Class<T> type) throws SQLException {
    return delegate.unwrap(type);
  }

  @Override
  public boolean isWrapperFor(Class<?> type) throws SQLException {
    return delegate.isWrapperFor(type);
  }
}

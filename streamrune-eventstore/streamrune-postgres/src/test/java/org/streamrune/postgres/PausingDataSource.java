package org.streamrune.postgres;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Test-only {@link DataSource} wrapper that stops a transaction at a chosen statement, or makes a
 * commit that landed look failed to its caller.
 *
 * <ul>
 *   <li>{@link #pauseBefore(String)}: the first {@code prepareStatement} on any of the wrapper's
 *       connections whose SQL starts with the prefix parks its calling thread before the driver
 *       sees the statement, until {@link Pause#release()}. The transaction's backend is then
 *       <em>idle in transaction</em>, holding everything its earlier statements took.
 *   <li>{@link #pauseBeforeCommit()}: the next {@code commit()} on any of the wrapper's connections
 *       parks its calling thread before the driver sends it, until {@link Pause#release()}; the
 *       transaction's backend is then idle in transaction, holding what its statements took.
 *   <li>{@link #failCommitAfterDelegating(String)}: the first {@code commit()} of a transaction
 *       that prepared a statement starting with the prefix is delegated — the transaction commits —
 *       and then throws a {@link SQLException} with SQLState {@code 08006}, as a connection lost
 *       while the commit's reply was on the wire looks to the caller.
 * </ul>
 *
 * <p>Only the connection is wrapped: the statements are the driver's own, so {@code
 * isWrapperFor(PgStatement.class)} holds for them, and {@code unwrap}/{@code isWrapperFor} on the
 * connection answer for the driver's connection.
 */
final class PausingDataSource implements DataSource {

  /** A pause armed by {@link #pauseBefore(String)}. */
  static final class Pause {
    private final String sqlPrefix;
    private final CountDownLatch paused = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);

    private Pause(String sqlPrefix) {
      this.sqlPrefix = sqlPrefix;
    }

    /** Waits until a thread is parked at the statement. */
    void awaitPaused() throws InterruptedException {
      if (!paused.await(30, TimeUnit.SECONDS)) {
        throw new AssertionError("no statement starting with '" + sqlPrefix + "' was prepared");
      }
    }

    /** Lets the parked thread prepare its statement. */
    void release() {
      released.countDown();
    }

    private void park() {
      paused.countDown();
      try {
        if (!released.await(60, TimeUnit.SECONDS)) {
          throw new IllegalStateException("a paused statement was never released");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while paused", e);
      }
    }
  }

  private final DataSource delegate;
  private final AtomicReference<Pause> armedPause = new AtomicReference<>();
  private final AtomicReference<String> commitFailurePrefix = new AtomicReference<>();
  private final AtomicReference<Pause> armedCommitPause = new AtomicReference<>();

  PausingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** Arms a one-shot pause before the first statement whose SQL starts with {@code sqlPrefix}. */
  Pause pauseBefore(String sqlPrefix) {
    Pause pause = new Pause(sqlPrefix);
    armedPause.set(pause);
    return pause;
  }

  /** Arms a one-shot pause before the next commit of any of the wrapper's connections. */
  Pause pauseBeforeCommit() {
    Pause pause = new Pause("COMMIT");
    armedCommitPause.set(pause);
    return pause;
  }

  /**
   * Arms a one-shot failure of the first commit of a transaction that prepared a statement starting
   * with {@code sqlPrefix}: the commit is delegated, then a {@link SQLException} is thrown.
   */
  void failCommitAfterDelegating(String sqlPrefix) {
    commitFailurePrefix.set(sqlPrefix);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return wrap(delegate.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return wrap(delegate.getConnection(username, password));
  }

  private Connection wrap(Connection connection) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            new Handler(connection));
  }

  private final class Handler implements InvocationHandler {
    private final Connection connection;

    /** The SQL of every statement this connection prepared since its last commit or rollback. */
    private final StringBuilder transactionSql = new StringBuilder();

    Handler(Connection connection) {
      this.connection = connection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (name.equals("prepareStatement") && args != null && args[0] instanceof String sql) {
        Pause pause = armedPause.get();
        if (pause != null
            && sql.startsWith(pause.sqlPrefix)
            && armedPause.compareAndSet(pause, null)) {
          pause.park();
        }
        transactionSql.append(sql).append('\n');
      }
      if (name.equals("unwrap") || name.equals("isWrapperFor")) {
        return call(method, args);
      }
      if (name.equals("rollback") && (args == null || args.length == 0)) {
        transactionSql.setLength(0);
      }
      if (name.equals("commit")) {
        Pause commitPause = armedCommitPause.getAndSet(null);
        if (commitPause != null) {
          commitPause.park();
        }
        Object result = call(method, args);
        String prefix = commitFailurePrefix.get();
        boolean fail =
            prefix != null
                && containsStatementStartingWith(prefix)
                && commitFailurePrefix.compareAndSet(prefix, null);
        transactionSql.setLength(0);
        if (fail) {
          throw new SQLException(
              "the connection was lost while the commit's reply was on the wire", "08006");
        }
        return result;
      }
      return call(method, args);
    }

    private boolean containsStatementStartingWith(String prefix) {
      for (String line : transactionSql.toString().split("\n")) {
        if (line.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }

    private Object call(Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(connection, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
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
  public Logger getParentLogger() {
    return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
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

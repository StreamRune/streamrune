package org.streamrune.crypto.sqlite;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A data source over a real SQLite one that records what runs on each connection it hands out, and
 * can inject one failure. Every statement still runs on the real driver, so the file's tables and
 * SQLite's transactions are the real ones; the recording only observes.
 *
 * <p>Each {@link #getConnection()} hands out a {@link Proxy} around a new connection of the
 * delegate, numbered from 1 in the order handed out (since the last {@link #resetRecording()}). The
 * transcript ({@link #calls()}) records, in order and per connection: {@code getConnection}, {@code
 * prepare <sql>} for every {@code prepareStatement}, {@code execute <sql>} for every SQL string run
 * through a {@code createStatement()} statement ({@code execute}, {@code executeUpdate}, {@code
 * executeQuery}: how {@code SqliteTransactions} issues {@code BEGIN IMMEDIATE}, {@code COMMIT} and
 * {@code ROLLBACK}), {@code setAutoCommit(<flag>)}, {@code commit}, {@code rollback} and {@code
 * close}. Each entry is written before the call runs.
 *
 * <p>It also keeps a reference to every {@code byte[]} bound with {@code setBytes} ({@link
 * #keyBytesBound()}) and every {@code byte[]} a result set hands out from {@code getBytes} ({@link
 * #keyBytesHandedOut()}), so a test can see whether the code zeroed its buffer afterwards.
 */
final class RecordingDataSource implements DataSource {

  /** One transcript entry: what ran, on which connection (numbered from 1). */
  record Call(int connection, String call) {
    @Override
    public String toString() {
      return "#" + connection + " " + call;
    }
  }

  private record Fault(String sqlPrefix, Throwable failure, boolean afterTheStatement) {}

  /** The methods that run a statement. */
  private static final Set<String> RUNS =
      Set.of("execute", "executeUpdate", "executeQuery", "executeLargeUpdate");

  private final DataSource delegate;
  private final List<Call> calls = Collections.synchronizedList(new ArrayList<>());
  private final List<String> prepared = Collections.synchronizedList(new ArrayList<>());
  private final List<byte[]> keyBytesBound = Collections.synchronizedList(new ArrayList<>());
  private final List<byte[]> keyBytesHandedOut = Collections.synchronizedList(new ArrayList<>());
  private int nextConnection = 1;
  private Fault fault;
  private SQLException connectionFailure;

  RecordingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** The transcript since the last {@link #resetRecording()}, in order. */
  List<Call> calls() {
    synchronized (calls) {
      return List.copyOf(calls);
    }
  }

  /** The transcript as {@code "#<connection> <call>"} strings. */
  List<String> transcript() {
    return calls().stream().map(Call::toString).toList();
  }

  /** The SQL of every {@code prepareStatement} since the last reset, in order. */
  List<String> sqlSeen() {
    synchronized (prepared) {
      return List.copyOf(prepared);
    }
  }

  /** Every array bound with {@code setBytes} since the last reset (the references, not copies). */
  List<byte[]> keyBytesBound() {
    synchronized (keyBytesBound) {
      return List.copyOf(keyBytesBound);
    }
  }

  /** Every array a result set's {@code getBytes} handed out since the last reset. */
  List<byte[]> keyBytesHandedOut() {
    synchronized (keyBytesHandedOut) {
      return List.copyOf(keyBytesHandedOut);
    }
  }

  /** Clears every recording and numbers the next connection 1; the tables are untouched. */
  synchronized void resetRecording() {
    calls.clear();
    prepared.clear();
    keyBytesBound.clear();
    keyBytesHandedOut.clear();
    nextConnection = 1;
  }

  /**
   * The next prepared statement whose SQL starts with {@code sqlPrefix} throws {@code failure}
   * instead of running. Fires once. {@code failure} is a {@link SQLException} or unchecked.
   */
  synchronized void failAt(String sqlPrefix, Throwable failure) {
    fault = new Fault(sqlPrefix, failure, false);
  }

  /**
   * The next prepared statement whose SQL starts with {@code sqlPrefix} runs, then throws {@code
   * failure}. Fires once. {@code failure} is a {@link SQLException} or unchecked.
   */
  synchronized void failAfter(String sqlPrefix, Throwable failure) {
    fault = new Fault(sqlPrefix, failure, true);
  }

  /** From now on every {@link #getConnection()} throws {@code failure}, as an outage does. */
  synchronized void failConnections(SQLException failure) {
    connectionFailure = failure;
  }

  @Override
  public Connection getConnection() throws SQLException {
    int number;
    synchronized (this) {
      if (connectionFailure != null) {
        throw connectionFailure;
      }
      number = nextConnection++;
    }
    calls.add(new Call(number, "getConnection"));
    Connection real = delegate.getConnection();
    return proxy(Connection.class, new ConnectionHandler(number, real));
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

  /** The armed fault for {@code sql}, disarmed as it is taken, or {@code null}. */
  private synchronized Fault takeFault(String sql) {
    if (fault != null && sql.startsWith(fault.sqlPrefix())) {
      Fault taken = fault;
      fault = null;
      return taken;
    }
    return null;
  }

  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return type.cast(
        Proxy.newProxyInstance(
            RecordingDataSource.class.getClassLoader(), new Class<?>[] {type}, handler));
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private final class ConnectionHandler implements InvocationHandler {
    private final int number;
    private final Connection real;

    ConnectionHandler(int number, Connection real) {
      this.number = number;
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
        case "prepareStatement" -> {
          String sql = (String) args[0];
          calls.add(new Call(number, "prepare " + sql));
          prepared.add(sql);
          PreparedStatement statement =
              (PreparedStatement) RecordingDataSource.invoke(real, method, args);
          return proxy(PreparedStatement.class, new PreparedStatementHandler(sql, statement));
        }
        case "createStatement" -> {
          Statement statement = (Statement) RecordingDataSource.invoke(real, method, args);
          return proxy(Statement.class, new StatementHandler(number, statement));
        }
        case "setAutoCommit" -> calls.add(new Call(number, "setAutoCommit(" + args[0] + ")"));
        case "commit", "rollback", "close" -> {
          if (args == null) {
            calls.add(new Call(number, method.getName()));
          }
        }
        case "toString" -> {
          return "RecordingConnection#" + number + "[" + real + "]";
        }
        case "equals" -> {
          return proxy == args[0];
        }
        case "hashCode" -> {
          return System.identityHashCode(proxy);
        }
        default -> {}
      }
      return RecordingDataSource.invoke(real, method, args);
    }
  }

  private final class StatementHandler implements InvocationHandler {
    private final int number;
    private final Statement real;

    StatementHandler(int number, Statement real) {
      this.number = number;
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (RUNS.contains(method.getName()) && args != null && args[0] instanceof String) {
        calls.add(new Call(number, "execute " + args[0]));
      }
      return RecordingDataSource.invoke(real, method, args);
    }
  }

  private final class PreparedStatementHandler implements InvocationHandler {
    private final String sql;
    private final PreparedStatement real;

    PreparedStatementHandler(String sql, PreparedStatement real) {
      this.sql = sql;
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (name.equals("setBytes")) {
        keyBytesBound.add((byte[]) args[1]);
      }
      if (RUNS.contains(name) && args == null) {
        Fault armed = takeFault(sql);
        if (armed != null && !armed.afterTheStatement()) {
          throw armed.failure();
        }
        Object result = RecordingDataSource.invoke(real, method, args);
        if (armed != null) {
          throw armed.failure();
        }
        if (result instanceof ResultSet rs) {
          return proxy(ResultSet.class, new ResultSetHandler(rs));
        }
        return result;
      }
      return RecordingDataSource.invoke(real, method, args);
    }
  }

  private final class ResultSetHandler implements InvocationHandler {
    private final ResultSet real;

    ResultSetHandler(ResultSet real) {
      this.real = real;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object result = RecordingDataSource.invoke(real, method, args);
      if (method.getName().equals("getBytes") && result instanceof byte[] bytes) {
        keyBytesHandedOut.add(bytes);
      }
      return result;
    }
  }
}

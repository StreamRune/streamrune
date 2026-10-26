package org.streamrune.integration.tck;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A {@link DataSource} that hands out the connections of another one and records how it is used:
 * how many connections it granted, how many of them were still in use when it closed, whether it is
 * closed, and which threads asked it for a connection after it closed. Once closed it grants none,
 * the way a closed connection pool refuses a borrow.
 *
 * <p>{@link IntegrationWiringContract} gives it to an application as the application's {@code
 * DataSource}, the one the integration builds its PostgreSQL event store and stores on, and reads
 * from it whether the integration's runners still use the database after the container closed it.
 */
public final class RecordingDataSource implements DataSource, AutoCloseable {

  private final DataSource delegate;
  private final AtomicLong granted = new AtomicLong();
  private final AtomicInteger inUse = new AtomicInteger();
  private volatile int inUseAtClose = -1;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final List<String> requestsAfterClose = new CopyOnWriteArrayList<>();

  /**
   * Records the use of {@code delegate}.
   *
   * @param delegate the data source whose connections this one hands out
   */
  public RecordingDataSource(DataSource delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("delegate is required");
    }
    this.delegate = delegate;
  }

  @Override
  public Connection getConnection() throws SQLException {
    refuseWhenClosed();
    return inUse(delegate.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    refuseWhenClosed();
    return inUse(delegate.getConnection(username, password));
  }

  /**
   * Counts a granted connection as in use until its borrower closes it; every other call goes to
   * the connection itself, unwrapping to the driver's types included.
   */
  private Connection inUse(Connection connection) {
    granted.incrementAndGet();
    inUse.incrementAndGet();
    AtomicBoolean returned = new AtomicBoolean();
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "equals" -> {
                  return proxy == args[0];
                }
                case "hashCode" -> {
                  return System.identityHashCode(proxy);
                }
                case "close" -> {
                  if (returned.compareAndSet(false, true)) {
                    inUse.decrementAndGet();
                  }
                }
                default -> {}
              }
              try {
                return method.invoke(connection, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private void refuseWhenClosed() throws SQLException {
    if (closed.get()) {
      requestsAfterClose.add(Thread.currentThread().getName());
      throw new SQLException("the DataSource is closed");
    }
  }

  /**
   * Closes this data source: it grants no connection from now on. The delegate is closed too when
   * it is {@link AutoCloseable}.
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      inUseAtClose = inUse.get();
      if (delegate instanceof AutoCloseable closeable) {
        closeable.close();
      }
    }
  }

  /**
   * @return whether {@link #close()} was called
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * @return how many connections this data source handed out
   */
  public long connectionsGranted() {
    return granted.get();
  }

  /**
   * @return how many connections it handed out were not yet closed by their borrower when this data
   *     source closed; {@code -1} while it is open
   */
  public int connectionsInUseAtClose() {
    return inUseAtClose;
  }

  /**
   * @return the name of the thread of every request for a connection after the close, in the order
   *     they came
   */
  public List<String> requestsAfterClose() {
    return List.copyOf(requestsAfterClose);
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
    if (type.isInstance(this)) {
      return type.cast(this);
    }
    return delegate.unwrap(type);
  }

  @Override
  public boolean isWrapperFor(Class<?> type) throws SQLException {
    return type.isInstance(this) || delegate.isWrapperFor(type);
  }
}

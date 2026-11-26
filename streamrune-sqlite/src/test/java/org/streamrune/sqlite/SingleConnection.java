package org.streamrune.sqlite;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A data source that hands out one physical connection, the same one on every borrow: closing a
 * borrowed connection does nothing, so a test can leave the connection in a state (an open {@code
 * BEGIN}) and have a store borrow it in that state. {@link #close()} closes the physical
 * connection; a test closes it when it is done.
 */
final class SingleConnection implements DataSource, AutoCloseable {
  private final DataSource delegate;
  private Connection shared;

  SingleConnection(DataSource delegate) {
    this.delegate = delegate;
  }

  @Override
  public synchronized Connection getConnection() throws SQLException {
    if (shared == null) {
      shared = delegate.getConnection();
    }
    Connection physical = shared;
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (method.getName().equals("close")) {
                return null;
              }
              try {
                return method.invoke(physical, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  /** Closes the physical connection, if one was opened; a later borrow opens a new one. */
  @Override
  public synchronized void close() throws SQLException {
    if (shared != null) {
      shared.close();
      shared = null;
    }
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
  public Logger getParentLogger() {
    return Logger.getGlobal();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("not a wrapper");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }
}

package org.streamrune.sqlite;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A data source that passes connections through from a delegate until a test switches it: to fail
 * every connection request with an unchecked exception, as a closed pool does, or to hand out
 * connections in manual-commit mode, as a misconfigured pool or an application transaction does.
 * {@link SqliteDatabase#open} checks the connections it is opened on, so a test opens the handle
 * first and switches afterwards.
 */
final class SwitchableDataSource implements DataSource {

  private final DataSource delegate;
  private volatile Supplier<RuntimeException> failure;
  private volatile boolean manualCommit;

  SwitchableDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** From now on every connection request throws the exception {@code failure} supplies. */
  void failWith(Supplier<RuntimeException> failure) {
    this.failure = failure;
  }

  /** From now on every connection is handed out in manual-commit mode. */
  void manualCommit() {
    this.manualCommit = true;
  }

  @Override
  public Connection getConnection() throws SQLException {
    Supplier<RuntimeException> fail = failure;
    if (fail != null) {
      throw fail.get();
    }
    Connection conn = delegate.getConnection();
    if (manualCommit) {
      conn.setAutoCommit(false);
    }
    return conn;
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
}

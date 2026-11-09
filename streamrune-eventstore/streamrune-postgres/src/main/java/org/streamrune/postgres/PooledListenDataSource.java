package org.streamrune.postgres;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * The {@link ListenDataSource} {@link PostgresNotificationSubscription#createListenDataSource}
 * returns: every call goes to the HikariCP pool behind it, and {@link #unwrap} hands that pool out.
 */
final class PooledListenDataSource implements ListenDataSource {

  private final HikariDataSource pool;

  PooledListenDataSource(HikariDataSource pool) {
    this.pool = Objects.requireNonNull(pool, "pool is required");
  }

  @Override
  public Connection getConnection() throws SQLException {
    return pool.getConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return pool.getConnection(username, password);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return pool.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    pool.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    pool.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return pool.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return pool.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    if (iface.isInstance(pool)) {
      return iface.cast(pool);
    }
    return pool.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this) || iface.isInstance(pool) || pool.isWrapperFor(iface);
  }

  @Override
  public void close() {
    pool.close();
  }

  @Override
  public String toString() {
    return "ListenDataSource[" + pool.getPoolName() + "]";
  }
}

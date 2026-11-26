package org.streamrune.spring;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

/**
 * SQLite data sources with the connection settings the SQLite backend requires. {@code
 * SQLiteDataSource} takes them from its {@link SQLiteConfig}, not from URL parameters; the source
 * of truth is {@code SqliteSetup.verify} in streamrune-sqlite, so a setting added there fails the
 * tests that use this helper loudly.
 */
final class SqliteDataSources {

  private SqliteDataSources() {}

  /** A data source on {@code file}, with WAL, synchronous=FULL, foreign keys and IMMEDIATE. */
  static SQLiteDataSource file(Path file) {
    return file(file, 5000);
  }

  /** {@link #file(Path)} with the given busy_timeout. */
  static SQLiteDataSource file(Path file, int busyTimeoutMillis) {
    var config = new SQLiteConfig();
    config.setJournalMode(SQLiteConfig.JournalMode.WAL);
    config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
    config.enforceForeignKeys(true);
    config.setBusyTimeout(busyTimeoutMillis);
    config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
    var dataSource = new SQLiteDataSource(config);
    dataSource.setUrl("jdbc:sqlite:" + file);
    return dataSource;
  }

  /**
   * A data source on {@code file} with the driver's defaults: rollback journal, not WAL, so the
   * backend's setup check refuses it.
   */
  static SQLiteDataSource wrongSetup(Path file) {
    var dataSource = new SQLiteDataSource();
    dataSource.setUrl("jdbc:sqlite:" + file);
    return dataSource;
  }

  /**
   * {@link #file} counting the connections borrowed and not yet closed in {@code open}: a shutdown
   * test reads it when the backend's stores are closed.
   */
  static DataSource counting(Path file, AtomicInteger open) {
    DataSource real = file(file);
    ClassLoader loader = SqliteDataSources.class.getClassLoader();
    return (DataSource)
        Proxy.newProxyInstance(
            loader,
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result = invoke(method, real, args);
              if (!"getConnection".equals(method.getName())) {
                return result;
              }
              Connection connection = (Connection) result;
              open.incrementAndGet();
              AtomicInteger closed = new AtomicInteger();
              return Proxy.newProxyInstance(
                  loader,
                  new Class<?>[] {Connection.class},
                  (p, m, a) -> {
                    if ("close".equals(m.getName()) && closed.getAndIncrement() == 0) {
                      open.decrementAndGet();
                    }
                    return invoke(m, connection, a);
                  });
            });
  }

  private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}

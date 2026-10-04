package org.streamrune.postgres;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Test-only {@link DataSource} wrapper that injects failures into {@link #getConnection()} so
 * resilience paths (reconnect-with-backoff, max-retry giveup, heartbeat survival) can be driven
 * deterministically instead of relying on flaky real connection drops.
 *
 * <p>Configurable fault modes, all counting only {@code getConnection()} calls:
 *
 * <ul>
 *   <li>{@code failFirst(n)} — the first {@code n} calls throw, then every later call delegates to
 *       the wrapped source. Drives "reconnect after N failures, then recover".
 *   <li>{@code failAfter(n)} — the first {@code n} calls delegate to the wrapped source, then every
 *       later call throws forever. Drives "succeed once, then fail until giveup".
 *   <li>{@code failNth(nth)} — exactly the {@code nth} call throws {@link SQLException}; every
 *       other call delegates. Drives "healthy, one transient blip, healthy again".
 *   <li>{@code failNthUnchecked(nth, fault)} — exactly the {@code nth} call throws the supplied
 *       <em>unchecked</em> throwable ({@link RuntimeException} or {@link Error}); every other call
 *       delegates. Drives failure classes a plain {@link SQLException} cannot — e.g. a
 *       wrapped/routing DataSource throwing {@link IllegalStateException}, or an {@link Error} on a
 *       long-lived loop's thread.
 * </ul>
 *
 * Modes are mutually exclusive; the last configured one wins. Only {@code getConnection()} (no-arg)
 * is intercepted — that is the single method the listener uses.
 */
final class FaultInjectingDataSource implements DataSource {

  private final DataSource delegate;
  private final AtomicInteger connectionAttempts = new AtomicInteger(0);
  private volatile int failFirst = 0;
  private volatile int failAfter = Integer.MAX_VALUE;
  private volatile int failNth = 0; // 0 = disabled; 1-based attempt index
  private volatile Supplier<? extends Throwable> uncheckedFault;

  FaultInjectingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  /** First {@code n} {@link #getConnection()} calls throw; subsequent calls succeed. */
  FaultInjectingDataSource failFirst(int n) {
    this.failFirst = n;
    this.failAfter = Integer.MAX_VALUE;
    this.failNth = 0;
    this.uncheckedFault = null;
    return this;
  }

  /** First {@code n} {@link #getConnection()} calls succeed; subsequent calls throw forever. */
  FaultInjectingDataSource failAfter(int n) {
    this.failAfter = n;
    this.failFirst = 0;
    this.failNth = 0;
    this.uncheckedFault = null;
    return this;
  }

  /**
   * Exactly the {@code nth} (1-based) {@link #getConnection()} call throws {@link SQLException};
   * every other call delegates. Drives "healthy, one transient blip, healthy again" — e.g. a
   * momentary pool-exhaustion timeout on an otherwise live connection path.
   */
  FaultInjectingDataSource failNth(int nth) {
    this.failNth = nth;
    this.uncheckedFault = null;
    this.failFirst = 0;
    this.failAfter = Integer.MAX_VALUE;
    return this;
  }

  /**
   * Exactly the {@code nth} (1-based) {@link #getConnection()} call throws the supplied unchecked
   * throwable ({@link RuntimeException} or {@link Error}); every other call delegates. Models a
   * one-off failure class that {@link SQLException} cannot express — a wrapped/routing DataSource
   * throwing {@link IllegalStateException}, or an {@link Error} escaping into a long-lived loop.
   */
  FaultInjectingDataSource failNthUnchecked(int nth, Supplier<? extends Throwable> fault) {
    this.failNth = nth;
    this.uncheckedFault = fault;
    this.failFirst = 0;
    this.failAfter = Integer.MAX_VALUE;
    return this;
  }

  /** Total number of {@link #getConnection()} calls observed so far. */
  int connectionAttempts() {
    return connectionAttempts.get();
  }

  @Override
  public Connection getConnection() throws SQLException {
    int attempt = connectionAttempts.incrementAndGet(); // 1-based
    if (attempt == failNth) {
      Supplier<? extends Throwable> fault = uncheckedFault;
      if (fault == null) {
        throw new SQLException(
            "injected getConnection failure (attempt " + attempt + " == failNth)", "08006");
      }
      Throwable t = fault.get();
      if (t instanceof RuntimeException re) {
        throw re;
      }
      if (t instanceof Error err) {
        throw err;
      }
      throw new IllegalStateException("unchecked fault must be a RuntimeException or Error", t);
    }
    if (attempt <= failFirst) {
      throw new SQLException(
          "injected getConnection failure (attempt " + attempt + " of failFirst=" + failFirst + ")",
          "08006"); // connection_failure
    }
    if (attempt > failAfter) {
      throw new SQLException(
          "injected getConnection failure (attempt "
              + attempt
              + " past failAfter="
              + failAfter
              + ")",
          "08006");
    }
    return delegate.getConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return delegate.getConnection(username, password);
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
    return Logger.getLogger("FaultInjectingDataSource");
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

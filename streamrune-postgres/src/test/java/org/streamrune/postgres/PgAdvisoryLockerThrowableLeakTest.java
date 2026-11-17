package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/**
 * {@link PgAdvisoryLocker#acquireLock} must release the already-checked-out connection on ANY
 * throwable on the acquire path — not just {@link java.sql.SQLException}. The {@code setAutoCommit}
 * try previously caught only SQLException, so a RuntimeException from a proxying/instrumented pool
 * leaked the pool slot; an Error from the lock-attempt block leaked it too; and the aggregate-id
 * hash was computed after checkout, outside every guard, so a null argument NPE'd with the
 * connection already borrowed.
 *
 * <p>Pure unit test — no PostgreSQL needed. A fake single-connection {@link DataSource} hands out a
 * proxied {@link Connection} that fails from {@code setAutoCommit} and records whether {@code
 * close()} ran.
 */
class PgAdvisoryLockerThrowableLeakTest {

  private enum FailMode {
    RUNTIME_ON_SET_AUTOCOMMIT,
    ERROR_ON_SET_AUTOCOMMIT
  }

  @Test
  void runtimeExceptionFromSetAutoCommit_releasesTheConnection() {
    var closed = new AtomicBoolean(false);
    var locker =
        new PgAdvisoryLocker(
            singleConnectionDataSource(closed, FailMode.RUNTIME_ON_SET_AUTOCOMMIT));

    assertThrows(
        RuntimeException.class,
        () -> locker.acquireLock(TestStreams.stream("victim"), Duration.ofSeconds(5)),
        "a RuntimeException from setAutoCommit must propagate");

    assertTrue(
        closed.get(),
        "a RuntimeException on the acquire path must close the checked-out connection (no leak)");
  }

  @Test
  void errorFromSetAutoCommit_releasesTheConnection() {
    var closed = new AtomicBoolean(false);
    var locker =
        new PgAdvisoryLocker(singleConnectionDataSource(closed, FailMode.ERROR_ON_SET_AUTOCOMMIT));

    assertThrows(
        Error.class,
        () -> locker.acquireLock(TestStreams.stream("victim"), Duration.ofSeconds(5)),
        "an Error on the acquire path must propagate");

    assertTrue(
        closed.get(),
        "an Error on the acquire path must close the checked-out connection (no leak)");
  }

  private static DataSource singleConnectionDataSource(AtomicBoolean closed, FailMode mode) {
    return new SingleConnectionDataSource(() -> fakeConnection(closed, mode));
  }

  private static Connection fakeConnection(AtomicBoolean closed, FailMode mode) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "setAutoCommit" ->
                      throw mode == FailMode.RUNTIME_ON_SET_AUTOCOMMIT
                          ? new IllegalStateException("connection is closed")
                          : new AssertionError("driver linkage error");
                  case "close" -> {
                    closed.set(true);
                    yield null;
                  }
                  case "rollback", "commit" -> null; // no-ops; closeQuietly calls rollback + close
                  case "isClosed" -> closed.get();
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  case "toString" -> "FakeConnection";
                  default -> defaultValue(method.getReturnType());
                });
  }

  private static Object defaultValue(Class<?> returnType) {
    if (!returnType.isPrimitive()) {
      return null;
    }
    if (returnType == boolean.class) {
      return false;
    }
    if (returnType == void.class) {
      return null;
    }
    return 0;
  }

  /** Minimal single-connection {@link DataSource} that hands out the supplied connection. */
  private record SingleConnectionDataSource(java.util.function.Supplier<Connection> supplier)
      implements DataSource {
    @Override
    public Connection getConnection() {
      return supplier.get();
    }

    @Override
    public Connection getConnection(String username, String password) {
      return supplier.get();
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
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException("not needed for tests");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}

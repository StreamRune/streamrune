package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.LockException;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Regression: a DB failover / connection blip <em>during</em> a lock acquisition must never leak
 * the pooled connection. {@link PgAdvisoryLocker}'s release paths ({@code closeQuietly} after a
 * failed acquire, and the getConnection/setAutoCommit try) must ALWAYS {@code close()} the
 * connection — even when {@code conn.rollback()} itself throws on a dead connection — or the pool
 * slot stays permanently checked out and, after {@code maxPoolSize} such events, the whole command
 * path deadlocks long after the database recovered.
 *
 * <p>The scenario is reproduced deterministically against a real PostgreSQL: a small fixed-capacity
 * {@link SlotTrackingPool} hands out real connections and only frees a slot when the borrower calls
 * {@code close()}. When armed, it terminates the backend of the connection it just handed out (via
 * {@code pg_terminate_backend}, mimicking a failover), so the locker's next server round-trip fails
 * with an {@link SQLException} and {@code rollback()} on the now-dead connection also throws — the
 * exact path where the buggy code skipped {@code close()}. Unlike HikariCP (which self-evicts a
 * connection on a fatal SQLException regardless of {@code close()}), this pool frees a slot ONLY on
 * an explicit {@code close()}, so a skipped {@code close()} is directly observable as a leaked
 * slot.
 */
@Testcontainers
class PgAdvisoryLockerConnectionLeakTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_leak_test");

  static PGSimpleDataSource realDs;

  @BeforeAll
  static void init() {
    realDs = new PGSimpleDataSource();
    realDs.setUrl(PG.getJdbcUrl());
    realDs.setUser(PG.getUsername());
    realDs.setPassword(PG.getPassword());
  }

  @Test
  @Timeout(120)
  void brokenConnectionDuringAcquireReleasesThePoolSlot() throws Exception {
    try (SlotTrackingPool pool = new SlotTrackingPool(realDs, 1)) {
      var locker = new PgAdvisoryLocker(pool);

      // Arm the failover: the next connection the locker borrows has its backend terminated the
      // instant it is checked out, so the locker's advisory SQL — and then rollback() during the
      // release — both fail on the dead connection.
      pool.armKill();
      assertThrows(
          LockException.class,
          () -> locker.acquireLock(TestStreams.stream("victim"), Duration.ofSeconds(5)),
          "acquire on a backend killed mid-flight must fail");

      // The single pool slot must have been released (close() ran despite rollback throwing).
      // Before the fix, closeQuietly's rollback() throws and close() is skipped, leaking the slot.
      assertEquals(
          1,
          pool.availableSlots(),
          "the broken connection must be closed and its pool slot released");

      // And the pool must not be exhausted: a subsequent healthy acquire still succeeds.
      AutoCloseable lock = locker.acquireLock(TestStreams.stream("healthy"), Duration.ofSeconds(5));
      assertNotNull(lock);
      lock.close();
      assertEquals(1, pool.availableSlots(), "the healthy lock's slot must be released on close()");
    }
  }

  /**
   * A minimal fixed-capacity connection pool over a real {@link DataSource}. A slot is freed ONLY
   * when the borrowed connection's {@code close()} is invoked (tracked via a {@link Semaphore}); it
   * never self-heals on a connection error the way HikariCP does. When armed, it terminates the
   * backend of the connection it hands out so subsequent use — and the release-path rollback —
   * fail.
   */
  private static final class SlotTrackingPool implements DataSource, AutoCloseable {
    private final DataSource delegate;
    private final DataSource adminDs;
    private final Semaphore slots;
    private final AtomicBoolean killNext = new AtomicBoolean(false);

    SlotTrackingPool(DataSource delegate, int capacity) {
      this.delegate = delegate;
      this.adminDs = delegate; // a fresh physical connection is used only for the terminate call
      this.slots = new Semaphore(capacity);
    }

    void armKill() {
      killNext.set(true);
    }

    int availableSlots() {
      return slots.availablePermits();
    }

    @Override
    public Connection getConnection() throws SQLException {
      try {
        if (!slots.tryAcquire(3, TimeUnit.SECONDS)) {
          throw new SQLException("pool exhausted: a connection slot was leaked");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted while acquiring a pool slot", e);
      }
      Connection real;
      try {
        real = delegate.getConnection();
      } catch (SQLException e) {
        slots.release();
        throw e;
      }
      if (killNext.compareAndSet(true, false)) {
        terminateBackend(real);
      }
      return trackingProxy(real);
    }

    private void terminateBackend(Connection real) throws SQLException {
      int pid;
      try (Statement st = real.createStatement();
          var rs = st.executeQuery("SELECT pg_backend_pid()")) {
        rs.next();
        pid = rs.getInt(1);
      }
      try (Connection a = adminDs.getConnection();
          Statement st = a.createStatement()) {
        st.execute("SELECT pg_terminate_backend(" + pid + ")");
      }
      try {
        Thread.sleep(150); // let PostgreSQL tear the backend down before the next use
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
      }
    }

    private Connection trackingProxy(Connection real) {
      var released = new AtomicBoolean(false);
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if ("close".equals(method.getName())) {
                  try {
                    real.close();
                  } finally {
                    if (released.compareAndSet(false, true)) {
                      slots.release();
                    }
                  }
                  return null;
                }
                try {
                  return method.invoke(real, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                  throw e.getCause(); // surface the real SQLException, not an UndeclaredThrowable
                }
              });
    }

    @Override
    public void close() {
      // nothing pooled long-term; connections are physical PGSimpleDataSource connections
    }

    @Override
    public Connection getConnection(String username, String password) {
      throw new UnsupportedOperationException("not needed for tests");
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

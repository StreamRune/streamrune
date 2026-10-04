package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.streamrune.core.StreamRuneMetrics;

/**
 * Verifies FIX 5: {@link PgNotificationListener} no longer permanently gives up after a small fixed
 * number of reconnects. It survives many more consecutive reconnect failures than the old cap of 5,
 * reconnects once the connection recovers, and emits a reconnect metric on each failure. A finite
 * {@code maxReconnectAttempts} still stops the listener (making the dead push path observable).
 */
class PgNotificationListenerResilienceTest {

  /** Counts reconnect-metric calls. */
  private static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger reconnects = new AtomicInteger();

    @Override
    public void recordListenerReconnect(String channel) {
      reconnects.incrementAndGet();
    }
  }

  /**
   * A DataSource whose {@link #getConnection()} throws {@code failuresBeforeSuccess} times, then
   * returns a working mock connection that reports one notification per {@code getNotifications}
   * call. The failure count drives the reconnect loop.
   */
  private static final class FlakyDataSource implements DataSource {
    private final int failuresBeforeSuccess;
    final AtomicInteger connectionAttempts = new AtomicInteger();
    final AtomicInteger notificationsFired = new AtomicInteger();

    FlakyDataSource(int failuresBeforeSuccess) {
      this.failuresBeforeSuccess = failuresBeforeSuccess;
    }

    @Override
    public Connection getConnection() throws SQLException {
      int attempt = connectionAttempts.incrementAndGet();
      if (attempt <= failuresBeforeSuccess) {
        throw new SQLException("simulated connection failure #" + attempt);
      }
      return workingConnection();
    }

    private Connection workingConnection() throws SQLException {
      Connection conn = mock(Connection.class);
      Statement stmt = mock(Statement.class);
      when(conn.createStatement()).thenReturn(stmt);
      PGConnection pgConn = mock(PGConnection.class);
      when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
      // Return a single notification on the first poll so the callback fires, then null (block).
      PGNotification note = mock(PGNotification.class);
      when(pgConn.getNotifications(anyInt()))
          .thenAnswer(
              inv -> {
                notificationsFired.incrementAndGet();
                return new PGNotification[] {note};
              });
      return conn;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  @Test
  void survivesManyReconnectFailuresBeyondOldCapAndRecovers() throws Exception {
    // 8 consecutive failures — well beyond the old hard cap of 5 — then success.
    var ds = new FlakyDataSource(8);
    var metrics = new CountingMetrics();
    var listener =
        PgNotificationListener.builder()
            .dataSource(ds)
            .onNotification(() -> {})
            .metrics(metrics)
            .backoff(5, 20) // fast backoff so 8 failures finish within the test window
            // Default is UNBOUNDED_RECONNECTS; assert the default survives past 5.
            .build();

    listener.start();
    try {
      // Wait until the listener finally connects (9th attempt) and fires a notification.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (ds.notificationsFired.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(
          ds.notificationsFired.get() > 0,
          "listener should have reconnected and delivered after 8 failures (old cap was 5)");
      assertTrue(listener.isRunning(), "listener should still be running after recovery");
      // Each of the 8 failures emitted a reconnect metric.
      assertTrue(
          metrics.reconnects.get() >= 8,
          "expected >=8 reconnect metrics, got " + metrics.reconnects.get());
    } finally {
      listener.close();
    }
  }

  @Test
  void finiteCapStopsListenerAfterExhaustion() throws Exception {
    // Never succeeds; with a finite cap of 3 the listener must stop and report not-running.
    var ds = new FlakyDataSource(Integer.MAX_VALUE);
    var metrics = new CountingMetrics();
    var listener =
        PgNotificationListener.builder()
            .dataSource(ds)
            .onNotification(() -> {})
            .metrics(metrics)
            .maxReconnectAttempts(3)
            .backoff(5, 20)
            .build();

    listener.start();
    // The listener stops itself once the finite cap is exhausted.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (listener.isRunning() && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertFalse(
        listener.isRunning(), "finite cap exhausted → listener stops, push path observable");
    assertTrue(metrics.reconnects.get() >= 3, "each attempt emitted the reconnect metric");
    listener.close();
  }

  @Test
  void errorFromTheCallbackClosesTheListenerInsteadOfLeavingItLiveWithNoThread() throws Exception {
    // The working mock connection reports a notification on every poll, so the callback fires at
    // once. An Error from it must not kill the listen thread while the state still says RUNNING:
    // isRunning() would then report a live push path that nothing serves, forever.
    var ds = new FlakyDataSource(0);
    var listener =
        PgNotificationListener.builder()
            .dataSource(ds)
            .onNotification(
                () -> {
                  throw new AssertionError("simulated Error from the notification callback");
                })
            .backoff(5, 20)
            .build();

    listener.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (listener.isRunning() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(ds.notificationsFired.get() > 0, "the callback must have been invoked");
      assertFalse(listener.isRunning(), "an Error ends the listener as CLOSED, not as LIVE");
    } finally {
      listener.close();
    }
  }

  @Test
  void connectFailureWhileCreatingTheDedicatedPoolGoesThroughTheReconnectLoop() throws Exception {
    // A HikariCP source makes the listener derive a dedicated one-connection pool. Nothing listens
    // on port 1, so the first connection fails: that failure must be the reconnect loop's (one
    // reconnect metric, then the finite cap closes the listener), not the pool constructor's,
    // which would end the push path before the loop ever ran.
    var config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://127.0.0.1:1/nowhere");
    config.setUsername("nobody");
    config.setPassword("nothing");
    config.setInitializationFailTimeout(-1); // the outer pool itself must construct
    config.setMaximumPoolSize(1);
    var metrics = new CountingMetrics();
    try (var outer = new HikariDataSource(config)) {
      var listener =
          PgNotificationListener.builder()
              .dataSource(outer)
              .onNotification(() -> {})
              .metrics(metrics)
              .maxReconnectAttempts(1)
              .backoff(5, 20)
              .build();
      listener.start();
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (listener.isRunning() && System.nanoTime() < deadline) {
          Thread.sleep(50);
        }
        assertFalse(listener.isRunning(), "the finite cap closes the listener");
        assertEquals(
            1,
            metrics.reconnects.get(),
            "the connect failure must be counted by the reconnect loop, not swallowed by the"
                + " pool constructor");
      } finally {
        listener.close();
      }
    }
  }

  @Test
  void builderRejectsInvalidMaxReconnectAttempts() {
    var ds = new FlakyDataSource(0);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PgNotificationListener(
                ds, () -> {}, "streamrune_events", -2, StreamRuneMetrics.NOOP));
  }
}

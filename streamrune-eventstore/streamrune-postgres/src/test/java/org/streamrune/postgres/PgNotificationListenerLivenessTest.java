package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.streamrune.core.StreamRuneMetrics;

/**
 * A LISTEN connection can die without the listener ever being told: when the database host vanishes
 * without a FIN or an RST, every timed wait for notifications simply returns nothing. The listener
 * proves its connection alive with a bounded liveness probe whenever it has heard nothing for a
 * probe interval, and reports its push path live only while it holds a connection or has been
 * without one for less than its reconnect grace.
 */
class PgNotificationListenerLivenessTest {

  private static final long PROBE_INTERVAL_MS = 50;
  private static final long PROBE_TIMEOUT_MS = 500;

  @Test
  void aConnectionThatFailsItsLivenessProbeIsReplaced() throws Exception {
    // First connection: idle (no notification ever arrives) and its probe fails the way a timed
    // read on a vanished host fails. Second connection: idle and healthy.
    Connection dead = idleConnection(new SQLException("Read timed out", "08006"));
    Connection healthy = idleConnection(null);
    var dataSource = new ScriptedDataSource(dead, healthy);
    var metrics = new CountingMetrics();
    var listener = listener(dataSource, metrics, Duration.ofSeconds(30));

    listener.start();
    try {
      await(() -> dataSource.handedOut.size() == 2, "the dead connection was never replaced");
      await(listener::isListening, "the listener did not listen on the replacement connection");
      assertEquals(1, metrics.reconnects.get(), "a failed liveness probe is a lost connection");
      assertTrue(listener.isRunning());
      verify(dead).setNetworkTimeout(any(), eq((int) PROBE_TIMEOUT_MS));
      verify(dead).close();
    } finally {
      listener.close();
    }
  }

  @Test
  void anIdleHealthyConnectionAnswersItsProbesAndStaysInUse() throws Exception {
    Connection healthy = idleConnection(null);
    var dataSource = new ScriptedDataSource(healthy);
    var metrics = new CountingMetrics();
    var listener = listener(dataSource, metrics, Duration.ofSeconds(30));

    listener.start();
    try {
      Statement probe = healthy.createStatement();
      await(() -> probes(probe) >= 3, "an idle connection must be probed once per probe interval");
      assertEquals(1, dataSource.handedOut.size(), "a connection that answers is kept");
      assertEquals(0, metrics.reconnects.get());
      assertTrue(listener.isListening());
      // The mock connection is in manual-commit mode: every probe must end its transaction, or
      // the backend would hold every NOTIFY until it ended.
      verify(healthy, atLeast(3)).rollback();
    } finally {
      listener.close();
    }
  }

  @Test
  void aConnectionThatKeepsDeliveringNotificationsIsNotProbed() throws Exception {
    Connection busy = notifyingConnection();
    var dataSource = new ScriptedDataSource(busy);
    var notified = new AtomicInteger();
    var listener =
        PgNotificationListener.builder()
            .dataSource(dataSource)
            .onNotification(notified::incrementAndGet)
            .livenessProbe(PROBE_INTERVAL_MS, PROBE_TIMEOUT_MS)
            .build();

    listener.start();
    try {
      await(() -> notified.get() >= 20, "notifications were not delivered");
      Thread.sleep(PROBE_INTERVAL_MS * 4);
      verify(busy.createStatement(), never()).execute("SELECT 1");
    } finally {
      listener.close();
    }
  }

  @Test
  void aPushPathWithoutAConnectionIsReportedDeadOnceTheGraceRunsOutAndLiveOnceItReconnects()
      throws Exception {
    // Rejected credentials, an unreachable primary, max_connections reached: every connect fails.
    var dataSource = new SwitchableDataSource();
    dataSource.down = true;
    var listener = listener(dataSource, new CountingMetrics(), Duration.ofMillis(200));

    listener.start();
    try {
      assertTrue(listener.isPushPathLive(), "a listener that has just started is within its grace");
      await(() -> !listener.isPushPathLive(), "a push path that cannot connect stayed live");
      assertTrue(listener.isRunning(), "the listener keeps reconnecting — it never gives up");
      assertFalse(listener.isListening());

      dataSource.down = false;
      await(listener::isListening, "the listener did not reconnect");
      assertTrue(listener.isPushPathLive(), "a reconnected push path is live again");
    } finally {
      listener.close();
    }
    assertFalse(listener.isPushPathLive(), "a closed listener has no push path");
    assertFalse(listener.isListening());
  }

  private static PgNotificationListener listener(
      DataSource dataSource, StreamRuneMetrics metrics, Duration reconnectGrace) {
    return PgNotificationListener.builder()
        .dataSource(dataSource)
        .onNotification(() -> {})
        .metrics(metrics)
        .backoff(10, 20)
        .livenessProbe(PROBE_INTERVAL_MS, PROBE_TIMEOUT_MS)
        .reconnectGrace(reconnectGrace.toMillis())
        .build();
  }

  /**
   * A connection on which no notification ever arrives. Its liveness probe throws {@code
   * probeFailure}, or succeeds when that is {@code null}. Manual-commit, like a connection from a
   * pool configured with {@code auto-commit=false}.
   */
  private static Connection idleConnection(SQLException probeFailure) throws SQLException {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(conn.getAutoCommit()).thenReturn(false);
    if (probeFailure != null) {
      when(stmt.execute("SELECT 1")).thenThrow(probeFailure);
    }
    PGConnection pgConn = mock(PGConnection.class);
    when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
    when(pgConn.getNotifications(anyInt()))
        .thenAnswer(
            inv -> {
              Thread.sleep(5);
              return null;
            });
    return conn;
  }

  /** A connection that returns a notification on every wait. */
  private static Connection notifyingConnection() throws SQLException {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    when(conn.getAutoCommit()).thenReturn(true);
    PGConnection pgConn = mock(PGConnection.class);
    when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
    PGNotification note = mock(PGNotification.class);
    when(pgConn.getNotifications(anyInt()))
        .thenAnswer(
            inv -> {
              Thread.sleep(5);
              return new PGNotification[] {note};
            });
    return conn;
  }

  private static long probes(Statement stmt) {
    return mockingDetails(stmt).getInvocations().stream()
        .filter(i -> i.getMethod().getName().equals("execute"))
        .filter(i -> "SELECT 1".equals(i.getArgument(0)))
        .count();
  }

  private static void await(BooleanSupplier condition, String failure) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(5);
    }
    fail(failure);
  }

  private static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger reconnects = new AtomicInteger();

    @Override
    public void recordListenerReconnect(String channel) {
      reconnects.incrementAndGet();
    }
  }

  /** Hands out the given connections in order, then fails every further connect. */
  private static final class ScriptedDataSource extends StubDataSource {
    private final Deque<Connection> script;
    final List<Connection> handedOut = new CopyOnWriteArrayList<>();

    ScriptedDataSource(Connection... connections) {
      this.script = new ArrayDeque<>(List.of(connections));
    }

    @Override
    public synchronized Connection getConnection() throws SQLException {
      Connection next = script.poll();
      if (next == null) {
        throw new SQLException("no more scripted connections", "08001");
      }
      handedOut.add(next);
      return next;
    }
  }

  /** Fails every connect while {@link #down}; hands out a healthy idle connection otherwise. */
  private static final class SwitchableDataSource extends StubDataSource {
    volatile boolean down;

    @Override
    public Connection getConnection() throws SQLException {
      if (down) {
        throw new SQLException("connection refused", "08001");
      }
      return idleConnection(null);
    }
  }

  private abstract static class StubDataSource implements DataSource {
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
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
}

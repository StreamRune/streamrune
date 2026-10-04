package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * Proves a long-lived {@code LISTEN} connection alive.
 *
 * <p>A wait for notifications cannot tell a quiet channel from a dead connection. When the database
 * host vanishes without closing the connection — a crashed or powered-off node, an instance
 * replaced behind a virtual IP or a DNS name — no FIN or RST ever arrives, the client socket stays
 * ESTABLISHED, and every timed wait returns empty, forever. The push path is then silently gone
 * while everything about the listener still looks healthy.
 *
 * <p>The probe closes that gap. {@link #probeIfQuiet} runs {@code SELECT 1} whenever nothing has
 * been heard from the connection for one probe interval, under a network timeout that bounds how
 * long it waits for the answer. A connection to a vanished host cannot answer, so the probe fails
 * within the timeout with an {@link SQLException} and the caller's reconnect path replaces the
 * connection. Notifications received in the meantime count as proof of life, so a busy channel is
 * never probed. A NOTIFY that arrives while the probe runs is kept by the driver and returned by
 * the next wait. The connection's own network timeout is restored after each probe, so a connection
 * borrowed from an application pool goes back to it unchanged.
 *
 * <p>Not thread-safe: one instance belongs to one listen thread.
 */
final class ListenConnectionProbe {

  private static final Executor CALLING_THREAD = Runnable::run;

  private final long intervalNanos;
  private final int timeoutMs;
  private long lastHeardNanos;

  /**
   * @param intervalMs how long the connection may stay silent before it is probed; positive
   * @param timeoutMs how long a probe may wait for its answer; positive
   * @throws IllegalArgumentException if either bound is not positive or the timeout exceeds {@link
   *     Integer#MAX_VALUE} milliseconds
   */
  ListenConnectionProbe(long intervalMs, long timeoutMs) {
    if (intervalMs <= 0 || timeoutMs <= 0 || timeoutMs > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "liveness probe needs a positive interval and a positive timeout of at most "
              + Integer.MAX_VALUE
              + " ms, got interval="
              + intervalMs
              + " timeout="
              + timeoutMs);
    }
    this.intervalNanos = intervalMs * 1_000_000L;
    this.timeoutMs = (int) timeoutMs;
  }

  /** Starts the silence clock for a newly established LISTEN connection. */
  void connected() {
    lastHeardNanos = System.nanoTime();
  }

  /** Records that the connection delivered notifications, which proves it alive. */
  void heard() {
    lastHeardNanos = System.nanoTime();
  }

  /**
   * Probes the connection when it has been silent for a probe interval.
   *
   * @throws SQLException if the connection did not answer within the probe timeout, or failed
   *     otherwise; the connection is unusable and must be replaced
   */
  void probeIfQuiet(Connection conn) throws SQLException {
    if (System.nanoTime() - lastHeardNanos < intervalNanos) {
      return;
    }
    int previousTimeoutMs = conn.getNetworkTimeout();
    try {
      conn.setNetworkTimeout(CALLING_THREAD, timeoutMs);
      try (var stmt = conn.createStatement()) {
        stmt.execute("SELECT 1");
      }
      if (!conn.getAutoCommit()) {
        // A transaction left open would hold back every NOTIFY until it ended.
        conn.rollback();
      }
    } catch (SQLException e) {
      throw new SQLException(
          "LISTEN connection failed its liveness probe (timeout "
              + timeoutMs
              + " ms): "
              + e.getMessage(),
          e.getSQLState(),
          e);
    } finally {
      restoreNetworkTimeout(conn, previousTimeoutMs);
    }
    lastHeardNanos = System.nanoTime();
  }

  /** Best effort: a connection that failed its probe is replaced by the caller anyway. */
  private static void restoreNetworkTimeout(Connection conn, int timeoutMs) {
    try {
      conn.setNetworkTimeout(CALLING_THREAD, timeoutMs);
    } catch (SQLException _) {
      // the connection is broken; the caller replaces it
    }
  }
}

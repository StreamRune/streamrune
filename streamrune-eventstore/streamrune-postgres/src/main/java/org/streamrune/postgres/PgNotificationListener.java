package org.streamrune.postgres;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;

/**
 * Daemon listener that receives PostgreSQL NOTIFY events on the 'streamrune_events' channel. Runs
 * on a dedicated platform thread with a long-lived JDBC connection.
 *
 * <p>When the supplied {@link DataSource} is a {@link HikariDataSource}, the listener does
 * <b>not</b> park one of its pooled connections forever — that would permanently consume a pool
 * slot. Instead it creates its own dedicated single-connection data source (via {@link
 * PostgresNotificationSubscription#createListenDataSource}) and closes it on {@link #close()}. Any
 * other data source is used directly: a plain one opens a physical connection for the listener,
 * while any other pool — Agroal, or a proxy that wraps a HikariCP pool — lends the listener one of
 * its connections for as long as it runs. Size such a pool with one extra connection per listener,
 * or hand the listener the {@link HikariDataSource} itself.
 *
 * <p>On connection loss the listener reconnects with capped exponential backoff, logging each
 * attempt at WARN and emitting a reconnect metric. By default it <b>never</b> permanently gives up
 * — a transient outage (e.g. a 20-second PostgreSQL failover) must not kill the push path forever,
 * so the listener keeps retrying until it reconnects or is {@link #close() closed}. A finite {@link
 * Builder#maxReconnectAttempts(int) cap} can be configured for testing or fail-fast deployments; on
 * exhaustion the listener logs at ERROR, stops, and {@link #isRunning()} reports {@code false} so
 * the dead push path is observable.
 *
 * <p><b>A connection that dies silently is detected.</b> When the database host vanishes without
 * closing the connection — a crashed node, an instance replaced behind a virtual IP or a DNS name —
 * no error ever reaches the listener: its socket stays open and every wait for notifications just
 * returns nothing. So after 10 s without a notification the listener runs {@code SELECT 1} on the
 * connection, under a 5 s network timeout. A connection that cannot answer fails the probe and is
 * replaced through the reconnect path above, so a silent loss is noticed within about 15 s and
 * counts as a reconnect. A busy channel proves the connection alive with every notification and is
 * never probed.
 *
 * <p>{@link #isRunning()} says whether the listener is still trying; {@link #isListening()} says
 * whether it holds a LISTEN connection right now.
 *
 * <p>When a notification is received, the provided callback is invoked. Typically this wakes up
 * subscribers to poll for new events.
 */
public final class PgNotificationListener implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PgNotificationListener.class);

  private static final String DEFAULT_CHANNEL = "streamrune_events";
  private static final String CHANNEL_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";

  /** Sentinel for {@link #maxReconnectAttempts}: reconnect forever, never permanently give up. */
  public static final int UNBOUNDED_RECONNECTS = -1;

  /** How long each wait for notifications blocks before the loop checks its state again. */
  private static final int NOTIFICATION_WAIT_MS = 500;

  private static final long DEFAULT_INITIAL_BACKOFF_MS = 500;
  private static final long DEFAULT_MAX_BACKOFF_MS = 10_000;

  /** Silence after which the LISTEN connection is probed; see the class documentation. */
  static final long DEFAULT_LIVENESS_PROBE_INTERVAL_MS = 10_000;

  /** How long a liveness probe may wait for its answer. */
  static final long DEFAULT_LIVENESS_PROBE_TIMEOUT_MS = 5_000;

  /**
   * How long the listener may be without a LISTEN connection — reconnecting, or connecting for the
   * first time — before {@link #isPushPathLive()} stops reporting the push path as live.
   */
  static final long DEFAULT_RECONNECT_GRACE_MS = 30_000;

  /**
   * Upper bound {@link #close()} waits to join the listener thread before returning. Closing the
   * connection unblocks the LISTEN read promptly, so this only caps a stuck listener.
   */
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

  private final DataSource dataSource;
  private final Runnable onNotification;
  private final String channel;
  private final int maxReconnectAttempts;
  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final ListenConnectionProbe probe;
  private final long reconnectGraceNanos;
  private final StreamRuneMetrics metrics;

  /**
   * Listener lifecycle. A single flag cannot be both the start-guard and the loop condition: {@link
   * #close()} must unset the loop condition <em>before</em> joining the thread, which with a plain
   * boolean left a window where a racing {@link #start()} CAS'd it back to true and resurrected a
   * second listener. A three-state machine makes {@code CLOSED} terminal — {@code start()} can only
   * leave {@code NEW}, so once closed (by {@code close()} or a self-stop) the listener can never be
   * restarted into a zombie; the loop condition is a separate {@code == RUNNING} read that {@code
   * close()} flips to {@code CLOSED} to unblock the join.
   */
  private enum State {
    NEW,
    RUNNING,
    CLOSED
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private volatile Thread listenerThread;
  private volatile Connection connection;
  private volatile HikariDataSource dedicatedListenDataSource;
  private final Object connectionLock = new Object();

  /** {@code true} while the listen thread holds a connection on which LISTEN is registered. */
  private volatile boolean listening;

  /**
   * When the listener last had no LISTEN connection: its start, or the loss of its last connection.
   * Written before {@link #listening} is cleared, so a reader that sees {@code listening == false}
   * also sees the time it became false.
   */
  private volatile long notListeningSinceNanos = System.nanoTime();

  public PgNotificationListener(DataSource dataSource, Runnable onNotification) {
    this(dataSource, onNotification, DEFAULT_CHANNEL);
  }

  public PgNotificationListener(DataSource dataSource, Runnable onNotification, String channel) {
    this(dataSource, onNotification, channel, UNBOUNDED_RECONNECTS, StreamRuneMetrics.NOOP);
  }

  /**
   * Full constructor.
   *
   * @param maxReconnectAttempts consecutive reconnect failures tolerated before the listener stops,
   *     or {@link #UNBOUNDED_RECONNECTS} (the default) to reconnect forever
   * @param metrics collector for the reconnect meter; {@code null} means {@link
   *     StreamRuneMetrics#NOOP}
   */
  public PgNotificationListener(
      DataSource dataSource,
      Runnable onNotification,
      String channel,
      int maxReconnectAttempts,
      StreamRuneMetrics metrics) {
    this(
        dataSource,
        onNotification,
        channel,
        maxReconnectAttempts,
        metrics,
        DEFAULT_INITIAL_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS,
        DEFAULT_LIVENESS_PROBE_INTERVAL_MS,
        DEFAULT_LIVENESS_PROBE_TIMEOUT_MS,
        DEFAULT_RECONNECT_GRACE_MS);
  }

  private PgNotificationListener(
      DataSource dataSource,
      Runnable onNotification,
      String channel,
      int maxReconnectAttempts,
      StreamRuneMetrics metrics,
      long initialBackoffMs,
      long maxBackoffMs,
      long livenessProbeIntervalMs,
      long livenessProbeTimeoutMs,
      long reconnectGraceMs) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (onNotification == null) {
      throw new IllegalArgumentException("onNotification is required");
    }
    if (channel == null || channel.isBlank()) {
      throw new IllegalArgumentException("channel is required");
    }
    if (!channel.matches(CHANNEL_PATTERN)) {
      throw new IllegalArgumentException(
          "channel must match pattern " + CHANNEL_PATTERN + ", got: " + channel);
    }
    if (maxReconnectAttempts < 0 && maxReconnectAttempts != UNBOUNDED_RECONNECTS) {
      throw new IllegalArgumentException(
          "maxReconnectAttempts must be positive or UNBOUNDED_RECONNECTS (-1), got: "
              + maxReconnectAttempts);
    }
    if (initialBackoffMs <= 0 || maxBackoffMs < initialBackoffMs) {
      throw new IllegalArgumentException(
          "backoff must satisfy 0 < initial <= max, got initial="
              + initialBackoffMs
              + " max="
              + maxBackoffMs);
    }
    if (reconnectGraceMs < 0) {
      throw new IllegalArgumentException(
          "reconnectGrace must not be negative, got: " + reconnectGraceMs);
    }
    this.dataSource = dataSource;
    this.onNotification = onNotification;
    this.channel = channel;
    this.maxReconnectAttempts = maxReconnectAttempts;
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.probe = new ListenConnectionProbe(livenessProbeIntervalMs, livenessProbeTimeoutMs);
    this.reconnectGraceNanos = reconnectGraceMs * 1_000_000L;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
  }

  public void start() {
    if (state.get() == State.NEW) {
      // The reconnect grace runs from the start, not from construction.
      notListeningSinceNanos = System.nanoTime();
    }
    State prev = state.compareAndExchange(State.NEW, State.RUNNING);
    if (prev != State.NEW) {
      throw new IllegalStateException(
          prev == State.CLOSED
              ? "Listener is closed and cannot be restarted; create a new instance"
              : "Listener already running");
    }
    listenerThread =
        Thread.ofPlatform().name("streamrune-pg-listener").daemon(true).start(this::listenLoop);
  }

  public boolean isRunning() {
    return state.get() == State.RUNNING;
  }

  /**
   * Returns {@code true} while the listener holds a connection on which {@code LISTEN} is
   * registered and that has not failed since — not while it is connecting, reconnecting, or closed.
   * A connection whose server vanished silently counts as held until its liveness probe fails, at
   * most about 15 s after the last sign of life.
   */
  public boolean isListening() {
    return state.get() == State.RUNNING && listening;
  }

  /**
   * Returns {@code true} while the push path can be counted on: the listener is running and either
   * {@link #isListening() listening}, or without a connection for less than the reconnect grace (30
   * s) — so a routine reconnect or the first connect after {@link #start()} does not read as an
   * outage, while a listener that cannot get a connection back does.
   */
  boolean isPushPathLive() {
    if (state.get() != State.RUNNING) {
      return false;
    }
    if (listening) {
      return true;
    }
    return System.nanoTime() - notListeningSinceNanos < reconnectGraceNanos;
  }

  @Override
  public void close() {
    // Terminal transition: CLOSED wins over NEW/RUNNING and can never be reversed, so a concurrent
    // or subsequent start() (including one racing the join below) sees CLOSED and refuses instead
    // of resurrecting a second listener thread.
    state.set(State.CLOSED);
    synchronized (connectionLock) {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException _) {
          // ignore
        }
        connection = null;
      }
    }
    HikariDataSource dedicated = dedicatedListenDataSource;
    if (dedicated != null && !dedicated.isClosed()) {
      dedicated.close();
    }
    Thread t = listenerThread;
    listenerThread = null;
    if (t != null) {
      t.interrupt();
      // Join the listener thread before returning so this instance does not leave a live
      // thread behind. Resurrection is already prevented by the terminal CLOSED state above (a
      // post-close start() throws), but the join still bounds the wind-down so close() does not
      // return while the old thread is mid-cycle. Self-join guard: a thread cannot join itself
      // (close() from the listen thread).
      if (t != Thread.currentThread()) {
        try {
          t.join(CLOSE_JOIN_TIMEOUT_MS);
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private void listenLoop() {
    DataSource listenSource;
    try {
      listenSource = resolveListenSource();
    } catch (RuntimeException e) {
      log.error("Failed to create dedicated LISTEN data source; listener stopping", e);
      state.set(State.CLOSED);
      return;
    }

    try {
      listenUntilClosed(listenSource);
    } finally {
      // Every exit of the listen thread — close(), an exhausted reconnect cap, or an Error from
      // the driver or the callback — leaves the listener CLOSED, never RUNNING with no thread
      // behind it, and releases the dedicated pool. This also covers the race where close() ran
      // before the dedicated data source was assigned.
      state.set(State.CLOSED);
      HikariDataSource dedicated = dedicatedListenDataSource;
      if (dedicated != null && !dedicated.isClosed()) {
        dedicated.close();
      }
    }
  }

  private void listenUntilClosed(DataSource listenSource) {
    int consecutiveFailures = 0;
    long backoffMs = initialBackoffMs;
    while (state.get() == State.RUNNING) {
      Connection conn = null;
      try {
        conn = listenSource.getConnection();
        synchronized (connectionLock) {
          connection = conn;
        }
        probe.connected();
        try (var stmt = conn.createStatement()) {
          stmt.execute("LISTEN \"" + channel + "\"");
        }
        if (!conn.getAutoCommit()) {
          // LISTEN takes effect at commit. On an autoCommit=false connection the statement above
          // opened a transaction that nothing else in this loop would end, so the backend would
          // never register the channel.
          conn.commit();
        }
        var pgConn = conn.unwrap(PGConnection.class);
        if (consecutiveFailures > 0) {
          log.info("Reconnected to PostgreSQL, listening on channel '{}'", channel);
        }
        consecutiveFailures = 0;
        backoffMs = initialBackoffMs;
        listening = true;

        while (state.get() == State.RUNNING) {
          PGNotification[] notifications = pgConn.getNotifications(NOTIFICATION_WAIT_MS);
          if (notifications != null && notifications.length > 0) {
            probe.heard();
            onNotification.run();
          } else {
            // A connection whose server vanished without closing it never fails the wait above;
            // only a probe that has to be answered can expose it.
            probe.probeIfQuiet(conn);
          }
        }
      } catch (SQLException | RuntimeException e) {
        if (state.get() != State.RUNNING) {
          break;
        }
        consecutiveFailures++;
        // Every reconnect attempt bumps the metric: a rising count is the signal that the push path
        // is flapping even while polling keeps delivering. A listener that has permanently died
        // stops incrementing it — pair the two to distinguish "flapping" from "dead".
        recordReconnect();
        // Only a finite, exhausted cap is terminal. With the default UNBOUNDED_RECONNECTS the
        // listener never gives up — a multi-second PG failover must not kill the push path forever.
        if (maxReconnectAttempts != UNBOUNDED_RECONNECTS
            && consecutiveFailures >= maxReconnectAttempts) {
          log.error(
              "LISTEN connection on channel '{}' failed; giving up after {} consecutive attempts"
                  + " (maxReconnectAttempts={})",
              channel,
              consecutiveFailures,
              maxReconnectAttempts,
              e);
          state.set(State.CLOSED);
          break;
        }
        log.warn(
            "LISTEN connection on channel '{}' lost (attempt {}{}); reconnecting in {} ms",
            channel,
            consecutiveFailures,
            maxReconnectAttempts == UNBOUNDED_RECONNECTS ? "" : "/" + maxReconnectAttempts,
            backoffMs,
            e);
        if (!sleep(backoffMs)) {
          break;
        }
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      } finally {
        if (listening) {
          notListeningSinceNanos = System.nanoTime();
          listening = false;
        }
        synchronized (connectionLock) {
          if (conn != null) {
            try {
              conn.close();
            } catch (SQLException _) {
              // ignore
            }
          }
          connection = null;
        }
      }
    }
  }

  /**
   * Returns the data source to draw the LISTEN connection from. A {@link HikariDataSource} gets a
   * dedicated single-connection pool so the long-lived LISTEN connection never occupies a shared
   * pool slot; any other data source is used directly (see the class documentation).
   */
  private DataSource resolveListenSource() {
    if (dataSource instanceof HikariDataSource) {
      HikariDataSource dedicated =
          PostgresNotificationSubscription.createListenDataSource(dataSource);
      dedicatedListenDataSource = dedicated;
      return dedicated;
    }
    return dataSource;
  }

  /** Records a reconnect attempt; a metrics failure never disrupts the listen loop. */
  private void recordReconnect() {
    try {
      metrics.recordListenerReconnect(channel);
    } catch (RuntimeException e) {
      log.warn("Metrics recordListenerReconnect failed for channel '{}'", channel, e);
    }
  }

  /** Returns {@code false} if interrupted. */
  private boolean sleep(long ms) {
    try {
      Thread.sleep(ms);
      return true;
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  // Builder
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private DataSource dataSource;
    private Runnable onNotification;
    private String channel = DEFAULT_CHANNEL;
    private int maxReconnectAttempts = UNBOUNDED_RECONNECTS;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private long initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS;
    private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
    private long livenessProbeIntervalMs = DEFAULT_LIVENESS_PROBE_INTERVAL_MS;
    private long livenessProbeTimeoutMs = DEFAULT_LIVENESS_PROBE_TIMEOUT_MS;
    private long reconnectGraceMs = DEFAULT_RECONNECT_GRACE_MS;

    public Builder dataSource(DataSource ds) {
      this.dataSource = ds;
      return this;
    }

    public Builder onNotification(Runnable r) {
      this.onNotification = r;
      return this;
    }

    public Builder channel(String channel) {
      this.channel = channel;
      return this;
    }

    /**
     * Consecutive reconnect failures tolerated before the listener stops. Defaults to {@link
     * #UNBOUNDED_RECONNECTS} — reconnect forever so a transient outage never kills the push path
     * permanently. Set a finite positive value for fail-fast/testing behavior.
     */
    public Builder maxReconnectAttempts(int maxReconnectAttempts) {
      this.maxReconnectAttempts = maxReconnectAttempts;
      return this;
    }

    /**
     * Sets the metrics collector for the reconnect meter. Optional; defaults to {@link
     * StreamRuneMetrics#NOOP} (behavior unchanged).
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /**
     * Overrides the reconnect backoff bounds. Package-private: production uses the 500 ms → 10 s
     * defaults; tests use a small backoff so the resilience path can be exercised quickly.
     */
    Builder backoff(long initialMs, long maxMs) {
      this.initialBackoffMs = initialMs;
      this.maxBackoffMs = maxMs;
      return this;
    }

    /**
     * Overrides the liveness probe: the silence after which the LISTEN connection is probed and how
     * long the probe may wait for its answer. Package-private: production uses 10 s and 5 s; tests
     * use short values so a silent connection loss is detected quickly.
     */
    Builder livenessProbe(long intervalMs, long timeoutMs) {
      this.livenessProbeIntervalMs = intervalMs;
      this.livenessProbeTimeoutMs = timeoutMs;
      return this;
    }

    /**
     * Overrides the reconnect grace of {@link PgNotificationListener#isPushPathLive()}.
     * Package-private: production uses 30 s.
     */
    Builder reconnectGrace(long graceMs) {
      this.reconnectGraceMs = graceMs;
      return this;
    }

    public PgNotificationListener build() {
      return new PgNotificationListener(
          dataSource,
          onNotification,
          channel,
          maxReconnectAttempts,
          metrics,
          initialBackoffMs,
          maxBackoffMs,
          livenessProbeIntervalMs,
          livenessProbeTimeoutMs,
          reconnectGraceMs);
    }
  }
}

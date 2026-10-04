package org.streamrune.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import javax.sql.DataSource;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.LockException;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * PostgreSQL advisory-lock based {@link AggregateLocker}. Uses transaction-scoped advisory locks
 * ({@code pg_advisory_xact_lock}) to prevent concurrent modification of the same stream across
 * multiple application instances. The lock key is a 64-bit hash of the stream's {@code type:id}
 * text, so two aggregate types sharing an id value take independent locks.
 *
 * <p>Waiting happens <em>inside</em> PostgreSQL: the lock call blocks server-side and {@code SET
 * LOCAL lock_timeout} bounds the wait. This avoids the pathology of a client-side retry loop that
 * sleeps while holding both a pooled connection and an open transaction (pool exhaustion, vacuum
 * xmin hold-back) and wakes the waiter the moment the lock is released instead of on the next poll.
 * A {@link Duration#ZERO} timeout means "try once, do not wait" and uses {@code
 * pg_try_advisory_xact_lock}.
 *
 * <p>The returned {@link AutoCloseable} commits the transaction and closes the connection, which
 * releases the advisory lock.
 *
 * <p><b>Connection budgeting — use a dedicated lock pool.</b> The lock connection is held open for
 * the <em>entire</em> command, and inside that held lock the command bus calls {@code
 * eventStore.load()} and {@code eventStore.append()}, each of which borrows <em>another</em>
 * connection. So every in-flight command needs two connections concurrently. If the locker draws
 * its connection from the <em>same</em> pool the event store uses, then at ~half the pool size in
 * concurrent commands the pool deadlocks: each command holds a lock connection and all block
 * waiting for a second connection that no one can release (a hold-and-wait herd stall that surfaces
 * as connection-timeout failures under load). Prefer {@link #withDedicatedPool(String, String,
 * String, int)}, which backs the locker with its own small pool so lock connections never compete
 * with the event store's load/append connections — the event-store pool then needs to cover only
 * concurrent load/append (size ≈ concurrent commands), not 2× that. If you instead pass the shared
 * event-store {@link DataSource} to {@link #PgAdvisoryLocker(DataSource)}, size that pool for at
 * least 2× the expected concurrent command throughput.
 *
 * <p><b>A nested dispatch on a DIFFERENT aggregate needs one more lock connection.</b> {@code
 * VirtualThreadCommandBus} is reentrant on the SAME thread for the SAME aggregate — a follow-up
 * command an INLINE process-manager projection dispatches from inside {@code after()} against the
 * aggregate it is already processing never calls this locker again, so it costs nothing extra. But
 * a follow-up against a DIFFERENT aggregate is a genuinely independent acquisition: it checks out a
 * SECOND lock connection while the outer one is still held for the whole duration of the outer
 * command (including its own {@code after()} phase), so that in-flight command now needs THREE
 * connections concurrently (two lock, one event-store), not the two the paragraph above budgets
 * for. Size the dedicated lock pool with that in mind for any deployment that dispatches
 * cross-aggregate commands from INLINE projections — or register those process managers
 * CONTINUOUS/SCHEDULED instead, where there is no held outer lock to nest under.
 *
 * <p>A locker built with {@link #withDedicatedPool} owns its pool and must be {@link #close()
 * closed} to release it. A locker built from a caller-supplied {@link DataSource} does not own that
 * pool, so {@link #close()} is a no-op.
 */
public final class PgAdvisoryLocker implements AggregateLocker, AutoCloseable {

  /** PostgreSQL SQLSTATE for "lock_not_available" (raised when lock_timeout expires). */
  private static final String LOCK_NOT_AVAILABLE = "55P03";

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final DataSource dataSource;

  /**
   * The pool this locker owns and must close, or {@code null} when the {@link DataSource} was
   * supplied by the caller (who owns its lifecycle). Only set by {@link #withDedicatedPool}.
   */
  private final HikariDataSource ownedPool;

  public PgAdvisoryLocker(DataSource dataSource) {
    this(dataSource, null);
  }

  private PgAdvisoryLocker(DataSource dataSource, HikariDataSource ownedPool) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
    this.ownedPool = ownedPool;
  }

  /**
   * Creates a locker backed by its <em>own</em> dedicated connection pool, separate from the event
   * store's pool, so lock connections never compete with load/append connections. This is the
   * recommended way to run {@code PgAdvisoryLocker} in a multi-instance deployment: with a
   * dedicated lock pool, the event-store pool needs to cover only concurrent load/append work
   * rather than 2× the concurrent command count.
   *
   * <p>The returned locker <em>owns</em> the pool and must be {@link #close() closed} to release
   * it.
   *
   * @param jdbcUrl the JDBC URL of the same database the event store uses (required)
   * @param username the database username (may be null if encoded in the URL)
   * @param password the database password (may be null if encoded in the URL)
   * @param maxPoolSize the maximum size of the dedicated lock pool; size it for the expected
   *     maximum number of concurrent in-flight commands (must be >= 1)
   * @return a locker backed by a dedicated pool
   */
  public static PgAdvisoryLocker withDedicatedPool(
      String jdbcUrl, String username, String password, int maxPoolSize) {
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      throw new IllegalArgumentException("jdbcUrl is required");
    }
    if (maxPoolSize < 1) {
      throw new IllegalArgumentException("maxPoolSize must be >= 1, got " + maxPoolSize);
    }
    var config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    if (username != null) {
      config.setUsername(username);
    }
    if (password != null) {
      config.setPassword(password);
    }
    config.setMaximumPoolSize(maxPoolSize);
    config.setPoolName("streamrune-advisory-lock");
    // Fail fast instead of queueing forever if the dedicated pool is itself exhausted.
    config.setConnectionTimeout(10_000);
    config.setKeepaliveTime(300_000);
    config.setMaxLifetime(1_800_000);
    var pool = new HikariDataSource(config);
    return new PgAdvisoryLocker(pool, pool);
  }

  /**
   * Closes the dedicated pool this locker owns, if any. A no-op for a locker built from a
   * caller-supplied {@link DataSource} (the caller owns that pool's lifecycle). Idempotent.
   */
  @Override
  public void close() {
    if (ownedPool != null && !ownedPool.isClosed()) {
      ownedPool.close();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>A negative timeout used to be replaced by this locker's hardcoded 30-second default and then
   * BLOCK, while {@code LocalStripedLocker} — the auto-configured default — passed the same value
   * to {@code tryLock} and did not wait at all. One {@code streamrune.lock-timeout}, two
   * behaviours, selected by which locker happened to be wired. It is now rejected here (before any
   * connection is checked out, so no pool slot can leak) and at boot by all three integrations. A
   * {@code null} timeout still falls back to that 30-second default: that is a programmatic-wiring
   * omission, not an operator-supplied value — no property binding can produce it.
   *
   * <p>A positive SUB-MILLISECOND timeout is rejected on the same terms, here and at boot. It used
   * to survive every check — positive, so not the negative arm; not {@code isZero()}, so it routed
   * to {@link #acquireBlocking} — where {@code toMillis()} truncated it to {@code SET LOCAL
   * lock_timeout = '0ms'}, PostgreSQL's spelling of DISABLED. A configured 0.5 ms bound became
   * {@code pg_advisory_xact_lock} waiting forever, one pinned pool connection per command thread,
   * while the auto-configured {@code LocalStripedLocker} on the identical value made a single
   * attempt. Rejection happens before {@code getConnection()} for the same reason the negative arm
   * does: nothing has been checked out, so no pool slot can leak.
   */
  @Override
  public AutoCloseable acquireLock(StreamId streamId, Duration timeout) {
    if (timeout == null) {
      timeout = DEFAULT_TIMEOUT;
    }
    AggregateLocker.requireValidTimeout(timeout, "lock timeout");

    // Compute the hash BEFORE checking out a connection. It runs on the caller's
    // argument only, so a caller-bug NPE (null streamId) here can never leak a pool slot —
    // nothing has been checked out yet.
    long hash = hashStreamId(streamId.value());
    String sanitized = LogSanitizer.sanitizeForLog(streamId.value());

    Connection conn;
    try {
      conn = dataSource.getConnection();
    } catch (SQLException e) {
      throw new LockException("Failed to acquire database connection for stream: " + sanitized, e);
    }
    // A single guard over EVERYTHING from setAutoCommit through the
    // successful lock releases the already-checked-out connection on ANY throwable, not just
    // SQLException — a RuntimeException from a proxying/instrumented pool's setAutoCommit (the arm
    // that previously caught only SQLException), or an Error (OOME, a lazy driver LinkageError)
    // from the lock attempt. Without this the pool slot leaks permanently and, after maxPoolSize
    // such events, the whole command path deadlocks long after the fault cleared.
    try {
      try {
        conn.setAutoCommit(false);
      } catch (SQLException e) {
        throw new LockException(
            "Failed to acquire database connection for stream: " + sanitized, e);
      }
      try {
        if (timeout.isZero()) {
          // Single non-blocking attempt — Duration.ZERO means "do not wait at all".
          if (!tryAcquireLock(conn, hash)) {
            throw new LockException(
                "Advisory lock for stream: " + sanitized + " is held by another session");
          }
        } else {
          acquireBlocking(conn, hash, timeout);
        }
      } catch (SQLException e) {
        if (LOCK_NOT_AVAILABLE.equals(e.getSQLState())) {
          throw new LockException(
              "Failed to acquire advisory lock for stream: "
                  + sanitized
                  + " within timeout: "
                  + timeout);
        }
        throw new LockException("Failed to execute advisory lock attempt", e);
      }
    } catch (Throwable t) {
      // The connection was checked out; release it before propagating, whatever the throwable
      // (LockException from the inner catches, an escaped RuntimeException, or an Error).
      closeQuietly(conn);
      throw t;
    }

    return () -> {
      try {
        conn.commit();
      } finally {
        conn.close();
      }
    };
  }

  /**
   * Blocks server-side on {@code pg_advisory_xact_lock} with {@code lock_timeout} bounding the
   * wait. {@code SET LOCAL} scopes the timeout to this transaction only.
   *
   * <p>{@code lock_timeout = 0} means DISABLED to PostgreSQL, not "do not wait", so this method
   * must never emit it. The zero arm of the contract never reaches here (it routes to {@code
   * pg_try_advisory_xact_lock}) and {@link AggregateLocker#requireValidTimeout} has already
   * rejected everything positive below 1 ms, so {@code timeoutMs >= 1} holds — asserted rather than
   * clamped, because a clamp here would silently absorb a future caller that skipped the contract.
   * Package-private so that assertion can be exercised directly; it throws before touching {@code
   * conn}.
   */
  void acquireBlocking(Connection conn, long hash, Duration timeout) throws SQLException {
    long timeoutMs = Math.min(timeout.toMillis(), Integer.MAX_VALUE);
    if (timeoutMs < AggregateLocker.MIN_TIMEOUT_MILLIS) {
      throw new IllegalStateException(
          "refusing to emit lock_timeout = '"
              + timeoutMs
              + "ms', which PostgreSQL reads as \"no timeout at all\"; the timeout contract"
              + " (AggregateLocker#requireValidTimeout) should have rejected "
              + timeout
              + " before this point");
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET LOCAL lock_timeout = '" + timeoutMs + "ms'");
    }
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
      ps.setLong(1, hash);
      ps.execute();
    }
  }

  /**
   * Attempts to acquire an advisory lock using pg_try_advisory_xact_lock without waiting. Returns
   * true if the lock was acquired, false otherwise.
   */
  private boolean tryAcquireLock(Connection conn, long hash) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_try_advisory_xact_lock(?)")) {
      ps.setLong(1, hash);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private void closeQuietly(Connection conn) {
    if (conn == null) {
      return;
    }
    // close() must ALWAYS run, even when rollback() throws on a dead connection. Putting both
    // in one try meant a throwing rollback() (broken-connection failure path) skipped close(),
    // permanently leaking the pool slot until the process restarted. The finally guarantees the
    // connection is returned to the pool regardless of how rollback() fares.
    try {
      conn.rollback(); // Release any held locks before closing
    } catch (SQLException _) {
      // The connection is being discarded; a failed rollback is expected on a dead connection.
    } finally {
      try {
        conn.close();
      } catch (SQLException _) {
        // best effort — the pool evicts a connection that fails to close
      }
    }
  }

  /** FNV-1a 64-bit hash — better avalanche/collision resistance than polynomial hash. */
  private long hashStreamId(String text) {
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < text.length(); i++) {
      hash ^= text.charAt(i);
      hash *= 0x100000001b3L;
    }
    return hash;
  }
}

package org.streamrune.crypto.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.crypto.CryptoForgetSignal;

/**
 * PostgreSQL {@code LISTEN/NOTIFY} implementation of {@link CryptoForgetSignal}: the cross-instance
 * cache-invalidation channel that makes GDPR crypto-shredding fleet-effective. When a replica's
 * {@code CachedCryptoEngine.deleteKey} publishes a forgotten subject's opaque token, every peer
 * replica listening on the same {@code streamrune_crypto_forget} channel receives it and evicts
 * that subject's cached plaintext promptly — rather than serving it until the cache write-TTL
 * elapses.
 *
 * <h2>Architecture</h2>
 *
 * <ul>
 *   <li><b>Publish</b> ({@link #publish(String)}) issues {@code SELECT pg_notify(channel, token)}
 *       on a short-lived pooled connection borrowed from the shared {@code DataSource}. Best-effort
 *       per the SPI contract: a publish failure is logged and swallowed so it can never fail the
 *       originating {@code deleteKey} (the durable key deletion already succeeded, and peers are
 *       still bounded by the write-TTL ceiling).
 *   <li><b>Listen</b> ({@link #subscribe(Consumer)}) starts a single daemon virtual thread on a
 *       <b>dedicated</b> single-connection pool (never the app's main pool, so it consumes no
 *       shared connection) that issues {@code LISTEN streamrune_crypto_forget} on an auto-commit
 *       connection and dispatches each received token to every subscribed listener. It reconnects
 *       automatically if the connection drops, and a bounded {@code getNotifications} poll means
 *       {@link #close()} is honored within ~500ms.
 * </ul>
 *
 * <h2>Where the LISTEN connection comes from</h2>
 *
 * <p>{@link #PostgresCryptoForgetSignal(DataSource)} opens the dedicated pool itself, from the JDBC
 * URL and credentials it reads off the shared {@code DataSource}: a {@code HikariDataSource}, or
 * any {@code DataSource} exposing {@code getJdbcUrl()}, {@code getUrl()} or {@code getURL()} (plus
 * {@code getUsername()}/{@code getUser()} and {@code getPassword()}), such as {@code
 * PGSimpleDataSource}. A {@code DataSource} exposing none of them — a proxy, a wrapper, or a pool
 * such as Agroal that keeps its URL in a configuration object — cannot be read that way; hand such
 * a source's dedicated LISTEN pool in through {@link #PostgresCryptoForgetSignal(DataSource,
 * Supplier)} (the Quarkus integration does this for Agroal). When no LISTEN pool can be opened, the
 * channel logs a {@code WARN} and stays publish-only: peers of this replica still evict on its
 * forgets, but this replica serves a peer-forgotten subject's cached plaintext until the cache
 * write-TTL elapses.
 *
 * <p><b>Payload is an opaque, non-PII token.</b> {@code CachedCryptoEngine} publishes a SHA-256
 * hash of the subject id, never the raw id (which may itself be PII and which PostgreSQL may log
 * with the NOTIFY). This class treats the token as an opaque string and relays it verbatim.
 */
public final class PostgresCryptoForgetSignal implements CryptoForgetSignal {

  private static final Logger log = LoggerFactory.getLogger(PostgresCryptoForgetSignal.class);

  /** The single well-known channel every {@code CachedCryptoEngine} forgets/listens on. */
  static final String CHANNEL = "streamrune_crypto_forget";

  private static final int GET_NOTIFICATIONS_TIMEOUT_MS = 500;
  private static final long RECONNECT_BACKOFF_MS = 1000;

  private final DataSource dataSource;
  private final Supplier<? extends DataSource> listenDataSourceFactory;
  private final List<Consumer<String>> listeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final Object lifecycle = new Object();
  private DataSource listenDataSource;
  private Thread listenerThread;

  /**
   * Creates a channel that publishes through {@code dataSource} and listens on a dedicated
   * single-connection pool opened from the JDBC URL and credentials read off {@code dataSource}
   * (see the class javadoc for the shapes it can read).
   *
   * @param dataSource the shared application {@code DataSource}
   * @throws IllegalArgumentException if {@code dataSource} is null
   */
  public PostgresCryptoForgetSignal(DataSource dataSource) {
    this(requireDataSource(dataSource), () -> createListenDataSource(dataSource));
  }

  /**
   * Creates a channel that publishes through {@code dataSource} and listens on a connection of the
   * {@code DataSource} that {@code listenDataSourceFactory} returns.
   *
   * <p>The factory runs once, on the first {@link #subscribe(Consumer)}: a channel nobody
   * subscribes to — e.g. when the cache is disabled — opens nothing. The channel owns what the
   * factory returns and closes it in {@link #close()} when it is {@link AutoCloseable}. Return a
   * pool dedicated to this channel, holding at most one connection, that validates a connection
   * when it is borrowed: the channel holds that connection for its whole life, and after the
   * connection breaks it borrows again, so a pool that handed back the broken connection would
   * never deliver again. Never return the application's own pool, whose slot the LISTEN connection
   * would occupy for good. The channel switches the connection to auto-commit itself; PostgreSQL
   * registers a {@code LISTEN} only when its transaction commits.
   *
   * <p>If the factory throws, the channel logs a {@code WARN} and stays publish-only.
   *
   * @param dataSource the shared application {@code DataSource}, used to publish
   * @param listenDataSourceFactory opens the dedicated pool to listen on
   * @throws IllegalArgumentException if either argument is null
   */
  public PostgresCryptoForgetSignal(
      DataSource dataSource, Supplier<? extends DataSource> listenDataSourceFactory) {
    if (listenDataSourceFactory == null) {
      throw new IllegalArgumentException("listenDataSourceFactory must not be null");
    }
    // Publish uses only the shared DataSource; the dedicated LISTEN pool is opened lazily on the
    // first subscribe, so merely constructing an unused channel allocates no connection.
    this.dataSource = requireDataSource(dataSource);
    this.listenDataSourceFactory = listenDataSourceFactory;
  }

  private static DataSource requireDataSource(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource must not be null");
    return dataSource;
  }

  @Override
  public void publish(String token) {
    if (token == null) return;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT pg_notify(?, ?)")) {
      ps.setString(1, CHANNEL);
      ps.setString(2, token);
      // pg_notify is delivered at commit; on an autoCommit=false connection the statement alone
      // would leave it in a transaction the pool rolls back.
      PostgresCryptoTransactions.commitIfManual(conn, c -> ps.execute());
    } catch (SQLException e) {
      // Best-effort per the SPI contract: never fail deleteKey. Peers still converge via write-TTL.
      log.warn(
          "Failed to publish crypto-forget NOTIFY on channel '{}'; peer replicas will converge via"
              + " the cache write-TTL ceiling instead of promptly",
          CHANNEL,
          e);
    }
  }

  @Override
  public void subscribe(Consumer<String> listener) {
    if (listener == null) throw new IllegalArgumentException("listener must not be null");
    listeners.add(listener);
    // Start the single LISTEN loop on the first subscription (subsequent subscribers just share
    // it). Opening the dedicated LISTEN pool is best-effort: a DataSource the channel cannot open
    // one from (a test double, a proxied/wrapped source) degrades to publish-only + the write-TTL
    // ceiling rather than failing the engine's construction. Done synchronously here (not in the
    // loop thread) so that by the time the engine is built the listener is already establishing.
    if (running.compareAndSet(false, true)) {
      synchronized (lifecycle) {
        if (!running.get()) {
          return; // closed concurrently: open nothing that close() would no longer see
        }
        DataSource listenSource;
        try {
          listenSource = listenDataSourceFactory.get();
          if (listenSource == null) {
            throw new IllegalStateException("the LISTEN DataSource factory returned null");
          }
        } catch (RuntimeException e) {
          log.warn(
              "Crypto-forget LISTEN disabled: cannot open a dedicated LISTEN connection from {}."
                  + " Prompt cross-replica cache invalidation is off on this replica: it serves a"
                  + " subject another replica forgot from its cache until the cache write-TTL"
                  + " elapses. Publishing (pg_notify) still works.",
              dataSource.getClass().getName(),
              e);
          return;
        }
        listenDataSource = listenSource;
        listenerThread =
            Thread.ofVirtual()
                .name("streamrune-crypto-forget-listen")
                .start(() -> listenLoop(listenSource));
      }
    }
  }

  private void listenLoop(DataSource listenSource) {
    while (running.get()) {
      try (Connection conn = listenSource.getConnection()) {
        if (!conn.getAutoCommit()) {
          // A LISTEN inside a transaction registers only on commit, and notifications are
          // delivered only between transactions: on a non-auto-commit connection nothing arrives.
          conn.setAutoCommit(true);
        }
        try (var stmt = conn.createStatement()) {
          stmt.execute("LISTEN " + CHANNEL);
        }
        receiveUntilStopped(conn.unwrap(PGConnection.class));
      } catch (SQLException e) {
        if (running.get()) {
          log.warn(
              "Crypto-forget LISTEN connection on channel '{}' failed; reconnecting in {} ms",
              CHANNEL,
              RECONNECT_BACKOFF_MS,
              e);
          sleep(RECONNECT_BACKOFF_MS);
        }
      }
    }
  }

  /** Dispatches every notification the LISTEN connection receives until the signal is closed. */
  private void receiveUntilStopped(PGConnection pgConn) throws SQLException {
    while (running.get()) {
      PGNotification[] notifications = pgConn.getNotifications(GET_NOTIFICATIONS_TIMEOUT_MS);
      if (notifications != null) {
        for (PGNotification n : notifications) {
          dispatch(n.getParameter());
        }
      }
    }
  }

  private void dispatch(String token) {
    for (Consumer<String> listener : listeners) {
      try {
        listener.accept(token);
      } catch (RuntimeException e) {
        // One misbehaving listener must not stop the others or wedge the listen loop.
        log.warn("A crypto-forget listener threw while handling a token", e);
      }
    }
  }

  @Override
  public void close() {
    running.set(false);
    DataSource lds;
    synchronized (lifecycle) {
      if (listenerThread != null) {
        listenerThread.interrupt();
      }
      lds = listenDataSource;
      listenDataSource = null;
    }
    if (lds instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        log.warn("Failed to close the crypto-forget LISTEN pool", e);
      }
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Builds a dedicated single-connection pool for LISTEN, separate from the app's main pool (LISTEN
   * needs a long-lived connection, and borrowing one from the shared pool for the process lifetime
   * would silently consume a slot). Tolerant of the common {@link DataSource} shapes — {@code
   * HikariDataSource}, {@code PGSimpleDataSource}, and other beans exposing the usual accessors.
   * Hikari validates a connection that sat idle when it is borrowed, so a broken LISTEN connection
   * is replaced on the loop's next borrow.
   */
  private static HikariDataSource createListenDataSource(DataSource dataSource) {
    String jdbcUrl;
    String username = null;
    String password = null;
    if (dataSource instanceof HikariDataSource hds) {
      jdbcUrl = hds.getJdbcUrl();
      username = hds.getUsername();
      password = hds.getPassword();
    } else {
      jdbcUrl = firstString(dataSource, "getJdbcUrl", "getUrl", "getURL");
      username = firstString(dataSource, "getUsername", "getUser");
      password = firstString(dataSource, "getPassword");
    }
    if (jdbcUrl == null) {
      throw new IllegalArgumentException(
          "Cannot derive a JDBC URL from "
              + dataSource.getClass().getName()
              + " to open the crypto-forget LISTEN connection. Provide a HikariDataSource or a"
              + " DataSource exposing getJdbcUrl()/getUrl()/getURL(), or pass the LISTEN pool to"
              + " PostgresCryptoForgetSignal(DataSource, Supplier).");
    }
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    if (username != null) config.setUsername(username);
    if (password != null) config.setPassword(password);
    config.setMaximumPoolSize(1);
    config.setMinimumIdle(1);
    config.setPoolName("streamrune-crypto-forget-listen");
    config.setConnectionTimeout(5000);
    config.setIdleTimeout(0); // never idle-close the LISTEN connection
    config.setMaxLifetime(0); // never recycle the LISTEN connection
    return new HikariDataSource(config);
  }

  private static String firstString(DataSource ds, String... methodNames) {
    for (String methodName : methodNames) {
      try {
        Object value = ds.getClass().getMethod(methodName).invoke(ds);
        if (value instanceof String s && !s.isBlank()) {
          return s;
        }
      } catch (ReflectiveOperationException _) {
        // try the next accessor
      }
    }
    return null;
  }
}

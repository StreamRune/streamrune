package org.streamrune.crypto.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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
 *   <li><b>Listen</b> ({@link #subscribe(Consumer, Runnable)}) starts a single daemon virtual
 *       thread on a <b>dedicated</b> single-connection pool (never the app's main pool, so it
 *       consumes no shared connection) that issues {@code LISTEN streamrune_crypto_forget} on an
 *       auto-commit connection and dispatches each received token to every subscribed listener. It
 *       reconnects automatically if the connection drops, and a bounded {@code getNotifications}
 *       poll means {@link #close()} is honored within ~500ms — within about 5.5 s when a liveness
 *       probe is in flight against a dead server on a pool that does not abort its connection when
 *       it closes (the framework's own Hikari pool does; Agroal and a pool you supply may not).
 * </ul>
 *
 * <p><b>A connection that dies silently is detected.</b> When the database host vanishes without
 * closing the connection — a crashed node, an instance replaced behind a virtual IP or a DNS name —
 * no error ever reaches the listen thread: the connection only reads, its socket stays open, and
 * every wait for notifications returns nothing. So after 30 s without a notification the thread
 * asks the connection to prove itself alive ({@link Connection#isValid(int)}, answered within 5 s).
 * A connection that does not answer is aborted and replaced like a dropped one — the replica stops
 * reporting that it listens, re-issues its {@code LISTEN} on a new connection and evicts again — so
 * a silent loss is noticed within about 35 s. A channel that keeps delivering is never probed.
 *
 * <p>A subscriber callback that throws, an {@link Error} included, is logged and stops neither the
 * other subscribers nor the listen thread.
 *
 * <h2>When the {@code LISTEN} takes effect</h2>
 *
 * <p>A forget published before this replica's {@code LISTEN} has taken effect — before the first
 * one, or while the connection is lost and not yet re-established — is not delivered to it. Each
 * time the {@code LISTEN} statement has returned on the auto-commit connection, it is committed and
 * PostgreSQL queues for this session every notification committed from then on. Only then does the
 * listen thread run every subscriber's {@code onListening} callback, mark the channel {@link
 * #isListening() listening}, and start receiving; {@code CachedCryptoEngine} evicts its whole
 * plaintext cache in that callback. In this order nothing is lost: a forget committed before the
 * {@code LISTEN} deleted its key before the eviction, so the next read finds the key gone, and a
 * forget committed after it is queued and delivered once the callbacks have run. Evicting first and
 * listening after would leave a gap in which a read caches a subject whose forget is committed
 * before the {@code LISTEN} and never arrives. A token that arrives while the callbacks run waits
 * in the connection's queue until they return.
 *
 * <p>{@link #isSubscribed()} is {@code true} from the first {@link #subscribe(Consumer, Runnable)}
 * until {@link #close()}, also when no LISTEN pool can be opened; {@link #isListening()} only while
 * the replica hears. The framework health indicators report the difference as {@code DEGRADED}.
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

  /**
   * The {@code application_name} of the dedicated LISTEN session, appended after the application's
   * own name when it has one, so that {@code pg_stat_activity} tells the LISTEN session from the
   * application's. See {@link #listenApplicationName(String, String)}.
   */
  static final String LISTEN_APPLICATION_NAME = "streamrune-crypto-forget-listen";

  /** PostgreSQL keeps at most 63 bytes of an {@code application_name} (NAMEDATALEN - 1). */
  private static final int MAX_APPLICATION_NAME_BYTES = 63;

  /** The single well-known channel every {@code CachedCryptoEngine} forgets/listens on. */
  static final String CHANNEL = "streamrune_crypto_forget";

  private static final int GET_NOTIFICATIONS_TIMEOUT_MS = 500;

  /** Silence after which the LISTEN connection is probed; see the class javadoc. */
  static final long DEFAULT_LIVENESS_PROBE_INTERVAL_MS = 30_000;

  /** How long a liveness probe may wait for the server's answer, in seconds. */
  private static final int LIVENESS_PROBE_TIMEOUT_SECONDS = 5;

  /** How long the listen loop waits before it borrows a connection again after one failed. */
  static final long DEFAULT_RECONNECT_BACKOFF_MS = 1000;

  private final DataSource dataSource;
  private final Supplier<? extends DataSource> listenDataSourceFactory;
  private final long livenessProbeIntervalNanos;
  private final long reconnectBackoffMs;
  private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * {@code true} while the listen thread holds a connection whose {@code LISTEN} took effect and
   * whose subscribers have run {@code onListening} since.
   */
  private volatile boolean listening;

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
   * <p>The factory runs once, on the first {@link #subscribe(Consumer, Runnable)}: a channel nobody
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
    this(
        dataSource,
        listenDataSourceFactory,
        DEFAULT_LIVENESS_PROBE_INTERVAL_MS,
        DEFAULT_RECONNECT_BACKOFF_MS);
  }

  /**
   * As {@link #PostgresCryptoForgetSignal(DataSource, Supplier)}, with the silence after which the
   * LISTEN connection is probed and the wait before a failed connection is replaced.
   * Package-private: production uses 30 s and 1 s; tests shorten or lengthen them.
   *
   * @throws IllegalArgumentException if an argument is null or a duration is not positive
   */
  PostgresCryptoForgetSignal(
      DataSource dataSource,
      Supplier<? extends DataSource> listenDataSourceFactory,
      long livenessProbeIntervalMs,
      long reconnectBackoffMs) {
    if (listenDataSourceFactory == null) {
      throw new IllegalArgumentException("listenDataSourceFactory must not be null");
    }
    if (livenessProbeIntervalMs <= 0 || reconnectBackoffMs <= 0) {
      throw new IllegalArgumentException(
          "the liveness probe interval and the reconnect backoff must be positive, got "
              + livenessProbeIntervalMs
              + " ms and "
              + reconnectBackoffMs
              + " ms");
    }
    // Publish uses only the shared DataSource; the dedicated LISTEN pool is opened lazily on the
    // first subscribe, so merely constructing an unused channel allocates no connection.
    this.dataSource = requireDataSource(dataSource);
    this.listenDataSourceFactory = listenDataSourceFactory;
    this.livenessProbeIntervalNanos = livenessProbeIntervalMs * 1_000_000L;
    this.reconnectBackoffMs = reconnectBackoffMs;
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

  private record Subscriber(Consumer<String> onForget, Runnable onListening) {}

  @Override
  public void subscribe(Consumer<String> onForget, Runnable onListening) {
    if (onForget == null) throw new IllegalArgumentException("listener must not be null");
    if (onListening == null) throw new IllegalArgumentException("onListening must not be null");
    subscribers.add(new Subscriber(onForget, onListening));
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

  /**
   * {@inheritDoc}
   *
   * <p>{@code true} from the first subscribe until {@link #close()}, also when no LISTEN pool could
   * be opened.
   */
  @Override
  public boolean isSubscribed() {
    return running.get();
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code true} while the listen thread holds a connection on which {@code LISTEN} took effect
   * and that has not failed since, once every subscriber's {@code onListening} has run on it — not
   * while it connects or reconnects (from the moment the connection failed, or failed its liveness
   * probe, through the wait before the next attempt), when no LISTEN pool could be opened, or after
   * {@link #close()}. A connection whose server vanished silently counts as listening until its
   * liveness probe fails (see the class javadoc).
   */
  @Override
  public boolean isListening() {
    return running.get() && listening;
  }

  private void listenLoop(DataSource listenSource) {
    boolean reconnecting = false;
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
        // The LISTEN has committed: every forget committed from here on is queued for this
        // session. Only now may subscribers drop what they hold (see the class javadoc).
        startedListening(reconnecting);
        receiveUntilStopped(conn);
      } catch (SQLException | RuntimeException e) {
        // Not listening from the moment the connection failed: the backoff below is part of the
        // time this replica cannot hear.
        listening = false;
        if (running.get()) {
          log.warn(
              "Crypto-forget LISTEN connection on channel '{}' failed; reconnecting in {} ms",
              CHANNEL,
              reconnectBackoffMs,
              e);
          reconnecting = true;
          sleep(reconnectBackoffMs);
        }
      } finally {
        listening = false;
      }
    }
  }

  /**
   * Runs every subscriber's {@code onListening} and then marks the channel listening. A callback
   * that throws — an {@link Error} as well — is logged and stops neither the others nor the listen
   * thread.
   */
  private void startedListening(boolean reconnecting) {
    for (Subscriber subscriber : subscribers) {
      try {
        subscriber.onListening().run();
      } catch (Throwable e) {
        // An escaping Error would end the listen thread for good: the replica would stay deaf,
        // never reconnect and never evict again.
        log.warn("A crypto-forget subscriber threw while the channel started listening", e);
      }
    }
    listening = true;
    if (reconnecting) {
      log.info(
          "Crypto-forget LISTEN on channel '{}' re-established; its subscribers dropped what they"
              + " cached while it could not hear",
          CHANNEL);
    }
  }

  /**
   * Dispatches every notification the LISTEN connection receives until the signal is closed, and
   * probes the connection after each {@link #DEFAULT_LIVENESS_PROBE_INTERVAL_MS probe interval} in
   * which it delivered nothing.
   *
   * @throws SQLException when the connection fails, or fails its liveness probe
   */
  private void receiveUntilStopped(Connection conn) throws SQLException {
    PGConnection pgConn = conn.unwrap(PGConnection.class);
    long lastHeardNanos = System.nanoTime();
    while (running.get()) {
      PGNotification[] notifications = pgConn.getNotifications(GET_NOTIFICATIONS_TIMEOUT_MS);
      if (notifications != null && notifications.length > 0) {
        lastHeardNanos = System.nanoTime();
        for (PGNotification n : notifications) {
          dispatch(n.getParameter());
        }
      } else if (System.nanoTime() - lastHeardNanos >= livenessProbeIntervalNanos) {
        probe(conn);
        lastHeardNanos = System.nanoTime();
      }
    }
  }

  /**
   * Proves a quiet LISTEN connection alive: a connection whose server vanished without closing it
   * returns nothing from every wait for notifications and raises no error, so only a round trip
   * that needs an answer can tell. A NOTIFY that arrives during the round trip is kept by the
   * driver and returned by the next wait.
   *
   * @throws SQLException if the connection did not answer within the probe timeout; the connection
   *     has been aborted, so its pool does not hand it out again
   */
  private void probe(Connection conn) throws SQLException {
    if (conn.isValid(LIVENESS_PROBE_TIMEOUT_SECONDS)) {
      return;
    }
    try {
      conn.abort(Runnable::run);
    } catch (SQLException | RuntimeException e) {
      log.debug("Could not abort the crypto-forget LISTEN connection that failed its probe", e);
    }
    throw new SQLException(
        "the crypto-forget LISTEN connection did not answer its liveness probe within "
            + LIVENESS_PROBE_TIMEOUT_SECONDS
            + " s",
        "08006");
  }

  private void dispatch(String token) {
    for (Subscriber subscriber : subscribers) {
      try {
        subscriber.onForget().accept(token);
      } catch (Throwable e) {
        // One misbehaving listener must not stop the others or end the listen thread.
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
   * is replaced on the loop's next borrow. The driver's {@code tcpKeepAlive} is on (a {@code
   * tcpKeepAlive} parameter in the JDBC URL takes precedence), and its sessions are named by {@link
   * #listenApplicationName(String, String)}; the liveness probe is what finds a dead connection
   * within seconds, the operating system's keepalive only after its own idle time.
   */
  static HikariDataSource createListenDataSource(DataSource dataSource) {
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
    String configuredName =
        dataSource instanceof HikariDataSource hds
            ? hds.getDataSourceProperties().getProperty("ApplicationName")
            : null;
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(withApplicationName(jdbcUrl, listenApplicationName(jdbcUrl, configuredName)));
    if (username != null) config.setUsername(username);
    if (password != null) config.setPassword(password);
    config.setMaximumPoolSize(1);
    config.setMinimumIdle(1);
    config.setPoolName("streamrune-crypto-forget-listen");
    config.setConnectionTimeout(5000);
    config.setIdleTimeout(0); // never idle-close the LISTEN connection
    config.setMaxLifetime(0); // never recycle the LISTEN connection
    config.addDataSourceProperty("tcpKeepAlive", "true");
    return new HikariDataSource(config);
  }

  /**
   * The {@code application_name} the dedicated LISTEN session of a replica carries: the
   * application's own name, when its JDBC URL or data source configures one, followed by {@code
   * -streamrune-crypto-forget-listen}; just {@code streamrune-crypto-forget-listen} when it
   * configures none. It tells the LISTEN session from the application's in {@code pg_stat_activity}
   * — whose {@code query} column cannot: a liveness probe blanks it — and keeps apart the sessions
   * of replicas that name themselves. The application's part is shortened when needed, so the name
   * stays within the 63 bytes PostgreSQL keeps and the suffix survives.
   *
   * <p>Public for the integrations that open the dedicated pool of a connection pool of their own,
   * such as Quarkus' Agroal.
   *
   * @param jdbcUrl the application's JDBC URL, whose {@code ApplicationName} parameter is the name
   *     its connections carry — the driver lets it win over a property; {@code null} or a URL
   *     without one contributes no name
   * @param configuredApplicationName the application name set on the data source as a property, or
   *     {@code null}; used when the URL has none
   * @return the name of the LISTEN session
   */
  public static String listenApplicationName(String jdbcUrl, String configuredApplicationName) {
    String base = urlApplicationName(jdbcUrl);
    if (base == null || base.isBlank()) {
      base = configuredApplicationName;
    }
    if (base == null || base.isBlank()) {
      return LISTEN_APPLICATION_NAME;
    }
    String suffix = "-" + LISTEN_APPLICATION_NAME;
    int room = MAX_APPLICATION_NAME_BYTES - suffix.length();
    StringBuilder kept = new StringBuilder();
    int bytes = 0;
    for (int i = 0; i < base.length(); ) {
      int codePoint = base.codePointAt(i);
      int width = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8).length;
      if (bytes + width > room) {
        break;
      }
      kept.appendCodePoint(codePoint);
      bytes += width;
      i += Character.charCount(codePoint);
    }
    return kept.isEmpty() ? LISTEN_APPLICATION_NAME : kept + suffix;
  }

  /**
   * {@code jdbcUrl} with its {@code ApplicationName} parameter set to {@code applicationName}: an
   * existing one is replaced, since a parameter in the URL takes precedence over the properties
   * handed to the driver. Public for the same integrations as {@link #listenApplicationName(String,
   * String)}.
   *
   * @param jdbcUrl a PostgreSQL JDBC URL
   * @param applicationName the application name to set
   * @return the URL carrying that application name
   */
  public static String withApplicationName(String jdbcUrl, String applicationName) {
    int query = jdbcUrl.indexOf('?');
    String head = query < 0 ? jdbcUrl : jdbcUrl.substring(0, query);
    List<String> parameters = new ArrayList<>();
    if (query >= 0) {
      for (String parameter : jdbcUrl.substring(query + 1).split("&")) {
        if (!parameter.isEmpty() && !parameter.startsWith("ApplicationName=")) {
          parameters.add(parameter);
        }
      }
    }
    parameters.add("ApplicationName=" + URLEncoder.encode(applicationName, StandardCharsets.UTF_8));
    return head + "?" + String.join("&", parameters);
  }

  private static String urlApplicationName(String jdbcUrl) {
    int query = jdbcUrl == null ? -1 : jdbcUrl.indexOf('?');
    if (query < 0) {
      return null;
    }
    for (String parameter : jdbcUrl.substring(query + 1).split("&")) {
      if (parameter.startsWith("ApplicationName=")) {
        return URLDecoder.decode(
            parameter.substring("ApplicationName=".length()), StandardCharsets.UTF_8);
      }
    }
    return null;
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

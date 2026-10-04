package org.streamrune.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.EventSubscription;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.ProjectionReadPoisonException;
import org.streamrune.runtime.ReadPoisonAware;
import org.streamrune.runtime.ReadPoisonClassifier;

/**
 * PostgreSQL-backed {@link EventSubscription} that uses {@code LISTEN}/{@code NOTIFY} for
 * low-latency event delivery.
 *
 * <h2>Architecture</h2>
 *
 * <ul>
 *   <li>Maintains one <b>dedicated</b> connection for LISTEN (never pooled)
 *   <li>Uses the shared {@code DataSource} for event polling and offset saves (pooled)
 *   <li>When a notification arrives, <b>drains</b> to the head — reading successive pages until the
 *       backlog is exhausted, so a burst that appended more than {@code fetchSize} events in one
 *       append (one NOTIFY) is fully delivered rather than stranding everything past the first page
 *   <li>Polls once right after LISTEN is established — both on initial start and after every
 *       reconnect — so events appended while the subscription was down (and whose NOTIFY was
 *       therefore missed) are caught up without waiting for the next notification
 *   <li>Polls periodically on {@link SubscriptionConfig#pollingInterval()} as a fallback, so a
 *       dropped/failed/coalesced best-effort NOTIFY is still recovered even if no further
 *       notification ever arrives
 *   <li>If the connection drops, automatically reconnects — including a connection whose server
 *       vanished without closing it (a crashed node, an instance replaced behind a virtual IP or a
 *       DNS name), which no wait for notifications can tell from a quiet channel: after 10 s
 *       without a notification the connection must answer {@code SELECT 1} within 5 s or is
 *       replaced
 * </ul>
 *
 * <p>For production, {@link HybridEventSubscription} is the recommended subscription; this class is
 * the pure LISTEN/NOTIFY variant with an equivalent drain-and-poll-fallback delivery guarantee.
 *
 * <p><b>Read-poison bound.</b> A read that fails DETERMINISTICALLY — an event whose type is not in
 * this replica's {@code EventTypeRegistry} after a rolling deploy, or a corrupt /
 * schema-incompatible payload — fails identically forever. Retrying it on every NOTIFY and every
 * fallback tick froze the read model at that checkpoint while {@code isRunning()} stayed {@code
 * true}, so the owning {@code ContinuousProjectionRunner}'s liveness check never fired, its
 * read-poison branch could not apply (this class was not {@link ReadPoisonAware}), and /health
 * reported UP — for a low-volume projection, indefinitely, because the lag never crossed the health
 * threshold. The SAME event on the default polling path HALTs the projection after {@code
 * readPoisonBound} attempts. This class now applies that identical bound: after {@code
 * readPoisonBound} consecutive deterministic poison reads AT THE SAME CHECKPOINT it stops itself
 * ({@code isRunning() == false}) and exposes the terminal {@link ProjectionReadPoisonException} via
 * {@link #readPoisonError()}, which is exactly what the runner reads to HALT with health DOWN. A
 * transient infrastructure read failure, or any progress of the checkpoint, resets the streak and
 * keeps the previous unbounded-retry behaviour. {@code readPoisonBound == 0} (the {@link Builder}
 * default, so standalone callers are unaffected) disables the bound entirely.
 *
 * <h2>Setup</h2>
 *
 * <p>{@link PostgresEventStore} issues {@code pg_notify('streamrune_events', ...)} (the payload is
 * the last appended global offset) after each append it commits — best-effort, which is why the
 * periodic poll fallback exists. Code that appends through another path can call {@link
 * #notifyNewEvents()} on an active subscription to trigger an immediate poll.
 *
 * <p>The notification channel is {@code streamrune_events}. The dedicated connection issues {@code
 * LISTEN streamrune_events} on start.
 *
 * <h2>Where the LISTEN connection comes from</h2>
 *
 * <p>{@link Builder#listenDataSource(DataSource)} takes the DataSource the subscription listens on;
 * the subscription owns it and closes it with itself. {@link #createListenDataSource(DataSource)}
 * builds one from the application's DataSource: a {@code HikariDataSource}, or any DataSource
 * exposing {@code getJdbcUrl()}, {@code getUrl()} or {@code getURL()} (plus {@code getUsername()} /
 * {@code getUser()} and {@code getPassword()}), such as {@code PGSimpleDataSource}. A DataSource
 * exposing none of them — Agroal on Quarkus, which keeps its URL in a configuration object, or a
 * proxy around a pool — cannot be read that way and {@code createListenDataSource} refuses it.
 * Build the LISTEN DataSource from the same settings yourself (on Quarkus, from {@code
 * quarkus.datasource.*}) and hand it to {@code listenDataSource}: a pool dedicated to this
 * subscription, holding at most one connection, that validates a connection when it is borrowed
 * (the subscription holds that connection for its whole life and borrows again after it breaks).
 * Never hand it the application's own pool, whose connection the subscription would keep for good
 * and close with itself. The subscription switches the connection's transaction mode itself:
 * PostgreSQL registers a {@code LISTEN} only when its transaction commits. {@code
 * HybridEventSubscription} has no such blind spot: on a DataSource that is not a {@code
 * HikariDataSource} it borrows its LISTEN connection from that DataSource.
 *
 * <pre>{@code
 * var subscription =
 *     PostgresNotificationSubscription.builder(SubscriptionName.of("order-summary"))
 *         .listenDataSource(PostgresNotificationSubscription.createListenDataSource(dataSource))
 *         .eventStore(eventStore)
 *         .offsetStore(offsetStore)
 *         .listener(listener)
 *         .config(config)
 *         .fetchSize(100)
 *         .build();
 * }</pre>
 */
public final class PostgresNotificationSubscription implements EventSubscription, ReadPoisonAware {

  private static final Logger log = LoggerFactory.getLogger(PostgresNotificationSubscription.class);

  private static final String CHANNEL = "streamrune_events";

  /** How long {@link #close()} waits for the listen thread, matching the sibling runners. */
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000;

  private final SubscriptionName subscriptionName;
  private final DataSource listenDataSource;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final EventListener listener;
  private final SubscriptionConfig config;
  private final int fetchSize;
  private final ListenConnectionProbe probe;

  /**
   * Consecutive deterministic poison reads at the same checkpoint tolerated before this
   * subscription stops itself terminally. {@code 0} disables the bound (the standalone default).
   */
  private final int readPoisonBound;

  /** The terminal read-poison error that stopped this subscription; {@code null} otherwise. */
  private final AtomicReference<ProjectionReadPoisonException> readPoison = new AtomicReference<>();

  // Read-poison streak state. Touched only from pollAndDeliver, which runs solely on the single
  // listen thread, so plain fields suffice (mirrors PollingEventSubscription's under-pollLock
  // fields).
  private GlobalOffset readPoisonCheckpoint;
  private int readPoisonFailures;

  private final AtomicBoolean running = new AtomicBoolean(false);

  /** Set once by {@link #close()}; makes close terminal so a restart cannot be accepted. */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile Thread listenerThread;

  // close() interrupts the listen thread through these: never while a page is being delivered
  // (see deliver), but when that delivery ends. Both booleans guarded by interruptLock, so close()
  // cannot interrupt between the listen thread clearing its flag and starting the delivery.
  private final ReentrantLock interruptLock = new ReentrantLock();
  private boolean delivering;
  private boolean stopRequested;
  private final AtomicBoolean notified = new AtomicBoolean(false);
  private final AtomicBoolean connected = new AtomicBoolean(false);

  private PostgresNotificationSubscription(Builder builder) {
    if (builder.subscriptionName == null) {
      throw new IllegalArgumentException("subscriptionName is required");
    }
    if (builder.listenDataSource == null) {
      throw new IllegalArgumentException("listenDataSource is required");
    }
    if (builder.eventStore == null) throw new IllegalArgumentException("eventStore is required");
    if (builder.offsetStore == null) throw new IllegalArgumentException("offsetStore is required");
    if (builder.listener == null) throw new IllegalArgumentException("listener is required");
    if (builder.config == null) throw new IllegalArgumentException("config is required");
    if (builder.fetchSize <= 0)
      throw new IllegalArgumentException("fetchSize is required and must be positive");
    this.subscriptionName = builder.subscriptionName;
    this.listenDataSource = builder.listenDataSource;
    this.eventStore = builder.eventStore;
    this.offsetStore = builder.offsetStore;
    this.listener = builder.listener;
    this.config = builder.config;
    this.fetchSize = builder.fetchSize;
    this.readPoisonBound = Math.max(0, builder.readPoisonBound);
    this.probe =
        new ListenConnectionProbe(builder.livenessProbeIntervalMs, builder.livenessProbeTimeoutMs);
  }

  /**
   * Starts a builder for a subscription named {@code subscriptionName}. {@link
   * Builder#listenDataSource}, {@link Builder#eventStore}, {@link Builder#offsetStore}, {@link
   * Builder#listener}, {@link Builder#config} and a positive {@link Builder#fetchSize} are
   * required; {@link Builder#readPoisonBound} is optional.
   *
   * @param subscriptionName the subscription's name; it keys the checkpoint in the offset store and
   *     names the listen thread
   */
  public static Builder builder(SubscriptionName subscriptionName) {
    return new Builder(subscriptionName);
  }

  /** Builder for {@link PostgresNotificationSubscription}; see {@link #builder}. */
  public static final class Builder {

    private final SubscriptionName subscriptionName;
    private DataSource listenDataSource;
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private EventListener listener;
    private SubscriptionConfig config;
    private int fetchSize;
    private int readPoisonBound;
    private long livenessProbeIntervalMs =
        PgNotificationListener.DEFAULT_LIVENESS_PROBE_INTERVAL_MS;
    private long livenessProbeTimeoutMs = PgNotificationListener.DEFAULT_LIVENESS_PROBE_TIMEOUT_MS;

    private Builder(SubscriptionName subscriptionName) {
      this.subscriptionName = subscriptionName;
    }

    /**
     * The DataSource the subscription takes its LISTEN connection from, usually {@link
     * #createListenDataSource(DataSource)}. Any DataSource dedicated to this subscription works —
     * build one yourself when {@code createListenDataSource} cannot read the application's settings
     * (see "Where the LISTEN connection comes from" in the class documentation). The subscription
     * owns it and closes it in {@link PostgresNotificationSubscription#close()} when it is {@link
     * AutoCloseable}; never pass the application's own pool.
     */
    public Builder listenDataSource(DataSource listenDataSource) {
      this.listenDataSource = listenDataSource;
      return this;
    }

    /** The event store the subscription reads the global stream from. */
    public Builder eventStore(EventStore eventStore) {
      this.eventStore = eventStore;
      return this;
    }

    /** The store holding this subscription's checkpoint. */
    public Builder offsetStore(OffsetStore offsetStore) {
      this.offsetStore = offsetStore;
      return this;
    }

    /** The listener each read page is delivered to. */
    public Builder listener(EventListener listener) {
      this.listener = listener;
      return this;
    }

    /** The subscription config; its polling interval drives the poll fallback. */
    public Builder config(SubscriptionConfig config) {
      this.config = config;
      return this;
    }

    /** The page size of each global-stream read; must be positive. */
    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    /**
     * The read-poison bound: consecutive deterministic poison reads at the same checkpoint
     * tolerated before the subscription stops terminally. {@code 0} (the default) disables the
     * bound (unbounded retry); a negative value is treated as {@code 0}. Set it from a {@code
     * ContinuousProjectionRunner.SubscriptionFactory}, threading the {@code readPoisonBound} the
     * factory is handed, so a deterministic live read poison HALTs the projection instead of
     * freezing the read model while health reports UP.
     */
    public Builder readPoisonBound(int readPoisonBound) {
      this.readPoisonBound = readPoisonBound;
      return this;
    }

    /**
     * Overrides the LISTEN connection's liveness probe: the silence after which it is probed and
     * how long the probe may wait for its answer. Package-private: production uses 10 s and 5 s.
     */
    Builder livenessProbe(long intervalMs, long timeoutMs) {
      this.livenessProbeIntervalMs = intervalMs;
      this.livenessProbeTimeoutMs = timeoutMs;
      return this;
    }

    /**
     * Builds the subscription; it does not start it.
     *
     * @throws IllegalArgumentException if a required value is missing or {@code fetchSize} is not
     *     positive
     */
    public PostgresNotificationSubscription build() {
      return new PostgresNotificationSubscription(this);
    }
  }

  /**
   * Starts the listen loop.
   *
   * <p><b>Start guard.</b> The guard used to be {@code running.compareAndSet(false, true)} alone,
   * which succeeded the instant {@code close()} flipped the flag — while the previous listen thread
   * was still inside {@code pollAndDeliver()}, whose delivery close() does not interrupt. Two
   * listen threads then shared one checkpoint: both read the same {@code lastOffset}, both
   * delivered the same batch to the same listener, and both saved. It also broke this class's own
   * single-thread assumption for the read-poison streak fields, which are plain (non-volatile)
   * precisely because "pollAndDeliver runs solely on the single listen thread". {@code close()} is
   * terminal here, matching {@link org.streamrune.runtime.PollingEventSubscription} and {@code
   * StreamEventSubscription} — and doubly so, because it closes the dedicated LISTEN pool, which no
   * restart could reopen.
   *
   * @throws IllegalStateException if the subscription is already running, has been closed, or its
   *     previous listen thread is still winding down
   */
  @Override
  public void start() {
    if (closed.get()) {
      throw new IllegalStateException("Subscription is closed and cannot be restarted");
    }
    Thread previous = listenerThread;
    if (previous != null && previous.isAlive()) {
      throw new IllegalStateException(
          "Previous listen thread is still running — refusing to start a second listener on the"
              + " same checkpoint");
    }
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Subscription already running");
    }
    listenerThread =
        Thread.ofVirtual()
            .name("streamrune-listen-" + subscriptionName.value())
            .start(this::listenLoop);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Stops the subscription, waits for the in-flight delivery to finish, then closes the dedicated
   * LISTEN pool.
   *
   * <p><b>Why the join.</b> The interrupt does not stop {@code pollAndDeliver()} at once: the
   * thread may be inside {@code readGlobalStream} / {@code listener.onEvents} / {@code saveOffset}
   * on the SHARED event DataSource, and the delivery and its checkpoint save run with the interrupt
   * held back (on a virtual thread an interrupt fails every blocking socket call made while the
   * flag is set, so the listener's own store writes and the checkpoint save would fail and the page
   * be delivered again). The interrupt reaches the thread once the page is done. Without the join,
   * {@code close()} returned while that drain was still running, so it kept delivering — and saved
   * its checkpoint — after the caller had moved on to closing that shared DataSource. Joining
   * before tearing down the LISTEN pool also means the listen loop is never handed a pool that
   * closed underneath it. Same rationale, same shape as {@code PollingEventSubscription.close()}
   * and {@code PgNotificationListener.close()}.
   *
   * <p>Waits up to {@value #CLOSE_JOIN_TIMEOUT_MS} ms; if the thread is still alive after that a
   * warning is logged and {@code close()} returns anyway, but the subscription stays permanently
   * non-restartable so a {@link #start()} cannot put a second listener on the checkpoint next to
   * the stuck one. Calling {@code close()} from the listener itself only signals — joining the
   * current thread would deadlock. Idempotent.
   */
  @Override
  public void close() {
    closed.set(true);
    running.set(false);
    Thread t = listenerThread;
    if (t != null) {
      interruptListenThread(t);
      if (t != Thread.currentThread()) {
        try {
          t.join(CLOSE_JOIN_TIMEOUT_MS);
          if (t.isAlive()) {
            log.warn(
                "Subscription '{}' listen thread did not stop within {} ms after close() — the"
                    + " in-flight batch is still processing and may save its offset after this"
                    + " call",
                subscriptionName.value(),
                CLOSE_JOIN_TIMEOUT_MS);
          }
        } catch (InterruptedException _) {
          Thread.currentThread().interrupt();
        }
      }
    }
    if (listenDataSource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        log.warn(
            "Subscription '{}' could not close its LISTEN DataSource", subscriptionName.value(), e);
      }
    }
  }

  private void listenLoop() {
    try {
      while (running.get()) {
        try (Connection conn = listenDataSource.getConnection()) {
          connected.set(true);
          probe.connected();
          try (var stmt = conn.createStatement()) {
            stmt.execute("LISTEN " + CHANNEL);
          }
          if (!conn.getAutoCommit()) {
            // LISTEN takes effect at commit. On an autoCommit=false connection the statement
            // above opened a transaction that nothing else in this loop would end, so the backend
            // would never register the channel.
            conn.commit();
          }

          // Catch-up poll: events appended before start() or while the connection was down
          // produced NOTIFYs this subscription never saw. Poll once now that LISTEN is active
          // so they are delivered without waiting for the next notification.
          pollAndDeliver();
          long nextPollDueAt = System.nanoTime() + pollFallbackNanos();

          while (running.get()) {
            try {
              // getNotifications blocks for timeoutMs milliseconds, returns null on timeout
              var notifications =
                  conn.unwrap(org.postgresql.PGConnection.class).getNotifications(500);
              if (notifications != null && notifications.length > 0) {
                probe.heard();
                notified.set(true);
              } else {
                // A connection whose server vanished without closing it never fails the wait
                // above; only a probe that has to be answered can expose it.
                probe.probeIfQuiet(conn);
              }

              // Periodic poll fallback. pg_notify is best-effort — a dropped, failed,
              // or coalesced NOTIFY would otherwise never be recovered because there is no other
              // trigger. Poll on the configured interval even when no notification was observed
              // (mirrors HybridEventSubscription's polling path), so a lost NOTIFY is always
              // caught up.
              boolean pollDue = System.nanoTime() - nextPollDueAt >= 0;
              if (notified.get() || pollDue) {
                notified.set(false);
                pollAndDeliver();
                nextPollDueAt = System.nanoTime() + pollFallbackNanos();
              }
            } catch (SQLException e) {
              // Connection lost — reconnect
              connected.set(false);
              log.warn(
                  "Subscription '{}' lost its LISTEN connection; reconnecting",
                  subscriptionName.value(),
                  e);
              break;
            }
          }
        } catch (SQLException | RuntimeException e) {
          // RuntimeException is joined to the SQLException retry. pollAndDeliver()
          // catches its own Exceptions, so a RuntimeException normally surfaces here from the
          // connect phase — the LISTEN pool's lazy initialisation or the driver (Hikari raises
          // PoolInitializationException for a non-SQL cause; a DataSource wrapper can raise
          // anything) — but this catch also takes anything else that escapes the LISTEN loop
          // (the driver while waiting for notifications, closing the connection after a loss).
          // A RuntimeException used to escape this catch and kill the listen thread silently,
          // with `running` still true. A pool that cannot start is as transient as a connection
          // that cannot be established: log and retry, exactly like the SQLException path.
          if (running.get()) {
            log.warn(
                "Subscription '{}' failed to establish LISTEN connection, or its LISTEN loop"
                    + " failed; retrying in 1000 ms",
                subscriptionName.value(),
                e);
            sleep(1000);
          }
        }
      }
    } catch (Throwable t) {
      // Everything above retries RuntimeException/SQLException, so
      // what reaches here is a java.lang.Error — AssertionError, NoClassDefFoundError/LinkageError
      // from a missing optional dependency, StackOverflowError on pathological data,
      // OutOfMemoryError — thrown by the listener, the store or the offset store inside
      // pollAndDeliver() (whose catch is Exception-only, on purpose: an Error is not retryable).
      // Uncaught, it killed this virtual listen thread while `running` stayed true, so
      // isRunning() lied forever: the owning ContinuousProjectionRunner's !isRunning() halt check
      // never fired, the read model silently froze and health stayed UP. Mirror
      // PollingEventSubscription.runPollLoop: STOP terminally and report — flip `running` FIRST so
      // the truthful state does not depend on the log call succeeding (an OutOfMemoryError may
      // fail it), then log with the subscription name; the owning runner observes
      // isRunning()==false and surfaces a terminal ERROR (health DOWN). Deliberately not rethrown:
      // stop-and-report is the intended handling, and the log line preserves the error.
      running.set(false);
      connected.set(false);
      log.error(
          "Subscription '{}' listen thread terminated on an unrecoverable error — stopping it"
              + " terminally; the owning projection runner will observe isRunning()==false and"
              + " surface a terminal ERROR (health DOWN)",
          subscriptionName.value(),
          t);
    }
  }

  private void pollAndDeliver() {
    ProjectionName name = ProjectionName.of(subscriptionName.value());
    try {
      // DRAIN to the head, not a single page. A single append fires exactly ONE
      // pg_notify regardless of how many events it wrote (and NOTIFYs coalesce), so reading only
      // one fetchSize page per wake-up strands every event beyond the first page until some LATER
      // append happens to notify again — which may never come. Loop until a read genuinely returns
      // nothing: each page advances the offset, so re-reading from beyond it either yields the next
      // page (a >fetchSize burst) or, when the contiguity guard truncated at a permanent hole, the
      // events past the hole — draining the whole backlog on this one wake-up. The uncommitted
      // in-flight tail append never appears, so the loop terminates instead of spinning.
      while (running.get()) {
        GlobalOffset lastOffset = offsetStore.getLastOffset(name);
        List<EventEnvelope> events;
        try {
          events = eventStore.readGlobalStream(lastOffset, fetchSize);
        } catch (RuntimeException readError) {
          if (closed.get()) {
            // close() interrupted the read; nothing was delivered from it.
            log.debug(
                "Subscription '{}' read ended by close()", subscriptionName.value(), readError);
            return;
          }
          // Only the READ is a read-poison candidate. A listener/delivery failure below
          // is a processing failure the owning runner's error strategy handles, never bounded here.
          if (recordReadFailure(lastOffset, readError)) {
            return; // stopped terminally; the error is already logged with its remedy
          }
          throw readError;
        }
        // A successful read clears any read-poison streak.
        resetReadPoison();
        if (events.isEmpty()) {
          break;
        }
        deliver(name, events);
      }
    } catch (Exception e) {
      // Don't let delivery errors stop the listener loop — but never swallow them silently. The
      // periodic poll fallback (and the next notification) re-attempts from the last saved offset.
      log.error(
          "Subscription '{}' failed to poll/deliver events; will retry on the next"
              + " poll/notification",
          subscriptionName.value(),
          e);
    }
  }

  /**
   * Delivers one page and saves its checkpoint with close()'s interrupt held back: on a virtual
   * thread an interrupt fails every blocking socket call made while the flag is set, so the
   * listener's own store writes (a projection runner commits its batch here) and the checkpoint
   * save would fail and the page be delivered again. The interrupt is delivered when the page is
   * done; close() waits for it. Reads stay interruptible.
   */
  private void deliver(ProjectionName name, List<EventEnvelope> events) {
    boolean interrupted;
    interruptLock.lock();
    try {
      delivering = true;
      interrupted = Thread.interrupted();
    } finally {
      interruptLock.unlock();
    }
    try {
      listener.onEvents(events);
      offsetStore.saveOffset(name, events.getLast().globalOffset());
    } finally {
      interruptLock.lock();
      try {
        delivering = false;
        if (interrupted || stopRequested) {
          Thread.currentThread().interrupt();
        }
      } finally {
        interruptLock.unlock();
      }
    }
  }

  /** Interrupts the listen thread now, or, while it is delivering a page, when that ends. */
  private void interruptListenThread(Thread t) {
    interruptLock.lock();
    try {
      stopRequested = true;
      if (!delivering) {
        t.interrupt();
      }
    } finally {
      interruptLock.unlock();
    }
  }

  /**
   * Records one failed READ against the read-poison bound. Returns {@code true} when the bound is
   * exhausted and this subscription has been stopped terminally (so the caller must return rather
   * than rethrow), {@code false} when the failure is transient, under the bound, or the bound is
   * disabled — in which case the previous unbounded-retry behaviour applies and the next NOTIFY /
   * fallback tick re-attempts from the same checkpoint.
   *
   * <p>The streak is keyed on the CHECKPOINT: a poison event is only proven poison when the same
   * offset fails deterministically over and over. Any checkpoint movement, and any read that is not
   * provably deterministic ({@link ReadPoisonClassifier}), resets it — so a DB outage never
   * permanently kills the subscription.
   */
  private boolean recordReadFailure(GlobalOffset checkpoint, RuntimeException readError) {
    if (readPoisonBound <= 0 || !ReadPoisonClassifier.isDeterministicReadPoison(readError)) {
      resetReadPoison();
      return false;
    }
    if (!checkpoint.equals(readPoisonCheckpoint)) {
      readPoisonCheckpoint = checkpoint;
      readPoisonFailures = 0;
    }
    readPoisonFailures++;
    if (readPoisonFailures < readPoisonBound) {
      return false;
    }
    var poison =
        new ProjectionReadPoisonException(subscriptionName.value(), readPoisonFailures, readError);
    readPoison.set(poison);
    // Stop the LISTEN loop. isRunning() now reports false, which is what the owning
    // ContinuousProjectionRunner polls for; it then reads readPoisonError() and HALTs the
    // projection with a terminal ERROR and health DOWN instead of leaving it frozen and UP.
    running.set(false);
    log.error(
        "Subscription '{}' stopping terminally: a read/deserialization poison failed"
            + " deterministically {} time(s) at checkpoint {} and can never be read on retry",
        subscriptionName.value(),
        readPoisonFailures,
        checkpoint.value(),
        poison);
    return true;
  }

  private void resetReadPoison() {
    if (readPoisonBound > 0) {
      readPoisonCheckpoint = null;
      readPoisonFailures = 0;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Non-null only after the read-poison bound stopped this subscription.
   */
  @Override
  public ProjectionReadPoisonException readPoisonError() {
    return readPoison.get();
  }

  /**
   * Call this method after new events are appended to the event store. This triggers an immediate
   * poll and delivery for this subscription without waiting for a PostgreSQL notification.
   *
   * <p>Usage: call from {@link org.streamrune.core.EventStore} wrapper or from the append path.
   */
  public void notifyNewEvents() {
    notified.set(true);
  }

  /**
   * Interval between periodic fallback polls, in nanoseconds, taken from {@link
   * SubscriptionConfig#pollingInterval()}. The fallback covers a lost/coalesced NOTIFY; a real
   * NOTIFY still delivers immediately, so this only bounds the worst-case catch-up latency.
   */
  private long pollFallbackNanos() {
    return config.pollingInterval().toNanos();
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Creates a dedicated single-connection HikariDataSource for LISTEN. This is separate from the
   * main connection pool because LISTEN requires a long-lived, dedicated connection. The pool makes
   * no connection of its own on construction: the listen loop that borrows from it owns the first
   * connect and every reconnect, so a server that is unreachable at that moment is a retried
   * failure there, not a failed construction here.
   *
   * <p>For a HikariCP {@code dataSource} the LISTEN pool inherits its JDBC URL — or, for a pool
   * configured by data source class, its {@code dataSourceClassName} — its credentials and its
   * driver properties ({@code dataSourceProperties}: TLS settings, application name, timeouts), so
   * the LISTEN connection reaches the database the way every other connection does. It also sets
   * the driver property {@code tcpKeepAlive=true}, unless the main pool sets {@code tcpKeepAlive}
   * itself, so the operating system probes the peer of a connection that is idle most of the time.
   * Fast detection of a peer that vanished does not depend on it: the listen loop probes the
   * connection itself.
   *
   * <p>Any other {@code dataSource} must expose {@code getJdbcUrl()}, {@code getUrl()} or {@code
   * getURL()}; its {@code getUsername()} / {@code getUser()} and {@code getPassword()} are read
   * when present. Agroal on Quarkus and DataSource proxies expose none of the URL getters, so this
   * method refuses them: build the LISTEN DataSource yourself and pass it to {@link
   * Builder#listenDataSource(DataSource)}.
   *
   * <p>The returned DataSource should be closed when the subscription is closed.
   *
   * @param dataSource the original DataSource (used to extract JDBC URL)
   * @return a dedicated HikariDataSource with maxPoolSize=1 for LISTEN
   * @throws IllegalArgumentException if no JDBC URL can be read off {@code dataSource}
   */
  public static HikariDataSource createListenDataSource(DataSource dataSource) {
    String jdbcUrl;
    String dataSourceClassName = null;
    String username = null;
    String password = null;
    var driverProperties = new Properties();

    if (dataSource instanceof HikariDataSource hds) {
      jdbcUrl = hds.getJdbcUrl();
      if (jdbcUrl == null) {
        dataSourceClassName = hds.getDataSourceClassName();
      }
      username = hds.getUsername();
      password = hds.getPassword();
      driverProperties.putAll(hds.getDataSourceProperties());
    } else {
      jdbcUrl = firstString(dataSource, "getJdbcUrl", "getUrl", "getURL");
      if (jdbcUrl == null) {
        throw new IllegalArgumentException(
            "Cannot derive a JDBC URL from "
                + dataSource.getClass().getName()
                + " to open the dedicated LISTEN connection: it is not a HikariDataSource and"
                + " exposes no getJdbcUrl()/getUrl()/getURL() (Agroal on Quarkus and DataSource"
                + " proxies do not). Build a LISTEN DataSource from the same settings and pass it"
                + " to PostgresNotificationSubscription.Builder.listenDataSource(DataSource).");
      }
      // Credentials may also be embedded in the JDBC URL or supplied by the driver.
      username = firstString(dataSource, "getUsername", "getUser");
      password = firstString(dataSource, "getPassword");
    }

    var config = new HikariConfig();
    config.setJdbcUrl(jdbcUrl);
    if (dataSourceClassName != null) config.setDataSourceClassName(dataSourceClassName);
    config.setDataSourceProperties(driverProperties);
    // The PostgreSQL driver understands tcpKeepAlive; another data source class may have no such
    // property and would reject it.
    boolean viaPostgresDriver =
        jdbcUrl != null
            || (dataSourceClassName != null && dataSourceClassName.startsWith("org.postgresql."));
    if (viaPostgresDriver && !driverProperties.containsKey("tcpKeepAlive")) {
      config.addDataSourceProperty("tcpKeepAlive", "true");
    }
    if (username != null) config.setUsername(username);
    if (password != null) config.setPassword(password);
    config.setMaximumPoolSize(1);
    config.setMinimumIdle(1);
    config.setPoolName("streamrune-listen");
    config.setConnectionTimeout(5000);
    config.setIdleTimeout(0); // Never idle-close the LISTEN connection
    config.setMaxLifetime(0); // Never recycle the LISTEN connection
    // The first connection is made by the listen loop, not here. With the default the pool
    // constructor connects once and throws on failure, so a transient connect failure while the
    // pool is being created ended the push path for good, while the loop's reconnect-with-backoff
    // handles the very same failure a moment later.
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }

  /** The first non-null String returned by a public no-arg getter of {@code ds}, or null. */
  private static String firstString(DataSource ds, String... getters) {
    for (String getter : getters) {
      try {
        Object value = ds.getClass().getMethod(getter).invoke(ds);
        if (value instanceof String text) {
          return text;
        }
      } catch (ReflectiveOperationException | RuntimeException _) {
        // No such getter on this DataSource; try the next one.
      }
    }
    return null;
  }
}

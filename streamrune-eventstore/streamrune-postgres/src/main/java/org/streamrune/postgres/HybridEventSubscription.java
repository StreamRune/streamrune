package org.streamrune.postgres;

import java.util.Objects;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.LeaseRoleAware;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.ProjectionReadPoisonException;
import org.streamrune.runtime.PushHealthAware;
import org.streamrune.runtime.ReadPoisonAware;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.runtime.SubscriptionLease;

/**
 * Hybrid event subscription combining PostgreSQL LISTEN/NOTIFY for low-latency push with periodic
 * polling for guaranteed delivery.
 *
 * <p>Architecture:
 *
 * <ol>
 *   <li>PgNotificationListener receives NOTIFY → triggers immediate poll (only when RUNNING)
 *   <li>Periodic polling on virtual thread catches missed notifications
 *   <li>Both paths converge on the same poll-and-deliver logic
 * </ol>
 *
 * <p>Implements {@link SubscriptionLifecycle}: {@link #pause()} suspends both polling and
 * notification-triggered polls. {@link #resume()} restores delivery from the position at which
 * delivery was paused.
 *
 * <p>Lifecycle state is delegated entirely to the internal {@link PollingEventSubscription}.
 *
 * <p>Offset durability: the subscription name is the key under which the delivery position is
 * checkpointed in the {@link OffsetStore}. It must be stable across restarts — a subscription
 * restarted with the same name resumes after the last checkpointed offset. Changing the name
 * re-delivers the full event history under the new key and leaves the old checkpoint row behind.
 *
 * <p>This is the recommended subscription for production use with PostgreSQL.
 */
public final class HybridEventSubscription
    implements SubscriptionLifecycle, ReadPoisonAware, PushHealthAware, LeaseRoleAware {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HybridEventSubscription.class);

  private final SubscriptionName subscriptionName;
  private final PollingEventSubscription pollingSubscription;
  private final PgNotificationListener notificationListener;

  // The contributor this subscription registers itself with on start() and leaves on close(); null
  // registers nothing. The Hybrid registers itself, not the polling subscription inside it, so the
  // contributor reads the push-path health from the Hybrid.
  private final SubscriptionHealthContributor healthContributor;

  public HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize) {
    this(
        subscriptionName,
        dataSource,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        StreamRuneMetrics.NOOP);
  }

  /**
   * Constructor with metrics but no read-poison bound (unbounded live retry). The {@code metrics}
   * collector is wired into the LISTEN/NOTIFY listener so reconnects emit {@link
   * org.streamrune.core.metrics.MetricNames#SUBSCRIPTIONS_LISTENER_RECONNECTS} ; {@code null} means
   * {@link StreamRuneMetrics#NOOP} (behavior unchanged).
   */
  public HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics) {
    this(
        subscriptionName,
        dataSource,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        metrics,
        0);
  }

  /**
   * Full constructor. {@code readPoisonBound} bounds a deterministic read/deserialization poison on
   * the live read path: after that many consecutive poison reads at the same checkpoint the
   * (internal polling) subscription stops terminally so the owning {@link
   * org.streamrune.runtime.ContinuousProjectionRunner} HALTs the projection (surfacing the poison
   * via {@link #readPoisonError()}) instead of retrying an unreadable event forever. {@code 0}
   * disables the bound (unbounded retry, the standalone default). The projection runner threads its
   * configured {@code maxReadPoisonRetries} here through the {@code SubscriptionFactory} so the
   * documented factory/Hybrid production path is poison-bounded exactly like the default-polling
   * path.
   */
  public HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      int readPoisonBound) {
    this(
        subscriptionName,
        dataSource,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        metrics,
        readPoisonBound,
        UnaryOperator.identity());
  }

  /**
   * The full constructor plus {@code listenerTuning}, applied to the push-path listener's builder
   * before it is built, so tests can shorten its backoff, liveness probe and reconnect grace.
   */
  HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      int readPoisonBound,
      UnaryOperator<PgNotificationListener.Builder> listenerTuning) {
    this(
        subscriptionName,
        dataSource,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        metrics,
        readPoisonBound,
        listenerTuning,
        SubscriptionLease.ungated());
  }

  /**
   * The constructor with the push-path tuning plus the {@code lease} the polling subscription
   * inside delivers under; the builder resolves it from {@link Builder#lease}, {@link
   * Builder#leadership} or {@link Builder#sagaRunner}. A notification then reaches the same gate as
   * a poll. Registers nothing with a health contributor.
   */
  HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      int readPoisonBound,
      UnaryOperator<PgNotificationListener.Builder> listenerTuning,
      SubscriptionLease lease) {
    this(
        subscriptionName,
        dataSource,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        metrics,
        readPoisonBound,
        listenerTuning,
        lease,
        null);
  }

  /**
   * The full constructor plus the {@code healthContributor} this subscription registers itself with
   * on {@link #start()} and leaves on {@link #close()} ({@code null} = none).
   */
  HybridEventSubscription(
      SubscriptionName subscriptionName,
      DataSource dataSource,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      int readPoisonBound,
      UnaryOperator<PgNotificationListener.Builder> listenerTuning,
      SubscriptionLease lease,
      SubscriptionHealthContributor healthContributor) {

    if (subscriptionName == null) {
      throw new IllegalArgumentException("subscriptionName is required");
    }
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (eventStore == null) {
      throw new IllegalArgumentException("eventStore is required");
    }
    if (offsetStore == null) {
      throw new IllegalArgumentException("offsetStore is required");
    }
    if (listener == null) {
      throw new IllegalArgumentException("listener is required");
    }
    if (config == null) {
      throw new IllegalArgumentException("config is required");
    }

    this.subscriptionName = subscriptionName;
    this.healthContributor = healthContributor;
    this.pollingSubscription =
        PollingEventSubscription.builder()
            .subscriptionName(subscriptionName.value())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(listener)
            .config(config)
            .fetchSize(fetchSize)
            // Thread the live read-poison bound into the delivery-owning polling
            // subscription so a live read-poison stops it terminally (surfaced via
            // readPoisonError()) instead of the resilient poll loop retrying an unreadable event
            // forever.
            .readPoisonBound(readPoisonBound)
            .lease(lease)
            .build();

    // Honor SubscriptionConfig.listenNotifyEnabled(). When LISTEN/NOTIFY is disabled
    // (SubscriptionConfig.pollingOnly(...), the documented PgBouncer/transaction-pooler mitigation)
    // do NOT build the PgNotificationListener at all — otherwise it thrashes its unbounded
    // reconnect loop against a pooler that cannot hold a stable LISTEN session, creating a
    // dedicated single-connection pool and spamming WARN logs + the reconnect meter while the
    // operator believes push is off. A null listener means pure polling; delivery is unaffected.
    this.notificationListener =
        config.listenNotifyEnabled()
            ? listenerTuning
                .apply(
                    PgNotificationListener.builder()
                        .dataSource(dataSource)
                        .onNotification(this::onNotificationReceived)
                        // Default UNBOUNDED_RECONNECTS: the push path reconnects forever so a
                        // transient PG failover never permanently kills it. Reconnects emit the
                        // reconnect meter.
                        .metrics(Objects.requireNonNullElse(metrics, StreamRuneMetrics.NOOP)))
                .build()
            : null;
  }

  /**
   * The LISTEN/NOTIFY push-path callback: triggers an immediate catch-up poll when a NOTIFY
   * arrives, but only when RUNNING (not paused/stopped — the notification callback guard).
   * Package-private so a unit test can drive the notify path directly.
   */
  void onNotificationReceived() {
    if (state() != SubscriptionLifecycleState.RUNNING) {
      return;
    }
    try {
      pollingSubscription.triggerImmediatePoll();
    } catch (RuntimeException e) {
      // The polling path owns retry/backoff and benign-signal handling
      // (ResilientPollLoop), so a routine exception on the notify path — a DLQ-TRANSIENT/HALT
      // rethrow, a lost-leadership signal, a dead-letter-write blip — must be swallowed here. If it
      // escaped, it would reach PgNotificationListener's listen loop, which treats a callback
      // failure as a LISTEN-connection failure: it closes the perfectly healthy LISTEN connection,
      // bumps the reconnect meter (the operator's push-flapping signal), and backs off — so a
      // routine failover would masquerade as push-path loss. No data loss: the offset is not
      // advanced and the polling loop still delivers and retries. A read-poison HALT still stops
      // delivery via the polling path (onReadFailure flips the lifecycle to STOPPED, which the
      // owning runner observes), not via this thread's exception. DEBUG — this is a benign re-poll
      // blip on the low-latency push path.
      logger.debug(
          "Notification-triggered immediate poll for subscription '{}' failed ({}) — the polling"
              + " path will retry with backoff; LISTEN connection left intact",
          subscriptionName.value(),
          e.toString());
    }
  }

  @Override
  public void start() {
    // State machine is owned by pollingSubscription (CREATED → RUNNING CAS happens inside).
    // A null listener means LISTEN/NOTIFY is disabled (pollingOnly) — start polling
    // only.
    if (notificationListener != null) {
      notificationListener.start();
    }
    try {
      pollingSubscription.start();
    } catch (Exception e) {
      if (notificationListener != null) {
        notificationListener.close(); // roll back listener if subscription start fails
      }
      throw e;
    }
    if (healthContributor != null) {
      healthContributor.register(subscriptionName.value(), this);
    }
  }

  @Override
  public void pause() {
    // Delegates state CAS (RUNNING → PAUSED) to pollingSubscription.
    // Throws IllegalStateException if not RUNNING.
    // Notification callback guard (state() == RUNNING) stops forwarding automatically.
    pollingSubscription.pause();
  }

  @Override
  public void resume() {
    // Delegates state CAS (PAUSED → RUNNING) to pollingSubscription.
    // Throws IllegalStateException if not PAUSED.
    pollingSubscription.resume();
  }

  /** The role of the polling subscription inside: its lease's role. */
  @Override
  public LeaseRole leaseRole() {
    return pollingSubscription.leaseRole();
  }

  @Override
  public SubscriptionLifecycleState state() {
    return pollingSubscription.state();
  }

  @Override
  public boolean isRunning() {
    // Delivery liveness is the polling subscription: polling alone guarantees at-least-once
    // delivery even if the LISTEN/NOTIFY push path is temporarily down (it only reduces latency).
    // Tying this to the listener would tear down a still-delivering subscription on a transient
    // push blip. Use {@link #pushHealthy()} to observe the push path independently.
    return pollingSubscription.isRunning();
  }

  /**
   * Returns {@code true} while the LISTEN/NOTIFY push path is live, exactly when {@link
   * #pushPathStatus()} is {@link PushPathStatus#LIVE}: the listener holds a LISTEN connection, or
   * has been without one for less than its 30 s reconnect grace. The listener reconnects with
   * capped backoff and, by default, never permanently gives up, so a routine reconnect does not
   * flip this; a connection that cannot be re-established within the grace does, until it is.
   * Delivery continues via polling regardless; a {@code false} here means notifications are no
   * longer lowering delivery latency and warrants attention.
   */
  public boolean pushHealthy() {
    // With LISTEN/NOTIFY disabled (pollingOnly) there is no push path — report false
    // (not active). This is expected for a pollingOnly subscription, not a fault; delivery runs via
    // polling regardless, so isRunning() (not this) is the delivery-liveness signal.
    return pushPathStatus() == PushPathStatus.LIVE;
  }

  /** The push-path listener, or {@code null} when LISTEN/NOTIFY is disabled; for tests. */
  PgNotificationListener notificationListener() {
    return notificationListener;
  }

  /**
   * The tri-state {@link PushHealthAware} view {@link
   * org.streamrune.runtime.SubscriptionHealthContributor} folds into subscription health, so a dead
   * push path is finally observable at {@code /health} instead of being invisible to every metric.
   *
   * <p>Distinguishing {@link PushPathStatus#NOT_CONFIGURED} from {@link PushPathStatus#DEAD} is the
   * whole point of the tri-state: {@link #pushHealthy()} collapses both to {@code false}, and
   * reporting a {@code pollingOnly} deployment as degraded forever would train operators to ignore
   * the signal.
   *
   * <p>{@link PushPathStatus#LIVE} means the listener holds a LISTEN connection — one whose server
   * vanished silently is caught by the listener's liveness probe within about 15 s — or has been
   * without one for less than its 30 s reconnect grace (the first connect after {@link #start()}
   * included). {@link PushPathStatus#DEAD} means the listener stopped, or has been without a LISTEN
   * connection for longer than the grace; it keeps reconnecting and the status returns to {@code
   * LIVE} once it has one again.
   */
  @Override
  public PushPathStatus pushPathStatus() {
    if (notificationListener == null) {
      return PushPathStatus.NOT_CONFIGURED;
    }
    return notificationListener.isPushPathLive() ? PushPathStatus.LIVE : PushPathStatus.DEAD;
  }

  /**
   * The terminal read-poison error that stopped delivery, or {@code null} if still running or
   * stopped for another reason. Delegated to the internal {@link PollingEventSubscription} — the
   * delivery-owning path that bounds live read-poison. Read by {@link
   * org.streamrune.runtime.ContinuousProjectionRunner} (via {@link ReadPoisonAware}) after it
   * observes {@code isRunning() == false}, so a Hybrid live read-poison surfaces as the distinct
   * read-poison HALT rather than a generic "subscription died".
   */
  @Override
  public ProjectionReadPoisonException readPoisonError() {
    return pollingSubscription.readPoisonError();
  }

  @Override
  public void close() {
    pollingSubscription.close();
    // Null when LISTEN/NOTIFY is disabled (pollingOnly) — nothing to close.
    if (notificationListener != null) {
      notificationListener.close();
    }
    if (healthContributor != null) {
      healthContributor.unregister(subscriptionName.value());
    }
  }

  // Builder
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private SubscriptionName subscriptionName;
    private DataSource dataSource;
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private EventListener listener;
    private SubscriptionConfig config = SubscriptionConfig.DEFAULT;
    private int fetchSize = 100;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private int readPoisonBound = 0;
    private SubscriptionLease lease;
    private SubscriptionLeadership leadership;
    private SagaRunner<?> sagaRunner;
    private SubscriptionHealthContributor healthContributor;

    /**
     * Sets the stable subscription name (required). The name is the durable offset key in the
     * {@link OffsetStore} — reuse the same name across restarts to resume from the last
     * checkpointed position. Changing the name re-delivers the full event history.
     */
    public Builder subscriptionName(String name) {
      this.subscriptionName = SubscriptionName.of(name);
      return this;
    }

    /** Sets the JDBC data source for LISTEN/NOTIFY (required). */
    public Builder dataSource(DataSource ds) {
      this.dataSource = ds;
      return this;
    }

    /** Sets the event store to poll (required). */
    public Builder eventStore(EventStore store) {
      this.eventStore = store;
      return this;
    }

    /** Sets the offset store for durable position tracking (required). */
    public Builder offsetStore(OffsetStore store) {
      this.offsetStore = store;
      return this;
    }

    /** Sets the event listener callback (required). */
    public Builder listener(EventListener listener) {
      this.listener = listener;
      return this;
    }

    /** Sets the subscription configuration (default: {@link SubscriptionConfig#DEFAULT}). */
    public Builder config(SubscriptionConfig config) {
      this.config = config;
      return this;
    }

    /** Sets the maximum number of events per poll cycle (default: 100). */
    public Builder fetchSize(int size) {
      this.fetchSize = size;
      return this;
    }

    /**
     * Sets the metrics collector wired into the LISTEN/NOTIFY listener so reconnects emit the
     * reconnect meter. Optional; defaults to {@link StreamRuneMetrics#NOOP} (behavior unchanged).
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
      return this;
    }

    /**
     * Bounds deterministic read/deserialization poison on the live read path: after {@code bound}
     * consecutive poison reads at the same checkpoint the subscription stops terminally so the
     * owning {@link org.streamrune.runtime.ContinuousProjectionRunner} HALTs (health DOWN) instead
     * of retrying an unreadable event forever. Default {@code 0} disables the bound (unbounded
     * retry). The projection runner threads its {@code maxReadPoisonRetries} here via the {@code
     * SubscriptionFactory}; set it directly only when wiring a standalone Hybrid subscription.
     */
    public Builder readPoisonBound(int bound) {
      this.readPoisonBound = bound;
      return this;
    }

    /**
     * The lease this subscription delivers under: it delivers only while this process holds it, and
     * anchors it from {@code start()} to its close. Optional; without a lease, a leadership or a
     * saga runner the subscription delivers on every replica that runs it. Not together with {@link
     * #leadership} or {@link #sagaRunner}.
     */
    public Builder lease(SubscriptionLease lease) {
      this.lease = lease;
      return this;
    }

    /**
     * Delivers under the lease named after this subscription over {@code leadership}: the same as
     * {@code lease(SubscriptionLease.of(leadership, subscriptionName))}. One replica then delivers
     * this subscription's events; the others stand by and take over when it stops or dies. Not
     * together with {@link #lease} or {@link #sagaRunner}.
     */
    public Builder leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership;
      return this;
    }

    /**
     * Delivers a saga type's events to {@code sagaRunner}: the same as {@code
     * listener(sagaRunner.asEventListener())} together with {@code lease(sagaRunner.lease())}. This
     * subscription is then the anchor of the saga type's lease in this process, and the runner's
     * timeout runner and compensation-retry sweeper act while it leads. Not together with {@link
     * #listener}, {@link #lease} or {@link #leadership}.
     */
    public Builder sagaRunner(SagaRunner<?> sagaRunner) {
      this.sagaRunner = sagaRunner;
      return this;
    }

    /**
     * Registers this subscription with {@code contributor} under its name on {@link #start()} and
     * unregisters it on {@link #close()}, so health reports its state, lag, errors, push path and
     * lease role. Optional; {@code null} registers nothing. The integrations cannot find a
     * subscription the application builds itself.
     */
    public Builder healthContributor(SubscriptionHealthContributor contributor) {
      this.healthContributor = contributor;
      return this;
    }

    /**
     * Builds the {@link HybridEventSubscription}. Requires {@code subscriptionName}, {@code
     * dataSource}, {@code eventStore}, {@code offsetStore}, and {@code listener} or {@code
     * sagaRunner}. A leased subscription's standby attempts are spaced by the polling interval of
     * {@link #config} even when notifications wake it: keep that fallback interval short.
     *
     * @throws IllegalStateException if {@code subscriptionName} was not set or is blank
     * @throws IllegalArgumentException when two of the settings name one thing: {@code sagaRunner}
     *     with {@code listener}, {@code lease} or {@code leadership}; {@code lease} with {@code
     *     leadership}
     */
    public HybridEventSubscription build() {
      if (subscriptionName == null) {
        throw new IllegalStateException(
            "subscriptionName is required: set Builder.subscriptionName(...) to a stable name —"
                + " it is the durable offset key for this subscription");
      }
      refuseTwoSources("HybridEventSubscription.Builder");
      return new HybridEventSubscription(
          subscriptionName,
          dataSource,
          eventStore,
          offsetStore,
          sagaRunner != null ? sagaRunner.asEventListener() : listener,
          config,
          fetchSize,
          metrics,
          readPoisonBound,
          UnaryOperator.identity(),
          resolvedLease(),
          healthContributor);
    }

    /** Refuses one setting named twice: a saga runner sets the listener and the lease. */
    private void refuseTwoSources(String prefix) {
      if (sagaRunner != null && listener != null) {
        throw new IllegalArgumentException(
            prefix
                + ": sagaRunner(...) and listener(...) are both set — sagaRunner(runner) delivers"
                + " to runner.asEventListener(); name one of them");
      }
      if (sagaRunner != null && lease != null) {
        throw new IllegalArgumentException(
            prefix
                + ": sagaRunner(...) and lease(...) are both set — sagaRunner(runner) takes"
                + " runner.lease(); name one of them");
      }
      if (sagaRunner != null && leadership != null) {
        throw new IllegalArgumentException(
            prefix
                + ": sagaRunner(...) and leadership(...) are both set — sagaRunner(runner) takes"
                + " runner.lease(); name one of them");
      }
      if (lease != null && leadership != null) {
        throw new IllegalArgumentException(
            prefix
                + ": lease(...) and leadership(...) are both set — leadership(l) is"
                + " lease(SubscriptionLease.of(l, subscriptionName)); name one of them");
      }
    }

    /** The saga runner's lease, else the one named, else one over the leadership, else none. */
    private SubscriptionLease resolvedLease() {
      if (sagaRunner != null) {
        return sagaRunner.lease();
      }
      if (lease != null) {
        return lease;
      }
      if (leadership != null && subscriptionName != null) {
        return SubscriptionLease.of(leadership, subscriptionName.value());
      }
      return SubscriptionLease.ungated();
    }
  }
}

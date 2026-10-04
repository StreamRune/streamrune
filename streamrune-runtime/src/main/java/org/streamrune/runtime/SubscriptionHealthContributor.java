package org.streamrune.runtime;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * Tracks the health of registered subscriptions — lifecycle state, lag and consecutive delivery
 * errors — computed on demand when a health endpoint is read.
 *
 * <p>Each subscription gets one of three statuses:
 *
 * <ul>
 *   <li>{@code DOWN} — the subscription is not processing and will not resume on its own: its
 *       lifecycle is {@code STOPPED}, or its runner marked it terminally halted ({@link
 *       #markTerminalError}). A {@code DOWN} subscription turns the framework health checks {@code
 *       DOWN}, and those are the checks a readiness probe consults.
 *   <li>{@code DEGRADED} — still running, but behind or failing: lag at or above {@link
 *       #lagThreshold()}, one or more consecutive delivery errors, {@code PAUSED}, a dead
 *       LISTEN/NOTIFY push path, or a checkpoint ahead of the event stream head (a database whose
 *       event table was restored to an earlier point than the checkpoint; logged once per episode
 *       at {@code WARN}, with lag reported as 0). It shows in the subscription's own health detail
 *       and never changes the overall check.
 *   <li>{@code UP} — otherwise.
 * </ul>
 *
 * <p>Lag never makes a subscription {@code DOWN}. It is measured against the checkpoint all
 * replicas share, so every replica reports the same value: a new projection replaying the store, a
 * projection reset for a rebuild, or a burst of appends would read {@code DOWN} on every replica at
 * once, and a readiness probe would take the whole fleet out of service until the one active
 * consumer caught up. Alert on the {@code streamrune.subscriptions.lag} gauge instead. Consecutive
 * errors are not {@code DOWN} either: the subscription is still retrying (a dead-lettering or
 * skipping projection is even still advancing), and taking the instance out of service does not
 * help it recover.
 *
 * <p>When a {@link StreamRuneMetrics} is supplied, every {@link #health()} sample also reports each
 * subscription's lag via {@link StreamRuneMetrics#recordSubscriptionLag(SubscriptionName, long)} so
 * the lag is graphable/alertable as a meter, not just observable through the health status. With
 * the default {@link StreamRuneMetrics#NOOP} the behaviour is unchanged.
 */
public final class SubscriptionHealthContributor {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(SubscriptionHealthContributor.class);

  /** The default {@link #lagThreshold()}: 1000 events. */
  public static final long DEFAULT_LAG_THRESHOLD = 1000;

  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final long lagThreshold;
  private final StreamRuneMetrics metrics;

  private final ConcurrentHashMap<String, SubscriptionEntry> subscriptions =
      new ConcurrentHashMap<>();

  /**
   * The last successfully computed lag per subscription, so the live {@code subscriptions.lag}
   * gauge can hold it across a failed store read instead of reporting 0 (the caught-up value).
   * Keyed separately from {@link #subscriptions} because {@link #register} wires the supplier gauge
   * BEFORE publishing the map entry, so the supplier can legitimately fire while no entry exists
   * yet.
   */
  private final ConcurrentHashMap<String, AtomicLong> lastKnownLag = new ConcurrentHashMap<>();

  /** A contributor with the {@link #DEFAULT_LAG_THRESHOLD} and no metrics. */
  public SubscriptionHealthContributor(EventStore eventStore, OffsetStore offsetStore) {
    this(eventStore, offsetStore, DEFAULT_LAG_THRESHOLD, StreamRuneMetrics.NOOP);
  }

  /**
   * A contributor with the {@link #DEFAULT_LAG_THRESHOLD} and an explicit metrics collector, so the
   * integrations can wire the auto-configured {@link StreamRuneMetrics} bean (and {@code
   * streamrune.subscriptions.lag} is emitted). With {@link StreamRuneMetrics#NOOP} the behaviour is
   * identical to {@link #SubscriptionHealthContributor(EventStore, OffsetStore)}.
   */
  public SubscriptionHealthContributor(
      EventStore eventStore, OffsetStore offsetStore, StreamRuneMetrics metrics) {
    this(eventStore, offsetStore, DEFAULT_LAG_THRESHOLD, metrics);
  }

  /** A contributor with the given {@link #lagThreshold()} and no metrics. */
  public SubscriptionHealthContributor(
      EventStore eventStore, OffsetStore offsetStore, long lagThreshold) {
    this(eventStore, offsetStore, lagThreshold, StreamRuneMetrics.NOOP);
  }

  /**
   * @param lagThreshold the lag, in events, at which a running subscription is reported {@code
   *     DEGRADED} — see {@link #lagThreshold()}; at least 1
   * @param metrics the collector that receives the lag gauge; {@code null} means {@link
   *     StreamRuneMetrics#NOOP}
   * @throws IllegalArgumentException if {@code lagThreshold} is below 1
   */
  public SubscriptionHealthContributor(
      EventStore eventStore,
      OffsetStore offsetStore,
      long lagThreshold,
      StreamRuneMetrics metrics) {
    if (lagThreshold < 1) {
      throw new IllegalArgumentException(
          "lagThreshold must be at least 1 event, was " + lagThreshold);
    }
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.lagThreshold = lagThreshold;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
  }

  /**
   * The lag, in events between the global head and a subscription's checkpoint, at or above which a
   * running subscription is reported {@code DEGRADED}. Lag alone never reports {@code DOWN}.
   */
  public long lagThreshold() {
    return lagThreshold;
  }

  public void register(String name, SubscriptionLifecycle subscription) {
    // A fresh registration starts with no held lag, so a restarted/reset projection
    // never inherits the previous incarnation's last known value. Cleared before the gauge is
    // registered, since registration may sample the supplier immediately.
    lastKnownLag.remove(name);
    // Register the live supplier lag gauge BEFORE publishing the map entry. If the
    // map entry went in first, a concurrent health scrape could observe it and call
    // metrics.recordSubscriptionLag(name, ...) — registering a competing PUSH gauge with the same
    // name+tag — before this supplier gauge is registered. Micrometer then dedups by id and
    // DISCARDS the supplier, permanently freezing streamrune.subscriptions.lag for that
    // subscription at its startup sample. Registering the supplier first means
    // the push-path guard (supplierBackedLagGauges.containsKey) always short-circuits, so no
    // competing meter is created. currentLag(name) reads the event store / offset store, not the
    // subscriptions map, and returns 0 defensively on any failure, so it is safe to call before the
    // map entry exists. Idempotent per name (the metrics impl registers the meter once).
    //
    // Log-and-continue on a registry failure — this sits BEFORE subscriptions.put.
    // registerSubscriptionLagGauge performs live registry operations (MicrometerStreamRuneMetrics
    // does registry.remove + Gauge.builder(...).register(registry)) that genuinely throw in
    // production: a PrometheusMeterRegistry rejects a meter whose id collides with an existing
    // collector using different label keys (classic when the app also emits
    // streamrune.subscriptions.lag), a user MeterFilter can throw, and a registry mid-shutdown
    // throws IllegalStateException. Propagating meant NO health entry was ever published for the
    // projection AND — because ContinuousProjectionRunner.run() called this from its standby-view
    // registration — the exception escaped run() entirely: running stuck true, the runDone latch
    // never counted down (close() then blocked its full 30s timeout), and markTerminalError never
    // ran, so /health reported UP over a read model that processed nothing. Health tracking must
    // degrade to "no lag meter", never to "no entry". Catches RuntimeException, matching every
    // other metrics call site in the runners ("metric failures never disrupt projection
    // processing"); an Error still propagates, and run() now registers inside its try so even that
    // hits the finally bookkeeping.
    // This comment used to claim registerSubscriptionLagGauge was "the ONLY unwrapped
    // metrics call in the projection stack" — false even at the time: computeHealth()'s
    // recordSubscriptionLag below was bare too, and is now guarded the same way.
    try {
      metrics.registerSubscriptionLagGauge(SubscriptionName.of(name), () -> currentLag(name));
    } catch (RuntimeException e) {
      log.warn(
          "Metrics registerSubscriptionLagGauge failed for subscription '{}' — the subscription is"
              + " still registered for health, but streamrune.subscriptions.lag will not be"
              + " emitted for it",
          name,
          e);
    }
    // A fresh registration replaces any prior entry, clearing a previous terminal-error marking so
    // a restarted/reset projection starts healthy again.
    subscriptions.put(
        name,
        new SubscriptionEntry(
            subscription,
            new AtomicInteger(0),
            new AtomicBoolean(false),
            new AtomicBoolean(false),
            new AtomicBoolean(false),
            new AtomicBoolean(false)));
  }

  /**
   * Swaps the {@link SubscriptionLifecycle} view backing an existing, non-terminal entry for {@code
   * name}, preserving its accrued {@code errorCount} (and the terminal-error / push- path-warned
   * flags) instead of starting a fresh episode.
   *
   * <p>{@link #register} always starts fresh (errorCount reset to 0) — correct for a genuinely new
   * registration, but wrong for the catch-up-&gt;live transition: the standby view registered by
   * {@code ContinuousProjectionRunner.registerStandbyHealthView} may have just accrued errors from
   * a dead-lettered/skipped catch-up chunk, and swapping in the live lifecycle via plain {@code
   * register} silently discarded that count the instant the runner reported itself LIVE — a poison
   * in the very last catch-up page followed by a quiet live stream then read UP over a read model
   * with a hole, with no clean batch ever having run to legitimately earn it. This keeps the error
   * count intact across the swap so the DEGRADED verdict earned during catch-up survives into LIVE
   * until a genuinely clean batch resets it via {@link #recordSuccess}.
   *
   * <p>Falls back to {@link #register(String, SubscriptionLifecycle)} — a fresh episode — only when
   * no entry is currently registered for {@code name} (nothing to preserve). A <b>terminally
   * errored</b> existing entry is NOT a fresh-register case — swapping in a fresh entry silently
   * cleared {@code terminalError} back to {@code false}, i.e. DOWN silently became UP, exactly the
   * "silently resurrect" outcome this javadoc has always said must not happen. A terminal entry now
   * takes the SAME swap-only path as any other existing entry: only the {@link
   * SubscriptionLifecycle} reference changes, and {@code errorCount}/{@code terminalError}/{@code
   * pushPathDeadWarned} — including a {@code true} {@code terminalError} — carry over unchanged,
   * because those are the SAME {@link java.util.concurrent.atomic.AtomicBoolean}/{@link
   * java.util.concurrent.atomic.AtomicInteger} instances, not copied values. The runner does not
   * call this after {@link #markTerminalError} in practice, but the method stays total and correct
   * either way.
   */
  public void replaceLifecycle(String name, SubscriptionLifecycle subscription) {
    SubscriptionEntry existing = subscriptions.get(name);
    if (existing == null) {
      register(name, subscription);
      return;
    }
    subscriptions.put(
        name,
        new SubscriptionEntry(
            subscription,
            existing.errorCount(),
            existing.terminalError(),
            existing.pushPathDeadWarned(),
            existing.lagMetricFailedWarned(),
            existing.checkpointAheadWarned()));
  }

  /**
   * Current processing lag for {@code name} — the gap between the global stream head and the
   * subscription's last committed offset — computed on demand. Backs the live {@code
   * subscriptions.lag} supplier gauge so a metrics scrape reads current lag without a
   * health-endpoint scrape. Never throws into a scrape.
   *
   * <p><b>A failed read must not look healthy.</b> This used to swallow every {@link
   * RuntimeException} and return {@code 0}, silently, which is the value a perfectly caught-up
   * subscription reports. So a {@code statement_timeout} on {@code lastGlobalOffset()}, a revoked
   * {@code projection_offset} SELECT grant, or a momentarily exhausted pool made every scrape read
   * lag=0 while the projection made no progress and the backlog grew — and {@code
   * docs/guide/production.md}'s "alert on rising {@code streamrune.subscriptions.lag}" was
   * structurally unfirable during exactly the store trouble it exists to catch, with no log line to
   * correlate against.
   *
   * <p>The gauge now HOLDS ITS LAST KNOWN VALUE on a read failure and a WARN naming the
   * subscription is logged for the scrape — the same discipline as {@code
   * OutboxPoller.sampleObservability} / {@code DeadLetterRetryRunner.sampleObservability}, whose
   * backlog gauges also hold rather than reset. Before the first successful sample there is no last
   * known value and 0 is reported, accompanied by that WARN.
   *
   * <p>An {@link UnsupportedOperationException} from the store is not a failure but a permanent
   * capability gap (the store cannot report its head at all): that path returns 0 without a WARN,
   * see {@link #computeLag(String)}.
   */
  public long currentLag(String name) {
    AtomicLong lastKnown = lastKnownLag.computeIfAbsent(name, k -> new AtomicLong(0));
    try {
      long lag = computeLag(name).lag();
      lastKnown.set(lag);
      return lag;
    } catch (RuntimeException e) {
      long held = lastKnown.get();
      log.warn(
          "Failed to compute lag for subscription '{}' this scrape;"
              + " streamrune.subscriptions.lag holds its last known value ({}): {}",
          name,
          held,
          e.getMessage());
      return held;
    }
  }

  public void unregister(String name) {
    subscriptions.remove(name);
    lastKnownLag.remove(name);
  }

  /**
   * Marks the subscription {@code name} as terminally halted: its health is forced to {@link
   * SubscriptionHealth.Status#DOWN} and STAYS DOWN, surviving the runner thread's exit. Unlike
   * {@link #unregister(String)}, the subscription remains in the health view so {@link
   * #overallStatus()} keeps reporting DOWN while the read model is permanently frozen.
   *
   * <p>Called by a projection runner on a <em>permanent ERROR exit</em> (e.g. the default HALT
   * strategy exhausting its retry budget, or a live subscription dying unexpectedly) instead of
   * unregistering. A deliberate CLEAN stop (app shutdown / {@code stopProjections}) still
   * unregisters; a leadership-loss STANDBY wait never reaches this path — so only a genuine halt
   * reports DOWN.
   *
   * <p>If the subscription is not currently registered (a projection that halted during catch-up,
   * before its live subscription was registered), a synthetic terminal-DOWN entry is created so the
   * halt is still visible in the health endpoint.
   */
  public void markTerminalError(String name) {
    subscriptions.compute(
        name,
        (k, existing) -> {
          if (existing == null) {
            return new SubscriptionEntry(
                null,
                new AtomicInteger(0),
                new AtomicBoolean(true),
                new AtomicBoolean(false),
                new AtomicBoolean(false),
                new AtomicBoolean(false));
          }
          existing.terminalError().set(true);
          return existing;
        });
  }

  public void recordError(String name) {
    var entry = subscriptions.get(name);
    if (entry != null) {
      entry.errorCount().incrementAndGet();
    }
  }

  public void recordSuccess(String name) {
    var entry = subscriptions.get(name);
    if (entry != null) {
      entry.errorCount().set(0);
    }
  }

  public List<SubscriptionHealth> health() {
    return subscriptions.entrySet().stream()
        .map(e -> computeHealth(e.getKey(), e.getValue()))
        .toList();
  }

  /**
   * The aggregate status of a fresh {@link #health()} sample. A health check that also reports the
   * individual subscriptions should use {@link #overallStatus(List)} on the sample it reports: each
   * call here samples every subscription again, so the two could disagree.
   */
  public SubscriptionHealth.Status overallStatus() {
    return overallStatus(health());
  }

  /**
   * The aggregate status of one health sample: {@code DOWN} if any subscription is {@code DOWN},
   * otherwise {@code DEGRADED} if any is {@code DEGRADED}, otherwise {@code UP} (also for an empty
   * sample).
   */
  public static SubscriptionHealth.Status overallStatus(List<SubscriptionHealth> healths) {
    if (healths.isEmpty()) {
      return SubscriptionHealth.Status.UP;
    }
    if (healths.stream().anyMatch(h -> h.status() == SubscriptionHealth.Status.DOWN)) {
      return SubscriptionHealth.Status.DOWN;
    }
    if (healths.stream().anyMatch(h -> h.status() == SubscriptionHealth.Status.DEGRADED)) {
      return SubscriptionHealth.Status.DEGRADED;
    }
    return SubscriptionHealth.Status.UP;
  }

  private SubscriptionHealth computeHealth(String name, SubscriptionEntry entry) {
    int errors = entry.errorCount().get();
    LagSample sample = computeLag(name);
    long lag = sample.lag();
    // Guarded — was bare, and computeHealth() runs once per subscription for EVERY
    // health() call, so a throw here failed the WHOLE /health scrape (not just this
    // subscription's entry) — the entire endpoint down over a metrics-backend hiccup that has
    // nothing to do with subscription health itself.
    //
    // A PERSISTENTLY throwing sink used to WARN-with-stack-trace on every single call —
    // i.e. once per subscription, every health() scrape — flooding the log at scrape cadence x N
    // subscriptions. One-shot latch per subscription, mirroring the class's own pushPathDeadWarned
    // / registration guard: warn once when the sink starts failing, stay
    // quiet while it keeps failing, and re-arm the moment a call succeeds again so a flapping sink
    // is visible as repeated warnings rather than either one lost line or an unbounded flood.
    try {
      metrics.recordSubscriptionLag(SubscriptionName.of(name), lag);
      entry.lagMetricFailedWarned().set(false);
    } catch (RuntimeException e) {
      if (entry.lagMetricFailedWarned().compareAndSet(false, true)) {
        log.warn("Metrics recordSubscriptionLag failed for subscription '{}'", name, e);
      }
    }
    if (entry.terminalError().get()) {
      // Definitively halted projection: report DOWN regardless of lag/error-count, and do not
      // require a live subscription — it may already be gone after the runner thread exited.
      SubscriptionLifecycleState state =
          entry.subscription() != null
              ? entry.subscription().state()
              : SubscriptionLifecycleState.STOPPED;
      return new SubscriptionHealth(name, state, lag, errors, SubscriptionHealth.Status.DOWN);
    }
    SubscriptionLifecycleState state = entry.subscription().state();
    SubscriptionHealth.Status status = determineStatus(state, sample, errors);
    status = applyPushPathHealth(name, entry, state, status);
    return new SubscriptionHealth(name, state, lag, errors, status);
  }

  /**
   * Folds a dead LISTEN/NOTIFY push path into the subscription's status as DEGRADED.
   *
   * <p>A hybrid subscription that loses its push path keeps delivering — polling guarantees it — so
   * this must never be DOWN, and it must never downgrade an already-DOWN/DEGRADED verdict. But it
   * must not read UP either: without this, a listener that failed at startup, or that cannot get a
   * LISTEN connection back ({@code max_connections} reached, credentials scoped to the main pool
   * only, TLS handshake failure, a failed-over primary that is not reachable), left every
   * subscription silently delivering at the poll interval instead of in milliseconds, with {@code
   * /health} UP and every metric reading healthy. {@code
   * streamrune.subscriptions.listener.reconnects} could not express it: a listener that dies before
   * its first reconnect never increments it, and one that dies after exhausting a finite cap stops
   * incrementing it — both read exactly what a healthy listener reads.
   *
   * <p>The WARN fires once per transition into the dead state, not once per health scrape: a health
   * endpoint is scraped continuously and a per-scrape log would bury the signal it is meant to
   * raise. Recovery (a push path back to LIVE) re-arms it, so a flapping listener is visible as
   * repeated warnings rather than one lost line.
   */
  private SubscriptionHealth.Status applyPushPathHealth(
      String name,
      SubscriptionEntry entry,
      SubscriptionLifecycleState state,
      SubscriptionHealth.Status status) {
    if (!(entry.subscription() instanceof PushHealthAware pushAware)) {
      return status;
    }
    PushHealthAware.PushPathStatus pushStatus;
    try {
      pushStatus = pushAware.pushPathStatus();
    } catch (RuntimeException e) {
      // Health reporting must never be taken out by a subscription's own probe.
      log.warn("Push-path health probe failed for subscription '{}'", name, e);
      return status;
    }
    if (pushStatus != PushHealthAware.PushPathStatus.DEAD) {
      // LIVE, or NOT_CONFIGURED (a pollingOnly subscription — expected, never a fault).
      entry.pushPathDeadWarned().set(false);
      return status;
    }
    // A subscription that is not RUNNING is already described by its lifecycle state; a stopped
    // subscription's listener is legitimately down and must not be reported as a degradation.
    if (state != SubscriptionLifecycleState.RUNNING) {
      return status;
    }
    if (entry.pushPathDeadWarned().compareAndSet(false, true)) {
      log.warn(
          "Subscription '{}' has a DEAD LISTEN/NOTIFY push path — delivery continues via polling"
              + " but at the poll interval instead of in milliseconds until the push path is back."
              + " The listener failed to start, exhausted its reconnect cap, or has been without a"
              + " LISTEN connection for longer than its reconnect grace; check the listener's"
              + " earlier ERROR and WARN lines. Reported as DEGRADED in subscription health.",
          name);
    }
    return status == SubscriptionHealth.Status.UP ? SubscriptionHealth.Status.DEGRADED : status;
  }

  /**
   * One lag sample: the gap between the stream head and the checkpoint, never negative, and whether
   * the checkpoint was found ahead of the head.
   */
  private record LagSample(long lag, boolean checkpointAhead) {
    static final LagSample UNKNOWN = new LagSample(0, false);
  }

  private LagSample computeLag(String name) {
    // The checkpoint is read BEFORE the head. The head only grows and a checkpoint never passes
    // the head it was read against, so in this order a checkpoint above the head cannot come from
    // a batch committing between the two reads (read the other way round, an old head and a newer
    // checkpoint can pair up). It means the database is inconsistent: the event table was
    // restored to an earlier point than the checkpoint, or rows were deleted, and the events later
    // appended at the offsets in between are never delivered to this subscription.
    GlobalOffset checkpoint = offsetStore.getLastOffset(ProjectionName.of(name));
    // EventStore.lastGlobalOffset() reports the head of the global stream. The previous
    // implementation read readGlobalStream(Long.MAX_VALUE - 1, 1), which returns events with an
    // offset STRICTLY GREATER than the argument — always empty against a real store, so the
    // computed head was 0 and no lag was ever reported.
    GlobalOffset globalHead;
    try {
      globalHead = eventStore.lastGlobalOffset();
    } catch (UnsupportedOperationException _) {
      // Store cannot report its head — lag is unknown; report 0 rather than failing the health
      // endpoint. Lifecycle- and error-based checks still apply.
      return LagSample.UNKNOWN;
    }
    SubscriptionEntry entry = subscriptions.get(name);
    if (checkpoint.value() > globalHead.value()) {
      if (entry != null && entry.checkpointAheadWarned().compareAndSet(false, true)) {
        log.warn(
            "Subscription '{}' has its checkpoint at offset {}, ahead of the event stream head {}."
                + " A consistent database never shows this: the event table was restored to an"
                + " earlier point than the checkpoint, or events were deleted, and the events"
                + " appended at the offsets in between will never reach this subscription. Restore"
                + " both to the same point (see the backup and restore section of the production"
                + " guide). Reported as DEGRADED in subscription health, with lag 0.",
            name,
            checkpoint.value(),
            globalHead.value());
      }
      return new LagSample(0, true);
    }
    if (entry != null) {
      entry.checkpointAheadWarned().set(false);
    }
    return new LagSample(globalHead.value() - checkpoint.value(), false);
  }

  /**
   * Status of a subscription that is not terminally halted. Only a stopped subscription is {@code
   * DOWN}; lag and errors describe a subscription that is still running, so they are {@code
   * DEGRADED} however large they grow — see the class documentation.
   */
  private SubscriptionHealth.Status determineStatus(
      SubscriptionLifecycleState state, LagSample sample, int errors) {
    if (state == SubscriptionLifecycleState.STOPPED) {
      return SubscriptionHealth.Status.DOWN;
    }
    if (sample.lag() >= lagThreshold
        || sample.checkpointAhead()
        || errors > 0
        || state == SubscriptionLifecycleState.PAUSED) {
      return SubscriptionHealth.Status.DEGRADED;
    }
    return SubscriptionHealth.Status.UP;
  }

  /**
   * @param pushPathDeadWarned one-shot latch so the dead-push-path WARN fires once per transition
   *     rather than once per health scrape; cleared whenever the push path is observed live (or not
   *     configured) again, so a flapping listener warns per episode.
   * @param lagMetricFailedWarned one-shot latch, same shape as {@code pushPathDeadWarned}, so a
   *     persistently throwing {@code metrics.recordSubscriptionLag} WARNs once per failure episode
   *     instead of once per health scrape; cleared on the next successful call.
   * @param checkpointAheadWarned one-shot latch, same shape, so a checkpoint found ahead of the
   *     stream head WARNs once per episode; cleared when a sample finds it at or behind the head.
   */
  private record SubscriptionEntry(
      SubscriptionLifecycle subscription,
      AtomicInteger errorCount,
      AtomicBoolean terminalError,
      AtomicBoolean pushPathDeadWarned,
      AtomicBoolean lagMetricFailedWarned,
      AtomicBoolean checkpointAheadWarned) {}
}

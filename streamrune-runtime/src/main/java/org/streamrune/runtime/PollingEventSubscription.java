package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * Subscription that polls the global event stream on a virtual thread. Supports configurable
 * interval with jitter to avoid thundering herd. This is the reliable baseline — works without
 * LISTEN/NOTIFY and serves as fallback.
 *
 * <p>Implements {@link SubscriptionLifecycle}: supports {@link #pause()} and {@link #resume()}.
 * Position is stored via {@link OffsetStore} on every batch, so resume continues from the last
 * committed offset automatically.
 */
public final class PollingEventSubscription implements SubscriptionLifecycle, ReadPoisonAware {

  private static final Logger logger = LoggerFactory.getLogger(PollingEventSubscription.class);
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);
  private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000;

  private final SubscriptionName subscriptionName;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final EventListener listener;
  private final SubscriptionConfig config;
  private final int fetchSize;
  private final StreamRuneMetrics metrics;
  // Consecutive deterministic read-poison reads at the same checkpoint
  // tolerated before this subscription stops itself terminally, so the owning projection runner can
  // HALT (health DOWN) instead of the resilient loop retrying an unreadable event forever. 0 =
  // disabled (standalone subscriptions retain their unbounded-retry behavior); the projection
  // runner sets it to its own maxReadPoisonRetries to mirror the catch-up bound.
  private final int readPoisonBound;
  // The terminal read-poison error that stopped this subscription (null otherwise); read by the
  // owning runner to surface a distinct read-poison ERROR. Only the poll thread writes it.
  private final AtomicReference<ProjectionReadPoisonException> readPoison = new AtomicReference<>();
  // Read-poison streak state — mutated only under pollLock, so plain fields suffice.
  private GlobalOffset readPoisonCheckpoint;
  private int readPoisonFailures;
  private final AtomicReference<SubscriptionLifecycleState> lifecycleState =
      new AtomicReference<>(SubscriptionLifecycleState.CREATED);
  private final ResilientPollLoop pollLoop;
  // Serializes pollOnce() between the background poll loop and triggerImmediatePoll() (called
  // from a LISTEN/NOTIFY dispatcher thread). Without it, two concurrent polls read the same
  // offset, deliver the same batch twice, and can save offsets backwards.
  private final ReentrantLock pollLock = new ReentrantLock();
  // A wake-up (triggerImmediatePoll) that may not have been served yet. Set before the wake-up
  // tries the lock, cleared when a drain starts; checked by every cycle after it releases the lock,
  // so a wake-up turned away by a cycle that was ending still gets a poll.
  private final AtomicBoolean pollRequested = new AtomicBoolean();
  private volatile PollThread pollThread;

  /** A started polling thread and the interrupt deferral pause() and close() stop it through. */
  private record PollThread(Thread thread, InterruptDeferral interrupts) {}

  /**
   * Creates a new polling subscription.
   *
   * @param subscriptionName unique name used for offset tracking and thread naming (required)
   * @param eventStore the event store to poll (required)
   * @param offsetStore durable store for tracking the last processed offset (required)
   * @param listener callback invoked with each batch of events (required)
   * @param config polling interval and jitter configuration (required)
   * @param fetchSize maximum number of events per poll cycle (must be positive)
   */
  public PollingEventSubscription(
      SubscriptionName subscriptionName,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize) {
    this(subscriptionName, eventStore, offsetStore, listener, config, fetchSize, null);
  }

  /**
   * Creates a new polling subscription with an optional metrics collector.
   *
   * @param metrics the metrics collector; {@code null} disables metrics (behavior unchanged). When
   *     present, records the number of events received and the delivery latency per poll cycle,
   *     tagged with the subscription name.
   */
  public PollingEventSubscription(
      SubscriptionName subscriptionName,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics) {
    this(subscriptionName, eventStore, offsetStore, listener, config, fetchSize, metrics, 0);
  }

  /**
   * Full constructor with an optional live read-poison bound. Package-private: the projection
   * runner uses it to make the default live reader poison-aware; public callers get the
   * unbounded-retry behavior via {@code readPoisonBound == 0}.
   */
  PollingEventSubscription(
      SubscriptionName subscriptionName,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      int readPoisonBound) {
    if (subscriptionName == null) {
      throw new IllegalArgumentException("subscriptionName is required");
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
    if (fetchSize <= 0) {
      throw new IllegalArgumentException("fetchSize must be positive");
    }
    this.subscriptionName = subscriptionName;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.listener = listener;
    this.config = config;
    this.fetchSize = fetchSize;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.readPoisonBound = Math.max(0, readPoisonBound);
    // Backoff starts at the polling interval and is capped at MAX_FAILURE_BACKOFF (or the
    // polling interval itself when that is already larger).
    Duration maxBackoff =
        config.pollingInterval().compareTo(MAX_FAILURE_BACKOFF) > 0
            ? config.pollingInterval()
            : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "subscription '" + subscriptionName.value() + "'",
            () -> lifecycleState.get() == SubscriptionLifecycleState.RUNNING,
            this::sleepWithJitter,
            config.pollingInterval(),
            maxBackoff);
  }

  @Override
  public void start() {
    if (!lifecycleState.compareAndSet(
        SubscriptionLifecycleState.CREATED, SubscriptionLifecycleState.RUNNING)) {
      throw new IllegalStateException(
          "Cannot start subscription in state: " + lifecycleState.get());
    }
    startPollThread();
  }

  /** Publishes the new polling thread with its deferral before starting it. */
  private void startPollThread() {
    Thread t =
        Thread.ofVirtual()
            .name("streamrune-poll-" + subscriptionName.value())
            .unstarted(this::runPollLoop);
    pollThread = new PollThread(t, new InterruptDeferral());
    t.start();
  }

  @Override
  public void pause() {
    if (!lifecycleState.compareAndSet(
        SubscriptionLifecycleState.RUNNING, SubscriptionLifecycleState.PAUSED)) {
      throw new IllegalStateException(
          "Cannot pause subscription in state: " + lifecycleState.get());
    }
    PollThread current = pollThread;
    if (current != null) {
      current.interrupts().interrupt(current.thread());
    }
    // Note: pause() returns as soon as the state CAS and interrupt are sent, but the polling
    // thread may still be inside pollOnce() (which has no interruptible blocking points). One
    // extra batch may be delivered after pause() returns. This is at-least-once semantics —
    // callers must tolerate duplicates. The thread will exit on the next Thread.sleep() call.
  }

  @Override
  public void resume() {
    if (!lifecycleState.compareAndSet(
        SubscriptionLifecycleState.PAUSED, SubscriptionLifecycleState.RUNNING)) {
      throw new IllegalStateException(
          "Cannot resume subscription in state: " + lifecycleState.get());
    }
    // The previous virtual thread may still be finishing its last pollOnce() call (Thread.sleep
    // is the only interruptible point — pollOnce itself is not). A brief overlap where two
    // threads read the same offset and deliver the same batch is possible. This is intentional:
    // PollingEventSubscription provides at-least-once delivery, and the second delivery will
    // be a duplicate the listener must tolerate or deduplicate.
    startPollThread();
  }

  @Override
  public SubscriptionLifecycleState state() {
    return lifecycleState.get();
  }

  @Override
  public boolean isRunning() {
    return lifecycleState.get() == SubscriptionLifecycleState.RUNNING;
  }

  /**
   * Stops the subscription and waits for the polling thread to finish its in-flight poll cycle.
   * {@code pollOnce()} has no interruptible points, so without the join a batch in flight would
   * keep running — and save its offset — after close() returns, racing any resource teardown (e.g.
   * closing a DataSource) the caller does next.
   *
   * <p>Waits up to {@value #CLOSE_JOIN_TIMEOUT_MS} ms; if the thread is still alive after that
   * (e.g. a stuck listener), a warning is logged and close() returns anyway. Calling close() from
   * the listener itself (i.e. on the polling thread) only signals and returns — joining the current
   * thread would deadlock. If the closing thread is interrupted while waiting, the wait is
   * abandoned and the interrupt flag restored.
   */
  @Override
  public void close() {
    // Using set() not CAS — close() is terminal and must win regardless of current state.
    // If pause() has already done its CAS (RUNNING → PAUSED) but not yet interrupted the thread,
    // close() will set STOPPED and interrupt the thread first. pause() will then also interrupt
    // (a no-op on an already-interrupted/finished thread). Callers may observe STOPPED even if
    // pause() returned without throwing — this is the accepted concurrent-close race per the
    // SubscriptionLifecycle contract.
    lifecycleState.set(SubscriptionLifecycleState.STOPPED);
    PollThread current = pollThread;
    if (current == null) {
      return;
    }
    Thread t = current.thread();
    current.interrupts().interrupt(t);
    if (t == Thread.currentThread()) {
      return;
    }
    try {
      t.join(CLOSE_JOIN_TIMEOUT_MS);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      return;
    }
    if (t.isAlive()) {
      logger.warn(
          "Subscription '{}' polling thread did not stop within {} ms after close() — the"
              + " in-flight batch is still processing and may save its offset after this call",
          subscriptionName.value(),
          CLOSE_JOIN_TIMEOUT_MS);
    }
  }

  /**
   * Triggers an immediate poll. Used by HybridEventSubscription to poll immediately when a
   * notification is received. The caller must verify that the subscription is RUNNING before
   * calling this method.
   *
   * <p>If a poll cycle is already in progress on another thread, this method returns {@code 0}
   * without polling instead of racing it on the same offset or waiting for it. The wake-up is not
   * lost: the cycle in progress drains once more after it releases the lock, so an event committed
   * after that cycle's last read is still delivered at once, not after the next polling interval.
   *
   * @return number of events processed; {@code 0} if no events were available or a concurrent poll
   *     was already in progress
   */
  public int triggerImmediatePoll() {
    pollRequested.set(true);
    if (!pollLock.tryLock()) {
      // The thread holding the lock sees the request after it releases the lock and polls again.
      return 0;
    }
    return drainAndRelease();
  }

  /**
   * Runs one poll cycle, DRAINING to the head of the global stream, and returns the total number of
   * events delivered across the cycle. Poll cycles are serialized via {@code pollLock} so that the
   * background loop and notification-driven immediate polls never process the same offset range
   * concurrently; the lock is held for the whole drain, so a {@link #triggerImmediatePoll()}
   * arriving mid-drain simply lets the in-flight drain pick its events up.
   *
   * <p><b>Why this drains rather than reading one page.</b> {@link ResilientPollLoop} sleeps the
   * idle delay after EVERY successful poll, including one that returned a completely full page. A
   * one-page-per-cycle reader therefore caps a projection at {@code fetchSize / pollingInterval}
   * events per second — 50 / 5.5 s, about 9 e/s at the auto-configured defaults — forever, with no
   * recovery mode (the owning runner never re-enters catch-up once it is LIVE). A sustained write
   * rate above that falls behind without bound until the lag crosses the health contributor's
   * threshold and the projection is reported DOWN. Draining removes the ceiling entirely:
   * throughput is bounded by the store and the listener, not by the polling interval. This is the
   * same treatment the sibling {@code PostgresNotificationSubscription} already applies, and {@code
   * HybridEventSubscription} inherits it by delegating here.
   *
   * <p><b>The drain ends on an EMPTY read, not on a short page.</b> A page shorter than {@code
   * fetchSize} does not prove the head was reached: {@code PostgresEventStore}'s contiguity guard
   * deliberately splits a page at a PERMANENT hole (an offset no committed event holds — the {@code
   * EventStore} contract does not promise contiguity; a restore or import that left the offset
   * counter ahead of the data, or a row deleted out of band, leaves one) and the next read re-seeds
   * from its own first row and returns the events past it. Stopping at a short page would strand
   * those for a full polling interval — the same defect, relocated. Reading on cannot skip a
   * still-in-flight lower offset, because the store's serialized (gapless) offset counter
   * guarantees none exists beneath a committed one; and the uncommitted tail append never appears,
   * so the loop always terminates.
   *
   * <p><b>Failure and cancellation.</b> Each page saves its own checkpoint before the next read, so
   * a read or listener failure part-way through a drain keeps every page already delivered (forward
   * progress is never given back) and the resilient loop's backoff-retry resumes from there. The
   * lifecycle is re-checked between pages, so {@code pause()} / {@code close()} bound the cycle at
   * one more page instead of waiting out an unbounded catch-up — the same "one extra batch may be
   * delivered after pause() returns" contract as before.
   *
   * <p><b>Wake-ups that arrive during a drain.</b> A {@link #triggerImmediatePoll()} that finds the
   * lock held records its request and returns. After every drain the cycle releases the lock and,
   * when a request is recorded, takes the lock again and drains once more (unless paused or
   * stopped). A request recorded after that check finds the lock free and polls by itself, so no
   * wake-up waits for the next polling interval.
   *
   * @return total number of events delivered in this cycle
   */
  int pollOnce() {
    pollLock.lock();
    return drainAndRelease();
  }

  /**
   * Drains with {@code pollLock} held by the caller (exactly once) and releases it; drains again
   * while a wake-up was recorded in the meantime and the lock can be taken without waiting. A
   * failed {@code tryLock} means another thread holds the lock, and that thread checks the request
   * after its own release.
   */
  private int drainAndRelease() {
    int drained = 0;
    while (true) {
      try {
        pollRequested.set(false);
        drained += drainToHead();
      } finally {
        pollLock.unlock();
      }
      if (!pollRequested.get() || !mayContinueDraining() || !pollLock.tryLock()) {
        return drained;
      }
    }
  }

  /** One drain to the head of the global stream; the caller holds {@code pollLock}. */
  private int drainToHead() {
    ProjectionName name = ProjectionName.of(subscriptionName.value());
    int drained = 0;
    while (true) {
      GlobalOffset lastOffset = offsetStore.getLastOffset(name);
      List<EventEnvelope> events;
      try {
        events = eventStore.readGlobalStream(lastOffset, fetchSize);
      } catch (RuntimeException readError) {
        // Only the READ is a read-poison candidate — a listener/delivery failure below is a
        // processing failure handled by the caller's error strategy, never bounded here. A poison
        // read may throw itself (stopping the loop); a transient read is rethrown for the
        // resilient loop's unbounded backoff-retry.
        onReadFailure(lastOffset, readError);
        throw readError;
      }
      // A successful read clears any read-poison streak.
      resetReadPoison();
      if (events.isEmpty()) {
        return drained;
      }

      deliver(name, events);
      drained += events.size();

      if (!mayContinueDraining()) {
        return drained;
      }
    }
  }

  /**
   * Delivers one page and saves its checkpoint. On the polling thread this runs with pause()'s and
   * close()'s interrupt held back: on a virtual thread an interrupt fails every blocking socket
   * call made while the flag is set, so the listener's own store writes (a projection runner
   * commits its batch here) and the checkpoint save would fail and the page be delivered again. The
   * interrupt is delivered when the page is done; close() waits for it. Reads stay interruptible.
   */
  private void deliver(ProjectionName name, List<EventEnvelope> events) {
    PollThread current = pollThread;
    InterruptDeferral interrupts =
        current != null && current.thread() == Thread.currentThread() ? current.interrupts() : null;
    if (interrupts != null) {
      interrupts.defer();
    }
    try {
      long start = System.nanoTime();
      listener.onEvents(events);
      // Delivery succeeded: record received count and the latency of delivering this batch.
      // A listener failure above propagates (no metrics, checkpoint not advanced) so only
      // successfully delivered events are counted.
      recordDelivery(events.size(), System.nanoTime() - start);
      offsetStore.saveOffset(name, events.getLast().globalOffset());
    } finally {
      if (interrupts != null) {
        interrupts.resume();
      }
    }
  }

  /**
   * Whether the drain may read another page. Deliberately "not stopped and not paused" rather than
   * "RUNNING": {@code pollOnce()} is also driven directly (by {@link #triggerImmediatePoll()} and
   * by tests) on a subscription that was never {@code start()}ed, and such a caller must still get
   * a full drain. Only an explicit {@code pause()} / {@code close()} cuts the cycle short, matching
   * the poll loop's own {@code keepRunning} condition where it applies.
   */
  private boolean mayContinueDraining() {
    SubscriptionLifecycleState s = lifecycleState.get();
    return s != SubscriptionLifecycleState.STOPPED && s != SubscriptionLifecycleState.PAUSED;
  }

  /**
   * Bounds a deterministic read/deserialization poison on the LIVE read path, mirroring the
   * catch-up bound in {@link ContinuousProjectionRunner}. A poison read (an unregistered event
   * type, or a corrupt/schema-incompatible payload) fails identically forever; without a bound the
   * {@link ResilientPollLoop} would retry it as a "transient" store error indefinitely, wedging the
   * projection with no progress, nothing dead-lettered, and health stuck UP. After {@code
   * readPoisonBound} consecutive deterministic poison reads at the SAME checkpoint this
   * subscription stops itself (STOPPED) and records a terminal {@link
   * ProjectionReadPoisonException} the owning runner surfaces as a terminal ERROR / health-DOWN. A
   * genuine transient infra read error (SQLException-caused), or a changed checkpoint, resets the
   * streak and is left to the resilient loop's unbounded retry. No-op when {@code readPoisonBound
   * == 0} (standalone subscriptions retry every read forever, unchanged).
   */
  private void onReadFailure(GlobalOffset checkpoint, RuntimeException readError) {
    if (readPoisonBound <= 0 || !ReadPoisonClassifier.isDeterministicReadPoison(readError)) {
      resetReadPoison();
      return;
    }
    if (!checkpoint.equals(readPoisonCheckpoint)) {
      readPoisonCheckpoint = checkpoint;
      readPoisonFailures = 0;
    }
    readPoisonFailures++;
    if (readPoisonFailures >= readPoisonBound) {
      var poison =
          new ProjectionReadPoisonException(
              subscriptionName.value(), readPoisonFailures, readError);
      readPoison.set(poison);
      // Leave RUNNING so the loop exits and the owning runner observes a non-running subscription
      // and HALTs terminally. set() (not CAS) — this terminal stop must win.
      lifecycleState.set(SubscriptionLifecycleState.STOPPED);
      throw poison;
    }
  }

  private void resetReadPoison() {
    if (readPoisonBound > 0) {
      readPoisonCheckpoint = null;
      readPoisonFailures = 0;
    }
  }

  /**
   * The terminal read-poison error that stopped this subscription, or {@code null} if it is still
   * running or stopped for another reason. The owning projection runner reads this (via {@link
   * ReadPoisonAware}) after observing a non-running subscription to distinguish a bounded
   * read-poison HALT from any other stop.
   */
  @Override
  public ProjectionReadPoisonException readPoisonError() {
    return readPoison.get();
  }

  /**
   * Records {@code count} received events and the batch delivery latency. Metric failures are
   * logged and never disrupt event delivery (delivery has already happened).
   */
  private void recordDelivery(int count, long latencyNanos) {
    try {
      for (int i = 0; i < count; i++) {
        metrics.recordSubscriptionEventReceived(subscriptionName);
      }
      metrics.recordSubscriptionDeliveryLatency(subscriptionName, latencyNanos);
    } catch (RuntimeException e) {
      logger.warn("Metrics recording failed for subscription '{}'", subscriptionName.value(), e);
    }
  }

  /**
   * Number of consecutive poll-cycle failures; {@code 0} when the last cycle succeeded. A non-zero
   * value means the subscription is degraded — still RUNNING, retrying with backoff.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  private void runPollLoop() {
    // ResilientPollLoop logs failures and retries with capped exponential backoff. The loop only
    // exits when the lifecycle leaves RUNNING (pause/close) or the thread is interrupted — a
    // transient event-store or listener error never silently kills the subscription.
    try {
      pollLoop.run(this::pollOnce);
    } catch (Throwable t) {
      // ResilientPollLoop retries only RuntimeException. An uncaught java.lang.Error
      // (AssertionError, NoClassDefFoundError/LinkageError from a missing optional dependency,
      // StackOverflowError on pathological data) escapes it and would kill this virtual poll
      // thread. Such an Error is NOT a transient hiccup and is NOT retryable — retrying it would
      // spin forever — so the subscription must STOP terminally and report it, rather than leave a
      // dead thread with lifecycleState still RUNNING (isRunning()==true) while the read model
      // silently freezes and /health stays UP. Log it with the subscription name, then flip the
      // lifecycle to STOPPED so the owning ContinuousProjectionRunner's liveness check observes
      // !isRunning() and surfaces a terminal ERROR (health DOWN). We deliberately do NOT rethrow:
      // the stop-and-report is the intended handling, and the log line above already preserves the
      // error for diagnostics.
      //
      // The CAS lives here in the abnormal-exit (Error) path ONLY — NOT in a finally that runs on
      // every exit. A clean pause() exit returns normally (no throwable), and pause→resume can
      // restart a fresh poll thread and CAS the state back to RUNNING before THIS (now-stale)
      // thread finishes unwinding; a finally-CAS would then race and clobber the resumed RUNNING
      // back to STOPPED. Confining the CAS to the catch means a normal (paused/closed/interrupted)
      // exit never touches the lifecycle, and only a genuine Error converts a RUNNING→dead exit
      // into a visible STOPPED. If close() concurrently set STOPPED, the CAS is a no-op.
      logger.error(
          "Subscription '{}' poll thread terminated on an unrecoverable error — stopping it"
              + " terminally so the projection stops reporting live instead of silently freezing",
          subscriptionName.value(),
          t);
      lifecycleState.compareAndSet(
          SubscriptionLifecycleState.RUNNING, SubscriptionLifecycleState.STOPPED);
    }
  }

  private Duration sleepWithJitter() {
    long baseMs = config.pollingInterval().toMillis();
    long jitterMs = config.pollingJitter().toMillis();
    long jitter = jitterMs > 0 ? ThreadLocalRandom.current().nextLong(jitterMs) : 0;
    return Duration.ofMillis(baseMs + jitter);
  }

  // Builder
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private SubscriptionName subscriptionName;
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private EventListener listener;
    private SubscriptionConfig config = SubscriptionConfig.DEFAULT;
    private int fetchSize = 100;
    private StreamRuneMetrics metrics;
    private int readPoisonBound;

    /** Sets the unique subscription name (required). Accepts String for convenience. */
    public Builder subscriptionName(String name) {
      this.subscriptionName = SubscriptionName.of(name);
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

    /** Sets the polling configuration (default: {@link SubscriptionConfig#DEFAULT}). */
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
     * Sets the metrics collector. Optional; when absent (or {@code null}) subscription metrics are
     * not recorded and behavior is unchanged. Records events received and delivery latency per poll
     * cycle, tagged with the subscription name.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    /**
     * Bounds deterministic read/deserialization poison on the live read path: after {@code bound}
     * consecutive poison reads at the same checkpoint the subscription stops terminally so the
     * owning projection runner HALTs (health DOWN) instead of retrying an unreadable event forever.
     * Default {@code 0} disables the bound (unbounded transient-style retry, the
     * standalone-subscription behavior). The projection runner opts in via its catch-up {@code
     * maxReadPoisonRetries}; {@code HybridEventSubscription} threads the same bound here so the
     * documented factory/Hybrid path is poison-bounded too.
     */
    public Builder readPoisonBound(int bound) {
      this.readPoisonBound = bound;
      return this;
    }

    /**
     * Builds the {@link PollingEventSubscription}. Requires {@code subscriptionName}, {@code
     * eventStore}, {@code offsetStore}, and {@code listener}.
     */
    public PollingEventSubscription build() {
      return new PollingEventSubscription(
          subscriptionName,
          eventStore,
          offsetStore,
          listener,
          config,
          fetchSize,
          metrics,
          readPoisonBound);
    }
  }
}

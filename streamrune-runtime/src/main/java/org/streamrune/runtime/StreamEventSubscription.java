package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.StreamSubscription;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;

/**
 * Subscription that delivers events for a specific aggregate stream. Polls the event store for new
 * events on the given streamId.
 *
 * <p>The {@code subscriptionName} is the checkpoint key in the {@link OffsetStore}, which is a
 * single namespace shared with projection names and other subscription names. It must therefore be
 * unique: two subscriptions on the same stream need distinct names, otherwise they share one
 * checkpoint and steal events from each other.
 *
 * <p>Transient poll failures (event store or listener errors) are logged and retried with capped
 * exponential backoff — they never silently terminate the subscription. {@link #isRunning()}
 * reflects the actual liveness of the polling thread.
 *
 * <p><b>Deterministic read poison is bounded.</b> This subscription reads the GLOBAL stream and
 * filters by {@link #streamId()} only after the store has deserialized the whole page, so an
 * unregistered or corrupt event on <em>any</em> stream fails the read identically forever. After
 * {@link #DEFAULT_READ_POISON_BOUND} (5) consecutive such reads at the same checkpoint
 * (configurable via {@link Builder#readPoisonBound(int)}; {@code 0} disables the bound) the
 * subscription stops its polling thread — {@link #isRunning()} turns {@code false}, {@link
 * #readPoisonError()} exposes the {@link ProjectionReadPoisonException} and an ERROR log names the
 * remedy — instead of retrying forever while looking merely idle. It then STAYS stopped; nothing
 * restarts it. The remedy: register the missing event type or repair/erase the offending event
 * (which may belong to another stream), then call {@link #start()} again, which begins a fresh
 * streak.
 *
 * <p>The bound defaults ON here, unlike {@link PollingEventSubscription} and {@code
 * PostgresNotificationSubscription}, which default it to {@code 0} because the projection runner
 * they serve hands them its own bound. A {@code StreamEventSubscription} has no owning runner and
 * no health surface — no integration builds or health-checks one — so its caller must watch {@link
 * #isRunning()}, {@link #readPoisonError()} and {@link #consecutiveFailures()} itself.
 *
 * <p><b>Lifecycle is one-way.</b> {@link #close()} joins the polling thread before returning, and
 * is terminal: {@link #start()} refuses a closed subscription, and refuses one whose previous
 * polling thread is still winding down after a join timeout. This is the close-then-join discipline
 * the sibling runners apply ({@link PollingEventSubscription#close()}, {@code
 * PgNotificationListener}, {@link OutboxPoller}, {@code SagaTimeoutRunner}, the projection
 * runners); see {@link #close()} and {@link #start()} for why each half is needed here.
 */
public final class StreamEventSubscription implements StreamSubscription, ReadPoisonAware {

  private static final Logger logger = LoggerFactory.getLogger(StreamEventSubscription.class);
  private static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  /** Default bound on the {@link #close()} join, matching the sibling runners. */
  private static final long DEFAULT_CLOSE_JOIN_TIMEOUT_MS = 30_000;

  /**
   * Default bound on consecutive deterministic read/deserialization poison reads at the same
   * checkpoint before the subscription stops itself terminally — the same default the projection
   * runners apply to their catch-up and live reads. {@code 0} disables the bound.
   */
  public static final int DEFAULT_READ_POISON_BOUND = 5;

  private final SubscriptionName subscriptionName;
  private final StreamId streamId;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final EventListener listener;
  private final SubscriptionConfig config;
  private final int fetchSize;
  private final StreamRuneMetrics metrics;

  /**
   * Consecutive deterministic poison reads at the same checkpoint tolerated before this
   * subscription stops itself terminally; {@code 0} disables the bound (unbounded retry).
   */
  private final int readPoisonBound;

  /** The terminal read-poison error that stopped this subscription; {@code null} otherwise. */
  private final AtomicReference<ProjectionReadPoisonException> readPoison = new AtomicReference<>();

  // Read-poison streak state. Touched only from pollOnce(), which runs on the single poll thread
  // (start() refuses a second poller while the previous one is alive) or on a direct caller of a
  // never-started subscription, so plain fields suffice — mirrors PostgresNotificationSubscription.
  private GlobalOffset readPoisonCheckpoint;
  private int readPoisonFailures;

  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Set once by {@link #close()}. Distinct from {@code running}, which is only true between {@code
   * start()} and {@code close()}: {@link #pollOnce()} is also driven directly on a subscription
   * that was never started, and such a caller must still get a full drain. Only an explicit close
   * cuts a drain short.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** How long {@link #close()} waits for the polling thread; package-private knob for tests. */
  private final long closeJoinTimeoutMs;

  private final ResilientPollLoop pollLoop;
  private volatile PollThread pollThread;

  /** A started polling thread and the interrupt deferral close() stops it through. */
  private record PollThread(Thread thread, InterruptDeferral interrupts) {}

  public StreamEventSubscription(
      SubscriptionName subscriptionName,
      StreamId streamId,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize) {
    this(subscriptionName, streamId, eventStore, offsetStore, listener, config, fetchSize, null);
  }

  /**
   * Creates a new stream subscription with an optional metrics collector.
   *
   * @param metrics the metrics collector; {@code null} disables metrics (behavior unchanged). When
   *     present, records the number of events delivered for this stream and the delivery latency
   *     per poll cycle, tagged with the subscription name.
   */
  public StreamEventSubscription(
      SubscriptionName subscriptionName,
      StreamId streamId,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics) {
    this(
        subscriptionName,
        streamId,
        eventStore,
        offsetStore,
        listener,
        config,
        fetchSize,
        metrics,
        DEFAULT_CLOSE_JOIN_TIMEOUT_MS,
        DEFAULT_READ_POISON_BOUND);
  }

  /**
   * Full constructor with a configurable {@link #close()} join timeout and read-poison bound.
   * Package-private: tests use it to drive the join-timeout branch (a poll thread still alive when
   * {@code close()} returns) without waiting out the 30-second production bound; {@link Builder}
   * exposes both knobs.
   */
  StreamEventSubscription(
      SubscriptionName subscriptionName,
      StreamId streamId,
      EventStore eventStore,
      OffsetStore offsetStore,
      EventListener listener,
      SubscriptionConfig config,
      int fetchSize,
      StreamRuneMetrics metrics,
      long closeJoinTimeoutMs,
      int readPoisonBound) {
    if (subscriptionName == null) {
      throw new IllegalArgumentException("subscriptionName is required");
    }
    if (streamId == null) {
      throw new IllegalArgumentException("streamId is required");
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
    if (closeJoinTimeoutMs < 0) {
      throw new IllegalArgumentException("closeJoinTimeoutMs must not be negative");
    }
    this.closeJoinTimeoutMs = closeJoinTimeoutMs;
    this.subscriptionName = subscriptionName;
    this.streamId = streamId;
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.listener = listener;
    this.config = config;
    this.fetchSize = fetchSize;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.readPoisonBound = Math.max(0, readPoisonBound);
    Duration maxBackoff =
        config.pollingInterval().compareTo(MAX_FAILURE_BACKOFF) > 0
            ? config.pollingInterval()
            : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            "stream subscription '" + subscriptionName.value() + "'",
            running::get,
            this::sleepWithJitter,
            config.pollingInterval(),
            maxBackoff);
  }

  @Override
  public StreamId streamId() {
    return streamId;
  }

  /**
   * Starts the polling thread.
   *
   * <p><b>A restart is refused, not silently accepted.</b> The guard used to be {@code
   * running.compareAndSet(false, true)} alone, which succeeded the instant {@code close()} flipped
   * the flag — while the previous poll thread was still inside {@code pollOnce()}, whose delivery
   * close() does not interrupt. Two poll threads then shared one checkpoint: both read {@code
   * lastOffset = N}, both delivered the same batch to the same listener, and both saved (with an
   * OffsetStore that is not monotonic-guarded, the later save can even regress the checkpoint).
   * This is the same guard {@code OutboxPoller} / {@code SagaTimeoutRunner} / the projection
   * runners apply after a join that timed out and {@code ContinuousProjectionRunner} applies via
   * its {@code previousRunThread.isAlive()} check.
   *
   * @throws IllegalStateException if the subscription is already running, has been closed, or its
   *     previous polling thread has not finished winding down
   */
  @Override
  public void start() {
    if (closed.get()) {
      throw new IllegalStateException("Subscription is closed and cannot be restarted");
    }
    PollThread previous = pollThread;
    if (previous != null && previous.thread().isAlive()) {
      throw new IllegalStateException(
          "Previous polling thread is still running — refusing to start a second poller on the"
              + " same checkpoint");
    }
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Subscription already running");
    }
    // A fresh start is a fresh read-poison streak: a subscription stopped by the bound
    // may be started again once the offending event is repaired. The previous poll thread is dead
    // (checked above) and Thread.start() below publishes these writes to the new one.
    readPoison.set(null);
    readPoisonCheckpoint = null;
    readPoisonFailures = 0;
    Thread t =
        Thread.ofVirtual()
            .name("streamrune-stream-poll-" + subscriptionName.value())
            .unstarted(this::runPollLoop);
    pollThread = new PollThread(t, new InterruptDeferral());
    t.start();
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Stops the subscription and waits for the polling thread to finish its in-flight poll cycle.
   *
   * <p><b>Why the join.</b> The interrupt does not stop {@link #pollOnce()} at once: the thread may
   * be inside {@code readGlobalStream} / {@code listener.onEvents} / {@code saveOffset}, and the
   * delivery and its checkpoint save run with the interrupt held back (on a virtual thread an
   * interrupt fails every blocking socket call made while the flag is set, so the listener's own
   * store writes and the checkpoint save would fail and the page be delivered again). The interrupt
   * reaches the thread once the page is done. Without the join, {@code close()} returned while that
   * batch was still in flight, so it kept running — and saved its checkpoint — after the caller had
   * moved on to the next teardown step, typically closing the shared {@code DataSource} out from
   * under it. Either the batch dies mid-delivery with the read model half-applied and no checkpoint
   * saved, or it commits a checkpoint after the subscription was supposed to be gone. Same
   * rationale, same shape as {@link PollingEventSubscription#close()}.
   *
   * <p>Waits up to the configured join timeout (30 s by default); if the thread is still alive
   * after that (a stuck listener), a warning is logged and {@code close()} returns anyway — but the
   * subscription stays permanently non-restartable, so a {@link #start()} cannot put a second
   * poller on the checkpoint next to the stuck one. Calling {@code close()} from the listener
   * itself (i.e. on the polling thread) only signals and returns; joining the current thread would
   * deadlock. If the closing thread is interrupted while waiting, the wait is abandoned and the
   * interrupt flag restored. Idempotent.
   */
  @Override
  public void close() {
    closed.set(true);
    running.set(false);
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
      t.join(closeJoinTimeoutMs);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      return;
    }
    if (t.isAlive()) {
      logger.warn(
          "Stream subscription '{}' polling thread did not stop within {} ms after close() — the"
              + " in-flight batch is still processing and may save its offset after this call",
          subscriptionName.value(),
          closeJoinTimeoutMs);
    }
  }

  /**
   * Runs one poll cycle, DRAINING the global stream to its head, and returns how many of this
   * stream's events were delivered.
   *
   * <p><b>Throughput bound.</b> This subscription reads the GLOBAL stream and filters it, so a
   * single page per {@code pollingInterval} caps it at {@code fetchSize / pollingInterval} GLOBAL
   * events per second — and {@link ResilientPollLoop} sleeps the idle delay after every successful
   * poll, full page included. Any application whose total write rate exceeds that ceiling starves
   * this subscription of its own stream's events without bound, even when that stream is nearly
   * idle. Draining removes the ceiling, matching {@link PollingEventSubscription#pollOnce()},
   * {@code PollingProjectionRunner.run}, {@code PostgresNotificationSubscription.pollAndDeliver}
   * and both projection runners' catch-up loops.
   *
   * <p><b>No {@code pollLock}, deliberately.</b> The sibling {@link
   * PollingEventSubscription#pollOnce()} serializes under one because it has a SECOND public entry
   * point — {@code triggerImmediatePoll()}, called from a LISTEN/NOTIFY dispatcher thread. This
   * class has none: {@code pollOnce()} is package-private with exactly one production caller (the
   * poll loop below), and {@link #start()} refuses to spawn a second poll thread while the previous
   * one is alive. Single-poller is therefore enforced by the lifecycle guard rather than by a lock
   * that no reachable caller could contend, and adding one would advertise a concurrent-poll
   * contract this class does not offer.
   *
   * <p>The drain ends on an EMPTY read, never on a short page: {@code readGlobalStream}'s
   * contiguity guard truncates a page at a permanent hole, so a short page does not mean the head
   * was reached. Each page saves its own checkpoint before the next read, so a failure part-way
   * through keeps the progress already made; {@code close()} is re-checked between pages so
   * teardown is not blocked behind an unbounded catch-up.
   */
  int pollOnce() {
    ProjectionName name = ProjectionName.of(subscriptionName.value());
    int delivered = 0;
    while (true) {
      GlobalOffset lastOffset = offsetStore.getLastOffset(name);
      List<EventEnvelope> events;
      try {
        events = eventStore.readGlobalStream(lastOffset, fetchSize);
      } catch (RuntimeException readError) {
        // Only the READ is a read-poison candidate — and because this subscription
        // reads the GLOBAL page and filters by stream afterwards, a FOREIGN stream's unregistered
        // or corrupt event poisons it just the same. A listener failure below is a processing
        // failure and is never bounded here.
        onReadFailure(lastOffset, readError); // throws the terminal poison once the bound is hit
        throw readError;
      }
      // A successful read clears any read-poison streak.
      resetReadPoison();

      if (events.isEmpty()) {
        return delivered;
      }

      // Filter events for this stream
      List<EventEnvelope> streamEvents =
          events.stream().filter(e -> streamId.equals(e.streamId())).toList();

      deliver(name, streamEvents, events.getLast().globalOffset());
      delivered += streamEvents.size();

      if (closed.get()) {
        return delivered;
      }
    }
  }

  /**
   * Delivers this stream's events of one page and saves the checkpoint past the whole page. On the
   * polling thread this runs with close()'s interrupt held back: on a virtual thread an interrupt
   * fails every blocking socket call made while the flag is set, so the listener's own store writes
   * and the checkpoint save would fail and the page be delivered again. The interrupt is delivered
   * when the page is done; close() waits for it. Reads stay interruptible.
   */
  private void deliver(
      ProjectionName name, List<EventEnvelope> streamEvents, GlobalOffset pageEnd) {
    PollThread current = pollThread;
    InterruptDeferral interrupts =
        current != null && current.thread() == Thread.currentThread() ? current.interrupts() : null;
    if (interrupts != null) {
      interrupts.defer();
    }
    try {
      if (!streamEvents.isEmpty()) {
        long start = System.nanoTime();
        listener.onEvents(streamEvents);
        // Record only events actually delivered to the listener (this stream's events). A listener
        // failure above propagates before recording and leaves the checkpoint untouched.
        recordDelivery(streamEvents.size(), System.nanoTime() - start);
      }
      // Advance the checkpoint past the entire fetched page, including foreign-stream events.
      // Saving only the last delivered event's offset would stall the subscription forever once a
      // full page contains nothing but other streams' events. Saved only after a successful
      // delivery (at-least-once: a listener failure above leaves the checkpoint untouched).
      offsetStore.saveOffset(name, pageEnd);
    } finally {
      if (interrupts != null) {
        interrupts.resume();
      }
    }
  }

  /**
   * Bounds a deterministic read/deserialization poison, mirroring {@link PollingEventSubscription}
   * and {@code PostgresNotificationSubscription}. This subscription reads the GLOBAL stream and
   * filters by {@link #streamId()} only after {@code readGlobalStream} has deserialized the whole
   * page, so an unregistered or corrupt event on ANY stream — one this subscription would have
   * discarded anyway — fails the read identically forever. Without a bound the {@link
   * ResilientPollLoop} retried it as a "transient" store error indefinitely: the checkpoint never
   * advanced, {@link #isRunning()} stayed {@code true}, and nothing distinguished "wedged on a
   * poison" from "idle". After {@code readPoisonBound} consecutive deterministic poison reads at
   * the SAME checkpoint the subscription stops itself terminally ({@code isRunning() == false}),
   * logs the remedy — there is no owning runner to surface a stream subscription's halt — and
   * exposes the {@link ProjectionReadPoisonException} via {@link #readPoisonError()}. A transient
   * infrastructure read error (SQLException-caused), or a moved checkpoint, resets the streak and
   * is left to the resilient loop's unbounded retry. No-op when {@code readPoisonBound == 0}.
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
    if (readPoisonFailures < readPoisonBound) {
      return;
    }
    var poison =
        new ProjectionReadPoisonException(subscriptionName.value(), readPoisonFailures, readError);
    readPoison.set(poison);
    // Stop the poll loop: `running` is its keepRunning condition, so it exits after this cycle's
    // backoff and isRunning() reports false. set() (not CAS) — this terminal stop must win, and it
    // is flipped BEFORE the log so the truthful state never depends on the log call.
    running.set(false);
    logger.error(
        "Stream subscription '{}' stopping terminally: a read/deserialization poison failed"
            + " deterministically {} time(s) at checkpoint {} and can never be read on retry —"
            + " register the missing event type or repair/erase the offending event (it may belong"
            + " to ANOTHER stream: this subscription reads the global stream and filters"
            + " afterwards), then start() the subscription again",
        subscriptionName.value(),
        readPoisonFailures,
        checkpoint.value(),
        poison);
    throw poison;
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
   * Records {@code count} delivered events and the batch delivery latency. Metric failures are
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
   * value means the subscription is degraded — still running, retrying with backoff.
   */
  public int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  private void runPollLoop() {
    try {
      // ResilientPollLoop logs failures and retries with capped exponential backoff; it exits
      // only on close() or interrupt — a transient error never silently kills the subscription.
      pollLoop.run(this::pollOnce);
    } finally {
      // Keep isRunning() truthful even if the thread exits for any reason other than close().
      running.set(false);
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
    private StreamId streamId;
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private EventListener listener;
    private SubscriptionConfig config = SubscriptionConfig.DEFAULT;
    private int fetchSize = 100;
    private StreamRuneMetrics metrics;
    private long closeJoinTimeoutMs = DEFAULT_CLOSE_JOIN_TIMEOUT_MS;
    private int readPoisonBound = DEFAULT_READ_POISON_BOUND;

    /**
     * Unique checkpoint name for this subscription. Shares the {@link OffsetStore} namespace with
     * projection names and other subscription names — required, no default, so two subscriptions on
     * the same stream can never silently share a checkpoint.
     */
    public Builder subscriptionName(String name) {
      this.subscriptionName = SubscriptionName.of(name);
      return this;
    }

    public Builder streamId(StreamId streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder eventStore(EventStore store) {
      this.eventStore = store;
      return this;
    }

    public Builder offsetStore(OffsetStore store) {
      this.offsetStore = store;
      return this;
    }

    public Builder listener(EventListener listener) {
      this.listener = listener;
      return this;
    }

    public Builder config(SubscriptionConfig config) {
      this.config = config;
      return this;
    }

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
     * How long {@link StreamEventSubscription#close()} waits for the polling thread before logging
     * a warning and returning. Package-private: tests use it to drive the join-timeout branch
     * without waiting out the 30-second production bound.
     */
    Builder closeJoinTimeoutMs(long millis) {
      this.closeJoinTimeoutMs = millis;
      return this;
    }

    /**
     * Consecutive deterministic read/deserialization poison reads at the same checkpoint tolerated
     * before the subscription stops itself terminally and exposes the error via {@link
     * ReadPoisonAware#readPoisonError()}. Defaults to {@link
     * StreamEventSubscription#DEFAULT_READ_POISON_BOUND}; {@code 0} disables the bound and keeps
     * the unbounded retry.
     */
    public Builder readPoisonBound(int bound) {
      this.readPoisonBound = bound;
      return this;
    }

    /** Builds the subscription. Requires {@code subscriptionName}. */
    public StreamEventSubscription build() {
      return new StreamEventSubscription(
          subscriptionName,
          streamId,
          eventStore,
          offsetStore,
          listener,
          config,
          fetchSize,
          metrics,
          closeJoinTimeoutMs,
          readPoisonBound);
    }
  }
}

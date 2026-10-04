package org.streamrune.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.ProjectionName;

/**
 * Manages multiple projections, each running on its own virtual thread via an internal {@link
 * ContinuousProjectionRunner}. Provides coordinated lifecycle ({@link #start()}, {@link #stop()})
 * and status reporting ({@link #status()}).
 */
public final class MultiProjectionRunner implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MultiProjectionRunner.class);
  static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(30);

  private final List<ProjectionRegistration> registrations;
  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final int fetchSize;
  private final int batchSize;
  private final SubscriptionConfig subscriptionConfig;
  private final AtomicBatchProcessor atomicProcessor;
  private final ProjectionDeadLetterStore deadLetterStore;
  private final long stopTimeoutMs;
  private final StreamRuneMetrics metrics;
  private final SubscriptionLeadership leadership;
  private final org.streamrune.core.projection.ProjectionErrorClassifier classifier;
  private final SubscriptionHealthContributor healthContributor;

  private final ConcurrentHashMap<ProjectionName, ContinuousProjectionRunner> runners =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Thread> threads = new ConcurrentHashMap<>();
  private final AtomicBoolean started = new AtomicBoolean(false);

  MultiProjectionRunner(
      List<ProjectionRegistration> registrations,
      EventStore eventStore,
      OffsetStore offsetStore,
      int fetchSize,
      int batchSize,
      SubscriptionConfig subscriptionConfig,
      AtomicBatchProcessor atomicProcessor,
      ProjectionDeadLetterStore deadLetterStore,
      Duration stopTimeout,
      StreamRuneMetrics metrics,
      SubscriptionLeadership leadership,
      org.streamrune.core.projection.ProjectionErrorClassifier classifier,
      SubscriptionHealthContributor healthContributor) {
    this.registrations = List.copyOf(registrations);
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.fetchSize = fetchSize;
    this.batchSize = batchSize;
    this.subscriptionConfig = subscriptionConfig;
    this.atomicProcessor = atomicProcessor;
    this.deadLetterStore = deadLetterStore;
    this.stopTimeoutMs = stopTimeout.toMillis();
    this.metrics = metrics;
    this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
    this.classifier =
        classifier != null
            ? classifier
            : org.streamrune.core.projection.ProjectionErrorClassifier.DEFAULT;
    this.healthContributor = healthContributor;
  }

  /**
   * Starts all registered projections, each on its own virtual thread. Logs nothing about delivery
   * modes itself: each per-registration {@code ContinuousProjectionRunner.run} logs its verdict
   * once.
   *
   * @throws IllegalStateException if already started
   */
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("MultiProjectionRunner already started");
    }

    for (var reg : registrations) {
      var runner =
          ContinuousProjectionRunner.builder()
              .eventStore(eventStore)
              .offsetStore(offsetStore)
              .fetchSize(fetchSize)
              .batchSize(batchSize)
              .subscriptionConfig(subscriptionConfig)
              .atomicProcessor(atomicProcessor)
              .errorStrategy(reg.errorStrategy())
              .deadLetterStore(deadLetterStore)
              .metrics(metrics)
              // Same leadership instance threaded into every per-registration runner: the
              // projection name is the leadership consumerName, so each name is led independently
              // (one dedicated connection can hold many advisory locks).
              .leadership(leadership)
              // Same poison-vs-transient classifier threaded into every per-registration runner
              // (mirrors how leadership is threaded); defaults to
              // ProjectionErrorClassifier.DEFAULT.
              .classifier(classifier)
              // Same health contributor threaded into every per-registration runner so each
              // projection's live subscription registers itself and reports delivery health; null
              // (no contributor) makes health tracking a no-op.
              .healthContributor(healthContributor)
              .build();

      runners.put(reg.name(), runner);

      Thread thread =
          Thread.ofVirtual()
              .name("projection-" + reg.name().value())
              .start(
                  () -> {
                    try {
                      runner.run(reg.name(), reg.projection(), reg.deliveryMode());
                    } catch (Throwable e) {
                      // Catch Throwable, not just Exception. An Error propagating out
                      // of run() (e.g. an AssertionError during catch-up) would otherwise unwind
                      // the virtual thread via the default handler and go unlogged here. The run()
                      // finally has already set state=ERROR and marked the projection terminally
                      // DOWN and resigned leadership for this name, so a standby
                      // replica takes over immediately — nothing to resign here; we only log.
                      logger.error("Projection '{}' failed", reg.name().value(), e);
                    }
                  });

      threads.put(reg.name(), thread);
    }
  }

  /**
   * Stops all projections and waits up to the configured stop timeout for each thread to exit.
   * Threads are interrupted so backoff sleeps and live-mode waits end promptly; an in-flight batch
   * is not cut short: the interrupt is held until its commit finishes, as in {@link
   * ContinuousProjectionRunner#close()}.
   *
   * <p>If a thread does not exit within the timeout (e.g. stuck inside a projection), this runner
   * stays in the started state and {@link #start()} keeps throwing {@link IllegalStateException} —
   * restarting would run a duplicate runner against the same projection names, clobbering their
   * offset rows while the old thread is still alive.
   */
  public void stop() {
    // requestStop() only flips each runner's flag (non-blocking), so all runners are signalled
    // before any join; runner.close() would interrupt and join serially per runner instead.
    for (var runner : runners.values()) {
      runner.requestStop();
    }
    boolean allStopped = true;
    for (var entry : threads.entrySet()) {
      Thread thread = entry.getValue();
      // The interrupt wakes backoff/live-mode sleeps (up to 60s — longer than the stop timeout)
      // so the flag is observed before the join times out. It goes through the runner, which
      // holds it back while the run is in a store write (a batch commit, a resign) and delivers it
      // afterwards; a thread whose run() has not claimed the runner yet is interrupted directly
      // (the stop requested above is held for that run()).
      ContinuousProjectionRunner runner = runners.get(entry.getKey());
      if (runner == null || !runner.interruptRun()) {
        thread.interrupt();
      }
      try {
        thread.join(stopTimeoutMs);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        logger.warn(
            "Interrupted while waiting for projection '{}' to stop", entry.getKey().value());
      }
      if (thread.isAlive()) {
        allStopped = false;
        logger.error(
            "Projection '{}' did not stop within {} ms — restart stays blocked so a duplicate"
                + " runner cannot clobber its offsets",
            entry.getKey().value(),
            stopTimeoutMs);
      }
    }
    if (allStopped) {
      started.set(false);
    }
  }

  /**
   * Returns current status for all projections.
   *
   * @return map of projection name to status
   */
  public Map<String, ProjectionStatus> status() {
    var result = new LinkedHashMap<String, ProjectionStatus>();
    for (var reg : registrations) {
      var runner = runners.get(reg.name());
      ProjectionState state = runner != null ? runner.state() : ProjectionState.PENDING;
      String lastError = runner != null ? runner.lastError() : null;
      var lastOffset = offsetStore.getLastOffset(reg.name());
      result.put(
          reg.name().value(), new ProjectionStatus(reg.name(), state, lastOffset, lastError));
    }
    return result;
  }

  @Override
  public void close() {
    stop();
  }

  /** Creates a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private EventStore eventStore;
    private OffsetStore offsetStore;
    private int fetchSize = 100;
    private int batchSize = 50;
    private SubscriptionConfig subscriptionConfig = SubscriptionConfig.DEFAULT;
    private AtomicBatchProcessor atomicProcessor;
    private ProjectionDeadLetterStore deadLetterStore;
    private Duration stopTimeout = DEFAULT_STOP_TIMEOUT;
    private StreamRuneMetrics metrics;
    private SubscriptionLeadership leadership = SubscriptionLeadership.NOOP;
    private org.streamrune.core.projection.ProjectionErrorClassifier classifier =
        org.streamrune.core.projection.ProjectionErrorClassifier.DEFAULT;
    private SubscriptionHealthContributor healthContributor;
    private final List<ProjectionRegistration> registrations = new ArrayList<>();

    public Builder eventStore(EventStore store) {
      this.eventStore = store;
      return this;
    }

    public Builder offsetStore(OffsetStore store) {
      this.offsetStore = store;
      return this;
    }

    public Builder fetchSize(int size) {
      this.fetchSize = size;
      return this;
    }

    public Builder batchSize(int size) {
      this.batchSize = size;
      return this;
    }

    public Builder subscriptionConfig(SubscriptionConfig config) {
      this.subscriptionConfig = config;
      return this;
    }

    /**
     * Sets the batch processor every registration on this runner commits through. Required: pass
     * the {@code JdbcProjectionRepository} your {@code TRANSACTIONAL_LOCAL} / {@code
     * EXTERNAL_EFFECT} projections write to, or {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}
     * for a runner of {@code AT_LEAST_ONCE_IDEMPOTENT} projections. {@link #build()} checks each
     * registration's declared mode against it.
     */
    public Builder atomicProcessor(AtomicBatchProcessor processor) {
      this.atomicProcessor = processor;
      return this;
    }

    public Builder deadLetterStore(ProjectionDeadLetterStore store) {
      this.deadLetterStore = store;
      return this;
    }

    /**
     * How long {@link #stop()} waits for each projection thread to exit before declaring it stuck
     * and blocking restarts. Defaults to 30 seconds.
     */
    public Builder stopTimeout(Duration timeout) {
      this.stopTimeout = timeout;
      return this;
    }

    /**
     * Sets the metrics collector propagated to every per-projection {@link
     * ContinuousProjectionRunner}. Optional; when absent (or {@code null}) projection metrics are
     * not recorded and behavior is unchanged.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    /**
     * Sets the single-active-consumer leadership coordinator threaded into every per-projection
     * {@link ContinuousProjectionRunner}. Optional; defaults to {@link SubscriptionLeadership#NOOP}
     * (always leader), so single-instance behavior is unchanged. Each projection name is led
     * independently.
     */
    public Builder leadership(SubscriptionLeadership leadership) {
      this.leadership = leadership != null ? leadership : SubscriptionLeadership.NOOP;
      return this;
    }

    /**
     * Sets the poison-vs-transient error classifier threaded into every per-projection {@link
     * ContinuousProjectionRunner} (mirrors how {@link #leadership(SubscriptionLeadership)} is
     * threaded). Optional; defaults to {@link
     * org.streamrune.core.projection.ProjectionErrorClassifier#DEFAULT} (infrastructure/SQL/IO
     * errors are TRANSIENT and retried; everything else is POISON and dead-lettered).
     */
    public Builder classifier(org.streamrune.core.projection.ProjectionErrorClassifier classifier) {
      this.classifier =
          classifier != null
              ? classifier
              : org.streamrune.core.projection.ProjectionErrorClassifier.DEFAULT;
      return this;
    }

    /**
     * Sets the subscription health contributor threaded into every per-projection {@link
     * ContinuousProjectionRunner}. Optional; when present, each projection's live subscription
     * registers itself so the health endpoint lists it and reflects its state, and delivery
     * outcomes feed the contributor. Absent (or {@code null}) makes health tracking a no-op.
     */
    public Builder healthContributor(SubscriptionHealthContributor healthContributor) {
      this.healthContributor = healthContributor;
      return this;
    }

    /** Registers a projection with the default HALT error strategy and its delivery mode. */
    public Builder register(String name, Projection projection, ProjectionDeliveryMode mode) {
      registrations.add(new ProjectionRegistration(name, projection, mode));
      return this;
    }

    /** Registers a projection with an explicit error strategy and its delivery mode. */
    public Builder register(
        String name,
        Projection projection,
        ProjectionErrorStrategy strategy,
        ProjectionDeliveryMode mode) {
      registrations.add(new ProjectionRegistration(name, projection, strategy, mode));
      return this;
    }

    public MultiProjectionRunner build() {
      if (eventStore == null) throw new IllegalArgumentException("eventStore is required");
      if (offsetStore == null) throw new IllegalArgumentException("offsetStore is required");
      if (atomicProcessor == null) {
        throw new IllegalArgumentException(
            "atomicProcessor is required: pass the JdbcProjectionRepository your"
                + " TRANSACTIONAL_LOCAL projections write to, or"
                + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
                + " AT_LEAST_ONCE_IDEMPOTENT projections");
      }
      // toMillis() < 1 also rejects a positive sub-millisecond value, which the
      // old null/negative/zero check let through — it truncated to stopTimeoutMs == 0, and
      // Thread.join(0) waits FOREVER, turning the tightest configurable stop bound into an
      // unbounded shutdown hang on a stuck projection thread. Same sentinel-inversion class as the
      // sub-ms lock-timeout band: reject, never round, so the runner only ever honours
      // a bound the caller actually wrote.
      if (stopTimeout == null || stopTimeout.toMillis() < 1) {
        throw new IllegalArgumentException(
            "stopTimeout must be non-null and at least 1ms (a sub-millisecond value would truncate"
                + " to Thread.join(0), which waits forever), got: "
                + stopTimeout);
      }
      if (registrations.isEmpty()) {
        throw new IllegalArgumentException("at least one projection must be registered");
      }

      var names = new HashSet<ProjectionName>();
      for (var reg : registrations) {
        // A name the processor cannot store read models under fails here, at startup. With
        // the JDBC repository every valid name has a table of its own, so the exact-name duplicate
        // check below is also the check that no two registrations share a read-model table.
        atomicProcessor.checkProjectionName(reg.name());
        if (!names.add(reg.name())) {
          throw new IllegalArgumentException("Duplicate projection name: " + reg.name().value());
        }
      }

      boolean hasDlq =
          registrations.stream().anyMatch(r -> r.errorStrategy() == ProjectionErrorStrategy.DLQ);
      if (hasDlq && deadLetterStore == null) {
        throw new IllegalArgumentException(
            "deadLetterStore is required when any registration uses DLQ strategy");
      }

      // Fail here rather than at start(), where the same pairing would be handed to
      // every per-registration ContinuousProjectionRunner on its own virtual thread.
      ProjectionFencingPolicy.requireFencingCapable(
          leadership, atomicProcessor, "MultiProjectionRunner");

      // Every registration shares this runner's single atomicProcessor, so each is checked
      // against its OWN declared mode; a refusal here is the same refusal the per-registration
      // ContinuousProjectionRunner would raise at run(), surfaced at build() instead of on a
      // virtual thread.
      for (var reg : registrations) {
        ProjectionDeliveryPolicy.require(
            reg.projection(),
            reg.deliveryMode(),
            atomicProcessor,
            "MultiProjectionRunner",
            reg.name().value());
      }

      return new MultiProjectionRunner(
          registrations,
          eventStore,
          offsetStore,
          fetchSize,
          batchSize,
          subscriptionConfig,
          atomicProcessor,
          deadLetterStore,
          stopTimeout,
          metrics,
          leadership,
          classifier,
          healthContributor);
    }
  }
}

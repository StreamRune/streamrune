package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCheckpointSaveException;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * A {@link ProjectionRunner} that catches up by polling the global event stream. Each fetched page
 * is handed to the projection in chunks of {@code batchSize} events, every chunk committed through
 * the {@link AtomicBatchProcessor}.
 *
 * <p>{@link #run(ProjectionName, Projection, ProjectionDeliveryMode)} drains until the projection
 * is caught up: it keeps fetching pages of up to {@code fetchSize} events and returns only when the
 * store has no more events past the projection's checkpoint — a projection that is many pages
 * behind catches up in a single call.
 *
 * <p>The processor is required: {@code JdbcProjectionRepository} for {@code TRANSACTIONAL_LOCAL} /
 * {@code EXTERNAL_EFFECT} projections, {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()} for a
 * runner of {@code AT_LEAST_ONCE_IDEMPOTENT} ones. The offset store must read the checkpoint that
 * processor advances: for a {@code JdbcProjectionRepository}, {@code PostgresReadModelCheckpoints}
 * on the same {@code DataSource} (a {@code PostgresReadModelStore} hands out the pair), never a
 * {@code PostgresOffsetStore}. The commit is unfenced (epoch 0) — this runner has no leadership.
 *
 * <p><b>Routing.</b> {@link #run(ProjectionName, Projection, ProjectionDeliveryMode,
 * CheckpointTarget)} runs a projection on a {@link CheckpointTarget} instead of the runner's own
 * pair: its position is read from the target's checkpoints and every batch commits through the
 * target's processor, which is also the processor its mode is checked against. The runner's own
 * offset store is not read for it. {@link #reset(ProjectionName, CheckpointTarget)} rewinds a
 * projection's checkpoint in a target, whether or not it has run here.
 *
 * <p>Under the nonatomic processor a checkpoint save that fails after the batch was applied ({@link
 * ProjectionCheckpointSaveException}) is retried inside {@code run()}: the runner backs off (100 ms
 * doubling to 60 s) and re-reads from the un-advanced checkpoint, so the idempotent projection
 * applies the batch again. It is never skipped and never dead-lettered.
 */
public final class PollingProjectionRunner implements ProjectionRunner {

  private static final Logger logger = LoggerFactory.getLogger(PollingProjectionRunner.class);

  /**
   * Backoff bounds for a failed checkpoint save. 100 ms, not the continuous runner's 1 s: this is
   * the unit-test and batch-job runner.
   */
  private static final Duration SAVE_RETRY_INITIAL_BACKOFF = Duration.ofMillis(100);

  private static final Duration SAVE_RETRY_MAX_BACKOFF = Duration.ofSeconds(60);

  private static final String RUNNER_NAME = "PollingProjectionRunner";

  private final EventStore eventStore;
  // The runner's own pair: the offset store and processor it was built with.
  private final RegistrationCheckpoints ownPair;
  private final int fetchSize;
  private final int batchSize;
  private final StreamRuneMetrics metrics;
  // The pair each projection last ran on, so reset() rewinds it where its run reads it.
  private final Map<ProjectionName, RegistrationCheckpoints> lastRunPairs =
      new ConcurrentHashMap<>();

  /**
   * Creates a new runner with no metrics collector.
   *
   * @param eventStore the event store to read from
   * @param offsetStore the offset store for checkpoint tracking
   * @param fetchSize how many events to fetch from the store per poll cycle
   * @param batchSize how many events to group into a single batch for the projection
   * @param atomicProcessor the atomic batch processor: a {@code JdbcProjectionRepository}, which
   *     runs a transactional projection's update and the offset save in one transaction, or {@link
   *     AtomicBatchProcessor#nonAtomicAtLeastOnce()} for a runner of {@code
   *     AT_LEAST_ONCE_IDEMPOTENT} projections
   */
  public PollingProjectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      int fetchSize,
      int batchSize,
      AtomicBatchProcessor atomicProcessor) {
    this(eventStore, offsetStore, fetchSize, batchSize, atomicProcessor, StreamRuneMetrics.NOOP);
  }

  /**
   * Creates a new runner with an explicit atomic batch processor and a metrics collector. The
   * runner records processed/failed counts and processing duration per batch, tagged with the
   * projection name.
   *
   * @param eventStore the event store to read from
   * @param offsetStore the offset store for checkpoint tracking
   * @param fetchSize how many events to fetch from the store per poll cycle
   * @param batchSize how many events to group into a single batch for the projection
   * @param atomicProcessor the atomic batch processor: a {@code JdbcProjectionRepository} or {@link
   *     AtomicBatchProcessor#nonAtomicAtLeastOnce()}
   * @param metrics the metrics collector; {@link StreamRuneMetrics#NOOP} records nothing, not
   *     {@code null}
   */
  public PollingProjectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      int fetchSize,
      int batchSize,
      AtomicBatchProcessor atomicProcessor,
      StreamRuneMetrics metrics) {
    if (eventStore == null) {
      throw new IllegalArgumentException("eventStore is required");
    }
    if (offsetStore == null) {
      throw new IllegalArgumentException("offsetStore is required");
    }
    if (fetchSize <= 0) {
      throw new IllegalArgumentException("fetchSize must be positive");
    }
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    if (atomicProcessor == null) {
      throw new IllegalArgumentException("atomicProcessor is required");
    }
    this.eventStore = eventStore;
    this.ownPair = new RegistrationCheckpoints(offsetStore, atomicProcessor, null);
    this.fetchSize = fetchSize;
    this.batchSize = batchSize;
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
  }

  @Override
  public void run(
      ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
    run(projectionName, projection, mode, ownPair);
  }

  /**
   * Catches the projection up like {@link #run(ProjectionName, Projection,
   * ProjectionDeliveryMode)}, reading its position from {@code target}'s checkpoints and committing
   * every batch through {@code target}'s processor; the runner's own pair is not used.
   *
   * @throws IllegalArgumentException if {@code target} is {@code null}, or its processor refuses
   *     the projection's name or mode
   */
  @Override
  public void run(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      CheckpointTarget target) {
    run(
        projectionName,
        projection,
        mode,
        RegistrationCheckpoints.ofTarget(
            target, RUNNER_NAME, projectionName, "run(name, projection, mode)"));
  }

  private void run(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      RegistrationCheckpoints pair) {
    AtomicBatchProcessor atomicProcessor = pair.processor();
    // A name the processor cannot store read models under fails before any event is read.
    atomicProcessor.checkProjectionName(projectionName);
    var verdict =
        ProjectionDeliveryPolicy.require(
            projection,
            mode,
            atomicProcessor,
            RUNNER_NAME,
            projectionName.value(),
            pair.targetName());
    lastRunPairs.put(projectionName, pair);
    logger.info(
        "Projection '{}' delivery mode {} on {} — {}",
        LogSanitizer.sanitizeForLog(projectionName.value()),
        mode,
        pair.processorLabel(),
        verdict.describe());
    recordDeliveryMode(projectionName, mode);
    int consecutiveSaveFailures = 0;
    while (true) {
      GlobalOffset lastOffset = pair.checkpoints().getLastOffset(projectionName);
      var events = eventStore.readGlobalStream(lastOffset, fetchSize);
      if (events.isEmpty()) {
        return;
      }

      boolean reReadFromCheckpoint = false;
      for (int i = 0; i < events.size() && !reReadFromCheckpoint; i += batchSize) {
        var batch = events.subList(i, Math.min(i + batchSize, events.size()));
        try {
          processBatch(pair, projectionName, projection, mode, batch);
          consecutiveSaveFailures = 0;
        } catch (ProjectionCheckpointSaveException saveFailed) {
          // Applied, not checkpointed, store unavailable — back off and re-read from the
          // un-advanced checkpoint; the idempotent projection applies the batch again.
          consecutiveSaveFailures++;
          var backoff =
              ResilientPollLoop.backoffDelay(
                  SAVE_RETRY_INITIAL_BACKOFF, SAVE_RETRY_MAX_BACKOFF, consecutiveSaveFailures);
          logger.warn(
              "Projection '{}' applied the batch up to {} but the checkpoint save failed; re-reading"
                  + " from the checkpoint after {}ms: {}",
              LogSanitizer.sanitizeForLog(projectionName.value()),
              saveFailed.offset().value(),
              backoff.toMillis(),
              String.valueOf(saveFailed.getCause()));
          try {
            Thread.sleep(backoff.toMillis());
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return;
          }
          reReadFromCheckpoint = true;
        }
      }

      // A short page is not the end of the stream: a store may return fewer events than asked
      // for while more are committed, and readGlobalStream withholds everything from the append
      // still in flight at the tail onwards. The offset advanced past this page, so the loop reads
      // again from there and returns only when a read yields nothing (the isEmpty() check above),
      // which keeps this method's catch-up-in-a-single-call contract.
    }
  }

  private void processBatch(
      RegistrationCheckpoints pair,
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      List<EventEnvelope> batch) {
    GlobalOffset newOffset = batch.getLast().globalOffset();
    long start = System.nanoTime();
    try {
      // Only a registration that writes through the handed repository has a read model in the
      // processor's store to prepare.
      if (mode.writesInCheckpointTransaction()) {
        pair.processor().prepareReadModel(projectionName);
      }
      pair.processor()
          .executeAtomically(
              projectionName,
              batch,
              newOffset,
              // Single-runner mode: no leadership on this runner, so the commit is unfenced by
              // design.
              0L,
              // Only a TRANSACTIONAL_LOCAL / EXTERNAL_EFFECT registration may write inside the
              // processor's transaction; an AT_LEAST_ONCE_IDEMPOTENT one is handed null.
              txRepository ->
                  projection.process(
                      batch, mode.writesInCheckpointTransaction() ? txRepository : null),
              pair.checkpoints());
      recordProcessed(projectionName, batch.size());
    } catch (RuntimeException e) {
      recordFailed(projectionName);
      throw e;
    } finally {
      recordDuration(projectionName, System.nanoTime() - start);
    }
  }

  /**
   * Records {@code count} processed events; metric failures never disrupt projection processing.
   */
  private void recordProcessed(ProjectionName projectionName, int count) {
    try {
      for (int i = 0; i < count; i++) {
        metrics.recordProjectionProcessed(projectionName);
      }
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionProcessed failed for projection '{}'",
          projectionName.value(),
          e);
    }
  }

  private void recordFailed(ProjectionName projectionName) {
    try {
      metrics.recordProjectionFailed(projectionName);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionFailed failed for projection '{}'", projectionName.value(), e);
    }
  }

  /** Records the delivery-mode gauge; metric failures never disrupt projection processing. */
  private void recordDeliveryMode(ProjectionName projectionName, ProjectionDeliveryMode mode) {
    try {
      metrics.recordProjectionDeliveryMode(projectionName, mode);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDeliveryMode failed for projection '{}'",
          LogSanitizer.sanitizeForLog(projectionName.value()),
          e);
    }
  }

  private void recordDuration(ProjectionName projectionName, long durationNanos) {
    try {
      metrics.recordProjectionDuration(projectionName, durationNanos);
    } catch (RuntimeException e) {
      logger.warn(
          "Metrics recordProjectionDuration failed for projection '{}'", projectionName.value(), e);
    }
  }

  /**
   * Rewinds the projection's checkpoint where its last run on this instance read it: the target's
   * checkpoints after a {@link #run(ProjectionName, Projection, ProjectionDeliveryMode,
   * CheckpointTarget) routed run}, otherwise the runner's own offset store. It knows only the
   * projections that have run on this instance: a routed projection that has not run here yet (a
   * fresh process rebuilding before its first run) would be rewound in the runner's own offset
   * store, where it has no checkpoint — reset it with {@link #reset(ProjectionName,
   * CheckpointTarget)}. The rewind uses the unguarded {@link OffsetStore#reset} ({@code
   * saveOffset(initial())} would be no-op'd by the monotonic guard) and is read back, so a reset
   * that returns without rewinding fails loudly.
   *
   * @throws IllegalStateException if the checkpoint is not at {@link GlobalOffset#initial()} after
   *     the reset
   */
  @Override
  public void reset(ProjectionName projectionName) {
    lastRunPairs.getOrDefault(projectionName, ownPair).reset(projectionName);
  }

  /**
   * Rewinds the projection's checkpoint in {@code target}'s checkpoints, whether or not it has run
   * on this instance; the runner's own offset store is not touched. Unguarded and read back like
   * {@link #reset(ProjectionName)}.
   *
   * @throws IllegalArgumentException if {@code target} is {@code null}
   * @throws IllegalStateException if the checkpoint is not at {@link GlobalOffset#initial()} after
   *     the reset
   */
  @Override
  public void reset(ProjectionName projectionName, CheckpointTarget target) {
    RegistrationCheckpoints.ofTarget(target, RUNNER_NAME, projectionName, "reset(name)")
        .reset(projectionName);
  }
}

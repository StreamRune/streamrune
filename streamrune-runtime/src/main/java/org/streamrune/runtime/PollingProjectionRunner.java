package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
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
 * runner of {@code AT_LEAST_ONCE_IDEMPOTENT} ones. The commit is unfenced (epoch 0) — this runner
 * has no leadership.
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

  private final EventStore eventStore;
  private final OffsetStore offsetStore;
  private final int fetchSize;
  private final int batchSize;
  private final AtomicBatchProcessor atomicProcessor;
  private final StreamRuneMetrics metrics;

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
    this(eventStore, offsetStore, fetchSize, batchSize, atomicProcessor, null);
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
   * @param metrics the metrics collector; {@code null} disables metrics (behavior unchanged)
   */
  public PollingProjectionRunner(
      EventStore eventStore,
      OffsetStore offsetStore,
      int fetchSize,
      int batchSize,
      AtomicBatchProcessor atomicProcessor,
      StreamRuneMetrics metrics) {
    if (eventStore == null) throw new IllegalArgumentException("eventStore is required");
    if (offsetStore == null) throw new IllegalArgumentException("offsetStore is required");
    if (fetchSize <= 0) throw new IllegalArgumentException("fetchSize must be positive");
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");
    if (atomicProcessor == null) throw new IllegalArgumentException("atomicProcessor is required");
    this.eventStore = eventStore;
    this.offsetStore = offsetStore;
    this.fetchSize = fetchSize;
    this.batchSize = batchSize;
    this.atomicProcessor = atomicProcessor;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
  }

  @Override
  public void run(
      ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
    // A name the processor cannot store read models under fails before any event is read.
    atomicProcessor.checkProjectionName(projectionName);
    var verdict =
        ProjectionDeliveryPolicy.require(
            projection, mode, atomicProcessor, "PollingProjectionRunner", projectionName.value());
    logger.info(
        "Projection '{}' delivery mode {} on {} — {}",
        LogSanitizer.sanitizeForLog(projectionName.value()),
        mode,
        ProjectionDeliveryPolicy.processorLabel(atomicProcessor),
        verdict.describe());
    recordDeliveryMode(projectionName, mode);
    int consecutiveSaveFailures = 0;
    while (true) {
      GlobalOffset lastOffset = offsetStore.getLastOffset(projectionName);
      var events = eventStore.readGlobalStream(lastOffset, fetchSize);
      if (events.isEmpty()) return;

      boolean reReadFromCheckpoint = false;
      for (int i = 0; i < events.size() && !reReadFromCheckpoint; i += batchSize) {
        var batch = events.subList(i, Math.min(i + batchSize, events.size()));
        try {
          processBatch(projectionName, projection, mode, batch);
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

      // Do NOT treat a short page as end-of-stream. readGlobalStream's contiguity guard
      // returns only the contiguous prefix when a permanent hole (an offset no committed event
      // holds — the store does not promise contiguity) falls inside a page, so `events.size() <
      // fetchSize` may just mean a hole truncated this page, not that the tail was reached.
      // Returning here would leave the read model silently incomplete past the first hole,
      // breaking this method's catch-up-in-a-single-call contract. The offset advanced past this
      // page, so we loop and re-read from beyond the hole; the loop returns only when a read
      // genuinely yields nothing (the isEmpty() check above), which correctly excludes the single
      // uncommitted in-flight tail.
    }
  }

  private void processBatch(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      List<EventEnvelope> batch) {
    GlobalOffset newOffset = batch.getLast().globalOffset();
    long start = System.nanoTime();
    try {
      atomicProcessor.executeAtomically(
          projectionName,
          batch,
          newOffset,
          // Single-runner mode: no leadership on this runner, so the commit is unfenced by design.
          0L,
          // Only a TRANSACTIONAL_LOCAL / EXTERNAL_EFFECT registration may write inside the
          // processor's transaction; an AT_LEAST_ONCE_IDEMPOTENT one is handed null.
          txRepository ->
              projection.process(batch, mode.writesInCheckpointTransaction() ? txRepository : null),
          offsetStore);
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

  @Override
  public void reset(ProjectionName projectionName) {
    // Unguarded rewind: saveOffset(initial()) would be no-op'd by the monotonic offset guard, so a
    // reset must use the dedicated OffsetStore.reset() path (see OffsetStore contract).
    offsetStore.reset(projectionName);
    // Defense-in-depth: even a correctly-overridden reset() could have a latent bug (e.g. an
    // unguarded write that targets the wrong row/key) that leaves the checkpoint un-rewound.
    // Verify the rewind actually took before declaring the reset successful, converting a silent
    // partial rewind into a loud failure instead of a rebuild that quietly resumes from the old
    // offset.
    GlobalOffset after = offsetStore.getLastOffset(projectionName);
    if (!GlobalOffset.initial().equals(after)) {
      throw new IllegalStateException(
          "OffsetStore.reset(\""
              + projectionName.value()
              + "\") returned but the checkpoint did not rewind to initial (still at "
              + after.value()
              + "); this OffsetStore's reset() is not performing an unguarded rewind as required by"
              + " the OffsetStore contract.");
    }
  }
}

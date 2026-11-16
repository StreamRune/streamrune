package org.streamrune.runtime;

import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * The checkpoint pair one registration of a multi-registration runner ({@link
 * MultiProjectionRunner}, {@link ScheduledProjectionRunner}) reads its position from and commits
 * through: the registration's own {@link CheckpointTarget}, or the runner's default pair (its
 * runner-wide {@code offsetStore} and {@code atomicProcessor}). Both halves always come from the
 * same source — a target's processor is never paired with the runner-wide offset store, nor the
 * reverse — because a processor's guards (epoch fence, overlap and monotonic checks) only protect
 * the checkpoint it writes itself.
 *
 * @param checkpoints the offset store the registration's position is read from and reset in
 * @param processor the processor that commits the registration's batches and advances {@code
 *     checkpoints}
 * @param targetName the target's name, or {@code null} for the runner's default pair
 */
record RegistrationCheckpoints(
    OffsetStore checkpoints, AtomicBatchProcessor processor, String targetName) {

  /**
   * Refuses half a runner-wide default pair: an offset store without a processor, or a processor
   * without an offset store. Both or neither; neither means every registration must bring a target.
   *
   * @param offsetStore the runner-wide offset store, or {@code null}
   * @param atomicProcessor the runner-wide processor, or {@code null}
   * @throws IllegalArgumentException if exactly one of the two is set
   */
  static void requireWholeDefaultPair(
      OffsetStore offsetStore, AtomicBatchProcessor atomicProcessor) {
    if (offsetStore != null && atomicProcessor == null) {
      throw new IllegalArgumentException(
          "atomicProcessor is required: pass the JdbcProjectionRepository your"
              + " TRANSACTIONAL_LOCAL projections write to, or"
              + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
              + " AT_LEAST_ONCE_IDEMPOTENT projections");
    }
    if (offsetStore == null && atomicProcessor != null) {
      throw new IllegalArgumentException(
          "offsetStore is required: the runner-wide atomicProcessor commits the checkpoints of"
              + " every registration without a CheckpointTarget, and those checkpoints are read"
              + " from the runner-wide offsetStore. Set both, or neither and register every"
              + " projection with a CheckpointTarget.");
    }
  }

  /**
   * Resolves one registration's pair: its {@code target} when it has one, otherwise the runner's
   * default pair.
   *
   * @param target the registration's target, or {@code null}
   * @param defaultCheckpoints the runner-wide offset store, or {@code null}
   * @param defaultProcessor the runner-wide processor, or {@code null}
   * @param runnerName the runner type, for the message
   * @param projectionName the registration's name, for the message
   * @return the registration's pair
   * @throws IllegalArgumentException if the registration has no target and the runner no default
   *     pair
   */
  static RegistrationCheckpoints resolve(
      CheckpointTarget target,
      OffsetStore defaultCheckpoints,
      AtomicBatchProcessor defaultProcessor,
      String runnerName,
      ProjectionName projectionName) {
    if (target != null) {
      return new RegistrationCheckpoints(target.checkpoints(), target.processor(), target.name());
    }
    if (defaultCheckpoints == null || defaultProcessor == null) {
      throw new IllegalArgumentException(
          runnerName
              + " projection '"
              + LogSanitizer.sanitizeForLog(projectionName.value())
              + "' has no checkpoint source: register it with a CheckpointTarget, or give the"
              + " runner a default pair — offsetStore and atomicProcessor (the"
              + " JdbcProjectionRepository your TRANSACTIONAL_LOCAL projections write to, or"
              + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for AT_LEAST_ONCE_IDEMPOTENT"
              + " projections)");
    }
    return new RegistrationCheckpoints(defaultCheckpoints, defaultProcessor, null);
  }

  /** The processor's label for logs and messages, naming the target when there is one. */
  String processorLabel() {
    return ProjectionDeliveryPolicy.processorLabel(processor, targetName);
  }

  /**
   * Rewinds {@code projectionName}'s checkpoint in {@link #checkpoints()} through the unguarded
   * {@link OffsetStore#reset} (a {@code saveOffset(initial())} would be no-op'd by the monotonic
   * guard), then reads it back: a reset that returns without rewinding fails loudly instead of
   * letting a rebuild quietly resume from the old offset.
   *
   * @param projectionName the registration to rewind
   * @throws IllegalStateException if the checkpoint is not at {@link GlobalOffset#initial()} after
   *     the reset
   */
  void reset(ProjectionName projectionName) {
    checkpoints.reset(projectionName);
    GlobalOffset after = checkpoints.getLastOffset(projectionName);
    if (!GlobalOffset.initial().equals(after)) {
      throw new IllegalStateException(
          "OffsetStore.reset(\""
              + LogSanitizer.sanitizeForLog(projectionName.value())
              + "\") of "
              + (targetName == null
                  ? "the runner's default offset store"
                  : "checkpoint target '" + LogSanitizer.sanitizeForLog(targetName) + "'")
              + " returned but the checkpoint did not rewind to initial (still at "
              + after.value()
              + "); this OffsetStore's reset() is not performing an unguarded rewind as required by"
              + " the OffsetStore contract.");
    }
  }
}

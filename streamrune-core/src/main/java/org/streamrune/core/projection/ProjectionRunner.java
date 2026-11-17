package org.streamrune.core.projection;

import java.util.Objects;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/** Drives a projection by feeding it events from the event store. */
public interface ProjectionRunner {

  /**
   * Runs a catch-up cycle: reads events from the last stored offset, processes them in batches, and
   * saves the offset after each batch. The mode is a property of what is being run, so it rides the
   * call: the runner checks it against its processor and the projection before it reads a single
   * event ({@code ProjectionDeliveryPolicy}), hands the projection the processor's
   * transaction-scoped repository only when {@link
   * ProjectionDeliveryMode#writesInCheckpointTransaction()} is {@code true}, and logs the verdict
   * once.
   *
   * @param projectionName unique name for offset tracking
   * @param projection the projection to drive
   * @param mode the delivery guarantee this projection declares; required
   */
  void run(ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode);

  /**
   * Runs the projection like {@link #run(ProjectionName, Projection, ProjectionDeliveryMode)}, but
   * reads its checkpoint from and commits it through {@code target} instead of the pair the runner
   * was built with. The target is the registration's explicit declaration: a {@link
   * ReadModelStore#checkpointTarget()} for a projection whose read models live in that store, or
   * the event store's {@link OffsetStore#subscriptionCheckpoint()} for an at-least-once projection
   * routed to no store. The runner checks {@code mode} against the target's processor, never
   * against its own.
   *
   * <p>The default refuses with {@link UnsupportedOperationException}: a runner that only knows its
   * own pair must not run the projection there instead, where its checkpoint is not.
   *
   * @param projectionName unique name for offset tracking
   * @param projection the projection to drive
   * @param mode the delivery guarantee this projection declares; required
   * @param target the checkpoints this projection's position is read from and the processor that
   *     commits it; required
   * @throws UnsupportedOperationException if this runner cannot route a projection to a target
   */
  default void run(
      ProjectionName projectionName,
      Projection projection,
      ProjectionDeliveryMode mode,
      CheckpointTarget target) {
    Objects.requireNonNull(projectionName, "projectionName is required");
    Objects.requireNonNull(target, "target is required");
    throw new UnsupportedOperationException(
        getClass().getName()
            + " cannot run projection '"
            + LogSanitizer.sanitizeForLog(projectionName.value())
            + "' on checkpoint target '"
            + LogSanitizer.sanitizeForLog(target.name())
            + "': it reads and commits every projection through the checkpoints and processor it"
            + " was built with. Use a runner that routes a registration to its target"
            + " (ContinuousProjectionRunner, PollingProjectionRunner), or build this runner with the"
            + " target's checkpoints and processor and call run(name, projection, mode).");
  }

  /**
   * Resets the projection offset to zero so the projection replays from the beginning on the next
   * run.
   *
   * <p><b>The offset is all this resets.</b> Existing read models and {@link
   * ProjectionDeadLetterStore} entries for the projection are left untouched. Replaying upserts
   * over existing read models leaves behind rows the current event history would not produce — for
   * example after events were deleted or crypto-shredded for GDPR erasure. A caller that needs a
   * clean rebuild must delete the projection's read models (via {@link ProjectionRepository}) and
   * prune its dead-letter entries before the next {@link #run} cycle.
   *
   * <p>Calling this concurrently with an in-flight {@link #run} cycle has implementation-defined
   * results: the running cycle may save a higher offset after the reset, undoing it. Reset while
   * the projection is not running.
   *
   * @param projectionName unique name of the projection to reset
   */
  void reset(ProjectionName projectionName);
}

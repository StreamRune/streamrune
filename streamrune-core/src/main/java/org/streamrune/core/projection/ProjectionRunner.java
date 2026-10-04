package org.streamrune.core.projection;

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
   * Resets the projection offset to zero so the projection replays from the beginning on the next
   * run.
   *
   * <p><b>The offset is all this resets.</b> Existing read models and {@link
   * ProjectionDeadLetterStore} entries for the projection are left untouched. Replaying upserts
   * over existing read models leaves behind rows the current event history would no longer produce
   * — for example after events were deleted or crypto-shredded for GDPR erasure. A caller that
   * needs a clean rebuild must delete the projection's read models (via {@link
   * ProjectionRepository}) and prune its dead-letter entries before the next {@link #run} cycle.
   *
   * <p>Calling this concurrently with an in-flight {@link #run} cycle has implementation-defined
   * results: the running cycle may save a higher offset after the reset, undoing it. Reset while
   * the projection is not running.
   *
   * @param projectionName unique name of the projection to reset
   */
  void reset(ProjectionName projectionName);
}

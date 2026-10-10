package org.streamrune.runtime;

import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.types.LogSanitizer;

/**
 * Terminal signal that a projection was halted because its {@code AtomicBatchProcessor} rejected
 * two commits in a row, through the overlap or the monotonic guard, while the checkpoint the runner
 * reads from its {@code OffsetStore} did not move. The processor holds a checkpoint ahead of the
 * one the runner reads, so the runner re-reads the same batch and the processor rejects it again,
 * for as long as the process runs. That is a wiring error, not a leadership change.
 *
 * <p>One such rejection is expected and is not this error: a commit whose acknowledgement was lost
 * after the server applied it is rejected when it is retried, and the re-read that follows starts
 * after the moved checkpoint. A rejection by the epoch fence is not this error either: it names a
 * newer leader, and the runner stands by.
 *
 * <p>Recovery is operational: make the runner's {@code OffsetStore} read the checkpoint its {@code
 * AtomicBatchProcessor} advances — for {@code JdbcProjectionRepository}, a {@code
 * PostgresOffsetStore} over the same {@code DataSource} — and restart the projection, which resumes
 * from the last committed checkpoint.
 */
public final class ProjectionCheckpointDivergedException extends RuntimeException {

  /**
   * Creates the halt signal for one projection.
   *
   * @param projectionName the projection whose commits are rejected
   * @param checkpoint the checkpoint the runner's {@code OffsetStore} returned at both rejections
   * @param rejection the second rejection
   */
  ProjectionCheckpointDivergedException(
      String projectionName, long checkpoint, ProjectionCommitFencedException rejection) {
    super(
        "Projection '"
            + LogSanitizer.sanitizeForLog(projectionName)
            + "' halted: its AtomicBatchProcessor rejected two commits in a row ("
            + rejection.guard()
            + " guard) while the checkpoint this runner reads from its OffsetStore stayed at "
            + checkpoint
            + ". The processor's checkpoint is ahead of the one the runner reads, so every batch"
            + " read from "
            + checkpoint
            + " is rejected and no retry can succeed. Likely causes: (1) the AtomicBatchProcessor"
            + " and the OffsetStore are not over the same DataSource (or the same projection_offset"
            + " table) — for example a JdbcProjectionRepository over a read-model DataSource with a"
            + " PostgresOffsetStore over the primary one; (2) two runners are registered under this"
            + " projection name and commit through this processor while reading their checkpoint"
            + " from different OffsetStores. Give the runner an OffsetStore over the processor's"
            + " DataSource, register the projection name once, and restart the projection. Last"
            + " rejection: "
            + rejection.getMessage(),
        rejection);
  }
}

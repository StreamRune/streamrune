package org.streamrune.runtime.jdbc;

import org.streamrune.core.Internal;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * The pure decisions of the checkpoint-row fence that guards a projection commit against
 * split-brain double-apply and lost events: each backend's fence reads the checkpoint row under its
 * own lock and hands the stored values here. Each rejection is a {@link
 * ProjectionCommitFencedException} naming its {@link ProjectionCommitFencedException.Guard}; the
 * caller rolls its transaction back, so neither its writes nor the checkpoint move.
 *
 * <p><b>The check order is a contract.</b> A caller runs {@link #rejectStaleEpoch} before {@link
 * #rejectOverlappingBatch}, so a superseded leader whose batch also overlaps is reported as fenced
 * out by the epoch, not as an overlap. The monotonic guard is the backend's statement (a {@code
 * 0}-row advance); {@link #monotonicRejection} builds its exception and the caller throws it.
 *
 * <p>No I/O, no state: the caller holds the lock, runs the statements and throws where it threw
 * before (a backend whose write callback must not throw a domain exception calls {@link
 * #rejectEpochRegression} after the callback returned).
 *
 * @hidden
 */
@Internal
public final class CheckpointFenceRules {

  private CheckpointFenceRules() {}

  /**
   * Fences out a commit whose caller epoch is below the epoch stored on the checkpoint row — a
   * superseded leader whose lease a newer one took over (bumping the epoch). A {@code fencingEpoch}
   * of {@code 0} is unfenced and always passes, so a single-node deployment commits unfenced.
   *
   * @param name the projection
   * @param fencingEpoch the caller's leadership epoch
   * @param storedEpoch the epoch stored on the checkpoint row
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#EPOCH_FENCE} on a superseded epoch
   */
  public static void rejectStaleEpoch(ProjectionName name, long fencingEpoch, long storedEpoch) {
    if (fencingEpoch != 0L && fencingEpoch < storedEpoch) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.EPOCH_FENCE,
          "Projection '"
              + shown(name)
              + "' commit fenced out: caller epoch "
              + fencingEpoch
              + " is below the stored epoch "
              + storedEpoch
              + " — a newer leader has taken over the lease. Rolling back the stale leader's write"
              + " (epoch fencing).");
    }
  }

  /**
   * Rejects a batch whose first event offset is at or before the committed checkpoint — it overlaps
   * events already applied by another writer (a concurrent/failed-over stale leader or a
   * redelivery). A forward batch always starts strictly after the checkpoint (the runners read the
   * global stream from the stored offset); an overlapping start means part of the batch would be
   * applied a second time. An empty batch ({@code null} first offset) is governed solely by the
   * monotonic guard on the advance.
   *
   * @param name the projection
   * @param firstOffsetOfBatch the global offset of the batch's first event, or {@code null} for an
   *     empty batch
   * @param storedOffset the offset stored on the checkpoint row
   * @throws ProjectionCommitFencedException with {@link
   *     ProjectionCommitFencedException.Guard#OVERLAP} on an overlapping batch
   */
  public static void rejectOverlappingBatch(
      ProjectionName name, GlobalOffset firstOffsetOfBatch, long storedOffset) {
    if (firstOffsetOfBatch == null) {
      return;
    }
    long firstOffset = firstOffsetOfBatch.value();
    if (firstOffset <= storedOffset) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.OVERLAP,
          "Projection '"
              + shown(name)
              + "' batch starting at offset "
              + firstOffset
              + " overlaps the committed checkpoint ("
              + storedOffset
              + "): those events were already applied by another writer (a concurrent or"
              + " failed-over stale leader, or a redelivery). Rolling back so the read-model write is"
              + " not applied a second time.");
    }
  }

  /**
   * The rejection of an offset advance the monotonic guard refused (the advance updated no row: the
   * stored offset is already at or past {@code newOffset}, so committing would apply the batch a
   * second time). Returned, not thrown: the caller throws it where its statement ran.
   *
   * @param name the projection
   * @param newOffset the offset the batch advanced to
   * @return a {@link ProjectionCommitFencedException} with {@link
   *     ProjectionCommitFencedException.Guard#MONOTONIC}
   */
  public static ProjectionCommitFencedException monotonicRejection(
      ProjectionName name, GlobalOffset newOffset) {
    return new ProjectionCommitFencedException(
        ProjectionCommitFencedException.Guard.MONOTONIC,
        "Projection '"
            + shown(name)
            + "' offset advance to "
            + newOffset.value()
            + " was rejected by the monotonic guard (stored offset already at/past it): the"
            + " batch is already applied. Rolling back so the read-model write is not committed"
            + " a second time.");
  }

  /**
   * Refuses a takeover stamp that wrote nothing because the row already stores a strictly greater
   * epoch: the caller's epoch is not authoritative, and running under it would have every commit
   * fenced out. An equal stored epoch (the ordinary re-stamp) passes.
   *
   * @param name the projection
   * @param fencingEpoch the newly acquired leadership epoch
   * @param storedEpoch the epoch stored on the checkpoint row
   * @throws ProjectionEpochRegressionException if {@code storedEpoch > fencingEpoch}
   */
  public static void rejectEpochRegression(
      ProjectionName name, long fencingEpoch, long storedEpoch) {
    if (storedEpoch > fencingEpoch) {
      throw new ProjectionEpochRegressionException(name, fencingEpoch, storedEpoch);
    }
  }

  /**
   * A projection name rendered for an exception message: caller text, so sanitized.
   *
   * @param name the projection
   * @return the sanitized name
   */
  public static String shown(ProjectionName name) {
    return LogSanitizer.sanitizeForLog(name.value());
  }
}

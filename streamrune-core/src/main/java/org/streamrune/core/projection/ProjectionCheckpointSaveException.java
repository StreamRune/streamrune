package org.streamrune.core.projection;

import java.util.Objects;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Thrown by {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()} when {@link OffsetStore#saveOffset}
 * fails AFTER the updater returned: the batch is applied, the checkpoint did not move. It names the
 * one state only the nonatomic processor can produce, so the runners treat it as what it is — the
 * store is unavailable, the range is not poison: WARN, back off, re-read from the checkpoint,
 * re-apply (at-least-once), never SKIP and never dead-letter. A transactional processor never
 * throws it: its save runs inside the transaction and a failure rolls everything back.
 */
public class ProjectionCheckpointSaveException extends RuntimeException {

  private final transient ProjectionName projectionName;
  private final transient GlobalOffset offset;

  /**
   * Creates the exception for a batch that was applied but whose checkpoint did not advance.
   *
   * @param projectionName the projection whose checkpoint save failed; required
   * @param offset the offset the batch was applied up to; required
   * @param cause the offset store's failure
   */
  public ProjectionCheckpointSaveException(
      ProjectionName projectionName, GlobalOffset offset, Throwable cause) {
    super(
        "projection '"
            + LogSanitizer.sanitizeForLog(
                Objects.requireNonNull(projectionName, "projectionName").value())
            + "': batch up to offset "
            + Objects.requireNonNull(offset, "offset").value()
            + " was applied but the checkpoint save failed — the batch will be re-read and"
            + " re-applied (at-least-once)",
        cause);
    this.projectionName = projectionName;
    this.offset = offset;
  }

  /**
   * The projection whose checkpoint did not advance.
   *
   * @return the projection name
   */
  public ProjectionName projectionName() {
    return projectionName;
  }

  /**
   * The offset the batch was applied up to and the checkpoint should have reached.
   *
   * @return the un-saved checkpoint offset
   */
  public GlobalOffset offset() {
    return offset;
  }
}

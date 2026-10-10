package org.streamrune.core.projection;

import org.streamrune.core.OptimisticLockException;

/**
 * Signals that an {@link AtomicBatchProcessor#executeAtomically atomic projection commit} was
 * rejected by one of the framework's own commit-boundary guards — the epoch fence (a newer leader
 * took over the lease), the first-offset overlap guard, or the monotonic offset guard —
 * <em>before</em> any read-model row was written. It is a leadership/CAS signal, never a
 * projection-logic failure; {@link #guard()} says which guard rejected the commit.
 *
 * <p><b>Why a dedicated subtype.</b> A projection may legitimately use the versioned read-model
 * save API ({@code findById(..., LockMode.OPTIMISTIC)} + {@code save(name, id, model,
 * expectedVersion)}), which throws a plain {@link OptimisticLockException} on a version mismatch —
 * <em>inside</em> the updater lambda, i.e. inside the very same {@code executeAtomically} call. The
 * projection runners treat a fence rejection as "abandon the batch, do not advance, stand by and
 * re-read", which is correct for a genuine fence but catastrophic for a user version conflict: it
 * bypasses the whole error-strategy machinery (retry bound, SKIP/DLQ/HALT, health escalation), so a
 * persistent conflict silently freezes the projection instead of reaching HALT/DLQ. By throwing
 * this subtype from the framework guards and narrowing the runner catches to it, a user {@code
 * OptimisticLockException} flows to the configured error strategy while genuine fence/CAS
 * rejections keep the abandon-and-re-read semantics.
 *
 * <p>Extends {@link OptimisticLockException} so any broad handler that already treats a projection
 * commit conflict as retryable continues to see it; the runners catch this narrower type explicitly
 * to separate it from a user conflict.
 */
public class ProjectionCommitFencedException extends OptimisticLockException {

  /** The commit-boundary guard that rejected the commit. */
  public enum Guard {
    /**
     * The caller's epoch is below the epoch stored on the checkpoint: a newer leader holds the
     * lease. Standing by is the answer.
     */
    EPOCH_FENCE,
    /**
     * The batch starts at or before the committed checkpoint: the checkpoint the processor holds is
     * ahead of the one the caller read its batch from.
     */
    OVERLAP,
    /** The offset advance does not move the committed checkpoint forward. */
    MONOTONIC
  }

  private final Guard guard;

  /**
   * Creates a fence-rejection exception with a pre-formatted message.
   *
   * @param guard the guard that rejected the commit
   * @param message the detail message describing the rejection
   * @throws IllegalArgumentException if {@code guard} is {@code null}
   */
  public ProjectionCommitFencedException(Guard guard, String message) {
    super(message);
    if (guard == null) {
      throw new IllegalArgumentException("guard is required");
    }
    this.guard = guard;
  }

  /**
   * The guard that rejected the commit. The runners stand by on {@link Guard#EPOCH_FENCE}, which
   * names a newer leader. {@link Guard#OVERLAP} and {@link Guard#MONOTONIC} name a checkpoint that
   * is ahead of the one the caller read: once, that is a commit whose acknowledgement was lost and
   * the re-read resolves it; again at the same checkpoint, the caller's {@link OffsetStore} is not
   * reading the checkpoint this processor advances, and the runners stop with a configuration
   * error.
   *
   * @return the guard that rejected the commit
   */
  public Guard guard() {
    return guard;
  }
}

package org.streamrune.core.projection;

import org.streamrune.core.OptimisticLockException;

/**
 * Signals that an {@link AtomicBatchProcessor#executeAtomically atomic projection commit} was
 * rejected by one of the framework's own commit-boundary guards — the epoch fence (a newer leader
 * took over the lease), the first-offset overlap guard, or the monotonic offset guard —
 * <em>before</em> any read-model row was written. It is a leadership/CAS signal, never a
 * projection-logic failure.
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

  /**
   * Creates a fence-rejection exception with a pre-formatted message.
   *
   * @param message the detail message describing which guard rejected the commit
   */
  public ProjectionCommitFencedException(String message) {
    super(message);
  }
}

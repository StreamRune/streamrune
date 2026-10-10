package org.streamrune.core.projection;

/**
 * Defines how a projection runner handles a failed batch.
 *
 * <ul>
 *   <li>{@link #HALT} — retry the batch from the checkpoint, and stop the runner when it keeps
 *       failing.
 *   <li>{@link #SKIP} — log the failure, move the checkpoint past the batch and continue.
 *   <li>{@link #DLQ} — record the failed range in the dead-letter store, move the checkpoint past
 *       it and continue.
 * </ul>
 */
public enum ProjectionErrorStrategy {

  /**
   * Retry the failed batch from the checkpoint with backoff, and stop the projection runner with an
   * error once the same batch has failed a bounded number of consecutive times.
   */
  HALT,

  /**
   * Move the checkpoint past the failed batch and continue; the range is recorded nowhere. Under
   * {@link ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} and {@link
   * ProjectionDeliveryMode#EXTERNAL_EFFECT} the failed batch's writes roll back, so the whole batch
   * is missing from the read model. Under {@link ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT}
   * the writes the projection made before it threw stand: the skipped batch may be <b>partially
   * applied</b>. Prefer {@link #DLQ} there, which records the range for a replay that an idempotent
   * projection completes.
   */
  SKIP,

  /**
   * Record the failed batch's offset range in the dead-letter store, move the checkpoint past it
   * and continue. A transient failure is retried from the checkpoint first; {@code
   * ProjectionDeadLetterReplayer} applies a recorded range later.
   */
  DLQ
}

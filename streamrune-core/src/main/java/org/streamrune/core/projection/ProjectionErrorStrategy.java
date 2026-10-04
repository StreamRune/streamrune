package org.streamrune.core.projection;

/**
 * Defines how a projection runner handles a failed batch.
 *
 * <ul>
 *   <li>{@link #HALT} — stop processing and surface the error immediately.
 *   <li>{@link #SKIP} — log the failure and continue with the next batch.
 *   <li>{@link #DLQ} — persist the failed batch to the dead-letter store and continue.
 * </ul>
 */
public enum ProjectionErrorStrategy {

  /** Stop the projection runner on the first failure. */
  HALT,

  /** Skip the failed batch and continue processing. */
  SKIP,

  /** Send the failed batch to the dead-letter store and continue processing. */
  DLQ
}

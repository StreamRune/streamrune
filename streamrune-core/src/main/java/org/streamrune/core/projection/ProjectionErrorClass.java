package org.streamrune.core.projection;

/**
 * Classifies a projection processing failure as either a recoverable infrastructure blip or a
 * deterministic bad batch, so a runner under the {@link ProjectionErrorStrategy#DLQ} strategy can
 * retry the former and dead-letter the latter. Mirrors {@code SagaRunner}'s poison-vs-transient
 * split.
 */
public enum ProjectionErrorClass {

  /**
   * A recoverable infrastructure failure (store I/O, SQL, IO) that a bounded retry is likely to
   * clear. The batch is retried with backoff and only dead-lettered if the bound is exhausted.
   */
  TRANSIENT,

  /**
   * A deterministic failure (projection-logic bug, mapping error) that retrying cannot fix. The
   * batch is dead-lettered after the first failure — no retry storm.
   */
  POISON
}

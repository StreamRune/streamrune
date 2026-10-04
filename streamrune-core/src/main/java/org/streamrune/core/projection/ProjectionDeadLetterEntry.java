package org.streamrune.core.projection;

import java.time.Instant;
import java.util.Objects;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Represents a failed projection batch persisted in the dead-letter store.
 *
 * @param projectionName the name of the projection that failed; must not be null
 * @param fromOffset the inclusive start offset of the failed batch; must not be null
 * @param toOffset the inclusive end offset of the failed batch; must not be null
 * @param batchSize the number of events in the failed batch; must be > 0
 * @param errorType the fully-qualified class name of the exception; must not be null or blank
 * @param errorMessage the detail message of the exception; may be null
 * @param attempts the number of processing attempts made
 * @param failedAt the timestamp of the last failure; must not be null
 */
public record ProjectionDeadLetterEntry(
    ProjectionName projectionName,
    GlobalOffset fromOffset,
    GlobalOffset toOffset,
    int batchSize,
    String errorType,
    String errorMessage,
    int attempts,
    Instant failedAt) {

  /** Compact constructor — validates required fields. */
  public ProjectionDeadLetterEntry {
    Objects.requireNonNull(projectionName, "projectionName is required");
    Objects.requireNonNull(fromOffset, "fromOffset is required");
    Objects.requireNonNull(toOffset, "toOffset is required");
    Objects.requireNonNull(errorType, "errorType is required");
    if (errorType.isBlank()) {
      throw new IllegalArgumentException("errorType must not be blank");
    }
    Objects.requireNonNull(failedAt, "failedAt is required");
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be > 0");
    }
    if (attempts < 0) {
      throw new IllegalArgumentException("attempts must not be negative");
    }
  }
}

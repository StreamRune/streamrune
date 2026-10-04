package org.streamrune.core.saga;

import org.streamrune.core.AggregateState;

/**
 * Marker interface for saga state. Implementations must be Jackson-serializable (annotate with
 * {@code @JsonCreator} and {@code @JsonProperty} where needed).
 */
public interface SagaState extends AggregateState {

  /** Current lifecycle status of this saga. */
  SagaStatus status();
}

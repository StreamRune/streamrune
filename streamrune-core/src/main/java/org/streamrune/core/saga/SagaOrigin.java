package org.streamrune.core.saga;

import org.streamrune.core.types.SagaType;

/**
 * The saga whose dispatch caused an event, directly (hop 0) or through trusted continuations.
 *
 * <p>The command bus writes it into {@link org.streamrune.core.EventMetadata#sagaOrigin()} of every
 * event a command produces while a saga's dispatch is in progress. The value a saga dispatch binds
 * is a function of the saga type and the saga id alone, so a first attempt, a redelivery, a replay
 * and a resume of the same saga all record the same origin.
 *
 * <p>An origin states who caused an event. It grants no authority.
 *
 * @param sagaType the type of the dispatching saga, as its runner derives it from the saga state
 *     class (required)
 * @param sagaId the dispatching saga instance (required)
 * @param hop 0 for an event produced by a command the saga dispatched itself; never negative
 */
public record SagaOrigin(SagaType sagaType, SagaId sagaId, int hop) {

  public SagaOrigin {
    if (sagaType == null) {
      throw new IllegalArgumentException("sagaType is required");
    }
    if (sagaId == null) {
      throw new IllegalArgumentException("sagaId is required");
    }
    if (hop < 0) {
      throw new IllegalArgumentException("hop must not be negative");
    }
  }

  /**
   * The origin of an event produced by a command the saga dispatched itself.
   *
   * @param sagaType the type of the dispatching saga (required)
   * @param sagaId the dispatching saga instance (required)
   * @return the origin at hop 0
   */
  public static SagaOrigin dispatchedBy(SagaType sagaType, SagaId sagaId) {
    return new SagaOrigin(sagaType, sagaId, 0);
  }
}

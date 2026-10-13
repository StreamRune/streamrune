package org.streamrune.core.saga;

import org.streamrune.core.types.SagaType;

/**
 * The saga whose dispatch caused an event, directly (hop 0) or through trusted continuations.
 *
 * <p>The command bus writes it into {@link org.streamrune.core.EventMetadata#sagaOrigin()} of every
 * event a command produces while a saga's dispatch is in progress, or while a listener continues
 * such an event's flow through {@link
 * org.streamrune.core.StreamRuneContext#continuing(org.streamrune.core.EventEnvelope)}. The value a
 * saga dispatch binds is a function of the saga type and the saga id alone, so a first attempt, a
 * redelivery, a replay and a resume of the same saga all record the same origin; the value a
 * continuation binds is a function of the stored event it reacts to.
 *
 * <p>A saga runner reads it: a non-start event whose origin names the runner's saga type, compared
 * exactly, belongs to the saga the origin names. The hop count takes no part in that.
 *
 * <p>An origin states who caused an event. It grants no authority.
 *
 * @param sagaType the type of the dispatching saga, as its runner derives it from the saga state
 *     class (required)
 * @param sagaId the dispatching saga instance (required)
 * @param hop 0 for an event produced by a command the saga dispatched itself; n for an event
 *     produced by the n-th continuation in a row after such an event; never negative
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

  /**
   * The origin of an event produced by a command that continues the flow of an event carrying this
   * origin: the same saga, one hop further. {@link
   * org.streamrune.core.StreamRuneContext#continuing(org.streamrune.core.EventEnvelope)} is its one
   * caller.
   *
   * @return this origin with {@code hop + 1}
   * @throws IllegalStateException if this origin is at hop {@link Integer#MAX_VALUE}: the hop count
   *     cannot grow, and an origin with a wrong hop is not written. No chain of continuations
   *     through the framework reaches that hop; metadata written outside it can hold it
   */
  public SagaOrigin nextHop() {
    if (hop == Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "a saga origin at hop "
              + hop
              + " cannot be taken one hop further: the hop count is at its limit");
    }
    return new SagaOrigin(sagaType, sagaId, hop + 1);
  }
}

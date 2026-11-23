package org.streamrune.runtime;

import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaId;

/**
 * One routed event of a page: the saga it goes to, whether it is that saga's start event, and the
 * event. The runner's page executor groups the items of a page by {@link #sagaId()}.
 *
 * @param sagaId the saga the event was routed to
 * @param startPath whether the event is the saga's start event
 * @param event the event
 */
record SagaWorkItem(SagaId sagaId, boolean startPath, EventEnvelope event) {

  /** Where the item routes, as {@link SagaRunner#resolveTarget} said it. */
  SagaRunner.ReplayTarget target() {
    return new SagaRunner.ReplayTarget(sagaId, startPath);
  }
}

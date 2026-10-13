package org.streamrune.runtime;

import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.SagaType;

/** Saga origins for tests that call {@link SagaCommandDispatch} directly, without a runner. */
final class SagaOrigins {

  private SagaOrigins() {}

  /** The hop-0 origin of the saga {@code sagaId} of a fixed test saga type. */
  static SagaOrigin of(String sagaId) {
    return new SagaOrigin(
        SagaType.of("org.streamrune.runtime.TestSaga$State"), SagaId.of(sagaId), 0);
  }
}

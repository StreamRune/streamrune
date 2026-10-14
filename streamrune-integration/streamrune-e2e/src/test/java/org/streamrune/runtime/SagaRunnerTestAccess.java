package org.streamrune.runtime;

import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;

/**
 * Reaches {@link SagaRunner}'s dead-letter replay feed from the end-to-end tests. The feed is
 * package-private: the replayer calls it for a held entry, and a test calls it here to feed one
 * event to one saga at a chosen moment, with no entry to drain.
 */
public final class SagaRunnerTestAccess {

  private SagaRunnerTestAccess() {}

  /**
   * Feeds {@code event} to the saga as the replayer does, without the operator's force.
   *
   * @return the name of the step's outcome
   */
  public static <S extends SagaState> String feedReplay(
      SagaRunner<S> runner, SagaId sagaId, EventEnvelope event, boolean startPath) {
    return runner.feedReplay(sagaId, event, startPath, false).name();
  }
}

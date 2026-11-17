package org.streamrune.runtime;

import org.streamrune.core.saga.SagaState;

/**
 * Reaches {@link SagaCompensationRetrySweeper#sweepOnce()} from the PostgreSQL tests: one sweep
 * cycle on the test's thread, against the sweeper's own clock, with no background loop.
 */
public final class SagaCompensationRetrySweeperTestAccess {

  private SagaCompensationRetrySweeperTestAccess() {}

  /** Runs one sweep cycle. */
  public static <S extends SagaState> void sweepOnce(SagaCompensationRetrySweeper<S> sweeper) {
    sweeper.sweepOnce();
  }
}

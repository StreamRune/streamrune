package org.streamrune.runtime;

import org.streamrune.core.saga.SagaState;

/**
 * Reaches {@link SagaTimeoutRunner#pollOnce()} from the PostgreSQL tests. The method is
 * package-private so that a test drives exactly one poll, on its own thread and against its own
 * clock, with no background loop.
 */
public final class SagaTimeoutRunnerTestAccess {

  private SagaTimeoutRunnerTestAccess() {}

  /** Runs one poll: every saga past {@code clock.now() - timeout()} is processed once. */
  public static <S extends SagaState> void pollOnce(SagaTimeoutRunner<S> runner) {
    runner.pollOnce();
  }

  /**
   * Sets the bounded wait of a claim for a step in flight, which the builder keeps package-private.
   */
  public static <S extends SagaState> SagaTimeoutRunner.Builder<S> claimWait(
      SagaTimeoutRunner.Builder<S> builder, java.time.Duration wait) {
    return builder.claimWait(wait);
  }
}

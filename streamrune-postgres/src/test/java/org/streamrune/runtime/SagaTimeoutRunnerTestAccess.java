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
}

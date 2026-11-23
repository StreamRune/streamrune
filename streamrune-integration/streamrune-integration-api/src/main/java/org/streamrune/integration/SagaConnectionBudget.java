package org.streamrune.integration;

import java.util.Optional;
import java.util.OptionalInt;
import org.streamrune.core.AggregateLocker;

/**
 * The connection budget of the saga runners: how many connections of the application's pool their
 * steps can hold at once, the warning for a budget that takes more than half the pool, and the
 * refusal for locks that can take all of it. The three integrations log and throw the same text.
 */
public final class SagaConnectionBudget {

  private SagaConnectionBudget() {}

  /**
   * The connections one saga step holds at once: 1 for the load and the append, plus what the
   * locker holds.
   *
   * @param locker the aggregate locker of the application
   * @return {@code 1 + locker.applicationPoolConnectionsPerLock()}
   * @throws IllegalStateException if the locker answers a negative number
   */
  public static int connectionsPerStep(AggregateLocker locker) {
    int perLock = checkedPerLock(locker.applicationPoolConnectionsPerLock(), locker.getClass());
    return 1 + perLock;
  }

  /**
   * The warning for a budget above half the pool.
   *
   * @param stepsInFlightAtMost the steps that can run at once, over all saga runners
   * @param connectionsPerStep the connections one step holds, see {@link
   *     #connectionsPerStep(AggregateLocker)}
   * @param sagaTypes the number of saga types the budget covers
   * @param poolMaximum the pool's maximum size, or empty when it cannot be read
   * @return the warning text; empty when the budget fits or the pool's size is unknown
   * @throws IllegalStateException if {@code connectionsPerStep} is below 1
   * @throws IllegalArgumentException if {@code poolMaximum} is present and below 1
   */
  public static Optional<String> warning(
      int stepsInFlightAtMost, int connectionsPerStep, int sagaTypes, OptionalInt poolMaximum) {
    if (connectionsPerStep < 1) {
      throw new IllegalStateException(
          "connections per step must be at least 1, got " + connectionsPerStep);
    }
    if (poolMaximum.isEmpty()) {
      return Optional.empty();
    }
    checkedPoolMaximum(poolMaximum.getAsInt());
    long budget = (long) stepsInFlightAtMost * connectionsPerStep;
    int max = poolMaximum.getAsInt();
    if (budget * 2 <= max) {
      return Optional.empty();
    }
    return Optional.of(
        "StreamRune: saga steps can hold up to "
            + budget
            + " connections at once ("
            + connectionsPerStep
            + " per step, "
            + stepsInFlightAtMost
            + " steps over "
            + sagaTypes
            + " saga type(s)), more than half of the connection pool's maximum of "
            + max
            + (connectionsPerStep == 1
                ? ". Raise the pool, or lower streamrune.saga.parallelism."
                : ". Raise the pool, give the aggregate locker a pool of its own"
                    + " (PgAdvisoryLocker.withDedicatedPool), or lower"
                    + " streamrune.saga.parallelism."));
  }

  /**
   * The refusal when the aggregate lock shares the application's pool and its locks alone can take
   * all of it: {@code lockConnections = stepsInFlightAtMost × connectionsPerLock}, refused when
   * {@code lockConnections >= poolMaximum}. Empty below it, when the locker holds no pool
   * connection, or when the pool's size is unknown.
   *
   * @param stepsInFlightAtMost the steps that can run at once, over all saga runners
   * @param connectionsPerLock the locker's {@link
   *     AggregateLocker#applicationPoolConnectionsPerLock()}
   * @param poolMaximum the pool's maximum size, or empty when it cannot be read
   * @return the refusal text; empty when startup may go on
   * @throws IllegalStateException if {@code connectionsPerLock} is negative
   * @throws IllegalArgumentException if {@code poolMaximum} is present and below 1
   */
  public static Optional<String> refusal(
      int stepsInFlightAtMost, int connectionsPerLock, OptionalInt poolMaximum) {
    checkedPerLock(connectionsPerLock, null);
    if (poolMaximum.isEmpty()) {
      return Optional.empty();
    }
    int max = checkedPoolMaximum(poolMaximum.getAsInt());
    if (connectionsPerLock == 0) {
      return Optional.empty();
    }
    long lockConnections = (long) stepsInFlightAtMost * connectionsPerLock;
    if (lockConnections < max) {
      return Optional.empty();
    }
    return Optional.of(
        "StreamRune: saga steps can hold up to "
            + lockConnections
            + " connections of the connection pool as aggregate locks at once ("
            + stepsInFlightAtMost
            + " steps, "
            + connectionsPerLock
            + " per lock), at least the pool's maximum of "
            + max
            + ": every connection can be taken by a lock while its step waits for a second one."
            + " Give the aggregate locker a pool of its own (PgAdvisoryLocker.withDedicatedPool),"
            + " raise the pool, or lower streamrune.saga.parallelism.");
  }

  private static int checkedPoolMaximum(int max) {
    if (max < 1) {
      throw new IllegalArgumentException("the pool's maximum must be at least 1, got " + max);
    }
    return max;
  }

  private static int checkedPerLock(int perLock, Class<?> lockerClass) {
    if (perLock < 0) {
      throw new IllegalStateException(
          (lockerClass == null ? "The aggregate locker" : lockerClass.getName())
              + " answers a negative applicationPoolConnectionsPerLock(): "
              + perLock);
    }
    return perLock;
  }
}

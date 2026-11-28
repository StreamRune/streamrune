package org.streamrune.test;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Stops the worker pools of the contract suites, so that no worker of a test still calls the store
 * under test once the test has ended: the next test, or the store's teardown, would see it write.
 */
final class WorkerPools {

  /** How long {@link #stop(ExecutorService)} waits for the interrupted workers to end. */
  static final Duration STOP_WAIT = Duration.ofSeconds(10);

  private WorkerPools() {}

  /**
   * Interrupts the workers of {@code pool} and waits up to {@link #STOP_WAIT} for them to end. When
   * every worker has already finished, as it has after a test that passed, it returns at once.
   *
   * @param pool the pool to stop
   * @return whether every worker has ended
   */
  static boolean stop(ExecutorService pool) {
    return stop(pool, STOP_WAIT);
  }

  /**
   * Interrupts the workers of {@code pool} and waits up to {@code wait} for them to end. A worker
   * whose store call does not answer the interrupt ends when that call returns; one still running
   * after {@code wait} is left behind, so a store that hangs does not hang the test as well. It
   * never throws, so it does not replace the failure of the test it ends. An interrupt of the
   * caller ends the wait and stays set.
   *
   * @param pool the pool to stop
   * @param wait how long to wait for the workers to end
   * @return whether every worker has ended
   */
  static boolean stop(ExecutorService pool, Duration wait) {
    pool.shutdownNow();
    try {
      return pool.awaitTermination(wait.toNanos(), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return pool.isTerminated();
    }
  }
}

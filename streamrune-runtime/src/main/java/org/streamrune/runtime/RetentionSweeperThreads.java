package org.streamrune.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * How a retention sweeper stops its thread: interrupt it, then wait for a sweep in flight to
 * return, bounded by {@link #CLOSE_JOIN_TIMEOUT_MS}. A sweeper's {@code close()} therefore returns
 * only when the sweeper no longer touches its store, so whoever closes the store's database after
 * the sweeper (an integration closing the backend's stores at shutdown) never races a sweep. A
 * SQLite store keeps writing its file after the instance guard is released; a sweep that outlived
 * the close could write while a second process already holds the file.
 */
final class RetentionSweeperThreads {

  /** The bound on the wait for a sweep in flight, as for the other runtime loops' close. */
  static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

  private static final Logger LOG = LoggerFactory.getLogger(RetentionSweeperThreads.class);

  private RetentionSweeperThreads() {}

  /**
   * Interrupts {@code thread} and waits for it to end, at most {@code joinTimeoutMs}; a sweeper
   * closed from its own sweep (a store calling back) does not wait for itself.
   *
   * @param thread the sweep thread, or {@code null} when none was started
   * @param name the sweeper's name, for the WARN when the sweep does not end in time
   * @param joinTimeoutMs the bound on the wait
   * @return whether the thread has ended (also {@code true} for {@code null})
   */
  static boolean stop(Thread thread, String name, long joinTimeoutMs) {
    if (thread == null) {
      return true;
    }
    thread.interrupt();
    if (thread == Thread.currentThread()) {
      return false;
    }
    try {
      thread.join(joinTimeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (thread.isAlive()) {
      LOG.warn(
          "The {} did not end its sweep within {} ms of close(); it may still touch its store",
          name,
          joinTimeoutMs);
      return false;
    }
    return true;
  }
}

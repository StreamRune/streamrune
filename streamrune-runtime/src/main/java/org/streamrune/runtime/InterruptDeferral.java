package org.streamrune.runtime;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Lets a runner's {@code close()} interrupt its poll thread without the interrupt landing in the
 * middle of a store write.
 *
 * <p>{@code close()} interrupts the poll thread to cut a blocking wait short: an idle sleep, a
 * publish waiting for the broker. On a virtual thread the same interrupt also breaks every blocking
 * socket call the thread makes while the flag is set, so a JDBC write issued afterwards fails at
 * the socket (or the pool refuses to hand out a connection), and work that was already done is
 * never recorded. The poll thread brackets such writes with {@link #defer()} and {@link #resume()};
 * a stop requested in between is held and delivered by {@link #resume()}.
 *
 * <p>One instance per poll thread. {@link #interrupt(Thread)} is called by the closing thread (or
 * by the poll thread itself); {@link #defer()} and {@link #resume()} only by the poll thread.
 */
final class InterruptDeferral {

  private final ReentrantLock lock = new ReentrantLock();

  // Both guarded by lock: the closing thread must never interrupt the poll thread between the
  // poll thread clearing its flag and starting the write that the flag would break.
  private boolean deferring;
  private boolean stopRequested;

  /**
   * Requests a stop: interrupts {@code pollThread} now, or, while it is inside a deferred section,
   * when that section ends.
   */
  void interrupt(Thread pollThread) {
    lock.lock();
    try {
      stopRequested = true;
      if (!deferring) {
        pollThread.interrupt();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Starts a section that must not be interrupted. Clears the calling thread's interrupt flag; a
   * flag that was set counts as a stop request.
   *
   * @return whether a stop has been requested
   */
  boolean defer() {
    lock.lock();
    try {
      deferring = true;
      if (Thread.interrupted()) {
        stopRequested = true;
      }
      return stopRequested;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Ends the deferred section and, when a stop was requested, sets the calling thread's interrupt
   * flag again so its poll loop exits.
   */
  void resume() {
    lock.lock();
    try {
      deferring = false;
      if (stopRequested) {
        Thread.currentThread().interrupt();
      }
    } finally {
      lock.unlock();
    }
  }
}

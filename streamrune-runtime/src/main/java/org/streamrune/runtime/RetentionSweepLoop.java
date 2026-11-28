package org.streamrune.runtime;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The thread lifecycle a retention sweeper runs its sweeps on: the started guard, one named virtual
 * thread driving a {@link ResilientPollLoop}, the liveness trio of {@link RetentionSweeper}, and a
 * {@code close()} that waits for a sweep in flight through {@link RetentionSweeperThreads#stop}.
 *
 * <p>Each sweeper owns one and delegates its lifecycle methods to it; what a sweep does, whether
 * retention is enabled and what the sweeper logs stay in the sweeper. The thread and the poll
 * loop's failure and backoff log lines carry the sweeper's component name.
 *
 * <p><b>Resilience:</b> a {@link RuntimeException} from a sweep is retried with capped exponential
 * backoff, starting at the interval and capped at the larger of the interval and {@link
 * #MAX_FAILURE_BACKOFF}. An {@link Error} ends the thread, which {@link #isStarted()} and {@link
 * #isAlive()} then report as started but not alive.
 */
final class RetentionSweepLoop {

  /** The cap on the failure backoff, unless the interval is longer. */
  static final Duration MAX_FAILURE_BACKOFF = Duration.ofSeconds(60);

  private final String ownerClassName;
  private final String name;
  private final Runnable sweep;
  private final ResilientPollLoop pollLoop;
  private volatile Thread thread;
  private volatile long closeJoinTimeoutMs = RetentionSweeperThreads.CLOSE_JOIN_TIMEOUT_MS;
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * Creates the loop of one sweeper; nothing runs until {@link #start(boolean)}.
   *
   * @param ownerClassName the sweeper's simple class name, for the message of a second start
   * @param name the component name: the thread's name and the poll loop's name in its log lines
   * @param interval the sleep between sweeps and the first failure backoff; validated positive by
   *     the sweeper
   * @param sweep one sweep cycle, run on the loop's thread
   */
  RetentionSweepLoop(String ownerClassName, String name, Duration interval, Runnable sweep) {
    this.ownerClassName = ownerClassName;
    this.name = name;
    this.sweep = sweep;
    Duration maxBackoff =
        interval.compareTo(MAX_FAILURE_BACKOFF) > 0 ? interval : MAX_FAILURE_BACKOFF;
    this.pollLoop =
        new ResilientPollLoop(
            name, this::isRunningOnItsThread, () -> interval, interval, maxBackoff);
  }

  /**
   * Claims the started guard and, when {@code retentionEnabled}, starts the sweep thread. A
   * disabled sweeper still holds the guard, so a second start fails the same way, and launches no
   * thread, so it never reads as started for health.
   *
   * @param retentionEnabled whether to launch the sweep thread
   * @return whether a thread was launched; {@code false} means retention is disabled
   * @throws IllegalStateException if already started
   */
  boolean start(boolean retentionEnabled) {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(
          ownerClassName + " already started — call start() exactly once");
    }
    if (!retentionEnabled) {
      return false;
    }
    Thread t = Thread.ofVirtual().name(name).unstarted(this::loop);
    // Assigned before the thread starts: the loop runs only while it is this sweeper's thread.
    thread = t;
    t.start();
    return true;
  }

  /**
   * Whether the loop keeps running: on this sweeper's thread until {@link #close()} clears it, and
   * not interrupted. The thread check stops the loop after a close whose interrupt a store
   * swallowed (a pool, a driver or a wrapper that catches the interruption and returns normally),
   * and a thread that outlived its close and a restart does not run beside the new one.
   */
  private boolean isRunningOnItsThread() {
    Thread current = Thread.currentThread();
    return thread == current && !current.isInterrupted();
  }

  private void loop() {
    pollLoop.run(sweep::run);
  }

  /** Whether a sweep thread was launched and not yet closed; {@code false} when disabled. */
  boolean isStarted() {
    return started.get() && thread != null;
  }

  /** Whether the sweep thread is alive. */
  boolean isAlive() {
    Thread t = thread;
    return t != null && t.isAlive();
  }

  /** The sweeps that failed in a row. */
  int consecutiveFailures() {
    return pollLoop.consecutiveFailures();
  }

  /**
   * Interrupts the sweep thread, waits for a sweep in flight to return, bounded by the join
   * timeout, and resets the started guard so {@link #start(boolean)} may be called again. Called on
   * the sweep thread itself, or while another close waits, it returns at once.
   */
  void close() {
    Thread running = thread;
    // Cleared before the interrupt: a sweep whose store swallows the interrupt still ends the loop.
    thread = null;
    RetentionSweeperThreads.stop(running, name, closeJoinTimeoutMs);
    started.set(false);
  }

  /** Lowers the bound on the wait for a sweep in flight; for tests. */
  void closeJoinTimeoutMs(long millis) {
    this.closeJoinTimeoutMs = millis;
  }
}

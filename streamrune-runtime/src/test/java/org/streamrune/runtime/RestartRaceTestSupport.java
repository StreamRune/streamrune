package org.streamrune.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.stubbing.Answer;

/**
 * Shared support for the {@code close()}/{@code stop()} → {@code start()} restart-race tests across
 * the lifecycle runners ({@link DeadLetterRetryRunner}, {@link SagaCompensationRetrySweeper},
 * {@link SagaTimeoutRunner}, {@link OutboxPoller}).
 *
 * <p>The bug: {@code close()}/{@code stop()} interrupts the poll thread but does not JOIN it before
 * resetting the {@code started} guard, so a stale poll thread can still be winding down when a
 * subsequent {@code start()} runs. For a runner whose loop condition is a shared {@code volatile
 * boolean running} that the thread flips false in a {@code finally}, that stale write clobbers the
 * restarted runner's {@code running = true} and the new poll thread dies silently; for the others
 * it leaks a duplicate concurrent poll thread.
 *
 * <p>The fix is to join the old thread (with a self-join guard) before resetting the guard — after
 * which the poll thread is provably dead the instant {@code close()} returns. This support produces
 * a collaborator answer that parks the poll thread until {@code close()} interrupts it, then
 * performs a deliberately SLOW (post-interrupt) shutdown so the poll thread is still alive when a
 * NON-joining {@code close()} returns — exactly the window a correct join must wait out.
 */
final class RestartRaceTestSupport {

  private RestartRaceTestSupport() {}

  /**
   * How long the parked poll thread deliberately stays alive after {@code close()} interrupts it.
   */
  static final Duration SLOW_SHUTDOWN = Duration.ofMillis(500);

  /**
   * A collaborator answer that records the poll thread, signals {@code entered}, then blocks until
   * {@code close()} interrupts it and lingers for {@link #SLOW_SHUTDOWN} before returning {@code
   * returnValue}. A non-joining {@code close()} returns while this thread is still lingering; a
   * correct join-in-close() does not return until it has finished.
   */
  static Answer<Object> parkThenSlowShutdown(
      AtomicReference<Thread> pollThread, CountDownLatch entered, Object returnValue) {
    return invocation -> {
      pollThread.set(Thread.currentThread());
      entered.countDown();
      try {
        Thread.sleep(Duration.ofSeconds(60).toMillis());
      } catch (InterruptedException _) {
        // Simulate a slow shutdown so the poll thread is provably still alive when a NON-joining
        // close() returns. Thread.sleep cleared the interrupt flag on throw, so this second sleep
        // runs to completion; close()'s join (the fix) must wait it out.
        try {
          Thread.sleep(SLOW_SHUTDOWN.toMillis());
        } catch (InterruptedException _) {
          // No second interrupt is expected in these single-close tests.
        }
        // Restore the interrupt so the runner's poll loop exits promptly once this answer returns.
        Thread.currentThread().interrupt();
      }
      return returnValue;
    };
  }

  /**
   * A collaborator answer that records the poll thread, signals {@code entered}, then ignores every
   * interrupt until the test counts {@code release} down, and returns {@code returnValue} with the
   * interrupt restored: a poll stuck where an interrupt does not reach it, which outlives any join
   * bound until it is released.
   */
  static Answer<Object> parkIgnoringInterruptsUntilReleased(
      AtomicReference<Thread> pollThread,
      CountDownLatch entered,
      CountDownLatch release,
      Object returnValue) {
    return invocation -> {
      pollThread.set(Thread.currentThread());
      entered.countDown();
      boolean interrupted = false;
      while (true) {
        try {
          if (release.await(10, TimeUnit.SECONDS)) {
            break;
          }
        } catch (InterruptedException _) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return returnValue;
    };
  }

  /**
   * Calls {@code close()} on two threads at once and waits for both: two shutdown paths, such as a
   * container's destroy callback and a lifecycle hook, closing one component together.
   */
  static void closeConcurrently(AutoCloseable component) throws Exception {
    var go = new CountDownLatch(1);
    var failures = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();
    List<Thread> closers = new java.util.ArrayList<>();
    for (int i = 0; i < 2; i++) {
      closers.add(
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      go.await();
                      component.close();
                    } catch (Throwable e) {
                      failures.add(e);
                    }
                  }));
    }
    go.countDown();
    for (Thread closer : closers) {
      closer.join(30_000);
    }
    if (!failures.isEmpty()) {
      throw new AssertionError("a concurrent close() failed", failures.peek());
    }
  }
}

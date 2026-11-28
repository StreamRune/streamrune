package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class WorkerPoolsTest {

  @Test
  void aWorkerThatEndsOnTheInterrupt_hasEndedWhenStopReturns() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    var started = new CountDownLatch(2);
    var never = new CountDownLatch(1);
    for (int i = 0; i < 2; i++) {
      pool.submit(
          () -> {
            started.countDown();
            never.await();
            return null;
          });
    }
    started.await();

    assertTrue(WorkerPools.stop(pool));
    assertTrue(pool.isTerminated());
  }

  @Test
  void aWorkerThatFinishesItsCallAfterTheInterrupt_hasEndedWhenStopReturns() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    var started = new CountDownLatch(1);
    pool.submit(
        () -> {
          started.countDown();
          // A store call that does not answer the interrupt: it runs to its end regardless.
          long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
          while (System.nanoTime() < end) {
            // Cleared, or park would return at once and spin while the status is set.
            Thread.interrupted();
            LockSupport.parkNanos(end - System.nanoTime());
          }
        });
    started.await();

    assertTrue(WorkerPools.stop(pool));
    assertTrue(pool.isTerminated(), "the worker no longer runs after stop");
  }

  @Test
  void aWorkerThatOutlivesTheWait_isLeftBehindAndStopReturns() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    pool.submit(
        () -> {
          started.countDown();
          while (release.getCount() > 0) {
            Thread.interrupted(); // ignores the interrupt without spinning on it
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
          }
        });
    started.await();
    try {
      long before = System.nanoTime();

      assertFalse(WorkerPools.stop(pool, Duration.ofMillis(200)));
      assertTrue(System.nanoTime() - before >= Duration.ofMillis(200).toNanos(), "stop waited");
      assertFalse(pool.isTerminated());
    } finally {
      release.countDown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void aCallerInterruptedWhileItWaits_returnsWithItsInterruptStatusSet() throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    pool.submit(
        () -> {
          started.countDown();
          while (release.getCount() > 0) {
            Thread.interrupted(); // ignores the interrupt without spinning on it
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
          }
        });
    started.await();
    try {
      Thread.currentThread().interrupt();

      assertFalse(WorkerPools.stop(pool, Duration.ofSeconds(10)));
      assertTrue(Thread.interrupted(), "the interrupt status is kept");
    } finally {
      release.countDown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}

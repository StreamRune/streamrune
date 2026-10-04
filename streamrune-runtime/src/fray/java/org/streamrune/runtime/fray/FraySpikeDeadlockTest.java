package org.streamrune.runtime.fray;

import java.util.concurrent.locks.ReentrantLock;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/**
 * Throwaway spike: determines empirically whether Fray's controlled scheduler can drive and
 * interleave {@code Thread.ofVirtual()} threads on JDK 25.
 *
 * <p>Two virtual threads acquire two {@link ReentrantLock}s in opposite order (A&rarr;B vs
 * B&rarr;A). This is a classic lock-ordering inversion: under a real OS scheduler it deadlocks only
 * on an unlucky interleaving (so it is flaky and usually passes), but under Fray's controlled
 * scheduler the deadlock-prone interleaving is reachable and should be found deterministically. If
 * Fray controls virtual threads, running this {@link FrayTest} reports a deadlock; if it does not,
 * every iteration completes cleanly (or Fray refuses to instrument the threads).
 *
 * <p>This class lives in the dedicated {@code fray} source set and runs only under the {@code
 * frayTest} task (the JVMTI agent attaches there); it is excluded from the normal {@code test} run
 * and the JaCoCo gate.
 */
public class FraySpikeDeadlockTest {

  @FrayTest(iterations = 200)
  public void virtualThreadLockOrderingDeadlock() throws InterruptedException {
    ReentrantLock lockA = new ReentrantLock();
    ReentrantLock lockB = new ReentrantLock();

    Thread t1 =
        Thread.ofVirtual()
            .name("fray-spike-AB")
            .start(
                () -> {
                  lockA.lock();
                  try {
                    lockB.lock();
                    try {
                      // critical section
                    } finally {
                      lockB.unlock();
                    }
                  } finally {
                    lockA.unlock();
                  }
                });

    Thread t2 =
        Thread.ofVirtual()
            .name("fray-spike-BA")
            .start(
                () -> {
                  lockB.lock();
                  try {
                    lockA.lock();
                    try {
                      // critical section
                    } finally {
                      lockA.unlock();
                    }
                  } finally {
                    lockB.unlock();
                  }
                });

    t1.join();
    t2.join();
  }
}

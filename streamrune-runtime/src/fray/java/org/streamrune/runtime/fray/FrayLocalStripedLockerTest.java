package org.streamrune.runtime.fray;

import java.time.Duration;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.LocalStripedLocker;

/**
 * Fray (CMU-PASTA) concurrency PROPERTY test for {@link LocalStripedLocker}.
 *
 * <p>The load-bearing invariant of the aggregate locker is MUTUAL EXCLUSION: two threads that
 * acquire a lock for aggregate ids mapping to the same stripe must never execute the critical
 * section at the same time. This is what prevents concurrent modification of the same aggregate
 * (lost updates, double-applied commands) in the command bus.
 *
 * <p>To force a stripe collision deterministically the locker is created with {@code stripeCount =
 * 1}: every aggregate id then hashes to the single stripe, so {@code acquireLock} on any two ids is
 * mutually exclusive — exactly the worst case the locker must serialize. (We do not rely on finding
 * two ids that collide in a larger stripe array; one stripe guarantees collision for all ids.)
 *
 * <p>The critical section deliberately uses NON-atomic, unsynchronized shared state — a plain
 * {@code int} counter and a plain {@code boolean} "occupied" flag — guarded ONLY by the locker. If
 * mutual exclusion holds, every interleaving Fray explores observes the flag clear on entry and the
 * final counter equals the number of increments. If the lock did NOT serialize (e.g. a buggy locker
 * that returned a no-op AutoCloseable, or per-id locks that failed to collide), Fray would find an
 * interleaving where:
 *
 * <ul>
 *   <li>thread B enters the section while thread A is still inside it &rarr; B observes {@code
 *       occupied == true} &rarr; assertion trips ("mutual exclusion violated"); or
 *   <li>two unsynchronized {@code counter++} read-modify-write sequences interleave and lose an
 *       update &rarr; final {@code counter != THREADS} &rarr; assertion trips.
 * </ul>
 *
 * The test is therefore NON-VACUOUS: the assertions are reachable under a broken lock and the
 * controlled scheduler is built to drive exactly those interleavings. With the real locker the
 * {@link java.util.concurrent.locks.ReentrantLock} stripe serializes the section, so no such
 * interleaving exists and every iteration passes.
 *
 * <p>No event/offset stores are needed — the test is self-contained. It lives in the dedicated
 * {@code fray} source set and runs only under the {@code frayTest} task (Linux x86_64 CI, where the
 * JVMTI agent exists); it is excluded from the normal {@code test} run and the JaCoCo gate.
 */
public class FrayLocalStripedLockerTest {

  /** Number of virtual threads contending for the single stripe. */
  private static final int THREADS = 3;

  /**
   * Generous timeout — under Fray's controlled scheduler wall-clock time is not meaningful, but a
   * non-trivial value keeps the property "no deadlock/hang" honest if the lock were misused.
   */
  private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Mutable shared state protected SOLELY by the locker. Non-atomic on purpose so a
   * mutual-exclusion failure is observable as either a re-entry into an already-occupied section or
   * a lost counter increment.
   */
  private static final class CriticalSection {
    boolean occupied;
    int counter;
  }

  @FrayTest(iterations = 200)
  public void sameStripeAcquisitionIsMutuallyExclusive() throws InterruptedException {
    // stripeCount = 1 forces EVERY aggregate id onto the same stripe → guaranteed collision.
    LocalStripedLocker locker = new LocalStripedLocker(1);
    CriticalSection cs = new CriticalSection();

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; i++) {
      // Distinct streams that all map to the single stripe.
      StreamId id = StreamId.of(AggregateType.of("aggregate"), AggregateId.of("aggregate-" + i));
      threads[i] =
          Thread.ofVirtual()
              .name("fray-locker-" + i)
              .start(
                  () -> {
                    try (var _ = locker.acquireLock(id, LOCK_TIMEOUT)) {
                      // --- critical section, guarded only by the locker ---
                      if (cs.occupied) {
                        // Another thread is already inside → mutual exclusion broken.
                        throw new AssertionError(
                            "mutual exclusion violated: critical section was already occupied");
                      }
                      cs.occupied = true;
                      // Non-atomic read-modify-write: a lost update here would also signal a
                      // missing lock once Fray interleaves two unguarded increments.
                      cs.counter = cs.counter + 1;
                      cs.occupied = false;
                      // --- end critical section ---
                    } catch (RuntimeException | Error e) {
                      throw e;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });
    }

    for (Thread t : threads) {
      t.join();
    }

    // Every increment must have landed: a lost update (interleaved counter++) proves the section
    // was not serialized.
    if (cs.counter != THREADS) {
      throw new AssertionError(
          "lost update: expected counter == " + THREADS + " but was " + cs.counter);
    }
    // The section must be clear once all threads exited.
    if (cs.occupied) {
      throw new AssertionError("critical section left occupied after all threads finished");
    }
  }
}

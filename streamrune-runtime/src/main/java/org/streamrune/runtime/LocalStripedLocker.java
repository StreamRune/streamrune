package org.streamrune.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.types.StreamId;

/**
 * Local in-process striped locker. Distributes streams across a fixed number of lock stripes to
 * reduce contention while preventing concurrent modification of the same stream. Two streams can
 * share a stripe (including two aggregate types with the same id value); they then contend, as any
 * two streams on one stripe do.
 */
public final class LocalStripedLocker implements AggregateLocker {

  private final Lock[] stripes;

  /**
   * Creates a new striped locker with the given number of stripes using non-fair locking.
   *
   * @param stripeCount number of lock stripes (must be >= 1)
   */
  public LocalStripedLocker(int stripeCount) {
    this(stripeCount, false);
  }

  /**
   * Creates a new striped locker with the given number of stripes and fairness setting.
   *
   * @param stripeCount number of lock stripes (must be >= 1)
   * @param fair if true, threads will acquire locks in arrival order; if false, no guarantee
   */
  public LocalStripedLocker(int stripeCount, boolean fair) {
    if (stripeCount < 1) {
      throw new IllegalArgumentException("stripeCount must be >= 1");
    }
    this.stripes = new Lock[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      stripes[i] = new ReentrantLock(fair);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>A negative timeout is rejected rather than passed to {@code tryLock}, where it silently
   * degraded to "try once, do not wait" — the same configuration made {@code PgAdvisoryLocker}
   * substitute a 30-second default and block instead. {@link Duration#ZERO} keeps the documented
   * single-attempt meaning.
   *
   * <p>A positive SUB-MILLISECOND timeout is now rejected on the same terms. {@code toMillis()}
   * below turned it into the {@code tryLock(0, MILLISECONDS)} that the zero arm means, while {@code
   * PgAdvisoryLocker} turned the identical value into an unbounded server-side wait — the same
   * knob, the same two behaviours, one band lower. Both arms go through {@link
   * AggregateLocker#requireValidTimeout}, so this locker cannot drift from the contract or from the
   * other implementation. A {@code null} timeout still throws {@link NullPointerException} here
   * (from {@code toMillis()}): it is unreachable from property binding and deliberately left as the
   * programmatic-wiring bug it is.
   */
  @Override
  public AutoCloseable acquireLock(StreamId streamId, Duration timeout) {
    AggregateLocker.requireValidTimeout(timeout, "lock timeout");
    // Use improved hash distribution to avoid poor distribution with similar string prefixes
    int hash = Objects.hash(streamId.value());
    // Mix bits to improve distribution (similar to MurmurHash3 finalization)
    hash ^= hash >>> 16;
    int index = Math.floorMod(hash, stripes.length);
    Lock lock = stripes[index];
    try {
      if (!lock.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new LockAcquisitionException(streamId, "Timeout acquiring lock");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LockAcquisitionException(streamId, "Interrupted acquiring lock", e);
    }
    return lock::unlock;
  }
}

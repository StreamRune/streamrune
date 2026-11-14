package org.streamrune.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;

/**
 * Local in-process striped locker. Distributes streams across a fixed number of lock stripes to
 * reduce contention while preventing concurrent modification of the same stream. Two streams can
 * share a stripe (including two aggregate types with the same id value); they then contend, as any
 * two streams on one stripe do.
 *
 * <p>{@link #traced(StreamRuneTracing)} returns the locker that starts the span {@value
 * TraceAttributes#LOCK_ACQUIRE} around each acquisition when the {@link TracingArea#LOCK} area is
 * on.
 */
public final class LocalStripedLocker implements AggregateLocker {

  private final Lock[] stripes;

  /**
   * The handle of each stripe, whose {@code close()} unlocks it, built once: an acquisition
   * allocates nothing between the grant of the lock and the handle it returns, so nothing can be
   * thrown there that would leave the stripe locked with no one holding its handle.
   */
  private final AutoCloseable[] handles;

  private final StreamRuneTracing tracing;

  /** Whether {@link #tracing} records the lock area, read once. */
  private final boolean traced;

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
    this.handles = new AutoCloseable[stripeCount];
    for (int i = 0; i < stripeCount; i++) {
      handles[i] = stripes[i]::unlock;
    }
    this.tracing = StreamRuneTracing.NOOP;
    this.traced = false;
  }

  private LocalStripedLocker(Lock[] stripes, AutoCloseable[] handles, StreamRuneTracing tracing) {
    this.stripes = stripes;
    this.handles = handles;
    this.tracing = GuardedTracing.of(tracing);
    this.traced = this.tracing.enabled(TracingArea.LOCK);
  }

  /**
   * A locker on the same stripes that starts the span {@value TraceAttributes#LOCK_ACQUIRE} around
   * each acquisition through {@code tracing}, when its {@link TracingArea#LOCK} area is on. The
   * span carries the stream and the outcome: {@code ACQUIRED}, {@code TIMEOUT} or {@code
   * INTERRUPTED} (expected outcomes, not errors). Any other failure records the exception. The
   * returned locker and this one share their locks, so a stream locked through one is locked for
   * the other.
   *
   * @param tracing the tracing; {@link StreamRuneTracing#NOOP} traces nothing
   * @return a locker that traces through {@code tracing}
   * @throws NullPointerException if {@code tracing} is {@code null}
   */
  public LocalStripedLocker traced(StreamRuneTracing tracing) {
    return new LocalStripedLocker(
        stripes, handles, Objects.requireNonNull(tracing, "tracing is required"));
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
    if (!traced || streamId == null) {
      return acquire(streamId, timeout);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.LOCK,
            TraceAttributes.LOCK_ACQUIRE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .build());
    AutoCloseable lock;
    try {
      lock = acquire(streamId, timeout);
    } catch (Throwable notAcquired) {
      // Nothing is held. The span ends with the outcome, and the caller sees the acquisition's
      // own failure whatever the span does.
      try {
        try {
          if (notAcquired instanceof LockAcquisitionException) {
            scope.setAttribute(
                TraceAttributes.LOCK_OUTCOME,
                notAcquired.getCause() instanceof InterruptedException ? "INTERRUPTED" : "TIMEOUT");
          } else {
            scope.recordError(notAcquired);
          }
        } finally {
          scope.close();
        }
      } catch (Throwable tracingFailure) {
        if (tracingFailure != notAcquired) {
          notAcquired.addSuppressed(tracingFailure);
        }
      }
      throw notAcquired;
    }
    // The lock is held from here on, and the caller does not have it yet: nothing below may leave
    // this method without either handing the lock over or releasing it. The tracing is guarded
    // and does not throw; if anything is thrown here all the same, the lock is released first.
    try {
      try {
        scope.setAttribute(TraceAttributes.LOCK_OUTCOME, "ACQUIRED");
        scope.recordSuccess();
      } finally {
        scope.close();
      }
    } catch (Throwable afterAcquisition) {
      releaseAfterFailure(lock, afterAcquisition);
      throw afterAcquisition;
    }
    return lock;
  }

  /** Releases {@code lock}, which the caller never received, before {@code failure} propagates. */
  private static void releaseAfterFailure(AutoCloseable lock, Throwable failure) {
    try {
      lock.close();
    } catch (Throwable releaseFailure) {
      if (releaseFailure != failure) {
        failure.addSuppressed(releaseFailure);
      }
    }
  }

  private AutoCloseable acquire(StreamId streamId, Duration timeout) {
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
    return handles[index];
  }
}

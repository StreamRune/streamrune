package org.streamrune.runtime;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Counts the failures of a guarded collaborator and decides when they are logged: the first one,
 * then at most once per {@value #INTERVAL_MINUTES} minutes, with the number of failures since the
 * last report. Counting is lock-free, so it may run under another lock or at a high rate; the
 * caller logs where it chooses, with the number {@link #claim()} returns.
 */
final class FailureLogLimiter {

  /** The least time between two reports after the first. */
  static final long INTERVAL_MINUTES = 5;

  private static final long INTERVAL_NANOS = Duration.ofMinutes(INTERVAL_MINUTES).toNanos();

  private final AtomicLong failures = new AtomicLong();
  private final AtomicLong unlogged = new AtomicLong();
  private final LongSupplier nanoTime;

  /** Guarded by {@code this}. */
  private boolean reported;

  /** {@code nanoTime} of the last report; guarded by {@code this}, meaningful once reported. */
  private long lastReportNanos;

  FailureLogLimiter() {
    this(System::nanoTime);
  }

  /** A limiter on a clock of the test's. */
  FailureLogLimiter(LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
  }

  /** Counts one failure; logs nothing. */
  void count() {
    failures.incrementAndGet();
    unlogged.incrementAndGet();
  }

  /** Every failure counted. */
  long total() {
    return failures.get();
  }

  /**
   * Whether to report now: the number of failures since the last report when the caller logs them
   * now, {@code 0} when it logs nothing (none since, or the interval has not passed). Of callers
   * that race, one gets the number.
   */
  synchronized long claim() {
    long since = unlogged.get();
    if (since == 0) {
      return 0;
    }
    long now = nanoTime.getAsLong();
    if (reported && now - lastReportNanos < INTERVAL_NANOS) {
      return 0;
    }
    reported = true;
    lastReportNanos = now;
    unlogged.addAndGet(-since);
    return since;
  }
}

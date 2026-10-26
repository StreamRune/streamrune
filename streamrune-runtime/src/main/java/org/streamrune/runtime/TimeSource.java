package org.streamrune.runtime;

import java.time.Clock;
import java.time.Instant;

/**
 * The time a runner, a poller or a subscription reads: the instants it stamps on what it records,
 * and the elapsed time it measures for a duration metric. Built from the clock its builder was
 * given, or from none.
 *
 * <p>Without a clock, instants come from {@link Clock#systemUTC()} and elapsed time from {@link
 * System#nanoTime()}, which no step of the wall clock disturbs. With a clock both come from it, so
 * a test clock decides the durations as well as the timestamps: a listener that advances a {@code
 * MutableClock} by 250 ms is timed at 250 ms.
 */
final class TimeSource {

  private static final TimeSource SYSTEM = new TimeSource(Clock.systemUTC(), false);

  private final Clock clock;
  private final boolean measuresOnTheClock;

  private TimeSource(Clock clock, boolean measuresOnTheClock) {
    this.clock = clock;
    this.measuresOnTheClock = measuresOnTheClock;
  }

  /** The time of {@code clock}, or the system's when it is {@code null}. */
  static TimeSource of(Clock clock) {
    return clock == null ? SYSTEM : new TimeSource(clock, true);
  }

  /** The current instant. */
  Instant instant() {
    return clock.instant();
  }

  /**
   * A reading in nanoseconds for measuring elapsed time: only the difference of two readings has a
   * meaning.
   */
  long nanoTime() {
    if (!measuresOnTheClock) {
      return System.nanoTime();
    }
    Instant now = clock.instant();
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }
}

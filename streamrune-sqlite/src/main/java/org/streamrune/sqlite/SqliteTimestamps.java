package org.streamrune.sqlite;

import java.time.Instant;

/**
 * The one timestamp representation of every SQLite table: microseconds since the epoch
 * (1970-01-01T00:00:00Z) in an {@code INTEGER} column.
 *
 * <p>Microseconds are Postgres' {@code TIMESTAMPTZ} precision, so an {@link Instant} reads back as
 * it does on Postgres. An integer sorts chronologically as a column, answers range queries with
 * plain comparisons, and is negative before 1970; a 64-bit count of microseconds reaches about
 * 292,000 years either side of 1970 (years -290308 to 294247), so 2038 is no limit. Sub-microsecond
 * digits round half up to the nearest microsecond, as the Postgres driver rounds them before
 * Postgres stores the value.
 *
 * <p>The application {@code Clock} stamps every timestamp; no table takes a default from SQLite's
 * clock.
 */
final class SqliteTimestamps {

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_MICRO = 1_000L;

  private SqliteTimestamps() {}

  /**
   * The column value of {@code instant}.
   *
   * @throws IllegalArgumentException if {@code instant} is {@code null} or beyond what a 64-bit
   *     count of microseconds holds
   */
  static long toMicros(Instant instant) {
    if (instant == null) {
      throw new IllegalArgumentException("instant must not be null");
    }
    // getNano() is never negative, so rounding half up on it rounds toward the future for
    // instants before 1970 as well; a value of 999_999_500 or more carries into the next second.
    long micros = (instant.getNano() + NANOS_PER_MICRO / 2) / NANOS_PER_MICRO;
    try {
      return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND), micros);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "instant " + instant + " is outside the range of epoch microseconds", e);
    }
  }

  /** The instant a column value stands for. */
  static Instant fromMicros(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, MICROS_PER_SECOND),
        Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICRO);
  }
}

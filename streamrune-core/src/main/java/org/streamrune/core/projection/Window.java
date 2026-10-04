package org.streamrune.core.projection;

import java.time.Instant;

/** Half-open time interval {@code [start, end)} used for windowed projections. */
public record Window(Instant start, Instant end) {

  public Window {
    if (start == null) {
      throw new IllegalArgumentException("start is required");
    }
    if (end == null) {
      throw new IllegalArgumentException("end is required");
    }
    if (!end.isAfter(start)) {
      throw new IllegalArgumentException("end must be > start");
    }
  }

  /** Returns true iff {@code start <= t < end}. */
  public boolean contains(Instant t) {
    if (t == null) {
      throw new IllegalArgumentException("t is required");
    }
    return !t.isBefore(start) && t.isBefore(end);
  }
}

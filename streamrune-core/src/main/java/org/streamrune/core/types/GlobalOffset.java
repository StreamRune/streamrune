package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the global offset - a monotonically increasing sequence number across all event
 * streams. Unlike {@link Version} which is per-stream, globalOffset is unique across the entire
 * event store.
 *
 * @param value the offset value, must be >= 0
 */
public record GlobalOffset(@JsonValue long value) {

  private static final GlobalOffset INITIAL = new GlobalOffset(0);

  @JsonCreator
  public GlobalOffset {
    if (value < 0) {
      throw new IllegalArgumentException("globalOffset must be >= 0");
    }
  }

  /** Creates a GlobalOffset from a long value. */
  public static GlobalOffset of(long value) {
    return new GlobalOffset(value);
  }

  /** Returns the initial global offset (0). */
  public static GlobalOffset initial() {
    return INITIAL;
  }

  /** Returns the next global offset. */
  public GlobalOffset next() {
    return new GlobalOffset(value + 1);
  }

  @Override
  public String toString() {
    return "GlobalOffset{" + value + "}";
  }
}

package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A type-safe version value object representing the version of an aggregate stream.
 *
 * <p>A stream with no events has version 0 ({@link #initial()}); the first appended event is stored
 * as version 1, and each further event increments the version by one. Used for optimistic locking
 * to prevent concurrent modification conflicts. Serializes as a plain JSON number, like {@link
 * GlobalOffset}.
 *
 * @param value the version number
 */
public record Version(@JsonValue long value) {

  /** Creates a new Version with the given value. */
  @JsonCreator
  public Version {
    if (value < 0) {
      throw new IllegalArgumentException("Version must not be negative");
    }
  }

  /** Returns the initial version (0) for a new stream. */
  public static Version initial() {
    return new Version(0);
  }

  /**
   * Returns the next version in the sequence.
   *
   * @return a new Version that is one greater than this version
   */
  public Version next() {
    return new Version(value + 1);
  }

  /**
   * Checks if this version is at the initial state.
   *
   * @return true if this version is 0
   */
  public boolean isInitial() {
    return value == 0;
  }
}

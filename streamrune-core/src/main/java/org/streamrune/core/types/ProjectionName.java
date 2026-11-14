package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Type-safe name for a projection.
 *
 * <p>Replaces loose String parameters for projection name keys. Serializes as a plain JSON string,
 * like the other identifier value types.
 */
public record ProjectionName(@JsonValue String value) {

  /**
   * The longest projection name, in code points: the width of the {@code
   * projection_offset.projection_name} and {@code projection_dead_letters.projection_name} columns.
   * A longer one is refused on construction, so a projection name that is accepted here is stored
   * by every store.
   */
  public static final int MAX_LENGTH = 255;

  /**
   * Creates the projection name.
   *
   * @throws IllegalArgumentException if {@code value} is blank or longer than {@value #MAX_LENGTH}
   *     characters (code points); the message states the length and does not echo the value
   */
  @JsonCreator
  public ProjectionName {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("projectionName is required");
    }
    StoredLength.requireAtMost(value, MAX_LENGTH, "projectionName");
  }

  /**
   * Creates a ProjectionName from a String value.
   *
   * @param value the projection name
   * @return new ProjectionName
   */
  public static ProjectionName of(String value) {
    return new ProjectionName(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

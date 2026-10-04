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

  @JsonCreator
  public ProjectionName {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("projectionName is required");
    }
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

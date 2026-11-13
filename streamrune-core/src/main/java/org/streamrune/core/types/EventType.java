package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * A type-safe wrapper for event type names.
 *
 * <p>EventType provides stronger typing compared to raw Strings, making the codebase more type-safe
 * and reducing the risk of typos and invalid type references. Serializes as a plain JSON string,
 * like the identifier value types.
 *
 * <p>A name is non-blank and at most {@value #MAX_LENGTH} characters, counted in code points as the
 * {@code event_type VARCHAR(255)} columns count it, so a name that is accepted here is stored by
 * every event store. The refusal states the length and the maximum and does not echo the name.
 *
 * @param value the event type name (typically simple class name or fully-qualified name)
 */
public record EventType(@JsonValue String value) {

  /** The longest name, in code points: the width of the {@code event_type} columns. */
  public static final int MAX_LENGTH = 255;

  /**
   * Creates a new EventType with the given name.
   *
   * @throws IllegalArgumentException if the name is blank or longer than {@value #MAX_LENGTH}
   *     characters (code points); the name is not echoed
   */
  @JsonCreator
  public EventType {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("EventType name must not be blank");
    }
    int length = value.codePointCount(0, value.length());
    if (length > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "EventType name is " + length + " characters; at most " + MAX_LENGTH);
    }
  }

  /**
   * Creates an EventType from a String name.
   *
   * @param name the event type name
   * @return a new EventType with the given name
   */
  public static EventType of(String name) {
    return new EventType(name);
  }

  /**
   * Creates an EventType from a Class using its simple name. Note: for inner classes with
   * potentially ambiguous simple names, use the {@link #of(String)} factory with an explicit name
   * instead.
   *
   * @param clazz the class to derive the event type name from
   * @return an EventType with the simple class name
   */
  public static EventType fromClass(Class<?> clazz) {
    return new EventType(clazz.getSimpleName());
  }

  @Override
  public String toString() {
    return value;
  }
}

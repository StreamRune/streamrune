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
 * @param name the event type name (typically simple class name or fully-qualified name)
 */
public record EventType(@JsonValue String name) {

  /** Creates a new EventType with the given name. */
  @JsonCreator
  public EventType {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("EventType name must not be blank");
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
    return name;
  }
}

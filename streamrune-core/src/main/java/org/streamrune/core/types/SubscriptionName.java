package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Type-safe name for an event subscription.
 *
 * <p>Replaces loose String parameters for subscription name keys. Serializes as a plain JSON
 * string, like the other identifier value types.
 */
public record SubscriptionName(@JsonValue String value) {

  /**
   * The longest subscription name, in code points: the width of the {@code
   * projection_offset.projection_name} (a subscription keeps its offset under its name) columns. A
   * longer one is refused on construction, so a subscription name that is accepted here is stored
   * by every store.
   */
  public static final int MAX_LENGTH = 255;

  /**
   * Creates the subscription name.
   *
   * @throws IllegalArgumentException if {@code value} is blank or longer than {@value #MAX_LENGTH}
   *     characters (code points); the message states the length and does not echo the value
   */
  @JsonCreator
  public SubscriptionName {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("subscriptionName is required");
    }
    StoredLength.requireAtMost(value, MAX_LENGTH, "subscriptionName");
  }

  /**
   * Creates a SubscriptionName from a String value.
   *
   * @param value the subscription name
   * @return new SubscriptionName
   */
  public static SubscriptionName of(String value) {
    return new SubscriptionName(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

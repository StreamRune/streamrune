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

  @JsonCreator
  public SubscriptionName {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("subscriptionName is required");
    }
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

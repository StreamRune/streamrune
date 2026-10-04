package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifies the direct predecessor event ID in an event chain. Used for distributed tracing and
 * causal ordering of reactions. Nullable in {@link org.streamrune.core.EventMetadata}.
 */
public record CausationId(@JsonValue String value) {

  @JsonCreator
  public CausationId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("causationId is required");
    }
  }

  public static CausationId of(String value) {
    return new CausationId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

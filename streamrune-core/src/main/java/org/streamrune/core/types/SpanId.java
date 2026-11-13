package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The span id of a {@link org.streamrune.core.tracing.SpanLink}: the id of the span another span
 * links to, the command span a command bus started for the events it appended. No request header
 * builds one.
 */
public record SpanId(@JsonValue String value) {

  @JsonCreator
  public SpanId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("spanId is required");
    }
  }

  public static SpanId of(String value) {
    return new SpanId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

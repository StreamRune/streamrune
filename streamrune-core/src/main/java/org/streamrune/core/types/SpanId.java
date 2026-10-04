package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** OpenTelemetry span ID propagated from the incoming HTTP request. Nullable. */
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

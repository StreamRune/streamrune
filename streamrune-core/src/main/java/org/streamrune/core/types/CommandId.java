package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Unique identifier assigned to each command execution. Used for idempotency checks and dead-letter
 * queue correlation.
 */
public record CommandId(@JsonValue String value) {

  @JsonCreator
  public CommandId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("commandId is required");
    }
  }

  public static CommandId of(String value) {
    return new CommandId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

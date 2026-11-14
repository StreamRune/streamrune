package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Unique identifier assigned to each command execution. Used for idempotency checks and dead-letter
 * queue correlation.
 */
public record CommandId(@JsonValue String value) {

  /**
   * The longest command id, in code points: the width of the {@code dead_letter_queue.command_id}
   * columns. A longer one is refused on construction, so a command id that is accepted here is
   * stored by every store.
   */
  public static final int MAX_LENGTH = 255;

  /**
   * Creates the command id.
   *
   * @throws IllegalArgumentException if {@code value} is blank or longer than {@value #MAX_LENGTH}
   *     characters (code points); the message states the length and does not echo the value
   */
  @JsonCreator
  public CommandId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("commandId is required");
    }
    StoredLength.requireAtMost(value, MAX_LENGTH, "commandId");
  }

  public static CommandId of(String value) {
    return new CommandId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

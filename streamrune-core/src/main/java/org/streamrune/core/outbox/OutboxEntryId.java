package org.streamrune.core.outbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identity of an {@link OutboxEntry}. Acts as the idempotency key: two save calls with the same
 * {@code OutboxEntryId} produce only one entry.
 */
public record OutboxEntryId(@JsonValue String value) {

  public OutboxEntryId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("OutboxEntryId value must not be null or blank");
    }
  }

  @JsonCreator
  public static OutboxEntryId of(String value) {
    return new OutboxEntryId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

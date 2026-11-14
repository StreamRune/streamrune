package org.streamrune.core.outbox;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.streamrune.core.types.StoredLength;

/**
 * Identity of an {@link OutboxEntry}. Acts as the idempotency key: two save calls with the same
 * {@code OutboxEntryId} produce only one entry.
 */
public record OutboxEntryId(@JsonValue String value) {

  /**
   * The longest id, in code points: the width of the {@code outbox_events.entry_id} column. A
   * longer one is refused on construction, so an id that is accepted here is stored by every outbox
   * store.
   */
  public static final int MAX_LENGTH = 255;

  /**
   * Creates the id.
   *
   * @throws IllegalArgumentException if {@code value} is blank or longer than {@value #MAX_LENGTH}
   *     characters (code points); the message states the length and does not echo the value
   */
  public OutboxEntryId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("OutboxEntryId value must not be null or blank");
    }
    StoredLength.requireAtMost(value, MAX_LENGTH, "OutboxEntryId value");
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

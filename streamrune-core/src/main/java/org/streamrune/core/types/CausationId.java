package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The id of the event a command reacted to: the direct predecessor of the events that command
 * produced. Nullable in {@link org.streamrune.core.EventMetadata}; an event without one is a
 * causation root.
 *
 * <p>The command bus alone writes it, from {@link org.streamrune.core.StreamRuneContext#CAUSE}: the
 * triggering event under a saga's forward dispatch, the event that opened the compensation episode
 * under its compensation dispatch, and the causing event under {@link
 * org.streamrune.core.StreamRuneContext#continuing(org.streamrune.core.EventEnvelope)}. No request
 * header, baggage entry or dead-letter row sets it, so a causation chain read from the event log is
 * the framework's own statement.
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

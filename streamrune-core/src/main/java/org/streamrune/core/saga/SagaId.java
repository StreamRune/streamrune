package org.streamrune.core.saga;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Identifies a single saga instance. Typically derived from business identifiers (e.g., {@code
 * "order-saga-" + orderId}).
 */
public record SagaId(@JsonValue String value) {

  /**
   * Maximum length, matching the {@code saga_state.saga_id} and {@code saga_dead_letters.saga_id}
   * VARCHAR(255) columns this id is persisted into. Bounding it here fails a too-long id fast at
   * construction (inside the saga runner's poison() wrapper, so it is quarantined as a poison
   * event) BEFORE any forward command is dispatched — rather than after commands commit but before
   * the over-length INSERT into saga_state fails and wedges the subscription.
   */
  public static final int MAX_LENGTH = 255;

  public SagaId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("SagaId value must not be null or blank");
    }
    if (value.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          "SagaId value must be at most "
              + MAX_LENGTH
              + " characters (was "
              + value.length()
              + ")");
    }
  }

  @JsonCreator
  public static SagaId of(String value) {
    return new SagaId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}

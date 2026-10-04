package org.streamrune.core;

import java.util.Objects;

/**
 * A single validation error for a command.
 *
 * @param field the field that failed validation (dot-separated path, e.g. "address.city"; empty for
 *     class-level constraints)
 * @param message the human-readable error message
 * @param code the constraint violation code (e.g. "NotBlank", "NotNull", "Positive")
 */
public record ValidationError(String field, String message, String code) {

  public ValidationError {
    Objects.requireNonNull(field, "field is required");
    Objects.requireNonNull(message, "message is required");
    Objects.requireNonNull(code, "code is required");
  }
}

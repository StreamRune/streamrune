package org.streamrune.core;

import java.util.List;

/**
 * Exception thrown when command validation fails. Contains one or more structured {@link
 * ValidationError} instances describing each failure.
 *
 * <p>Extends {@link DomainException} so that all business-rule command rejections (validation,
 * authorization, domain invariants) share one catchable root. Infrastructure misconfiguration such
 * as {@link NoDeciderException} deliberately stays outside this hierarchy.
 */
public class ValidationException extends DomainException {

  private final List<ValidationError> errors;

  /**
   * Creates a validation exception with a custom message and error list.
   *
   * @param message the detail message
   * @param errors the validation errors; may be null (treated as empty)
   */
  public ValidationException(String message, List<ValidationError> errors) {
    super(message);
    this.errors = errors != null ? List.copyOf(errors) : List.of();
  }

  /**
   * Creates a validation exception with a default message and error list.
   *
   * @param errors the validation errors; may be null (treated as empty)
   */
  public ValidationException(List<ValidationError> errors) {
    this("Command validation failed", errors);
  }

  /** Returns the list of validation errors. Never null, may be empty. */
  public List<ValidationError> errors() {
    return errors;
  }
}

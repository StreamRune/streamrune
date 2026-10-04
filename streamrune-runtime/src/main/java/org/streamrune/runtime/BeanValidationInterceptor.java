package org.streamrune.runtime;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.List;
import java.util.Set;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.ValidationError;
import org.streamrune.core.ValidationException;
import org.streamrune.core.ValidationGroups;

/**
 * A {@link CommandInterceptor} that validates command objects using JSR-380 Bean Validation before
 * the command reaches the Decider.
 *
 * <p>Requires {@code jakarta.validation-api} on the classpath. Uses {@link Validator} to check all
 * constraint annotations on the command type and its fields.
 *
 * <p>Violations are converted to {@link ValidationException} and thrown, short-circuiting command
 * execution. If no violations are found, returns {@code true} and execution proceeds.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * var validator = Validation.byDefaultProvider().configure().buildValidatorFactory().getValidator();
 * var interceptor = new BeanValidationInterceptor(validator);
 *
 * var streamRune = StreamRune.builder()
 *     .eventStore(eventStore)
 *     .interceptors(List.of(interceptor))
 *     .build();
 * }</pre>
 */
public class BeanValidationInterceptor implements CommandInterceptor {

  private final Validator validator;

  /**
   * Creates a new interceptor with the given validator. May be null for a no-op.
   *
   * @param validator the JSR-380 Validator used to validate commands, or null for a no-op
   */
  public BeanValidationInterceptor(Validator validator) {
    this.validator = validator;
  }

  @Override
  public boolean before(CommandContext ctx) {
    if (validator == null) {
      return true;
    }

    Command command = ctx.command();
    if (command == null) {
      return true;
    }

    ValidationGroups groupsAnnotation = command.getClass().getAnnotation(ValidationGroups.class);
    Set<ConstraintViolation<Command>> violations;
    if (groupsAnnotation != null) {
      violations = validator.validate(command, groupsAnnotation.value());
    } else {
      violations = validator.validate(command);
    }
    if (violations.isEmpty()) {
      return true;
    }

    List<ValidationError> errors = violations.stream().map(this::toValidationError).toList();

    throw new ValidationException(
        "Command validation failed for " + command.getClass().getSimpleName(), errors);
  }

  private ValidationError toValidationError(ConstraintViolation<Command> v) {
    String propertyPath = v.getPropertyPath().toString();
    String code = v.getConstraintDescriptor().getAnnotation().annotationType().getSimpleName();
    return new ValidationError(propertyPath, v.getMessage(), code);
  }
}

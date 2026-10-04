package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.core.beans.exceptions.IntrospectionException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for {@link IntrospectionSafeValidator}. */
class IntrospectionSafeValidatorTest {

  record PlainCommand(String name) {}

  @Test
  @SuppressWarnings("unchecked")
  void returnsDelegateViolations() {
    Validator delegate = mock(Validator.class);
    ConstraintViolation<PlainCommand> violation = mock(ConstraintViolation.class);
    when(delegate.validate(any(PlainCommand.class))).thenReturn(Set.of(violation));

    var validator = new IntrospectionSafeValidator(delegate);
    var violations = validator.validate(new PlainCommand("x"));

    assertEquals(Set.of(violation), violations);
  }

  @Test
  void treatsMissingIntrospectionAsNoConstraints() {
    Validator delegate = mock(Validator.class);
    when(delegate.validate(any(PlainCommand.class)))
        .thenThrow(new IntrospectionException("No bean introspection available"));

    var validator = new IntrospectionSafeValidator(delegate);

    assertTrue(validator.validate(new PlainCommand("x")).isEmpty());
    // second call hits the once-per-type warn path
    assertTrue(validator.validate(new PlainCommand("y")).isEmpty());
  }

  @Test
  void skipsNonIntrospectedTypesForMicronautValidator() {
    io.micronaut.validation.validator.Validator micronautValidator =
        mock(io.micronaut.validation.validator.Validator.class);

    var validator = new IntrospectionSafeValidator(micronautValidator);

    // PlainCommand has no @Introspected — must be skipped before reaching the delegate,
    // which would otherwise throw jakarta.validation.ValidationException.
    assertTrue(validator.validate(new PlainCommand("x")).isEmpty());
    assertTrue(validator.validateProperty(new PlainCommand("x"), "name").isEmpty());
    assertTrue(validator.validateValue(PlainCommand.class, "name", "x").isEmpty());
    verifyNoInteractions(micronautValidator);
  }

  @Test
  void validatePropertyAndValueAreGuarded() {
    Validator delegate = mock(Validator.class);
    when(delegate.validateProperty(any(), anyString(), any(Class[].class)))
        .thenThrow(new IntrospectionException("missing"));
    when(delegate.validateValue(any(), anyString(), any(), any(Class[].class)))
        .thenThrow(new IntrospectionException("missing"));

    var validator = new IntrospectionSafeValidator(delegate);

    assertTrue(validator.validateProperty(new PlainCommand("x"), "name").isEmpty());
    assertTrue(validator.validateValue(PlainCommand.class, "name", "x").isEmpty());
  }

  @Test
  void delegatesNonValidationMethods() {
    Validator delegate = mock(Validator.class);
    var validator = new IntrospectionSafeValidator(delegate);

    validator.getConstraintsForClass(PlainCommand.class);
    verify(delegate).getConstraintsForClass(PlainCommand.class);

    validator.forExecutables();
    verify(delegate).forExecutables();

    validator.unwrap(Validator.class);
    verify(delegate).unwrap(Validator.class);
  }
}

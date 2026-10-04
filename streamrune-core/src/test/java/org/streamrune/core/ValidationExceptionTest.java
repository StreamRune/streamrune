package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class ValidationExceptionTest {

  @Test
  void shouldContainValidationErrors() {
    var errors =
        List.of(
            new ValidationError("name", "must not be blank", "NotBlank"),
            new ValidationError("age", "must be positive", "Positive"));

    var ex = new ValidationException("Validation failed", errors);

    assertEquals("Validation failed", ex.getMessage());
    assertEquals(2, ex.errors().size());
    assertEquals("name", ex.errors().get(0).field());
    assertEquals("must not be blank", ex.errors().get(0).message());
    assertEquals("NotBlank", ex.errors().get(0).code());
  }

  @Test
  void shouldBuildFromSingleFieldError() {
    var error = new ValidationError("email", "invalid format", "Pattern");
    var ex = new ValidationException(List.of(error));

    assertEquals(1, ex.errors().size());
    assertEquals("email", ex.errors().get(0).field());
    assertEquals("Command validation failed", ex.getMessage());
  }

  @Test
  void shouldBeRuntimeException() {
    var ex = new ValidationException(List.of());
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void shouldBeDomainExceptionSoBusinessRuleHandlersCatchIt() {
    var ex = new ValidationException(List.of());
    assertInstanceOf(DomainException.class, ex);
  }

  @Test
  void shouldHandleNullErrorsList() {
    var ex = new ValidationException("test", null);
    assertTrue(ex.errors().isEmpty());
  }
}

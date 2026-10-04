package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ValidationErrorTest {

  @Test
  void shouldExposeComponents() {
    var error = new ValidationError("address.city", "must not be blank", "NotBlank");
    assertEquals("address.city", error.field());
    assertEquals("must not be blank", error.message());
    assertEquals("NotBlank", error.code());
  }

  @Test
  void shouldAllowEmptyFieldForClassLevelConstraints() {
    var error = new ValidationError("", "order lines must not overlap", "ConsistentLines");
    assertEquals("", error.field());
  }

  @Test
  void shouldRejectNullField() {
    var ex =
        assertThrows(
            NullPointerException.class, () -> new ValidationError(null, "message", "NotNull"));
    assertEquals("field is required", ex.getMessage());
  }

  @Test
  void shouldRejectNullMessage() {
    var ex =
        assertThrows(
            NullPointerException.class, () -> new ValidationError("field", null, "NotNull"));
    assertEquals("message is required", ex.getMessage());
  }

  @Test
  void shouldRejectNullCode() {
    var ex =
        assertThrows(
            NullPointerException.class, () -> new ValidationError("field", "message", null));
    assertEquals("code is required", ex.getMessage());
  }
}

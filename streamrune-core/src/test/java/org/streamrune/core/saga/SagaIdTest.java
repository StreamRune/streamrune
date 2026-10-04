package org.streamrune.core.saga;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SagaIdTest {

  @Test
  void constructs_with_value() {
    var id = new SagaId("order-saga-1");
    assertEquals("order-saga-1", id.value());
  }

  @Test
  void of_is_alias() {
    assertEquals(new SagaId("order-saga-1"), SagaId.of("order-saga-1"));
  }

  @Test
  void rejects_null() {
    assertThrows(IllegalArgumentException.class, () -> new SagaId(null));
  }

  @Test
  void rejects_blank() {
    assertThrows(IllegalArgumentException.class, () -> new SagaId(""));
    assertThrows(IllegalArgumentException.class, () -> new SagaId("  "));
  }

  @Test
  void toString_returns_value() {
    assertEquals("saga-42", SagaId.of("saga-42").toString());
  }

  @Test
  void accepts_exactly_255_chars() {
    var value = "s".repeat(255);
    assertEquals(value, new SagaId(value).value());
  }

  @Test
  void rejects_over_255_chars() {
    var value = "s".repeat(256);
    var ex = assertThrows(IllegalArgumentException.class, () -> new SagaId(value));
    assertTrue(ex.getMessage().contains("255"), ex.getMessage());
  }
}

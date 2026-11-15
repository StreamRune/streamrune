package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FieldEqualsTest {

  @Test
  void ofKeepsTheFieldAndTheValue() {
    var filter = FieldEquals.of("customerId", "c-1");

    assertEquals("customerId", filter.field());
    assertEquals("c-1", filter.value());
    assertEquals(new FieldEquals("customerId", "c-1"), filter);
  }

  @Test
  void anEmptyValueIsAValue() {
    assertEquals("", FieldEquals.of("customerId", "").value());
  }

  @Test
  void aNullOrBlankFieldIsRefused() {
    assertThrows(IllegalArgumentException.class, () -> FieldEquals.of(null, "v"));
    assertThrows(IllegalArgumentException.class, () -> FieldEquals.of("", "v"));
    assertThrows(IllegalArgumentException.class, () -> FieldEquals.of("  ", "v"));
  }

  @Test
  void aNullValueIsRefusedWithTheFieldSanitizedInTheMessage() {
    var refused = assertThrows(IllegalArgumentException.class, () -> FieldEquals.of("a\nb", null));

    assertTrue(refused.getMessage().contains("value cannot be null"), refused.getMessage());
    assertFalse(refused.getMessage().contains("\n"), refused.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"id", "version", "updated_at"})
  void aRepositoryColumnIsRefused(String column) {
    var refused = assertThrows(IllegalArgumentException.class, () -> FieldEquals.of(column, "1"));

    assertTrue(refused.getMessage().contains("'" + column + "'"), refused.getMessage());
  }
}

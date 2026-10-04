package org.streamrune.core.outbox;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class OutboxEntryIdTest {

  @Test
  void constructs_with_value() {
    var id = new OutboxEntryId("entry-1");
    assertEquals("entry-1", id.value());
  }

  @Test
  void of_is_alias() {
    assertEquals(new OutboxEntryId("entry-1"), OutboxEntryId.of("entry-1"));
  }

  @Test
  void rejects_null() {
    assertThrows(IllegalArgumentException.class, () -> new OutboxEntryId(null));
  }

  @Test
  void rejects_blank() {
    assertThrows(IllegalArgumentException.class, () -> new OutboxEntryId(""));
    assertThrows(IllegalArgumentException.class, () -> new OutboxEntryId("  "));
  }

  @Test
  void toString_returns_value() {
    assertEquals("entry-42", OutboxEntryId.of("entry-42").toString());
  }
}

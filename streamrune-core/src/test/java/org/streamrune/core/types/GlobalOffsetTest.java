package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GlobalOffsetTest {

  @Test
  void initialIsZero() {
    assertEquals(0, GlobalOffset.initial().value());
  }

  @Test
  void ofConstructs() {
    assertEquals(42, GlobalOffset.of(42).value());
  }

  @Test
  void nextIncrements() {
    assertEquals(43, GlobalOffset.of(42).next().value());
  }

  @Test
  void rejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> GlobalOffset.of(-1));
  }

  @Test
  void toStringIncludesValue() {
    assertTrue(GlobalOffset.of(7).toString().contains("7"));
  }

  @Test
  void serializesAsLong() throws Exception {
    var mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(GlobalOffset.of(7));
    assertEquals("7", json);
    GlobalOffset parsed = mapper.readValue("7", GlobalOffset.class);
    assertEquals(GlobalOffset.of(7), parsed);
  }
}

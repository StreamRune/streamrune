package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class VersionTest {

  @Test
  void initialIsZero() {
    assertEquals(0, Version.initial().value());
    assertTrue(Version.initial().isInitial());
  }

  @Test
  void nextIncrementsByOne() {
    assertEquals(1, Version.initial().next().value());
    assertEquals(5, new Version(4).next().value());
  }

  @Test
  void isInitialFalseForNonZero() {
    assertFalse(new Version(1).isInitial());
  }

  @Test
  void rejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> new Version(-1));
  }

  @Test
  void serializesAsPlainNumberAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var version = new Version(5);
    assertEquals("5", mapper.writeValueAsString(version));
    assertEquals(version, mapper.readValue("5", Version.class));
  }
}

package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class VersionedTest {

  @Test
  void wrapsDataAndVersion() {
    var versioned = new Versioned<>("hello", 5);
    assertEquals("hello", versioned.data());
    assertEquals(5, versioned.version());
  }

  @Test
  void rejectsNullData() {
    assertThrows(NullPointerException.class, () -> new Versioned<>(null, 0));
  }

  @Test
  void rejectsNegativeVersion() {
    assertThrows(IllegalArgumentException.class, () -> new Versioned<>("data", -1));
  }

  @Test
  void zeroVersionAllowed() {
    var versioned = new Versioned<>("data", 0);
    assertEquals(0, versioned.version());
  }

  @Test
  void equality() {
    var a = new Versioned<>("data", 1);
    var b = new Versioned<>("data", 1);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  void inequality_differentVersion() {
    var a = new Versioned<>("data", 1);
    var b = new Versioned<>("data", 2);
    assertNotEquals(a, b);
  }

  @Test
  void inequality_differentData() {
    var a = new Versioned<>("data1", 1);
    var b = new Versioned<>("data2", 1);
    assertNotEquals(a, b);
  }
}

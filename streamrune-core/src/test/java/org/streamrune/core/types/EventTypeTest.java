package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class EventTypeTest {

  @Test
  void constructsWithName() {
    assertEquals("OrderPlaced", new EventType("OrderPlaced").name());
  }

  @Test
  void fromClassUsesSimpleName() {
    assertEquals("EventTypeTest", EventType.fromClass(EventTypeTest.class).name());
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new EventType(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new EventType(" "));
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var type = new EventType("OrderPlaced");
    assertEquals("OrderPlaced", type.toString());
    assertEquals("\"OrderPlaced\"", mapper.writeValueAsString(type));
    assertEquals(type, mapper.readValue("\"OrderPlaced\"", EventType.class));
  }
}

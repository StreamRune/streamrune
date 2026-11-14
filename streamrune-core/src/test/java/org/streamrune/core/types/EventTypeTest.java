package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class EventTypeTest {

  @Test
  void constructsWithValue() {
    assertEquals("OrderPlaced", new EventType("OrderPlaced").value());
  }

  @Test
  void fromClassUsesSimpleName() {
    assertEquals("EventTypeTest", EventType.fromClass(EventTypeTest.class).value());
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

  @Test
  void acceptsANameOfExactlyTheColumnWidth() {
    assertEquals(255, EventType.MAX_LENGTH);
    assertEquals(255, new EventType("e".repeat(EventType.MAX_LENGTH)).value().length());
  }

  @Test
  void countsCodePointsAsTheColumnDoes() {
    String supplementary = "\uD83D\uDE00".repeat(EventType.MAX_LENGTH);

    assertEquals(supplementary, new EventType(supplementary).value());
    assertThrows(IllegalArgumentException.class, () -> new EventType(supplementary + "x"));
  }

  @Test
  void refusesANameOverTheBoundWithoutEchoingIt() {
    String tooLong = "SecretName".repeat(26);

    var thrown = assertThrows(IllegalArgumentException.class, () -> new EventType(tooLong));
    assertEquals("EventType name is 260 characters; at most 255", thrown.getMessage());
    assertFalse(thrown.getMessage().contains("SecretName"));
    assertThrows(IllegalArgumentException.class, () -> EventType.of("e".repeat(256)));
  }

  @Test
  void refusesAJsonNameOverTheBound() {
    var mapper = new ObjectMapper();

    assertThrows(
        com.fasterxml.jackson.databind.JsonMappingException.class,
        () -> mapper.readValue("\"" + "e".repeat(256) + "\"", EventType.class));
  }
}

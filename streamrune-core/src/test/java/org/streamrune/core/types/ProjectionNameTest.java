package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class ProjectionNameTest {

  @Test
  void constructsWithValue() {
    assertEquals("orders", new ProjectionName("orders").value());
  }

  @Test
  void ofIsAlias() {
    assertEquals(ProjectionName.of("orders"), new ProjectionName("orders"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new ProjectionName(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new ProjectionName(""));
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var name = ProjectionName.of("orders");
    assertEquals("orders", name.toString());
    assertEquals("\"orders\"", mapper.writeValueAsString(name));
    assertEquals(name, mapper.readValue("\"orders\"", ProjectionName.class));
  }
}

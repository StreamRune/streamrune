package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class SubscriptionNameTest {

  @Test
  void constructsWithValue() {
    assertEquals("order-fulfillment-saga", new SubscriptionName("order-fulfillment-saga").value());
  }

  @Test
  void ofIsAlias() {
    assertEquals(
        SubscriptionName.of("order-fulfillment-saga"),
        new SubscriptionName("order-fulfillment-saga"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SubscriptionName(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new SubscriptionName(""));
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var name = SubscriptionName.of("order-fulfillment-saga");
    assertEquals("order-fulfillment-saga", name.toString());
    assertEquals("\"order-fulfillment-saga\"", mapper.writeValueAsString(name));
    assertEquals(name, mapper.readValue("\"order-fulfillment-saga\"", SubscriptionName.class));
  }
}

package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.EventType;
import org.streamrune.core.upcasting.EventUpcaster;

class StrictEventPayloadsTest {

  record Placed(String orderId, String currency, Instant at) implements DomainEvent {}

  /** Writes a property it cannot read back: the getter is serialized, nothing binds it. */
  record Drifted(String orderId) implements DomainEvent {
    public String getSummary() {
      return "order " + orderId;
    }
  }

  @Test
  void anEventThatRoundTripsIsReturnedBound() {
    var event = new Placed("o-1", "EUR", Instant.parse("2026-01-01T00:00:00Z"));
    assertEquals(event, StrictEventPayloads.assertRoundTrips(event));
  }

  @Test
  void anEventThatDoesNotReadWhatItWrites_fails() {
    var failure =
        assertThrows(
            AssertionError.class, () -> StrictEventPayloads.assertRoundTrips(new Drifted("o-1")));
    assertTrue(failure.getMessage().contains("summary"), failure.getMessage());
  }

  @Test
  void anUpcasterThatWritesTheRightKeyBinds() {
    Placed upcast =
        StrictEventPayloads.upcastStrictly(
            addingKey("currency"),
            Map.of("orderId", "o-1", "at", "2026-01-01T00:00:00Z"),
            1,
            Placed.class);
    assertEquals("EUR", upcast.currency());
  }

  @Test
  void anUpcasterThatWritesAMisspelledKey_fails_whereTheTolerantStoreWouldBindANull() {
    var failure =
        assertThrows(
            AssertionError.class,
            () ->
                StrictEventPayloads.upcastStrictly(
                    addingKey("curency"),
                    Map.of("orderId", "o-1", "at", "2026-01-01T00:00:00Z"),
                    1,
                    Placed.class));
    assertTrue(failure.getMessage().contains("does not bind strictly"), failure.getMessage());
    assertTrue(failure.getMessage().contains("curency"), failure.getMessage());
  }

  private static EventUpcaster addingKey(String key) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return new EventType("Placed");
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        var next = new HashMap<>(eventData);
        next.putIfAbsent(key, "EUR");
        return next;
      }
    };
  }
}

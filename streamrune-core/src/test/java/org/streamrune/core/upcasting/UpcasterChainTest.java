package org.streamrune.core.upcasting;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventType;

class UpcasterChainTest {

  /**
   * Test upcaster for "OrderCreated": - V1 -> V2: adds "currency" = "CZK" - V2 -> V3: renames
   * "amount" to "totalAmount"
   */
  static class OrderCreatedUpcaster implements EventUpcaster {

    @Override
    public EventType eventType() {
      return new EventType("OrderCreated");
    }

    @Override
    public int currentVersion() {
      return 3;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
      var result = new HashMap<>(eventData);
      return switch (fromVersion) {
        case 1 -> {
          result.put("currency", "CZK");
          yield result;
        }
        case 2 -> {
          result.put("totalAmount", result.remove("amount"));
          yield result;
        }
        default -> result;
      };
    }
  }

  private final UpcasterChain chain = new UpcasterChain(List.of(new OrderCreatedUpcaster()));

  @Test
  void chainsV1toV3() {
    var v1Data = new HashMap<String, Object>();
    v1Data.put("amount", 100);

    var result = chain.upcast(new EventType("OrderCreated"), v1Data, 1);

    assertEquals(100, result.get("totalAmount"));
    assertEquals("CZK", result.get("currency"));
    assertNull(result.get("amount"), "amount should have been renamed to totalAmount");
  }

  @Test
  void unknownEventTypePassesThrough() {
    var data = Map.<String, Object>of("foo", "bar");

    var result = chain.upcast(new EventType("UnknownEvent"), data, 1);

    assertSame(data, result);
  }

  @Test
  void currentVersionNoOp() {
    var data = new HashMap<String, Object>();
    data.put("totalAmount", 200);
    data.put("currency", "EUR");

    var result = chain.upcast(new EventType("OrderCreated"), data, 3);

    assertEquals(200, result.get("totalAmount"));
    assertEquals("EUR", result.get("currency"));
  }

  /** Upcaster that mutates the supplied map in place, as the contract permits. */
  static class InPlaceMutatingUpcaster implements EventUpcaster {

    @Override
    public EventType eventType() {
      return new EventType("PaymentTaken");
    }

    @Override
    public int currentVersion() {
      return 2;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
      eventData.put("method", "CARD");
      return eventData;
    }
  }

  @Test
  void immutableInputIsSafeForInPlaceMutatingUpcasters() {
    var mutatingChain = new UpcasterChain(List.of(new InPlaceMutatingUpcaster()));
    var immutableData = Map.<String, Object>of("amount", 100);

    var result = mutatingChain.upcast(new EventType("PaymentTaken"), immutableData, 1);

    assertEquals("CARD", result.get("method"));
    assertEquals(100, result.get("amount"));
    assertEquals(Map.of("amount", 100), immutableData, "caller's map must not be modified");
  }

  @Test
  void dataAtCurrentVersionIsReturnedUnchangedWithoutCopying() {
    var data = Map.<String, Object>of("totalAmount", 200);

    var result = chain.upcast(new EventType("OrderCreated"), data, 3);

    assertSame(data, result);
  }

  @Test
  void rejectsDuplicateUpcastersForOneEventType() {
    var upcasters = List.<EventUpcaster>of(new OrderCreatedUpcaster(), new OrderCreatedUpcaster());
    assertThrows(IllegalStateException.class, () -> new UpcasterChain(upcasters));
  }

  @Test
  void dataAboveCurrentVersionIsReturnedUnchangedWithoutCopying() {
    var data = Map.<String, Object>of("totalAmount", 200);

    var result = chain.upcast(new EventType("OrderCreated"), data, 4);

    assertSame(data, result);
  }

  @Test
  void upcastRejectsNullData() {
    assertThrows(
        IllegalArgumentException.class, () -> chain.upcast(new EventType("OrderCreated"), null, 1));
  }

  @Test
  void upcastRejectsFromVersionBelowOne() {
    var data = Map.<String, Object>of("amount", 100);
    assertThrows(
        IllegalArgumentException.class, () -> chain.upcast(new EventType("OrderCreated"), data, 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> chain.upcast(new EventType("OrderCreated"), data, -1));
  }

  @Test
  void currentVersionComesFromTheRegisteredUpcaster() {
    assertEquals(3, chain.currentVersion(new EventType("OrderCreated")));
  }

  @Test
  void currentVersionDefaultsToOneForUnregisteredEventTypes() {
    assertEquals(1, chain.currentVersion(new EventType("UnknownEvent")));
  }

  @Test
  void singleVersionUpcast() {
    var v2Data = new HashMap<String, Object>();
    v2Data.put("amount", 50);
    v2Data.put("currency", "USD");

    var result = chain.upcast(new EventType("OrderCreated"), v2Data, 2);

    assertEquals(50, result.get("totalAmount"));
    assertEquals("USD", result.get("currency"));
    assertNull(result.get("amount"), "amount should have been renamed to totalAmount");
  }
}

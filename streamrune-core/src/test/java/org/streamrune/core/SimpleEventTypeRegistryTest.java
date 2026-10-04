package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventType;

class SimpleEventTypeRegistryTest {

  record OrderCreated(String orderId) {}

  record OrderShipped(String orderId) {}

  record OrderState(String orderId, boolean shipped) {}

  @Test
  void resolvesRegisteredEventType() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("OrderCreated", OrderCreated.class)
            .registerEvent("OrderShipped", OrderShipped.class)
            .build();
    assertEquals(OrderCreated.class, registry.resolveEventType(new EventType("OrderCreated")));
    assertEquals(OrderShipped.class, registry.resolveEventType(new EventType("OrderShipped")));
  }

  @Test
  void resolvesRegisteredStateType() {
    var registry =
        SimpleEventTypeRegistry.builder().registerState("OrderState", OrderState.class).build();
    assertEquals(OrderState.class, registry.resolveStateType("OrderState"));
  }

  @Test
  void throwsForUnknownEventType() {
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("OrderCreated", OrderCreated.class).build();
    var ex =
        assertThrows(
            UnknownEventTypeException.class,
            () -> registry.resolveEventType(new EventType("Unknown")));
    assertEquals("Unknown", ex.typeName());
    assertEquals(List.of("OrderCreated"), ex.registeredTypes());
    assertTrue(ex.getMessage().contains("Unknown"), ex.getMessage());
    assertTrue(ex.getMessage().contains("OrderCreated"), ex.getMessage());
    // Read-path callers must be able to catch it together with other store failures.
    assertInstanceOf(EventStoreException.class, ex);
  }

  @Test
  void throwsForUnknownStateType() {
    var registry =
        SimpleEventTypeRegistry.builder().registerState("OrderState", OrderState.class).build();
    var ex =
        assertThrows(UnknownEventTypeException.class, () -> registry.resolveStateType("Unknown"));
    assertEquals("Unknown", ex.typeName());
    assertEquals(List.of("OrderState"), ex.registeredTypes());
    assertTrue(ex.getMessage().contains("state type"), ex.getMessage());
  }

  @Test
  void registersEventClassUnderItsSimpleName() {
    var registry = SimpleEventTypeRegistry.builder().registerEvent(OrderCreated.class).build();
    assertEquals(
        OrderCreated.class, registry.resolveEventType(EventType.fromClass(OrderCreated.class)));
    assertEquals(OrderCreated.class, registry.resolveEventType(new EventType("OrderCreated")));
  }

  @Test
  void classRegistrationRejectsSimpleNameCollisionAtBuildTime() {
    var builder = SimpleEventTypeRegistry.builder().registerEvent(OrderCreated.class);
    assertThrows(
        IllegalStateException.class,
        () -> builder.registerEvent(Elsewhere.OrderCreated.class),
        "two classes sharing a simple name must collide when registered via fromClass");
  }

  /** Simulates a same-simple-name event class from another package. */
  static class Elsewhere {
    record OrderCreated(String id) {}
  }

  @Test
  void registersEventUnderAnExplicitEventType() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent(new EventType("order.created.v2"), OrderCreated.class)
            .build();
    assertEquals(OrderCreated.class, registry.resolveEventType(new EventType("order.created.v2")));
  }

  @Test
  void eventTypeOverloadRejectsDuplicates() {
    var builder =
        SimpleEventTypeRegistry.builder().registerEvent(new EventType("X"), OrderCreated.class);
    assertThrows(
        IllegalStateException.class,
        () -> builder.registerEvent(new EventType("X"), OrderShipped.class));
  }

  @Test
  void oneClassMayBeAliasedUnderSeveralEventTypeNames() {
    // Aliasing is intentional: it supports renaming an event type while old events still
    // reference the previous name. Duplicate detection is keyed by name, not by class.
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("OrderCreated", OrderCreated.class)
            .registerEvent("order.created", OrderCreated.class)
            .build();
    assertEquals(OrderCreated.class, registry.resolveEventType(new EventType("OrderCreated")));
    assertEquals(OrderCreated.class, registry.resolveEventType(new EventType("order.created")));
  }

  @Test
  void rejectsDuplicateEventType() {
    assertThrows(
        IllegalStateException.class,
        () ->
            SimpleEventTypeRegistry.builder()
                .registerEvent("OrderCreated", OrderCreated.class)
                .registerEvent("OrderCreated", OrderShipped.class)
                .build());
  }

  @Test
  void rejectsDuplicateStateType() {
    assertThrows(
        IllegalStateException.class,
        () ->
            SimpleEventTypeRegistry.builder()
                .registerState("OrderState", OrderState.class)
                .registerState("OrderState", OrderShipped.class)
                .build());
  }

  @Test
  void registeredTypesReturnsAllRegisteredEventAndStateClasses() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("OrderCreated", OrderCreated.class)
            .registerEvent("OrderShipped", OrderShipped.class)
            .registerState("OrderState", OrderState.class)
            .build();

    assertEquals(
        java.util.Set.of(OrderCreated.class, OrderShipped.class, OrderState.class),
        java.util.Set.copyOf(registry.registeredTypes()));
  }

  @Test
  void registeredTypesDeduplicatesAClassRegisteredAsBothEventAndState() {
    var registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("OrderCreated", OrderCreated.class)
            .registerState("OrderCreated", OrderCreated.class)
            .build();

    assertEquals(
        java.util.List.of(OrderCreated.class), registry.registeredTypes().stream().toList());
  }

  @Test
  void registeredTypesIsEmptyForAnEmptyRegistry() {
    var registry = SimpleEventTypeRegistry.builder().build();
    assertTrue(registry.registeredTypes().isEmpty());
  }

  @Test
  void registeredTypesIsUnmodifiable() {
    var registry =
        SimpleEventTypeRegistry.builder().registerEvent("OrderCreated", OrderCreated.class).build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> registry.registeredTypes().add(OrderShipped.class));
  }

  @Test
  void defaultRegisteredTypesThrowsForImplementationsThatDoNotOverrideIt() {
    // The no-op List.of() default used to let a custom EventTypeRegistry silently disable
    // the @Encrypted plaintext-PII startup guard (CryptoConfigValidator short-circuits on an empty
    // collection). The default must now fail loudly instead of returning an empty collection.
    EventTypeRegistry minimal =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            throw new UnknownEventTypeException("event type", eventType.name(), List.of());
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            throw new UnknownEventTypeException("state type", stateType, List.of());
          }
        };
    var ex = assertThrows(UnsupportedOperationException.class, minimal::registeredTypes);
    assertTrue(ex.getMessage().contains("registeredTypes"), ex.getMessage());
  }
}

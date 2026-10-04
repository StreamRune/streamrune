package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.streamrune.core.Query;

class InMemoryQueryBusTest {

  record GetById(String id) implements Query<ItemView> {}

  record GetAll() implements Query<ItemView> {}

  record ItemView(String id, String name) {}

  @Test
  void shouldDispatchQueryToRegisteredHandler() {
    var bus =
        InMemoryQueryBus.builder()
            .register(GetById.class, q -> new ItemView(q.id(), "Item-" + q.id()))
            .build();

    ItemView result = bus.dispatch(new GetById("42"));

    assertEquals("42", result.id());
    assertEquals("Item-42", result.name());
  }

  @Test
  void shouldSupportMultipleHandlers() {
    var bus =
        InMemoryQueryBus.builder()
            .register(GetById.class, q -> new ItemView(q.id(), "item"))
            .register(GetAll.class, q -> new ItemView("*", "all"))
            .build();

    ItemView byId = bus.dispatch(new GetById("1"));
    ItemView all = bus.dispatch(new GetAll());

    assertEquals("1", byId.id());
    assertEquals("item", byId.name());
    assertEquals("*", all.id());
    assertEquals("all", all.name());
  }

  @Test
  void shouldThrowForUnregisteredQuery() {
    var bus = InMemoryQueryBus.builder().build();

    var ex = assertThrows(IllegalArgumentException.class, () -> bus.dispatch(new GetById("1")));

    assertTrue(ex.getMessage().contains("GetById"));
    assertTrue(ex.getMessage().contains("builder"), "Error message should guide to the fix");
  }

  @Test
  void shouldThrowForNullQuery() {
    var bus = InMemoryQueryBus.builder().build();

    assertThrows(IllegalArgumentException.class, () -> bus.dispatch(null));
  }

  @Test
  void shouldThrowForDuplicateHandlerRegistration() {
    var builder =
        InMemoryQueryBus.builder().register(GetById.class, q -> new ItemView(q.id(), "first"));

    assertThrows(
        IllegalArgumentException.class,
        () -> builder.register(GetById.class, q -> new ItemView(q.id(), "second")));
  }
}

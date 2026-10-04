package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class InMemoryOffsetStoreTest {

  private final InMemoryOffsetStore store = new InMemoryOffsetStore();
  private final ProjectionName orders = ProjectionName.of("orders");

  @Test
  void anUnknownNameStartsAtTheInitialOffset() {
    assertEquals(GlobalOffset.initial(), store.getLastOffset(orders));
  }

  @Test
  void aBackwardOrSidewaysSaveIsASilentNoOp() {
    store.saveOffset(orders, GlobalOffset.of(5));
    store.saveOffset(orders, GlobalOffset.of(3));
    store.saveOffset(orders, GlobalOffset.of(5));
    assertEquals(GlobalOffset.of(5), store.getLastOffset(orders));
    store.saveOffset(orders, GlobalOffset.of(6));
    assertEquals(GlobalOffset.of(6), store.getLastOffset(orders));
  }

  @Test
  void resetRewindsToTheInitialOffset_bypassingTheGuard() {
    store.saveOffset(orders, GlobalOffset.of(5));
    store.reset(orders);
    assertEquals(GlobalOffset.initial(), store.getLastOffset(orders));
    assertEquals(GlobalOffset.initial(), store.getLastOffset(ProjectionName.of("customers")));
  }
}

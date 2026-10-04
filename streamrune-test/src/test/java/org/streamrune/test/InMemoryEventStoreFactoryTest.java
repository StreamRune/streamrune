package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InMemoryEventStoreFactoryTest {

  @Test
  void createsInMemoryEventStore() {
    var factory = InMemoryEventStoreFactory.builder().build();
    var store = factory.create();
    assertNotNull(store);
    assertTrue(store instanceof InMemoryEventStore);
  }
}

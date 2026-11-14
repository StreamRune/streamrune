package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class InMemoryEventStoreFactoryTest {

  record Noted(String text) implements DomainEvent {}

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("note"), AggregateId.of("n-1"));

  @Test
  void createsStoresThatReadThroughTheRegistry() {
    var factory =
        InMemoryEventStoreFactory.builder(
                SimpleEventTypeRegistry.builder().registerEvent(Noted.class).build())
            .cryptoEngine(new InMemoryCryptoEngine())
            .build();

    var store = assertInstanceOf(InMemoryEventStore.class, factory.create());
    var event = new Noted("hello");
    store.append(
        STREAM,
        List.of(
            EventStoreFixture.event(
                STREAM, new Version(1), EventType.fromClass(Noted.class), event)),
        Version.initial());

    var read = store.load(STREAM).events().getFirst().event();
    assertEquals(event, read);
    assertNotSame(event, read);
    assertNotSame(store, factory.create());
  }

  @Test
  void requiresARegistry() {
    assertThrows(NullPointerException.class, () -> InMemoryEventStoreFactory.builder(null));
  }
}

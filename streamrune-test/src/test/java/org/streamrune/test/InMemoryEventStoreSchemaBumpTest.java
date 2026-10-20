package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * An added field, rolled out the way the production guide describes: the new release bumps the
 * type's schema version with an upcaster whose step returns the payload unchanged. The new release
 * reads the old events with the field {@code null}; the previous release pauses on the new events
 * with {@link EventSchemaAheadException} instead of misreading them. Without the bump, the strict
 * read of the previous release fails on the unknown property.
 */
class InMemoryEventStoreSchemaBumpTest {

  /** The event as the previous release declares it. */
  record Placed(String orderId) implements DomainEvent {}

  /** The same event type as the new release declares it: one optional field more. */
  record PlacedWithGiftMessage(String orderId, String giftMessage) implements DomainEvent {}

  private final StreamId stream = StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));

  /** The class the reading release binds "Placed" to. */
  private final AtomicReference<Class<?>> placedClass = new AtomicReference<>(Placed.class);

  private final InMemoryEventStore store =
      InMemoryEventStore.serializing(
          new EventTypeRegistry() {
            @Override
            public Class<?> resolveEventType(EventType eventType) {
              return placedClass.get();
            }

            @Override
            public Class<?> resolveStateType(String stateType) {
              throw new UnsupportedOperationException();
            }

            @Override
            public Collection<Class<?>> registeredTypes() {
              return List.of(Placed.class, PlacedWithGiftMessage.class);
            }
          },
          null);

  @Test
  void theNewReleaseReadsTheOldEventsThroughAnUpcasterThatChangesNothing() {
    store.append(stream, List.of(envelope(1, new Placed("o-1"))), Version.initial());

    newRelease();

    assertEquals(
        new PlacedWithGiftMessage("o-1", null), store.load(stream).events().getFirst().event());
  }

  @Test
  void thePreviousReleasePausesOnTheNewEvents_insteadOfMisreadingThem() {
    newRelease();
    store.append(
        stream,
        List.of(envelope(1, new PlacedWithGiftMessage("o-1", "Happy birthday"))),
        Version.initial());

    previousRelease();

    var refusal = assertThrows(EventSchemaAheadException.class, () -> store.load(stream));
    assertEquals(2, refusal.storedVersion());
    assertEquals(1, refusal.readerVersion());
  }

  @Test
  void withoutTheBump_thePreviousReleaseFailsOnTheUnknownProperty() {
    placedClass.set(PlacedWithGiftMessage.class); // the new class, but no upcaster: stamped 1
    store.append(
        stream,
        List.of(envelope(1, new PlacedWithGiftMessage("o-1", "Happy birthday"))),
        Version.initial());

    previousRelease();

    assertThrows(EventDeserializationException.class, () -> store.load(stream));
  }

  private void newRelease() {
    placedClass.set(PlacedWithGiftMessage.class);
    store.withUpcasters(List.of(addedGiftMessage()));
  }

  private void previousRelease() {
    placedClass.set(Placed.class);
    store.withUpcasters(null);
  }

  /** Version 2 added an optional field: the 1 -> 2 step has nothing to rewrite. */
  private static EventUpcaster addedGiftMessage() {
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
        return eventData;
      }
    };
  }

  private EventEnvelope envelope(long version, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(version),
        stream,
        new Version(version),
        new EventType("Placed"),
        event,
        new EventMetadata(
            EventId.of("evt_" + version),
            CommandId.of("cmd_" + version),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z")));
  }
}

package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.test.EventStoreFixture.event;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class EventStoreFixtureTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  record ItemAdded(String sku) implements DomainEvent {}

  @Test
  void appendsSucceedWithEventsPersisted() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    EventStoreFixture.store()
        .when()
        .append(
            s,
            List.of(event(s, new Version(1), new EventType("Added"), new Added("X"))),
            new Version(0))
        .eventsPersisted()
        .expectEventCount(s, 1);
  }

  @Test
  void givenThenWhenAppendsTwoBatches() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-2"));
    EventStoreFixture.store()
        .given()
        .append(s, List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))))
        .when()
        .append(
            s,
            List.of(event(s, new Version(2), new EventType("Added"), new Added("B"))),
            new Version(1))
        .eventsPersisted()
        .expectEventCount(s, 2);
  }

  @Test
  void expectOptimisticLockOnVersionConflict() {
    var store = new InMemoryEventStore();
    var s = StreamId.of(TYPE, AggregateId.of("cart-3"));
    // Seed the store so version is 1
    EventStoreFixture.store(store)
        .when()
        .append(
            s,
            List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))),
            new Version(0))
        .eventsPersisted();
    // Now expect a lock failure when using stale version 0
    EventStoreFixture.store(store)
        .when()
        .append(
            s,
            List.of(event(s, new Version(2), new EventType("Added"), new Added("B"))),
            new Version(0))
        .expectOptimisticLockException();
  }

  // Regression: given() + expectOptimisticLockException() used to be impossible — the
  // given-stage operations were replayed by the then stage and always broke the assertion.
  @Test
  void givenStateFollowedByStaleAppendExpectsOptimisticLock() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-4"));
    EventStoreFixture.store()
        .given()
        .append(s, List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))))
        .when()
        .append(
            s,
            List.of(event(s, new Version(2), new EventType("Added"), new Added("B"))),
            new Version(0)) // stale version
        .expectOptimisticLockException();
  }

  @Test
  void eventsPersistedFailsWhenTheAppendThrew() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-5"));
    var then =
        EventStoreFixture.store()
            .given()
            .append(s, List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))))
            .when()
            .append(
                s,
                List.of(event(s, new Version(2), new EventType("Added"), new Added("B"))),
                new Version(0)); // stale version
    assertThrows(AssertionError.class, then::eventsPersisted);
  }

  @Test
  void expectOptimisticLockExceptionFailsWhenNothingWasThrown() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-6"));
    var then =
        EventStoreFixture.store()
            .when()
            .append(
                s,
                List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))),
                new Version(0));
    assertThrows(AssertionError.class, then::expectOptimisticLockException);
  }

  @Test
  void expectOptimisticLockExceptionFailsOnADifferentException() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-7"));
    var then =
        EventStoreFixture.store(new ThrowingEventStore())
            .when()
            .append(
                s,
                List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))),
                new Version(0));
    var error = assertThrows(AssertionError.class, then::expectOptimisticLockException);
    assertInstanceOf(IllegalStateException.class, error.getCause());
  }

  @Test
  void loadCapturesHistoryForAssertions() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-8"));
    var then =
        EventStoreFixture.store()
            .given()
            .append(s, List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))))
            .when()
            .load(s)
            .expectLoadedVersion(1);
    assertEquals(1, then.loadedHistory().events().size());
    assertInstanceOf(Added.class, then.loadedHistory().events().getFirst().event());
  }

  @Test
  void loadedHistoryWithoutLoadThrowsIllegalState() {
    var s = StreamId.of(TYPE, AggregateId.of("cart-9"));
    var then =
        EventStoreFixture.store()
            .when()
            .append(
                s,
                List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))),
                new Version(0));
    assertThrows(IllegalStateException.class, then::loadedHistory);
  }

  // Regression: operations used to accumulate inside the fixture across scenarios; each
  // when() must start a fresh outcome against the live store state.
  @Test
  void multipleScenariosOnTheSameFixtureAreIndependent() {
    var fixture = EventStoreFixture.store();
    var s = StreamId.of(TYPE, AggregateId.of("cart-10"));

    fixture
        .when()
        .append(
            s,
            List.of(event(s, new Version(1), new EventType("Added"), new Added("A"))),
            new Version(0))
        .eventsPersisted();
    fixture
        .when()
        .append(
            s,
            List.of(event(s, new Version(2), new EventType("Added"), new Added("B"))),
            new Version(1))
        .eventsPersisted()
        .expectEventCount(s, 2);
    fixture
        .when()
        .append(
            s,
            List.of(event(s, new Version(3), new EventType("Added"), new Added("C"))),
            new Version(1)) // stale again
        .expectOptimisticLockException()
        .expectEventCount(s, 2);
  }

  @Test
  void storeWithProvidedEventStore() {
    var store = new InMemoryEventStore();
    var s = StreamId.of(TYPE, AggregateId.of("cart-11"));
    EventStoreFixture.store(store)
        .when()
        .append(
            s,
            List.of(event(s, new Version(1), new EventType("Added"), new Added("X"))),
            new Version(0))
        .eventsPersisted();

    assertEquals(1, store.load(s).events().size());
  }

  @Test
  void getEventStoreExposesUnderlyingStore() {
    var underlying = new InMemoryEventStore();
    var s = StreamId.of(TYPE, AggregateId.of("cart-12"));
    var store =
        EventStoreFixture.store(underlying)
            .when()
            .append(
                s,
                List.of(event(s, new Version(1), new EventType("Added"), new Added("X"))),
                new Version(0))
            .eventsPersisted()
            .getEventStore();
    assertSame(underlying, store);
    var history = underlying.load(s);
    assertEquals(1, history.events().size());
    assertInstanceOf(Added.class, history.events().get(0).event());
    assertEquals("X", ((Added) history.events().get(0).event()).sku());
  }

  // Mirrors the class javadoc examples.
  @Test
  void javadocExamplesWork() {
    StreamId cart = StreamId.of(TYPE, AggregateId.of("cart-1"));

    EventStoreFixture.store()
        .given()
        .append(
            cart,
            List.of(
                event(cart, new Version(1), new EventType("ItemAdded"), new ItemAdded("SKU-1"))))
        .when()
        .append(
            cart,
            List.of(
                event(cart, new Version(2), new EventType("ItemAdded"), new ItemAdded("SKU-2"))),
            new Version(1))
        .eventsPersisted()
        .expectEventCount(cart, 2);

    EventStoreFixture.store()
        .given()
        .append(
            cart,
            List.of(
                event(cart, new Version(1), new EventType("ItemAdded"), new ItemAdded("SKU-1"))))
        .when()
        .append(
            cart,
            List.of(
                event(cart, new Version(2), new EventType("ItemAdded"), new ItemAdded("SKU-2"))),
            new Version(0)) // stale
        .expectOptimisticLockException();

    EventStoreFixture.store()
        .given()
        .append(
            cart,
            List.of(
                event(cart, new Version(1), new EventType("ItemAdded"), new ItemAdded("SKU-1"))))
        .when()
        .load(cart)
        .expectLoadedVersion(1);
  }

  /** EventStore stub whose append always fails with a non-optimistic-lock exception. */
  private static final class ThrowingEventStore implements EventStore {

    @Override
    public AggregateHistory load(StreamId streamId) {
      return AggregateHistory.empty();
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      throw new IllegalStateException("append always fails");
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return List.of();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return List.of();
    }
  }
}

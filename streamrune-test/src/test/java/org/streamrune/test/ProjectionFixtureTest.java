package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.test.ProjectionFixture.event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class ProjectionFixtureTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  record ItemAdded(String sku) implements DomainEvent {}

  static class RecordingProjection implements Projection {
    final List<EventEnvelope> received = new ArrayList<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      received.addAll(batch);
    }

    List<EventEnvelope> state() {
      return List.copyOf(received);
    }
  }

  @Test
  void whenReceiveSingleEvent() {
    var p = new RecordingProjection();
    ProjectionFixture.given(p)
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("A")))
        .expectEventCount(1);
    assertEquals(1, p.received.size());
  }

  @Test
  void whenReceiveMultipleEventsVarargs() {
    var p = new RecordingProjection();
    ProjectionFixture.given(p)
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("A")),
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("B")))
        .expectEventCount(2);
    assertEquals(2, p.received.size());
  }

  @Test
  void givenEventsAreProcessed() {
    var p = new RecordingProjection();
    ProjectionFixture.given(p)
        .givenEvents(
            List.of(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("Added"),
                    new Added("A"))))
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("B")))
        .expectEventCount(2);
    assertEquals(2, p.received.size());
  }

  @Test
  void getEventsReturnsAccumulatedList() {
    var p = new RecordingProjection();
    var events =
        ProjectionFixture.given(p)
            .when()
            .receive(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("Added"),
                    new Added("A")))
            .getEvents();
    assertEquals(1, events.size());
    assertInstanceOf(Added.class, events.get(0).event());
    assertEquals("A", ((Added) events.get(0).event()).sku());
    assertThrows(UnsupportedOperationException.class, () -> events.add(null));
  }

  @Test
  void stateRunsTheAssertionAgainstTheSuppliedState() {
    var p = new RecordingProjection();
    var assertionRan = new AtomicBoolean(false);
    ProjectionFixture.given(p, p::state)
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("A")))
        .state(
            state -> {
              assertionRan.set(true);
              assertEquals(1, state.size());
              assertEquals("A", ((Added) state.getFirst().event()).sku());
            });
    assertTrue(assertionRan.get(), "state() must execute the assertion, not discard it");
  }

  // Meta-test: a failing state assertion must fail the surrounding test. Before the fix,
  // state() silently discarded the assertion and a test like this would pass vacuously.
  @Test
  void stateFailuresPropagate() {
    var p = new RecordingProjection();
    var then =
        ProjectionFixture.given(p, p::state)
            .when()
            .receive(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("Added"),
                    new Added("A")));
    assertThrows(AssertionError.class, () -> then.state(state -> fail("boom")));
  }

  @Test
  void stateWithoutSupplierThrowsIllegalState() {
    var p = new RecordingProjection();
    var then =
        ProjectionFixture.<List<EventEnvelope>>given(p)
            .when()
            .receive(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("Added"),
                    new Added("A")));
    var ex = assertThrows(IllegalStateException.class, () -> then.state(state -> {}));
    assertTrue(ex.getMessage().contains("state supplier"));
  }

  @Test
  void stateRejectsNullAssertion() {
    var p = new RecordingProjection();
    var then =
        ProjectionFixture.given(p, p::state)
            .when()
            .receive(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("Added"),
                    new Added("A")));
    assertThrows(NullPointerException.class, () -> then.state(null));
  }

  @Test
  void givenRejectsNullStateSupplier() {
    var p = new RecordingProjection();
    assertThrows(NullPointerException.class, () -> ProjectionFixture.given(p, null));
  }

  @Test
  void givenEventsSkipsWhenListIsEmpty() {
    // Covers the isEmpty() branch in givenEvents()
    var p = new RecordingProjection();
    ProjectionFixture.<String>given(p)
        .givenEvents(List.of())
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("A")))
        .expectEventCount(1);
  }

  // Before the fix, every event() envelope carried GlobalOffset 0, so projections doing
  // offset-based idempotency ("skip if offset <= last seen") processed at most one event.
  @Test
  void eventsGetUniqueMonotonicallyIncreasingOffsets() {
    var e1 =
        event(StreamId.of(TYPE, AggregateId.of("cart-1")), new EventType("Added"), new Added("A"));
    var e2 =
        event(StreamId.of(TYPE, AggregateId.of("cart-2")), new EventType("Added"), new Added("B"));
    var e3 =
        event(StreamId.of(TYPE, AggregateId.of("cart-1")), new EventType("Added"), new Added("C"));
    assertTrue(e1.globalOffset().value() > 0, "delivered offsets start above initial (0)");
    assertTrue(e1.globalOffset().value() < e2.globalOffset().value());
    assertTrue(e2.globalOffset().value() < e3.globalOffset().value());
  }

  @Test
  void offsetDeduplicatingProjectionProcessesEveryEvent() {
    class OffsetDeduplicatingProjection implements Projection {
      final List<EventEnvelope> processed = new ArrayList<>();
      long lastSeenOffset = 0;

      @Override
      public void process(List<EventEnvelope> batch) {
        for (var envelope : batch) {
          if (envelope.globalOffset().value() <= lastSeenOffset) {
            continue; // duplicate or out-of-order delivery — skip
          }
          lastSeenOffset = envelope.globalOffset().value();
          processed.add(envelope);
        }
      }
    }
    var p = new OffsetDeduplicatingProjection();
    ProjectionFixture.given(p)
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("A")),
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("Added"),
                new Added("B")))
        .expectEventCount(2);
    assertEquals(2, p.processed.size(), "distinct offsets must not be skipped as duplicates");
  }

  @Test
  void eventDefaultsToVersionOne() {
    var envelope =
        event(StreamId.of(TYPE, AggregateId.of("cart-1")), new EventType("Added"), new Added("A"));
    assertEquals(1, envelope.version().value(), "store assigns version 1 to a fresh stream event");
  }

  @Test
  void eventAcceptsExplicitVersion() {
    var envelope =
        event(
            StreamId.of(TYPE, AggregateId.of("cart-1")),
            new EventType("Added"),
            new Added("A"),
            new Version(7));
    assertEquals(7, envelope.version().value());
  }

  // Mirrors the class javadoc examples.
  @Test
  void javadocExamplesWork() {
    var projection = new RecordingProjection();
    ProjectionFixture.given(projection, projection::state)
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("ItemAdded"),
                new ItemAdded("SKU-1")))
        .state(state -> assertEquals(1, state.size()));

    ProjectionFixture.given(projection, projection::state)
        .givenEvents(
            List.of(
                event(
                    StreamId.of(TYPE, AggregateId.of("cart-1")),
                    new EventType("ItemAdded"),
                    new ItemAdded("SKU-1"))))
        .when()
        .receive(
            event(
                StreamId.of(TYPE, AggregateId.of("cart-1")),
                new EventType("ItemAdded"),
                new ItemAdded("SKU-2")))
        .expectEventCount(2);
  }
}

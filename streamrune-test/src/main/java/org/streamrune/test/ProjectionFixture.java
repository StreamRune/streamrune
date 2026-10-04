package org.streamrune.test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Testing fixture for {@link Projection} testing.
 *
 * <p>To assert on projection state with {@link ThenStage#state(Consumer)}, create the fixture with
 * a state supplier that reads the state from the projection under test:
 *
 * <pre>{@code
 * var projection = new RecordingProjection(); // any Projection with readable state
 * StreamId cart = StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-1"));
 * ProjectionFixture.given(projection, projection::state)
 *     .when()
 *     .receive(event(cart, "ItemAdded", new ItemAdded("SKU-1")))
 *     .state(state -> assertEquals(1, state.size()));
 *
 * ProjectionFixture.given(projection, projection::state)
 *     .givenEvents(List.of(event(cart, "ItemAdded", new ItemAdded("SKU-1"))))
 *     .when()
 *     .receive(event(cart, "ItemAdded", new ItemAdded("SKU-2")))
 *     .expectEventCount(2);
 * }</pre>
 *
 * @param <S> State type exposed by the projection under test
 */
public final class ProjectionFixture<S> {

  /**
   * Source of globally unique, monotonically increasing offsets for {@link #event}. Shared across
   * all fixtures in the JVM so that every created envelope mimics the store contract: delivered
   * offsets strictly increase in creation order.
   */
  private static final AtomicLong NEXT_OFFSET = new AtomicLong();

  private final Projection projection;
  private final Supplier<S> stateSupplier;
  private final List<EventEnvelope> events = new ArrayList<>();

  private ProjectionFixture(Projection projection, Supplier<S> stateSupplier) {
    this.projection = Objects.requireNonNull(projection, "projection is required");
    this.stateSupplier = stateSupplier;
  }

  /**
   * Creates a new fixture for the given projection. State assertions via {@code state(...)} are not
   * available on fixtures created this way — use {@link #given(Projection, Supplier)} instead.
   *
   * @param <S> state type exposed by the projection under test
   * @param projection the projection under test
   * @return a new fixture without a state supplier
   */
  public static <S> ProjectionFixture<S> given(Projection projection) {
    return new ProjectionFixture<>(projection, null);
  }

  /**
   * Creates a new fixture for the given projection with a state supplier, enabling {@code
   * state(...)} assertions. The supplier is invoked each time {@code state(...)} is called and must
   * return the projection's current state.
   *
   * @param <S> state type exposed by the projection under test
   * @param projection the projection under test
   * @param stateSupplier reads the projection's current state for {@code state(...)} assertions
   * @return a new fixture with state assertions enabled
   */
  public static <S> ProjectionFixture<S> given(Projection projection, Supplier<S> stateSupplier) {
    return new ProjectionFixture<>(
        projection, Objects.requireNonNull(stateSupplier, "stateSupplier is required"));
  }

  /** Sets up initial events (for building initial state). */
  public GivenStage givenEvents(List<EventEnvelope> events) {
    if (!events.isEmpty()) {
      this.events.addAll(events);
      projection.process(events);
    }
    return new GivenStage();
  }

  /** Starts receiving events. */
  public WhenStage when() {
    return new WhenStage();
  }

  /**
   * Creates a test event with version 1 — the version the store assigns to the first event of a
   * fresh stream. For projections that are version-sensitive (per-stream ordering or
   * deduplication), pass an explicit version via {@link #event(StreamId, EventType, DomainEvent,
   * Version)} instead.
   *
   * <p>The envelope's global offset is unique and monotonically increasing across all envelopes
   * created by this method in the JVM, matching how the store delivers events to projections.
   * Offset-based idempotency ("skip if offset &lt;= last seen") therefore works under the fixture,
   * but tests must not assert exact offset values.
   *
   * @param streamId stream the event belongs to
   * @param eventType event type
   * @param event the domain event payload
   * @return an envelope with a fresh unique global offset and version 1
   */
  public static EventEnvelope event(StreamId streamId, EventType eventType, DomainEvent event) {
    return event(streamId, eventType, event, Version.initial().next());
  }

  /**
   * Creates a test event with an explicit per-stream version. The global offset is assigned
   * automatically and is unique and monotonically increasing across all envelopes created by the
   * {@code event(...)} factories in the JVM — see {@link #event(StreamId, EventType, DomainEvent)}.
   *
   * @param streamId stream the event belongs to
   * @param eventType event type
   * @param event the domain event payload
   * @param version per-stream version of the event (the store assigns 1, 2, 3, ... per stream)
   * @return an envelope with a fresh unique global offset and the given version
   */
  public static EventEnvelope event(
      StreamId streamId, EventType eventType, DomainEvent event, Version version) {
    return new EventEnvelope(
        GlobalOffset.of(NEXT_OFFSET.incrementAndGet()),
        streamId,
        version,
        eventType,
        event,
        new EventMetadata(
            org.streamrune.core.IdGenerator.generateEventId(),
            org.streamrune.core.IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.now()));
  }

  public final class GivenStage {

    /** Moves to when stage. */
    public WhenStage when() {
      return ProjectionFixture.this.when();
    }
  }

  public final class WhenStage {

    /** Receives a batch of events. */
    public ThenStage receive(List<EventEnvelope> events) {
      ProjectionFixture.this.events.addAll(events);
      projection.process(events);
      return new ThenStage();
    }

    /** Receives a single event. */
    public ThenStage receive(EventEnvelope event) {
      return receive(List.of(event));
    }

    /** Receives multiple events. */
    public ThenStage receive(EventEnvelope... events) {
      return receive(List.of(events));
    }
  }

  public final class ThenStage {

    /**
     * Asserts on the projection state. The state is obtained from the state supplier passed to
     * {@code given(Projection, Supplier)} and handed to the assertion, which runs immediately.
     *
     * @param assertion assertion to run against the projection's current state
     * @return this stage for chaining
     * @throws IllegalStateException if the fixture was created without a state supplier
     */
    public ThenStage state(Consumer<S> assertion) {
      Objects.requireNonNull(assertion, "assertion is required");
      if (stateSupplier == null) {
        throw new IllegalStateException(
            "No state supplier configured — create the fixture with"
                + " ProjectionFixture.given(projection, stateSupplier) to use state assertions");
      }
      assertion.accept(stateSupplier.get());
      return this;
    }

    /** Asserts the projection received the expected number of events. */
    public ThenStage expectEventCount(int count) {
      org.junit.jupiter.api.Assertions.assertEquals(count, events.size());
      return this;
    }

    /** Gets all received events. */
    public List<EventEnvelope> getEvents() {
      return List.copyOf(events);
    }
  }
}

package org.streamrune.test;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Testing fixture for {@link EventStore} operations.
 *
 * <p>Every stage executes against the store immediately: {@code given().append(...)} seeds state
 * (failures propagate as setup errors), {@code when().append(...)} captures the outcome — success
 * or exception — for the assertions that follow. A fixture can run multiple when/then scenarios
 * against the same store; each {@code when()} starts a fresh outcome.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * StreamId cart = StreamId.of(AggregateType.of("cart"), AggregateId.of("cart-1"));
 *
 * EventStoreFixture.store()
 *     .given()
 *     .append(cart, List.of(event(cart, 1, "ItemAdded", new ItemAdded("SKU-1"))))
 *     .when()
 *     .append(cart, List.of(event(cart, 2, "ItemAdded", new ItemAdded("SKU-2"))), 1)
 *     .eventsPersisted()
 *     .expectEventCount(cart, 2);
 *
 * EventStoreFixture.store()
 *     .given()
 *     .append(cart, List.of(event(cart, 1, "ItemAdded", new ItemAdded("SKU-1"))))
 *     .when()
 *     .append(cart, List.of(event(cart, 2, "ItemAdded", new ItemAdded("SKU-2"))), 0) // stale
 *     .expectOptimisticLockException();
 *
 * EventStoreFixture.store()
 *     .given()
 *     .append(cart, List.of(event(cart, 1, "ItemAdded", new ItemAdded("SKU-1"))))
 *     .when()
 *     .load(cart)
 *     .expectLoadedVersion(1);
 * }</pre>
 */
public final class EventStoreFixture {

  private final EventStore eventStore;

  private EventStoreFixture(EventStore eventStore) {
    this.eventStore = Objects.requireNonNull(eventStore, "eventStore is required");
  }

  /** Creates a new fixture with an in-memory event store. */
  public static EventStoreFixture store() {
    return new EventStoreFixture(new InMemoryEventStore());
  }

  /** Creates a new fixture with the given event store. */
  public static EventStoreFixture store(EventStore eventStore) {
    return new EventStoreFixture(eventStore);
  }

  /** Starts setting up given state. */
  public GivenStage given() {
    return new GivenStage();
  }

  /** Starts the when stage. */
  public WhenStage when() {
    return new WhenStage();
  }

  /**
   * Creates a test event envelope.
   *
   * <p>The given {@code version} is carried as-is only when the envelope is used standalone, e.g.
   * fed directly to a projection. On {@code append}, every store recomputes versions as {@code
   * expectedVersion + 1..n} and assigns global offsets — the version and offset placeholders set
   * here are then discarded.
   */
  public static EventEnvelope event(
      StreamId streamId, Version version, EventType eventType, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(), // placeholder — reassigned by the store on append
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
            java.time.Instant.now()));
  }

  public final class GivenStage {

    /**
     * Appends events at the stream's current version, immediately. Failures propagate — given state
     * is test setup, not the behavior under test.
     */
    public GivenStage append(StreamId streamId, List<EventEnvelope> events) {
      var history = eventStore.load(streamId);
      eventStore.append(streamId, events, history.version());
      return this;
    }

    /** Moves to when stage. */
    public WhenStage when() {
      return EventStoreFixture.this.when();
    }
  }

  public final class WhenStage {

    /**
     * Appends events with the given expected version, immediately. Any {@link RuntimeException} is
     * captured for the assertions that follow instead of propagating.
     */
    public ThenStage append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      try {
        eventStore.append(streamId, events, expectedVersion);
        return new ThenStage(null, null);
      } catch (RuntimeException e) {
        return new ThenStage(e, null);
      }
    }

    /** Loads the aggregate history, immediately, for the assertions that follow. */
    public ThenStage load(StreamId streamId) {
      return new ThenStage(null, eventStore.load(streamId));
    }
  }

  public final class ThenStage {

    private final RuntimeException thrown;
    private final AggregateHistory loadedHistory;

    private ThenStage(RuntimeException thrown, AggregateHistory loadedHistory) {
      this.thrown = thrown;
      this.loadedHistory = loadedHistory;
    }

    /** Asserts the when-stage append succeeded. */
    public ThenStage eventsPersisted() {
      if (thrown != null) {
        throw new AssertionError("Expected append to succeed but it threw: " + thrown, thrown);
      }
      return this;
    }

    /** Asserts the when-stage append threw an {@link OptimisticLockException}. */
    public ThenStage expectOptimisticLockException() {
      if (thrown == null) {
        throw new AssertionError("Expected OptimisticLockException but no exception was thrown");
      }
      if (!(thrown instanceof OptimisticLockException)) {
        throw new AssertionError("Expected OptimisticLockException but got: " + thrown, thrown);
      }
      return this;
    }

    /** Asserts the stream currently has the expected number of events (snapshot excluded). */
    public ThenStage expectEventCount(StreamId streamId, int count) {
      var history = eventStore.load(streamId);
      org.junit.jupiter.api.Assertions.assertEquals(count, history.events().size());
      return this;
    }

    /** Asserts the version of the history captured by {@code when().load(...)}. */
    public ThenStage expectLoadedVersion(long version) {
      org.junit.jupiter.api.Assertions.assertEquals(version, loadedHistory().version().value());
      return this;
    }

    /**
     * Returns the history captured by {@code when().load(...)} for custom assertions.
     *
     * @throws IllegalStateException if the when stage was not a load
     */
    public AggregateHistory loadedHistory() {
      if (loadedHistory == null) {
        throw new IllegalStateException(
            "No history captured — use when().load(streamId) before asserting on it");
      }
      return loadedHistory;
    }

    /** Gets the event store for further assertions. */
    public EventStore getEventStore() {
      return eventStore;
    }
  }
}

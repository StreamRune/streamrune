package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The order {@link SseEventPublisher#hasSubscribers()} rests on: a subscription is counted before
 * it joins the list of its stream, and counted out only after it has left that list. The count is
 * therefore never below the number of subscriptions a published event would be handed to, and
 * {@code hasSubscribers()} never answers "none" while a client is registered: the property the
 * {@link SseEventFeed} needs before it moves its position without reading.
 *
 * <p>Each test stands between the two steps of one registration or one removal, through {@link
 * SseEventPublisher.RegistrySteps}, and reads the publisher there, on the thread that performs the
 * change. Nothing is timed and nothing races: the outcome is the same on every run.
 */
class SseEventPublisherRegistryOrderTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("test"), AggregateId.of("ordered"));

  /** What the publisher answers at one stop between two steps. */
  record Seen(boolean hasSubscribers, int listedOnTheStream) {}

  /** Records what the publisher answers between the two steps of each change. */
  static final class BetweenTheSteps implements SseEventPublisher.RegistrySteps {

    private SseEventPublisher publisher;
    final List<Seen> onTheWayIn = new CopyOnWriteArrayList<>();
    final List<Seen> onTheWayOut = new CopyOnWriteArrayList<>();

    SseEventPublisher publisher() {
      publisher = new SseEventPublisher(SseEventPublisher.DEFAULT_QUEUE_CAPACITY, this);
      return publisher;
    }

    @Override
    public void countedAndNotListedYet(StreamId streamId) {
      onTheWayIn.add(seen(streamId));
    }

    @Override
    public void unlistedAndStillCounted(StreamId streamId) {
      onTheWayOut.add(seen(streamId));
    }

    private Seen seen(StreamId streamId) {
      return new Seen(publisher.hasSubscribers(), publisher.subscriberCount(streamId));
    }
  }

  /**
   * The first subscription of a publisher: between the two steps it is counted, so the answer is
   * already "somebody", and no list holds it yet. An answer taken from the size of the map of
   * streams reads "none" at this stop, because the stream has no entry until the registration is
   * complete.
   */
  @Test
  void theFirstSubscriptionIsCountedBeforeAnyListHoldsIt() {
    var steps = new BetweenTheSteps();
    try (var publisher = steps.publisher()) {
      assertFalse(publisher.hasSubscribers());

      publisher.subscribe(STREAM, envelope -> {});

      assertEquals(List.of(new Seen(true, 0)), steps.onTheWayIn);
      assertTrue(publisher.hasSubscribers());
      assertEquals(1, publisher.subscriberCount(STREAM));
    }
  }

  /**
   * A second subscription of a stream that has a list already: between the two steps that list
   * still holds the first one only. The stop sits before the join, so a join that came first would
   * show two.
   */
  @Test
  void aSubscriptionIsCountedBeforeItJoinsTheListOfItsStream() {
    var steps = new BetweenTheSteps();
    try (var publisher = steps.publisher()) {
      publisher.subscribe(STREAM, envelope -> {});

      publisher.subscribe(STREAM, envelope -> {});

      assertEquals(List.of(new Seen(true, 0), new Seen(true, 1)), steps.onTheWayIn);
      assertEquals(2, publisher.subscriberCount(STREAM));
    }
  }

  /**
   * An unsubscribe of the only subscription: between the two steps the list is empty and the
   * subscription is still counted, so the answer turns to "none" only after nothing is listed.
   */
  @Test
  void anUnsubscribedSubscriptionLeavesTheListBeforeItIsCountedOut() {
    var steps = new BetweenTheSteps();
    try (var publisher = steps.publisher()) {
      SseEventPublisher.SseSubscriber subscriber = envelope -> {};
      publisher.subscribe(STREAM, subscriber);

      publisher.unsubscribe(STREAM, subscriber);

      assertEquals(List.of(new Seen(true, 0)), steps.onTheWayOut);
      assertFalse(publisher.hasSubscribers());
    }
  }

  /** An unsubscribe that removes nobody takes neither step. */
  @Test
  void anUnsubscribeThatMatchesNobodyCountsNobodyOut() {
    var steps = new BetweenTheSteps();
    try (var publisher = steps.publisher()) {
      publisher.subscribe(STREAM, envelope -> {});

      publisher.unsubscribe(STREAM, envelope -> {});

      assertEquals(List.of(), steps.onTheWayOut);
      assertTrue(publisher.hasSubscribers());
      assertEquals(1, publisher.subscriberCount(STREAM));
    }
  }

  /**
   * An eviction of the only subscription (its {@code send} throws): the same order as an
   * unsubscribe, taken on the subscription's delivery thread.
   */
  @Test
  void anEvictedSubscriptionLeavesTheListBeforeItIsCountedOut() {
    var steps = new BetweenTheSteps();
    try (var publisher = steps.publisher()) {
      var evicted = new CountDownLatch(1);
      publisher.subscribe(
          STREAM,
          envelope -> {
            throw new IllegalStateException("the client is gone");
          },
          cause -> evicted.countDown());

      publisher.publish(envelope());

      // The disconnect hook runs after the eviction has taken both steps.
      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> evicted.getCount() == 0);
      assertEquals(List.of(new Seen(true, 0)), steps.onTheWayOut);
      assertFalse(publisher.hasSubscribers());
    }
  }

  private static EventEnvelope envelope() {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        Version.initial(),
        new EventType("TestEvent"),
        new Stored("test"),
        new EventMetadata(
            EventId.of("test-event-id"),
            CommandId.of("test-command-id"),
            null,
            null,
            CorrelationId.of("test-correlation-id"),
            null,
            null,
            Instant.now()));
  }

  private record Stored(String data) implements DomainEvent {}
}

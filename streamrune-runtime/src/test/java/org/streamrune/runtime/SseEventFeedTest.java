package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;

/**
 * The feed publishes the events appended to the store after it started, each to the subscribers of
 * the event's own stream.
 */
class SseEventFeedTest {

  private static final Duration FAST = Duration.ofMillis(20);
  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId ORDER_1 = StreamId.of(ORDER, AggregateId.of("o-1"));
  private static final StreamId ORDER_2 = StreamId.of(ORDER, AggregateId.of("o-2"));

  record Placed(String note) implements DomainEvent {}

  @Test
  void anEventAppendedAfterTheStartReachesTheSubscribersOfItsOwnStreamOnly() {
    var store = new InMemoryEventStore();
    var first = new CopyOnWriteArrayList<EventEnvelope>();
    var second = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, first::add);
      publisher.subscribe(ORDER_2, second::add);
      feed.start();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "for-o-1")), Version.initial());
      store.append(ORDER_2, List.of(envelope(ORDER_2, 1, "for-o-2")), Version.initial());

      await().atMost(Duration.ofSeconds(5)).until(() -> !first.isEmpty() && !second.isEmpty());
      assertThat(first).extracting(e -> ((Placed) e.event()).note()).containsExactly("for-o-1");
      assertThat(second).extracting(e -> ((Placed) e.event()).note()).containsExactly("for-o-2");
    }
  }

  @Test
  void theEventsStoredBeforeTheStartAreNotReplayed() {
    var store = new InMemoryEventStore();
    store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "history")), Version.initial());
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, received::add);
      feed.start();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "live")), new Version(1));

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received).extracting(e -> ((Placed) e.event()).note()).containsExactly("live");
    }
  }

  @Test
  void aRestartedFeedContinuesFromTheHeadAtItsRestart() {
    var store = new InMemoryEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, received::add);
      feed.start();
      feed.close();
      assertThat(feed.isRunning()).isFalse();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "while-stopped")), Version.initial());
      feed.start();
      assertThat(feed.isRunning()).isTrue();
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "after-restart")), new Version(1));

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("after-restart");
    }
  }

  @Test
  void closeStopsThePollingThreadAndIsIdempotent() {
    var store = new InMemoryEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher()) {
      var feed = new SseEventFeed(store, publisher, FAST);
      publisher.subscribe(ORDER_1, received::add);
      feed.start();
      feed.start();
      assertThat(feed.isRunning()).isTrue();

      feed.close();
      feed.close();

      assertThat(feed.isRunning()).isFalse();
      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "after-close")), Version.initial());
      await().during(Duration.ofMillis(200)).atMost(Duration.ofSeconds(2)).until(received::isEmpty);
    }
  }

  @Test
  void aStoreThatCannotReportItsHeadRefusesTheStart() {
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(new HeadlessEventStore(), publisher, FAST)) {
      assertThatThrownBy(feed::start)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("lastGlobalOffset()")
          .hasMessageContaining(HeadlessEventStore.class.getName());
      assertThat(feed.isRunning()).isFalse();
    }
  }

  @Test
  void rejectsMissingCollaboratorsAndANonPositiveInterval() {
    var store = new InMemoryEventStore();
    try (var publisher = new SseEventPublisher()) {
      assertThatThrownBy(() -> new SseEventFeed(null, publisher, FAST))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new SseEventFeed(store, null, FAST))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new SseEventFeed(store, publisher, null))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new SseEventFeed(store, publisher, Duration.ZERO))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("streamrune.sse.polling-interval");
    }
  }

  private static EventEnvelope envelope(StreamId stream, long version, String note) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        stream,
        new Version(version),
        new EventType("Placed"),
        new Placed(note),
        new EventMetadata(
            EventId.of("evt-" + stream.value() + "-" + version),
            CommandId.of("cmd-" + stream.value() + "-" + version),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  /** An event store that keeps the default {@code lastGlobalOffset()}. */
  private static final class HeadlessEventStore implements EventStore {

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public EventStore.AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      throw new UnsupportedOperationException();
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

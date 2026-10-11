package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
      assertThatThrownBy(() -> new SseEventFeed(store, publisher, Duration.ofMillis(-5)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("streamrune.sse.polling-interval");
    }
  }

  @Test
  void rejectsAPositiveIntervalBelowOneMillisecond() {
    // The polling thread sleeps whole milliseconds: half a millisecond would be no sleep at all,
    // one read of the global stream after another.
    var store = new InMemoryEventStore();
    try (var publisher = new SseEventPublisher()) {
      assertThatThrownBy(() -> new SseEventFeed(store, publisher, Duration.ofNanos(500_000)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("streamrune.sse.polling-interval")
          .hasMessageContaining("at least 1ms")
          .hasMessageContaining("PT0.0005S");
      assertThatThrownBy(() -> new SseEventFeed(store, publisher, Duration.ofNanos(999_999)))
          .isInstanceOf(IllegalArgumentException.class);

      try (var feed = new SseEventFeed(store, publisher, Duration.ofMillis(1))) {
        assertThat(feed.isRunning()).as("one millisecond is the smallest interval").isFalse();
      }
    }
  }

  @Test
  void aFailedReadIsRetriedFromTheSamePosition() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, received::add);
      feed.start();
      store.failNextReads(3, new IllegalStateException("the database is away"));

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "first")), Version.initial());
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "second")), new Version(1));

      await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
      assertThat(received)
          .as("nothing stored while the reads failed is skipped, nothing is delivered twice")
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("first", "second");
      assertThat(store.failedReadsAfter())
          .as("every failed read and the read that succeeded asked for the same position")
          .hasSize(3)
          .containsOnly(store.firstSuccessfulReadAfter());
      assertThat(feed.isRunning()).as("a failed read does not stop the feed").isTrue();
    }
  }

  @Test
  void anEventThePublisherRefusesDoesNotStopItsPageAndIsNotRetried() {
    var store = new InMemoryEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var publishCalls = new CopyOnWriteArrayList<String>();
    var publisher =
        new SseEventPublisher() {
          @Override
          public void publish(EventEnvelope envelope) {
            String note = ((Placed) envelope.event()).note();
            publishCalls.add(note);
            if (note.equals("refused")) {
              throw new IllegalStateException("the publisher refuses this event");
            }
            super.publish(envelope);
          }
        };
    try (publisher;
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, received::add);
      feed.start();

      // One append, so the three events are read as one page.
      store.append(
          ORDER_1,
          List.of(
              envelope(ORDER_1, 1, "before"),
              envelope(ORDER_1, 2, "refused"),
              envelope(ORDER_1, 3, "after")),
          Version.initial());

      await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 2);
      assertThat(received)
          .as("the events around the refused one are delivered")
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("before", "after");
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .untilAsserted(
              () ->
                  assertThat(publishCalls)
                      .as("the page is not read again: no event is published twice")
                      .containsExactly("before", "refused", "after"));
    }
  }

  @Test
  void aFeedWhosePollingThreadDiedReportsItAndIsReplacedByTheNextStart() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      publisher.subscribe(ORDER_1, received::add);
      feed.start();
      assertThat(feed.isStarted()).isTrue();
      assertThat(feed.isRunning()).isTrue();

      // An Error is not retried: it ends the polling thread.
      store.failNextReads(1, new NoClassDefFoundError("an event class is missing"));
      await().atMost(Duration.ofSeconds(5)).until(() -> !feed.isRunning());
      assertThat(feed.isStarted())
          .as("started and not running: the state a health check reports as DOWN")
          .isTrue();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "while-dead")), Version.initial());
      feed.start();

      assertThat(feed.isRunning()).as("start() replaces the dead subscription").isTrue();
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "after-restart")), new Version(1));
      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .as("the replacement begins at the head of its own start")
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("after-restart");
    }
  }

  @Test
  void theFeedCountsItsConsecutiveFailedReads() {
    var store = new FailingReadsEventStore();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      assertThat(feed.isStarted()).isFalse();
      assertThat(feed.consecutiveFailures()).as("a stopped feed has failed nothing").isZero();
      // With a client subscribed the feed reads events, and those reads are the ones that fail.
      publisher.subscribe(ORDER_1, _ -> {});
      feed.start();

      store.failNextReads(Integer.MAX_VALUE, new IllegalStateException("the database is away"));
      await().atMost(Duration.ofSeconds(5)).until(() -> feed.consecutiveFailures() >= 2);
      assertThat(feed.isRunning()).as("retrying, not dead").isTrue();

      store.failNextReads(0, null);
      await().atMost(Duration.ofSeconds(10)).until(() -> feed.consecutiveFailures() == 0);

      feed.close();
      assertThat(feed.isStarted()).isFalse();
      assertThat(feed.consecutiveFailures()).isZero();
    }
  }

  // ---- with no client subscribed the feed reads no event --------------------------------------

  @Test
  void withNoSubscriberTheFeedReadsNoEventAndItsPositionFollowsTheHead() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      feed.start();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "nobody-1")), Version.initial());
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "nobody-2")), new Version(1));
      store.append(ORDER_2, List.of(envelope(ORDER_2, 1, "nobody-3")), Version.initial());
      awaitIdlePolls(store, 3);

      assertThat(store.payloadReadsAfter())
          .as("no event is read, so none is decrypted, while nobody is subscribed")
          .isEmpty();

      publisher.subscribe(ORDER_1, received::add);
      store.append(ORDER_1, List.of(envelope(ORDER_1, 3, "live")), new Version(2));

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .as("the events stored while nobody was subscribed are behind the feed's position")
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("live");
      assertThat(store.payloadReadsAfter().getFirst())
          .as("the first read of events starts at the head the idle feed had followed")
          .isEqualTo(GlobalOffset.of(3));
    }
  }

  @Test
  void whenTheLastSubscriberLeavesTheFeedReturnsToReadingNoEvent() {
    var store = new FailingReadsEventStore();
    var first = new CopyOnWriteArrayList<EventEnvelope>();
    var second = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      SseEventPublisher.SseSubscriber firstClient = first::add;
      publisher.subscribe(ORDER_1, firstClient);
      feed.start();
      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "for-the-first")), Version.initial());
      await().atMost(Duration.ofSeconds(5)).until(() -> !first.isEmpty());

      publisher.unsubscribe(ORDER_1, firstClient);
      // The feed reads the head only while it is idle: these polls found nobody subscribed.
      awaitIdlePolls(store, 3);
      int payloadReadsWhileSubscribed = store.payloadReadsAfter().size();
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "nobody-1")), new Version(1));
      store.append(ORDER_1, List.of(envelope(ORDER_1, 3, "nobody-2")), new Version(2));
      awaitIdlePolls(store, 3);

      assertThat(store.payloadReadsAfter())
          .as("no event is read after the last subscriber left")
          .hasSize(payloadReadsWhileSubscribed);

      publisher.subscribe(ORDER_1, second::add);
      store.append(ORDER_1, List.of(envelope(ORDER_1, 4, "for-the-second")), new Version(3));
      await().atMost(Duration.ofSeconds(5)).until(() -> !second.isEmpty());
      assertThat(second)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("for-the-second");
      assertThat(first).as("the client that left receives nothing more").hasSize(1);
    }
  }

  // ---- a client that registers while the feed is idle loses no event stored after it ----------

  /**
   * The interleaving that loses an event when the head is read after the look for subscribers and
   * the position is moved to it unconditionally: nobody subscribed, then a client registers, then
   * an event is stored, then the head is captured, with that event in it.
   */
  @Test
  void aClientRegisteredAndAnEventStoredBetweenTheIdleLookAndTheHeadCaptureAreNotSkipped() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      feed.start();
      awaitIdlePolls(store, 2);

      store.beforeNextHeadRead(
          () -> {
            publisher.subscribe(ORDER_1, received::add);
            store.append(
                ORDER_1,
                List.of(envelope(ORDER_1, 1, "stored-after-the-registration")),
                Version.initial());
          });

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("stored-after-the-registration");
    }
  }

  /** The client registers and the event is stored after the head was captured, before the check. */
  @Test
  void aClientRegisteredAndAnEventStoredBetweenTheHeadCaptureAndTheCheckAreNotSkipped() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      feed.start();
      awaitIdlePolls(store, 2);

      store.afterNextHeadRead(
          () -> {
            publisher.subscribe(ORDER_1, received::add);
            store.append(
                ORDER_1,
                List.of(envelope(ORDER_1, 1, "stored-after-the-registration")),
                Version.initial());
          });

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("stored-after-the-registration");
    }
  }

  /**
   * The client registers and the event is stored after the check found nobody, before the position
   * moves: the position moves to the head captured before both, so the event stays ahead of it.
   */
  @Test
  void aClientRegisteredAndAnEventStoredBetweenTheCheckAndTheMoveAreNotSkipped() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new ObservedPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      store.append(ORDER_2, List.of(envelope(ORDER_2, 1, "history")), Version.initial());
      feed.start();
      awaitIdlePolls(store, 2);

      // Armed from inside a read of the head, so the next answer of the publisher is the check
      // that follows the capture of that head.
      store.afterNextHeadRead(
          () ->
              publisher.afterNextEmptyAnswer(
                  () -> {
                    publisher.subscribe(ORDER_1, received::add);
                    store.append(
                        ORDER_1,
                        List.of(envelope(ORDER_1, 1, "stored-after-the-registration")),
                        Version.initial());
                  }));

      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("stored-after-the-registration");
    }
  }

  /**
   * What a client may receive from before its registration: the events stored since the feed last
   * moved its position, here while the reads of the head failed. Nothing stored after the
   * registration is lost to the failures.
   */
  @Test
  void aClientRegisteredWhileTheHeadReadsFailReceivesWhatWasStoredSinceTheLastPosition() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      feed.start();
      awaitIdlePolls(store, 3);

      store.failNextHeadReads(Integer.MAX_VALUE);
      await().atMost(Duration.ofSeconds(5)).until(() -> feed.consecutiveFailures() >= 1);
      store.append(
          ORDER_1, List.of(envelope(ORDER_1, 1, "during-the-failures")), Version.initial());
      publisher.subscribe(ORDER_1, received::add);
      store.append(
          ORDER_1, List.of(envelope(ORDER_1, 2, "after-the-registration")), new Version(1));

      await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 2);
      assertThat(received)
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("during-the-failures", "after-the-registration");
    }
  }

  @Test
  void aFailedReadOfTheHeadIsCountedAndRetried() {
    var store = new FailingReadsEventStore();
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(store, publisher, FAST)) {
      feed.start();
      awaitIdlePolls(store, 2);

      store.failNextHeadReads(Integer.MAX_VALUE);
      await().atMost(Duration.ofSeconds(5)).until(() -> feed.consecutiveFailures() >= 2);
      assertThat(feed.isRunning()).as("retrying, not dead").isTrue();
      assertThat(store.payloadReadsAfter()).as("a failed head read reads no event").isEmpty();

      store.append(ORDER_1, List.of(envelope(ORDER_1, 1, "nobody")), Version.initial());
      store.failNextHeadReads(0);
      await().atMost(Duration.ofSeconds(10)).until(() -> feed.consecutiveFailures() == 0);
      awaitIdlePolls(store, 3);

      publisher.subscribe(ORDER_1, received::add);
      store.append(ORDER_1, List.of(envelope(ORDER_1, 2, "live")), new Version(1));
      await().atMost(Duration.ofSeconds(5)).until(() -> !received.isEmpty());
      assertThat(received)
          .as("the retried head read moved the position past what nobody was subscribed for")
          .extracting(e -> ((Placed) e.event()).note())
          .containsExactly("live");
    }
  }

  /** Waits until {@code count} more reads of the head have returned: that many idle polls. */
  private static void awaitIdlePolls(FailingReadsEventStore store, int count) {
    int target = store.completedHeadReads() + count;
    await().atMost(Duration.ofSeconds(5)).until(() -> store.completedHeadReads() >= target);
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

  /**
   * An in-memory store that records how the feed reads it, fails its next reads on request, and
   * lets a test run an action on the polling thread around the next read of the head.
   */
  private static final class FailingReadsEventStore implements EventStore {

    private final InMemoryEventStore delegate = new InMemoryEventStore();
    private final AtomicInteger readsToFail = new AtomicInteger();
    private volatile Throwable failure;
    private final List<GlobalOffset> failedReadsAfter = new CopyOnWriteArrayList<>();
    private volatile GlobalOffset firstSuccessfulReadAfter;
    private final List<GlobalOffset> payloadReadsAfter = new CopyOnWriteArrayList<>();
    private final AtomicInteger headReadsToFail = new AtomicInteger();
    private final AtomicInteger completedHeadReads = new AtomicInteger();
    private final AtomicReference<Runnable> beforeNextHeadRead = new AtomicReference<>();
    private final AtomicReference<Runnable> afterNextHeadRead = new AtomicReference<>();

    void failNextReads(int count, Throwable failure) {
      this.failure = failure;
      this.firstSuccessfulReadAfter = null;
      readsToFail.set(count);
    }

    /** Makes the next {@code count} reads of the head fail. */
    void failNextHeadReads(int count) {
      headReadsToFail.set(count);
    }

    List<GlobalOffset> failedReadsAfter() {
      return failedReadsAfter;
    }

    GlobalOffset firstSuccessfulReadAfter() {
      return firstSuccessfulReadAfter;
    }

    /** The position of every read of event payloads: each call of {@code readGlobalStream}. */
    List<GlobalOffset> payloadReadsAfter() {
      return payloadReadsAfter;
    }

    /** The reads of the head that have returned. The feed reads the head only while it is idle. */
    int completedHeadReads() {
      return completedHeadReads.get();
    }

    /**
     * Runs {@code action} once, on the thread of the next read of the head, before the head is
     * taken: what happens between the feed's first look for subscribers and its capture of the
     * head.
     */
    void beforeNextHeadRead(Runnable action) {
      beforeNextHeadRead.set(action);
    }

    /**
     * Runs {@code action} once, on the thread of the next read of the head, after the head is taken
     * and before it is returned: what happens between the feed's capture of the head and its check
     * for subscribers.
     */
    void afterNextHeadRead(Runnable action) {
      afterNextHeadRead.set(action);
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      payloadReadsAfter.add(afterOffset);
      if (readsToFail.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
        failedReadsAfter.add(afterOffset);
        switch (failure) {
          case Error error -> throw error;
          case RuntimeException exception -> throw exception;
          default -> throw new IllegalStateException(failure);
        }
      }
      if (firstSuccessfulReadAfter == null && !failedReadsAfter.isEmpty()) {
        firstSuccessfulReadAfter = afterOffset;
      }
      return delegate.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return delegate.load(streamId);
    }

    @Override
    public EventStore.AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      return delegate.append(streamId, events, expectedVersion);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      delegate.saveSnapshot(streamId, version, state);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return delegate.readStream(streamId, afterVersion, maxCount);
    }

    @Override
    public GlobalOffset lastGlobalOffset() {
      if (headReadsToFail.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
        throw new IllegalStateException("the head cannot be read");
      }
      runOnce(beforeNextHeadRead);
      GlobalOffset head = delegate.lastGlobalOffset();
      runOnce(afterNextHeadRead);
      completedHeadReads.incrementAndGet();
      return head;
    }

    private static void runOnce(AtomicReference<Runnable> action) {
      Runnable armed = action.getAndSet(null);
      if (armed != null) {
        armed.run();
      }
    }
  }

  /**
   * A publisher that lets a test run an action on the polling thread right after the feed was told
   * that nobody is subscribed: what happens between the feed's check for subscribers and its move
   * of the position.
   */
  private static final class ObservedPublisher extends SseEventPublisher {

    private final AtomicReference<Runnable> afterNextEmptyAnswer = new AtomicReference<>();

    void afterNextEmptyAnswer(Runnable action) {
      afterNextEmptyAnswer.set(action);
    }

    @Override
    boolean hasSubscribers() {
      boolean any = super.hasSubscribers();
      if (!any) {
        Runnable armed = afterNextEmptyAnswer.getAndSet(null);
        if (armed != null) {
          armed.run();
        }
      }
      return any;
    }
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

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The live delivery path must DRAIN to the head on each wake-up, not read a single {@code
 * fetchSize} page and then sleep the polling interval.
 *
 * <p>{@code ResilientPollLoop} sleeps unconditionally after every successful poll, including one
 * that returned a completely full page, so a one-page-per-cycle reader caps a projection at {@code
 * fetchSize / pollingInterval} events per second forever — 50 / 5.5 s at the auto-configured
 * defaults. This is the identical hazard the sibling {@code PostgresNotificationSubscription}
 * already fixed ("reading only one page per wake-up strands every event beyond the first page").
 */
class PollingEventSubscriptionDrainTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  private static final SubscriptionConfig CONFIG =
      new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(5));

  static final class InMemoryOffsetStore implements OffsetStore {
    final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      offsets.put(projectionName.value(), offset);
    }
  }

  /**
   * Minimal read-only global stream that counts the reads the subscription issues, so "one page per
   * wake-up" is directly observable. {@code InMemoryEventStore} is final, hence a stub rather than
   * a subclass; the subscription only ever calls {@link #readGlobalStream}.
   */
  static final class CountingEventStore implements EventStore {
    final AtomicInteger reads = new AtomicInteger();
    private final List<EventEnvelope> events = new CopyOnWriteArrayList<>();

    /** Runs once, right after the first read that returns an empty page; then cleared. */
    volatile Runnable afterFirstEmptyRead;

    /**
     * Mirrors {@code PostgresEventStore.readGlobalStream}, contiguity guard included: the
     * expectation is seeded from the FIRST returned row (so a gap between the checkpoint and where
     * data resumes is paged over) and the page STOPS at an internal discontinuity. A page shorter
     * than {@code maxCount} therefore does not imply the head was reached.
     */
    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      reads.incrementAndGet();
      List<EventEnvelope> page = new ArrayList<>(maxCount);
      long expectedNext = -1;
      for (EventEnvelope e : events) {
        long offset = e.globalOffset().value();
        if (offset <= afterOffset.value()) {
          continue;
        }
        if (expectedNext == -1) {
          expectedNext = offset;
        } else if (offset != expectedNext) {
          break;
        }
        page.add(e);
        expectedNext++;
        if (page.size() == maxCount) {
          break;
        }
      }
      Runnable hook = afterFirstEmptyRead;
      if (page.isEmpty() && hook != null) {
        afterFirstEmptyRead = null;
        hook.run();
      }
      return page;
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public EventStore.AppendResult append(
        StreamId streamId, List<EventEnvelope> newEvents, Version expectedVersion) {
      events.addAll(newEvents);
      return null;
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  private static EventEnvelope envelope(StreamId s, long v) {
    return new EventEnvelope(
        GlobalOffset.of(v),
        s,
        new Version(v),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt_" + v),
            CommandId.of("cmd_" + v),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  /** Seeds offsets {@code from..to} inclusive. */
  private static void seed(CountingEventStore store, long from, long to) {
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-drain"));
    List<EventEnvelope> all = new ArrayList<>();
    for (long i = from; i <= to; i++) {
      all.add(envelope(s, i));
    }
    store.append(s, all, Version.initial());
  }

  @Test
  void pollOnce_drainsABacklogLargerThanFetchSizeInASingleCycle() {
    var store = new CountingEventStore();
    seed(store, 1, 250);

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("drain-sub")
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(CONFIG)
            .fetchSize(50)
            .build();

    int delivered = sub.pollOnce();

    assertThat(delivered)
        .as("one poll cycle must drain the whole backlog, not one fetchSize page")
        .isEqualTo(250);
    assertThat(received).hasSize(250);
    assertThat(offsetStore.getLastOffset(ProjectionName.of("drain-sub")).value()).isEqualTo(250L);
    // 5 full pages + one short/empty read that proves the head was reached.
    assertThat(store.reads.get()).isEqualTo(6);
  }

  /**
   * The drain must run until a read genuinely returns NOTHING, not until a page comes back shorter
   * than {@code fetchSize} — the finding's own proposed stop condition, which is wrong here. {@code
   * PostgresEventStore}'s contiguity guard splits a page at a PERMANENT hole (an offset no
   * committed event holds — the store does not promise contiguity), and the next read re-seeds from
   * its own first row and returns the events past the hole. Stopping on a short page would strand
   * them for a whole polling interval — the very defect being fixed, just relocated. Reading past
   * the hole cannot skip an in-flight lower offset: the store's serialized (gapless) offset counter
   * guarantees none exists beneath a committed one.
   */
  @Test
  void pollOnce_continuesPastAShortPageCausedByAPermanentHole() {
    var store = new CountingEventStore();
    seed(store, 1, 30); // page 1 truncates here (30 < fetchSize)
    seed(store, 41, 100); // offsets 31..40 are a permanent hole

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("drain-hole")
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(received::addAll)
            .config(CONFIG)
            .fetchSize(50)
            .build();

    assertThat(sub.pollOnce()).isEqualTo(90);
    assertThat(received).hasSize(90);
    assertThat(offsetStore.getLastOffset(ProjectionName.of("drain-hole")).value()).isEqualTo(100L);
    // 30 (hole-truncated), 50, 10, then the empty read that ends the drain.
    assertThat(store.reads.get()).isEqualTo(4);
  }

  /**
   * A drain over a huge backlog must not outlive {@code close()}: the lifecycle flag is re-checked
   * between pages, so teardown is not blocked behind an unbounded catch-up.
   */
  @Test
  void pollOnce_abandonsTheDrainWhenTheSubscriptionIsClosedMidBacklog() {
    var store = new CountingEventStore();
    seed(store, 1, 250);

    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var offsetStore = new InMemoryOffsetStore();
    var subRef = new PollingEventSubscription[1];
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("drain-closed")
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(
                events -> {
                  received.addAll(events);
                  subRef[0].close(); // stop after the first page
                })
            .config(CONFIG)
            .fetchSize(50)
            .build();
    subRef[0] = sub;

    int delivered = sub.pollOnce();

    assertThat(received).hasSize(50);
    assertThat(delivered).isEqualTo(50);
    assertThat(offsetStore.getLastOffset(ProjectionName.of("drain-closed")).value()).isEqualTo(50L);
    assertThat(store.reads.get()).isEqualTo(1);
  }

  @Test
  void aWakeUpThatArrivesWhileADrainIsEnding_isPolledRightAfterTheDrain() throws Exception {
    // The drain's final read has come back empty but the cycle still holds the poll lock when a
    // new event commits and its NOTIFY calls triggerImmediatePoll(). The wake-up cannot poll (the
    // lock is held) and used to be dropped: the event then waited for the next polling interval.
    var store = new CountingEventStore();
    seed(store, 1, 3);
    var received = new CopyOnWriteArrayList<Long>();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("notify-during-drain")
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> events.forEach(e -> received.add(e.globalOffset().value())))
            .config(CONFIG)
            .fetchSize(50)
            .build();
    var wakeUpResult = new AtomicInteger(-1);
    store.afterFirstEmptyRead =
        () -> {
          StreamId s = StreamId.of(TYPE, AggregateId.of("cart-drain"));
          store.append(s, List.of(envelope(s, 4)), Version.initial());
          Thread wakeUp =
              Thread.ofVirtual().start(() -> wakeUpResult.set(sub.triggerImmediatePoll()));
          try {
            wakeUp.join();
          } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        };

    int delivered = sub.pollOnce();

    assertThat(wakeUpResult.get())
        .as("the wake-up found the drain still holding the poll lock")
        .isZero();
    assertThat(received)
        .as("the cycle that held the lock polls again for the wake-up it turned away")
        .containsExactly(1L, 2L, 3L, 4L);
    assertThat(delivered).isEqualTo(4);
  }

  @Test
  void aWakeUpThatFindsNoPollInProgress_drainsItself() {
    var store = new CountingEventStore();
    seed(store, 1, 2);
    var received = new CopyOnWriteArrayList<EventEnvelope>();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("notify-idle")
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(received::addAll)
            .config(CONFIG)
            .fetchSize(50)
            .build();

    assertThat(sub.triggerImmediatePoll()).isEqualTo(2);
    assertThat(received).hasSize(2);
    assertThat(store.reads.get())
        .as("one page, then the empty read that ends the drain")
        .isEqualTo(2);
  }
}

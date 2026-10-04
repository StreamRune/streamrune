package org.streamrune.runtime.fray;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
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
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.test.InMemoryEventStore;

/**
 * Fray (CMU-PASTA) concurrency PROPERTY test for {@link PollingEventSubscription}.
 *
 * <p>{@code pollOnce()} and {@code triggerImmediatePoll()} are serialized by the subscription's
 * private {@code pollLock}. The comment on that lock in production states the invariant directly:
 * "Without it, two concurrent polls read the same offset, deliver the same batch twice, and can
 * save offsets backwards." This test guards exactly that serialization with two PROPERTIES:
 *
 * <ul>
 *   <li><b>No double delivery</b>: across all interleavings each event is delivered to the listener
 *       at most once.
 *   <li><b>Offset monotonicity</b>: the offset saved to the {@link OffsetStore} never regresses
 *       (each {@code saveOffset} is &gt;= the previously saved value).
 * </ul>
 *
 * <p>Two virtual threads each call {@code pollOnce()} against the same subscription, backed by an
 * {@link InMemoryEventStore} preloaded with a known set of events, an in-memory {@link
 * OffsetStore}, and a recording {@link EventListener}. The store is appended through {@code
 * append()} so it assigns real sequential global offsets and {@code readGlobalStream(afterOffset,
 * n)} returns the not-yet-consumed tail — the same read path production uses.
 *
 * <p>Why it is NON-VACUOUS: under the real {@code pollLock} the two polls are mutually exclusive,
 * so the first poll reads-delivers-saves the whole batch atomically and the second sees an advanced
 * offset and delivers nothing. The properties hold on every interleaving. If {@code pollLock} were
 * removed (or the offset were saved BEFORE delivery), Fray would find an interleaving where both
 * threads read the SAME {@code lastOffset} before either saves, both deliver the same events &rarr;
 * the listener records a duplicate &rarr; the double-delivery assertion trips; or where a stale
 * read causes a lower offset to be saved after a higher one &rarr; the monotonicity assertion
 * trips. The controlled scheduler is built to reach exactly those read/deliver/save interleavings.
 *
 * <p>Note on faithfulness: this test lives in {@code org.streamrune.runtime.fray}, a different
 * package from {@code PollingEventSubscription}, so the package-private {@code pollOnce()} is not
 * reachable here without widening production visibility (which these probes must not do). We
 * therefore drive the public {@code triggerImmediatePoll()} from BOTH threads — the closest
 * faithful expression of the concurrent-poll race using the public API, and exactly the caller
 * {@code pollLock}'s own production comment names (a LISTEN/NOTIFY dispatcher firing immediate
 * polls). Both threads attempt to enter the same critical region; whichever acquires {@code
 * pollLock} reads, delivers, and saves the batch, while the other must NOT also deliver it. If
 * {@code pollLock} were removed, both calls would fall through to {@code pollOnce}'s body, read the
 * same {@code lastOffset}, and double-deliver — which is the interleaving Fray is built to find.
 *
 * <p>Lives in the dedicated {@code fray} source set; runs only under the {@code frayTest} task
 * (Linux x86_64 CI, where the JVMTI agent exists), excluded from the normal {@code test} run and
 * the JaCoCo gate.
 */
public class FrayPollingEventSubscriptionTest {

  private static final AggregateType TYPE = AggregateType.of("fray");

  private static final int EVENT_COUNT = 4;
  private static final String SUBSCRIPTION = "fray-poll-sub";

  /** A trivial domain event for the preloaded stream. */
  record Item(String sku) implements DomainEvent {}

  /**
   * In-memory {@link OffsetStore} that also records every saved value in order, so the test can
   * assert the saved offset never regressed across the concurrent polls. Reads/writes are
   * synchronized so the recording itself does not introduce a separate race — the property under
   * test is the subscription's serialization, not this double's.
   */
  private static final class RecordingOffsetStore implements OffsetStore {
    private GlobalOffset current = GlobalOffset.initial();
    private final List<Long> savedHistory = new ArrayList<>();

    @Override
    public synchronized GlobalOffset getLastOffset(ProjectionName subscriptionName) {
      return current;
    }

    @Override
    public synchronized void saveOffset(ProjectionName subscriptionName, GlobalOffset offset) {
      savedHistory.add(offset.value());
      current = offset;
    }

    synchronized List<Long> savedHistory() {
      return new ArrayList<>(savedHistory);
    }
  }

  /**
   * Recording listener that captures every delivered event's global offset under a lock, so a
   * double delivery (the same offset delivered twice across the two poll threads) is detectable
   * after the threads join.
   */
  private static final class RecordingListener implements EventListener {
    private final List<Long> deliveredOffsets = new ArrayList<>();

    @Override
    public void onEvents(List<EventEnvelope> events) {
      synchronized (this) {
        for (EventEnvelope e : events) {
          deliveredOffsets.add(e.globalOffset().value());
        }
      }
    }

    synchronized List<Long> deliveredOffsets() {
      return new ArrayList<>(deliveredOffsets);
    }
  }

  private static EventEnvelope envelope(StreamId stream, long version) {
    // globalOffset here is a placeholder — InMemoryEventStore.append() reassigns a real sequential
    // global offset, which is what readGlobalStream / the subscription actually observe.
    return new EventEnvelope(
        GlobalOffset.of(1),
        stream,
        new Version(version),
        new EventType("Item"),
        new Item("sku-" + version),
        new EventMetadata(
            EventId.of("evt_" + version),
            CommandId.of("cmd_" + version),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  @FrayTest(iterations = 200)
  public void concurrentPollsDoNotDoubleDeliverOrRegressOffset() throws InterruptedException {
    var store = new InMemoryEventStore();
    StreamId stream = StreamId.of(TYPE, AggregateId.of("fray-cart"));
    List<EventEnvelope> preload = new ArrayList<>();
    for (long v = 1; v <= EVENT_COUNT; v++) {
      preload.add(envelope(stream, v));
    }
    // Append through the store so it assigns real sequential global offsets 1..EVENT_COUNT.
    store.append(stream, preload, Version.initial());

    var offsetStore = new RecordingOffsetStore();
    var listener = new RecordingListener();

    var sub =
        PollingEventSubscription.builder()
            .subscriptionName(SUBSCRIPTION)
            .eventStore(store)
            .offsetStore(offsetStore)
            .listener(listener)
            // fetchSize >= EVENT_COUNT so a single poll can grab the whole batch; that is the case
            // most likely to expose a double delivery if pollLock did not serialize the two polls.
            .config(new SubscriptionConfig(false, Duration.ofMillis(10), Duration.ofMillis(1)))
            .fetchSize(EVENT_COUNT + 1)
            .build();

    // Two virtual threads racing the real, lock-protected triggerImmediatePoll() (public entry
    // point that delegates to the pollLock-guarded pollOnce()).
    Thread p1 = Thread.ofVirtual().name("fray-poll-1").start(sub::triggerImmediatePoll);
    Thread p2 = Thread.ofVirtual().name("fray-poll-2").start(sub::triggerImmediatePoll);
    p1.join();
    p2.join();

    // PROPERTY 1 — no double delivery: each global offset appears at most once across both polls.
    List<Long> delivered = listener.deliveredOffsets();
    boolean[] seen = new boolean[EVENT_COUNT + 1];
    for (Long off : delivered) {
      int idx = off.intValue();
      if (idx >= 1 && idx <= EVENT_COUNT) {
        if (seen[idx]) {
          throw new AssertionError(
              "double delivery: event at global offset "
                  + idx
                  + " was delivered more than once "
                  + "(deliveries="
                  + delivered
                  + ")");
        }
        seen[idx] = true;
      }
    }

    // PROPERTY 2 — offset monotonicity: each saved offset is >= the previous saved offset.
    List<Long> saved = offsetStore.savedHistory();
    long prev = GlobalOffset.initial().value();
    for (Long s : saved) {
      if (s < prev) {
        throw new AssertionError(
            "offset regression: saved offset "
                + s
                + " is lower than a previously saved "
                + prev
                + " (saveHistory="
                + saved
                + ")");
      }
      prev = s;
    }
  }
}

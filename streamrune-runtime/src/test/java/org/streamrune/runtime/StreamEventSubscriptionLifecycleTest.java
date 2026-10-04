package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
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
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * {@link StreamEventSubscription} must apply the close-then-join lifecycle discipline every sibling
 * poll-loop runner already applies.
 *
 * <p>{@code close()} used to set {@code running=false}, interrupt, and return IMMEDIATELY. {@code
 * pollOnce()} has no interruptible point, so the in-flight batch kept reading, delivering and
 * saving its checkpoint after {@code close()} returned — racing the caller's next teardown step
 * (typically closing the shared DataSource). And because {@code start()} only CAS-ed {@code
 * running} back to true, a {@code close(); start();} was accepted while the previous poll thread
 * was still inside {@code pollOnce()}, putting two poll threads on one checkpoint: both read {@code
 * lastOffset=N}, both deliver the same batch, both save.
 */
class StreamEventSubscriptionLifecycleTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  private static final StreamId TARGET = StreamId.of(TYPE, AggregateId.of("cart-1"));
  private static final SubscriptionConfig FAST =
      new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ZERO);

  record Added(String sku) implements DomainEvent {}

  private static EventEnvelope envelope() {
    return new EventEnvelope(
        GlobalOffset.of(1),
        TARGET,
        new Version(1),
        new EventType("Added"),
        new Added("X"),
        new EventMetadata(
            EventId.of("evt-1"),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  /** Builds a subscription whose global read parks until {@code close()} interrupts it. */
  private StreamEventSubscription blockedSubscription(
      AtomicReference<Thread> pollThread, CountDownLatch entered, Long joinTimeoutMs) {
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.readGlobalStream(any(), anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());

    var builder =
        StreamEventSubscription.builder()
            .subscriptionName("conc1-sub")
            .streamId(TARGET)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(FAST)
            .fetchSize(10);
    if (joinTimeoutMs != null) {
      builder.closeJoinTimeoutMs(joinTimeoutMs);
    }
    return builder.build();
  }

  @Test
  void close_joinsThePollThreadBeforeReturning() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var sub = blockedSubscription(pollThread, entered, null);

    sub.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "the poll thread must reach readGlobalStream");

    sub.close();

    assertFalse(
        pollThread.get().isAlive(),
        "close() must join the poll thread: pollOnce() has no interruptible point, so without the"
            + " join the in-flight batch keeps delivering and saves its checkpoint after close()"
            + " returned, racing the caller's resource teardown");
  }

  @Test
  void start_isRefusedWhileTheClosedPollThreadIsStillWindingDown() throws Exception {
    // When the join TIMES OUT and the poll thread is still alive, close() must not
    // hand back a restartable subscription — a restart would put a SECOND poll thread on the same
    // checkpoint alongside the stuck one, and both would deliver the same batch and save.
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var sub = blockedSubscription(pollThread, entered, 100L); // times out before the 500ms unwind

    sub.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "the poll thread must reach readGlobalStream");

    sub.close(); // join(100ms) times out; the poll thread is still winding down

    assertTrue(pollThread.get().isAlive(), "pre-condition: the join timed out, thread still alive");
    assertThrows(
        IllegalStateException.class,
        sub::start,
        "a restart must be refused while the previous poll thread is still on the checkpoint");
  }

  @Test
  void start_isRefusedAfterAFullyJoinedClose() throws Exception {
    // close() is terminal, exactly as on the sibling PollingEventSubscription: the `closed` latch
    // is one-way (pollOnce consults it to cut a drain short), so a restarted subscription would
    // silently run in the degraded read-one-page-per-interval mode that was removed.
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var sub = blockedSubscription(pollThread, entered, null);

    sub.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    sub.close();
    assertFalse(pollThread.get().isAlive());

    assertThrows(IllegalStateException.class, sub::start);
  }

  @Test
  void close_fromTheListenerThreadDoesNotDeadlock() throws Exception {
    // A listener that closes its own subscription runs ON the poll thread; joining the current
    // thread would deadlock, so close() must only signal in that case. The test thread's own
    // close() then still joins, and must find the poll thread finished.
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.readGlobalStream(any(), anyInt())).thenReturn(List.of(envelope()));
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());

    var pollThread = new AtomicReference<Thread>();
    var listenerRan = new CountDownLatch(1);
    var subRef = new StreamEventSubscription[1];
    subRef[0] =
        StreamEventSubscription.builder()
            .subscriptionName("self-close-sub")
            .streamId(TARGET)
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .listener(
                events -> {
                  pollThread.set(Thread.currentThread());
                  subRef[0].close(); // self-close on the poll thread — must not deadlock
                  listenerRan.countDown();
                })
            .config(FAST)
            .fetchSize(10)
            .build();

    subRef[0].start();
    assertTrue(listenerRan.await(5, TimeUnit.SECONDS), "the self-closing listener must return");

    subRef[0].close(); // from the test thread: joins for real
    assertFalse(subRef[0].isRunning());
    assertFalse(pollThread.get().isAlive(), "the poll thread must be joined by the outer close()");
  }

  @Test
  void close_isIdempotent() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var sub = blockedSubscription(pollThread, entered, null);
    sub.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    sub.close();
    sub.close();
    assertFalse(sub.isRunning());
  }
}

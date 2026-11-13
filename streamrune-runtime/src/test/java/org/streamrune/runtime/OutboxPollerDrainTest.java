package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEntrySpans;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.MutableClock;

/**
 * One poll cycle of {@link OutboxPoller} keeps claiming while each batch comes back full and every
 * entry of it resolved, and waits for the next interval after a partial batch, a stop, a deadline
 * cut, a transport failure or an in-flight hand-off. The drain is bounded by {@link
 * OutboxPoller#DRAIN_BUDGET_POLL_INTERVALS} poll intervals.
 */
class OutboxPollerDrainTest {

  private static final AggregateType TYPE = AggregateType.of("lamp");
  private static final int BATCH = 10;

  /** A strict store holding one entry in each of {@code count} streams, ids e0, e1, … */
  private static InMemoryOutboxStore backlog(int count) {
    var store = new InMemoryOutboxStore();
    for (int i = 0; i < count; i++) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of("e" + i),
              "{}",
              "Event",
              StreamId.of(TYPE, AggregateId.of("a" + i))));
    }
    return store;
  }

  private static OutboxPoller.Builder poller(OutboxStore store, OutboxPublisher publisher) {
    return OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .batchSize(BATCH)
        .pollInterval(Duration.ofSeconds(60));
  }

  private static long count(InMemoryOutboxStore store, OutboxStatus status) {
    return store.countByStatus(status);
  }

  /** A publisher that records the size of every batch and hands each batch to {@code outcome}. */
  private static class RecordingPublisher implements OutboxPublisher {
    final List<Integer> batches = new CopyOnWriteArrayList<>();
    private final Function<List<OutboxEntry>, BatchResult> outcome;

    RecordingPublisher(Function<List<OutboxEntry>, BatchResult> outcome) {
      this.outcome = outcome;
    }

    static RecordingPublisher confirmingAll() {
      return new RecordingPublisher(RecordingPublisher::allConfirmed);
    }

    static BatchResult allConfirmed(List<OutboxEntry> entries) {
      var confirmed = new LinkedHashSet<OutboxEntryId>();
      entries.forEach(e -> confirmed.add(e.id()));
      return new BatchResult(confirmed, Map.of());
    }

    /** All entries but the first confirmed; the first failed with {@code failure}. */
    static BatchResult firstFailed(List<OutboxEntry> entries, Exception failure) {
      return new BatchResult(
          allConfirmed(entries.subList(1, entries.size())).confirmed(),
          Map.of(entries.getFirst().id(), failure));
    }

    @Override
    public void publish(OutboxEntry entry) {
      throw new UnsupportedOperationException("publishBatch is overridden");
    }

    @Override
    public BatchResult publishBatch(
        List<OutboxEntry> entries, Instant deadline, OutboxEntrySpans spans) {
      batches.add(entries.size());
      return outcome.apply(entries);
    }
  }

  @Test
  void aBacklogOfSeveralBatches_isDeliveredInOneCycle() {
    var store = backlog(35);
    var publisher = RecordingPublisher.confirmingAll();

    poller(store, publisher).build().pollCycle();

    assertThat(publisher.batches).containsExactly(10, 10, 10, 5);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(35);
  }

  @Test
  void aBacklogOfExactlyOneBatch_endsTheCycleOnTheEmptyClaimAfterIt() {
    var store = backlog(BATCH);
    var publisher = RecordingPublisher.confirmingAll();

    poller(store, publisher).build().pollCycle();

    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(BATCH);
  }

  @Test
  void aPartialBatch_waitsForTheNextInterval_althoughTheBacklogIsDeep() {
    // One stream with 25 entries: a strict claim takes one head per stream, so every batch is
    // partial although the backlog is deep.
    var store = new InMemoryOutboxStore();
    for (int i = 0; i < 25; i++) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of("s" + i), "{}", "Event", StreamId.of(TYPE, AggregateId.of("one"))));
    }
    var publisher = RecordingPublisher.confirmingAll();

    var poller = poller(store, publisher).build();
    poller.pollCycle();

    assertThat(publisher.batches).containsExactly(1);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(1);
    poller.pollCycle();
    assertThat(publisher.batches).containsExactly(1, 1);
  }

  @Test
  void aTransportFailure_inAFullBatch_doesNotClaimTheNextBatchAtOnce() {
    var store = backlog(30);
    var publisher =
        new RecordingPublisher(
            entries ->
                RecordingPublisher.firstFailed(entries, new IllegalStateException("broker down")));

    poller(store, publisher).build().pollCycle();

    // classifyFailure's default is TRANSPORT: the transport is suspect, so the relay backs off.
    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(9);
  }

  @Test
  void anInFlightHandOff_inAFullBatch_doesNotClaimTheNextBatchAtOnce() {
    var store = backlog(30);
    var publisher =
        new RecordingPublisher(
            entries ->
                RecordingPublisher.firstFailed(
                    entries, new IllegalStateException("confirm timeout"))) {
          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.IN_FLIGHT;
          }

          @Override
          public Duration inFlightHorizon() {
            return Duration.ofSeconds(30);
          }
        };

    poller(store, publisher).build().pollCycle();

    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(store, OutboxStatus.IN_PROGRESS)).isEqualTo(1);
  }

  @Test
  void anInterruptedPublish_inAFullBatch_doesNotClaimTheNextBatchAtOnce() {
    var store = backlog(30);
    var publisher =
        new RecordingPublisher(
            entries ->
                RecordingPublisher.firstFailed(
                    entries, new IllegalStateException(new InterruptedException("stopped")))) {
          @Override
          public Duration inFlightHorizon() {
            return Duration.ofSeconds(30);
          }
        };

    poller(store, publisher).build().pollCycle();

    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(store, OutboxStatus.IN_PROGRESS)).isEqualTo(1);
  }

  @Test
  void anEntryRejection_inAFullBatch_keepsTheDrainGoing() {
    var store = backlog(25);
    var publisher =
        new RecordingPublisher(
            entries ->
                RecordingPublisher.firstFailed(
                    entries, new IllegalArgumentException("bad payload"))) {
          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.ENTRY;
          }
        };

    // A long backoff keeps each rejected head out of the later claims of this cycle.
    poller(store, publisher)
        .retryPolicy(new RetryPolicy(3, Duration.ofMinutes(1), 2.0, false))
        .build()
        .pollCycle();

    // A per-entry rejection says nothing against the transport: the full batches keep coming.
    assertThat(publisher.batches).containsExactly(10, 10, 5);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(22);
  }

  @Test
  void aBatchCutShortByTheDeadline_doesNotClaimTheNextBatchAtOnce() {
    var store = backlog(30);
    // The publisher stops after five entries, as at a passed deadline: the rest is unattempted.
    var publisher =
        new RecordingPublisher(entries -> RecordingPublisher.allConfirmed(entries.subList(0, 5)));

    poller(store, publisher).build().pollCycle();

    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(5);
    assertThat(count(store, OutboxStatus.PENDING)).isEqualTo(25);
  }

  @Test
  void theDrainBudget_endsTheCycle_afterTenPollIntervals() {
    var store = backlog(100);
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    // Each batch takes four poll intervals on the poller's clock: after the third the cycle has
    // run twelve intervals, past the budget of ten, and yields.
    var publisher =
        new RecordingPublisher(
            entries -> {
              clock.advance(Duration.ofSeconds(4));
              return RecordingPublisher.allConfirmed(entries);
            });

    var poller = poller(store, publisher).pollInterval(Duration.ofSeconds(1)).clock(clock).build();
    poller.pollCycle();

    assertThat(OutboxPoller.DRAIN_BUDGET_POLL_INTERVALS).isEqualTo(10);
    assertThat(publisher.batches).containsExactly(10, 10, 10);
    assertThat(count(store, OutboxStatus.DELIVERED)).isEqualTo(30);
    assertThat(count(store, OutboxStatus.PENDING)).isEqualTo(70);
  }

  @Test
  void aPollIntervalTooLongForTheBudget_drainsTheWholeBacklog() {
    var store = backlog(25);
    var publisher = RecordingPublisher.confirmingAll();

    poller(store, publisher)
        .pollInterval(Duration.ofDays(365L * 1000))
        .clock(Clock.systemUTC())
        .build()
        .pollCycle();

    assertThat(publisher.batches).containsExactly(10, 10, 5);
  }

  /**
   * Wraps {@code inner} so that any store call made while the calling thread is interrupted is
   * recorded and fails, as a JDBC call on an interrupted virtual thread does; {@code
   * afterMarkDelivered} runs after every {@code markDeliveredAll}.
   */
  private static OutboxStore interruptSensitive(
      OutboxStore inner, List<String> callsOnInterruptedThread, Runnable afterMarkDelivered) {
    return (OutboxStore)
        Proxy.newProxyInstance(
            OutboxStore.class.getClassLoader(),
            new Class<?>[] {OutboxStore.class},
            (proxy, method, args) -> {
              if (Thread.currentThread().isInterrupted()) {
                callsOnInterruptedThread.add(method.getName());
                throw new IllegalStateException(
                    "store call " + method.getName() + " on an interrupted thread");
              }
              Object result;
              try {
                result = method.invoke(inner, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              if (method.getName().equals("markDeliveredAll")) {
                afterMarkDelivered.run();
              }
              return result;
            });
  }

  @Test
  @Timeout(20)
  void aStopDuringADrain_holdsTheInterruptedEntry_releasesTheRest_andClaimsNoFurtherBatch()
      throws Exception {
    var inner = backlog(50);
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    var store = interruptSensitive(inner, callsOnInterruptedThread, () -> {});
    var parked = new CountDownLatch(1);
    var published = new AtomicInteger();
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) throws Exception {
            // The fifth entry of the third batch waits for its acknowledgement when close() comes.
            if (published.incrementAndGet() == 25) {
              parked.countDown();
              new CountDownLatch(1).await();
            }
          }

          @Override
          public Duration inFlightHorizon() {
            return Duration.ofSeconds(30);
          }
        };
    var poller = poller(store, publisher).build();

    poller.start();
    assertThat(parked.await(5, TimeUnit.SECONDS)).as("the relay reaches the third batch").isTrue();
    long closeStarted = System.nanoTime();
    poller.close();

    assertThat(Duration.ofNanos(System.nanoTime() - closeStarted))
        .isLessThan(Duration.ofSeconds(5));
    assertThat(poller.isAlive()).isFalse();
    assertThat(callsOnInterruptedThread).isEmpty();
    // Two full batches and four entries of the third were confirmed and recorded.
    assertThat(count(inner, OutboxStatus.DELIVERED)).isEqualTo(24);
    // The interrupted hand-off is held until its lease expires.
    assertThat(count(inner, OutboxStatus.IN_PROGRESS)).isEqualTo(1);
    // The rest of the third batch was released, and batches four and five were never claimed.
    assertThat(count(inner, OutboxStatus.PENDING)).isEqualTo(25);
  }

  @Test
  @Timeout(20)
  void aStopWhileAFullBatchIsRecorded_endsTheCycleBeforeTheNextClaim() throws Exception {
    var inner = backlog(30);
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    var closer = new AtomicReference<Thread>();
    var recording = new CountDownLatch(1);
    // The first batch's markDeliveredAll returns, still inside the relay's deferred section, only
    // once close() has requested the stop and is joining the poll thread.
    Runnable waitForTheStop =
        () -> {
          if (recording.getCount() == 0) {
            return;
          }
          recording.countDown();
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
          while (closer.get() == null || closer.get().getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() > deadline) {
              throw new AssertionError("close() did not start joining the poll thread");
            }
            Thread.onSpinWait();
          }
        };
    var store = interruptSensitive(inner, callsOnInterruptedThread, waitForTheStop);
    var publisher = RecordingPublisher.confirmingAll();
    var poller = poller(store, publisher).build();

    poller.start();
    assertThat(recording.await(5, TimeUnit.SECONDS)).isTrue();
    closer.set(Thread.ofPlatform().start(poller::close));
    closer.get().join(TimeUnit.SECONDS.toMillis(15));

    assertThat(poller.isAlive()).isFalse();
    assertThat(callsOnInterruptedThread).isEmpty();
    assertThat(publisher.batches).containsExactly(10);
    assertThat(count(inner, OutboxStatus.DELIVERED)).isEqualTo(10);
    assertThat(count(inner, OutboxStatus.PENDING)).isEqualTo(20);
  }
}

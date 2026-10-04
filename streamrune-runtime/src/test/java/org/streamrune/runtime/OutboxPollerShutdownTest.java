package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * Stopping the relay in the middle of a batch. {@link OutboxPoller#close()} interrupts the poll
 * thread to cut a long publish short; the batch's outcome must still be recorded afterwards. On a
 * virtual thread a JDBC call made while the interrupt flag is set fails at the socket, so the store
 * used here refuses every call made on an interrupted thread, the way the PostgreSQL store does.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class OutboxPollerShutdownTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private static final Duration TRANSPORT_HORIZON = Duration.ofSeconds(10);

  /**
   * Wraps {@code inner} so every call made while the calling thread's interrupt flag is set is
   * recorded and fails, and lets a test intercept one method by name.
   */
  private static OutboxStore interruptSensitive(
      OutboxStore inner,
      List<String> callsOnInterruptedThread,
      Map<String, BiFunction<OutboxStore, Object[], Object>> overrides) {
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
              var override = overrides.get(method.getName());
              if (override != null) {
                return override.apply(inner, args);
              }
              try {
                return method.invoke(inner, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  private static InMemoryOutboxStore storeWith(String... ids) {
    var store = new InMemoryOutboxStore();
    for (String id : ids) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of(id), "{}", "Event", StreamId.of(TYPE, AggregateId.of("agg-" + id))));
    }
    return store;
  }

  private static OutboxEntry entry(InMemoryOutboxStore store, String id) {
    return store.findById(OutboxEntryId.of(id)).orElseThrow();
  }

  private static OutboxPoller poller(OutboxStore store, OutboxPublisher publisher) {
    return OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build();
  }

  @Test
  void closeMidBatch_recordsConfirmedDeliveries_holdsTheInterruptedEntry_releasesTheRest()
      throws Exception {
    var inner = storeWith("e1", "e2", "e3", "e4", "e5");
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    var store = interruptSensitive(inner, callsOnInterruptedThread, Map.of());
    var parked = new CountDownLatch(1);
    var attempted = new CopyOnWriteArrayList<String>();
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) throws Exception {
            attempted.add(entry.id().value());
            if (entry.id().value().equals("e3")) {
              // Handed to the transport, now waiting for its acknowledgement when close() comes.
              parked.countDown();
              new CountDownLatch(1).await();
            }
          }

          @Override
          public Duration inFlightHorizon() {
            return TRANSPORT_HORIZON;
          }
        };
    var poller = poller(store, publisher);

    poller.start();
    assertTrue(parked.await(5, TimeUnit.SECONDS), "the relay must reach e3");
    poller.close();

    assertFalse(poller.isAlive(), "the poll thread has stopped");
    assertEquals(List.of(), callsOnInterruptedThread, "no store call on the interrupted thread");
    assertEquals(OutboxStatus.DELIVERED, entry(inner, "e1").status());
    assertEquals(OutboxStatus.DELIVERED, entry(inner, "e2").status());
    // e3 may still be delivered by the transport: its claim is held until the lease expires.
    assertEquals(OutboxStatus.IN_PROGRESS, entry(inner, "e3").status());
    assertEquals(0, entry(inner, "e3").attempts(), "an interrupted publish burns no attempt");
    // e4 and e5 were never attempted: released at once for the next relay.
    assertEquals(OutboxStatus.PENDING, entry(inner, "e4").status());
    assertEquals(OutboxStatus.PENDING, entry(inner, "e5").status());
    assertEquals(List.of("e1", "e2", "e3"), attempted);
    assertEquals(0, poller.consecutiveFailures(), "stopping is not a failed cycle");
  }

  @Test
  void closeWhileTheBatchIsBeingRecorded_waitsForTheStoreCallInsteadOfInterruptingIt()
      throws Exception {
    var inner = storeWith("e1", "e2");
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    var recording = new CountDownLatch(1);
    var finishRecording = new CountDownLatch(1);
    var interruptedWhileRecording = new CopyOnWriteArrayList<Boolean>();
    BiFunction<OutboxStore, Object[], Object> slowMarkDelivered =
        (target, args) -> {
          recording.countDown();
          try {
            finishRecording.await();
          } catch (InterruptedException e) {
            interruptedWhileRecording.add(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("markDeliveredAll interrupted", e);
          }
          @SuppressWarnings("unchecked")
          var ids = (java.util.Collection<OutboxEntryId>) args[0];
          return target.markDeliveredAll(ids, (String) args[1]);
        };
    var store =
        interruptSensitive(
            inner, callsOnInterruptedThread, Map.of("markDeliveredAll", slowMarkDelivered));
    OutboxPublisher publisher = entry -> {};
    var poller = poller(store, publisher);

    poller.start();
    assertTrue(recording.await(5, TimeUnit.SECONDS), "the relay must be recording the batch");
    var closer = Thread.ofPlatform().name("closer").start(poller::close);
    closer.join(300);
    assertTrue(closer.isAlive(), "close() waits for the batch to be recorded");
    finishRecording.countDown();
    closer.join(5_000);

    assertFalse(closer.isAlive(), "close() returns once the batch is recorded");
    assertFalse(poller.isAlive(), "the poll thread has stopped");
    assertEquals(List.of(), interruptedWhileRecording, "the store call was not interrupted");
    assertEquals(List.of(), callsOnInterruptedThread, "no store call on the interrupted thread");
    assertEquals(OutboxStatus.DELIVERED, entry(inner, "e1").status());
    assertEquals(OutboxStatus.DELIVERED, entry(inner, "e2").status());
    assertFalse(poller.isStarted(), "the relay can be started again");
  }

  @Test
  void interruptedPublishFailure_isHeldInFlight_whateverThePublisherClassifies() {
    var inner = storeWith("e1", "e2");
    var metrics = mock(StreamRuneMetrics.class);
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {}

          @Override
          public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
            // A custom batch publisher reporting its own shutdown interruption for e1.
            return new BatchResult(
                Set.of(OutboxEntryId.of("e2")),
                Map.of(
                    OutboxEntryId.of("e1"),
                    new RuntimeException("worker stopped", new InterruptedException())));
          }

          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.ENTRY; // would burn an attempt and re-arm the entry
          }
        };
    var poller =
        OutboxPoller.builder()
            .outboxStore(inner)
            .publisher(publisher)
            .metrics(metrics)
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    poller.processBatch();

    assertEquals(OutboxStatus.IN_PROGRESS, entry(inner, "e1").status(), "claim held");
    assertEquals(0, entry(inner, "e1").attempts(), "no attempt burned");
    assertNull(entry(inner, "e1").lastError(), "no failure recorded");
    assertEquals(OutboxStatus.DELIVERED, entry(inner, "e2").status());
    // Holding an interrupted publish is the relay's own decision, not a publisher that classified
    // a failure in flight while declaring a zero horizon.
    verify(metrics, never()).recordOutboxInFlightHorizonViolation();
  }

  @Test
  void stopPendingWhenTheBatchIsClaimed_publishesNothing_andReleasesTheClaims() {
    var inner = storeWith("e1", "e2");
    var callsOnInterruptedThread = new CopyOnWriteArrayList<String>();
    BiFunction<OutboxStore, Object[], Object> claimThenInterrupt =
        (target, args) -> {
          List<OutboxEntry> claimed = target.loadPending((Integer) args[0]);
          Thread.currentThread().interrupt(); // close() lands just after the claim returned
          return claimed;
        };
    var store =
        interruptSensitive(
            inner, callsOnInterruptedThread, Map.of("loadPending", claimThenInterrupt));
    var published = Collections.synchronizedList(new ArrayList<OutboxEntryId>());
    OutboxPublisher publisher =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            published.add(entry.id());
          }

          @Override
          public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
            // A batch publisher that does not look at the interrupt flag itself.
            entries.forEach(this::publish);
            return new BatchResult(
                Set.copyOf(entries.stream().map(OutboxEntry::id).toList()), Map.of());
          }
        };
    var poller = poller(store, publisher);

    boolean stillInterrupted;
    try {
      poller.processBatch();
    } finally {
      stillInterrupted = Thread.interrupted();
    }

    assertTrue(stillInterrupted, "the stop request is handed back to the poll loop");
    assertEquals(List.of(), published, "nothing is handed off once a stop is pending");
    assertEquals(List.of(), callsOnInterruptedThread, "no store call on the interrupted thread");
    assertEquals(OutboxStatus.PENDING, entry(inner, "e1").status());
    assertEquals(OutboxStatus.PENDING, entry(inner, "e2").status());
  }
}

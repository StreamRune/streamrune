package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;

/**
 * Regression: {@link OutboxPoller#close()} must JOIN the poll thread before resetting the {@code
 * started} guard. Without the join, a {@code close()}/{@code start()} restart can leave the old
 * poll thread running concurrently with the new one (a leaked duplicate relay).
 */
class OutboxPollerRestartRaceTest {

  private static final OutboxPublisher.BatchResult NOTHING_PUBLISHED =
      new OutboxPublisher.BatchResult(Set.of(), Map.of());

  /** A store whose every claim returns the same single entry. */
  private static OutboxStore storeWithOneEntry() {
    OutboxStore store = mock(OutboxStore.class);
    when(store.claimedBy()).thenReturn("test-relay");
    when(store.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(store.claimLease()).thenReturn(Duration.ofMinutes(4));
    when(store.loadPending(anyInt()))
        .thenReturn(List.of(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event")));
    return store;
  }

  @Test
  void closeJoinsPollThreadSoRestartDoesNotLeakADuplicateRelay() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    // The poll thread parks in a publish, where close()'s interrupt reaches it; a claim runs with
    // the interrupt held back.
    OutboxStore store = storeWithOneEntry();
    OutboxPublisher blockingPublisher = mock(OutboxPublisher.class);
    when(blockingPublisher.publishBatch(any(), any(), any()))
        .thenAnswer(
            RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, NOTHING_PUBLISHED));

    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(blockingPublisher)
            .pollInterval(Duration.ofSeconds(1))
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered the publish");

    poller.close();

    assertFalse(
        pollThread.get().isAlive(),
        "close() must join the poll thread before returning so a restart cannot leak a duplicate"
            + " relay");
  }

  @Test
  void closeWithStuckPollThread_leavesRunnerNotRestartable() throws Exception {
    // When the join TIMES OUT and the poll thread is still alive (a black-holed poll),
    // close() must NOT reset the started guard — otherwise a restart spawns a SECOND poll thread
    // alongside the stuck one (silent duplicate relay). The join timeout is shrunk so the join
    // provably times out while the (slow-shutdown) poll thread is still alive.
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    // The poll thread parks in a publish, where close()'s interrupt reaches it; a claim runs with
    // the interrupt held back.
    OutboxStore store = storeWithOneEntry();
    OutboxPublisher blockingPublisher = mock(OutboxPublisher.class);
    when(blockingPublisher.publishBatch(any(), any(), any()))
        .thenAnswer(
            RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, NOTHING_PUBLISHED));

    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(blockingPublisher)
            .pollInterval(Duration.ofSeconds(1))
            .closeJoinTimeoutMs(100) // join times out well before the 500ms slow shutdown
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered the publish");
    assertTrue(poller.isStarted());

    poller.close(); // join(100ms) times out; the poll thread is still winding down (alive)

    // Not restartable: started stays set, so a restart is refused rather than silently duplicating.
    assertTrue(poller.isStarted(), "a join-timeout close() must leave the runner not-restartable");
    assertThrows(IllegalStateException.class, poller::start);

    // Wait for the thread the timed-out join left behind: it releases its claim and logs while it
    // winds down, and those lines belong to this test, not to whichever test runs next.
    pollThread.get().join(2_000);
    assertFalse(pollThread.get().isAlive(), "the poll thread ends after its slow shutdown");
    poller.close(); // the thread has stopped: the guard is reset
    assertFalse(poller.isStarted());
  }

  @Test
  void aSecondCloseAfterATimedOutJoin_keepsTheRelayNotRestartableUntilTheThreadStops()
      throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    OutboxStore blockingStore = mock(OutboxStore.class);
    when(blockingStore.claimedBy()).thenReturn("test-relay");
    when(blockingStore.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(blockingStore.claimLease()).thenReturn(Duration.ofMinutes(4));
    when(blockingStore.loadPending(anyInt()))
        .thenAnswer(
            RestartRaceTestSupport.parkIgnoringInterruptsUntilReleased(
                pollThread, entered, release, List.of()));
    var poller =
        OutboxPoller.builder()
            .outboxStore(blockingStore)
            .publisher(mock(OutboxPublisher.class))
            .pollInterval(Duration.ofSeconds(1))
            .closeJoinTimeoutMs(20)
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered loadPending");

    poller.close(); // the join times out
    poller.close(); // a second close finds the same stuck thread, not none

    assertTrue(pollThread.get().isAlive(), "the poll thread is still running");
    assertTrue(poller.isStarted(), "still not restartable while the stale thread runs");
    assertTrue(poller.isAlive(), "the stuck thread is still reported");
    assertThrows(IllegalStateException.class, poller::start);

    release.countDown();
    pollThread.get().join(5_000);
    poller.close(); // the thread has stopped: the guard is reset

    assertFalse(pollThread.get().isAlive());
    assertFalse(poller.isStarted(), "restartable once the stale thread has stopped");
    assertFalse(poller.isAlive());
  }

  @Test
  void twoConcurrentClosesDuringATimedOutJoin_keepsTheRelayNotRestartableUntilTheThreadStops()
      throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    OutboxStore blockingStore = mock(OutboxStore.class);
    when(blockingStore.claimedBy()).thenReturn("test-relay");
    when(blockingStore.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(blockingStore.claimLease()).thenReturn(Duration.ofMinutes(4));
    when(blockingStore.loadPending(anyInt()))
        .thenAnswer(
            RestartRaceTestSupport.parkIgnoringInterruptsUntilReleased(
                pollThread, entered, release, List.of()));
    var poller =
        OutboxPoller.builder()
            .outboxStore(blockingStore)
            .publisher(mock(OutboxPublisher.class))
            .pollInterval(Duration.ofSeconds(1))
            .closeJoinTimeoutMs(300)
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered loadPending");

    RestartRaceTestSupport.closeConcurrently(poller); // both joins time out

    assertTrue(pollThread.get().isAlive(), "the poll thread is still running");
    assertTrue(poller.isStarted(), "still not restartable while the stale thread runs");
    assertTrue(poller.isAlive(), "the stuck thread is still reported");
    assertThrows(IllegalStateException.class, poller::start);

    release.countDown();
    pollThread.get().join(5_000);
    poller.close(); // the thread has stopped: the guard is reset

    assertFalse(pollThread.get().isAlive());
    assertFalse(poller.isStarted(), "restartable once the stale thread has stopped");
    assertFalse(poller.isAlive());
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;

/**
 * Regression: {@link OutboxPoller#close()} must JOIN the poll thread before resetting the {@code
 * started} guard. Without the join, a {@code close()}/{@code start()} restart can leave the old
 * poll thread running concurrently with the new one (a leaked duplicate relay).
 */
class OutboxPollerRestartRaceTest {

  @Test
  void closeJoinsPollThreadSoRestartDoesNotLeakADuplicateRelay() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    OutboxStore blockingStore = mock(OutboxStore.class);
    when(blockingStore.claimedBy()).thenReturn("test-relay");
    when(blockingStore.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(blockingStore.claimLease()).thenReturn(Duration.ofMinutes(4));
    when(blockingStore.loadPending(anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));

    var poller =
        OutboxPoller.builder()
            .outboxStore(blockingStore)
            .publisher(mock(OutboxPublisher.class))
            .pollInterval(Duration.ofSeconds(1))
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered loadPending");

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
    OutboxStore blockingStore = mock(OutboxStore.class);
    when(blockingStore.claimedBy()).thenReturn("test-relay");
    when(blockingStore.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(blockingStore.claimLease()).thenReturn(Duration.ofMinutes(4));
    when(blockingStore.loadPending(anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));

    var poller =
        OutboxPoller.builder()
            .outboxStore(blockingStore)
            .publisher(mock(OutboxPublisher.class))
            .pollInterval(Duration.ofSeconds(1))
            .closeJoinTimeoutMs(100) // join times out well before the 500ms slow shutdown
            .build();
    poller.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered loadPending");
    assertTrue(poller.isStarted());

    poller.close(); // join(100ms) times out; the poll thread is still winding down (alive)

    // Not restartable: started stays set, so a restart is refused rather than silently duplicating.
    assertTrue(poller.isStarted(), "a join-timeout close() must leave the runner not-restartable");
    assertThrows(IllegalStateException.class, poller::start);
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaDeadLetterStore;

/**
 * A retention sweeper's {@code close()} returns only after a sweep in flight returned: whoever
 * closes the store's database after the sweeper (an integration closing the backend's stores at
 * shutdown) must not race a sweep, which on SQLite would write after the instance guard was
 * released.
 */
class RetentionSweeperCloseTest {

  private static final Duration TICK = Duration.ofHours(1);
  private static final Duration ENABLED = Duration.ofDays(7);
  private static final Clock CLOCK = Clock.systemUTC();

  /** A delete that signals it began, then ignores interrupts until released, then records it. */
  private static final class SlowDelete {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();

    Answer<Integer> answer() {
      return invocation -> {
        entered.countDown();
        boolean interrupted = false;
        while (true) {
          try {
            if (release.await(10, TimeUnit.SECONDS)) {
              break;
            }
          } catch (InterruptedException interruptedByClose) {
            // A JDBC call does not stop on an interrupt either: keep going until released, and
            // leave the interrupt pending, as a native call does.
            interrupted = true;
          }
        }
        finished.set(true);
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return 0;
      };
    }
  }

  private static void assertCloseWaitsForTheSweep(RetentionSweeper sweeper, SlowDelete delete)
      throws Exception {
    sweeper.start();
    assertThat(delete.entered.await(5, TimeUnit.SECONDS)).as("the first sweep began").isTrue();
    Thread releaser =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    Thread.sleep(200);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  delete.release.countDown();
                });
    sweeper.close();
    assertThat(delete.finished).as("close() returned after the sweep in flight").isTrue();
    assertThat(sweeper.isAlive()).isFalse();
    releaser.join();
  }

  @Test
  void inboxSweeperCloseWaitsForTheSweepInFlight() throws Exception {
    var delete = new SlowDelete();
    var inbox = mock(CommandInbox.class);
    when(inbox.deleteProcessedBefore(any())).thenAnswer(delete.answer());
    assertCloseWaitsForTheSweep(
        new InboxRetentionSweeper(inbox, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP), delete);
  }

  @Test
  void sagaDeadLetterSweeperCloseWaitsForTheSweepInFlight() throws Exception {
    var delete = new SlowDelete();
    var store = mock(SagaDeadLetterStore.class);
    when(store.deleteOlderThan(any())).thenAnswer(delete.answer());
    assertCloseWaitsForTheSweep(
        new SagaDeadLetterRetentionSweeper(store, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        delete);
  }

  @Test
  void outboxSweeperCloseWaitsForTheSweepInFlight() throws Exception {
    var delete = new SlowDelete();
    var store = mock(OutboxStore.class);
    when(store.deleteDelivered(any())).thenAnswer(delete.answer());
    assertCloseWaitsForTheSweep(
        new OutboxRetentionSweeper(store, ENABLED, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        delete);
  }

  @Test
  void deadLetterSweeperCloseWaitsForTheSweepInFlight() throws Exception {
    var delete = new SlowDelete();
    var queue = mock(DeadLetterQueue.class);
    when(queue.deleteOlderThan(any())).thenAnswer(delete.answer());
    assertCloseWaitsForTheSweep(
        new DeadLetterRetentionSweeper(queue, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP),
        delete);
  }

  @Test
  void aSweepThatDoesNotEndInTimeIsLeftBehindWithAWarning() throws Exception {
    var delete = new SlowDelete();
    var inbox = mock(CommandInbox.class);
    when(inbox.deleteProcessedBefore(any())).thenAnswer(delete.answer());
    var sweeper = new InboxRetentionSweeper(inbox, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP);
    sweeper.closeJoinTimeoutMs(50);
    sweeper.start();
    assertThat(delete.entered.await(5, TimeUnit.SECONDS)).isTrue();

    sweeper.close();

    assertThat(delete.finished).as("close() gave up waiting").isFalse();
    delete.release.countDown();
  }

  @Test
  void stopOfNoThreadOrOfItselfDoesNotWait() {
    assertThat(RetentionSweeperThreads.stop(null, "sweeper", 10)).isTrue();
    assertThat(RetentionSweeperThreads.stop(Thread.currentThread(), "sweeper", 10)).isFalse();
    assertThat(Thread.interrupted()).as("the self-stop interrupted this thread").isTrue();
  }

  @Test
  void aCloserInterruptedWhileWaitingKeepsItsInterrupt() throws Exception {
    var delete = new SlowDelete();
    var inbox = mock(CommandInbox.class);
    when(inbox.deleteProcessedBefore(any())).thenAnswer(delete.answer());
    var sweeper = new InboxRetentionSweeper(inbox, ENABLED, TICK, CLOCK, StreamRuneMetrics.NOOP);
    sweeper.start();
    assertThat(delete.entered.await(5, TimeUnit.SECONDS)).isTrue();
    var interruptKept = new AtomicBoolean();
    Thread closer =
        Thread.ofVirtual()
            .start(
                () -> {
                  Thread.currentThread().interrupt();
                  sweeper.close();
                  interruptKept.set(Thread.currentThread().isInterrupted());
                });
    closer.join(5_000);
    assertThat(interruptKept).isTrue();
    delete.release.countDown();
  }
}

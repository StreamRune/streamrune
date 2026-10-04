package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.event.StartupEvent;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.ScheduledProjectionRunner;

/** Tests for {@link StreamRuneLifecycle}. */
class StreamRuneLifecycleTest {

  @Test
  void startsAllPresentRunnersOnStartup() {
    var multi = mock(MultiProjectionRunner.class);
    var scheduled = mock(ScheduledProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    var sweeper = mock(OutboxRetentionSweeper.class);
    var inboxSweeper = mock(InboxRetentionSweeper.class);
    var sagaDlqSweeper = mock(SagaDeadLetterRetentionSweeper.class);
    var dlqSweeper = mock(DeadLetterRetentionSweeper.class);
    var retry = mock(DeadLetterRetryRunner.class);
    var leadership = mock(org.streamrune.core.subscription.SubscriptionLeadership.class);
    var lifecycle =
        new StreamRuneLifecycle(
            multi,
            scheduled,
            poller,
            sweeper,
            inboxSweeper,
            sagaDlqSweeper,
            dlqSweeper,
            retry,
            leadership);

    lifecycle.onApplicationEvent(mock(StartupEvent.class));

    verify(multi).start();
    verify(scheduled).start();
    verify(poller).start();
    verify(sweeper).start();
    verify(inboxSweeper).start();
    verify(sagaDlqSweeper).start();
    verify(dlqSweeper).start();
    verify(retry).start();
  }

  @Test
  void closesAllPresentRunnersOnShutdown() {
    var multi = mock(MultiProjectionRunner.class);
    var scheduled = mock(ScheduledProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    var sweeper = mock(OutboxRetentionSweeper.class);
    var inboxSweeper = mock(InboxRetentionSweeper.class);
    var sagaDlqSweeper = mock(SagaDeadLetterRetentionSweeper.class);
    var dlqSweeper = mock(DeadLetterRetentionSweeper.class);
    var retry = mock(DeadLetterRetryRunner.class);
    var leadership = mock(org.streamrune.core.subscription.SubscriptionLeadership.class);
    var lifecycle =
        new StreamRuneLifecycle(
            multi,
            scheduled,
            poller,
            sweeper,
            inboxSweeper,
            sagaDlqSweeper,
            dlqSweeper,
            retry,
            leadership);

    lifecycle.close();

    verify(multi).close();
    verify(scheduled).close();
    verify(poller).close();
    verify(sweeper).close();
    verify(inboxSweeper).close();
    verify(sagaDlqSweeper).close();
    verify(dlqSweeper).close();
    verify(retry).close();
    // Leadership is released after the runners have all been closed.
    verify(leadership).close();
  }

  @Test
  void toleratesAbsentRunners() {
    var lifecycle = new StreamRuneLifecycle(null, null, null, null, null, null, null, null, null);

    assertDoesNotThrow(() -> lifecycle.onApplicationEvent(mock(StartupEvent.class)));
    assertDoesNotThrow(lifecycle::close);
  }

  @Test
  void closeFailureDoesNotPreventClosingRemainingRunners() {
    var multi = mock(MultiProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    doThrow(new RuntimeException("boom")).when(poller).close();
    var lifecycle =
        new StreamRuneLifecycle(multi, null, poller, null, null, null, null, null, null);

    assertDoesNotThrow(lifecycle::close);

    verify(multi).close();
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DeadLetterQueue;

/**
 * Regression: {@link DeadLetterRetryRunner#close()} must JOIN the poll thread before resetting the
 * {@code started} guard. Without the join, {@code close()} returns while the poll thread is still
 * winding down, and its {@code finally { running = false }} can then run <em>after</em> a
 * subsequent {@code start()} set {@code running = true} — silently killing the restarted runner.
 */
class DeadLetterRetryRunnerRestartRaceTest {

  @Test
  void closeJoinsPollThreadSoRestartCannotSilentlyKillTheRunner() throws Exception {
    var pollThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);
    DeadLetterQueue blockingDlq = mock(DeadLetterQueue.class);
    when(blockingDlq.readRetryable(anyInt(), anyInt()))
        .thenAnswer(RestartRaceTestSupport.parkThenSlowShutdown(pollThread, entered, List.of()));

    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(blockingDlq)
            .commandBus(mock(CommandBus.class))
            .objectMapper(new ObjectMapper())
            .pollInterval(Duration.ofSeconds(1))
            .build();
    runner.start();
    assertTrue(entered.await(5, TimeUnit.SECONDS), "poll thread should have entered readRetryable");

    runner.close();

    assertFalse(
        pollThread.get().isAlive(),
        "close() must join the poll thread before returning so a restart is not silently killed"
            + "");
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.MutableClock;

class DeadLetterRetentionSweeperTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  /** Minimal no-op queue; subclasses override only the methods they need. */
  private static DeadLetterQueue noopQueue() {
    return new DeadLetterQueue() {
      @Override
      public void publish(DeadLetterPublishRequest request) {}

      @Override
      public List<DeadLetterEntry> read(int limit) {
        return List.of();
      }

      @Override
      public void discard(CommandId commandId) {}

      @Override
      public int deleteOlderThan(Instant cutoff) {
        return 0;
      }
    };
  }

  /** Records every recordDeadLetterSwept(rows) call; all other methods are no-ops. */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final List<Long> swept = new ArrayList<>();

    @Override
    public void recordDeadLetterSwept(long rows) {
      swept.add(rows);
    }
  }

  private static DeadLetterQueue.DeadLetterPublishRequest request(String commandId) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        "SomeCommand",
        CommandId.of(commandId),
        StreamId.of(TYPE, AggregateId.of("s1")),
        "java.lang.RuntimeException",
        "boom",
        1,
        Instant.now(),
        null,
        null,
        null,
        null);
  }

  @Test
  void sweepOnce_passesNowMinusMaxAge() {
    AtomicReference<Instant> cutoff = new AtomicReference<>();
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    DeadLetterQueue queue =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {}

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public int deleteOlderThan(Instant olderThan) {
            cutoff.set(olderThan);
            return 3;
          }
        };
    var sweeper =
        new DeadLetterRetentionSweeper(
            queue, Duration.ofDays(30), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();
    assertThat(cutoff.get()).isEqualTo(Instant.parse("2026-05-22T00:00:00Z"));
  }

  @Test
  void sweepOnce_removesOldEntryKeepsFreshEntry() {
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    InMemoryDeadLetterQueue queue = new InMemoryDeadLetterQueue(clock);

    queue.publish(request("cmd-old")); // published_at = 2026-06-21T00:00:00Z (30 days ago in test)
    clock.advance(Duration.ofDays(31));
    queue.publish(request("cmd-fresh")); // published_at = 31 days later

    var sweeper =
        new DeadLetterRetentionSweeper(
            queue, Duration.ofDays(30), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    var remainingIds = queue.all().stream().map(e -> e.commandId().value()).toList();
    assertThat(remainingIds).containsExactly("cmd-fresh");
  }

  @Test
  void sweepOnce_removesOnlyStrictlyOlder() {
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    Instant cutoff = now.minus(Duration.ofDays(30));

    AtomicReference<Instant> capturedCutoff = new AtomicReference<>();
    DeadLetterQueue queue =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {}

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public int deleteOlderThan(Instant olderThan) {
            capturedCutoff.set(olderThan);
            return 1;
          }
        };

    var sweeper =
        new DeadLetterRetentionSweeper(
            queue, Duration.ofDays(30), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(capturedCutoff.get()).isEqualTo(cutoff);
  }

  @Test
  void start_close_start_sweeps_again() throws Exception {
    var deleteCount = new AtomicInteger(0);
    DeadLetterQueue countingQueue =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {}

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public int deleteOlderThan(Instant cutoff) {
            deleteCount.incrementAndGet();
            return 1;
          }
        };

    var sweeper =
        new DeadLetterRetentionSweeper(
            countingQueue,
            Duration.ofDays(30),
            Duration.ofMillis(50),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP);

    sweeper.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (deleteCount.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertNotEquals(0, deleteCount.get(), "sweeper must have called deleteOlderThan at least once");
    sweeper.close();

    deleteCount.set(0);
    assertDoesNotThrow(sweeper::start, "restart after close must not throw");
    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (deleteCount.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertNotEquals(0, deleteCount.get(), "sweeper must sweep again after restart");
    sweeper.close();
  }

  @Test
  void start_close_start_disabled_path_is_clean() {
    // When maxAge<=0 retention is disabled — start() must be a clean no-op that can be repeated.
    var sweeper =
        new DeadLetterRetentionSweeper(
            noopQueue(),
            Duration.ZERO,
            Duration.ofMillis(50),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP);

    sweeper.start(); // no-op: sets started=true, no thread
    sweeper.close(); // must clear started so next start() is also a no-op
    assertDoesNotThrow(sweeper::start, "second start on disabled sweeper must not throw");
    sweeper.close();
  }

  @Test
  void sweepOnce_recordsDeadLetterSwept_withExactRowCount() {
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    InMemoryDeadLetterQueue queue = new InMemoryDeadLetterQueue(clock);

    queue.publish(request("cmd-1"));
    queue.publish(request("cmd-2"));
    clock.advance(Duration.ofDays(31));

    RecordingMetrics metrics = new RecordingMetrics();
    var sweeper =
        new DeadLetterRetentionSweeper(
            queue, Duration.ofDays(30), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.swept)
        .as("sweep must record the exact number of rows removed")
        .containsExactly(2L);
  }

  @Test
  void sweepOnce_survivesThrowingSweptMetric() {
    // recordDeadLetterSwept ran bare. The delete already committed by the time it's
    // called, so a throwing metrics backend must not turn a successful sweep into a thrown
    // exception that could kill whatever periodic driver calls sweepOnce().
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    InMemoryDeadLetterQueue queue = new InMemoryDeadLetterQueue(clock);
    queue.publish(request("cmd-1"));
    clock.advance(Duration.ofDays(31));

    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordDeadLetterSwept(long rows) {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var sweeper =
        new DeadLetterRetentionSweeper(
            queue, Duration.ofDays(30), Duration.ofHours(1), clock, throwing);

    assertDoesNotThrow(sweeper::sweepOnce, "a throwing metrics backend must not abort the sweep");
  }

  @Test
  void sweepOnce_withNoEligibleRows_doesNotRecordMetric() {
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    RecordingMetrics metrics = new RecordingMetrics();

    var sweeper =
        new DeadLetterRetentionSweeper(
            noopQueue(), Duration.ofDays(30), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.swept)
        .as("a sweep with zero eligible rows must not record any metric")
        .isEmpty();
  }
}

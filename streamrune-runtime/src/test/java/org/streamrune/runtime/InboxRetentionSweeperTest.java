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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;

class InboxRetentionSweeperTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  /** Minimal no-op inbox; subclasses override only the methods they need. */
  private static CommandInbox noopInbox() {
    return new CommandInbox() {
      public Optional<InboxResult> find(IdempotencyKey key) {
        return Optional.empty();
      }

      public int deleteProcessedBefore(Instant cutoff) {
        return 0;
      }
    };
  }

  /** Records every recordInboxSwept(rows) call; all other methods are no-ops. */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final List<Long> inboxSwept = new ArrayList<>();

    @Override
    public void recordInboxSwept(long rows) {
      inboxSwept.add(rows);
    }
  }

  @Test
  void sweepOnce_passesNowMinusMaxAge() {
    AtomicReference<Instant> cutoff = new AtomicReference<>();
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    CommandInbox inbox =
        new CommandInbox() {
          public Optional<InboxResult> find(IdempotencyKey key) {
            return Optional.empty();
          }

          public int deleteProcessedBefore(Instant olderThan) {
            cutoff.set(olderThan);
            return 3;
          }
        };
    var sweeper =
        new InboxRetentionSweeper(
            inbox, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();
    assertThat(cutoff.get()).isEqualTo(Instant.parse("2026-06-14T00:00:00Z"));
  }

  @Test
  void sweepOnce_removesOldRowKeepsFreshRow() {
    // Seed the in-memory inbox with one old row (now - 8d) and one fresh row (now - 1d).
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    IdempotencyKey oldKey = IdempotencyKey.of("old-key");
    IdempotencyKey freshKey = IdempotencyKey.of("fresh-key");
    StreamId stream = StreamId.of(TYPE, AggregateId.of("test-stream"));
    Version v = new Version(1L);

    // Old: processed 8 days ago — before the 7-day cutoff.
    inbox.record(
        new CommandInbox.InboxResult(
            oldKey,
            "SomeCommand",
            stream,
            v,
            List.of(GlobalOffset.of(1L)),
            now.minus(Duration.ofDays(8))));
    // Fresh: processed 1 day ago — after the 7-day cutoff.
    inbox.record(
        new CommandInbox.InboxResult(
            freshKey,
            "SomeCommand",
            stream,
            v,
            List.of(GlobalOffset.of(2L)),
            now.minus(Duration.ofDays(1))));

    var sweeper =
        new InboxRetentionSweeper(
            inbox, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(inbox.find(oldKey)).isEmpty();
    assertThat(inbox.find(freshKey)).isPresent();
  }

  @Test
  void start_close_start_sweeps_again() throws Exception {
    var deleteCount = new AtomicInteger(0);
    CommandInbox countingInbox =
        new CommandInbox() {
          public Optional<InboxResult> find(IdempotencyKey key) {
            return Optional.empty();
          }

          public int deleteProcessedBefore(Instant cutoff) {
            deleteCount.incrementAndGet();
            return 1;
          }
        };

    var sweeper =
        new InboxRetentionSweeper(
            countingInbox,
            Duration.ofDays(7),
            Duration.ofMillis(50),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP);

    sweeper.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (deleteCount.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertNotEquals(
        0, deleteCount.get(), "sweeper must have called deleteProcessedBefore at least once");
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
    // When maxAge=ZERO retention is disabled — start() must be a clean no-op that can be repeated.
    var sweeper =
        new InboxRetentionSweeper(
            noopInbox(),
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
  void sweepOnce_recordsInboxSwept_withExactRowCount() {
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    // Two rows older than the 7-day cutoff — both must be swept.
    inbox.record(
        new CommandInbox.InboxResult(
            IdempotencyKey.of("old-key-1"),
            "SomeCommand",
            StreamId.of(TYPE, AggregateId.of("test-stream")),
            new Version(1L),
            List.of(GlobalOffset.of(1L)),
            now.minus(Duration.ofDays(8))));
    inbox.record(
        new CommandInbox.InboxResult(
            IdempotencyKey.of("old-key-2"),
            "SomeCommand",
            StreamId.of(TYPE, AggregateId.of("test-stream")),
            new Version(1L),
            List.of(GlobalOffset.of(2L)),
            now.minus(Duration.ofDays(9))));

    RecordingMetrics metrics = new RecordingMetrics();
    var sweeper =
        new InboxRetentionSweeper(inbox, Duration.ofDays(7), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.inboxSwept)
        .as("sweep must record the exact number of rows removed")
        .containsExactly(2L);
  }

  @Test
  void sweepOnce_survivesThrowingSweptMetric() {
    // recordInboxSwept ran bare. The delete already committed by the time it's called, so
    // a throwing metrics backend must not turn a successful sweep into a thrown exception that
    // could kill whatever periodic driver calls sweepOnce().
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    inbox.record(
        new CommandInbox.InboxResult(
            IdempotencyKey.of("old-key-1"),
            "SomeCommand",
            StreamId.of(TYPE, AggregateId.of("test-stream")),
            new Version(1L),
            List.of(GlobalOffset.of(1L)),
            now.minus(Duration.ofDays(8))));

    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordInboxSwept(long rows) {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var sweeper =
        new InboxRetentionSweeper(inbox, Duration.ofDays(7), Duration.ofHours(1), clock, throwing);

    assertDoesNotThrow(sweeper::sweepOnce, "a throwing metrics backend must not abort the sweep");
  }

  @Test
  void sweepOnce_withNoEligibleRows_doesNotRecordMetric() {
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    RecordingMetrics metrics = new RecordingMetrics();

    var sweeper =
        new InboxRetentionSweeper(
            noopInbox(), Duration.ofDays(7), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.inboxSwept)
        .as("a sweep with zero eligible rows must not record any metric")
        .isEmpty();
  }
}

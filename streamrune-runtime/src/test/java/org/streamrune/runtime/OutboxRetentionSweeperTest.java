package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.*;

class OutboxRetentionSweeperTest {

  /** Minimal no-op store; subclasses override only the methods they need. */
  private static OutboxStore noopStore() {
    return new OutboxStore() {
      public void save(OutboxEntry e) {}

      public java.util.List<OutboxEntry> loadPending(int l) {
        return java.util.List.of();
      }

      public String claimedBy() {
        return "test";
      }

      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        return true;
      }

      public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
        return true;
      }

      public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
        return true;
      }

      public void delete(OutboxEntryId id) {}

      public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
        return java.util.List.of();
      }

      public boolean resetFailedToPending(OutboxEntryId id) {
        return false;
      }

      public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
        return java.util.Optional.empty();
      }

      public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
        return false;
      }

      @Override
      public int deleteDelivered(Instant olderThan) {
        return 0;
      }

      @Override
      public java.time.Duration claimLease() {
        return java.time.Duration.ofMinutes(4);
      }
    };
  }

  /** Records recordOutboxSwept / recordOutboxSkippedSwept calls; all other methods are no-ops. */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final List<Long> outboxSwept = new ArrayList<>();
    final List<Long> outboxSkippedSwept = new ArrayList<>();

    @Override
    public void recordOutboxSwept(long rows) {
      outboxSwept.add(rows);
    }

    @Override
    public void recordOutboxSkippedSwept(long rows) {
      outboxSkippedSwept.add(rows);
    }
  }

  /**
   * A store that records the cutoff passed to deleteDelivered/deleteSkipped and returns a fixed row
   * count for each. All other methods are no-ops.
   */
  private static final class SweepRecordingStore implements OutboxStore {
    final AtomicReference<Instant> deliveredCutoff = new AtomicReference<>();
    final AtomicReference<Instant> skippedCutoff = new AtomicReference<>();
    final int deliveredRows;
    final int skippedRows;

    SweepRecordingStore(int deliveredRows, int skippedRows) {
      this.deliveredRows = deliveredRows;
      this.skippedRows = skippedRows;
    }

    public void save(OutboxEntry e) {}

    public java.util.List<OutboxEntry> loadPending(int l) {
      return java.util.List.of();
    }

    public String claimedBy() {
      return "test";
    }

    public boolean markDelivered(OutboxEntryId id, String claimedBy) {
      return true;
    }

    public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
      return true;
    }

    public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
      return true;
    }

    public void delete(OutboxEntryId id) {}

    public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
      return java.util.List.of();
    }

    public boolean resetFailedToPending(OutboxEntryId id) {
      return false;
    }

    public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
      return java.util.Optional.empty();
    }

    public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
      return false;
    }

    @Override
    public int deleteDelivered(Instant olderThan) {
      deliveredCutoff.set(olderThan);
      return deliveredRows;
    }

    @Override
    public int deleteSkipped(Instant cutoff) {
      skippedCutoff.set(cutoff);
      return skippedRows;
    }

    @Override
    public java.time.Duration claimLease() {
      return java.time.Duration.ofMinutes(4);
    }
  }

  @Test
  void sweep_passesNowMinusMaxAge() {
    AtomicReference<Instant> cutoff = new AtomicReference<>();
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    OutboxStore store =
        new OutboxStore() {
          public void save(OutboxEntry e) {}

          public java.util.List<OutboxEntry> loadPending(int l) {
            return java.util.List.of();
          }

          public String claimedBy() {
            return "test";
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
            return java.util.List.of();
          }

          public boolean resetFailedToPending(OutboxEntryId id) {
            return false;
          }

          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          @Override
          public int deleteDelivered(Instant olderThan) {
            cutoff.set(olderThan);
            return 3;
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };
    var sweeper =
        new OutboxRetentionSweeper(
            store,
            Duration.ofDays(7),
            Duration.ZERO,
            Duration.ofHours(1),
            clock,
            StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();
    assertThat(cutoff.get()).isEqualTo(Instant.parse("2026-06-14T00:00:00Z"));
  }

  @Test
  void start_close_start_sweeps_again() throws Exception {
    var deleteCount = new AtomicInteger(0);
    OutboxStore countingStore =
        new OutboxStore() {
          public void save(OutboxEntry e) {}

          public java.util.List<OutboxEntry> loadPending(int l) {
            return java.util.List.of();
          }

          public String claimedBy() {
            return "test";
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
            return java.util.List.of();
          }

          public boolean resetFailedToPending(OutboxEntryId id) {
            return false;
          }

          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          @Override
          public int deleteDelivered(Instant olderThan) {
            deleteCount.incrementAndGet();
            return 1;
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };

    var sweeper =
        new OutboxRetentionSweeper(
            countingStore,
            Duration.ofDays(7),
            Duration.ZERO,
            Duration.ofMillis(50),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP);

    sweeper.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (deleteCount.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertNotEquals(0, deleteCount.get(), "sweeper must have called deleteDelivered at least once");
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
        new OutboxRetentionSweeper(
            noopStore(),
            Duration.ZERO,
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
  void sweepOnce_recordsOutboxSwept_withExactRowCount() {
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    OutboxStore store =
        new OutboxStore() {
          public void save(OutboxEntry e) {}

          public java.util.List<OutboxEntry> loadPending(int l) {
            return java.util.List.of();
          }

          public String claimedBy() {
            return "test";
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
            return java.util.List.of();
          }

          public boolean resetFailedToPending(OutboxEntryId id) {
            return false;
          }

          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          @Override
          public int deleteDelivered(Instant olderThan) {
            return 5;
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };

    RecordingMetrics metrics = new RecordingMetrics();
    var sweeper =
        new OutboxRetentionSweeper(
            store, Duration.ofDays(7), Duration.ZERO, Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.outboxSwept)
        .as("sweep must record the exact number of rows removed")
        .containsExactly(5L);
  }

  @Test
  void sweepOnce_withNoEligibleRows_doesNotRecordMetric() {
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    RecordingMetrics metrics = new RecordingMetrics();

    var sweeper =
        new OutboxRetentionSweeper(
            noopStore(), Duration.ofDays(7), Duration.ZERO, Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.outboxSwept)
        .as("a sweep with zero eligible rows must not record any metric")
        .isEmpty();
  }

  // ── SKIPPED retention sweep (replaces the FAILED window) ────────────────

  @Test
  void sweepOnce_sweepsSkipped_recordsSkippedSwept_andPassesSkippedCutoff() {
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    var store = new SweepRecordingStore(2, 5);
    var metrics = new RecordingMetrics();
    var sweeper =
        new OutboxRetentionSweeper(
            store,
            Duration.ofDays(7), // DELIVERED window
            Duration.ofDays(30), // SKIPPED window
            Duration.ofHours(1),
            clock,
            metrics);

    sweeper.sweepOnce();

    // DELIVERED retention still runs with its own cutoff (now - 7d).
    assertThat(store.deliveredCutoff.get()).isEqualTo(Instant.parse("2026-06-14T00:00:00Z"));
    assertThat(metrics.outboxSwept).containsExactly(2L);
    // SKIPPED retention runs with cutoff now - 30d.
    assertThat(store.skippedCutoff.get()).isEqualTo(Instant.parse("2026-05-22T00:00:00Z"));
    assertThat(metrics.outboxSkippedSwept).containsExactly(5L);
  }

  @Test
  void sweepOnce_survivesThrowingSweptMetrics_bothDeliveredAndSkipped() {
    // recordOutboxSwept / recordOutboxSkippedSwept are guarded. Both deletes already
    // committed by the time either is called, so a throwing metrics backend must not turn a
    // successful sweep into a thrown exception — and, critically, a throw from the DELIVERED
    // metric must not abort the SKIPPED sweep that follows it in the same cycle.
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    var store = new SweepRecordingStore(2, 5);
    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordOutboxSwept(long rows) {
            throw new IllegalStateException("simulated metrics backend failure (delivered)");
          }

          @Override
          public void recordOutboxSkippedSwept(long rows) {
            throw new IllegalStateException("simulated metrics backend failure (skipped)");
          }
        };
    var sweeper =
        new OutboxRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofDays(30), Duration.ofHours(1), clock, throwing);

    assertDoesNotThrow(sweeper::sweepOnce, "a throwing metrics backend must not abort the sweep");
    // Both deletes ran despite the first metric throwing — the guard did not cut the cycle short.
    assertThat(store.deliveredCutoff.get()).isNotNull();
    assertThat(store.skippedCutoff.get()).isNotNull();
  }

  @Test
  void sweepOnce_skippedMaxAgeDisabled_neverCallsDeleteSkipped() {
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    var store = new SweepRecordingStore(3, 9);
    var metrics = new RecordingMetrics();
    // skippedMaxAge = ZERO → SKIPPED retention disabled; only DELIVERED runs.
    var sweeper =
        new OutboxRetentionSweeper(
            store, Duration.ofDays(7), Duration.ZERO, Duration.ofHours(1), clock, metrics);

    sweeper.sweepOnce();

    assertThat(store.deliveredCutoff.get())
        .as("DELIVERED retention still runs")
        .isEqualTo(Instant.parse("2026-06-14T00:00:00Z"));
    assertThat(store.skippedCutoff.get()).as("deleteSkipped must NOT be called").isNull();
    assertThat(metrics.outboxSkippedSwept).as("no SKIPPED metric when disabled").isEmpty();
  }

  @Test
  void sweepOnce_skippedSweepWithZeroRows_doesNotRecordSkippedMetric() {
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    var store = new SweepRecordingStore(0, 0);
    var metrics = new RecordingMetrics();
    var sweeper =
        new OutboxRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofDays(30), Duration.ofHours(1), clock, metrics);

    sweeper.sweepOnce();

    // deleteSkipped WAS called (cutoff recorded) but returned 0 → no metric.
    assertThat(store.skippedCutoff.get()).isNotNull();
    assertThat(metrics.outboxSkippedSwept).isEmpty();
    assertThat(metrics.outboxSwept).isEmpty();
  }

  @Test
  void spi_hasNoFailedDelete_INV4() {
    // No sweeper, no default, no fallback deletes a FAILED row — the SPI cannot express it.
    assertThat(
            java.util.Arrays.stream(OutboxStore.class.getMethods())
                .map(java.lang.reflect.Method::getName))
        .doesNotContain("deleteFailed")
        .contains("deleteDelivered", "deleteSkipped");
  }

  @Test
  void start_disabledMessage_namesBothWindows() {
    // Both windows off: no thread, and the single INFO names the SKIPPED window and the two
    // retention methods that exist — never the removed FAILED sweep. Captured the way
    // OutboxPollerTest.captureOutboxPollerLogs does it (logback ListAppender on the class logger).
    // logback-test.xml keeps the root at WARN, so the logger is raised to INFO for the test's
    // duration (the OutboxPollerTest.start_logsOrderingMode precedent) and restored in finally.
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(OutboxRetentionSweeper.class);
    var previousLevel = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try (var sweeper =
        new OutboxRetentionSweeper(
            noopStore(),
            Duration.ZERO,
            Duration.ZERO,
            Duration.ofHours(1),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP)) {
      sweeper.start();
      assertThat(sweeper.isStarted()).isFalse();
      assertThat(appender.list)
          .filteredOn(e -> e.getLevel() == ch.qos.logback.classic.Level.INFO)
          .singleElement()
          .satisfies(
              e -> {
                String msg = e.getFormattedMessage();
                assertThat(msg)
                    .startsWith("Outbox retention is disabled (maxAge=PT0S, skippedMaxAge=PT0S)");
                assertThat(msg)
                    .contains("neither deleteDelivered nor deleteSkipped will be called");
                assertThat(msg).doesNotContain("deleteFailed").doesNotContain("failedMaxAge");
              });
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
    }
  }
}

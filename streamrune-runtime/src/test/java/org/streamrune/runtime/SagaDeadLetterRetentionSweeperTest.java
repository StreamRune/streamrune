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
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.InMemorySagaDeadLetterStore;

class SagaDeadLetterRetentionSweeperTest {

  /** Minimal no-op store; subclasses override only the methods they need. */
  private static SagaDeadLetterStore noopStore() {
    return new SagaDeadLetterStore() {
      @Override
      public void publish(SagaDeadLetterEntry entry) {}

      @Override
      public List<SagaDeadLetterEntry> findAll(int limit) {
        return List.of();
      }

      @Override
      public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
        return List.of();
      }

      @Override
      public int deleteOlderThan(Instant cutoff) {
        return 0;
      }

      @Override
      public boolean discard(
          SagaId sagaId,
          org.streamrune.core.types.SagaType sagaType,
          org.streamrune.core.types.GlobalOffset eventOffset) {
        return false;
      }

      @Override
      public void publishShielded(SagaDeadLetterEntry entry) {
        throw new UnsupportedOperationException("publishShielded");
      }

      @Override
      public void establishFirstReplayAnchor(
          SagaId sagaId,
          org.streamrune.core.types.SagaType sagaType,
          org.streamrune.core.types.GlobalOffset eventOffset,
          java.time.Instant anchorIfAbsent) {
        throw new UnsupportedOperationException("establishFirstReplayAnchor");
      }

      @Override
      public void setResolvedTarget(
          org.streamrune.core.types.SagaType sagaType,
          org.streamrune.core.types.GlobalOffset eventOffset,
          SagaId targetSagaId) {
        throw new UnsupportedOperationException("setResolvedTarget");
      }

      @Override
      public boolean clearShieldIfDrained(
          SagaId sagaId, org.streamrune.core.types.SagaType sagaType) {
        throw new UnsupportedOperationException("clearShieldIfDrained");
      }
    };
  }

  /** Records every recordSagaDeadLetterSwept(rows) call; all other methods are no-ops. */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final List<Long> swept = new ArrayList<>();
    final List<Long> faultedBacklog = new ArrayList<>();

    @Override
    public void recordSagaDeadLetterSwept(long rows) {
      swept.add(rows);
    }

    @Override
    public void recordSagaFaultedBacklog(long count) {
      faultedBacklog.add(count);
    }
  }

  private static SagaDeadLetterStore.SagaDeadLetterEntry entry(
      String sagaId, long offset, Instant faultedAt) {
    return new SagaDeadLetterStore.SagaDeadLetterEntry(
        sagaId == null ? null : SagaId.of(sagaId),
        SagaType.of("SomeSaga"),
        GlobalOffset.of(offset),
        EventType.of("SomeEvent"),
        "java.lang.RuntimeException",
        "boom",
        faultedAt);
  }

  /**
   * A dead-letter store bound to a saga store that holds a {@code COMPLETED} row for each named
   * saga: their entries are unshielded, so the sweep judges them on age alone.
   */
  private static InMemorySagaDeadLetterStore storeWithCompletedOwners(String... sagaIds) {
    var sagaStore = new org.streamrune.test.InMemorySagaStore();
    for (String sagaId : sagaIds) {
      sagaStore.create(
          SagaId.of(sagaId),
          SagaType.of("SomeSaga"),
          new GuardState(org.streamrune.core.saga.SagaStatus.COMPLETED),
          org.streamrune.core.saga.SagaStatus.COMPLETED);
    }
    return new InMemorySagaDeadLetterStore(sagaStore);
  }

  @Test
  void sweepOnce_passesNowMinusMaxAge() {
    AtomicReference<Instant> cutoff = new AtomicReference<>();
    Clock clock = Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC);
    SagaDeadLetterStore store =
        new SagaDeadLetterStore() {
          @Override
          public void publish(SagaDeadLetterEntry entry) {}

          @Override
          public List<SagaDeadLetterEntry> findAll(int limit) {
            return List.of();
          }

          @Override
          public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
            return List.of();
          }

          @Override
          public int deleteOlderThan(Instant olderThan) {
            cutoff.set(olderThan);
            return 3;
          }

          @Override
          public boolean discard(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset) {
            return false;
          }

          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            throw new UnsupportedOperationException("publishShielded");
          }

          @Override
          public void establishFirstReplayAnchor(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              java.time.Instant anchorIfAbsent) {
            throw new UnsupportedOperationException("establishFirstReplayAnchor");
          }

          @Override
          public void setResolvedTarget(
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              SagaId targetSagaId) {
            throw new UnsupportedOperationException("setResolvedTarget");
          }

          @Override
          public boolean clearShieldIfDrained(
              SagaId sagaId, org.streamrune.core.types.SagaType sagaType) {
            throw new UnsupportedOperationException("clearShieldIfDrained");
          }
        };
    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();
    assertThat(cutoff.get()).isEqualTo(Instant.parse("2026-06-14T00:00:00Z"));
  }

  @Test
  void sweepOnce_removesOldEntryKeepsFreshEntry() {
    InMemorySagaDeadLetterStore store = storeWithCompletedOwners("saga-old", "saga-fresh");
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    // Old: faulted 8 days ago — before the 7-day cutoff.
    store.publish(entry("saga-old", 1L, now.minus(Duration.ofDays(8))));
    // Fresh: faulted 1 day ago — after the 7-day cutoff.
    store.publish(entry("saga-fresh", 2L, now.minus(Duration.ofDays(1))));

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(store.findBySaga(SagaId.of("saga-old"))).isEmpty();
    assertThat(store.findBySaga(SagaId.of("saga-fresh"))).hasSize(1);
  }

  @Test
  void sweepOnce_removesEntryByFirstFault_despiteFreshReQuarantineFaultedAt() {
    // A failed replay re-quarantines the entry and refreshes faultedAt; retention must
    // still be bounded by the FIRST fault, or every failed replay would grant the entry another
    // full window — past what the dead-letter <= inbox boot validator reasons about.
    InMemorySagaDeadLetterStore store = storeWithCompletedOwners("saga-requarantined");
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    store.publish(entry("saga-requarantined", 1L, now.minus(Duration.ofDays(8)))); // first fault
    store.publish(entry("saga-requarantined", 1L, now.minus(Duration.ofHours(1)))); // failed replay

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(store.findBySaga(SagaId.of("saga-requarantined")))
        .as("first fault was 8d ago (> 7d window) — the fresh faultedAt must not keep it alive")
        .isEmpty();
  }

  @Test
  void sweepOnce_removesOnlyStrictlyOlder() {
    InMemorySagaDeadLetterStore store = storeWithCompletedOwners("saga-at-cutoff", "saga-older");
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    Instant cutoff = now.minus(Duration.ofDays(7));

    // Exactly at cutoff — must be retained (strictly-before semantics).
    store.publish(entry("saga-at-cutoff", 1L, cutoff));
    // Strictly older than cutoff — must be swept.
    store.publish(entry("saga-older", 2L, cutoff.minusSeconds(1)));

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(store.findBySaga(SagaId.of("saga-at-cutoff"))).hasSize(1);
    assertThat(store.findBySaga(SagaId.of("saga-older"))).isEmpty();
  }

  @Test
  void start_close_start_sweeps_again() throws Exception {
    var deleteCount = new AtomicInteger(0);
    SagaDeadLetterStore countingStore =
        new SagaDeadLetterStore() {
          @Override
          public void publish(SagaDeadLetterEntry entry) {}

          @Override
          public List<SagaDeadLetterEntry> findAll(int limit) {
            return List.of();
          }

          @Override
          public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
            return List.of();
          }

          @Override
          public int deleteOlderThan(Instant cutoff) {
            deleteCount.incrementAndGet();
            return 1;
          }

          @Override
          public boolean discard(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset) {
            return false;
          }

          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            throw new UnsupportedOperationException("publishShielded");
          }

          @Override
          public void establishFirstReplayAnchor(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              java.time.Instant anchorIfAbsent) {
            throw new UnsupportedOperationException("establishFirstReplayAnchor");
          }

          @Override
          public void setResolvedTarget(
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              SagaId targetSagaId) {
            throw new UnsupportedOperationException("setResolvedTarget");
          }

          @Override
          public boolean clearShieldIfDrained(
              SagaId sagaId, org.streamrune.core.types.SagaType sagaType) {
            throw new UnsupportedOperationException("clearShieldIfDrained");
          }
        };

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            countingStore,
            Duration.ofDays(7),
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
        new SagaDeadLetterRetentionSweeper(
            noopStore(),
            Duration.ZERO,
            Duration.ofMillis(50),
            Clock.systemUTC(),
            StreamRuneMetrics.NOOP);

    sweeper.start(); // no-op: sets started=true, no thread
    sweeper.close(); // must clear started so next start() is also a no-op
    assertDoesNotThrow(sweeper::start, "second start on disabled sweeper must not throw");
    sweeper.close();
  }

  /** Minimal saga state for wiring an {@link InMemorySagaStore} into the retention guard. */
  record GuardState(org.streamrune.core.saga.SagaStatus status)
      implements org.streamrune.core.saga.SagaState {}

  @Test
  void sweepOnce_neverPrunesTheOnlyReplayHandleOfAFaultedSaga() {
    // The quarantine entry is the saga's sole un-fault/replay handle. A sweep that
    // prunes it while the saga is still FAULTED strands the saga forever (replay ->
    // ENTRY_NOT_FOUND; every runner excludes FAULTED) with no observable signal. The store-level
    // guard must exclude such entries; entries of recovered/terminal sagas and null-saga entries
    // remain prunable.
    var sagaStore = new org.streamrune.test.InMemorySagaStore();
    InMemorySagaDeadLetterStore store = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    Instant old = now.minus(Duration.ofDays(8));

    sagaStore.create(
        SagaId.of("saga-still-faulted"),
        SagaType.of("SomeSaga"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    sagaStore.create(
        SagaId.of("saga-recovered"),
        SagaType.of("SomeSaga"),
        new GuardState(org.streamrune.core.saga.SagaStatus.COMPENSATED),
        org.streamrune.core.saga.SagaStatus.COMPENSATED);
    store.publish(entry("saga-still-faulted", 1L, old));
    store.publish(entry("saga-recovered", 2L, old));
    store.publish(entry(null, 3L, old)); // null-saga entry: names no row, stays prunable

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, StreamRuneMetrics.NOOP);
    sweeper.sweepOnce();

    assertThat(store.findBySaga(SagaId.of("saga-still-faulted")))
        .as("the FAULTED saga's only replay handle must survive the sweep")
        .hasSize(1);
    assertThat(store.findBySaga(SagaId.of("saga-recovered"))).isEmpty();
    assertThat(store.all()).as("recovered + null-saga entries were pruned").hasSize(1);
  }

  @Test
  void sweepOnce_recordsSagaDeadLetterSwept_withExactRowCount() {
    InMemorySagaDeadLetterStore store = storeWithCompletedOwners("saga-1", "saga-2");
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    // Two entries older than the 7-day cutoff — both must be swept.
    store.publish(entry("saga-1", 1L, now.minus(Duration.ofDays(8))));
    store.publish(entry("saga-2", 2L, now.minus(Duration.ofDays(9))));

    RecordingMetrics metrics = new RecordingMetrics();
    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.swept)
        .as("sweep must record the exact number of rows removed")
        .containsExactly(2L);
  }

  @Test
  void sweepOnce_survivesThrowingSweptMetric() {
    // recordSagaDeadLetterSwept ran bare. The delete already committed by the time it's
    // called, so a throwing metrics backend must not turn a successful sweep into a thrown
    // exception that could kill whatever periodic driver calls sweepOnce().
    InMemorySagaDeadLetterStore store = storeWithCompletedOwners("saga-1");
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    store.publish(entry("saga-1", 1L, now.minus(Duration.ofDays(8))));

    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordSagaDeadLetterSwept(long rows) {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, throwing);

    assertDoesNotThrow(sweeper::sweepOnce, "a throwing metrics backend must not abort the sweep");
  }

  @Test
  void sweepOnce_samplesTheFaultedBacklogGauge() {
    // The sweep now PROTECTS the replay handle of a still-FAULTED
    // saga, so those entries accumulate silently — SAGA_FAULTED is a counter fired once at fault
    // time, not a standing backlog. This gauge makes the stranded population visible: the count of
    // dead-letter entries whose owning saga row is currently FAULTED (the exact complement of the
    // retention guard's NOT-EXISTS clause). Sampled once per sweep cycle.
    var sagaStore = new org.streamrune.test.InMemorySagaStore();
    InMemorySagaDeadLetterStore store = new InMemorySagaDeadLetterStore(sagaStore);
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);

    sagaStore.create(
        SagaId.of("saga-faulted-a"),
        SagaType.of("SomeSaga"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    sagaStore.create(
        SagaId.of("saga-faulted-b"),
        SagaType.of("SomeSaga"),
        new GuardState(org.streamrune.core.saga.SagaStatus.FAULTED),
        org.streamrune.core.saga.SagaStatus.FAULTED);
    sagaStore.create(
        SagaId.of("saga-recovered"),
        SagaType.of("SomeSaga"),
        new GuardState(org.streamrune.core.saga.SagaStatus.COMPENSATED),
        org.streamrune.core.saga.SagaStatus.COMPENSATED);
    store.publish(entry("saga-faulted-a", 1L, now));
    store.publish(entry("saga-faulted-b", 2L, now));
    store.publish(entry("saga-recovered", 3L, now)); // not faulted — excluded
    store.publish(entry(null, 4L, now)); // names no saga row — excluded

    RecordingMetrics metrics = new RecordingMetrics();
    new SagaDeadLetterRetentionSweeper(
            store, Duration.ofDays(7), Duration.ofHours(1), clock, metrics)
        .sweepOnce();

    assertThat(metrics.faultedBacklog)
        .as("one sample per sweep, counting only entries owned by a currently FAULTED saga")
        .containsExactly(2L);
  }

  @Test
  void sweepOnce_faultedBacklogSamplingDegradesGracefully_andNeverAbortsTheSweep() {
    // Mirrors OutboxPoller's backlog-gauge discipline: a store that does not support the count
    // (a third-party SagaDeadLetterStore inheriting the SPI default) disables the sample
    // permanently instead of throwing every cycle, and the prune itself must still run.
    AtomicInteger deletes = new AtomicInteger();
    SagaDeadLetterStore unsupported =
        new SagaDeadLetterStore() {
          @Override
          public void publish(SagaDeadLetterEntry entry) {}

          @Override
          public List<SagaDeadLetterEntry> findAll(int limit) {
            return List.of();
          }

          @Override
          public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
            return List.of();
          }

          @Override
          public int deleteOlderThan(Instant cutoff) {
            deletes.incrementAndGet();
            return 3;
          }

          @Override
          public boolean discard(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset) {
            return false;
          }

          @Override
          public void publishShielded(SagaDeadLetterEntry entry) {
            throw new UnsupportedOperationException("publishShielded");
          }

          @Override
          public void establishFirstReplayAnchor(
              SagaId sagaId,
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              java.time.Instant anchorIfAbsent) {
            throw new UnsupportedOperationException("establishFirstReplayAnchor");
          }

          @Override
          public void setResolvedTarget(
              org.streamrune.core.types.SagaType sagaType,
              org.streamrune.core.types.GlobalOffset eventOffset,
              SagaId targetSagaId) {
            throw new UnsupportedOperationException("setResolvedTarget");
          }

          @Override
          public boolean clearShieldIfDrained(
              SagaId sagaId, org.streamrune.core.types.SagaType sagaType) {
            throw new UnsupportedOperationException("clearShieldIfDrained");
          }
        };

    RecordingMetrics metrics = new RecordingMetrics();
    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            unsupported,
            Duration.ofDays(7),
            Duration.ofHours(1),
            Clock.fixed(Instant.parse("2026-06-21T00:00:00Z"), ZoneOffset.UTC),
            metrics);

    assertDoesNotThrow(sweeper::sweepOnce);
    assertDoesNotThrow(sweeper::sweepOnce);

    assertThat(metrics.faultedBacklog).as("unsupported count reports nothing").isEmpty();
    assertThat(deletes.get()).as("the prune still ran on both cycles").isEqualTo(2);
    assertThat(metrics.swept).containsExactly(3L, 3L);
  }

  @Test
  void sweepOnce_withNoEligibleRows_doesNotRecordMetric() {
    Instant now = Instant.parse("2026-06-21T00:00:00Z");
    Clock clock = Clock.fixed(now, ZoneOffset.UTC);
    RecordingMetrics metrics = new RecordingMetrics();

    var sweeper =
        new SagaDeadLetterRetentionSweeper(
            noopStore(), Duration.ofDays(7), Duration.ofHours(1), clock, metrics);
    sweeper.sweepOnce();

    assertThat(metrics.swept)
        .as("a sweep with zero eligible rows must not record any metric")
        .isEmpty();
  }
}

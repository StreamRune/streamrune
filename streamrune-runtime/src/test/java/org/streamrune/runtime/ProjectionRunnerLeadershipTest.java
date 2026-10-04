package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * Verifies that both projection runners gate offset advancement on live {@link
 * SubscriptionLeadership}: a non-leader stands by (reads/processes/advances nothing), and flipping
 * to leader resumes processing. Also verifies the {@link ContinuousProjectionRunner} pre-commit
 * re-check abandons a batch without committing when leadership is lost after the batch started.
 */
class ProjectionRunnerLeadershipTest {

  private static final AggregateType TYPE = AggregateType.of("live");

  private ContinuousProjectionRunner continuousRunner;
  private ScheduledProjectionRunner scheduledRunner;

  @AfterEach
  void tearDown() {
    if (continuousRunner != null) {
      continuousRunner.close();
    }
    if (scheduledRunner != null) {
      scheduledRunner.close();
    }
  }

  /** A settable leadership test double: acquire/lead flags flipped by the test. */
  private static final class StubLeadership implements SubscriptionLeadership {
    // The single fencing epoch this stub hands out for every held lease (epoch 1 — any non-zero
    // value; the runners thread it into executeAtomically but this in-memory path never fences).
    private static final Optional<Lease> LEASE = Optional.of(new Lease(1L));
    private final AtomicBoolean canAcquire;
    private final AtomicBoolean lead;
    // When >= 0, current() returns a lease this many more times, then empty — models a lease that
    // is
    // live when a batch starts but lost by the pre-commit re-check.
    private final java.util.concurrent.atomic.AtomicInteger leadForNextCalls =
        new java.util.concurrent.atomic.AtomicInteger(-1);
    // Records resign() calls per consumer name so a test can prove a terminated runner resigned.
    final java.util.concurrent.ConcurrentLinkedQueue<String> resigned =
        new java.util.concurrent.ConcurrentLinkedQueue<>();
    // When set, every resign() throws it AFTER recording — models a leadership implementation
    // breaking its no-throw contract, e.g. a wrapped DataSource throwing
    // IllegalStateException at exactly the moment the runner is shutting down.
    private volatile RuntimeException resignFailure;

    StubLeadership(boolean initial) {
      this.canAcquire = new AtomicBoolean(initial);
      this.lead = new AtomicBoolean(initial);
    }

    void failResignWith(RuntimeException failure) {
      this.resignFailure = failure;
    }

    void become(boolean value) {
      canAcquire.set(value);
      lead.set(value);
      leadForNextCalls.set(-1);
    }

    /**
     * Models "lease live when the batch starts, lost at the pre-commit re-check": {@code
     * tryAcquire} keeps returning a lease (so the runner enters processing) and {@code current}
     * returns a lease for the first {@code n} calls, then empty forever.
     */
    void leadThenLoseAfter(int n) {
      canAcquire.set(true);
      lead.set(false); // once the counter is exhausted, current() falls back to empty
      leadForNextCalls.set(n);
    }

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return canAcquire.get() ? LEASE : Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return isLeaderNow() ? LEASE : Optional.empty();
    }

    /** Boolean bookkeeping preserved from the pre-lease stub, now backing {@link #current}. */
    private boolean isLeaderNow() {
      int remaining = leadForNextCalls.get();
      if (remaining < 0) {
        return lead.get();
      }
      return leadForNextCalls.getAndDecrement() > 0;
    }

    @Override
    public void resign(String consumerName) {
      resigned.add(consumerName);
      RuntimeException failure = resignFailure;
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public void close() {}
  }

  private static EventStore storeWith(int count) {
    var events = new java.util.ArrayList<EventEnvelope>();
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    for (int i = 0; i < count; i++) {
      events.add(evt(base.plusSeconds(i)));
    }
    return SimpleTestEventStore.of(events);
  }

  // ==================== Continuous runner ====================

  @Test
  void continuousStandby_advancesNothing_thenResumesWhenLeader() throws Exception {
    ProjectionName name = ProjectionName.of("cont-standby");
    EventStore store = storeWith(3);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    StubLeadership leadership = new StubLeadership(false); // start as non-leader

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));

    // As a non-leader it must sit in STANDBY and process/advance nothing.
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> continuousRunner.state() == ProjectionState.STANDBY);
    Thread.sleep(200);
    assertThat(processed).isEmpty();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());

    // Flip to leader → processing resumes and the offset advances.
    leadership.become(true);
    Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> processed.size() == 3);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(3));

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void continuousRunner_leadershipLostBeforeCommit_abandonsBatchNoCommit() throws Exception {
    ProjectionName name = ProjectionName.of("cont-lost");
    EventStore store = storeWith(3);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    // tryAcquire → present (enter processing); current → present once (top of catch-up), then empty
    // at the pre-commit re-check inside processBatch, so the batch is abandoned without committing.
    StubLeadership leadership = new StubLeadership(true);
    leadership.leadThenLoseAfter(1);

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));

    // The pre-commit re-check fails → batch abandoned WITHOUT committing; runner drops to STANDBY.
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> continuousRunner.state() == ProjectionState.STANDBY);
    Thread.sleep(200);
    assertThat(processed).isEmpty(); // never committed
    assertThat(offsets.getLastOffset(name))
        .isEqualTo(GlobalOffset.initial()); // checkpoint unchanged

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void continuousCatchUpSkip_leadershipLostBeforeSkipSave_doesNotAdvanceCheckpoint()
      throws Exception {
    ProjectionName name = ProjectionName.of("cont-catchup-skip-lost");
    EventStore store = storeWith(1); // one poison event at offset 1
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();

    // Leader when the batch starts (top-of-loop check + pre-commit re-check both see true), then
    // the
    // projection throws AND flips leadership away right as it fails — so the SKIP re-check inside
    // handleBatchError must observe the lost lease and refuse to advance the checkpoint. Without
    // the
    // fix the SKIP path saves the skip offset unconditionally and the checkpoint jumps to 1.
    StubLeadership leadership = new StubLeadership(true);
    Projection poison =
        events -> {
          leadership.become(false); // lease stolen concurrently with this failing batch
          throw new RuntimeException("poison catch-up batch");
        };

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(org.streamrune.core.projection.ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> continuousRunner.run(name, poison, AT_LEAST_ONCE_IDEMPOTENT));

    // Losing leadership at the SKIP re-check abandons the batch WITHOUT committing → STANDBY.
    Awaitility.await()
        .atMost(Duration.ofSeconds(2))
        .until(() -> continuousRunner.state() == ProjectionState.STANDBY);
    Thread.sleep(200);
    assertThat(offsets.getLastOffset(name))
        .as("SKIP must not advance the checkpoint on a lost lease")
        .isEqualTo(GlobalOffset.initial());

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void continuousLiveSkip_leadershipLostBeforeSkip_doesNotAdvanceSubscriptionOffset()
      throws Exception {
    ProjectionName name = ProjectionName.of("cont-live-skip-lost");
    var eventStore = new InMemoryEventStore();
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();

    // Leader through catch-up (store empty → drains immediately) and into LIVE. When the poison
    // live
    // batch is processed the projection flips leadership away, so the runner's live SKIP path must
    // rethrow LeadershipLostException — leaving the subscription checkpoint un-advanced. Without
    // the
    // fix the SKIP path swallows the error and returns normally, so the subscription advances to 1.
    StubLeadership leadership = new StubLeadership(true);
    Projection poison =
        events -> {
          leadership.become(false); // lease stolen concurrently with this failing live batch
          throw new RuntimeException("poison live batch");
        };

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(org.streamrune.core.projection.ProjectionErrorStrategy.SKIP)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> continuousRunner.run(name, poison, AT_LEAST_ONCE_IDEMPOTENT));

    // Reach LIVE while still leader.
    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> continuousRunner.state() == ProjectionState.LIVE);

    // Append the poison event; the live SKIP path loses leadership and must NOT advance the offset.
    StreamId s = StreamId.of(TYPE, AggregateId.of("live-skip-lost-stream"));
    eventStore.append(s, List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))), Version.initial());

    Thread.sleep(300); // give the subscription time to poll, deliver, fail, and (wrongly) advance
    assertThat(offsets.getLastOffset(name))
        .as("live SKIP must not advance the subscription checkpoint on a lost lease")
        .isEqualTo(GlobalOffset.initial());

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void continuousNoopLeadership_processesImmediately() throws Exception {
    ProjectionName name = ProjectionName.of("cont-noop");
    EventStore store = storeWith(2);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    // Default (no .leadership()) is NOOP — always leader; behavior identical to pre-leadership.
    continuousRunner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));
    Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> processed.size() == 2);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(2));

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  // ==================== Scheduled runner ====================

  @Test
  void scheduledStandby_advancesNothing_thenResumesWhenLeader() {
    ProjectionName name = ProjectionName.of("sched-standby");
    EventStore store = storeWith(1);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    StubLeadership leadership = new StubLeadership(false); // non-leader

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .register("sched-standby", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // Let several 1-second ticks fire as a non-leader — each is skipped, so nothing is processed or
    // advanced. A non-leader never records a fire, so we wait on wall-clock, not on status.
    try {
      Thread.sleep(2500);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
    assertThat(processed).isEmpty();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());

    // Flip to leader → the next tick drains and advances.
    leadership.become(true);
    Awaitility.await().atMost(Duration.ofSeconds(4)).until(() -> processed.size() == 1);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(1));
  }

  // ==================== Resign on runner termination ====================

  @Test
  void continuousRunner_onTermination_resignsLeadership() throws Exception {
    ProjectionName name = ProjectionName.of("cont-resign");
    EventStore store = storeWith(1);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    StubLeadership leadership = new StubLeadership(true);

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));
    Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> processed.size() == 1);

    // Stopping the runner terminates its run loop; the finally block must resign leadership so a
    // standby takes over immediately rather than waiting for process/context shutdown.
    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    assertThat(leadership.resigned).contains(name.value());
  }

  @Test
  void scheduledRunner_haltTermination_resignsLeadership() {
    ProjectionName name = ProjectionName.of("sched-resign-halt");
    EventStore store = storeWith(1);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    Projection poison =
        batch -> {
          throw new RuntimeException("halt boom");
        };

    StubLeadership leadership = new StubLeadership(true);

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            // HALT now retries the same chunk up to maxTransientRetries before
            // halting;
            // shrink the bound + backoff so the terminal HALT (and the resign) is reached quickly.
            .maxTransientRetries(1)
            .drainRetryBackoff(Duration.ofMillis(2), Duration.ofMillis(10))
            .register(
                "sched-resign-halt",
                poison,
                "* * * * * *",
                org.streamrune.core.projection.ProjectionErrorStrategy.HALT,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // HALT sets ERROR and breaks the loop; the finally resigns leadership so a standby takes over.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> leadership.resigned.contains(name.value()));
    assertThat(scheduledRunner.status().get("sched-resign-halt").state())
        .isEqualTo(ScheduledProjectionState.ERROR);
  }

  // ============= Scheduled loses leadership mid-drain, stops advancing =============

  @Test
  void scheduledRunner_leadershipLostMidDrain_stopsAdvancingOffsets() {
    ProjectionName name = ProjectionName.of("sched-mid-drain-lost");
    // Two chunks of one event each (batchSize 1). The runner should advance the first chunk while
    // leader, then lose leadership before the second chunk's advance and stop — leaving the offset
    // at 1, not 2.
    EventStore store = storeWith(2);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    // isLeader call sequence within one tick's drain (batchSize=1, fetchSize=10, 2 events):
    //   1: outer-loop top check (before read)              -> true
    //   2: chunk-1 pre-advance check                       -> true  (advances to offset 1)
    //   3: chunk-2 pre-advance check                       -> false (abandon WITHOUT advancing)
    // chunk-2's projection.process() runs before its pre-advance check, so the read model may be
    // touched twice — but the OFFSET must not advance past 1 (at-least-once re-processing is safe;
    // a stale leader advancing the checkpoint is not). tryAcquire returns a lease independently
    // so the drain is entered.
    StubLeadership leadership = new StubLeadership(true);
    leadership.leadThenLoseAfter(2);

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(1)
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .register("sched-mid-drain-lost", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // The first chunk advances to offset 1; the second chunk's pre-advance leadership check fails,
    // so the drain stops and the offset must NOT reach 2.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(name).equals(GlobalOffset.of(1)));
    // Give a couple more ticks a chance to (wrongly) advance further; leadership stays lost.
    try {
      Thread.sleep(2500);
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
    }
    assertThat(offsets.getLastOffset(name))
        .as("a stale leader must not advance past the chunk it lost the lease on")
        .isEqualTo(GlobalOffset.of(1));
  }

  // ============= Per-chunk transient budget reset after a dead-letter =============

  @Test
  void scheduledRunner_dlq_secondChunkGetsOwnTransientBudget_notImmediateDeadLetter() {
    ProjectionName name = ProjectionName.of("sched-dlq-budget");
    // Two chunks (batchSize 1). Chunk 1 fails TRANSIENT forever → exhausts the (test-shrunk) retry
    // bound and is dead-lettered + advanced. Chunk 2 then fails TRANSIENT exactly once and
    // recovers.
    // With the bug, chunk 2 inherits chunk 1's exhausted counter and is dead-lettered on its FIRST
    // failure. With the fix, the per-chunk budget resets after chunk 1's dead-letter, so chunk 2
    // gets its own full budget, recovers, and is processed cleanly — ONLY chunk 1 is dead-lettered.
    //
    // The retry bound and backoff are shrunk via package-private test knobs so the tick-level
    // exhaustion that reproduces the defect runs in well under a second instead of minutes.
    EventStore store = storeWith(2);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var dlq = new CountingDlq();

    var chunk2FailedOnce = new AtomicBoolean(false);
    Projection p =
        batch -> {
          long off = batch.get(0).globalOffset().value();
          if (off == 1L) {
            throw new RuntimeException("chunk1 stuck transient"); // never recovers
          }
          // chunk 2 (offset 2): fail exactly once, then succeed on the next attempt.
          if (chunk2FailedOnce.compareAndSet(false, true)) {
            throw new RuntimeException("chunk2 transient once");
          }
          // success on retry
        };

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(1)
            .deadLetterStore(dlq)
            // Every error is TRANSIENT so the retry-budget logic (not the POISON fast-path)
            // governs.
            .classifier(e -> org.streamrune.core.projection.ProjectionErrorClass.TRANSIENT)
            .leadership(SubscriptionLeadership.NOOP)
            .maxTransientRetries(3)
            .drainRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .register(
                "sched-dlq-budget",
                p,
                "* * * * * *",
                org.streamrune.core.projection.ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // Eventually chunk 2 is processed cleanly (offset reaches 2) after chunk 1 is dead-lettered.
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> offsets.getLastOffset(name).equals(GlobalOffset.of(2)));

    assertThat(dlq.count.get())
        .as("only the genuinely-stuck chunk 1 is dead-lettered; chunk 2 keeps its own budget")
        .isEqualTo(1);
  }

  /** Minimal counting dead-letter store for the per-chunk budget tests. */
  private static final class CountingDlq
      implements org.streamrune.core.projection.ProjectionDeadLetterStore {
    final java.util.concurrent.atomic.AtomicInteger count =
        new java.util.concurrent.atomic.AtomicInteger(0);

    @Override
    public void save(org.streamrune.core.projection.ProjectionDeadLetterEntry entry) {
      count.incrementAndGet();
    }

    @Override
    public List<org.streamrune.core.projection.ProjectionDeadLetterEntry> read(
        ProjectionName projectionName, int limit) {
      return List.of();
    }

    @Override
    public List<org.streamrune.core.projection.ProjectionDeadLetterEntry> readAll(int limit) {
      return List.of();
    }

    @Override
    public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {}
  }

  // ===== Per-chunk transient budget must NOT be seeded from the tick counter
  // =====

  @Test
  void scheduledRunner_dlq_secondChunkNeedingReDrain_keepsOwnBudget_notDeadLettered() {
    // Facet 6a (cross-drain re-seed). Chunk 1 fails TRANSIENT forever → it exhausts the retry
    // bound,
    // is dead-lettered, and the offset advances past it. Chunk 2 then fails TRANSIENT >= 2 times
    // (so it genuinely needs a RE-DRAIN — unlike the older test where chunk 2 failed exactly once
    // in
    // the same pass) and finally succeeds. With the bug, the per-chunk budget is re-seeded from the
    // tick-level consecutiveFailures on every re-drain, so chunk 2's first re-drain failure already
    // sees a count >= bound and is dead-lettered on its FIRST failure → dlqCount == 2. With the fix
    // the budget is per-chunk and survives re-drains for the SAME chunk, resetting on advance, so
    // ONLY chunk 1 is dead-lettered.
    ProjectionName name = ProjectionName.of("sched-dlq-budget-redrain");
    EventStore store = storeWith(2);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var dlq = new CountingDlq();

    var chunk2Failures = new java.util.concurrent.atomic.AtomicInteger(0);
    Projection p =
        batch -> {
          long off = batch.get(0).globalOffset().value();
          if (off == 1L) {
            throw new RuntimeException("chunk1 stuck transient"); // never recovers
          }
          // chunk 2 (offset 2): fail TRANSIENT the first TWO attempts, then succeed. Two failures
          // guarantee at least one re-drain of chunk 2 after chunk 1 was dead-lettered.
          if (chunk2Failures.getAndIncrement() < 2) {
            throw new RuntimeException("chunk2 transient (needs re-drain)");
          }
          // success on the third attempt
        };

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(1)
            .deadLetterStore(dlq)
            .classifier(e -> org.streamrune.core.projection.ProjectionErrorClass.TRANSIENT)
            .leadership(SubscriptionLeadership.NOOP)
            .maxTransientRetries(3)
            .drainRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .register(
                "sched-dlq-budget-redrain",
                p,
                "* * * * * *",
                org.streamrune.core.projection.ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // Chunk 2 eventually succeeds and the offset reaches 2 after chunk 1 is dead-lettered.
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> offsets.getLastOffset(name).equals(GlobalOffset.of(2)));

    assertThat(dlq.count.get())
        .as("only the genuinely-stuck chunk 1 is dead-lettered; chunk 2 keeps its own budget")
        .isEqualTo(1);
  }

  @Test
  void scheduledRunner_dlq_readOutageDoesNotConsumeFirstChunkTransientBudget() {
    // Facet 6b (non-chunk seed inflation). A readGlobalStream outage produces >=
    // maxTransientRetries
    // WHOLE-TICK failures (before the chunk loop even runs), driving the tick-level counter high.
    // Then the store recovers and the first chunk fails TRANSIENT exactly ONCE before succeeding.
    // With the bug the first chunk's budget is seeded from the inflated tick counter (>= bound) and
    // it is dead-lettered on its FIRST failure — a permanent read-model hole. With the fix the read
    // outage only drives the tick backoff and never touches any chunk's transient budget, so the
    // first chunk gets its own full budget, recovers, and advances with NO dead-letter.
    ProjectionName name = ProjectionName.of("sched-dlq-read-outage");
    EventStore delegate = storeWith(1);
    // maxTransientRetries is shrunk to 3; make the read fail 4 times (> bound) before recovering.
    var readFailuresLeft = new java.util.concurrent.atomic.AtomicInteger(4);
    EventStore flakyStore =
        new EventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            if (readFailuresLeft.getAndDecrement() > 0) {
              throw new RuntimeException("read outage"); // whole-tick infra failure
            }
            return delegate.readGlobalStream(afterOffset, maxCount);
          }

          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, org.streamrune.core.AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var dlq = new CountingDlq();

    var chunkFailedOnce = new AtomicBoolean(false);
    Projection p =
        batch -> {
          // The single chunk fails TRANSIENT exactly once, then succeeds.
          if (chunkFailedOnce.compareAndSet(false, true)) {
            throw new RuntimeException("chunk transient once");
          }
        };

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(flakyStore)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(1)
            .deadLetterStore(dlq)
            .classifier(e -> org.streamrune.core.projection.ProjectionErrorClass.TRANSIENT)
            .leadership(SubscriptionLeadership.NOOP)
            .maxTransientRetries(3)
            .drainRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .register(
                "sched-dlq-read-outage",
                p,
                "* * * * * *",
                org.streamrune.core.projection.ProjectionErrorStrategy.DLQ,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    // After the read outage clears, the chunk recovers on its second attempt and the offset
    // advances — with NO dead-letter (it was given its own full transient budget).
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> offsets.getLastOffset(name).equals(GlobalOffset.of(1)));

    assertThat(dlq.count.get())
        .as("a read outage must not consume the first chunk's transient budget")
        .isZero();
  }

  @Test
  void scheduledNoopLeadership_drainsImmediately() {
    ProjectionName name = ProjectionName.of("sched-noop");
    EventStore store = storeWith(1);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    // Default NOOP leadership — always leader.
    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .register("sched-noop", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> processed.size() == 1);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(1));
  }

  // ========== Runner shutdown bookkeeping survives a throwing resign ==========

  @Test
  void continuousRunner_throwingResign_cleanStopStillReachesStoppedAndCloseReturns()
      throws Exception {
    // Repro (clean-stop shape): run()'s finally executes running.set(false) → resign →
    // state/health bookkeeping → runThread.set(null) → done.countDown(). A resign that throws
    // unchecked (a wrapped DataSource dying at exactly the moment the runner shuts down) truncated
    // the finally: the runner never reached STOPPED, the completion latch never counted down (a
    // subsequent close() blocked its full 30 s join timeout), and the throw escaped run() on the
    // runner thread. The finally must complete its bookkeeping regardless of what resign does.
    ProjectionName name = ProjectionName.of("cont-resign-throw-clean");
    EventStore store = storeWith(0);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    StubLeadership leadership = new StubLeadership(true);
    leadership.failResignWith(new IllegalStateException("wrapped DataSource torn down"));

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .build();

    var escaped = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    continuousRunner.run(name, b -> {}, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    escaped.set(e);
                  }
                });
    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> continuousRunner.state() == ProjectionState.LIVE);

    continuousRunner.requestStop();
    t.interrupt();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertThat(t.isAlive()).isFalse();

    assertThat(leadership.resigned).contains(name.value()); // resign WAS attempted
    assertThat(continuousRunner.state())
        .as("a throwing resign must not skip the STOPPED transition")
        .isEqualTo(ProjectionState.STOPPED);
    assertThat(escaped.get())
        .as("a clean stop must not let the resign failure escape run()")
        .isNull();

    // The completion latch must have counted down: close() returns promptly instead of blocking
    // its full 30 s join timeout against a latch nobody will ever count.
    long closeStart = System.nanoTime();
    continuousRunner.close();
    assertThat(Duration.ofNanos(System.nanoTime() - closeStart))
        .as("close() must not hang on the skipped completion latch")
        .isLessThan(Duration.ofSeconds(5));
  }

  @Test
  void continuousRunner_throwingResign_errorExitStillMarksHealthTerminallyDown() throws Exception {
    // Repro (permanent-ERROR shape — the headline harm): a catch-up read-poison HALT
    // exits run() through the ERROR branch, whose finally must call markTerminalError so /health
    // reports DOWN while the read model is permanently frozen (the terminal-DOWN guarantee).
    // A throwing resign sat BEFORE that call in the finally, so the health marking was skipped —
    // for a catch-up halt no health entry exists at all, so overallStatus() stayed UP forever —
    // and the resign failure replaced the real poison as the thread's terminal throwable.
    ProjectionName name = ProjectionName.of("cont-resign-throw-error");
    EventStore poisonReadStore =
        new EventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            // UncheckedIOException is classified as a deterministic read poison.
            throw new java.io.UncheckedIOException(
                new java.io.IOException("corrupt payload — unreadable forever"));
          }

          @Override
          public org.streamrune.core.AggregateHistory load(StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(
              StreamId streamId, Version version, org.streamrune.core.AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readStream(
              StreamId streamId, Version afterVersion, int maxCount) {
            throw new UnsupportedOperationException();
          }
        };
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var contributor = new SubscriptionHealthContributor(poisonReadStore, offsets);
    StubLeadership leadership = new StubLeadership(true);
    leadership.failResignWith(new IllegalStateException("wrapped DataSource torn down"));

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(poisonReadStore)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .maxReadPoisonRetries(1)
            .build();

    var escaped = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    continuousRunner.run(name, b -> {}, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable e) {
                    escaped.set(e);
                  }
                });
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertThat(t.isAlive()).isFalse();

    assertThat(continuousRunner.state()).isEqualTo(ProjectionState.ERROR);
    assertThat(contributor.overallStatus())
        .as("a throwing resign must not skip terminal-DOWN health marking on an ERROR exit")
        .isEqualTo(org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN);
    assertThat(escaped.get())
        .as("the real poison must stay the terminal throwable, not be replaced by the resign blip")
        .isInstanceOf(ProjectionReadPoisonException.class);
  }

  @Test
  void scheduledRunner_throwingResign_stopStillMarksStopped() {
    // Repro (scheduled shape): runLoop's finally resigns, then marks STOPPED (or terminal
    // ERROR health). A throwing resign truncated the finally — the projection stayed SLEEPING in
    // the status view although its thread was dead, mislabeling the death.
    StubLeadership leadership = new StubLeadership(true);
    leadership.failResignWith(new IllegalStateException("wrapped DataSource torn down"));

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWith(1))
            .offsetStore(new InMemoryOffsetStore())
            .fetchSize(10)
            .batchSize(10)
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .register("sched-resign-throw", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();
    scheduledRunner.stop();

    assertThat(leadership.resigned).contains("sched-resign-throw"); // resign WAS attempted
    assertThat(scheduledRunner.status().get("sched-resign-throw").state())
        .as("a throwing resign must not skip the STOPPED transition")
        .isEqualTo(ScheduledProjectionState.STOPPED);
  }

  // ==================== Epoch regression at the takeover stamp ====================

  /**
   * A fencing processor whose takeover stamp reports that the checkpoint row already carries a
   * HIGHER epoch than the one leadership handed out — the shape produced by {@code DELETE FROM
   * subscription_leases} against a live {@code projection_offset.epoch}.
   */
  private static AtomicBatchProcessor regressingStampProcessor(long storedEpoch) {
    return new AtomicBatchProcessor() {
      @Override
      public boolean supportsFencing() {
        return true;
      }

      @Override
      public void executeAtomically(
          ProjectionName projectionName,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater projectionUpdater,
          OffsetStore offsetStore) {
        // Never reached once the stamp fails closed; if it ever is, behave like the real fence.
        throw new ProjectionCommitFencedException("stale epoch " + fencingEpoch);
      }

      @Override
      public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
        throw new ProjectionEpochRegressionException(projectionName, fencingEpoch, storedEpoch);
      }
    };
  }

  @Test
  void continuousRunner_stampBelowStoredEpoch_terminatesLoudlyInsteadOfLoopingFenced()
      throws Exception {
    // The stamp used to discard its row count, so "I am below the stored epoch" was
    // indistinguishable from "already stamped": the runner reported the takeover armed, went
    // CATCHING_UP, had every commit rejected by the epoch fence, classified that rejection as a
    // benign leadership/CAS signal, re-checked leadership (still legitimately held), went LIVE and
    // looped forever — read model frozen, INFO-level logs, state LIVE, health UP. The runner must
    // instead stop, report ERROR, and surface the two epochs.
    ProjectionName name = ProjectionName.of("cont-epoch-regression");
    EventStore store = storeWith(3);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    StubLeadership leadership = new StubLeadership(true); // we DO hold the lease, at epoch 1

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(regressingStampProcessor(57L))
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));

    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> continuousRunner.state() == ProjectionState.ERROR);
    assertThat(processed).as("nothing may be processed under a non-authoritative epoch").isEmpty();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
    assertThat(continuousRunner.lastError())
        .as("the terminal error must name both epochs so an operator can act")
        .contains("epoch 1")
        .contains("epoch 57");
    // The run loop exited, so leadership was resigned — a standby replica may try (and will hit the
    // same durable inconsistency, equally loudly).
    assertThat(leadership.resigned).contains(name.value());
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void scheduledRunner_stampBelowStoredEpoch_terminatesLoudlyInsteadOfSkippingTicksForever()
      throws Exception {
    // Same defect on the cron sibling: it skipped the tick and re-tried on every fire, so the read
    // model froze at cron cadence with the projection reported healthy.
    ProjectionName name = ProjectionName.of("sched-epoch-regression");
    EventStore store = storeWith(3);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();

    StubLeadership leadership = new StubLeadership(true);

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .leadership(leadership)
            .atomicProcessor(regressingStampProcessor(57L))
            .register(
                name.value(),
                (Projection) processed::addAll,
                "* * * * * *",
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                scheduledRunner.status().get(name.value()).state()
                    == ScheduledProjectionState.ERROR);
    assertThat(processed).isEmpty();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());
    assertThat(scheduledRunner.status().get(name.value()).lastError())
        .contains("epoch 1")
        .contains("epoch 57");
  }

  // ============ An epoch regression observed AFTER supersession is benign ==========
  //
  // ProjectionEpochRegressionException was made FATAL without asking whether this runner is
  // still the leader. A replica that stalls between tryAcquire and the stamp (a GC pause, a
  // HikariCP getConnection wait past the lease TTL) is legitimately superseded: the winner stamps a
  // HIGHER epoch, so the straggler's stamp regresses — exactly the race the fence exists to handle.
  // Declaring that FATAL terminates a HEALTHY replica with health DOWN and removes it from the
  // failover pool for good. The discriminator is leadership: superseded -> STANDBY (the response
  // the runner already takes when tryAcquire fails one instruction earlier); a regression observed
  // while this runner still holds its lease -> the lease/checkpoint divergence -> FATAL.

  /**
   * Models the supersession race: {@code tryAcquire} hands out a lease (the runner enters the
   * stamp) but {@code current} reports empty from the first call on — this replica was superseded
   * while it stalled, which is what made the stored epoch higher in the first place.
   */
  private static SubscriptionLeadership supersededAfterAcquire() {
    StubLeadership leadership = new StubLeadership(true);
    leadership.leadThenLoseAfter(0);
    return leadership;
  }

  /**
   * {@link #regressingStampProcessor} plus a stamp-attempt counter. Reaching a SECOND attempt is
   * the observable proof that the runner survived the first regression and came back round its
   * standby loop — the current FATAL classification breaks out after exactly one.
   */
  private static AtomicBatchProcessor countingRegressingStampProcessor(
      long storedEpoch, java.util.concurrent.atomic.AtomicInteger attempts) {
    AtomicBatchProcessor delegate = regressingStampProcessor(storedEpoch);
    return new AtomicBatchProcessor() {
      @Override
      public boolean supportsFencing() {
        return delegate.supportsFencing();
      }

      @Override
      public void executeAtomically(
          ProjectionName projectionName,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater projectionUpdater,
          OffsetStore offsetStore) {
        delegate.executeAtomically(
            projectionName, batch, newOffset, fencingEpoch, projectionUpdater, offsetStore);
      }

      @Override
      public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
        attempts.incrementAndGet();
        delegate.stampFencingEpoch(projectionName, fencingEpoch);
      }
    };
  }

  @Test
  void continuousRunner_epochRegressionAfterSupersession_standsByInsteadOfTerminating()
      throws Exception {
    ProjectionName name = ProjectionName.of("cont-epoch-regression-superseded");
    EventStore store = storeWith(3);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    var contributor = new SubscriptionHealthContributor(store, offsets);
    var attempts = new java.util.concurrent.atomic.AtomicInteger();

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(supersededAfterAcquire())
            .atomicProcessor(countingRegressingStampProcessor(57L, attempts))
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> continuousRunner.run(name, processed::addAll, AT_LEAST_ONCE_IDEMPOTENT));
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> attempts.get() >= 3 || continuousRunner.state() == ProjectionState.ERROR);

    assertThat(continuousRunner.state())
        .as("a superseded replica must stand by, not terminate")
        .isNotEqualTo(ProjectionState.ERROR);
    assertThat(continuousRunner.lastError())
        .as("a benign supersession is not a terminal error")
        .isNull();
    assertThat(contributor.overallStatus())
        .as("a healthy standby must not report DOWN — it is still in the failover pool")
        .isNotEqualTo(org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN);
    assertThat(processed).as("nothing may be processed without an armed fence").isEmpty();

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void scheduledRunner_epochRegressionAfterSupersession_sleepsInsteadOfTerminating() {
    ProjectionName name = ProjectionName.of("sched-epoch-regression-superseded");
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    var attempts = new java.util.concurrent.atomic.AtomicInteger();

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWith(3))
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .leadership(supersededAfterAcquire())
            .atomicProcessor(countingRegressingStampProcessor(57L, attempts))
            .register(
                name.value(),
                (Projection) processed::addAll,
                "* * * * * *",
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(8))
        .until(
            () ->
                attempts.get() >= 2
                    || scheduledRunner.status().get(name.value()).state()
                        == ScheduledProjectionState.ERROR);
    assertThat(scheduledRunner.status().get(name.value()).state())
        .as("a superseded tick must sleep, not terminate the registration")
        .isNotEqualTo(ScheduledProjectionState.ERROR);
    assertThat(scheduledRunner.status().get(name.value()).lastError()).isNull();
    assertThat(processed).isEmpty();
  }

  /**
   * The leadership re-check is a diagnosis, not a safety gate: if it cannot answer, the loud
   * verdict must stand. A {@code current()} that throws must therefore still terminate the runner.
   */
  @Test
  void continuousRunner_epochRegressionWithUnanswerableLeadership_staysFatal() throws Exception {
    ProjectionName name = ProjectionName.of("cont-epoch-regression-blind");
    SubscriptionLeadership blind =
        new SubscriptionLeadership() {
          @Override
          public Optional<SubscriptionLeadership.Lease> tryAcquire(String consumerName) {
            return Optional.of(new SubscriptionLeadership.Lease(1L));
          }

          @Override
          public Optional<SubscriptionLeadership.Lease> current(String consumerName) {
            throw new IllegalStateException("lease backend unreachable");
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(storeWith(3))
            .offsetStore(new InMemoryOffsetStore())
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(blind)
            .atomicProcessor(regressingStampProcessor(57L))
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> continuousRunner.run(name, b -> {}, AT_LEAST_ONCE_IDEMPOTENT));
    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> continuousRunner.state() == ProjectionState.ERROR);
    assertThat(continuousRunner.lastError()).contains("epoch 57");
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  @Test
  void scheduledRunner_epochRegressionWithUnanswerableLeadership_staysFatal() {
    ProjectionName name = ProjectionName.of("sched-epoch-regression-blind");
    SubscriptionLeadership blind =
        new SubscriptionLeadership() {
          @Override
          public Optional<SubscriptionLeadership.Lease> tryAcquire(String consumerName) {
            return Optional.of(new SubscriptionLeadership.Lease(1L));
          }

          @Override
          public Optional<SubscriptionLeadership.Lease> current(String consumerName) {
            throw new IllegalStateException("lease backend unreachable");
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWith(3))
            .offsetStore(new InMemoryOffsetStore())
            .fetchSize(10)
            .batchSize(10)
            .leadership(blind)
            .atomicProcessor(regressingStampProcessor(57L))
            .register(name.value(), (Projection) b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                scheduledRunner.status().get(name.value()).state()
                    == ScheduledProjectionState.ERROR);
    assertThat(scheduledRunner.status().get(name.value()).lastError()).contains("epoch 57");
  }

  // ==================== Runner composed with a VARYING epoch ====================
  //
  // Every runner-level leadership test in this repo used a hand-rolled SubscriptionLeadership that
  // hands out a CONSTANT epoch (this file's StubLeadership: new Lease(1L);
  // ContinuousProjectionRunnerTest: 7 and 9; ScheduledProjectionRunnerTest likewise), and every
  // real-lease test (LeaseBasedLeadershipTest, LeaseLeadershipPgBouncerTxModeIT,
  // TwoRunnerLeadershipFailoverIT) drives no runner at all. So the one thing the takeover
  // stamp exists to prevent — a runner threading a STALE epoch into a later commit, e.g. by caching
  // the epoch from its first awaitLeadership across a re-acquisition — would pass the entire suite.
  // These two tests compose a runner with a leadership whose epoch actually MOVES and pin that
  // every stamp and every commit after a re-acquisition carries the NEW epoch.

  /** Leadership whose epoch increments on every fresh acquisition, like a real lease takeover. */
  private static final class EpochIncrementingLeadership implements SubscriptionLeadership {
    private final java.util.concurrent.atomic.AtomicLong nextEpoch =
        new java.util.concurrent.atomic.AtomicLong();
    private final AtomicBoolean available = new AtomicBoolean(true);
    private final java.util.concurrent.atomic.AtomicReference<Lease> held =
        new java.util.concurrent.atomic.AtomicReference<>();

    /**
     * Models the lease being lost: current() reports empty and the next acquire bumps the epoch.
     */
    void loseLease() {
      held.set(null);
      available.set(false);
    }

    void allowReacquire() {
      available.set(true);
    }

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      if (!available.get()) {
        return Optional.empty();
      }
      Lease current = held.get();
      if (current == null) {
        current = new Lease(nextEpoch.incrementAndGet());
        held.set(current);
      }
      return Optional.of(current);
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.ofNullable(held.get());
    }

    @Override
    public void resign(String consumerName) {
      held.set(null);
    }

    @Override
    public void close() {}
  }

  /** Records the epoch threaded into every commit and every takeover stamp. */
  private static final class EpochRecordingProcessor implements AtomicBatchProcessor {
    final java.util.concurrent.ConcurrentLinkedQueue<Long> committedEpochs =
        new java.util.concurrent.ConcurrentLinkedQueue<>();
    final java.util.concurrent.ConcurrentLinkedQueue<Long> stampedEpochs =
        new java.util.concurrent.ConcurrentLinkedQueue<>();

    @Override
    public boolean supportsFencing() {
      return true;
    }

    @Override
    public void executeAtomically(
        ProjectionName projectionName,
        List<EventEnvelope> batch,
        GlobalOffset newOffset,
        long fencingEpoch,
        ProjectionUpdater projectionUpdater,
        OffsetStore offsetStore) {
      committedEpochs.add(fencingEpoch);
      projectionUpdater.update(null);
      offsetStore.saveOffset(projectionName, newOffset);
    }

    @Override
    public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
      stampedEpochs.add(fencingEpoch);
    }
  }

  @Test
  void continuousRunner_afterReacquisition_threadsTheNEWEpochIntoStampAndCommits()
      throws Exception {
    ProjectionName name = ProjectionName.of("cont-epoch-moves");
    EventStore store = storeWith(6);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    var leadership = new EpochIncrementingLeadership();
    var processor = new EpochRecordingProcessor();

    continuousRunner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(10)))
            .leadership(leadership)
            .atomicProcessor(processor)
            .build();

    Thread t =
        Thread.ofVirtual().start(() -> continuousRunner.run(name, p, AT_LEAST_ONCE_IDEMPOTENT));

    // Epoch 1: catch up the whole stream.
    Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> processed.size() == 6);
    assertThat(processor.stampedEpochs).as("the takeover stamp carries epoch 1").contains(1L);
    assertThat(processor.committedEpochs)
        .as("every epoch-1 commit carries epoch 1")
        .allSatisfy(e -> assertThat(e).isEqualTo(1L));

    // Lose the lease, then hand it back: a real takeover issues a STRICTLY GREATER epoch.
    leadership.loseLease();
    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> continuousRunner.state() == ProjectionState.STANDBY);
    processor.stampedEpochs.clear();
    processor.committedEpochs.clear();
    offsets.reset(name); // rewind so the new epoch has something to commit
    leadership.allowReacquire();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> !processor.stampedEpochs.isEmpty() && !processor.committedEpochs.isEmpty());

    assertThat(processor.stampedEpochs)
        .as("the takeover stamp after re-acquisition must carry the NEW epoch, never a cached one")
        .allSatisfy(e -> assertThat(e).isEqualTo(2L));
    assertThat(processor.committedEpochs)
        .as("every commit after re-acquisition must carry the NEW epoch")
        .allSatisfy(e -> assertThat(e).isEqualTo(2L));

    continuousRunner.close();
    t.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void scheduledRunner_afterReacquisition_threadsTheNEWEpochIntoStampAndCommits() throws Exception {
    ProjectionName name = ProjectionName.of("sched-epoch-moves");
    EventStore store = storeWith(4);
    InMemoryOffsetStore offsets = new InMemoryOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();

    var leadership = new EpochIncrementingLeadership();
    var processor = new EpochRecordingProcessor();

    scheduledRunner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .fetchSize(10)
            .batchSize(10)
            .leadership(leadership)
            .atomicProcessor(processor)
            .register(
                name.value(),
                (Projection) processed::addAll,
                "* * * * * *",
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    scheduledRunner.start();

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> processed.size() == 4);
    assertThat(processor.committedEpochs).allSatisfy(e -> assertThat(e).isEqualTo(1L));

    leadership.loseLease();
    processor.stampedEpochs.clear();
    processor.committedEpochs.clear();
    offsets.reset(name);
    leadership.allowReacquire();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> !processor.stampedEpochs.isEmpty() && !processor.committedEpochs.isEmpty());

    assertThat(processor.stampedEpochs)
        .as("every tick after re-acquisition stamps the NEW epoch")
        .allSatisfy(e -> assertThat(e).isEqualTo(2L));
    assertThat(processor.committedEpochs)
        .as("every commit after re-acquisition carries the NEW epoch")
        .allSatisfy(e -> assertThat(e).isEqualTo(2L));
  }
}

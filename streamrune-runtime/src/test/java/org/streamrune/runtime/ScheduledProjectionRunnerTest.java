package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;
import static org.streamrune.runtime.WindowedProjectionTestSupport.evt;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ScheduledProjectionRunnerTest {

  private ScheduledProjectionRunner runner;

  @AfterEach
  void tearDown() {
    if (runner != null) {
      runner.close();
    }
  }

  @Test
  void builder_invalidCron_throwsAtRegister() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .register("p", b -> {}, "not a cron", AT_LEAST_ONCE_IDEMPOTENT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid cron");
  }

  @Test
  void builder_blankCron_throwsAtRegister() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .register("p", b -> {}, "", AT_LEAST_ONCE_IDEMPOTENT))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void builder_duplicateName_throwsAtBuild() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .register("p", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .register("p", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate");
  }

  /**
   * build() asks the processor about every registered name, so a name it cannot store read models
   * under fails at startup instead of on the projection's first scheduled drain.
   */
  @Test
  void builder_refusesANameTheProcessorCannotStore_atBuild() {
    var processor = new NameRuleProcessor();
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .atomicProcessor(processor)
                    .register("orders", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .register("order-summary", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("order-summary");
    assertThat(processor.checked)
        .containsExactly(ProjectionName.of("orders"), ProjectionName.of("order-summary"));
    assertThat(processor.commits.get()).isZero();
  }

  @Test
  void builder_dlqWithoutStore_throwsAtBuild() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .register(
                        "p",
                        b -> {},
                        "* * * * * *",
                        ProjectionErrorStrategy.DLQ,
                        AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("deadLetterStore");
  }

  @Test
  void builder_emptyRegistrations_throwsAtBuild() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one");
  }

  // A fencing-claiming processor that hands null to the updater: a plain projection declared
  // AT_LEAST_ONCE_IDEMPOTENT runs under it; a write-through projection declared TRANSACTIONAL_LOCAL
  // is accepted as declared (it exposes no write target). Every registration shares this runner's
  // single atomicProcessor, so build() checks each one against its own declared mode.
  private static AtomicBatchProcessor transactionalProcessor() {
    return new AtomicBatchProcessor() {
      @Override
      public boolean supportsFencing() {
        return true;
      }

      @Override
      public void executeAtomically(
          ProjectionName pn,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater updater,
          OffsetStore os) {
        updater.update(null);
        os.saveOffset(pn, newOffset);
      }
    };
  }

  /**
   * {@code build()} refuses a {@code TRANSACTIONAL_LOCAL} lambda under an in-memory transactional
   * repository; the same lambda declared {@code AT_LEAST_ONCE_IDEMPOTENT} builds.
   */
  @Test
  void build_refusesATransactionalLocalLambda_andBuildsTheSameLambdaAtLeastOnce() {
    Projection lambda = batch -> {};
    var repository = new InMemoryProjectionRepository();
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .eventStore(emptyStore())
                    .offsetStore(repository)
                    .atomicProcessor(repository)
                    .register("non-write-through", lambda, "* * * * * *", TRANSACTIONAL_LOCAL)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ScheduledProjectionRunner")
        .hasMessageContaining("'non-write-through'")
        .hasMessageContaining("TRANSACTIONAL_LOCAL")
        .hasMessageContaining("does not write through the handed repository");

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(emptyStore())
            .offsetStore(repository)
            .atomicProcessor(repository)
            .register("non-write-through", lambda, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    assertThat(runner).isNotNull();
  }

  @Test
  void buildRefusesAMissingProcessor_namingBothWaysOut() {
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .register("orders", batch -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "atomicProcessor is required: pass the JdbcProjectionRepository your TRANSACTIONAL_LOCAL"
                + " projections write to, or AtomicBatchProcessor.nonAtomicAtLeastOnce() for a"
                + " runner of AT_LEAST_ONCE_IDEMPOTENT projections");
  }

  @Test
  void atomicProcessorNullIsRefusedAtTheSetter() {
    assertThatThrownBy(() -> ScheduledProjectionRunner.builder().atomicProcessor(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "atomicProcessor cannot be null: pass the JdbcProjectionRepository your"
                + " TRANSACTIONAL_LOCAL projections write to, or"
                + " AtomicBatchProcessor.nonAtomicAtLeastOnce()");
  }

  @Test
  void build_acceptsAPlainProjectionDeclaredAtLeastOnce_underAFencingProcessor() {
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection nonWriteThrough = batch -> processed.addAll(batch);

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(transactionalProcessor())
            .register("at-least-once", nonWriteThrough, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> processed.size() == 1);
  }

  /** Overrides the 2-arg overload, so {@code writesThroughRepository()} reports {@code true}. */
  private static final class WriteThroughProjection implements Projection {
    final ConcurrentLinkedQueue<EventEnvelope> processed = new ConcurrentLinkedQueue<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      processed.addAll(batch);
    }

    @Override
    public void process(
        List<EventEnvelope> batch, org.streamrune.core.projection.ProjectionRepository repository) {
      process(batch);
    }
  }

  // The mode is PER REGISTRATION on ScheduledProjectionRunner too: a plain at-least-once lambda and
  // a transactional write-through projection share one runner, each checked against its own
  // declaration.
  @Test
  void build_checksEachRegistrationAgainstItsOwnMode() {
    var store =
        listStore(
            List.of(
                evt(Instant.parse("2026-01-01T00:00:00Z")),
                evt(Instant.parse("2026-01-01T00:00:01Z"))));
    var offsets = memOffsetStore();
    var atLeastOnceProcessed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection atLeastOnceLambda = batch -> atLeastOnceProcessed.addAll(batch);
    var writeThrough = new WriteThroughProjection();

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(transactionalProcessor())
            .register(
                "at-least-once",
                atLeastOnceLambda,
                "* * * * * *",
                ProjectionErrorStrategy.HALT,
                AT_LEAST_ONCE_IDEMPOTENT)
            .register("write-through", writeThrough, "* * * * * *", TRANSACTIONAL_LOCAL)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> !atLeastOnceProcessed.isEmpty() && !writeThrough.processed.isEmpty());
  }

  // The flip side: a sibling's AT_LEAST_ONCE_IDEMPOTENT never loosens the check for a registration
  // that declares TRANSACTIONAL_LOCAL on the same runner.
  @Test
  void build_aSiblingsAtLeastOnceNeverLoosensATransactionalRegistrationsCheck() {
    Projection atLeastOnce = batch -> {};
    Projection transactional = batch -> {};
    assertThatThrownBy(
            () ->
                ScheduledProjectionRunner.builder()
                    .eventStore(emptyStore())
                    .offsetStore(memOffsetStore())
                    .atomicProcessor(transactionalProcessor())
                    .register(
                        "at-least-once",
                        atLeastOnce,
                        "* * * * * *",
                        ProjectionErrorStrategy.HALT,
                        AT_LEAST_ONCE_IDEMPOTENT)
                    .register("transactional", transactional, "* * * * * *", TRANSACTIONAL_LOCAL)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'transactional'")
        .satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("'at-least-once'"));
  }

  @Test
  void cronFires_drainsToTail() {
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = batch -> processed.addAll(batch);

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .register("p", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> processed.size() == 1);

    assertThat(offsets.getLastOffset(ProjectionName.of("p"))).isEqualTo(GlobalOffset.of(1L));
  }

  @Test
  void drainRoutesEachChunkThroughAtomicCheckpoint() {
    // The OLD drain() called projection.process(chunk) then
    // offsetStore.saveOffset(...) as two non-transactional ops — a crash between them re-delivered
    // and DOUBLE-APPLIED the chunk, and there was no split-brain overlap guard. The fix routes
    // every
    // chunk through AtomicBatchProcessor.executeAtomically so the read-model write and the
    // checkpoint
    // commit as one guarded unit (exactly like Continuous/Polling). Here a guard-enforcing
    // processor
    // is the ONLY thing that applies the read model AND advances the offset: if the runner bypassed
    // it (old behavior) applyCount would stay 0 even though the offset advanced.
    var store =
        listStore(
            List.of(
                evt(Instant.parse("2026-01-01T00:00:00Z")),
                evt(Instant.parse("2026-01-01T00:00:01Z")))); // offsets 1, 2
    var offsets = memOffsetStore();
    var applyCount = new AtomicInteger(0);
    var reDeliveryApplies = new AtomicInteger(0);

    // Guard-enforcing atomic processor mirroring JdbcProjectionRepository: applies the read model
    // and advances the offset as ONE unit, and SKIPS a re-delivered already-applied batch (the
    // in-transaction monotonic/overlap guard). The side effects here stand in for the read-model
    // write that a transactional processor would commit atomically with the offset.
    AtomicBatchProcessor guarded =
        (name, batch, newOffset, fencingEpoch, updater, os) -> {
          if (newOffset.value() <= os.getLastOffset(name).value()) {
            reDeliveryApplies.incrementAndGet(); // would fire only if the guard were bypassed
            return; // already applied — skip (exactly-once)
          }
          updater.update(null); // read-model write via the transaction-scoped updater
          applyCount.addAndGet(batch.size());
          os.saveOffset(name, newOffset); // checkpoint advance — same guarded unit
        };

    Projection p = batch -> {};
    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(1)
            .fetchSize(10)
            .atomicProcessor(guarded)
            .register("p", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(6))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 2L);

    assertThat(applyCount.get())
        .as("each chunk must be routed through executeAtomically exactly once")
        .isEqualTo(2);
    assertThat(reDeliveryApplies.get())
        .as("no re-delivery / double-apply of an already-applied chunk")
        .isZero();
  }

  @Test
  void skip_epochFenceRejection_doesNotAdvanceCheckpoint_noDeadLetter() throws Exception {
    // An epoch-fence OptimisticLockException from executeAtomically is a LEADERSHIP signal,
    // not a projection error. Under errorStrategy=SKIP a stale leader (whose local
    // leadership.current
    // is still present during the partition window) must NOT convert the fence REJECTION into a
    // forward checkpoint advance — that would silently, permanently skip the fenced events. The
    // drain is abandoned, the checkpoint is untouched, and nothing is dead-lettered.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))); // offset 1
    var offsets = memOffsetStore();

    var staleLeadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(1L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    var applyCount = new AtomicInteger(0);
    var atomicCalls = new CountDownLatch(1);
    AtomicBatchProcessor fencingProcessor =
        (name, batch, newOffset, fencingEpoch, updater, os) -> {
          atomicCalls.countDown();
          throw new org.streamrune.core.projection.ProjectionCommitFencedException(
              org.streamrune.core.projection.ProjectionCommitFencedException.Guard.EPOCH_FENCE,
              "commit fenced out: caller epoch 1 is below the stored epoch 2");
        };

    var dlq = new RecordingDlq();
    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(TestFencingProcessor.fencing(fencingProcessor))
            .leadership(staleLeadership)
            .deadLetterStore(dlq)
            .register(
                "fenced",
                batch -> applyCount.addAndGet(batch.size()),
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    assertThat(atomicCalls.await(5, TimeUnit.SECONDS))
        .as("the scheduled drain must attempt the fenced atomic commit")
        .isTrue();
    Thread.sleep(300); // give a buggy SKIP path time to advance the checkpoint if it were going to

    assertThat(offsets.getLastOffset(ProjectionName.of("fenced")))
        .as("the fence rejection must NOT advance the checkpoint past the un-applied events")
        .isEqualTo(GlobalOffset.initial());
    assertThat(dlq.written).as("a fence rejection must never be dead-lettered").isEmpty();
    assertThat(applyCount.get()).as("no events were applied by the fenced-out leader").isZero();
  }

  @Test
  void leaderTick_stampsEpochOnCheckpointBeforeDrain() {
    // The epoch fence compares against the epoch STORED on the checkpoint row, which
    // only a committed advance used to stamp — for a cron-cadence runner the takeover window
    // (acquire -> first committed advance) could span a whole idle cron period, during which a
    // superseded leader's writes passed the fence. Every leader tick must stamp the held epoch
    // (stampFencingEpoch) BEFORE the drain reads or processes anything.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))); // offset 1
    var offsets = memOffsetStore();

    var leadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(7L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    // Records the exact order of stamp vs drain-commit calls (anonymous class: a lambda could
    // not override the default stampFencingEpoch).
    var calls = new ConcurrentLinkedQueue<String>();
    var stampingProcessor =
        new AtomicBatchProcessor() {
          @Override
          public boolean supportsFencing() {
            return true;
          }

          @Override
          public void executeAtomically(
              ProjectionName name,
              java.util.List<org.streamrune.core.EventEnvelope> batch,
              GlobalOffset newOffset,
              long fencingEpoch,
              ProjectionUpdater updater,
              org.streamrune.core.projection.OffsetStore os) {
            calls.add("execute:" + fencingEpoch);
            updater.update(null);
            os.saveOffset(name, newOffset);
          }

          @Override
          public void stampFencingEpoch(ProjectionName name, long fencingEpoch) {
            calls.add("stamp:" + fencingEpoch);
          }
        };

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(stampingProcessor)
            .leadership(leadership)
            .register("stamped", batch -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> calls.contains("execute:7"));

    assertThat(calls.peek())
        .as("the takeover stamp (held epoch) must precede any drain work; calls: " + calls)
        .isEqualTo("stamp:7");
    assertThat(offsets.getLastOffset(ProjectionName.of("stamped"))).isEqualTo(GlobalOffset.of(1L));
  }

  @Test
  void leaderTick_stampFailure_skipsTheTick_failsClosed() {
    // Fail-closed discipline: draining WITHOUT the takeover stamp would leave the fence
    // inert for the superseded leader, so a stamp failure must skip the whole tick (no read, no
    // process, no advance) and the next cron fire retries the stamp.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))); // offset 1
    var offsets = memOffsetStore();

    var leadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(9L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    var stampAttempts = new AtomicInteger();
    var drainedBeforeStamp = new java.util.concurrent.atomic.AtomicBoolean(false);
    var stampSucceeded = new java.util.concurrent.atomic.AtomicBoolean(false);
    var failingThenStampingProcessor =
        new AtomicBatchProcessor() {
          @Override
          public boolean supportsFencing() {
            return true;
          }

          @Override
          public void executeAtomically(
              ProjectionName name,
              java.util.List<org.streamrune.core.EventEnvelope> batch,
              GlobalOffset newOffset,
              long fencingEpoch,
              ProjectionUpdater updater,
              org.streamrune.core.projection.OffsetStore os) {
            if (!stampSucceeded.get()) {
              drainedBeforeStamp.set(true); // the violation this test pins against
            }
            updater.update(null);
            os.saveOffset(name, newOffset);
          }

          @Override
          public void stampFencingEpoch(ProjectionName name, long fencingEpoch) {
            if (stampAttempts.incrementAndGet() <= 1) {
              throw new RuntimeException("simulated transient stamp failure");
            }
            stampSucceeded.set(true);
          }
        };

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(failingThenStampingProcessor)
            .leadership(leadership)
            .register("stamp-fail", batch -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> offsets.getLastOffset(ProjectionName.of("stamp-fail")).value() == 1L);

    assertThat(stampAttempts.get())
        .as("the failed stamp is retried on the next cron fire")
        .isGreaterThanOrEqualTo(2);
    assertThat(drainedBeforeStamp.get())
        .as("FAIL-CLOSED: nothing may be drained on a tick whose stamp did not commit")
        .isFalse();
  }

  @Test
  void skip_staleLeaderCheckpointAdvance_isEpochFenced_neverSkipsEvents() throws Exception {
    // A genuine PROJECTION error (NOT a fence) under errorStrategy=SKIP triggers the SKIP
    // checkpoint advance in handleError. Pre-fix that advance was a RAW offsetStore.saveOffset — a
    // forward move the monotonic guard cannot reject — so a stale leader whose local
    // leadership.current() still reports "leader" (the ttl/3 staleness window) would advance the
    // checkpoint past events a newer leader has not applied, SILENTLY skipping them with NO
    // dead-letter. The fix routes the checkpoint-only advance through the EPOCH-FENCED empty-batch
    // executeAtomically. The fencing processor RUNS the projection on the non-empty processing
    // batch
    // (so its error surfaces → SKIP) but FENCES the empty-batch advance (epoch 1 < stored epoch 2).
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z")))); // offset 1
    var offsets = memOffsetStore();

    var staleLeadership =
        new org.streamrune.core.subscription.SubscriptionLeadership() {
          final java.util.Optional<Lease> held = java.util.Optional.of(new Lease(1L));

          @Override
          public java.util.Optional<Lease> tryAcquire(String consumerName) {
            return held;
          }

          @Override
          public java.util.Optional<Lease> current(String consumerName) {
            return held;
          }

          @Override
          public void resign(String consumerName) {}

          @Override
          public void close() {}
        };

    var emptyBatchAdvanceAttempted = new CountDownLatch(1);
    AtomicBatchProcessor fencingProcessor =
        (name, batch, newOffset, fencingEpoch, updater, os) -> {
          if (batch.isEmpty()) {
            emptyBatchAdvanceAttempted.countDown();
            throw new org.streamrune.core.projection.ProjectionCommitFencedException(
                org.streamrune.core.projection.ProjectionCommitFencedException.Guard.EPOCH_FENCE,
                "commit fenced out: caller epoch " + fencingEpoch + " is below the stored epoch 2");
          }
          updater.update(null); // runs the projection → it throws → drives the SKIP path
          os.saveOffset(name, newOffset);
        };

    var dlq = new RecordingDlq();
    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .atomicProcessor(TestFencingProcessor.fencing(fencingProcessor))
            .leadership(staleLeadership)
            .deadLetterStore(dlq)
            .drainRetryBackoff(Duration.ofMillis(2), Duration.ofMillis(10))
            .register(
                "a6-skip-fenced",
                batch -> {
                  throw new RuntimeException("poison batch"); // a genuine processing error → SKIP
                },
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    assertThat(emptyBatchAdvanceAttempted.await(5, TimeUnit.SECONDS))
        .as("the SKIP checkpoint advance must be routed through the fenced empty-batch commit")
        .isTrue();
    Thread.sleep(300); // give a buggy (unfenced) SKIP path time to advance if it were going to

    assertThat(offsets.getLastOffset(ProjectionName.of("a6-skip-fenced")))
        .as("a fenced-out stale leader must NOT advance the checkpoint past the un-applied events")
        .isEqualTo(GlobalOffset.initial());
    assertThat(dlq.written).as("SKIP must never dead-letter").isEmpty();
  }

  @Test
  void skip_singleNodeEpochZeroCheckpointAdvance_isFencedButStillCommits() {
    // No-regression: with NOOP leadership (epoch 0, unfenced) the SKIP checkpoint advance
    // now flows through the empty-batch executeAtomically. A guard-enforcing processor mirroring
    // the
    // real Jdbc fence (rejects epoch != 0 && < stored, epoch 0 always passes) must STILL advance
    // the
    // checkpoint past the skipped chunk — the common single-node path must not regress.
    var store =
        listStore(
            List.of(
                evt(Instant.parse("2026-01-01T00:00:00Z")),
                evt(Instant.parse("2026-01-01T00:00:01Z")))); // offsets 1, 2
    var offsets = memOffsetStore();

    var emptyBatchAdvances = new java.util.concurrent.CopyOnWriteArrayList<Long>();
    AtomicBatchProcessor guarded =
        (name, batch, newOffset, fencingEpoch, updater, os) -> {
          if (fencingEpoch != 0L && fencingEpoch < 2L) {
            throw new org.streamrune.core.projection.ProjectionCommitFencedException(
                org.streamrune.core.projection.ProjectionCommitFencedException.Guard.EPOCH_FENCE,
                "fenced");
          }
          if (batch.isEmpty()) {
            emptyBatchAdvances.add(newOffset.value()); // the SKIP checkpoint-only advance
          }
          updater.update(null);
          os.saveOffset(name, newOffset);
        };

    Projection p =
        batch -> {
          if (batch.get(0).globalOffset().value() == 1L) {
            throw new RuntimeException("poison"); // chunk 1 → SKIP
          }
        };

    runner =
        ScheduledProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(1)
            .fetchSize(10)
            .atomicProcessor(guarded)
            .register(
                "a6-skip-single-node",
                p,
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(6))
        .until(() -> offsets.getLastOffset(ProjectionName.of("a6-skip-single-node")).value() == 2L);
    assertThat(emptyBatchAdvances)
        .as("the SKIP advance must route through the empty-batch fenced commit at epoch 0")
        .contains(1L);
  }

  @Test
  void singleTickDrainsPastPermanentHoles() {
    // readGlobalStream's contiguity guard truncates a page at a permanent offset hole,
    // so `events.size() < fetchSize` is NOT a reliable end-of-stream signal. The buggy drain
    // treated
    // it as the tail and advanced only ONE hole per cron tick. Here every event is separated by a
    // hole (offsets 1,3,5,…), so the buggy runner would need one tick (~1s) PER event — 30 ticks,
    // ~30s — while the fixed runner drains all 30 within the FIRST tick. Asserting all 30 are
    // processed inside a 6s window fails against the old code and passes against the fixed drain.
    int n = 30;
    var events = new ArrayList<EventEnvelope>();
    for (int i = 0; i < n; i++) {
      long offset = 2L * i + 1; // 1,3,5,… — a permanent hole between every event
      events.add(offsetEvt(offset, Instant.parse("2026-01-01T00:00:00Z").plusSeconds(i)));
    }
    var store = new ContiguityTruncatingEventStore(events);
    var offsets = memOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = batch -> processed.addAll(batch);

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(100)
            .fetchSize(100)
            .register("holed", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await().atMost(Duration.ofSeconds(6)).until(() -> processed.size() == n);
    assertThat(offsets.getLastOffset(ProjectionName.of("holed")))
        .isEqualTo(GlobalOffset.of(2L * n - 1));
  }

  @Test
  void readPoison_haltsTerminallyInsteadOfRetryingTheTickForever() {
    // A read/deserialization poison (here an unregistered event type) fails identically
    // every tick. The buggy runner classified it as a transient store error and retried forever,
    // never advancing. The fixed runner bounds it and HALTs terminally with a distinct signal.
    var poisonStore =
        new ReadThrowingEventStore(
            () ->
                new UnknownEventTypeException(
                    "event type", "RemovedEvent", java.util.List.of("KnownEvent")));
    var offsets = memOffsetStore();

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(poisonStore)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            // Fast, small bounds so the terminal HALT is reached quickly instead of over minutes.
            .drainRetryBackoff(Duration.ofMillis(1), Duration.ofMillis(5))
            .maxReadPoisonRetries(3)
            .register("poison", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.status().get("poison").state() == ScheduledProjectionState.ERROR);
    assertThat(runner.status().get("poison").lastError())
        .contains("read/deserialization-level poison");
    // The projection never advanced past the un-readable event.
    assertThat(offsets.getLastOffset(ProjectionName.of("poison")))
        .isEqualTo(GlobalOffset.initial());
  }

  @Test
  void status_reportsLastFireAndNextFire() {
    var store = emptyStore();
    var offsets = memOffsetStore();

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .register("p", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(() -> runner.status().get("p").lastFireAt() != null);

    var status = runner.status().get("p");
    assertThat(status.name()).isEqualTo(ProjectionName.of("p"));
    assertThat(status.lastFireAt()).isNotNull();
    assertThat(status.nextFireAt()).isAfter(status.lastFireAt());
  }

  @Test
  void projectionThrows_HALT_transientThenRecovers_doesNotHalt() {
    // A HALT-strategy projection that hits a single transient read-model blip must
    // self-heal — retry the tick from the checkpoint and recover — exactly like the CONTINUOUS
    // runner (which retries up to MAX_CONSECUTIVE_ERRORS). The OLD scheduled HALT branch halted
    // terminally on the FIRST failure, so a 2-second blip became a permanent outage. Here the
    // projection throws once, then succeeds; the offset advances and the projection is NOT ERROR.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var failedOnce = new AtomicInteger(0);
    Projection p =
        batch -> {
          if (failedOnce.incrementAndGet() == 1) {
            throw new RuntimeException("transient blip");
          }
          // succeeds on the retry
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            // Tiny backoff so the tick-retry after the transient failure is fast.
            .drainRetryBackoff(Duration.ofMillis(5), Duration.ofMillis(20))
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.HALT, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    // The retry recovers and advances past the event; the projection never goes terminal.
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 1L);
    assertThat(runner.status().get("p").state()).isNotEqualTo(ScheduledProjectionState.ERROR);
  }

  @Test
  void projectionThrows_HALT_strategy_retriesThenHaltsAfterBound() {
    // A genuinely-stuck HALT projection still halts terminally, but only after
    // maxTransientRetries consecutive failures OF THE SAME CHUNK (parity with the CONTINUOUS runner
    // halting after MAX_CONSECUTIVE_ERRORS), not on the first failure. Bounds are shrunk so the
    // terminal HALT is reached in well under a second.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var attempts = new AtomicInteger(0);
    Projection p =
        batch -> {
          attempts.incrementAndGet();
          throw new RuntimeException("boom");
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .maxTransientRetries(2)
            .drainRetryBackoff(Duration.ofMillis(2), Duration.ofMillis(10))
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.HALT, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.status().get("p").state() == ScheduledProjectionState.ERROR);

    assertThat(runner.status().get("p").lastError()).contains("boom");
    assertThat(offsets.getLastOffset(ProjectionName.of("p")))
        .isEqualTo(GlobalOffset.initial()); // never advanced
    // Halts after EXACTLY maxTransientRetries (=2) consecutive failures of the same chunk,
    // matching ContinuousProjectionRunner (which increments the count BEFORE the bound check and
    // halts on the Nth failure where N == the bound). The pre-fix pre-increment passed a 0-based
    // count and halted one attempt late (3 attempts here); the post-increment count halts on the
    // 2nd.
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  void projectionThrowsError_terminatesAsErrorNotCleanStop() {
    // A java.lang.Error (AssertionError under -ea, NoClassDefFoundError,
    // StackOverflowError)
    // from a SCHEDULED projection's process() must terminate the projection as ERROR — lastError
    // populated so an operator/health check distinguishes it from a deliberate clean stop — NOT as
    // a
    // silent STOPPED. drainWithRetry catches only RuntimeException, so an Error would otherwise
    // unwind straight to the finally's STOPPED branch (state=STOPPED, lastError=null). Mirrors the
    // CONTINUOUS runner's catch-Throwable discipline.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    Projection p =
        batch -> {
          throw new AssertionError("scheduled fatal");
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.HALT, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.status().get("p").state() == ScheduledProjectionState.ERROR);
    assertThat(runner.status().get("p").lastError()).contains("scheduled fatal");
    // The projection never advanced past the fatal chunk.
    assertThat(offsets.getLastOffset(ProjectionName.of("p"))).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void projectionThrows_SKIP_strategy_advancesOffset() {
    var store =
        listStore(
            List.of(
                evt(Instant.parse("2026-01-01T00:00:00Z")),
                evt(Instant.parse("2026-01-01T00:00:01Z"))));
    var offsets = memOffsetStore();
    Projection p =
        batch -> {
          throw new RuntimeException("boom");
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.SKIP, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 2L);

    assertThat(runner.status().get("p").state()).isNotEqualTo(ScheduledProjectionState.ERROR);
  }

  @Test
  void projectionThrows_DLQ_strategy_writesToStoreAndAdvances() {
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var dlq = recordingDlq();
    Projection p =
        batch -> {
          throw new RuntimeException("boom");
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 1L);

    assertThat(((RecordingDlq) dlq).written).hasSize(1);
  }

  /**
   * {@code projection_dead_letters.error_message} is a persisted-text sink — a projection's message
   * can echo event data; the entry carries the sanitized text.
   */
  @Test
  void projectionThrows_DLQ_strategy_persistsTheErrorMessageSanitized() {
    String raw = "unknown order 'o-1\r\n2026-10-02 INFO proj - caught up'\u0000";
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var dlq = recordingDlq();
    Projection p =
        batch -> {
          throw new RuntimeException(raw);
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(dlq)
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 1L);

    var written = ((RecordingDlq) dlq).written;
    assertThat(written).hasSize(1);
    assertThat(written.getFirst().errorMessage())
        .isEqualTo(LogSanitizer.sanitizeFreeText(raw))
        .doesNotContain("\n")
        .doesNotContain("\u0000");
  }

  @Test
  void projectionThrows_DLQ_deadLetterWriteFails_doesNotAdvanceAndRetriesUntilStoreRecovers() {
    // When the dead-letter STORE WRITE itself fails (DLQ store down), a failed write
    // is a TRANSIENT infra failure — the runner must NOT advance the offset past the un-recorded
    // poison range (which would silently, permanently lose it from the read model). It leaves the
    // offset put and retries the whole tick with backoff; once the store recovers the range is
    // dead-lettered and the offset advances.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    // Store stays DOWN until the test flips storeUp — so the "offset did not advance" assertion is
    // observed against a store that is reliably still failing, not one that already recovered.
    var storeUp = new java.util.concurrent.atomic.AtomicBoolean(false);
    var writeAttempts = new java.util.concurrent.atomic.AtomicInteger(0);
    var written = Collections.synchronizedList(new ArrayList<ProjectionDeadLetterEntry>());
    ProjectionDeadLetterStore flakyDlq =
        new ProjectionDeadLetterStore() {
          @Override
          public void save(ProjectionDeadLetterEntry entry) {
            writeAttempts.incrementAndGet();
            if (!storeUp.get()) {
              throw new RuntimeException("DLQ store unavailable");
            }
            written.add(entry);
          }

          @Override
          public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
            return List.copyOf(written);
          }

          @Override
          public List<ProjectionDeadLetterEntry> readAll(int limit) {
            return List.copyOf(written);
          }

          @Override
          public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {}
        };
    Projection p =
        batch -> {
          throw new RuntimeException("boom"); // POISON
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .deadLetterStore(flakyDlq)
            // Tiny drain-retry backoff so the whole-tick retry after a failed write is fast.
            .drainRetryBackoff(Duration.ofMillis(10), Duration.ofMillis(50))
            .register("p", p, "* * * * * *", ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    // While the store is down the write is attempted repeatedly but the offset must NOT advance and
    // the projection must NOT go ERROR — the tick is retried with backoff. Wait for several failed
    // attempts so we know retrying (not a one-shot skip) is happening while still at offset 0.
    Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> writeAttempts.get() >= 3);
    assertThat(offsets.getLastOffset(ProjectionName.of("p")).value())
        .as("offset must not advance while the dead-letter write is failing")
        .isZero();
    assertThat(runner.status().get("p").state()).isNotEqualTo(ScheduledProjectionState.ERROR);

    // Recover the store: the next retry dead-letters the range and advances the offset past it.
    storeUp.set(true);
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> offsets.getLastOffset(ProjectionName.of("p")).value() == 1L);
    assertThat(written).hasSize(1);
    assertThat(runner.status().get("p").state()).isNotEqualTo(ScheduledProjectionState.ERROR);
  }

  /**
   * Rule S2139: a failed dead-letter WRITE is rethrown into drainWithRetry, which ERROR-logs it
   * with its stack trace on every retry. The WARN at the write site keeps its context line but no
   * longer attaches the same throwable, so each DLQ-store outage prints one stack trace per attempt
   * instead of two.
   */
  @Test
  void projectionThrows_DLQ_deadLetterWriteFails_logsTheStackTraceOncePerAttempt() {
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var dlqDown = new RuntimeException("DLQ store unavailable");
    ProjectionDeadLetterStore downDlq =
        new ProjectionDeadLetterStore() {
          @Override
          public void save(ProjectionDeadLetterEntry entry) {
            throw dlqDown;
          }

          @Override
          public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
            return List.of();
          }

          @Override
          public List<ProjectionDeadLetterEntry> readAll(int limit) {
            return List.of();
          }

          @Override
          public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {}
        };
    Projection p =
        batch -> {
          throw new RuntimeException("boom"); // POISON
        };
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(ScheduledProjectionRunner.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      runner =
          ScheduledProjectionRunner.builder()
              .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
              .eventStore(store)
              .offsetStore(memOffsetStore())
              .batchSize(10)
              .fetchSize(10)
              .deadLetterStore(downDlq)
              .drainRetryBackoff(Duration.ofMillis(10), Duration.ofMillis(50))
              .register(
                  "p", p, "* * * * * *", ProjectionErrorStrategy.DLQ, AT_LEAST_ONCE_IDEMPOTENT)
              .build();
      runner.start();

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () ->
                  List.copyOf(appender.list).stream()
                      .anyMatch(e -> e.getFormattedMessage().contains("drain failed")));
      runner.close();
      runner = null;

      var events = List.copyOf(appender.list);
      var writeWarns =
          events.stream()
              .filter(e -> e.getFormattedMessage().contains("DLQ dead-letter WRITE failed"))
              .toList();
      assertThat(writeWarns).isNotEmpty();
      assertThat(writeWarns)
          .allSatisfy(
              e -> {
                assertThat(e.getLevel()).isEqualTo(ch.qos.logback.classic.Level.WARN);
                assertThat(e.getThrowableProxy())
                    .as("the write-site WARN must not repeat the stack trace drainWithRetry logs")
                    .isNull();
              });
      var drainErrors =
          events.stream().filter(e -> e.getFormattedMessage().contains("drain failed")).toList();
      assertThat(drainErrors)
          .isNotEmpty()
          .allSatisfy(
              e -> {
                assertThat(e.getLevel()).isEqualTo(ch.qos.logback.classic.Level.ERROR);
                assertThat(e.getThrowableProxy()).isNotNull();
                assertThat(e.getThrowableProxy().getMessage()).isEqualTo("DLQ store unavailable");
              });
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  @Test
  void transientStoreFailure_retriesDrainInsteadOfKillingThread() {
    var delegate = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var failuresLeft = new java.util.concurrent.atomic.AtomicInteger(2);
    // Store whose readGlobalStream fails twice before recovering — a transient DB blip.
    EventStore flakyStore =
        new EventStore() {
          @Override
          public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
            if (failuresLeft.getAndDecrement() > 0) {
              throw new RuntimeException("connection refused");
            }
            return delegate.readGlobalStream(afterOffset, maxCount);
          }

          @Override
          public org.streamrune.core.AggregateHistory load(
              org.streamrune.core.types.StreamId streamId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public AppendResult append(
              org.streamrune.core.types.StreamId streamId,
              List<EventEnvelope> events,
              org.streamrune.core.types.Version expectedVersion) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void saveSnapshot(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version version,
              org.streamrune.core.AggregateState state) {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<EventEnvelope> readStream(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version afterVersion,
              int maxCount) {
            throw new UnsupportedOperationException();
          }
        };

    var offsets = memOffsetStore();
    var processed = new ConcurrentLinkedQueue<EventEnvelope>();
    Projection p = processed::addAll;

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(flakyStore)
            .offsetStore(offsets)
            .batchSize(10)
            .fetchSize(10)
            .register("p", p, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    // The drain must retry through the two failures and still process the event — the
    // projection thread must NOT die and must NOT be misreported as a clean STOPPED.
    Awaitility.await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> processed.size() == 1 && runner.status().get("p").lastError() == null);

    var state = runner.status().get("p").state();
    assertThat(state).isNotEqualTo(ScheduledProjectionState.STOPPED);
    assertThat(state).isNotEqualTo(ScheduledProjectionState.ERROR);
  }

  @Test
  void stop_interruptsAndJoins() {
    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(emptyStore())
            .offsetStore(memOffsetStore())
            .register("p", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    runner.stop();

    assertThat(runner.status().get("p").state()).isEqualTo(ScheduledProjectionState.STOPPED);
  }

  @Test
  void multipleProjections_independentThreads() {
    var store = emptyStore();
    var offsets = memOffsetStore();
    var firedA = new AtomicReference<Instant>();
    var firedB = new AtomicReference<Instant>();

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .register("a", b -> firedA.set(Instant.now()), "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .register("b", b -> firedB.set(Instant.now()), "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .until(
            () ->
                runner.status().get("a").lastFireAt() != null
                    && runner.status().get("b").lastFireAt() != null);

    assertThat(runner.status()).containsKeys("a", "b");
  }

  // === restart: stop()->start() on the same instance ===

  @Test
  void stopAndRestartAllowsSecondStart() {
    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(emptyStore())
            .offsetStore(memOffsetStore())
            .register("p", b -> {}, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();

    // Wait for at least one cron fire (reported via status.lastFireAt), then stop
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.status().get("p").lastFireAt() != null);
    runner.stop();
    assertThat(runner.status().get("p").state()).isEqualTo(ScheduledProjectionState.STOPPED);

    // Second start must not throw and must fire again (lastFireAt advances)
    Instant fireBeforeRestart = runner.status().get("p").lastFireAt();
    assertThatCode(() -> runner.start()).doesNotThrowAnyException();
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              Instant lastFire = runner.status().get("p").lastFireAt();
              return lastFire != null && lastFire.isAfter(fireBeforeRestart);
            });
    runner.stop();
  }

  @Test
  void stop_stuckProjectionThread_leavesRunnerNotRestartable() throws Exception {
    // DeadLetterRetryRunner, OutboxPoller, SagaTimeoutRunner and
    // SagaCompensationRetrySweeper were fixed to refuse a restart when close()'s timed join times
    // out with the poll thread still alive; ScheduledProjectionRunner.stop() was the
    // missed 5th runner. Before the fix it unconditionally reset `stopping`/`started` after the
    // timed join, so: a stuck process() -> join times out -> guards reset -> start() spawns a
    // fresh thread for the same projection -> the stuck thread eventually unblocks, re-reads
    // !stopping.get() as true again, and RESUMES its drain loop — two threads then clobber the
    // same projection's offset.
    var store = listStore(List.of(evt(Instant.parse("2026-01-01T00:00:00Z"))));
    var offsets = memOffsetStore();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Projection stuck =
        batch -> {
          entered.countDown();
          // Simulates a wedged projection: blocks and swallows interrupts until released.
          while (release.getCount() > 0) {
            try {
              release.await();
            } catch (InterruptedException _) {
              // keep blocking — deliberately interrupt-proof
            }
          }
        };

    runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(store)
            .offsetStore(offsets)
            .stopTimeoutMs(100)
            .register("p", stuck, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    runner.start();
    try {
      assertThat(entered.await(5, TimeUnit.SECONDS))
          .as("projection should have started processing")
          .isTrue();

      runner.stop(); // join(100ms) times out: the thread is stuck inside process()

      assertThatThrownBy(() -> runner.start())
          .as("restart must be refused while the old projection thread is still alive")
          .isInstanceOf(IllegalStateException.class);
    } finally {
      release.countDown();
      runner.stop(); // thread can now exit; this stop completes and releases the started flag
    }
  }

  // === test fixtures ===

  /**
   * Rewraps the shared {@link WindowedProjectionTestSupport#evt} envelope at an explicit offset.
   */
  private static EventEnvelope offsetEvt(long offset, Instant t) {
    var e = evt(t);
    return new EventEnvelope(
        GlobalOffset.of(offset), e.streamId(), e.version(), e.eventType(), e.event(), e.metadata());
  }

  private static EventStore emptyStore() {
    return SimpleTestEventStore.of(List.of());
  }

  private static EventStore listStore(List<EventEnvelope> events) {
    return SimpleTestEventStore.of(events);
  }

  private static OffsetStore memOffsetStore() {
    return new InMemoryOffsetStore();
  }

  private static ProjectionDeadLetterStore recordingDlq() {
    return new RecordingDlq();
  }

  static class RecordingDlq implements ProjectionDeadLetterStore {
    final List<ProjectionDeadLetterEntry> written = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void save(ProjectionDeadLetterEntry entry) {
      written.add(entry);
    }

    @Override
    public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
      return List.of();
    }

    @Override
    public List<ProjectionDeadLetterEntry> readAll(int limit) {
      return List.of();
    }

    @Override
    public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
      // no-op
    }
  }
}

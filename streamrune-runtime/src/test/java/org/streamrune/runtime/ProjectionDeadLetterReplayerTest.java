package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryProjectionDeadLetterStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ProjectionDeadLetterReplayerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  private static final ProjectionName PROJ = ProjectionName.of("replay-proj");

  record TestEvent(String payload) implements DomainEvent {}

  private static EventEnvelope envelope(long globalOffset) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(TYPE, AggregateId.of("s1")),
        new Version(globalOffset),
        new EventType("TestEvent"),
        new TestEvent("e" + globalOffset),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }

  private static ProjectionDeadLetterEntry entry(long from, long to, int size, Instant failedAt) {
    return new ProjectionDeadLetterEntry(
        PROJ,
        GlobalOffset.of(from),
        GlobalOffset.of(to),
        size,
        "java.lang.RuntimeException",
        "boom",
        1,
        failedAt);
  }

  private static List<Long> offsets(List<EventEnvelope> batch) {
    return batch.stream().map(e -> e.globalOffset().value()).toList();
  }

  @Test
  void replaysFailedRangeAndDiscardsEntry() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3), envelope(4), envelope(5));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(2, 3, 2, Instant.now()));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(1, processed.size());
    assertEquals(List.of(2L, 3L), offsets(processed.getFirst()));
    assertTrue(dlqStore.all().isEmpty(), "replayed entry must be discarded");
  }

  @Test
  void replaysOldestRangeFirst() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3), envelope(4));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    // The higher-offset entry failed LATER. The store already returns oldest-first (by failedAt
    // ascending), and the replayer's own re-sort is a defensive no-op pinning the same order —
    // either way the lower range must be patched first.
    dlqStore.save(entry(2, 2, 1, Instant.parse("2026-01-01T00:00:00Z")));
    dlqStore.save(entry(4, 4, 1, Instant.parse("2026-02-01T00:00:00Z")));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(2, 0, 0), result);
    assertEquals(List.of(2L), offsets(processed.get(0)));
    assertEquals(List.of(4L), offsets(processed.get(1)));
  }

  @Test
  void failingEntryIsKeptAndRemainingEntriesStillReplay() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.parse("2026-01-01T00:00:00Z")));
    dlqStore.save(entry(3, 3, 1, Instant.parse("2026-01-02T00:00:00Z")));

    var processed = new ArrayList<List<EventEnvelope>>();
    Projection poisonedAtOffset1 =
        batch -> {
          if (batch.getFirst().globalOffset().value() == 1) {
            throw new RuntimeException("still poisoned");
          }
          processed.add(batch);
        };
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, poisonedAtOffset1, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 1, 0), result);
    assertEquals(List.of(3L), offsets(processed.getFirst()));
    assertEquals(1, dlqStore.all().size(), "failed entry must be kept");
    assertEquals(1, dlqStore.all().getFirst().fromOffset().value());
  }

  @Test
  void erasedRangeIsDiscardedWithoutProcessing() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    // No event of the range is left in the event store (rows deleted out of band — crypto-shredding
    // keeps the events and only destroys the subject's key).
    dlqStore.save(entry(10, 12, 3, Instant.now()));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertTrue(processed.isEmpty(), "an erased range has nothing to process");
    assertTrue(dlqStore.all().isEmpty(), "unactionable entry must be discarded");
  }

  @Test
  void partiallyErasedRangeReplaysOnlyTheSurvivingEvents() {
    var eventStore = new FixedOffsetEventStore();
    // Offset 3 was erased after the batch [2-4] was dead-lettered; 5 is outside the range.
    eventStore.addEvents(envelope(1), envelope(2), envelope(4), envelope(5));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(2, 4, 3, Instant.now()));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(List.of(2L, 4L), offsets(processed.getFirst()));
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void rangeIsReassembledAcrossShortPages() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3));
    eventStore.capPagesAt(1); // store returns at most one event per read
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 3, 3, Instant.now()));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(List.of(1L, 2L, 3L), offsets(processed.getFirst()));
  }

  @Test
  void maxEntriesLimitsThePass() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.parse("2026-01-01T00:00:00Z")));
    dlqStore.save(entry(2, 2, 1, Instant.parse("2026-01-02T00:00:00Z")));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 1);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(1, processed.size());
    assertEquals(1, dlqStore.all().size(), "the other entry stays for the next pass");
  }

  @Test
  void failingDiscardKeepsEntryAndCountsAsFailed() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new FailingDiscardStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));

    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    // The projection ran, but the entry could not be discarded — at-least-once: a later pass
    // replays it again, which the (idempotent) projection must tolerate.
    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(0, 1, 0), result);
    assertEquals(1, processed.size());
    assertEquals(1, dlqStore.delegate.all().size());
  }

  // --- A feed that applied nothing must keep the entry ---

  @Test
  void whollyFencedReplay_keepsTheEntry_andCountsItAsFenced() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(2, 3, 2, Instant.now()));
    // A self-fencing projection reporting the whole range as already accumulated.
    Projection fencing =
        new Projection() {
          @Override
          public void process(List<EventEnvelope> batch) {}

          @Override
          public boolean processDeadLetterReplay(
              List<EventEnvelope> batch, ProjectionRepository repository) {
            return false;
          }
        };
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, fencing, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(0, 0, 1), result);
    assertEquals(
        1, dlqStore.all().size(), "a feed that applied NOTHING must keep the range's only record");
  }

  @Test
  void whollyFencedWindowedProjection_endToEnd_keepsTheEntry() {
    // On the real projection: the live runner dead-lettered [2-3], skipped past it and applied a
    // LATER batch, advancing WindowedProjection's offset fence past the hole. The replay feed then
    // returns normally having accumulated nothing, and the entry must be kept.
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3), envelope(4));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(2, 3, 2, Instant.now()));
    var windowed =
        WindowedProjection.<Integer>builder()
            .size(java.time.Duration.ofMinutes(10))
            .grace(java.time.Duration.ZERO)
            .initialState(() -> 0)
            .accumulator((acc, e) -> acc + 1)
            .sink(r -> {})
            .build();
    windowed.process(List.of(envelope(4))); // the live runner's later batch: fence = 4
    var replayer = replayer(eventStore, dlqStore);

    var result = replayer.replay(PROJ, windowed, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(0, 0, 1), result);
    assertEquals(1, dlqStore.all().size(), "the never-accumulated range's entry must survive");
  }

  /**
   * A decorator that inherited the {@code processDeadLetterReplay} default would call its OWN
   * {@code process}, reach the delegate's {@code process} and never the delegate's override, and
   * report {@code true}: a decorated {@code WindowedProjection} would read as "applied" on a wholly
   * fenced replay and the replayer would discard the range's only record. Each shipped decorator
   * forwards to the delegate's {@code processDeadLetterReplay} and reports ITS answer.
   */
  @Test
  void whollyFencedWindowedProjection_throughEachShippedDecorator_keepsTheEntry() {
    record Decorator(String name, java.util.function.Function<Projection, Projection> wrap) {}
    var invalidated = new ArrayList<List<EventEnvelope>>();
    List<Decorator> decorators =
        List.of(
            new Decorator(
                "tracing",
                p ->
                    new TracingProjectionDecorator(
                        PROJ, p, io.opentelemetry.api.OpenTelemetry.noop())),
            new Decorator(
                "validating",
                p ->
                    new ValidatingProjectionDecorator(
                        p,
                        jakarta.validation.Validation.buildDefaultValidatorFactory()
                            .getValidator())),
            new Decorator("cache-aware", p -> new CacheAwareProjection(p, invalidated::add)));

    for (var decorator : decorators) {
      var eventStore = new FixedOffsetEventStore();
      eventStore.addEvents(envelope(1), envelope(2), envelope(3), envelope(4));
      var dlqStore = new InMemoryProjectionDeadLetterStore();
      dlqStore.save(entry(2, 3, 2, Instant.now()));
      var windowed =
          WindowedProjection.<Integer>builder()
              .size(java.time.Duration.ofMinutes(10))
              .grace(java.time.Duration.ZERO)
              .initialState(() -> 0)
              .accumulator((acc, e) -> acc + 1)
              .sink(r -> {})
              .build();
      windowed.process(List.of(envelope(4))); // the live runner's later batch: fence = 4
      var replayer = replayer(eventStore, dlqStore);

      var result =
          replayer.replay(PROJ, decorator.wrap().apply(windowed), AT_LEAST_ONCE_IDEMPOTENT, 10);

      assertEquals(
          new ProjectionDeadLetterReplayer.ReplayResult(0, 0, 1),
          result,
          decorator.name() + ": a decorated self-fencing projection must still report 'fenced'");
      assertEquals(
          1,
          dlqStore.all().size(),
          decorator.name() + ": the never-accumulated range's entry must survive the decorator");
    }
    assertTrue(
        invalidated.isEmpty(),
        "a fenced replay applied nothing, so the cache-aware decorator must not invalidate");
  }

  /** The other half of the CacheAware contract: an APPLIED replay invalidates, exactly once. */
  @Test
  void appliedReplay_throughCacheAwareProjection_invalidatesOnce() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2), envelope(3));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(2, 3, 2, Instant.now()));
    var processed = new ArrayList<EventEnvelope>();
    Projection plain = processed::addAll; // the default processDeadLetterReplay: process + true
    var invalidated = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(eventStore, dlqStore);

    var result =
        replayer.replay(
            PROJ, new CacheAwareProjection(plain, invalidated::add), AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(List.of(2L, 3L), offsets(processed));
    assertEquals(1, invalidated.size(), "one applied batch, one invalidation");
    assertEquals(List.of(2L, 3L), offsets(invalidated.get(0)));
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void ordinaryProjection_defaultReplayProbe_reportsApplied_andDiscards() {
    // The SPI default (process, then report applied) is the truthful answer for every projection
    // honouring the idempotent-upsert contract — its entry is discarded after a successful feed.
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    var processed = new ArrayList<List<EventEnvelope>>();

    var result =
        replayer(eventStore, dlqStore).replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(1, processed.size());
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void constructorRejectsNullArguments() {
    var eventStore = new FixedOffsetEventStore();
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    var processor = new InMemoryProjectionRepository();
    assertThrows(
        IllegalArgumentException.class,
        () -> new ProjectionDeadLetterReplayer(null, dlqStore, processor));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ProjectionDeadLetterReplayer(eventStore, null, processor));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ProjectionDeadLetterReplayer(eventStore, dlqStore, null));
  }

  @Test
  void replayRejectsInvalidArguments() {
    var replayer = replayer(new FixedOffsetEventStore(), new InMemoryProjectionDeadLetterStore());
    Projection noop = batch -> {};
    assertThrows(
        IllegalArgumentException.class,
        () -> replayer.replay((ProjectionName) null, noop, AT_LEAST_ONCE_IDEMPOTENT, 1));
    // A blank projection name never reaches replay: ProjectionName rejects it first.
    assertThrows(IllegalArgumentException.class, () -> ProjectionName.of("  "));
    assertThrows(
        IllegalArgumentException.class,
        () -> replayer.replay(PROJ, null, AT_LEAST_ONCE_IDEMPOTENT, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> replayer.replay(PROJ, noop, AT_LEAST_ONCE_IDEMPOTENT, 0));
    assertThrows(IllegalArgumentException.class, () -> replayer.replay(PROJ, noop, null, 1));
  }

  // --- Replay runs through the processor, under the lock a live batch takes ---

  /** A read-modify-write read model: one row, one counter per kind of event. */
  record Tally(int replayed, int live) {}

  /** Reads the row, adds one to the replayed or the live count, saves it. */
  private static final class TallyProjection extends BaseProjection {
    volatile Runnable afterLiveRead = () -> {};
    volatile RuntimeException failAfterReplayWrite;
    final java.util.concurrent.atomic.AtomicBoolean replayEntered =
        new java.util.concurrent.atomic.AtomicBoolean();

    TallyProjection(ProjectionRepository repository) {
      super(repository, PROJ.value());
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (var envelope : batch) {
        boolean live = ((TestEvent) envelope.event()).payload().startsWith("live");
        if (!live) {
          replayEntered.set(true);
        }
        Tally row = findById("row", Tally.class).orElse(new Tally(0, 0));
        if (live) {
          afterLiveRead.run();
          save("row", new Tally(row.replayed(), row.live() + 1));
        } else {
          save("row", new Tally(row.replayed() + 1, row.live()));
          if (failAfterReplayWrite != null) {
            throw failAfterReplayWrite;
          }
        }
      }
    }
  }

  private static EventEnvelope liveEnvelope(long globalOffset) {
    var template = envelope(globalOffset);
    return new EventEnvelope(
        template.globalOffset(),
        template.streamId(),
        template.version(),
        template.eventType(),
        new TestEvent("live" + globalOffset),
        template.metadata());
  }

  @Test
  void replay_waitsForAnOpenLiveBatch_andBothReadModifyWritesSurvive() throws Exception {
    var repository = new InMemoryProjectionRepository();
    var projection = new TallyProjection(repository);
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), liveEnvelope(2));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    repository.saveOffset(PROJ, GlobalOffset.of(1)); // the runner moved past the dead-lettered [1]

    var liveIsOpen = new java.util.concurrent.CountDownLatch(1);
    var releaseLive = new java.util.concurrent.CountDownLatch(1);
    projection.afterLiveRead =
        () -> {
          liveIsOpen.countDown();
          try {
            releaseLive.await(30, java.util.concurrent.TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    var liveBatch = List.of(liveEnvelope(2));
    Thread live =
        Thread.ofPlatform()
            .start(
                () ->
                    repository.executeAtomically(
                        PROJ,
                        liveBatch,
                        GlobalOffset.of(2),
                        0L,
                        tx -> projection.process(liveBatch, tx),
                        repository));
    assertTrue(liveIsOpen.await(10, java.util.concurrent.TimeUnit.SECONDS));

    var result = new java.util.concurrent.atomic.AtomicReference<Object>();
    Thread replay =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    result.set(
                        new ProjectionDeadLetterReplayer(eventStore, dlqStore, repository)
                            .replay(PROJ, projection, TRANSACTIONAL_LOCAL, 10));
                  } catch (RuntimeException e) {
                    result.set(e);
                  }
                });
    replay.join(300);
    boolean replayWaited = replay.isAlive();
    boolean replayReadTheRow = projection.replayEntered.get();
    releaseLive.countDown();
    live.join(10_000);
    replay.join(10_000);

    assertTrue(replayWaited, "the replay waits while a live batch of the projection is open");
    assertFalse(replayReadTheRow, "the replay must not read the row the open live batch changes");
    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result.get());
    assertEquals(
        new Tally(1, 1),
        repository.findById(PROJ, "row", Tally.class).orElseThrow(),
        "the replayed change and the live change both survive");
    assertEquals(
        GlobalOffset.of(2), repository.getLastOffset(PROJ), "replay does not move the checkpoint");
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void transactionalReplay_thatFailsAfterItsWrite_rollsBack_keepsTheEntry_thenConverges() {
    var repository = new InMemoryProjectionRepository();
    var projection = new TallyProjection(repository);
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    repository.saveOffset(PROJ, GlobalOffset.of(5));
    var replayer = new ProjectionDeadLetterReplayer(eventStore, dlqStore, repository);

    projection.failAfterReplayWrite = new IllegalStateException("crash before the replay commit");
    var crashed = replayer.replay(PROJ, projection, TRANSACTIONAL_LOCAL, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(0, 1, 0), crashed);
    assertTrue(
        repository.findById(PROJ, "row", Tally.class).isEmpty(),
        "the write went through the replay transaction and rolled back with it");
    assertEquals(1, dlqStore.all().size());

    projection.failAfterReplayWrite = null;
    var rerun = replayer.replay(PROJ, projection, TRANSACTIONAL_LOCAL, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), rerun);
    assertEquals(new Tally(1, 0), repository.findById(PROJ, "row", Tally.class).orElseThrow());
    assertEquals(GlobalOffset.of(5), repository.getLastOffset(PROJ));
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void atLeastOnceReplay_handsNoRepository_andWritesStandWhenItFails() {
    var repository = new InMemoryProjectionRepository();
    var projection = new TallyProjection(repository);
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    var replayer = new ProjectionDeadLetterReplayer(eventStore, dlqStore, repository);

    projection.failAfterReplayWrite = new IllegalStateException("crash after the write");
    var crashed = replayer.replay(PROJ, projection, AT_LEAST_ONCE_IDEMPOTENT, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(0, 1, 0), crashed);
    assertEquals(
        new Tally(1, 0),
        repository.findById(PROJ, "row", Tally.class).orElseThrow(),
        "an at-least-once projection writes through its own repository, outside the replay"
            + " transaction");
    assertEquals(1, dlqStore.all().size(), "the entry is kept, so the range is applied again");
  }

  @Test
  void appliedTransactionalReplay_throughCacheAwareProjection_invalidatesAfterTheCommit() {
    var repository = new InMemoryProjectionRepository();
    var projection = new TallyProjection(repository);
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    var rowsSeenByTheInvalidator = new ArrayList<java.util.Optional<Tally>>();
    var cacheAware =
        new CacheAwareProjection(
            projection,
            batch -> rowsSeenByTheInvalidator.add(repository.findById(PROJ, "row", Tally.class)));

    var result =
        new ProjectionDeadLetterReplayer(eventStore, dlqStore, repository)
            .replay(PROJ, cacheAware, TRANSACTIONAL_LOCAL, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(
        List.of(java.util.Optional.of(new Tally(1, 0))),
        rowsSeenByTheInvalidator,
        "the invalidation runs once, after the replayed row is committed");
  }

  @Test
  void replay_refusesAProcessorWithoutAReplayLock_beforeItReadsAnEntry() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer =
        new ProjectionDeadLetterReplayer(
            eventStore, dlqStore, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var refusal =
        assertThrows(
            IllegalStateException.class,
            () -> replayer.replay(PROJ, processed::add, AT_LEAST_ONCE_IDEMPOTENT, 10));

    assertTrue(refusal.getMessage().contains("nonAtomicAtLeastOnce"), refusal.getMessage());
    assertTrue(refusal.getMessage().contains("replayWithRunnerStopped"), refusal.getMessage());
    assertTrue(processed.isEmpty(), "nothing is fed to the projection");
    assertEquals(1, dlqStore.all().size(), "the entry is untouched");
  }

  @Test
  void replayWithRunnerStopped_feedsTheRangeOnAProcessorWithoutAReplayLock() {
    var eventStore = new FixedOffsetEventStore();
    eventStore.addEvents(envelope(1), envelope(2));
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 2, 2, Instant.now()));
    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer =
        new ProjectionDeadLetterReplayer(
            eventStore, dlqStore, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var result = replayer.replayWithRunnerStopped(PROJ, processed::add, 10);

    assertEquals(new ProjectionDeadLetterReplayer.ReplayResult(1, 0, 0), result);
    assertEquals(List.of(1L, 2L), offsets(processed.getFirst()));
    assertTrue(dlqStore.all().isEmpty());
  }

  @Test
  void replayWithRunnerStopped_isRefusedOnAProcessorThatSerializesReplays() {
    var dlqStore = new InMemoryProjectionDeadLetterStore();
    dlqStore.save(entry(1, 1, 1, Instant.now()));
    var processed = new ArrayList<List<EventEnvelope>>();
    var replayer = replayer(new FixedOffsetEventStore(), dlqStore);

    var refusal =
        assertThrows(
            IllegalStateException.class,
            () -> replayer.replayWithRunnerStopped(PROJ, processed::add, 10));

    assertTrue(refusal.getMessage().contains("replay(...)"), refusal.getMessage());
    assertTrue(processed.isEmpty());
    assertEquals(1, dlqStore.all().size());
  }

  @Test
  void replay_refusesATransactionalRegistrationThatDoesNotWriteThroughTheHandedRepository() {
    var replayer = replayer(new FixedOffsetEventStore(), new InMemoryProjectionDeadLetterStore());
    Projection ignoresTheRepository = batch -> {};

    assertThrows(
        IllegalArgumentException.class,
        () -> replayer.replay(PROJ, ignoresTheRepository, TRANSACTIONAL_LOCAL, 10));
  }

  /** A replayer over an in-memory processor, which serializes replays. */
  private static ProjectionDeadLetterReplayer replayer(
      EventStore eventStore, ProjectionDeadLetterStore dlqStore) {
    return new ProjectionDeadLetterReplayer(
        eventStore, dlqStore, new InMemoryProjectionRepository());
  }

  // --- In-memory test doubles ---

  /** Event store with explicit global offsets and an optional cap on the page size it returns. */
  private static final class FixedOffsetEventStore implements EventStore {

    private final List<EventEnvelope> events = new ArrayList<>();
    private int pageCap = Integer.MAX_VALUE;

    void addEvents(EventEnvelope... envelopes) {
      events.addAll(List.of(envelopes));
    }

    void capPagesAt(int cap) {
      this.pageCap = cap;
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return events.stream()
          .filter(e -> e.globalOffset().value() > afterOffset.value())
          .limit(Math.min(maxCount, pageCap))
          .toList();
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException("not needed for replay tests");
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> envelopes, Version expectedVersion) {
      throw new UnsupportedOperationException("not needed for replay tests");
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException("not needed for replay tests");
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException("not needed for replay tests");
    }
  }

  /** Delegates to the in-memory store but fails every discard. */
  private static final class FailingDiscardStore implements ProjectionDeadLetterStore {

    final InMemoryProjectionDeadLetterStore delegate = new InMemoryProjectionDeadLetterStore();

    @Override
    public void save(ProjectionDeadLetterEntry entry) {
      delegate.save(entry);
    }

    @Override
    public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
      return delegate.read(projectionName, limit);
    }

    @Override
    public List<ProjectionDeadLetterEntry> readAll(int limit) {
      return delegate.readAll(limit);
    }

    @Override
    public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
      throw new RuntimeException("discard unavailable");
    }
  }
}

package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryOffsetStore;

class PollingProjectionRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  private InMemoryEventStore eventStore;
  private InMemoryOffsetStore offsetStore;
  private PollingProjectionRunner runner;

  @BeforeEach
  void setUp() {
    eventStore = new InMemoryEventStore();
    offsetStore = new InMemoryOffsetStore();
    runner =
        new PollingProjectionRunner(
            eventStore, offsetStore, 100, 3, AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }

  @Test
  void catchUpProcessesEventsInBatches() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    StreamId s2 = StreamId.of(TYPE, AggregateId.of("s2"));
    eventStore.addEvents(
        envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")),
        envelope(GlobalOffset.of(2), s1, 2, new TestEvent("B")),
        envelope(GlobalOffset.of(3), s1, 3, new TestEvent("C")),
        envelope(GlobalOffset.of(4), s2, 1, new TestEvent("D")),
        envelope(GlobalOffset.of(5), s2, 2, new TestEvent("E")));

    var collected = new ArrayList<List<EventEnvelope>>();
    Projection projection = collected::add;

    runner.run(ProjectionName.of("test-proj"), projection, AT_LEAST_ONCE_IDEMPOTENT);

    // With batchSize=3: first batch [1,2,3], second batch [4,5]
    assertEquals(2, collected.size());
    assertEquals(3, collected.get(0).size());
    assertEquals(2, collected.get(1).size());

    // Offset should be at 5 (last event)
    assertEquals(GlobalOffset.of(5), offsetStore.getLastOffset(ProjectionName.of("test-proj")));
  }

  @Test
  void singleRunDrainsBacklogLargerThanFetchSize() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(
        envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")),
        envelope(GlobalOffset.of(2), s1, 2, new TestEvent("B")),
        envelope(GlobalOffset.of(3), s1, 3, new TestEvent("C")),
        envelope(GlobalOffset.of(4), s1, 4, new TestEvent("D")),
        envelope(GlobalOffset.of(5), s1, 5, new TestEvent("E")));

    // fetchSize=2: the backlog spans three pages; one run() call must drain all of them.
    runner =
        new PollingProjectionRunner(
            eventStore, offsetStore, 2, 2, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var collected = new ArrayList<List<EventEnvelope>>();
    Projection projection = collected::add;

    runner.run(ProjectionName.of("test-proj"), projection, AT_LEAST_ONCE_IDEMPOTENT);

    assertEquals(3, collected.size());
    assertEquals(5, collected.stream().mapToInt(List::size).sum());
    assertEquals(GlobalOffset.of(5), offsetStore.getLastOffset(ProjectionName.of("test-proj")));
  }

  @Test
  void singleRunDrainsPastAPermanentHole() {
    // A permanent offset hole (an offset no committed event holds) truncates a fetch
    // page to its contiguous prefix. run() must NOT treat that short page as the tail — it must
    // advance past the hole and drain to the true tail within the single call.
    // Present offsets: 1,2,3, [hole 4], 5,6.
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    var holed =
        new ContiguityTruncatingEventStore(
            List.of(
                envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")),
                envelope(GlobalOffset.of(2), s1, 2, new TestEvent("B")),
                envelope(GlobalOffset.of(3), s1, 3, new TestEvent("C")),
                envelope(GlobalOffset.of(5), s1, 5, new TestEvent("E")),
                envelope(GlobalOffset.of(6), s1, 6, new TestEvent("F"))));

    // fetchSize=100 is larger than the whole stream, so the first read returns only the truncated
    // prefix [1,2,3] (size 3 < fetchSize) — the exact "short page because of a hole" case.
    runner =
        new PollingProjectionRunner(
            holed, offsetStore, 100, 100, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var collected = new ArrayList<List<EventEnvelope>>();
    runner.run(ProjectionName.of("holed-proj"), collected::add, AT_LEAST_ONCE_IDEMPOTENT);

    int totalProcessed = collected.stream().mapToInt(List::size).sum();
    assertEquals(
        5, totalProcessed, "run() must drain all events past the hole in a single call, not stall");
    assertEquals(GlobalOffset.of(6), offsetStore.getLastOffset(ProjectionName.of("holed-proj")));
  }

  @Test
  void catchUpResumesFromLastOffset() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(
        envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")),
        envelope(GlobalOffset.of(2), s1, 2, new TestEvent("B")),
        envelope(GlobalOffset.of(3), s1, 3, new TestEvent("C")));

    // Simulate that events 1-2 were already processed
    offsetStore.saveOffset(ProjectionName.of("test-proj"), GlobalOffset.of(2));

    var collected = new ArrayList<List<EventEnvelope>>();
    Projection projection = collected::add;

    runner.run(ProjectionName.of("test-proj"), projection, AT_LEAST_ONCE_IDEMPOTENT);

    // Only event 3 should be processed
    assertEquals(1, collected.size());
    assertEquals(1, collected.get(0).size());
    assertEquals(3, collected.get(0).getFirst().globalOffset().value());
    assertEquals(GlobalOffset.of(3), offsetStore.getLastOffset(ProjectionName.of("test-proj")));
  }

  @Test
  void catchUpDoesNothingWhenNoNewEvents() {
    var collected = new ArrayList<List<EventEnvelope>>();
    Projection projection = collected::add;

    runner.run(ProjectionName.of("test-proj"), projection, AT_LEAST_ONCE_IDEMPOTENT);

    assertTrue(collected.isEmpty());
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("test-proj")));
  }

  @Test
  void resetSetsOffsetToZero() {
    offsetStore.saveOffset(ProjectionName.of("test-proj"), GlobalOffset.of(42));

    runner.reset(ProjectionName.of("test-proj"));

    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("test-proj")));
  }

  @Test
  void saveOffsetGuardRejectsRegression() {
    ProjectionName name = ProjectionName.of("guarded-proj");
    offsetStore.saveOffset(name, GlobalOffset.of(300));
    offsetStore.saveOffset(name, GlobalOffset.of(200)); // backward → guarded no-op
    assertEquals(GlobalOffset.of(300), offsetStore.getLastOffset(name));
    offsetStore.saveOffset(name, GlobalOffset.of(301)); // forward → advances
    assertEquals(GlobalOffset.of(301), offsetStore.getLastOffset(name));
  }

  @Test
  void resetUsesUnguardedPath_notSaveOffset() {
    // With the store now guarded, reset() rewinding to 0 would be a no-op if it went through
    // saveOffset(initial()). This asserts reset() actually rewinds — i.e. it uses
    // OffsetStore.reset,
    // the unguarded path. It would FAIL if reset() called saveOffset(name, initial()).
    ProjectionName name = ProjectionName.of("reset-proj");
    offsetStore.saveOffset(name, GlobalOffset.of(100));
    runner.reset(name);
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(name));
  }

  @Test
  void resetThrowsWhenOffsetStoreResetDoesNotActuallyRewind() {
    // Defense-in-depth: a broken OffsetStore whose reset() overrides the interface (so the
    // throwing default never fires) but doesn't actually rewind the checkpoint must still be
    // caught loudly by the runner, not silently accepted.
    var brokenStore =
        new OffsetStore() {
          private final Map<String, GlobalOffset> offsets = new HashMap<>();

          @Override
          public GlobalOffset getLastOffset(ProjectionName projectionName) {
            return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
          }

          @Override
          public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
            offsets.put(projectionName.value(), offset);
          }

          @Override
          public void reset(ProjectionName projectionName) {
            // Deliberately broken: claims success (no exception) but does not rewind.
          }
        };
    var name = ProjectionName.of("broken-reset-proj");
    brokenStore.saveOffset(name, GlobalOffset.of(50));
    var brokenRunner =
        new PollingProjectionRunner(
            eventStore, brokenStore, 100, 3, AtomicBatchProcessor.nonAtomicAtLeastOnce());

    var ex = assertThrows(IllegalStateException.class, () -> brokenRunner.reset(name));
    assertTrue(ex.getMessage().contains("reset"), ex.getMessage());
  }

  @Test
  void resetAllowsFullReplay() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(
        envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")),
        envelope(GlobalOffset.of(2), s1, 2, new TestEvent("B")));

    // First run
    var collected = new ArrayList<List<EventEnvelope>>();
    runner.run(ProjectionName.of("first-proj"), collected::add, AT_LEAST_ONCE_IDEMPOTENT);
    assertEquals(1, collected.size());

    // Reset
    runner.reset(ProjectionName.of("first-proj"));
    collected.clear();

    // Second run replays everything
    runner.run(ProjectionName.of("first-proj"), collected::add, AT_LEAST_ONCE_IDEMPOTENT);
    assertEquals(1, collected.size());
    assertEquals(2, collected.get(0).size());
  }

  @Test
  void forwardsProcessorSuppliedRepositoryToATransactionalProjection() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")));

    // Stands in for the transaction-scoped repository a JDBC-backed processor would supply. Only a
    // transactional registration is handed it, so the processor must claim fencing and the
    // projection must declare TRANSACTIONAL_LOCAL.
    ProjectionRepository txRepository = mock(ProjectionRepository.class);
    AtomicBatchProcessor processor =
        TestFencingProcessor.fencing(
            (name, batch, newOffset, fencingEpoch, projectionUpdater, offsets) -> {
              projectionUpdater.update(txRepository);
              offsets.saveOffset(name, newOffset);
            });
    runner = new PollingProjectionRunner(eventStore, offsetStore, 100, 3, processor);

    var received = new ArrayList<ProjectionRepository>();
    Projection projection =
        new Projection() {
          @Override
          public void process(List<EventEnvelope> batch) {
            fail("runner must invoke the repository-aware variant");
          }

          @Override
          public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
            received.add(repository);
          }
        };

    runner.run(ProjectionName.of("tx-proj"), projection, TRANSACTIONAL_LOCAL);

    assertEquals(1, received.size());
    assertSame(txRepository, received.getFirst());
    assertEquals(GlobalOffset.of(1), offsetStore.getLastOffset(ProjectionName.of("tx-proj")));
  }

  @Test
  void nonatomicProcessorPassesNullRepositoryToProjection() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")));

    var received = new ArrayList<ProjectionRepository>();
    Projection projection =
        new Projection() {
          @Override
          public void process(List<EventEnvelope> batch) {
            fail("runner must invoke the repository-aware variant");
          }

          @Override
          public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
            received.add(repository);
          }
        };

    // nonAtomicAtLeastOnce() hands the updater null — no transaction, no repository
    runner.run(ProjectionName.of("nonatomic-proj"), projection, AT_LEAST_ONCE_IDEMPOTENT);

    assertEquals(1, received.size());
    assertNull(received.getFirst());
    assertEquals(
        GlobalOffset.of(1), offsetStore.getLastOffset(ProjectionName.of("nonatomic-proj")));
  }

  // A fencing-claiming processor that hands null to the updater: a plain projection declared
  // AT_LEAST_ONCE_IDEMPOTENT runs under it; one declared TRANSACTIONAL_LOCAL is refused, since its
  // read-model writes would silently NOT be part of the checkpoint's transaction.
  private static final AtomicBatchProcessor TRANSACTIONAL =
      new AtomicBatchProcessor() {
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

  @Test
  void run_refusesATransactionalLocalLambda_underTheNonatomicProcessorAndUnderAFencingOne() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")));
    Projection lambda = batch -> {};

    runner =
        new PollingProjectionRunner(
            eventStore, offsetStore, 100, 3, AtomicBatchProcessor.nonAtomicAtLeastOnce());
    var nonatomic =
        assertThrows(
            IllegalArgumentException.class,
            () -> runner.run(ProjectionName.of("plain"), lambda, TRANSACTIONAL_LOCAL));
    assertTrue(nonatomic.getMessage().contains("PollingProjectionRunner"), nonatomic.getMessage());
    assertTrue(nonatomic.getMessage().contains("'plain'"), nonatomic.getMessage());
    assertTrue(
        nonatomic.getMessage().contains("needs a transactional processor"), nonatomic.getMessage());

    runner = new PollingProjectionRunner(eventStore, offsetStore, 100, 3, TRANSACTIONAL);
    var fencing =
        assertThrows(
            IllegalArgumentException.class,
            () -> runner.run(ProjectionName.of("plain"), lambda, TRANSACTIONAL_LOCAL));
    assertTrue(
        fencing.getMessage().contains("does not write through the handed repository"),
        fencing.getMessage());
    assertEquals(GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("plain")));
  }

  /** The processor is required: only the five- and six-argument constructors remain. */
  @Test
  void onlyTheTwoConstructorsWithARequiredProcessorExist() {
    var arities =
        java.util.Arrays.stream(PollingProjectionRunner.class.getConstructors())
            .map(java.lang.reflect.Constructor::getParameterCount)
            .sorted()
            .toList();
    assertEquals(List.of(5, 6), arities);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PollingProjectionRunner(eventStore, offsetStore, 100, 3, null));
    assertEquals("atomicProcessor is required", ex.getMessage());
  }

  /**
   * run() asks the processor about the name before it reads an event, so a name the processor
   * cannot store read models under fails before anything is processed or checkpointed.
   */
  @Test
  void run_refusesANameTheProcessorCannotStore_beforeReadingAnEvent() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")));
    var processor = new NameRuleProcessor();
    runner = new PollingProjectionRunner(eventStore, offsetStore, 100, 3, processor);

    var collected = new ArrayList<List<EventEnvelope>>();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                runner.run(
                    ProjectionName.of("order-summary"), collected::add, AT_LEAST_ONCE_IDEMPOTENT));
    assertTrue(ex.getMessage().contains("order-summary"), ex.getMessage());
    assertEquals(List.of(ProjectionName.of("order-summary")), processor.checked);
    assertEquals(0, processor.commits.get());
    assertTrue(collected.isEmpty());
    assertEquals(
        GlobalOffset.initial(), offsetStore.getLastOffset(ProjectionName.of("order-summary")));
  }

  @Test
  void run_acceptsAPlainProjectionDeclaredAtLeastOnce_underAFencingProcessor() {
    StreamId s1 = StreamId.of(TYPE, AggregateId.of("s1"));
    eventStore.addEvents(envelope(GlobalOffset.of(1), s1, 1, new TestEvent("A")));
    runner = new PollingProjectionRunner(eventStore, offsetStore, 100, 3, TRANSACTIONAL, null);

    var collected = new ArrayList<List<EventEnvelope>>();
    Projection nonWriteThrough = collected::add;

    runner.run(ProjectionName.of("at-least-once"), nonWriteThrough, AT_LEAST_ONCE_IDEMPOTENT);

    assertEquals(1, collected.size());
  }

  @Test
  void constructorRejectsNullArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PollingProjectionRunner(
                null, offsetStore, 100, 50, AtomicBatchProcessor.nonAtomicAtLeastOnce()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PollingProjectionRunner(
                eventStore, null, 100, 50, AtomicBatchProcessor.nonAtomicAtLeastOnce()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PollingProjectionRunner(
                eventStore, offsetStore, 0, 50, AtomicBatchProcessor.nonAtomicAtLeastOnce()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PollingProjectionRunner(
                eventStore, offsetStore, 100, -1, AtomicBatchProcessor.nonAtomicAtLeastOnce()));
  }

  // --- In-memory test doubles ---

  // Simple test event record implementing DomainEvent
  record TestEvent(String payload) implements DomainEvent {}

  private static EventEnvelope envelope(
      GlobalOffset globalOffset, StreamId streamId, long version, TestEvent value) {
    return new EventEnvelope(
        globalOffset,
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        value,
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

  private static final class InMemoryEventStore implements EventStore {

    private final List<EventEnvelope> events = new ArrayList<>();

    void addEvents(EventEnvelope... envelopes) {
      events.addAll(List.of(envelopes));
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException("not needed for projection tests");
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      throw new UnsupportedOperationException("not needed for projection tests");
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException("not needed for projection tests");
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return events.stream()
          .filter(e -> e.globalOffset().value() > afterOffset.value())
          .limit(maxCount)
          .toList();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return events.stream()
          .filter(e -> e.version().value() > afterVersion.value())
          .limit(maxCount)
          .toList();
    }
  }

  // InMemoryOffsetStore lives in its own top-level test class (shared by the runtime tests).
  // ContiguityTruncatingEventStore lives in its own top-level test class (shared by runtime tests).
}

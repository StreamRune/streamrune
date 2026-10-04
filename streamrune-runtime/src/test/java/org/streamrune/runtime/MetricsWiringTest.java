package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * Verifies that the previously-unrecorded StreamRuneMetrics meters now fire at their call sites.
 */
class MetricsWiringTest {

  private static final AggregateType TYPE = AggregateType.of("counter");

  record TestEvent(String payload) implements DomainEvent {}

  private static EventEnvelope envelope(long globalOffset, StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("p" + globalOffset),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  // ==================== Projection runner ====================

  @Test
  void pollingProjectionRunnerRecordsProcessedAndDurationOnHappyPath() {
    StreamId s = StreamId.of(TYPE, AggregateId.of("s1"));
    var store =
        SimpleTestEventStore.of(List.of(envelope(1, s, 1), envelope(2, s, 2), envelope(3, s, 3)));
    var offsets = new InMemoryOffsetStore();
    var metrics = new RecordingStreamRuneMetrics();
    var runner =
        new PollingProjectionRunner(
            store,
            offsets,
            100,
            2,
            org.streamrune.core.projection.AtomicBatchProcessor.nonAtomicAtLeastOnce(),
            metrics);

    runner.run(ProjectionName.of("orders"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT);

    // 3 events processed (counted per event), two batches → two duration records.
    assertEquals(3, metrics.count("projection.processed", "orders"));
    assertEquals(2, metrics.count("projection.duration", "orders"));
    assertEquals(0, metrics.count("projection.failed"));
  }

  @Test
  void pollingProjectionRunnerRecordsFailedOnProcessingError() {
    StreamId s = StreamId.of(TYPE, AggregateId.of("s1"));
    var store = SimpleTestEventStore.of(List.of(envelope(1, s, 1)));
    var offsets = new InMemoryOffsetStore();
    var metrics = new RecordingStreamRuneMetrics();
    var runner =
        new PollingProjectionRunner(
            store,
            offsets,
            100,
            50,
            org.streamrune.core.projection.AtomicBatchProcessor.nonAtomicAtLeastOnce(),
            metrics);

    Projection failing =
        batch -> {
          throw new RuntimeException("boom");
        };

    assertThrows(
        RuntimeException.class,
        () -> runner.run(ProjectionName.of("orders"), failing, AT_LEAST_ONCE_IDEMPOTENT));
    assertEquals(1, metrics.count("projection.failed", "orders"));
    assertEquals(1, metrics.count("projection.duration", "orders"));
    assertEquals(0, metrics.count("projection.processed"));
  }

  // ==================== Subscription ====================

  @Test
  void pollingSubscriptionRecordsReceivedAndLatency() {
    StreamId s = StreamId.of(TYPE, AggregateId.of("s1"));
    var store = SimpleTestEventStore.of(List.of(envelope(1, s, 1), envelope(2, s, 2)));
    var offsets = new InMemoryOffsetStore();
    var metrics = new RecordingStreamRuneMetrics();
    var received = new ArrayList<EventEnvelope>();
    var sub =
        PollingEventSubscription.builder()
            .subscriptionName("sub-1")
            .eventStore(store)
            .offsetStore(offsets)
            .listener(received::addAll)
            .config(SubscriptionConfig.DEFAULT)
            .metrics(metrics)
            .build();

    int delivered = sub.pollOnce();

    assertEquals(2, delivered);
    assertEquals(2, metrics.count("subscription.received", "sub-1"));
    assertEquals(1, metrics.count("subscription.latency", "sub-1"));
  }

  // ==================== Command bus ====================

  sealed interface CounterCommand extends Command {
    record Inc(String id) implements CounterCommand {}
  }

  sealed interface CounterEvent extends DomainEvent {
    record Incremented() implements CounterEvent {}
  }

  record CounterState(int count) implements AggregateState {
    CounterState() {
      this(0);
    }
  }

  static final class CounterDecider implements Decider<CounterCommand, CounterState, CounterEvent> {
    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public List<CounterEvent> decide(CounterCommand command, CounterState state) {
      return List.of(new CounterEvent.Incremented());
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return new CounterState(state.count() + 1);
    }
  }

  /** Mutable in-memory store that can be configured to always conflict (to force retries/DLQ). */
  static final class MutableStore implements EventStore {
    private final Map<String, List<EventEnvelope>> streams = new HashMap<>();
    private long global = 0;
    boolean alwaysConflict = false;

    @Override
    public AggregateHistory load(StreamId streamId) {
      var events = streams.getOrDefault(streamId.value(), List.of());
      Version version = events.isEmpty() ? Version.initial() : events.getLast().version();
      return new AggregateHistory(null, events, version, Version.initial());
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return load(streamId);
    }

    @Override
    public synchronized AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      if (alwaysConflict) {
        throw new OptimisticLockException("forced conflict");
      }
      var existing = streams.getOrDefault(streamId.value(), List.of());
      long currentVersion = existing.isEmpty() ? 0 : existing.getLast().version().value();
      if (currentVersion != expectedVersion.value()) {
        throw new OptimisticLockException("version mismatch");
      }
      var updated = new ArrayList<>(existing);
      List<GlobalOffset> offsets = new ArrayList<>();
      for (EventEnvelope envelope : events) {
        global++;
        GlobalOffset offset = GlobalOffset.of(global);
        offsets.add(offset);
        updated.add(
            new EventEnvelope(
                offset,
                envelope.streamId(),
                envelope.version(),
                envelope.eventType(),
                envelope.event(),
                envelope.metadata()));
      }
      streams.put(streamId.value(), List.copyOf(updated));
      return new AppendResult(List.copyOf(offsets), new Version(currentVersion + events.size()));
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {}

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {}

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return streams.values().stream()
          .flatMap(List::stream)
          .filter(e -> e.globalOffset().value() > afterOffset.value())
          .limit(maxCount)
          .toList();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return streams.getOrDefault(streamId.value(), List.of()).stream()
          .filter(e -> e.version().value() > afterVersion.value())
          .limit(maxCount)
          .toList();
    }
  }

  static final class RecordingDlq implements org.streamrune.core.DeadLetterQueue {
    final List<DeadLetterPublishRequest> entries = new ArrayList<>();

    @Override
    public void publish(DeadLetterPublishRequest request) {
      entries.add(request);
    }

    @Override
    public List<DeadLetterEntry> read(int limit) {
      return List.of();
    }

    @Override
    public void discard(org.streamrune.core.types.CommandId commandId) {}
  }

  private void runWithContext(Runnable action) {
    var ctx =
        new org.streamrune.core.StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    ScopedValue.where(org.streamrune.core.StreamRuneContext.CURRENT, ctx).run(action);
  }

  @Test
  void commandBusRecordsRetriedAndLockWaitOnTransientConflict() {
    var store = new MutableStore();
    store.alwaysConflict = true;
    var dlq = new RecordingDlq();
    var metrics = new RecordingStreamRuneMetrics();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(1), 1.0))
            .deadLetterQueue(dlq)
            .objectMapper(new ObjectMapper())
            .metrics(metrics)
            .register(
                TYPE,
                CounterCommand.class,
                cmd -> AggregateId.of(((CounterCommand.Inc) cmd).id()),
                new CounterDecider())
            .build();

    runWithContext(
        () ->
            assertThrows(RuntimeException.class, () -> bus.execute(new CounterCommand.Inc("c-1"))));

    // 3 attempts → 2 retries recorded before the final failure.
    assertEquals(2, metrics.count("command.retried", "Inc"));
    // Each attempt acquires the lock once.
    assertEquals(3, metrics.count("command.lockWait"));
    // Retries exhausted → published to the DLQ once.
    assertEquals(1, metrics.count("command.dlqPublished", "Inc"));
    assertEquals(1, dlq.entries.size());
  }

  static final class DenyingInterceptor implements org.streamrune.core.CommandInterceptor {
    @Override
    public boolean before(CommandContext ctx) {
      return false; // short-circuit
    }
  }

  @Test
  void commandBusRecordsShortCircuitedWhenInterceptorDenies() {
    var store = new MutableStore();
    var metrics = new RecordingStreamRuneMetrics();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .metrics(metrics)
            .interceptors(new DenyingInterceptor())
            .register(
                TYPE,
                CounterCommand.class,
                cmd -> AggregateId.of(((CounterCommand.Inc) cmd).id()),
                new CounterDecider())
            .build();

    runWithContext(() -> bus.execute(new CounterCommand.Inc("c-1")));

    assertEquals(1, metrics.count("command.shortCircuited", "Inc"));
    // Short-circuited command is never executed → no events.
    assertTrue(store.readGlobalStream(GlobalOffset.initial(), 100).isEmpty());
  }

  // ==================== Snapshot / replay / append-duration series ====================

  /**
   * Dead-series guard. {@code streamrune.snapshots.created}, {@code streamrune.snapshots.loaded},
   * {@code streamrune.events.replayed} and {@code streamrune.events.duration} were registered
   * eagerly by the Micrometer collector but recorded by NO production code path, so an operator
   * following the production guide ("monitor snapshots.created / snapshots.loaded") saw four
   * permanently flat-zero series that looked wired, and a p99 alert on the documented
   * append-latency timer could never fire.
   *
   * <p>This drives the REAL snapshot round trip through the bus and pins the exact emission counts
   * at every site. With {@code everyNEvents(3)} over 7 commands on one aggregate the bus:
   *
   * <ul>
   *   <li>writes a snapshot at v3 and v6 → 2 × {@code snapshots.created};
   *   <li>loads a snapshot on commands 4-7 → 4 × {@code snapshots.loaded};
   *   <li>replays 0,1,2,0,1,2,0 post-snapshot events → 6 × {@code events.replayed};
   *   <li>appends once per command → 7 × {@code events.duration} samples and 7 × {@code
   *       events.appended}.
   * </ul>
   */
  @Test
  void commandBusRecordsSnapshotAndReplayAndAppendDurationSeries() {
    int total = 7;
    var store = new InMemoryEventStore();
    var metrics = new RecordingStreamRuneMetrics();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .metrics(metrics)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(3))
            .register(
                TYPE,
                CounterCommand.class,
                cmd -> AggregateId.of(((CounterCommand.Inc) cmd).id()),
                new CounterDecider())
            .build();

    runWithContext(
        () -> {
          for (int i = 0; i < total; i++) {
            bus.execute(new CounterCommand.Inc("counter-1"));
          }
        });

    assertEquals(2, metrics.count("snapshot.created"), "snapshots written at v3 and v6");
    assertEquals(4, metrics.count("snapshot.loaded"), "commands 4-7 rehydrate from a snapshot");
    assertEquals(
        6,
        metrics.count("event.replayed"),
        "post-snapshot events folded during reconstruction: 0+1+2+0+1+2+0");
    assertEquals(total, metrics.count("event.appended"));
    assertEquals(
        total, metrics.count("event.appendDuration"), "one append-latency sample per append");
  }

  /**
   * Without a snapshot policy every event of the stream is replayed on every load, and no snapshot
   * is ever created or loaded — the replay counter must still fire (it counts events folded during
   * aggregate reconstruction, not only events after a snapshot).
   */
  @Test
  void commandBusRecordsEveryReplayedEventWhenNoSnapshotPolicyIsConfigured() {
    var store = new InMemoryEventStore();
    var metrics = new RecordingStreamRuneMetrics();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .metrics(metrics)
            .register(
                TYPE,
                CounterCommand.class,
                cmd -> AggregateId.of(((CounterCommand.Inc) cmd).id()),
                new CounterDecider())
            .build();

    runWithContext(
        () -> {
          for (int i = 0; i < 4; i++) {
            bus.execute(new CounterCommand.Inc("counter-2"));
          }
        });

    // Loads see 0, 1, 2 and 3 prior events.
    assertEquals(6, metrics.count("event.replayed"));
    assertEquals(0, metrics.count("snapshot.created"));
    assertEquals(0, metrics.count("snapshot.loaded"));
    assertEquals(4, metrics.count("event.appendDuration"));
  }

  // ==================== Query bus ====================

  record PlainQuery(String key) implements org.streamrune.core.Query<String> {}

  @Cacheable(ttlSeconds = 60, maxEntries = 100, scope = Cacheable.Scope.GLOBAL)
  record CachedQuery(String key) implements org.streamrune.core.Query<String> {}

  @Test
  void simpleQueryBusRecordsDispatchedAndDuration() {
    var metrics = new RecordingStreamRuneMetrics();
    var bus = new SimpleQueryBus(metrics);
    bus.register(PlainQuery.class, (QueryHandler<PlainQuery, String>) q -> "r:" + q.key());

    String result = bus.dispatch(new PlainQuery("a"));

    assertEquals("r:a", result);
    assertEquals(1, metrics.count("query.dispatched", "PlainQuery"));
    assertEquals(1, metrics.count("query.duration", "PlainQuery"));
  }

  @Test
  void cachingQueryBusRecordsCacheMissThenHit() {
    var metrics = new RecordingStreamRuneMetrics();
    QueryBus delegate = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(delegate).metrics(metrics).build();
    cachingBus.register(CachedQuery.class, (QueryHandler<CachedQuery, String>) q -> "v:" + q.key());

    cachingBus.dispatch(new CachedQuery("k")); // miss → delegate computes
    cachingBus.dispatch(new CachedQuery("k")); // hit → served from cache

    assertEquals(1, metrics.count("query.cacheMiss", "CachedQuery"));
    assertEquals(1, metrics.count("query.cacheHit", "CachedQuery"));
  }

  @Test
  void auditingQueryBusRecordsDispatchedAndDuration() {
    var metrics = new RecordingStreamRuneMetrics();
    AuditStore auditStore = mock(AuditStore.class);
    QueryBus delegate = new SimpleQueryBus();
    var bus = new AuditingQueryBus(delegate, auditStore, metrics);
    bus.register(PlainQuery.class, (QueryHandler<PlainQuery, String>) q -> "r:" + q.key());

    bus.dispatch(new PlainQuery("a"));

    assertEquals(1, metrics.count("query.dispatched", "PlainQuery"));
    assertEquals(1, metrics.count("query.duration", "PlainQuery"));
  }
}

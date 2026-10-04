package org.streamrune.runtime;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;

/**
 * JMH Benchmarks for StreamRune framework performance.
 *
 * <p>Run with: ./gradlew :streamrune-runtime:jmh
 *
 * <p><b>Scope.</b> Every benchmark here runs against {@link InMemoryEventStore} — pure in-memory
 * JVM data structures, no I/O, no network, no transaction machinery. These numbers characterize the
 * framework's own overhead (dispatch, locking, serialization-free event bookkeeping) in isolation;
 * they are NOT a proxy for {@code PostgresEventStore} throughput or latency, which is dominated by
 * disk fsync, network round trips, and connection-pool contention that no in-memory benchmark can
 * exercise. A companion smoke test, {@code StreamRuneBenchmarkSmokeTest} (same package, same {@code
 * src/jmh/java} source set), invokes every benchmark method twice against a real {@code @Setup}
 * state and is wired into {@code check} — see the root build for why plain compilation was not
 * enough to catch this class of bug: both {@code eventStoreAppend} and {@code eventStoreLoad}
 * compiled cleanly for months while throwing (or measuring an always-empty stream) on every real
 * invocation.
 */
public class StreamRuneBenchmark {

  private static final AggregateType TYPE = AggregateType.of("benchmark");

  // ===== Command Bus Benchmarks =====

  @State(Scope.Thread)
  public static class CommandBusState {
    public CommandBus bus;

    @Setup(Level.Iteration)
    public void setup() {
      var store = new InMemoryEventStore();
      bus =
          VirtualThreadCommandBus.builder()
              .eventStore(store)
              .register(TYPE, BenchmarkCommand.class, BenchmarkCommand::id, new BenchmarkDecider())
              .build();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void commandBusThroughput(CommandBusState state) {
    var cmd = new BenchmarkCommand(AggregateId.of("aggregate-" + UUID.randomUUID()));
    state.bus.execute(cmd);
  }

  // ===== Event Store Benchmarks =====

  @State(Scope.Thread)
  public static class EventStoreState {
    public EventStore store;

    /**
     * Current append target for {@link #eventStoreAppend}. Rotates onto a fresh stream once it
     * accumulates {@link #MAX_STREAM_EVENTS} events — see that constant's javadoc.
     */
    public StreamId streamId;

    /**
     * Pre-populated with exactly {@link #batchSize} events; read-only target for {@code
     * eventStoreLoad}.
     */
    public StreamId loadStreamId;

    /**
     * Pre-populated with {@link #batchSize} events, snapshotted, then {@link #POST_SNAPSHOT_EVENTS}
     * more — read-only target for {@code eventStoreLoadWithSnapshot}.
     */
    public StreamId snapshotStreamId;

    @Param({"1", "10", "100"})
    public int batchSize;

    /**
     * Events appended after the snapshot on {@link #snapshotStreamId}. Deliberately small and
     * fixed, independent of {@link #batchSize}: that gap is exactly what a snapshot exists to
     * bound, regardless of how long the total stream history is.
     */
    private static final int POST_SNAPSHOT_EVENTS = 5;

    /**
     * Caps how many events {@link #streamId} may accumulate before {@link #eventStoreAppend}
     * rotates onto a brand new stream.
     *
     * <p>{@link InMemoryEventStore} backs each stream with a {@code CopyOnWriteArrayList}:
     * appending to one ever-growing stream for an entire Throughput-mode iteration (up to ~1s of
     * back-to-back invocations) would make each append copy an ever-larger backing array — an
     * O(n^2) blowup across the iteration that both grinds the run to a halt and stops measuring
     * steady-state append cost. Rotating onto a fresh stream every {@code MAX_STREAM_EVENTS} events
     * bounds each individual stream's size — and so each append's copy cost — to a small constant,
     * independent of iteration length or throughput. The number of DISTINCT streams created over an
     * iteration is unbounded (grows with total throughput), but that is ordinary linear {@code
     * ConcurrentHashMap} growth, not the quadratic blowup this constant exists to avoid.
     */
    private static final int MAX_STREAM_EVENTS = 200;

    private long currentHead;
    private int eventsSinceRotation;
    private int streamSequence;

    @Setup(Level.Iteration)
    public void setup() {
      store = new InMemoryEventStore();
      rotateAppendStream();

      loadStreamId = StreamId.of(TYPE, AggregateId.of("load-bench-stream"));
      store.append(loadStreamId, createEventBatch(loadStreamId, batchSize, 0), Version.initial());

      snapshotStreamId = StreamId.of(TYPE, AggregateId.of("load-bench-snapshot-stream"));
      store.append(
          snapshotStreamId, createEventBatch(snapshotStreamId, batchSize, 0), Version.initial());
      store.saveSnapshot(
          snapshotStreamId,
          new Version(batchSize),
          new BenchmarkState("bench-aggregate", "bench-tenant", batchSize));
      store.append(
          snapshotStreamId,
          createEventBatch(snapshotStreamId, POST_SNAPSHOT_EVENTS, batchSize),
          new Version(batchSize));
    }

    /**
     * Starts a brand new, empty append target — always safe to append to at {@link
     * Version#initial()} — and resets the per-stream bookkeeping {@link #eventStoreAppend} uses to
     * decide when to rotate again.
     */
    void rotateAppendStream() {
      streamId = StreamId.of(TYPE, AggregateId.of("append-bench-stream-" + streamSequence++));
      currentHead = 0;
      eventsSinceRotation = 0;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void eventStoreAppend(EventStoreState state) {
    // The original version always appended at Version.initial(), which
    // only the very FIRST invocation of a JMH iteration could ever succeed at -- every subsequent
    // call in that iteration re-targeted the same already-populated stream at version 0 and threw
    // OptimisticLockException (the strict head check makes this fail even more certainly than
    // the old collision-scan check did). Track this state's own append head instead of assuming
    // the stream is empty, and rotate onto a fresh stream periodically (see MAX_STREAM_EVENTS) to
    // keep each individual stream's backing list small.
    if (state.eventsSinceRotation >= EventStoreState.MAX_STREAM_EVENTS) {
      state.rotateAppendStream();
    }
    var events = createEventBatch(state.streamId, state.batchSize, state.currentHead);
    state.store.append(state.streamId, events, new Version(state.currentHead));
    state.currentHead += state.batchSize;
    state.eventsSinceRotation += state.batchSize;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void eventStoreLoad(EventStoreState state, Blackhole bh) {
    // The original version read state.streamId, which EventStoreState.setup() never appended
    // anything to -- every invocation measured the cost of loading an always-empty stream, not the
    // cost of loading batchSize events. loadStreamId is populated with exactly batchSize events in
    // setup(); load() is a pure read, so repeating it across every invocation of an iteration is
    // safe (unlike eventStoreAppend, there is nothing here for repetition to conflict with).
    bh.consume(state.store.load(state.loadStreamId));
  }

  /**
   * Same shape as {@link #eventStoreLoad}, but the target stream has a snapshot taken at version
   * {@code batchSize} with only {@link EventStoreState#POST_SNAPSHOT_EVENTS} events after it — so
   * {@code load()} replays a small, fixed number of events regardless of {@code batchSize}, instead
   * of the whole history. Compare against {@link #eventStoreLoad} at the same {@code batchSize} to
   * see the snapshot's effect on load cost.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void eventStoreLoadWithSnapshot(EventStoreState state, Blackhole bh) {
    bh.consume(state.store.load(state.snapshotStreamId));
  }

  // ===== Decider Benchmarks =====

  @State(Scope.Thread)
  public static class DeciderState {
    public Decider<BenchmarkCommand, BenchmarkState, BenchmarkEvent> decider;
    public BenchmarkState state;

    @Param({"0", "10", "100"})
    public int stateSize;

    @Setup(Level.Iteration)
    public void setup() {
      decider = new BenchmarkDecider();
      state = new BenchmarkState("aggregate-1", "tenant-1", stateSize);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void deciderDecide(DeciderState state) {
    var cmd = new BenchmarkCommand(AggregateId.of("aggregate-1"));
    state.decider.decide(cmd, state.state);
  }

  // ===== Helper Methods =====

  /**
   * Builds {@code count} events for {@code streamId} at versions {@code startVersion + 1 ..
   * startVersion + count} — i.e. the versions the store will actually assign an append made with
   * {@code expectedVersion = new Version(startVersion)}. An event envelope's own version/offset
   * fields are not consulted by {@link EventStore#append} for persistence (see its javadoc); this
   * just keeps them consistent with what the store will assign anyway, for interceptor/audit
   * realism.
   */
  private static List<EventEnvelope> createEventBatch(
      StreamId streamId, int count, long startVersion) {
    var builder = new java.util.ArrayList<EventEnvelope>();
    long offset = 0;
    for (int i = 0; i < count; i++) {
      var event = new BenchmarkEvent("data-" + i);
      builder.add(
          new EventEnvelope(
              GlobalOffset.of(offset++),
              streamId,
              new Version(startVersion + i + 1),
              new EventType("BenchmarkEvent"),
              event,
              createMetadata()));
    }
    return builder;
  }

  private static org.streamrune.core.EventMetadata createMetadata() {
    return new org.streamrune.core.EventMetadata(
        org.streamrune.core.types.EventId.of("test-id"),
        org.streamrune.core.types.CommandId.of("test-command"),
        null,
        null,
        org.streamrune.core.types.CorrelationId.of("test-correlation"),
        null,
        null,
        Instant.now());
  }

  // ===== Test Data Classes =====

  public record BenchmarkCommand(AggregateId id) implements Command {}

  public record BenchmarkEvent(String data) implements DomainEvent {}

  public record BenchmarkState(String aggregateId, String tenantId, int size)
      implements AggregateState {}

  public static class BenchmarkDecider
      implements Decider<BenchmarkCommand, BenchmarkState, BenchmarkEvent> {
    @Override
    public BenchmarkState initialState() {
      return new BenchmarkState("", "", 0);
    }

    @Override
    public List<BenchmarkEvent> decide(BenchmarkCommand cmd, BenchmarkState state) {
      return List.of(new BenchmarkEvent("event-" + System.nanoTime()));
    }

    @Override
    public BenchmarkState evolve(BenchmarkState state, BenchmarkEvent event) {
      return state;
    }
  }
}

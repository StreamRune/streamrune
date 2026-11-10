package org.streamrune.postgres;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * {@link PostgresEventStore} against a real PostgreSQL: append a batch to a new stream and to a
 * stream that has events, load a stream, and load a stream whose events are read through an
 * upcaster chain. Every store is built by {@link PostgresEventStoreFactory} on the schema the
 * framework's Flyway migrations create, so each operation runs the production path end to end:
 * Jackson serialization of payload and metadata, the global-offset reservation, the insert and the
 * commit on append; the query, deserialization and upcasting on load.
 *
 * <p>The database is described in {@link BenchmarkDatabase}: the nightly workflow's PostgreSQL
 * service, or a Testcontainers container when no JDBC URL is configured. Each trial gets a fresh
 * schema. The append state also empties the event stream before every iteration, so the table an
 * iteration appends to holds at most what the iteration itself wrote: without that, every iteration
 * would append to a table and an index grown by all the iterations before it, and the later
 * iterations of a trial would measure a bigger table than the first. The load states write their
 * streams once per trial and only read them afterwards.
 *
 * <p>The annotations give a local run its settings; the nightly run overrides mode, unit and
 * iterations with {@code -PbenchCi} (root build). {@code PostgresEventStoreBenchmarkSmokeTest} runs
 * every method twice outside the harness, as part of {@code check}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class PostgresEventStoreBenchmark {

  static final AggregateType TYPE = AggregateType.of("bench_order");

  /** The event of the append and load benchmarks. */
  public record OrderPlaced(String orderId, String customerId, long amountCents, String currency)
      implements DomainEvent {}

  /** The event type the upcasting benchmark stores at schema version 1 and reads at 2 or 3. */
  static final EventType ORDER_RECORDED = new EventType("OrderRecorded");

  /** {@link #ORDER_RECORDED} as written: version 1. */
  public record OrderRecordedV1(String orderId, String customer, long amount)
      implements DomainEvent {}

  /** {@link #ORDER_RECORDED} after one step: {@code amount} renamed to {@code amountCents}. */
  public record OrderRecordedV2(String orderId, String customer, long amountCents)
      implements DomainEvent {}

  /** {@link #ORDER_RECORDED} after two steps: a {@code currency} added. */
  public record OrderRecordedV3(String orderId, String customer, long amountCents, String currency)
      implements DomainEvent {}

  // ===== Append =====

  @State(Scope.Benchmark)
  public static class AppendState {

    @Param({"1", "10"})
    public int batchSize;

    /** Events the existing stream holds when an iteration starts. */
    static final int EXISTING_EVENTS = 10;

    BenchmarkDatabase database;
    EventStore store;
    StreamId existingStream;
    long existingHead;
    private long nextStream;

    @Setup(Level.Trial)
    public void openDatabase() {
      database = BenchmarkDatabase.open();
      store =
          database.eventStore(
              SimpleEventTypeRegistry.builder().registerEvent(OrderPlaced.class).build(), null);
    }

    @Setup(Level.Iteration)
    public void startIteration() {
      database.clearEvents();
      existingStream = newStream();
      store.append(existingStream, orderEvents(existingStream, EXISTING_EVENTS), Version.initial());
      existingHead = EXISTING_EVENTS;
    }

    @TearDown(Level.Trial)
    public void closeDatabase() {
      database.close();
    }

    StreamId newStream() {
      return StreamId.of(TYPE, AggregateId.of("order-" + nextStream++));
    }
  }

  /** Appends {@code batchSize} events to a stream that does not exist yet. */
  @Benchmark
  public AppendResult appendToNewStream(AppendState state) {
    StreamId stream = state.newStream();
    return state.store.append(stream, orderEvents(stream, state.batchSize), Version.initial());
  }

  /** Appends {@code batchSize} events at the head of a stream that has events. */
  @Benchmark
  public AppendResult appendToExistingStream(AppendState state) {
    AppendResult result =
        state.store.append(
            state.existingStream,
            orderEvents(state.existingStream, state.batchSize),
            new Version(state.existingHead));
    state.existingHead = result.finalVersion().value();
    return result;
  }

  // ===== Load =====

  @State(Scope.Benchmark)
  public static class LoadState {

    @Param({"10", "100"})
    public int streamLength;

    BenchmarkDatabase database;
    EventStore store;
    StreamId stream;

    @Setup(Level.Trial)
    public void writeStream() {
      database = BenchmarkDatabase.open();
      store =
          database.eventStore(
              SimpleEventTypeRegistry.builder().registerEvent(OrderPlaced.class).build(), null);
      stream = StreamId.of(TYPE, AggregateId.of("order-load"));
      store.append(stream, orderEvents(stream, streamLength), Version.initial());
    }

    @TearDown(Level.Trial)
    public void closeDatabase() {
      database.close();
    }
  }

  /** Loads a stream of {@code streamLength} events, no snapshot. */
  @Benchmark
  public AggregateHistory load(LoadState state) {
    return state.store.load(state.stream);
  }

  // ===== Load through an upcaster chain =====

  @State(Scope.Benchmark)
  public static class UpcastLoadState {

    /** Events in the stream: enough that the per-event read cost dominates the round trip. */
    static final int STREAM_LENGTH = 100;

    /** Upcasting steps from the stored version 1 to the version the reader expects. */
    @Param({"1", "2"})
    public int upcastSteps;

    BenchmarkDatabase database;
    EventStore reader;
    StreamId stream;

    @Setup(Level.Trial)
    public void writeStream() {
      database = BenchmarkDatabase.open();
      // A store without upcasters stamps every event with schema version 1.
      EventStore writer =
          database.eventStore(
              SimpleEventTypeRegistry.builder()
                  .registerEvent(ORDER_RECORDED, OrderRecordedV1.class)
                  .build(),
              null);
      stream = StreamId.of(TYPE, AggregateId.of("order-upcast"));
      var events = new ArrayList<EventEnvelope>(STREAM_LENGTH);
      for (int i = 0; i < STREAM_LENGTH; i++) {
        events.add(
            envelope(
                stream, ORDER_RECORDED, new OrderRecordedV1("order-" + i, "customer-" + i, i)));
      }
      writer.append(stream, events, Version.initial());
      // The reader expects version 1 + upcastSteps, so every event runs every step on load.
      Class<?> current = upcastSteps == 1 ? OrderRecordedV2.class : OrderRecordedV3.class;
      reader =
          database.eventStore(
              SimpleEventTypeRegistry.builder().registerEvent(ORDER_RECORDED, current).build(),
              List.of(new OrderRecordedUpcaster(1 + upcastSteps)));
    }

    @TearDown(Level.Trial)
    public void closeDatabase() {
      database.close();
    }
  }

  /** Loads a stream of 100 events stored at version 1, each upcast {@code upcastSteps} times. */
  @Benchmark
  public AggregateHistory loadWithUpcasting(UpcastLoadState state) {
    return state.reader.load(state.stream);
  }

  /** Version 1 to 2 renames {@code amount}; version 2 to 3 adds {@code currency}. */
  record OrderRecordedUpcaster(int currentVersion) implements EventUpcaster {

    @Override
    public EventType eventType() {
      return ORDER_RECORDED;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
      var upcast = new HashMap<>(eventData);
      switch (fromVersion) {
        case 1 -> upcast.put("amountCents", upcast.remove("amount"));
        case 2 -> upcast.put("currency", "EUR");
        default -> throw new IllegalArgumentException("No upcast from version " + fromVersion);
      }
      return upcast;
    }
  }

  // ===== Events =====

  static List<EventEnvelope> orderEvents(StreamId stream, int count) {
    var events = new ArrayList<EventEnvelope>(count);
    for (int i = 0; i < count; i++) {
      events.add(
          envelope(
              stream,
              EventType.fromClass(OrderPlaced.class),
              new OrderPlaced(stream.aggregateId().value(), "customer-" + i, 1_000L + i, "EUR")));
    }
    return events;
  }

  /**
   * An envelope as a command bus hands it to the store. The store assigns the global offset and the
   * version on append and ignores the placeholders here.
   */
  static EventEnvelope envelope(StreamId stream, EventType type, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        type,
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("bench-correlation"),
            null,
            null,
            Instant.now()));
  }
}

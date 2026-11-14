package org.streamrune.runtime;

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
import org.openjdk.jmh.annotations.Warmup;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
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
import org.streamrune.test.InMemoryEventStore;

/**
 * Event deserialization and upcasting without a database: a load of 100 events from an {@link
 * InMemoryEventStore}, which stores every event as JSON on append and reads it back the way {@code
 * PostgresEventStore} does:
 *
 * <p>an event stored at the reader's current schema version is bound to its class with {@code
 * readValue}; an older one is read to a map, run through the {@code UpcasterChain} step by step,
 * and bound with {@code convertValue}. Event metadata is read with {@code readValue} either way.
 *
 * <p>This is the low-noise CPU signal of the Jackson and upcaster-chain mechanism. The production
 * path is {@code PostgresEventStoreBenchmark.loadWithUpcasting} in {@code streamrune-postgres},
 * which runs the same mechanism inside {@code PostgresEventStore} behind a database round trip;
 * that round trip and its variance dwarf the difference between one upcast step and two, which this
 * benchmark shows on its own.
 *
 * <p>{@code upcastSteps} is the number of upcasting steps between the schema version the events are
 * stored at and the version the reader expects (3): 0 for events stored at version 3, read with
 * {@code readValue}; 1 and 2 for events stored at version 2 and 1. The reader, its class and its
 * chain are the same for every value, so only the stored version differs.
 *
 * <p>The annotations give a local run its settings; the nightly run overrides mode, unit and
 * iterations with {@code -PbenchCi} (root build). {@code EventSerializationBenchmarkSmokeTest} runs
 * the method twice outside the harness, as part of {@code check}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class EventSerializationBenchmark {

  static final AggregateType TYPE = AggregateType.of("order");
  static final EventType ORDER_RECORDED = new EventType("OrderRecorded");

  /** The version of {@link #ORDER_RECORDED} the reader expects. */
  static final int CURRENT_VERSION = 3;

  /** Events in the loaded stream. */
  static final int STREAM_LENGTH = 100;

  /** {@link #ORDER_RECORDED} at version 1. */
  public record OrderRecordedV1(String orderId, String customer, long amount)
      implements DomainEvent {}

  /** {@link #ORDER_RECORDED} at version 2: {@code amount} renamed to {@code amountCents}. */
  public record OrderRecordedV2(String orderId, String customer, long amountCents)
      implements DomainEvent {}

  /** {@link #ORDER_RECORDED} at version 3, the current one: a {@code currency} added. */
  public record OrderRecordedV3(String orderId, String customer, long amountCents, String currency)
      implements DomainEvent {}

  @State(Scope.Benchmark)
  public static class LoadState {

    @Param({"0", "1", "2"})
    public int upcastSteps;

    InMemoryEventStore store;
    StreamId stream;

    @Setup(Level.Trial)
    public void appendEvents() {
      store =
          new InMemoryEventStore(
              SimpleEventTypeRegistry.builder()
                  .registerEvent(ORDER_RECORDED, OrderRecordedV3.class)
                  .build(),
              null);
      int storedVersion = CURRENT_VERSION - upcastSteps;
      // The store stamps each event with the current version of the chain in effect at append.
      store.withUpcasters(upcastersAt(storedVersion));
      stream = StreamId.of(TYPE, AggregateId.of("order-stream"));
      var events = new ArrayList<EventEnvelope>(STREAM_LENGTH);
      for (int i = 0; i < STREAM_LENGTH; i++) {
        events.add(envelope(stream, eventAt(storedVersion, i)));
      }
      store.append(stream, events, Version.initial());
      store.withUpcasters(upcastersAt(CURRENT_VERSION));
    }
  }

  /** Loads the stream: 100 events deserialized, each upcast {@code upcastSteps} times. */
  @Benchmark
  public AggregateHistory loadWithUpcasting(LoadState state) {
    return state.store.load(state.stream);
  }

  /** The chain whose current version of {@link #ORDER_RECORDED} is {@code version}. */
  static List<EventUpcaster> upcastersAt(int version) {
    return version == 1 ? List.of() : List.of(new OrderRecordedUpcaster(version));
  }

  static DomainEvent eventAt(int version, int i) {
    return switch (version) {
      case 1 -> new OrderRecordedV1("order-" + i, "customer-" + i, i);
      case 2 -> new OrderRecordedV2("order-" + i, "customer-" + i, i);
      case 3 -> new OrderRecordedV3("order-" + i, "customer-" + i, i, "EUR");
      default -> throw new IllegalArgumentException("No version " + version);
    };
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

  private static EventEnvelope envelope(StreamId stream, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        ORDER_RECORDED,
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of("bench-correlation"),
            null,
            null,
            Instant.now()));
  }
}

package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * Micro-benchmark for the outbox emit hot-path: cost of {@link OutboxEventMapper#toOutbox} for a
 * SKIPPED event (type-check only, returns empty list) vs a PUBLISHED event (one Jackson {@code
 * writeValueAsString} call). No database — pure CPU, laptop-reproducible.
 *
 * <p>Run: {@code ./gradlew :streamrune-runtime:jmh -Pjmh.include=OutboxEventMapperBenchmark}
 */
public class OutboxEventMapperBenchmark {

  private static final AggregateType TYPE = AggregateType.of("order");

  /** Small integration event DTO — the payload serialized into the outbox for downstream. */
  public record OrderPlacedIntegrationEvent(
      String orderId, String customerId, long amountCents, String currency) {}

  /** Domain event marker for the "published" type. */
  record OrderPlaced(String orderId, String customerId, long amountCents, String currency)
      implements DomainEvent {}

  /** Domain event marker for the "skipped" type (no outbox entry should be produced). */
  record OrderDraftCreated(String orderId) implements DomainEvent {}

  @State(Scope.Benchmark)
  public static class MapperState {

    ObjectMapper objectMapper;
    OutboxEventMapper mapper;
    EventEnvelope publishedEnvelope;
    EventEnvelope skippedEnvelope;

    @Setup
    public void setup() {
      objectMapper = new ObjectMapper();

      mapper =
          envelope -> {
            if (envelope.event() instanceof OrderPlaced evt) {
              try {
                String payload =
                    objectMapper.writeValueAsString(
                        new OrderPlacedIntegrationEvent(
                            evt.orderId(), evt.customerId(), evt.amountCents(), evt.currency()));
                return List.of(
                    OutboxEntry.pending(
                        OutboxEntryId.of("outbox-bench-1"),
                        payload,
                        "OrderPlaced",
                        envelope.streamId()));
              } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new RuntimeException("serialization failed", e);
              }
            }
            return List.of();
          };

      EventMetadata metadata =
          new EventMetadata(
              EventId.of("evt-bench-1"),
              CommandId.of("cmd-bench-1"),
              TraceId.of("trace-bench-1"),
              SpanId.of("span-bench-1"),
              CorrelationId.of("corr-bench-1"),
              CausationId.of("cause-bench-1"),
              UserId.of("user-bench-1"),
              Instant.parse("2026-01-01T00:00:00Z"));

      publishedEnvelope =
          new EventEnvelope(
              GlobalOffset.of(1L),
              StreamId.of(TYPE, AggregateId.of("order-abc-123")),
              new Version(1L),
              EventType.fromClass(OrderPlaced.class),
              new OrderPlaced("order-abc-123", "cust-xyz-456", 4999L, "USD"),
              metadata);

      skippedEnvelope =
          new EventEnvelope(
              GlobalOffset.of(2L),
              StreamId.of(TYPE, AggregateId.of("order-abc-123")),
              new Version(2L),
              EventType.fromClass(OrderDraftCreated.class),
              new OrderDraftCreated("order-abc-123"),
              metadata);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public void mapSkippedEvent(MapperState s, Blackhole bh) {
    bh.consume(s.mapper.toOutbox(s.skippedEnvelope));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public void mapPublishedEvent(MapperState s, Blackhole bh) {
    bh.consume(s.mapper.toOutbox(s.publishedEnvelope));
  }
}

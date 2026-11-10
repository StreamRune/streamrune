package org.streamrune.runtime;

import java.time.Instant;
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
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * Projection processing: {@link PollingProjectionRunner} catching a {@code TRANSACTIONAL_LOCAL}
 * projection up over {@code batchSize} events of an {@link InMemoryEventStore}, as one batch. One
 * invocation is the cost of one batch: the read of the global stream, the batch handed through the
 * {@link org.streamrune.core.projection.AtomicBatchProcessor} to the projection, a read and a write
 * of a read model per event, and the checkpoint the batch commits with it. Precisely, the number is
 * one catch-up call of {@link PollingProjectionRunner#run}, so it includes what that method does
 * once per call besides the batch: the delivery-policy check of the projection and the processor,
 * and the INFO line naming the delivery mode, whose cost depends on the logging configuration on
 * the classpath (this module's {@code logback-test.xml} logs at WARN, so the line is filtered).
 *
 * <p>The processor and the offset store are one {@link InMemoryProjectionRepository}, the
 * Docker-free counterpart of {@code JdbcProjectionRepository}: the measured cost is the runner's
 * and the projection's own, without a database. The runner is the polling one because it returns
 * once the projection has caught up; the continuous runner of {@link MultiProjectionRunner} hands
 * its batches to the same processor and projection call, from a loop that runs until it is stopped.
 *
 * <p>Every invocation rewinds the checkpoint ({@link PollingProjectionRunner#reset}) and processes
 * the same events again. The projection writes each read model from its event alone, so the second
 * pass leaves the read models as the first left them, and every invocation sees the same state.
 *
 * <p>The annotations give a local run its settings; the nightly run overrides mode, unit and
 * iterations with {@code -PbenchCi} (root build). {@code ProjectionRunnerBenchmarkSmokeTest} runs
 * the method twice outside the harness, as part of {@code check}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ProjectionRunnerBenchmark {

  static final ProjectionName NAME = ProjectionName.of("order_summary");
  static final AggregateType TYPE = AggregateType.of("order");

  /** The customers the events are spread over, one stream each. */
  static final int CUSTOMERS = 10;

  public record OrderPlaced(String customerId, String orderId, long amountCents)
      implements DomainEvent {}

  /** The read model: a customer's latest order and how many orders the stream holds up to it. */
  public record CustomerOrders(
      String customerId, long orders, String lastOrderId, long lastAmount) {}

  /** Keeps one {@link CustomerOrders} per customer, read and written once per event. */
  public static final class CustomerOrdersProjection extends BaseProjection {

    public CustomerOrdersProjection(ProjectionRepository repository) {
      super(repository, NAME.value());
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        if (envelope.event() instanceof OrderPlaced placed) {
          long orders =
              findById(placed.customerId(), CustomerOrders.class)
                  .map(previous -> Math.max(previous.orders(), envelope.version().value()))
                  .orElse(envelope.version().value());
          save(
              placed.customerId(),
              new CustomerOrders(
                  placed.customerId(), orders, placed.orderId(), placed.amountCents()));
        }
      }
    }
  }

  @State(Scope.Benchmark)
  public static class RunnerState {

    @Param({"10", "100"})
    public int batchSize;

    InMemoryProjectionRepository repository;
    PollingProjectionRunner runner;
    CustomerOrdersProjection projection;

    @Setup(Level.Trial)
    public void appendEvents() {
      var store = new InMemoryEventStore();
      Map<StreamId, Long> heads = new HashMap<>();
      for (int i = 0; i < batchSize; i++) {
        String customerId = "customer-" + (i % CUSTOMERS);
        var stream = StreamId.of(TYPE, AggregateId.of(customerId));
        long head = heads.getOrDefault(stream, 0L);
        var event = new OrderPlaced(customerId, "order-" + i, 1_000L + i);
        store.append(stream, List.of(envelope(stream, head + 1, event)), new Version(head));
        heads.put(stream, head + 1);
      }
      repository = new InMemoryProjectionRepository();
      // fetchSize and batchSize both batchSize: the events arrive as one page and one batch.
      runner = new PollingProjectionRunner(store, repository, batchSize, batchSize, repository);
      projection = new CustomerOrdersProjection(repository);
    }
  }

  /** Rewinds the checkpoint and catches the projection up over {@code batchSize} events. */
  @Benchmark
  public GlobalOffset processBatch(RunnerState state) {
    state.runner.reset(NAME);
    state.runner.run(NAME, state.projection, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    return state.repository.getLastOffset(NAME);
  }

  private static EventEnvelope envelope(StreamId stream, long version, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(version),
        EventType.fromClass(event.getClass()),
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

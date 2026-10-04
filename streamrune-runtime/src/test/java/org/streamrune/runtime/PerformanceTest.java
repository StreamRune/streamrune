package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
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

/** Performance tests for StreamRune framework. */
class PerformanceTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  private EventStore store;
  private CommandBus bus;

  @BeforeEach
  void setup() {
    store = new InMemoryEventStore();

    bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(TYPE, TestCommand.class, cmd -> AggregateId.of(cmd.id()), new TestDecider())
            .build();
  }

  // ===== Throughput Tests =====

  @Test
  void commandBusShouldHandleHighThroughput() throws InterruptedException {
    int threads = 10;
    int commandsPerThread = 1000;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threads);
    AtomicInteger successCount = new AtomicInteger(0);

    // Warmup
    for (int i = 0; i < 100; i++) {
      bus.execute(new TestCommand("warmup-" + i));
    }

    long startTime = System.nanoTime();

    for (int t = 0; t < threads; t++) {
      final int threadNum = t;
      new Thread(
              () -> {
                try {
                  startLatch.await();
                  for (int i = 0; i < commandsPerThread; i++) {
                    bus.execute(new TestCommand("thread-" + threadNum + "-cmd-" + i));
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  doneLatch.countDown();
                }
              })
          .start();
    }

    startLatch.countDown();
    doneLatch.await(30, TimeUnit.SECONDS);

    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    int totalCommands = threads * commandsPerThread;
    double throughput = totalCommands / durationSeconds;

    System.out.println("\n=== Command Bus Throughput ===");
    System.out.println("Threads: " + threads);
    System.out.println("Commands: " + totalCommands);
    System.out.println("Duration: " + durationSeconds + "s");
    System.out.println("Throughput: " + String.format("%,.0f", throughput) + " commands/sec");

    assertTrue(throughput > 1000, "Should handle at least 1000 commands/sec");
  }

  @Test
  void eventStoreShouldHandleHighAppendThroughput() throws InterruptedException {
    int threads = 10;
    int appendsPerThread = 1000;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threads);
    AtomicInteger successCount = new AtomicInteger(0);

    long startTime = System.nanoTime();

    for (int t = 0; t < threads; t++) {
      final int threadNum = t;
      new Thread(
              () -> {
                try {
                  startLatch.await();
                  for (int i = 0; i < appendsPerThread; i++) {
                    StreamId streamId =
                        StreamId.of(TYPE, AggregateId.of("stream-" + threadNum + "-" + i));
                    var events = List.of(createEvent(streamId, i + 1));
                    store.append(streamId, events, Version.initial());
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                  doneLatch.countDown();
                }
              })
          .start();
    }

    startLatch.countDown();
    doneLatch.await(30, TimeUnit.SECONDS);

    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    int totalAppends = threads * appendsPerThread;
    double throughput = totalAppends / durationSeconds;

    System.out.println("\n=== Event Store Append Throughput ===");
    System.out.println("Threads: " + threads);
    System.out.println("Appends: " + totalAppends);
    System.out.println("Duration: " + durationSeconds + "s");
    System.out.println("Throughput: " + String.format("%,.0f", throughput) + " appends/sec");

    assertTrue(throughput > 1000, "Should handle at least 1000 appends/sec");
  }

  // ===== Latency Tests =====

  @RepeatedTest(10)
  void commandExecutionShouldHaveLowLatency() {
    int iterations = 10000;
    long[] latencies = new long[iterations];

    // Warmup
    for (int i = 0; i < 100; i++) {
      bus.execute(new TestCommand("warmup-" + i));
    }

    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      bus.execute(new TestCommand("latency-test-" + i));
      long end = System.nanoTime();
      latencies[i] = end - start;
    }

    long sum = 0;
    long max = 0;
    for (long latency : latencies) {
      sum += latency;
      max = Math.max(max, latency);
    }
    long avgNanos = sum / iterations;
    double avgMicros = avgNanos / 1000.0;

    // Calculate percentiles
    java.util.Arrays.sort(latencies);
    long p50 = latencies[iterations / 2];
    long p95 = latencies[(int) (iterations * 0.95)];
    long p99 = latencies[(int) (iterations * 0.99)];

    System.out.println("\n=== Command Execution Latency ===");
    System.out.println("Avg: " + String.format("%.2f", avgMicros) + " µs");
    System.out.println("P50: " + (p50 / 1000.0) + " µs");
    System.out.println("P95: " + (p95 / 1000.0) + " µs");
    System.out.println("P99: " + (p99 / 1000.0) + " µs");
    System.out.println("Max: " + (max / 1000.0) + " µs");

    assertTrue(avgMicros < 1000, "Average latency should be under 1ms");
  }

  @RepeatedTest(10)
  void eventStoreLoadShouldHaveLowLatency() {
    // Pre-populate store
    StreamId streamId = StreamId.of(TYPE, AggregateId.of("load-test-stream"));
    for (int i = 0; i < 100; i++) {
      var events = List.of(createEvent(streamId, i + 1));
      store.append(streamId, events, new Version(i));
    }

    int iterations = 10000;
    long[] latencies = new long[iterations];

    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      store.load(streamId);
      long end = System.nanoTime();
      latencies[i] = end - start;
    }

    long sum = 0;
    for (long latency : latencies) {
      sum += latency;
    }
    double avgMicros = (sum / iterations) / 1000.0;

    java.util.Arrays.sort(latencies);
    long p99 = latencies[(int) (iterations * 0.99)];

    System.out.println("\n=== Event Store Load Latency ===");
    System.out.println("Avg: " + String.format("%.2f", avgMicros) + " µs");
    System.out.println("P99: " + (p99 / 1000.0) + " µs");

    assertTrue(avgMicros < 500, "Average load latency should be under 500µs");
  }

  // ===== Concurrent Load Tests =====

  @Test
  void shouldHandleConcurrentReadsAndWrites() throws InterruptedException {
    int writeCount = 100;
    int readCount = 1000;
    CountDownLatch doneLatch = new CountDownLatch(writeCount + readCount);
    AtomicInteger writeSuccess = new AtomicInteger(0);
    AtomicInteger readSuccess = new AtomicInteger(0);

    // Writer threads - each writes to a different stream to avoid optimistic lock conflicts
    for (int i = 0; i < writeCount; i++) {
      final int streamNum = i;
      final int version = 0;
      new Thread(
              () -> {
                try {
                  StreamId streamId =
                      StreamId.of(TYPE, AggregateId.of("concurrent-stream-" + streamNum));
                  var events = List.of(createEvent(streamId, version + 1));
                  store.append(streamId, events, new Version(version));
                  writeSuccess.incrementAndGet();
                } finally {
                  doneLatch.countDown();
                }
              })
          .start();
    }

    // Reader threads - read from multiple streams
    for (int i = 0; i < readCount; i++) {
      final int streamNum = i % writeCount;
      new Thread(
              () -> {
                try {
                  StreamId streamId =
                      StreamId.of(TYPE, AggregateId.of("concurrent-stream-" + streamNum));
                  store.load(streamId);
                  readSuccess.incrementAndGet();
                } finally {
                  doneLatch.countDown();
                }
              })
          .start();
    }

    assertTrue(doneLatch.await(30, TimeUnit.SECONDS));

    System.out.println("\n=== Concurrent Read/Write ===");
    System.out.println("Writes: " + writeSuccess.get() + "/" + writeCount);
    System.out.println("Reads: " + readSuccess.get() + "/" + readCount);

    assertEquals(writeCount, writeSuccess.get());
    assertEquals(readCount, readSuccess.get());
  }

  // ===== Helper Methods =====

  private EventEnvelope createEvent(StreamId streamId, int version) {
    return new EventEnvelope(
        GlobalOffset.of(version),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("data-" + version),
        new org.streamrune.core.EventMetadata(
            org.streamrune.core.types.EventId.of("id-" + version),
            org.streamrune.core.types.CommandId.of("cmd-" + version),
            null,
            null,
            org.streamrune.core.types.CorrelationId.of("corr-" + version),
            null,
            null,
            Instant.now()));
  }

  // ===== Test Data =====

  public record TestCommand(String id) implements Command {}

  public record TestEvent(String data) implements DomainEvent {}

  public record TestState(String aggregateId) implements AggregateState {}

  public static class TestDecider implements Decider<TestCommand, TestState, TestEvent> {
    @Override
    public TestState initialState() {
      return new TestState("");
    }

    @Override
    public List<TestEvent> decide(TestCommand cmd, TestState state) {
      return List.of(new TestEvent("event-" + cmd.id()));
    }

    @Override
    public TestState evolve(TestState state, TestEvent event) {
      return state;
    }
  }
}

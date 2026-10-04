package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/** Performance tests for PostgreSQL EventStore. */
class PostgresPerformanceTest {

  static PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_perf");

  static EventStore store;
  static CommandBus bus;
  static com.zaxxer.hikari.HikariDataSource pool;

  @BeforeAll
  static void setup() {
    PG.start();

    // The application's pool: the store borrows from it directly.
    var config = new com.zaxxer.hikari.HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(10);
    pool = new com.zaxxer.hikari.HikariDataSource(config);

    var registry =
        org.streamrune.core.SimpleEventTypeRegistry.builder()
            .registerEvent("TestEvent", PgTestEvent.class)
            .registerState("TestState", PgTestState.class)
            .build();

    store = new PostgresEventStoreFactory(pool, registry).create();

    bus =
        org.streamrune.runtime.VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(
                TestStreams.TYPE,
                PgTestCommand.class,
                cmd -> AggregateId.of(cmd.id()),
                new PgTestDecider())
            .build();
  }

  @AfterAll
  static void teardown() {
    pool.close();
    PG.stop();
  }

  @Test
  void commandBusThroughputWithPostgres() throws InterruptedException {
    int threads = 10;
    int commandsPerThread = 500;
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
                  for (int i = 0; i < commandsPerThread; i++) {
                    bus.execute(new PgTestCommand("pg-thread-" + threadNum + "-cmd-" + i));
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
    doneLatch.await(60, TimeUnit.SECONDS);

    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    int totalCommands = threads * commandsPerThread;
    double throughput = totalCommands / durationSeconds;

    System.out.println("\n=== PostgreSQL Command Bus Throughput ===");
    System.out.println("Threads: " + threads);
    System.out.println("Commands: " + totalCommands);
    System.out.println("Duration: " + durationSeconds + "s");
    System.out.println("Throughput: " + String.format("%,.0f", throughput) + " commands/sec");

    assertTrue(throughput > 100, "Should handle at least 100 commands/sec with PostgreSQL");
  }

  @Test
  void eventStoreAppendWithPostgres() throws InterruptedException {
    int threads = 10;
    int appendsPerThread = 200;
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
                    StreamId streamId = TestStreams.stream("pg-stream-" + threadNum + "-" + i);
                    var events = List.of(createPgEvent(streamId, i + 1));
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
    doneLatch.await(60, TimeUnit.SECONDS);

    long endTime = System.nanoTime();
    double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
    int totalAppends = threads * appendsPerThread;
    double throughput = totalAppends / durationSeconds;

    System.out.println("\n=== PostgreSQL Event Store Append ===");
    System.out.println("Threads: " + threads);
    System.out.println("Appends: " + totalAppends);
    System.out.println("Duration: " + durationSeconds + "s");
    System.out.println("Throughput: " + String.format("%,.0f", throughput) + " appends/sec");

    assertTrue(throughput > 100, "Should handle at least 100 appends/sec with PostgreSQL");
  }

  @Test
  void singleCommandLatencyWithPostgres() {
    int iterations = 1000;
    long[] latencies = new long[iterations];

    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      bus.execute(new PgTestCommand("latency-pg-" + i));
      long end = System.nanoTime();
      latencies[i] = end - start;
    }

    long sum = 0;
    for (long latency : latencies) {
      sum += latency;
    }
    double avgMs = (sum / iterations) / 1_000_000.0;

    java.util.Arrays.sort(latencies);
    long p50 = latencies[iterations / 2];
    long p95 = latencies[(int) (iterations * 0.95)];
    long p99 = latencies[(int) (iterations * 0.99)];

    System.out.println("\n=== PostgreSQL Latency ===");
    System.out.println("Avg: " + String.format("%.2f", avgMs) + " ms");
    System.out.println("P50: " + String.format("%.2f", p50 / 1_000_000.0) + " ms");
    System.out.println("P95: " + String.format("%.2f", p95 / 1_000_000.0) + " ms");
    System.out.println("P99: " + String.format("%.2f", p99 / 1_000_000.0) + " ms");

    // PostgreSQL should be under 100ms average
    assertTrue(
        avgMs < 100,
        "Average latency should be under 100ms, was: " + String.format("%.2f", avgMs) + " ms");
  }

  private EventEnvelope createPgEvent(StreamId streamId, int version) {
    return new EventEnvelope(
        GlobalOffset.of(version),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new PgTestEvent("data-" + version),
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

  // Test data classes
  public record PgTestCommand(String id) implements org.streamrune.core.Command {}

  public record PgTestEvent(String data) implements DomainEvent {}

  public record PgTestState(String aggregateId) implements AggregateState {}

  public static class PgTestDecider implements Decider<PgTestCommand, PgTestState, PgTestEvent> {
    @Override
    public PgTestState initialState() {
      return new PgTestState("");
    }

    @Override
    public List<PgTestEvent> decide(PgTestCommand cmd, PgTestState state) {
      return List.of(new PgTestEvent("event-" + cmd.id()));
    }

    @Override
    public PgTestState evolve(PgTestState state, PgTestEvent event) {
      return state;
    }
  }
}

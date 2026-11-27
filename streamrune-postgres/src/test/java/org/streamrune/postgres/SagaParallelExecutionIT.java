package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.streamrune.postgres.CrashItSupport.dataSource;
import static org.streamrune.postgres.CrashItSupport.dataSourceForSchema;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.postgres.SagaParallelPostgresNodes.EndState;
import org.streamrune.postgres.SagaParallelPostgresNodes.Node;
import org.streamrune.postgres.SagaParallelPostgresNodes.Options;
import org.streamrune.postgres.SagaParallelPostgresNodes.Row;
import org.streamrune.postgres.SagaParallelPostgresNodes.Started;
import org.streamrune.test.RecordingStreamRuneMetrics;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * A saga type whose runner steps the sagas of a page eight at a time, on PostgreSQL: many sagas of
 * several steps, sagas whose steps command one aggregate, deadline claims landing while workers
 * step, and a connection pool smaller than the steps in flight need. Every saga ends; no command is
 * stored twice and no forward command is stored after its saga's first compensation command.
 */
@Testcontainers
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class SagaParallelExecutionIT {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("saga_parallel_execution");

  private static final String SERIAL_SCHEMA = "serial_run";
  private static final int MAX_CYCLES = 2_000;
  private static final int HELD_STEPS = 6;
  private static final Duration GATE_LIMIT = Duration.ofSeconds(30);

  static PGSimpleDataSource source;
  static PGSimpleDataSource serialSource;

  private SagaParallelPostgresNodes nodes;

  @BeforeAll
  static void schema() throws SQLException {
    source = dataSource(PG, "par-exec");
    new PostgresEventStoreFactory(source, SagaParallelPostgresNodes.REGISTRY).create();
    try (Connection conn = source.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SERIAL_SCHEMA);
    }
    serialSource = dataSourceForSchema(PG, "par-exec-serial", SERIAL_SCHEMA);
    new PostgresEventStoreFactory(serialSource, SagaParallelPostgresNodes.REGISTRY).create();
  }

  @BeforeEach
  void fresh() throws Exception {
    nodes = new SagaParallelPostgresNodes(source);
    nodes.clean();
  }

  @AfterEach
  void closeAll() {
    nodes.closeAll();
  }

  /**
   * Two hundred sagas of five steps each, eight steps at once: every saga completes, every stream
   * holds the events a serial run of the same sagas stores (on a second schema), every command was
   * stored under a key of its own, and more than one step was in flight at a time.
   */
  @Test
  void twoHundredSagasOfFiveStepsAllEnd_withEightStepsAtOnce() throws Exception {
    List<String> sagas = sagaIds(200);

    // Both runs go through a small pool: a physical connection per statement made this case the
    // slowest of the module without testing anything more.
    EndState serialEnd;
    try (HikariDataSource serialPool = pool(serialSource, "par-exec-serial-pool")) {
      SagaParallelPostgresNodes serial = new SagaParallelPostgresNodes(serialPool);
      try {
        serial.clean();
        serial.stepsToComplete = 5;
        sagas.forEach(id -> serial.append(new Started(id)));
        serialEnd = runToQuiescence(serial, serial.node(Options.defaults().withParallelism(1)));
      } finally {
        serial.closeAll();
      }
    }

    RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
    EndState end;
    try (HikariDataSource parallelPool = pool(source, "par-exec-parallel-pool")) {
      SagaParallelPostgresNodes parallel = new SagaParallelPostgresNodes(parallelPool);
      try {
        parallel.stepsToComplete = 5;
        sagas.forEach(id -> parallel.append(new Started(id)));
        end =
            runToQuiescence(
                parallel,
                parallel.node(Options.defaults().withParallelism(8).withMetrics(metrics)));
        assertThat(parallel.lateForwardCommands()).isEmpty();
      } finally {
        parallel.closeAll();
      }
    }

    assertAllEnd(end, sagas, Set.of(SagaStatus.COMPLETED));
    assertThat(end.eventsByAggregate())
        .as("every stream holds what the serial run stored")
        .isEqualTo(serialEnd.eventsByAggregate());
    assertThat(count(source, "SELECT count(*) FROM command_inbox"))
        .as("one inbox row per command: no key was used twice")
        .isEqualTo(sagas.size() * 5L);
    assertThat(metrics.maxSagaStepsInFlight()).isBetween(2, 8);
  }

  /**
   * A pool over {@code target} with room for eight steps in flight and the subscription's reads.
   */
  private static HikariDataSource pool(PGSimpleDataSource target, String name) {
    HikariConfig config = new HikariConfig();
    config.setDataSource(target);
    config.setPoolName(name);
    config.setMaximumPoolSize(12);
    return new HikariDataSource(config);
  }

  /**
   * Fifty sagas whose steps all command one stock aggregate: the bus serialises them on the
   * aggregate's lock, every saga completes, and no page fails on a lock it could not take.
   */
  @Test
  void fiftySagasOnOneAggregateAllEnd() throws Exception {
    List<String> sagas = sagaIds(50);
    RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
    nodes.stepsToComplete = 1;
    nodes.stepAggregate = "stock";
    sagas.forEach(id -> nodes.append(new Started(id)));
    EndState end =
        runToQuiescence(
            nodes, nodes.node(Options.defaults().withParallelism(8).withMetrics(metrics)));

    assertAllEnd(end, sagas, Set.of(SagaStatus.COMPLETED));
    assertThat(
            end.eventsByAggregate().entrySet().stream()
                .filter(e -> e.getKey().aggregateId().value().equals("stock"))
                .mapToInt(e -> e.getValue().size())
                .sum())
        .as("one stored event per saga on the stock aggregate")
        .isEqualTo(sagas.size());
    for (String stop :
        List.of(
            MetricNames.STOP_CONNECTION, MetricNames.STOP_THIRD_FAILURE, MetricNames.STOP_NONE)) {
      assertThat(metrics.sagaPageFailed(stop)).as("pages failed, stop " + stop).isZero();
    }
    assertThat(metrics.maxSagaStepsInFlight()).isGreaterThanOrEqualTo(2);
  }

  /**
   * Sagas of five steps with a deadline of 200 ms and a timeout runner polling every 50 ms that
   * waits at most 20 ms for a step in flight: claims land while workers step. The first few third
   * commands are held until their saga's row reads compensating or compensated (at most thirty
   * seconds), so the claim lands underneath that step whatever the speed of the machine; the claim
   * does not wait on the step, so the hold cannot deadlock. The forward command of a held step is
   * refused at its commit, which the test asserts happened. No saga is both compensated and
   * advanced — no forward command is stored after its saga's first compensation command — and the
   * timeout runner did claim.
   */
  @Test
  void deadlineClaimsWhileWorkersStep_noSagaIsBothCompensatedAndAdvanced() throws Exception {
    List<String> sagas = sagaIds(40);
    nodes.stepsToComplete = 5;
    sagas.forEach(id -> nodes.append(new Started(id)));
    RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
    // A pool keeps the timeout runner's first poll — the claim and compensation of every saga not
    // held — short on a slow machine: with a physical connection per statement it alone could
    // outlast the gate limit before the poll reached a held saga.
    HikariConfig config = new HikariConfig();
    config.setDataSource(source);
    config.setPoolName("par-exec-deadline-pool");
    config.setMaximumPoolSize(24);
    try (HikariDataSource pool = new HikariDataSource(config)) {
      runTheDeadlineCase(sagas, metrics, pool);
    }
  }

  private void runTheDeadlineCase(
      List<String> sagas, RecordingStreamRuneMetrics metrics, HikariDataSource pool)
      throws Exception {
    Node node =
        nodes.node(
            Options.defaults()
                .withSource(pool)
                .withMetrics(metrics)
                .withParallelism(8)
                .withDeadline(Duration.ofMillis(200), Clock.systemUTC(), Duration.ofMillis(50))
                .withClaimWait(Duration.ofMillis(20)));
    AtomicInteger commands = new AtomicInteger();
    AtomicInteger held = new AtomicInteger();
    AtomicBoolean gateExpired = new AtomicBoolean();
    node.crash.beforeCall =
        (method, sagaId) -> {
          if (method.equals("Step")
              && commands.incrementAndGet() % 3 == 0
              && held.incrementAndGet() <= HELD_STEPS) {
            long until = System.nanoTime() + GATE_LIMIT.toNanos();
            while (!claimed(sagaId)) {
              if (System.nanoTime() > until) {
                gateExpired.set(true);
                return;
              }
              sleep(Duration.ofMillis(10));
            }
          }
        };
    node.timeoutRunner.start();
    node.subscription.start();
    try {
      await()
          .atMost(Duration.ofSeconds(120))
          .pollInterval(Duration.ofMillis(200))
          .until(() -> allTerminal(sagas));
    } finally {
      node.subscription.close();
      node.timeoutRunner.close();
    }

    assertThat(gateExpired).as("a held step saw its saga claimed within the limit").isFalse();
    assertThat(nodes.lateForwardCommands()).isEmpty();
    assertThat(
            count(
                source,
                "SELECT count(*) FROM saga_state WHERE status IN ('COMPENSATING', 'COMPENSATED')"))
        .as("the timeout runner claimed (without a claim the invariant holds vacuously)")
        .isPositive();
    assertThat(metrics.count(MetricNames.SAGA_FORWARD_COMMANDS_FENCED))
        .as("a claim landed underneath a step, whose forward command was refused at its commit")
        .isPositive();
  }

  /**
   * Twenty sagas past their deadline, the compensation of the first one held until every other saga
   * has reached its terminal status (at most thirty seconds): the timeout runner claims the sagas
   * of its query in parallel on the runner's slots, so one slow compensation delays no other claim.
   * Every saga ends compensated; none is both compensated and advanced.
   */
  @Test
  void aSlowCompensationDelaysNoOtherDeadlineClaim() throws Exception {
    List<String> sagas = sagaIds(20);
    nodes.compensationCommands = 2;
    sagas.forEach(id -> nodes.append(new Started(id)));
    HikariConfig config = new HikariConfig();
    config.setDataSource(source);
    config.setPoolName("par-exec-slow-compensation-pool");
    config.setMaximumPoolSize(16);
    try (HikariDataSource pool = new HikariDataSource(config)) {
      Node node = nodes.node(Options.defaults().withSource(pool).withParallelism(4));
      runToQuiescence(nodes, node);
      List<String> others = sagas.subList(1, sagas.size());
      AtomicBoolean held = new AtomicBoolean();
      AtomicBoolean othersEndedWhileHeld = new AtomicBoolean();
      node.crash.beforeCall =
          (method, sagaId) -> {
            if (method.equals("Undo")
                && sagaId.equals(sagas.getFirst())
                && held.compareAndSet(false, true)) {
              long until = System.nanoTime() + GATE_LIMIT.toNanos();
              while (System.nanoTime() < until) {
                if (allTerminal(others)) {
                  othersEndedWhileHeld.set(true);
                  return;
                }
                sleep(Duration.ofMillis(10));
              }
            }
          };

      nodes.timeOut(node);

      assertThat(held).as("the first saga's compensation was held").isTrue();
      assertThat(othersEndedWhileHeld)
          .as("every other saga ended while the first one's compensation was held")
          .isTrue();
      for (String id : sagas) {
        assertThat(nodes.row(id).orElseThrow().status()).as(id).isEqualTo(SagaStatus.COMPENSATED);
      }
      assertThat(nodes.lateForwardCommands()).isEmpty();
    }
  }

  /**
   * A pool of four connections shared by the stores and the advisory locker, eight steps at once
   * and a connection timeout of 250 ms: steps fail on a connection they cannot get, which stops
   * their page, and the page is delivered again; each delivery finishes some steps, which the next
   * one drops on their rows, so every saga still ends.
   */
  @Test
  void aPoolSmallerThanTheBudgetFailsStepsRetryLater_andEverySagaStillEnds() throws Exception {
    // Few sagas of one step: with eight steps in flight on four connections most redeliveries end
    // in a timed-out connection again, so the page completes a step or two per delivery.
    List<String> sagas = sagaIds(16);
    RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();
    nodes.stepsToComplete = 1;
    sagas.forEach(id -> nodes.append(new Started(id)));
    HikariConfig config = new HikariConfig();
    config.setDataSource(dataSource(PG, "par-exec-pool"));
    config.setPoolName("par-exec-pool");
    config.setMaximumPoolSize(4);
    config.setConnectionTimeout(250);
    try (HikariDataSource pool = new HikariDataSource(config)) {
      assertThePoolCase(sagas, metrics, pool);
    } finally {
      nodes.closeAll();
    }
  }

  private void assertThePoolCase(
      List<String> sagas, RecordingStreamRuneMetrics metrics, HikariDataSource pool)
      throws Exception {
    EndState end =
        runToQuiescence(
            nodes,
            nodes.node(
                Options.defaults()
                    .withSource(pool)
                    .withLocker(PgAdvisoryLocker::new)
                    .withParallelism(8)
                    .withMetrics(metrics)));
    assertAllEnd(end, sagas, Set.of(SagaStatus.COMPLETED));
    assertThat(metrics.sagaPageFailed(MetricNames.STOP_CONNECTION))
        .as("pages stopped on a connection the pool could not give")
        .isPositive();
    assertThat(nodes.lateForwardCommands()).isEmpty();
  }

  // ==================== helpers ====================

  private static List<String> sagaIds(int count) {
    return IntStream.rangeClosed(1, count).mapToObj(n -> "s" + n).toList();
  }

  /** Every saga has a row in one of {@code statuses}, and every event was delivered. */
  private static void assertAllEnd(EndState end, List<String> sagas, Set<SagaStatus> statuses) {
    assertThat(end.rowsBySaga().keySet()).containsExactlyInAnyOrderElementsOf(sagas);
    for (Row row : end.rowsBySaga().values()) {
      assertThat(row.status()).as(row.stateSagaId()).isIn(statuses);
    }
  }

  private boolean claimed(String sagaId) {
    return nodes
        .row(sagaId)
        .map(r -> r.status() == SagaStatus.COMPENSATING || r.status() == SagaStatus.COMPENSATED)
        .orElse(false);
  }

  private boolean allTerminal(List<String> sagas) {
    return sagas.stream()
        .allMatch(id -> nodes.row(id).map(r -> r.status().isTerminal()).orElse(false));
  }

  /**
   * Runs poll cycles of {@code node} until one delivers nothing; a failed page is delivered again
   * on the next cycle. Returns the end state.
   */
  private static EndState runToQuiescence(SagaParallelPostgresNodes world, Node node)
      throws Exception {
    RuntimeException last = null;
    for (int cycle = 0; cycle < MAX_CYCLES; cycle++) {
      try {
        if (node.pollOnce() == 0) {
          assertThat(world.checkpoint())
              .as("every event delivered")
              .isEqualTo((long) world.stored().size());
          return world.endState();
        }
      } catch (RuntimeException failedPage) {
        last = failedPage;
      }
    }
    throw new AssertionError("no quiescence after " + MAX_CYCLES + " cycles", last);
  }

  private static long count(PGSimpleDataSource ds, String sql) throws SQLException {
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void sleep(Duration pause) {
    try {
      Thread.sleep(pause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}

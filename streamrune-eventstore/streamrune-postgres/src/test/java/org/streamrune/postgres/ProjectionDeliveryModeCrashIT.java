package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.postgres.CrashItSupport.CountView;
import org.streamrune.postgres.CrashItSupport.CountingProjection;
import org.streamrune.postgres.CrashItSupport.FailingOnceOffsetStore;
import org.streamrune.runtime.ContinuousProjectionRunner;
import org.streamrune.runtime.PollingProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The crash test between the read-model write and the checkpoint, for both delivery modes, on
 * PostgreSQL. "Crash" = {@code pg_terminate_backend} of the processor's transaction connection
 * while the updater is running (the connection is idle-in-transaction between the updater's
 * statements), observed from a second connection.
 *
 * <table>
 * <caption>Crash points pinned, and the convergence each test proves</caption>
 * <tr><th>test</th><th>crash point</th><th>durable state at the crash</th><th>rerun</th></tr>
 * <tr><td>(a) transactionalLocal_…</td><td>inside the one transaction</td><td>no read-model row, no checkpoint row</td>
 * <td>one application, checkpoint at the top</td></tr>
 * <tr><td>(b) atLeastOnce_nonAtomic_…</td><td>between the autocommit write and the checkpoint</td><td>the autocommit write
 * stands, checkpoint behind</td><td>re-applied: counter doubles (pinned), upsert unchanged, no
 * dead-letter row, checkpoint at the top</td></tr>
 * <tr><td>(c) atLeastOnce_underJdbcProcessor_…</td><td>between the projection's autocommit write and the checkpoint; the epoch fence</td><td>the projection's
 * autocommit write stands, the processor's checkpoint transaction rolled back; a pre-stamped higher
 * epoch rejects before the updater and nothing is written</td><td>a two-event batch: applied 2
 * after the crash, 4 after the rerun; in the fence case 2 (the new leader applies once)</td></tr>
 * <tr><td>(d) atLeastOnce_baseProjectionOver…</td><td>after the write to schema B, checkpoint in schema A</td><td>rows in B only,
 * checkpoint in A</td><td>re-applied to B (counter 2) — converges iff idempotent</td></tr>
 * </table>
 */
@Testcontainers
class ProjectionDeliveryModeCrashIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_crash_it");

  static PGSimpleDataSource processorDs; // application_name = crash-it-processor
  static PGSimpleDataSource ownDs; // application_name = crash-it-own (the projection's own writes)
  static PGSimpleDataSource killerDs; // application_name = crash-it-killer
  static PGSimpleDataSource schemaBDs; // currentSchema = b, application_name = crash-it-b

  @BeforeAll
  static void init() throws Exception {
    processorDs = CrashItSupport.dataSource(PG, "crash-it-processor");
    ownDs = CrashItSupport.dataSource(PG, "crash-it-own");
    killerDs = CrashItSupport.dataSource(PG, "crash-it-killer");
    org.flywaydb.core.Flyway.configure()
        .dataSource(processorDs)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
    try (var c = killerDs.getConnection();
        var st = c.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS b");
    }
    schemaBDs = CrashItSupport.dataSourceForSchema(PG, "crash-it-b", "b");
  }

  @BeforeEach
  void clean() throws Exception {
    try (var c = killerDs.getConnection();
        var st = c.createStatement()) {
      st.execute("DELETE FROM projection_offset");
      st.execute("DELETE FROM projection_dead_letters");
      st.execute("DROP SCHEMA b CASCADE");
      st.execute("CREATE SCHEMA b");
      try (var rs =
          st.executeQuery(
              "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                  + " AND tablename LIKE '%\\_view'")) {
        var views = new ArrayList<String>();
        while (rs.next()) {
          views.add(rs.getString(1));
        }
        for (var v : views) {
          st.execute("DROP TABLE IF EXISTS " + v);
        }
      }
    }
  }

  // ---------- (a) TRANSACTIONAL_LOCAL ----------

  @Test
  void transactionalLocal_crashBetweenWriteAndCommit_rollsBackBoth_rerunAppliesOnce() {
    var name = ProjectionName.of("tl_crash");
    var repo = new JdbcProjectionRepository(processorDs);
    var projection = new CountingProjection(repo, name.value());
    var batch = CrashItSupport.batch(1, 3);
    var offsets = new PostgresOffsetStore(processorDs);

    assertCrashedAtTheCheckpointSave(
        () ->
            repo.executeAtomically(
                name,
                batch,
                GlobalOffset.of(3),
                0L,
                tx -> {
                  projection.process(batch, tx); // the row is written on the tx connection
                  CrashItSupport.terminateIdleInTransaction(killerDs, "crash-it-processor");
                },
                offsets));

    // The processor creates the read-model table before the transaction opens (ensureTableExists
    // runs in autocommit), so the table survives the crash: what rolls back is the row and the
    // seeded checkpoint row.
    assertThat(repo.findById(name, "row", CountView.class))
        .as("the row was written inside the terminated transaction and is gone")
        .isEmpty();
    assertThat(CrashItSupport.rowCount(killerDs, "public", "tl_crash_view")).isZero();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());

    // rerun: the next poll applies the batch exactly once
    repo.executeAtomically(
        name, batch, GlobalOffset.of(3), 0L, tx -> projection.process(batch, tx), offsets);
    assertThat(repo.findById(name, "row", CountView.class).map(CountView::applied)).contains(3);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(3));
  }

  // ---------- (b) AT_LEAST_ONCE_IDEMPOTENT on nonAtomicAtLeastOnce() ----------

  @ParameterizedTest
  @EnumSource(ProjectionErrorStrategy.class)
  void atLeastOnce_nonAtomic_saveFailure_underEveryStrategy_isRetried_neverDeadLettered(
      ProjectionErrorStrategy strategy) throws Exception {
    var name = ProjectionName.of("alo_" + strategy.name().toLowerCase(Locale.ROOT));
    var own = new JdbcProjectionRepository(ownDs);
    var counter = new CountingProjection(own, name.value()); // NOT idempotent: pins the contract
    var upsert = new StatusProjection(own, name.value() + "_u"); // idempotent: the honest model
    var failingOnce = new FailingOnceOffsetStore(new PostgresOffsetStore(processorDs));
    var dlq = new PostgresProjectionDeadLetterStore(processorDs);
    var events = new InMemoryEventStore();
    CrashItSupport.appendPings(events, 2);

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(events)
            .offsetStore(failingOnce)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .batchSize(10)
            .errorStrategy(strategy)
            .deadLetterStore(dlq)
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(50)))
            .build();
    Projection both =
        batch -> {
          counter.process(batch);
          upsert.process(batch);
        };
    var runnerLog = attachRunnerLogAppender();
    Thread t = Thread.ofVirtual().start(() -> runner.run(name, both, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> failingOnce.getLastOffset(name).equals(GlobalOffset.of(2)));
    } finally {
      runner.close();
      t.join(10_000);
      detachRunnerLogAppender(runnerLog);
    }
    assertThat(t.isAlive()).as("runner stopped").isFalse();
    assertThat(runnerLog.list)
        .filteredOn(e -> e.getLevel() == Level.WARN)
        .extracting(ILoggingEvent::getFormattedMessage)
        .filteredOn(m -> m.contains("applied the batch up to 2 but the checkpoint save failed"))
        .as(
            "the save failure took the checkpoint-save arm, not the error strategy, under "
                + strategy)
        .hasSize(1);
    assertThat(own.findById(name, "row", CountView.class).map(CountView::applied))
        .as("pinned: a counter double-counts the redelivered batch")
        .contains(4);
    assertThat(own.findById(ProjectionName.of(name.value() + "_u"), "row", String.class))
        .as("an idempotent upsert converges")
        .contains("status-2");
    assertThat(dlq.countPending()).as("never dead-lettered under " + strategy).isZero();
    assertThat(failingOnce.saveAttempts.get()).isEqualTo(2);
  }

  // ---------- (c) AT_LEAST_ONCE_IDEMPOTENT under the JDBC processor ----------

  @Test
  void atLeastOnce_underJdbcProcessor_crashBeforeCommit_writeStands_checkpointDoesNot() {
    var name = ProjectionName.of("alo_jdbc_crash");
    var processor = new JdbcProjectionRepository(processorDs);
    var own = new JdbcProjectionRepository(ownDs);
    var projection = new CountingProjection(own, name.value());
    var batch = CrashItSupport.batch(1, 2);
    var offsets = new PostgresOffsetStore(processorDs);

    assertCrashedAtTheCheckpointSave(
        () ->
            processor.executeAtomically(
                name,
                batch,
                GlobalOffset.of(2),
                0L,
                tx -> {
                  // what the runner does for an at-least-once registration: handed null, the
                  // projection writes in autocommit through its own repository
                  projection.process(batch, null);
                  CrashItSupport.terminateIdleInTransaction(killerDs, "crash-it-processor");
                },
                offsets));
    assertThat(own.findById(name, "row", CountView.class).map(CountView::applied))
        .as("the projection's write stands")
        .contains(2);
    assertThat(offsets.getLastOffset(name))
        .as("the checkpoint did not move")
        .isEqualTo(GlobalOffset.initial());

    processor.executeAtomically(
        name, batch, GlobalOffset.of(2), 0L, tx -> projection.process(batch, null), offsets);
    assertThat(own.findById(name, "row", CountView.class).map(CountView::applied))
        .as("re-applied: 4 — converges iff idempotent")
        .contains(4);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(2));
  }

  @Test
  void atLeastOnce_underJdbcProcessor_staleEpochIsRejectedBeforeTheProjectionWrites() {
    var name = ProjectionName.of("alo_jdbc_fence");
    var processor = new JdbcProjectionRepository(processorDs);
    var own = new JdbcProjectionRepository(ownDs);
    var projection = new CountingProjection(own, name.value());
    var batch = CrashItSupport.batch(1, 2);
    var offsets = new PostgresOffsetStore(processorDs);
    processor.stampFencingEpoch(name, 2L); // the new leader took over

    assertThatThrownBy(
            () ->
                processor.executeAtomically(
                    name,
                    batch,
                    GlobalOffset.of(2),
                    1L,
                    tx -> projection.process(batch, null),
                    offsets))
        .isInstanceOf(ProjectionCommitFencedException.class);
    assertThat(own.findById(name, "row", CountView.class))
        .as("the fence runs before the updater")
        .isEmpty();

    processor.executeAtomically(
        name, batch, GlobalOffset.of(2), 2L, tx -> projection.process(batch, null), offsets);
    assertThat(own.findById(name, "row", CountView.class).map(CountView::applied)).contains(2);
  }

  // ---------- (d) BaseProjection over repository B, at-least-once, under processor A ----------

  @Test
  void
      atLeastOnce_baseProjectionOverSecondSchema_underProcessorA_writesOnlyToB_andReappliesAfterACrash() {
    var name = ProjectionName.of("alo_schema_b");
    var processorA = new JdbcProjectionRepository(processorDs);
    var repoB = new JdbcProjectionRepository(schemaBDs);
    var projection = new CountingProjection(repoB, name.value());
    var batch = CrashItSupport.batch(1, 1);
    var offsets = new PostgresOffsetStore(processorDs);

    assertCrashedAtTheCheckpointSave(
        () ->
            processorA.executeAtomically(
                name,
                batch,
                GlobalOffset.of(1),
                0L,
                tx -> {
                  projection.process(batch, null); // what the runner does for at-least-once
                  CrashItSupport.terminateIdleInTransaction(killerDs, "crash-it-processor");
                },
                offsets));
    assertThat(CrashItSupport.tableExists(killerDs, "b", "alo_schema_b_view")).isTrue();
    assertThat(CrashItSupport.rowCount(killerDs, "b", "alo_schema_b_view")).isEqualTo(1L);
    // Processor A's table bookkeeping creates an EMPTY alo_schema_b_view in its own schema before
    // its transaction (ensureTableExists); none of this projection's rows ever lands there.
    assertThat(CrashItSupport.rowCount(killerDs, "public", "alo_schema_b_view"))
        .as("none of this projection's rows lands in A's schema")
        .isZero();
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.initial());

    processorA.executeAtomically(
        name, batch, GlobalOffset.of(1), 0L, tx -> projection.process(batch, null), offsets);
    assertThat(repoB.findById(name, "row", CountView.class).map(CountView::applied)).contains(2);
    assertThat(offsets.getLastOffset(name)).isEqualTo(GlobalOffset.of(1));
    assertThat(CrashItSupport.rowCount(killerDs, "public", "alo_schema_b_view")).isZero();
  }

  @Test
  void atLeastOnce_baseProjectionOverInMemory_underJdbcProcessor_throughAPollingRunner() {
    var name = ProjectionName.of("alo_mem");
    var processorA = new JdbcProjectionRepository(processorDs);
    var memory = new InMemoryProjectionRepository();
    var projection = new CountingProjection(memory, name.value());
    var events = new InMemoryEventStore();
    CrashItSupport.appendPings(events, 3);
    var runner =
        new PollingProjectionRunner(
            events, new PostgresOffsetStore(processorDs), 100, 10, processorA);

    runner.run(name, projection, AT_LEAST_ONCE_IDEMPOTENT);

    assertThat(memory.findById(name, "row", CountView.class).map(CountView::applied)).contains(3);
    assertThat(CrashItSupport.rowCount(killerDs, "public", "alo_mem_view"))
        .as("the rows are in memory; processor A's schema holds none of them")
        .isZero();
    assertThat(new PostgresOffsetStore(processorDs).getLastOffset(name))
        .isEqualTo(GlobalOffset.of(3));
  }

  /**
   * The kill must have landed: the processor's next statement after the updater, the checkpoint
   * save, fails at the SQL level on the terminated connection and the processor wraps it. The
   * killer's own {@code IllegalStateException} (no idle-in-transaction backend found, so nothing
   * was terminated) is never wrapped, so the type alone tells a landed kill from a kill that never
   * happened; the SQLState pins the cause to the terminated backend.
   */
  private static void assertCrashedAtTheCheckpointSave(ThrowingCallable processorCall) {
    assertThatThrownBy(processorCall)
        .isInstanceOf(EventStoreException.class)
        .hasCauseInstanceOf(SQLException.class)
        .extracting(e -> ((SQLException) e.getCause()).getSQLState())
        .as("admin_shutdown (57P01) or connection_failure (08006) from the terminated backend")
        .isIn("57P01", "08006");
  }

  private static ListAppender<ILoggingEvent> attachRunnerLogAppender() {
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    ((Logger) LoggerFactory.getLogger(ContinuousProjectionRunner.class)).addAppender(appender);
    return appender;
  }

  private static void detachRunnerLogAppender(ListAppender<ILoggingEvent> appender) {
    ((Logger) LoggerFactory.getLogger(ContinuousProjectionRunner.class)).detachAppender(appender);
    appender.stop();
  }

  /** Idempotent companion for (b): sets the status to the last offset seen. */
  static final class StatusProjection extends BaseProjection {
    StatusProjection(ProjectionRepository repo, String name) {
      super(repo, name);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("row", "status-" + batch.getLast().globalOffset().value());
    }
  }
}

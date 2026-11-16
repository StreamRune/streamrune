package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresReadModelStore;
import org.streamrune.runtime.ContinuousProjectionRunner;

/**
 * A read-model store in a different database than the event store, under real leadership: the
 * leases stay with the events, and the epoch they issue is stamped into the read-model store's own
 * {@code readmodel_checkpoint} row, so a superseded leader is fenced in the store it writes to.
 *
 * <p>The event store, the leases and {@code subscription_checkpoint} live in database A (the shared
 * e2e database); a {@link PostgresReadModelStore} lives in database B, which holds the read-model
 * series only. Two leaders take turns on one {@code TRANSACTIONAL_LOCAL} projection routed to that
 * store:
 *
 * <ul>
 *   <li>leader 1 drains the stream; the checkpoint and the read model are in B, stamped with epoch
 *       1, and nothing about the projection is written to A's checkpoint tables;
 *   <li>leader 1 resigns, leader 2 takes over at epoch 2 and stamps it into B at takeover, before
 *       it commits anything;
 *   <li>a commit leader 1 still had in flight (epoch 1) is then rejected by B's row before it
 *       writes a read-model row: the checkpoint, the epoch and the read model stay as leader 2 left
 *       them.
 * </ul>
 */
@Timeout(120)
class ReadModelStoreOnAnotherDatabaseLeadershipIT extends E2ETestBase {

  private static final String OTHER_DATABASE = "readmodel_elsewhere";
  private static final ProjectionName PROJECTION = ProjectionName.of("elsewhere_projection");
  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("elsewhere"), AggregateId.of("elsewhere-stream"));

  private DataSource readModelDatabase;
  private PostgresReadModelStore store;

  @BeforeEach
  void freshReadModelDatabase() throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP DATABASE IF EXISTS " + OTHER_DATABASE + " WITH (FORCE)");
      stmt.execute("CREATE DATABASE " + OTHER_DATABASE);
    }
    var other = new PGSimpleDataSource();
    other.setServerNames(new String[] {POSTGRES.getHost()});
    other.setPortNumbers(new int[] {POSTGRES.getMappedPort(5432)});
    other.setDatabaseName(OTHER_DATABASE);
    other.setUser(POSTGRES.getUsername());
    other.setPassword(POSTGRES.getPassword());
    readModelDatabase = other;
    store =
        PostgresReadModelStore.create(
            "elsewhere",
            readModelDatabase,
            JdbcProjectionRepository.createObjectMapper(null),
            true,
            true);
  }

  @Test
  void theLeaseEpochIsStampedIntoTheStoresRow_andFencesASupersededLeaderThere() throws Exception {
    PostgresEventStore events = eventStore(dataSource);
    List<GlobalOffset> offsets = appendCountEvents(events, STREAM, 0, 0, 6);
    long top = offsets.getLast().value();
    CheckpointTarget target = store.checkpointTarget();

    // Leader 1: leases in database A, read model and checkpoint in database B.
    var leadership1 = new LeaseBasedLeadership(dataSource, Duration.ofMillis(400));
    var runner1 = newRunner(events, target, leadership1);
    Thread t1 =
        Thread.ofVirtual()
            .start(
                () ->
                    runner1.run(
                        PROJECTION,
                        new OffsetCountingProjection(PROJECTION),
                        ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(target.checkpoints().getLastOffset(PROJECTION).value())
                      .isEqualTo(top));
    } finally {
      runner1.close();
      t1.join(Duration.ofSeconds(10).toMillis());
      leadership1.close(); // resigns: the lease is up for grabs
    }
    assertThat(leaseEpoch()).as("leader 1 held the first epoch").isEqualTo(1L);
    assertThat(storedEpoch(readModelDatabase)).as("stamped into B's row").isEqualTo(1L);
    assertThat(checkpointRows(dataSource, "readmodel_checkpoint"))
        .as("nothing in A's read-model checkpoint table")
        .isZero();
    assertThat(checkpointRows(dataSource, "subscription_checkpoint"))
        .as("nothing in A's subscription checkpoint table")
        .isZero();
    assertThat(appliedByOffset().keySet()).hasSize(offsets.size());

    // Leader 2 takes over at epoch 2 and stamps it into B before it commits anything (the stream
    // is drained, so it never commits here).
    var leadership2 = new LeaseBasedLeadership(dataSource, Duration.ofSeconds(30));
    var runner2 = newRunner(events, target, leadership2);
    Thread t2 =
        Thread.ofVirtual()
            .start(
                () ->
                    runner2.run(
                        PROJECTION,
                        new OffsetCountingProjection(PROJECTION),
                        ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(() -> assertThat(storedEpoch(readModelDatabase)).isEqualTo(2L));
      assertThat(leaseEpoch()).as("the takeover bumped the lease epoch").isEqualTo(2L);

      // Leader 1's in-flight commit of the next range, under its superseded epoch 1.
      var stale = new OffsetCountingProjection(PROJECTION);
      var staleBatch = List.of(envelope(top + 1));
      assertThatThrownBy(
              () ->
                  target
                      .processor()
                      .executeAtomically(
                          PROJECTION,
                          staleBatch,
                          GlobalOffset.of(top + 1),
                          1L,
                          tx -> stale.process(staleBatch, tx),
                          target.checkpoints()))
          .as("B's checkpoint row carries epoch 2: the epoch-1 commit is fenced")
          .isInstanceOf(ProjectionCommitFencedException.class);
      assertThat(stale.totalProcessCalls.get()).as("fenced before the updater ran").isZero();
      assertThat(target.checkpoints().getLastOffset(PROJECTION).value()).isEqualTo(top);
      assertThat(storedEpoch(readModelDatabase)).isEqualTo(2L);
      assertThat(appliedByOffset().keySet())
          .as("the fenced range left no read-model row")
          .doesNotContain(top + 1);
    } finally {
      runner2.close();
      t2.join(Duration.ofSeconds(10).toMillis());
      leadership2.close();
    }
    assertThat(appliedByOffset().values())
        .as("every committed event applied exactly once")
        .allSatisfy(row -> assertThat(row.applyCount()).isEqualTo(1));
  }

  private static ContinuousProjectionRunner newRunner(
      PostgresEventStore events, CheckpointTarget target, LeaseBasedLeadership leadership) {
    return ContinuousProjectionRunner.builder()
        .eventStore(events)
        .offsetStore(target.checkpoints())
        .atomicProcessor(target.processor())
        .leadership(leadership)
        .fetchSize(2)
        .batchSize(2)
        .subscriptionConfig(fastConfig())
        .build();
  }

  private static org.streamrune.core.EventEnvelope envelope(long offset) {
    return new org.streamrune.core.EventEnvelope(
        GlobalOffset.of(offset),
        STREAM,
        new org.streamrune.core.types.Version(offset),
        new org.streamrune.core.types.EventType(CountEvent.TYPE),
        new CountEvent((int) offset),
        metadata((int) offset));
  }

  private Map<Long, AppliedRow> appliedByOffset() {
    return store.repository().findAll(PROJECTION, AppliedRow.class).stream()
        .collect(Collectors.toMap(AppliedRow::offset, row -> row));
  }

  /** The epoch the lease in database A carries for the projection. */
  private static long leaseEpoch() throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM subscription_leases WHERE consumer_name = ?")) {
      ps.setString(1, PROJECTION.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  /** The epoch on the projection's {@code readmodel_checkpoint} row in {@code database}. */
  private static long storedEpoch(DataSource database) throws SQLException {
    try (var conn = database.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM readmodel_checkpoint WHERE projection_name = ?")) {
      ps.setString(1, PROJECTION.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  /** The projection's rows in {@code table} of {@code database}. */
  private static int checkpointRows(DataSource database, String table) throws SQLException {
    try (var conn = database.getConnection();
        var ps =
            conn.prepareStatement("SELECT count(*) FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, PROJECTION.value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }
}

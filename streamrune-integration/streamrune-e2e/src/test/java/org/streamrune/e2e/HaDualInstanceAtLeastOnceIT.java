package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.e2e.ProjectionDeliveryModeChildMain.Count;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.ContinuousProjectionRunner;
import org.streamrune.runtime.ProjectionState;

/**
 * The {@code AT_LEAST_ONCE_IDEMPOTENT} contrast to {@link HaDualInstanceProjectionIT}, which keeps
 * asserting exactly one row per offset for {@code TRANSACTIONAL_LOCAL}. Two FENCED replicas (a
 * {@link LeaseBasedLeadership} each, as {@code TwoRunnerLeadershipFailoverIT} builds them; the JDBC
 * processor honours the epoch) run ONE at-least-once plain projection. The runner hands it no
 * transaction-scoped repository; it writes a {@link Count} row autocommit through its OWN {@link
 * JdbcProjectionRepository} — another object over the same {@code DataSource}.
 *
 * <p>The fence protects the checkpoint, not the read model. The epoch fence and the overlap guard
 * run under the {@code projection_offset} row lock BEFORE the projection's updater, so the two
 * replicas' updaters are serialised and a stale or overlapping batch is rejected before it is
 * applied; a range is re-applied only when a checkpoint transaction dies after the projection's
 * autocommit write, which this test does not induce. The checkpoint converges exactly, the row's
 * applied count is at least the number of events (here it equals it; the assertion stays at the
 * mode's promise), and no offset is lost.
 */
@Timeout(180)
class HaDualInstanceAtLeastOnceIT extends E2ETestBase {

  /**
   * A plain projection: records every offset it applied in memory and bumps the Count row
   * autocommit.
   */
  static final class AtLeastOnceCount implements Projection {
    final Set<Long> applied = ConcurrentHashMap.newKeySet();
    private final JdbcProjectionRepository own;
    private final ProjectionName name;

    AtLeastOnceCount(JdbcProjectionRepository own, ProjectionName name) {
      this.own = own;
      this.name = name;
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (var e : batch) {
        applied.add(e.globalOffset().value());
      }
      int before = own.findById(name, "row", Count.class).map(Count::applied).orElse(0);
      // Autocommit, outside the checkpoint transaction.
      own.save(name, "row", new Count("row", before + batch.size()));
    }
  }

  @Test
  void twoFencedReplicas_atLeastOnce_checkpointConverges_appliedAtLeastTotal_noOffsetLost()
      throws Exception {
    ProjectionName projection = ProjectionName.of("ha_alo_dual");
    StreamId stream = StreamId.of(AggregateType.of("ha"), AggregateId.of("ha-alo-stream"));
    int total = 40;

    PostgresEventStore store = eventStore(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    // Fences the checkpoint.
    JdbcProjectionRepository processor = new JdbcProjectionRepository(dataSource);
    // The projection's store: another object over the same DataSource.
    JdbcProjectionRepository own = new JdbcProjectionRepository(dataSource);

    List<GlobalOffset> offsets = appendCountEvents(store, stream, 0, 0, total);
    long topOffset = offsets.getLast().value();

    var projA = new AtLeastOnceCount(own, projection);
    var projB = new AtLeastOnceCount(own, projection);
    var leadershipA = new LeaseBasedLeadership(dataSource, Duration.ofMillis(400));
    var leadershipB = new LeaseBasedLeadership(dataSource, Duration.ofSeconds(30));
    var runnerA = newRunner(store, offsetStore, processor, leadershipA);
    var runnerB = newRunner(store, offsetStore, processor, leadershipB);

    Thread tA =
        Thread.ofVirtual()
            .start(
                () ->
                    runnerA.run(
                        projection, projA, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT));
    Thread tB =
        Thread.ofVirtual()
            .start(
                () ->
                    runnerB.run(
                        projection, projB, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT));
    try {
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofMillis(50))
          .untilAsserted(
              () -> assertThat(offsetStore.getLastOffset(projection).value()).isEqualTo(topOffset));
    } finally {
      runnerA.close();
      runnerB.close();
      tA.join(Duration.ofSeconds(15).toMillis());
      tB.join(Duration.ofSeconds(15).toMillis());
      leadershipA.close();
      leadershipB.close();
    }

    assertThat(runnerA.state()).isEqualTo(ProjectionState.STOPPED);
    assertThat(runnerB.state()).isEqualTo(ProjectionState.STOPPED);
    assertThat(offsetStore.getLastOffset(projection).value())
        .as("checkpoint converged")
        .isEqualTo(topOffset);

    int applied = own.findById(projection, "row", Count.class).map(Count::applied).orElse(0);
    assertThat(applied)
        .as(
            "at-least-once: every event applied at least once (a re-application needs a dying"
                + " checkpoint transaction, which this test does not induce)")
        .isGreaterThanOrEqualTo(total);

    var union = new HashSet<Long>();
    union.addAll(projA.applied);
    union.addAll(projB.applied);
    assertThat(union)
        .as("no loss: the union of both replicas' applications covers every offset")
        .containsExactlyInAnyOrderElementsOf(offsets.stream().map(GlobalOffset::value).toList());
  }

  private static ContinuousProjectionRunner newRunner(
      PostgresEventStore store,
      PostgresOffsetStore offsetStore,
      JdbcProjectionRepository processor,
      LeaseBasedLeadership leadership) {
    return ContinuousProjectionRunner.builder()
        .eventStore(store)
        .offsetStore(offsetStore)
        .atomicProcessor(processor)
        .leadership(leadership)
        .fetchSize(4)
        .batchSize(4)
        .subscriptionConfig(fastConfig())
        .build();
  }
}

package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.ContinuousProjectionRunner;
import org.streamrune.runtime.ProjectionState;

/**
 * High-availability dual-instance projection. Two {@link ContinuousProjectionRunner} instances run
 * the SAME projection name concurrently, sharing one {@link PostgresOffsetStore}, one event stream,
 * and one transactional read model (via {@link JdbcProjectionRepository}). This models two app
 * instances of the same service both running the projection for failover.
 *
 * <p>What is proven here (the genuine, non-vacuous HA invariant at the runner level):
 *
 * <ul>
 *   <li><b>Exactly-once effect:</b> the read model ends with exactly one row per global offset and
 *       the correct payload — no torn or duplicated rows — even though both instances may fetch and
 *       apply the same committed batch. This IT deliberately wires NO {@code leadership(...)}, so
 *       both runners run under {@code SubscriptionLeadership.NOOP} at epoch 0 — unfenced, by
 *       design, because the invariant under test is the at-least-once one the {@code Projection}
 *       contract mandates idempotency for, which this projection satisfies via offset-keyed upserts
 *       inside the atomic processor's transaction. (the claim that "StreamRune provides no
 *       projection-level leader election" is false — single-active-consumer leadership exists and
 *       is wired by every integration; the runner/varying-epoch composition is covered by {@code
 *       ProjectionRunnerLeadershipTest}'s re-acquisition tests and the DB fence seam by {@code
 *       TwoRunnerLeadershipFailoverIT}.)
 *   <li><b>No loss:</b> every offset is applied at least once across the two instances.
 *   <li><b>Offset convergence:</b> the shared committed offset reaches the stream's top offset.
 *   <li><b>Liveness:</b> both runners terminate cleanly on close — no deadlock or hang (bounded by
 *       the {@code @Timeout}).
 * </ul>
 *
 * <p>Non-vacuity: if a runner double-wrote a row in a way that broke idempotency, the read model
 * row count would exceed the event count; if an event were lost, a row would be missing / an offset
 * absent from the union of applied offsets; if a runner deadlocked, the offset would never converge
 * and the await would time out (failing the test). Each assertion fails under a real violation.
 */
@Timeout(180)
class HaDualInstanceProjectionIT extends E2ETestBase {

  @Test
  void twoRunnersShareOneOffsetStoreAndApplyEveryEventExactlyOnce() throws Exception {
    ProjectionName projection = ProjectionName.of("ha_dual_instance");
    StreamId stream = StreamId.of(AggregateType.of("ha"), AggregateId.of("ha-stream"));
    int total = 40;

    PostgresEventStore store = eventStore(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    JdbcProjectionRepository repo = new JdbcProjectionRepository(dataSource);

    List<GlobalOffset> offsets = appendCountEvents(store, stream, 0, 0, total);
    long topOffset = offsets.getLast().value();

    // Two independent runners — separate instances, separate projection objects (separate in-memory
    // counters), the SAME durable offset store and read model. Small fetch/batch so their poll
    // cycles genuinely interleave on the shared stream.
    var projA = new OffsetCountingProjection(projection);
    var projB = new OffsetCountingProjection(projection);
    var runnerA = newRunner(store, offsetStore, repo);
    var runnerB = newRunner(store, offsetStore, repo);

    Thread tA =
        Thread.ofVirtual()
            .start(
                () -> runnerA.run(projection, projA, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    Thread tB =
        Thread.ofVirtual()
            .start(
                () -> runnerB.run(projection, projB, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));

    try {
      // Offset convergence + liveness: the shared committed offset reaches the top. If either
      // runner deadlocked, this never happens and the test fails on timeout.
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
    }

    // Liveness: both run threads returned and ended in a clean STOPPED state.
    assertThat(tA.isAlive()).as("runner A terminated").isFalse();
    assertThat(tB.isAlive()).as("runner B terminated").isFalse();
    assertThat(runnerA.state()).isEqualTo(ProjectionState.STOPPED);
    assertThat(runnerB.state()).isEqualTo(ProjectionState.STOPPED);

    // Exactly-once effect: one row per offset, no extras, correct payloads.
    List<AppliedRow> applied = repo.findAll(projection, AppliedRow.class);
    assertThat(applied)
        .as("read model holds exactly one row per event — no duplicate or torn rows")
        .hasSize(total);
    assertThat(applied.stream().map(AppliedRow::offset).distinct().count()).isEqualTo(total);
    assertThat(applied.stream().map(AppliedRow::offset).sorted().toList())
        .isEqualTo(offsets.stream().map(GlobalOffset::value).sorted().toList());
    assertThat(applied.stream().map(AppliedRow::n).sorted().toList())
        .as("every CountEvent payload landed — none lost")
        .isEqualTo(java.util.stream.IntStream.range(0, total).boxed().toList());

    // No loss: the union of in-memory applications across both instances covers every offset
    // (each applied at least once). At-least-once is the runner-level guarantee here.
    var appliedAcrossBoth = new java.util.HashSet<Long>();
    appliedAcrossBoth.addAll(projA.inMemoryApplications.keySet());
    appliedAcrossBoth.addAll(projB.inMemoryApplications.keySet());
    assertThat(appliedAcrossBoth)
        .as("every offset was applied by at least one instance (no loss)")
        .containsExactlyInAnyOrderElementsOf(offsets.stream().map(GlobalOffset::value).toList());

    // Final committed offset is correct.
    assertThat(offsetStore.getLastOffset(projection).value()).isEqualTo(topOffset);
  }

  private static ContinuousProjectionRunner newRunner(
      PostgresEventStore store, PostgresOffsetStore offsetStore, JdbcProjectionRepository repo) {
    return ContinuousProjectionRunner.builder()
        .eventStore(store)
        .offsetStore(offsetStore)
        .atomicProcessor(repo)
        .fetchSize(4)
        .batchSize(4)
        .subscriptionConfig(fastConfig())
        .build();
  }
}

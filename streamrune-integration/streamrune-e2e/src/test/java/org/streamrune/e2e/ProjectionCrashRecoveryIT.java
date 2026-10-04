package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
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
 * Runner-level crash recovery: a {@link ContinuousProjectionRunner} processes part of a stream and
 * commits its offset into a real {@link PostgresOffsetStore}; the runner is then "crashed"
 * (closed); a FRESH runner is started against the SAME offset store; the rest of the stream is
 * appended; and we assert every event is applied EXACTLY ONCE — the fresh runner resumes from the
 * persisted offset rather than replaying from zero.
 *
 * <p>Non-vacuity: the read model holds one row per global offset with an {@code applyCount}.
 * Because there is no concurrency in this scenario, a correct resume writes {@code applyCount == 1}
 * for every offset. If the fresh runner restarted from offset 0 it would re-apply the
 * already-committed events, the {@code applyCount} for those offsets would become 2, and the
 * assertions below fail.
 */
@Timeout(120)
class ProjectionCrashRecoveryIT extends E2ETestBase {

  @Test
  void freshRunnerResumesFromCommittedOffsetAndAppliesEveryEventExactlyOnce() throws Exception {
    ProjectionName projection = ProjectionName.of("crash_recovery");
    StreamId stream = StreamId.of(AggregateType.of("crash"), AggregateId.of("crash-stream"));

    PostgresEventStore store = eventStore(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    // JdbcProjectionRepository is BOTH the read-model repository and the transactional
    // AtomicBatchProcessor: each batch's projection writes + the offset save commit together.
    JdbcProjectionRepository repo = new JdbcProjectionRepository(dataSource);

    // ---- Phase 1: append the first half, process it, commit the offset, then crash. ----
    int firstHalf = 6;
    List<GlobalOffset> firstOffsets = appendCountEvents(store, stream, 0, 0, firstHalf);
    long firstHalfTopOffset = firstOffsets.getLast().value();

    var proj1 = new OffsetCountingProjection(projection);
    var runner1 =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsetStore)
            .atomicProcessor(repo)
            .fetchSize(2) // small pages so progress is observable mid-stream
            .batchSize(2)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t1 =
        Thread.ofVirtual()
            .start(
                () -> runner1.run(projection, proj1, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    // Wait until the committed offset reaches the top of the first half — all 6 are durably
    // applied.
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(offsetStore.getLastOffset(projection).value())
                    .isEqualTo(firstHalfTopOffset));

    // "Crash" the first instance: close() interrupts the run thread and waits for run() to return.
    runner1.close();
    t1.join(Duration.ofSeconds(10).toMillis());
    assertThat(t1.isAlive()).isFalse();
    assertThat(runner1.state()).isEqualTo(ProjectionState.STOPPED);

    // Sanity: the persisted offset survived the crash and each of the 6 offsets applied once.
    assertThat(offsetStore.getLastOffset(projection).value()).isEqualTo(firstHalfTopOffset);
    assertThat(readApplied(repo, projection))
        .allSatisfy(r -> assertThat(r.applyCount()).isEqualTo(1));

    // ---- Phase 2: append the second half, start a FRESH runner against the SAME offset store.
    // ----
    int secondHalf = 6;
    int total = firstHalf + secondHalf;
    List<GlobalOffset> secondOffsets =
        appendCountEvents(store, stream, firstHalf, firstHalf, secondHalf);
    long finalOffset = secondOffsets.getLast().value();

    // A brand-new projection instance (empty in-memory counters) and a brand-new runner — only the
    // durable PostgresOffsetStore carries state across the "restart".
    var proj2 = new OffsetCountingProjection(projection);
    var runner2 =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsetStore)
            .atomicProcessor(repo)
            .fetchSize(2)
            .batchSize(2)
            .subscriptionConfig(fastConfig())
            .build();

    Thread t2 =
        Thread.ofVirtual()
            .start(
                () -> runner2.run(projection, proj2, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL));
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(offsetStore.getLastOffset(projection).value()).isEqualTo(finalOffset));
    } finally {
      runner2.close();
      t2.join(Duration.ofSeconds(10).toMillis());
    }

    // ---- Assertions: exactly once, no loss, no replay-from-zero. ----
    List<AppliedRow> applied = readApplied(repo, projection);

    // 1. One row per event — no gaps, no extras: exactly `total` distinct offsets.
    assertThat(applied).hasSize(total);
    assertThat(applied.stream().map(AppliedRow::offset).distinct().count())
        .as("every offset appears exactly once in the read model")
        .isEqualTo(total);

    // 2. EXACTLY ONCE: no committed event was applied more than once. This is the assertion that
    //    fails if the fresh runner replayed the first half from offset 0 (those rows -> applyCount
    // 2).
    assertThat(applied)
        .as("each event applied exactly once across the crash/restart")
        .allSatisfy(r -> assertThat(r.applyCount()).isEqualTo(1));

    // 3. The fresh instance only touched the SECOND half in memory — proof it resumed from the
    //    saved offset and did NOT reprocess the first half.
    assertThat(proj2.inMemoryApplications.keySet())
        .as("the fresh runner processed only the offsets appended after the crash")
        .containsExactlyInAnyOrderElementsOf(
            secondOffsets.stream().map(GlobalOffset::value).toList());

    // 4. The CountEvent payloads (n = 0..total-1) all landed, none lost.
    assertThat(applied.stream().map(AppliedRow::n).sorted().toList())
        .isEqualTo(java.util.stream.IntStream.range(0, total).boxed().toList());

    // 5. Final committed offset equals the total event count's top offset.
    assertThat(offsetStore.getLastOffset(projection).value()).isEqualTo(finalOffset);
  }

  private static List<AppliedRow> readApplied(
      ProjectionRepository repo, ProjectionName projection) {
    return new ArrayList<>(repo.findAll(projection, AppliedRow.class));
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.ForwardingOutboxStore;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * Crash points of the outbox ordering modes and where each is pinned. Every durable write is one
 * statement; "rerun" is the next poll, the next lease reclaim, or the operator repeating the call.
 * The {@code CP-n} labels simply number the rows of this table (crash point n); they mean nothing
 * outside it. Each row names the test that pins that crash point.
 *
 * <pre>
 * CP-1  claim committed, crash before publish      OutboxPollerTest.crashAfterPublishBeforeDelivered_entryStaysClaimable_andIsRedelivered_atLeastOnce (unchanged)
 * CP-2  publish done, crash before markFailed      same pin (at-least-once; the ladder commits on the rerun)
 * CP-3  markFailed committed, crash before log      THIS CLASS: blockageIsRecomputedFromRows_onTheNextRelaysFirstCycle
 * CP-4  markFailed loses its lease                  OutboxPollerTest.processBatch_lostLeaseOnFailure_isBenignNoOp_doesNotThrow (unchanged)
 * CP-5  replay(id) rerun                            OutboxFailedReplayerTest.replay_single_outcomes
 * CP-6  replayFailed(max) crash after k resets      THIS CLASS: bulkReplay_rerunAfterPartialRun_resetsOnlyRowsStillFailed
 * CP-7  skip(id) rerun                              OutboxFailedReplayerTest.skip_outcomes_andAuditOnTheRow
 * CP-8  skip vs replay race                         PostgresOutboxStoreStrictOrderingIT.skipVsReplay_race_exactlyOneResolution; OutboxStoreContract.skipThenReplay_andReplayThenSkip_exactlyOneResolutionStands_bothModes
 * CP-9  deleteSkipped crash mid-chunk               PostgresOutboxStoreTest.deleteSkipped_batchesPastTheChunkThreshold (chunks are atomic; batch > 0 loop)
 * CP-10 mode switch AF → strict over one table      OutboxStoreContract.failedRowBelowAPendingRow_blocksAfterSwitchToStrict_andIsReleasedBySkip, nullAggregateLegacyRows_stillDrain_afterSwitchToStrict; OutboxPollerTest.firstCycle_strict_warnsOnceAboutLegacyNullAggregateFailedRows_evenWithNoopMetrics
 * CP-11 strict append with a null-aggregate mapper  PostgresEventStoreOutboxIT.append_strictNullAggregateMapper_rollsBack_surfacesOrderingViolationUnwrapped
 * CP-12 skipFailed(max) crash after k skips         OutboxFailedReplayerTest.skipFailed_bulk_rerunAfterPartialRun_skipsOnlyRowsStillFailed
 * </pre>
 */
class OutboxOrderingCrashPointTest {

  private static final StreamId A = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-cp"));

  @Test
  void blockageIsRecomputedFromRows_onTheNextRelaysFirstCycle() {
    // The relay that committed markFailed dies before its ERROR line and its delivery_failed
    // sample. The blockage must not be invisible: a fresh relay over the same rows samples it on
    // its
    // first cycle from the rows alone.
    var store = new InMemoryOutboxStore(); // strict
    store.save(OutboxEntry.pending(OutboxEntryId.of("n0"), "{}", "E", A));
    store.save(OutboxEntry.pending(OutboxEntryId.of("n1"), "{}", "E", A));
    assertThat(store.loadPending(10)).extracting(e -> e.id().value()).containsExactly("n0");
    assertThat(store.markFailed(OutboxEntryId.of("n0"), 10, "poison", store.claimedBy())).isTrue();
    // (crash here: no log, no metric from the dying relay)

    var metrics = new OutboxPollerTest.RecordingMetrics();
    var freshRelay =
        OutboxPoller.builder()
            .outboxStore(store.withOrderingMode(store.orderingMode()))
            .publisher(entry -> {})
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(1), 2.0, false))
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();
    freshRelay.processBatch();
    assertThat(metrics.lastBlockedAggregates).isEqualTo(1);
    assertThat(metrics.lastBlockageAge).isGreaterThanOrEqualTo(0);
    assertThat(store.findById(OutboxEntryId.of("n1")).orElseThrow().status())
        .isEqualTo(OutboxStatus.PENDING);
  }

  @Test
  void bulkReplay_rerunAfterPartialRun_resetsOnlyRowsStillFailed() {
    var store =
        new InMemoryOutboxStore(
            java.time.Clock.systemUTC(),
            InMemoryOutboxStore.DEFAULT_CLAIM_LEASE,
            org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST);
    for (int i = 1; i <= 4; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("r" + i), "{}", "E"));
    }
    assertThat(store.loadPending(10)).hasSize(4);
    for (int i = 1; i <= 4; i++) {
      assertThat(store.markFailed(OutboxEntryId.of("r" + i), 10, "poison", store.claimedBy()))
          .isTrue();
    }
    var crashAfterTwo =
        new ForwardingOutboxStore(store) {
          int resets;

          @Override
          public boolean resetFailedToPending(OutboxEntryId id) {
            if (++resets == 3) {
              throw new IllegalStateException("crash after the 2nd committed reset");
            }
            return super.resetFailedToPending(id);
          }
        };
    assertThrows(
        IllegalStateException.class,
        () -> new OutboxFailedReplayer(crashAfterTwo, StreamRuneMetrics.NOOP).replayFailed(10));
    assertThat(store.findByStatus(OutboxStatus.PENDING, 10))
        .extracting(e -> e.id().value())
        .containsExactly("r1", "r2");
    assertThat(store.findByStatus(OutboxStatus.FAILED, 10))
        .extracting(e -> e.id().value())
        .containsExactly("r3", "r4");

    // Rerun: enumerates only r3, r4 (r1, r2 are no longer FAILED); nothing is reset twice.
    int rerun = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP).replayFailed(10);
    assertThat(rerun).isEqualTo(2);
    assertThat(store.findByStatus(OutboxStatus.FAILED, 10)).isEmpty();
    assertThat(store.findByStatus(OutboxStatus.PENDING, 10))
        .extracting(e -> e.id().value())
        .containsExactly("r1", "r2", "r3", "r4");
    assertThat(store.findByStatus(OutboxStatus.PENDING, 10))
        .allSatisfy(e -> assertThat(e.attempts()).isZero());
  }

  @Test
  void crashPointTable_namesEveryPinnedTest() throws Exception {
    // Keeps the javadoc table honest: every test it cites must exist in this module.
    for (var ref :
        List.of(
            "OutboxPollerTest#crashAfterPublishBeforeDelivered_entryStaysClaimable_andIsRedelivered_atLeastOnce",
            "OutboxPollerTest#processBatch_lostLeaseOnFailure_isBenignNoOp_doesNotThrow",
            "OutboxPollerTest#firstCycle_strict_warnsOnceAboutLegacyNullAggregateFailedRows_evenWithNoopMetrics",
            "OutboxFailedReplayerTest#replay_single_outcomes",
            "OutboxFailedReplayerTest#skip_outcomes_andAuditOnTheRow",
            "OutboxFailedReplayerTest#skipFailed_bulk_rerunAfterPartialRun_skipsOnlyRowsStillFailed")) {
      var cls = Class.forName("org.streamrune.runtime." + ref.substring(0, ref.indexOf('#')));
      assertThat(cls.getDeclaredMethod(ref.substring(ref.indexOf('#') + 1))).as(ref).isNotNull();
    }
  }
}

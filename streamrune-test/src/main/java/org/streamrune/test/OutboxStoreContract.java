package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

/**
 * The ordering-mode contract every {@link OutboxStore} must honour. Run by {@code
 * InMemoryOutboxStoreContractTest} and {@code PostgresOutboxStoreContractTest}, so the two shipped
 * stores cannot diverge in outcome: every status transition, both modes' head rules, the delete
 * guard (delete refuses FAILED), retention (never deletes FAILED), the strict null-aggregate
 * rejection, position-preserving replay, the audited skip, the blockage sample and the read-only
 * lookups. Cases where the mode matters exist once per mode and share a helper, so a reader sees
 * both behaviours side by side.
 */
public abstract class OutboxStoreContract {

  /** A fresh, empty store enforcing {@code mode}. */
  protected abstract OutboxStore newStore(OutboxOrderingMode mode);

  /**
   * A second relay over the SAME data as {@code store}, enforcing {@code mode} — the operational
   * event of restarting a channel with a different mode over one table.
   */
  protected abstract OutboxStore sameDataWithMode(OutboxStore store, OutboxOrderingMode mode);

  protected static final StreamId A = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-A"));
  protected static final StreamId B = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-B"));

  /** The same id value as {@link #A}, under another aggregate type: a different stream. */
  protected static final StreamId A_OTHER_TYPE =
      StreamId.of(AggregateType.of("other"), AggregateId.of("agg-A"));

  protected static OutboxEntry pending(String id, StreamId stream) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{\"k\":1}", "Event", stream);
  }

  protected static OutboxEntry pendingFree(String id) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{\"k\":1}", "Event");
  }

  protected static List<String> claimIds(OutboxStore store) {
    return store.loadPending(100).stream().map(e -> e.id().value()).toList();
  }

  protected static OutboxStatus statusOf(OutboxStore store, String id) {
    return store.findById(OutboxEntryId.of(id)).orElseThrow().status();
  }

  /** Claims {@code id} (it must be the only claimable row) and marks it terminal FAILED. */
  protected static void failHead(OutboxStore store, String id) {
    assertEquals(List.of(id), claimIds(store), "the head must be the only claimable row");
    assertTrue(store.markFailed(OutboxEntryId.of(id), 10, "poison", store.claimedBy()));
    assertEquals(OutboxStatus.FAILED, statusOf(store, id));
  }

  // ── Head-only claim, per mode ──────────────────────────────────────────────────────────

  @Test
  public void claimReturnsOnlyTheLowestSeqHeadOfEachAggregate_strict() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("a1", A));
    store.save(pending("a2", A));
    store.save(pending("b1", B));
    assertEquals(List.of("a1", "b1"), claimIds(store));
    assertTrue(claimIds(store).isEmpty(), "successors are gated behind IN_PROGRESS heads");
    assertTrue(store.markDelivered(OutboxEntryId.of("a1"), store.claimedBy()));
    assertEquals(List.of("a2"), claimIds(store));
  }

  @Test
  public void claimReturnsOnlyTheLowestSeqHeadOfEachAggregate_availabilityFirst_freeNullRows() {
    var store = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    store.save(pending("a1", A));
    store.save(pending("a2", A));
    store.save(pendingFree("n1"));
    store.save(pendingFree("n2"));
    // One head per aggregate plus every free null-aggregate row, in seq order.
    assertEquals(List.of("a1", "n1", "n2"), claimIds(store));
  }

  @Test
  public void transientFailureOnTheHeadHoldsTheWholeAggregate_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("h1", A));
      store.save(pending("h2", A));
      assertEquals(List.of("h1"), claimIds(store), mode.name());
      assertTrue(
          store.markRetry(
              OutboxEntryId.of("h1"), 1, "broker down", Duration.ofHours(1), store.claimedBy()));
      assertTrue(claimIds(store).isEmpty(), mode + ": a backing-off head holds its aggregate");
    }
  }

  @Test
  public void onceTheHeadIsEligibleAgain_itIsClaimedBeforeItsSuccessor_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("h1", A));
      store.save(pending("h2", A));
      assertEquals(List.of("h1"), claimIds(store));
      // A non-positive backoff makes the head immediately eligible again (store clock).
      assertTrue(
          store.markRetry(
              OutboxEntryId.of("h1"), 1, "blip", Duration.ofSeconds(-1), store.claimedBy()));
      assertEquals(List.of("h1"), claimIds(store), mode + ": the head, not its successor");
    }
  }

  // ── The FAILED head: blocks in strict mode, is skipped past in availability-first mode ────

  @Test
  public void failedHead_blocksSuccessors_untilResolved_strict() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("n0", A));
    store.save(pending("n1", A));
    store.save(pending("n2", A));
    store.save(pending("b1", B));
    assertEquals(List.of("n0", "b1"), claimIds(store));
    assertTrue(store.markDelivered(OutboxEntryId.of("b1"), store.claimedBy()));
    assertTrue(store.markFailed(OutboxEntryId.of("n0"), 10, "poison", store.claimedBy()));
    // Strict mode: a FAILED head keeps every later row of A unclaimable — poll after poll.
    assertTrue(claimIds(store).isEmpty(), "nothing of A while n0 is FAILED");
    assertTrue(claimIds(store).isEmpty(), "still nothing on the next poll");
    assertEquals(OutboxStatus.PENDING, statusOf(store, "n1"));
    assertEquals(OutboxStatus.PENDING, statusOf(store, "n2"));
  }

  @Test
  public void twoStreamsSharingAnIdValue_haveIndependentHeads_strict() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("p1", A));
    store.save(pending("i1", A_OTHER_TYPE));
    store.save(pending("i2", A_OTHER_TYPE));
    assertEquals(List.of("p1", "i1"), claimIds(store), "one head per stream, not per id value");
    assertTrue(store.markFailed(OutboxEntryId.of("p1"), 10, "poison", store.claimedBy()));
    assertTrue(store.markDelivered(OutboxEntryId.of("i1"), store.claimedBy()));
    assertEquals(
        List.of("i2"), claimIds(store), "a FAILED head of one type never blocks another type");
    var sample = store.sampleBlockage();
    assertEquals(1, sample.failedAggregates());
  }

  @Test
  public void sampleBlockage_countsTwoTypesSharingAnIdValueAsTwoStreams() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("p1", A));
    store.save(pending("i1", A_OTHER_TYPE));
    assertEquals(List.of("p1", "i1"), claimIds(store));
    assertTrue(store.markFailed(OutboxEntryId.of("p1"), 10, "p", store.claimedBy()));
    assertTrue(store.markFailed(OutboxEntryId.of("i1"), 10, "p", store.claimedBy()));
    assertEquals(2, store.sampleBlockage().failedAggregates());
  }

  @Test
  public void failedHead_doesNotGateSuccessors_availabilityFirst_gapAndReorderUntilReplay() {
    // Today's Postgres pin terminalFailedHeadDoesNotGateSuccessors_gapAndReorderUntilReplay, now
    // the documented behaviour of the opt-in mode only.
    var store = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    store.save(pending("f5", A));
    store.save(pending("f6", A));
    failHead(store, "f5");
    assertEquals(List.of("f6"), claimIds(store), "f6 is delivered past the FAILED f5 (gap)");
    assertTrue(store.markDelivered(OutboxEntryId.of("f6"), store.claimedBy()));
    assertTrue(store.resetFailedToPending(OutboxEntryId.of("f5")));
    assertEquals(List.of("f5"), claimIds(store), "replay arrives after its successor (reorder)");
  }

  // ── Replay preserves position ───────────────────────────────────────────────────────────

  @Test
  public void replay_returnsHeadToPending_atOriginalPosition_deliveredBeforeSuccessors_strict() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("n0", A));
    store.save(pending("n1", A));
    failHead(store, "n0");
    assertTrue(store.resetFailedToPending(OutboxEntryId.of("n0")));
    var replayed = store.findById(OutboxEntryId.of("n0")).orElseThrow();
    assertEquals(OutboxStatus.PENDING, replayed.status());
    assertEquals(0, replayed.attempts());
    assertNull(replayed.lastError());
    assertNull(replayed.nextRetryAt());
    assertEquals(List.of("n0"), claimIds(store), "the replayed head goes first");
    assertTrue(store.markDelivered(OutboxEntryId.of("n0"), store.claimedBy()));
    assertEquals(List.of("n1"), claimIds(store), "then its successor");
  }

  @Test
  public void replay_onNonFailed_returnsFalse_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("p1", A));
      assertFalse(store.resetFailedToPending(OutboxEntryId.of("p1")), mode + ": PENDING");
      assertFalse(store.resetFailedToPending(OutboxEntryId.of("absent")), mode + ": absent");
      assertEquals(OutboxStatus.PENDING, statusOf(store, "p1"));
    }
  }

  // ── The audited skip and the delete guard ──────────────────────────────────────

  @Test
  public void skip_releasesSuccessors_andRecordsAudit_strict() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    store.save(pending("n0", A));
    store.save(pending("n1", A));
    failHead(store, "n0");
    assertTrue(claimIds(store).isEmpty(), "blocked");
    Instant before = Instant.now().minusSeconds(5);

    assertTrue(store.skipFailed(OutboxEntryId.of("n0"), "ops-user", "payload rejected by schema"));

    var skipped = store.findById(OutboxEntryId.of("n0")).orElseThrow();
    assertEquals(OutboxStatus.SKIPPED, skipped.status());
    assertNotNull(skipped.skip(), "a SKIPPED row carries its audit");
    assertEquals("ops-user", skipped.skip().skippedBy());
    assertEquals("payload rejected by schema", skipped.skip().reason());
    assertTrue(skipped.skip().skippedAt().isAfter(before), "skippedAt is the store's now");
    assertNotNull(skipped.processedAt(), "the FAILED timestamp is kept");
    assertEquals(10, skipped.attempts(), "the ladder record is kept");
    assertEquals(List.of("n1"), claimIds(store), "the successor is claimable after the skip");
  }

  @Test
  public void skip_recordsAudit_releasesNothing_availabilityFirst() {
    var store = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    store.save(pending("n0", A));
    store.save(pending("n1", A));
    failHead(store, "n0");
    assertEquals(List.of("n1"), claimIds(store), "nothing was blocked");
    assertTrue(store.markDelivered(OutboxEntryId.of("n1"), store.claimedBy()));
    assertTrue(store.skipFailed(OutboxEntryId.of("n0"), "ops-user", "never deliverable"));
    assertEquals(OutboxStatus.SKIPPED, statusOf(store, "n0"));
    assertTrue(claimIds(store).isEmpty(), "the skip records the decision; nothing to release");
  }

  @Test
  public void skip_onNonHeadFailedRow_recordsAudit_releasesNothing_legacyAvailabilityFirstData() {
    // Only AVAILABILITY_FIRST can produce "a PENDING head backing off BELOW a FAILED row": n0
    // fails, n1 is claimed past it and fails too, n0 is replayed and then backs off.
    var store = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    store.save(pending("n0", A));
    store.save(pending("n1", A));
    failHead(store, "n0");
    failHead(store, "n1");
    assertTrue(store.resetFailedToPending(OutboxEntryId.of("n0")));
    assertEquals(List.of("n0"), claimIds(store));
    assertTrue(
        store.markRetry(OutboxEntryId.of("n0"), 1, "blip", Duration.ofHours(1), store.claimedBy()));
    assertTrue(claimIds(store).isEmpty(), "n0 backs off and holds A");

    assertTrue(store.skipFailed(OutboxEntryId.of("n1"), "ops-user", "stale"), "n1 is FAILED");

    assertEquals(OutboxStatus.SKIPPED, statusOf(store, "n1"));
    assertEquals(OutboxStatus.PENDING, statusOf(store, "n0"));
    assertTrue(claimIds(store).isEmpty(), "n0 is still the (backing-off) head; nothing released");
  }

  @Test
  public void skip_onNonFailed_returnsFalse_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("p1", A));
      assertFalse(store.skipFailed(OutboxEntryId.of("p1"), "ops", "r"), mode + ": PENDING");
      assertFalse(store.skipFailed(OutboxEntryId.of("absent"), "ops", "r"), mode + ": absent");
      assertEquals(List.of("p1"), claimIds(store));
      assertFalse(store.skipFailed(OutboxEntryId.of("p1"), "ops", "r"), mode + ": IN_PROGRESS");
      assertTrue(store.markDelivered(OutboxEntryId.of("p1"), store.claimedBy()));
      assertFalse(store.skipFailed(OutboxEntryId.of("p1"), "ops", "r"), mode + ": DELIVERED");
    }
  }

  @Test
  public void skipThenReplay_andReplayThenSkip_exactlyOneResolutionStands_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("x1", A));
      failHead(store, "x1");
      assertTrue(store.skipFailed(OutboxEntryId.of("x1"), "ops", "r"));
      assertFalse(store.resetFailedToPending(OutboxEntryId.of("x1")), "SKIPPED is terminal");
      assertEquals(OutboxStatus.SKIPPED, statusOf(store, "x1"));

      store.save(pending("x2", B));
      failHead(store, "x2");
      assertTrue(store.resetFailedToPending(OutboxEntryId.of("x2")));
      assertFalse(store.skipFailed(OutboxEntryId.of("x2"), "ops", "r"), "no longer FAILED");
      assertEquals(OutboxStatus.PENDING, statusOf(store, "x2"));
    }
  }

  @Test
  public void delete_refusesFailedRow_namesSkipAndReplay_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("n0", A));
      store.save(pending("n1", A));
      failHead(store, "n0");
      var ex =
          assertThrows(
              IllegalStateException.class, () -> store.delete(OutboxEntryId.of("n0")), mode.name());
      assertTrue(ex.getMessage().contains("FAILED"), ex.getMessage());
      assertTrue(
          ex.getMessage().contains("skip") && ex.getMessage().contains("replay"),
          "must name the two audited exits: " + ex.getMessage());
      assertEquals(OutboxStatus.FAILED, statusOf(store, "n0"), "the row is untouched");
      if (mode == OutboxOrderingMode.STRICT_PER_AGGREGATE) {
        assertTrue(claimIds(store).isEmpty(), "n1 stays blocked");
      }
    }
  }

  @Test
  public void delete_nonFailedRow_removesIt_andAbsentId_isNoop_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("d1", A));
      store.delete(OutboxEntryId.of("d1"));
      assertTrue(store.findById(OutboxEntryId.of("d1")).isEmpty());
      assertTrue(claimIds(store).isEmpty());
      store.delete(OutboxEntryId.of("never-existed")); // no exception
    }
  }

  // ── The strict channel rejects a null aggregate at save time ───────────────────────────

  @Test
  public void save_nullAggregateId_isRejected_strict_nothingInserted() {
    var store = newStore(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var ex =
        assertThrows(
            OutboxOrderingViolationException.class, () -> store.save(pendingFree("free-1")));
    assertEquals(OutboxEntryId.of("free-1"), ex.entryId());
    assertEquals("Event", ex.payloadType());
    assertTrue(store.findById(OutboxEntryId.of("free-1")).isEmpty(), "nothing persisted");
    assertEquals(0, store.countByStatus(OutboxStatus.PENDING));
  }

  @Test
  public void save_nullAggregateId_isAccepted_availabilityFirst() {
    var store = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    store.save(pendingFree("free-1"));
    assertEquals(List.of("free-1"), claimIds(store));
  }

  // ── Legacy rows after a mode switch over the same data ─────────────────────────────────

  @Test
  public void nullAggregateLegacyRows_stillDrain_afterSwitchToStrict() {
    var af = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    af.save(pendingFree("legacy-1"));
    af.save(pending("a1", A));
    var strict = sameDataWithMode(af, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    assertEquals(OutboxOrderingMode.STRICT_PER_AGGREGATE, strict.orderingMode());
    assertEquals(List.of("legacy-1", "a1"), claimIds(strict), "null_agg still drains in strict");
  }

  @Test
  public void failedRowBelowAPendingRow_blocksAfterSwitchToStrict_andIsReleasedBySkip() {
    var af = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    af.save(pending("n0", A));
    af.save(pending("n1", A));
    failHead(af, "n0");
    assertEquals(List.of("n1"), claimIds(af));
    assertTrue(
        af.markRetry(OutboxEntryId.of("n1"), 1, "blip", Duration.ofSeconds(-1), af.claimedBy()));
    var strict = sameDataWithMode(af, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    assertTrue(claimIds(strict).isEmpty(), "n0 FAILED below n1 PENDING now blocks A");
    assertTrue(strict.skipFailed(OutboxEntryId.of("n0"), "ops", "legacy poison"));
    assertEquals(List.of("n1"), claimIds(strict));
  }

  // ── Blockage sample ───────────────────────────────────────────────────────────────────────────

  @Test
  public void sampleBlockage_countsDistinctNonNullFailedAggregates_ageCoversEveryFailedRow() {
    // Seed under AVAILABILITY_FIRST so a null-aggregate FAILED row can exist; sample under both.
    var af = newStore(OutboxOrderingMode.AVAILABILITY_FIRST);
    var empty = af.sampleBlockage();
    assertEquals(0, empty.failedAggregates());
    assertNull(empty.oldestFailedAt());
    assertEquals(0, empty.nullAggregateFailed());

    af.save(pending("a1", A));
    af.save(pending("a2", A));
    af.save(pending("b1", B));
    af.save(pendingFree("free-1"));
    assertEquals(List.of("a1", "b1", "free-1"), claimIds(af));
    assertTrue(af.markFailed(OutboxEntryId.of("a1"), 10, "p", af.claimedBy()));
    assertTrue(af.markFailed(OutboxEntryId.of("free-1"), 10, "p", af.claimedBy()));
    assertTrue(af.markDelivered(OutboxEntryId.of("b1"), af.claimedBy()));
    assertEquals(List.of("a2"), claimIds(af)); // AF: a2 past the FAILED a1
    assertTrue(af.markFailed(OutboxEntryId.of("a2"), 10, "p", af.claimedBy()));

    for (var store : List.of(af, sameDataWithMode(af, OutboxOrderingMode.STRICT_PER_AGGREGATE))) {
      var s = store.sampleBlockage();
      assertEquals(1, s.failedAggregates(), "A once (two FAILED rows), the null row not at all");
      assertEquals(1, s.nullAggregateFailed(), "the legacy null-aggregate FAILED row");
      assertNotNull(s.oldestFailedAt(), "the age covers every FAILED row");
      var a1 = store.findById(OutboxEntryId.of("a1")).orElseThrow();
      assertFalse(s.oldestFailedAt().isAfter(a1.processedAt()), "oldest = first FAILED");
    }
  }

  // ── Retention ──────────────────────────────────────────────────────────────────────────

  @Test
  public void retention_neverDeletesFailed_deletesAgedSkippedOnly_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("f1", A));
      store.save(pending("s1", B));
      assertEquals(List.of("f1", "s1"), claimIds(store));
      assertTrue(store.markFailed(OutboxEntryId.of("f1"), 10, "p", store.claimedBy()));
      assertTrue(store.markFailed(OutboxEntryId.of("s1"), 10, "p", store.claimedBy()));
      assertTrue(store.skipFailed(OutboxEntryId.of("s1"), "ops", "r"));

      assertEquals(0, store.deleteSkipped(Instant.now().minusSeconds(3600)), "cutoff in the past");
      assertEquals(1, store.deleteSkipped(Instant.now().plusSeconds(3600)), "aged SKIPPED pruned");
      assertTrue(store.findById(OutboxEntryId.of("s1")).isEmpty());
      assertEquals(OutboxStatus.FAILED, statusOf(store, "f1"), mode + ": FAILED is never swept");
      assertEquals(1, store.countByStatus(OutboxStatus.FAILED));
    }
  }

  // ── Read-only lookups ─────────────────────────────────────────────────────────────────────────

  @Test
  public void findById_readOnly_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("r1", A));
      var found = store.findById(OutboxEntryId.of("r1")).orElseThrow();
      assertEquals(OutboxStatus.PENDING, found.status(), "a lookup never claims");
      assertEquals(A, found.streamId());
      assertNull(found.skip());
      assertEquals(List.of("r1"), claimIds(store), "still claimable after the lookup");
      assertTrue(store.findById(OutboxEntryId.of("absent")).isEmpty());
    }
  }

  @Test
  public void findByStatus_enumeratesSkipped_oldestFirst_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var store = newStore(mode);
      store.save(pending("k1", A));
      store.save(pending("k2", B));
      assertEquals(List.of("k1", "k2"), claimIds(store));
      assertTrue(store.markFailed(OutboxEntryId.of("k1"), 10, "p", store.claimedBy()));
      assertTrue(store.markFailed(OutboxEntryId.of("k2"), 10, "p", store.claimedBy()));
      assertTrue(store.skipFailed(OutboxEntryId.of("k2"), "ops", "r"));
      assertTrue(store.skipFailed(OutboxEntryId.of("k1"), "ops", "r"));
      var skipped = store.findByStatus(OutboxStatus.SKIPPED, 10);
      assertEquals(List.of("k1", "k2"), skipped.stream().map(e -> e.id().value()).toList());
      assertTrue(skipped.stream().allMatch(e -> e.skip() != null));
      assertEquals(2, store.countByStatus(OutboxStatus.SKIPPED));
    }
  }
}

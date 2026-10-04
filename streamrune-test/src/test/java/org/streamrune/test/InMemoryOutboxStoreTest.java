package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

/**
 * Store-specific pins of {@link InMemoryOutboxStore}: lease, compare-and-set, backoff and clock
 * mechanics, on (mostly) fixtures with no stream for brevity. Those fixtures need an {@link
 * OutboxOrderingMode#AVAILABILITY_FIRST} store — a strict store rejects a save with no stream — so
 * they are built through {@link #afStore}; only the constructor pins build the constructors they
 * pin. Ordering and mode behaviour (head rules, FAILED blocking, skip, replay, delete guard,
 * retention, blockage sample) is pinned in {@link OutboxStoreContract} under BOTH modes, run here
 * by {@code InMemoryOutboxStoreContractTest}; this class adds only what is particular to the double
 * (constructors, {@link InMemoryOutboxStore#withOrderingMode}, clock stamping).
 */
class InMemoryOutboxStoreTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  InMemoryOutboxStore store;

  private static InMemoryOutboxStore afStore(Clock clock, Duration lease) {
    return new InMemoryOutboxStore(clock, lease, OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @BeforeEach
  void setUp() {
    store = afStore(Clock.systemUTC(), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
  }

  /**
   * Claims {@code id} (moving it to IN_PROGRESS under the store's identity) so a guarded mark* can
   * transition it. mark* is a CAS on IN_PROGRESS + claimed_by; an unclaimed entry cannot be marked.
   */
  private void claim(OutboxEntryId id) {
    boolean found = store.loadPending(100).stream().anyMatch(e -> e.id().equals(id));
    assertTrue(found, "entry " + id + " must be claimable");
  }

  @Test
  void save_stores_pending_entry() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("id-1"), "{}", "OrderCreated"));
    // loadPending claims, so inspect the single returned batch (a second call would see nothing).
    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("id-1", pending.get(0).id().value());
    // The claim moved the entry to IN_PROGRESS — mirrors PostgresOutboxStore.
    assertEquals(OutboxStatus.IN_PROGRESS, pending.get(0).status());
  }

  @Test
  void save_is_idempotent_for_same_id() {
    var id = OutboxEntryId.of("id-dup");
    store.save(OutboxEntry.pending(id, "{\"a\":1}", "Event"));
    store.save(OutboxEntry.pending(id, "{\"a\":2}", "Event")); // second save ignored
    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("{\"a\":1}", pending.get(0).payload());
  }

  @Test
  void markDelivered_removes_from_pending_and_updates_status() {
    var id = OutboxEntryId.of("id-del");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    assertTrue(store.markDelivered(id, store.claimedBy()));

    assertTrue(store.loadPending(10).isEmpty());
    var delivered = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.DELIVERED, delivered.status());
    assertEquals(1, delivered.attempts());
    assertNotNull(delivered.processedAt());
  }

  @Test
  void markFailed_sets_terminal_status_and_records_error() {
    var id = OutboxEntryId.of("id-fail");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    assertTrue(store.markFailed(id, 3, "connection refused", store.claimedBy()));

    assertTrue(store.loadPending(10).isEmpty());
    var failed = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.FAILED, failed.status());
    assertEquals(3, failed.attempts());
    assertEquals("connection refused", failed.lastError());
    assertNotNull(failed.processedAt());
  }

  @Test
  void loadPending_returns_only_pending_entries() {
    var pendingId = OutboxEntryId.of("pend");
    var deliveredId = OutboxEntryId.of("deliv");
    // Save deliv first, claim + deliver it, THEN save pend so pend stays PENDING and unclaimed.
    store.save(OutboxEntry.pending(deliveredId, "{}", "Event"));
    claim(deliveredId);
    store.markDelivered(deliveredId, store.claimedBy());
    store.save(OutboxEntry.pending(pendingId, "{}", "Event"));

    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("pend", pending.get(0).id().value());
  }

  @Test
  void loadPending_respects_limit() {
    for (int i = 0; i < 5; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("id-" + i), "{}", "Event"));
    }
    assertEquals(3, store.loadPending(3).size());
  }

  @Test
  void loadPending_returns_entries_ordered_by_created_at() throws InterruptedException {
    store.save(OutboxEntry.pending(OutboxEntryId.of("first"), "{}", "Event"));
    Thread.sleep(5); // ensure different createdAt
    store.save(OutboxEntry.pending(OutboxEntryId.of("second"), "{}", "Event"));

    var pending = store.loadPending(10);
    assertEquals("first", pending.get(0).id().value());
    assertEquals("second", pending.get(1).id().value());
  }

  @Test
  void save_rejects_null_entry() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  @Test
  void markDelivered_rejects_null_id() {
    assertThrows(NullPointerException.class, () -> store.markDelivered(null, store.claimedBy()));
  }

  @Test
  void markFailed_rejects_null_id() {
    assertThrows(
        NullPointerException.class, () -> store.markFailed(null, 1, "err", store.claimedBy()));
  }

  @Test
  void delete_rejects_null_id() {
    assertThrows(NullPointerException.class, () -> store.delete(null));
  }

  @Test
  void all_returns_all_entries_regardless_of_status() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("p1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("f1"), "{}", "Event"));
    claim(OutboxEntryId.of("f1"));
    store.markFailed(OutboxEntryId.of("f1"), 1, "err", store.claimedBy());
    assertEquals(2, store.all().size());
  }

  @Test
  void markRetry_updates_attempts_and_error_and_nextRetryAt() {
    // next_retry_at is stamped from the STORE clock (clock.now() + backoff), not a caller-supplied
    // instant — so use a MutableClock and assert the exact stamped value deterministically.
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    var id = OutboxEntryId.of("retry-1");
    s.save(OutboxEntry.pending(id, "{}", "Event"));
    assertTrue(s.loadPending(10).stream().anyMatch(e -> e.id().equals(id))); // claim

    assertTrue(s.markRetry(id, 1, "connection refused", Duration.ofSeconds(10), s.claimedBy()));

    var entry = s.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.PENDING, entry.status());
    assertEquals(1, entry.attempts());
    assertEquals("connection refused", entry.lastError());
    assertEquals(clock.instant().plusSeconds(10), entry.nextRetryAt());
  }

  @Test
  void loadPending_skips_entries_with_nextRetryAt_in_the_future() {
    var id = OutboxEntryId.of("future-retry");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    store.markRetry(id, 1, "err", Duration.ofSeconds(3600), store.claimedBy());

    assertTrue(store.loadPending(10).isEmpty());
  }

  @Test
  void loadPending_includes_entries_with_nextRetryAt_in_the_past() {
    var id = OutboxEntryId.of("past-retry");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    store.markRetry(id, 1, "err", Duration.ofSeconds(-1), store.claimedBy());

    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("past-retry", pending.get(0).id().value());
  }

  @Test
  void loadPending_includes_entries_with_null_nextRetryAt() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("no-retry"), "{}", "Event"));
    assertEquals(1, store.loadPending(10).size());
  }

  @Test
  void markRetry_rejects_null_id() {
    assertThrows(
        NullPointerException.class,
        () -> store.markRetry(null, 1, "err", Duration.ZERO, store.claimedBy()));
  }

  @Test
  void constructor_rejects_null_clock() {
    assertThrows(NullPointerException.class, () -> new InMemoryOutboxStore(null));
    assertThrows(
        NullPointerException.class, () -> afStore(null, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE));
  }

  @Test
  void retry_backoff_is_testable_with_a_mutable_clock_without_sleeping() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    var id = OutboxEntryId.of("backoff");
    s.save(OutboxEntry.pending(id, "{}", "Event"));
    assertTrue(s.loadPending(10).stream().anyMatch(e -> e.id().equals(id))); // claim
    s.markRetry(id, 1, "err", Duration.ofSeconds(5), s.claimedBy());

    assertTrue(s.loadPending(10).isEmpty(), "backoff window active — entry must be hidden");
    clock.advance(Duration.ofSeconds(4));
    assertTrue(s.loadPending(10).isEmpty(), "still 1s before nextRetryAt");
    clock.advance(Duration.ofSeconds(1));
    assertEquals(1, s.loadPending(10).size(), "nextRetryAt reached — entry is due");
  }

  @Test
  void loadPending_claims_entries_so_a_second_poll_sees_nothing() {
    // Mirrors PostgresOutboxStore competing-consumer claim: an immediate second poll (a competing
    // relay instance) must not receive an entry the first poll already claimed.
    var id = OutboxEntryId.of("claim-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));

    var firstPoll = store.loadPending(10);
    assertEquals(1, firstPoll.size());
    assertEquals(OutboxStatus.IN_PROGRESS, firstPoll.get(0).status());

    assertTrue(store.loadPending(10).isEmpty(), "claimed entry must not be claimed twice");

    // The underlying entry stayed IN_PROGRESS (claimed) — not silently re-set to PENDING.
    var stored = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.IN_PROGRESS, stored.status());
  }

  @Test
  void loadPending_reclaims_entries_whose_claim_lease_expired() {
    // A relay that crashes after claiming leaves the entry stuck IN_PROGRESS; once the lease lapses
    // another poll must reclaim it (mirrors PostgresOutboxStore lease reclaim).
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = afStore(clock, Duration.ofMinutes(1));
    var id = OutboxEntryId.of("stale-claim");
    s.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, s.loadPending(10).size(), "first poll claims the entry");
    assertTrue(s.loadPending(10).isEmpty(), "claim is live — invisible within the lease");

    clock.advance(Duration.ofMinutes(2)); // past the 1-minute lease

    var reclaimed = s.loadPending(10);
    assertEquals(1, reclaimed.size(), "expired claim must be reclaimable");
    assertEquals(id.value(), reclaimed.get(0).id().value());
  }

  @Test
  void loadPending_does_not_reclaim_entries_within_the_lease() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var s = afStore(clock, Duration.ofMinutes(5));
    var id = OutboxEntryId.of("live-claim");
    s.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, s.loadPending(10).size());
    clock.advance(Duration.ofMinutes(1)); // still inside the 5-minute lease
    assertTrue(s.loadPending(10).isEmpty(), "live claim must stay invisible to other polls");
  }

  @Test
  void markRetry_releases_the_claim_so_it_is_claimable_again() {
    var id = OutboxEntryId.of("release");
    store.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, store.loadPending(10).size(), "first poll claims the entry");
    store.markRetry(id, 1, "timeout", Duration.ofSeconds(-1), store.claimedBy());

    var released = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(
        OutboxStatus.PENDING, released.status(), "markRetry releases the claim to PENDING");

    // Released and past nextRetryAt — a fresh poll re-claims it.
    assertEquals(1, store.loadPending(10).size());
  }

  @Test
  void constructor_rejects_non_positive_claim_lease() {
    assertThrows(
        IllegalArgumentException.class,
        () -> afStore(MutableClock.startingAt(Instant.now()), Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> afStore(MutableClock.startingAt(Instant.now()), Duration.ofSeconds(-1)));
    assertThrows(
        NullPointerException.class, () -> afStore(MutableClock.startingAt(Instant.now()), null));
  }

  @Test
  void default_claim_lease_is_four_minutes() {
    // The test double's default lease matches
    // PostgresOutboxStore.DEFAULT_CLAIM_LEASE and the OutboxStore.claimLease() SPI default (all
    // 4 minutes), so a user wiring `new InMemoryOutboxStore()` with a shipped KafkaOutboxPublisher
    // clears the OutboxPoller.build() guard instead of throwing. The shipped Kafka horizon is now
    // max.block.ms (60s) + delivery.timeout.ms (120s) = 180s, which the previous 3-minute default
    // merely EQUALLED — and the guard demands a STRICT excess.
    assertEquals(Duration.ofMinutes(4), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    assertEquals(Duration.ofMinutes(4), new InMemoryOutboxStore().claimLease());
  }

  // ── mark* CAS guard ──────────────────────────────────────────────────────

  @Test
  void claimedBy_isStableAndPerInstance() {
    assertNotNull(store.claimedBy());
    assertEquals(store.claimedBy(), store.claimedBy(), "identity is stable for the instance");
    assertNotEquals(
        store.claimedBy(),
        afStore(Clock.systemUTC(), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE).claimedBy(),
        "each store instance (relay process) has a distinct identity");
  }

  @Test
  void markDelivered_happyPath_transitionsUnderClaimingIdentity() {
    var id = OutboxEntryId.of("cas-happy");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    assertTrue(store.markDelivered(id, store.claimedBy()), "claiming identity must transition");
    var entry = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.DELIVERED, entry.status());
  }

  @Test
  void mark_onUnclaimedPendingEntry_isBenignNoOp() {
    var id = OutboxEntryId.of("cas-unclaimed");
    store.save(OutboxEntry.pending(id, "{}", "Event")); // never claimed → still PENDING
    assertFalse(
        store.markDelivered(id, store.claimedBy()), "cannot mark a PENDING (unclaimed) entry");
    assertFalse(store.markFailed(id, 1, "x", store.claimedBy()));
    assertFalse(store.markRetry(id, 1, "x", Duration.ZERO, store.claimedBy()));
    var entry = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.PENDING, entry.status(), "status is untouched by the no-op marks");
  }

  @Test
  void staleRelay_cannotOverwriteDelivered_markFailed() {
    // Relay X's lease was stolen: another relay reclaimed + delivered the entry (status DELIVERED,
    // held by a different claimed_by). X, resuming late, must NOT overwrite DELIVERED via
    // markFailed.
    // A stolen lease is modelled by a claimed_by that differs from what the row currently holds.
    var id = OutboxEntryId.of("stale-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id); // the entry is IN_PROGRESS, held by store.claimedBy()
    assertTrue(store.markDelivered(id, store.claimedBy()), "the live holder delivers → DELIVERED");

    // X's stale identity (it lost the lease) must not clobber the DELIVERED outcome.
    assertFalse(
        store.markFailed(id, 3, "boom", "outbox-STALE-X"), "stale X markFailed is a benign no-op");
    var entry = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.DELIVERED, entry.status(), "status stays DELIVERED");
  }

  @Test
  void staleRelay_cannotOverwriteInProgress_wrongIdentity() {
    // Even while the entry is still IN_PROGRESS, a relay whose identity does not match the current
    // claim holder cannot mark it — the guard is IN_PROGRESS AND claimed_by = ?.
    var id = OutboxEntryId.of("stale-inprog");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    assertFalse(store.markDelivered(id, "outbox-STALE-X"), "wrong identity cannot deliver");
    assertFalse(store.markFailed(id, 1, "x", "outbox-STALE-X"), "wrong identity cannot fail");
    assertFalse(
        store.markRetry(id, 1, "x", Duration.ZERO, "outbox-STALE-X"),
        "wrong identity cannot retry");
    var entry = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.IN_PROGRESS, entry.status(), "status untouched by stale marks");
  }

  @Test
  void staleRelay_cannotOverwriteDelivered_markRetry_and_markDeliveredAll() {
    var id = OutboxEntryId.of("stale-2");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    claim(id);
    assertTrue(store.markDelivered(id, store.claimedBy()));

    // A stale markRetry cannot resurrect the DELIVERED row.
    assertFalse(store.markRetry(id, 5, "boom", Duration.ZERO, "outbox-STALE-X"));
    // A stale markDeliveredAll transitions 0 rows.
    assertEquals(0, store.markDeliveredAll(java.util.List.of(id), "outbox-STALE-X"));

    var entry = store.all().stream().filter(e -> e.id().equals(id)).findFirst().orElseThrow();
    assertEquals(OutboxStatus.DELIVERED, entry.status());
  }

  @Test
  void markDeliveredAll_transitionsOnlyEntriesHeldByTheIdentity() {
    var a = OutboxEntryId.of("mda-a");
    var b = OutboxEntryId.of("mda-b");
    store.save(OutboxEntry.pending(a, "{}", "Event"));
    store.save(OutboxEntry.pending(b, "{}", "Event"));
    // One loadPending claims BOTH NULL-aggregate entries under the store identity.
    assertEquals(2, store.loadPending(10).size());
    assertEquals(2, store.markDeliveredAll(java.util.List.of(a, b), store.claimedBy()));

    // A stale identity transitions nothing.
    var c = OutboxEntryId.of("mda-c");
    store.save(OutboxEntry.pending(c, "{}", "Event"));
    claim(c);
    assertEquals(0, store.markDeliveredAll(java.util.List.of(c), "outbox-OTHER"));
  }

  // ── Per-aggregate head gating on the AF fixture (mode rules: OutboxStoreContract) ────────

  private static java.util.List<String> ids(java.util.List<OutboxEntry> entries) {
    return entries.stream().map(e -> e.id().value()).toList();
  }

  private static OutboxEntry pendingFor(String id, String aggregateId) {
    return OutboxEntry.pending(
        OutboxEntryId.of(id), "{}", "Event", StreamId.of(TYPE, AggregateId.of(aggregateId)));
  }

  @Test
  void nullAggregateEntriesParallelizeFreely() {
    for (int i = 0; i < 4; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("free-" + i), "{}", "Event"));
    }
    store.save(pendingFor("g1", "agg-G"));
    store.save(pendingFor("g2", "agg-G"));

    // All four NULL-aggregate entries plus agg-G's head — g2 stays gated.
    assertEquals(
        java.util.List.of("free-0", "free-1", "free-2", "free-3", "g1"),
        ids(store.loadPending(10)));
    assertTrue(store.loadPending(10).isEmpty(), "g2 must stay gated behind g1");
  }

  @Test
  void loadPending_respects_limit_across_the_gated_candidate_set() {
    store.save(pendingFor("x1", "agg-X"));
    store.save(pendingFor("y1", "agg-Y"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("z"), "{}", "Event"));

    // Three claimable candidates, limit 2 — the two lowest-seq ones win.
    assertEquals(java.util.List.of("x1", "y1"), ids(store.loadPending(2)));
  }

  @Test
  void concurrent_loadPending_never_claims_two_entries_of_one_aggregate() throws Exception {
    int totalA = 20;
    for (int i = 0; i < totalA; i++) {
      store.save(pendingFor(String.format("race-%02d", i), "agg-race"));
    }
    for (int i = 0; i < 10; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("free-" + i), "{}", "Event"));
    }
    int total = totalA + 10;

    var inFlightA = new java.util.concurrent.atomic.AtomicInteger();
    var maxInFlightA = new java.util.concurrent.atomic.AtomicInteger();
    var deliveredA = new java.util.concurrent.ConcurrentLinkedQueue<String>();
    var delivered = new java.util.concurrent.atomic.AtomicInteger();
    var errors = new java.util.concurrent.ConcurrentLinkedQueue<Throwable>();

    Runnable relay =
        () -> {
          try {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
            while (delivered.get() < total && System.nanoTime() < deadline) {
              for (var e : store.loadPending(4)) {
                boolean ofA = e.streamId() != null;
                if (ofA) {
                  int now = inFlightA.incrementAndGet();
                  maxInFlightA.accumulateAndGet(now, Math::max);
                  deliveredA.add(e.id().value());
                  Thread.sleep(1); // hold the claim briefly to widen any race window
                  // Decrement BEFORE markDelivered: the claim ends the instant markDelivered
                  // frees the aggregate, so decrementing after it would extend the measured
                  // window past the true claim and let the other relay's legitimate next claim
                  // read as a false >1-in-flight violation. Measured window must be a strict
                  // SUBSET of the true claim window (strict-seq-order below stays the
                  // deterministic ordering guard).
                  inFlightA.decrementAndGet();
                }
                store.markDelivered(e.id(), store.claimedBy());
                delivered.incrementAndGet();
              }
            }
          } catch (Throwable t) {
            errors.add(t);
          }
        };
    var t1 = new Thread(relay, "relay-1");
    var t2 = new Thread(relay, "relay-2");
    t1.start();
    t2.start();
    t1.join(60_000);
    t2.join(60_000);

    assertTrue(errors.isEmpty(), "relays must not error: " + errors);
    assertEquals(total, delivered.get(), "both relays together must drain every entry");
    assertEquals(
        1,
        maxInFlightA.get(),
        "the aggregate must never have more than one entry claimed at any instant");
    assertEquals(
        java.util.stream.IntStream.range(0, totalA)
            .mapToObj(i -> String.format("race-%02d", i))
            .toList(),
        java.util.List.copyOf(deliveredA),
        "the aggregate must be delivered in strict seq order");
  }

  @Test
  void processedAt_is_stamped_from_the_injected_clock() {
    var fixed = Instant.parse("2026-01-01T12:00:00Z");
    var s = afStore(MutableClock.startingAt(fixed), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    var delivered = OutboxEntryId.of("d");
    var failed = OutboxEntryId.of("f");
    s.save(OutboxEntry.pending(delivered, "{}", "Event"));
    s.save(OutboxEntry.pending(failed, "{}", "Event"));

    assertEquals(2, s.loadPending(10).size()); // claim both
    s.markDelivered(delivered, s.claimedBy());
    s.markFailed(failed, 2, "err", s.claimedBy());

    var deliveredEntry =
        s.all().stream().filter(e -> e.id().equals(delivered)).findFirst().orElseThrow();
    var failedEntry = s.all().stream().filter(e -> e.id().equals(failed)).findFirst().orElseThrow();
    assertEquals(fixed, deliveredEntry.processedAt());
    assertEquals(fixed, failedEntry.processedAt());
  }

  // ── FAILED operability ───────────────────────────────────────────────────────

  @Test
  void findByStatus_returnsOldestFirst_limited() {
    // seq is insertion order; save f1..f4 (increasing seq), fail all four. Claim all in ONE batch
    // (loadPending is stateful — a second call sees nothing) then markFailed each individually.
    for (int i = 1; i <= 4; i++) {
      var id = OutboxEntryId.of("f" + i);
      store.save(OutboxEntry.pending(id, "{}", "Event"));
    }
    assertEquals(4, store.loadPending(100).size(), "claim all four in one batch");
    for (int i = 1; i <= 4; i++) {
      assertTrue(store.markFailed(OutboxEntryId.of("f" + i), i, "boom", store.claimedBy()));
    }
    var found = store.findByStatus(OutboxStatus.FAILED, 2);
    assertEquals(
        java.util.List.of("f1", "f2"),
        found.stream().map(e -> e.id().value()).toList(),
        "findByStatus must return the two LOWEST-seq (oldest) FAILED entries");
  }

  @Test
  void findByStatus_isReadOnly_doesNotClaim() {
    var id = OutboxEntryId.of("ro-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    // No claim: a PENDING enumeration must not move the entry out of PENDING.
    var pending = store.findByStatus(OutboxStatus.PENDING, 10);
    assertEquals(java.util.List.of("ro-1"), pending.stream().map(e -> e.id().value()).toList());
    // Still claimable afterwards — findByStatus did not claim it.
    assertEquals(1, store.loadPending(10).size(), "findByStatus must not claim the entry");
  }

  @Test
  void deleteDelivered_removesStrictlyOlderThanCutoff_returnsCount() {
    // Parity with the production PostgresOutboxStore, which supports deleteDelivered. Before this
    // override, InMemoryOutboxStore inherited the throwing default deleteDelivered, so a unit test
    // of OutboxRetentionSweeper's delivered-retention path against this double would fail with
    // UnsupportedOperationException while the real store deletes.
    var clock = MutableClock.startingAt(Instant.parse("2026-06-20T00:00:00Z"));
    var s = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    var o = OutboxEntryId.of("old");
    var m = OutboxEntryId.of("mid");
    var n = OutboxEntryId.of("now");
    s.save(OutboxEntry.pending(o, "{}", "Event"));
    s.save(OutboxEntry.pending(m, "{}", "Event"));
    s.save(OutboxEntry.pending(n, "{}", "Event"));

    // processed_at is stamped from the clock on markDelivered. Advance between deliveries.
    assertEquals(3, s.loadPending(10).size());
    s.markDelivered(o, s.claimedBy()); // processed_at = start
    clock.advance(Duration.ofDays(1));
    s.markDelivered(m, s.claimedBy()); // processed_at = start + 1d
    clock.advance(Duration.ofDays(1));
    s.markDelivered(n, s.claimedBy()); // processed_at = start + 2d

    // cutoff = start + 1d (== m's processed_at). deleteDelivered removes STRICTLY older → only o.
    Instant cutoff = Instant.parse("2026-06-21T00:00:00Z");
    int deleted = s.deleteDelivered(cutoff);
    assertEquals(1, deleted, "only the strictly-older DELIVERED entry is removed");
    var remaining =
        s.findByStatus(OutboxStatus.DELIVERED, 10).stream().map(e -> e.id().value()).toList();
    assertEquals(
        java.util.List.of("mid", "now"), remaining, "boundary entry m is kept (not < cutoff)");
  }

  @Test
  void retentionSweeps_areStatusScoped_agedSiblingsOfOtherStatusesSurvive() {
    // Sibling-seeded coverage, mirrored from PostgresOutboxStoreRetentionIT: each sweep's
    // status predicate needs aged siblings of OTHER statuses, or a mutant deleting ANY aged row
    // survives. deleteSkipped prunes only SKIPPED rows whose skippedAt (not processedAt) is past
    // the cutoff; an aged FAILED row is never swept; deleteDelivered prunes only DELIVERED.
    var t0 = Instant.parse("2026-06-20T00:00:00Z");
    var clock = MutableClock.startingAt(t0);
    var s = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    var skippedOld1 = OutboxEntryId.of("sk-old-1");
    var skippedOld2 = OutboxEntryId.of("sk-old-2");
    var skippedFresh = OutboxEntryId.of("sk-fresh");
    var failedId = OutboxEntryId.of("still-failed");
    var deliveredId = OutboxEntryId.of("aged-delivered");
    for (var id :
        java.util.List.of(skippedOld1, skippedOld2, skippedFresh, failedId, deliveredId)) {
      s.save(OutboxEntry.pending(id, "{}", "Event"));
    }
    assertEquals(5, s.loadPending(10).size(), "claim all five in one batch");
    for (var id : java.util.List.of(skippedOld1, skippedOld2, skippedFresh, failedId)) {
      assertTrue(s.markFailed(id, 10, "poison", s.claimedBy())); // processedAt = t0
    }
    assertTrue(s.markDelivered(deliveredId, s.claimedBy())); // processedAt = t0
    var pendingId = OutboxEntryId.of("sib-pending");
    s.save(OutboxEntry.pending(pendingId, "{}", "Event")); // processedAt = null

    assertTrue(s.skipFailed(skippedOld1, "ops", "r")); // skippedAt = t0
    assertTrue(s.skipFailed(skippedOld2, "ops", "r")); // skippedAt = t0
    clock.advance(Duration.ofDays(31));
    // Failed at t0 like the others, skipped only now: its skippedAt is fresh.
    assertTrue(s.skipFailed(skippedFresh, "ops", "r")); // skippedAt = t0 + 31d

    Instant cutoff = t0.plus(Duration.ofDays(30));
    assertEquals(2, s.deleteSkipped(cutoff), "only the two SKIPPED rows skipped before the cutoff");
    assertEquals(
        java.util.List.of("sk-fresh"),
        ids(s.findByStatus(OutboxStatus.SKIPPED, 10)),
        "the freshly-skipped row survives: the cutoff reads skippedAt, not processedAt");
    assertEquals(
        java.util.List.of("still-failed"),
        ids(s.findByStatus(OutboxStatus.FAILED, 10)),
        "the aged FAILED row survives deleteSkipped");
    assertEquals(
        1, s.findByStatus(OutboxStatus.DELIVERED, 10).size(), "aged DELIVERED survives too");
    assertEquals(1, s.findByStatus(OutboxStatus.PENDING, 10).size(), "PENDING survives too");

    assertEquals(1, s.deleteDelivered(cutoff), "deleteDelivered prunes ONLY the DELIVERED row");
    assertTrue(s.findById(deliveredId).isEmpty());
    assertEquals(java.util.List.of("sk-fresh"), ids(s.findByStatus(OutboxStatus.SKIPPED, 10)));
    assertEquals(java.util.List.of("still-failed"), ids(s.findByStatus(OutboxStatus.FAILED, 10)));
    assertEquals(java.util.List.of("sib-pending"), ids(s.findByStatus(OutboxStatus.PENDING, 10)));
  }

  @Test
  void countByStatus_counts_IN_PROGRESS_separately_from_PENDING() {
    // The streamrune.outbox.in_flight gauge samples through this call. An
    // in-flight delivery failure holds the claim, so the backlog parks in IN_PROGRESS while PENDING
    // reads 0 — both counts must be exact and independent.
    store.save(OutboxEntry.pending(OutboxEntryId.of("count-1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("count-2"), "{}", "Event"));

    assertEquals(2, store.countByStatus(OutboxStatus.PENDING));
    assertEquals(0, store.countByStatus(OutboxStatus.IN_PROGRESS));

    assertEquals(2, store.loadPending(10).size(), "loadPending claims: PENDING -> IN_PROGRESS");

    assertEquals(0, store.countByStatus(OutboxStatus.PENDING));
    assertEquals(2, store.countByStatus(OutboxStatus.IN_PROGRESS));
  }

  // ── Ordering mode: constructors, the shared-data view, clock stamping ──────────────────

  @Test
  void defaultConstructors_areStrict() {
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    for (var s :
        java.util.List.of(
            new InMemoryOutboxStore(),
            new InMemoryOutboxStore(clock),
            new InMemoryOutboxStore(clock, Duration.ofMinutes(5)))) {
      assertEquals(OutboxOrderingMode.STRICT_PER_AGGREGATE, s.orderingMode());
      var free = OutboxEntry.pending(OutboxEntryId.of("free"), "{}", "Event");
      assertThrows(OutboxOrderingViolationException.class, () -> s.save(free));
      assertTrue(s.all().isEmpty(), "nothing is written");
    }
    assertEquals(
        OutboxOrderingMode.AVAILABILITY_FIRST,
        afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE).orderingMode());
    assertThrows(
        NullPointerException.class,
        () -> new InMemoryOutboxStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE, null));
    assertThrows(NullPointerException.class, () -> store.withOrderingMode(null));
  }

  @Test
  void withOrderingMode_sharesEntriesAndSeq_newClaimIdentity() {
    var af = afStore(Clock.systemUTC(), Duration.ofMinutes(7));
    af.save(OutboxEntry.pending(OutboxEntryId.of("n1"), "{}", "Event"));
    af.save(pendingFor("a1", "agg-A"));
    var strict = af.withOrderingMode(OutboxOrderingMode.STRICT_PER_AGGREGATE);

    assertEquals(OutboxOrderingMode.STRICT_PER_AGGREGATE, strict.orderingMode());
    assertEquals(OutboxOrderingMode.AVAILABILITY_FIRST, af.orderingMode(), "the source is kept");
    assertNotEquals(af.claimedBy(), strict.claimedBy(), "a second relay: its own claim identity");
    assertEquals(af.claimLease(), strict.claimLease(), "same lease");

    // A save through either view lands in the shared table, behind the shared seq counter.
    strict.save(pendingFor("a2", "agg-A"));
    assertEquals(3, af.all().size());

    // Claim through the strict view; the claim is visible to both.
    assertEquals(java.util.List.of("n1", "a1"), ids(strict.loadPending(10)));
    assertEquals(
        OutboxStatus.IN_PROGRESS, af.findById(OutboxEntryId.of("a1")).orElseThrow().status());
    assertTrue(af.loadPending(10).isEmpty(), "a2 is gated behind a1, claimed by the other relay");
    assertFalse(
        af.markDelivered(OutboxEntryId.of("a1"), af.claimedBy()),
        "the CAS is per identity: the other relay's claim cannot be marked");
    assertTrue(strict.markDelivered(OutboxEntryId.of("a1"), strict.claimedBy()));
    assertEquals(java.util.List.of("a2"), ids(af.loadPending(10)), "shared seq order");
  }

  @Test
  void strictView_neverClaimsALegacyNullAggregateFailedRow_andItBlocksNothing() {
    // Legacy availability-first data — a FAILED null-aggregate row — under a strict relay.
    // It is no head (null aggregates have none), so it is never claimed and gates nothing.
    var af = afStore(Clock.systemUTC(), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    af.save(OutboxEntry.pending(OutboxEntryId.of("legacy-failed"), "{}", "Event"));
    assertEquals(1, af.loadPending(10).size());
    assertTrue(af.markFailed(OutboxEntryId.of("legacy-failed"), 10, "poison", af.claimedBy()));
    af.save(OutboxEntry.pending(OutboxEntryId.of("legacy-pending"), "{}", "Event"));

    var strict = af.withOrderingMode(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    assertEquals(java.util.List.of("legacy-pending"), ids(strict.loadPending(10)));
    assertEquals(
        OutboxStatus.FAILED,
        strict.findById(OutboxEntryId.of("legacy-failed")).orElseThrow().status());
  }

  @Test
  void skippedAt_isStampedFromTheInjectedClock() {
    var fixed = Instant.parse("2026-06-21T00:00:00Z");
    var s =
        new InMemoryOutboxStore(
            MutableClock.startingAt(fixed),
            InMemoryOutboxStore.DEFAULT_CLAIM_LEASE,
            OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var id = OutboxEntryId.of("sk");
    s.save(pendingFor("sk", "agg-S"));
    assertEquals(1, s.loadPending(10).size());
    assertTrue(s.markFailed(id, 3, "boom", s.claimedBy()));
    assertTrue(s.skipFailed(id, "ops-user", "never deliverable"));
    var skip = s.findById(id).orElseThrow().skip();
    assertEquals(fixed, skip.skippedAt());
    assertEquals("ops-user", skip.skippedBy());
    assertEquals("never deliverable", skip.reason());
  }
}

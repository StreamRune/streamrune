package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Store-specific pins for {@link PostgresOutboxStore}: lease and claim mechanics, the mark* CAS,
 * the lock-time re-check probes, concurrency and the schema/constructor pins. Every ordering-mode
 * behaviour (head rules, the FAILED head, the invariants, the blockage sample) lives in {@code
 * OutboxStoreContract}, run against this store by {@code PostgresOutboxStoreContractTest}.
 *
 * <p>The fixture is an {@code AVAILABILITY_FIRST} store: these tests seed null-aggregate entries
 * for brevity, which the strict production default refuses at save time. Nothing here depends on
 * the mode except the tests that say so; the production defaults are pinned by {@link
 * #defaultConstructors_areStrict_threeArgCtorIsExplicit}.
 */
@Testcontainers
class PostgresOutboxStoreTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_outbox_test");

  static PGSimpleDataSource dataSource;
  PostgresOutboxStore store;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // Apply the event-store baseline migration (V001__streamrune_baseline.sql)
    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // No @Encrypted-bearing types are exercised through this registry in this test; report
            // none explicitly rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = afStore();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    }
  }

  /** An {@code AVAILABILITY_FIRST} store on the default lease — accepts null-aggregate entries. */
  private static PostgresOutboxStore afStore() {
    return afStore(PostgresOutboxStore.DEFAULT_CLAIM_LEASE);
  }

  private static PostgresOutboxStore afStore(java.time.Duration claimLease) {
    return new PostgresOutboxStore(dataSource, claimLease, OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @Test
  void claimLease_reportsConfiguredLease_defaultFourMinutes() {
    // The OutboxPoller reads claimLease() to bound its per-batch publish deadline below the
    // reclaim window. The store must expose its actual configured lease, not the SPI fallback. The
    // default is 4 minutes (raised from 1 minute, then from
    // 3, so it strictly exceeds Kafka's full 180s in-flight horizon — max.block.ms 60s +
    // delivery.timeout.ms 120s).
    assertEquals(java.time.Duration.ofMinutes(4), new PostgresOutboxStore(dataSource).claimLease());
    assertEquals(
        java.time.Duration.ofSeconds(90),
        new PostgresOutboxStore(dataSource, java.time.Duration.ofSeconds(90)).claimLease());
  }

  @Test
  void countByStatus_counts_IN_PROGRESS_separately_from_PENDING() {
    // The streamrune.outbox.in_flight gauge is sampled through this exact
    // call. A post-hand-off delivery outage holds the claim, so the backlog sits in IN_PROGRESS
    // while PENDING reads 0 — the two counts must be independent and both exact.
    store.save(OutboxEntry.pending(OutboxEntryId.of("pg-count-1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("pg-count-2"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("pg-count-3"), "{}", "Event"));

    assertEquals(3, store.countByStatus(OutboxStatus.PENDING));
    assertEquals(0, store.countByStatus(OutboxStatus.IN_PROGRESS));

    // loadPending CLAIMS the batch: PENDING -> IN_PROGRESS, which is exactly what an in-flight
    // failure then leaves behind (it never calls markRetry).
    assertEquals(3, store.loadPending(10).size());

    assertEquals(
        0,
        store.countByStatus(OutboxStatus.PENDING),
        "the pending gauge reads 0 while a claimed batch is parked — the blind spot in_flight"
            + " closes");
    assertEquals(3, store.countByStatus(OutboxStatus.IN_PROGRESS));
  }

  @Test
  void save_then_load_pending_returns_entry() {
    var id = OutboxEntryId.of("pg-id-1");
    store.save(OutboxEntry.pending(id, "{\"order\":\"1\"}", "OrderCreated"));

    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("pg-id-1", pending.get(0).id().value());
    // JSONB normalises the stored JSON (adds spaces after colons)
    assertEquals("{\"order\": \"1\"}", pending.get(0).payload());
    assertEquals("OrderCreated", pending.get(0).payloadType());
    // loadPending claims the entry: status moves PENDING -> IN_PROGRESS
    assertEquals(OutboxStatus.IN_PROGRESS, pending.get(0).status());
    assertEquals(0, pending.get(0).attempts());
    assertNull(pending.get(0).lastError());
    assertNotNull(pending.get(0).createdAt());
  }

  @Test
  void loadPending_claims_entries_so_a_second_poll_sees_nothing() throws Exception {
    var id = OutboxEntryId.of("pg-claim-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, store.loadPending(10).size());

    // The claim is persisted: a competing relay instance must not receive the same entry.
    var competingStore = afStore();
    assertTrue(competingStore.loadPending(10).isEmpty(), "claimed entry was claimed twice");

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, claimed_at, claimed_by FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("IN_PROGRESS", rs.getString("status"));
        assertNotNull(rs.getTimestamp("claimed_at"));
        assertNotNull(rs.getString("claimed_by"));
      }
    }
  }

  @Test
  void loadPending_reclaims_entries_whose_claim_lease_expired() throws Exception {
    var id = OutboxEntryId.of("pg-stale-claim");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size());

    // Simulate a crashed relay: age the claim past the lease.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '10 minutes' "
              + "WHERE entry_id = '"
              + id.value()
              + "'");
    }

    var reclaimed = store.loadPending(10);
    assertEquals(1, reclaimed.size(), "expired claim should be reclaimable");
    assertEquals(id.value(), reclaimed.get(0).id().value());
  }

  @Test
  void loadPending_does_not_reclaim_entries_within_the_lease() {
    var id = OutboxEntryId.of("pg-live-claim");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size());

    // Fresh claim, lease (3 minute default) not expired — invisible to other polls.
    assertTrue(store.loadPending(10).isEmpty());
  }

  /**
   * Regression: the reclaim cutoff must be decided by the DATABASE clock alone, never by the relay
   * JVM's wall clock. In multi-relay HA a relay whose clock is skewed AHEAD of the DB must not be
   * able to reclaim another relay's still-live, mid-publish claim (which would double-publish and
   * reorder that aggregate). We seed one still-live and one genuinely-expired claim by the DB
   * clock, then run reclaim as a relay whose {@code Instant.now()} is one hour ahead of the DB, and
   * assert only the truly-expired claim is reset — the skewed relay clock changes nothing.
   *
   * <p>Fails against the pre-fix code, which computes the cutoff as {@code relayNow - lease}: the
   * +1h-skewed cutoff lands well after the live claim's {@code claimed_at}, so it wrongly reclaims
   * it.
   */
  @Test
  void reclaimExpired_isDecidedByDbClockAlone_notTheRelayWallClock() throws Exception {
    var store60 = afStore(java.time.Duration.ofSeconds(60));
    var live = OutboxEntryId.of("skew-live-claim");
    var expired = OutboxEntryId.of("skew-expired-claim");
    store60.save(OutboxEntry.pending(live, "{}", "Event"));
    store60.save(OutboxEntry.pending(expired, "{}", "Event"));

    // Seed both IN_PROGRESS with a DB-clock claimed_at at controlled offsets from NOW():
    //   live    → claimed_at = NOW() - 55s  (5s of the 60s lease left → must NOT be reclaimed)
    //   expired → claimed_at = NOW() - 65s  (past the lease            → must be reclaimed)
    exec(
        "UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_by = 'relayA', "
            + "claimed_at = NOW() - INTERVAL '55 seconds' WHERE entry_id = '"
            + live.value()
            + "'");
    exec(
        "UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_by = 'relayA', "
            + "claimed_at = NOW() - INTERVAL '65 seconds' WHERE entry_id = '"
            + expired.value()
            + "'");

    // Reclaim as a relay whose wall clock is skewed +1h ahead of the DB clock.
    java.time.Instant skewedRelayNow = java.time.Instant.now().plus(java.time.Duration.ofHours(1));
    try (var conn = dataSource.getConnection()) {
      store60.reclaimExpired(conn, skewedRelayNow);
    }

    assertEquals(
        "IN_PROGRESS",
        statusOf(live),
        "a still-live claim must survive reclaim even when the relay's wall clock is skewed far "
            + "ahead of the DB clock — the DB clock alone governs the lease");
    assertEquals(
        "PENDING", statusOf(expired), "a genuinely expired claim must be reclaimed to PENDING");
  }

  @Test
  void markRetry_releases_the_claim() throws Exception {
    var id = OutboxEntryId.of("pg-release");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size());

    assertTrue(
        store.markRetry(id, 1, "timeout", java.time.Duration.ofSeconds(-1), store.claimedBy()));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, claimed_at, claimed_by FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("PENDING", rs.getString("status"));
        assertNull(rs.getTimestamp("claimed_at"));
        assertNull(rs.getString("claimed_by"));
      }
    }
    // Released and past next_retry_at — claimable again.
    assertEquals(1, store.loadPending(10).size());
  }

  @Test
  void save_persists_aggregate_id_and_loadPending_returns_it() {
    var id = OutboxEntryId.of("pg-agg-1");
    store.save(OutboxEntry.pending(id, "{}", "OrderCreated", TestStreams.stream("order-42")));

    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals(TestStreams.stream("order-42"), pending.get(0).streamId());
  }

  /**
   * {@code AggregateId.of} refuses control characters — it is the INGRESS door. Both outbox row
   * mappers (the claim behind {@code loadPending} and the read-only {@code findByStatus}) are
   * DECODE doors: an {@code aggregate_id} an {@code OutboxEventMapper} wrote through the canonical
   * constructor must read back verbatim. Through {@code of}, the row would throw inside the claim
   * and stall the relay for every entry behind it.
   */
  @Test
  void aStoredAggregateIdCarryingAControlCharacterReadsBackVerbatim_onBothReadPaths() {
    var raw = "order\r\n\u0085-1";
    store.save(
        OutboxEntry.pending(
            OutboxEntryId.of("pg-agg-ctl"), "{}", "OrderCreated", TestStreams.stream(raw)));

    var found = store.findByStatus(OutboxStatus.PENDING, 10);
    assertEquals(1, found.size());
    assertEquals(TestStreams.stream(raw), found.get(0).streamId());

    var claimed = store.loadPending(10);
    assertEquals(1, claimed.size());
    assertEquals(TestStreams.stream(raw), claimed.get(0).streamId());
  }

  @Test
  void loadPending_returns_null_aggregate_id_for_entries_without_one() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("pg-agg-null"), "{}", "Event"));
    assertNull(store.loadPending(10).get(0).streamId());
  }

  @Test
  void loadPending_orders_by_seq_even_when_created_at_ties() throws Exception {
    // Rows written in one transaction share the same NOW(); seq disambiguates.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO outbox_events (entry_id, payload, payload_type, status, attempts, created_at) "
                    + "VALUES (?, '{}'::jsonb, 'Event', 'PENDING', 0, ?)")) {
      var sharedCreatedAt = java.sql.Timestamp.from(java.time.Instant.now());
      for (String entryId : java.util.List.of("seq-first", "seq-second", "seq-third")) {
        ps.setString(1, entryId);
        ps.setTimestamp(2, sharedCreatedAt);
        ps.addBatch();
      }
      ps.executeBatch();
    }

    var claimed = store.loadPending(10);
    assertEquals(
        java.util.List.of("seq-first", "seq-second", "seq-third"),
        claimed.stream().map(e -> e.id().value()).toList());
  }

  @Test
  void constructor_rejects_non_positive_claim_lease() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresOutboxStore(dataSource, java.time.Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresOutboxStore(dataSource, java.time.Duration.ofSeconds(-1)));
    assertThrows(IllegalArgumentException.class, () -> new PostgresOutboxStore(dataSource, null));
  }

  @Test
  void save_is_idempotent_on_conflict() {
    var id = OutboxEntryId.of("pg-dup");
    store.save(OutboxEntry.pending(id, "{\"a\":1}", "Event"));
    store.save(OutboxEntry.pending(id, "{\"a\":2}", "Event")); // conflict — ignored

    assertEquals(1, store.loadPending(10).size());
  }

  @Test
  void markDelivered_removes_from_pending_and_increments_attempts() throws Exception {
    var id = OutboxEntryId.of("pg-del");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim under this store's identity
    assertTrue(store.markDelivered(id, store.claimedBy()));

    assertTrue(store.loadPending(10).isEmpty());
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, attempts FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("DELIVERED", rs.getString("status"));
        assertEquals(1, rs.getInt("attempts"));
      }
    }
  }

  @Test
  void markFailed_sets_terminal_status_and_removes_from_pending() throws Exception {
    var id = OutboxEntryId.of("pg-fail");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim
    assertTrue(store.markFailed(id, 3, "connection refused", store.claimedBy()));

    assertTrue(store.loadPending(10).isEmpty());
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, attempts, last_error FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("FAILED", rs.getString("status"));
        assertEquals(3, rs.getInt("attempts"));
        assertEquals("connection refused", rs.getString("last_error"));
      }
    }
  }

  @Test
  void save_rejects_null_entry() {
    assertThrows(IllegalArgumentException.class, () -> store.save(null));
  }

  @Test
  void loadPending_respects_limit_and_returns_oldest_first() throws InterruptedException {
    store.save(OutboxEntry.pending(OutboxEntryId.of("old"), "{}", "Event"));
    Thread.sleep(10);
    store.save(OutboxEntry.pending(OutboxEntryId.of("new"), "{}", "Event"));

    var result = store.loadPending(1);
    assertEquals(1, result.size());
    assertEquals("old", result.get(0).id().value());
  }

  @Test
  void constructor_rejects_null_datasource() {
    assertThrows(IllegalArgumentException.class, () -> new PostgresOutboxStore(null));
  }

  @Test
  void loadPending_rejects_zero_limit() {
    assertThrows(IllegalArgumentException.class, () -> store.loadPending(0));
  }

  @Test
  void markDelivered_rejects_null_id() {
    assertThrows(
        IllegalArgumentException.class, () -> store.markDelivered(null, store.claimedBy()));
  }

  @Test
  void markFailed_rejects_null_id() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.markFailed(null, 1, "error", store.claimedBy()));
  }

  @Test
  void delete_rejects_null_id() {
    assertThrows(IllegalArgumentException.class, () -> store.delete(null));
  }

  @Test
  void markRetry_persists_attempts_error_and_nextRetryAt() throws Exception {
    var id = OutboxEntryId.of("pg-retry-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim

    assertTrue(
        store.markRetry(id, 2, "timeout", java.time.Duration.ofSeconds(30), store.claimedBy()));

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, attempts, last_error, next_retry_at FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("PENDING", rs.getString("status"));
        assertEquals(2, rs.getInt("attempts"));
        assertEquals("timeout", rs.getString("last_error"));
        assertNotNull(rs.getTimestamp("next_retry_at"));
      }
    }
  }

  /**
   * Regression: the backoff is DB-stamped ({@code next_retry_at = NOW() + backoff}) so it is
   * measured against the SAME clock the claim gate reads, immune to relay JVM clock skew. markRetry
   * takes a {@link java.time.Duration}, never a relay-computed absolute instant, so the relay wall
   * clock can no longer influence when the entry becomes eligible. Here a 1-hour backoff must land
   * ~1 hour ahead of the DATABASE clock and the entry must not be claimable meanwhile.
   */
  @Test
  void markRetry_stampsNextRetryAtFromTheDbClock_notTheRelayClock() throws Exception {
    var id = OutboxEntryId.of("pg-db-stamped-backoff");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim

    assertTrue(
        store.markRetry(id, 1, "broker down", java.time.Duration.ofHours(1), store.claimedBy()));

    // Backing off — not claimable while next_retry_at is in the (DB) future.
    assertTrue(store.loadPending(10).isEmpty(), "entry must back off for the DB-measured interval");

    // next_retry_at is ~1h ahead of the DB clock (NOW()), proving it was stamped from NOW() + the
    // backoff duration rather than any relay-supplied instant.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT next_retry_at > NOW() + INTERVAL '59 minutes' AS backed_off, "
                    + "next_retry_at <= NOW() + INTERVAL '61 minutes' AS bounded "
                    + "FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertTrue(rs.getBoolean("backed_off"), "next_retry_at must be ~1h ahead of the DB clock");
        assertTrue(rs.getBoolean("bounded"), "next_retry_at must not overshoot the 1h backoff");
      }
    }
  }

  @Test
  void loadPending_skips_entries_with_nextRetryAt_in_the_future() {
    var id = OutboxEntryId.of("pg-future");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim
    store.markRetry(id, 1, "err", java.time.Duration.ofSeconds(3600), store.claimedBy());

    assertTrue(store.loadPending(10).isEmpty());
  }

  @Test
  void loadPending_includes_entries_with_nextRetryAt_in_the_past() {
    var id = OutboxEntryId.of("pg-past");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim
    store.markRetry(id, 1, "err", java.time.Duration.ofSeconds(-10), store.claimedBy());

    var pending = store.loadPending(10);
    assertEquals(1, pending.size());
    assertEquals("pg-past", pending.get(0).id().value());
    assertEquals(1, pending.get(0).attempts());
    assertEquals("err", pending.get(0).lastError());
  }

  @Test
  void loadPending_includes_entries_with_null_nextRetryAt() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("pg-null-retry"), "{}", "Event"));
    assertEquals(1, store.loadPending(10).size());
  }

  @Test
  void markRetry_rejects_null_id() {
    assertThrows(
        IllegalArgumentException.class,
        () -> store.markRetry(null, 1, "err", java.time.Duration.ZERO, store.claimedBy()));
  }

  // ── Claim mechanics — the mode-dependent head rules live in OutboxStoreContract ──

  private static List<String> ids(List<OutboxEntry> entries) {
    return entries.stream().map(e -> e.id().value()).toList();
  }

  private void exec(String sql) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Test
  void nullAggregateEntriesParallelizeFreelyAcrossRelays() {
    for (int i = 0; i < 10; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("free-" + i), "{}", "Event"));
    }
    var relayB = afStore();

    assertEquals(5, store.loadPending(5).size());
    // Relay A holds 5 claims; relay B still gets the other 5 — no head-of-line blocking.
    assertEquals(5, relayB.loadPending(10).size());
  }

  /**
   * The one-claim-per-aggregate invariant under real concurrency: two relay instances drain the
   * outbox in parallel; at any instant aggregate A has at most ONE entry claimed (IN_PROGRESS), and
   * A's entries are delivered in strict seq order. Other aggregates and NULL-aggregate entries
   * drain in parallel.
   *
   * <p>Runs under BOTH ordering modes: that invariant is mode-independent, and the strict claim
   * statement is a different SQL text whose {@code ux_outbox_one_inflight_per_stream} interplay
   * must be exercised under real concurrency too. Seeding stays on the availability-first fixture
   * (the eight {@code race-null-*} rows are null-aggregate), and both relays — strict included —
   * must still drain them through {@code null_agg} (a strict channel drains legacy null-aggregate
   * rows). The strict run additionally proves the strict claim never hands two relays two heads of
   * A.
   */
  @ParameterizedTest
  @EnumSource(OutboxOrderingMode.class)
  void twoConcurrentRelaysNeverHoldTwoEntriesOfOneAggregate(OutboxOrderingMode mode)
      throws Exception {
    int totalA = 12;
    var a = TestStreams.stream("agg-race");
    for (int i = 0; i < totalA; i++) {
      store.save(
          OutboxEntry.pending(OutboxEntryId.of(String.format("race-a-%02d", i)), "{}", "Event", a));
    }
    for (String other : List.of("agg-o1", "agg-o2", "agg-o3")) {
      for (int i = 0; i < 3; i++) {
        store.save(
            OutboxEntry.pending(
                OutboxEntryId.of(other + "-" + i), "{}", "Event", TestStreams.stream(other)));
      }
    }
    for (int i = 0; i < 8; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("race-null-" + i), "{}", "Event"));
    }
    int total = totalA + 9 + 8;

    var inFlightA = new java.util.concurrent.atomic.AtomicInteger();
    var maxInFlightA = new java.util.concurrent.atomic.AtomicInteger();
    var deliveredA = new ConcurrentLinkedQueue<String>();
    var delivered = new java.util.concurrent.atomic.AtomicInteger();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var barrier = new CyclicBarrier(2);

    Runnable relay =
        () -> {
          try {
            // distinct claimed_by per relay; the relay's own store runs the mode under test
            var own =
                new PostgresOutboxStore(dataSource, PostgresOutboxStore.DEFAULT_CLAIM_LEASE, mode);
            barrier.await();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
            while (delivered.get() < total && System.nanoTime() < deadline) {
              var batch = own.loadPending(4);
              for (var e : batch) {
                boolean ofA = a.equals(e.streamId());
                if (ofA) {
                  int now = inFlightA.incrementAndGet();
                  maxInFlightA.accumulateAndGet(now, Math::max);
                  deliveredA.add(e.id().value());
                  Thread.sleep(2); // hold the claim briefly to widen any race window
                }
                own.markDelivered(e.id(), own.claimedBy());
                if (ofA) {
                  inFlightA.decrementAndGet();
                }
                delivered.incrementAndGet();
              }
              if (batch.isEmpty()) {
                Thread.sleep(1);
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
    t1.join(90_000);
    t2.join(90_000);

    assertTrue(errors.isEmpty(), "relays must not error: " + errors);
    assertEquals(total, delivered.get(), "both relays together must drain every entry");
    assertEquals(
        1,
        maxInFlightA.get(),
        "aggregate A must never have more than one entry claimed at any instant");
    assertEquals(
        java.util.stream.IntStream.range(0, totalA)
            .mapToObj(i -> String.format("race-a-%02d", i))
            .toList(),
        List.copyOf(deliveredA),
        "aggregate A must be delivered in strict seq order");
  }

  /**
   * Competing-consumer guarantee under real concurrency: K relay instances each call {@code
   * loadPending} at the same instant (released together by a {@link CyclicBarrier}). The {@code FOR
   * UPDATE SKIP LOCKED} claim must hand every PENDING entry to at most one instance — the union of
   * claimed entry-ids contains zero duplicates and covers exactly the inserted set.
   */
  @Test
  void concurrentLoadPendingPartitionsWithNoDuplicates() throws Exception {
    int total = 200;
    int threads = 8;
    for (int i = 0; i < total; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("cc-" + i), "{}", "Event"));
    }

    var barrier = new CyclicBarrier(threads);
    // Each instance is its own store (distinct claimed_by), mirroring separate relay processes.
    var stores = new ArrayList<PostgresOutboxStore>();
    for (int t = 0; t < threads; t++) {
      stores.add(afStore());
    }

    var claimed = new ConcurrentLinkedQueue<String>();
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var workers = new ArrayList<Thread>();
    for (int t = 0; t < threads; t++) {
      var instance = stores.get(t);
      var worker =
          new Thread(
              () -> {
                try {
                  barrier.await(); // all instances poll at once
                  // Drain in batches so the combined demand exceeds the available rows.
                  List<OutboxEntry> batch;
                  while (!(batch = instance.loadPending(25)).isEmpty()) {
                    for (OutboxEntry e : batch) {
                      claimed.add(e.id().value());
                    }
                  }
                } catch (Throwable th) {
                  errors.add(th);
                }
              },
              "outbox-consumer-" + t);
      workers.add(worker);
      worker.start();
    }
    for (Thread w : workers) {
      w.join(30_000);
    }

    assertTrue(errors.isEmpty(), "no consumer should error: " + errors);
    var claimedList = new ArrayList<>(claimed);
    var uniqueClaimed = new HashSet<>(claimedList);
    assertEquals(
        claimedList.size(),
        uniqueClaimed.size(),
        "an entry was claimed by more than one instance (SKIP LOCKED violated)");
    assertEquals(total, uniqueClaimed.size(), "every PENDING entry should be claimed exactly once");
  }

  // ── mark* CAS guard ─────────────────────────────────────────────────────────

  private String statusOf(OutboxEntryId id) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString("status");
      }
    }
  }

  /** Ages the current claim past the lease so another store instance can reclaim it. */
  private void ageClaimPastLease(OutboxEntryId id) throws Exception {
    exec(
        "UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '10 minutes' WHERE entry_id = '"
            + id.value()
            + "'");
  }

  @Test
  void claimedBy_isStableAndDistinctPerInstance() {
    assertNotNull(store.claimedBy());
    assertEquals(store.claimedBy(), store.claimedBy(), "identity is stable for the instance");
    assertNotEquals(
        store.claimedBy(),
        afStore().claimedBy(),
        "each store instance (relay process) has a distinct identity");
  }

  @Test
  void markDelivered_happyPath_transitionsUnderClaimingIdentity() throws Exception {
    var id = OutboxEntryId.of("cas-happy");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    assertEquals(1, store.loadPending(10).size()); // claim
    assertTrue(store.markDelivered(id, store.claimedBy()), "claiming identity transitions the row");
    assertEquals("DELIVERED", statusOf(id));
  }

  @Test
  void mark_onUnclaimedPendingEntry_isBenignNoOp() throws Exception {
    var id = OutboxEntryId.of("cas-unclaimed");
    store.save(OutboxEntry.pending(id, "{}", "Event")); // never claimed → PENDING
    assertFalse(store.markDelivered(id, store.claimedBy()), "cannot mark a PENDING entry");
    assertFalse(store.markFailed(id, 1, "x", store.claimedBy()));
    assertFalse(store.markRetry(id, 1, "x", java.time.Duration.ZERO, store.claimedBy()));
    assertEquals("PENDING", statusOf(id), "status untouched by the no-op marks");
  }

  @Test
  void staleRelay_cannotOverwriteDelivered_markFailed() throws Exception {
    // Relay X claims, then crashes. Relay Y (a DISTINCT store instance → distinct claimed_by)
    // reclaims after the lease expires and delivers. X, resuming late, must NOT overwrite
    // DELIVERED.
    var relayX = afStore();
    var relayY = afStore();
    var id = OutboxEntryId.of("cas-stale-fail");
    relayX.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, relayX.loadPending(10).size(), "X claims"); // held by X
    ageClaimPastLease(id);
    assertEquals(1, relayY.loadPending(10).size(), "Y reclaims the expired claim"); // now held by Y
    assertTrue(relayY.markDelivered(id, relayY.claimedBy()), "Y delivers → DELIVERED");

    // X's stale mark (its own, now-losing identity) transitions 0 rows.
    assertFalse(
        relayX.markFailed(id, 3, "boom", relayX.claimedBy()), "stale X markFailed → 0 rows");
    assertEquals("DELIVERED", statusOf(id), "status stays DELIVERED");
  }

  @Test
  void staleRelay_cannotOverwriteDelivered_markRetry() throws Exception {
    var relayX = afStore();
    var relayY = afStore();
    var id = OutboxEntryId.of("cas-stale-retry");
    relayX.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, relayX.loadPending(10).size());
    ageClaimPastLease(id);
    assertEquals(1, relayY.loadPending(10).size());
    assertTrue(relayY.markDelivered(id, relayY.claimedBy()));

    // A stale markRetry from X cannot resurrect the DELIVERED row to PENDING.
    assertFalse(
        relayX.markRetry(id, 5, "boom", java.time.Duration.ZERO, relayX.claimedBy()),
        "stale X markRetry → 0 rows");
    assertEquals("DELIVERED", statusOf(id));
  }

  @Test
  void staleRelay_cannotOverwriteDelivered_markDeliveredAll() throws Exception {
    var relayX = afStore();
    var relayY = afStore();
    var id = OutboxEntryId.of("cas-stale-all");
    relayX.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, relayX.loadPending(10).size());
    ageClaimPastLease(id);
    assertEquals(1, relayY.loadPending(10).size());
    assertTrue(relayY.markDelivered(id, relayY.claimedBy()));

    // A stale markDeliveredAll from X transitions 0 rows (already DELIVERED, not held by X).
    assertEquals(0, relayX.markDeliveredAll(List.of(id), relayX.claimedBy()));
    assertEquals("DELIVERED", statusOf(id));
  }

  @Test
  void markDeliveredAll_happyPath_returnsTransitionedCount() throws Exception {
    var a = OutboxEntryId.of("cas-mda-a");
    var b = OutboxEntryId.of("cas-mda-b");
    store.save(OutboxEntry.pending(a, "{}", "Event"));
    store.save(OutboxEntry.pending(b, "{}", "Event"));
    assertEquals(2, store.loadPending(10).size()); // claim both under this identity
    assertEquals(
        2,
        store.markDeliveredAll(List.of(a, b), store.claimedBy()),
        "both entries transition under the claiming identity");
    assertEquals("DELIVERED", statusOf(a));
    assertEquals("DELIVERED", statusOf(b));
  }

  // ── releaseClaims ────────────────────────────────────────────────────

  @Test
  void releaseClaims_returnsUnattemptedEntriesToPending_leavingTheRetryLadderUntouched()
      throws Exception {
    // The never-attempted tail of a deadline-truncated cycle goes straight back to PENDING and is
    // immediately re-claimable — its aggregate is un-gated at once instead of after a full lease.
    // attempts / last_error / next_retry_at are NOT touched: nothing was attempted, so there is no
    // attempt to count, no error to record and no backoff to serve.
    var a = OutboxEntryId.of("cas-rc-a");
    var b = OutboxEntryId.of("cas-rc-b");
    store.save(OutboxEntry.pending(a, "{}", "Event"));
    store.save(OutboxEntry.pending(b, "{}", "Event"));
    var firstClaim = store.loadPending(10);
    assertEquals(2, firstClaim.size());
    var bEntry = firstClaim.stream().filter(e -> e.id().equals(b)).findFirst().orElseThrow();
    // Give "a" prior retry history so the "untouched" assertions are non-vacuous, then re-claim it
    // ("b" is still IN_PROGRESS from the first claim, so only "a" comes back).
    assertTrue(store.markRetry(a, 4, "earlier nack", java.time.Duration.ZERO, store.claimedBy()));
    var reclaimed = store.loadPending(10);
    assertEquals(1, reclaimed.size(), "only the released entry is claimable again");
    var beforeA = reclaimed.get(0);
    assertEquals(a, beforeA.id());
    assertEquals(4, beforeA.attempts());

    assertEquals(
        2,
        store.releaseClaims(List.of(beforeA, bEntry), store.claimedBy()),
        "both claims are released under the claiming identity");

    assertEquals("PENDING", statusOf(a));
    assertEquals("PENDING", statusOf(b));
    var afterA = loadRow(a);
    assertEquals(4, afterA.attempts(), "releasing must not burn a retry-ladder attempt");
    assertEquals(
        "earlier nack", afterA.lastError(), "the real last error must survive the release");
    assertEquals(
        beforeA.nextRetryAt(), afterA.nextRetryAt(), "no backoff is (re)stamped for a non-attempt");
    assertNull(claimedByOf(a), "the claim owner is cleared");
    assertNull(claimedAtOf(a), "the claim timestamp is cleared");
    assertEquals(2, store.loadPending(10).size(), "released entries are immediately re-claimable");
  }

  @Test
  void releaseClaims_isACas_aStolenLeaseIsSkippedNotStolenBack() throws Exception {
    // Relay X's cycle overran, Y reclaimed the entry past the lease and holds it. X's end-of-cycle
    // release must be a benign no-op: releasing it would hand Y's in-flight entry back to the
    // claim pool while Y is still publishing it — a duplicate plus a same-aggregate reorder.
    var relayX = afStore();
    var relayY = afStore();
    var id = OutboxEntryId.of("cas-rc-stolen");
    relayX.save(OutboxEntry.pending(id, "{}", "Event"));

    var xClaimed = relayX.loadPending(10);
    assertEquals(1, xClaimed.size());
    ageClaimPastLease(id);
    assertEquals(1, relayY.loadPending(10).size());

    assertEquals(0, relayX.releaseClaims(xClaimed, relayX.claimedBy()), "X released nothing");
    assertEquals("IN_PROGRESS", statusOf(id), "the entry stays claimed — by Y");
    assertEquals(relayY.claimedBy(), claimedByOf(id));
  }

  @Test
  void releaseClaims_emptyCollection_isANoOpWithoutAQuery() {
    assertEquals(0, store.releaseClaims(List.of(), store.claimedBy()));
  }

  private OutboxEntry loadRow(OutboxEntryId id) {
    return store.findByStatus(OutboxStatus.PENDING, 100).stream()
        .filter(e -> e.id().equals(id))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no PENDING row for " + id));
  }

  private String claimedByOf(OutboxEntryId id) throws Exception {
    return stringColumn(id, "claimed_by");
  }

  private String claimedAtOf(OutboxEntryId id) throws Exception {
    return stringColumn(id, "claimed_at");
  }

  private String stringColumn(OutboxEntryId id, String column) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT " + column + "::text AS v FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString("v");
      }
    }
  }

  // ── Backoff-shave race regression ───────────────────────────────────────────

  // The `locked` CTE's SELECT, verbatim, as concatenated from the production constant. We inject a
  // pg_sleep cross-join here so the claiming statement holds a deterministic window: after it
  // snapshots `eligible` but before it re-checks the row under the lock, a competitor connection
  // fully claims → markRetry(future) → commits. FOR UPDATE must become FOR UPDATE OF o once the
  // FROM has a second (function) relation.
  private static final String PROD_LOCKED_SELECT =
      "  SELECT entry_id FROM outbox_events"
          + "  WHERE entry_id IN (SELECT entry_id FROM eligible)"
          + "  ORDER BY seq ASC"
          + "  FOR UPDATE SKIP LOCKED";
  private static final String SLEEPING_LOCKED_SELECT =
      "  SELECT o.entry_id FROM outbox_events o, pg_sleep(2) s"
          + "  WHERE o.entry_id IN (SELECT entry_id FROM eligible)"
          + "  ORDER BY o.seq ASC"
          + "  FOR UPDATE OF o SKIP LOCKED";

  // The claimed-UPDATE WHERE, verbatim: the two lock-time re-check predicates that close the race.
  // status alone is insufficient — markRetry RESTORES status='PENDING' while setting a future
  // next_retry_at, so both predicates must be present.
  private static final String CLAIMED_WHERE_WITH_FIX =
      "  WHERE entry_id IN (SELECT entry_id FROM locked) AND status = 'PENDING'"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= NOW())";
  private static final String CLAIMED_WHERE_WITHOUT_FIX =
      "  WHERE entry_id IN (SELECT entry_id FROM locked) AND status = 'PENDING'";

  // The availability-first claim statement, verbatim, as CLAIM_PENDING stood before the ordering
  // modes — the ONLY
  // delta is the RETURNING list, which now carries the three skip columns every outbox SELECT
  // reads. Pins that the mode switch left the availability-first claim logic untouched.
  private static final String CLAIM_PENDING_TODAY =
      "WITH null_agg AS ("
          + "  SELECT entry_id, seq FROM outbox_events"
          + "  WHERE status = 'PENDING' AND aggregate_id IS NULL"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
          + "), agg_head AS ("
          + "  SELECT DISTINCT ON (aggregate_type, aggregate_id)"
          + "    entry_id, aggregate_type, aggregate_id, seq, next_retry_at"
          + "  FROM outbox_events"
          + "  WHERE status = 'PENDING' AND aggregate_id IS NOT NULL"
          + "  ORDER BY aggregate_type, aggregate_id, seq ASC"
          + "), agg_claimable AS ("
          + "  SELECT h.entry_id, h.seq FROM agg_head h"
          + "  WHERE (h.next_retry_at IS NULL OR h.next_retry_at <= NOW())"
          + "    AND NOT EXISTS ("
          + "      SELECT 1 FROM outbox_events ip"
          + "      WHERE ip.aggregate_type = h.aggregate_type AND ip.aggregate_id = h.aggregate_id"
          + "        AND ip.status = 'IN_PROGRESS')"
          + "), eligible AS ("
          + "  SELECT entry_id, seq FROM null_agg"
          + "  UNION ALL"
          + "  SELECT entry_id, seq FROM agg_claimable"
          + "  ORDER BY seq ASC LIMIT ?"
          + "), locked AS ("
          + "  SELECT entry_id FROM outbox_events"
          + "  WHERE entry_id IN (SELECT entry_id FROM eligible)"
          + "  ORDER BY seq ASC"
          + "  FOR UPDATE SKIP LOCKED"
          + "), claimed AS ("
          + "  UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_at = NOW(), claimed_by = ?"
          + "  WHERE entry_id IN (SELECT entry_id FROM locked) AND status = 'PENDING'"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
          + "  RETURNING entry_id, payload::text AS payload, payload_type, aggregate_type,"
          + "    aggregate_id, status,"
          + "    attempts, last_error, created_at, processed_at, next_retry_at,"
          + "    skipped_at, skipped_by, skip_reason, seq"
          + ") SELECT * FROM claimed ORDER BY seq ASC";

  /**
   * Guards against SQL drift: the derivations below only make sense if BOTH production statements
   * really contain the exact fragments we splice on. If someone reformats a claim statement, this
   * fails loudly instead of the probe silently testing a stale statement.
   */
  @Test
  void productionClaimSqlContainsBothLockTimeRecheckPredicates_bothModes() {
    for (var mode : OutboxOrderingMode.values()) {
      var sql = PostgresOutboxStore.claimPendingSql(mode);
      assertTrue(sql.contains(PROD_LOCKED_SELECT), mode + ": locked-CTE SELECT drifted");
      assertTrue(sql.contains(CLAIMED_WHERE_WITH_FIX), mode + ": both re-check predicates");
    }
  }

  @Test
  void strictClaimSql_headSetIncludesFailed_claimableRequiresPending() {
    // The strict statement differs from the availability-first one in exactly two CTEs.
    var strict = PostgresOutboxStore.claimPendingSql(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var af = PostgresOutboxStore.claimPendingSql(OutboxOrderingMode.AVAILABILITY_FIRST);
    assertTrue(
        strict.contains("WHERE status IN ('PENDING', 'FAILED') AND aggregate_id IS NOT NULL"));
    assertTrue(strict.contains("WHERE h.status = 'PENDING'"), "a FAILED head blocks");
    assertTrue(af.contains("WHERE status = 'PENDING' AND aggregate_id IS NOT NULL"));
    assertFalse(af.contains("h.status"), "availability-first has no head-status gate");
    assertEquals(
        CLAIM_PENDING_TODAY,
        af,
        "the availability-first statement is today's CLAIM_PENDING verbatim");
  }

  /**
   * Backoff-shave race: relay B's claiming statement snapshots the aggregate head as eligible (past
   * {@code next_retry_at}); inside B's statement window relay A claims it, fails, and {@code
   * markRetry} restores {@code status='PENDING'} while pushing {@code next_retry_at} one hour into
   * the future — then commits. B's lock-time re-check must then NOT claim the row despite the
   * restored {@code PENDING} status, because the second predicate ({@code next_retry_at <= NOW()},
   * transaction-stable) now excludes it. Ordering is never violated; without the predicate the
   * backoff window is merely shaved (one premature delivery attempt) — see the fail-first companion
   * {@link #withoutThePredicateTheShaveRaceWouldClaimTheBackedOffRow()}.
   */
  @Test
  void loadPending_doesNotShaveBackoffWhenMarkRetryCommitsInsideTheClaimWindow() throws Exception {
    // Derive B's probe SQL from the PRODUCTION constant (fix included) — only the sleep is
    // injected.
    // The "with fix" probe is derivable from BOTH production statements (the lock-time re-check
    // predicates are shared); the race itself runs the availability-first statement the fixture
    // uses.
    for (var mode : OutboxOrderingMode.values()) {
      var derived =
          PostgresOutboxStore.claimPendingSql(mode)
              .replace(PROD_LOCKED_SELECT, SLEEPING_LOCKED_SELECT);
      assertTrue(derived.contains("pg_sleep(2)"), mode + ": sleep injection failed");
      assertTrue(derived.contains(CLAIMED_WHERE_WITH_FIX), mode + ": probe must retain the fix");
    }
    var probeSql =
        PostgresOutboxStore.claimPendingSql(OutboxOrderingMode.AVAILABILITY_FIRST)
            .replace(PROD_LOCKED_SELECT, SLEEPING_LOCKED_SELECT);

    boolean bClaimed = runShaveRace(probeSql);

    assertFalse(
        bClaimed, "backoff was shaved: B claimed a row whose next_retry_at is in the future");
    assertRowIsPendingWithFutureBackoff();
  }

  /**
   * Fail-first companion / documentation: the SAME race, but B runs a probe with the {@code
   * next_retry_at} re-check STRIPPED (mechanically derived from the production constant). This is
   * exactly the pre-fix statement, and it DOES claim the backed-off row — proving the predicate is
   * load-bearing rather than incidental. Run first against the pre-fix SQL, this is the failure
   * that was captured; here it is asserted as the negative control.
   */
  @Test
  void withoutThePredicateTheShaveRaceWouldClaimTheBackedOffRow() throws Exception {
    var preFixSql =
        PostgresOutboxStore.claimPendingSql(OutboxOrderingMode.AVAILABILITY_FIRST)
            .replace(PROD_LOCKED_SELECT, SLEEPING_LOCKED_SELECT)
            .replace(CLAIMED_WHERE_WITH_FIX, CLAIMED_WHERE_WITHOUT_FIX);
    assertTrue(preFixSql.contains("pg_sleep(2)"), "sleep injection failed");
    // The strip must remove the fix predicate from the claimed UPDATE (the other two occurrences,
    // in null_agg / agg_claimable, are untouched — those gate the snapshot, not the lock-time
    // claim).
    assertFalse(preFixSql.contains(CLAIMED_WHERE_WITH_FIX), "fix predicate was not stripped");
    assertTrue(
        preFixSql.contains(CLAIMED_WHERE_WITHOUT_FIX),
        "stripped claimed-UPDATE WHERE must retain the status re-check");

    boolean bClaimed = runShaveRace(preFixSql);

    assertTrue(
        bClaimed,
        "negative control: WITHOUT the predicate B claims the backed-off row (the shave bug)");
  }

  /**
   * Drives one race and returns whether relay B's claim returned the row. Seeds a single aggregate
   * head, PENDING with a past next_retry_at. Runs {@code bProbeSql} (which sleeps 2s mid-statement)
   * on one connection; ~0.7s in, a competitor connection claims → markRetry(now + 1h) → commits.
   */
  private boolean runShaveRace(String bProbeSql) throws Exception {
    var id = OutboxEntryId.of("shave-head");
    var agg = TestStreams.stream("shave-agg");
    store.save(OutboxEntry.pending(id, "{}", "Event", agg));
    // Seed the head as an already-backed-off-but-now-eligible PENDING row (attempts=1, past
    // next_retry_at). This is a direct UPDATE, not the guarded markRetry — markRetry is now a CAS
    // on IN_PROGRESS + claimed_by and would no-op on this freshly-saved PENDING entry.
    exec(
        "UPDATE outbox_events SET attempts = 1, last_error = 'boom', "
            + "next_retry_at = NOW() - INTERVAL '5 seconds' WHERE entry_id = '"
            + id.value()
            + "'");

    var pool = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> bClaim =
          pool.submit(
              () -> {
                try (var conn = dataSource.getConnection();
                    var ps = conn.prepareStatement(bProbeSql)) {
                  ps.setInt(1, 10); // eligible LIMIT
                  ps.setString(2, "relay-B"); // claimed_by
                  try (var rs = ps.executeQuery()) {
                    return rs.next(); // true == B claimed the row
                  }
                }
              });

      // Competitor A: fire inside B's 2s sleep window.
      Future<?> aRace =
          pool.submit(
              () -> {
                try {
                  Thread.sleep(700);
                  // A claims the head, "fails" delivery, and markRetry restores PENDING while
                  // pushing next_retry_at into the future — committed before B's re-check runs.
                  try (var conn = dataSource.getConnection();
                      var claim =
                          conn.prepareStatement(
                              "UPDATE outbox_events SET status='IN_PROGRESS', claimed_by='relay-A' "
                                  + "WHERE entry_id = ? AND status = 'PENDING'")) {
                    claim.setString(1, id.value());
                    claim.executeUpdate();
                  }
                  // A holds the claim as 'relay-A' (raw claim above), so its guarded markRetry
                  // transitions: restores PENDING while pushing next_retry_at 1h out.
                  store.markRetry(
                      id, 2, "boom-again", java.time.Duration.ofSeconds(3600), "relay-A");
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      aRace.get(30, TimeUnit.SECONDS);
      return bClaim.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "probe pool did not terminate");
    }
  }

  private void assertRowIsPendingWithFutureBackoff() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, (next_retry_at > NOW()) AS future FROM outbox_events "
                    + "WHERE entry_id = 'shave-head'")) {
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(
            "PENDING", rs.getString("status"), "row must return to PENDING, not stay claimed");
        assertTrue(
            rs.getBoolean("future"), "row must keep its future next_retry_at (backoff intact)");
      }
    }
  }

  // ── FAILED operability ──────────────────────────────────────────────────────

  @Test
  void findByStatus_returnsOldestFirst_limited() {
    // seq is a BIGSERIAL assigned on INSERT; save f1..f4 in order (increasing seq), fail all four
    // in
    // ONE claimed batch, then enumerate.
    for (int i = 1; i <= 4; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("f" + i), "{}", "Event"));
    }
    assertEquals(4, store.loadPending(100).size(), "claim all four in one batch");
    for (int i = 1; i <= 4; i++) {
      assertTrue(store.markFailed(OutboxEntryId.of("f" + i), i, "boom", store.claimedBy()));
    }
    assertEquals(
        List.of("f1", "f2"),
        ids(store.findByStatus(OutboxStatus.FAILED, 2)),
        "findByStatus returns the two LOWEST-seq (oldest) FAILED entries");
  }

  @Test
  void findByStatus_isReadOnly_doesNotClaim() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("ro-1"), "{}", "Event"));
    // Enumerating PENDING must not claim the row (claimed_by untouched).
    assertEquals(List.of("ro-1"), ids(store.findByStatus(OutboxStatus.PENDING, 10)));
    assertEquals(
        1, store.loadPending(10).size(), "row is still claimable — findByStatus did not claim");
  }

  @Test
  void deleteSkipped_batchesPastTheChunkThreshold() throws Exception {
    // Seed 1500 aged SKIPPED rows so the chunked delete (LIMIT 1000 loop) must run more than once.
    // Shape: the three audit columns are set together on every SKIPPED row.
    exec(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
            + " status, attempts, last_error, created_at, processed_at, skipped_at, skipped_by,"
            + " skip_reason) "
            + "SELECT 'bulk-' || g, '{}'::jsonb, 'Event', 'test', 'agg-' || g, 'SKIPPED', 3, 'boom',"
            + " NOW(),"
            + " NOW() - INTERVAL '41 days', NOW() - INTERVAL '40 days', 'ops', 'bulk'"
            + " FROM generate_series(1, 1500) g");
    int deleted = store.deleteSkipped(java.time.Instant.now().minus(java.time.Duration.ofDays(30)));
    assertEquals(1500, deleted, "chunked delete must remove all 1500 across multiple batches");
    assertEquals(0, store.countByStatus(OutboxStatus.SKIPPED), "no SKIPPED rows remain");
    try (var conn = dataSource.getConnection();
        var rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM outbox_events")) {
      assertTrue(rs.next());
      assertEquals(0, rs.getLong(1), "the table is empty");
    }
  }

  // ── one-IN_PROGRESS-per-aggregate index (reset-vs-claim race) ─────────────────────────────────

  /**
   * The reset-vs-claim race that the application-level agg_claimable NOT EXISTS gate cannot close
   * under READ COMMITTED:
   *
   * <p>Aggregate A holds seq5 FAILED + seq6 PENDING (seq6 is the only PENDING head). claim1 sets
   * seq6 IN_PROGRESS in an OPEN, uncommitted transaction. resetFailedToPending(seq5) commits
   * seq5→PENDING (now the lower-seq head). A fresh claim2 snapshots post-reset: seq5 is A's head
   * and seq6's IN_PROGRESS is invisible (claim1 uncommitted), so the NOT EXISTS gate passes and
   * claim2 tries to set seq5 IN_PROGRESS — a DIFFERENT row, so no SKIP-LOCKED conflict.
   *
   * <p>WITHOUT the ux_outbox_one_inflight_per_stream unique index both commit → seq5 AND seq6
   * IN_PROGRESS (invariant + ordering violated). WITH the index, claim2's IN_PROGRESS write
   * conflicts with claim1's pending per-aggregate index entry, blocks, and — once claim1 commits —
   * fails with 23505. Exactly one IN_PROGRESS row for A survives.
   */
  @Test
  void resetRace_secondClaimOfSameAggregate_isRejectedByUniqueIndex() throws Exception {
    var agg = TestStreams.stream("race-agg");
    var seq5 = OutboxEntryId.of("race-seq5");
    var seq6 = OutboxEntryId.of("race-seq6");
    // Save in order so seq5 has the lower BIGSERIAL seq; fail seq5, leave seq6 PENDING.
    store.save(OutboxEntry.pending(seq5, "{}", "Event", agg));
    store.save(OutboxEntry.pending(seq6, "{}", "Event", agg));
    // Claim + fail seq5 only (loadPending claims one head per aggregate → seq5), leaving seq6
    // PENDING as the new head.
    assertEquals(List.of("race-seq5"), ids(store.loadPending(10)));
    assertTrue(store.markFailed(seq5, 3, "boom", store.claimedBy()));
    assertEquals("PENDING", statusOf(seq6));

    // claim1: set seq6 IN_PROGRESS in a HELD transaction (uncommitted) — mimics an in-flight claim.
    try (var held = dataSource.getConnection()) {
      held.setAutoCommit(false);
      try (var ps =
          held.prepareStatement(
              "UPDATE outbox_events SET status='IN_PROGRESS', claimed_at=NOW(), "
                  + "claimed_by='relay-1' WHERE entry_id = ? AND status='PENDING'")) {
        ps.setString(1, seq6.value());
        assertEquals(1, ps.executeUpdate());
      }

      // Reset seq5 FAILED→PENDING and commit (separate connection): seq5 is now A's lower-seq head.
      assertTrue(store.resetFailedToPending(seq5));

      // claim2 races on a background thread: its IN_PROGRESS write on seq5 must BLOCK on claim1's
      // pending per-aggregate index entry (it cannot see the uncommitted IN_PROGRESS otherwise).
      var pool = Executors.newSingleThreadExecutor();
      try {
        Future<List<OutboxEntry>> claim2 = pool.submit(() -> store.loadPending(10));
        // Give claim2 time to reach and block on the index conflict.
        Thread.sleep(500);
        assertFalse(claim2.isDone(), "claim2 must block on the one-inflight index, not race ahead");

        held.commit(); // claim1 wins the aggregate; claim2 unblocks and hits 23505 → benign empty.

        List<OutboxEntry> claim2Result = claim2.get(30, TimeUnit.SECONDS);
        assertTrue(
            claim2Result.isEmpty(),
            "claim2 must lose the aggregate (benign 23505 → empty), NOT double-claim");
      } finally {
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      }
    }

    // Final invariant: at most ONE IN_PROGRESS row for aggregate A.
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM outbox_events "
                    + "WHERE aggregate_type = ? AND aggregate_id = ? AND status = 'IN_PROGRESS'")) {
      ps.setString(1, agg.aggregateType().value());
      ps.setString(2, agg.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1), "≤1 IN_PROGRESS per aggregate must hold");
      }
    }
    assertEquals("IN_PROGRESS", statusOf(seq6), "the first claim (seq6) keeps the aggregate");
    assertEquals("PENDING", statusOf(seq5), "the losing claim's row stays PENDING for re-claim");
  }

  /**
   * The one-inflight index exists after migration and is a partial UNIQUE index on the stream
   * (aggregate_type, aggregate_id) scoped to IN_PROGRESS + non-null aggregate_id — the shape that
   * makes the reset-race DB-impossible without touching NULL-stream or non-IN_PROGRESS rows.
   */
  @Test
  void oneInflightUniqueIndex_isPresentWithExpectedPredicate() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT indexdef FROM pg_indexes "
                    + "WHERE schemaname = current_schema() AND indexname = ?")) {
      ps.setString(1, "ux_outbox_one_inflight_per_stream");
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "ux_outbox_one_inflight_per_stream must exist");
        String def = rs.getString("indexdef");
        assertTrue(def.contains("UNIQUE"), "must be a UNIQUE index: " + def);
        assertTrue(
            def.contains("(aggregate_type, aggregate_id)"), "must key on the stream pair: " + def);
        // Partial predicate: Postgres renders 'IN_PROGRESS' with a ::text cast and includes the
        // aggregate_id IS NOT NULL guard.
        assertTrue(
            def.contains("'IN_PROGRESS'") && def.contains("aggregate_id IS NOT NULL"),
            "must be partial on status = 'IN_PROGRESS' AND aggregate_id IS NOT NULL: " + def);
      }
    }
  }

  /**
   * Normal sequential claim of two DISTINCT aggregates is unaffected by the one-inflight index:
   * both heads are claimed in one batch (one IN_PROGRESS per aggregate), so the unique index never
   * fires.
   */
  @Test
  void oneInflightIndex_doesNotAffectNormalClaimAcrossDistinctAggregates() {
    store.save(
        OutboxEntry.pending(OutboxEntryId.of("n-a"), "{}", "Event", TestStreams.stream("agg-a")));
    store.save(
        OutboxEntry.pending(OutboxEntryId.of("n-b"), "{}", "Event", TestStreams.stream("agg-b")));
    assertEquals(
        2,
        store.loadPending(10).size(),
        "two distinct aggregates → two claims in one batch, index never trips");
  }

  // ── Lease-steal CAS regression ───────────────────────────────────

  /**
   * Relay X claims a row; its lease ages; relay Y re-claims it (→ IN_PROGRESS held by Y). X's late
   * markDelivered(X) must transition 0 rows (CAS on claimed_by), and the row stays IN_PROGRESS held
   * by Y — proving the mark* guard rejects a stolen-lease writer without corrupting Y's claim.
   */
  @Test
  void agedLeaseReclaimedByY_thenStaleXMarkDelivered_isNoOp_rowStaysInProgressHeldByY()
      throws Exception {
    var relayX = afStore();
    var relayY = afStore();
    var id = OutboxEntryId.of("m1-lease-steal");
    relayX.save(OutboxEntry.pending(id, "{}", "Event"));

    assertEquals(1, relayX.loadPending(10).size(), "X claims"); // held by X
    ageClaimPastLease(id);
    assertEquals(
        1, relayY.loadPending(10).size(), "Y re-claims the expired claim"); // now held by Y

    // X's stale markDelivered → 0 rows (not held by X). Row must NOT flip to DELIVERED.
    assertFalse(relayX.markDelivered(id, relayX.claimedBy()), "stale X markDelivered → 0 rows");
    assertEquals(
        "IN_PROGRESS", statusOf(id), "row stays IN_PROGRESS — Y still holds the live claim");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT claimed_by FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(relayY.claimedBy(), rs.getString("claimed_by"), "claim still held by Y");
      }
    }
    // Y can still complete its work normally.
    assertTrue(relayY.markDelivered(id, relayY.claimedBy()), "Y completes its claim");
    assertEquals("DELIVERED", statusOf(id));
  }

  // ── Ordering modes: constructor defaults, strict-mode stream check unwrapped, the V001 index
  // shapes ─────────

  @Test
  void defaultConstructors_areStrict_threeArgCtorIsExplicit() {
    assertEquals(
        OutboxOrderingMode.STRICT_PER_AGGREGATE,
        new PostgresOutboxStore(dataSource).orderingMode());
    assertEquals(
        OutboxOrderingMode.STRICT_PER_AGGREGATE,
        new PostgresOutboxStore(dataSource, java.time.Duration.ofSeconds(90)).orderingMode());
    assertEquals(OutboxOrderingMode.AVAILABILITY_FIRST, afStore().orderingMode());
    assertThrows(
        IllegalArgumentException.class,
        () -> new PostgresOutboxStore(dataSource, java.time.Duration.ofSeconds(90), null));
  }

  @Test
  void saveAll_nullAggregateId_strict_throwsOrderingViolation_notEventStoreException()
      throws Exception {
    // The check runs BEFORE the try block that wraps every Exception into
    // EventStoreException, so the caller (PostgresEventStore.append) sees the named type unwrapped
    // and nothing has touched the connection.
    var strict = new PostgresOutboxStore(dataSource);
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      var ex =
          assertThrows(
              OutboxOrderingViolationException.class,
              () ->
                  strict.saveAll(
                      List.of(
                          OutboxEntry.pending(
                              OutboxEntryId.of("ok-1"), "{}", "Event", TestStreams.stream("a")),
                          OutboxEntry.pending(OutboxEntryId.of("bad-1"), "{}", "Event")),
                      conn));
      assertEquals("bad-1", ex.entryId().value());
      conn.rollback();
    }
    assertEquals(
        0, strict.countByStatus(OutboxStatus.PENDING), "all-or-nothing: ok-1 not inserted");
    assertThrows(
        OutboxOrderingViolationException.class,
        () -> strict.save(OutboxEntry.pending(OutboxEntryId.of("bad-2"), "{}", "Event")));
  }

  @Test
  void streamSeqIndex_isPartialOverPendingAndFailed() throws Exception {
    String def = indexDef("idx_outbox_stream_seq");
    assertTrue(def.contains("(aggregate_type, aggregate_id, seq)"), def);
    assertTrue(def.contains("aggregate_id IS NOT NULL"), def);
    assertTrue(def.contains("'PENDING'") && def.contains("'FAILED'"), def);
    assertFalse(def.contains("(aggregate_id, status, seq)"), "the old three-column key is gone");
  }

  @Test
  void failedIndex_includesTheStreamPair_forAnIndexOnlyBlockageSample() throws Exception {
    String def = indexDef("idx_outbox_failed");
    assertTrue(def.contains("INCLUDE (aggregate_type, aggregate_id)"), def);
    assertTrue(def.contains("'FAILED'"), def);
  }

  @Test
  void skippedIndex_isPartialOnSkippedAt() throws Exception {
    String def = indexDef("idx_outbox_skipped");
    assertTrue(def.contains("(skipped_at)"), def);
    assertTrue(def.contains("'SKIPPED'"), def);
  }

  @Test
  void aStreamHalfPair_isRefusedByTheCheck() throws Exception {
    assertRefusedByCheck(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type)"
            + " VALUES ('half', '{}', 'E', 'order')",
        "outbox_events_stream_both_or_neither");
  }

  @Test
  void anIdWithoutAType_isRefusedByTheCheck() throws Exception {
    assertRefusedByCheck(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_id)"
            + " VALUES ('half-id', '{}', 'E', 'p-1')",
        "outbox_events_stream_both_or_neither");
  }

  @Test
  void aTypeThatBreaksTheSyntax_isRefusedByTheCheck() throws Exception {
    assertRefusedByCheck(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id)"
            + " VALUES ('bad-type', '{}', 'E', 'Order', 'p-1')",
        "outbox_events_aggregate_type_syntax");
  }

  private void assertRefusedByCheck(String insert, String constraint) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      var ex = assertThrows(java.sql.SQLException.class, () -> stmt.execute(insert));
      assertEquals("23514", ex.getSQLState(), ex.getMessage());
      assertTrue(ex.getMessage().contains(constraint), ex.getMessage());
    }
  }

  @Test
  void sampleBlockage_isOneIndexOnlyScanOnTheFailedIndex() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      // 20 000 rows; 2 000 FAILED, over streams of both types sharing id values, 100 of them with a
      // NULL stream; then fresh statistics and a set visibility map, and the scans that would hide
      // the index on a small table switched off.
      stmt.execute(
          "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
              + " status, attempts, created_at, processed_at)"
              + " SELECT 'blk-' || g, '{}'::jsonb, 'E',"
              + " CASE WHEN g % 200 = 0 THEN NULL WHEN g % 3 = 0 THEN 'product' ELSE 'inventory' END,"
              + " CASE WHEN g % 200 = 0 THEN NULL ELSE 'p-' || (g % 200) END,"
              + " CASE WHEN g % 10 = 0 THEN 'FAILED' ELSE 'DELIVERED' END, 1, NOW(), NOW()"
              + " FROM generate_series(1, 20000) g");
      stmt.execute("VACUUM ANALYZE outbox_events");
      stmt.execute("SET enable_seqscan = off");
      stmt.execute("SET enable_bitmapscan = off");
      // The statement the store runs, read from the store itself so the pin cannot drift from it.
      var field = PostgresOutboxStore.class.getDeclaredField("SAMPLE_BLOCKAGE");
      field.setAccessible(true);
      var plan = ExplainPlans.explainJson(conn, (String) field.get(null));
      var scan = ExplainPlans.findIndexScan(plan, "idx_outbox_failed");
      assertNotNull(scan, plan.toString());
      assertEquals("Index Only Scan", scan.path("Node Type").asText(), plan.toString());
    }
  }

  private String indexDef(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND"
                    + " indexname = ?")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), name + " must exist");
        return rs.getString("indexdef");
      }
    }
  }
}

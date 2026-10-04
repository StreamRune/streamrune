package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Ordering under real PostgreSQL READ COMMITTED: two relays never claim past a FAILED head in
 * strict mode, a reclaimed head is the head again, skip-vs-replay resolves exactly once, the
 * reset-vs-claim race that needs ux_outbox_one_inflight_per_stream in availability-first mode
 * cannot even start in strict mode, and both claim statements read the head scan from
 * idx_outbox_stream_seq with no Sort above it.
 */
@Testcontainers
class PostgresOutboxStoreStrictOrderingIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("outbox_strict_ordering");

  static PGSimpleDataSource dataSource;
  static final StreamId A = TestStreams.stream("agg-strict");

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
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
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void clean() throws Exception {
    exec("DELETE FROM outbox_events");
  }

  private static PostgresOutboxStore strict(Duration lease) {
    return new PostgresOutboxStore(dataSource, lease, OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  private static void exec(String sql) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private static String statusOf(String id) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString(1);
      }
    }
  }

  private static List<String> ids(List<OutboxEntry> entries) {
    return entries.stream().map(e -> e.id().value()).toList();
  }

  /** n0 FAILED head of A plus {@code successors} PENDING rows n1..nK. */
  private void seedBlockedAggregate(PostgresOutboxStore store, int successors) {
    store.save(OutboxEntry.pending(OutboxEntryId.of("n0"), "{}", "E", A));
    for (int i = 1; i <= successors; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("n" + i), "{}", "E", A));
    }
    assertEquals(List.of("n0"), ids(store.loadPending(10)));
    assertTrue(store.markFailed(OutboxEntryId.of("n0"), 10, "poison", store.claimedBy()));
  }

  @Test
  void twoConcurrentRelays_strict_neverClaimPastAFailedHead_thenDeliverInSeqOrderAfterSkip()
      throws Exception {
    var r1 = strict(Duration.ofMinutes(4));
    var r2 = strict(Duration.ofMinutes(4));
    seedBlockedAggregate(r1, 50);

    var claimed = new ConcurrentLinkedQueue<String>();
    var barrier = new CyclicBarrier(2);
    var pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (var relay : List.of(r1, r2)) {
        futures.add(
            pool.submit(
                () -> {
                  barrier.await();
                  for (int i = 0; i < 100; i++) {
                    claimed.addAll(ids(relay.loadPending(10)));
                  }
                  return null;
                }));
      }
      for (var f : futures) f.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    assertTrue(claimed.isEmpty(), "a FAILED head blocks A for every relay: " + claimed);
    assertEquals("PENDING", statusOf("n1"));

    // Skip releases the aggregate; the successors are then delivered in strict seq order even
    // with two relays racing for each head.
    assertTrue(r1.skipFailed(OutboxEntryId.of("n0"), "ops", "poison"));
    var delivered = new ArrayList<String>();
    // Bounded: a regression in which a poll pair claims nothing must fail here, not hang the task.
    for (int round = 0; round < 200 && delivered.size() < 50; round++) {
      var batch = new ArrayList<OutboxEntry>();
      batch.addAll(r1.loadPending(10));
      batch.addAll(r2.loadPending(10));
      assertTrue(batch.size() <= 1, "at most one entry of A in flight: " + ids(batch));
      for (var e : batch) {
        var relay = r1.claimedBy().equals(claimedByOf(e)) ? r1 : r2;
        assertTrue(relay.markDelivered(e.id(), relay.claimedBy()));
        delivered.add(e.id().value());
      }
    }
    assertEquals(50, delivered.size(), "every successor delivered after the skip: " + delivered);
    var expected = new ArrayList<String>();
    for (int i = 1; i <= 50; i++) expected.add("n" + i);
    assertEquals(expected, delivered, "strict seq order after the skip");
  }

  private static String claimedByOf(OutboxEntry e) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT claimed_by FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, e.id().value());
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString(1);
      }
    }
  }

  @Test
  void leaseExpiry_reclaimedHeadIsHeadAgain_noSuccessorClaimedMeanwhile() throws Exception {
    // R1 claims the head and dies. For one lease A is held (NOT EXISTS); after it the reclaim
    // returns n0 to PENDING and n0 — not n1 — is claimed next.
    var r1 = strict(Duration.ofSeconds(1));
    var r2 = strict(Duration.ofSeconds(1));
    r1.save(OutboxEntry.pending(OutboxEntryId.of("n0"), "{}", "E", A));
    r1.save(OutboxEntry.pending(OutboxEntryId.of("n1"), "{}", "E", A));
    assertEquals(List.of("n0"), ids(r1.loadPending(10)));
    assertTrue(r2.loadPending(10).isEmpty(), "A is held while n0 is IN_PROGRESS");
    exec(
        "UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '5 seconds' WHERE entry_id = 'n0'");
    assertEquals(List.of("n0"), ids(r2.loadPending(10)), "the reclaimed head is the head again");
    assertEquals("PENDING", statusOf("n1"));
  }

  @Test
  void skipVsReplay_race_exactlyOneResolution() throws Exception {
    // Both are UPDATE … WHERE status = 'FAILED' on one row; the row lock serializes
    // them and the loser's EvalPlanQual re-check sees status <> 'FAILED' → 0 rows.
    var store = strict(Duration.ofMinutes(4));
    for (int round = 0; round < 20; round++) {
      exec("DELETE FROM outbox_events");
      seedBlockedAggregate(store, 1);
      var start = new CountDownLatch(1);
      var pool = Executors.newFixedThreadPool(2);
      try {
        Future<Boolean> skip =
            pool.submit(
                () -> {
                  start.await();
                  return store.skipFailed(OutboxEntryId.of("n0"), "ops", "race");
                });
        Future<Boolean> replay =
            pool.submit(
                () -> {
                  start.await();
                  return store.resetFailedToPending(OutboxEntryId.of("n0"));
                });
        start.countDown();
        boolean s = skip.get(30, TimeUnit.SECONDS);
        boolean r = replay.get(30, TimeUnit.SECONDS);
        assertTrue(
            s ^ r, "round " + round + ": exactly one wins (skip=" + s + ", replay=" + r + ")");
        assertEquals(s ? "SKIPPED" : "PENDING", statusOf("n0"));
      } finally {
        pool.shutdownNow();
      }
    }
  }

  @Test
  void skipThenClaim_releasesOnNextPoll_claimThenSkip_releasesOnThePollAfter() throws Exception {
    var store = strict(Duration.ofMinutes(4));
    seedBlockedAggregate(store, 2);
    assertTrue(store.loadPending(10).isEmpty());
    assertTrue(store.skipFailed(OutboxEntryId.of("n0"), "ops", "r"));
    assertEquals(List.of("n1"), ids(store.loadPending(10)), "released on the next poll");
    assertTrue(store.loadPending(10).isEmpty(), "n2 waits for n1 (in flight)");
    assertTrue(store.markDelivered(OutboxEntryId.of("n1"), store.claimedBy()));
    assertEquals(List.of("n2"), ids(store.loadPending(10)));
  }

  @Test
  void resetRace_isUnreachableInStrictMode_noClaimWhileHeadIsFailed() throws Exception {
    // In availability-first mode the reset-vs-claim race needs the unique index
    // (PostgresOutboxStoreTest.resetRace_secondClaimOfSameAggregate_isRejectedByUniqueIndex). In
    // strict mode S2 sees n0 as a FAILED head and claims nothing of A, so the race cannot start:
    // after the reset, the only claimable row is n0 itself and no 23505 is ever raised.
    var store = strict(Duration.ofMinutes(4));
    seedBlockedAggregate(store, 1);
    assertTrue(store.loadPending(10).isEmpty(), "S2 claims nothing of A");
    assertTrue(store.resetFailedToPending(OutboxEntryId.of("n0")));
    assertEquals(List.of("n0"), ids(store.loadPending(10)), "n0 is the head again");
    assertEquals("PENDING", statusOf("n1"));
  }

  /**
   * Two streams of different aggregate types sharing one id value have separate heads: an in-flight
   * head of one never holds the other back (in either claim statement), and neither does a FAILED
   * head.
   */
  @Test
  void aHeadOfOneType_inFlightOrFailed_neverHoldsAnotherTypeSharingItsIdValue() throws Exception {
    var product = StreamId.of(AggregateType.of("product"), AggregateId.of("p-1"));
    var inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"));
    for (var mode : OutboxOrderingMode.values()) {
      exec("DELETE FROM outbox_events");
      var relayA = new PostgresOutboxStore(dataSource, Duration.ofMinutes(4), mode);
      var relayB = new PostgresOutboxStore(dataSource, Duration.ofMinutes(4), mode);
      relayA.save(OutboxEntry.pending(OutboxEntryId.of("prod-1"), "{}", "ProductCreated", product));
      assertEquals(List.of("prod-1"), ids(relayA.loadPending(10)), mode.toString());
      for (int i = 1; i <= 3; i++) {
        relayA.save(
            OutboxEntry.pending(OutboxEntryId.of("inv-" + i), "{}", "StockReserved", inventory));
      }

      // product:p-1 is in flight with relay A; inventory:p-1 is a different stream.
      var firstB = relayB.loadPending(10);
      assertEquals(
          List.of("inv-1"),
          ids(firstB),
          mode + ": an in-flight head of product:p-1 must not hold back inventory:p-1");
      var secondB = relayB.loadPending(10);
      assertTrue(secondB.isEmpty(), mode + ": inv-2 waits for inv-1 (in flight): " + ids(secondB));
      assertEquals("IN_PROGRESS", statusOf("prod-1"), mode.toString());

      // product:p-1 fails; inventory:p-1 keeps flowing in seq order.
      assertTrue(relayA.markFailed(OutboxEntryId.of("prod-1"), 10, "poison", relayA.claimedBy()));
      assertTrue(relayB.markDelivered(OutboxEntryId.of("inv-1"), relayB.claimedBy()));
      var delivered = new ArrayList<String>(List.of("inv-1"));
      for (int round = 0; round < 3; round++) {
        var claimedA = relayA.loadPending(10);
        var claimedB = relayB.loadPending(10);
        var batch = new ArrayList<OutboxEntry>(claimedA);
        batch.addAll(claimedB);
        assertTrue(
            batch.size() <= 1, mode + ": at most one entry of a stream in flight: " + ids(batch));
        for (var e : claimedA) {
          delivered.add(e.id().value());
          assertTrue(relayA.markDelivered(e.id(), relayA.claimedBy()));
        }
        for (var e : claimedB) {
          delivered.add(e.id().value());
          assertTrue(relayB.markDelivered(e.id(), relayB.claimedBy()));
        }
      }
      assertEquals(List.of("inv-1", "inv-2", "inv-3"), delivered, mode.toString());
      assertEquals(
          OutboxStatus.FAILED,
          relayA.findById(OutboxEntryId.of("prod-1")).orElseThrow().status(),
          mode.toString());
    }
  }

  // ── Acceptance: the head scan reads idx_outbox_stream_seq with no Sort above it ──────────

  @Test
  void explain_claim_headScanUsesStreamSeqIndex_noSort_bothModes() throws Exception {
    // 100k rows, 20k streams, every 50th row FAILED: the shape the partial index exists for.
    exec(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
            + " status, processed_at) SELECT 'seed-' || g, '{}'::jsonb, 'E', 'test', 'agg-' ||"
            + " lpad((g % 20000)::text, 6, '0'), CASE WHEN g % 50 = 0 THEN 'FAILED' ELSE 'PENDING'"
            + " END, CASE WHEN g % 50 = 0 THEN NOW() END FROM generate_series(1, 100000) g");
    exec("VACUUM ANALYZE outbox_events");
    var mapper = new ObjectMapper();
    for (var mode : OutboxOrderingMode.values()) {
      String sql = "EXPLAIN (FORMAT JSON) " + PostgresOutboxStore.claimPendingSql(mode);
      JsonNode plan;
      try (var conn = dataSource.getConnection();
          var ps = conn.prepareStatement(sql)) {
        ps.setInt(1, 100);
        ps.setString(2, "explain-relay");
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next());
          plan = mapper.readTree(rs.getString(1)).get(0).get("Plan");
        }
      }
      JsonNode headScan = ExplainPlans.findIndexScan(plan, "idx_outbox_stream_seq");
      assertNotNull(headScan, mode + ": the head scan must use idx_outbox_stream_seq:\n" + plan);
      assertFalse(
          sortBetweenUniqueAndIndexScan(plan, headScan),
          mode
              + ": no Sort/Incremental Sort between the Unique (DISTINCT ON) and the index scan:\n"
              + plan);
    }
  }

  /**
   * True iff a Sort/Incremental Sort sits on the path from the nearest Unique ancestor down to the
   * scan. Fails when the path has no Unique ancestor at all: the DISTINCT ON head selection must be
   * a Unique fed by the index scan, otherwise "no Sort below the Unique" would hold vacuously.
   */
  private static boolean sortBetweenUniqueAndIndexScan(JsonNode root, JsonNode scan) {
    List<JsonNode> path = new ArrayList<>();
    assertTrue(pathTo(root, scan, path), "scan node must be reachable from the root");
    boolean belowUnique = false;
    boolean sortBelowUnique = false;
    for (JsonNode n : path) {
      String type = n.path("Node Type").asText("");
      if (type.equals("Unique")) belowUnique = true;
      if (belowUnique && (type.equals("Sort") || type.equals("Incremental Sort"))) {
        sortBelowUnique = true;
      }
    }
    assertTrue(belowUnique, "the head scan must sit under the Unique (DISTINCT ON) node:\n" + root);
    return sortBelowUnique;
  }

  private static boolean pathTo(JsonNode node, JsonNode target, List<JsonNode> path) {
    path.add(node);
    if (node == target) return true;
    for (JsonNode child : node.path("Plans")) {
      if (pathTo(child, target, path)) return true;
    }
    path.remove(path.size() - 1);
    return false;
  }
}

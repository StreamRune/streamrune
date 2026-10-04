package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.types.EventType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for {@link PostgresOutboxStore#markDeliveredAll}, {@link
 * PostgresOutboxStore#deleteDelivered} and {@link PostgresOutboxStore#deleteSkipped} — the two
 * retention sweeps. There is no FAILED sweep: the framework never deletes an unresolved {@code
 * FAILED} row, and the tests here pin that an aged {@code FAILED} row survives both sweeps. The
 * store runs on the production default ({@code STRICT_PER_AGGREGATE}); every entry carries an
 * aggregate.
 */
@Testcontainers
class PostgresOutboxStoreRetentionIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_outbox_retention_it");

  static PGSimpleDataSource dataSource;
  PostgresOutboxStore store;

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
            // No @Encrypted-bearing types are exercised through this registry in this test; report
            // none explicitly rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    store = new PostgresOutboxStore(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
    }
  }

  // ── Helper ────────────────────────────────────────────────────────────────

  private void backdateProcessedAt(String entryId, String interval) throws Exception {
    backdate("processed_at", entryId, interval);
  }

  private void backdateSkippedAt(String entryId, String interval) throws Exception {
    backdate("skipped_at", entryId, interval);
  }

  private void backdate(String column, String entryId, String interval) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE outbox_events SET "
                    + column
                    + " = NOW() - INTERVAL '"
                    + interval
                    + "' WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      ps.executeUpdate();
    }
  }

  private long countRows() throws Exception {
    try (var conn = dataSource.getConnection();
        var rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM outbox_events")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private String statusOf(String entryId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  /** save → claim → markFailed, then backdate processed_at so the FAILED row is past any cutoff. */
  private void makeAgedFailed(String entryId, String agg) throws Exception {
    var id = OutboxEntryId.of(entryId);
    store.save(OutboxEntry.pending(id, "{}", "T", TestStreams.stream(agg)));
    store.loadPending(10); // → IN_PROGRESS under store.claimedBy()
    store.markFailed(id, 3, "boom", store.claimedBy()); // → FAILED, processed_at = NOW()
    backdateProcessedAt(entryId, "10 days");
  }

  /** {@link #makeAgedFailed}, then the audited operator skip and an aged skipped_at. */
  private void makeAgedSkipped(String entryId, String agg) throws Exception {
    makeAgedFailed(entryId, agg);
    assertTrue(store.skipFailed(OutboxEntryId.of(entryId), "ops", "r"), "FAILED -> SKIPPED");
    backdateSkippedAt(entryId, "10 days");
  }

  // ── Tests ─────────────────────────────────────────────────────────────────

  @Test
  void deleteDelivered_skips_rows_within_cutoff() {
    var id = OutboxEntryId.of("ret-e1");
    store.save(OutboxEntry.pending(id, "{}", "T", TestStreams.stream("agg")));
    store.loadPending(10); // claims it → IN_PROGRESS
    store.markDelivered(id, store.claimedBy()); // processed_at = NOW()

    // cutoff = now - 1 day: the row is too recent → nothing deleted
    int deleted = store.deleteDelivered(Instant.now().minusSeconds(86_400));
    assertEquals(0, deleted, "row processed just now should be newer than the cutoff");
  }

  @Test
  void deleteDelivered_removes_old_delivered_rows() throws Exception {
    var id = OutboxEntryId.of("ret-e2");
    store.save(OutboxEntry.pending(id, "{}", "T", TestStreams.stream("agg")));
    store.loadPending(10);
    store.markDelivered(id, store.claimedBy());

    backdateProcessedAt("ret-e2", "10 days");

    // cutoff = now - 7 days: row is 10 days old → deleted
    int deleted = store.deleteDelivered(Instant.now().minusSeconds(7L * 86_400));
    assertEquals(1, deleted);
    assertEquals(0, countRows());
  }

  @Test
  void markDeliveredAll_marks_multiple_entries_and_delete_cleans_them_up() throws Exception {
    var idA = OutboxEntryId.of("ret-batch-a");
    var idB = OutboxEntryId.of("ret-batch-b");

    // Distinct aggregates so a single loadPending claims BOTH heads (per-aggregate ordering claims
    // at most one entry per aggregate per batch).
    store.save(OutboxEntry.pending(idA, "{}", "T", TestStreams.stream("agg-a")));
    store.save(OutboxEntry.pending(idB, "{}", "T", TestStreams.stream("agg-b")));

    // Claim both
    assertEquals(2, store.loadPending(10).size());

    // Mark both delivered in one batch call under the claiming identity
    assertEquals(2, store.markDeliveredAll(List.of(idA, idB), store.claimedBy()));

    backdateProcessedAt("ret-batch-a", "10 days");
    backdateProcessedAt("ret-batch-b", "10 days");

    int deleted = store.deleteDelivered(Instant.now().minusSeconds(7L * 86_400));
    assertEquals(2, deleted);
    assertEquals(0, countRows());
  }

  @Test
  void deleteSkipped_removes_old_skipped_rows() throws Exception {
    makeAgedSkipped("ret-skip-1", "agg-s1");

    // cutoff = now - 7 days: skipped_at is 10 days old → deleted
    int deleted = store.deleteSkipped(Instant.now().minusSeconds(7L * 86_400));
    assertEquals(1, deleted);
    assertEquals(0, countRows());
  }

  /**
   * An aged {@code FAILED} row is never a retention candidate — the only exits from {@code FAILED}
   * are the two audited operator operations. Even a cutoff in the FUTURE (every {@code SKIPPED} row
   * is aged relative to it) leaves the {@code FAILED} row standing.
   */
  @Test
  void deleteSkipped_neverTouchesFailed_evenWhenAged() throws Exception {
    makeAgedFailed("ret-keep-failed", "agg-keep-f");
    makeAgedSkipped("ret-keep-skipped", "agg-keep-s");

    int deleted = store.deleteSkipped(Instant.now().plusSeconds(3600));

    assertEquals(1, deleted, "only the SKIPPED row is a retention candidate");
    assertEquals("FAILED", statusOf("ret-keep-failed"), "the aged FAILED row survives");
    assertEquals(1, countRows());
  }

  /**
   * Regression guard for the SQL itself: the outer DELETE must re-check status on the live row, not
   * only entry_id membership. Nothing leaves {@code SKIPPED} today, so the EvalPlanQual race the
   * FAILED sweep once reproduced cannot occur — which is exactly why the guard has to be
   * structural: nothing else pins the predicate, and a future transition out of {@code SKIPPED} (or
   * an edit loosening the subselect) would silently reopen the hole.
   */
  @Test
  void deleteSkippedBatchSql_has_outer_status_guard() throws Exception {
    var field = PostgresOutboxStore.class.getDeclaredField("DELETE_SKIPPED_BATCH");
    field.setAccessible(true);
    String sql = ((String) field.get(null)).replaceAll("\\s+", " ");
    assertTrue(
        sql.startsWith("DELETE FROM outbox_events WHERE status = 'SKIPPED' AND entry_id IN ("),
        "DELETE_SKIPPED_BATCH must re-check status='SKIPPED' on the OUTER delete; was: " + sql);
  }

  /**
   * The DELIVERED retention sweep must carry the SAME outer status re-check as {@code
   * DELETE_SKIPPED_BATCH}. The subselect's status filter alone is only snapshot-deep: under READ
   * COMMITTED, EvalPlanQual re-evaluates just the outer predicates against a concurrently-updated
   * row version, so without the outer guard a row whose status changed inside the delete window
   * would still be deleted on entry_id membership alone. {@code DELIVERED} is terminal today, which
   * is exactly why this guard needs a ratchet: nothing else pins the predicate, and a future status
   * transition out of DELIVERED (or an edit that loosens the subselect) would silently reopen the
   * hole the FAILED sweep closed and {@code DELETE_SKIPPED_BATCH} keeps closed.
   */
  @Test
  void deleteDeliveredBatchSql_has_outer_status_guard() throws Exception {
    var field = PostgresOutboxStore.class.getDeclaredField("DELETE_DELIVERED_BATCH");
    field.setAccessible(true);
    String sql = ((String) field.get(null)).replaceAll("\\s+", " ");
    assertTrue(
        sql.matches("(?i)DELETE FROM outbox_events WHERE status = 'DELIVERED' AND entry_id IN.*"),
        "DELETE_DELIVERED_BATCH must re-check status='DELIVERED' on the OUTER delete; was: " + sql);
  }

  /**
   * Sibling-seeded coverage: the {@code status = 'DELIVERED'} predicate had zero mutant coverage —
   * every prior retention test seeded delivered-only rows, so a mutant that dropped the status
   * filter (deleting ANY aged row) survived the suite. An aged FAILED row and a PENDING row of
   * other aggregates must survive {@code deleteDelivered}; symmetrically, an aged DELIVERED row and
   * the aged FAILED row must survive {@code deleteSkipped}.
   */
  @Test
  void deleteDelivered_leavesAgedFailedAndPendingSiblings() throws Exception {
    // Aged DELIVERED row (the only legitimate target).
    var delivered = OutboxEntryId.of("ret-sib-delivered");
    store.save(OutboxEntry.pending(delivered, "{}", "T", TestStreams.stream("agg-sib-d")));
    store.loadPending(10);
    store.markDelivered(delivered, store.claimedBy());
    backdateProcessedAt("ret-sib-delivered", "10 days");

    // Aged FAILED sibling: same age, wrong status — must survive.
    makeAgedFailed("ret-sib-failed", "agg-sib-f");

    // PENDING sibling (processed_at NULL): must survive.
    store.save(
        OutboxEntry.pending(
            OutboxEntryId.of("ret-sib-pending"), "{}", "T", TestStreams.stream("agg-sib-p")));

    int deleted = store.deleteDelivered(Instant.now().minusSeconds(7L * 86_400));

    assertEquals(1, deleted, "only the aged DELIVERED row may be swept");
    assertEquals(2, countRows(), "the FAILED and PENDING siblings must survive deleteDelivered");
    assertEquals("FAILED", statusOf("ret-sib-failed"), "the aged FAILED sibling must survive");
    assertEquals("PENDING", statusOf("ret-sib-pending"), "the PENDING sibling must survive");

    // Symmetric direction: deleteSkipped must not touch an aged DELIVERED row (nor the aged FAILED
    // sibling). Re-seed a DELIVERED row next to an aged SKIPPED one.
    var delivered2 = OutboxEntryId.of("ret-sib-delivered-2");
    store.save(OutboxEntry.pending(delivered2, "{}", "T", TestStreams.stream("agg-sib-d2")));
    store.loadPending(10);
    store.markDelivered(delivered2, store.claimedBy());
    backdateProcessedAt("ret-sib-delivered-2", "10 days");
    makeAgedSkipped("ret-sib-skipped", "agg-sib-s");

    int skippedSwept = store.deleteSkipped(Instant.now().minusSeconds(7L * 86_400));
    assertEquals(1, skippedSwept, "only the aged SKIPPED row may be swept by deleteSkipped");
    assertEquals(
        "DELIVERED",
        statusOf("ret-sib-delivered-2"),
        "an aged DELIVERED row must survive deleteSkipped");
    assertEquals(
        "FAILED", statusOf("ret-sib-failed"), "the aged FAILED sibling survives deleteSkipped too");
  }
}

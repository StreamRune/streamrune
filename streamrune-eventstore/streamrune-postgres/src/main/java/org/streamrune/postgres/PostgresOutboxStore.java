package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.types.LogSanitizer;

/**
 * PostgreSQL-backed {@link OutboxStore}. Persists outbox entries as JSONB in the {@code
 * outbox_events} table, created by the framework's baseline Flyway migration ({@code
 * V001__streamrune_baseline.sql}); this class never creates it.
 *
 * <p>Idempotent save: uses {@code ON CONFLICT (entry_id) DO NOTHING} so duplicate saves (same
 * {@link OutboxEntryId}) are silently ignored.
 *
 * <p>Competing consumers: {@link #loadPending} <em>claims</em> entries instead of merely reading
 * them. Eligible {@code PENDING} rows are atomically moved to {@code IN_PROGRESS} (with {@code
 * claimed_at}/{@code claimed_by} set) via {@code FOR UPDATE SKIP LOCKED}, so two relay instances
 * polling concurrently never receive the same entry. Claims are ordered by the {@code seq} column —
 * {@code created_at} uses {@code NOW()}, which ties for rows written in one transaction.
 *
 * <p>Per-stream ordering: for entries with a non-null stream ({@code aggregate_type}, {@code
 * aggregate_id}), at most one entry per stream is {@code IN_PROGRESS} at any instant — always that
 * stream's lowest-{@code seq} eligible head. If the head is still backing off ({@code
 * next_retry_at} in the future), the whole stream waits; a higher {@code seq} is never claimed past
 * it. {@code NULL}-stream entries have no ordering constraint and are claimed freely. Consequently
 * a single batch contains at most one entry per stream; two aggregate types sharing an id value are
 * two streams with independent heads.
 *
 * <p><b>Two ordering modes</b> ({@link OutboxOrderingMode}, chosen in the constructor and nowhere
 * else; strict by default). Under {@link OutboxOrderingMode#STRICT_PER_AGGREGATE} the head of an
 * aggregate is its lowest-{@code seq} row among {@code PENDING} <em>and</em> {@code FAILED}, so a
 * terminal {@code FAILED} head <b>blocks</b> every later row of its aggregate until an operator
 * replays it ({@link #resetFailedToPending} — same row, same {@code seq}, delivered before its
 * successors) or skips it ({@link #skipFailed} — audited {@code SKIPPED}, releases the aggregate).
 * On a strict channel every entry must carry a {@code streamId}; {@link #save} and {@code saveAll}
 * refuse a {@code null} one with {@link OutboxOrderingViolationException} before any write,
 * unwrapped, so an enclosing append rolls back. Under {@link OutboxOrderingMode#AVAILABILITY_FIRST}
 * the head set is {@code PENDING} only (the original statement): a {@code FAILED} row is not a
 * head, the next {@code PENDING} row of its aggregate is delivered past it (a permanent gap, and a
 * later replay arrives after its already-delivered successors), and a {@code null} aggregate is
 * accepted. In both modes {@link #delete} refuses a {@code FAILED} row and no retention sweep ever
 * removes one ({@link #deleteSkipped} prunes only {@code SKIPPED}). See {@link
 * org.streamrune.core.outbox.OutboxStore#loadPending}.
 *
 * <p>Lease reclaim: a relay that crashes after claiming leaves rows stuck in {@code IN_PROGRESS}.
 * Each {@link #loadPending} call first resets claims older than the configured lease back to {@code
 * PENDING} so another instance can pick them up.
 */
public final class PostgresOutboxStore implements OutboxStore {

  /**
   * Default claim lease: an {@code IN_PROGRESS} claim older than this is considered abandoned and
   * reclaimed. <b>Four minutes</b> — raised from one minute and then from three so the framework's
   * own out-of-the-box defaults stay internally consistent: the lease must strictly exceed every
   * wired {@code OutboxPublisher}'s in-flight horizon (enforced by {@code OutboxPoller.build()}),
   * and the largest shipped default horizon is Kafka's {@code max.block.ms} (60s — the pre-enqueue
   * {@code send()} block) plus {@code delivery.timeout.ms} (120s — the post-enqueue internal retry
   * window) = <b>180s</b>. Three minutes exactly EQUALLED that horizon and the guard demands a
   * strict excess, so the shipped defaults would have refused to boot; four minutes clears 180s
   * with the guard's own 30s clock-skew margin.
   *
   * <p>The cost is crash-reclaim latency: a relay that dies mid-batch leaves its rows {@code
   * IN_PROGRESS} for up to four minutes before another relay reclaims them. That is the deliberate
   * trade — a shorter lease buys faster failover at the price of reclaiming entries a live
   * transport may still deliver (a duplicate <em>and</em> a same-aggregate reorder, which the
   * ordering contract forbids). A deployment that wants a shorter lease should <b>shrink the
   * horizon first</b>: lowering {@code max.block.ms} to a few seconds (ample on a healthy cluster)
   * and/or {@code delivery.timeout.ms} shrinks it one-for-one, and a deployment using only
   * RabbitMQ/HTTP (default horizons 60s/10s) can configure a shorter lease outright — but note that
   * RabbitMQ's 60s is {@code publishBlockBudget} (30s) + {@code confirmTimeout} (30s), and only the
   * second half is a timeout the client actually enforces, so shrink the first half by bounding the
   * write (NIO {@code writeEnqueuingTimeoutInMs}) rather than by simply declaring a smaller budget.
   *
   * <p><b>Caveat on lowering {@code delivery.timeout.ms}</b>: keep it <em>above</em> the {@code
   * KafkaOutboxPublisher} {@code sendTimeout} (10s default). Below it, the producer expires each
   * send before the publisher's own bounded {@code send().get()} ever fires, so every degraded-send
   * failure surfaces Kafka's own {@code TimeoutException} rather than the send-bound one. That
   * expiry does NOT prove the record is dead — a produce request carrying it may still be in flight
   * and the broker may still append it — so the publisher classifies it {@code IN_FLIGHT} and the
   * entry is held for a full claim lease instead of retrying on the fast transport cadence. Safe,
   * but slower than intended, and the {@code KafkaOutboxPublisher} builder WARNs about the inverted
   * tuning when it can see the producer config.
   */
  static final Duration DEFAULT_CLAIM_LEASE = Duration.ofMinutes(4);

  // created_at uses NOW() server-side; entry.createdAt() is intentionally ignored so that
  // the database clock governs lease arithmetic (avoids clock-skew issues in distributed writers).
  private static final String INSERT =
      "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
          + " status, attempts, last_error, created_at, processed_at) "
          + "VALUES (?, ?::jsonb, ?, ?, ?, 'PENDING', 0, NULL, NOW(), NULL) "
          + "ON CONFLICT (entry_id) DO NOTHING";

  // Per-stream claim, one statement per OutboxOrderingMode (claimPendingSql). Invariant: at any
  // instant, for any non-null stream (aggregate_type, aggregate_id), at most ONE entry is
  // IN_PROGRESS, and it is that stream's lowest-seq eligible head. NULL-stream entries carry no
  // ordering constraint and are claimed freely in both modes. The both-or-neither CHECK on the pair
  // lets a single `aggregate_id IS [NOT] NULL` test stand for the whole stream.
  //
  // Correctness notes (do not "simplify" these away):
  // (1) FOR UPDATE SKIP LOCKED sits on `locked`, a plain single-table SELECT — never on the
  //     UNION (`eligible` is non-locking; PostgreSQL rejects FOR UPDATE with UNION).
  // (2) `agg_claimable`'s NOT EXISTS on the stream enforces "≤1 in-flight per stream":
  //     while any row of the stream is IN_PROGRESS, its head is excluded, so seq N+1 is
  //     unclaimable until seq N leaves IN_PROGRESS.
  // (3) The next_retry_at eligibility filter applies to `agg_head` (the lowest-seq PENDING row)
  //     — not to each PENDING row — so a backing-off head holds the WHOLE aggregate; a higher
  //     seq is never claimed past it. `eligible` only ever contains an aggregate's HEAD, never
  //     a successor, so a relay that loses the head to SKIP LOCKED simply claims fewer rows —
  //     it cannot fall back to the aggregate's next seq within the same statement.
  // (4) The UPDATE re-checks status = 'PENDING' under the lock: a competing relay that claimed
  //     and committed between this statement's snapshot and its lock acquisition leaves the row
  //     IN_PROGRESS/DELIVERED — the re-evaluated predicate (EvalPlanQual) then excludes it, so
  //     a stale eligible set can neither double-claim nor resurrect a non-PENDING row.
  // (5) The UPDATE ALSO re-checks (next_retry_at IS NULL OR next_retry_at <= NOW()) under the lock,
  //     closing a backoff-shave race: relay A can claim the head, fail, and markRetry (setting a
  //     FUTURE next_retry_at and RESTORING status = 'PENDING') and commit entirely inside relay B's
  //     statement window. B's EvalPlanQual re-check would then see status = 'PENDING' again and
  //     claim the row despite its future backoff. NOW() is transaction-stable, so this predicate is
  //     snapshot-consistent with the CTEs; markRetry is the only transition that sets a future
  //     next_retry_at and RECLAIM_EXPIRED preserves a past one, so re-checking it here is
  // sufficient.
  //     Both re-check predicates must stay — status alone does not detect the restored-PENDING
  // case.
  // (6) STRICT: the head set includes FAILED rows so a FAILED head is a head and not a gap;
  //     agg_claimable admits only a PENDING head. No lock-time re-check for "a FAILED predecessor
  //     appeared" is needed: for n1 to be a claimable head, n0 was DELIVERED or SKIPPED in the
  //     snapshot — both terminal — and a new FAILED row of A can only come from markFailed on an
  //     IN_PROGRESS row of A, which the NOT EXISTS excluded.
  //
  // UPDATE ... RETURNING has no guaranteed order, hence the final SELECT over the CTE.
  //
  // AVAILABILITY_FIRST: the original statement — the head set is PENDING only, so a FAILED row is
  // not a head and the next PENDING row of its aggregate is claimed past it.
  private static final String CLAIM_PENDING_AVAILABILITY_FIRST =
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

  // STRICT_PER_AGGREGATE: differs from the availability-first statement in exactly two CTEs —
  // agg_head selects over PENDING and FAILED (and carries status), agg_claimable admits only a
  // PENDING head. Everything else (null_agg, eligible, locked, claimed) is identical.
  private static final String CLAIM_PENDING_STRICT =
      "WITH null_agg AS ("
          + "  SELECT entry_id, seq FROM outbox_events"
          + "  WHERE status = 'PENDING' AND aggregate_id IS NULL"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
          + "), agg_head AS ("
          // STRICT: the head is the lowest-seq row among PENDING *and* FAILED. A FAILED head is
          // still the head: it is selected here so that agg_claimable can refuse the stream.
          + "  SELECT DISTINCT ON (aggregate_type, aggregate_id)"
          + "    entry_id, aggregate_type, aggregate_id, seq, status, next_retry_at"
          + "  FROM outbox_events"
          + "  WHERE status IN ('PENDING', 'FAILED') AND aggregate_id IS NOT NULL"
          + "  ORDER BY aggregate_type, aggregate_id, seq ASC"
          + "), agg_claimable AS ("
          + "  SELECT h.entry_id, h.seq FROM agg_head h"
          + "  WHERE h.status = 'PENDING'"
          + "    AND (h.next_retry_at IS NULL OR h.next_retry_at <= NOW())"
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
   * The claim statement the store runs for {@code mode}; package-private for the probe tests, which
   * derive a probe SQL from this exact string (rather than duplicating it) so a test can never
   * silently drift from the statement the store actually runs.
   */
  static String claimPendingSql(OutboxOrderingMode mode) {
    return mode == OutboxOrderingMode.STRICT_PER_AGGREGATE
        ? CLAIM_PENDING_STRICT
        : CLAIM_PENDING_AVAILABILITY_FIRST;
  }

  // Lease reclaim: abandoned claims (relay crashed mid-delivery) go back to PENDING. The cutoff is
  // computed on the DATABASE clock — NOW() minus the lease — NOT the relay JVM's wall clock, so it
  // shares one clock with claimed_at (also stamped by the server's NOW() at claim time; see the
  // claim statements and INSERT). A relay whose JVM clock is skewed AHEAD of the DB therefore
  // cannot reclaim another relay's still-live, mid-publish claim — which would double-publish and
  // reorder that aggregate fleet-wide, on every poll. The lease is bound in seconds. (Previously
  // the cutoff was the relay's Instant.now() - lease, reintroducing exactly the distributed-writer
  // clock skew the INSERT's NOW() was chosen to avoid.)
  private static final String RECLAIM_EXPIRED =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL "
          + "WHERE status = 'IN_PROGRESS' AND claimed_at < NOW() - make_interval(secs => ?)";

  // Every mark* UPDATE is a compare-and-set: it transitions the entry only while it is still
  // IN_PROGRESS AND claimed by the SAME claimed_by that won the claim. A relay whose lease was
  // stolen (blocked past the lease, reclaimed + delivered by another relay) therefore updates 0
  // rows and cannot clobber the outcome the current holder recorded. A 0-row update is a benign
  // lost-lease no-op, NOT an error (asymmetric with the saga CAS, which surfaces
  // OptimisticLockException) — the poller logs a WARN and skips.

  // markRetry releases the claim: the entry returns to PENDING and becomes claimable again
  // once next_retry_at passes.
  //
  // next_retry_at is DB-stamped: NOW() + the caller's backoff duration, NOT a caller-supplied
  // absolute instant. This shares the ONE clock the claim gate reads (the claim statements'
  // next_retry_at <= NOW()), so a relay whose JVM wall clock lags the DB cannot write an
  // already-past next_retry_at that collapses exponential backoff into a per-poll retry storm.
  // Same principle as RECLAIM_EXPIRED's DB-clock cutoff; the backoff is bound in seconds.
  private static final String MARK_RETRY =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL, "
          + "attempts = ?, last_error = ?, next_retry_at = NOW() + make_interval(secs => ?) "
          + "WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  private static final String MARK_DELIVERED =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = NOW() "
          + "WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  private static final String MARK_FAILED =
      "UPDATE outbox_events SET status = 'FAILED', attempts = ?, last_error = ?, processed_at = NOW() "
          + "WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  // A FAILED row is never deleted by the framework. 0 rows + row present → refused.
  private static final String DELETE_NON_FAILED =
      "DELETE FROM outbox_events WHERE entry_id = ? AND status <> 'FAILED'";
  private static final String EXISTS = "SELECT 1 FROM outbox_events WHERE entry_id = ?";

  // Operator skip: FAILED -> SKIPPED with the three audit columns in ONE statement. Guarded
  // on status='FAILED' only, like RESET_FAILED_TO_PENDING: a FAILED row has no live claim.
  private static final String SKIP_FAILED =
      "UPDATE outbox_events SET status = 'SKIPPED', skipped_at = NOW(), skipped_by = ?,"
          + " skip_reason = ? WHERE entry_id = ? AND status = 'FAILED'";

  private static final String MARK_DELIVERED_ALL =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = NOW() "
          + "WHERE entry_id = ANY(?) AND status = 'IN_PROGRESS' AND claimed_by = ?";

  // Release the claim on an entry the publish cycle never reached. Claim
  // columns only — attempts / last_error / next_retry_at are untouched, because nothing was
  // attempted. Same IN_PROGRESS + claimed_by CAS as every other mark*.
  private static final String RELEASE_CLAIMS =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL "
          + "WHERE entry_id = ANY(?) AND status = 'IN_PROGRESS' AND claimed_by = ?";

  /** Rows deleted per chunk by {@link #deleteDelivered} and {@link #deleteSkipped}. */
  private static final int DELETE_BATCH_SIZE = 1000;

  // The OUTER DELETE re-checks status='DELIVERED' on the live row, mirroring
  // DELETE_SKIPPED_BATCH's EvalPlanQual defense below. The subselect's status filter is only
  // snapshot-deep: under READ COMMITTED, EvalPlanQual re-evaluates just the OUTER predicates
  // against a concurrently-updated row version, so without this guard a row whose status changed
  // inside the delete window would still be deleted on entry_id membership alone. DELIVERED is
  // terminal today — no shipped transition leaves it — which is exactly why the guard must be
  // structural: nothing else pins the predicate, and a future transition out of DELIVERED (or an
  // edit loosening the subselect) would silently reopen the hole the FAILED sweep closed in
  // And DELETE_SKIPPED_BATCH keeps closed.
  // Pinned by PostgresOutboxStoreRetentionIT.deleteDeliveredBatchSql_has_outer_status_guard.
  private static final String DELETE_DELIVERED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'DELIVERED' AND entry_id IN ("
          + "  SELECT entry_id FROM outbox_events"
          + "  WHERE status = 'DELIVERED' AND processed_at < ?"
          + "  ORDER BY processed_at LIMIT ?)";

  private static final String ENTRY_COLUMNS =
      "entry_id, payload::text AS payload, payload_type, aggregate_type, aggregate_id, status,"
          + " attempts,"
          + " last_error, created_at, processed_at, next_retry_at, skipped_at, skipped_by,"
          + " skip_reason, seq";

  // Operator enumeration: oldest-first (seq ASC), read-only — claimed_by is NOT touched.
  private static final String FIND_BY_STATUS =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE status = ? ORDER BY seq ASC LIMIT ?";

  private static final String FIND_BY_ID =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE entry_id = ?";

  // One index-only read on idx_outbox_failed (processed_at) INCLUDE (aggregate_type, aggregate_id):
  // distinct non-null streams with an unresolved FAILED row, the oldest FAILED processed_at of ANY
  // row, and the legacy null-stream FAILED count (OutboxPoller's first-cycle WARN). The FILTER is
  // load-bearing: a row value of two NULLs is a non-null composite, so without it every legacy
  // NULL-stream FAILED row would count as one more blocked stream.
  private static final String SAMPLE_BLOCKAGE =
      "SELECT count(DISTINCT (aggregate_type, aggregate_id)) FILTER (WHERE aggregate_id IS NOT NULL),"
          + " min(processed_at), count(*) FILTER (WHERE aggregate_id IS NULL)"
          + " FROM outbox_events WHERE status = 'FAILED'";

  // SKIPPED retention: the DELETE_DELIVERED_BATCH shape over status='SKIPPED' AND skipped_at < ?.
  // The OUTER status re-check is structural: SKIPPED is terminal today, and the guard
  // keeps a future transition out of SKIPPED from reopening the EvalPlanQual hole the FAILED sweep
  // once had. Pinned by
  // PostgresOutboxStoreRetentionIT.deleteSkippedBatchSql_has_outer_status_guard.
  private static final String DELETE_SKIPPED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'SKIPPED' AND entry_id IN ("
          + "  SELECT entry_id FROM outbox_events"
          + "  WHERE status = 'SKIPPED' AND skipped_at < ?"
          + "  ORDER BY skipped_at LIMIT ?)";

  private static final String COUNT_BY_STATUS =
      "SELECT count(*) FROM outbox_events WHERE status = ?";

  // Operator reset of a terminal FAILED row back to PENDING. Deliberately guarded on
  // status='FAILED' ONLY (no claimed_by CAS): a FAILED row has no live claim, and this is an
  // operator action on a terminal row. Clears attempts/last_error/next_retry_at and any stale claim
  // bookkeeping so the claim picks the row up as its aggregate's eligible head.
  private static final String RESET_FAILED_TO_PENDING =
      "UPDATE outbox_events SET status = 'PENDING', attempts = 0, last_error = NULL, "
          + "next_retry_at = NULL, claimed_at = NULL, claimed_by = NULL "
          + "WHERE entry_id = ? AND status = 'FAILED'";

  private static final Logger LOG = LoggerFactory.getLogger(PostgresOutboxStore.class);

  // PostgreSQL unique_violation. A claim statement that trips the ux_outbox_one_inflight_per_stream
  // partial unique index raises this; see loadPending for why it is benign.
  private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

  // The baseline's partial unique index enforcing ≤1 IN_PROGRESS per stream. Only the
  // resetFailedToPending-vs-claim race can trip it; a 23505 naming THIS constraint is a benign
  // retry, not an error.
  static final String ONE_INFLIGHT_INDEX = "ux_outbox_one_inflight_per_stream";

  private final DataSource dataSource;
  private final Duration claimLease;
  private final OutboxOrderingMode orderingMode;
  private final String claimPending;
  private final String claimedBy;

  /** Default lease, strict by default; see {@link OutboxOrderingMode}. */
  public PostgresOutboxStore(DataSource dataSource) {
    this(dataSource, DEFAULT_CLAIM_LEASE);
  }

  /**
   * Creates a store with an explicit claim lease. Strict by default; see {@link
   * OutboxOrderingMode}.
   *
   * @param dataSource the data source (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before other instances may
   *     reclaim it; must be positive. Choose a value comfortably above the worst-case publish
   *     duration for one batch.
   */
  public PostgresOutboxStore(DataSource dataSource, Duration claimLease) {
    this(dataSource, claimLease, OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * Creates a store with an explicit claim lease and ordering mode.
   *
   * @param dataSource the data source (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before other instances may
   *     reclaim it; must be positive. Choose a value comfortably above the worst-case publish
   *     duration for one batch.
   * @param orderingMode which ordering this channel enforces in the claim and in {@code save}.
   *     Chosen where the channel is built and nowhere else; the poller logs it at start.
   */
  public PostgresOutboxStore(
      DataSource dataSource, Duration claimLease, OutboxOrderingMode orderingMode) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    if (claimLease == null || claimLease.isZero() || claimLease.isNegative()) {
      throw new IllegalArgumentException("claimLease must be positive");
    }
    if (orderingMode == null) throw new IllegalArgumentException("orderingMode is required");
    this.dataSource = dataSource;
    this.claimLease = claimLease;
    this.orderingMode = orderingMode;
    this.claimPending = claimPendingSql(orderingMode);
    this.claimedBy = "outbox-" + UUID.randomUUID();
  }

  @Override
  public OutboxOrderingMode orderingMode() {
    return orderingMode;
  }

  /** A strict channel refuses a null stream before any write, unwrapped. */
  private void requireStreamIfStrict(OutboxEntry entry) {
    if (orderingMode == OutboxOrderingMode.STRICT_PER_AGGREGATE && entry.streamId() == null) {
      throw new OutboxOrderingViolationException(entry.id(), entry.payloadType());
    }
  }

  @Override
  public void save(OutboxEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry is required");
    requireStreamIfStrict(entry); // strict-mode stream check, before the try: surfaces unwrapped
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(INSERT)) {
      ps.setString(1, entry.id().value());
      ps.setString(2, entry.payload());
      ps.setString(3, entry.payloadType());
      PostgresStreamKeys.bind(ps, 4, entry.streamId());
      PostgresTransactions.commitIfManual(conn, "outbox save", c -> ps.executeUpdate());
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to save outbox entry: " + LogSanitizer.sanitizeForLog(entry.id().value()), e);
    }
  }

  /**
   * Saves outbox entries on the caller's connection, participating in the caller's transaction.
   * Neither commits nor rolls back — the caller owns the transaction boundary. This is what {@code
   * PostgresEventStore.append(..., outboxEntries)} uses to make the domain-event write and the
   * outbox write atomic (the transactional outbox pattern).
   *
   * @param entries entries to save (required; may be empty, which is a no-op)
   * @param conn an open connection with an active transaction (required)
   * @throws OutboxOrderingViolationException if this channel is {@code STRICT_PER_AGGREGATE} and an
   *     entry has no {@code streamId} — checked over the whole list before any write, so the
   *     caller's transaction is untouched and the exception surfaces unwrapped
   */
  void saveAll(List<OutboxEntry> entries, Connection conn) {
    if (entries == null) throw new IllegalArgumentException("entries is required");
    if (conn == null) throw new IllegalArgumentException("conn is required");
    if (entries.isEmpty()) {
      return;
    }
    for (OutboxEntry entry : entries) {
      requireStreamIfStrict(
          entry); // strict-mode stream check over the whole batch before any addBatch
    }
    try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
      for (OutboxEntry entry : entries) {
        ps.setString(1, entry.id().value());
        ps.setString(2, entry.payload());
        ps.setString(3, entry.payloadType());
        PostgresStreamKeys.bind(ps, 4, entry.streamId());
        ps.addBatch();
      }
      ps.executeBatch();
    } catch (Exception e) {
      throw new EventStoreException("Failed to save " + entries.size() + " outbox entries", e);
    }
  }

  /**
   * Claims up to {@code limit} eligible entries for this instance and returns them (status {@code
   * IN_PROGRESS}). Entries claimed here are invisible to concurrent relay instances until {@link
   * #markRetry} releases them, {@link #markDelivered}/{@link #markFailed} finishes them, or the
   * claim lease expires. Per non-null aggregate, only the head is ever claimed, and only while it
   * is {@code PENDING}, eligible and nothing of that aggregate is in flight; which rows count as
   * the head depends on the {@link #orderingMode()} (see the class javadoc).
   */
  @Override
  public List<OutboxEntry> loadPending(int limit) {
    if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
    try (Connection conn = dataSource.getConnection()) {
      return PostgresTransactions.commitIfManual(
          conn,
          "outbox claim",
          c -> {
            reclaimExpired(c);
            try (PreparedStatement ps = c.prepareStatement(claimPending)) {
              ps.setInt(1, limit);
              ps.setString(2, claimedBy);
              try (ResultSet rs = ps.executeQuery()) {
                List<OutboxEntry> result = new ArrayList<>();
                while (rs.next()) {
                  result.add(mapRow(rs, true));
                }
                return result;
              }
            }
          });
    } catch (SQLException e) {
      if (isBenignInflightUniqueViolation(e)) {
        // The ux_outbox_one_inflight_per_stream partial unique index fired: a concurrent claim
        // already set this stream IN_PROGRESS in the window opened by resetFailedToPending. The
        // 23505 aborts the WHOLE claim statement (so this claim of N aggregates loses all
        // N), but that is functionally safe — at-least-once delivery means the next poll re-claims
        // whatever is now eligible. NORMAL operation never reaches this index (agg_head is a
        // DISTINCT ON = one head per aggregate, and the claimed CTE re-checks status='PENDING' so a
        // single winner emerges), so this fires only on the rare reset-race and must NOT become an
        // ERROR/WARN log storm. Log at DEBUG and return empty so the poller simply polls again.
        LOG.debug(
            "Outbox claim hit the one-inflight-per-stream guard ({}); a concurrent claim won "
                + "this stream. Returning no entries; next poll re-claims.",
            ONE_INFLIGHT_INDEX,
            e);
        return List.of();
      }
      throw new EventStoreException("Failed to load pending outbox entries", e);
    } catch (Exception e) {
      throw new EventStoreException("Failed to load pending outbox entries", e);
    }
  }

  /**
   * True iff {@code e} (or a cause in its chain) is a PostgreSQL {@code 23505} unique-violation
   * naming the {@link #ONE_INFLIGHT_INDEX} partial unique index. Any OTHER SQLException — including
   * a 23505 on a different constraint — is NOT swallowed.
   */
  static boolean isBenignInflightUniqueViolation(SQLException e) {
    // Bounded walk (depth 50) so a self-referential/cyclic cause chain
    // terminates instead of spinning forever, matching PostgresEventStore.hasCryptoCause /
    // isRetryableConflict.
    Throwable t = e;
    for (int depth = 0; t != null && depth < 50; depth++, t = t.getCause()) {
      if (t instanceof SQLException sqlEx
          && SQLSTATE_UNIQUE_VIOLATION.equals(sqlEx.getSQLState())) {
        String message = sqlEx.getMessage();
        if (message != null && message.contains(ONE_INFLIGHT_INDEX)) {
          return true;
        }
      }
    }
    return false;
  }

  private void reclaimExpired(Connection conn) throws java.sql.SQLException {
    reclaimExpired(conn, Instant.now());
  }

  /**
   * Resets abandoned {@code IN_PROGRESS} claims (a relay that crashed mid-delivery) back to {@code
   * PENDING}. The reclaim cutoff is computed entirely on the database clock ({@code NOW() -
   * lease}), so it shares one clock with {@code claimed_at}. The {@code relayNow} parameter models
   * the relay JVM's wall clock and is INTENTIONALLY IGNORED — it exists only so the regression test
   * can pass a wildly skewed instant and prove the reclaim decision is made by the DB clock alone.
   * Binding the relay clock into the cutoff (as this once did) is precisely the bug that let a
   * clock-skewed relay reclaim other relays' healthy in-flight claims fleet-wide.
   */
  void reclaimExpired(Connection conn, Instant relayNow) throws java.sql.SQLException {
    try (PreparedStatement ps = conn.prepareStatement(RECLAIM_EXPIRED)) {
      ps.setDouble(1, claimLease.toMillis() / 1000.0);
      ps.executeUpdate();
    }
  }

  @Override
  public String claimedBy() {
    return claimedBy;
  }

  /**
   * The configured claim lease — the cutoff {@code RECLAIM_EXPIRED} uses ({@code NOW() - lease}) to
   * reclaim abandoned {@code IN_PROGRESS} claims. Exposed so the {@code OutboxPoller} can bound its
   * per-batch publish deadline strictly below this lease: a live relay must never keep publishing
   * past the point where another relay would reclaim and re-deliver its still-in-flight entries out
   * of order.
   */
  @Override
  public Duration claimLease() {
    return claimLease;
  }

  @Override
  public boolean markDelivered(OutboxEntryId id, String claimedBy) {
    if (id == null) throw new IllegalArgumentException("id is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MARK_DELIVERED)) {
      ps.setString(1, id.value());
      ps.setString(2, claimedBy);
      return PostgresTransactions.commitIfManual(
              conn, "outbox mark delivered", c -> ps.executeUpdate())
          == 1;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to mark outbox entry as delivered: " + LogSanitizer.sanitizeForLog(id.value()),
          e);
    }
  }

  @Override
  public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
    if (id == null) throw new IllegalArgumentException("id is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MARK_FAILED)) {
      ps.setInt(1, attempts);
      ps.setString(2, error);
      ps.setString(3, id.value());
      ps.setString(4, claimedBy);
      return PostgresTransactions.commitIfManual(
              conn, "outbox mark failed", c -> ps.executeUpdate())
          == 1;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to mark outbox entry as failed: " + LogSanitizer.sanitizeForLog(id.value()), e);
    }
  }

  /**
   * Removes a non-{@code FAILED} entry; an absent id is a no-op. A {@code FAILED} row is refused
   * with {@link IllegalStateException} — its only exits are the audited {@link #skipFailed} and
   * {@link #resetFailedToPending}, and a raw delete of a blocking head would release its aggregate
   * with no record.
   */
  @Override
  public void delete(OutboxEntryId id) {
    if (id == null) throw new IllegalArgumentException("id is required");
    try (Connection conn = dataSource.getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(DELETE_NON_FAILED)) {
        ps.setString(1, id.value());
        if (PostgresTransactions.commitIfManual(conn, "outbox delete", c -> ps.executeUpdate())
            == 1) {
          return;
        }
      }
      // 0 rows: absent (no-op) or FAILED (refused). The two statements are deliberately not one
      // transaction: the only writers that can change a FAILED row in between are the two operator
      // operations, and if one did, the delete is still refused — the operator re-reads.
      try (PreparedStatement ps = conn.prepareStatement(EXISTS)) {
        ps.setString(1, id.value());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            throw new IllegalStateException(
                "outbox entry "
                    + LogSanitizer.sanitizeForLog(id.value())
                    + " is FAILED; resolve it with OutboxFailedReplayer.skip or .replay — delete"
                    + " would release its aggregate with no record");
          }
        }
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to delete outbox entry: " + LogSanitizer.sanitizeForLog(id.value()), e);
    }
  }

  @Override
  public boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
    if (id == null) throw new IllegalArgumentException("id is required");
    if (backoff == null) throw new IllegalArgumentException("backoff is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MARK_RETRY)) {
      ps.setInt(1, attempts);
      ps.setString(2, error);
      // DB-stamped: next_retry_at = NOW() + make_interval(secs => ?). The DB clock — not the relay
      // JVM clock — decides eligibility, so backoff is immune to clock skew.
      ps.setDouble(3, backoff.toMillis() / 1000.0);
      ps.setString(4, id.value());
      ps.setString(5, claimedBy);
      return PostgresTransactions.commitIfManual(conn, "outbox mark retry", c -> ps.executeUpdate())
          == 1;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to mark outbox entry for retry: " + LogSanitizer.sanitizeForLog(id.value()), e);
    }
  }

  @Override
  public int markDeliveredAll(java.util.Collection<OutboxEntryId> ids, String claimedBy) {
    if (ids.isEmpty()) return 0;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MARK_DELIVERED_ALL)) {
      String[] arr = ids.stream().map(OutboxEntryId::value).toArray(String[]::new);
      ps.setArray(1, conn.createArrayOf("varchar", arr));
      ps.setString(2, claimedBy);
      return PostgresTransactions.commitIfManual(
          conn, "outbox mark delivered (batch)", c -> ps.executeUpdate());
    } catch (java.sql.SQLException e) {
      throw new EventStoreException("Failed to mark outbox entries delivered", e);
    }
  }

  /**
   * One set-based UPDATE for the whole never-attempted tail of a deadline-truncated cycle, instead
   * of the SPI default's one {@code markRetry} round-trip per entry — the tail is at its longest
   * (batchSize minus a handful) exactly when the broker is degraded and the DB is the last thing
   * that should be hammered.
   *
   * <p>Deliberately touches ONLY the claim columns: {@code attempts}, {@code last_error} and {@code
   * next_retry_at} are left exactly as they were. Nothing was attempted, so there is no attempt to
   * count, no error to record, and no backoff to serve — and the row was claimable moments ago, so
   * its {@code next_retry_at} is already in the past and the entry is immediately eligible again.
   * The same {@code IN_PROGRESS} + {@code claimed_by} CAS as every other {@code mark*} keeps a
   * relay whose lease was stolen from clobbering the new owner.
   */
  @Override
  public int releaseClaims(java.util.Collection<OutboxEntry> entries, String claimedBy) {
    if (entries.isEmpty()) return 0;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(RELEASE_CLAIMS)) {
      String[] arr = entries.stream().map(e -> e.id().value()).toArray(String[]::new);
      ps.setArray(1, conn.createArrayOf("varchar", arr));
      ps.setString(2, claimedBy);
      return PostgresTransactions.commitIfManual(
          conn, "outbox release claims", c -> ps.executeUpdate());
    } catch (java.sql.SQLException e) {
      throw new EventStoreException("Failed to release unattempted outbox claims", e);
    }
  }

  @Override
  public int deleteDelivered(Instant olderThan) {
    int total = 0;
    int batch;
    do {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(DELETE_DELIVERED_BATCH)) {
        ps.setObject(1, Timestamp.from(olderThan));
        ps.setInt(2, DELETE_BATCH_SIZE);
        batch =
            PostgresTransactions.commitIfManual(
                conn, "outbox delivered retention", c -> ps.executeUpdate());
        total += batch;
      } catch (java.sql.SQLException e) {
        throw new EventStoreException("Failed to delete delivered outbox entries", e);
      }
      // Loop while ANY row was deleted, mirroring deleteSkipped's termination rationale.
      // With the outer status='DELIVERED' EvalPlanQual guard a chunk can delete FEWER than LIMIT
      // even when older aged rows remain (a candidate concurrently updated is skipped by the
      // re-check), so a `batch == DELETE_BATCH_SIZE` termination could exit prematurely.
      // `batch > 0` still terminates (each non-empty chunk strictly shrinks the finite aged set)
      // and drains fully.
    } while (batch > 0);
    return total;
  }

  @Override
  public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
    if (status == null) throw new IllegalArgumentException("status is required");
    if (limit < 1) throw new IllegalArgumentException("limit must be >= 1");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_BY_STATUS)) {
      ps.setString(1, status.name());
      ps.setInt(2, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<OutboxEntry> result = new ArrayList<>();
        while (rs.next()) {
          result.add(mapRow(rs, false));
        }
        return result;
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to find outbox entries by status: " + status, e);
    }
  }

  /**
   * The one row mapper for every outbox SELECT ({@code ENTRY_COLUMNS} and both claim statements'
   * {@code RETURNING} lists name the same columns). {@code claimed} rows report a {@code null}
   * {@code processedAt}: a claim leaves the column untouched, and a replayed row still carries the
   * stamp of its earlier {@code FAILED}, which is not this claim's outcome. The skip audit is read
   * iff the row is {@code SKIPPED} (enforced by the {@link OutboxEntry} invariant).
   */
  private static OutboxEntry mapRow(ResultSet rs, boolean claimed) throws SQLException {
    var status = OutboxStatus.valueOf(rs.getString("status"));
    var processedAtTs = claimed ? null : rs.getTimestamp("processed_at");
    var nextRetryAtTs = rs.getTimestamp("next_retry_at");
    OutboxEntry.SkipRecord skip = null;
    if (status == OutboxStatus.SKIPPED) {
      skip =
          new OutboxEntry.SkipRecord(
              rs.getTimestamp("skipped_at").toInstant(),
              rs.getString("skipped_by"),
              rs.getString("skip_reason"));
    }
    return new OutboxEntry(
        OutboxEntryId.of(rs.getString("entry_id")),
        rs.getString("payload"),
        rs.getString("payload_type"),
        // The decode door — the lenient constructors, never the ingress factories — so a stored
        // stream carrying a control character still reads back.
        PostgresStreamKeys.read(rs),
        status,
        rs.getInt("attempts"),
        rs.getString("last_error"),
        rs.getTimestamp("created_at").toInstant(),
        processedAtTs != null ? processedAtTs.toInstant() : null,
        nextRetryAtTs != null ? nextRetryAtTs.toInstant() : null,
        skip);
  }

  @Override
  public long countByStatus(OutboxStatus status) {
    if (status == null) throw new IllegalArgumentException("status is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNT_BY_STATUS)) {
      ps.setString(1, status.name());
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to count outbox entries by status: " + status, e);
    }
  }

  @Override
  public boolean resetFailedToPending(OutboxEntryId id) {
    if (id == null) throw new IllegalArgumentException("id is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(RESET_FAILED_TO_PENDING)) {
      ps.setString(1, id.value());
      return PostgresTransactions.commitIfManual(
              conn, "outbox reset failed", c -> ps.executeUpdate())
          == 1;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to reset failed outbox entry to pending: "
              + LogSanitizer.sanitizeForLog(id.value()),
          e);
    }
  }

  @Override
  public Optional<OutboxEntry> findById(OutboxEntryId id) {
    if (id == null) throw new IllegalArgumentException("id is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_BY_ID)) {
      ps.setString(1, id.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(mapRow(rs, false)) : Optional.empty();
      }
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to find outbox entry: " + LogSanitizer.sanitizeForLog(id.value()), e);
    }
  }

  @Override
  public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
    if (id == null) throw new IllegalArgumentException("id is required");
    if (skippedBy == null) throw new IllegalArgumentException("skippedBy is required");
    if (reason == null) throw new IllegalArgumentException("reason is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SKIP_FAILED)) {
      ps.setString(1, skippedBy);
      ps.setString(2, reason);
      ps.setString(3, id.value());
      return PostgresTransactions.commitIfManual(
              conn, "outbox skip failed", c -> ps.executeUpdate())
          == 1;
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to skip failed outbox entry: " + LogSanitizer.sanitizeForLog(id.value()), e);
    }
  }

  @Override
  public BlockageSample sampleBlockage() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SAMPLE_BLOCKAGE);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      var oldest = rs.getTimestamp(2);
      return new BlockageSample(
          rs.getLong(1), oldest != null ? oldest.toInstant() : null, rs.getLong(3));
    } catch (Exception e) {
      throw new EventStoreException("Failed to sample outbox blockage", e);
    }
  }

  @Override
  public int deleteSkipped(Instant cutoff) {
    if (cutoff == null) throw new IllegalArgumentException("cutoff is required");
    int total = 0;
    int batch;
    do {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps = conn.prepareStatement(DELETE_SKIPPED_BATCH)) {
        ps.setObject(1, Timestamp.from(cutoff));
        ps.setInt(2, DELETE_BATCH_SIZE);
        batch =
            PostgresTransactions.commitIfManual(
                conn, "outbox skipped retention", c -> ps.executeUpdate());
        total += batch;
      } catch (java.sql.SQLException e) {
        throw new EventStoreException("Failed to delete skipped outbox entries", e);
      }
      // `batch > 0` (not `== DELETE_BATCH_SIZE`): the same termination rationale as
      // deleteDelivered — each non-empty chunk strictly shrinks the finite aged set.
    } while (batch > 0);
    return total;
  }
}

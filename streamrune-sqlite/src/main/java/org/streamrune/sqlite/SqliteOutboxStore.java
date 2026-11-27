package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.backend.OutboxOptions;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * The SQLite {@link OutboxStore}: the {@code outbox_events} table of a {@link SqliteDatabase}, with
 * the semantics of {@code PostgresOutboxStore}. {@link SqliteEventStore} writes entries into the
 * same table in its append transaction (see {@link SqliteEventStoreFactory#outboxStore}).
 *
 * <p><b>Claims under the write lock.</b> {@link #loadPending} <em>claims</em> entries: in one write
 * transaction ({@code BEGIN IMMEDIATE} … {@code COMMIT}, see {@code SqliteTransactions}) it returns
 * expired claims to {@code PENDING} and then moves the eligible heads to {@code IN_PROGRESS} with
 * one {@code UPDATE … RETURNING}, stamping {@code claimed_at} and {@code claimed_by}. The
 * transaction holds SQLite's database-wide write lock from its {@code BEGIN} to its {@code COMMIT},
 * so no other claim, mark or append of any connection on the file runs in between: two relays, or
 * two threads of one relay, never claim the same entry, and the claim reads the newest committed
 * rows. That lock is what {@code FOR UPDATE SKIP LOCKED} and the lock-time re-checks give the
 * Postgres claim; a relay that waits for it then claims what the one before it left. The race the
 * Postgres store tolerates between an operator's {@link #resetFailedToPending} and a claim (a
 * unique violation on {@code ux_outbox_one_inflight_per_stream}) cannot happen here: the two writes
 * are serialized.
 *
 * <p><b>Ordering.</b> As on Postgres, the claim takes only heads. The head of a non-null stream is
 * its lowest-{@code seq} row among {@code PENDING} and, on a {@link
 * OutboxOrderingMode#STRICT_PER_AGGREGATE} channel, {@code FAILED}; it is claimed only while it is
 * {@code PENDING}, eligible ({@code next_retry_at} absent or reached) and no row of its stream is
 * {@code IN_PROGRESS}. Entries without a stream are claimed freely. A strict channel refuses an
 * entry without a stream in {@link #save} and {@code saveAll} with {@link
 * OutboxOrderingViolationException}, before any write, unwrapped. See {@link
 * OutboxStore#loadPending}.
 *
 * <p><b>One clock.</b> Every timestamp ({@code created_at}, {@code claimed_at}, {@code
 * processed_at}, {@code next_retry_at}, {@code skipped_at}) is the application {@link Clock}'s
 * instant in epoch microseconds, read inside the write transaction once the lock is granted, and so
 * is the "now" the claim compares {@code next_retry_at} and the lease with: the clock the Postgres
 * store gets from the database's {@code NOW()}. A claim older than the lease ({@code claimed_at <
 * now - lease}) is reclaimed by the next claim of any relay. The clock is called while the write
 * lock is held, so it must return at once. {@code entry.createdAt()} is ignored, as on Postgres.
 *
 * <p><b>Failures.</b> A write waits for the write lock at most the connection's {@code
 * busy_timeout} and then fails with an {@link EventStoreException} naming it, having written
 * nothing; any other storage failure is an {@link EventStoreException} with the Postgres store's
 * message. A connection handed out in manual-commit mode is refused with {@link
 * IllegalStateException} before anything is written, as every SQLite store refuses it.
 */
public final class SqliteOutboxStore implements OutboxStore {

  // created_at is the store clock's instant; entry.createdAt() is ignored, as on Postgres.
  private static final String INSERT =
      "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
          + " trace_id, span_id, status, attempts, last_error, created_at, processed_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, ?, NULL)"
          + " ON CONFLICT (entry_id) DO NOTHING";

  private static final String ENTRY_COLUMNS =
      "entry_id, payload, payload_type, aggregate_type, aggregate_id, status, attempts,"
          + " last_error, created_at, processed_at, next_retry_at, skipped_at, skipped_by,"
          + " skip_reason, trace_id, span_id, seq";

  // The claim, one statement per mode (claimPendingSql); ?1 is now, ?2 the limit, ?3 claimed_by.
  // heads: the lowest seq of each non-null stream over the statuses a head can have (SQLite has no
  // DISTINCT ON; min(seq) per stream, joined back for the row). eligible: the free rows and the
  // claimable heads, in seq order, at most ?2. The UPDATE moves exactly those rows; it re-checks
  // PENDING although nothing else can write in between (the write lock), as a guard against a
  // future edit of the subqueries. RETURNING has no order: the caller sorts by seq.
  private static final String CLAIM_TEMPLATE =
      "WITH heads AS ("
          + "  SELECT min(seq) AS seq FROM outbox_events"
          + "  WHERE status IN ('PENDING', 'FAILED')%s AND aggregate_id IS NOT NULL"
          + "  GROUP BY aggregate_type, aggregate_id"
          + "), eligible AS ("
          + "  SELECT seq FROM outbox_events"
          + "  WHERE status = 'PENDING' AND aggregate_id IS NULL"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= ?1)"
          + "  UNION ALL"
          + "  SELECT o.seq AS seq FROM heads h JOIN outbox_events o ON o.seq = h.seq"
          + "  WHERE o.status = 'PENDING'"
          + "    AND (o.next_retry_at IS NULL OR o.next_retry_at <= ?1)"
          + "    AND NOT EXISTS ("
          + "      SELECT 1 FROM outbox_events ip"
          + "      WHERE ip.aggregate_type = o.aggregate_type AND ip.aggregate_id = o.aggregate_id"
          + "        AND ip.status = 'IN_PROGRESS')"
          + "  ORDER BY seq LIMIT ?2"
          + ") UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_at = ?1, claimed_by = ?3"
          + " WHERE seq IN (SELECT seq FROM eligible) AND status = 'PENDING'"
          + " RETURNING "
          + ENTRY_COLUMNS;

  /** STRICT_PER_AGGREGATE: a FAILED row is a head too, so it holds its stream. */
  private static final String CLAIM_PENDING_STRICT = CLAIM_TEMPLATE.formatted("");

  /**
   * AVAILABILITY_FIRST: only PENDING rows are heads; a FAILED row is claimed past. The predicate
   * keeps the strict statement's {@code status IN ('PENDING', 'FAILED')}, which is the predicate of
   * the partial index {@code idx_outbox_stream_seq}, and narrows it to {@code PENDING}: SQLite uses
   * a partial index only for a query whose WHERE contains the index's own terms, so {@code status =
   * 'PENDING'} alone would read the whole table, the delivered backlog included, under the write
   * lock on every poll.
   */
  private static final String CLAIM_PENDING_AVAILABILITY_FIRST =
      CLAIM_TEMPLATE.formatted(" AND status = 'PENDING'");

  /** The claim statement the store runs for {@code mode}. */
  static String claimPendingSql(OutboxOrderingMode mode) {
    return mode == OutboxOrderingMode.STRICT_PER_AGGREGATE
        ? CLAIM_PENDING_STRICT
        : CLAIM_PENDING_AVAILABILITY_FIRST;
  }

  // Abandoned claims go back to PENDING; the cutoff (now - lease) is bound from the store clock,
  // the clock that stamped claimed_at.
  private static final String RECLAIM_EXPIRED =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL"
          + " WHERE status = 'IN_PROGRESS' AND claimed_at < ?";

  /** The reclaim statement the claim runs first. */
  static String reclaimExpiredSql() {
    return RECLAIM_EXPIRED;
  }

  // Every mark* is a compare-and-set on IN_PROGRESS + claimed_by, as on Postgres: a relay whose
  // lease was stolen updates 0 rows, a benign no-op.
  private static final String MARK_RETRY =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL,"
          + " attempts = ?, last_error = ?, next_retry_at = ?"
          + " WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  private static final String MARK_DELIVERED =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = ?"
          + " WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  private static final String MARK_FAILED =
      "UPDATE outbox_events SET status = 'FAILED', attempts = ?, last_error = ?, processed_at = ?"
          + " WHERE entry_id = ? AND status = 'IN_PROGRESS' AND claimed_by = ?";

  // The set-based forms bind the ids as an IN list (Postgres: = ANY(?)), at most ID_CHUNK per
  // statement, all chunks in one transaction. %s is the list of placeholders.
  private static final String MARK_DELIVERED_ALL =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = ?"
          + " WHERE entry_id IN (%s) AND status = 'IN_PROGRESS' AND claimed_by = ?";

  private static final String RELEASE_CLAIMS =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL"
          + " WHERE entry_id IN (%s) AND status = 'IN_PROGRESS' AND claimed_by = ?";

  /** Ids bound per statement by the set-based marks, well below SQLite's parameter limit. */
  private static final int ID_CHUNK = 500;

  // A FAILED row is never deleted. 0 rows and the row present: refused.
  private static final String DELETE_NON_FAILED =
      "DELETE FROM outbox_events WHERE entry_id = ? AND status <> 'FAILED'";
  private static final String EXISTS = "SELECT 1 FROM outbox_events WHERE entry_id = ?";

  private static final String SKIP_FAILED =
      "UPDATE outbox_events SET status = 'SKIPPED', skipped_at = ?, skipped_by = ?,"
          + " skip_reason = ? WHERE entry_id = ? AND status = 'FAILED'";

  private static final String RESET_FAILED_TO_PENDING =
      "UPDATE outbox_events SET status = 'PENDING', attempts = 0, last_error = NULL,"
          + " next_retry_at = NULL, claimed_at = NULL, claimed_by = NULL"
          + " WHERE entry_id = ? AND status = 'FAILED'";

  /** Rows deleted per chunk (and per write transaction) by the two retention sweeps. */
  private static final int DELETE_BATCH_SIZE = 1000;

  // Retention: the outer status check keeps the chunk to rows still in that status.
  private static final String DELETE_DELIVERED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'DELIVERED' AND seq IN ("
          + "  SELECT seq FROM outbox_events"
          + "  WHERE status = 'DELIVERED' AND processed_at < ?"
          + "  ORDER BY processed_at LIMIT ?)";

  private static final String DELETE_SKIPPED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'SKIPPED' AND seq IN ("
          + "  SELECT seq FROM outbox_events"
          + "  WHERE status = 'SKIPPED' AND skipped_at < ?"
          + "  ORDER BY skipped_at LIMIT ?)";

  private static final String FIND_BY_STATUS =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE status = ? ORDER BY seq LIMIT ?";

  private static final String FIND_BY_ID =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE entry_id = ?";

  private static final String COUNT_BY_STATUS =
      "SELECT count(*) FROM outbox_events WHERE status = ?";

  // Postgres: count(DISTINCT (aggregate_type, aggregate_id)) FILTER (WHERE aggregate_id IS NOT
  // NULL); SQLite's count(DISTINCT) takes one expression, so the distinct streams are counted in a
  // subquery. The null-stream rows are counted apart, never as one more stream.
  private static final String SAMPLE_BLOCKAGE =
      "SELECT (SELECT count(*) FROM (SELECT DISTINCT aggregate_type, aggregate_id"
          + "   FROM outbox_events WHERE status = 'FAILED' AND aggregate_id IS NOT NULL)),"
          + " (SELECT min(processed_at) FROM outbox_events WHERE status = 'FAILED'),"
          + " (SELECT count(*) FROM outbox_events WHERE status = 'FAILED' AND aggregate_id IS NULL)";

  private final DataSource dataSource;
  private final Path file;
  private final Duration claimLease;
  private final OutboxOrderingMode orderingMode;
  private final String claimPending;
  private final String claimedBy;
  private final Clock clock;

  /**
   * A strict store with the default claim lease ({@link OutboxOptions#DEFAULT_CLAIM_LEASE}),
   * stamping with {@link Clock#systemUTC()}.
   *
   * @param database the database whose {@code outbox_events} table the store keeps (required)
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteOutboxStore(SqliteDatabase database) {
    this(database, OutboxOptions.DEFAULT_CLAIM_LEASE);
  }

  /**
   * A strict store with {@code claimLease}, stamping with {@link Clock#systemUTC()}.
   *
   * @param database the database whose {@code outbox_events} table the store keeps (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before another claim may
   *     take it back; positive
   * @throws IllegalArgumentException if an argument is {@code null} or the lease is not positive
   */
  public SqliteOutboxStore(SqliteDatabase database, Duration claimLease) {
    this(database, claimLease, OutboxOrderingMode.STRICT_PER_AGGREGATE);
  }

  /**
   * A store with {@code claimLease} and {@code orderingMode}, stamping with {@link
   * Clock#systemUTC()}.
   *
   * @param database the database whose {@code outbox_events} table the store keeps (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before another claim may
   *     take it back; positive
   * @param orderingMode the ordering this channel enforces in the claim and in {@code save}
   * @throws IllegalArgumentException if an argument is {@code null} or the lease is not positive
   */
  public SqliteOutboxStore(
      SqliteDatabase database, Duration claimLease, OutboxOrderingMode orderingMode) {
    this(database, claimLease, orderingMode, Clock.systemUTC());
  }

  /**
   * A store with {@code claimLease} and {@code orderingMode}, stamping and measuring the lease with
   * {@code clock}.
   *
   * @param database the database whose {@code outbox_events} table the store keeps (required)
   * @param claimLease how long an {@code IN_PROGRESS} claim is honored before another claim may
   *     take it back; positive
   * @param orderingMode the ordering this channel enforces in the claim and in {@code save}
   * @param clock the clock of every timestamp and of the lease; read while the write lock is held,
   *     so it must return at once
   * @throws IllegalArgumentException if an argument is {@code null} or the lease is not positive
   */
  public SqliteOutboxStore(
      SqliteDatabase database, Duration claimLease, OutboxOrderingMode orderingMode, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (claimLease == null || claimLease.isZero() || claimLease.isNegative()) {
      throw new IllegalArgumentException("claimLease must be positive");
    }
    if (orderingMode == null) {
      throw new IllegalArgumentException("orderingMode is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.dataSource = database.dataSource();
    this.file = database.file();
    this.claimLease = claimLease;
    this.orderingMode = orderingMode;
    this.claimPending = claimPendingSql(orderingMode);
    this.claimedBy = "outbox-" + UUID.randomUUID();
    this.clock = clock;
  }

  /** The database file the store keeps its entries in. */
  Path file() {
    return file;
  }

  @Override
  public OutboxOrderingMode orderingMode() {
    return orderingMode;
  }

  @Override
  public String claimedBy() {
    return claimedBy;
  }

  /** The configured claim lease: a claim older than this is taken back by the next claim. */
  @Override
  public Duration claimLease() {
    return claimLease;
  }

  /** A strict channel refuses a null stream before any write, unwrapped. */
  private void requireStreamIfStrict(OutboxEntry entry) {
    if (orderingMode == OutboxOrderingMode.STRICT_PER_AGGREGATE && entry.streamId() == null) {
      throw new OutboxOrderingViolationException(entry.id(), entry.payloadType());
    }
  }

  private long now() {
    return SqliteTimestamps.toMicros(clock.instant());
  }

  @Override
  public void save(OutboxEntry entry) {
    if (entry == null) {
      throw new IllegalArgumentException("entry is required");
    }
    requireStreamIfStrict(entry);
    String shown = shown(entry.id());
    write(
        "outbox save",
        "outbox save of entry " + shown,
        "Failed to save outbox entry: " + shown,
        conn -> {
          insert(conn, List.of(entry));
          return null;
        });
  }

  /**
   * Saves {@code entries} on the caller's connection, inside the caller's open write transaction:
   * neither begins, commits nor rolls back. {@link SqliteEventStore} calls it inside its append
   * transaction, so the entries commit with the events or not at all.
   *
   * @param entries the entries to save (required; empty is a no-op)
   * @param conn the connection of the caller's open transaction (required)
   * @throws OutboxOrderingViolationException if this channel is {@code STRICT_PER_AGGREGATE} and an
   *     entry has no stream, checked over the whole list before any write, unwrapped
   * @throws EventStoreException if the write fails; the caller rolls its transaction back
   */
  void saveAll(List<OutboxEntry> entries, Connection conn) {
    if (entries == null) {
      throw new IllegalArgumentException("entries is required");
    }
    if (conn == null) {
      throw new IllegalArgumentException("conn is required");
    }
    if (entries.isEmpty()) {
      return;
    }
    for (OutboxEntry entry : entries) {
      requireStreamIfStrict(entry);
    }
    try {
      insert(conn, entries);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to save " + entries.size() + " outbox entries", e);
    }
  }

  private void insert(Connection conn, List<OutboxEntry> entries) throws SQLException {
    long createdAt = now();
    try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
      for (OutboxEntry entry : entries) {
        ps.setString(1, entry.id().value());
        ps.setString(2, entry.payload());
        ps.setString(3, entry.payloadType());
        StreamKeys.bind(ps, 4, entry.streamId());
        bindTraceLink(ps, 6, entry.traceLink());
        ps.setLong(8, createdAt);
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }

  /**
   * Claims up to {@code limit} eligible entries for this store and returns them ({@code
   * IN_PROGRESS}, in {@code seq} order), after taking back every claim older than the lease; both
   * in one write transaction (see the class javadoc).
   */
  @Override
  public List<OutboxEntry> loadPending(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
    return write(
        "outbox claim",
        "outbox claim",
        "Failed to load pending outbox entries",
        conn -> {
          long now = now();
          try (PreparedStatement reclaim = conn.prepareStatement(RECLAIM_EXPIRED)) {
            reclaim.setLong(1, SqliteTimestamps.plusSaturating(now, claimLease.negated()));
            reclaim.executeUpdate();
          }
          List<ClaimedRow> claimed = new ArrayList<>();
          try (PreparedStatement ps = conn.prepareStatement(claimPending)) {
            ps.setLong(1, now);
            ps.setInt(2, limit);
            ps.setString(3, claimedBy);
            try (ResultSet rs = ps.executeQuery()) {
              while (rs.next()) {
                claimed.add(new ClaimedRow(rs.getLong("seq"), mapRow(rs, true)));
              }
            }
          }
          claimed.sort(Comparator.comparingLong(ClaimedRow::seq));
          return claimed.stream().map(ClaimedRow::entry).toList();
        });
  }

  /** A claimed row and its {@code seq}, the order the claim returns. */
  private record ClaimedRow(long seq, OutboxEntry entry) {}

  @Override
  public boolean markDelivered(OutboxEntryId id, String claimedBy) {
    requireId(id);
    String shown = shown(id);
    return write(
            "outbox mark delivered",
            "outbox mark delivered of entry " + shown,
            "Failed to mark outbox entry as delivered: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(MARK_DELIVERED)) {
                ps.setLong(1, now());
                ps.setString(2, id.value());
                ps.setString(3, claimedBy);
                return ps.executeUpdate();
              }
            })
        == 1;
  }

  @Override
  public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
    requireId(id);
    String shown = shown(id);
    return write(
            "outbox mark failed",
            "outbox mark failed of entry " + shown,
            "Failed to mark outbox entry as failed: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(MARK_FAILED)) {
                ps.setInt(1, attempts);
                ps.setString(2, error);
                ps.setLong(3, now());
                ps.setString(4, id.value());
                ps.setString(5, claimedBy);
                return ps.executeUpdate();
              }
            })
        == 1;
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code next_retry_at} is the store clock's instant plus {@code backoff}, the clock the claim
   * compares it with; a backoff beyond the timestamp range saturates (see {@code
   * SqliteTimestamps.plusSaturating}).
   */
  @Override
  public boolean markRetry(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
    requireId(id);
    if (backoff == null) {
      throw new IllegalArgumentException("backoff is required");
    }
    String shown = shown(id);
    return write(
            "outbox mark retry",
            "outbox mark retry of entry " + shown,
            "Failed to mark outbox entry for retry: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(MARK_RETRY)) {
                ps.setInt(1, attempts);
                ps.setString(2, error);
                ps.setLong(3, SqliteTimestamps.plusSaturating(now(), backoff));
                ps.setString(4, id.value());
                ps.setString(5, claimedBy);
                return ps.executeUpdate();
              }
            })
        == 1;
  }

  @Override
  public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
    if (ids.isEmpty()) {
      return 0;
    }
    List<String> values = ids.stream().map(OutboxEntryId::value).toList();
    return write(
        "outbox mark delivered (batch)",
        "outbox mark delivered of " + values.size() + " entries",
        "Failed to mark outbox entries delivered",
        conn -> {
          long now = now();
          int transitioned = 0;
          for (List<String> chunk : chunks(values)) {
            try (PreparedStatement ps =
                conn.prepareStatement(MARK_DELIVERED_ALL.formatted(placeholders(chunk.size())))) {
              ps.setLong(1, now);
              bindIds(ps, 2, chunk);
              ps.setString(chunk.size() + 2, claimedBy);
              transitioned += ps.executeUpdate();
            }
          }
          return transitioned;
        });
  }

  /**
   * One transaction for the whole never-attempted tail, touching only the claim columns: {@code
   * attempts}, {@code last_error} and {@code next_retry_at} stay as they were. The same {@code
   * IN_PROGRESS} + {@code claimed_by} compare-and-set as every mark.
   */
  @Override
  public int releaseClaims(Collection<OutboxEntry> entries, String claimedBy) {
    if (entries.isEmpty()) {
      return 0;
    }
    List<String> values = entries.stream().map(e -> e.id().value()).toList();
    return write(
        "outbox release claims",
        "outbox release of " + values.size() + " claims",
        "Failed to release unattempted outbox claims",
        conn -> {
          int released = 0;
          for (List<String> chunk : chunks(values)) {
            try (PreparedStatement ps =
                conn.prepareStatement(RELEASE_CLAIMS.formatted(placeholders(chunk.size())))) {
              bindIds(ps, 1, chunk);
              ps.setString(chunk.size() + 1, claimedBy);
              released += ps.executeUpdate();
            }
          }
          return released;
        });
  }

  private static List<List<String>> chunks(List<String> values) {
    List<List<String>> chunks = new ArrayList<>();
    for (int from = 0; from < values.size(); from += ID_CHUNK) {
      chunks.add(values.subList(from, Math.min(values.size(), from + ID_CHUNK)));
    }
    return chunks;
  }

  private static String placeholders(int count) {
    return String.join(", ", Collections.nCopies(count, "?"));
  }

  private static void bindIds(PreparedStatement ps, int firstIndex, List<String> ids)
      throws SQLException {
    for (int i = 0; i < ids.size(); i++) {
      ps.setString(firstIndex + i, ids.get(i));
    }
  }

  /**
   * Removes a non-{@code FAILED} entry; an absent id is a no-op. A {@code FAILED} row is refused
   * with {@link IllegalStateException}: its only exits are the audited {@link #skipFailed} and
   * {@link #resetFailedToPending}. The delete and the check of what stopped it run in one
   * transaction.
   */
  @Override
  public void delete(OutboxEntryId id) {
    requireId(id);
    String shown = shown(id);
    boolean refused =
        write(
            "outbox delete",
            "outbox delete of entry " + shown,
            "Failed to delete outbox entry: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(DELETE_NON_FAILED)) {
                ps.setString(1, id.value());
                if (ps.executeUpdate() == 1) {
                  return false;
                }
              }
              try (PreparedStatement ps = conn.prepareStatement(EXISTS)) {
                ps.setString(1, id.value());
                try (ResultSet rs = ps.executeQuery()) {
                  return rs.next();
                }
              }
            });
    if (refused) {
      throw new IllegalStateException(
          "outbox entry "
              + shown
              + " is FAILED; resolve it with OutboxFailedReplayer.skip or .replay — delete would"
              + " release its aggregate with no record");
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Deletes in chunks of {@value #DELETE_BATCH_SIZE} rows, each in a write transaction of its
   * own, so the sweep never holds the write lock for long.
   *
   * @throws NullPointerException if {@code olderThan} is {@code null}, as on Postgres
   */
  @Override
  public int deleteDelivered(Instant olderThan) {
    Objects.requireNonNull(olderThan, "olderThan is required");
    return deleteInChunks(
        DELETE_DELIVERED_BATCH,
        olderThan,
        "outbox delivered retention",
        "Failed to delete delivered outbox entries");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Deletes in chunks of {@value #DELETE_BATCH_SIZE} rows, each in a write transaction of its
   * own.
   */
  @Override
  public int deleteSkipped(Instant cutoff) {
    if (cutoff == null) {
      throw new IllegalArgumentException("cutoff is required");
    }
    return deleteInChunks(
        DELETE_SKIPPED_BATCH,
        cutoff,
        "outbox skipped retention",
        "Failed to delete skipped outbox entries");
  }

  /**
   * Runs {@code sql} chunk by chunk while a chunk deletes a row: each chunk deletes rows older than
   * the past cutoff only, so the set shrinks with every non-empty chunk and the loop ends.
   */
  private int deleteInChunks(String sql, Instant cutoff, String transaction, String failure) {
    long cutoffMicros = SqliteTimestamps.toMicros(cutoff);
    int total = 0;
    int batch;
    do {
      batch =
          write(
              transaction,
              transaction,
              failure,
              conn -> {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                  ps.setLong(1, cutoffMicros);
                  ps.setInt(2, DELETE_BATCH_SIZE);
                  return ps.executeUpdate();
                }
              });
      total += batch;
    } while (batch > 0);
    return total;
  }

  @Override
  public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
    if (status == null) {
      throw new IllegalArgumentException("status is required");
    }
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
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

  @Override
  public Optional<OutboxEntry> findById(OutboxEntryId id) {
    requireId(id);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_BY_ID)) {
      ps.setString(1, id.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(mapRow(rs, false)) : Optional.empty();
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to find outbox entry: " + shown(id), e);
    }
  }

  @Override
  public long countByStatus(OutboxStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("status is required");
    }
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
    requireId(id);
    String shown = shown(id);
    return write(
            "outbox reset failed",
            "outbox reset of entry " + shown,
            "Failed to reset failed outbox entry to pending: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(RESET_FAILED_TO_PENDING)) {
                ps.setString(1, id.value());
                return ps.executeUpdate();
              }
            })
        == 1;
  }

  @Override
  public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
    requireId(id);
    if (skippedBy == null) {
      throw new IllegalArgumentException("skippedBy is required");
    }
    if (reason == null) {
      throw new IllegalArgumentException("reason is required");
    }
    OutboxStore.requireSkippedBy(skippedBy);
    String shown = shown(id);
    return write(
            "outbox skip failed",
            "outbox skip of entry " + shown,
            "Failed to skip failed outbox entry: " + shown,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(SKIP_FAILED)) {
                ps.setLong(1, now());
                ps.setString(2, skippedBy);
                ps.setString(3, reason);
                ps.setString(4, id.value());
                return ps.executeUpdate();
              }
            })
        == 1;
  }

  @Override
  public BlockageSample sampleBlockage() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SAMPLE_BLOCKAGE);
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      long failedAggregates = rs.getLong(1);
      long oldest = rs.getLong(2);
      Instant oldestFailedAt = rs.wasNull() ? null : SqliteTimestamps.fromMicros(oldest);
      return new BlockageSample(failedAggregates, oldestFailedAt, rs.getLong(3));
    } catch (Exception e) {
      throw new EventStoreException("Failed to sample outbox blockage", e);
    }
  }

  private <T> T write(
      String transaction, String what, String failureMessage, SqliteTransactions.Work<T> work) {
    return SqliteTransactions.write(dataSource, transaction, what, failureMessage, work);
  }

  private static void requireId(OutboxEntryId id) {
    if (id == null) {
      throw new IllegalArgumentException("id is required");
    }
  }

  /** An entry id rendered for a message: caller text, so sanitized. */
  private static String shown(OutboxEntryId id) {
    return LogSanitizer.sanitizeForLog(id.value());
  }

  /**
   * Binds {@code link} to {@code trace_id} and {@code span_id} at {@code index} and {@code index +
   * 1}: both ids when they are valid W3C ids, two {@code NULL}s otherwise, as on Postgres.
   */
  private static void bindTraceLink(PreparedStatement ps, int index, SpanLink link)
      throws SQLException {
    boolean valid = link.isPresent() && link.isW3c();
    ps.setString(index, valid ? link.traceId().value() : null);
    ps.setString(index + 1, valid ? link.spanId().value() : null);
  }

  /**
   * The one row mapper of every outbox read, as on Postgres: a {@code claimed} row reports no
   * {@code processedAt} (a replayed row still carries the stamp of its earlier {@code FAILED}), the
   * skip audit is read iff the row is {@code SKIPPED}, and a stored trace id or span id that is not
   * a valid W3C id reads back as {@link SpanLink#NONE}.
   */
  private static OutboxEntry mapRow(ResultSet rs, boolean claimed) throws SQLException {
    var status = OutboxStatus.valueOf(rs.getString("status"));
    OutboxEntry.SkipRecord skip = null;
    if (status == OutboxStatus.SKIPPED) {
      skip =
          new OutboxEntry.SkipRecord(
              instantOrNull(rs, "skipped_at"),
              rs.getString("skipped_by"),
              rs.getString("skip_reason"));
    }
    return new OutboxEntry(
        OutboxEntryId.of(rs.getString("entry_id")),
        rs.getString("payload"),
        rs.getString("payload_type"),
        StreamKeys.read(rs),
        status,
        rs.getInt("attempts"),
        rs.getString("last_error"),
        instantOrNull(rs, "created_at"),
        claimed ? null : instantOrNull(rs, "processed_at"),
        instantOrNull(rs, "next_retry_at"),
        skip,
        SpanLink.ofW3c(rs.getString("trace_id"), rs.getString("span_id")));
  }

  private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
    long micros = rs.getLong(column);
    return rs.wasNull() ? null : SqliteTimestamps.fromMicros(micros);
  }
}

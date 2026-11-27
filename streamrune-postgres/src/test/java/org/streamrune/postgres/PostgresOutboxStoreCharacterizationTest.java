package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import javax.sql.DataSource;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore.BlockageSample;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.test.EventStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Characterization of the PostgreSQL outbox store: the statements, connections and commits of every
 * read and write, the claim order of both ordering modes (sorted by {@code seq} in the claim
 * statement itself), the database clock behind every timestamp, the lease and the backoff, the row
 * mapping (a claimed row reports no {@code processedAt}; a skip audit without its instant is a
 * storage failure), the exact type, message and cause of every refusal and failure, which failures
 * each operation wraps (most catch every exception, the set-based marks and the retention sweeps
 * only {@code SQLException}), and the benign in-flight unique violation a claim swallows with one
 * DEBUG line. The values are what the store does on a real database today, written down as they
 * come out; these tests stay unchanged while the store's validation and row mapping move into
 * shared helpers.
 *
 * <p>Every store borrows from a {@link RecordingDataSource}. Over a pool that hands out {@code
 * autoCommit=true} connections every operation is one connection and its statements; over a pool
 * that hands out {@code autoCommit=false} connections every write also commits. The store takes no
 * clock: every timestamp is the database's {@code NOW()}, and {@code entry.createdAt()} is ignored.
 */
@Testcontainers
class PostgresOutboxStoreCharacterizationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_outbox_store_characterization");

  private static final AggregateType AGG = AggregateType.of("agg");
  private static final Duration LEASE = Duration.ofMinutes(4);
  private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String SPAN = "00f067aa0ba902b7";

  /** The instant {@code fixed_clock.now()} answers; see {@link #onTheFixedClock}. */
  private static final Instant FIXED = Instant.parse("2026-11-27T09:00:00.654321Z");

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String INSERT =
      "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
          + " trace_id, span_id, status, attempts, last_error, created_at, processed_at) "
          + "VALUES (?, ?::jsonb, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, NOW(), NULL) "
          + "ON CONFLICT (entry_id) DO NOTHING";
  private static final String CLAIM_AVAILABILITY_FIRST =
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
          + "    skipped_at, skipped_by, skip_reason, trace_id, span_id, seq"
          + ") SELECT * FROM claimed ORDER BY seq ASC";
  private static final String CLAIM_STRICT =
      "WITH null_agg AS ("
          + "  SELECT entry_id, seq FROM outbox_events"
          + "  WHERE status = 'PENDING' AND aggregate_id IS NULL"
          + "    AND (next_retry_at IS NULL OR next_retry_at <= NOW())"
          + "), agg_head AS ("
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
          + "    skipped_at, skipped_by, skip_reason, trace_id, span_id, seq"
          + ") SELECT * FROM claimed ORDER BY seq ASC";

  /** The prefix both claim statements share. */
  private static final String CLAIM_PREFIX = "WITH null_agg AS (";

  private static final String RECLAIM_EXPIRED =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL "
          + "WHERE status = 'IN_PROGRESS' AND claimed_at < NOW() - make_interval(secs => ?)";
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
  private static final String DELETE_NON_FAILED =
      "DELETE FROM outbox_events WHERE entry_id = ? AND status <> 'FAILED'";
  private static final String EXISTS = "SELECT 1 FROM outbox_events WHERE entry_id = ?";
  private static final String SKIP_FAILED =
      "UPDATE outbox_events SET status = 'SKIPPED', skipped_at = NOW(), skipped_by = ?,"
          + " skip_reason = ? WHERE entry_id = ? AND status = 'FAILED'";
  private static final String MARK_DELIVERED_ALL =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = NOW() "
          + "WHERE entry_id = ANY(?) AND status = 'IN_PROGRESS' AND claimed_by = ?";
  private static final String RELEASE_CLAIMS =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL "
          + "WHERE entry_id = ANY(?) AND status = 'IN_PROGRESS' AND claimed_by = ?";
  private static final String DELETE_DELIVERED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'DELIVERED' AND entry_id IN ("
          + "  SELECT entry_id FROM outbox_events"
          + "  WHERE status = 'DELIVERED' AND processed_at < ?"
          + "  ORDER BY processed_at LIMIT ?)";
  private static final String ENTRY_COLUMNS =
      "entry_id, payload::text AS payload, payload_type, aggregate_type, aggregate_id, status,"
          + " attempts,"
          + " last_error, created_at, processed_at, next_retry_at, skipped_at, skipped_by,"
          + " skip_reason, trace_id, span_id, seq";
  private static final String FIND_BY_STATUS =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE status = ? ORDER BY seq ASC LIMIT ?";
  private static final String FIND_BY_ID =
      "SELECT " + ENTRY_COLUMNS + " FROM outbox_events WHERE entry_id = ?";
  private static final String SAMPLE_BLOCKAGE =
      "SELECT count(DISTINCT (aggregate_type, aggregate_id)) FILTER (WHERE aggregate_id IS NOT NULL),"
          + " min(processed_at), count(*) FILTER (WHERE aggregate_id IS NULL)"
          + " FROM outbox_events WHERE status = 'FAILED'";
  private static final String DELETE_SKIPPED_BATCH =
      "DELETE FROM outbox_events WHERE status = 'SKIPPED' AND entry_id IN ("
          + "  SELECT entry_id FROM outbox_events"
          + "  WHERE status = 'SKIPPED' AND skipped_at < ?"
          + "  ORDER BY skipped_at LIMIT ?)";
  private static final String COUNT_BY_STATUS =
      "SELECT count(*) FROM outbox_events WHERE status = ?";
  private static final String RESET_FAILED_TO_PENDING =
      "UPDATE outbox_events SET status = 'PENDING', attempts = 0, last_error = NULL, "
          + "next_retry_at = NULL, claimed_at = NULL, claimed_by = NULL "
          + "WHERE entry_id = ? AND status = 'FAILED'";

  /** The benign in-flight unique violation's DEBUG line, formatted. */
  private static final String INFLIGHT_DEBUG =
      "Outbox claim hit the one-inflight-per-stream guard (ux_outbox_one_inflight_per_stream); a"
          + " concurrent claim won this stream. Returning no entries; next poll re-claims.";

  /** A pool that hands out {@code autoCommit=false} connections, as a configured pool may. */
  static final class ManualCommitDataSource extends PGSimpleDataSource {
    @Override
    public Connection getConnection() throws SQLException {
      Connection connection = super.getConnection();
      connection.setAutoCommit(false);
      return connection;
    }
  }

  static PGSimpleDataSource dataSource;
  static ManualCommitDataSource manualDataSource;

  private RecordingDataSource recorder;
  private PostgresOutboxStore strict;
  private PostgresOutboxStore availabilityFirst;

  @BeforeAll
  static void initSchema() throws SQLException {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    manualDataSource = new ManualCommitDataSource();
    manualDataSource.setUrl(PG.getJdbcUrl());
    manualDataSource.setUser(PG.getUsername());
    manualDataSource.setPassword(PG.getPassword());
    EventTypeRegistry registry = EventStoreContract.Setup.defaults().registry();
    new PostgresEventStoreFactory(dataSource, registry).create();
    // A now() that answers one fixed instant, found before pg_catalog's only by a session whose
    // search_path names its schema first (see onTheFixedClock).
    execute("CREATE SCHEMA fixed_clock");
    execute(
        "CREATE FUNCTION fixed_clock.now() RETURNS timestamptz LANGUAGE sql IMMUTABLE AS"
            + " $$ SELECT TIMESTAMPTZ '2026-11-27 09:00:00.654321+00' $$");
  }

  /** Every test starts from an empty table and a fresh recorder over an autoCommit=true pool. */
  @BeforeEach
  void emptyTable() throws SQLException {
    execute("DELETE FROM outbox_events");
    use(dataSource);
  }

  /** Switches the test to a recorder over the autoCommit=false pool. */
  private void useManualCommitPool() {
    use(manualDataSource);
  }

  private void use(DataSource pool) {
    recorder = new RecordingDataSource(pool);
    strict = new PostgresOutboxStore(recorder, LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE);
    availabilityFirst =
        new PostgresOutboxStore(recorder, LEASE, OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  private static StreamId stream(String id) {
    return StreamId.of(AGG, AggregateId.of(id));
  }

  private static OutboxEntry entry(String id, StreamId stream) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{\"k\":1}", "Event", stream);
  }

  private static OutboxEntryId id(String id) {
    return OutboxEntryId.of(id);
  }

  private static List<String> ids(List<OutboxEntry> entries) {
    return entries.stream().map(e -> e.id().value()).toList();
  }

  /** Saves {@code id} on {@code stream}, claims it and marks it FAILED: a FAILED row. */
  private void failed(String id, StreamId stream) {
    availabilityFirst.save(entry(id, stream));
    assertThat(ids(availabilityFirst.loadPending(1))).containsExactly(id);
    assertThat(availabilityFirst.markFailed(id(id), 3, "boom", availabilityFirst.claimedBy()))
        .isTrue();
  }

  // ── Claim order ─────────────────────────────────────────────────────────────────────────────

  /**
   * Twenty-four entries, the i-th saved (seq order i) with the entry id {@code r(25-i)}, so the ids
   * run against seq: r24, r20, … on stream a; r23, r19, … on b; r22, r18, … on c; every fourth
   * (r21, r17, …) without a stream. Then c's head r22 is FAILED, and every row's created_at is
   * rewritten to run against seq too, highest seq first, so neither the id, nor created_at, nor the
   * order the rows were last written in gives seq order.
   */
  private void interleavedRows() throws SQLException {
    interleavedRows(i -> "r%02d".formatted(25 - i));
  }

  private void interleavedRows(IntFunction<String> name) throws SQLException {
    String[] streams = {null, "a", "b", "c"};
    for (int i = 1; i <= 24; i++) {
      String s = streams[i % 4];
      availabilityFirst.save(entry(name.apply(i), s == null ? null : stream(s)));
    }
    execute(
        "UPDATE outbox_events SET status = 'FAILED', processed_at = NOW() WHERE entry_id = '"
            + name.apply(3)
            + "'");
    execute(
        "DO $$ DECLARE r record; BEGIN"
            + " FOR r IN SELECT entry_id, seq FROM outbox_events ORDER BY seq DESC LOOP"
            + " UPDATE outbox_events"
            + " SET created_at = TIMESTAMPTZ '2026-11-27 09:00:00+00' - make_interval(secs => r.seq)"
            + " WHERE entry_id = r.entry_id;"
            + " END LOOP; END $$");
    recorder.clear();
  }

  @Test
  void claimOrder_strict() throws SQLException {
    interleavedRows();

    // The FAILED head r22 holds stream c; the stream-less rows are claimed freely; seq order.
    assertThat(ids(strict.loadPending(100)))
        .containsExactly("r24", "r23", "r21", "r17", "r13", "r09", "r05", "r01");

    // a's and b's heads are IN_PROGRESS: their streams stay blocked until the heads leave it.
    assertThat(strict.loadPending(100)).isEmpty();
    assertThat(strict.markDelivered(id("r24"), strict.claimedBy())).isTrue();
    assertThat(ids(strict.loadPending(100))).containsExactly("r20");
  }

  @Test
  void claimOrder_availabilityFirst() throws SQLException {
    interleavedRows();

    // The FAILED row is claimed past: c's head is its lowest PENDING row, r18.
    assertThat(ids(availabilityFirst.loadPending(100)))
        .containsExactly("r24", "r23", "r21", "r18", "r17", "r13", "r09", "r05", "r01");

    assertThat(availabilityFirst.loadPending(100)).isEmpty();
    assertThat(availabilityFirst.markDelivered(id("r18"), availabilityFirst.claimedBy())).isTrue();
    assertThat(ids(availabilityFirst.loadPending(100))).containsExactly("r14");
  }

  @Test
  void claimOrder_limitTakesTheLowestSeqs() throws SQLException {
    interleavedRows();

    assertThat(ids(availabilityFirst.loadPending(3))).containsExactly("r24", "r23", "r21");
    assertThat(ids(availabilityFirst.loadPending(100)))
        .containsExactly("r18", "r17", "r13", "r09", "r05", "r01");
  }

  // ── The database clock ──────────────────────────────────────────────────────────────────────

  /**
   * Every timestamp is the database's {@code NOW()} of the write's own transaction: over an
   * autoCommit=false pool a statement run first on the store's connection opens that transaction
   * and reads its {@code now()}, and every stored timestamp equals it exactly. {@code
   * entry.createdAt()} is ignored, and the store takes no JVM clock that could skew a value.
   */
  @Test
  void databaseClockIsAuthoritative() throws SQLException {
    useManualCommitPool();
    OutboxEntry e1 =
        new OutboxEntry(
            id("e1"),
            "{}",
            "Event",
            stream("a"),
            OutboxStatus.PENDING,
            0,
            null,
            Instant.parse("2001-02-03T04:05:06.789Z"),
            null,
            null,
            null,
            SpanLink.NONE);
    AtomicReference<Instant> now = new AtomicReference<>();

    captureTransactionNow(INSERT, now);
    strict.save(e1);
    Instant inserted = now.get();
    assertThat(instantValue("SELECT created_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(inserted);
    assertThat(strict.findById(id("e1")).orElseThrow().createdAt()).isEqualTo(inserted);

    captureTransactionNow(RECLAIM_EXPIRED, now);
    List<OutboxEntry> claimed = strict.loadPending(10);
    assertThat(ids(claimed)).containsExactly("e1");
    assertThat(claimed.getFirst().createdAt()).isEqualTo(inserted);
    assertThat(instantValue("SELECT claimed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(now.get());
    assertThat(stringValue("SELECT claimed_by FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(strict.claimedBy());

    // The backoff is bound in seconds with millisecond precision: 1.5 s, the nanoseconds dropped.
    captureTransactionNow(MARK_RETRY, now);
    assertThat(
            strict.markRetry(
                id("e1"),
                1,
                "later",
                Duration.ofMillis(1_500).plusNanos(999_999),
                strict.claimedBy()))
        .isTrue();
    Instant retryAt = now.get().plusMillis(1_500);
    assertThat(instantValue("SELECT next_retry_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(retryAt);
    assertThat(strict.findById(id("e1")).orElseThrow().nextRetryAt()).isEqualTo(retryAt);

    strict.save(entry("e2", stream("b")));
    strict.save(entry("e3", stream("c")));
    assertThat(ids(strict.loadPending(10))).containsExactly("e2", "e3");

    captureTransactionNow(MARK_DELIVERED, now);
    assertThat(strict.markDelivered(id("e2"), strict.claimedBy())).isTrue();
    assertThat(instantValue("SELECT processed_at FROM outbox_events WHERE entry_id = 'e2'"))
        .isEqualTo(now.get());
    assertThat(strict.findById(id("e2")).orElseThrow().processedAt()).isEqualTo(now.get());

    captureTransactionNow(MARK_FAILED, now);
    assertThat(strict.markFailed(id("e3"), 2, "boom", strict.claimedBy())).isTrue();
    assertThat(strict.findById(id("e3")).orElseThrow().processedAt()).isEqualTo(now.get());

    captureTransactionNow(SKIP_FAILED, now);
    assertThat(strict.skipFailed(id("e3"), "operator", "obsolete")).isTrue();
    assertThat(instantValue("SELECT skipped_at FROM outbox_events WHERE entry_id = 'e3'"))
        .isEqualTo(now.get());
    assertThat(strict.findById(id("e3")).orElseThrow().skip())
        .isEqualTo(new OutboxEntry.SkipRecord(now.get(), "operator", "obsolete"));
  }

  /**
   * The statements call the session's {@code now()}: a session whose {@code now()} answers a fixed
   * instant stamps exactly that instant, and the lease and the backoff gate are measured on it. A
   * claim older than the lease by exactly the lease stays; one microsecond older is reclaimed. A
   * {@code next_retry_at} equal to now is due; one microsecond later is not.
   */
  @Test
  void theStatementsReadTheSessionClock() throws SQLException {
    onTheFixedClock(INSERT);
    strict.save(entry("e1", stream("a")));
    assertThat(instantValue("SELECT created_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(FIXED);

    // e1 is claimed by another relay exactly one lease before the fixed now: not reclaimed.
    execute(
        "UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_by = 'other',"
            + " claimed_at = TIMESTAMPTZ '2026-11-27 08:56:00.654321+00' WHERE entry_id = 'e1'");
    onTheFixedClock(RECLAIM_EXPIRED);
    assertThat(strict.loadPending(10)).isEmpty();
    assertThat(stringValue("SELECT claimed_by FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo("other");

    // One microsecond older: reclaimed and claimed again in the same transaction, at the fixed now.
    execute(
        "UPDATE outbox_events SET claimed_at = TIMESTAMPTZ '2026-11-27 08:56:00.65432+00'"
            + " WHERE entry_id = 'e1'");
    onTheFixedClock(RECLAIM_EXPIRED);
    assertThat(ids(strict.loadPending(10))).containsExactly("e1");
    assertThat(instantValue("SELECT claimed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(FIXED);
    assertThat(stringValue("SELECT claimed_by FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(strict.claimedBy());

    // The backoff gate: a stream head and a stream-less row, due one microsecond after now.
    availabilityFirst.save(entry("h", stream("b")));
    availabilityFirst.save(entry("n", null));
    execute(
        "UPDATE outbox_events SET next_retry_at = TIMESTAMPTZ '2026-11-27 09:00:00.654322+00'"
            + " WHERE entry_id IN ('h', 'n')");
    onTheFixedClock(RECLAIM_EXPIRED);
    assertThat(availabilityFirst.loadPending(10)).isEmpty();
    execute(
        "UPDATE outbox_events SET next_retry_at = TIMESTAMPTZ '2026-11-27 09:00:00.654321+00'"
            + " WHERE entry_id IN ('h', 'n')");
    onTheFixedClock(RECLAIM_EXPIRED);
    assertThat(ids(availabilityFirst.loadPending(10))).containsExactly("h", "n");

    // markRetry's next_retry_at is now plus the backoff; processed_at and skipped_at are now.
    onTheFixedClock(MARK_RETRY);
    assertThat(
            availabilityFirst.markRetry(
                id("h"), 1, "x", Duration.ofSeconds(30), availabilityFirst.claimedBy()))
        .isTrue();
    assertThat(availabilityFirst.findById(id("h")).orElseThrow().nextRetryAt())
        .isEqualTo(Instant.parse("2026-11-27T09:00:30.654321Z"));
    onTheFixedClock(MARK_FAILED);
    assertThat(availabilityFirst.markFailed(id("n"), 1, "x", availabilityFirst.claimedBy()))
        .isTrue();
    assertThat(availabilityFirst.findById(id("n")).orElseThrow().processedAt()).isEqualTo(FIXED);
    onTheFixedClock(SKIP_FAILED);
    assertThat(availabilityFirst.skipFailed(id("n"), "operator", "obsolete")).isTrue();
    assertThat(availabilityFirst.findById(id("n")).orElseThrow().skip().skippedAt())
        .isEqualTo(FIXED);
    onTheFixedClock(MARK_DELIVERED);
    assertThat(strict.markDelivered(id("e1"), strict.claimedBy())).isTrue();
    assertThat(strict.findById(id("e1")).orElseThrow().processedAt()).isEqualTo(FIXED);
  }

  /** A second save of an id is a silent no-op: the first row, and its created_at, stay. */
  @Test
  void saveOfAnExistingId_isASilentNoOp() throws SQLException {
    strict.save(entry("e1", stream("a")));
    Instant created = instantValue("SELECT created_at FROM outbox_events WHERE entry_id = 'e1'");
    recorder.clear();

    strict.save(OutboxEntry.pending(id("e1"), "{\"other\":2}", "Other", stream("b")));

    assertThat(recorder.transcript()).containsExactly(single(INSERT));
    OutboxEntry row = strict.findById(id("e1")).orElseThrow();
    assertThat(row.payload()).isEqualTo("{\"k\": 1}");
    assertThat(row.payloadType()).isEqualTo("Event");
    assertThat(row.streamId()).isEqualTo(stream("a"));
    assertThat(row.createdAt()).isEqualTo(created);
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);
  }

  // ── Row mapping ─────────────────────────────────────────────────────────────────────────────

  /**
   * A claimed row reports no processedAt, though the row still carries the stamp of its earlier
   * FAILED; any other read of the same row reports it. Every other column of the claimed row is
   * read: a past next_retry_at, the stream and the trace link.
   */
  @Test
  void claimedRowsReportNoProcessedAt() throws SQLException {
    availabilityFirst.save(entry("e1", stream("a")).withTraceLink(SpanLink.ofW3c(TRACE, SPAN)));
    assertThat(ids(availabilityFirst.loadPending(1))).containsExactly("e1");
    assertThat(availabilityFirst.markFailed(id("e1"), 3, "boom", availabilityFirst.claimedBy()))
        .isTrue();
    assertThat(availabilityFirst.resetFailedToPending(id("e1"))).isTrue();
    execute(
        "UPDATE outbox_events SET created_at = TIMESTAMPTZ '2026-11-27 08:00:00.000001+00',"
            + " processed_at = TIMESTAMPTZ '2026-11-27 08:30:00.000002+00',"
            + " next_retry_at = TIMESTAMPTZ '2026-11-27 08:45:00.000003+00',"
            + " attempts = 2, last_error = 'earlier' WHERE entry_id = 'e1'");

    List<OutboxEntry> claimed = availabilityFirst.loadPending(10);

    assertThat(claimed)
        .containsExactly(
            new OutboxEntry(
                id("e1"),
                "{\"k\": 1}",
                "Event",
                stream("a"),
                OutboxStatus.IN_PROGRESS,
                2,
                "earlier",
                Instant.parse("2026-11-27T08:00:00.000001Z"),
                null,
                Instant.parse("2026-11-27T08:45:00.000003Z"),
                null,
                SpanLink.ofW3c(TRACE, SPAN)));
    Instant stamp = Instant.parse("2026-11-27T08:30:00.000002Z");
    assertThat(instantValue("SELECT processed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(stamp);
    assertThat(
            availabilityFirst.findByStatus(OutboxStatus.IN_PROGRESS, 10).getFirst().processedAt())
        .isEqualTo(stamp);
    assertThat(availabilityFirst.findById(id("e1")).orElseThrow().processedAt()).isEqualTo(stamp);
  }

  @Test
  void rowsAssembleEveryColumn() throws SQLException {
    failed("t3", stream("b"));
    availabilityFirst.save(entry("t1", stream("a")).withTraceLink(SpanLink.ofW3c(TRACE, SPAN)));
    // A link that is present but not W3C is stored as two NULLs.
    availabilityFirst.save(
        entry("t2", null).withTraceLink(new SpanLink(new TraceId("corr-1"), new SpanId("s-1"))));
    availabilityFirst.save(entry("t4", stream("c")).withTraceLink(SpanLink.ofW3c(TRACE, SPAN)));
    // A stored id that is not a valid W3C id reads back as NONE.
    execute("UPDATE outbox_events SET trace_id = 'zz' WHERE entry_id = 't4'");
    // A stored stream with a control character reads back through the lenient constructors.
    execute("UPDATE outbox_events SET aggregate_id = 'x' || chr(10) || 'y' WHERE entry_id = 't1'");
    execute(
        "UPDATE outbox_events SET created_at = TIMESTAMPTZ '2026-11-27 07:00:00.000001+00',"
            + " next_retry_at = TIMESTAMPTZ '2026-11-27 07:30:00.000002+00' WHERE entry_id = 't1'");
    execute(
        "UPDATE outbox_events SET created_at = TIMESTAMPTZ '2026-11-27 07:00:00.000003+00'"
            + " WHERE entry_id IN ('t2', 't4')");
    execute(
        "UPDATE outbox_events SET created_at = TIMESTAMPTZ '2026-11-27 06:00:00.000004+00',"
            + " processed_at = TIMESTAMPTZ '2026-11-27 06:30:00.000005+00' WHERE entry_id = 't3'");

    assertThat(stringValue("SELECT trace_id FROM outbox_events WHERE entry_id = 't2'")).isNull();
    assertThat(stringValue("SELECT span_id FROM outbox_events WHERE entry_id = 't2'")).isNull();
    assertThat(stringValue("SELECT trace_id FROM outbox_events WHERE entry_id = 't1'"))
        .isEqualTo(TRACE);
    assertThat(stringValue("SELECT span_id FROM outbox_events WHERE entry_id = 't1'"))
        .isEqualTo(SPAN);
    assertThat(availabilityFirst.findByStatus(OutboxStatus.PENDING, 10))
        .containsExactly(
            new OutboxEntry(
                id("t1"),
                "{\"k\": 1}",
                "Event",
                StreamId.of(new AggregateType("agg"), new AggregateId("x\ny")),
                OutboxStatus.PENDING,
                0,
                null,
                Instant.parse("2026-11-27T07:00:00.000001Z"),
                null,
                Instant.parse("2026-11-27T07:30:00.000002Z"),
                null,
                SpanLink.ofW3c(TRACE, SPAN)),
            new OutboxEntry(
                id("t2"),
                "{\"k\": 1}",
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                Instant.parse("2026-11-27T07:00:00.000003Z"),
                null,
                null,
                null,
                SpanLink.NONE),
            new OutboxEntry(
                id("t4"),
                "{\"k\": 1}",
                "Event",
                stream("c"),
                OutboxStatus.PENDING,
                0,
                null,
                Instant.parse("2026-11-27T07:00:00.000003Z"),
                null,
                null,
                null,
                SpanLink.NONE));
    assertThat(availabilityFirst.findById(id("t3")))
        .contains(
            new OutboxEntry(
                id("t3"),
                "{\"k\": 1}",
                "Event",
                stream("b"),
                OutboxStatus.FAILED,
                3,
                "boom",
                Instant.parse("2026-11-27T06:00:00.000004Z"),
                Instant.parse("2026-11-27T06:30:00.000005Z"),
                null,
                null,
                SpanLink.NONE));

    assertThat(availabilityFirst.skipFailed(id("t3"), "operator", "obsolete")).isTrue();
    execute(
        "UPDATE outbox_events SET skipped_at = TIMESTAMPTZ '2026-11-27 06:45:00.000006+00'"
            + " WHERE entry_id = 't3'");
    assertThat(availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10))
        .containsExactly(
            new OutboxEntry(
                id("t3"),
                "{\"k\": 1}",
                "Event",
                stream("b"),
                OutboxStatus.SKIPPED,
                3,
                "boom",
                Instant.parse("2026-11-27T06:00:00.000004Z"),
                Instant.parse("2026-11-27T06:30:00.000005Z"),
                null,
                new OutboxEntry.SkipRecord(
                    Instant.parse("2026-11-27T06:45:00.000006Z"), "operator", "obsolete"),
                SpanLink.NONE));
    assertThat(availabilityFirst.findById(id("absent"))).isEmpty();
  }

  /**
   * A SKIPPED row whose skipped_at was cleared does not read as a skip with no instant: the read
   * fails as a storage failure with a NullPointerException as the cause, on the open connection.
   */
  @Test
  void skippedRowWithNullSkippedAt_isEventStoreException() throws SQLException {
    failed("e1", stream("a"));
    assertThat(availabilityFirst.skipFailed(id("e1"), "operator", "obsolete")).isTrue();
    execute("UPDATE outbox_events SET skipped_at = NULL WHERE entry_id = 'e1'");
    recorder.clear();

    Throwable byStatus =
        catchThrowable(() -> availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10));

    assertThat(byStatus)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find outbox entries by status: SKIPPED");
    assertThat(byStatus.getCause()).isExactlyInstanceOf(NullPointerException.class).hasNoCause();
    assertThat(byStatus.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(single(FIND_BY_STATUS));
    assertThat(recorder.openConnections()).isZero();

    Throwable byId = catchThrowable(() -> availabilityFirst.findById(id("e1")));
    assertThat(byId)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find outbox entry: e1");
    assertThat(byId.getCause()).isExactlyInstanceOf(NullPointerException.class).hasNoCause();

    assertThat(availabilityFirst.countByStatus(OutboxStatus.SKIPPED)).isEqualTo(1L);
  }

  // ── The delete refusal ──────────────────────────────────────────────────────────────────────

  @Test
  void deleteRefusal_message() {
    String longId = "f\n" + "x".repeat(70);
    failed(longId, stream("a"));

    Throwable thrown = catchThrowable(() -> availabilityFirst.delete(id(longId)));

    // The id is sanitized: the line break is dropped and the id cut at 64 characters.
    assertThat(thrown)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(
            "outbox entry f"
                + "x".repeat(63)
                + "... is FAILED; resolve it with OutboxFailedReplayer.skip or .replay — delete"
                + " would release its aggregate with no record")
        .hasNoCause();
  }

  /**
   * The refusal is thrown on the open connection, after the delete statement found no row and the
   * existence check found one: exactly an IllegalStateException, never wrapped. Over an
   * autoCommit=false pool the delete is committed before the check; a close failure after that
   * commit is suppressed on the refusal.
   */
  @Test
  void deleteRefusal_isIllegalStateOnTheOpenConnection() {
    failed("f", stream("a"));
    recorder.clear();

    Throwable thrown = catchThrowable(() -> availabilityFirst.delete(id("f")));

    assertThat(thrown)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(
            "outbox entry f is FAILED; resolve it with OutboxFailedReplayer.skip or .replay —"
                + " delete would release its aggregate with no record")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + DELETE_NON_FAILED, "#1 prepare " + EXISTS, "#1 close");
    assertThat(recorder.openConnections()).isZero();
    assertThat(availabilityFirst.findById(id("f")).orElseThrow().status())
        .isEqualTo(OutboxStatus.FAILED);

    useManualCommitPool();
    Throwable manual = catchThrowable(() -> availabilityFirst.delete(id("f")));
    assertThat(manual).isExactlyInstanceOf(IllegalStateException.class).hasNoCause();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + DELETE_NON_FAILED,
            "#1 commit",
            "#1 prepare " + EXISTS,
            "#1 close");

    recorder.clear();
    recorder.failNextCloseAfterCommit();
    Throwable closeFailure = catchThrowable(() -> availabilityFirst.delete(id("f")));
    assertThat(closeFailure)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(
            "outbox entry f is FAILED; resolve it with OutboxFailedReplayer.skip or .replay —"
                + " delete would release its aggregate with no record")
        .hasNoCause();
    assertThat(closeFailure.getSuppressed()).hasSize(1);
    assertThat(closeFailure.getSuppressed()[0])
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
  }

  /** The quiet branches: a deleted row skips the existence check; an absent id is no refusal. */
  @Test
  void delete_deletedAndAbsent() {
    availabilityFirst.save(entry("p", null));
    recorder.clear();

    availabilityFirst.delete(id("p"));
    assertThat(recorder.transcript()).containsExactly(single(DELETE_NON_FAILED));
    assertThat(availabilityFirst.findById(id("p"))).isEmpty();

    recorder.clear();
    availabilityFirst.delete(id("absent"));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + DELETE_NON_FAILED, "#1 prepare " + EXISTS, "#1 close");
  }

  // ── Refusals before any I/O ─────────────────────────────────────────────────────────────────

  @Test
  void strictRefusesAStreamlessEntryBeforeAnyIo() throws SQLException {
    Throwable thrown = catchThrowable(() -> strict.save(entry("n\n1", null)));

    assertThat(thrown)
        .isExactlyInstanceOf(OutboxOrderingViolationException.class)
        .hasMessage(
            "Outbox entry n1 of type Event has no streamId, but this outbox channel is"
                + " STRICT_PER_AGGREGATE: every entry must carry the stream whose delivery order"
                + " the consumer depends on (normally the event's own stream). The append was"
                + " rolled back; fix the OutboxEventMapper, or declare the channel"
                + " AVAILABILITY_FIRST if its consumers tolerate unordered delivery.")
        .hasNoCause();
    assertThat(recorder.connectionsOpened()).isZero();
    assertThat(recorder.transcript()).isEmpty();

    // saveAll checks the whole list before its one insert.
    try (Connection conn = recorder.getConnection()) {
      Throwable all =
          catchThrowable(
              () -> strict.saveAll(List.of(entry("s1", stream("a")), entry("n2", null)), conn));
      assertThat(all)
          .isExactlyInstanceOf(OutboxOrderingViolationException.class)
          .hasMessageStartingWith("Outbox entry n2 of type Event has no streamId");
    }
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isZero();

    // The other mode accepts both.
    availabilityFirst.save(entry("n1", null));
    assertThat(availabilityFirst.findById(id("n1")).orElseThrow().streamId()).isNull();
    assertThat(stringValue("SELECT aggregate_type FROM outbox_events WHERE entry_id = 'n1'"))
        .isNull();
  }

  @Test
  void constructorMessages() {
    var mode = OutboxOrderingMode.STRICT_PER_AGGREGATE;

    assertRefusedBeforeIo(() -> new PostgresOutboxStore(null), "dataSource is required");
    assertRefusedBeforeIo(() -> new PostgresOutboxStore(null, null), "dataSource is required");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(null, null, null), "dataSource is required");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(recorder, null, mode), "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(recorder, Duration.ZERO, mode),
        "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(recorder, Duration.ofNanos(-1), mode),
        "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(recorder, null, null), "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new PostgresOutboxStore(recorder, LEASE, null), "orderingMode is required");

    // The defaults, and a claimedBy of its own per instance.
    PostgresOutboxStore defaults = new PostgresOutboxStore(recorder);
    assertThat(defaults.claimLease()).isEqualTo(Duration.ofMinutes(4));
    assertThat(defaults.orderingMode()).isEqualTo(mode);
    assertThat(defaults.claimedBy())
        .matches("outbox-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
        .isNotEqualTo(new PostgresOutboxStore(recorder).claimedBy());
    assertThat(new PostgresOutboxStore(recorder, Duration.ofSeconds(7)).claimLease())
        .isEqualTo(Duration.ofSeconds(7));
    assertThat(new PostgresOutboxStore(recorder, Duration.ofSeconds(7)).orderingMode())
        .isEqualTo(mode);
    assertThat(
            new PostgresOutboxStore(recorder, LEASE, OutboxOrderingMode.AVAILABILITY_FIRST)
                .orderingMode())
        .isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
    assertThat(new PostgresOutboxStore(recorder, Duration.ofNanos(1), mode).claimLease())
        .isEqualTo(Duration.ofNanos(1));
    assertThat(recorder.connectionsOpened()).isZero();
  }

  @Test
  void argumentMessages() throws SQLException {
    OutboxEntryId e = id("e");
    String c = strict.claimedBy();

    assertRefusedBeforeIo(() -> strict.save(null), "entry is required");
    assertRefusedBeforeIo(() -> strict.loadPending(0), "limit must be >= 1");
    assertRefusedBeforeIo(() -> strict.loadPending(-1), "limit must be >= 1");
    assertRefusedBeforeIo(() -> strict.markDelivered(null, c), "id is required");
    assertRefusedBeforeIo(() -> strict.markFailed(null, 1, "x", c), "id is required");
    assertRefusedBeforeIo(() -> strict.markRetry(null, 1, "x", Duration.ZERO, c), "id is required");
    assertRefusedBeforeIo(() -> strict.markRetry(null, 1, "x", null, c), "id is required");
    assertRefusedBeforeIo(() -> strict.markRetry(e, 1, "x", null, c), "backoff is required");
    assertRefusedBeforeIo(() -> strict.delete(null), "id is required");
    assertRefusedBeforeIo(() -> strict.deleteSkipped(null), "cutoff is required");
    assertRefusedBeforeIo(() -> strict.findByStatus(null, 1), "status is required");
    assertRefusedBeforeIo(() -> strict.findByStatus(null, 0), "status is required");
    assertRefusedBeforeIo(() -> strict.findByStatus(OutboxStatus.PENDING, 0), "limit must be >= 1");
    assertRefusedBeforeIo(() -> strict.findById(null), "id is required");
    assertRefusedBeforeIo(() -> strict.countByStatus(null), "status is required");
    assertRefusedBeforeIo(() -> strict.resetFailedToPending(null), "id is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(null, "op", "r"), "id is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(null, null, null), "id is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(e, null, "r"), "skippedBy is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(e, null, null), "skippedBy is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(e, "op", null), "reason is required");
    assertRefusedBeforeIo(
        () -> strict.skipFailed(e, "o".repeat(256), "r"),
        "skippedBy is 256 characters; at most 255");

    // The set-based marks: an empty set is 0 with no I/O; a null set is a NullPointerException.
    recorder.clear();
    assertThat(strict.markDeliveredAll(List.of(), c)).isZero();
    assertThat(strict.releaseClaims(List.of(), c)).isZero();
    assertThat(catchThrowable(() -> strict.markDeliveredAll(null, c)))
        .isExactlyInstanceOf(NullPointerException.class);
    assertThat(catchThrowable(() -> strict.releaseClaims(null, c)))
        .isExactlyInstanceOf(NullPointerException.class);
    assertThat(recorder.connectionsOpened()).isZero();

    // saveAll: its own arguments, then an empty list, before anything is prepared.
    try (Connection conn = recorder.getConnection()) {
      Throwable entries = catchThrowable(() -> strict.saveAll(null, conn));
      assertThat(entries)
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("entries is required");
      Throwable noConn = catchThrowable(() -> strict.saveAll(List.of(), null));
      assertThat(noConn)
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("conn is required");
      strict.saveAll(List.of(), conn);
    }
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
  }

  /**
   * deleteDelivered has no argument check: a null cutoff is a bare NullPointerException from the
   * binding, after the connection is open and the statement prepared, and not wrapped.
   */
  @Test
  void deleteDeliveredWithANullCutoff_isABareNullPointerException() {
    Throwable thrown = catchThrowable(() -> strict.deleteDelivered(null));

    assertThat(thrown).isExactlyInstanceOf(NullPointerException.class).hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly(single(DELETE_DELIVERED_BATCH));
    assertThat(recorder.openConnections()).isZero();
  }

  private void assertRefusedBeforeIo(ThrowingCallable call, String message) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(message)
        .hasNoCause();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  /** A read is one connection and one statement, with no commit over either pool. */
  @Test
  void transcript_findByStatus() {
    availabilityFirst.save(entry("e1", null));
    recorder.clear();

    assertThat(availabilityFirst.findByStatus(OutboxStatus.PENDING, 10)).hasSize(1);
    assertThat(recorder.transcript()).containsExactly(single(FIND_BY_STATUS));

    recorder.clear();
    assertThat(availabilityFirst.findByStatus(OutboxStatus.FAILED, 10)).isEmpty();
    assertThat(recorder.transcript()).containsExactly(single(FIND_BY_STATUS));

    useManualCommitPool();
    assertThat(availabilityFirst.findByStatus(OutboxStatus.PENDING, 1)).hasSize(1);
    assertThat(recorder.transcript()).containsExactly(single(FIND_BY_STATUS));
  }

  @Test
  void transcript_countByStatus() {
    availabilityFirst.save(entry("e1", null));
    recorder.clear();

    assertThat(availabilityFirst.countByStatus(OutboxStatus.PENDING)).isEqualTo(1L);
    assertThat(recorder.transcript()).containsExactly(single(COUNT_BY_STATUS));

    recorder.clear();
    assertThat(availabilityFirst.countByStatus(OutboxStatus.SKIPPED)).isZero();
    assertThat(recorder.transcript()).containsExactly(single(COUNT_BY_STATUS));

    useManualCommitPool();
    assertThat(availabilityFirst.countByStatus(OutboxStatus.PENDING)).isEqualTo(1L);
    assertThat(recorder.transcript()).containsExactly(single(COUNT_BY_STATUS));
  }

  @Test
  void transcript_otherReads() throws SQLException {
    failed("f1", stream("a"));
    failed("f2", stream("a"));
    failed("f3", null);
    execute(
        "UPDATE outbox_events SET processed_at = TIMESTAMPTZ '2026-11-27 05:00:00.000007+00'"
            + " WHERE entry_id = 'f2'");
    recorder.clear();

    assertThat(availabilityFirst.findById(id("f1"))).isPresent();
    assertThat(recorder.transcript()).containsExactly(single(FIND_BY_ID));

    recorder.clear();
    assertThat(availabilityFirst.sampleBlockage())
        .isEqualTo(new BlockageSample(1L, Instant.parse("2026-11-27T05:00:00.000007Z"), 1L));
    assertThat(recorder.transcript()).containsExactly(single(SAMPLE_BLOCKAGE));

    execute("DELETE FROM outbox_events");
    assertThat(availabilityFirst.sampleBlockage()).isEqualTo(new BlockageSample(0L, null, 0L));
  }

  /** Over an autoCommit=true pool every write is one connection and its statements, no commit. */
  @Test
  void transcript_writes() {
    String c = availabilityFirst.claimedBy();

    availabilityFirst.save(entry("e1", stream("a")));
    assertThat(recorder.transcript()).containsExactly(single(INSERT));

    recorder.clear();
    assertThat(availabilityFirst.loadPending(10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + RECLAIM_EXPIRED,
            "#1 prepare " + CLAIM_AVAILABILITY_FIRST,
            "#1 close");

    recorder.clear();
    assertThat(strict.loadPending(10)).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + RECLAIM_EXPIRED, "#1 prepare " + CLAIM_STRICT, "#1 close");

    recorder.clear();
    assertThat(availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(MARK_RETRY));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markFailed(id("e1"), 2, "x", c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(MARK_FAILED));

    recorder.clear();
    assertThat(availabilityFirst.resetFailedToPending(id("e1"))).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(RESET_FAILED_TO_PENDING));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markDelivered(id("e1"), c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(MARK_DELIVERED));

    failed("e2", stream("b"));
    recorder.clear();
    assertThat(availabilityFirst.skipFailed(id("e2"), "operator", "obsolete")).isTrue();
    assertThat(recorder.transcript()).containsExactly(single(SKIP_FAILED));

    // Compare-and-set misses are a false result from the same one-statement operation.
    recorder.clear();
    assertThat(availabilityFirst.markDelivered(id("e1"), c)).isFalse();
    assertThat(availabilityFirst.markFailed(id("e1"), 1, "x", c)).isFalse();
    assertThat(availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c)).isFalse();
    assertThat(availabilityFirst.skipFailed(id("e2"), "operator", "again")).isFalse();
    assertThat(availabilityFirst.resetFailedToPending(id("absent"))).isFalse();
    assertThat(recorder.connectionsOpened()).isEqualTo(5);
    assertThat(recorder.openConnections()).isZero();
  }

  /** Over an autoCommit=false pool every write commits once, after its statements. */
  @Test
  void transcript_writesOnAManualCommitPool() {
    useManualCommitPool();
    String c = availabilityFirst.claimedBy();

    availabilityFirst.save(entry("e1", stream("a")));
    assertThat(recorder.transcript()).containsExactly(committed(INSERT));

    recorder.clear();
    assertThat(availabilityFirst.loadPending(10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly(committed(RECLAIM_EXPIRED, CLAIM_AVAILABILITY_FIRST));

    recorder.clear();
    assertThat(strict.loadPending(10)).isEmpty();
    assertThat(recorder.transcript()).containsExactly(committed(RECLAIM_EXPIRED, CLAIM_STRICT));

    recorder.clear();
    assertThat(availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(committed(MARK_RETRY));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markFailed(id("e1"), 2, "x", c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(committed(MARK_FAILED));

    recorder.clear();
    assertThat(availabilityFirst.resetFailedToPending(id("e1"))).isTrue();
    assertThat(recorder.transcript()).containsExactly(committed(RESET_FAILED_TO_PENDING));

    List<OutboxEntry> claimed = availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.releaseClaims(claimed, c)).isEqualTo(1);
    assertThat(recorder.transcript()).containsExactly(committed(RELEASE_CLAIMS));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markDelivered(id("e1"), c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(committed(MARK_DELIVERED));

    availabilityFirst.save(entry("e3", null));
    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markDeliveredAll(List.of(id("e3")), c)).isEqualTo(1);
    assertThat(recorder.transcript()).containsExactly(committed(MARK_DELIVERED_ALL));

    failed("e2", stream("b"));
    recorder.clear();
    assertThat(availabilityFirst.skipFailed(id("e2"), "operator", "obsolete")).isTrue();
    assertThat(recorder.transcript()).containsExactly(committed(SKIP_FAILED));

    recorder.clear();
    availabilityFirst.delete(id("e1"));
    assertThat(recorder.transcript()).containsExactly(committed(DELETE_NON_FAILED));

    recorder.clear();
    assertThat(availabilityFirst.deleteDelivered(Instant.parse("2000-01-01T00:00:00Z"))).isZero();
    assertThat(recorder.transcript()).containsExactly(committed(DELETE_DELIVERED_BATCH));

    recorder.clear();
    assertThat(availabilityFirst.deleteSkipped(Instant.parse("2000-01-01T00:00:00Z"))).isZero();
    assertThat(recorder.transcript()).containsExactly(committed(DELETE_SKIPPED_BATCH));
  }

  /**
   * The set-based marks bind every id as one array in one statement; the count is the rows that
   * moved.
   */
  @Test
  void transcript_setBasedMarks() {
    String c = availabilityFirst.claimedBy();
    availabilityFirst.save(entry("e1", null));
    availabilityFirst.save(entry("e2", null));
    availabilityFirst.save(entry("e3", null));
    List<OutboxEntry> claimed = availabilityFirst.loadPending(10);
    List<OutboxEntryId> many = new ArrayList<>();
    for (int i = 0; i < 1_500; i++) {
      many.add(id("absent-" + i));
    }
    many.add(id("e1"));
    many.add(id("e2"));
    recorder.clear();

    assertThat(availabilityFirst.markDeliveredAll(many, "someone-else")).isZero();
    assertThat(availabilityFirst.markDeliveredAll(many, c)).isEqualTo(2);

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + MARK_DELIVERED_ALL,
            "#1 close",
            "open #2",
            "#2 prepare " + MARK_DELIVERED_ALL,
            "#2 close");

    recorder.clear();
    assertThat(availabilityFirst.releaseClaims(claimed, c)).isEqualTo(1);
    assertThat(recorder.transcript()).containsExactly(single(RELEASE_CLAIMS));
    assertThat(availabilityFirst.findById(id("e3")).orElseThrow().status())
        .isEqualTo(OutboxStatus.PENDING);
    assertThat(availabilityFirst.findById(id("e3")).orElseThrow().attempts()).isZero();
  }

  /** Each retention chunk (at most 1000 rows) is a connection of its own. */
  @Test
  void transcript_deleteDelivered() throws SQLException {
    insertRows("DELIVERED", "processed_at", 1001, "2026-11-27 09:00:00.65432+00");
    insertRows("DELIVERED", "processed_at", 1, "2026-11-27 09:00:00.654321+00");
    recorder.clear();

    assertThat(availabilityFirst.deleteDelivered(FIXED)).isEqualTo(1001);

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + DELETE_DELIVERED_BATCH,
            "#1 close",
            "open #2",
            "#2 prepare " + DELETE_DELIVERED_BATCH,
            "#2 close",
            "open #3",
            "#3 prepare " + DELETE_DELIVERED_BATCH,
            "#3 close");
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);

    recorder.clear();
    assertThat(availabilityFirst.deleteDelivered(FIXED)).isZero();
    assertThat(recorder.transcript()).containsExactly(single(DELETE_DELIVERED_BATCH));
  }

  @Test
  void transcript_deleteSkipped() throws SQLException {
    insertRows("SKIPPED", "skipped_at", 1001, "2026-11-27 09:00:00.65432+00");
    insertRows("SKIPPED", "skipped_at", 1, "2026-11-27 09:00:00.654321+00");
    recorder.clear();

    assertThat(availabilityFirst.deleteSkipped(FIXED)).isEqualTo(1001);

    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + DELETE_SKIPPED_BATCH,
            "#1 close",
            "open #2",
            "#2 prepare " + DELETE_SKIPPED_BATCH,
            "#2 close",
            "open #3",
            "#3 prepare " + DELETE_SKIPPED_BATCH,
            "#3 close");
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);

    recorder.clear();
    assertThat(availabilityFirst.deleteSkipped(FIXED)).isZero();
    assertThat(recorder.transcript()).containsExactly(single(DELETE_SKIPPED_BATCH));
  }

  /**
   * saveAll joins the caller's connection: one prepared insert, no commit or close; the trace link
   * is bound as save binds it.
   */
  @Test
  void transcript_saveAll() throws SQLException {
    try (Connection conn = recorder.getConnection()) {
      strict.saveAll(
          List.of(
              entry("s1", stream("a")).withTraceLink(SpanLink.ofW3c(TRACE, SPAN)),
              entry("s2", stream("b"))
                  .withTraceLink(new SpanLink(new TraceId("corr-1"), new SpanId("s-1")))),
          conn);
      assertThat(recorder.transcript()).containsExactly("open #1", "#1 prepare " + INSERT);
    }
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(2L);
    assertThat(
            stringValue(
                "SELECT trace_id || '/' || span_id FROM outbox_events WHERE entry_id = 's1'"))
        .isEqualTo(TRACE + "/" + SPAN);
    assertThat(stringValue("SELECT trace_id FROM outbox_events WHERE entry_id = 's2'")).isNull();
    assertThat(stringValue("SELECT span_id FROM outbox_events WHERE entry_id = 's2'")).isNull();

    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        INSERT,
        c -> {
          throw injected;
        });
    try (Connection conn = recorder.getConnection()) {
      Throwable thrown =
          catchThrowable(
              () ->
                  strict.saveAll(
                      List.of(entry("s3", stream("a")), entry("s4", stream("b"))), conn));
      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save 2 outbox entries");
      assertThat(thrown.getCause()).isSameAs(injected);
    }

    RuntimeException runtime = new RuntimeException("injected runtime");
    recorder.beforePrepare(
        INSERT,
        c -> {
          throw runtime;
        });
    try (Connection conn = recorder.getConnection()) {
      Throwable thrown =
          catchThrowable(() -> strict.saveAll(List.of(entry("s5", stream("a"))), conn));
      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save 1 outbox entries");
      assertThat(thrown.getCause()).isSameAs(runtime);
    }
  }

  /** One single-statement operation on connection #1 with no commit. */
  private static String[] single(String sql) {
    return new String[] {"open #1", "#1 prepare " + sql, "#1 close"};
  }

  /** One operation on connection #1 preparing {@code statements} in order, then committing. */
  private static String[] committed(String... statements) {
    List<String> events = new ArrayList<>();
    events.add("open #1");
    for (String statement : statements) {
      events.add("#1 prepare " + statement);
    }
    events.add("#1 commit");
    events.add("#1 close");
    return events.toArray(String[]::new);
  }

  // ── Failures ────────────────────────────────────────────────────────────────────────────────

  @Test
  void sqlFailureMessages() {
    String c = availabilityFirst.claimedBy();
    OutboxEntryId e = id("e\n1");
    List<OutboxEntry> one = List.of(entry("e1", null));

    assertFails(
        INSERT,
        () -> availabilityFirst.save(entry("e\n1", null)),
        "Failed to save outbox entry: e1");
    assertFails(
        RECLAIM_EXPIRED,
        () -> availabilityFirst.loadPending(1),
        "Failed to load pending outbox entries");
    assertFails(
        CLAIM_PREFIX,
        () -> availabilityFirst.loadPending(1),
        "Failed to load pending outbox entries",
        RECLAIM_EXPIRED);
    assertFails(
        MARK_DELIVERED,
        () -> availabilityFirst.markDelivered(e, c),
        "Failed to mark outbox entry as delivered: e1");
    assertFails(
        MARK_FAILED,
        () -> availabilityFirst.markFailed(e, 1, "x", c),
        "Failed to mark outbox entry as failed: e1");
    assertFails(
        MARK_RETRY,
        () -> availabilityFirst.markRetry(e, 1, "x", Duration.ZERO, c),
        "Failed to mark outbox entry for retry: e1");
    assertFails(
        MARK_DELIVERED_ALL,
        () -> availabilityFirst.markDeliveredAll(List.of(e), c),
        "Failed to mark outbox entries delivered");
    assertFails(
        RELEASE_CLAIMS,
        () -> availabilityFirst.releaseClaims(one, c),
        "Failed to release unattempted outbox claims");
    assertFails(
        DELETE_NON_FAILED, () -> availabilityFirst.delete(e), "Failed to delete outbox entry: e1");
    assertFails(
        EXISTS,
        () -> availabilityFirst.delete(e),
        "Failed to delete outbox entry: e1",
        DELETE_NON_FAILED);
    assertFails(
        DELETE_DELIVERED_BATCH,
        () -> availabilityFirst.deleteDelivered(FIXED),
        "Failed to delete delivered outbox entries");
    assertFails(
        DELETE_SKIPPED_BATCH,
        () -> availabilityFirst.deleteSkipped(FIXED),
        "Failed to delete skipped outbox entries");
    assertFails(
        RESET_FAILED_TO_PENDING,
        () -> availabilityFirst.resetFailedToPending(e),
        "Failed to reset failed outbox entry to pending: e1");
    assertFails(
        SKIP_FAILED,
        () -> availabilityFirst.skipFailed(e, "op", "r"),
        "Failed to skip failed outbox entry: e1");
    assertFails(
        FIND_BY_STATUS,
        () -> availabilityFirst.findByStatus(OutboxStatus.FAILED, 1),
        "Failed to find outbox entries by status: FAILED");
    assertFails(FIND_BY_ID, () -> availabilityFirst.findById(e), "Failed to find outbox entry: e1");
    assertFails(
        COUNT_BY_STATUS,
        () -> availabilityFirst.countByStatus(OutboxStatus.DELIVERED),
        "Failed to count outbox entries by status: DELIVERED");
    assertFails(
        SAMPLE_BLOCKAGE,
        () -> availabilityFirst.sampleBlockage(),
        "Failed to sample outbox blockage");
  }

  /** A SQLException while preparing {@code prefix} is wrapped with {@code message}. */
  private void assertFails(
      String prefix, ThrowingCallable call, String message, String... preparedBefore) {
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        prefix,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    List<String> expected = new ArrayList<>(List.of("open #1"));
    for (String statement : preparedBefore) {
      expected.add("#1 prepare " + statement);
    }
    expected.add("#1 close");
    assertThat(recorder.transcript()).containsExactlyElementsOf(expected);
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * Which failures are wrapped: a RuntimeException is wrapped as a storage failure by every
   * operation except the set-based marks and the retention sweeps, which catch only SQLException
   * and let it through unwrapped. delete passes every IllegalStateException through unwrapped, the
   * way it passes its own refusal.
   */
  @Test
  void runtimeFailures_wrappedOrNot() {
    String c = availabilityFirst.claimedBy();
    OutboxEntryId e = id("e1");
    List<OutboxEntry> one = List.of(entry("e1", null));

    assertRuntimeWrapped(
        INSERT, () -> availabilityFirst.save(entry("e1", null)), "Failed to save outbox entry: e1");
    assertRuntimeWrapped(
        RECLAIM_EXPIRED,
        () -> availabilityFirst.loadPending(1),
        "Failed to load pending outbox entries");
    assertRuntimeWrapped(
        MARK_DELIVERED,
        () -> availabilityFirst.markDelivered(e, c),
        "Failed to mark outbox entry as delivered: e1");
    assertRuntimeWrapped(
        MARK_FAILED,
        () -> availabilityFirst.markFailed(e, 1, "x", c),
        "Failed to mark outbox entry as failed: e1");
    assertRuntimeWrapped(
        MARK_RETRY,
        () -> availabilityFirst.markRetry(e, 1, "x", Duration.ZERO, c),
        "Failed to mark outbox entry for retry: e1");
    assertRuntimeWrapped(
        DELETE_NON_FAILED, () -> availabilityFirst.delete(e), "Failed to delete outbox entry: e1");
    assertRuntimeWrapped(
        RESET_FAILED_TO_PENDING,
        () -> availabilityFirst.resetFailedToPending(e),
        "Failed to reset failed outbox entry to pending: e1");
    assertRuntimeWrapped(
        SKIP_FAILED,
        () -> availabilityFirst.skipFailed(e, "op", "r"),
        "Failed to skip failed outbox entry: e1");
    assertRuntimeWrapped(
        FIND_BY_STATUS,
        () -> availabilityFirst.findByStatus(OutboxStatus.FAILED, 1),
        "Failed to find outbox entries by status: FAILED");
    assertRuntimeWrapped(
        FIND_BY_ID, () -> availabilityFirst.findById(e), "Failed to find outbox entry: e1");
    assertRuntimeWrapped(
        COUNT_BY_STATUS,
        () -> availabilityFirst.countByStatus(OutboxStatus.DELIVERED),
        "Failed to count outbox entries by status: DELIVERED");
    assertRuntimeWrapped(
        SAMPLE_BLOCKAGE,
        () -> availabilityFirst.sampleBlockage(),
        "Failed to sample outbox blockage");

    // delete lets any IllegalStateException through unwrapped, not only its own refusal.
    IllegalStateException state = new IllegalStateException("injected state");
    recorder.beforePrepare(
        EXISTS,
        conn -> {
          throw state;
        });
    recorder.clear();
    assertThat(catchThrowable(() -> availabilityFirst.delete(id("absent")))).isSameAs(state);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + DELETE_NON_FAILED, "#1 close");

    assertRuntimeUnwrapped(
        MARK_DELIVERED_ALL, () -> availabilityFirst.markDeliveredAll(List.of(e), c));
    assertRuntimeUnwrapped(RELEASE_CLAIMS, () -> availabilityFirst.releaseClaims(one, c));
    assertRuntimeUnwrapped(DELETE_DELIVERED_BATCH, () -> availabilityFirst.deleteDelivered(FIXED));
    assertRuntimeUnwrapped(DELETE_SKIPPED_BATCH, () -> availabilityFirst.deleteSkipped(FIXED));
  }

  private void assertRuntimeWrapped(String prefix, ThrowingCallable call, String message) {
    RuntimeException injected = new RuntimeException("injected runtime");
    recorder.beforePrepare(
        prefix,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
  }

  private void assertRuntimeUnwrapped(String prefix, ThrowingCallable call) {
    RuntimeException injected = new RuntimeException("injected runtime");
    recorder.beforePrepare(
        prefix,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
  }

  /**
   * A pool that cannot hand out a connection: a SQLException is wrapped by every operation; a
   * RuntimeException is wrapped by every operation except the set-based marks and the retention
   * sweeps.
   */
  @Test
  void connectionFailures_wrappedOrNot() {
    SQLException refused = new SQLException("no connection", "08001");
    PostgresOutboxStore down = new PostgresOutboxStore(failingPool(refused), LEASE);
    String c = down.claimedBy();
    OutboxEntryId e = id("e1");
    List<OutboxEntry> one = List.of(entry("e1", stream("a")));

    assertWrapped(refused, () -> down.save(one.getFirst()), "Failed to save outbox entry: e1");
    assertWrapped(refused, () -> down.loadPending(1), "Failed to load pending outbox entries");
    assertWrapped(
        refused, () -> down.markDelivered(e, c), "Failed to mark outbox entry as delivered: e1");
    assertWrapped(
        refused, () -> down.markFailed(e, 1, "x", c), "Failed to mark outbox entry as failed: e1");
    assertWrapped(
        refused,
        () -> down.markRetry(e, 1, "x", Duration.ZERO, c),
        "Failed to mark outbox entry for retry: e1");
    assertWrapped(
        refused,
        () -> down.markDeliveredAll(List.of(e), c),
        "Failed to mark outbox entries delivered");
    assertWrapped(
        refused, () -> down.releaseClaims(one, c), "Failed to release unattempted outbox claims");
    assertWrapped(refused, () -> down.delete(e), "Failed to delete outbox entry: e1");
    assertWrapped(
        refused, () -> down.deleteDelivered(FIXED), "Failed to delete delivered outbox entries");
    assertWrapped(
        refused, () -> down.deleteSkipped(FIXED), "Failed to delete skipped outbox entries");
    assertWrapped(
        refused,
        () -> down.resetFailedToPending(e),
        "Failed to reset failed outbox entry to pending: e1");
    assertWrapped(
        refused, () -> down.skipFailed(e, "op", "r"), "Failed to skip failed outbox entry: e1");
    assertWrapped(
        refused,
        () -> down.findByStatus(OutboxStatus.PENDING, 1),
        "Failed to find outbox entries by status: PENDING");
    assertWrapped(refused, () -> down.findById(e), "Failed to find outbox entry: e1");
    assertWrapped(
        refused,
        () -> down.countByStatus(OutboxStatus.PENDING),
        "Failed to count outbox entries by status: PENDING");
    assertWrapped(refused, down::sampleBlockage, "Failed to sample outbox blockage");

    RuntimeException broken = new RuntimeException("pool closed");
    PostgresOutboxStore closed = new PostgresOutboxStore(failingPool(broken), LEASE);
    assertWrapped(broken, () -> closed.save(one.getFirst()), "Failed to save outbox entry: e1");
    assertWrapped(broken, () -> closed.loadPending(1), "Failed to load pending outbox entries");
    assertWrapped(
        broken,
        () -> closed.findByStatus(OutboxStatus.PENDING, 1),
        "Failed to find outbox entries by status: PENDING");
    assertWrapped(
        broken,
        () -> closed.countByStatus(OutboxStatus.PENDING),
        "Failed to count outbox entries by status: PENDING");
    assertWrapped(broken, () -> closed.delete(e), "Failed to delete outbox entry: e1");
    assertWrapped(broken, closed::sampleBlockage, "Failed to sample outbox blockage");
    assertThat(catchThrowable(() -> closed.markDeliveredAll(List.of(e), c))).isSameAs(broken);
    IllegalStateException state = new IllegalStateException("pool shut down");
    PostgresOutboxStore shutDown = new PostgresOutboxStore(failingPool(state), LEASE);
    assertThat(catchThrowable(() -> shutDown.delete(e))).isSameAs(state);
    assertWrapped(state, () -> shutDown.save(one.getFirst()), "Failed to save outbox entry: e1");
    assertThat(catchThrowable(() -> closed.releaseClaims(one, c))).isSameAs(broken);
    assertThat(catchThrowable(() -> closed.deleteDelivered(FIXED))).isSameAs(broken);
    assertThat(catchThrowable(() -> closed.deleteSkipped(FIXED))).isSameAs(broken);
  }

  private static void assertWrapped(Throwable cause, ThrowingCallable call, String message) {
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(cause);
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  /** A data source whose {@code getConnection} throws {@code failure}. */
  private static DataSource failingPool(Throwable failure) {
    return (DataSource)
        Proxy.newProxyInstance(
            PostgresOutboxStoreCharacterizationTest.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw failure;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  /**
   * A statement that fails on the database inside an autoCommit=false write is rolled back before
   * the connection closes; the cause is the driver's exception, here a read-only transaction.
   */
  @Test
  void databaseFailureOnAManualCommitPool_rollsBack() {
    useManualCommitPool();
    String c = availabilityFirst.claimedBy();

    readOnlyTransaction(MARK_DELIVERED);
    Throwable mark = catchThrowable(() -> availabilityFirst.markDelivered(id("e1"), c));
    assertThat(mark)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to mark outbox entry as delivered: e1");
    assertReadOnlyCause(mark);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + MARK_DELIVERED, "#1 rollback", "#1 close");

    recorder.clear();
    readOnlyTransaction(MARK_DELIVERED_ALL);
    Throwable all = catchThrowable(() -> availabilityFirst.markDeliveredAll(List.of(id("e1")), c));
    assertThat(all)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to mark outbox entries delivered");
    assertReadOnlyCause(all);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + MARK_DELIVERED_ALL, "#1 rollback", "#1 close");

    recorder.clear();
    readOnlyTransaction(RECLAIM_EXPIRED);
    Throwable claim = catchThrowable(() -> availabilityFirst.loadPending(1));
    assertThat(claim)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to load pending outbox entries");
    assertReadOnlyCause(claim);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + RECLAIM_EXPIRED, "#1 rollback", "#1 close");

    // A failure while preparing the claim, inside the claim's transaction, rolls back too.
    recorder.clear();
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        CLAIM_PREFIX,
        conn -> {
          throw injected;
        });
    Throwable prepare = catchThrowable(() -> availabilityFirst.loadPending(1));
    assertThat(prepare).isExactlyInstanceOf(EventStoreException.class);
    assertThat(prepare.getCause()).isSameAs(injected);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + RECLAIM_EXPIRED, "#1 rollback", "#1 close");

    // A failure while preparing a single-statement write comes before its transaction: no
    // rollback.
    recorder.clear();
    recorder.beforePrepare(
        INSERT,
        conn -> {
          throw injected;
        });
    Throwable save = catchThrowable(() -> availabilityFirst.save(entry("e1", null)));
    assertThat(save).isExactlyInstanceOf(EventStoreException.class);
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
  }

  /** Makes the transaction of the store's next statement starting with {@code sql} read-only. */
  private void readOnlyTransaction(String sql) {
    recorder.beforePrepare(
        sql,
        sameConnection -> {
          try (Statement st = sameConnection.createStatement()) {
            st.execute("SET TRANSACTION READ ONLY");
          }
        });
  }

  private static void assertReadOnlyCause(Throwable thrown) {
    assertThat(thrown.getCause()).isExactlyInstanceOf(PSQLException.class);
    assertThat(((PSQLException) thrown.getCause()).getSQLState()).isEqualTo("25006");
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  /** A write whose connection fails to close after its commit is a storage failure. */
  @Test
  void closeFailureAfterCommit_isAStorageFailureOfAWrittenRow() {
    useManualCommitPool();
    recorder.failNextCloseAfterCommit();

    Throwable thrown = catchThrowable(() -> availabilityFirst.save(entry("e1", null)));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save outbox entry: e1");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(recorder.transcript()).containsExactly(committed(INSERT));
    assertThat(availabilityFirst.findById(id("e1"))).isPresent();

    availabilityFirst.loadPending(10);
    recorder.clear();
    recorder.failNextCloseAfterCommit();
    Throwable all =
        catchThrowable(
            () ->
                availabilityFirst.markDeliveredAll(
                    List.of(id("e1")), availabilityFirst.claimedBy()));
    assertThat(all)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to mark outbox entries delivered");
    assertThat(all.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(availabilityFirst.findById(id("e1")).orElseThrow().status())
        .isEqualTo(OutboxStatus.DELIVERED);
  }

  // ── The benign in-flight unique violation ───────────────────────────────────────────────────

  /**
   * A claim that fails with a {@code 23505} naming the one-inflight-per-stream index, anywhere in
   * the cause chain, returns no entries and logs one DEBUG line on the store's category, carrying
   * the exception. Any other {@code 23505}, and a benign one wrapped in a RuntimeException, is a
   * storage failure with no log line.
   */
  @Test
  void benignInflightUniqueViolation_isAnEmptyClaimAndOneDebugLine() {
    try (LogCapture logs = LogCapture.attach(PostgresOutboxStore.class)) {
      SQLException benign =
          new SQLException(
              "duplicate key value violates unique constraint"
                  + " \"ux_outbox_one_inflight_per_stream\"",
              "23505");
      injectAtClaim(benign);
      assertThat(strict.loadPending(10)).isEmpty();
      assertThat(recorder.transcript())
          .containsExactly("open #1", "#1 prepare " + RECLAIM_EXPIRED, "#1 close");
      assertOneInflightDebug(logs, benign);

      SQLException wrapped =
          new SQLException(
              "outer", "XX000", new SQLException("ux_outbox_one_inflight_per_stream", "23505"));
      injectAtClaim(wrapped);
      assertThat(availabilityFirst.loadPending(10)).isEmpty();
      assertThat(logs.events(Level.DEBUG)).hasSize(2);
      assertThat(logs.events(Level.DEBUG).get(1).getThrowableProxy())
          .isInstanceOfSatisfying(
              ThrowableProxy.class, p -> assertThat(p.getThrowable()).isSameAs(wrapped));

      SQLException otherIndex =
          new SQLException(
              "duplicate key value violates unique constraint \"outbox_events_pkey\"", "23505");
      injectAtClaim(otherIndex);
      Throwable other = catchThrowable(() -> strict.loadPending(10));
      assertThat(other)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to load pending outbox entries");
      assertThat(other.getCause()).isSameAs(otherIndex);

      SQLException wrongState = new SQLException("ux_outbox_one_inflight_per_stream", "23503");
      injectAtClaim(wrongState);
      assertThat(catchThrowable(() -> strict.loadPending(10)).getCause()).isSameAs(wrongState);

      SQLException noMessage = new SQLException(null, "23505");
      injectAtClaim(noMessage);
      assertThat(catchThrowable(() -> strict.loadPending(10)).getCause()).isSameAs(noMessage);

      RuntimeException runtime = new IllegalStateException("wrapping", benign);
      injectAtClaim(runtime);
      Throwable wrappedInRuntime = catchThrowable(() -> strict.loadPending(10));
      assertThat(wrappedInRuntime)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to load pending outbox entries");
      assertThat(wrappedInRuntime.getCause()).isSameAs(runtime);

      assertThat(logs.events(Level.DEBUG)).hasSize(2);
      for (Level level : List.of(Level.TRACE, Level.INFO, Level.WARN, Level.ERROR)) {
        assertThat(logs.events(level)).as("events at %s", level).isEmpty();
      }
    }
  }

  /**
   * The real race: a head reset to PENDING while another transaction holds its stream's successor
   * IN_PROGRESS, uncommitted. The claim's write waits on the unique index, and once the other
   * transaction commits the database refuses it with {@code 23505} on that index: no entries, one
   * DEBUG line.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void benignInflightUniqueViolation_fromTheResetRace() throws Exception {
    StreamId agg = stream("race");
    availabilityFirst.save(entry("seq5", agg));
    availabilityFirst.save(entry("seq6", agg));
    assertThat(ids(availabilityFirst.loadPending(10))).containsExactly("seq5");
    assertThat(availabilityFirst.markFailed(id("seq5"), 3, "boom", availabilityFirst.claimedBy()))
        .isTrue();

    try (LogCapture logs = LogCapture.attach(PostgresOutboxStore.class);
        Connection held = dataSource.getConnection()) {
      held.setAutoCommit(false);
      try (Statement st = held.createStatement()) {
        assertThat(
                st.executeUpdate(
                    "UPDATE outbox_events SET status = 'IN_PROGRESS', claimed_at = NOW(),"
                        + " claimed_by = 'relay-1' WHERE entry_id = 'seq6'"))
            .isEqualTo(1);
      }
      assertThat(availabilityFirst.resetFailedToPending(id("seq5"))).isTrue();
      recorder.clear();

      ExecutorService pool = Executors.newSingleThreadExecutor();
      try {
        Future<List<OutboxEntry>> claim = pool.submit(() -> strict.loadPending(10));
        awaitClaimBlockedOnALock();
        held.commit();
        assertThat(claim.get(30, TimeUnit.SECONDS)).isEmpty();
      } finally {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }

      assertThat(recorder.transcript())
          .containsExactly(
              "open #1", "#1 prepare " + RECLAIM_EXPIRED, "#1 prepare " + CLAIM_STRICT, "#1 close");
      List<ILoggingEvent> debug = logs.events(Level.DEBUG);
      assertThat(debug).hasSize(1);
      assertThat(debug.getFirst().getLoggerName())
          .isEqualTo("org.streamrune.postgres.PostgresOutboxStore");
      assertThat(debug.getFirst().getFormattedMessage()).isEqualTo(INFLIGHT_DEBUG);
      Throwable logged = ((ThrowableProxy) debug.getFirst().getThrowableProxy()).getThrowable();
      assertThat(logged).isExactlyInstanceOf(PSQLException.class);
      PSQLException psql = (PSQLException) logged;
      assertThat(psql.getSQLState()).isEqualTo("23505");
      assertThat(psql.getServerErrorMessage().getConstraint())
          .isEqualTo("ux_outbox_one_inflight_per_stream");
      for (Level level : List.of(Level.TRACE, Level.INFO, Level.WARN, Level.ERROR)) {
        assertThat(logs.events(level)).as("events at %s", level).isEmpty();
      }
    }
    assertThat(stringValue("SELECT status FROM outbox_events WHERE entry_id = 'seq5'"))
        .isEqualTo("PENDING");
    assertThat(stringValue("SELECT status FROM outbox_events WHERE entry_id = 'seq6'"))
        .isEqualTo("IN_PROGRESS");
  }

  private void injectAtClaim(Throwable failure) {
    recorder.beforePrepare(
        CLAIM_PREFIX,
        c -> {
          if (failure instanceof SQLException sql) {
            throw sql;
          }
          throw (RuntimeException) failure;
        });
    recorder.clear();
  }

  private static void assertOneInflightDebug(LogCapture logs, SQLException expected) {
    List<ILoggingEvent> debug = logs.events(Level.DEBUG);
    assertThat(debug).hasSize(1);
    assertThat(debug.getFirst().getLoggerName())
        .isEqualTo("org.streamrune.postgres.PostgresOutboxStore");
    assertThat(debug.getFirst().getFormattedMessage()).isEqualTo(INFLIGHT_DEBUG);
    assertThat(debug.getFirst().getArgumentArray()).hasSize(1);
    assertThat(((ThrowableProxy) debug.getFirst().getThrowableProxy()).getThrowable())
        .isSameAs(expected);
  }

  /** Waits until a session runs the claim statement and waits on a lock. */
  private static void awaitClaimBlockedOnALock() throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (longValue(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
                + " AND query LIKE 'WITH null_agg AS (%'")
        == 0) {
      assertThat(System.nanoTime()).as("claim blocked on a lock").isLessThan(deadline);
      Thread.onSpinWait();
    }
  }

  // ── Quiet ───────────────────────────────────────────────────────────────────────────────────

  /**
   * Outside the benign in-flight violation the store logs nothing at any level: not on a claim, a
   * reclaim, a mark, a refusal, a retention sweep or a failed read.
   */
  @Test
  void theStoreLogsNothingElse() throws SQLException {
    try (LogCapture logs = LogCapture.attach(PostgresOutboxStore.class)) {
      String c = availabilityFirst.claimedBy();
      availabilityFirst.save(entry("e1", stream("a")));
      availabilityFirst.save(entry("e2", null));
      availabilityFirst.save(entry("e1", stream("a")));
      availabilityFirst.loadPending(10);
      availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c);
      availabilityFirst.loadPending(10);
      availabilityFirst.markFailed(id("e1"), 2, "x", c);
      availabilityFirst.markDeliveredAll(List.of(id("e2")), c);
      availabilityFirst.markDelivered(id("e2"), c);
      catchThrowable(() -> availabilityFirst.delete(id("e1")));
      catchThrowable(() -> strict.save(entry("n", null)));
      availabilityFirst.skipFailed(id("e1"), "operator", "obsolete");
      execute("UPDATE outbox_events SET skipped_at = NULL WHERE entry_id = 'e1'");
      catchThrowable(() -> availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10));
      availabilityFirst.save(entry("e3", stream("b")));
      availabilityFirst.loadPending(10);
      execute(
          "UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '1 hour' WHERE entry_id = 'e3'");
      availabilityFirst.loadPending(10);
      availabilityFirst.deleteDelivered(Instant.now().plusSeconds(60));
      execute("UPDATE outbox_events SET skipped_at = NOW() WHERE entry_id = 'e1'");
      availabilityFirst.deleteSkipped(Instant.now().plusSeconds(60));
      availabilityFirst.sampleBlockage();
      availabilityFirst.countByStatus(OutboxStatus.PENDING);
      injectAtClaim(new SQLException("injected"));
      catchThrowable(() -> availabilityFirst.loadPending(1));

      for (Level level : List.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR)) {
        assertThat(logs.events(level)).as("events at %s", level).isEmpty();
      }
    }
  }

  // ── raw SQL on an unrecorded connection ─────────────────────────────────────────────────────

  /**
   * Runs {@code hook}'s {@code SELECT now()} on the store's own connection right before its next
   * statement starting with {@code sql}; over an autoCommit=false pool that opens the write's
   * transaction, so {@code into} receives the transaction's {@code now()}.
   */
  private void captureTransactionNow(String sql, AtomicReference<Instant> into) {
    recorder.beforePrepare(
        sql,
        sameConnection -> {
          try (Statement st = sameConnection.createStatement();
              ResultSet rs = st.executeQuery("SELECT now()")) {
            assertThat(rs.next()).isTrue();
            into.set(rs.getTimestamp(1).toInstant());
          }
        });
  }

  /**
   * Makes the session of the store's next statement starting with {@code sql} resolve {@code now()}
   * to {@code fixed_clock.now()}, which answers {@link #FIXED}.
   */
  private void onTheFixedClock(String sql) {
    recorder.beforePrepare(
        sql,
        sameConnection -> {
          try (Statement st = sameConnection.createStatement()) {
            st.execute("SET search_path TO fixed_clock, pg_catalog, public");
          }
        });
  }

  /** {@code count} rows in {@code status} with {@code column} at the timestamp {@code at}. */
  private static void insertRows(String status, String column, int count, String at)
      throws SQLException {
    String skipColumns = status.equals("SKIPPED") ? ", skipped_by, skip_reason" : "";
    String skipValues = status.equals("SKIPPED") ? ", 'operator', 'obsolete'" : "";
    execute(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, status, "
            + column
            + skipColumns
            + ") SELECT '"
            + status
            + "-"
            + at
            + "-' || i, '{}', 'Event', '"
            + status
            + "', TIMESTAMPTZ '"
            + at
            + "'"
            + skipValues
            + " FROM generate_series(1, "
            + count
            + ") i");
  }

  private static void execute(String statement) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement()) {
      st.execute(statement);
    }
  }

  private static long longValue(String query) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getLong(1);
    }
  }

  private static String stringValue(String query) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getString(1);
    }
  }

  private static Instant instantValue(String query) throws SQLException {
    try (Connection c = dataSource.getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getTimestamp(1).toInstant();
    }
  }
}

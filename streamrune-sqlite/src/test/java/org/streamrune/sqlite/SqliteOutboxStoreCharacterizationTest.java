package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;
import javax.sql.DataSource;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStoreException;
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
import org.streamrune.test.MutableClock;

/**
 * Characterization of the SQLite outbox store: the statements and connections of every read and
 * write, the claim order of both ordering modes (sorted by {@code seq} in Java, because {@code
 * UPDATE … RETURNING} has no order), the JVM clock behind every timestamp and the lease, the row
 * mapping (a claimed row reports no {@code processedAt}; a skip audit without its instant is a
 * storage failure), the exact type, message and cause of every refusal and failure, and where the
 * delete refusal surfaces relative to the write transaction. The values are what the store does on
 * a real database file today, written down as they come out; these tests stay unchanged while the
 * store's validation and row mapping move into shared helpers.
 *
 * <p>Every store borrows from a {@link RecordingDataSource}: reads run in autocommit on one
 * connection, writes in one connection's {@code BEGIN IMMEDIATE} transaction committed with a
 * {@code COMMIT} statement. The delete refusal is thrown after that transaction committed, so it
 * keeps its own type.
 */
class SqliteOutboxStoreCharacterizationTest {

  private static final AggregateType AGG = AggregateType.of("agg");
  private static final Instant T0 = Instant.parse("2026-11-27T09:00:00.654321Z");
  private static final long T0_MICROS = 1_795_770_000_654_321L;
  private static final Duration LEASE = Duration.ofMinutes(4);
  private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String SPAN = "00f067aa0ba902b7";

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String INSERT =
      "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
          + " trace_id, span_id, status, attempts, last_error, created_at, processed_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, ?, NULL)"
          + " ON CONFLICT (entry_id) DO NOTHING";
  private static final String ENTRY_COLUMNS =
      "entry_id, payload, payload_type, aggregate_type, aggregate_id, status, attempts,"
          + " last_error, created_at, processed_at, next_retry_at, skipped_at, skipped_by,"
          + " skip_reason, trace_id, span_id, seq";
  private static final String CLAIM_HEAD =
      "WITH heads AS ("
          + "  SELECT min(seq) AS seq FROM outbox_events"
          + "  WHERE status IN ('PENDING', 'FAILED')";
  private static final String CLAIM_TAIL =
      " AND aggregate_id IS NOT NULL"
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
  private static final String CLAIM_STRICT = CLAIM_HEAD + CLAIM_TAIL;
  private static final String CLAIM_AVAILABILITY_FIRST =
      CLAIM_HEAD + " AND status = 'PENDING'" + CLAIM_TAIL;
  private static final String RECLAIM_EXPIRED =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL"
          + " WHERE status = 'IN_PROGRESS' AND claimed_at < ?";
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
  private static final String MARK_DELIVERED_ALL_HEAD =
      "UPDATE outbox_events SET status = 'DELIVERED', attempts = attempts + 1, processed_at = ?"
          + " WHERE entry_id IN (";
  private static final String RELEASE_CLAIMS_HEAD =
      "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL"
          + " WHERE entry_id IN (";
  private static final String ID_SET_TAIL = ") AND status = 'IN_PROGRESS' AND claimed_by = ?";
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
  private static final String SAMPLE_BLOCKAGE =
      "SELECT (SELECT count(*) FROM (SELECT DISTINCT aggregate_type, aggregate_id"
          + "   FROM outbox_events WHERE status = 'FAILED' AND aggregate_id IS NOT NULL)),"
          + " (SELECT min(processed_at) FROM outbox_events WHERE status = 'FAILED'),"
          + " (SELECT count(*) FROM outbox_events WHERE status = 'FAILED' AND aggregate_id IS NULL)";

  private static final String BUSY_ADVICE =
      " stored nothing. Most likely an application transaction on the same database file holds"
          + " the write lock: StreamRune's stores never join an application's transaction (for"
          + " example a Spring @Transactional method on the same DataSource), so call them outside"
          + " it. Otherwise another connection of this process held the lock that long: a schema"
          + " migration or a stalled write. Retry later, or raise busy_timeout.";

  @TempDir Path dir;

  private final List<AutoCloseable> resources = new ArrayList<>();
  private final MutableClock clock = MutableClock.startingAt(T0);

  private Path file;
  private RecordingDataSource recorder;
  private SqliteDatabase database;
  private SqliteOutboxStore strict;
  private SqliteOutboxStore availabilityFirst;

  @BeforeEach
  void openDatabase() {
    use("outbox.db", null, UnaryOperator.identity());
  }

  @AfterEach
  void closeDatabases() throws Exception {
    for (AutoCloseable resource : resources.reversed()) {
      resource.close();
    }
  }

  /**
   * Points the test at a fresh, migrated database file, through a new recorder (wrapped by {@code
   * decorate}) and two stores on the shared clock, one per ordering mode.
   */
  private void use(String name, String override, UnaryOperator<DataSource> decorate) {
    file = dir.resolve(name);
    recorder =
        new RecordingDataSource(
            override == null
                ? TestDatabases.dataSource(file)
                : TestDatabases.dataSource(file, override));
    database = SqliteDatabase.open(decorate.apply(recorder));
    resources.add(database);
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
    strict = store(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    availabilityFirst = store(OutboxOrderingMode.AVAILABILITY_FIRST);
    recorder.clear();
  }

  private SqliteOutboxStore store(OutboxOrderingMode mode) {
    return new SqliteOutboxStore(database, LEASE, mode, clock);
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
   * Twenty-four entries saved in a known seq order: e01, e05, … on stream a; e02, e06, … on b; e03,
   * e07, … on c; every fourth (e04, e08, …) without a stream. Then c's head e03 is FAILED.
   */
  private void interleavedRows() throws SQLException {
    String[] streams = {null, "a", "b", "c"};
    for (int i = 1; i <= 24; i++) {
      String s = streams[i % 4];
      availabilityFirst.save(entry("e%02d".formatted(i), s == null ? null : stream(s)));
    }
    sql("UPDATE outbox_events SET status = 'FAILED', processed_at = 1 WHERE entry_id = 'e03'");
  }

  @Test
  void claimOrder_strict() throws SQLException {
    interleavedRows();

    // The FAILED head e03 holds stream c; the stream-less rows are claimed freely; seq order.
    assertThat(ids(strict.loadPending(100)))
        .containsExactly("e01", "e02", "e04", "e08", "e12", "e16", "e20", "e24");

    // a's and b's heads are IN_PROGRESS: their streams stay blocked until the heads leave it.
    assertThat(strict.loadPending(100)).isEmpty();
    assertThat(strict.markDelivered(id("e01"), strict.claimedBy())).isTrue();
    assertThat(ids(strict.loadPending(100))).containsExactly("e05");
  }

  @Test
  void claimOrder_availabilityFirst() throws SQLException {
    interleavedRows();

    // The FAILED row is claimed past: c's head is its lowest PENDING row, e07.
    assertThat(ids(availabilityFirst.loadPending(100)))
        .containsExactly("e01", "e02", "e04", "e07", "e08", "e12", "e16", "e20", "e24");
  }

  @Test
  void claimOrder_limitTakesTheLowestSeqs() throws SQLException {
    interleavedRows();

    assertThat(ids(availabilityFirst.loadPending(3))).containsExactly("e01", "e02", "e04");
    assertThat(ids(availabilityFirst.loadPending(100)))
        .containsExactly("e07", "e08", "e12", "e16", "e20", "e24");
  }

  /**
   * The claim's rows come back from the driver in reverse seq order (a decorator reverses the
   * RETURNING result set; on today's SQLite they happen to come in seq order): the store still
   * returns them in seq order, in both modes, because it sorts them itself.
   */
  @Test
  void claimOrder_sortsTheReturnedRowsBySeq() throws SQLException {
    List<Long> served = new CopyOnWriteArrayList<>();
    use("reversed.db", null, delegate -> returningRowsReversed(delegate, served));
    interleavedRows();

    assertThat(ids(strict.loadPending(100)))
        .containsExactly("e01", "e02", "e04", "e08", "e12", "e16", "e20", "e24");
    assertThat(served).containsExactly(24L, 20L, 16L, 12L, 8L, 4L, 2L, 1L);

    sql(
        "UPDATE outbox_events SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL"
            + " WHERE status = 'IN_PROGRESS'");
    served.clear();
    assertThat(ids(availabilityFirst.loadPending(100)))
        .containsExactly("e01", "e02", "e04", "e07", "e08", "e12", "e16", "e20", "e24");
    assertThat(served).containsExactly(24L, 20L, 16L, 12L, 8L, 7L, 4L, 2L, 1L);
  }

  // ── The JVM clock ───────────────────────────────────────────────────────────────────────────

  /**
   * Every timestamp is the store clock's instant in epoch microseconds, read in the write; so is
   * the "now" the claim compares {@code next_retry_at} and the lease with. {@code
   * entry.createdAt()} is ignored.
   */
  @Test
  void jvmClockIsAuthoritative() throws SQLException {
    OutboxEntry e1 = entry("e1", stream("a"));
    assertThat(e1.createdAt()).isNotEqualTo(T0);
    strict.save(e1);
    strict.save(entry("e2", stream("b")));
    assertThat(longValue("SELECT created_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(T0_MICROS);
    assertThat(strict.findById(id("e1")).orElseThrow().createdAt()).isEqualTo(T0);

    Instant t1 = T0.plusSeconds(10);
    clock.setInstant(t1);
    assertThat(ids(strict.loadPending(10))).containsExactly("e1", "e2");
    assertThat(longValue("SELECT claimed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(T0_MICROS + 10_000_000L);
    assertThat(stringValue("SELECT claimed_by FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(strict.claimedBy());

    Instant t2 = T0.plusSeconds(20);
    clock.setInstant(t2);
    assertThat(strict.markDelivered(id("e1"), strict.claimedBy())).isTrue();
    assertThat(longValue("SELECT processed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(T0_MICROS + 20_000_000L);
    assertThat(strict.findById(id("e1")).orElseThrow().processedAt()).isEqualTo(t2);

    Instant t3 = T0.plusSeconds(30);
    clock.setInstant(t3);
    assertThat(strict.markRetry(id("e2"), 1, "later", Duration.ofSeconds(30), strict.claimedBy()))
        .isTrue();
    Instant retryAt = t3.plusSeconds(30);
    assertThat(longValue("SELECT next_retry_at FROM outbox_events WHERE entry_id = 'e2'"))
        .isEqualTo(T0_MICROS + 60_000_000L);
    assertThat(strict.findById(id("e2")).orElseThrow().nextRetryAt()).isEqualTo(retryAt);

    // next_retry_at <= now: not a microsecond before, exactly at it.
    clock.setInstant(retryAt.minusNanos(1_000));
    assertThat(strict.loadPending(10)).isEmpty();
    clock.setInstant(retryAt);
    assertThat(ids(strict.loadPending(10))).containsExactly("e2");

    Instant t4 = T0.plusSeconds(70);
    clock.setInstant(t4);
    assertThat(strict.markFailed(id("e2"), 2, "boom", strict.claimedBy())).isTrue();
    assertThat(strict.findById(id("e2")).orElseThrow().processedAt()).isEqualTo(t4);

    Instant t5 = T0.plusSeconds(80);
    clock.setInstant(t5);
    assertThat(strict.skipFailed(id("e2"), "operator", "obsolete")).isTrue();
    assertThat(longValue("SELECT skipped_at FROM outbox_events WHERE entry_id = 'e2'"))
        .isEqualTo(T0_MICROS + 80_000_000L);
    assertThat(strict.findById(id("e2")).orElseThrow().skip())
        .isEqualTo(new OutboxEntry.SkipRecord(t5, "operator", "obsolete"));

    // The lease, on the same clock: claimed_at < now - lease, strictly.
    strict.save(entry("e3", stream("c")));
    Instant t6 = T0.plusSeconds(90);
    clock.setInstant(t6);
    assertThat(ids(strict.loadPending(10))).containsExactly("e3");
    clock.setInstant(t6.plus(LEASE));
    assertThat(strict.loadPending(10)).isEmpty();
    Instant expired = t6.plus(LEASE).plusNanos(1_000);
    clock.setInstant(expired);
    assertThat(ids(strict.loadPending(10))).containsExactly("e3");
    assertThat(longValue("SELECT claimed_at FROM outbox_events WHERE entry_id = 'e3'"))
        .isEqualTo(T0_MICROS + 90_000_000L + LEASE.toNanos() / 1_000 + 1);
  }

  /** The clock's nanoseconds are rounded half up to microseconds. */
  @Test
  void jvmClockIsRoundedToMicroseconds() throws SQLException {
    clock.setInstant(T0.plusNanos(500));
    strict.save(entry("up", stream("a")));
    clock.setInstant(T0.plusNanos(499));
    strict.save(entry("down", stream("b")));

    assertThat(longValue("SELECT created_at FROM outbox_events WHERE entry_id = 'up'"))
        .isEqualTo(T0_MICROS + 1);
    assertThat(longValue("SELECT created_at FROM outbox_events WHERE entry_id = 'down'"))
        .isEqualTo(T0_MICROS);
    assertThat(strict.findById(id("up")).orElseThrow().createdAt()).isEqualTo(T0.plusNanos(1_000));
  }

  /** A second save of an id is a silent no-op: the first row, and its created_at, stay. */
  @Test
  void saveOfAnExistingId_isASilentNoOp() throws SQLException {
    strict.save(entry("e1", stream("a")));
    clock.setInstant(T0.plusSeconds(5));
    recorder.clear();

    strict.save(OutboxEntry.pending(id("e1"), "{\"other\":2}", "Other", stream("b")));

    assertThat(recorder.transcript()).containsExactly(write(INSERT));
    OutboxEntry row = strict.findById(id("e1")).orElseThrow();
    assertThat(row.payload()).isEqualTo("{\"k\":1}");
    assertThat(row.payloadType()).isEqualTo("Event");
    assertThat(row.streamId()).isEqualTo(stream("a"));
    assertThat(row.createdAt()).isEqualTo(T0);
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);
  }

  // ── Row mapping ─────────────────────────────────────────────────────────────────────────────

  /**
   * A claimed row reports no processedAt, though the row still carries the stamp of its earlier
   * FAILED; any other read of the same row reports it.
   */
  @Test
  void claimedRowsReportNoProcessedAt() throws SQLException {
    failed("e1", stream("a"));
    assertThat(availabilityFirst.resetFailedToPending(id("e1"))).isTrue();
    clock.setInstant(T0.plusSeconds(1));

    List<OutboxEntry> claimed = availabilityFirst.loadPending(10);

    assertThat(claimed).hasSize(1);
    assertThat(claimed.getFirst().processedAt()).isNull();
    assertThat(claimed.getFirst().status()).isEqualTo(OutboxStatus.IN_PROGRESS);
    assertThat(claimed.getFirst().attempts()).isZero();
    assertThat(claimed.getFirst().lastError()).isNull();
    assertThat(longValue("SELECT processed_at FROM outbox_events WHERE entry_id = 'e1'"))
        .isEqualTo(T0_MICROS);
    assertThat(
            availabilityFirst.findByStatus(OutboxStatus.IN_PROGRESS, 10).getFirst().processedAt())
        .isEqualTo(T0);
    assertThat(availabilityFirst.findById(id("e1")).orElseThrow().processedAt()).isEqualTo(T0);
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
    sql("UPDATE outbox_events SET trace_id = 'zz' WHERE entry_id = 't4'");

    assertThat(stringValue("SELECT trace_id FROM outbox_events WHERE entry_id = 't2'")).isNull();
    assertThat(stringValue("SELECT span_id FROM outbox_events WHERE entry_id = 't2'")).isNull();
    assertThat(availabilityFirst.findByStatus(OutboxStatus.PENDING, 10))
        .containsExactly(
            new OutboxEntry(
                id("t1"),
                "{\"k\":1}",
                "Event",
                stream("a"),
                OutboxStatus.PENDING,
                0,
                null,
                T0,
                null,
                null,
                null,
                SpanLink.ofW3c(TRACE, SPAN)),
            new OutboxEntry(
                id("t2"),
                "{\"k\":1}",
                "Event",
                null,
                OutboxStatus.PENDING,
                0,
                null,
                T0,
                null,
                null,
                null,
                SpanLink.NONE),
            new OutboxEntry(
                id("t4"),
                "{\"k\":1}",
                "Event",
                stream("c"),
                OutboxStatus.PENDING,
                0,
                null,
                T0,
                null,
                null,
                null,
                SpanLink.NONE));
    assertThat(availabilityFirst.findById(id("t3")))
        .contains(
            new OutboxEntry(
                id("t3"),
                "{\"k\":1}",
                "Event",
                stream("b"),
                OutboxStatus.FAILED,
                3,
                "boom",
                T0,
                T0,
                null,
                null,
                SpanLink.NONE));

    clock.setInstant(T0.plusSeconds(1));
    assertThat(availabilityFirst.skipFailed(id("t3"), "operator", "obsolete")).isTrue();
    assertThat(availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10))
        .containsExactly(
            new OutboxEntry(
                id("t3"),
                "{\"k\":1}",
                "Event",
                stream("b"),
                OutboxStatus.SKIPPED,
                3,
                "boom",
                T0,
                T0,
                null,
                new OutboxEntry.SkipRecord(T0.plusSeconds(1), "operator", "obsolete"),
                SpanLink.NONE));
    assertThat(availabilityFirst.findById(id("absent"))).isEmpty();
  }

  /**
   * A SKIPPED row whose skipped_at was cleared does not read as a skip with no instant: the skip
   * audit refuses it, and the read fails as a storage failure with that refusal as the cause.
   */
  @Test
  void skippedRowWithNullSkippedAt_isEventStoreException() throws SQLException {
    failed("e1", stream("a"));
    assertThat(availabilityFirst.skipFailed(id("e1"), "operator", "obsolete")).isTrue();
    sql("UPDATE outbox_events SET skipped_at = NULL WHERE entry_id = 'e1'");
    recorder.clear();

    Throwable byStatus =
        catchThrowable(() -> availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10));

    assertThat(byStatus)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find outbox entries by status: SKIPPED");
    assertThat(byStatus.getCause())
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("skippedAt is required")
        .hasNoCause();
    assertThat(byStatus.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + FIND_BY_STATUS, "#1 close");
    assertThat(recorder.openConnections()).isZero();

    Throwable byId = catchThrowable(() -> availabilityFirst.findById(id("e1")));
    assertThat(byId)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to find outbox entry: e1");
    assertThat(byId.getCause())
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("skippedAt is required");

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
   * The refusal is thrown after the write transaction that found the FAILED row committed and
   * returned its connection: exactly an IllegalStateException, never wrapped as a storage failure.
   * A close failure after that commit is the storage failure instead.
   */
  @Test
  void deleteRefusal_isIllegalStateOutsideTheWrite() {
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
    assertThat(recorder.transcript()).containsExactly(write(DELETE_NON_FAILED, EXISTS));
    assertThat(recorder.openConnections()).isZero();
    assertThat(availabilityFirst.findById(id("f")).orElseThrow().status())
        .isEqualTo(OutboxStatus.FAILED);

    recorder.failNextCloseAfterCommit();
    Throwable closeFailure = catchThrowable(() -> availabilityFirst.delete(id("f")));
    assertThat(closeFailure)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to delete outbox entry: f");
    assertThat(closeFailure.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
  }

  /** The quiet branches: a deleted row skips the existence check; an absent id is no refusal. */
  @Test
  void delete_deletedAndAbsent() {
    availabilityFirst.save(entry("p", null));
    recorder.clear();

    availabilityFirst.delete(id("p"));
    assertThat(recorder.transcript()).containsExactly(write(DELETE_NON_FAILED));
    assertThat(availabilityFirst.findById(id("p"))).isEmpty();

    recorder.clear();
    availabilityFirst.delete(id("absent"));
    assertThat(recorder.transcript()).containsExactly(write(DELETE_NON_FAILED, EXISTS));
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
  }

  @Test
  void constructorMessages() {
    var mode = OutboxOrderingMode.STRICT_PER_AGGREGATE;
    Clock utc = Clock.systemUTC();

    assertRefusedBeforeIo(() -> new SqliteOutboxStore(null), "database is required");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(null, null, null, null), "database is required");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(database, null, mode, utc), "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(database, Duration.ZERO, mode, utc),
        "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(database, Duration.ofNanos(-1), mode, utc),
        "claimLease must be positive");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(database, LEASE, null, utc), "orderingMode is required");
    assertRefusedBeforeIo(
        () -> new SqliteOutboxStore(database, LEASE, mode, null), "clock is required");

    // The defaults, and a claimedBy of its own per instance.
    SqliteOutboxStore defaults = new SqliteOutboxStore(database);
    assertThat(defaults.claimLease()).isEqualTo(Duration.ofMinutes(4));
    assertThat(defaults.orderingMode()).isEqualTo(mode);
    assertThat(defaults.claimedBy())
        .matches("outbox-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
        .isNotEqualTo(new SqliteOutboxStore(database).claimedBy());
    assertThat(new SqliteOutboxStore(database, Duration.ofSeconds(7)).claimLease())
        .isEqualTo(Duration.ofSeconds(7));
    assertThat(
            new SqliteOutboxStore(database, LEASE, OutboxOrderingMode.AVAILABILITY_FIRST)
                .orderingMode())
        .isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
    assertThat(recorder.connectionsOpened()).isZero();
  }

  @Test
  void argumentMessages() throws SQLException {
    OutboxEntryId e = id("e");
    String c = strict.claimedBy();

    assertRefusedBeforeIo(() -> strict.save(null), "entry is required");
    assertRefusedBeforeIo(() -> strict.loadPending(0), "limit must be >= 1");
    assertRefusedBeforeIo(() -> strict.markDelivered(null, c), "id is required");
    assertRefusedBeforeIo(() -> strict.markFailed(null, 1, "x", c), "id is required");
    assertRefusedBeforeIo(() -> strict.markRetry(null, 1, "x", Duration.ZERO, c), "id is required");
    assertRefusedBeforeIo(() -> strict.markRetry(e, 1, "x", null, c), "backoff is required");
    assertRefusedBeforeIo(() -> strict.delete(null), "id is required");
    assertRefusedBeforeIo(() -> strict.deleteSkipped(null), "cutoff is required");
    assertRefusedBeforeIo(() -> strict.findByStatus(null, 1), "status is required");
    assertRefusedBeforeIo(() -> strict.findByStatus(OutboxStatus.PENDING, 0), "limit must be >= 1");
    assertRefusedBeforeIo(() -> strict.findById(null), "id is required");
    assertRefusedBeforeIo(() -> strict.countByStatus(null), "status is required");
    assertRefusedBeforeIo(() -> strict.resetFailedToPending(null), "id is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(null, "op", "r"), "id is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(e, null, "r"), "skippedBy is required");
    assertRefusedBeforeIo(() -> strict.skipFailed(e, "op", null), "reason is required");
    assertRefusedBeforeIo(
        () -> strict.skipFailed(e, "o".repeat(256), "r"),
        "skippedBy is 256 characters; at most 255");

    // deleteDelivered refuses a null cutoff with a NullPointerException, as on Postgres.
    Throwable olderThan = catchThrowable(() -> strict.deleteDelivered(null));
    assertThat(olderThan)
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("olderThan is required");

    // The set-based marks: an empty set is 0 with no I/O; a null set is a NullPointerException.
    assertThat(strict.markDeliveredAll(List.of(), c)).isZero();
    assertThat(strict.releaseClaims(List.of(), c)).isZero();
    assertThat(catchThrowable(() -> strict.markDeliveredAll(null, c)))
        .isExactlyInstanceOf(NullPointerException.class);
    assertThat(catchThrowable(() -> strict.releaseClaims(null, c)))
        .isExactlyInstanceOf(NullPointerException.class);

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

  @Test
  void transcript_findByStatus() {
    availabilityFirst.save(entry("e1", null));
    recorder.clear();

    assertThat(availabilityFirst.findByStatus(OutboxStatus.PENDING, 10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + FIND_BY_STATUS, "#1 close");

    recorder.clear();
    assertThat(availabilityFirst.findByStatus(OutboxStatus.FAILED, 10)).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + FIND_BY_STATUS, "#1 close");
  }

  @Test
  void transcript_countByStatus() {
    availabilityFirst.save(entry("e1", null));
    recorder.clear();

    assertThat(availabilityFirst.countByStatus(OutboxStatus.PENDING)).isEqualTo(1L);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + COUNT_BY_STATUS, "#1 close");

    recorder.clear();
    assertThat(availabilityFirst.countByStatus(OutboxStatus.SKIPPED)).isZero();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + COUNT_BY_STATUS, "#1 close");
  }

  @Test
  void transcript_otherReads() {
    failed("f1", stream("a"));
    failed("f2", stream("a"));
    failed("f3", null);
    recorder.clear();

    assertThat(availabilityFirst.findById(id("f1"))).isPresent();
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + FIND_BY_ID, "#1 close");

    recorder.clear();
    assertThat(availabilityFirst.sampleBlockage()).isEqualTo(new BlockageSample(1L, T0, 1L));
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 prepare " + SAMPLE_BLOCKAGE, "#1 close");

    sqlUnchecked("DELETE FROM outbox_events");
    assertThat(availabilityFirst.sampleBlockage()).isEqualTo(new BlockageSample(0L, null, 0L));
  }

  @Test
  void transcript_writes() {
    String c = availabilityFirst.claimedBy();

    availabilityFirst.save(entry("e1", stream("a")));
    assertThat(recorder.transcript()).containsExactly(write(INSERT));

    recorder.clear();
    assertThat(availabilityFirst.loadPending(10)).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly(write(RECLAIM_EXPIRED, CLAIM_AVAILABILITY_FIRST));

    recorder.clear();
    assertThat(strict.loadPending(10)).isEmpty();
    assertThat(recorder.transcript()).containsExactly(write(RECLAIM_EXPIRED, CLAIM_STRICT));

    recorder.clear();
    assertThat(availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(MARK_RETRY));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markFailed(id("e1"), 2, "x", c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(MARK_FAILED));

    recorder.clear();
    assertThat(availabilityFirst.resetFailedToPending(id("e1"))).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(RESET_FAILED_TO_PENDING));

    availabilityFirst.loadPending(10);
    recorder.clear();
    assertThat(availabilityFirst.markDelivered(id("e1"), c)).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(MARK_DELIVERED));

    failed("e2", stream("b"));
    recorder.clear();
    assertThat(availabilityFirst.skipFailed(id("e2"), "operator", "obsolete")).isTrue();
    assertThat(recorder.transcript()).containsExactly(write(SKIP_FAILED));

    // Compare-and-set misses are a false result from the same one-statement transaction.
    recorder.clear();
    assertThat(availabilityFirst.markDelivered(id("e1"), c)).isFalse();
    assertThat(availabilityFirst.skipFailed(id("e2"), "operator", "again")).isFalse();
    assertThat(availabilityFirst.resetFailedToPending(id("absent"))).isFalse();
    assertThat(recorder.connectionsOpened()).isEqualTo(3);
    assertThat(recorder.openConnections()).isZero();
  }

  /**
   * The set-based marks bind at most 500 ids per statement, every chunk in one transaction on one
   * connection; the count is the rows that moved.
   */
  @Test
  void transcript_setBasedMarks() {
    String c = availabilityFirst.claimedBy();
    availabilityFirst.save(entry("e1", null));
    availabilityFirst.save(entry("e2", null));
    availabilityFirst.save(entry("e3", null));
    List<OutboxEntry> claimed = availabilityFirst.loadPending(10);
    List<OutboxEntryId> many = new ArrayList<>();
    for (int i = 0; i < 499; i++) {
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
            "#1 execute BEGIN IMMEDIATE",
            "#1 prepare " + MARK_DELIVERED_ALL_HEAD + placeholders(500) + ID_SET_TAIL,
            "#1 prepare " + MARK_DELIVERED_ALL_HEAD + placeholders(1) + ID_SET_TAIL,
            "#1 execute COMMIT",
            "#1 close",
            "open #2",
            "#2 execute BEGIN IMMEDIATE",
            "#2 prepare " + MARK_DELIVERED_ALL_HEAD + placeholders(500) + ID_SET_TAIL,
            "#2 prepare " + MARK_DELIVERED_ALL_HEAD + placeholders(1) + ID_SET_TAIL,
            "#2 execute COMMIT",
            "#2 close");

    recorder.clear();
    assertThat(availabilityFirst.releaseClaims(claimed, c)).isEqualTo(1);
    assertThat(recorder.transcript())
        .containsExactly(write(RELEASE_CLAIMS_HEAD + placeholders(3) + ID_SET_TAIL));
    assertThat(availabilityFirst.findById(id("e3")).orElseThrow().status())
        .isEqualTo(OutboxStatus.PENDING);
  }

  /** Each retention chunk (at most 1000 rows) is a write transaction on a connection of its own. */
  @Test
  void transcript_deleteDelivered() throws SQLException {
    insertRows("DELIVERED", "processed_at", 1001, T0_MICROS - 1);
    insertRows("DELIVERED", "processed_at", 1, T0_MICROS);
    recorder.clear();

    assertThat(availabilityFirst.deleteDelivered(T0)).isEqualTo(1001);

    List<String> expected = new ArrayList<>();
    for (int n = 1; n <= 3; n++) {
      expected.addAll(writeOn(n, DELETE_DELIVERED_BATCH));
    }
    assertThat(recorder.transcript()).containsExactlyElementsOf(expected);
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);

    recorder.clear();
    assertThat(availabilityFirst.deleteDelivered(T0)).isZero();
    assertThat(recorder.transcript()).containsExactly(write(DELETE_DELIVERED_BATCH));
  }

  @Test
  void transcript_deleteSkipped() throws SQLException {
    insertRows("SKIPPED", "skipped_at", 1001, T0_MICROS - 1);
    insertRows("SKIPPED", "skipped_at", 1, T0_MICROS);
    recorder.clear();

    assertThat(availabilityFirst.deleteSkipped(T0)).isEqualTo(1001);

    List<String> expected = new ArrayList<>();
    for (int n = 1; n <= 3; n++) {
      expected.addAll(writeOn(n, DELETE_SKIPPED_BATCH));
    }
    assertThat(recorder.transcript()).containsExactlyElementsOf(expected);
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(1L);

    recorder.clear();
    assertThat(availabilityFirst.deleteSkipped(T0)).isZero();
    assertThat(recorder.transcript()).containsExactly(write(DELETE_SKIPPED_BATCH));
  }

  /** saveAll joins the caller's connection: one prepared insert, no begin, commit or close. */
  @Test
  void transcript_saveAll() throws SQLException {
    try (Connection conn = recorder.getConnection()) {
      strict.saveAll(List.of(entry("s1", stream("a")), entry("s2", stream("b"))), conn);
      assertThat(recorder.transcript()).containsExactly("open #1", "#1 prepare " + INSERT);
    }
    assertThat(longValue("SELECT count(*) FROM outbox_events")).isEqualTo(2L);
    assertThat(longValue("SELECT min(created_at) FROM outbox_events")).isEqualTo(T0_MICROS);
    assertThat(longValue("SELECT max(created_at) FROM outbox_events")).isEqualTo(T0_MICROS);

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
  }

  /** One committed write transaction on connection #1 preparing {@code statements} in order. */
  private static String[] write(String... statements) {
    return writeOn(1, statements).toArray(String[]::new);
  }

  private static List<String> writeOn(int n, String... statements) {
    List<String> events = new ArrayList<>();
    events.add("open #" + n);
    events.add("#" + n + " execute BEGIN IMMEDIATE");
    for (String statement : statements) {
      events.add("#" + n + " prepare " + statement);
    }
    events.add("#" + n + " execute COMMIT");
    events.add("#" + n + " close");
    return events;
  }

  private static String placeholders(int count) {
    return String.join(", ", Collections.nCopies(count, "?"));
  }

  // ── Failures ────────────────────────────────────────────────────────────────────────────────

  @Test
  void writeFailureMessages() {
    String c = availabilityFirst.claimedBy();
    OutboxEntryId e = id("e\n1");
    List<OutboxEntry> one = List.of(entry("e1", null));

    assertWriteFails(
        INSERT,
        () -> availabilityFirst.save(entry("e\n1", null)),
        "Failed to save outbox entry: e1");
    assertWriteFails(
        RECLAIM_EXPIRED,
        () -> availabilityFirst.loadPending(1),
        "Failed to load pending outbox entries");
    assertWriteFails(
        CLAIM_HEAD,
        () -> availabilityFirst.loadPending(1),
        "Failed to load pending outbox entries",
        RECLAIM_EXPIRED);
    assertWriteFails(
        MARK_DELIVERED,
        () -> availabilityFirst.markDelivered(e, c),
        "Failed to mark outbox entry as delivered: e1");
    assertWriteFails(
        MARK_FAILED,
        () -> availabilityFirst.markFailed(e, 1, "x", c),
        "Failed to mark outbox entry as failed: e1");
    assertWriteFails(
        MARK_RETRY,
        () -> availabilityFirst.markRetry(e, 1, "x", Duration.ZERO, c),
        "Failed to mark outbox entry for retry: e1");
    assertWriteFails(
        MARK_DELIVERED_ALL_HEAD,
        () -> availabilityFirst.markDeliveredAll(List.of(e), c),
        "Failed to mark outbox entries delivered");
    assertWriteFails(
        RELEASE_CLAIMS_HEAD,
        () -> availabilityFirst.releaseClaims(one, c),
        "Failed to release unattempted outbox claims");
    assertWriteFails(
        DELETE_NON_FAILED, () -> availabilityFirst.delete(e), "Failed to delete outbox entry: e1");
    assertWriteFails(
        EXISTS,
        () -> availabilityFirst.delete(e),
        "Failed to delete outbox entry: e1",
        DELETE_NON_FAILED);
    assertWriteFails(
        DELETE_DELIVERED_BATCH,
        () -> availabilityFirst.deleteDelivered(T0),
        "Failed to delete delivered outbox entries");
    assertWriteFails(
        DELETE_SKIPPED_BATCH,
        () -> availabilityFirst.deleteSkipped(T0),
        "Failed to delete skipped outbox entries");
    assertWriteFails(
        RESET_FAILED_TO_PENDING,
        () -> availabilityFirst.resetFailedToPending(e),
        "Failed to reset failed outbox entry to pending: e1");
    assertWriteFails(
        SKIP_FAILED,
        () -> availabilityFirst.skipFailed(e, "op", "r"),
        "Failed to skip failed outbox entry: e1");
  }

  private void assertWriteFails(
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
    List<String> expected = new ArrayList<>(List.of("open #1", "#1 execute BEGIN IMMEDIATE"));
    for (String statement : preparedBefore) {
      expected.add("#1 prepare " + statement);
    }
    expected.addAll(List.of("#1 execute ROLLBACK", "#1 close"));
    assertThat(recorder.transcript()).containsExactlyElementsOf(expected);
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void readFailureMessages() {
    assertReadFails(
        FIND_BY_STATUS,
        () -> availabilityFirst.findByStatus(OutboxStatus.FAILED, 1),
        "Failed to find outbox entries by status: FAILED");
    assertReadFails(
        FIND_BY_ID,
        () -> availabilityFirst.findById(id("e\n1")),
        "Failed to find outbox entry: e1");
    assertReadFails(
        COUNT_BY_STATUS,
        () -> availabilityFirst.countByStatus(OutboxStatus.DELIVERED),
        "Failed to count outbox entries by status: DELIVERED");
    assertReadFails(
        SAMPLE_BLOCKAGE,
        () -> availabilityFirst.sampleBlockage(),
        "Failed to sample outbox blockage");
  }

  private void assertReadFails(String sql, ThrowingCallable call, String message) {
    SQLException injected = new SQLException("injected");
    recorder.beforePrepare(
        sql,
        c -> {
          throw injected;
        });
    recorder.clear();

    Throwable thrown = catchThrowable(call);

    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isSameAs(injected);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript()).containsExactly("open #1", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  /** A write whose connection fails to close after its COMMIT is a storage failure. */
  @Test
  void closeFailureAfterCommit_isAStorageFailureOfAWrittenRow() {
    recorder.failNextCloseAfterCommit();

    Throwable thrown = catchThrowable(() -> availabilityFirst.save(entry("e1", null)));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save outbox entry: e1");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(SQLException.class)
        .hasMessage("injected close failure");
    assertThat(recorder.transcript()).containsExactly(write(INSERT));
    assertThat(availabilityFirst.findById(id("e1"))).isPresent();
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void lockFailureMessages() throws SQLException {
    use("busy.db", "busy_timeout=200", UnaryOperator.identity());
    String c = availabilityFirst.claimedBy();
    OutboxEntryId e = id("e\n1");
    List<OutboxEntry> two = List.of(entry("e1", null), entry("e2", null));

    try (Connection holder = TestDatabases.dataSource(file).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      try {
        assertBusy(() -> availabilityFirst.save(entry("e\n1", null)), "outbox save of entry e1");
        assertBusy(() -> availabilityFirst.loadPending(1), "outbox claim");
        assertBusy(
            () -> availabilityFirst.markDelivered(e, c), "outbox mark delivered of entry e1");
        assertBusy(
            () -> availabilityFirst.markFailed(e, 1, "x", c), "outbox mark failed of entry e1");
        assertBusy(
            () -> availabilityFirst.markRetry(e, 1, "x", Duration.ZERO, c),
            "outbox mark retry of entry e1");
        assertBusy(
            () -> availabilityFirst.markDeliveredAll(List.of(e, id("e2")), c),
            "outbox mark delivered of 2 entries");
        assertBusy(() -> availabilityFirst.releaseClaims(two, c), "outbox release of 2 claims");
        assertBusy(() -> availabilityFirst.delete(e), "outbox delete of entry e1");
        assertBusy(() -> availabilityFirst.deleteDelivered(T0), "outbox delivered retention");
        assertBusy(() -> availabilityFirst.deleteSkipped(T0), "outbox skipped retention");
        assertBusy(() -> availabilityFirst.resetFailedToPending(e), "outbox reset of entry e1");
        assertBusy(() -> availabilityFirst.skipFailed(e, "op", "r"), "outbox skip of entry e1");
      } finally {
        st.execute("ROLLBACK");
      }
    }
  }

  private void assertBusy(ThrowingCallable call, String what) {
    recorder.clear();
    Throwable thrown = catchThrowable(call);
    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "SQLite's write lock was not granted within the connection's busy_timeout, so the "
                + what
                + BUSY_ADVICE);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(thrown.getCause()).isExactlyInstanceOf(SQLiteException.class);
    assertThat(((SQLiteException) thrown.getCause()).getResultCode())
        .isEqualTo(SQLiteErrorCode.SQLITE_BUSY);
    assertThat(recorder.transcript())
        .containsExactly("open #1", "#1 execute BEGIN IMMEDIATE", "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  // ── Quiet ───────────────────────────────────────────────────────────────────────────────────

  /**
   * The store logs nothing at any level: not on a claim, a mark, a refusal, a retention sweep or a
   * failed read.
   */
  @Test
  void theStoreLogsNothing() throws SQLException {
    try (LogCapture logs = LogCapture.attach(SqliteOutboxStore.class)) {
      String c = availabilityFirst.claimedBy();
      availabilityFirst.save(entry("e1", stream("a")));
      availabilityFirst.save(entry("e2", null));
      availabilityFirst.loadPending(10);
      availabilityFirst.markRetry(id("e1"), 1, "x", Duration.ZERO, c);
      availabilityFirst.loadPending(10);
      availabilityFirst.markFailed(id("e1"), 2, "x", c);
      availabilityFirst.markDeliveredAll(List.of(id("e2")), c);
      catchThrowable(() -> availabilityFirst.delete(id("e1")));
      catchThrowable(() -> strict.save(entry("n", null)));
      availabilityFirst.skipFailed(id("e1"), "operator", "obsolete");
      sql("UPDATE outbox_events SET skipped_at = NULL WHERE entry_id = 'e1'");
      catchThrowable(() -> availabilityFirst.findByStatus(OutboxStatus.SKIPPED, 10));
      clock.setInstant(T0.plus(Duration.ofDays(1)));
      availabilityFirst.deleteDelivered(T0.plusSeconds(1));
      availabilityFirst.deleteSkipped(T0.plusSeconds(1));
      availabilityFirst.sampleBlockage();
      availabilityFirst.countByStatus(OutboxStatus.PENDING);

      for (Level level : List.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR)) {
        assertThat(logs.events(level)).as("events at %s", level).isEmpty();
      }
    }
  }

  // ── A driver that returns RETURNING rows in reverse ─────────────────────────────────────────

  /** What a decorator does with the result of a call it forwarded. */
  @FunctionalInterface
  private interface AfterCall {
    Object apply(Method method, Object[] args, Object result) throws SQLException;
  }

  private static <T> T forwarding(Class<T> type, T target, AfterCall after) {
    return type.cast(
        Proxy.newProxyInstance(
            SqliteOutboxStoreCharacterizationTest.class.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              Object result;
              try {
                result = method.invoke(target, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
              return after.apply(method, args, result);
            }));
  }

  /**
   * {@code delegate}, except that the result set of an {@code … RETURNING} statement hands its rows
   * back in reverse order, recording each served row's {@code seq} in {@code served}. Everything
   * else, {@code unwrap} included, is forwarded.
   */
  private static DataSource returningRowsReversed(DataSource delegate, List<Long> served) {
    return forwarding(
        DataSource.class,
        delegate,
        (m, a, r) ->
            r instanceof Connection conn
                ? forwarding(
                    Connection.class,
                    conn,
                    (cm, ca, cr) ->
                        cr instanceof PreparedStatement ps
                                && ((String) ca[0]).contains(" RETURNING ")
                            ? forwarding(
                                PreparedStatement.class,
                                ps,
                                (pm, pa, pr) ->
                                    pr instanceof ResultSet rs ? reversed(rs, served) : pr)
                            : cr)
                : r);
  }

  private static ResultSet reversed(ResultSet rs, List<Long> served) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (rs) {
      ResultSetMetaData meta = rs.getMetaData();
      while (rs.next()) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          row.put(meta.getColumnLabel(i), rs.getObject(i));
        }
        rows.add(row);
      }
    }
    Collections.reverse(rows);
    int[] at = {-1};
    boolean[] lastWasNull = {false};
    return (ResultSet)
        Proxy.newProxyInstance(
            SqliteOutboxStoreCharacterizationTest.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "next" -> {
                  boolean has = ++at[0] < rows.size();
                  if (has) {
                    served.add(((Number) rows.get(at[0]).get("seq")).longValue());
                  }
                  return has;
                }
                case "close" -> {
                  return null;
                }
                case "wasNull" -> {
                  return lastWasNull[0];
                }
                case "getString", "getLong", "getInt" -> {
                  Object value = rows.get(at[0]).get((String) args[0]);
                  lastWasNull[0] = value == null;
                  return switch (method.getName()) {
                    case "getString" -> value == null ? null : value.toString();
                    case "getLong" -> value == null ? 0L : ((Number) value).longValue();
                    default -> value == null ? 0 : ((Number) value).intValue();
                  };
                }
                default -> throw new UnsupportedOperationException(method.getName());
              }
            });
  }

  // ── raw SQL on an unrecorded connection ─────────────────────────────────────────────────────

  /** {@code count} rows in {@code status} with {@code column} at {@code micros}. */
  private void insertRows(String status, String column, int count, long micros)
      throws SQLException {
    String skipColumns = status.equals("SKIPPED") ? ", skipped_by, skip_reason" : "";
    String skipValues = status.equals("SKIPPED") ? ", 'operator', 'obsolete'" : "";
    sql(
        "INSERT INTO outbox_events (entry_id, payload, payload_type, status, created_at, "
            + column
            + skipColumns
            + ") WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < "
            + count
            + ") SELECT '"
            + status
            + "-"
            + micros
            + "-' || i, '{}', 'Event', '"
            + status
            + "', 0, "
            + micros
            + skipValues
            + " FROM n");
  }

  private void sql(String statement) throws SQLException {
    try (Connection c = TestDatabases.dataSource(file).getConnection();
        Statement st = c.createStatement()) {
      st.executeUpdate(statement);
    }
  }

  private void sqlUnchecked(String statement) {
    try {
      sql(statement);
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
  }

  private long longValue(String query) throws SQLException {
    try (Connection c = TestDatabases.dataSource(file).getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getLong(1);
    }
  }

  private String stringValue(String query) throws SQLException {
    try (Connection c = TestDatabases.dataSource(file).getConnection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      assertThat(rs.next()).isTrue();
      return rs.getString(1);
    }
  }
}

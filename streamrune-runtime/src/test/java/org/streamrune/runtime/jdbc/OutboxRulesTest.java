package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;

class OutboxRulesTest {

  private static final OutboxEntryId ID = OutboxEntryId.of("entry-1");

  /** An entry id that renders differently once sanitized for a log line. */
  private static final OutboxEntryId UNSAFE_ID = OutboxEntryId.of("entry\n1");

  private static final StreamId STREAM =
      StreamId.of(new AggregateType("Order"), new AggregateId("order-1"));

  private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
  private static final String SPAN_ID = "00f067aa0ba902b7";

  private static final Instant CREATED = Instant.parse("2026-11-27T10:15:30.123456Z");
  private static final Instant PROCESSED = Instant.parse("2026-11-27T10:15:31.234567Z");
  private static final Instant NEXT_RETRY = Instant.parse("2026-11-27T10:15:32.345678Z");
  private static final Instant SKIPPED = Instant.parse("2026-11-27T10:15:33.456789Z");

  /** The columns every outbox read names, in the stores' order. */
  private static final String ENTRY_COLUMNS =
      "entry_id, aggregate_type, aggregate_id, payload, payload_type, status, attempts,"
          + " last_error, created_at, processed_at, next_retry_at, skipped_at, skipped_by,"
          + " skip_reason, trace_id, span_id, seq";

  @TempDir Path dir;

  private JdbcTestDatabase db;

  @BeforeEach
  void createTable() throws SQLException {
    db =
        JdbcTestDatabase.create(
            dir,
            "CREATE TABLE outbox_events (seq INTEGER PRIMARY KEY AUTOINCREMENT, entry_id TEXT,"
                + " aggregate_type TEXT, aggregate_id TEXT, payload TEXT, payload_type TEXT,"
                + " status TEXT, attempts INTEGER, last_error TEXT, created_at INTEGER,"
                + " processed_at INTEGER, next_retry_at INTEGER, skipped_at INTEGER,"
                + " skipped_by TEXT, skip_reason TEXT, trace_id TEXT, span_id TEXT)");
  }

  // ---- rules ----

  @Test
  void strictRefusesAStreamlessEntry() {
    OutboxEntry streamless = OutboxEntry.pending(UNSAFE_ID, "{}", "OrderPlaced");

    var e =
        assertThrows(
            OutboxOrderingViolationException.class,
            () ->
                OutboxRules.requireStreamIfStrict(
                    OutboxOrderingMode.STRICT_PER_AGGREGATE, streamless));

    assertThat(e.entryId()).isEqualTo(UNSAFE_ID);
    assertThat(e.payloadType()).isEqualTo("OrderPlaced");
    assertThat(e)
        .hasMessage(new OutboxOrderingViolationException(UNSAFE_ID, "OrderPlaced").getMessage())
        .hasNoCause();

    // An entry that carries its stream passes a strict channel.
    OutboxRules.requireStreamIfStrict(
        OutboxOrderingMode.STRICT_PER_AGGREGATE,
        OutboxEntry.pending(ID, "{}", "OrderPlaced", STREAM));
  }

  @Test
  void availabilityFirstAcceptsIt() {
    OutboxRules.requireStreamIfStrict(
        OutboxOrderingMode.AVAILABILITY_FIRST, OutboxEntry.pending(ID, "{}", "OrderPlaced"));
    OutboxRules.requireStreamIfStrict(
        OutboxOrderingMode.AVAILABILITY_FIRST,
        OutboxEntry.pending(ID, "{}", "OrderPlaced", STREAM));
  }

  @Test
  void bindTraceLink_bindsOnlyAValidW3cLink() throws SQLException {
    insertTraceLink("valid", SpanLink.ofW3c(TRACE_ID, SPAN_ID));
    insertTraceLink("none", SpanLink.NONE);
    insertTraceLink(
        "uppercase", new SpanLink(new TraceId(TRACE_ID.toUpperCase()), new SpanId(SPAN_ID)));
    insertTraceLink("short-span", new SpanLink(new TraceId(TRACE_ID), new SpanId("abc")));

    assertThat(storedTrace("valid")).containsExactly(TRACE_ID, SPAN_ID);
    assertThat(storedTrace("none")).containsExactly(null, null);
    assertThat(storedTrace("uppercase")).containsExactly(null, null);
    assertThat(storedTrace("short-span")).containsExactly(null, null);
  }

  @Test
  void requireIdMessage() {
    assertIllegal(() -> OutboxRules.requireId(null), "id is required");
    OutboxRules.requireId(ID);
  }

  @Test
  void requireOptionsMessages() {
    var strict = OutboxOrderingMode.STRICT_PER_AGGREGATE;
    assertIllegal(() -> OutboxRules.requireOptions(null, strict), "claimLease must be positive");
    assertIllegal(
        () -> OutboxRules.requireOptions(Duration.ZERO, strict), "claimLease must be positive");
    assertIllegal(
        () -> OutboxRules.requireOptions(Duration.ofSeconds(-1), strict),
        "claimLease must be positive");
    assertIllegal(
        () -> OutboxRules.requireOptions(Duration.ofSeconds(30), null), "orderingMode is required");
    // The lease is checked before the mode.
    assertIllegal(() -> OutboxRules.requireOptions(null, null), "claimLease must be positive");

    OutboxRules.requireOptions(Duration.ofNanos(1), strict);
    OutboxRules.requireOptions(Duration.ofSeconds(30), OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @Test
  void requireClaimLimitMessage() {
    assertIllegal(() -> OutboxRules.requireClaimLimit(0), "limit must be >= 1");
    assertIllegal(() -> OutboxRules.requireClaimLimit(-5), "limit must be >= 1");
    assertIllegal(() -> OutboxRules.requireClaimLimit(Integer.MIN_VALUE), "limit must be >= 1");

    OutboxRules.requireClaimLimit(1);
    OutboxRules.requireClaimLimit(Integer.MAX_VALUE);
  }

  @Test
  void deleteRefusalMessage() {
    IllegalStateException e = OutboxRules.deleteRefusal(ID);

    assertThat(e)
        .hasMessage(
            "outbox entry entry-1 is FAILED; resolve it with OutboxFailedReplayer.skip or .replay"
                + " — delete would release its aggregate with no record")
        .hasNoCause();
    assertThat(OutboxRules.deleteRefusal(UNSAFE_ID).getMessage())
        .startsWith("outbox entry " + LogSanitizer.sanitizeForLog(UNSAFE_ID.value()) + " is FAILED")
        .doesNotContain("\n");
  }

  // ---- row mapping ----

  @Test
  void mapRow_decodesEveryColumn() throws SQLException {
    insertRow("entry-1", "DELIVERED", STREAM, PROCESSED, NEXT_RETRY, null, TRACE_ID, SPAN_ID);
    var expected =
        new OutboxEntry(
            ID,
            "{\"n\":1}",
            "OrderPlaced",
            STREAM,
            OutboxStatus.DELIVERED,
            3,
            "boom",
            CREATED,
            PROCESSED,
            NEXT_RETRY,
            null,
            SpanLink.ofW3c(TRACE_ID, SPAN_ID));

    for (RecordingCodec codec : bothCodecs()) {
      assertThat(readRow(codec, false)).as(codec.name).isEqualTo(expected);
    }
  }

  @Test
  void mapRow_decodesNullableColumnsAsNull() throws SQLException {
    insertRow("entry-1", "PENDING", null, null, null, null, null, null);

    for (RecordingCodec codec : bothCodecs()) {
      OutboxEntry entry = readRow(codec, false);

      assertThat(entry.streamId()).as(codec.name).isNull();
      assertThat(entry.processedAt()).as(codec.name).isNull();
      assertThat(entry.nextRetryAt()).as(codec.name).isNull();
      assertThat(entry.skip()).as(codec.name).isNull();
      assertThat(entry.traceLink()).as(codec.name).isSameAs(SpanLink.NONE);
      assertThat(entry.createdAt()).as(codec.name).isEqualTo(CREATED);
    }
  }

  @Test
  void mapRow_storedTraceIdsThatAreNotW3cReadBackAsNone() throws SQLException {
    insertRow("entry-1", "PENDING", STREAM, null, null, null, "not-a-trace-id", SPAN_ID);
    insertRow("entry-2", "PENDING", STREAM, null, null, null, TRACE_ID, null);

    List<OutboxEntry> entries =
        readAll(
            pgLikeCodec(), false, "SELECT " + ENTRY_COLUMNS + " FROM outbox_events ORDER BY seq");

    assertThat(entries).extracting(OutboxEntry::traceLink).containsOnly(SpanLink.NONE);
  }

  @Test
  void mapRow_claimedRowsHaveNoProcessedAt() throws SQLException {
    // A replayed row still carries the stamp of its earlier FAILED; a claim does not report it.
    insertRow("entry-1", "IN_PROGRESS", STREAM, PROCESSED, NEXT_RETRY, null, null, null);

    for (RecordingCodec codec : bothCodecs()) {
      OutboxEntry claimed = readRow(codec, true);

      assertThat(claimed.processedAt()).as(codec.name).isNull();
      assertThat(claimed.nextRetryAt()).as(codec.name).isEqualTo(NEXT_RETRY);
      assertThat(codec.reads).as(codec.name).doesNotContain("nullable processed_at");
      assertThat(readRow(codec, false).processedAt()).as(codec.name).isEqualTo(PROCESSED);
    }
  }

  @Test
  void mapRow_skippedRowsCarryTheSkipRecord() throws SQLException {
    insertRow("entry-1", "SKIPPED", STREAM, PROCESSED, null, SKIPPED, null, null);

    for (RecordingCodec codec : bothCodecs()) {
      OutboxEntry entry = readRow(codec, false);

      assertThat(entry.status()).as(codec.name).isEqualTo(OutboxStatus.SKIPPED);
      assertThat(entry.skip())
          .as(codec.name)
          .isEqualTo(new OutboxEntry.SkipRecord(SKIPPED, "operator", "poison message"));
      // The resolution time is the skip's; processedAt keeps the time the entry reached FAILED.
      assertThat(entry.processedAt()).as(codec.name).isEqualTo(PROCESSED);
    }
  }

  @Test
  void mapRow_skipColumnsAreIgnoredOnARowThatIsNotSkipped() throws SQLException {
    insertRow("entry-1", "FAILED", STREAM, PROCESSED, null, SKIPPED, null, null);

    for (RecordingCodec codec : bothCodecs()) {
      assertThat(readRow(codec, false).skip()).as(codec.name).isNull();
      assertThat(codec.reads).as(codec.name).doesNotContain("required skipped_at");
    }
  }

  @Test
  void mapRow_requiredReadsGoThroughReadInstant() throws SQLException {
    insertRow("entry-1", "SKIPPED", STREAM, PROCESSED, NEXT_RETRY, SKIPPED, null, null);
    RecordingCodec codec = pgLikeCodec();

    readRow(codec, false);
    assertThat(codec.reads)
        .containsExactly(
            "required skipped_at",
            "required created_at",
            "nullable processed_at",
            "nullable next_retry_at");

    codec.reads.clear();
    readRow(codec, true);
    assertThat(codec.reads)
        .containsExactly("required skipped_at", "required created_at", "nullable next_retry_at");
  }

  @Test
  void mapRow_aNullCreatedAtFailsAsEachBackendDoesToday() throws SQLException {
    insertRow("entry-1", "PENDING", STREAM, null, null, null, null, null);
    exec("UPDATE outbox_events SET created_at = NULL");

    // Postgres: getTimestamp(column).toInstant() on SQL NULL.
    NullPointerException pg =
        assertThrows(NullPointerException.class, () -> readRow(pgLikeCodec(), false));
    assertThat(pg).isExactlyInstanceOf(NullPointerException.class);
    assertThat(pg.getMessage()).as("the codec's bare NPE, not the entry's refusal").isNull();
    // SQLite: the column reads as null, and the entry refuses it.
    NullPointerException sqlite =
        assertThrows(NullPointerException.class, () -> readRow(outboxRowsCodec(), false));
    assertThat(sqlite).hasMessage("createdAt is required");
  }

  @Test
  void mapRow_aSkippedRowWithoutItsSkipTimeFailsAsEachBackendDoesToday() throws SQLException {
    insertRow("entry-1", "SKIPPED", STREAM, PROCESSED, null, null, null, null);

    NullPointerException pg =
        assertThrows(NullPointerException.class, () -> readRow(pgLikeCodec(), false));
    assertThat(pg).isExactlyInstanceOf(NullPointerException.class);
    assertThat(pg.getMessage()).as("the codec's bare NPE, not the record's refusal").isNull();
    NullPointerException sqlite =
        assertThrows(NullPointerException.class, () -> readRow(outboxRowsCodec(), false));
    assertThat(sqlite).hasMessage("skippedAt is required");
  }

  @Test
  void mapRow_corruptStatusThrowsIllegalArgument() throws SQLException {
    insertRow("entry-1", "NOT_A_STATUS", STREAM, null, null, null, null, null);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> readRow(pgLikeCodec(), false));

    assertThat(e).hasMessageContaining("NOT_A_STATUS");
  }

  @Test
  void mapRow_readsEveryColumnByLabel() throws SQLException {
    insertRow("entry-1", "SKIPPED", STREAM, PROCESSED, NEXT_RETRY, SKIPPED, TRACE_ID, SPAN_ID);
    // The columns in another order than the stores name them, with an extra one in front: a mapper
    // reading by position would decode the wrong columns.
    String shuffled =
        "SELECT seq AS unrelated, span_id, trace_id, skip_reason, skipped_by, skipped_at,"
            + " next_retry_at, processed_at, created_at, last_error, attempts, status,"
            + " payload_type, payload, aggregate_id, aggregate_type, entry_id FROM outbox_events";
    String inOrder = "SELECT " + ENTRY_COLUMNS + " FROM outbox_events";

    for (RecordingCodec codec : bothCodecs()) {
      assertThat(readAll(codec, false, shuffled))
          .as(codec.name)
          .isEqualTo(readAll(codec, false, inOrder));
    }
  }

  @Test
  void mapRow_neverReadsAColumnByIndex() throws SQLException {
    insertRow("entry-1", "SKIPPED", STREAM, PROCESSED, NEXT_RETRY, SKIPPED, TRACE_ID, SPAN_ID);
    List<String> calls = new ArrayList<>();

    try (Connection c = db.connection();
        Statement st = c.createStatement();
        ResultSet real = st.executeQuery("SELECT " + ENTRY_COLUMNS + " FROM outbox_events")) {
      assertThat(real.next()).isTrue();
      ResultSet byLabelOnly = byLabelOnly(real, calls);

      for (RecordingCodec codec : bothCodecs()) {
        assertThat(OutboxRules.mapRow(byLabelOnly, codec, false).id()).isEqualTo(ID);
      }
    }
    assertThat(calls).isNotEmpty().allMatch(call -> !call.startsWith("index "));
  }

  // ---- fixtures ----

  private List<RecordingCodec> bothCodecs() {
    return List.of(pgLikeCodec(), outboxRowsCodec());
  }

  /**
   * Decodes as the Postgres store does: a required read of SQL NULL throws {@link
   * NullPointerException}, as {@code getTimestamp(column).toInstant()} does.
   */
  private static RecordingCodec pgLikeCodec() {
    return new RecordingCodec("postgres-like", false);
  }

  /** Decodes as the SQLite outbox store does: every timestamp column reads SQL NULL as null. */
  private static RecordingCodec outboxRowsCodec() {
    return new RecordingCodec("sqlite outbox rows", true);
  }

  /**
   * A codec over epoch microseconds in an {@code INTEGER}, recording every column it is asked for.
   */
  private static final class RecordingCodec implements JdbcInstantCodec {

    final String name;
    final List<String> reads = new ArrayList<>();
    private final boolean nullTolerantRequiredRead;

    RecordingCodec(String name, boolean nullTolerantRequiredRead) {
      this.name = name;
      this.nullTolerantRequiredRead = nullTolerantRequiredRead;
    }

    @Override
    public Instant readInstant(ResultSet rs, String column) throws SQLException {
      reads.add("required " + column);
      Instant value = nullable(rs, column);
      if (nullTolerantRequiredRead) {
        return value;
      }
      if (value == null) {
        // On purpose: pgjdbc's getTimestamp(column) is null for SQL NULL, and the Postgres codec's
        // toInstant() on it fails with a bare NullPointerException, which this mirrors.
        throw new NullPointerException();
      }
      return value;
    }

    @Override
    public Instant readNullableInstant(ResultSet rs, String column) throws SQLException {
      reads.add("nullable " + column);
      return nullable(rs, column);
    }

    @Override
    public boolean readFlag(ResultSet rs, String column) throws SQLException {
      throw new AssertionError("an outbox row has no flag column: " + column);
    }

    @Override
    public void bindInstant(PreparedStatement ps, int index, Instant instant) throws SQLException {
      ps.setLong(index, micros(instant));
    }

    private static Instant nullable(ResultSet rs, String column) throws SQLException {
      long micros = rs.getLong(column);
      return rs.wasNull() ? null : fromMicros(micros);
    }
  }

  /**
   * {@code real} behind a proxy that records every getter call as {@code label <name>} or {@code
   * index <n>}, and refuses the by-index ones.
   */
  private static ResultSet byLabelOnly(ResultSet real, List<String> calls) {
    return (ResultSet)
        Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (proxy, method, args) -> {
              if (method.getName().startsWith("get") && args != null && args.length > 0) {
                if (args[0] instanceof Integer index) {
                  calls.add("index " + index);
                  throw new AssertionError("read column " + index + " by index");
                }
                calls.add("label " + args[0]);
              }
              try {
                return method.invoke(real, args);
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }

  @FunctionalInterface
  private interface StatementWork {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private void insertTraceLink(String entryId, SpanLink link) throws SQLException {
    update(
        "INSERT INTO outbox_events (entry_id, trace_id, span_id) VALUES (?, ?, ?)",
        ps -> {
          ps.setString(1, entryId);
          OutboxRules.bindTraceLink(ps, 2, link);
        });
  }

  private List<String> storedTrace(String entryId) throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps =
            c.prepareStatement("SELECT trace_id, span_id FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        List<String> values = new ArrayList<>();
        values.add(rs.getString("trace_id"));
        values.add(rs.getString("span_id"));
        return values;
      }
    }
  }

  private void insertRow(
      String entryId,
      String status,
      StreamId stream,
      Instant processedAt,
      Instant nextRetryAt,
      Instant skippedAt,
      String traceId,
      String spanId)
      throws SQLException {
    update(
        "INSERT INTO outbox_events (entry_id, aggregate_type, aggregate_id, payload,"
            + " payload_type, status, attempts, last_error, created_at, processed_at,"
            + " next_retry_at, skipped_at, skipped_by, skip_reason, trace_id, span_id)"
            + " VALUES (?, ?, ?, '{\"n\":1}', 'OrderPlaced', ?, 3, 'boom', ?, ?, ?, ?, ?, ?, ?, ?)",
        ps -> {
          ps.setString(1, entryId);
          ps.setString(2, stream == null ? null : stream.aggregateType().value());
          ps.setString(3, stream == null ? null : stream.aggregateId().value());
          ps.setString(4, status);
          ps.setLong(5, micros(CREATED));
          setMicros(ps, 6, processedAt);
          setMicros(ps, 7, nextRetryAt);
          setMicros(ps, 8, skippedAt);
          ps.setString(9, skippedAt == null ? null : "operator");
          ps.setString(10, skippedAt == null ? null : "poison message");
          ps.setString(11, traceId);
          ps.setString(12, spanId);
        });
  }

  private OutboxEntry readRow(RecordingCodec codec, boolean claimed) throws SQLException {
    List<OutboxEntry> rows =
        readAll(codec, claimed, "SELECT " + ENTRY_COLUMNS + " FROM outbox_events");
    assertThat(rows).hasSize(1);
    return rows.get(0);
  }

  private List<OutboxEntry> readAll(JdbcInstantCodec codec, boolean claimed, String sql)
      throws SQLException {
    List<OutboxEntry> rows = new ArrayList<>();
    try (Connection c = db.connection();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) {
        rows.add(OutboxRules.mapRow(rs, codec, claimed));
      }
    }
    return rows;
  }

  private void update(String sql, StatementWork work) throws SQLException {
    try (Connection c = db.connection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      work.bind(ps);
      ps.executeUpdate();
    }
  }

  private void exec(String sql) throws SQLException {
    try (Connection c = db.connection();
        Statement st = c.createStatement()) {
      st.execute(sql);
    }
  }

  private static void setMicros(PreparedStatement ps, int index, Instant instant)
      throws SQLException {
    if (instant == null) {
      ps.setNull(index, Types.BIGINT);
    } else {
      ps.setLong(index, micros(instant));
    }
  }

  private static long micros(Instant instant) {
    return Math.addExact(
        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
  }

  private static Instant fromMicros(long micros) {
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
  }

  private static void assertIllegal(Executable call, String message) {
    assertThat(assertThrows(IllegalArgumentException.class, call)).hasMessage(message);
  }
}

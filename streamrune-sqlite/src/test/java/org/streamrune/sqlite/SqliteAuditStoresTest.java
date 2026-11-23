package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;

/**
 * What the SQLite audit stores and queries add to the shared contracts: every write is durable on
 * return and refuses a connection that carries an application's open transaction or is in
 * manual-commit mode, waits for the write lock at most {@code busy_timeout}, and a write killed at
 * any point leaves the trail as before or as after it, with a rerun that converges; every read and
 * write turns a storage failure into an {@link EventStoreException}; instants are kept to the
 * microsecond; and the causation trace reads the causation index at each step.
 */
class SqliteAuditStoresTest {

  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");
  private static final Instant FAR_PAST = Instant.parse("2000-01-01T00:00:00Z");
  private static final Instant FAR_FUTURE = Instant.parse("9999-01-01T00:00:00Z");
  private static final StreamId ORDER =
      StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
  private static final UserId USER = UserId.of("user-1");

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();
  private final List<SingleConnection> connections = new ArrayList<>();

  @AfterEach
  void close() throws SQLException {
    handles.forEach(SqliteDatabase::close);
    for (SingleConnection connection : connections) {
      connection.close();
    }
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private SqliteDatabase migrated(String file, String override) {
    SqliteDatabase database =
        open(
            override.isEmpty()
                ? TestDatabases.dataSource(dir.resolve(file))
                : TestDatabases.dataSource(dir.resolve(file), override));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  private static AuditEntry command(String commandId, Instant at) {
    return new AuditEntry(
        CommandId.of(commandId),
        "PlaceOrder",
        AggregateType.of("order"),
        AggregateId.of("order-1"),
        USER,
        at,
        AuditOutcome.SUCCESS,
        null,
        1,
        CorrelationId.of("corr-1"));
  }

  private static EventAuditEntry event(String eventId, String causedBy) {
    return new EventAuditEntry(
        EventId.of(eventId),
        EventType.of("OrderPlaced"),
        ORDER,
        new Version(1),
        CommandId.of("cmd-1"),
        CorrelationId.of("corr-1"),
        causedBy == null ? null : CausationId.of(causedBy),
        USER,
        T0);
  }

  private static long count(SqliteDatabase database, String table) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** The writes of both stores, over the stores {@code database} builds. */
  private static Map<String, Runnable> writes(SqliteDatabase database) {
    var audit = new SqliteAuditStore(database);
    var events = new SqliteEventAuditStore(database);
    Map<String, Runnable> writes = new LinkedHashMap<>();
    writes.put("audit save", () -> audit.save(command("c-1", T0)));
    writes.put("event audit save", () -> events.save(event("e-1", null)));
    writes.put("event audit saveAll", () -> events.saveAll(List.of(event("e-2", null))));
    return writes;
  }

  @Test
  void everyConstructorRefusesANullDatabase() {
    List<Function<SqliteDatabase, Object>> constructors =
        List.of(
            SqliteAuditStore::new,
            SqliteEventAuditStore::new,
            SqliteCommandAuditQuery::new,
            SqliteEventAuditQuery::new,
            SqliteComplianceReportQuery::new);
    for (Function<SqliteDatabase, Object> constructor : constructors) {
      assertThatThrownBy(() -> constructor.apply(null))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("database is required");
    }
  }

  /** An instant is kept to the microsecond, its nanoseconds rounded half up as on Postgres. */
  @Test
  void nanosecondsAreRoundedHalfUpToTheMicrosecond() {
    SqliteDatabase database = migrated("nanos.db", "");
    new SqliteAuditStore(database)
        .save(command("c-1", Instant.parse("2026-11-23T10:00:00.123456500Z")));

    assertThat(
            new SqliteCommandAuditQuery(database)
                .findByCommandId(CommandId.of("c-1"))
                .orElseThrow()
                .occurredAt())
        .isEqualTo(Instant.parse("2026-11-23T10:00:00.123457Z"));
  }

  /**
   * A write on a connection that carries a transaction the application opened is refused, not
   * joined: the application's later ROLLBACK undoes its own write only, and no audit row was
   * written through it.
   */
  @Test
  void everyWriteRefusesAConnectionCarryingAnApplicationTransaction() throws SQLException {
    SqliteDatabase plain = migrated("foreign.db", "");
    SingleConnection single = new SingleConnection(TestDatabases.dataSource(plain.file()));
    connections.add(single);
    Map<String, Runnable> writes = writes(open(single));
    try (Connection app = single.getConnection();
        Statement st = app.createStatement()) {
      st.execute("BEGIN");
      for (var write : writes.entrySet()) {
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .rootCause()
            .hasMessageContaining("cannot start a transaction within a transaction");
      }
      st.execute("ROLLBACK"); // still the application's transaction: the refusal left it open
    }

    assertThat(count(plain, "audit_log")).isZero();
    assertThat(count(plain, "event_audit_log")).isZero();
  }

  @Test
  void aConnectionInManualCommitModeIsRefusedBeforeAnythingIsWritten() throws SQLException {
    SqliteDatabase database = migrated("manual.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    Map<String, Runnable> writes = writes(open(source));
    source.manualCommit();

    for (var write : writes.entrySet()) {
      assertThatThrownBy(write.getValue()::run)
          .as(write.getKey())
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    }
    assertThat(count(database, "audit_log")).isZero();
    assertThat(count(database, "event_audit_log")).isZero();
  }

  /**
   * Every write waits for the write lock at most {@code busy_timeout} and then fails having written
   * nothing; every query reads alongside the writer that holds the lock.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void everyWriteWaitsAtMostBusyTimeout_andQueriesDoNotWait() throws Exception {
    SqliteDatabase database = migrated("busy.db", "busy_timeout=200");
    new SqliteAuditStore(database).save(command("stored", T0));
    new SqliteEventAuditStore(database).save(event("stored", null));
    var commands = new SqliteCommandAuditQuery(database);
    var events = new SqliteEventAuditQuery(database);
    var reports = new SqliteComplianceReportQuery(database);

    try (Connection holder = TestDatabases.dataSource(database.file()).getConnection();
        Statement st = holder.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      for (var write : writes(database).entrySet()) {
        assertThatThrownBy(write.getValue()::run)
            .as(write.getKey())
            .isExactlyInstanceOf(EventStoreException.class)
            .hasMessageContaining("busy_timeout");
      }
      assertThat(commands.findByCommandId(CommandId.of("stored"))).isPresent();
      assertThat(commands.findByUserId(USER, FAR_PAST, FAR_FUTURE, PageRequest.of(0, 5)).content())
          .hasSize(1);
      assertThat(events.traceFullCausationChain(EventId.of("stored"))).hasSize(1);
      assertThat(events.findByStreamId(ORDER, FAR_PAST, FAR_FUTURE, PageRequest.of(0, 5)).content())
          .hasSize(1);
      assertThat(reports.generate(FAR_PAST, FAR_FUTURE).totalCommands()).isEqualTo(1);
      st.execute("ROLLBACK");
    }

    assertThat(count(database, "audit_log")).as("nothing was written").isEqualTo(1);
    assertThat(count(database, "event_audit_log")).isEqualTo(1);
  }

  /**
   * An audit save killed inside its row leaves nothing; killed right after its COMMIT, the entry.
   * The trail is append-only, so a rerun after the COMMIT adds a second entry for the same command,
   * as a retry does, and findByCommandId answers the latest.
   */
  @Test
  void anAuditSaveKilledAtAnyPointLeavesNothingOrTheEntry() throws SQLException {
    Path file = dir.resolve("crash-audit.db");
    SqliteDatabase plain = migrated("crash-audit.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    var store = new SqliteAuditStore(open(crash.connections(TestDatabases.dataSource(file))));

    crash.arm(new Point("audit_log", 1));
    assertThatThrownBy(() -> store.save(command("c-1", T0)))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("c-1");
    assertThat(count(plain, "audit_log")).as("killed inside its row").isZero();

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.save(command("c-1", T0)))
        .isExactlyInstanceOf(EventStoreException.class);
    assertThat(count(plain, "audit_log")).as("killed after its COMMIT").isEqualTo(1);

    store.save(command("c-1", T0.plusMillis(1)));
    assertThat(count(plain, "audit_log")).isEqualTo(2);
    assertThat(
            new SqliteCommandAuditQuery(plain)
                .findByCommandId(CommandId.of("c-1"))
                .orElseThrow()
                .occurredAt())
        .isEqualTo(T0.plusMillis(1));
  }

  /**
   * An event audit batch killed inside its second row leaves none of the batch; killed after its
   * COMMIT, all of it; its rerun is then refused as a duplicate and writes nothing more.
   */
  @Test
  void anEventAuditBatchKilledAtAnyPointLeavesNoneOrAll_andItsRerunConverges() throws SQLException {
    Path file = dir.resolve("crash-events.db");
    SqliteDatabase plain = migrated("crash-events.db", "");
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    var store = new SqliteEventAuditStore(open(crash.connections(TestDatabases.dataSource(file))));
    List<EventAuditEntry> batch = List.of(event("e-1", null), event("e-2", "e-1"));

    crash.arm(new Point("event_audit_log", 2));
    assertThatThrownBy(() -> store.saveAll(batch)).isExactlyInstanceOf(EventStoreException.class);
    assertThat(count(plain, "event_audit_log")).as("killed inside its second row").isZero();

    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.saveAll(batch)).isExactlyInstanceOf(EventStoreException.class);
    assertThat(count(plain, "event_audit_log")).as("killed after its COMMIT").isEqualTo(2);

    assertThatThrownBy(() -> store.saveAll(batch)).isExactlyInstanceOf(EventStoreException.class);
    assertThat(count(plain, "event_audit_log")).isEqualTo(2);
  }

  @Test
  void aStorageFailureIsAnEventStoreExceptionForEveryReadAndWrite() throws SQLException {
    SqliteDatabase database = migrated("missing.db", "");
    var source = new SwitchableDataSource(TestDatabases.dataSource(database.file()));
    SqliteDatabase failing = open(source);
    var commands = new SqliteCommandAuditQuery(failing);
    var events = new SqliteEventAuditQuery(failing);
    var reports = new SqliteComplianceReportQuery(failing);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE audit_log");
      st.execute("DROP TABLE event_audit_log");
    }
    PageRequest page = PageRequest.of(0, 5);
    Map<String, Runnable> calls = new LinkedHashMap<>(writes(failing));
    calls.put("findByCommandId", () -> commands.findByCommandId(CommandId.of("c")));
    calls.put("findByUserId", () -> commands.findByUserId(USER, FAR_PAST, FAR_FUTURE, page));
    calls.put(
        "findByOutcome",
        () -> commands.findByOutcome(AuditOutcome.SUCCESS, FAR_PAST, FAR_FUTURE, page));
    calls.put(
        "findByCommandType", () -> commands.findByCommandType("T", FAR_PAST, FAR_FUTURE, page));
    calls.put("events by correlation", () -> events.findByCorrelationId(CorrelationId.of("c")));
    calls.put(
        "events by correlation, paged",
        () -> events.findByCorrelationId(CorrelationId.of("c"), page));
    calls.put("events by command", () -> events.findByCommandId(CommandId.of("c")));
    calls.put("events by command, paged", () -> events.findByCommandId(CommandId.of("c"), page));
    calls.put("events by causation", () -> events.findByCausationId(CausationId.of("c")));
    calls.put(
        "events by causation, paged", () -> events.findByCausationId(CausationId.of("c"), page));
    calls.put("events by stream", () -> events.findByStreamId(ORDER, FAR_PAST, FAR_FUTURE));
    calls.put(
        "events by stream, paged", () -> events.findByStreamId(ORDER, FAR_PAST, FAR_FUTURE, page));
    calls.put("events by user", () -> events.findByUserId(USER, FAR_PAST, FAR_FUTURE));
    calls.put("events by user, paged", () -> events.findByUserId(USER, FAR_PAST, FAR_FUTURE, page));
    calls.put("trace", () -> events.traceFullCausationChain(EventId.of("e")));
    calls.put("report", () -> reports.generate(FAR_PAST, FAR_FUTURE));
    calls.put("user report", () -> reports.generateForUser(USER, FAR_PAST, FAR_FUTURE));
    for (var call : calls.entrySet()) {
      assertThatThrownBy(call.getValue()::run)
          .as(call.getKey())
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(SQLException.class);
    }

    source.failWith(() -> new IllegalStateException("pool closed"));
    for (var call : calls.entrySet()) {
      assertThatThrownBy(call.getValue()::run)
          .as(call.getKey())
          .isExactlyInstanceOf(EventStoreException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
    }
  }

  /** A stored row the id types refuse on decode is a store failure, not an argument error. */
  @Test
  void aStoredRowTheIdTypesRefuseIsAStoreFailure() throws SQLException {
    SqliteDatabase database = migrated("decode.db", "");
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(
          "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
              + " version, command_id, correlation_id, occurred_at)"
              + " VALUES ('', 'T', 'order', 'o-1', 1, 'c', 'corr', 0)");
      st.execute(
          "INSERT INTO audit_log (command_id, command_type, occurred_at, outcome)"
              + " VALUES ('c', 'T', 0, 'NOT_AN_OUTCOME')");
    }

    assertThatThrownBy(
            () -> new SqliteEventAuditQuery(database).findByCorrelationId(CorrelationId.of("corr")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new SqliteCommandAuditQuery(database).findByCommandId(CommandId.of("c")))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Each step of the causation trace finds the children of the rows found so far through the
   * partial index on causation_id, not by scanning the trail.
   */
  @Test
  void theCausationTraceReadsTheCausationIndex() throws SQLException {
    SqliteDatabase database = migrated("plan.db", "");
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement("EXPLAIN QUERY PLAN " + SqliteEventAuditQuery.TRACE_CHAIN)) {
      ps.setString(1, "e-1");
      StringBuilder plan = new StringBuilder();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.append(rs.getString("detail")).append('\n');
        }
      }
      assertThat(plan.toString())
          .contains("USING INDEX idx_event_audit_causation_id")
          .doesNotContain("SCAN event_audit_log\n")
          .doesNotContain("SCAN e\n");
    }
  }
}

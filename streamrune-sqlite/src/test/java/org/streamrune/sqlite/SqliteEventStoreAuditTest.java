package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;

/**
 * The event audit rows an append writes on its own connection, inside its own write transaction,
 * with its events: a transaction that fails or dies leaves none of them, a committed one has one
 * row per event, its stream and versions taken from the append's arguments. And the factory's
 * wiring of the audit store.
 */
class SqliteEventStoreAuditTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
  private static final String COMMAND = "com.example.PlaceOrder";
  private static final Instant AT = Instant.parse("2026-01-01T00:00:00.123456Z");

  /** One outbox entry per event: the event's id. */
  private static final OutboxEventMapper ONE_PER_EVENT =
      event ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(event.metadata().eventId().value()),
                  "{}",
                  "A",
                  event.streamId()));

  /** Fails the insert of every event audit row after the first one in the table. */
  private static final String FAIL_AFTER_FIRST_AUDIT_ROW =
      "CREATE TRIGGER fail_audit AFTER INSERT ON event_audit_log"
          + " WHEN (SELECT count(*) FROM event_audit_log) > 1"
          + " BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END";

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase database(String file) {
    return open(TestDatabases.dataSource(dir.resolve(file)));
  }

  private SqliteDatabase open(javax.sql.DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private static SqliteEventStoreFactory factory(SqliteDatabase database) {
    return new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry());
  }

  /** A store with an event audit store, an outbox and an inbox, all over {@code database}. */
  private static EventStore audited(SqliteDatabase database) {
    return factory(database)
        .eventAuditStore(new SqliteEventAuditStore(database))
        .outboxStore(new SqliteOutboxStore(database))
        .outboxEventMapper(ONE_PER_EVENT)
        .commandInbox(new SqliteCommandInbox(database))
        .create();
  }

  private static EventEnvelope event(StreamId stream, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(version),
        EventStoreContract.EVENT_TYPE,
        new ContractEvent("v" + version),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            CausationId.of("cause-" + version),
            UserId.of("user-1"),
            AT,
            null,
            null));
  }

  private static List<EventEnvelope> events(long from, int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(event(STREAM, from + i));
    }
    return events;
  }

  /** The audit entry of {@code event} appended to {@code stream} at {@code version}. */
  private static EventAuditEntry auditOf(EventEnvelope event, StreamId stream, long version) {
    EventMetadata m = event.metadata();
    return new EventAuditEntry(
        m.eventId(),
        event.eventType(),
        stream,
        new Version(version),
        m.commandId(),
        m.correlationId(),
        m.causationId(),
        m.userId(),
        m.timestamp());
  }

  private static List<EventAuditEntry> auditRows(SqliteDatabase database) {
    return new SqliteEventAuditQuery(database)
        .findByStreamId(STREAM, Instant.EPOCH, Instant.parse("9999-01-01T00:00:00Z"));
  }

  private static long count(SqliteDatabase database, String table) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static void execute(SqliteDatabase database, String sql) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  // ── Atomicity ───────────────────────────────────────────────────────────────────────────────

  /**
   * An append whose transaction fails after its first event audit row leaves no audit row, no
   * event, no outbox row and no offset: the next append takes offset 1. Without the failure it
   * stores one audit row per event, each as the append wrote its event.
   */
  @Test
  void eventAuditRowsCommitWithTheAppend() throws Exception {
    SqliteDatabase database = database("audit-atomic.db");
    EventStore store = audited(database);
    execute(database, FAIL_AFTER_FIRST_AUDIT_ROW);

    assertThatThrownBy(() -> store.append(STREAM, events(1, 2), Version.initial()))
        .isInstanceOf(EventStoreException.class);
    assertThat(count(database, "event_audit_log")).isZero();
    assertThat(count(database, "event_stream")).isZero();
    assertThat(count(database, "outbox_events")).isZero();

    execute(database, "DROP TRIGGER fail_audit");
    List<EventEnvelope> appended = events(1, 2);
    assertThat(store.append(STREAM, appended, Version.initial()).globalOffsets())
        .as("the failed append's offsets were rolled back with it")
        .containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(auditRows(database))
        .containsExactly(auditOf(appended.get(0), STREAM, 1), auditOf(appended.get(1), STREAM, 2));
  }

  /** A refused append (a version conflict) writes no audit row; an empty append writes none. */
  @Test
  void aRefusedOrEmptyAppendWritesNoAuditRow() throws Exception {
    SqliteDatabase database = database("audit-refused.db");
    EventStore store = audited(database);
    store.append(STREAM, events(1, 1), Version.initial());

    assertThatThrownBy(() -> store.append(STREAM, events(1, 1), Version.initial()))
        .isInstanceOf(OptimisticLockException.class);
    store.append(STREAM, List.of(), new Version(1));

    assertThat(count(database, "event_audit_log")).isEqualTo(1);
  }

  /**
   * The audit row's stream and version come from the append's arguments, as the event row's do,
   * never from the envelope: a caller whose envelope names another stream and version still gets an
   * audit row that mirrors event_stream.
   */
  @Test
  void auditRowsMirrorTheEventStreamWhenEnvelopeFieldsDiverge() throws Exception {
    SqliteDatabase database = database("audit-diverge.db");
    EventStore store = audited(database);
    EventEnvelope divergent =
        event(StreamId.of(AggregateType.of("cart"), AggregateId.of("elsewhere")), 99);

    store.append(STREAM, List.of(divergent), Version.initial());

    assertThat(auditRows(database)).containsExactly(auditOf(divergent, STREAM, 1));
  }

  /**
   * A keyed append writes its audit rows in its transaction: one per event when it records the key,
   * none when the key was recorded before, and a failure of an audit row rolls the claim back too,
   * so the rerun records the key and writes the rows.
   */
  @Test
  void aKeyedAppendWritesItsAuditRowsWithTheClaim() throws Exception {
    SqliteDatabase database = database("audit-keyed.db");
    EventStore store = audited(database);
    IdempotencyKey key = IdempotencyKey.of("k-1");
    execute(database, FAIL_AFTER_FIRST_AUDIT_ROW);
    List<EventEnvelope> appended = events(1, 2);

    assertThatThrownBy(() -> store.appendWithKey(STREAM, appended, Version.initial(), key, COMMAND))
        .isInstanceOf(EventStoreException.class);
    assertThat(count(database, "command_inbox")).isZero();
    assertThat(count(database, "event_audit_log")).isZero();

    execute(database, "DROP TRIGGER fail_audit");
    assertThat(
            store.appendWithKey(STREAM, appended, Version.initial(), key, COMMAND).alreadyApplied())
        .isFalse();
    assertThat(
            store.appendWithKey(STREAM, appended, Version.initial(), key, COMMAND).alreadyApplied())
        .isTrue();

    assertThat(auditRows(database))
        .containsExactly(auditOf(appended.get(0), STREAM, 1), auditOf(appended.get(1), STREAM, 2));
  }

  /** Without an event audit store the append writes no audit row. */
  @Test
  void aStoreWithoutAnAuditStoreWritesNoAuditRow() throws Exception {
    SqliteDatabase database = database("audit-none.db");
    EventStore store = factory(database).create();

    store.append(STREAM, events(1, 2), Version.initial());

    assertThat(count(database, "event_audit_log")).isZero();
  }

  // ── Crash points ────────────────────────────────────────────────────────────────────────────

  /**
   * An append killed inside its second audit row leaves nothing (no event, no audit row, no outbox
   * row) and its rerun stores everything once. Killed right after its COMMIT it is whole; its
   * unkeyed rerun is refused as a version conflict and adds no audit row.
   */
  @Test
  void anAppendKilledInsideItsAuditRowsLeavesNothing_andItsRerunConverges() throws Exception {
    Path file = dir.resolve("audit-crash.db");
    SqliteDatabase plain = database("audit-crash.db");
    audited(plain); // migrates
    SqliteCrashPoints crash = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
    SqliteDatabase killable = open(crash.connections(TestDatabases.dataSource(file)));
    EventStore store = audited(killable);
    List<EventEnvelope> first = events(1, 2);

    crash.arm(new Point("event_audit_log", 2));
    assertThatThrownBy(() -> store.append(STREAM, first, Version.initial()))
        .isInstanceOf(EventStoreException.class);
    assertThat(count(plain, "event_audit_log")).isZero();
    assertThat(count(plain, "event_stream")).isZero();
    assertThat(count(plain, "outbox_events")).isZero();

    store.append(STREAM, first, Version.initial());
    assertThat(auditRows(plain)).hasSize(2);

    List<EventEnvelope> second = events(3, 1);
    crash.arm(Point.AFTER_COMMIT);
    assertThatThrownBy(() -> store.append(STREAM, second, new Version(2)))
        .isInstanceOf(EventStoreException.class);
    assertThat(count(plain, "event_stream")).as("whole after its COMMIT").isEqualTo(3);
    assertThatThrownBy(() -> store.append(STREAM, second, new Version(2)))
        .isInstanceOf(OptimisticLockException.class);
    assertThat(auditRows(plain)).extracting(e -> e.version().value()).containsExactly(1L, 2L, 3L);
  }

  // ── Wiring ──────────────────────────────────────────────────────────────────────────────────

  /** An audit store over another database file cannot join the append's transaction. */
  @Test
  void anAuditStoreOverAnotherFileIsRefused() {
    SqliteDatabase database = database("audit-wiring.db");
    SqliteDatabase other = database("audit-other.db");

    assertThatThrownBy(
            () -> factory(database).eventAuditStore(new SqliteEventAuditStore(other)).create())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("eventAuditStore")
        .hasMessageContaining(other.file().toString());
  }

  /** Unsetting the audit store (null) builds a store without one. */
  @Test
  void aNullAuditStoreMeansNone() throws Exception {
    SqliteDatabase database = database("audit-unset.db");
    EventStore store =
        factory(database)
            .eventAuditStore(new SqliteEventAuditStore(database))
            .eventAuditStore(null)
            .create();

    store.append(STREAM, events(1, 1), Version.initial());

    assertThat(count(database, "event_audit_log")).isZero();
  }
}

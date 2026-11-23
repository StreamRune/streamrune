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
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;

/**
 * The outbox entries, the inbox claim and the offsets an append writes on its own connection, in
 * its own write transaction: a transaction that fails leaves none of them, a committed one has all
 * of them. And the factory's wiring of the two stores.
 */
class SqliteEventStoreOutboxInboxTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
  private static final String COMMAND = "com.example.PlaceOrder";
  private static final SpanLink LINK =
      new SpanLink(TraceId.of("4bf92f3577b34da6a3ce929d0e0e4736"), SpanId.of("00f067aa0ba902b7"));

  /** Two entries per event: the event's id with "-a" and "-b". */
  private static final OutboxEventMapper TWO_PER_EVENT =
      event ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(event.metadata().eventId().value() + "-a"),
                  "{}",
                  "A",
                  event.streamId()),
              OutboxEntry.pending(
                  OutboxEntryId.of(event.metadata().eventId().value() + "-b"),
                  "{}",
                  "B",
                  event.streamId()));

  /** Fails the insert of every outbox row after the first one in the table. */
  private static final String FAIL_AFTER_FIRST_OUTBOX_ROW =
      "CREATE TRIGGER fail_outbox AFTER INSERT ON outbox_events"
          + " WHEN (SELECT count(*) FROM outbox_events) > 1"
          + " BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END";

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase database(String file) {
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve(file)));
    handles.add(database);
    return database;
  }

  private static SqliteEventStoreFactory factory(SqliteDatabase database) {
    return new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry());
  }

  private static EventStore wired(SqliteDatabase database) {
    return factory(database)
        .outboxStore(new SqliteOutboxStore(database))
        .outboxEventMapper(TWO_PER_EVENT)
        .commandInbox(new SqliteCommandInbox(database))
        .create();
  }

  private static EventEnvelope event(long version, SpanLink link) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        new Version(version),
        EventStoreContract.EVENT_TYPE,
        new ContractEvent("v" + version),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            null,
            null,
            link));
  }

  private static List<EventEnvelope> events(long from, int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(event(from + i, SpanLink.NONE));
    }
    return events;
  }

  // ── Atomicity ───────────────────────────────────────────────────────────────────────────────

  /**
   * An append whose transaction fails after its first outbox row leaves no outbox row, no event and
   * no offset: the next append takes offset 1. Without the failure it stores all of them.
   */
  @Test
  void outboxRowsCommitWithTheAppend() throws Exception {
    SqliteDatabase database = database("outbox-atomic.db");
    EventStore store = wired(database);
    execute(database, FAIL_AFTER_FIRST_OUTBOX_ROW);

    assertThatThrownBy(() -> store.append(STREAM, events(1, 2), Version.initial()))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save 4 outbox entries");

    assertThat(count(database, "outbox_events")).isZero();
    assertThat(count(database, "event_stream")).isZero();
    assertThat(counter(database)).as("the offset reservation rolled back").isZero();

    execute(database, "DROP TRIGGER fail_outbox");
    var result = store.append(STREAM, events(1, 2), Version.initial());

    assertThat(result.globalOffsets()).containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(count(database, "outbox_events")).isEqualTo(4);
    assertThat(count(database, "event_stream")).isEqualTo(2);
  }

  /**
   * A keyed append writes its key, its offsets and its outbox rows with its events: a failure at
   * the outbox write leaves no claim, no event, no offset and no outbox row, and its retry under
   * the same key stores once; a version conflict leaves the key unclaimed and no outbox row.
   */
  @Test
  void inboxClaimIsAtomicWithTheAppend() throws Exception {
    SqliteDatabase database = database("inbox-atomic.db");
    EventStore store = wired(database);
    var inbox = new SqliteCommandInbox(database);
    IdempotencyKey key = IdempotencyKey.of("place-1");
    execute(database, FAIL_AFTER_FIRST_OUTBOX_ROW);

    assertThatThrownBy(
            () -> store.appendWithKey(STREAM, events(1, 2), Version.initial(), key, COMMAND))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save 4 outbox entries");

    assertThat(inbox.find(key)).as("no claim").isEmpty();
    assertThat(count(database, "event_stream")).isZero();
    assertThat(count(database, "outbox_events")).isZero();
    assertThat(counter(database)).isZero();

    execute(database, "DROP TRIGGER fail_outbox");
    var stored = store.appendWithKey(STREAM, events(1, 2), Version.initial(), key, COMMAND);
    var replay = store.appendWithKey(STREAM, events(1, 2), Version.initial(), key, COMMAND);

    assertThat(stored.alreadyApplied()).isFalse();
    assertThat(replay.alreadyApplied()).isTrue();
    assertThat(replay.globalOffsets()).isEqualTo(stored.globalOffsets());
    var row = inbox.find(key).orElseThrow();
    assertThat(row.globalOffsets()).containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(row.finalVersion()).isEqualTo(new Version(2));
    assertThat(count(database, "outbox_events")).as("the replay wrote none").isEqualTo(4);

    IdempotencyKey stale = IdempotencyKey.of("place-stale");
    assertThatThrownBy(
            () -> store.appendWithKey(STREAM, events(1, 1), Version.initial(), stale, COMMAND))
        .isExactlyInstanceOf(OptimisticLockException.class);
    assertThat(inbox.find(stale)).isEmpty();
    assertThat(count(database, "outbox_events")).isEqualTo(4);
  }

  /** A zero-event keyed append claims its key without consulting the head and writes no offset. */
  @Test
  void aZeroEventKeyedAppendClaimsItsKeyOnly() {
    SqliteDatabase database = database("zero.db");
    EventStore store = wired(database);
    store.append(STREAM, events(1, 1), Version.initial());

    var result =
        store.appendWithKey(STREAM, List.of(), new Version(7), IdempotencyKey.of("k0"), COMMAND);

    assertThat(result.globalOffsets()).isEmpty();
    assertThat(result.finalVersion()).isEqualTo(new Version(7));
    assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(1));
  }

  // ── The entries' trace link ─────────────────────────────────────────────────────────────────

  /** Each mapped entry takes the link of its own event, replacing what the mapper set. */
  @Test
  void mappedEntriesTakeTheLinkOfTheirEvent() {
    SqliteDatabase database = database("links.db");
    EventStore store =
        factory(database)
            .outboxStore(new SqliteOutboxStore(database))
            .outboxEventMapper(
                e -> List.of(TWO_PER_EVENT.toOutbox(e).getFirst().withTraceLink(LINK)))
            .create();
    EventEnvelope traced = event(1, LINK);
    EventEnvelope untraced = event(2, SpanLink.NONE);

    store.append(STREAM, List.of(traced, untraced), Version.initial());

    var outbox = new SqliteOutboxStore(database);
    assertThat(linkOf(outbox, traced)).isEqualTo(LINK);
    assertThat(linkOf(outbox, untraced)).as("the mapper's link replaced").isSameAs(SpanLink.NONE);
  }

  /** The entries given to the four-argument append all take the link of its first event. */
  @Test
  void givenEntriesTakeTheLinkOfTheFirstEvent() {
    SqliteDatabase database = database("given.db");
    SqliteEventStore store =
        (SqliteEventStore)
            factory(database)
                .outboxStore(new SqliteOutboxStore(database))
                .outboxEventMapper(e -> List.of())
                .create();
    OutboxEntry given = OutboxEntry.pending(OutboxEntryId.of("given"), "{}", "G", STREAM);

    store.append(
        STREAM,
        List.of(event(1, LINK), event(2, SpanLink.NONE)),
        Version.initial(),
        List.of(given));

    var stored = new SqliteOutboxStore(database).findById(OutboxEntryId.of("given")).orElseThrow();
    assertThat(stored.traceLink()).isEqualTo(LINK);
    assertThat(stored.status()).isEqualTo(OutboxStatus.PENDING);
  }

  /** The four-argument append writes the given entries only, not the mapper's. */
  @Test
  void givenEntriesAreTheOnlyEntriesTheFourArgumentAppendWrites() {
    SqliteDatabase database = database("given-only.db");
    SqliteEventStore store =
        (SqliteEventStore)
            factory(database)
                .outboxStore(new SqliteOutboxStore(database))
                .outboxEventMapper(TWO_PER_EVENT)
                .create();
    OutboxEntry given = OutboxEntry.pending(OutboxEntryId.of("given"), "{}", "G", STREAM);
    EventEnvelope event = event(1, SpanLink.NONE);

    store.append(STREAM, List.of(event), Version.initial(), List.of(given));

    var outbox = new SqliteOutboxStore(database);
    assertThat(outbox.findByStatus(OutboxStatus.PENDING, 10))
        .extracting(e -> e.id().value())
        .containsExactly("given");
    assertThat(outbox.findById(OutboxEntryId.of(event.metadata().eventId().value() + "-a")))
        .as("the mapper's entries are not written")
        .isEmpty();
  }

  @Test
  void givenEntriesNeedAnOutboxStoreAndEvents() {
    SqliteDatabase database = database("given-refused.db");
    SqliteEventStore bare = (SqliteEventStore) factory(database).create();
    SqliteEventStore wired =
        (SqliteEventStore)
            factory(database)
                .outboxStore(new SqliteOutboxStore(database))
                .outboxEventMapper(e -> List.of())
                .create();
    List<OutboxEntry> given =
        List.of(OutboxEntry.pending(OutboxEntryId.of("given"), "{}", "G", STREAM));

    assertThatThrownBy(() -> bare.append(STREAM, events(1, 1), Version.initial(), given))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no SqliteOutboxStore is configured");
    assertThatThrownBy(() -> wired.append(STREAM, List.of(), Version.initial(), given))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty events list");
    assertThatThrownBy(() -> wired.append(STREAM, events(1, 1), Version.initial(), null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage("outboxEntries is required");
    assertThat(wired.append(STREAM, List.of(), Version.initial(), List.of()).globalOffsets())
        .isEmpty();
    assertThat(bare.append(STREAM, events(1, 1), Version.initial(), List.of()).globalOffsets())
        .containsExactly(GlobalOffset.of(1));
  }

  // ── The factory's wiring ────────────────────────────────────────────────────────────────────

  @Test
  void theOutboxStoreAndItsMapperAreSetTogetherOrNotAtAll() {
    SqliteDatabase database = database("pairing.db");

    assertThatThrownBy(
            () -> factory(database).outboxStore(new SqliteOutboxStore(database)).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("outboxStore and outboxEventMapper must be set together");
    assertThatThrownBy(() -> factory(database).outboxEventMapper(TWO_PER_EVENT).create())
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("outboxStore and outboxEventMapper must be set together");
    assertThat(factory(database).outboxStore(null).outboxEventMapper(null).create()).isNotNull();
  }

  /** A store of another database file would write where the relay never reads: refused. */
  @Test
  void storesOfAnotherDatabaseFileAreRefused() {
    SqliteDatabase database = database("mine.db");
    SqliteDatabase other = database("other.db");

    assertThatThrownBy(
            () ->
                factory(database)
                    .outboxStore(new SqliteOutboxStore(other))
                    .outboxEventMapper(TWO_PER_EVENT)
                    .create())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outboxStore");
    assertThatThrownBy(() -> factory(database).commandInbox(new SqliteCommandInbox(other)).create())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("commandInbox");
    SqliteDatabase sameFile = database("mine.db");
    assertThat(factory(database).commandInbox(new SqliteCommandInbox(sameFile)).create())
        .as("another handle on the same file is the same database")
        .isNotNull();
  }

  @Test
  void aStoreWithoutAnInboxRefusesAKeyedAppend() {
    SqliteDatabase database = database("no-inbox.db");
    EventStore store = factory(database).create();

    assertThatThrownBy(
            () ->
                store.appendWithKey(
                    STREAM, events(1, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND))
        .isExactlyInstanceOf(UnsupportedOperationException.class)
        .hasMessage("This SqliteEventStore was built without a command inbox");
  }

  /** A keyed append that does not get the write lock within busy_timeout says so. */
  @Test
  void aKeyedAppendThatWaitsLongerThanBusyTimeoutSaysSo() throws Exception {
    Path file = dir.resolve("busy.db");
    wired(database("busy.db"));
    SqliteDatabase impatient =
        SqliteDatabase.open(TestDatabases.dataSource(file, "busy_timeout=50"));
    handles.add(impatient);
    EventStore store = wired(impatient);
    try (Connection holder = TestDatabases.dataSource(file).getConnection()) {
      SqliteTransactions.begin(holder);
      assertThatThrownBy(
              () ->
                  store.appendWithKey(
                      STREAM, events(1, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("busy_timeout");
      SqliteTransactions.rollback(holder, "test");
    }
  }

  /** Any other storage failure of a keyed append is the Postgres store's message. */
  @Test
  void aKeyedAppendThatFailsInStorageIsAnEventStoreException() throws Exception {
    SqliteDatabase database = database("broken.db");
    EventStore store = wired(database);
    execute(database, "DROP TABLE event_stream");

    assertThatThrownBy(
            () ->
                store.appendWithKey(
                    STREAM, events(1, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed idempotent append to stream: order:o-1");
    assertThat(new SqliteCommandInbox(database).find(IdempotencyKey.of("k"))).isEmpty();
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  private static SpanLink linkOf(SqliteOutboxStore outbox, EventEnvelope event) {
    return outbox
        .findById(OutboxEntryId.of(event.metadata().eventId().value() + "-a"))
        .orElseThrow()
        .traceLink();
  }

  private static void execute(SqliteDatabase database, String sql) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private static long count(SqliteDatabase database, String table) throws SQLException {
    return scalar(database, "SELECT count(*) FROM " + table);
  }

  private static long counter(SqliteDatabase database) throws SQLException {
    return scalar(database, "SELECT next_value FROM global_offset_counter WHERE id = 1");
  }

  private static long scalar(SqliteDatabase database, String sql) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }
}

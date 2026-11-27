package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PSQLException;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;
import org.streamrune.test.EventStoreContract.ContractState;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.RecordingStreamRuneMetrics;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Characterization of the PostgreSQL event store: the statements, connections, transaction settings
 * and statement timeouts of every read and write, the exact exception type, message, cause and
 * suppressed exceptions of every refusal and failure, the log lines (category {@link
 * PostgresEventStore}) with their text, the discard metric, and the spans. The values are what the
 * store does on a real database today, written down as they come out; these tests stay unchanged
 * while the store's read path and append support move into shared helpers.
 *
 * <p>Every store borrows from a {@link RecordingDataSource} and bounds its statements at seven
 * seconds: a load reads in one connection's explicit transaction (autocommit off, fetch size 500,
 * committed, autocommit restored), the other reads run in autocommit with the driver's millisecond
 * timeout, and an append opens its transaction with one statement that pins READ COMMITTED and sets
 * the two transaction-local timeouts, commits, and then sends its NOTIFY in autocommit.
 */
@Testcontainers
class PostgresEventStoreCharacterizationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_event_store_characterization");

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId A = StreamId.of(ORDER, AggregateId.of("A"));
  private static final StreamId B = StreamId.of(ORDER, AggregateId.of("B"));
  private static final String COMMAND = "com.example.PlaceOrder";
  private static final EventTypeRegistry REGISTRY = EventStoreContract.Setup.defaults().registry();

  // ── today's statements, as literals ─────────────────────────────────────────────────────────

  private static final String LOAD_SNAPSHOT =
      "SELECT version, state_type, state_payload, snapshot_version, purge_generation"
          + " FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?";
  private static final String LEGACY_PROBE =
      "SELECT state_type FROM snapshot_store WHERE state_type IS NOT NULL LIMIT 1";
  private static final String LOAD_EVENTS =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ? AND version > ? ORDER BY version";
  private static final String BEGIN_APPEND =
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 7000;"
          + " SET LOCAL idle_in_transaction_session_timeout = 7000";
  private static final String INSERT_EVENT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, schema_version) VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)";
  private static final String RESERVE_OFFSETS =
      "UPDATE global_offset_sequence SET next_value = next_value + ? WHERE id = 1"
          + " RETURNING next_value";
  private static final String STREAM_HEAD_VERSION =
      "SELECT COALESCE(MAX(version), 0) FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ?";
  private static final String READ_GLOBAL =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream WHERE global_offset > ?"
          + " ORDER BY global_offset LIMIT ?";
  private static final String LAST_GLOBAL_OFFSET =
      "SELECT COALESCE(MAX(global_offset), 0) FROM event_stream";
  private static final String READ_STREAM =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ? AND version > ? ORDER BY version LIMIT ?";
  private static final String NOTIFY = "SELECT pg_notify('streamrune_events', ?)";
  private static final String UPSERT_SNAPSHOT =
      "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, state_type,"
          + " state_payload, snapshot_version, purge_generation)"
          + " VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)"
          + " ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET"
          + " version = EXCLUDED.version,"
          + " state_type = EXCLUDED.state_type,"
          + " state_payload = EXCLUDED.state_payload,"
          + " snapshot_version = EXCLUDED.snapshot_version,"
          + " created_at = NOW()"
          + " WHERE snapshot_store.purge_generation = EXCLUDED.purge_generation";
  private static final String OUTBOX_INSERT =
      "INSERT INTO outbox_events (entry_id, payload, payload_type, aggregate_type, aggregate_id,"
          + " trace_id, span_id, status, attempts, last_error, created_at, processed_at)"
          + " VALUES (?, ?::jsonb, ?, ?, ?, ?, ?, 'PENDING', 0, NULL, NOW(), NULL)"
          + " ON CONFLICT (entry_id) DO NOTHING";
  private static final String AUDIT_INSERT =
      "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id, version,"
          + " command_id, correlation_id, causation_id, user_id, occurred_at)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String INBOX_CLAIM =
      "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type, aggregate_id,"
          + " final_version) VALUES (?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING";
  private static final String INBOX_BACKFILL =
      "UPDATE command_inbox SET global_offsets = ? WHERE idempotency_key = ?";
  private static final String INBOX_FIND =
      "SELECT command_type, aggregate_type, aggregate_id, final_version, global_offsets,"
          + " processed_at FROM command_inbox WHERE idempotency_key = ?";

  /** A snapshot payload that is valid JSON but no ContractState. */
  private static final String UNREADABLE_SNAPSHOT =
      "UPDATE snapshot_store SET state_payload = '{\"marker\":\"s\",\"unknown\":1}'";

  /** A session setting, made on the real connection, that makes every table lookup fail. */
  private static final String NO_SUCH_SCHEMA = "SET search_path TO no_such_schema";

  private static final String DESYNC_ADVICE =
      "': global_offset_sequence.next_value has fallen at or below max(global_offset), so this"
          + " append reserved offsets that are already committed. This is NOT a version conflict"
          + " and retrying cannot resolve it — the reservation rolls back with the append, so"
          + " every attempt reserves the same used offsets. Re-seed the counter with: UPDATE"
          + " global_offset_sequence SET next_value = (SELECT COALESCE(max(global_offset), 0) FROM"
          + " event_stream) WHERE id = 1";

  /** An event whose JSON serialization always fails: its one property throws. */
  static final class Unserializable implements DomainEvent {
    public String getBoom() {
      throw new IllegalStateException("cannot serialize");
    }
  }

  /** An event carrying one {@code @Encrypted} field. */
  record SecretEvent(String subject, @Encrypted(subjectId = "subject") String email)
      implements DomainEvent {
    @JsonCreator
    SecretEvent(@JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      this.subject = subject;
      this.email = email;
    }
  }

  /** A snapshot state carrying one {@code @Encrypted} field. */
  record SecretState(String subject, @Encrypted(subjectId = "subject") String email)
      implements AggregateState {
    @JsonCreator
    SecretState(@JsonProperty("subject") String subject, @JsonProperty("email") String email) {
      this.subject = subject;
      this.email = email;
    }
  }

  private static final EventType SECRET = new EventType("SecretEvent");

  /** Holds a state class whose simple name is the contract's registered state type. */
  static final class Elsewhere {
    record ContractState(String marker) implements AggregateState {}
  }

  /** Records, on every decrypt, how many recorded connections are open at that moment. */
  static final class ConnectionCountingEngine implements CryptoEngine {
    private final CryptoEngine keys = new InMemoryCryptoEngine();
    private final RecordingDataSource recorder;
    final List<Integer> openAtDecrypt = new CopyOnWriteArrayList<>();
    volatile RuntimeException decryptFailure;

    ConnectionCountingEngine(RecordingDataSource recorder) {
      this.recorder = recorder;
    }

    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return keys.encrypt(subjectId, plaintext);
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      openAtDecrypt.add(recorder.openConnections());
      if (decryptFailure != null) {
        throw decryptFailure;
      }
      return keys.decrypt(subjectId, ciphertext);
    }

    @Override
    public void deleteKey(SubjectId subjectId) {
      keys.deleteKey(subjectId);
    }

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return keys.isKeyAvailable(subjectId);
    }
  }

  static PGSimpleDataSource dataSource;

  private RecordingDataSource recorder;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(dataSource, REGISTRY).create();
  }

  /** Every test starts from empty tables, the offset counter rewound, and a fresh recorder. */
  @BeforeEach
  void emptyTables() throws SQLException {
    execute("TRUNCATE event_stream, command_inbox, snapshot_store, outbox_events, event_audit_log");
    execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    recorder = new RecordingDataSource(dataSource);
  }

  private PostgresEventStore.Builder builder() {
    return PostgresEventStore.builder()
        .dataSource(recorder)
        .typeRegistry(REGISTRY)
        .statementTimeout(Duration.ofSeconds(7));
  }

  /** A store with no outbox, inbox or audit. */
  private PostgresEventStore plain() {
    PostgresEventStore store = builder().build();
    recorder.clear();
    return store;
  }

  /** A store with outbox (one entry per event), command inbox and event audit. */
  private PostgresEventStore wired() {
    return wired(builder());
  }

  private PostgresEventStore wired(PostgresEventStore.Builder builder) {
    PostgresEventStore store =
        builder
            .outboxStore(new PostgresOutboxStore(recorder))
            .outboxEventMapper(EventStoreContract.OUTBOX_MAPPER)
            .commandInbox(new PostgresCommandInbox(recorder))
            .eventAuditStore(new PostgresEventAuditStore(recorder))
            .build();
    recorder.clear();
    return store;
  }

  // ── Golden transcripts ──────────────────────────────────────────────────────────────────────

  @Test
  void transcript_loadWithSnapshot() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    recorder.clear();

    AggregateHistory history = store.load(A);

    assertThat(history.snapshotState()).isEqualTo(new ContractState("s"));
    assertThat(history.events()).hasSize(1);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + LOAD_SNAPSHOT,
            "#1 queryTimeout=driver",
            "#1 prepare " + LOAD_EVENTS,
            "#1 queryTimeout=driver",
            "#1 fetchSize=500",
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void transcript_loadDiscardedSnapshotReReadsOnAFreshConnection() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(UNREADABLE_SNAPSHOT);
    recorder.clear();

    AggregateHistory history = store.load(A);

    assertThat(history.snapshotState()).isNull();
    assertThat(history.events()).hasSize(2);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + LOAD_SNAPSHOT,
            "#1 queryTimeout=driver",
            "#1 prepare " + LOAD_EVENTS,
            "#1 queryTimeout=driver",
            "#1 fetchSize=500",
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close",
            "open #2",
            "#2 autoCommit=false",
            "#2 prepare " + LOAD_EVENTS,
            "#2 queryTimeout=driver",
            "#2 fetchSize=500",
            "#2 commit",
            "#2 autoCommit=true",
            "#2 close");
  }

  @Test
  void transcript_loadIgnoringSnapshot() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    recorder.clear();

    store.load(A, EventStore.IGNORE_SNAPSHOT);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 prepare " + LEGACY_PROBE,
            "#1 queryTimeout=driver",
            "#1 close",
            "open #2",
            "#2 autoCommit=false",
            "#2 prepare " + LOAD_EVENTS,
            "#2 queryTimeout=driver",
            "#2 fetchSize=500",
            "#2 commit",
            "#2 autoCommit=true",
            "#2 close");

    recorder.clear();
    store.load(A, EventStore.IGNORE_SNAPSHOT);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + LOAD_EVENTS,
            "#1 queryTimeout=driver",
            "#1 fetchSize=500",
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  @Test
  void transcript_readGlobalStream() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    recorder.clear();

    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10)).hasSize(2);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + READ_GLOBAL, "#1 queryTimeout=driver", "#1 close");
  }

  @Test
  void transcript_readStream() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    recorder.clear();

    assertThat(store.readStream(A, Version.initial(), 10)).hasSize(2);
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + READ_STREAM, "#1 queryTimeout=driver", "#1 close");
  }

  @Test
  void transcript_lastGlobalOffset() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    recorder.clear();

    assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(2));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + LAST_GLOBAL_OFFSET, "#1 queryTimeout=driver", "#1 close");
  }

  @Test
  void transcript_appendWithOutboxAndAudit() {
    PostgresEventStore store = wired();

    AppendResult result = store.append(A, events(A, 2), Version.initial());

    assertThat(result.globalOffsets()).containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(result.finalVersion()).isEqualTo(new Version(2));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + RESERVE_OFFSETS,
            "#1 prepare " + STREAM_HEAD_VERSION,
            "#1 prepare " + INSERT_EVENT,
            "#1 prepare " + OUTBOX_INSERT,
            "#1 prepare " + AUDIT_INSERT,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 prepare " + NOTIFY,
            "#1 queryTimeout=driver",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void transcript_appendWithKey() {
    PostgresEventStore store = wired();

    IdempotentAppendResult result =
        store.appendWithKey(A, events(A, 2), Version.initial(), IdempotencyKey.of("k"), COMMAND);

    assertThat(result.alreadyApplied()).isFalse();
    assertThat(result.globalOffsets()).containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(result.finalVersion()).isEqualTo(new Version(2));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + INBOX_CLAIM,
            "#1 prepare " + RESERVE_OFFSETS,
            "#1 prepare " + STREAM_HEAD_VERSION,
            "#1 prepare " + INSERT_EVENT,
            "#1 prepare " + INBOX_BACKFILL,
            "#1 prepare " + OUTBOX_INSERT,
            "#1 prepare " + AUDIT_INSERT,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 prepare " + NOTIFY,
            "#1 queryTimeout=driver",
            "#1 autoCommit=true",
            "#1 close");
  }

  /**
   * A key recorded before: the claim finds it, the transaction is rolled back and its connection
   * closed, and only then is the recorded row read, on a second connection.
   */
  @Test
  void transcript_appendWithARecordedKey() {
    PostgresEventStore store = wired();
    store.appendWithKey(A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND);
    recorder.clear();

    IdempotentAppendResult result =
        store.appendWithKey(A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND);

    assertThat(result)
        .isEqualTo(new IdempotentAppendResult(List.of(GlobalOffset.of(1)), new Version(1), true));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + INBOX_CLAIM,
            "#1 rollback",
            "#1 autoCommit=true",
            "#1 close",
            "open #2",
            "#2 prepare " + INBOX_FIND,
            "#2 close");
  }

  /**
   * A keyed command with no events claims its key and backfills no offset; it reserves no offsets,
   * makes no head check and sends no NOTIFY.
   */
  @Test
  void transcript_appendWithKeyAndNoEvents() {
    PostgresEventStore store = wired();

    IdempotentAppendResult result =
        store.appendWithKey(A, List.of(), new Version(3), IdempotencyKey.of("k"), COMMAND);

    assertThat(result).isEqualTo(new IdempotentAppendResult(List.of(), new Version(3), false));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + INBOX_CLAIM,
            "#1 prepare " + INBOX_BACKFILL,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
  }

  @Test
  void transcript_saveSnapshot() {
    PostgresEventStore store = plain();

    assertThat(store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 0)).isTrue();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + UPSERT_SNAPSHOT, "#1 queryTimeout=driver", "#1 close");
  }

  /**
   * The head check refuses inside the append transaction; the transaction is rolled back and the
   * connection closed before the conflict reaches the caller, unwrapped.
   */
  @Test
  void transcript_versionConflictRollsBackBeforeItSurfaces() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    recorder.clear();

    Throwable thrown = catchThrowable(() -> store.append(A, events(A, 1), new Version(1)));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Version conflict on stream 'order:A': expected version 1 but actual head is 2")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + RESERVE_OFFSETS,
            "#1 prepare " + STREAM_HEAD_VERSION,
            "#1 rollback",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(recorder.openConnections()).isZero();
  }

  @Test
  void transcript_emptyAppendBorrowsNoConnection() {
    PostgresEventStore store = wired();

    assertThat(store.append(A, List.of(), new Version(3)))
        .isEqualTo(new AppendResult(List.of(), new Version(3)));
    assertThat(recorder.transcript()).isEmpty();
  }

  // ── The negative global_offset row ──────────────────────────────────────────────────────────

  /**
   * A row stored with {@code global_offset = -1} (no constraint refuses it), a registered type and
   * a valid payload: the offset is parsed before the row's decoding is guarded, so the load fails
   * with the offset's own refusal, unwrapped.
   */
  @Test
  void negativeGlobalOffsetRow_isRawIllegalArgument() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 1), Version.initial());
    execute(copyOfFirstRow(-1, "N", "ContractEvent", 1));

    Throwable thrown = catchThrowable(() -> store.load(StreamId.of(ORDER, AggregateId.of("N"))));

    assertThat(thrown)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("globalOffset must be >= 0")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  /**
   * The same row with an unregistered type: the offset is parsed before the type is resolved, so
   * the offset's refusal wins over the unknown type.
   */
  @Test
  void negativeGlobalOffsetRowWithAnUnknownType_isStillIllegalArgument() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 1), Version.initial());
    execute(copyOfFirstRow(-1, "N", "RetiredEvent", 1));

    Throwable thrown = catchThrowable(() -> store.load(StreamId.of(ORDER, AggregateId.of("N"))));

    assertThat(thrown)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("globalOffset must be >= 0")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  /** A row of an unregistered type at a valid offset: the unknown type escapes unwrapped. */
  @Test
  void unknownEventTypeRow_isUnknownEventType() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 1), Version.initial());
    execute("UPDATE global_offset_sequence SET next_value = 2 WHERE id = 1");
    execute(copyOfFirstRow(2, "U", "RetiredEvent", 1));

    Throwable thrown = catchThrowable(() -> store.load(StreamId.of(ORDER, AggregateId.of("U"))));

    assertThat(thrown).isExactlyInstanceOf(UnknownEventTypeException.class).hasNoCause();
    assertThat(thrown.getMessage())
        .isEqualTo(
            "Unknown event type: 'RetiredEvent' is not registered. Registered event types:"
                + " [ContractEvent]");
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  // ── Serialization order ─────────────────────────────────────────────────────────────────────

  /**
   * An upcaster whose {@code currentVersion()} throws and an event whose serialization fails: the
   * schema version is asked first, so its failure wins, unwrapped, and no connection is borrowed.
   */
  @Test
  void schemaVersionIsAskedBeforeThePayloadIsSerialized() {
    var marker = new IllegalStateException("schema version asked");
    PostgresEventStore store = builder().upcasters(List.of(throwingUpcaster(marker))).build();
    recorder.clear();
    EventEnvelope unserializable = envelope(A, EventStoreContract.EVENT_TYPE, new Unserializable());

    assertThat(catchThrowable(() -> store.append(A, List.of(unserializable), Version.initial())))
        .isSameAs(marker);
    assertThat(recorder.connectionsOpened()).isZero();
  }

  @Test
  void unserializablePayload_isASerializationFailure() {
    PostgresEventStore store = plain();
    EventEnvelope unserializable = envelope(A, EventStoreContract.EVENT_TYPE, new Unserializable());

    Throwable thrown =
        catchThrowable(() -> store.append(A, List.of(unserializable), Version.initial()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to serialize event for stream: order:A");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(com.fasterxml.jackson.databind.JsonMappingException.class);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  @Test
  void nulCharacterInPayload_isRefusedBeforeAnyConnection() {
    PostgresEventStore store = wired();

    Throwable thrown =
        catchThrowable(
            () ->
                store.append(
                    A,
                    List.of(
                        envelope(A, EventStoreContract.EVENT_TYPE, new ContractEvent("a\u0000b"))),
                    Version.initial()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Serialized event payload for stream 'order:A' contains the Unicode NUL character"
                + " (\\u0000), which PostgreSQL jsonb cannot store — remove NUL characters from"
                + " the data before persisting")
        .hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  /**
   * Both the payload and the metadata of an event are serialized before either is checked for the
   * NUL character, so a metadata that cannot be serialized wins over a NUL in the payload.
   */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void metadataSerializationFailure_winsOverANulInThePayload() {
    PostgresEventStore store = plain();
    Map<String, String> poisoned = (Map) Map.of("k", new Unserializable());
    EventEnvelope event =
        new EventEnvelope(
            GlobalOffset.initial(),
            A,
            Version.initial(),
            EventStoreContract.EVENT_TYPE,
            new ContractEvent("a\u0000b"),
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                null,
                CorrelationId.of(UUID.randomUUID().toString()),
                null,
                null,
                Instant.parse("2026-11-23T10:00:00Z"),
                poisoned));

    Throwable thrown = catchThrowable(() -> store.append(A, List.of(event), Version.initial()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to serialize event for stream: order:A");
    assertThat(thrown.getCause())
        .isExactlyInstanceOf(com.fasterxml.jackson.databind.JsonMappingException.class);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  @Test
  void nulCharacterInSnapshotState_isRefusedBeforeAnyConnection() {
    PostgresEventStore store = plain();

    Throwable thrown =
        catchThrowable(() -> store.saveSnapshot(A, new Version(1), new ContractState("a\u0000b")));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Serialized snapshot state for stream 'order:A' contains the Unicode NUL character"
                + " (\\u0000), which PostgreSQL jsonb cannot store — remove NUL characters from"
                + " the data before persisting")
        .hasNoCause();
    assertThat(recorder.connectionsOpened()).isZero();
  }

  // ── Logging and the discard metric ──────────────────────────────────────────────────────────

  @Test
  void gapWarning_oncePerStreamPerInstance() throws SQLException {
    PostgresEventStore first = plain();
    PostgresEventStore second = plain();
    PostgresEventStore third = plain();
    first.append(A, events(A, 3), Version.initial());
    first.append(B, events(B, 3), Version.initial());
    execute("DELETE FROM event_stream WHERE version = 2");

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      first.load(A);
      first.load(A);
      first.load(A, EventStore.IGNORE_SNAPSHOT);
      assertThat(messages(logs, Level.WARN)).containsExactly(gapWarning("order:A"));

      second.load(A);
      second.load(A);
      first.load(B);
      first.load(B);
      assertThat(messages(logs, Level.WARN))
          .containsExactly(gapWarning("order:A"), gapWarning("order:A"), gapWarning("order:B"));

      // A fresh instance whose first load ignores snapshots warns on that load too.
      third.load(A, EventStore.IGNORE_SNAPSHOT);
      third.load(A);
      assertThat(messages(logs, Level.WARN))
          .containsExactly(
              gapWarning("order:A"),
              gapWarning("order:A"),
              gapWarning("order:B"),
              gapWarning("order:A"));
      assertThat(logs.events(Level.WARN))
          .allSatisfy(
              e -> {
                assertThat(e.getLoggerName()).isEqualTo(PostgresEventStore.class.getName());
                assertThat(e.getThrowableProxy()).isNull();
              });
      assertThat(logs.events(Level.ERROR)).isEmpty();
    }
  }

  @Test
  void legacyProbe_oncePerInstance() {
    PostgresEventStore first = plain();
    PostgresEventStore second = plain();
    first.append(A, events(A, 1), Version.initial());
    first.saveSnapshot(A, new Version(1), new ContractState("s"));
    recorder.clear();

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      first.load(A, EventStore.IGNORE_SNAPSHOT);
      first.load(A, EventStore.IGNORE_SNAPSHOT);
      assertThat(recorder.sql()).containsExactly(LEGACY_PROBE, LOAD_EVENTS, LOAD_EVENTS);
      second.load(A, EventStore.IGNORE_SNAPSHOT);
      assertThat(recorder.sql())
          .containsExactly(LEGACY_PROBE, LOAD_EVENTS, LOAD_EVENTS, LEGACY_PROBE, LOAD_EVENTS);
      String warning =
          "snapshot_store still has at least one row (e.g. state type ContractState), but a load"
              + " just ran with SnapshotPolicy.never() (EventStore.IGNORE_SNAPSHOT) — under"
              + " never() such rows are never read and never overwritten (never() also disables"
              + " new snapshots), so they will sit there indefinitely. If this deployment switched"
              + " from an active snapshot policy to never(), consider purging the now-unused rows"
              + " from snapshot_store. This warning is logged once.";
      assertThat(messages(logs, Level.WARN)).containsExactly(warning, warning);
      assertThat(logs.events(Level.WARN))
          .allSatisfy(
              e -> assertThat(e.getLoggerName()).isEqualTo(PostgresEventStore.class.getName()));
    }
  }

  /** A probe that fails is logged at DEBUG, and the load goes on. */
  @Test
  void legacyProbeFailure_isADebugLine() {
    PostgresEventStore store = plain();
    store.append(A, events(A, 1), Version.initial());
    recorder.beforePrepare(LEGACY_PROBE, conn -> execute(conn, NO_SUCH_SCHEMA));

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      assertThat(store.load(A, EventStore.IGNORE_SNAPSHOT).events()).hasSize(1);
      // The driver localizes the position line after the server's message ("Position: 24").
      assertThat(messages(logs, Level.DEBUG))
          .singleElement()
          .asString()
          .startsWith(
              "Could not probe snapshot_store for a legacy row: org.postgresql.util.PSQLException:"
                  + " ERROR: relation \"snapshot_store\" does not exist\n  ")
          .endsWith(": 24");
      assertThat(logs.events(Level.DEBUG).getFirst().getLoggerName())
          .isEqualTo(PostgresEventStore.class.getName());
      assertThat(logs.events(Level.WARN)).isEmpty();
    }
  }

  @Test
  void discardMetricAndErrorLog_deserializeFailure() throws SQLException {
    var metrics = new RecordingStreamRuneMetrics();
    PostgresEventStore store = builder().metrics(metrics).build();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(UNREADABLE_SNAPSHOT);

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      AggregateHistory history = store.load(A);

      assertThat(history.snapshotState()).isNull();
      assertThat(history.lastSnapshotVersion()).isEqualTo(Version.initial());
      assertThat(history.events()).hasSize(2);
      List<ILoggingEvent> errors = logs.events(Level.ERROR);
      assertThat(errors).hasSize(1);
      ILoggingEvent error = errors.getFirst();
      assertThat(error.getLoggerName()).isEqualTo(PostgresEventStore.class.getName());
      assertThat(error.getFormattedMessage())
          .startsWith(
              "Snapshot for stream order:A is undeserializable"
                  + " (com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: ")
          .endsWith(
              "); discarding the derived snapshot cache and replaying all events from the"
                  + " beginning. This usually means an aggregate-state field was renamed or removed"
                  + " without bumping the snapshot version — bump SnapshotPolicy's snapshotVersion"
                  + " so fresh snapshots are written.");
      assertThat(error.getThrowableProxy().getClassName())
          .isEqualTo("com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException");
      assertThat(logs.events(Level.WARN)).isEmpty();
    }
    assertThat(metrics.samples(MetricNames.SNAPSHOTS_DISCARDED))
        .containsExactly(
            new RecordingStreamRuneMetrics.Sample(
                MetricNames.SNAPSHOTS_DISCARDED,
                Map.of(MetricNames.TAG_REASON, "deserialize_failure"),
                1));
  }

  @Test
  void discardMetricAndErrorLog_migrationFailure() {
    var metrics = new RecordingStreamRuneMetrics();
    PostgresEventStore writer = plain();
    writer.append(A, events(A, 2), Version.initial());
    writer.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 0);
    PostgresEventStore store =
        builder().metrics(metrics).migrations(List.of(brokenMigration())).build();
    recorder.clear();

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      AggregateHistory history = store.load(A, 2);

      assertThat(history.snapshotState()).isNull();
      assertThat(history.events()).hasSize(2);
      List<ILoggingEvent> errors = logs.events(Level.ERROR);
      assertThat(errors).hasSize(1);
      assertThat(errors.getFirst().getLoggerName()).isEqualTo(PostgresEventStore.class.getName());
      assertThat(errors.getFirst().getFormattedMessage())
          .isEqualTo(
              "Snapshot migration for stream order:A failed (java.lang.IllegalStateException:"
                  + " migration bug); discarding the derived snapshot cache and replaying all"
                  + " events from the beginning. This means a registered SnapshotMigration step"
                  + " has a bug — inspect the migration chain for the version this stream's"
                  + " snapshot was stored at.");
      assertThat(errors.getFirst().getThrowableProxy().getClassName())
          .isEqualTo("java.lang.IllegalStateException");
    }
    assertThat(metrics.samples(MetricNames.SNAPSHOTS_DISCARDED))
        .containsExactly(
            new RecordingStreamRuneMetrics.Sample(
                MetricNames.SNAPSHOTS_DISCARDED,
                Map.of(MetricNames.TAG_REASON, "migration_failure"),
                1));
    // The narrowed read and the replay from the start, each on its own connection.
    assertThat(recorder.sql()).containsExactly(LOAD_SNAPSHOT, LOAD_EVENTS, LOAD_EVENTS);
    assertThat(recorder.connectionsOpened()).isEqualTo(2);
  }

  /** A metrics backend that fails while counting a discard is a WARN; the load goes on. */
  @Test
  void aFailingDiscardMetric_isAWarnAndTheLoadGoesOn() throws SQLException {
    var failure = new IllegalStateException("metrics down");
    StreamRuneMetrics failing =
        new RecordingStreamRuneMetrics() {
          @Override
          public void recordSnapshotDiscarded(String reason) {
            throw failure;
          }
        };
    PostgresEventStore store = builder().metrics(failing).build();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(UNREADABLE_SNAPSHOT);

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      assertThat(store.load(A).events()).hasSize(2);
      assertThat(messages(logs, Level.WARN))
          .containsExactly(
              "Metrics recordSnapshotDiscarded(deserialize_failure) failed for stream order:A");
      assertThat(logs.events(Level.WARN).getFirst().getThrowableProxy().getMessage())
          .isEqualTo("metrics down");
      assertThat(logs.events(Level.ERROR)).hasSize(1);
    }
  }

  // ── The quiet snapshot branches: no discard, no re-read, no log, no metric ──────────────────

  /**
   * A stored snapshot at a version the load does not expect and no migration chain: the load reads
   * every event in its one transaction and starts from no snapshot, quietly.
   */
  @Test
  void incompatibleSnapshotVersionWithoutAChain_readsEveryEventOnce() {
    var metrics = new RecordingStreamRuneMetrics();
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().metrics(metrics).tracing(tracing).build();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, 0);
    recorder.clear();
    tracing.clear();

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      AggregateHistory history = store.load(A, 2);

      assertQuietReplay(history, logs);
    }
    assertQuietTranscriptMetricsAndSpans(metrics, tracing);
  }

  /**
   * A stored snapshot whose state type is no longer registered: the load reads every event in its
   * one transaction and starts from no snapshot, quietly.
   */
  @Test
  void unregisteredSnapshotStateType_readsEveryEventOnce() throws SQLException {
    var metrics = new RecordingStreamRuneMetrics();
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().metrics(metrics).tracing(tracing).build();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute("UPDATE snapshot_store SET state_type = 'RetiredState'");
    recorder.clear();
    tracing.clear();

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      AggregateHistory history = store.load(A);

      assertQuietReplay(history, logs);
    }
    assertQuietTranscriptMetricsAndSpans(metrics, tracing);
  }

  private static void assertQuietReplay(AggregateHistory history, LogCapture logs) {
    assertThat(history.snapshotState()).isNull();
    assertThat(history.lastSnapshotVersion()).isEqualTo(Version.initial());
    assertThat(history.version()).isEqualTo(new Version(2));
    assertThat(history.events()).extracting(e -> e.version().value()).containsExactly(1L, 2L);
    assertThat(logs.events(Level.ERROR)).isEmpty();
    assertThat(logs.events(Level.WARN)).isEmpty();
    assertThat(logs.events(Level.INFO)).isEmpty();
  }

  private void assertQuietTranscriptMetricsAndSpans(
      RecordingStreamRuneMetrics metrics, RecordingTracing tracing) {
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 prepare " + LOAD_SNAPSHOT,
            "#1 queryTimeout=driver",
            "#1 prepare " + LOAD_EVENTS,
            "#1 queryTimeout=driver",
            "#1 fetchSize=500",
            "#1 commit",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(metrics.samples(MetricNames.SNAPSHOTS_DISCARDED)).isEmpty();
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD).attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID, "order:A",
                TraceAttributes.SNAPSHOT_OUTCOME, "DISCARDED",
                TraceAttributes.STREAM_VERSION, 0L));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).attributes())
        .containsEntry(TraceAttributes.EVENT_COUNT, 2L)
        .containsEntry(TraceAttributes.SNAPSHOT_USED, false);
  }

  /** A purged snapshot row: no snapshot, no snapshot span, and its purge generation is carried. */
  @Test
  void purgedSnapshotRow_startsNoSnapshotSpan_andCarriesItsGeneration() throws SQLException {
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().tracing(tracing).build();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(
        "UPDATE snapshot_store SET state_type = NULL, state_payload = NULL, purge_generation = 3");
    recorder.clear();
    tracing.clear();

    AggregateHistory history = store.load(A);

    assertThat(history.snapshotState()).isNull();
    assertThat(history.snapshotPurgeGeneration()).isEqualTo(3L);
    assertThat(history.events()).hasSize(2);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(recorder.sql()).containsExactly(LOAD_SNAPSHOT, LOAD_EVENTS);
  }

  @Test
  void globalReadHole_isAWarn() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 4), Version.initial());
    execute("DELETE FROM event_stream WHERE global_offset = 3");

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      assertThat(store.readGlobalStream(GlobalOffset.initial(), 10))
          .extracting(e -> e.globalOffset().value())
          .containsExactly(1L, 2L);
      assertThat(messages(logs, Level.WARN))
          .containsExactly(
              "non-contiguous global_offset observed after GlobalOffset{0}: expected"
                  + " 3, got 4");
    }
  }

  @Test
  void snapshotNotStoredForItsPurgeGeneration_isAnInfoLine() throws SQLException {
    PostgresEventStore store = plain();
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute("UPDATE snapshot_store SET purge_generation = 1");

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      assertThat(store.saveSnapshot(A, new Version(2), new ContractState("t"), 1, 0)).isFalse();
      assertThat(messages(logs, Level.INFO))
          .containsExactly(
              "Snapshot of stream order:A at version 2 not stored: its snapshot_store row carries"
                  + " a purge generation other than 0, so a GDPR forget purged the stream's"
                  + " snapshot after the state being saved was loaded; the next command loads and"
                  + " snapshots again");
      assertThat(logs.events(Level.INFO).getFirst().getLoggerName())
          .isEqualTo(PostgresEventStore.class.getName());
    }
  }

  // ── Decryption never runs under a held connection ───────────────────────────────────────────

  @Test
  void snapshotDecryptRunsWithNoConnectionOpen() {
    var engine = new ConnectionCountingEngine(recorder);
    PostgresEventStore store = secretStore(engine);
    store.append(A, List.of(secret("a"), secret("b")), Version.initial());
    store.saveSnapshot(A, new Version(1), new SecretState("subject", "a@example.com"));
    engine.openAtDecrypt.clear();

    AggregateHistory history = store.load(A);
    assertThat(history.snapshotState()).isEqualTo(new SecretState("subject", "a@example.com"));
    assertThat(history.events()).hasSize(1);
    assertThat(engine.openAtDecrypt).hasSize(2).containsOnly(0);

    engine.openAtDecrypt.clear();
    assertThat(store.load(A, EventStore.IGNORE_SNAPSHOT).events()).hasSize(2);
    assertThat(store.readStream(A, Version.initial(), 10)).hasSize(2);
    assertThat(store.readGlobalStream(GlobalOffset.initial(), 10)).hasSize(2);
    assertThat(engine.openAtDecrypt).hasSize(6).containsOnly(0);
  }

  /** The replay after a discarded snapshot decrypts its events with no connection open too. */
  @Test
  void discardedSnapshotReplayDecryptsWithNoConnectionOpen() throws SQLException {
    var engine = new ConnectionCountingEngine(recorder);
    PostgresEventStore store = secretStore(engine);
    store.append(A, List.of(secret("a"), secret("b")), Version.initial());
    store.saveSnapshot(A, new Version(1), new SecretState("subject", "a@example.com"));
    execute("UPDATE snapshot_store SET state_payload = '{\"subject\":\"subject\",\"unknown\":1}'");
    engine.openAtDecrypt.clear();
    recorder.clear();

    AggregateHistory history = store.load(A);

    assertThat(history.snapshotState()).isNull();
    assertThat(history.events()).hasSize(2);
    assertThat(recorder.connectionsOpened()).isEqualTo(2);
    assertThat(engine.openAtDecrypt).hasSize(2).containsOnly(0);
  }

  // ── After the commit nothing fails the append ───────────────────────────────────────────────

  @Test
  void closeFailureAfterCommit_stillReturnsTheAppend() {
    PostgresEventStore store = wired();
    recorder.failNextCloseAfterCommit();

    AppendResult result = store.append(A, events(A, 2), Version.initial());

    assertThat(result.globalOffsets()).containsExactly(GlobalOffset.of(1), GlobalOffset.of(2));
    assertThat(result.finalVersion()).isEqualTo(new Version(2));
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + RESERVE_OFFSETS,
            "#1 prepare " + STREAM_HEAD_VERSION,
            "#1 prepare " + INSERT_EVENT,
            "#1 prepare " + OUTBOX_INSERT,
            "#1 prepare " + AUDIT_INSERT,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 prepare " + NOTIFY,
            "#1 queryTimeout=driver",
            "#1 autoCommit=true",
            "#1 close");
    assertThat(store.load(A).events()).hasSize(2);
  }

  @Test
  void closeFailureAfterCommit_stillReturnsTheKeyedAppend() {
    PostgresEventStore store = wired();
    recorder.failNextCloseAfterCommit();

    IdempotentAppendResult result =
        store.appendWithKey(A, events(A, 2), Version.initial(), IdempotencyKey.of("k"), COMMAND);

    assertThat(result)
        .isEqualTo(
            new IdempotentAppendResult(
                List.of(GlobalOffset.of(1), GlobalOffset.of(2)), new Version(2), false));
    assertThat(recorder.transcript()).last().isEqualTo("#1 close");
    assertThat(store.load(A).events()).hasSize(2);
    assertThat(
            store.appendWithKey(
                A, events(A, 2), Version.initial(), IdempotencyKey.of("k"), COMMAND))
        .isEqualTo(
            new IdempotentAppendResult(
                List.of(GlobalOffset.of(1), GlobalOffset.of(2)), new Version(2), true));
  }

  /**
   * The connection dies right after the commit, before the NOTIFY: the NOTIFY's failure is
   * swallowed, the autocommit restore's failure is a DEBUG line, and the append returns.
   */
  @Test
  void connectionLostAfterCommit_stillReturnsTheAppend() {
    PostgresEventStore store = plain();
    recorder.beforePrepare(NOTIFY, Connection::close);

    try (LogCapture logs = LogCapture.attach(PostgresEventStore.class)) {
      AppendResult result = store.append(A, events(A, 2), Version.initial());

      assertThat(result)
          .isEqualTo(
              new AppendResult(List.of(GlobalOffset.of(1), GlobalOffset.of(2)), new Version(2)));
      // The tail is the driver's (localizable) message, "This connection has been closed.".
      assertThat(messages(logs, Level.DEBUG))
          .singleElement()
          .asString()
          .startsWith(
              "Failed to restore autoCommit before returning connection to pool (benign — pool"
                  + " resets on return): ");
      assertThat(logs.events(Level.DEBUG).getFirst().getLoggerName())
          .isEqualTo(PostgresEventStore.class.getName());
      assertThat(logs.events(Level.WARN)).isEmpty();
    }
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1",
            "#1 autoCommit=false",
            "#1 execute " + BEGIN_APPEND,
            "#1 prepare " + RESERVE_OFFSETS,
            "#1 prepare " + STREAM_HEAD_VERSION,
            "#1 prepare " + INSERT_EVENT,
            "#1 commit",
            "#1 autoCommit=true",
            "#1 prepare " + NOTIFY,
            "#1 autoCommit=true",
            "#1 close");
    assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.of(2));
  }

  // ── Failure messages ────────────────────────────────────────────────────────────────────────

  /** A unique violation behind the head check is the version conflict at the expected version. */
  @Test
  void failureMessages_retryableConflict() {
    PostgresEventStore store = wired();
    store.append(B, events(B, 2), Version.initial());
    recorder.clear();
    recorder.beforePrepare(INSERT_EVENT, PostgresEventStoreCharacterizationTest::moveRowTwoToA);

    Throwable thrown = catchThrowable(() -> store.append(A, events(A, 1), Version.initial()));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Version conflict on stream 'order:A' at expected version 0");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(thrown.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
    assertThat(recorder.transcript()).endsWith("#1 rollback", "#1 autoCommit=true", "#1 close");
    assertThat(store.readStream(B, Version.initial(), 10)).hasSize(2);
  }

  @Test
  void failureMessages_keyedRetryableConflict() {
    PostgresEventStore store = wired();
    store.append(B, events(B, 2), Version.initial());
    recorder.clear();
    recorder.beforePrepare(INSERT_EVENT, PostgresEventStoreCharacterizationTest::moveRowTwoToA);

    Throwable thrown =
        catchThrowable(
            () ->
                store.appendWithKey(
                    A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND));

    assertThat(thrown)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Concurrent modification of stream: order:A");
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(thrown.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
    assertThat(recorder.transcript()).endsWith("#1 rollback", "#1 autoCommit=true", "#1 close");
  }

  /** A row already at the reserved offset is the desynced counter, never a version conflict. */
  @Test
  void failureMessages_globalOffsetCollision() {
    PostgresEventStore store = wired();
    recorder.beforePrepare(INSERT_EVENT, conn -> insertOnSameConnection(conn, 1, "OTHER", 1));

    Throwable thrown = catchThrowable(() -> store.append(A, events(A, 1), Version.initial()));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("global_offset collision while appending to stream 'order:A" + DESYNC_ADVICE);
    assertThat(thrown.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
  }

  @Test
  void failureMessages_keyedGlobalOffsetCollision() {
    PostgresEventStore store = wired();
    recorder.beforePrepare(INSERT_EVENT, conn -> insertOnSameConnection(conn, 1, "OTHER", 1));

    Throwable thrown =
        catchThrowable(
            () ->
                store.appendWithKey(
                    A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("global_offset collision while appending to stream 'order:A" + DESYNC_ADVICE);
    assertThat(thrown.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
  }

  /** Any other storage failure of an append, unkeyed and keyed. */
  @Test
  void failureMessages_otherAppendFailures() {
    PostgresEventStore store = wired();
    recorder.beforePrepare(INSERT_EVENT, PostgresEventStoreCharacterizationTest::failInserts);
    Throwable unkeyed = catchThrowable(() -> store.append(A, events(A, 1), Version.initial()));
    recorder.beforePrepare(INSERT_EVENT, PostgresEventStoreCharacterizationTest::failInserts);
    Throwable keyed =
        catchThrowable(
            () ->
                store.appendWithKey(
                    A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND));

    assertThat(unkeyed)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to append events to stream: order:A");
    assertThat(unkeyed.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
    assertThat(unkeyed.getSuppressed()).isEmpty();
    assertThat(keyed)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed idempotent append to stream: order:A");
    assertThat(keyed.getCause()).isExactlyInstanceOf(java.sql.BatchUpdateException.class);
    assertThat(keyed.getSuppressed()).isEmpty();
    assertThat(store.lastGlobalOffset()).isEqualTo(GlobalOffset.initial());
  }

  @Test
  void failureMessages_counterRowMissing() throws SQLException {
    PostgresEventStore store = plain();
    execute("DELETE FROM global_offset_sequence");
    try {
      Throwable thrown = catchThrowable(() -> store.append(A, events(A, 1), Version.initial()));

      assertThat(thrown)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("global_offset_sequence row missing")
          .hasNoCause();
      assertThat(recorder.transcript()).endsWith("#1 rollback", "#1 autoCommit=true", "#1 close");
    } finally {
      execute("INSERT INTO global_offset_sequence (id, next_value) VALUES (1, 0)");
    }
  }

  /** The key's row is gone between the lost claim and its read. */
  @Test
  void failureMessages_inboxConflictWithoutRow() {
    PostgresEventStore store = wired();
    store.appendWithKey(A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND);
    recorder.beforePrepare(INBOX_FIND, conn -> execute(conn, "DELETE FROM command_inbox"));

    Throwable thrown =
        catchThrowable(
            () ->
                store.appendWithKey(
                    A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND));

    assertThat(thrown)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Inbox conflict but no row found for key: k")
        .hasNoCause();
  }

  @Test
  void failureMessages_reads() {
    PostgresEventStore store = plain();

    recorder.beforePrepare(LOAD_SNAPSHOT, conn -> execute(conn, NO_SUCH_SCHEMA));
    Throwable load = catchThrowable(() -> store.load(A));
    recorder.beforePrepare(LOAD_EVENTS, conn -> execute(conn, NO_SUCH_SCHEMA));
    Throwable ignoring = catchThrowable(() -> store.load(A, EventStore.IGNORE_SNAPSHOT));
    recorder.beforePrepare(READ_GLOBAL, conn -> execute(conn, NO_SUCH_SCHEMA));
    Throwable global = catchThrowable(() -> store.readGlobalStream(GlobalOffset.of(3), 10));
    recorder.beforePrepare(READ_STREAM, conn -> execute(conn, NO_SUCH_SCHEMA));
    Throwable stream = catchThrowable(() -> store.readStream(A, Version.initial(), 10));
    recorder.beforePrepare(LAST_GLOBAL_OFFSET, conn -> execute(conn, NO_SUCH_SCHEMA));
    Throwable last = catchThrowable(store::lastGlobalOffset);

    assertStorageFailure(load, "Failed to load stream: order:A");
    assertStorageFailure(ignoring, "Failed to load stream: order:A");
    assertStorageFailure(global, "Failed to read global stream from offset: GlobalOffset{3}");
    assertStorageFailure(stream, "Failed to read stream: order:A");
    assertStorageFailure(last, "Failed to read last global offset");
    assertThat(recorder.openConnections()).isZero();
  }

  /** The replay after a discarded snapshot fails with the load's own message. */
  @Test
  void failureMessages_replayAfterADiscard() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));
    execute(UNREADABLE_SNAPSHOT);
    // The narrowed read passes; the replay, the second LOAD_EVENTS, fails.
    recorder.beforePrepare(
        LOAD_EVENTS,
        first -> recorder.beforePrepare(LOAD_EVENTS, conn -> execute(conn, NO_SUCH_SCHEMA)));

    Throwable thrown = catchThrowable(() -> store.load(A));

    assertStorageFailure(thrown, "Failed to load stream: order:A");
  }

  @Test
  void failureMessages_snapshotStorage() {
    PostgresEventStore store = plain();
    recorder.beforePrepare(UPSERT_SNAPSHOT, conn -> execute(conn, NO_SUCH_SCHEMA));

    assertStorageFailure(
        catchThrowable(() -> store.saveSnapshot(A, new Version(1), new ContractState("s"))),
        "Failed to save snapshot for stream: order:A");
  }

  private static void assertStorageFailure(Throwable thrown, String message) {
    assertThat(thrown).isExactlyInstanceOf(EventStoreException.class).hasMessage(message);
    assertThat(thrown.getCause()).isExactlyInstanceOf(PSQLException.class);
    assertThat(((PSQLException) thrown.getCause()).getSQLState()).isEqualTo("42P01");
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void failureMessages_snapshotValidation() {
    PostgresEventStore store = plain();
    record Unregistered(String x) implements AggregateState {}

    assertRefusal(
        () -> store.saveSnapshot(A, new Version(1), null, 1, 0),
        IllegalArgumentException.class,
        "state must not be null");
    assertRefusal(
        () -> store.saveSnapshot(A, new Version(1), new ContractState("s"), 0, 0),
        IllegalArgumentException.class,
        "snapshotVersion must be >= 1");
    assertRefusal(
        () -> store.saveSnapshot(A, new Version(1), new ContractState("s"), 1, -1),
        IllegalArgumentException.class,
        "snapshotPurgeGeneration must not be negative, got -1");
    assertRefusal(
        () -> store.saveSnapshot(null, new Version(1), new ContractState("s"), 1, 0),
        NullPointerException.class,
        "streamId is required");

    Throwable unresolved =
        catchThrowable(() -> store.saveSnapshot(A, new Version(1), new Unregistered("x")));
    assertThat(unresolved)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "Snapshot state type 'Unregistered' does not resolve in the EventTypeRegistry —"
                + " register "
                + Unregistered.class.getName()
                + " under its simple name so the snapshot can be rehydrated on load");
    assertThat(unresolved.getCause()).isExactlyInstanceOf(UnknownEventTypeException.class);

    assertRefusal(
        () -> store.saveSnapshot(A, new Version(1), new Elsewhere.ContractState("x")),
        EventStoreException.class,
        "Snapshot state type 'ContractState' resolves to"
            + " org.streamrune.test.EventStoreContract$ContractState in the EventTypeRegistry, but"
            + " the state being saved is "
            + Elsewhere.ContractState.class.getName()
            + " — snapshots are stored by simple class name, so two state classes sharing a"
            + " simple name would rehydrate as the wrong type");
    assertThat(recorder.connectionsOpened()).isZero();
  }

  /**
   * A null version reaches the statement: the store borrows a connection and fails there with the
   * JVM's own null-pointer message.
   */
  @Test
  void failureMessages_snapshotWithANullVersion() {
    PostgresEventStore store = plain();

    Throwable thrown =
        catchThrowable(() -> store.saveSnapshot(A, null, new ContractState("s"), 1, 0));

    assertThat(thrown)
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage(
            "Cannot invoke \"org.streamrune.core.types.Version.value()\" because \"version\" is"
                + " null");
    assertThat(recorder.transcript())
        .containsExactly(
            "open #1", "#1 prepare " + UPSERT_SNAPSHOT, "#1 queryTimeout=driver", "#1 close");
  }

  @Test
  void failureMessages_argumentRefusals() {
    PostgresEventStore store = wired();
    PostgresEventStore bare = plain();
    OutboxEntry entry = OutboxEntry.pending(OutboxEntryId.of("e"), "{}", "T", A);

    assertRefusal(
        () -> store.load(A, -2),
        IllegalArgumentException.class,
        "expectedSnapshotVersion must be >= -1 (IGNORE_SNAPSHOT skips reading any snapshot, 0"
            + " skips the version check), got -2");
    assertRefusal(
        () -> store.readGlobalStream(GlobalOffset.initial(), -1),
        IllegalArgumentException.class,
        "maxCount must not be negative, got -1");
    assertRefusal(
        () -> store.readStream(A, Version.initial(), -1),
        IllegalArgumentException.class,
        "maxCount must not be negative, got -1");
    assertRefusal(
        () -> bare.append(A, events(A, 1), Version.initial(), List.of(entry)),
        IllegalStateException.class,
        "Outbox entries were passed but no PostgresOutboxStore is configured on this event"
            + " store. Configure a PostgresOutboxStore together with an OutboxEventMapper: on the"
            + " builder or factory, or as the outbox bean pair of an integration");
    assertRefusal(
        () -> store.append(A, List.of(), Version.initial(), List.of(entry)),
        IllegalArgumentException.class,
        "outboxEntries were passed with an empty events list — outbox entries are persisted"
            + " atomically with the events of this append and cannot be appended alone. To"
            + " emit an outbox entry that has no accompanying domain event, persist it"
            + " directly with OutboxStore.save(entry), one entry per call — that write commits"
            + " on its own connection and is atomic with nothing else.");
    assertRefusal(
        () ->
            bare.appendWithKey(A, events(A, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND),
        UnsupportedOperationException.class,
        "This PostgresEventStore was built without a command inbox");
    assertThat(recorder.connectionsOpened()).isZero();
  }

  private static void assertRefusal(
      ThrowingCallable call, Class<? extends Throwable> type, String message) {
    Throwable thrown = catchThrowable(call);
    assertThat(thrown).isExactlyInstanceOf(type).hasMessage(message).hasNoCause();
    assertThat(thrown.getSuppressed()).isEmpty();
  }

  @Test
  void failureMessages_rowsThatCannotBeRead() throws SQLException {
    PostgresEventStore store = plain();
    store.append(A, events(A, 1), Version.initial());
    execute("UPDATE global_offset_sequence SET next_value = 3 WHERE id = 1");
    execute(copyOfFirstRow(2, "S", "ContractEvent", 5));
    execute(copyOfFirstRow(3, "J", "ContractEvent", 1));
    execute("UPDATE event_stream SET payload = '{\"unknown\":1}' WHERE aggregate_id = 'J'");

    Throwable ahead = catchThrowable(() -> store.load(StreamId.of(ORDER, AggregateId.of("S"))));
    assertThat(ahead)
        .isExactlyInstanceOf(EventSchemaAheadException.class)
        .hasMessage(PostgresEventStore.schemaAheadMessage("ContractEvent", "order:S", 5, 1))
        .hasNoCause();

    Throwable corrupt = catchThrowable(() -> store.load(StreamId.of(ORDER, AggregateId.of("J"))));
    assertThat(corrupt)
        .isExactlyInstanceOf(EventDeserializationException.class)
        .hasMessage("Failed to deserialize event data for stream: order:J");
    assertThat(corrupt.getCause())
        .isExactlyInstanceOf(
            com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException.class);
  }

  /** A transient crypto failure is a plain EventStoreException, for snapshots and events. */
  @Test
  void failureMessages_transientCryptoFailures() {
    var engine = new ConnectionCountingEngine(recorder);
    PostgresEventStore store = secretStore(engine);
    store.append(A, List.of(secret("a")), Version.initial());
    store.saveSnapshot(A, new Version(1), new SecretState("subject", "a@example.com"));
    engine.decryptFailure = new CryptoOperationException("key store unreachable");

    Throwable snapshot = catchThrowable(() -> store.load(A));
    assertThat(snapshot)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to deserialize snapshot state for stream: order:A");
    assertThat(snapshot.getCause()).isSameAs(engine.decryptFailure);

    Throwable event = catchThrowable(() -> store.load(A, EventStore.IGNORE_SNAPSHOT));
    assertThat(event)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to deserialize event data for stream: order:A");
    assertThat(event.getCause()).isSameAs(engine.decryptFailure);
  }

  // ── Spans ───────────────────────────────────────────────────────────────────────────────────

  @Test
  void spans_load() throws SQLException {
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().tracing(tracing).build();
    store.append(A, events(A, 2), Version.initial());
    store.append(B, events(B, 2), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("s"));

    tracing.clear();
    store.load(A);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    var load = tracing.only(TraceAttributes.EVENT_STORE_LOAD);
    var snapshot = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    var deserialize = tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(load.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(load.attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID,
                "order:A",
                TraceAttributes.AGGREGATE_TYPE,
                "order",
                TraceAttributes.EVENT_COUNT,
                1L,
                TraceAttributes.SNAPSHOT_USED,
                true));
    assertThat(snapshot.attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID, "order:A",
                TraceAttributes.SNAPSHOT_OUTCOME, "HIT",
                TraceAttributes.STREAM_VERSION, 1L));
    assertThat(deserialize.attributes())
        .isEqualTo(Map.of(TraceAttributes.EVENT_COUNT, 1L, TraceAttributes.UPCAST_COUNT, 0L));
    assertThat(snapshot.parent).isSameAs(load);
    assertThat(deserialize.parent).isSameAs(load);
    assertThat(List.of(load, snapshot, deserialize))
        .allSatisfy(
            s -> {
              assertThat(s.kind).isEqualTo(SpanKind.INTERNAL);
              assertThat(s.ok).isTrue();
              assertThat(s.closed).isTrue();
              assertThat(s.startTime).isNull();
            });

    execute(UNREADABLE_SNAPSHOT);
    tracing.clear();
    store.load(A);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD).attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID, "order:A",
                TraceAttributes.SNAPSHOT_OUTCOME, "DISCARDED",
                TraceAttributes.STREAM_VERSION, 0L));
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD).attributes())
        .containsEntry(TraceAttributes.EVENT_COUNT, 2L)
        .containsEntry(TraceAttributes.SNAPSHOT_USED, false);
    assertThat(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE).attributes())
        .isEqualTo(Map.of(TraceAttributes.EVENT_COUNT, 2L, TraceAttributes.UPCAST_COUNT, 0L));

    tracing.clear();
    store.load(B);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_LOAD, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
  }

  @Test
  void spans_read() {
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().tracing(tracing).build();
    store.append(A, events(A, 2), Version.initial());

    tracing.clear();
    store.readGlobalStream(GlobalOffset.initial(), 10);
    store.readStream(A, Version.initial(), 10);
    assertThat(tracing.names()).as("no current span, no read span").isEmpty();

    var parent =
        tracing.inside(
            "parent",
            () -> {
              store.readGlobalStream(GlobalOffset.initial(), 10);
              store.readStream(A, new Version(1), 10);
              store.readStream(A, new Version(2), 10);
            });
    List<RecordingTracing.RecordedSpan> reads =
        tracing.spans.stream()
            .filter(s -> s.name.equals(TraceAttributes.EVENT_STORE_READ))
            .toList();
    assertThat(reads).hasSize(2);
    assertThat(reads.get(0).attributes())
        .isEqualTo(Map.of(TraceAttributes.EVENT_COUNT, 2L, TraceAttributes.READ_KIND, "GLOBAL"));
    assertThat(reads.get(1).attributes())
        .isEqualTo(Map.of(TraceAttributes.EVENT_COUNT, 1L, TraceAttributes.READ_KIND, "STREAM"));
    assertThat(reads)
        .allSatisfy(
            s -> {
              assertThat(s.parent).isSameAs(parent);
              assertThat(s.startTime).isNotNull();
              assertThat(s.kind).isEqualTo(SpanKind.INTERNAL);
              assertThat(s.ok).isTrue();
              assertThat(s.closed).isTrue();
            });
  }

  @Test
  void spans_append() {
    var tracing = new RecordingTracing();
    PostgresEventStore store =
        builder()
            .tracing(tracing)
            .outboxStore(new PostgresOutboxStore(recorder))
            .outboxEventMapper(EventStoreContract.OUTBOX_MAPPER)
            .commandInbox(new PostgresCommandInbox(recorder))
            .build();

    store.append(A, events(A, 2), Version.initial());
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_APPEND, TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(append.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(append.attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID,
                "order:A",
                TraceAttributes.AGGREGATE_TYPE,
                "order",
                TraceAttributes.EVENT_COUNT,
                2L,
                TraceAttributes.EXPECTED_VERSION,
                0L,
                TraceAttributes.OUTBOX_ENTRY_COUNT,
                2L,
                TraceAttributes.APPEND_OUTCOME,
                "APPENDED"));
    assertThat(append.ok).isTrue();
    var serialize = tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(serialize.attributes()).isEqualTo(Map.of(TraceAttributes.EVENT_COUNT, 2L));
    assertThat(serialize.parent).isSameAs(append);
    assertThat(serialize.ok).isTrue();

    tracing.clear();
    catchThrowable(() -> store.append(A, events(A, 1), new Version(1)));
    var conflict = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(conflict.attributes())
        .containsEntry(TraceAttributes.APPEND_OUTCOME, "CONFLICT")
        .containsEntry(TraceAttributes.EXPECTED_VERSION, 1L)
        .containsEntry(TraceAttributes.OUTBOX_ENTRY_COUNT, 1L);
    assertThat(conflict.ok).isFalse();
    assertThat(conflict.error).isNull();
    assertThat(conflict.closed).isTrue();

    tracing.clear();
    store.appendWithKey(B, events(B, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND);
    store.appendWithKey(B, events(B, 1), Version.initial(), IdempotencyKey.of("k"), COMMAND);
    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_APPEND,
            TraceAttributes.EVENT_STORE_APPEND_SERIALIZE,
            TraceAttributes.EVENT_STORE_APPEND,
            TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    List<RecordingTracing.RecordedSpan> keyed =
        tracing.spans.stream()
            .filter(s -> s.name.equals(TraceAttributes.EVENT_STORE_APPEND))
            .toList();
    assertThat(keyed.get(0).attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID,
                "order:B",
                TraceAttributes.AGGREGATE_TYPE,
                "order",
                TraceAttributes.EVENT_COUNT,
                1L,
                TraceAttributes.EXPECTED_VERSION,
                0L,
                TraceAttributes.OUTBOX_ENTRY_COUNT,
                1L,
                TraceAttributes.APPEND_OUTCOME,
                "APPENDED"));
    assertThat(keyed.get(1).attributes())
        .containsEntry(TraceAttributes.APPEND_OUTCOME, "DUPLICATE");
    assertThat(keyed).allSatisfy(s -> assertThat(s.ok).isTrue());

    tracing.clear();
    recorder.beforePrepare(INSERT_EVENT, PostgresEventStoreCharacterizationTest::failInserts);
    catchThrowable(() -> store.append(A, events(A, 1), new Version(2)));
    var failed = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(failed.attributes()).doesNotContainKey(TraceAttributes.APPEND_OUTCOME);
    assertThat(failed.error).isExactlyInstanceOf(EventStoreException.class);
    assertThat(failed.ok).isFalse();
    assertThat(failed.closed).isTrue();
  }

  @Test
  void spans_snapshotSave() throws SQLException {
    var tracing = new RecordingTracing();
    PostgresEventStore store = builder().tracing(tracing).build();

    store.saveSnapshot(A, new Version(3), new ContractState("s"));
    var saved = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(saved.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(saved.attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID, "order:A",
                TraceAttributes.STREAM_VERSION, 3L,
                TraceAttributes.SNAPSHOT_OUTCOME, "SAVED"));
    assertThat(saved.ok).isTrue();

    execute("UPDATE snapshot_store SET purge_generation = 1");
    tracing.clear();
    store.saveSnapshot(A, new Version(4), new ContractState("t"), 1, 0);
    var skipped = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(skipped.attributes())
        .isEqualTo(
            Map.of(
                TraceAttributes.STREAM_ID, "order:A",
                TraceAttributes.STREAM_VERSION, 4L,
                TraceAttributes.SNAPSHOT_OUTCOME, "SKIPPED"));
    assertThat(skipped.ok).isTrue();
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

  private PostgresEventStore secretStore(CryptoEngine engine) {
    EventTypeRegistry registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent(SECRET, SecretEvent.class)
            .registerState("SecretState", SecretState.class)
            .build();
    PostgresEventStore store =
        PostgresEventStore.builder()
            .dataSource(recorder)
            .typeRegistry(registry)
            .statementTimeout(Duration.ofSeconds(7))
            .cryptoEngine(engine)
            .build();
    recorder.clear();
    return store;
  }

  private static EventEnvelope secret(String value) {
    return envelope(A, SECRET, new SecretEvent("subject", value + "@example.com"));
  }

  private static SnapshotMigration brokenMigration() {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return 1;
      }

      @Override
      public int toVersion() {
        return 2;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        throw new IllegalStateException("migration bug");
      }
    };
  }

  private static EventUpcaster throwingUpcaster(RuntimeException failure) {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return EventStoreContract.EVENT_TYPE;
      }

      @Override
      public int currentVersion() {
        throw failure;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return eventData;
      }
    };
  }

  private static List<EventEnvelope> events(StreamId stream, int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(envelope(stream, EventStoreContract.EVENT_TYPE, new ContractEvent("e" + i)));
    }
    return events;
  }

  private static EventEnvelope envelope(StreamId stream, EventType type, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        type,
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.parse("2026-11-23T10:00:00Z")));
  }

  private static String gapWarning(String stream) {
    return "Stream '"
        + stream
        + "' has non-contiguous versions (expected 2 but found 3, right after version 1) — rows"
        + " were written, deleted or restored outside this store. This store never renumbers"
        + " events automatically; reconstructing the intended history is a data-recovery"
        + " decision. Logged once per stream per process.";
  }

  private static List<String> messages(LogCapture logs, Level level) {
    return logs.events(level).stream().map(ILoggingEvent::getFormattedMessage).toList();
  }

  /** An event row copied from the one at offset 1, with the given offset, id, type and schema. */
  private static String copyOfFirstRow(long offset, String id, String type, int schemaVersion) {
    return "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
        + " event_type, payload, metadata, created_at, schema_version) SELECT "
        + offset
        + ", 'order', '"
        + id
        + "', 1, '"
        + type
        + "', payload, metadata, created_at, "
        + schemaVersion
        + " FROM event_stream WHERE global_offset = 1";
  }

  /** Inserts a row on the append's own connection, inside its open transaction. */
  private static void insertOnSameConnection(Connection conn, long offset, String id, long version)
      throws SQLException {
    execute(
        conn,
        "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
            + " event_type, payload, metadata, schema_version) VALUES ("
            + offset
            + ", 'order', '"
            + id
            + "', "
            + version
            + ", 'ContractEvent', '{}', '{}', 1)");
  }

  /**
   * Turns the stored row at offset 2 into version 1 of stream A, on the append's own connection
   * after its head check: the insert then meets the unique {@code (stream, version)} constraint.
   */
  private static void moveRowTwoToA(Connection conn) throws SQLException {
    execute(
        conn, "UPDATE event_stream SET aggregate_id = 'A', version = 1 WHERE global_offset = 2");
  }

  /**
   * A check constraint, added inside the append's transaction (and rolled back with it), that every
   * new event row fails.
   */
  private static void failInserts(Connection conn) throws SQLException {
    execute(
        conn,
        "ALTER TABLE event_stream ADD CONSTRAINT injected_insert_failure CHECK (false) NOT VALID");
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  private static void execute(String sql) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      execute(conn, sql);
    }
  }
}

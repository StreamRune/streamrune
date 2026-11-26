package org.streamrune.sqlite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.EncryptedTypeGuard;
import org.streamrune.runtime.GuardedTracing;

/**
 * The SQLite {@link EventStore}: events, snapshots and the global stream in one database file, with
 * the semantics of {@code PostgresEventStore}. Built by {@link SqliteEventStoreFactory}.
 *
 * <p><b>Global offsets: a counter row, gap-free and commit-ordered.</b> An append runs in one write
 * transaction, which the driver opens with {@code BEGIN IMMEDIATE} (the required {@code
 * transaction_mode=IMMEDIATE}), so it holds SQLite's single write lock from its first statement to
 * its commit. Inside it the append reserves its {@code n} offsets from the one-row counter, {@code
 * UPDATE global_offset_counter SET next_value = next_value + n WHERE id = 1 RETURNING next_value},
 * checks the stream's head, inserts its events at the offsets {@code returned - n + 1 .. returned}
 * with each offset bound explicitly, and commits. Every write of the database is serialized by the
 * write lock, so appends commit in the order of their offsets: a committed offset {@code N} implies
 * every offset below {@code N} is committed, the global stream has no holes, and a reader that
 * pages by checkpoint needs no visibility gate. A rolled-back append (a version conflict, any
 * failure, a crash before its commit) rolls back its reservation with it, so the next append takes
 * the same offsets. {@code AUTOINCREMENT} is not used: SQLite consumes such an id even when the
 * insert does not happen. The column {@code global_offset} is the table's row id, which SQLite
 * would number itself for an insert that leaves it out; the trigger {@code
 * event_stream_offset_from_counter} refuses an event row above the counter.
 *
 * <p><b>Durability.</b> With the required {@code journal_mode=WAL} and {@code synchronous=FULL} a
 * committed append is on disk when its commit returns and survives a crash or a power loss; an
 * append that has not committed leaves no row and no counter advance.
 *
 * <p><b>One writer at a time, readers alongside.</b> Appends, snapshot saves and purges wait for
 * the write lock for at most the connection's {@code busy_timeout}; an append that does not get it
 * in time fails with an {@link EventStoreException} naming {@code busy_timeout} and stores nothing.
 * Reads run in autocommit and, under WAL, never wait for a writer. Every operation borrows one
 * connection from the {@link SqliteDatabase}'s {@link DataSource} and returns it when it ends;
 * connections must be handed out in autocommit, the pool's default (see {@code
 * SqliteTransactions}). There is no statement timeout: SQLite has none.
 *
 * <p><b>Failures.</b> A version conflict, behind or beyond the head, is an {@link
 * OptimisticLockException}: the head check inside the write transaction finds it, and the unique
 * {@code (aggregate_type, aggregate_id, version)} constraint ({@code SQLITE_CONSTRAINT_UNIQUE}) is
 * mapped the same way as a second line. An offset collision on {@code global_offset} ({@code
 * SQLITE_CONSTRAINT_PRIMARYKEY}) means the counter fell behind the stored events: an {@link
 * EventStoreException} that says how to re-seed it, because a retry reserves the same offsets. The
 * read path classifies a stored event it cannot read exactly as {@code PostgresEventStore} does.
 *
 * <p><b>Timestamps.</b> The store stamps {@code created_at} of the event and snapshot rows with the
 * application {@link Clock} it was built with, as epoch microseconds; there is no database clock.
 *
 * <p><b>Outbox and command inbox, in the append transaction.</b> With a {@link SqliteOutboxStore}
 * and an {@link OutboxEventMapper} the append writes the entries the mapper derives from its events
 * (and those given to {@link #append(StreamId, List, Version, List)}) on its own connection, inside
 * its own write transaction, after the events; with a {@link SqliteCommandInbox}, {@link
 * #appendWithKey} claims the key first in that transaction and records the events' offsets in the
 * key's row after inserting them. Claim, events, offsets and entries commit together or not at all:
 * a version conflict, a failure or a crash before the commit leaves none of them. A key recorded
 * before is answered from its row, verified against the command type and the stream ({@link
 * CommandInbox.InboxResult#requireBoundTo}). The write lock serializes the claims, so of two
 * executions of one key the first to get the lock records it and the other finds the record. Each
 * entry carries the trace link of the event it was derived from (given entries: of the first
 * event), as on Postgres.
 *
 * <p><b>Event audit, in the append transaction.</b> With a {@link SqliteEventAuditStore} the
 * append, keyed or not, writes one {@code event_audit_log} row per event on its own connection,
 * inside its own write transaction, after the events and the outbox entries, as on Postgres: the
 * rows commit with the events or not at all, and an append that writes no event (an empty one, a
 * refused one, a key recorded before) writes none. Each row's stream and version are the append's
 * own arguments, as the event row's are, never the envelope's; its ids and instant come from the
 * event's metadata.
 *
 * <p><b>The saga forward fence.</b> {@link #appendWithKey(StreamId, List, Version, IdempotencyKey,
 * String, SagaOrigin)} with a saga origin reads the saga's {@code saga_state} row inside the
 * append's own write transaction, right after the inbox claim and before the offset reservation,
 * and refuses the append ({@link SagaForwardFencedException}, transaction rolled back) when the row
 * carries an episode stamp, is terminal, is of another saga type, or is absent ({@code SagaFence}).
 * There is no fence lock: the write lock the append holds from its {@code BEGIN IMMEDIATE} to its
 * commit is held by every write of a {@link SqliteSagaStore} too, so the append commits wholly
 * before a write that takes the saga out of its forward phase, or begins after it and reads what it
 * wrote. The rows are those of the database file {@link #sagaFenceStorageIdentity()} names.
 *
 * <p><b>Tracing.</b> With a {@link StreamRuneTracing} whose {@link TracingArea#EVENT_STORE} area is
 * on, the store starts the spans {@code PostgresEventStore} starts: {@value
 * TraceAttributes#EVENT_STORE_APPEND} with {@value TraceAttributes#EVENT_STORE_APPEND_SERIALIZE},
 * {@value TraceAttributes#EVENT_STORE_LOAD} with {@value TraceAttributes#EVENT_STORE_SNAPSHOT_LOAD}
 * and {@value TraceAttributes#EVENT_STORE_LOAD_DESERIALIZE}, {@value
 * TraceAttributes#EVENT_STORE_SNAPSHOT_SAVE}, and {@value TraceAttributes#EVENT_STORE_READ} as the
 * child of a current span for a read that returns events.
 */
public final class SqliteEventStore implements EventStore {

  private static final Logger log = LoggerFactory.getLogger(SqliteEventStore.class);

  /** What SQLite names in the message of a {@code global_offset} primary-key violation. */
  private static final String GLOBAL_OFFSET_COLUMN = "event_stream.global_offset";

  private static final String LOAD_SNAPSHOT =
      "SELECT version, state_type, state_payload, snapshot_version, purge_generation"
          + " FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?";

  // Table-wide existence probe; never reads state_payload, so it never decrypts anything.
  private static final String SNAPSHOT_STATE_TYPE_FOR_WARNING =
      "SELECT state_type FROM snapshot_store WHERE state_type IS NOT NULL LIMIT 1";

  private static final String LOAD_EVENTS =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ? AND version > ? ORDER BY version";

  // Every offset is bound: global_offset is the row id, and an unbound one would be numbered by
  // SQLite instead of the counter.
  private static final String INSERT_EVENT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  // Reserves n dense offsets inside the append's write transaction; rolled back with it.
  private static final String RESERVE_OFFSETS =
      "UPDATE global_offset_counter SET next_value = next_value + ? WHERE id = 1"
          + " RETURNING next_value";

  // The strict optimistic-concurrency check, inside the write transaction: no other write can
  // change the head between this read and the commit. Uses the unique index on the stream key.
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

  // Writes only while the row still has the purge generation the command's load saw. The purge
  // (SqliteSnapshotStorePurger) writes the same row, and SQLite's write lock serializes the two: a
  // purge committed first makes the WHERE fail and the save writes nothing; a save committed first
  // is overwritten by the purge. purge_generation is never written here.
  private static final String UPSERT_SNAPSHOT_VERSIONED =
      "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, state_type,"
          + " state_payload, created_at, snapshot_version, purge_generation)"
          + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
          + " ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET"
          + " version = excluded.version,"
          + " state_type = excluded.state_type,"
          + " state_payload = excluded.state_payload,"
          + " snapshot_version = excluded.snapshot_version,"
          + " created_at = excluded.created_at"
          + " WHERE snapshot_store.purge_generation = excluded.purge_generation";

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /**
   * What {@link #resolveSnapshot} returns for a stored snapshot the load discards: the whole stream
   * replays from its start.
   */
  private static final Object DISCARDED_SNAPSHOT = new Object();

  private final DataSource dataSource;
  private final Path file;
  private final ObjectMapper objectMapper;
  private final EventTypeRegistry typeRegistry;
  private final UpcasterChain upcasterChain;
  private final SnapshotMigrationChain snapshotMigrations;
  private final StreamRuneMetrics metrics;
  private final StreamRuneTracing tracing;
  private final boolean traced;
  private final Clock clock;
  private final SqliteOutboxStore outboxStore;
  private final OutboxEventMapper outboxEventMapper;
  private final SqliteCommandInbox commandInbox;
  private final SqliteEventAuditStore eventAuditStore;

  // The legacy-snapshot probe of an IGNORE_SNAPSHOT load runs at most once per store instance.
  private final AtomicBoolean legacySnapshotProbed = new AtomicBoolean(false);

  // Streams whose non-contiguity WARN fired once for this instance.
  private final Set<String> gapWarnedStreams = ConcurrentHashMap.newKeySet();

  /**
   * Package-private: {@link SqliteEventStoreFactory} is the public entry point.
   *
   * @param database the open database handle whose data source the store borrows connections from
   * @param objectMapper the mapper of events and snapshot states, from {@link
   *     #createObjectMapper(CryptoEngine, StreamRuneMetrics)}
   * @param typeRegistry resolves stored event and state types
   * @param upcasterChain the upcasters applied on read
   * @param snapshotMigrations the snapshot migrations applied on load
   * @param metrics records discarded snapshots
   * @param tracing the tracing the spans start through
   * @param clock stamps {@code created_at}
   * @param outboxStore the outbox the append writes its entries to, or {@code null} for none; set
   *     together with {@code outboxEventMapper}
   * @param outboxEventMapper derives the outbox entries of each event, or {@code null} for none
   * @param commandInbox the inbox a keyed append claims its key in, or {@code null} for none
   * @param eventAuditStore the store the append writes its event audit rows to, or {@code null} for
   *     none
   */
  SqliteEventStore(
      SqliteDatabase database,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      SnapshotMigrationChain snapshotMigrations,
      StreamRuneMetrics metrics,
      StreamRuneTracing tracing,
      Clock clock,
      SqliteOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper,
      SqliteCommandInbox commandInbox,
      SqliteEventAuditStore eventAuditStore) {
    this.dataSource = Objects.requireNonNull(database, "database is required").dataSource();
    this.file = database.file();
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper is required");
    this.typeRegistry = Objects.requireNonNull(typeRegistry, "typeRegistry is required");
    this.upcasterChain = Objects.requireNonNull(upcasterChain, "upcasterChain is required");
    this.snapshotMigrations =
        Objects.requireNonNull(snapshotMigrations, "snapshotMigrations is required");
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
    this.tracing =
        GuardedTracing.of(Objects.requireNonNull(tracing, "tracing is required"), this.metrics);
    this.traced = this.tracing.enabled(TracingArea.EVENT_STORE);
    this.clock = Objects.requireNonNull(clock, "clock is required");
    this.outboxStore = outboxStore;
    this.outboxEventMapper = outboxEventMapper;
    this.commandInbox = commandInbox;
    this.eventAuditStore = eventAuditStore;
  }

  /**
   * The mapper the store (de)serializes with: {@link JavaTimeModule} always, plus {@link
   * CryptoShreddingModule} with {@code metrics} when {@code cryptoEngine} is not {@code null}, as
   * on Postgres.
   */
  static ObjectMapper createObjectMapper(CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics is required");
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine, metrics));
    }
    return mapper;
  }

  /**
   * Refuses a configuration that would persist {@code @Encrypted} components as plaintext, as
   * {@code PostgresEventStore.validateCryptoConfig} does: without an engine an {@code @Encrypted}
   * component in a registered type throws {@link IllegalStateException}, and a registry that cannot
   * list its types throws {@link UnsupportedOperationException}.
   */
  static void validateCryptoConfig(EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine) {
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    EncryptedTypeGuard.check(typeRegistry, cryptoEngine);
  }

  // ── Load ──────────────────────────────────────────────────────────────────────────────────────

  @Override
  public AggregateHistory load(StreamId streamId) {
    return load(streamId, 0);
  }

  @Override
  public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
    Objects.requireNonNull(streamId, "streamId is required");
    if (expectedSnapshotVersion < EventStore.IGNORE_SNAPSHOT) {
      throw new IllegalArgumentException(
          "expectedSnapshotVersion must be >= "
              + EventStore.IGNORE_SNAPSHOT
              + " (IGNORE_SNAPSHOT skips reading any snapshot, 0 skips the version check), got "
              + expectedSnapshotVersion);
    }
    if (!traced) {
      return loadHistory(streamId, expectedSnapshotVersion);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_LOAD,
            SpanKind.INTERNAL,
            streamAttributes(streamId));
    try {
      AggregateHistory history = loadHistory(streamId, expectedSnapshotVersion);
      scope.setAttribute(TraceAttributes.EVENT_COUNT, (long) history.events().size());
      scope.setAttribute(TraceAttributes.SNAPSHOT_USED, history.snapshotState() != null);
      scope.recordSuccess();
      return history;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private static SpanAttributes streamAttributes(StreamId streamId) {
    return SpanAttributes.builder()
        .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
        .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
        .build();
  }

  /** What the load read of a stream's snapshot row, before any deserialization. */
  private record StoredSnapshot(
      boolean stored,
      String stateJson,
      Class<?> stateClass,
      Version version,
      int migrateFromVersion,
      long purgeGeneration) {

    static final StoredSnapshot NONE =
        new StoredSnapshot(false, null, null, Version.initial(), -1, 0);
  }

  private AggregateHistory loadHistory(StreamId streamId, int expectedSnapshotVersion) {
    if (expectedSnapshotVersion == EventStore.IGNORE_SNAPSHOT) {
      return loadIgnoringSnapshot(streamId);
    }
    // The raw columns are read while the connection is open; deserialization (and any decrypt of
    // an @Encrypted field) runs after it is returned, as on Postgres.
    StoredSnapshot snapshot;
    List<RawEventRow> rawRows;
    try (var conn = dataSource.getConnection()) {
      snapshot = readSnapshot(conn, streamId, expectedSnapshotVersion);
      rawRows = readRawEvents(conn, streamId, snapshot.version());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to load stream: " + streamText(streamId), e);
    }

    Object snapshotState =
        snapshot.stored() && traced
            ? resolveSnapshotTraced(streamId, snapshot, expectedSnapshotVersion)
            : resolveSnapshot(streamId, snapshot, expectedSnapshotVersion);
    if (snapshotState == DISCARDED_SNAPSHOT) {
      // The event read was narrowed to the versions after the discarded snapshot: read the whole
      // stream again.
      List<EventEnvelope> replayEvents = deserialize(readRawEvents(streamId, Version.initial()));
      Version replayLast =
          replayEvents.isEmpty() ? Version.initial() : replayEvents.getLast().version();
      return new AggregateHistory(
          null, replayEvents, replayLast, Version.initial(), snapshot.purgeGeneration());
    }
    List<EventEnvelope> events = deserialize(rawRows);
    warnIfNonContiguous(streamId, snapshot.version(), events);
    Version lastVersion = events.isEmpty() ? snapshot.version() : events.getLast().version();
    return new AggregateHistory(
        (AggregateState) snapshotState,
        events,
        lastVersion,
        snapshot.version(),
        snapshot.purgeGeneration());
  }

  /**
   * Reads the stream's snapshot row and decides, without deserializing it, whether the load can
   * start from it: compatible and of a resolvable type, or migratable by a complete chain from a
   * resolvable stored type. Otherwise the load replays every event. A purged row has no state and
   * is no snapshot; its purge generation is read all the same.
   */
  private StoredSnapshot readSnapshot(Connection conn, StreamId streamId, int expectedVersion)
      throws SQLException {
    try (var ps = conn.prepareStatement(LOAD_SNAPSHOT)) {
      SqliteStreamKeys.bind(ps, 1, streamId);
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          return StoredSnapshot.NONE;
        }
        long purgeGeneration = rs.getLong("purge_generation");
        String stateType = rs.getString("state_type");
        if (stateType == null) {
          return new StoredSnapshot(false, null, null, Version.initial(), -1, purgeGeneration);
        }
        int storedVersion = rs.getInt("snapshot_version");
        // A retired or renamed state type is undeserializable: discard, never crash the load.
        Class<?> stateClass = resolveStateTypeOrNull(stateType);
        boolean compatible = expectedVersion == 0 || storedVersion == expectedVersion;
        boolean usable =
            stateClass != null
                && (compatible || snapshotMigrations.canReach(storedVersion, expectedVersion));
        if (!usable) {
          return new StoredSnapshot(true, null, null, Version.initial(), -1, purgeGeneration);
        }
        return new StoredSnapshot(
            true,
            rs.getString("state_payload"),
            stateClass,
            new Version(rs.getLong("version")),
            compatible ? -1 : storedVersion,
            purgeGeneration);
      }
    }
  }

  private List<RawEventRow> readRawEvents(Connection conn, StreamId streamId, Version after)
      throws SQLException {
    List<RawEventRow> rows = new ArrayList<>();
    try (var ps = conn.prepareStatement(LOAD_EVENTS)) {
      SqliteStreamKeys.bind(ps, 1, streamId);
      ps.setLong(3, after.value());
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          rows.add(readRawRow(rs));
        }
      }
    }
    return rows;
  }

  /** The raw rows of a stream after {@code afterVersion}, on a connection of their own. */
  private List<RawEventRow> readRawEvents(StreamId streamId, Version afterVersion) {
    try (var conn = dataSource.getConnection()) {
      return readRawEvents(conn, streamId, afterVersion);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to load stream: " + streamText(streamId), e);
    }
  }

  private Object resolveSnapshotTraced(
      StreamId streamId, StoredSnapshot snapshot, int expectedSnapshotVersion) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .build());
    try {
      Object state = resolveSnapshot(streamId, snapshot, expectedSnapshotVersion);
      boolean hit = snapshot.stateJson() != null && state != DISCARDED_SNAPSHOT;
      scope.setAttribute(TraceAttributes.SNAPSHOT_OUTCOME, hit ? "HIT" : "DISCARDED");
      scope.setAttribute(TraceAttributes.STREAM_VERSION, hit ? snapshot.version().value() : 0L);
      scope.recordSuccess();
      return state;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * The state of the snapshot the load read, deserialized and, when pending, migrated: {@code null}
   * when there is no usable snapshot, {@link #DISCARDED_SNAPSHOT} when the stored one cannot be
   * used. A crypto failure is never discarded: a transient one is a plain {@link
   * EventStoreException}, a deterministic one an {@link EventDeserializationException}.
   */
  private Object resolveSnapshot(
      StreamId streamId, StoredSnapshot snapshot, int expectedSnapshotVersion) {
    if (snapshot.stateJson() == null) {
      return null;
    }
    AggregateState deserialized;
    try {
      deserialized =
          (AggregateState) objectMapper.readValue(snapshot.stateJson(), snapshot.stateClass());
    } catch (IOException | RuntimeException e) {
      // Wider than IOException: a root-level property failure (a corrupt Base64 ciphertext) is a
      // raw RuntimeException and must reach the discard below too.
      if (hasCryptoCause(e)) {
        if (hasTransientCryptoCause(e)) {
          throw new EventStoreException(
              "Failed to deserialize snapshot state for stream: " + streamText(streamId), e);
        }
        throw new EventDeserializationException(
            "Failed to deserialize snapshot state for stream: " + streamText(streamId), e);
      }
      log.error(
          "Snapshot for stream {} is undeserializable ({}); discarding the derived snapshot cache"
              + " and replaying all events from the beginning. This usually means an"
              + " aggregate-state field was renamed or removed without bumping the snapshot"
              + " version — bump SnapshotPolicy's snapshotVersion so fresh snapshots are written.",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e.toString(),
          e);
      recordSnapshotDiscarded("deserialize_failure", streamId);
      return DISCARDED_SNAPSHOT;
    }
    if (snapshot.migrateFromVersion() < 0) {
      return deserialized;
    }
    int from = snapshot.migrateFromVersion();
    Optional<AggregateState> migrated;
    try {
      migrated = snapshotMigrations.migrate(deserialized, from, expectedSnapshotVersion);
    } catch (RuntimeException e) {
      if (hasCryptoCause(e)) {
        if (hasTransientCryptoCause(e)) {
          throw new EventStoreException(
              "Snapshot migration failed for stream: " + streamText(streamId), e);
        }
        throw new EventDeserializationException(
            "Snapshot migration failed for stream: " + streamText(streamId), e);
      }
      log.error(
          "Snapshot migration for stream {} failed ({}); discarding the derived snapshot"
              + " cache and replaying all events from the beginning. This means a registered"
              + " SnapshotMigration step has a bug — inspect the migration chain for the"
              + " version this stream's snapshot was stored at.",
          LogSanitizer.sanitizeForLog(streamId.value()),
          e.toString(),
          e);
      recordSnapshotDiscarded("migration_failure", streamId);
      return DISCARDED_SNAPSHOT;
    }
    // canReach held when the row was read; a chain that then fails to apply is a bug of the chain
    // itself, a hard failure rather than a silent discard.
    return migrated.orElseThrow(
        () ->
            new EventStoreException(
                "Snapshot migration chain reported reachable from version "
                    + from
                    + " to "
                    + expectedSnapshotVersion
                    + " but failed to apply for stream: "
                    + streamText(streamId)));
  }

  /** Records a discarded snapshot; a failing metrics backend must not wedge the load. */
  private void recordSnapshotDiscarded(String reason, StreamId streamId) {
    try {
      metrics.recordSnapshotDiscarded(reason);
    } catch (RuntimeException e) {
      log.warn(
          "Metrics recordSnapshotDiscarded({}) failed for stream {}",
          reason,
          LogSanitizer.sanitizeForLog(streamId.value()),
          e);
    }
  }

  private List<EventEnvelope> deserialize(List<RawEventRow> rows) {
    if (!traced) {
      return toEnvelopes(rows);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) rows.size())
                .put(TraceAttributes.UPCAST_COUNT, upcastCount(rows))
                .build());
    try {
      List<EventEnvelope> events = toEnvelopes(rows);
      scope.recordSuccess();
      return events;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private long upcastCount(List<RawEventRow> rows) {
    if (upcasterChain.isEmpty()) {
      return 0;
    }
    long count = 0;
    for (RawEventRow row : rows) {
      if (row.schemaVersion() < upcasterChain.currentVersion(new EventType(row.eventType()))) {
        count++;
      }
    }
    return count;
  }

  private List<EventEnvelope> toEnvelopes(List<RawEventRow> rows) {
    return rows.stream().map(this::toEnvelope).toList();
  }

  /** A global or stream read's events, under the read span when one is current and traced. */
  private List<EventEnvelope> readEvents(List<RawEventRow> rows, String readKind, Instant started) {
    if (!traced || rows.isEmpty()) {
      return toEnvelopes(rows);
    }
    TraceScope scope =
        tracing.startChild(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_READ,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) rows.size())
                .put(TraceAttributes.READ_KIND, readKind)
                .build(),
            started);
    try {
      List<EventEnvelope> events = toEnvelopes(rows);
      scope.recordSuccess();
      return events;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /** The {@link EventStore#IGNORE_SNAPSHOT} load: every event, never a snapshot. */
  private AggregateHistory loadIgnoringSnapshot(StreamId streamId) {
    warnIfLegacySnapshotExists();
    List<EventEnvelope> events = deserialize(readRawEvents(streamId, Version.initial()));
    warnIfNonContiguous(streamId, Version.initial(), events);
    Version version = events.isEmpty() ? Version.initial() : events.getLast().version();
    return new AggregateHistory(null, events, version, Version.initial());
  }

  /**
   * Once per store instance, a WARN when {@code snapshot_store} still holds a snapshot while loads
   * ignore snapshots (a deployment switched to {@code SnapshotPolicy.never()}); best effort, a
   * failure is logged at DEBUG.
   */
  private void warnIfLegacySnapshotExists() {
    if (!legacySnapshotProbed.compareAndSet(false, true)) {
      return;
    }
    String stateType;
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SNAPSHOT_STATE_TYPE_FOR_WARNING);
        var rs = ps.executeQuery()) {
      if (!rs.next()) {
        return;
      }
      stateType = rs.getString("state_type");
    } catch (SQLException e) {
      log.debug("Could not probe snapshot_store for a legacy row: {}", e.toString());
      return;
    }
    log.warn(
        "snapshot_store still has at least one row (e.g. state type {}), but a load just ran"
            + " with SnapshotPolicy.never() (EventStore.IGNORE_SNAPSHOT) — under never() such"
            + " rows are never read and never overwritten (never() also disables new snapshots),"
            + " so they will sit there indefinitely. If this deployment switched from an active"
            + " snapshot policy to never(), consider purging the now-unused rows from"
            + " snapshot_store. This warning is logged once.",
        LogSanitizer.sanitizeForLog(stateType));
  }

  /**
   * Once per stream and store instance, a WARN when a loaded stream's versions are not contiguous:
   * rows written, deleted or restored outside the store. Never renumbers.
   */
  private void warnIfNonContiguous(
      StreamId streamId, Version baseline, List<EventEnvelope> events) {
    long expected = baseline.value();
    for (EventEnvelope event : events) {
      long actual = event.version().value();
      if (actual != expected + 1) {
        if (gapWarnedStreams.add(streamId.value())) {
          log.warn(
              "Stream '{}' has non-contiguous versions (expected {} but found {}, right after"
                  + " version {}) — rows were written, deleted or restored outside this store."
                  + " This store never renumbers events automatically; reconstructing the intended"
                  + " history is a data-recovery decision. Logged once per stream per process.",
              LogSanitizer.sanitizeForLog(streamId.value()),
              expected + 1,
              actual,
              expected);
        }
        return;
      }
      expected = actual;
    }
  }

  /** True when {@code t}'s cause chain (bounded) holds a {@link CryptoOperationException}. */
  static boolean hasCryptoCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CryptoOperationException) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code t}'s cause chain holds a bare {@link CryptoOperationException}, not the
   * deterministic {@link CryptoMappingException}: engine-raised key-store evidence, retried rather
   * than quarantined (see {@code PostgresEventStore.hasTransientCryptoCause}).
   */
  static boolean hasTransientCryptoCause(Throwable t) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof CryptoOperationException && !(c instanceof CryptoMappingException)) {
        return true;
      }
    }
    return false;
  }

  private Class<?> resolveStateTypeOrNull(String stateType) {
    try {
      return typeRegistry.resolveStateType(stateType);
    } catch (UnknownEventTypeException _) {
      return null;
    }
  }

  // ── Append ────────────────────────────────────────────────────────────────────────────────────

  @Override
  public AppendResult append(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    // The arguments are checked before the outbox mapper, application code, sees the events.
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    return appendWithOutbox(streamId, events, expectedVersion, outboxEntriesOf(events));
  }

  /**
   * Appends {@code events} and {@code outboxEntries} in one write transaction: all of them commit,
   * or none does. Only the entries given here are written; the configured mapper is not applied to
   * these events, as on Postgres. The entries all take the trace link of the first event (one
   * append's events come from one command), replacing any link they carry.
   *
   * <p>Entries given with an empty {@code events} list are refused: they commit with the events,
   * and there are none to commit them with. An entry with no events to accompany it is saved alone
   * with {@link SqliteOutboxStore#save}, which commits on its own.
   *
   * @param streamId the stream to append to (required)
   * @param events the events to append (required)
   * @param expectedVersion the stream's version the append expects (required)
   * @param outboxEntries the entries to store with the events (required; may be empty); non-empty
   *     entries need an outbox store and at least one event
   * @return the events' offsets and the stream's new version
   * @throws IllegalStateException if {@code outboxEntries} is not empty and the store has no outbox
   *     store
   * @throws IllegalArgumentException if {@code outboxEntries} is not empty and {@code events} is
   *     empty
   * @throws NullPointerException if an argument is {@code null}
   */
  public AppendResult append(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries) {
    List<OutboxEntry> linked =
        outboxEntries == null || outboxEntries.isEmpty() || events == null || events.isEmpty()
            ? outboxEntries
            : withTraceLink(outboxEntries, traceLinkOf(events.getFirst()));
    return appendWithOutbox(streamId, events, expectedVersion, linked);
  }

  /** The append of {@code events} with {@code outboxEntries}, inside its span when traced. */
  private AppendResult appendWithOutbox(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries) {
    if (!traced
        || streamId == null
        || events == null
        || events.isEmpty()
        || expectedVersion == null
        || outboxEntries == null) {
      return appendEvents(streamId, events, expectedVersion, outboxEntries, false);
    }
    return tracedAppend(
        streamId,
        events.size(),
        expectedVersion,
        outboxEntries.size(),
        () -> appendEvents(streamId, events, expectedVersion, outboxEntries, true),
        result -> "APPENDED");
  }

  /**
   * Runs {@code append} inside the span {@value TraceAttributes#EVENT_STORE_APPEND}. A version
   * conflict is the outcome {@code CONFLICT}, and a saga's forward command the fence refuses the
   * outcome {@code FENCED}; neither is an error.
   */
  private <R> R tracedAppend(
      StreamId streamId,
      int eventCount,
      Version expectedVersion,
      int outboxEntryCount,
      Supplier<R> append,
      Function<R, String> outcome) {
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_APPEND,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .put(TraceAttributes.AGGREGATE_TYPE, streamId.aggregateType().value())
                .put(TraceAttributes.EVENT_COUNT, (long) eventCount)
                .put(TraceAttributes.EXPECTED_VERSION, expectedVersion.value())
                .put(TraceAttributes.OUTBOX_ENTRY_COUNT, (long) outboxEntryCount)
                .build());
    try {
      R result = append.get();
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, outcome.apply(result));
      scope.recordSuccess();
      return result;
    } catch (OptimisticLockException conflict) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "CONFLICT");
      throw conflict;
    } catch (SagaForwardFencedException fenced) {
      scope.setAttribute(TraceAttributes.APPEND_OUTCOME, "FENCED");
      throw fenced;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private List<EventRow> serialize(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion, boolean traced)
      throws JsonProcessingException {
    if (!traced) {
      return serializeRows(events, expectedVersion);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_APPEND_SERIALIZE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.EVENT_COUNT, (long) events.size())
                .build());
    try {
      List<EventRow> rows = serializeRows(events, expectedVersion);
      scope.recordSuccess();
      return rows;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  /**
   * The append: serialize (any {@code @Encrypted} field encrypted) before a connection is borrowed,
   * then one write transaction that reserves the offsets, checks the head, inserts the rows, writes
   * the outbox entries and the event audit rows, and commits.
   *
   * @param traced whether the append runs inside its span, so its serialization starts a child
   */
  private AppendResult appendEvents(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries,
      boolean traced) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    Objects.requireNonNull(outboxEntries, "outboxEntries is required");
    if (!outboxEntries.isEmpty() && outboxStore == null) {
      throw new IllegalStateException(
          "Outbox entries were passed but no SqliteOutboxStore is configured on this event store."
              + " Configure a SqliteOutboxStore together with an OutboxEventMapper on the"
              + " factory.");
    }
    if (events.isEmpty()) {
      // The no-op returns before any transaction opens; entries in hand would be lost silently.
      if (!outboxEntries.isEmpty()) {
        throw new IllegalArgumentException(
            "outboxEntries were passed with an empty events list — outbox entries are persisted"
                + " atomically with the events of this append and cannot be appended alone. To"
                + " emit an outbox entry that has no accompanying domain event, persist it"
                + " directly with OutboxStore.save(entry), one entry per call — that write commits"
                + " on its own connection and is atomic with nothing else.");
      }
      return new AppendResult(List.of(), expectedVersion);
    }
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());
    List<EventRow> rows;
    try {
      rows = serialize(streamId, events, expectedVersion, traced);
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize event for stream: " + shownStream, e);
    }

    // Set the moment the COMMIT returned: the events are durable from then on, and a failure of
    // anything after it (closing the connection) must not report the append as failed: a retry of
    // an unkeyed command would store its events twice.
    boolean committed = false;
    AppendResult result = null;
    // Closing the open transaction is the single rollback point of every exit without a commit: a
    // SQLException, the head check's OptimisticLockException, a failed outbox write, any other
    // Throwable. It also releases the file's write mutex, before the connection is returned.
    try (Connection conn = dataSource.getConnection();
        SqliteTransactions.OpenTransaction transaction = SqliteTransactions.open(conn, "append")) {
      List<GlobalOffset> offsets = insertEvents(conn, streamId, rows, expectedVersion);
      if (!outboxEntries.isEmpty()) {
        outboxStore.saveAll(outboxEntries, conn);
      }
      writeAuditEntries(conn, streamId, events, expectedVersion);
      transaction.commit();
      committed = true;
      result =
          new AppendResult(
              List.copyOf(offsets), new Version(expectedVersion.value() + events.size()));
      return result;
    } catch (SQLException e) {
      if (committed) {
        return result;
      }
      throw appendFailure(e, shownStream, expectedVersion);
    }
  }

  // ── Keyed append ──────────────────────────────────────────────────────────────────────────────

  /**
   * {@inheritDoc}
   *
   * <p>The six-argument method with no saga origin: unfenced.
   */
  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType) {
    return appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType, null);
  }

  /**
   * The database file this store appends to: the fence reads the {@code saga_state} rows of a
   * {@link SqliteSagaStore} over the same file, whose {@code storageIdentity()} is that file.
   */
  @Override
  public Object sagaFenceStorageIdentity() {
    return file;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Here the claim, the fence read ({@code SagaFence}, when {@code forwardOf} is given), the
   * events, the backfill of their offsets into the key's row and the outbox entries are one write
   * transaction on one connection (see the class javadoc). A key recorded before is answered from
   * its row after the transaction is rolled back and its connection returned, without reading the
   * fence, so a claim loser never holds one connection while it borrows another.
   */
  @Override
  public IdempotentAppendResult appendWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf) {
    if (!traced
        || commandInbox == null
        || streamId == null
        || events == null
        || events.isEmpty()
        || expectedVersion == null) {
      return appendEventsWithKey(
          streamId, events, expectedVersion, idempotencyKey, commandType, forwardOf, null, false);
    }
    // The rest of the arguments are checked before the outbox mapper, application code, sees the
    // events.
    Objects.requireNonNull(idempotencyKey, "idempotencyKey is required");
    CommandInbox.requireCommandType(commandType);
    List<OutboxEntry> outboxEntries = outboxEntriesOf(events);
    return tracedAppend(
        streamId,
        events.size(),
        expectedVersion,
        outboxEntries.size(),
        () ->
            appendEventsWithKey(
                streamId,
                events,
                expectedVersion,
                idempotencyKey,
                commandType,
                forwardOf,
                outboxEntries,
                true),
        result -> result.alreadyApplied() ? "DUPLICATE" : "APPENDED");
  }

  /**
   * The outbox entries the configured mapper derives from {@code events}, each with the trace link
   * of the event it was derived from; none without an outbox store.
   */
  private List<OutboxEntry> outboxEntriesOf(List<EventEnvelope> events) {
    return outboxStore != null && outboxEventMapper != null
        ? events.stream()
            .flatMap(e -> withTraceLink(outboxEventMapper.toOutbox(e), traceLinkOf(e)).stream())
            .toList()
        : List.of();
  }

  /**
   * The link to the span of the command that wrote {@code event}: its {@code spanContext}, via
   * {@link SpanLink#of}; {@link SpanLink#NONE} when the command was not traced.
   */
  private static SpanLink traceLinkOf(EventEnvelope event) {
    return event == null ? SpanLink.NONE : SpanLink.of(event.metadata());
  }

  /** {@code entries}, each with {@code link} as its trace link, replacing any link set before. */
  private static List<OutboxEntry> withTraceLink(List<OutboxEntry> entries, SpanLink link) {
    List<OutboxEntry> linked = null;
    for (int i = 0; i < entries.size(); i++) {
      OutboxEntry entry = entries.get(i);
      if (!entry.traceLink().equals(link)) {
        if (linked == null) {
          linked = new ArrayList<>(entries);
        }
        linked.set(i, entry.withTraceLink(link));
      }
    }
    return linked == null ? entries : linked;
  }

  /**
   * Writes the event audit rows of {@code events} on {@code conn}, inside the append's open write
   * transaction; nothing without an event audit store or without events. The stream and the
   * versions are the append's arguments, as the inserted events' are, never the envelope's, so the
   * audit rows mirror event_stream even when a caller's envelopes name another stream or version.
   */
  private void writeAuditEntries(
      Connection conn, StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    if (eventAuditStore == null || events.isEmpty()) {
      return;
    }
    List<EventAuditEntry> entries = new ArrayList<>(events.size());
    long version = expectedVersion.value();
    for (EventEnvelope envelope : events) {
      version++;
      EventMetadata metadata = envelope.metadata();
      entries.add(
          new EventAuditEntry(
              metadata.eventId(),
              envelope.eventType(),
              streamId,
              new Version(version),
              metadata.commandId(),
              metadata.correlationId(),
              metadata.causationId(),
              metadata.userId(),
              metadata.timestamp()));
    }
    eventAuditStore.saveAll(entries, conn);
  }

  /**
   * The keyed append: claim, fence, events, backfill, outbox entries, event audit rows, commit, in
   * one write transaction.
   *
   * @param forwardOf the saga whose forward command this is, or {@code null} for an unfenced append
   * @param mappedOutboxEntries the outbox entries of {@code events}, or {@code null} to derive them
   *     here
   * @param traced whether the append runs inside its span, so its serialization starts a child
   */
  private IdempotentAppendResult appendEventsWithKey(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      IdempotencyKey idempotencyKey,
      String commandType,
      SagaOrigin forwardOf,
      List<OutboxEntry> mappedOutboxEntries,
      boolean traced) {
    if (commandInbox == null) {
      throw new UnsupportedOperationException(
          "This SqliteEventStore was built without a command inbox");
    }
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    Objects.requireNonNull(idempotencyKey, "idempotencyKey is required");
    CommandInbox.requireCommandType(commandType);
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());
    List<OutboxEntry> outboxEntries =
        mappedOutboxEntries != null ? mappedOutboxEntries : outboxEntriesOf(events);
    List<EventRow> rows;
    try {
      rows = serialize(streamId, events, expectedVersion, traced);
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize event for stream: " + shownStream, e);
    }

    Version finalVersion = new Version(expectedVersion.value() + events.size());
    // Set when the claim finds the key recorded: the transaction is rolled back and the connection
    // returned before the recorded row is read.
    boolean claimLost = false;
    boolean committed = false;
    IdempotentAppendResult result = null;
    // Closing the open transaction is the single rollback point of every exit without a commit,
    // the lost claim included; it also releases the file's write mutex.
    try (Connection conn = dataSource.getConnection();
        SqliteTransactions.OpenTransaction transaction =
            SqliteTransactions.open(conn, "keyed append")) {
      if (!commandInbox.tryClaim(idempotencyKey, commandType, streamId, finalVersion, conn)) {
        claimLost = true;
      } else {
        // A refusal throws here before anything else is written; closing the transaction undoes
        // the claim with the rest of it.
        if (forwardOf != null) {
          SagaFence.requireForward(conn, forwardOf);
        }
        // An empty command claims its key and appends nothing: like an empty append it does not
        // consult the stream's head.
        List<GlobalOffset> offsets =
            rows.isEmpty() ? List.of() : insertEvents(conn, streamId, rows, expectedVersion);
        commandInbox.backfillOffsets(idempotencyKey, offsets, conn);
        if (!outboxEntries.isEmpty()) {
          outboxStore.saveAll(outboxEntries, conn);
        }
        writeAuditEntries(conn, streamId, events, expectedVersion);
        transaction.commit();
        committed = true;
        result = new IdempotentAppendResult(List.copyOf(offsets), finalVersion, false);
      }
    } catch (SQLException e) {
      if (committed) {
        return result;
      }
      throw keyedAppendFailure(e, shownStream);
    }
    if (claimLost) {
      CommandInbox.InboxResult prior =
          commandInbox
              .find(idempotencyKey)
              .orElseThrow(() -> inboxConflictWithoutRow(idempotencyKey));
      // Fails closed unless the recorded row is this command's on this stream (see the interface).
      prior.requireBoundTo(commandType, streamId);
      return new IdempotentAppendResult(prior.globalOffsets(), prior.finalVersion(), true);
    }
    return result;
  }

  /**
   * The claim-lost dead end: the key was found recorded, yet its row is gone (retention pruned it
   * in between). A retry claims the key anew.
   */
  static EventStoreException inboxConflictWithoutRow(IdempotencyKey idempotencyKey) {
    return new EventStoreException(
        "Inbox conflict but no row found for key: "
            + LogSanitizer.sanitizeForLog(idempotencyKey.value()));
  }

  /** The exception of a keyed append that did not commit, classified as Postgres classifies it. */
  private static RuntimeException keyedAppendFailure(SQLException e, String shownStream) {
    if (isGlobalOffsetCollision(e)) {
      return new EventStoreException(globalOffsetDesyncMessage(shownStream), e);
    }
    if (isRetryableConflict(e)) {
      return new OptimisticLockException("Concurrent modification of stream: " + shownStream, e);
    }
    EventStoreException lockFailure =
        lockFailure(e, "idempotent append to stream '" + shownStream + "'");
    if (lockFailure != null) {
      return lockFailure;
    }
    return new EventStoreException("Failed idempotent append to stream: " + shownStream, e);
  }

  /** One event's serialized row, ready for the insert. */
  private record EventRow(
      long version, String eventType, String payloadJson, String metadataJson, int schemaVersion) {}

  private List<EventRow> serializeRows(List<EventEnvelope> events, Version expectedVersion)
      throws JsonProcessingException {
    List<EventRow> rows = new ArrayList<>(events.size());
    long version = expectedVersion.value();
    for (EventEnvelope envelope : events) {
      version++;
      rows.add(
          new EventRow(
              version,
              envelope.eventType().value(),
              objectMapper.writeValueAsString(envelope.event()),
              objectMapper.writeValueAsString(envelope.metadata()),
              upcasterChain.currentVersion(envelope.eventType())));
    }
    return rows;
  }

  /**
   * Inside the caller's write transaction: reserves {@code rows.size()} offsets from the counter,
   * checks that the stream's head is {@code expectedVersion}, and inserts the rows at the reserved
   * offsets, each bound explicitly. Returns the offsets in row order.
   *
   * @throws OptimisticLockException if the stream's head is not {@code expectedVersion}
   */
  private List<GlobalOffset> insertEvents(
      Connection conn, StreamId streamId, List<EventRow> rows, Version expectedVersion)
      throws SQLException {
    int n = rows.size();
    long base;
    try (var reserve = conn.prepareStatement(RESERVE_OFFSETS)) {
      reserve.setInt(1, n);
      try (var rs = reserve.executeQuery()) {
        if (!rs.next()) {
          throw new EventStoreException(
              "global_offset_counter row missing: the event-store schema was not migrated, or the"
                  + " counter row was deleted. Re-seed it with: INSERT INTO global_offset_counter"
                  + " (id, next_value) SELECT 1, COALESCE(max(global_offset), 0) FROM event_stream");
        }
        base = rs.getLong(1) - n;
      }
    }

    try (var head = conn.prepareStatement(STREAM_HEAD_VERSION)) {
      SqliteStreamKeys.bind(head, 1, streamId);
      try (var rs = head.executeQuery()) {
        rs.next();
        long actualHead = rs.getLong(1);
        if (actualHead != expectedVersion.value()) {
          throw new OptimisticLockException(
              "Version conflict on stream '"
                  + LogSanitizer.sanitizeForLog(streamId.value())
                  + "': expected version "
                  + expectedVersion.value()
                  + " but actual head is "
                  + actualHead);
        }
      }
    }

    long createdAt = SqliteTimestamps.toMicros(clock.instant());
    List<GlobalOffset> offsets = new ArrayList<>(n);
    try (var insert = conn.prepareStatement(INSERT_EVENT)) {
      for (int i = 0; i < n; i++) {
        long offset = base + i + 1;
        EventRow row = rows.get(i);
        insert.setLong(1, offset);
        SqliteStreamKeys.bind(insert, 2, streamId);
        insert.setLong(4, row.version());
        insert.setString(5, row.eventType());
        insert.setString(6, row.payloadJson());
        insert.setString(7, row.metadataJson());
        insert.setLong(8, createdAt);
        insert.setInt(9, row.schemaVersion());
        insert.addBatch();
        offsets.add(GlobalOffset.of(offset));
      }
      insert.executeBatch();
    }
    return offsets;
  }

  /**
   * The exception an append that did not commit reports for {@code e}: a collision on {@code
   * global_offset} is a desynced counter, a unique violation or a stale snapshot a version
   * conflict, a write lock not granted within {@code busy_timeout} an infrastructure failure that
   * says so, anything else a plain {@link EventStoreException}.
   */
  private static RuntimeException appendFailure(
      SQLException e, String shownStream, Version expectedVersion) {
    if (isGlobalOffsetCollision(e)) {
      return new EventStoreException(globalOffsetDesyncMessage(shownStream), e);
    }
    if (isRetryableConflict(e)) {
      return new OptimisticLockException(
          "Version conflict on stream '"
              + shownStream
              + "' at expected version "
              + expectedVersion.value(),
          e);
    }
    EventStoreException lockFailure = lockFailure(e, "append to stream '" + shownStream + "'");
    if (lockFailure != null) {
      return lockFailure;
    }
    return new EventStoreException("Failed to append events to stream: " + shownStream, e);
  }

  /**
   * True when {@code e}'s cause chain holds SQLite's {@code SQLITE_CONSTRAINT_PRIMARYKEY} naming
   * {@code event_stream.global_offset}: the counter has fallen at or below a stored offset. One
   * sided, as on Postgres: only a positively identified collision is taken out of the retryable
   * conflicts.
   */
  static boolean isGlobalOffsetCollision(SQLException e) {
    SQLiteException sqlite = sqliteCause(e);
    return sqlite != null
        && sqlite.getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY
        && sqlite.getMessage() != null
        && sqlite.getMessage().contains(GLOBAL_OFFSET_COLUMN);
  }

  /**
   * True when {@code e}'s cause chain holds a conflict a retry can resolve: {@code
   * SQLITE_CONSTRAINT_UNIQUE} or a primary-key violation not identified as an offset collision (the
   * {@code (stream, version)} constraint behind the head check), or {@code SQLITE_BUSY_SNAPSHOT} (a
   * deferred transaction whose snapshot went stale, which {@code BEGIN IMMEDIATE} rules out; mapped
   * as Postgres maps its serialization failure).
   */
  static boolean isRetryableConflict(SQLException e) {
    SQLiteException sqlite = sqliteCause(e);
    if (sqlite == null) {
      return false;
    }
    SQLiteErrorCode code = sqlite.getResultCode();
    return code == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE
        || (code == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY && !isGlobalOffsetCollision(e))
        || code == SQLiteErrorCode.SQLITE_BUSY_SNAPSHOT;
  }

  /**
   * True when {@code e}'s cause chain holds {@code SQLITE_BUSY} or {@code SQLITE_BUSY_TIMEOUT}: the
   * write lock was not granted within {@code busy_timeout}.
   */
  static boolean isBusy(SQLException e) {
    SQLiteException sqlite = sqliteCause(e);
    if (sqlite == null) {
      return false;
    }
    SQLiteErrorCode code = sqlite.getResultCode();
    return code == SQLiteErrorCode.SQLITE_BUSY || code == SQLiteErrorCode.SQLITE_BUSY_TIMEOUT;
  }

  /**
   * True when {@code e}'s cause chain holds {@code SQLITE_LOCKED} or one of its extended codes: a
   * conflict with another statement or transaction on the same connection (or, with a shared cache,
   * the same cache), for which SQLite does not wait, whatever {@code busy_timeout} says.
   */
  static boolean isLocked(SQLException e) {
    SQLiteException sqlite = sqliteCause(e);
    if (sqlite == null) {
      return false;
    }
    SQLiteErrorCode code = sqlite.getResultCode();
    return code == SQLiteErrorCode.SQLITE_LOCKED
        || code == SQLiteErrorCode.SQLITE_LOCKED_SHAREDCACHE
        || code == SQLiteErrorCode.SQLITE_LOCKED_VTAB;
  }

  /**
   * The {@link EventStoreException} of a write SQLite refused for a lock, naming {@code what}:
   * {@link #busyMessage} for {@code SQLITE_BUSY}, or {@link #ownBatchMessage} when the refused
   * thread's own projection batch held the lock; {@link #lockedMessage} for {@code SQLITE_LOCKED};
   * {@code null} for any other failure.
   */
  static EventStoreException lockFailure(SQLException e, String what) {
    if (isBusy(e)) {
      return new EventStoreException(
          SqliteTransactions.isHeldByOwnBatch(e) ? ownBatchMessage(what) : busyMessage(what), e);
    }
    if (isLocked(e)) {
      return new EventStoreException(lockedMessage(what), e);
    }
    return null;
  }

  /**
   * The message of a write that waited busy_timeout for a write lock its own thread holds: it was
   * made from inside the updater of a projection batch or dead-letter replay on the same database
   * file, on a connection of its own.
   */
  static String ownBatchMessage(String what) {
    return "SQLite's write lock was not granted within the connection's busy_timeout, so the "
        + what
        + " stored nothing. This thread's own projection batch (or dead-letter replay) holds the"
        + " write lock of the database file: the write was made from inside the batch's updater,"
        + " on a connection of its own, so it waited for a transaction that cannot commit before"
        + " the updater returns. Inside an updater, write read models through the repository it is"
        + " handed or through the store's own ProjectionRepository, which join the batch; make any"
        + " other write to the same file (an append, a saga, outbox or checkpoint write) after the"
        + " batch, from a command handler or a saga step.";
  }

  /** The message of a write refused with SQLITE_LOCKED, which busy_timeout does not govern. */
  static String lockedMessage(String what) {
    return "SQLite refused the "
        + what
        + " with SQLITE_LOCKED: a table it needs is locked by another statement or transaction"
        + " of the same connection (or of the same shared cache). SQLite does not wait for that"
        + " lock, so busy_timeout does not apply; nothing was stored. Finish the other statement"
        + " (close its result set) or do not share the connection or the cache.";
  }

  /** The first {@link SQLiteException} of {@code e}'s cause chain, walked at most 50 deep. */
  private static SQLiteException sqliteCause(Throwable e) {
    Throwable t = e;
    for (int depth = 0; t != null && depth < 50; depth++, t = t.getCause()) {
      if (t instanceof SQLiteException sqlite) {
        return sqlite;
      }
    }
    return null;
  }

  /**
   * The message of a write that waited for SQLite's write lock longer than busy_timeout. The likely
   * cause comes first: StreamRune's stores never join an application's transaction, so one the
   * application holds on the same file (a Spring {@code @Transactional} method, for example) keeps
   * every store write waiting until busy_timeout runs out.
   */
  static String busyMessage(String what) {
    return "SQLite's write lock was not granted within the connection's busy_timeout, so the "
        + what
        + " stored nothing. Most likely an application transaction on the same database file holds"
        + " the write lock: StreamRune's stores never join an application's transaction (for"
        + " example a Spring @Transactional method on the same DataSource), so call them outside"
        + " it. Otherwise another connection of this process held the lock that long: a schema"
        + " migration or a stalled write. Retry later, or raise busy_timeout.";
  }

  private static String globalOffsetDesyncMessage(String shownStream) {
    return "global_offset collision while appending to stream '"
        + shownStream
        + "': global_offset_counter.next_value has fallen at or below max(global_offset), so this"
        + " append reserved offsets that are already stored. This is NOT a version conflict and"
        + " retrying cannot resolve it: the reservation rolls back with the append, so every"
        + " attempt reserves the same used offsets. Re-seed the counter with: UPDATE"
        + " global_offset_counter SET next_value = (SELECT COALESCE(max(global_offset), 0) FROM"
        + " event_stream) WHERE id = 1";
  }

  private static String streamText(StreamId streamId) {
    return LogSanitizer.sanitizeForLog(streamId.value());
  }

  // ── Snapshots ─────────────────────────────────────────────────────────────────────────────────

  /**
   * {@inheritDoc}
   *
   * <p>The snapshot's {@code state_type} is the state class's simple name, which must resolve back
   * to that class in the registry. The write is one conditional upsert, atomic on its own: it
   * stores the snapshot only while the stream's row still has {@code snapshotPurgeGeneration} (or
   * there is no row). {@link SqliteSnapshotStorePurger} raises the generation of the same row, and
   * SQLite's write lock serializes the two, so a save from a state loaded before a purge writes
   * nothing and returns {@code false}.
   */
  @Override
  public boolean saveSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    Objects.requireNonNull(streamId, "streamId is required");
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    if (snapshotVersion < 1) {
      throw new IllegalArgumentException("snapshotVersion must be >= 1");
    }
    if (snapshotPurgeGeneration < 0) {
      throw new IllegalArgumentException(
          "snapshotPurgeGeneration must not be negative, got " + snapshotPurgeGeneration);
    }
    Objects.requireNonNull(version, "version is required");
    if (!traced) {
      return writeSnapshot(streamId, version, state, snapshotVersion, snapshotPurgeGeneration);
    }
    TraceScope scope =
        tracing.start(
            TracingArea.EVENT_STORE,
            TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE,
            SpanKind.INTERNAL,
            SpanAttributes.builder()
                .put(TraceAttributes.STREAM_ID, LogSanitizer.sanitizeForTrace(streamId.value()))
                .put(TraceAttributes.STREAM_VERSION, version.value())
                .build());
    try {
      boolean saved =
          writeSnapshot(streamId, version, state, snapshotVersion, snapshotPurgeGeneration);
      scope.setAttribute(TraceAttributes.SNAPSHOT_OUTCOME, saved ? "SAVED" : "SKIPPED");
      scope.recordSuccess();
      return saved;
    } catch (Throwable t) {
      scope.recordError(t);
      throw t;
    } finally {
      scope.close();
    }
  }

  private boolean writeSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());
    String stateType = state.getClass().getSimpleName();
    EventStore.requireStateType(stateType);
    Class<?> resolved;
    try {
      resolved = typeRegistry.resolveStateType(stateType);
    } catch (RuntimeException e) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' does not resolve in the EventTypeRegistry — register "
              + state.getClass().getName()
              + " under its simple name so the snapshot can be rehydrated on load",
          e);
    }
    if (resolved != state.getClass()) {
      throw new EventStoreException(
          "Snapshot state type '"
              + stateType
              + "' resolves to "
              + resolved.getName()
              + " in the EventTypeRegistry, but the state being saved is "
              + state.getClass().getName()
              + " — snapshots are stored by simple class name, so two state classes sharing a"
              + " simple name would rehydrate as the wrong type");
    }
    // Serialized (and any @Encrypted field encrypted) before a connection is borrowed.
    String stateJson;
    try {
      stateJson = objectMapper.writeValueAsString(state);
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize snapshot for stream: " + shownStream, e);
    }
    long createdAt = SqliteTimestamps.toMicros(clock.instant());

    // One statement in autocommit: SQLite runs it as one transaction of its own, which takes the
    // write lock when the statement starts (waiting at most busy_timeout) and commits all of it or
    // nothing. It queues with the other writers of the process first, on the file's write mutex.
    int written;
    try (var conn = dataSource.getConnection()) {
      written =
          SqliteTransactions.writeStatement(
              conn,
              c -> {
                try (var ps = c.prepareStatement(UPSERT_SNAPSHOT_VERSIONED)) {
                  SqliteStreamKeys.bind(ps, 1, streamId);
                  ps.setLong(3, version.value());
                  ps.setString(4, stateType);
                  ps.setString(5, stateJson);
                  ps.setLong(6, createdAt);
                  ps.setInt(7, snapshotVersion);
                  ps.setLong(8, snapshotPurgeGeneration);
                  return ps.executeUpdate();
                }
              });
    } catch (SQLException e) {
      EventStoreException lockFailure = lockFailure(e, "snapshot save of stream " + shownStream);
      if (lockFailure != null) {
        throw lockFailure;
      }
      throw new EventStoreException("Failed to save snapshot for stream: " + shownStream, e);
    }
    if (written == 0) {
      log.info(
          "Snapshot of stream {} at version {} not stored: its snapshot_store row carries"
              + " a purge generation other than {}, so a GDPR forget purged the stream's"
              + " snapshot after the state being saved was loaded; the next command loads and"
              + " snapshots again",
          shownStream,
          version.value(),
          snapshotPurgeGeneration);
      return false;
    }
    return true;
  }

  // ── Reads ─────────────────────────────────────────────────────────────────────────────────────

  /**
   * {@inheritDoc}
   *
   * <p>Offsets are committed in their order (see the class javadoc), so the read needs no
   * visibility gate. As on Postgres it returns only a contiguous run, as a defense against rows
   * deleted or restored outside the store: the run starts at the first row above {@code
   * afterOffset}, so a leading hole is paged over, and stops at the first hole inside the page.
   *
   * @throws IllegalArgumentException if {@code maxCount} is negative
   */
  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    Objects.requireNonNull(afterOffset, "afterOffset is required");
    requireMaxCount(maxCount);
    Instant started = traced ? Instant.now() : null;
    List<RawEventRow> rawRows = new ArrayList<>();
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(READ_GLOBAL)) {
      ps.setLong(1, afterOffset.value());
      ps.setInt(2, maxCount);
      long expectedNext = -1;
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          long offset = rs.getLong("global_offset");
          if (expectedNext == -1) {
            expectedNext = offset;
          } else if (offset != expectedNext) {
            log.warn(
                "non-contiguous global_offset observed after {}: expected {}, got {}",
                afterOffset,
                expectedNext,
                offset);
            break;
          }
          rawRows.add(readRawRow(rs));
          expectedNext++;
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read global stream from offset: " + afterOffset, e);
    }
    return readEvents(rawRows, "GLOBAL", started);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The highest stored offset. Appends commit in the order of their offsets, so no event at or
   * below it becomes readable later.
   */
  @Override
  public GlobalOffset lastGlobalOffset() {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(LAST_GLOBAL_OFFSET);
        var rs = ps.executeQuery()) {
      rs.next();
      return GlobalOffset.of(rs.getLong(1));
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read last global offset", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if {@code maxCount} is negative
   */
  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(afterVersion, "afterVersion is required");
    requireMaxCount(maxCount);
    Instant started = traced ? Instant.now() : null;
    List<RawEventRow> rawRows = new ArrayList<>();
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(READ_STREAM)) {
      SqliteStreamKeys.bind(ps, 1, streamId);
      ps.setLong(3, afterVersion.value());
      ps.setInt(4, maxCount);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          rawRows.add(readRawRow(rs));
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read stream: " + streamText(streamId), e);
    }
    return readEvents(rawRows, "STREAM", started);
  }

  /**
   * A negative {@code maxCount} is refused: SQLite reads {@code LIMIT -1} as no limit at all, which
   * would return the whole stream for a request of at most a negative number of events.
   */
  private static void requireMaxCount(int maxCount) {
    if (maxCount < 0) {
      throw new IllegalArgumentException("maxCount must not be negative, got " + maxCount);
    }
  }

  /** One event row's raw columns, read while the connection is open; decoded after it closed. */
  private record RawEventRow(
      long globalOffset,
      String aggregateType,
      String aggregateId,
      long version,
      String eventType,
      String payloadJson,
      String metadataJson,
      int schemaVersion) {

    String streamText() {
      return LogSanitizer.sanitizeForLog(aggregateType + StreamId.SEPARATOR + aggregateId);
    }
  }

  private static RawEventRow readRawRow(ResultSet rs) throws SQLException {
    return new RawEventRow(
        rs.getLong("global_offset"),
        rs.getString("aggregate_type"),
        rs.getString("aggregate_id"),
        rs.getLong("version"),
        rs.getString("event_type"),
        rs.getString("payload"),
        rs.getString("metadata"),
        rs.getInt("schema_version"));
  }

  /**
   * The message of the refusal to read an event stored at a {@code schema_version} above this
   * reader's: the text {@code PostgresEventStore} uses.
   */
  static String schemaAheadMessage(
      String eventType, String stream, int storedVersion, int readerVersion) {
    return "Event type "
        + eventType
        + " for stream "
        + stream
        + " is stored at schema_version "
        + storedVersion
        + " but this event store's upcaster chain only reaches version "
        + readerVersion
        + ". A newer writer stamped it: either a replica of a newer release of this application"
        + " (a rolling deployment that bumps this event's schema is in progress, and this replica"
        + " reads it once it is upgraded), or an event store wired with upcasters this one lacks."
        + " Reading it would silently mis-bind the payload, so the read is refused; register the"
        + " same upcasters on every event store that reads this database.";
  }

  /**
   * Decodes one raw row, upcasting it when it is stored below the reader's version, with the
   * classification of {@code PostgresEventStore}: {@link EventSchemaAheadException} for a row
   * stored above the reader's version, a plain {@link EventStoreException} for a transient crypto
   * failure, {@link EventDeserializationException} for every other failure to bind the stored row.
   */
  private EventEnvelope toEnvelope(RawEventRow row) {
    EventType eventType = new EventType(row.eventType());
    int schemaVersion = row.schemaVersion();
    Class<?> eventClass = typeRegistry.resolveEventType(eventType);
    try {
      int currentVersion = upcasterChain.currentVersion(eventType);
      if (schemaVersion > currentVersion) {
        throw new EventSchemaAheadException(
            schemaAheadMessage(
                LogSanitizer.sanitizeForLog(eventType.value()),
                row.streamText(),
                schemaVersion,
                currentVersion),
            eventType,
            schemaVersion,
            currentVersion);
      }
      Object event;
      if (!upcasterChain.isEmpty() && schemaVersion < currentVersion) {
        Map<String, Object> rawData = objectMapper.readValue(row.payloadJson(), MAP_TYPE);
        Map<String, Object> upcasted = upcasterChain.upcast(eventType, rawData, schemaVersion);
        event = objectMapper.convertValue(upcasted, eventClass);
      } else {
        event = objectMapper.readValue(row.payloadJson(), eventClass);
      }
      EventMetadata metadata = objectMapper.readValue(row.metadataJson(), EventMetadata.class);
      return new EventEnvelope(
          GlobalOffset.of(row.globalOffset()),
          StreamId.of(new AggregateType(row.aggregateType()), new AggregateId(row.aggregateId())),
          new Version(row.version()),
          eventType,
          (DomainEvent) event,
          metadata);
    } catch (IOException | RuntimeException e) {
      if (e instanceof EventStoreException ese) {
        throw ese;
      }
      if (hasTransientCryptoCause(e)) {
        throw new EventStoreException(
            "Failed to deserialize event data for stream: " + row.streamText(), e);
      }
      throw new EventDeserializationException(
          "Failed to deserialize event data for stream: " + row.streamText(), e);
    }
  }
}

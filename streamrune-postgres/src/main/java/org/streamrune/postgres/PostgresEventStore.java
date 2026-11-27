package org.streamrune.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.EncryptedTypeGuard;
import org.streamrune.runtime.GuardedTracing;
import org.streamrune.runtime.jdbc.Causes;
import org.streamrune.runtime.jdbc.EventAppendSupport;
import org.streamrune.runtime.jdbc.EventAppendSupport.EventRow;
import org.streamrune.runtime.jdbc.EventAppendSupport.SnapshotPayload;
import org.streamrune.runtime.jdbc.EventReadPath;
import org.streamrune.runtime.jdbc.EventReadPath.RawEventRow;
import org.streamrune.runtime.jdbc.EventReadPath.StoredSnapshot;
import org.streamrune.runtime.jdbc.StoreObjectMappers;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * PostgreSQL-backed {@link EventStore} using JSONB for event payloads and metadata. Supports
 * optimistic locking via a unique constraint on {@code (aggregate_type, aggregate_id, version)}
 * ({@code event_stream_stream_version_key}) and optional snapshot storage for fast aggregate
 * rehydration.
 *
 * <p><b>All appends serialize on the global-offset counter.</b> {@code global_offset} is assigned
 * by reserving a dense block from the single-row {@code global_offset_sequence} counter (see {@link
 * #insertEvents}) inside the append transaction. That reservation {@code UPDATE ... WHERE id = 1}
 * takes a row-exclusive lock on the single counter row and PostgreSQL holds it until the append
 * transaction commits or rolls back — so a second append, <em>even to a completely unrelated
 * stream</em>, cannot reserve its offsets until the first append fully commits. Every append's
 * commit path (reserve → batch INSERT → outbox → audit → commit/fsync) is therefore globally
 * serialized: this is the deliberate price of a gap-free global offset, not a lock-free fast path.
 * Consequences operators must size for:
 *
 * <ul>
 *   <li><b>Single-writer throughput ceiling.</b> Sustained global write throughput is bounded by
 *       the serial single-append commit rate (roughly one critical section per fsync — a few
 *       hundred to low-thousands of append transactions/sec on healthy PostgreSQL with {@code
 *       synchronous_commit=on}), <em>not</em> by appender concurrency or aggregate count. Adding
 *       app instances or threads does not raise the append ceiling; provision for this serial rate.
 *   <li><b>Aggressive {@code lock_timeout}/{@code statement_timeout} surfaces as command
 *       failures.</b> Under high append concurrency, appends queued on the contended counter row
 *       can hit such a timeout and fail with an {@link EventStoreException} — expected on a
 *       serialization point, so set these timeouts with the serial append path in mind.
 *   <li><b>Unaffected: same-aggregate ordering and reads.</b> Per-stream ordering is enforced
 *       independently by the {@code UNIQUE (aggregate_type, aggregate_id, version)}
 *       optimistic-concurrency check, and all read/projection paths ({@link #readGlobalStream},
 *       {@link #load}, {@link #readStream}) never touch the counter and run fully in parallel with
 *       each other and with appends.
 * </ul>
 *
 * <p><b>Isolation: the append transaction runs at READ COMMITTED.</b> The gapless counter and the
 * {@code 23505} conflict mapping depend on READ COMMITTED semantics — a blocked {@code
 * RESERVE_OFFSETS} UPDATE must re-read the just-committed counter row and proceed when the holder
 * commits. Under REPEATABLE READ or SERIALIZABLE that same unblock instead aborts with {@code
 * serialization_failure} (SQLState {@code 40001}). {@link #append} and {@link #appendWithKey}
 * therefore pin READ COMMITTED with a transaction-scoped {@code SET TRANSACTION ISOLATION LEVEL}
 * issued as the first statement inside the transaction (see {@link #beginAppendTransaction}), so
 * the design is independent of the connection/server default (e.g. a server with {@code
 * default_transaction_isolation='repeatable read'} or a pool that sets it). As defense-in-depth a
 * {@code 40001} is additionally mapped to a retryable {@link OptimisticLockException} (see {@link
 * #isRetryableConflict}) rather than a dead-lettering {@link EventStoreException}.
 *
 * <p>The reservation runs after every event has already been serialized by {@link #serializeRows} —
 * which runs BEFORE the JDBC connection is even acquired, so neither the counter's row lock nor the
 * pooled connection itself is ever held across a potentially slow {@code @Encrypted} field
 * KMS/Vault round trip. A version conflict or any other abort rolls back the offset reservation
 * along with everything else, so the offsets are reused by the next append instead of leaving a
 * permanent hole — unlike a sequence-backed default, which is consumed before the {@code UNIQUE
 * (aggregate_type, aggregate_id, version)} check can fail the append.
 *
 * <p>The mechanics an append shares with the other JDBC event store (its spans, the per-event and
 * snapshot serialization, the argument rules, the strict head check and the mapping of a failed
 * append to its exception) live in {@link EventAppendSupport}; the transaction, the statements, the
 * NUL check of {@code jsonb} and the classification of PostgreSQL's errors stay here.
 *
 * <p>Because the counter's row lock is held until commit and released (and the reservation undone)
 * on rollback, a committed offset N implies every offset below N is already committed — the global
 * stream has no holes, and the only offset that can ever be missing is the single tail append
 * currently holding the counter. {@link #readGlobalStream(GlobalOffset, int)} exploits this
 * directly: it needs no visibility gate over transaction ids or {@code pg_snapshot_*} — it simply
 * selects committed rows above the checkpoint in offset order and, as a defense-in-depth guard,
 * stops at the first discontinuity inside a page (a hole at the start of a page — an offset that
 * will never appear — is paged over; see {@link #readGlobalStream(GlobalOffset, int)}). Because
 * delivery depends only on the contiguity of {@code event_stream} rows and never on any other
 * transaction in the database, an unrelated long-running transaction elsewhere can never stall it,
 * as a transaction-id watermark would, which withholds every reader until the oldest in-flight
 * transaction database-wide settles. Same-aggregate ordering is enforced independently by the
 * {@code UNIQUE (aggregate_type, aggregate_id, version)} optimistic-concurrency check.
 *
 * <p>Read paths ({@link #load}, {@link #readStream}, {@link #readGlobalStream}) apply the same
 * connection-scope discipline as writes, just in reverse: each collects raw column values (see
 * {@code RawEventRow}) while the connection is open, closes it, and only then deserializes —
 * decrypting any {@code @Encrypted} field through {@link EventReadPath} — so a slow KMS/Vault round
 * trip on the read side can never hold a pooled connection either.
 *
 * <p><b>Connections and the statement bound.</b> Every operation borrows one connection from the
 * {@link DataSource} it was built with and returns it when the operation ends; the store keeps no
 * connection and no pool of its own, so the application's pool is the only one to size. Each
 * statement the store runs is bounded by {@link Builder#statementTimeout(Duration)} (30 seconds by
 * default): an append transaction starts with {@code SET LOCAL statement_timeout}, which covers the
 * outbox, inbox and audit writes made in that transaction and ends with it; every other statement
 * carries the bound as its JDBC query timeout. Neither changes the connection's session, so the
 * application's own {@code statement_timeout} is back in force for the next borrower. A statement
 * that runs past the bound fails with SQLState {@code 57014}, surfaced as an {@link
 * EventStoreException}. The append transaction also starts with {@code SET LOCAL
 * idle_in_transaction_session_timeout}, the same number of milliseconds: a client that goes silent
 * inside an append — a process paused, or cut off from the database — has its session ended by the
 * server at the bound (SQLState {@code 25P03}), which releases the command's inbox key, a saga's
 * shared fence lock and the global offset counter. So a saga-row write waiting behind a silent
 * append waits for at most one statement timeout.
 *
 * <p><b>Tracing.</b> With a {@link StreamRuneTracing} whose {@link TracingArea#EVENT_STORE} area is
 * on ({@link Builder#tracing}), an append starts {@value TraceAttributes#EVENT_STORE_APPEND} with
 * the child {@value TraceAttributes#EVENT_STORE_APPEND_SERIALIZE}; a load starts {@value
 * TraceAttributes#EVENT_STORE_LOAD} with the children {@value
 * TraceAttributes#EVENT_STORE_SNAPSHOT_LOAD} (when the stream has a stored snapshot) and {@value
 * TraceAttributes#EVENT_STORE_LOAD_DESERIALIZE}; a snapshot write starts {@value
 * TraceAttributes#EVENT_STORE_SNAPSHOT_SAVE}; and a global or stream read that returns events,
 * under a span current on its thread, starts {@value TraceAttributes#EVENT_STORE_READ} as its
 * child, which covers its query and the events' deserialization ({@link
 * StreamRuneTracing#startChild}). A read that returns nothing starts no span, and neither does a
 * read with no span current, the poll of a subscription or a projection runner: its consumer's span
 * is the one trace of the poll.
 *
 * <p><b>The saga forward fence.</b> {@link #appendWithKey(StreamId, List, Version, IdempotencyKey,
 * String, SagaOrigin)} with a saga origin takes the saga's shared fence lock in the append
 * transaction — right after the inbox claim, before the offset reservation — then reads the saga's
 * {@code saga_state} row, and refuses the append ({@link SagaForwardFencedException}, transaction
 * rolled back) when the row carries an episode stamp, is terminal, or is absent. Every write of
 * {@code PostgresSagaStore} that can bring a row to one of those states takes the exclusive form of
 * the lock before its statement, in its transaction ({@link SagaFenceLock}); the lock is held to
 * commit on both sides, so the append commits wholly before such a write, or waits behind it, reads
 * the written row and is refused. The read is a statement of its own, after the lock is granted, so
 * READ COMMITTED gives it a snapshot that contains every write the lock waited for. The lock order
 * of a fenced append is inbox key → saga fence lock (shared) → offset counter → commit, so no
 * append waits for a saga while it holds the system-wide counter; a fence-changing write takes the
 * saga fence lock (exclusive) and then the row. The rows are those of a {@code PostgresSagaStore}
 * built over the same {@link DataSource} ({@link #sagaFenceStorageIdentity()}).
 */
public final class PostgresEventStore implements EventStore {

  private static final Logger log = LoggerFactory.getLogger(PostgresEventStore.class);

  /**
   * PostgreSQL {@code unique_violation} — the {@code UNIQUE (aggregate_type, aggregate_id,
   * version)} conflict ({@code event_stream_stream_version_key}).
   */
  private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

  /**
   * The PRIMARY KEY on {@code event_stream.global_offset} (V001). {@code event_stream} carries two
   * unique constraints; a 23505 on THIS one is a desynced offset counter, not a version conflict —
   * see {@link #isGlobalOffsetCollision}.
   */
  private static final String EVENT_STREAM_PK_CONSTRAINT = "event_stream_pkey";

  /**
   * PostgreSQL {@code serialization_failure} — a concurrent append aborted on the shared
   * global-offset counter row under REPEATABLE READ/SERIALIZABLE. See {@link #append} for why the
   * append transaction pins READ COMMITTED (so this normally cannot arise) and why it is still
   * mapped to a retryable {@link OptimisticLockException} as defense-in-depth.
   */
  private static final String SQLSTATE_SERIALIZATION_FAILURE = "40001";

  private static final String LOAD_SNAPSHOT =
      "SELECT version, state_type, state_payload, snapshot_version, purge_generation"
          + " FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?";

  // Table-wide, not per-stream — the warning is about whether snapshot_store has ANY
  // leftover row, not about the specific stream that happened to trigger the (now one-shot-per-
  // instance) probe. LIMIT 1: existence-only, deliberately NOT state_payload, so it never pulls
  // (and never needs to decrypt) the actual snapshot content.
  private static final String SNAPSHOT_STATE_TYPE_FOR_WARNING =
      "SELECT state_type FROM snapshot_store WHERE state_type IS NOT NULL LIMIT 1";

  private static final String LOAD_EVENTS =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ? AND version > ? ORDER BY version";

  private static final String INSERT_EVENT =
      "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version, event_type,"
          + " payload, metadata, schema_version) VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)";

  /**
   * The append transaction's isolation pin, issued as the FIRST statement inside the transaction so
   * PostgreSQL scopes it to that transaction and reverts it at commit/rollback — see {@link
   * #beginAppendTransaction}.
   */
  private static final String PIN_READ_COMMITTED = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";

  // Reserves N dense global offsets from the single-row application counter, inside the append
  // transaction. The row lock this UPDATE takes serializes offset assignment across concurrent
  // appenders and is held until commit/rollback: on commit the reserved block is durable and
  // never reused; on rollback (a version conflict or any other abort) the reservation is undone
  // by Postgres's normal MVCC rollback, so the SAME offsets are handed out to the next append —
  // no permanent hole, unlike a sequence-backed default (consumed before the
  // UNIQUE (aggregate_type, aggregate_id, version) check, so every conflict would burn an offset).
  // The saga forward fence's read, run inside a forward command's append transaction after the
  // saga's shared fence lock is granted (SagaFenceLock), as a statement of its own so its snapshot
  // contains every fence-changing write the lock waited for. No row, or forward = false, refuses
  // the append. A stamp is never cleared and a terminal status never left, so the predicate only
  // ever turns from true to false (a row created later turns it from false to true, which admits
  // nothing that was refused).
  private static final String FENCE_SAGA_FORWARD =
      "SELECT episode_version IS NULL AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')"
          + " AS forward FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  private static final String RESERVE_OFFSETS =
      "UPDATE global_offset_sequence SET next_value = next_value + ? WHERE id = 1 "
          + "RETURNING next_value";

  // The strict optimistic-concurrency head check insertEvents performs
  // right after RESERVE_OFFSETS. Uses the unique index event_stream_stream_version_key
  // (aggregate_type, aggregate_id, version), V001, for an index-only MAX(version).
  private static final String STREAM_HEAD_VERSION =
      "SELECT COALESCE(MAX(version), 0) FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ?";

  // Gapless global reads: event_stream.global_offset is assigned from a commit-ordered counter
  // (global_offset_sequence) whose row lock is held until the append commits, and rolled back on
  // abort — so a committed offset N implies every offset < N is committed. The global stream
  // therefore has no holes; a missing offset can only be the single tail append currently holding
  // the counter. Reading needs no visibility gate: select the committed rows above the checkpoint
  // in offset order and deliver the contiguous prefix. No dependence on transaction ids /
  // pg_snapshot_*, so delivery never stalls behind any other transaction.
  private static final String READ_GLOBAL =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream WHERE global_offset > ? "
          + "ORDER BY global_offset LIMIT ?";

  private static final String NOTIFY_NEW_EVENTS = "SELECT pg_notify('streamrune_events', ?)";

  // Writes only while the row still has the purge generation the command's load saw. The purge
  // (PostgresSnapshotStorePurger) upserts the same primary key, so the unique index serializes the
  // two statements: a purge committed first makes the WHERE fail and the save writes nothing; a
  // save committed first is overwritten by the purge. purge_generation is never written here.
  private static final String UPSERT_SNAPSHOT_VERSIONED =
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

  // Rows fetched per driver round trip when loading a stream. pgjdbc honors fetchSize only
  // inside an explicit transaction (autocommit off); under autocommit it materializes the whole
  // result set before returning the first row, doubling peak memory for long streams.
  private static final int LOAD_FETCH_SIZE = 500;

  private final DataSource dataSource;
  private final UpcasterChain upcasterChain;
  private final PostgresEventAuditStore eventAuditStore;
  private final PostgresOutboxStore outboxStore;
  private final OutboxEventMapper outboxEventMapper;
  private final PostgresCommandInbox commandInbox;
  private final StatementTimeout statementTimeout;

  // Set on the FIRST call to warnIfLegacySnapshotExists, regardless of outcome, so the probe query
  // itself runs at most ONCE per store instance for its whole lifetime. With legacySnapshotWarned
  // (below) as the only early-return — set ONLY when a legacy row is FOUND — every single aggregate
  // load on the default never() deployment with zero snapshot_store rows (the common case: an app
  // that never configured snapshots) would pay an extra pool checkout + query, forever. This flag
  // decouples "have we probed" from "did we find anything", which is what makes the probe actually
  // one-shot.
  private final AtomicBoolean legacySnapshotProbed = new AtomicBoolean(false);

  // Set once this instance has warned that snapshot_store still has a row — a leftover
  // from before SnapshotPolicy switched to never(), never read again and never overwritten.
  // Scoped to this store instance so a long-lived process logs it once. Reaching the WARN below at
  // all already implies legacySnapshotProbed just transitioned false->true (single-probe
  // guarantee), so this second flag is redundant as a GUARD — kept for its documentation value and
  // as a direct, self-contained pin target (ignoreSnapshotWarnsOnceWhenALegacySnapshotRowIsFound
  // asserts on the WARN, not on the probe count).
  private final AtomicBoolean legacySnapshotWarned = new AtomicBoolean(false);

  /** The tracing the event-store spans start through; {@link StreamRuneTracing#NOOP} by default. */
  private final StreamRuneTracing tracing;

  /** Whether {@link #tracing} records the event-store area, read once. */
  private final boolean traced;

  /**
   * The read path of this instance: snapshot decision and migration, event decoding, the load's
   * history and the contiguity guards, with their spans, logs (under this store's category) and
   * metrics. Owns this instance's once-per-stream version-gap warning state.
   */
  private final EventReadPath readPath;

  /**
   * The append support of this instance: the append and snapshot-save spans, event and snapshot
   * serialization, and the shared argument rules and failure mapping, logging under this store's
   * category.
   */
  private final EventAppendSupport support;

  /**
   * Creates a new PostgresEventStore with optional upcasting and audit support.
   *
   * @param dataSource the JDBC data source (required)
   * @param objectMapper Jackson object mapper (required)
   * @param typeRegistry event/state type registry (required)
   * @param upcasterChain upcaster chain for schema evolution, {@code UpcasterChain.empty()} if not
   *     needed
   * @param eventAuditStore event audit store for same-transaction audit writes, or null if not
   *     needed
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore) {
    this(dataSource, objectMapper, typeRegistry, upcasterChain, eventAuditStore, null, null, null);
  }

  /**
   * Creates a new PostgresEventStore with full control over all collaborators including the outbox
   * event mapper. Package-private: {@link #builder()}, {@link #create(DataSource,
   * EventTypeRegistry, CryptoEngine, List, StreamRuneMetrics)} and {@link
   * PostgresEventStoreFactory} are the public entry points.
   *
   * <p>Performs no {@code @Encrypted} check: {@code objectMapper} is used as given, and a mapper
   * without the crypto module writes {@code @Encrypted} components as plaintext. When a registered
   * type carries {@code @Encrypted}, pass {@link StoreObjectMappers#eventsAndSagas(CryptoEngine,
   * StreamRuneMetrics)} built with the engine, or use {@link #builder()}, which refuses such a type
   * without an engine.
   *
   * @param dataSource the JDBC data source (required)
   * @param objectMapper Jackson object mapper (required)
   * @param typeRegistry event/state type registry (required)
   * @param upcasterChain upcaster chain for schema evolution, {@code UpcasterChain.empty()} if not
   *     needed
   * @param eventAuditStore event audit store for same-transaction audit writes, or null if not
   *     needed
   * @param outboxStore outbox store for same-transaction outbox writes, or null if the outbox is
   *     not used
   * @param outboxEventMapper mapper that derives outbox entries from each appended event, or null
   *     if automatic outbox emission is not needed
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore,
      PostgresOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper) {
    this(
        dataSource,
        objectMapper,
        typeRegistry,
        upcasterChain,
        eventAuditStore,
        outboxStore,
        outboxEventMapper,
        null);
  }

  /**
   * Creates a new PostgresEventStore with full control over all collaborators including the command
   * inbox for idempotent appends. Package-private: {@link #builder()} is the public entry point.
   *
   * <p>Performs no {@code @Encrypted} check: {@code objectMapper} is used as given, and a mapper
   * without the crypto module writes {@code @Encrypted} components as plaintext. When a registered
   * type carries {@code @Encrypted}, pass {@link StoreObjectMappers#eventsAndSagas(CryptoEngine,
   * StreamRuneMetrics)} built with the engine, or use {@link #builder()}, which refuses such a type
   * without an engine.
   *
   * @param dataSource the JDBC data source (required)
   * @param objectMapper Jackson object mapper (required)
   * @param typeRegistry event/state type registry (required)
   * @param upcasterChain upcaster chain for schema evolution, {@code UpcasterChain.empty()} if not
   *     needed
   * @param eventAuditStore event audit store for same-transaction audit writes, or null if not
   *     needed
   * @param outboxStore outbox store for same-transaction outbox writes, or null if the outbox is
   *     not used
   * @param outboxEventMapper mapper that derives outbox entries from each appended event, or null
   *     if automatic outbox emission is not needed
   * @param commandInbox command inbox for idempotent appends, or null if not needed
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore,
      PostgresOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper,
      PostgresCommandInbox commandInbox) {
    this(
        dataSource,
        objectMapper,
        typeRegistry,
        upcasterChain,
        eventAuditStore,
        outboxStore,
        outboxEventMapper,
        commandInbox,
        SnapshotMigrationChain.of(null),
        StreamRuneMetrics.NOOP);
  }

  /**
   * Full constructor including the snapshot migration chain applied on load and the {@link
   * StreamRuneMetrics} collector used on the load path (a discarded-and-replayed snapshot is
   * recorded via {@link StreamRuneMetrics#recordSnapshotDiscarded(String)}). Package-private: the
   * migration chain is validated by {@link Builder#migrations(List)}, the only public entry point,
   * so callers register migrations there rather than constructing a chain directly; production
   * callers reach it through {@link #create(DataSource, EventTypeRegistry, CryptoEngine, List,
   * StreamRuneMetrics)} or {@link Builder}, which pass the same collector they wire into {@link
   * CryptoShreddingModule}.
   *
   * @param snapshotMigrations validated migration chain (never {@code null}; use {@code
   *     SnapshotMigrationChain.of(null)} for none)
   * @param metrics metrics collector; {@link StreamRuneMetrics#NOOP} for none, not {@code null}
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore,
      PostgresOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper,
      PostgresCommandInbox commandInbox,
      SnapshotMigrationChain snapshotMigrations,
      StreamRuneMetrics metrics) {
    this(
        dataSource,
        objectMapper,
        typeRegistry,
        upcasterChain,
        eventAuditStore,
        outboxStore,
        outboxEventMapper,
        commandInbox,
        snapshotMigrations,
        metrics,
        StatementTimeout.of(StatementTimeout.DEFAULT));
  }

  /**
   * Full constructor plus the bound on every statement the store runs (see the class javadoc).
   * Package-private: {@link Builder#statementTimeout(Duration)} is the public entry point.
   *
   * @param statementTimeout the statement bound; {@link StatementTimeout#DISABLED} for none
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore,
      PostgresOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper,
      PostgresCommandInbox commandInbox,
      SnapshotMigrationChain snapshotMigrations,
      StreamRuneMetrics metrics,
      StatementTimeout statementTimeout) {
    this(
        dataSource,
        objectMapper,
        typeRegistry,
        upcasterChain,
        eventAuditStore,
        outboxStore,
        outboxEventMapper,
        commandInbox,
        snapshotMigrations,
        metrics,
        statementTimeout,
        StreamRuneTracing.NOOP);
  }

  /**
   * Full constructor plus the tracing the event-store spans start through. Package-private: {@link
   * Builder#tracing(StreamRuneTracing)} is the public entry point.
   *
   * @param tracing the tracing; {@link StreamRuneTracing#NOOP} traces nothing, not {@code null}
   */
  PostgresEventStore(
      DataSource dataSource,
      ObjectMapper objectMapper,
      EventTypeRegistry typeRegistry,
      UpcasterChain upcasterChain,
      PostgresEventAuditStore eventAuditStore,
      PostgresOutboxStore outboxStore,
      OutboxEventMapper outboxEventMapper,
      PostgresCommandInbox commandInbox,
      SnapshotMigrationChain snapshotMigrations,
      StreamRuneMetrics metrics,
      StatementTimeout statementTimeout,
      StreamRuneTracing tracing) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    this.dataSource = dataSource;
    this.upcasterChain = Objects.requireNonNull(upcasterChain, "upcasterChain is required");
    this.eventAuditStore = eventAuditStore;
    this.outboxStore = outboxStore;
    this.outboxEventMapper = outboxEventMapper;
    this.commandInbox = commandInbox;
    SnapshotMigrationChain migrations =
        snapshotMigrations == null ? SnapshotMigrationChain.of(null) : snapshotMigrations;
    Objects.requireNonNull(metrics, "metrics is required");
    this.statementTimeout =
        statementTimeout == null ? StatementTimeout.of(StatementTimeout.DEFAULT) : statementTimeout;
    this.tracing =
        GuardedTracing.of(Objects.requireNonNull(tracing, "tracing is required"), metrics);
    this.traced = this.tracing.enabled(TracingArea.EVENT_STORE);
    this.readPath =
        new EventReadPath(
            objectMapper,
            typeRegistry,
            this.upcasterChain,
            migrations,
            metrics,
            this.tracing,
            this.traced,
            log);
    this.support = new EventAppendSupport(objectMapper, typeRegistry, this.tracing, log);
  }

  /**
   * Creates a new PostgresEventStore with auto-configured ObjectMapper and optional CryptoEngine
   * and Upcasters. The crypto-redaction metrics default to {@link StreamRuneMetrics#NOOP}; use
   * {@link #create(DataSource, EventTypeRegistry, CryptoEngine, List, StreamRuneMetrics)} to feed a
   * real collector.
   *
   * <p>Refuses an {@code @Encrypted} event or state type without a {@code cryptoEngine}, before it
   * touches the database; see {@link #create(DataSource, EventTypeRegistry, CryptoEngine, List,
   * StreamRuneMetrics)}.
   *
   * @param dataSource the JDBC data source (required)
   * @param typeRegistry event/state type registry (required)
   * @param cryptoEngine crypto engine for encryption; {@code null} only when no registered event or
   *     state type carries {@code @Encrypted}
   * @param upcasters list of upcasters for schema evolution (optional, can be null or empty)
   * @return configured PostgresEventStore
   * @throws IllegalArgumentException if {@code typeRegistry} is {@code null}
   * @throws IllegalStateException if {@code cryptoEngine} is {@code null} and a registered event or
   *     state type carries {@code @Encrypted}
   * @throws UnsupportedOperationException if {@code cryptoEngine} is {@code null} and {@code
   *     typeRegistry} does not implement {@link EventTypeRegistry#registeredTypes()}
   * @throws UnsupportedServerVersionException if the server is older than PostgreSQL 17
   */
  public static PostgresEventStore create(
      DataSource dataSource,
      EventTypeRegistry typeRegistry,
      CryptoEngine cryptoEngine,
      List<EventUpcaster> upcasters) {
    return create(dataSource, typeRegistry, cryptoEngine, upcasters, StreamRuneMetrics.NOOP);
  }

  /**
   * Creates a new PostgresEventStore with auto-configured ObjectMapper, optional CryptoEngine and
   * Upcasters, and a {@link StreamRuneMetrics} collector.
   *
   * <p>The metrics collector is threaded into {@link CryptoShreddingModule} so the crypto-redaction
   * signals ({@code streamrune.crypto.subject_redacted} and {@code keystore_systemic_failure})
   * actually fire on the event-replay/projection decrypt path — where a mass redaction (a
   * wiped/misconfigured key store) would otherwise be silent. Without this the documented
   * GDPR-outage alerting is permanently zero, because the event store built the mapper with the
   * NOOP-metrics constructor.
   *
   * <p>Refuses a server older than PostgreSQL 17 ({@link UnsupportedServerVersionException}) with
   * one read of the connection's server-version metadata, as {@link
   * PostgresEventStoreFactory#create()} does — this is the plain-Java entry point that does not go
   * through the factory.
   *
   * <p>Before that read it runs the same {@code @Encrypted} check as {@link
   * PostgresEventStoreFactory#create()}: without a {@code cryptoEngine} the mapper has no crypto
   * module and would write every {@code @Encrypted} component as plaintext into the append-only
   * log, with no key ever minted for a later erasure to delete. So when {@code cryptoEngine} is
   * {@code null}, a registered event or state type with an {@code @Encrypted} component (nested
   * records included) is refused with {@link IllegalStateException} naming every such field, and a
   * registry that cannot list its types is refused with the {@link UnsupportedOperationException}
   * of {@link EventTypeRegistry#registeredTypes()}. With an engine, the check only warns about a
   * property-level Jackson serializer override that would bypass encryption (see {@link
   * EncryptedTypeGuard}).
   *
   * @param dataSource the JDBC data source (required)
   * @param typeRegistry event/state type registry (required)
   * @param cryptoEngine crypto engine for encryption; {@code null} only when no registered event or
   *     state type carries {@code @Encrypted}
   * @param upcasters list of upcasters for schema evolution (optional, can be null or empty)
   * @param metrics the metrics collector for crypto-redaction observability; {@link
   *     StreamRuneMetrics#NOOP} for none, not {@code null}
   * @return configured PostgresEventStore
   * @throws IllegalArgumentException if {@code typeRegistry} is {@code null}
   * @throws IllegalStateException if {@code cryptoEngine} is {@code null} and a registered event or
   *     state type carries {@code @Encrypted}
   * @throws UnsupportedOperationException if {@code cryptoEngine} is {@code null} and {@code
   *     typeRegistry} does not implement {@link EventTypeRegistry#registeredTypes()}
   * @throws UnsupportedServerVersionException if the server is older than PostgreSQL 17
   */
  public static PostgresEventStore create(
      DataSource dataSource,
      EventTypeRegistry typeRegistry,
      CryptoEngine cryptoEngine,
      List<EventUpcaster> upcasters,
      StreamRuneMetrics metrics) {
    EventAppendSupport.validateCryptoConfig(typeRegistry, cryptoEngine);
    PostgresServerVersion.requireSupported(dataSource);

    UpcasterChain chain = upcasters == null ? UpcasterChain.empty() : new UpcasterChain(upcasters);

    return new PostgresEventStore(
        dataSource,
        StoreObjectMappers.eventsAndSagas(cryptoEngine, metrics),
        typeRegistry,
        chain,
        null,
        null,
        null,
        null,
        SnapshotMigrationChain.of(null),
        metrics);
  }

  /**
   * Creates a builder for PostgresEventStore with sensible defaults.
   *
   * @return builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public AggregateHistory load(StreamId streamId) {
    return load(streamId, 0);
  }

  @Override
  public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
    return readPath.load(
        streamId, expectedSnapshotVersion, () -> loadHistory(streamId, expectedSnapshotVersion));
  }

  private AggregateHistory loadHistory(StreamId streamId, int expectedSnapshotVersion) {
    // EventStore.IGNORE_SNAPSHOT means "never consult a snapshot at all" — the read-side
    // counterpart of SnapshotPolicy.never(). Skips the LOAD_SNAPSHOT query entirely rather than
    // reading-then-discarding, and takes a completely separate, simpler path: a snapshot left over
    // from before the policy switched to never() must never be deserialized (a stale schema, a
    // decrypt attempt against possibly-rotated keys) let alone fed to decide().
    if (expectedSnapshotVersion == EventStore.IGNORE_SNAPSHOT) {
      return loadIgnoringSnapshot(streamId);
    }
    // Raw snapshot columns read inside the connection; the snapshot state is deserialized (which
    // may trigger @Encrypted field KMS/Vault decryption via Jackson's CryptoShreddingModule) only
    // AFTER the connection is released below — see the class javadoc on why decrypt must never
    // run while a pooled connection is held.
    StoredSnapshot snapshot;
    List<RawEventRow> rawRows;

    try (var conn = dataSource.getConnection()) {
      // Read inside an explicit transaction so the driver streams the event rows in
      // LOAD_FETCH_SIZE batches instead of materializing the whole stream — see LOAD_FETCH_SIZE.
      conn.setAutoCommit(false);
      try {
        // Load snapshot if present, skipping if schema version is incompatible
        try (var ps = conn.prepareStatement(LOAD_SNAPSHOT)) {
          statementTimeout.applyTo(ps);
          StreamKeys.bind(ps, 1, streamId);
          try (var rs = ps.executeQuery()) {
            // Decided here, before LOAD_EVENTS, so a snapshot the load cannot use never narrows
            // the event read; its deserialization and migration run after the connection closed.
            snapshot =
                rs.next()
                    ? readPath.decideSnapshot(rs, expectedSnapshotVersion)
                    : StoredSnapshot.NONE;
          }
        }

        // Read raw events after snapshot version (Version.initial() = load all events). No
        // decryption here — just raw column values collected while the connection is open.
        rawRows = new ArrayList<>();

        try (var ps = conn.prepareStatement(LOAD_EVENTS)) {
          statementTimeout.applyTo(ps);
          ps.setFetchSize(LOAD_FETCH_SIZE);
          StreamKeys.bind(ps, 1, streamId);
          ps.setLong(3, snapshot.version().value());
          try (var rs = ps.executeQuery()) {
            while (rs.next()) {
              rawRows.add(EventReadPath.readRawRow(rs));
            }
          }
        }

        conn.commit();
      } catch (SQLException | RuntimeException e) {
        conn.rollback();
        throw e;
      } finally {
        // Restore auto-commit before the connection returns to the pool
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to load stream: " + streamText(streamId), e);
    }

    // Deserialize (and decrypt, if any field is @Encrypted) AFTER the connection is released. A
    // discarded snapshot re-reads the whole stream on a connection of its own (readRawEvents).
    return readPath.assembleHistory(
        streamId,
        snapshot,
        rawRows,
        expectedSnapshotVersion,
        after -> readRawEvents(streamId, after));
  }

  /**
   * Reads the raw event rows of a stream after {@code afterVersion}, in its own short transaction
   * (fetch-size streaming, exactly like the main load read). Used by the snapshot-discard fallback
   * to re-read the FULL stream ({@code afterVersion == Version.initial()}) once the derived
   * snapshot cache has been discarded — the original read was narrowed to post-snapshot versions.
   */
  private List<RawEventRow> readRawEvents(StreamId streamId, Version afterVersion) {
    List<RawEventRow> rows = new ArrayList<>();
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        try (var ps = conn.prepareStatement(LOAD_EVENTS)) {
          statementTimeout.applyTo(ps);
          ps.setFetchSize(LOAD_FETCH_SIZE);
          StreamKeys.bind(ps, 1, streamId);
          ps.setLong(3, afterVersion.value());
          try (var rs = ps.executeQuery()) {
            while (rs.next()) {
              rows.add(EventReadPath.readRawRow(rs));
            }
          }
        }
        conn.commit();
      } catch (SQLException | RuntimeException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to load stream: " + streamText(streamId), e);
    }
    return rows;
  }

  /**
   * The {@link EventStore#IGNORE_SNAPSHOT} load path — full replay from the beginning, {@code
   * snapshotState} always {@code null}, no {@code snapshot_store} read beyond the best-effort
   * existence probe for {@link #warnIfLegacySnapshotExists}. Reuses {@link #readRawEvents}, the
   * same fetch-size-streamed full-stream read the snapshot-discard fallback already relies on.
   */
  private AggregateHistory loadIgnoringSnapshot(StreamId streamId) {
    warnIfLegacySnapshotExists();
    return readPath.historyIgnoringSnapshot(streamId, readRawEvents(streamId, Version.initial()));
  }

  /**
   * Best-effort, one-shot-per-instance WARN when {@code snapshot_store} still has ANY row after a
   * load ran with {@link EventStore#IGNORE_SNAPSHOT} — evidence that {@code SnapshotPolicy}
   * switched to {@code never()} on a deployment that had been actively snapshotting. Those rows are
   * harmless (never read, never overwritten — {@code maybeSnapshot} no-ops under {@code never()}
   * too) but sit there indefinitely; this only tells an operator they exist so they can choose to
   * purge them. Deliberately reads only {@code state_type}, never {@code state_payload} — this is a
   * diagnostic, not a load, and must never trigger a decrypt. Any failure of this probe is logged
   * at DEBUG and swallowed: it must never affect the load it rides along with.
   *
   * <p>{@code legacySnapshotProbed} makes this method run its query body AT MOST ONCE per store
   * instance, regardless of outcome. With {@code legacySnapshotWarned} as the only guard — set only
   * when a row is FOUND — every single aggregate load on the default {@code never()} deployment
   * with zero {@code snapshot_store} rows (an app that never configured snapshots, the common case
   * and the exact population with nothing to find) would pay an extra pool checkout outside the
   * read transaction plus this query, forever. The probe is table-wide rather than per-stream
   * ({@link #SNAPSHOT_STATE_TYPE_FOR_WARNING}): the warning is about the table, not about whichever
   * stream happened to trigger the one shot.
   */
  private void warnIfLegacySnapshotExists() {
    if (!legacySnapshotProbed.compareAndSet(false, true)) {
      return;
    }
    String stateType;
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SNAPSHOT_STATE_TYPE_FOR_WARNING)) {
      statementTimeout.applyTo(ps);
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          return;
        }
        stateType = rs.getString("state_type");
      }
    } catch (SQLException e) {
      log.debug("Could not probe snapshot_store for a legacy row: {}", e.toString());
      return;
    }
    if (legacySnapshotWarned.compareAndSet(false, true)) {
      log.warn(
          "snapshot_store still has at least one row (e.g. state type {}), but a load just ran"
              + " with SnapshotPolicy.never() (EventStore.IGNORE_SNAPSHOT) — under never() such"
              + " rows are never read and never overwritten (never() also disables new snapshots),"
              + " so they will sit there indefinitely. If this deployment switched from an active"
              + " snapshot policy to never(), consider purging the now-unused rows from"
              + " snapshot_store. This warning is logged once.",
          LogSanitizer.sanitizeForLog(stateType));
    }
  }

  @Override
  public AppendResult append(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
    // The arguments are checked before the outbox mapper, application code, sees the events.
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(events, "events is required");
    Objects.requireNonNull(expectedVersion, "expectedVersion is required");
    return appendWithOutbox(
        streamId,
        events,
        expectedVersion,
        EventAppendSupport.outboxEntriesOf(events, outboxStore != null, outboxEventMapper));
  }

  /**
   * Appends events and outbox entries in one transaction (the transactional outbox pattern): either
   * the domain events and the outbox rows all commit, or none do. The outbox relay ({@code
   * OutboxPoller}) then publishes the committed entries with at-least-once semantics.
   *
   * <p><b>Empty appends.</b> An empty {@code events} list is the documented {@link
   * org.streamrune.core.EventStore#append} no-op ONLY when {@code outboxEntries} is empty too.
   * Entries passed alongside an empty events list are <b>refused</b> with {@link
   * IllegalArgumentException}: this method's contract is atomic co-commit WITH the events, and
   * there is nothing to co-commit with. An early return for empty events, before any transaction
   * opens, would discard the entries silently while the caller receives a success result —
   * permanent, undetectable loss of an explicitly-requested integration event. {@code
   * outbox_events} rows are self-contained (no event or offset reference), so an entry CAN
   * legitimately stand alone — persist it directly with {@link
   * org.streamrune.core.outbox.OutboxStore#save(org.streamrune.core.outbox.OutboxEntry)
   * OutboxStore.save(entry)}, one entry per call. That is a <em>standalone</em> write: the SPI has
   * no connection-accepting overload, and {@link PostgresOutboxStore#save} commits on its own
   * pooled connection, so such an entry co-commits with nothing (the batch write this method uses
   * to co-commit with the events, {@code PostgresOutboxStore.saveAll(List, Connection)}, is
   * deliberately package-private — enlisting an outbox write in a caller-supplied transaction is
   * not part of the SPI). Silently persisting the entries here in a private transaction would hand
   * the caller exactly that weaker guarantee under a method that promises co-commit, with no way to
   * tell the difference.
   *
   * <p><b>Trace link.</b> Every entry's {@link OutboxEntry#traceLink() traceLink} is the link of
   * the first event's metadata, its {@code spanContext}, via {@link SpanLink#of}: the events of one
   * append come from one command, so the relay's {@code outbox/publish} span links to that
   * command's span. A link the caller set on an entry is replaced; {@link SpanLink#NONE} when the
   * command was not traced.
   *
   * @param streamId the stream to append to (required)
   * @param events the events to append (required)
   * @param expectedVersion the expected current stream version for optimistic locking (required)
   * @param outboxEntries outbox entries to persist atomically with the events (required; may be
   *     empty). Non-empty entries require an outbox store — see {@link
   *     Builder#outboxStore(PostgresOutboxStore)} — and at least one event.
   * @return the append result with assigned global offsets and the new stream version
   * @throws IllegalStateException if outboxEntries is non-empty and no outbox store was configured
   * @throws IllegalArgumentException if outboxEntries is non-empty and events is empty
   * @throws NullPointerException if streamId, events, expectedVersion or outboxEntries is null
   */
  public AppendResult append(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries) {
    List<OutboxEntry> linked =
        outboxEntries == null || outboxEntries.isEmpty() || events == null || events.isEmpty()
            ? outboxEntries
            : EventAppendSupport.withTraceLink(
                outboxEntries, EventAppendSupport.traceLinkOf(events.getFirst()));
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
    return support.tracedAppend(
        streamId,
        events.size(),
        expectedVersion,
        outboxEntries.size(),
        () -> appendEvents(streamId, events, expectedVersion, outboxEntries, true),
        result -> "APPENDED");
  }

  /**
   * The append.
   *
   * @param traced whether the append runs inside its span, so its serialization starts a child
   */
  private AppendResult appendEvents(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries,
      boolean traced) {
    // The empty-events no-op returns BEFORE any transaction is opened, so outboxStore.saveAll
    // below is unreachable; entries passed with no events are refused rather than lost silently.
    Optional<AppendResult> noOp =
        EventAppendSupport.preamble(
            streamId,
            events,
            expectedVersion,
            outboxEntries,
            outboxStore != null,
            "Outbox entries were passed but no PostgresOutboxStore is configured on this event"
                + " store. Configure a PostgresOutboxStore together with an OutboxEventMapper: on the"
                + " builder or factory, or as the outbox bean pair of an integration");
    if (noOp.isPresent()) {
      return noOp.get();
    }

    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());

    // Serialize (Jackson, including any @Encrypted field KMS/Vault calls) BEFORE acquiring a
    // pooled connection — see serializeRows — so a slow crypto round trip never holds a connection
    // the pool could otherwise hand to another caller.
    List<EventRow> rows;
    try {
      rows =
          support.serialize(
              events.size(), traced, () -> serializeRows(streamId, events, expectedVersion));
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize event for stream: " + shownStream, e);
    }

    // Set true the instant conn.commit() returns — the events are durably persisted from then on.
    // Any exception AFTER this point (post-commit connection cleanup: setAutoCommit / close on a
    // connection that died in the window right after commit) must NOT be reported as an append
    // failure: the bus would then treat a committed command as failed, dead-letter it, and a DLQ
    // replay of an UNKEYED command (no inbox row to dedup) would re-append duplicate domain events.
    // Mirrors notifyAfterCommit's "nothing after the commit can fail the append" intent.
    boolean committed = false;
    AppendResult appendResult = null;
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        // Pin READ COMMITTED for the append transaction. The gapless counter + 23505 conflict
        // mapping assume READ COMMITTED semantics: when a blocked appender's RESERVE_OFFSETS UPDATE
        // unblocks after the holder commits, READ COMMITTED re-reads the just-committed counter row
        // and proceeds. Under REPEATABLE READ/SERIALIZABLE (a legitimate server/pool default) that
        // same unblock instead aborts with serialization_failure (40001) — which, unpinned, would
        // dead-letter appends under ANY concurrency. The pin removes the ambient-default dependency
        // entirely, and being TRANSACTION-scoped it leaves nothing behind. The
        // statement bound rides on the same round trip and is transaction-scoped too.
        beginAppendTransaction(conn);

        // insertEvents reserves the global-offset block from the single-row counter; that
        // reservation takes the counter's row-exclusive lock and holds it until THIS transaction
        // commits/rolls back, so every append — even to a different stream — serializes here. That
        // is the deliberate price of a gap-free global offset (see the class javadoc for the
        // single-writer throughput ceiling and the lock_timeout caveat), and it is exactly why
        // gap-free global reads need no read-side visibility gate: the gapless commit-ordered
        // counter (see READ_GLOBAL) guarantees it. Same-aggregate ordering is still enforced
        // by UNIQUE (aggregate_type, aggregate_id, version).
        List<GlobalOffset> globalOffsets = insertEvents(conn, streamId, rows, expectedVersion);

        // Write outbox entries in the same transaction (transactional outbox pattern)
        if (!outboxEntries.isEmpty()) {
          outboxStore.saveAll(outboxEntries, conn);
        }

        // Write event audit entries in the same transaction. Stream id and versions are derived
        // from the append parameters exactly like the INSERT above — never trusted from the
        // envelope — so audit rows always mirror what was written to event_stream even when a
        // direct caller passes envelopes whose streamId/version fields diverge.
        writeAuditEntries(conn, streamId, events, expectedVersion);

        conn.commit();
        committed = true;
        long finalVersion = expectedVersion.value() + events.size();
        appendResult = new AppendResult(List.copyOf(globalOffsets), new Version(finalVersion));

        // Notify subscribers about new events (best-effort — LISTEN connections will be woken up)
        // Reset auto-commit so NOTIFY is actually sent, then restore for pool return
        notifyAfterCommit(conn, globalOffsets);

        return appendResult;
      } catch (SQLException e) {
        // No rollback here — the finally below is the single rollback point (committed == false).
        // Only an append that did not commit reaches this catch: a failure after the commit is
        // post-commit cleanup, handled by the outer catch. The shared mapping discriminates BEFORE
        // the retryable mapping: a 23505 naming the global_offset PRIMARY KEY is a desynced
        // counter, not a version conflict, and no retry can resolve it — see
        // isGlobalOffsetCollision. Any other failure is rethrown for the outer catch's plain wrap.
        RuntimeException mapped =
            EventAppendSupport.appendConflict(
                e, PostgresAppendFailures.INSTANCE, shownStream, expectedVersion);
        if (mapped != null) {
          throw mapped;
        }
        throw e;
      } finally {
        // The SINGLE rollback point for every non-committed exit — SQLException,
        // EventStoreException, and any OTHER Throwable (an Error such as OutOfMemoryError, or an
        // unwrapped RuntimeException like GlobalOffset.of's IAE from a corrupted counter) escaping
        // between insertEvents and conn.commit(). It must run BEFORE restoring autoCommit:
        // pgjdbc's setAutoCommit(true) COMMITS an open transaction, so without this rollback a
        // partial write would become durable (event + inbox rows WITHOUT their outbox and audit
        // rows) while the caller sees a failure — silent outbox loss + an audit gap.
        // A FAILED rollback must not be followed by that restore either, and the pool must not
        // be left to commit it on return (Agroal resets autoCommit with no rollback first): the
        // transaction may still be open, so the physical connection is aborted instead and
        // autoCommit is left alone (PostgresTransactions). An Error from the rollback propagates
        // after the abort.
        if (committed || PostgresTransactions.rollbackOrAbort(conn, "append")) {
          restoreAutoCommitQuietly(conn);
        }
      }
    } catch (SQLException e) {
      // A SQLException reaching here AFTER the commit is post-commit connection cleanup failing
      // (e.g. conn.close() throwing because the connection died right after commit). The events are
      // durable — return the already-built result rather than reporting a spurious failure.
      // Static analysis reports this guard as always false; it is reachable, because closing a
      // pooled connection can throw after the commit.
      if (committed && appendResult != null) {
        return appendResult;
      }
      throw new EventStoreException("Failed to append events to stream: " + shownStream, e);
    }
  }

  /**
   * Restores autoCommit before the connection returns to the pool, after a commit or a successful
   * rollback. A failure is swallowed: after the commit the events are durable (a throw must not
   * fail a durable append; see the committed flag in {@link #append}), and after the rollback
   * re-throwing would only mask the real in-flight exception. Mirrors notifyAfterCommit's
   * swallow-after-commit intent.
   */
  private static void restoreAutoCommitQuietly(Connection conn) {
    try {
      conn.setAutoCommit(true);
    } catch (SQLException restoreEx) {
      log.debug(
          "Failed to restore autoCommit before returning connection to pool (benign — pool"
              + " resets on return): {}",
          restoreEx.getMessage());
    }
  }

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
   * The {@link DataSource} this store appends through: the fence reads the {@code saga_state} rows
   * of a {@code PostgresSagaStore} built over the same object, whose {@code storageIdentity()} is
   * that {@code DataSource}.
   */
  @Override
  public Object sagaFenceStorageIdentity() {
    return dataSource;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Here the fence is the saga's shared fence lock and one primary-key read of its {@code
   * saga_state} row, on the append's connection, right after the inbox claim and before the offset
   * reservation (see the class javadoc). A claim the inbox loses is answered from the winner's row
   * without reading the fence: the winner committed before. A refusal rolls back the inbox claim
   * with the rest of the transaction.
   *
   * <p>Implementation note: a claim conflict (another execution already holds this idempotency key)
   * releases this append's pooled connection BEFORE the lookback read of the winning inbox row, so
   * concurrent claim-losers never hold one connection while demanding a second — the pool cannot
   * self-starve under a duplicate-key storm.
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
    List<OutboxEntry> outboxEntries =
        EventAppendSupport.outboxEntriesOf(events, outboxStore != null, outboxEventMapper);
    return support.tracedAppend(
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
   * The keyed append.
   *
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
          "This PostgresEventStore was built without a command inbox");
    }
    EventAppendSupport.requireKeyedArguments(
        streamId, events, expectedVersion, idempotencyKey, commandType);
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());
    List<OutboxEntry> outboxEntries =
        mappedOutboxEntries != null
            ? mappedOutboxEntries
            : EventAppendSupport.outboxEntriesOf(events, outboxStore != null, outboxEventMapper);
    // Serialize (Jackson, including any @Encrypted field KMS/Vault calls) BEFORE acquiring a
    // pooled connection — see serializeRows — so a slow crypto round trip never holds a connection
    // the pool could otherwise hand to another caller.
    List<EventRow> rows;
    try {
      rows =
          support.serialize(
              events.size(), traced, () -> serializeRows(streamId, events, expectedVersion));
    } catch (JsonProcessingException e) {
      throw new EventStoreException("Failed to serialize event for stream: " + shownStream, e);
    }

    // Set when tryClaim loses the race (b): the append connection is released — closed by the
    // try-with-resources below — BEFORE the inbox lookback read runs, so a claim-loser never
    // holds one pool connection while demanding a second one from the same pool. Without this,
    // a retry storm of losers can each hold a connection while blocked waiting for another,
    // starving the pool until every one of them times out — a spurious failure that the caller
    // would otherwise mistake for a primary-command failure and wrongly compensate.
    boolean claimLost = false;
    IdempotentAppendResult result = null;
    // See append(): once conn.commit() returns the events are durable, so a post-commit connection
    // cleanup failure (setAutoCommit / close) must not be reported as a failed append. Keyed
    // appends are additionally reconciled by the inbox on replay, but reporting a committed append
    // as failed would still trigger spurious caller compensation/retries — so we harden it
    // identically.
    boolean committed = false;
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        // Pin READ COMMITTED for the keyed append transaction too — same reason as append(),
        // and transaction-scoped for the same reason; so is the statement bound.
        beginAppendTransaction(conn);
        long finalVersionLong = expectedVersion.value() + events.size();
        // (a) claim the key
        boolean claimed =
            commandInbox.tryClaim(
                idempotencyKey, commandType, streamId, new Version(finalVersionLong), conn);
        if (!claimed) {
          // (b) concurrent winner already committed — ON CONFLICT DO NOTHING blocks on the
          // uncommitted row, then loses once it commits, so by the time tryClaim returns false the
          // winning row is durable and visible on a fresh connection. The empty case is a
          // should-not-happen guard (e.g. the row was pruned by retention in the tiny window
          // between claim and find); throwing EventStoreException lets the caller retry safely.
          // No rollback here — committed stays false, so the finally below rolls back (its single
          // rollback point) before this connection is released for the fresh lookback read.
          claimLost = true;
        } else {
          // (c) fence a saga's forward command, then append events, backfill offsets,
          // outbox/audit, commit, notify. A refusal throws here with committed == false, so the
          // finally below rolls back the inbox claim.
          if (forwardOf != null) {
            requireSagaForward(conn, forwardOf);
          }
          List<GlobalOffset> globalOffsets = insertEvents(conn, streamId, rows, expectedVersion);
          commandInbox.backfillOffsets(idempotencyKey, globalOffsets, conn);
          if (!outboxEntries.isEmpty()) {
            outboxStore.saveAll(outboxEntries, conn);
          }
          writeAuditEntries(conn, streamId, events, expectedVersion);
          conn.commit();
          committed = true;
          result =
              new IdempotentAppendResult(
                  List.copyOf(globalOffsets), new Version(finalVersionLong), false);
          notifyAfterCommit(conn, globalOffsets);
        }
      } catch (SQLException e) {
        // No rollback here — the finally below is the single rollback point (committed == false).
        // Only a keyed append that did not commit reaches this catch. It reaches the same verdict
        // as append() — one guard, both call sites, so the classification cannot depend on whether
        // the caller passed a key.
        throw EventAppendSupport.keyedAppendFailure(
            e, PostgresAppendFailures.INSTANCE, shownStream);
      } finally {
        // The SINGLE rollback point for every non-committed exit — the claim-lost
        // branch, SQLException/EventStoreException aborts, and any OTHER Throwable (an Error, or
        // an unwrapped RuntimeException) escaping between the inbox claim and conn.commit(). It
        // must run BEFORE restoring autoCommit: pgjdbc's setAutoCommit(true) would otherwise
        // COMMIT that partial write (durable events + inbox claim WITHOUT their outbox/audit
        // rows). A failed rollback aborts the connection instead of restoring autoCommit. See
        // append()'s finally for the full rationale.
        if (committed || PostgresTransactions.rollbackOrAbort(conn, "keyed append")) {
          restoreAutoCommitQuietly(conn);
        }
      }
    } catch (SQLException e) {
      // Post-commit connection cleanup (e.g. conn.close()) failing after a durable commit: return
      // the committed result instead of reporting a spurious failure. claimLost is never set on the
      // committed path, so the claim-lookback below is unaffected.
      if (committed && result != null) {
        return result;
      }
      throw new EventStoreException("Failed idempotent append to stream: " + shownStream, e);
    }
    if (claimLost) {
      // The append connection above is already closed/returned to the pool at this point — the
      // inbox lookback opens its own connection without holding onto the append one. It fails
      // closed unless the winning row was recorded by the SAME command on the SAME stream: without
      // that the claim loser would be handed the winner's offsets and final version with
      // alreadyApplied=true, a fabricated success for a command whose events were never appended.
      // IllegalArgumentException (not EventStoreException) on purpose — the bus classifies it as
      // deterministic, so a colliding key is not retried or dead-lettered forever.
      return EventAppendSupport.replayLostClaim(
          commandInbox, idempotencyKey, commandType, streamId);
    }
    return result;
  }

  /**
   * The fence of {@link #appendWithKey(StreamId, List, Version, IdempotencyKey, String,
   * SagaOrigin)}: takes the saga's shared fence lock in the append's transaction, held to its end,
   * then reads the saga's row and throws {@link SagaForwardFencedException} unless it is in its
   * forward phase.
   */
  private static void requireSagaForward(Connection conn, SagaOrigin forwardOf)
      throws SQLException {
    SagaFenceLock.lockShared(conn, forwardOf.sagaType(), forwardOf.sagaId());
    try (PreparedStatement ps = conn.prepareStatement(FENCE_SAGA_FORWARD)) {
      ps.setString(1, forwardOf.sagaId().value());
      ps.setString(2, forwardOf.sagaType().value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next() || !rs.getBoolean(1)) {
          throw new SagaForwardFencedException(forwardOf);
        }
      }
    }
  }

  /**
   * Pins the CALLER'S OPEN TRANSACTION to READ COMMITTED with {@code SET TRANSACTION ISOLATION
   * LEVEL}, which PostgreSQL scopes to that transaction and reverts automatically at commit or
   * rollback. Must be the first statement of the transaction — {@code conn} is expected to have
   * {@code autoCommit == false} and no statement executed yet, so pgjdbc emits the implicit {@code
   * BEGIN} together with this statement.
   *
   * <p><b>Why not {@code Connection.setTransactionIsolation}:</b> pgjdbc implements that JDBC call
   * as {@code SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...}, a SESSION-scoped
   * change that outlives both the transaction and the connection's return to the pool. Every later
   * borrower of that physical connection would silently run at READ COMMITTED — non-repeatable
   * reads inside application transactions written against a REPEATABLE READ default, intermittent
   * and unattributable because only connections that once served an append are affected. The store
   * restores {@code autoCommit} meticulously and could restore isolation the same way, but that
   * requires reading the prior level back first ({@code getTransactionIsolation} is a {@code SHOW
   * TRANSACTION ISOLATION LEVEL} round trip on pgjdbc) and adds a restore that can itself fail on a
   * dying connection. Scoping the pin to the transaction removes the residue instead of cleaning it
   * up: nothing to capture, nothing to restore, nothing left behind on any exit path — commit,
   * rollback, or a connection that dies mid-append.
   *
   * <p>It is also the only form that works behind a transaction-pooling PgBouncer: a session-scoped
   * {@code SET} issued while {@code autoCommit == true} runs in its own implicit transaction, so
   * PgBouncer returns that server connection to its pool immediately and the append's real
   * transaction may land on a different backend that was never pinned — while the pin itself leaks
   * onto an unrelated client's session.
   *
   * <p>The statement bound is set the same way and in the same round trip: {@code SET LOCAL
   * statement_timeout} lasts until this transaction commits or rolls back, bounds every statement
   * in it — including the outbox, inbox and audit writes the other stores make on this connection —
   * and leaves the session's own {@code statement_timeout} untouched.
   *
   * <p>So is the idle bound: {@code SET LOCAL idle_in_transaction_session_timeout}, the same number
   * of milliseconds. A client that goes silent between two statements of the append — a process
   * paused or cut off from the database while its transaction is open — would otherwise keep what
   * the transaction holds for as long as its session lives: the command's inbox key, a saga's
   * shared fence lock, and from the offset reservation on the global offset counter, which stops
   * every append. The server ends such a session at the bound (SQLState {@code 25P03}) and releases
   * all of it. Transaction-scoped too, so it works behind a transaction-pooling PgBouncer and
   * leaves the session's own setting untouched. With the statement bound disabled neither is set.
   */
  private void beginAppendTransaction(Connection conn) throws SQLException {
    try (var st = conn.createStatement()) {
      st.execute(
          statementTimeout.enabled()
              ? PIN_READ_COMMITTED
                  + "; "
                  + statementTimeout.setLocalSql()
                  + "; "
                  + statementTimeout.idleInTransactionSql()
              : PIN_READ_COMMITTED);
    }
  }

  /**
   * Serializes {@code events} (Jackson, including any {@code @Encrypted} field KMS/Vault calls)
   * into {@link EventRow}s ready for {@link #insertEvents}. Touches no JDBC connection — callers
   * invoke this BEFORE {@code dataSource.getConnection()} so a pooled connection is never held
   * across potentially slow encryption round trips. Returns an empty list immediately if {@code
   * events} is empty.
   */
  private List<EventRow> serializeRows(
      StreamId streamId, List<EventEnvelope> events, Version expectedVersion)
      throws JsonProcessingException {
    if (events.isEmpty()) {
      return List.of();
    }
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());

    List<EventRow> rows = new ArrayList<>(events.size());
    long version = expectedVersion.value();
    for (EventEnvelope envelope : events) {
      version++;
      // The schema version is asked before the event is serialized, as this store always did.
      int schemaVersion = upcasterChain.currentVersion(envelope.eventType());
      rows.add(
          support.serializeRow(
              envelope,
              version,
              () -> schemaVersion,
              (json, what) -> requireNoNulCharacter(json, what, shownStream)));
    }
    return rows;
  }

  /**
   * Batch-inserts pre-serialized {@code rows} into {@code event_stream} with explicitly assigned,
   * dense {@code global_offset} values and returns them in row order. Returns an empty list
   * immediately if {@code rows} is empty.
   *
   * <p>Callers must serialize events via {@link #serializeRows} BEFORE opening {@code conn} — this
   * method only reserves offsets and batch-inserts, so the counter row's lock — which serializes
   * offset assignment across concurrent appenders — and the connection itself are held only across
   * fast DB work, never across Jackson/KMS serialization.
   */
  private List<GlobalOffset> insertEvents(
      java.sql.Connection conn, StreamId streamId, List<EventRow> rows, Version expectedVersion)
      throws SQLException {
    if (rows.isEmpty()) {
      // The documented zero-event no-op (see EventStore#append) carries no events to order
      // against the stream, so it is deliberately NOT subject to the strict head check below —
      // that contract is unchanged by this fix. A keyed zero-event claim's expectedVersion is
      // caller-supplied bookkeeping only; see appendWithKey's javadoc.
      return List.of();
    }

    // (1) Reserve N dense offsets (takes the counter row lock; released at tx end).
    int n = rows.size();
    long base;
    try (var res = conn.prepareStatement(RESERVE_OFFSETS)) {
      res.setInt(1, n);
      try (var rs = res.executeQuery()) {
        if (!rs.next()) {
          throw new EventStoreException("global_offset_sequence row missing");
        }
        base = rs.getLong(1) - n; // reserved offsets are base+1 .. base+n
      }
    }

    // (1.5) Strict optimistic-concurrency head check. RESERVE_OFFSETS
    // above already took the counter row's exclusive lock, which every append — even to a
    // DIFFERENT stream — serializes behind (see append()'s javadoc above); by this point no
    // concurrent append anywhere can be mid-flight, so this read of the stream's actual head is
    // race-free without a second lock, and a plain SELECT outside the transaction would not be
    // (TOCTOU). Before this check, a conflict was only detected when a computed version already
    // EXISTED (the UNIQUE (aggregate_type, aggregate_id, version) constraint below) — an
    // expectedVersion BEYOND the
    // current head passed silently and left a permanent version gap, so EventStore's contract said
    // "throws on any mismatch" and "a future version succeeds" at once. This makes the "throws on
    // any mismatch" half real. The 23505 mapping below stays as defense-in-depth (e.g. a direct SQL
    // writer bypassing this store) — lock order is unchanged (offsets are still reserved first).
    try (var head = conn.prepareStatement(STREAM_HEAD_VERSION)) {
      StreamKeys.bind(head, 1, streamId);
      try (var rs = head.executeQuery()) {
        rs.next();
        long actualHead = rs.getLong(1);
        EventAppendSupport.requireExpectedHead(streamId, expectedVersion, actualHead);
      }
    }

    // (2) Insert with explicit offsets.
    List<GlobalOffset> globalOffsets = new ArrayList<>(n);
    try (var ps = conn.prepareStatement(INSERT_EVENT)) {
      for (int i = 0; i < n; i++) {
        long offset = base + i + 1;
        EventRow row = rows.get(i);
        ps.setLong(1, offset);
        StreamKeys.bind(ps, 2, streamId);
        ps.setLong(4, row.version());
        ps.setString(5, row.eventType());
        ps.setString(6, row.payloadJson());
        ps.setString(7, row.metadataJson());
        ps.setInt(8, row.schemaVersion());
        ps.addBatch();
        globalOffsets.add(GlobalOffset.of(offset));
      }
      ps.executeBatch();
    }
    return globalOffsets;
  }

  /**
   * True iff {@code e} — or any exception reachable through its {@code getCause()} chain OR, for a
   * {@link java.sql.BatchUpdateException}, its {@code getNextException()} chain — is a PostgreSQL
   * conflict this store maps to a retryable {@link OptimisticLockException}:
   *
   * <ul>
   *   <li>{@code 23505} unique_violation — the {@code UNIQUE (aggregate_type, aggregate_id,
   *       version)} optimistic-concurrency check. A real aggregate emits 2+ events, so on a version
   *       conflict the FIRST row of the batch collides and {@code executeBatch()} throws a {@link
   *       java.sql.BatchUpdateException} whose underlying {@code 23505} PSQLException is linked via
   *       {@code getNextException()} (and, on most driver versions, {@code getCause()}) — NOT
   *       necessarily the top-level {@code getSQLState()}. Checking only the top-level SQLState (as
   *       this once did) risks misclassifying a routine multi-event conflict as a hard {@link
   *       EventStoreException}: the command bus retries only {@code OptimisticLockException}, so it
   *       would dead-letter the conflict, and an unkeyed DLQ replay would re-append duplicate
   *       domain events. Mirrors {@code PostgresOutboxStore}'s cause-chain 23505 walk.
   *   <li>{@code 40001} serialization_failure — a concurrent append aborted on the global-offset
   *       counter row under REPEATABLE READ/SERIALIZABLE (see {@link #append}). Retryable so the
   *       bus retries rather than dead-letters it. The append transaction pins READ COMMITTED, so
   *       this is defense-in-depth for any residual serialization failure.
   * </ul>
   */
  static boolean isRetryableConflict(SQLException e) {
    // Bounded walk (Causes.MAX_DEPTH) so a self-referential/cyclic cause chain terminates.
    return Causes.anyMatch(e, t -> t instanceof SQLException sqlEx && hasRetryableSqlState(sqlEx));
  }

  /**
   * True iff {@code e} — or anything in its {@code getCause()} / {@code getNextException()} chains
   * — is a {@code 23505} that positively names {@link #EVENT_STREAM_PK_CONSTRAINT}, the PRIMARY KEY
   * on the explicitly-assigned {@code global_offset} (V001).
   *
   * <p>{@code event_stream} carries two unique constraints and only one of them is an
   * optimistic-concurrency conflict. {@code UNIQUE (aggregate_type, aggregate_id, version)} is a
   * version conflict: a retry reloads the aggregate and succeeds. A {@code global_offset} PK
   * collision means {@code global_offset_sequence.next_value} has fallen at or below {@code
   * max(global_offset)} — the reservation RESERVE_OFFSETS takes is rolled back together with the
   * failed append, so every retry reserves the identical already-committed offsets and fails
   * identically. Retrying it burns the full retry budget with backoff on EVERY command in the
   * system and reports an optimistic conflict on a stream that has none; the desynced counter
   * appears in no message, metric or DLQ row.
   *
   * <p>The test is deliberately one-sided — only a POSITIVELY identified {@code event_stream_pkey}
   * is reclassified. A {@code 23505} whose constraint cannot be read keeps its retryable
   * classification, because the opposite default would dead-letter a routine version conflict and
   * an unkeyed DLQ replay of that would re-append duplicate domain events. Matching on the
   * constraint name in the message mirrors {@code
   * PostgresOutboxStore.isBenignInflightUniqueViolation}; the name inside PostgreSQL's quotes is
   * not localized even when {@code lc_messages} is.
   */
  static boolean isGlobalOffsetCollision(SQLException e) {
    // Bounded walk (Causes.MAX_DEPTH) so a self-referential/cyclic cause chain terminates.
    return Causes.anyMatch(
        e, t -> t instanceof SQLException sqlEx && namesEventStreamPrimaryKey(sqlEx));
  }

  /** Walks one {@link SQLException}'s {@code getNextException()} chain for the PK 23505. */
  private static boolean namesEventStreamPrimaryKey(SQLException e) {
    for (SQLException sqlEx = e; sqlEx != null; sqlEx = sqlEx.getNextException()) {
      if (SQLSTATE_UNIQUE_VIOLATION.equals(sqlEx.getSQLState())) {
        String message = sqlEx.getMessage();
        if (message != null && message.contains(EVENT_STREAM_PK_CONSTRAINT)) {
          return true;
        }
      }
      if (sqlEx.getNextException() == sqlEx) {
        break; // guard against a self-referential chain
      }
    }
    return false;
  }

  /**
   * A stream id rendered for an exception message: it is caller-supplied text, so it passes through
   * {@link LogSanitizer#sanitizeForLog} before it is embedded (a message is rendered by every log
   * appender, admin console and error tracker that quotes it).
   */
  private static String streamText(StreamId streamId) {
    return LogSanitizer.sanitizeForLog(streamId.value());
  }

  /**
   * Operator-facing diagnosis for a {@link #isGlobalOffsetCollision} failure: names the desynced
   * counter and the exact remediation, instead of "version conflict on stream X".
   */
  static String globalOffsetDesyncMessage(String shownStream) {
    return "global_offset collision while appending to stream '"
        + shownStream
        + "': global_offset_sequence.next_value has fallen at or below max(global_offset), so this"
        + " append reserved offsets that are already committed. This is NOT a version conflict and"
        + " retrying cannot resolve it — the reservation rolls back with the append, so every"
        + " attempt reserves the same used offsets. Re-seed the counter with: UPDATE"
        + " global_offset_sequence SET next_value = (SELECT COALESCE(max(global_offset), 0) FROM"
        + " event_stream) WHERE id = 1";
  }

  /**
   * Walks a single {@link SQLException}'s {@code getNextException()} chain (the JDBC-standard link
   * a {@link java.sql.BatchUpdateException} uses to attach the failing sub-statement's
   * PSQLException) checking each for a retryable SQLState.
   */
  private static boolean hasRetryableSqlState(SQLException e) {
    for (SQLException sqlEx = e; sqlEx != null; sqlEx = sqlEx.getNextException()) {
      String state = sqlEx.getSQLState();
      if (SQLSTATE_UNIQUE_VIOLATION.equals(state) || SQLSTATE_SERIALIZATION_FAILURE.equals(state)) {
        return true;
      }
      if (sqlEx.getNextException() == sqlEx) {
        break; // guard against a self-referential chain
      }
    }
    return false;
  }

  /**
   * Writes event audit entries in the caller's transaction. No-op if {@code eventAuditStore} is
   * null or {@code events} is empty.
   */
  private void writeAuditEntries(
      java.sql.Connection conn,
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion) {
    if (eventAuditStore == null || events.isEmpty()) {
      return;
    }
    eventAuditStore.saveAll(
        EventAppendSupport.auditEntries(streamId, events, expectedVersion), conn);
  }

  /**
   * Best-effort NOTIFY after commit. Sets autoCommit=true so the notification is actually sent.
   * Catches all exceptions — the events are already committed and subscribers fall back to polling.
   * No-op if {@code globalOffsets} is empty.
   */
  private void notifyAfterCommit(java.sql.Connection conn, List<GlobalOffset> globalOffsets) {
    if (globalOffsets.isEmpty()) {
      return;
    }
    try {
      conn.setAutoCommit(true);
      try (var notifyStmt = conn.prepareStatement(NOTIFY_NEW_EVENTS)) {
        statementTimeout.applyTo(notifyStmt);
        notifyStmt.setString(1, String.valueOf(globalOffsets.getLast().value()));
        notifyStmt.execute();
      }
    } catch (Exception _) {
      // Best-effort: the events are already committed — a failed append report here would
      // make the caller retry and hit a spurious OptimisticLockException. Catch everything,
      // not just SQLException; subscribers fall back to polling.
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>The snapshot's {@code state_type} column stores the state class's <b>simple name</b>, so the
   * state class must be registered in the {@link EventTypeRegistry} under exactly that name, and
   * simple names must be unique across all registered state classes. Both rules are validated here,
   * before anything is written.
   *
   * <p>The write is one conditional upsert: it stores the snapshot only while the stream's {@code
   * snapshot_store} row still has {@code snapshotPurgeGeneration} (or has no row, which inserts one
   * with it). {@link PostgresSnapshotStorePurger} upserts the same row and raises its generation,
   * so the purge and this save are serialized by the primary key and a save from a state loaded
   * before the purge writes nothing. A refused save is logged at INFO and returns {@code false}.
   * The three- and four-argument {@code saveSnapshot} save with generation {@code 0} and throw
   * {@link IllegalStateException} when the stream's snapshot was ever purged (see {@link
   * EventStore#saveSnapshot(StreamId, Version, AggregateState, int)}).
   */
  @Override
  public boolean saveSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    EventAppendSupport.requireSnapshotArguments(
        streamId, state, snapshotVersion, snapshotPurgeGeneration);
    Objects.requireNonNull(version, "version is required");
    if (!traced) {
      return writeSnapshot(streamId, version, state, snapshotVersion, snapshotPurgeGeneration);
    }
    return support.tracedSaveSnapshot(
        streamId,
        version,
        () -> writeSnapshot(streamId, version, state, snapshotVersion, snapshotPurgeGeneration));
  }

  private boolean writeSnapshot(
      StreamId streamId,
      Version version,
      AggregateState state,
      int snapshotVersion,
      long snapshotPurgeGeneration) {
    String shownStream = LogSanitizer.sanitizeForLog(streamId.value());
    // Validate and serialize (Jackson, including any @Encrypted field KMS/Vault calls) BEFORE
    // acquiring a pooled connection, so a slow crypto round trip never holds a connection the pool
    // could otherwise hand to another caller.
    SnapshotPayload snapshot =
        support.serializeSnapshot(
            state, shownStream, (json, what) -> requireNoNulCharacter(json, what, shownStream));

    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(UPSERT_SNAPSHOT_VERSIONED)) {
      statementTimeout.applyTo(ps);
      StreamKeys.bind(ps, 1, streamId);
      ps.setLong(3, version.value());
      ps.setString(4, snapshot.stateType());
      ps.setString(5, snapshot.stateJson());
      ps.setInt(6, snapshotVersion);
      ps.setLong(7, snapshotPurgeGeneration);
      int written =
          PostgresTransactions.commitIfManual(conn, "snapshot save", c -> ps.executeUpdate());
      if (written == 0) {
        return support.snapshotNotStored(shownStream, version, snapshotPurgeGeneration);
      }
      return true;
    } catch (SQLException e) {
      throw new EventStoreException("Failed to save snapshot for stream: " + shownStream, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Gap-freedom guarantee: {@code global_offset} is assigned from the gapless commit-ordered
   * counter ({@code global_offset_sequence}) — a committed offset N implies every offset below N is
   * already committed, so the only offset that can ever be missing is the single tail append still
   * holding the counter. This method reads without any visibility gate and, as a defense-in-depth
   * measure, returns only a contiguous run: the expected offset is seeded from the FIRST returned
   * row (a leading gap after {@code afterOffset} is paged over), and the scan stops at the first
   * INTERNAL discontinuity within the page, returning the prefix before it. Under normal operation
   * this never truncates a batch; a hole splits a page but never wedges the stream.
   */
  @Override
  public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
    EventReadPath.requireMaxCount(maxCount);
    Instant started = traced ? Instant.now() : null;
    List<RawEventRow> rawRows;
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(READ_GLOBAL)) {
      statementTimeout.applyTo(ps);
      ps.setLong(1, afterOffset.value());
      ps.setInt(2, maxCount);
      rawRows = new ArrayList<>();
      // Contiguity guard — runs here, on the raw offsets, BEFORE any decrypt, so the guard's
      // ordering guarantee is unaffected by moving deserialization out of the connection scope.
      // The gapless append serializes offset assignment on the counter row (the reservation
      // UPDATE's row lock is held until the append commits), so a committed offset N proves every
      // offset < N has SETTLED — no lower offset is ever in-flight-and-about-to-commit beneath a
      // committed higher one, and appends never create holes. event_stream can still contain
      // PERMANENT holes, though: offsets skipped by a restore or import that left the counter
      // ahead of the data, or rows deleted out of band. Those offsets will never appear, so the
      // reader must PAGE OVER them, not stall. The expectation is seeded from the FIRST returned
      // row (skipping any gap between the checkpoint and where committed data resumes) and stops
      // only at an INTERNAL discontinuity within the page; the next poll re-seeds from its own
      // first row, so a hole splits a page but never wedges the stream. This cannot strand a
      // still-in-flight lower offset, because the serialized append guarantees none exists below a
      // committed one.
      var contiguity = readPath.globalContiguity(afterOffset);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          if (!contiguity.admits(rs.getLong("global_offset"))) {
            break;
          }
          rawRows.add(EventReadPath.readRawRow(rs));
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read global stream from offset: " + afterOffset, e);
    }
    // Deserialize (and decrypt, if any field is @Encrypted) AFTER the connection is released.
    return readPath.readEvents(rawRows, "GLOBAL", started);
  }

  private static final String LAST_GLOBAL_OFFSET =
      "SELECT COALESCE(MAX(global_offset), 0) FROM event_stream";

  /**
   * {@inheritDoc}
   *
   * <p>The highest committed offset. An append reserves its offsets from the single-row {@code
   * global_offset_sequence} counter inside its own transaction and holds that row's lock until it
   * commits, and a rolled-back append returns its reservation, so offsets are committed in the
   * order they are assigned: a committed offset implies every lower one is committed, and the next
   * append reserves only after its predecessor is visible. No event at or below the returned offset
   * becomes readable later.
   */
  @Override
  public GlobalOffset lastGlobalOffset() {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(LAST_GLOBAL_OFFSET)) {
      statementTimeout.applyTo(ps);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return GlobalOffset.of(rs.getLong(1));
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read last global offset", e);
    }
  }

  private static final String READ_STREAM =
      "SELECT global_offset, aggregate_type, aggregate_id, version, event_type, payload, metadata,"
          + " schema_version FROM event_stream"
          + " WHERE aggregate_type = ? AND aggregate_id = ? AND version > ? ORDER BY version LIMIT ?";

  @Override
  public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
    Objects.requireNonNull(streamId, "streamId is required");
    EventReadPath.requireMaxCount(maxCount);
    Instant started = traced ? Instant.now() : null;
    List<RawEventRow> rawRows;
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(READ_STREAM)) {
      statementTimeout.applyTo(ps);
      StreamKeys.bind(ps, 1, streamId);
      ps.setLong(3, afterVersion.value());
      ps.setInt(4, maxCount);
      rawRows = new ArrayList<>();
      try (var rs = ps.executeQuery()) {
        while (rs.next() && rawRows.size() < maxCount) {
          rawRows.add(EventReadPath.readRawRow(rs));
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read stream: " + streamText(streamId), e);
    }
    // Deserialize (and decrypt, if any field is @Encrypted) AFTER the connection is released.
    return readPath.readEvents(rawRows, "STREAM", started);
  }

  /**
   * Rejects serialized JSON containing the Unicode NUL character. PostgreSQL {@code jsonb} cannot
   * store the NUL character in strings — without this check the INSERT fails with an opaque {@code
   * unsupported Unicode escape sequence} SQL error, naming neither the offending data nor the
   * limitation. Failing fast here gives the caller an actionable error instead.
   *
   * @param json the serialized JSON (Jackson escapes NUL as the six characters backslash-u0000)
   * @param what what the JSON represents, for the error message
   * @param shownStream the stream being written, for the error message
   * @throws EventStoreException if the JSON contains a NUL character
   */
  private static void requireNoNulCharacter(String json, String what, String shownStream) {
    if (containsNulEscape(json)) {
      throw new EventStoreException(
          "Serialized "
              + what
              + " for stream '"
              + shownStream
              + "' contains the Unicode NUL character (\\u0000), which PostgreSQL jsonb cannot"
              + " store — remove NUL characters from the data before persisting");
    }
  }

  /**
   * Returns whether the JSON text contains the escape sequence backslash-u0000 (a real NUL
   * character). The six literal characters backslash-u0000 inside a string value are serialized by
   * Jackson as {@code \\u0000} (escaped backslash) and are data, not a NUL — distinguished by
   * counting the consecutive backslashes before {@code u0000}: an odd count is a NUL escape.
   */
  private static boolean containsNulEscape(String json) {
    int idx = json.indexOf("\\u0000");
    while (idx >= 0) {
      int backslashes = 0;
      for (int i = idx; i >= 0 && json.charAt(i) == '\\'; i--) {
        backslashes++;
      }
      if (backslashes % 2 == 1) {
        return true;
      }
      idx = json.indexOf("\\u0000", idx + 1);
    }
    return false;
  }

  /** Builder for PostgresEventStore. */
  public static final class Builder {
    private DataSource dataSource;
    private EventTypeRegistry typeRegistry;
    private CryptoEngine cryptoEngine;
    private List<EventUpcaster> upcasters = List.of();
    private PostgresEventAuditStore eventAuditStore;
    private PostgresOutboxStore outboxStore;
    private OutboxEventMapper outboxEventMapper;
    private PostgresCommandInbox commandInbox;
    private List<SnapshotMigration> migrations;
    private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
    private StatementTimeout statementTimeout = StatementTimeout.of(StatementTimeout.DEFAULT);
    private StreamRuneTracing tracing = StreamRuneTracing.NOOP;

    /** Sets the JDBC data source (required). */
    public Builder dataSource(DataSource ds) {
      this.dataSource = ds;
      return this;
    }

    /** Sets the event/state type registry (required). */
    public Builder typeRegistry(EventTypeRegistry registry) {
      this.typeRegistry = registry;
      return this;
    }

    /**
     * Sets the crypto engine for field-level encryption. Optional only when no registered event or
     * state type carries {@code @Encrypted}; {@link #build()} refuses such a type without one.
     */
    public Builder cryptoEngine(CryptoEngine engine) {
      this.cryptoEngine = engine;
      return this;
    }

    /**
     * Sets event upcasters for schema evolution. Optional; the default is none.
     *
     * @param upcasters the upcasters, empty for none; not {@code null}
     * @return this builder
     */
    public Builder upcasters(List<EventUpcaster> upcasters) {
      this.upcasters = Objects.requireNonNull(upcasters, "upcasters is required");
      return this;
    }

    /**
     * Registers snapshot migrations applied on load (optional). When a stored snapshot's schema
     * version differs from the version the command bus expects, a complete migration chain among
     * these steps upgrades the snapshot in place — only events after the snapshot then replay. A
     * gapped or absent chain discards the snapshot and replays from events, exactly like a plain
     * version mismatch (never a partial migration). Validated at {@link #build()} (no duplicate
     * {@code fromVersion}; {@code toVersion > fromVersion}).
     *
     * @param migrations the migration steps, in any order (may be {@code null} or empty)
     */
    public Builder migrations(List<SnapshotMigration> migrations) {
      this.migrations = migrations;
      return this;
    }

    /** Sets the event audit store for same-transaction audit writes (optional). */
    public Builder eventAuditStore(PostgresEventAuditStore store) {
      this.eventAuditStore = store;
      return this;
    }

    /**
     * Sets the outbox store for same-transaction outbox writes (optional). Must be set together
     * with {@link #outboxEventMapper}; providing only one of the two causes {@link #build()} to
     * throw {@link IllegalStateException}. To write only the entries passed to {@link
     * PostgresEventStore#append(StreamId, List, Version, List)}, set a mapper that returns no
     * entries ({@code event -> List.of()}).
     */
    public Builder outboxStore(PostgresOutboxStore outboxStore) {
      this.outboxStore = outboxStore;
      return this;
    }

    /**
     * Sets the mapper that derives outbox entries from appended events (optional). Must be set
     * together with {@link #outboxStore}; providing only one of the two causes {@link #build()} to
     * throw {@link IllegalStateException}.
     */
    public Builder outboxEventMapper(OutboxEventMapper outboxEventMapper) {
      this.outboxEventMapper = outboxEventMapper;
      return this;
    }

    /** Sets the command inbox for idempotent appends via {@code appendWithKey} (optional). */
    public Builder commandInbox(PostgresCommandInbox commandInbox) {
      this.commandInbox = commandInbox;
      return this;
    }

    /**
     * Sets the metrics collector threaded into {@link CryptoShreddingModule} so the
     * crypto-redaction signals fire on this store's decrypt path (event replay / projection
     * rebuild). Optional; defaults to {@link StreamRuneMetrics#NOOP}.
     *
     * @param metrics the metrics; not {@code null}
     * @return this builder
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      this.metrics = Objects.requireNonNull(metrics, "metrics is required");
      return this;
    }

    /**
     * Sets the bound on every statement the store runs. Default is 30 seconds: a statement blocked
     * on a lock or running away is cancelled by the server after this long, fails with SQLState
     * {@code 57014} and surfaces as an {@link EventStoreException}, instead of holding its
     * connection indefinitely. Pass {@link Duration#ZERO} to set no bound of the store's own, so
     * the session's {@code statement_timeout} (from the application's pool or database role)
     * applies unchanged.
     *
     * <p>An append transaction carries the bound as {@code SET LOCAL statement_timeout}, which also
     * covers the outbox, inbox and audit writes made in it; every other statement carries it as its
     * JDBC query timeout. Neither outlives the operation, so the connection goes back to the
     * application's pool exactly as it was borrowed.
     *
     * <p>A positive value shorter than one millisecond is rejected: the bound is a whole number of
     * milliseconds, such a value truncates to {@code 0}, and {@code 0} is PostgreSQL's spelling of
     * "no timeout at all".
     *
     * @param timeout the per-statement bound; {@link Duration#ZERO} sets none
     * @return this builder
     * @throws IllegalArgumentException if timeout is null, negative, positive and shorter than one
     *     millisecond, or longer than 2147483647 ms (the largest {@code statement_timeout})
     */
    public Builder statementTimeout(Duration timeout) {
      this.statementTimeout = StatementTimeout.of(timeout);
      return this;
    }

    /**
     * The tracing the store starts its spans through when its {@link TracingArea#EVENT_STORE} area
     * is on: {@value TraceAttributes#EVENT_STORE_APPEND} and {@value
     * TraceAttributes#EVENT_STORE_LOAD} with their children, {@value
     * TraceAttributes#EVENT_STORE_SNAPSHOT_SAVE}, and {@value TraceAttributes#EVENT_STORE_READ} for
     * a read that returns events. Optional; defaults to {@link StreamRuneTracing#NOOP}, which
     * traces nothing.
     *
     * @param tracing the tracing; not {@code null}
     * @return this builder
     */
    public Builder tracing(StreamRuneTracing tracing) {
      this.tracing = Objects.requireNonNull(tracing, "tracing is required");
      return this;
    }

    /**
     * Builds the {@link PostgresEventStore}. Requires {@code dataSource} and {@code typeRegistry}.
     * {@code outboxStore} and {@code outboxEventMapper} must be configured together or not at all.
     * Does not touch the database.
     *
     * <p>Without a {@link #cryptoEngine(CryptoEngine) crypto engine}, refuses a registered event or
     * state type that carries {@code @Encrypted} (nested records included), because the store would
     * write those components as plaintext, and refuses a registry that cannot list its types. With
     * an engine it only warns about a property-level Jackson serializer override that would bypass
     * encryption. See {@link PostgresEventStore#create(DataSource, EventTypeRegistry, CryptoEngine,
     * List, StreamRuneMetrics)}.
     *
     * @throws IllegalStateException if exactly one of {@code outboxStore} / {@code
     *     outboxEventMapper} is set, or if no crypto engine is set and a registered event or state
     *     type carries {@code @Encrypted}
     * @throws IllegalArgumentException if {@code typeRegistry} or {@code dataSource} is missing
     * @throws UnsupportedOperationException if no crypto engine is set and the registry does not
     *     implement {@link EventTypeRegistry#registeredTypes()}
     */
    public PostgresEventStore build() {
      return build(true);
    }

    /**
     * {@link #build()} for {@link PostgresEventStoreFactory#create()}, which has already run the
     * {@code @Encrypted} check ({@link EventAppendSupport#validateCryptoConfig}) before it touched
     * the database; running it again would only repeat its warnings.
     */
    PostgresEventStore buildAfterCryptoCheck() {
      return build(false);
    }

    private PostgresEventStore build(boolean checkCrypto) {
      if ((outboxStore == null) != (outboxEventMapper == null)) {
        throw new IllegalStateException(
            "outboxStore and outboxEventMapper must be set together (or neither). To write only"
                + " the entries passed to append(..., outboxEntries), set a mapper that returns no"
                + " entries.");
      }
      if (checkCrypto) {
        EventAppendSupport.validateCryptoConfig(typeRegistry, cryptoEngine);
      }
      var mapper = StoreObjectMappers.eventsAndSagas(cryptoEngine, metrics);
      UpcasterChain chain = new UpcasterChain(upcasters);
      // Validates the chain here, at build time (fail fast on a misconfigured migration set rather
      // than on the first load).
      SnapshotMigrationChain migrationChain = SnapshotMigrationChain.of(migrations);
      return new PostgresEventStore(
          dataSource,
          mapper,
          typeRegistry,
          chain,
          eventAuditStore,
          outboxStore,
          outboxEventMapper,
          commandInbox,
          migrationChain,
          metrics,
          statementTimeout,
          tracing);
    }
  }
}

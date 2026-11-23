package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * The SQLite {@link SagaStore}: saga state as JSON text in the {@code saga_state} table of a {@link
 * SqliteDatabase}, with the semantics of {@code PostgresSagaStore}. The table belongs to the
 * event-store baseline: build the event store through {@link SqliteEventStoreFactory} on the same
 * database first, which migrates it.
 *
 * <p>Saga state implementations must be Jackson-serializable: annotate constructors with
 * {@code @JsonCreator} and parameters with {@code @JsonProperty}.
 *
 * <p><b>Create and compare-and-swap.</b> The creates insert at {@code version = 1} and do nothing
 * when the id exists; the conflict is then told apart by the stored row's type, in the same
 * transaction: another type is a {@link SagaTypeCollisionException}, the same type an {@link
 * OptimisticLockException}. Every update is a compare-and-swap on {@code version} that also refuses
 * a terminal row ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}) and a row of another
 * type; zero rows changed is an {@link OptimisticLockException}. {@code FAULTED} is not terminal: a
 * replay or an operator may move it on.
 *
 * <p><b>The episode stamp rule ({@link SagaStore#claimCompensating}).</b> Every statement that can
 * land a row at {@code COMPENSATING} — the two forward inserts and the two compare-and-swap updates
 * ({@link #claimCompensating} is the update with that status) — stamps {@code episode_version} (the
 * version the row lands at), {@code episode_claimed_at} (the write's instant) and {@code
 * episode_cause_event_id} (the cause the write is given: the event id for {@link #applyEvent} and
 * {@link #createGenesisPending}, the claim's cause, {@code NULL} for {@link #create} and {@link
 * #update}) in that same statement, unless the row already carries a stamp; no statement clears or
 * re-stamps one. The {@code saga_state_episode_stamped} CHECK constraint of the baseline refuses a
 * row that breaks the rule.
 *
 * <p><b>One write lock, so no row locks.</b> Every write runs as {@code BEGIN IMMEDIATE} … {@code
 * COMMIT} on a connection the driver keeps in autocommit (see {@code SqliteTransactions}), so it
 * holds SQLite's database-wide write lock from before its first statement until it commits. Nothing
 * else can write the database meanwhile, in this process or any connection, and the transaction
 * reads the newest committed rows. That replaces every row lock of the Postgres store: the read of
 * the existing type after a conflicting insert and the read of the prior status before {@link
 * #markFaulted}'s update see the row the statement next to them writes, with no {@code FOR UPDATE};
 * and the timeout and compensation scans need no {@code SKIP LOCKED}, which on Postgres only
 * skipped rows held at that instant (concurrent runners are made safe by the claim's
 * compare-and-swap and the episode-scoped compensation keys, not by those locks). A write that
 * reads a saga row and then writes it must keep both in one such transaction; a read in one
 * transaction and a write in another would let another writer in between.
 *
 * <p><b>The forward fence rests on the same lock.</b> The writes that can take a saga out of its
 * forward phase — {@link #update}, {@link #applyEvent} and {@link #claimCompensating} landing at
 * {@code COMPENSATING} or a terminal status, and {@link #delete} — are single statements inside
 * that transaction, so they need no fence lock of their own: an append of a saga's forward command
 * that reads the saga's row inside its own {@code BEGIN IMMEDIATE} transaction, on the same
 * database file, commits wholly before such a write or reads what it wrote. The rows are those of
 * the file {@link #storageIdentity()} names.
 *
 * <p><b>The row-write bound.</b> A write waits for the write lock at most the connection's {@code
 * busy_timeout} (5000 ms is the documented value, the bound {@code PostgresSagaStore} gives its row
 * writes) and then fails with an {@link EventStoreException} naming {@code busy_timeout}, having
 * written nothing; the caller retries, as it does after a Postgres write past its bound. Reads run
 * in autocommit and, under WAL, never wait for a writer. SQLite has no statement timeout; a write
 * holds the lock only for its own statements, which this store runs back to back. The one call out
 * of the store made while the lock is held is the injected {@link Clock}, read once per write for
 * its timestamps: it must return at once and never block, or every writer of the database waits for
 * it.
 *
 * <p><b>Durable when it returns.</b> A write returns after its {@code COMMIT}. The explicit
 * transaction also guards a single-statement write: on a connection where the application left a
 * {@code BEGIN} open a bare statement would join that transaction, but {@code BEGIN IMMEDIATE}
 * refuses it, so the write fails with an {@link EventStoreException} and the application's
 * transaction is left alone.
 *
 * <p><b>Timestamps.</b> {@code created_at}, {@code updated_at} and {@code episode_claimed_at} are
 * the application {@link Clock}'s instant in epoch microseconds, read inside the write transaction
 * once the write lock is granted; there is no database clock.
 *
 * <p><b>Failure taxonomy.</b> Every write converts the state to JSON <em>before</em> it borrows a
 * connection: with a crypto-aware mapper the conversion may call a key store, which must not wait
 * behind this store's own write lock or pool. A conversion failure is a {@link
 * SagaStateSerializationException} (deliberately not an {@link EventStoreException}, so a
 * deterministic failure is not retried forever as infrastructure); a storage failure — a lost
 * connection, the write lock not granted in time, a constraint — is an {@link EventStoreException}.
 * A connection handed out in manual-commit mode is refused with an {@link IllegalStateException},
 * before anything is written, as every SQLite store refuses it: a configuration error of the pool
 * or of an application transaction, which no retry can fix (see {@code SqliteTransactions#write}).
 *
 * <p><b>No untyped mode, no {@code null} argument.</b> Every operation rejects a {@code null} saga
 * type, saga id, state, status, applied event, state class or query bound with {@link
 * IllegalArgumentException} naming it, before it serializes or borrows a connection, and a negative
 * {@code limit} likewise (SQLite would read it as "no limit"). Saga ids in exception messages pass
 * through {@link LogSanitizer#sanitizeForLog(String)}.
 */
public final class SqliteSagaStore implements SagaStore {

  // The three creates insert at version 1 and do nothing when the id exists (0 rows: the conflict
  // is resolved by reading the stored type in the same transaction). The episode stamp of a row
  // born COMPENSATING and both timestamps are bound from Java: episode_version 1, the write's
  // instant, the cause; NULL for any other status.
  private static final String INSERT_NEW =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending, episode_version,"
          + " episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?, 1, ?, ?, 1, ?, ?, ?, ?)"
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The executor's create-first write, before any command is dispatched: genesis_applied = 0
  // until applyEvent(startPath) commits the start's forward step. Otherwise as INSERT_NEW.
  private static final String INSERT_GENESIS_PENDING =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending, episode_version,"
          + " episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?, 1, ?, ?, 0, ?, ?, ?, ?)"
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The evolve-poison create, written after the dead-letter entry: FAULTED, genesis-pending,
  // shielded.
  private static final String INSERT_FAULTED =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, created_at,"
          + " updated_at, genesis_applied, dead_letter_pending)"
          + " VALUES (?, ?, 'FAULTED', ?, 1, ?, ?, 0, 1)"
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The terminal-row guard of every compare-and-swap.
  private static final String TERMINAL_FILTER =
      " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";

  // The episode stamp rule for the compare-and-swap updates: the first, second and fourth binds are
  // the same boolean, status == COMPENSATING; the third is the write's instant, the fifth the
  // cause.
  // A row that already carries a stamp keeps all three columns. SQLite evaluates every SET
  // expression on the row as it was before the update, so "version + 1" is the version the row
  // lands at, as "saga_state.version + 1" is on Postgres.
  private static final String EPISODE_STAMP_SET =
      "episode_version = CASE WHEN ? AND episode_version IS NULL"
          + " THEN version + 1 ELSE episode_version END, "
          + "episode_claimed_at = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_claimed_at END, "
          + "episode_cause_event_id = CASE WHEN ? AND episode_version IS NULL"
          + " THEN ? ELSE episode_cause_event_id END ";

  // update and claimCompensating. saga_type is not in SET (immutable) but is in WHERE, so a
  // foreign type's row is never touched.
  private static final String UPDATE_CAS =
      "UPDATE saga_state SET status = ?, state_json = ?, version = version + 1, updated_at = ?, "
          + "pre_fault_status = NULL, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

  // The event compare-and-swap: UPDATE_CAS plus the recovery facts. genesis_applied is OR'd with
  // startPath (never cleared), last_applied_offset is a running maximum (SQLite's two-argument
  // max() is Postgres' GREATEST), last_replayed_offset is set exactly when the event is a replay.
  private static final String APPLY_EVENT =
      "UPDATE saga_state SET status = ?, state_json = ?, version = version + 1, updated_at = ?, "
          + "pre_fault_status = NULL, "
          + "genesis_applied = (genesis_applied OR ?), "
          + "last_applied_offset = max(coalesce(last_applied_offset, -1), ?), "
          + "last_replayed_offset = CASE WHEN ? THEN ? ELSE last_replayed_offset END, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

  // markFaulted, first statement: the row the compare-and-swap will change, and its status before
  // the fault, read in the fault's transaction (Postgres reads it in a CTE under FOR UPDATE).
  private static final String MARK_FAULTED_PRIOR =
      "SELECT status FROM saga_state WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

  // markFaulted, second statement: no state write; pre_fault_status records what a replay resumes
  // from (kept on a still-poison refresh, NULL on a genesis-pending forward fault).
  private static final String MARK_FAULTED =
      "UPDATE saga_state SET "
          + "pre_fault_status = CASE WHEN status = 'FAULTED' THEN pre_fault_status"
          + " WHEN genesis_applied = 0 AND status <> 'COMPENSATING' THEN NULL"
          + " ELSE status END, "
          + "status = 'FAULTED', version = version + 1, updated_at = ? "
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

  // The shield flag: no version bump, no updated_at; 0 rows (absent row) is the documented no-op.
  private static final String SET_DEAD_LETTER_PENDING =
      "UPDATE saga_state SET dead_letter_pending = ? WHERE saga_id = ? AND saga_type = ?";

  private static final String COLUMNS =
      "state_json, status, version, updated_at, episode_version, episode_claimed_at, "
          + "episode_cause_event_id, genesis_applied, pre_fault_status, dead_letter_pending, "
          + "last_applied_offset, last_replayed_offset";

  private static final String SELECT_BY_TYPE =
      "SELECT " + COLUMNS + " FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  private static final String SELECT_ROW_FACTS =
      "SELECT status, dead_letter_pending, last_applied_offset FROM saga_state"
          + " WHERE saga_id = ? AND saga_type = ?";

  private static final String SELECT_TYPE = "SELECT saga_type FROM saga_state WHERE saga_id = ?";

  private static final String DELETE_BY_TYPE =
      "DELETE FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  // The two timeout populations, each limited to the whole batch and ranked in its own deadline
  // order, interleaved forward first (see SagaStore#findTimedOut): forward timeouts by start
  // instant (served by idx_saga_state_created_timeout, whose partial predicate matches the status
  // set exactly), compensation re-picks by last write (idx_saga_type_status_updated).
  private static final String SELECT_TIMED_OUT =
      "WITH forward_timeouts AS ("
          + "SELECT saga_id, created_at FROM saga_state "
          + "WHERE saga_type = ? AND status IN ('STARTED', 'RUNNING') AND created_at < ? "
          + "ORDER BY created_at ASC LIMIT ?), "
          + "compensation_repicks AS ("
          + "SELECT saga_id, updated_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ?) "
          + "SELECT saga_id FROM ("
          + "SELECT saga_id, 0 AS population, "
          + "row_number() OVER (ORDER BY created_at ASC) AS slot FROM forward_timeouts "
          + "UNION ALL "
          + "SELECT saga_id, 1 AS population, "
          + "row_number() OVER (ORDER BY updated_at ASC) AS slot FROM compensation_repicks) "
          + "ORDER BY slot ASC, population ASC LIMIT ?";

  private static final String SELECT_BY_STATUS =
      "SELECT saga_id FROM saga_state WHERE saga_type = ? AND status = ? "
          + "ORDER BY updated_at DESC LIMIT ?";

  private static final String COUNT_BY_STATUS =
      "SELECT count(*) FROM saga_state WHERE saga_type = ? AND status = ?";

  private static final String SELECT_COMPENSATING =
      "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ?";

  private final DataSource dataSource;
  private final Path file;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  /**
   * Creates a store over {@code database} that serializes saga state with {@code objectMapper} and
   * stamps timestamps with {@link Clock#systemUTC()}.
   *
   * <p>There is deliberately no mapper-less constructor: a mapper without {@link
   * CryptoShreddingModule} stores {@code @Encrypted} saga-state fields as plaintext, outside GDPR
   * forget scope. Build it with {@link #createObjectMapper(CryptoEngine)}, or supply your own.
   *
   * @param database the database whose {@code saga_state} table the store keeps
   * @param objectMapper the mapper for saga-state (de)serialization
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteSagaStore(SqliteDatabase database, ObjectMapper objectMapper) {
    this(database, objectMapper, Clock.systemUTC());
  }

  /**
   * Creates a store over {@code database} that serializes saga state with {@code objectMapper} and
   * stamps {@code created_at}, {@code updated_at} and {@code episode_claimed_at} with {@code
   * clock}.
   *
   * @param database the database whose {@code saga_state} table the store keeps
   * @param objectMapper the mapper for saga-state (de)serialization
   * @param clock the clock that stamps the row's timestamps
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteSagaStore(SqliteDatabase database, ObjectMapper objectMapper, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.dataSource = database.dataSource();
    this.file = database.file();
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  /**
   * Builds an {@link ObjectMapper} configured as the event store configures its own: {@link
   * JavaTimeModule}, plus {@link CryptoShreddingModule} when {@code cryptoEngine} is not {@code
   * null}, so {@code @Encrypted} saga-state fields are encrypted at rest.
   *
   * @param cryptoEngine the crypto engine, or {@code null} for a crypto-blind mapper that writes
   *     {@code @Encrypted} fields as plaintext
   * @return a new {@link ObjectMapper}
   */
  public static ObjectMapper createObjectMapper(CryptoEngine cryptoEngine) {
    return createObjectMapper(cryptoEngine, StreamRuneMetrics.NOOP);
  }

  /**
   * {@link #createObjectMapper(CryptoEngine)} with a metrics collector for the crypto-redaction
   * signals that fire when an {@code @Encrypted} field decrypts to {@code [REDACTED]}.
   *
   * @param cryptoEngine the crypto engine, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector; {@link StreamRuneMetrics#NOOP} for none
   * @return a new {@link ObjectMapper}
   * @throws NullPointerException if {@code metrics} is {@code null}
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics is required");
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine, metrics));
    }
    return mapper;
  }

  /**
   * The canonical path of the database file: every store over a handle on that file keeps the same
   * rows, whatever data source or mapper it was built with.
   */
  @Override
  public Object storageIdentity() {
    return file;
  }

  /**
   * {@inheritDoc}
   *
   * @throws SagaTypeCollisionException if a saga of another type owns {@code sagaId}
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void create(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    insert(INSERT_NEW, sagaId, sagaType, state, status, deadLetterPending, null);
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending,
      EventId episodeCause) {
    insert(
        INSERT_GENESIS_PENDING, sagaId, sagaType, state, status, deadLetterPending, episodeCause);
  }

  /**
   * {@inheritDoc}
   *
   * @throws SagaTypeCollisionException if a saga of another type owns {@code sagaId}
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
    requireWrite(sagaId, sagaType, initialState, SagaStatus.FAULTED);
    String json = serialize(sagaId, initialState);
    String existingType =
        write(
            sagaId,
            "saga create",
            "Failed to create saga state: ",
            conn -> {
              long now = now();
              try (PreparedStatement ps = conn.prepareStatement(INSERT_FAULTED)) {
                ps.setString(1, sagaId.value());
                ps.setString(2, sagaType.value());
                ps.setString(3, json);
                ps.setLong(4, now);
                ps.setLong(5, now);
                return ps.executeUpdate() == 1 ? null : storedType(conn, sagaId);
              }
            });
    refuseConflict(sagaId, sagaType, existingType);
  }

  /** The two forward inserts: the row, its shield, and the episode stamp of a COMPENSATING row. */
  private void insert(
      String sql,
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending,
      EventId episodeCause) {
    requireWrite(sagaId, sagaType, state, status);
    String json = serialize(sagaId, state);
    boolean entersEpisode = status == SagaStatus.COMPENSATING; // the episode stamp rule
    String existingType =
        write(
            sagaId,
            "saga create",
            "Failed to create saga state: ",
            conn -> {
              long now = now();
              try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sagaId.value());
                ps.setString(2, sagaType.value());
                ps.setString(3, status.name());
                ps.setString(4, json);
                ps.setLong(5, now);
                ps.setLong(6, now);
                ps.setBoolean(7, deadLetterPending);
                setNullableLong(ps, 8, entersEpisode ? 1L : null);
                setNullableLong(ps, 9, entersEpisode ? now : null);
                ps.setString(10, entersEpisode ? causeValue(episodeCause) : null);
                return ps.executeUpdate() == 1 ? null : storedType(conn, sagaId);
              }
            });
    refuseConflict(sagaId, sagaType, existingType);
  }

  /**
   * The type of the row that made an insert do nothing, read in the insert's transaction: the write
   * lock keeps the row as the insert saw it.
   */
  private static String storedType(Connection conn, SagaId sagaId) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(SELECT_TYPE)) {
      ps.setString(1, sagaId.value());
      try (ResultSet rs = ps.executeQuery()) {
        // A row that made the insert do nothing exists in this transaction; "" stands for none.
        return rs.next() ? rs.getString(1) : "";
      }
    }
  }

  /**
   * Throws for an insert that found a row: {@code existingType} is {@code null} when it inserted.
   */
  private static void refuseConflict(SagaId sagaId, SagaType sagaType, String existingType) {
    if (existingType == null) {
      return;
    }
    if (!existingType.isEmpty() && !existingType.equals(sagaType.value())) {
      throw new SagaTypeCollisionException(sagaId, SagaType.of(existingType), sagaType);
    }
    throw new OptimisticLockException("Saga " + forLog(sagaId) + " already exists");
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void update(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expectedVersion) {
    casUpdate(
        sagaId, sagaType, state, status, expectedVersion, null, "Failed to update saga state: ");
  }

  /**
   * {@inheritDoc}
   *
   * <p>The same statement as {@link #update} with {@code status = COMPENSATING}: the episode stamp
   * is a property of the status the row lands at.
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void claimCompensating(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      long expectedVersion,
      EventId episodeCause) {
    casUpdate(
        sagaId,
        sagaType,
        state,
        SagaStatus.COMPENSATING,
        expectedVersion,
        episodeCause,
        "Failed to claim compensating saga: ");
  }

  private void casUpdate(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      EventId episodeCause,
      String failureMessage) {
    requireWrite(sagaId, sagaType, state, status);
    String json = serialize(sagaId, state);
    boolean entersEpisode = status == SagaStatus.COMPENSATING;
    int updated =
        write(
            sagaId,
            "saga update",
            failureMessage,
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(UPDATE_CAS)) {
                ps.setString(1, status.name());
                ps.setString(2, json);
                long now = now();
                ps.setLong(3, now);
                ps.setBoolean(4, entersEpisode);
                ps.setBoolean(5, entersEpisode);
                ps.setLong(6, now);
                ps.setBoolean(7, entersEpisode);
                ps.setString(8, causeValue(episodeCause));
                ps.setString(9, sagaId.value());
                ps.setString(10, sagaType.value());
                ps.setLong(11, expectedVersion);
                return ps.executeUpdate();
              }
            });
    if (updated == 0) {
      throw staleView(sagaId, expectedVersion);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void applyEvent(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      AppliedEvent applied) {
    requireWrite(sagaId, sagaType, state, status);
    if (applied == null) {
      throw new IllegalArgumentException("applied must not be null");
    }
    long eventOffset = applied.eventOffset().value();
    String json = serialize(sagaId, state);
    boolean entersEpisode = status == SagaStatus.COMPENSATING; // the episode stamp rule
    int updated =
        write(
            sagaId,
            "saga apply event",
            "Failed to apply event to saga state: ",
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(APPLY_EVENT)) {
                ps.setString(1, status.name());
                ps.setString(2, json);
                long now = now();
                ps.setLong(3, now);
                ps.setBoolean(4, applied.startPath());
                ps.setLong(5, eventOffset);
                ps.setBoolean(6, applied.replayRedrive());
                ps.setLong(7, eventOffset);
                ps.setBoolean(8, entersEpisode);
                ps.setBoolean(9, entersEpisode);
                ps.setLong(10, now);
                ps.setBoolean(11, entersEpisode);
                // The applied event is the cause of an episode this write opens.
                ps.setString(12, causeValue(applied.eventId()));
                ps.setString(13, sagaId.value());
                ps.setString(14, sagaType.value());
                ps.setLong(15, expectedVersion);
                return ps.executeUpdate();
              }
            });
    if (updated == 0) {
      throw staleView(sagaId, expectedVersion);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One transaction of two statements: the prior status is read, then the row is updated, both
   * under the write lock, so the answer is the status this update replaced.
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
    requireKey(sagaId, sagaType);
    Boolean transitioned =
        write(
            sagaId,
            "saga mark faulted",
            "Failed to mark saga FAULTED: ",
            conn -> {
              String prior;
              try (PreparedStatement ps = conn.prepareStatement(MARK_FAULTED_PRIOR)) {
                bindKey(ps, sagaId, sagaType, expectedVersion, 1);
                try (ResultSet rs = ps.executeQuery()) {
                  if (!rs.next()) {
                    return null; // stale version, terminal row, other type or no row
                  }
                  prior = rs.getString(1);
                }
              }
              try (PreparedStatement ps = conn.prepareStatement(MARK_FAULTED)) {
                ps.setLong(1, now());
                bindKey(ps, sagaId, sagaType, expectedVersion, 2);
                if (ps.executeUpdate() != 1) {
                  throw new SQLException("the row read in this transaction was not updated");
                }
              }
              return !SagaStatus.FAULTED.name().equals(prior);
            });
    if (transitioned == null) {
      throw staleView(sagaId, expectedVersion);
    }
    return transitioned;
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
    requireKey(sagaId, sagaType);
    write(
        sagaId,
        "saga dead-letter shield",
        "Failed to set saga dead-letter shield: ",
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(SET_DEAD_LETTER_PENDING)) {
            ps.setBoolean(1, pending);
            ps.setString(2, sagaId.value());
            ps.setString(3, sagaType.value());
            return ps.executeUpdate(); // 0 rows = absent row: the documented no-op
          }
        });
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void delete(SagaId sagaId, SagaType sagaType) {
    requireKey(sagaId, sagaType);
    write(
        sagaId,
        "saga delete",
        "Failed to delete saga state: ",
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(DELETE_BY_TYPE)) {
            ps.setString(1, sagaId.value());
            ps.setString(2, sagaType.value());
            return ps.executeUpdate();
          }
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Reads the row with one query, returns the connection, then deserializes (and, with a
   * crypto-aware mapper, decrypts) the state, so no key-store round trip holds a connection.
   */
  @Override
  public <S extends SagaState> Optional<LoadedSaga<S>> load(
      SagaId sagaId, SagaType sagaType, Class<S> stateType) {
    requireKey(sagaId, sagaType);
    if (stateType == null) {
      throw new IllegalArgumentException("stateType must not be null");
    }
    StoredRow row;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_BY_TYPE)) {
      ps.setString(1, sagaId.value());
      ps.setString(2, sagaType.value());
      try (ResultSet rs = ps.executeQuery()) {
        row = rs.next() ? StoredRow.of(rs) : null;
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to load saga state: " + forLog(sagaId), e);
    }
    if (row == null) {
      return Optional.empty();
    }
    S state;
    try {
      state = objectMapper.readValue(row.stateJson(), stateType);
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to deserialize saga state: " + forLog(sagaId), sagaId, e);
    }
    return Optional.of(
        new LoadedSaga<>(
            state,
            row.status(),
            row.version(),
            row.updatedAt(),
            row.episodeVersion(),
            row.episodeClaimedAt(),
            row.episodeCause(),
            row.genesisApplied(),
            row.preFaultStatus(),
            row.deadLetterPending(),
            row.lastAppliedOffset(),
            row.lastReplayedOffset()));
  }

  /** The columns of {@link #SELECT_BY_TYPE}, read while the connection is open. */
  private record StoredRow(
      String stateJson,
      SagaStatus status,
      long version,
      Instant updatedAt,
      Long episodeVersion,
      Instant episodeClaimedAt,
      EventId episodeCause,
      boolean genesisApplied,
      SagaStatus preFaultStatus,
      boolean deadLetterPending,
      Long lastAppliedOffset,
      Long lastReplayedOffset) {

    static StoredRow of(ResultSet rs) throws SQLException {
      Long claimedAt = nullableLong(rs, "episode_claimed_at");
      String cause = rs.getString("episode_cause_event_id");
      String preFault = rs.getString("pre_fault_status");
      return new StoredRow(
          rs.getString("state_json"),
          SagaStatus.valueOf(rs.getString("status")),
          rs.getLong("version"),
          SqliteTimestamps.fromMicros(rs.getLong("updated_at")),
          nullableLong(rs, "episode_version"),
          claimedAt == null ? null : SqliteTimestamps.fromMicros(claimedAt),
          cause == null ? null : new EventId(cause),
          rs.getInt("genesis_applied") != 0,
          preFault == null ? null : SagaStatus.valueOf(preFault),
          rs.getInt("dead_letter_pending") != 0,
          nullableLong(rs, "last_applied_offset"),
          nullableLong(rs, "last_replayed_offset"));
    }
  }

  /** {@inheritDoc} One plain read of three columns; the state is not read. */
  @Override
  public Optional<RowFacts> loadRowFacts(SagaId sagaId, SagaType sagaType) {
    requireKey(sagaId, sagaType);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_ROW_FACTS)) {
      ps.setString(1, sagaId.value());
      ps.setString(2, sagaType.value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        return Optional.of(
            new RowFacts(
                SagaStatus.valueOf(rs.getString("status")),
                rs.getInt("dead_letter_pending") != 0,
                nullableLong(rs, "last_applied_offset")));
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to load saga row facts: " + forLog(sagaId), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException also if {@code limit} is negative
   */
  @Override
  public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
    requireType(sagaType);
    if (cutoff == null) {
      throw new IllegalArgumentException("cutoff must not be null");
    }
    requireLimit(limit);
    long cutoffMicros = SqliteTimestamps.toMicros(cutoff);
    return sagaIds(
        SELECT_TIMED_OUT,
        "Failed to find timed-out sagas",
        ps -> {
          ps.setString(1, sagaType.value());
          ps.setLong(2, cutoffMicros);
          ps.setInt(3, limit);
          ps.setString(4, sagaType.value());
          ps.setLong(5, cutoffMicros);
          ps.setInt(6, limit);
          ps.setInt(7, limit);
        });
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException also if {@code limit} is negative
   */
  @Override
  public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
    requireType(sagaType);
    requireStatus(status);
    requireLimit(limit);
    return sagaIds(
        SELECT_BY_STATUS,
        "Failed to find sagas by status",
        ps -> {
          ps.setString(1, sagaType.value());
          ps.setString(2, status.name());
          ps.setInt(3, limit);
        });
  }

  @Override
  public long countByStatus(SagaType sagaType, SagaStatus status) {
    requireType(sagaType);
    requireStatus(status);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNT_BY_STATUS)) {
      ps.setString(1, sagaType.value());
      ps.setString(2, status.name());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to count sagas by status", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException also if {@code limit} is negative
   */
  @Override
  public List<CompensatingSaga> findCompensating(
      SagaType sagaType, Instant updatedBefore, int limit) {
    requireType(sagaType);
    if (updatedBefore == null) {
      throw new IllegalArgumentException("updatedBefore must not be null");
    }
    requireLimit(limit);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_COMPENSATING)) {
      ps.setString(1, sagaType.value());
      ps.setLong(2, SqliteTimestamps.toMicros(updatedBefore));
      ps.setInt(3, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<CompensatingSaga> result = new ArrayList<>();
        while (rs.next()) {
          // Never NULL on a COMPENSATING row here (the episode stamp rule and the CHECK
          // constraint); the record stays nullable so a non-conforming store is refused per saga.
          Long claimedAt = nullableLong(rs, "episode_claimed_at");
          result.add(
              new CompensatingSaga(
                  SagaId.of(rs.getString("saga_id")),
                  SqliteTimestamps.fromMicros(rs.getLong("updated_at")),
                  claimedAt == null ? null : SqliteTimestamps.fromMicros(claimedAt)));
        }
        return result;
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to find compensating sagas", e);
    }
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private List<SagaId> sagaIds(String sql, String failureMessage, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        List<SagaId> result = new ArrayList<>();
        while (rs.next()) {
          result.add(SagaId.of(rs.getString("saga_id")));
        }
        return result;
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
  }

  /** Runs {@code work} as one write transaction, through {@link SqliteTransactions#write}. */
  private <T> T write(
      SagaId sagaId, String transaction, String failureMessage, SqliteTransactions.Work<T> work) {
    return SqliteTransactions.write(
        dataSource,
        transaction,
        transaction + " of saga " + forLog(sagaId),
        failureMessage + forLog(sagaId),
        work);
  }

  /** The write's instant, read once the write lock is granted. */
  private long now() {
    return SqliteTimestamps.toMicros(clock.instant());
  }

  /**
   * Converts saga state to its stored JSON, before any connection is borrowed. A failure is a
   * state-conversion failure, never a storage one (see the class Javadoc).
   */
  private String serialize(SagaId sagaId, SagaState state) {
    try {
      return objectMapper.writeValueAsString(state);
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to serialize saga state: " + forLog(sagaId), sagaId, e);
    }
  }

  private static void bindKey(
      PreparedStatement ps, SagaId sagaId, SagaType sagaType, long version, int first)
      throws SQLException {
    ps.setString(first, sagaId.value());
    ps.setString(first + 1, sagaType.value());
    ps.setLong(first + 2, version);
  }

  private static void setNullableLong(PreparedStatement ps, int index, Long value)
      throws SQLException {
    if (value == null) {
      ps.setNull(index, Types.INTEGER);
    } else {
      ps.setLong(index, value);
    }
  }

  private static Long nullableLong(ResultSet rs, String column) throws SQLException {
    long value = rs.getLong(column);
    return rs.wasNull() ? null : value;
  }

  /** The bind value of an episode cause: the event id's text, or SQL {@code NULL} for none. */
  private static String causeValue(EventId episodeCause) {
    return episodeCause == null ? null : episodeCause.value();
  }

  private static void requireType(SagaType sagaType) {
    if (sagaType == null) {
      throw new IllegalArgumentException("sagaType must not be null");
    }
  }

  private static void requireStatus(SagaStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("status must not be null");
    }
  }

  private static void requireKey(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) {
      throw new IllegalArgumentException("sagaId must not be null");
    }
    requireType(sagaType);
  }

  private static void requireWrite(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    requireKey(sagaId, sagaType);
    if (state == null) {
      throw new IllegalArgumentException("state must not be null");
    }
    requireStatus(status);
  }

  /** SQLite reads a negative {@code LIMIT} as "no limit"; the store refuses it instead. */
  static void requireLimit(int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative, got " + limit);
    }
  }

  private static String forLog(SagaId sagaId) {
    return LogSanitizer.sanitizeForLog(sagaId.value());
  }

  private static OptimisticLockException staleView(SagaId sagaId, long expectedVersion) {
    return new OptimisticLockException(
        "Stale view of saga " + forLog(sagaId) + " (expectedVersion=" + expectedVersion + ")");
  }
}

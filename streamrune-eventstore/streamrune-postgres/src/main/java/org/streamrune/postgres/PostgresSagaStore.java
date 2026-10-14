package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
 * PostgreSQL-backed {@link SagaStore}. Persists saga state as JSONB in the {@code saga_state} table
 * (created by the event-store baseline migration, {@code V001__streamrune_baseline.sql}).
 *
 * <p>Saga state implementations must be Jackson-serializable. Annotate constructors with
 * {@code @JsonCreator} and parameters with {@code @JsonProperty}.
 *
 * <p>Write operations use an explicit create / CAS-update split:
 *
 * <ul>
 *   <li>{@link #create} performs an atomic {@code INSERT … ON CONFLICT (saga_id) DO NOTHING},
 *       starting the saga at {@code version = 1}.
 *   <li>{@link #update} performs a {@code UPDATE … WHERE saga_id=? AND version=? AND status NOT IN
 *       (terminal)}, providing compare-and-swap semantics.
 * </ul>
 *
 * <p>Terminal-state guard: once a saga reaches a terminal status ({@code COMPLETED}, {@code
 * COMPENSATED}, {@code FAILED}), {@link #update} refuses to overwrite it and throws {@link
 * OptimisticLockException}. {@code FAULTED} is intentionally write-through (not terminal-guarded)
 * so a replay or manual intervention may transition it.
 *
 * <p><b>Episode stamp rule ({@link SagaStore#claimCompensating}).</b> Every statement that can land
 * a row at {@code COMPENSATING} — the two forward inserts and the two CAS updates ({@link
 * #claimCompensating} is the CAS with that status) — stamps {@code episode_version} (the version
 * the row lands at) and {@code episode_claimed_at} ({@code NOW()}) in that same statement unless
 * the row already carries a stamp, and no statement ever clears or re-stamps one. The statement
 * that stamps the episode also writes {@code episode_cause_event_id}, the id of the event that
 * opened it: the cause given to {@link #claimCompensating} and {@link #createGenesisPending}, the
 * applied event's id for {@link #applyEvent}, {@code NULL} for {@link #create} and {@link #update}.
 * No other statement writes that column, so every resume of the episode loads the same cause. The
 * schema enforces the rule a second time: the {@code saga_state_episode_stamped} CHECK constraint
 * in {@code V001__streamrune_baseline.sql} refuses any row that is {@code COMPENSATING}, or {@code
 * FAULTED} out of {@code COMPENSATING}, without both halves of the stamp, and any row that names a
 * cause without a stamp.
 *
 * <p><b>The forward fence.</b> {@code PostgresEventStore} refuses a saga's forward command once the
 * saga's row carries an episode stamp, is terminal or is gone, reading the row after taking the
 * saga's shared fence lock ({@link SagaFenceLock}). Every write here that can bring a row to one of
 * those states — {@link #update}, {@link #applyEvent} and {@link #claimCompensating} landing at
 * {@code COMPENSATING} or a terminal status, and {@link #delete} — takes the saga's exclusive fence
 * lock in the same transaction before its statement, so each forward append of the saga commits
 * wholly before the write or reads what it wrote. The other writes cannot change the fence's
 * verdict and stay single statements: an insert creates a row the fence refused as absent, {@link
 * #markFaulted} keeps the stamp and is not terminal, and the dead-letter shield is not part of the
 * verdict.
 *
 * <p><b>The row-write bound.</b> Every {@code UPDATE} and {@code DELETE} of {@code saga_state}, and
 * the exclusive fence-lock statement before a fence-changing one, carries a JDBC query timeout of
 * five seconds: a write waits at most that long for the saga's fence lock — held by a forward
 * append in flight — and then for the row's lock. Past it the driver cancels the statement
 * (SQLState {@code 57014}, surfaced as {@link EventStoreException}, so the caller retries: the
 * timeout runner on its next poll, a delivery on its redelivery). A forward append that went silent
 * holds the fence lock for at most the event store's statement timeout (its transaction's idle
 * bound), so a claim behind it is made by a poll at most that long later. A fence-changing write's
 * own transaction carries the same five seconds as its idle bound ({@code SET LOCAL
 * idle_in_transaction_session_timeout}, in the round trip of its lock statement): a writer that
 * goes silent between its lock and its commit has its session ended by the server and releases the
 * fence lock and the row. On a pool that hands out {@code autoCommit=false} connections every
 * single-statement write — an insert included — runs in a transaction the store commits one round
 * trip after the statement, and that transaction sets the same idle bound first. {@code
 * PostgresSagaDeadLetterStore} bounds its {@code saga_state} statements and the transactions that
 * hold a saga row the same way. Behind a transaction-pooling PgBouncer the cancel request is
 * forwarded to the server connection the client is linked to. Reads and inserts carry no bound of
 * the store's own: {@code SELECT … FOR UPDATE SKIP LOCKED} never waits, and an insert that does
 * nothing on conflict waits only for an uncommitted insert of the same id.
 *
 * <p><b>Failure taxonomy.</b> Every write path converts the state to JSON <em>before</em> it
 * acquires a pooled connection, and every path reports the two failure classes distinctly:
 *
 * <ul>
 *   <li>a <b>storage</b> failure (connection lost, deadlock, constraint) → {@link
 *       EventStoreException}, which {@code SagaRunner} correctly treats as infrastructure and
 *       retries;
 *   <li>a <b>state-conversion</b> failure (Jackson mapping, or {@code CryptoShreddingModule}
 *       refusing to write plaintext for an {@code @Encrypted} component whose {@code subjectId} is
 *       null) → {@link SagaStateSerializationException}, which is deliberately NOT an {@link
 *       EventStoreException} so a deterministic failure cannot be retried forever as if it were a
 *       dropped connection.
 * </ul>
 *
 * <p>Classifying a {@link SagaStateSerializationException} as deterministic or transient is the
 * caller's job — see that type's javadoc for the taxonomy.
 *
 * <p><b>No untyped mode, no id-less row, no state-less or status-less write.</b> Every operation
 * rejects a {@code null} argument — the {@link SagaType}, the {@link SagaId}, the state, the
 * status, the applied event, the state class, a query's cutoff — with {@link
 * IllegalArgumentException} naming it, before it serializes or borrows a connection, like {@code
 * InMemorySagaStore} (pinned by {@code SagaStoreContract}). Left to the JDBC bind or the mapper a
 * {@code null} either surfaced as an {@link EventStoreException}, which the saga runtime retries as
 * an infrastructure failure forever, for what is a caller bug, or (a state) was stored as a JSON
 * {@code null}.
 *
 * <p><b>Log-safe messages.</b> Every saga id rendered into an exception message passes through
 * {@link LogSanitizer#sanitizeForLog(String)}: the subscription that retries or quarantines these
 * failures logs them, and a {@link SagaId} is usually derived from correlated event data and
 * deliberately carries no charset bound (its constructor also runs on the read path).
 */
public final class PostgresSagaStore implements SagaStore {

  // Atomic insert at version 1; ON CONFLICT DO NOTHING — 0 rows → already exists. The episode stamp
  // rule: a row born COMPENSATING is born stamped — episode_version = 1 (the version it
  // is born at), episode_claimed_at = NOW() — in this same statement; any other status leaves the
  // episode-identity columns NULL (no compensation episode exists yet). The three
  // CASE binds are the same boolean, status == COMPENSATING. The fourth bind is the cause of the
  // episode (the id of the event that opened it, or NULL), written only when the row is born
  // stamped; the cast gives the parameter a type when the bind is NULL.
  private static final String EPISODE_STAMP_VALUES =
      ", CASE WHEN ? THEN 1 END, CASE WHEN ? THEN NOW() END, CASE WHEN ? THEN CAST(? AS TEXT) END)";

  private static final String INSERT_NEW =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?::jsonb, 1, TRUE, ?"
          + EPISODE_STAMP_VALUES
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The executor's create-first write, inserted BEFORE any command is
  // dispatched. genesis_applied = FALSE until applyEvent(startPath = true) commits the forward
  // step (evolve -> handle -> dispatch loop); SagaRunner's live hold and
  // SagaDeadLetterReplayer's SAGA_ROW_PENDING deferral both read this flag via LoadedSaga.
  // The shield (dead_letter_pending) is bound, not defaulted: a saga may already own dead-letter
  // entries before its row exists (the row-less residual), and the row must be born
  // shielded then. Same on INSERT_NEW above. A row inserted at COMPENSATING (tooling; the saga
  // runtime inserts a start event's row with a forward status) is born stamped by the same
  // statement (EPISODE_STAMP_VALUES): no crash point between "row is COMPENSATING" and "episode
  // stamped".
  private static final String INSERT_GENESIS_PENDING =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at, episode_cause_event_id)"
          + " VALUES (?, ?, ?, ?::jsonb, 1, FALSE, ?"
          + EPISODE_STAMP_VALUES
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The evolve-poison create, written AFTER the dead-letter entry is published
  // so no FAULTED row ever exists without its entry. genesis_applied = FALSE (a replayed START
  // entry must re-drive, not dedup-skip); dead_letter_pending = TRUE (the shield holds live
  // correlated events for this saga until the entry is resolved).
  private static final String INSERT_FAULTED =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending) VALUES (?, ?, 'FAULTED', ?::jsonb, 1, FALSE, TRUE)"
          + " ON CONFLICT (saga_id) DO NOTHING";

  // CAS update: only touches rows where version matches AND status is not terminal.
  // saga_type is intentionally NOT in SET — it is immutable after create — but it IS in the WHERE:
  // a CAS stamped with a foreign saga_type touches 0 rows (→ OptimisticLockException)
  // so a cross-type id collision can never silently overwrite the owning type's state.
  // The terminal-row guard shared by both CAS statements (UPDATE_CAS, APPLY_EVENT) and
  // by MARK_FAULTED's snapshot CTE.
  private static final String TERMINAL_FILTER =
      " AND status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')";

  // The episode stamp rule for the CAS writes: when the write
  // lands the row at COMPENSATING (the three boolean binds are status == COMPENSATING) and the row
  // carries no stamp yet, stamp it in this same statement — episode_version with the version the
  // row lands at, episode_claimed_at with NOW(), episode_cause_event_id with the cause bind (the id
  // of the event that opened the episode, or NULL); otherwise leave all three exactly as they are.
  // The cause is therefore written once, by the stamping statement, and a later claim or applied
  // event with another id does not replace it. A stamped
  // row is never re-stamped (an episode is absorbing: COMPENSATING only leaves to a terminal status
  // or to FAULTED and back), a terminal write never clears the stamp, and MARK_FAULTED (a separate
  // statement) does not touch it, so a fault->replay cycle (+2 versions: fault, replayed step)
  // re-derives the ORIGINAL episode key and the key-age guards still see the original claim
  // instant. The right-hand saga_state.* read the OLD row, as in "version = saga_state.version +
  // 1".
  private static final String EPISODE_STAMP_SET =
      "episode_version = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN saga_state.version + 1 ELSE saga_state.episode_version END, "
          + "episode_claimed_at = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN NOW() ELSE saga_state.episode_claimed_at END, "
          + "episode_cause_event_id = CASE WHEN ? AND saga_state.episode_version IS NULL"
          + " THEN CAST(? AS TEXT) ELSE saga_state.episode_cause_event_id END ";

  // pre_fault_status = NULL on every plain CAS write -- a completed write means the
  // row no longer resumes from a fault. claimCompensating is this statement with status =
  // COMPENSATING: the stamp rides in EPISODE_STAMP_SET.
  private static final FenceChangingWrite UPDATE_CAS =
      new FenceChangingWrite(
          "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
              + "version = saga_state.version + 1, updated_at = NOW(), "
              + "pre_fault_status = NULL, "
              + EPISODE_STAMP_SET
              + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
              + TERMINAL_FILTER);

  // The event CAS -- like UPDATE_CAS but also maintains the three live/replay
  // recovery facts: genesis_applied (OR'd with startPath, never cleared), last_applied_offset
  // (a running maximum -- the live redelivery dedup), and last_replayed_offset (set exactly, not
  // maxed, only when replayRedrive -- the replay drain's feed->discard crash dedup).
  private static final FenceChangingWrite APPLY_EVENT =
      new FenceChangingWrite(
          "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
              + "version = saga_state.version + 1, updated_at = NOW(), "
              + "pre_fault_status = NULL, "
              + "genesis_applied = saga_state.genesis_applied OR ?, "
              + "last_applied_offset = GREATEST(COALESCE(saga_state.last_applied_offset, -1), ?), "
              + "last_replayed_offset = CASE WHEN ? THEN ? ELSE saga_state.last_replayed_offset"
              + " END, "
              + EPISODE_STAMP_SET
              + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
              + TERMINAL_FILTER);

  // One statement: the CTE snapshots the prior status under FOR UPDATE so the RETURNING clause
  // can report whether this write TRANSITIONED into FAULTED (prior <> 'FAULTED') or merely
  // refreshed a fault. 0 rows = stale version / terminal / foreign type -> OptimisticLock.
  private static final String MARK_FAULTED =
      "WITH prior AS ("
          + "SELECT saga_id, status AS prior_status FROM saga_state "
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER
          + " FOR UPDATE) "
          + "UPDATE saga_state s SET "
          + "pre_fault_status = CASE WHEN prior.prior_status = 'FAULTED' THEN s.pre_fault_status"
          + " WHEN NOT s.genesis_applied AND prior.prior_status <> 'COMPENSATING' THEN NULL"
          + " ELSE prior.prior_status END, "
          + "status = 'FAULTED', version = s.version + 1, updated_at = NOW() "
          + "FROM prior WHERE s.saga_id = prior.saga_id "
          + "RETURNING prior.prior_status <> 'FAULTED' AS transitioned";

  // The shield flag write -- no version bump, no updated_at change, no
  // status/version predicate (0 rows = absent row, the documented no-op). saga_type IS in the
  // WHERE so a foreign type never flips another type's flag.
  private static final String SET_DEAD_LETTER_PENDING =
      "UPDATE saga_state SET dead_letter_pending = ? WHERE saga_id = ? AND saga_type = ?";

  // The episode stamp with its cause and the five recorded recovery facts, read by load into
  // LoadedSaga.
  private static final String COLUMNS =
      "state_json, status, version, updated_at, episode_version, episode_claimed_at, "
          + "episode_cause_event_id, genesis_applied, pre_fault_status, dead_letter_pending, "
          + "last_applied_offset, last_replayed_offset";

  // Type-scoped load. A row owned by a different saga_type is NOT returned, so the
  // caller (SagaRunner) takes its create path and the loud cross-type collision surfaces there.
  private static final String SELECT_BY_TYPE =
      "SELECT " + COLUMNS + " FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  // Read the existing row's saga_type on a create conflict to distinguish a benign
  // same-type dedup (OptimisticLockException) from a cross-type collision
  // (SagaTypeCollisionException).
  private static final String SELECT_TYPE = "SELECT saga_type FROM saga_state WHERE saga_id = ?";

  // Type-scoped delete. A row owned by a different saga_type is NOT removed, so a
  // cross-type SagaId collision cannot delete another type's state. A fence-changing write: a
  // forward command of a saga whose row is gone is refused.
  private static final FenceChangingWrite DELETE_BY_TYPE =
      new FenceChangingWrite("DELETE FROM saga_state WHERE saga_id = ? AND saga_type = ?");

  // The timeout scan serves two populations, each in its own deadline order (see
  // SagaStore#findTimedOut):
  //  - forward timeouts: STARTED/RUNNING rows whose ABSOLUTE start (created_at) is before the
  //    cutoff, oldest start first. A multi-step saga that keeps receiving events (each bumping
  //    updated_at) still times out once it has been non-terminal past its deadline
  //    (SagaDecider#timeout = "time in a non-terminal state"). Served by
  //    idx_saga_state_created_timeout, whose partial predicate matches this status set exactly.
  //  - compensation re-picks: COMPENSATING rows idle (updated_at) past the cutoff, idle longest
  //    first, so a crashed or transiently failing episode is re-driven once per timeout interval.
  //    Served by idx_saga_type_status_updated.
  // The outer query interleaves the two by rank, forward first, and cuts at the batch limit. A
  // single ORDER BY over both populations is not enough: a re-drive that fails transiently writes
  // nothing, so a backlog of stuck episodes would keep the oldest last-write instants and hold
  // every slot on every poll while later sagas overran their deadline unserved. Each branch is
  // limited to the whole batch, so a population with fewer eligible rows leaves its slots to the
  // other. The status sets stay exactly the non-halted statuses (see SagaStatus#isHalted and the
  // pin test).
  // FOR UPDATE SKIP LOCKED only skips rows another statement holds at that instant: the store runs
  // in autocommit, so the locks end with this statement. Concurrent runners are made safe by the
  // claim CAS (a lost claim dispatches nothing) and by the episode-scoped compensation keys (a
  // duplicate re-drive dedups in the command inbox), not by these locks.
  private static final String SELECT_TIMED_OUT =
      "WITH forward_timeouts AS ("
          + "SELECT saga_id, created_at FROM saga_state "
          + "WHERE saga_type = ? AND status IN ('STARTED', 'RUNNING') AND created_at < ? "
          + "ORDER BY created_at ASC LIMIT ? FOR UPDATE SKIP LOCKED), "
          + "compensation_repicks AS ("
          + "SELECT saga_id, updated_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ? FOR UPDATE SKIP LOCKED) "
          + "SELECT saga_id FROM ("
          + "SELECT saga_id, 0 AS population, "
          + "row_number() OVER (ORDER BY created_at ASC) AS slot FROM forward_timeouts "
          + "UNION ALL "
          + "SELECT saga_id, 1 AS population, "
          + "row_number() OVER (ORDER BY updated_at ASC) AS slot FROM compensation_repicks) batch "
          + "ORDER BY slot ASC, population ASC LIMIT ?";

  // Served by idx_saga_type_status_updated.
  private static final String SELECT_BY_STATUS =
      "SELECT saga_id FROM saga_state WHERE saga_type = ? AND status = ? "
          + "ORDER BY updated_at DESC LIMIT ?";

  // Backlog gauge (streamrune.saga.compensating): indexed COUNT served by
  // idx_saga_type_status (saga_type, status).
  private static final String COUNT_BY_STATUS =
      "SELECT COUNT(*) FROM saga_state WHERE saga_type = ? AND status = ?";

  // COMPENSATING sagas due for a compensation re-drive by SagaCompensationRetrySweeper.
  // status = 'COMPENSATING' exactly (never STARTED/RUNNING/halted) + an updated_at age cutoff, so a
  // freshly-claimed episode is not re-driven while the event path is still compensating it. Served
  // by idx_saga_type_status_updated (saga_type, status, updated_at). FOR UPDATE SKIP LOCKED
  // mirrors SELECT_TIMED_OUT: it only skips rows another statement holds at that instant (the
  // locks end with this autocommit statement); leadership gates to one active sweeper per type.
  // episode_claimed_at (fault-cycle-immutable) is surfaced so the sweeper's give-up
  // horizon anchors on the durable episode claim — updated_at is refreshed by every CAS write
  // (markFaulted, a replayed step's applyEvent), which would reset the give-up clock across a
  // fault->replay cycle. updated_at itself remains the due cutoff (re-drive spacing).
  private static final String SELECT_COMPENSATING =
      "SELECT saga_id, updated_at, episode_claimed_at FROM saga_state "
          + "WHERE saga_type = ? AND status = 'COMPENSATING' AND updated_at < ? "
          + "ORDER BY updated_at ASC LIMIT ? "
          + "FOR UPDATE SKIP LOCKED";

  /**
   * How long one {@code saga_state} write may wait: for the saga's exclusive fence lock, then for
   * the row. Five seconds.
   */
  static final Duration ROW_WRITE_TIMEOUT = Duration.ofSeconds(5);

  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

  /**
   * The bound of every {@code UPDATE} and {@code DELETE} of {@code saga_state} and of the
   * exclusive-lock statement before a fence-changing one, as each statement's JDBC query timeout.
   */
  private final StatementTimeout rowWriteTimeout;

  /**
   * Creates a store that serializes saga state with {@code objectMapper}.
   *
   * <p>There is deliberately NO one-argument, mapper-less constructor. A mapper without {@link
   * CryptoShreddingModule} persists {@code @Encrypted} saga-state fields as PLAINTEXT — nothing is
   * ever encrypted or decrypted, and deleting a subject's key (GDPR forget) has no effect on saga
   * state stored that way. Making the mapper an explicit argument keeps that choice visible at the
   * call site: build it with {@link #createObjectMapper(CryptoEngine)} (crypto-aware when a {@link
   * CryptoEngine} is supplied, crypto-blind when it is {@code null}), or supply your own.
   *
   * @param dataSource the pool the store borrows connections from (required)
   * @param objectMapper the mapper used for saga-state (de)serialization (required)
   */
  public PostgresSagaStore(DataSource dataSource, ObjectMapper objectMapper) {
    this(dataSource, objectMapper, ROW_WRITE_TIMEOUT);
  }

  /**
   * A store whose saga-row writes wait at most {@code rowWriteTimeout}; for tests that reach the
   * bound in less than {@link #ROW_WRITE_TIMEOUT}.
   */
  PostgresSagaStore(DataSource dataSource, ObjectMapper objectMapper, Duration rowWriteTimeout) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    if (objectMapper == null) throw new IllegalArgumentException("objectMapper is required");
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
    this.rowWriteTimeout = StatementTimeout.of(rowWriteTimeout);
  }

  /**
   * The {@code DataSource} this store reaches its {@code saga_state} rows through. Two stores built
   * over one {@code DataSource} keep the same rows, whatever mapper each serializes with, and are
   * one store to the startup check that refuses two saga runners of one saga type over one store.
   * Two {@code DataSource} objects configured for the same database are two identities: the store
   * does not compare connection settings.
   */
  @Override
  public Object storageIdentity() {
    return dataSource;
  }

  /**
   * Builds an {@link ObjectMapper} configured the same way {@code PostgresEventStore} configures
   * its own mapper: {@link JavaTimeModule} always registered, plus {@link CryptoShreddingModule}
   * when {@code cryptoEngine} is non-null so {@code @Encrypted} saga-state fields are encrypted at
   * rest (and fall within GDPR forget scope).
   *
   * <p>Intended for framework auto-configs (Spring/Quarkus/Micronaut) wiring up the default {@link
   * SagaStore} bean, and for tests exercising the crypto-aware path.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} to build a crypto-blind
   *     mapper — {@link JavaTimeModule} only, so {@code @Encrypted} saga-state fields are written
   *     and read back as PLAINTEXT and are outside GDPR forget scope
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(CryptoEngine cryptoEngine) {
    return createObjectMapper(cryptoEngine, StreamRuneMetrics.NOOP);
  }

  /**
   * Same as {@link #createObjectMapper(CryptoEngine)} but threads a {@link StreamRuneMetrics}
   * collector into {@link CryptoShreddingModule} so the crypto-redaction signals ({@code
   * streamrune.crypto.subject_redacted} / systemic-failure) fire when {@code @Encrypted} saga-state
   * fields decrypt to {@code [REDACTED]} because a key is gone. Without this the saga-store decrypt
   * path builds the mapper with NOOP metrics and the alerting is silent.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector (null → {@link StreamRuneMetrics#NOOP})
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    if (cryptoEngine != null) {
      mapper.registerModule(
          new CryptoShreddingModule(
              cryptoEngine, metrics == null ? StreamRuneMetrics.NOOP : metrics));
    }
    return mapper;
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException if a saga of the SAME type with {@code sagaId} already exists
   *     (benign concurrent-create dedup)
   * @throws SagaTypeCollisionException if a saga of a DIFFERENT type already owns {@code sagaId} —
   *     a loud, actionable failure the {@code SagaRunner} cannot mis-swallow as dedup
   */
  @Override
  public void create(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending) {
    doCreate(sagaId, sagaType, state, status, deadLetterPending, null, INSERT_NEW);
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException if a saga of the SAME type with {@code sagaId} already exists
   * @throws SagaTypeCollisionException if a saga of a DIFFERENT type already owns {@code sagaId}
   */
  @Override
  public void createGenesisPending(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending,
      EventId episodeCause) {
    doCreate(
        sagaId, sagaType, state, status, deadLetterPending, episodeCause, INSERT_GENESIS_PENDING);
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException if a saga of the SAME type with {@code sagaId} already exists
   * @throws SagaTypeCollisionException if a saga of a DIFFERENT type already owns {@code sagaId} --
   *     routed through doCreate's existing conflict branch, exactly like create()
   */
  @Override
  public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
    doCreate(sagaId, sagaType, initialState, SagaStatus.FAULTED, true, null, INSERT_FAULTED);
  }

  /**
   * Converts saga state to its stored JSON form, OUTSIDE any JDBC connection scope.
   *
   * <p>CALLERS MUST INVOKE THIS BEFORE {@code dataSource.getConnection()}. With a crypto-aware
   * mapper (see {@link #createObjectMapper(CryptoEngine)}) {@code writeValueAsString} runs {@code
   * CryptoShreddingModule}'s encrypting writer, which calls {@code CryptoEngine.encrypt}
   * synchronously — a KMS/Vault HTTP round trip, or, with {@code PostgresCryptoEngine}, {@code
   * dataSource.getConnection()} on THIS SAME POOL (all three auto-configs wire the engine onto the
   * application {@code DataSource}). Serializing inside the connection scope self-deadlocks the
   * pool: at pool-size concurrency every connection is held by a thread now queueing for a second
   * one, and nothing drains until {@code connectionTimeout} — during which the pool is empty for
   * event append, projection commit and the command bus too. Mirrors {@code
   * PostgresEventStore.serializeRows} / {@code saveSnapshot}. Gated by {@code
   * PostgresSagaStoreCryptoConnectionIT}.
   *
   * <p>The failure is classified as a STATE-CONVERSION failure, never as an {@link
   * EventStoreException}. The blanket {@code catch (Exception)} that used to span serialization AND
   * the JDBC call made a deterministic mapper failure — most naturally a {@code
   * CryptoOperationException} because an {@code @Encrypted} component's {@code subjectId} is still
   * null on a partially-populated mid-flow state — indistinguishable from a dropped connection.
   * {@code SagaRunner} treats {@code SagaStore} failures as infrastructure and lets them propagate
   * so the subscription retries the batch; for a deterministic failure that means retrying forever
   * with capped backoff while no event is quarantined, no saga is FAULTED, no dead-letter row is
   * written and the subscription still reports RUNNING. See {@link SagaStateSerializationException}
   * for the deterministic-vs-transient taxonomy the caller must apply to the cause chain.
   */
  private String serialize(SagaId sagaId, SagaState state) {
    try {
      return objectMapper.writeValueAsString(state);
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to serialize saga state: " + forLog(sagaId), sagaId, e);
    }
  }

  private void doCreate(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      boolean deadLetterPending,
      EventId episodeCause,
      String sql) {
    // createFaulted binds no status (a SQL literal), but still takes one from doCreate: FAULTED.
    requireWrite(sagaId, sagaType, state, status);
    // DO NOT MOVE THIS BELOW getConnection(); see serialize()'s javadoc for
    // why (with PostgresCryptoEngine the encryption inside it asks THIS SAME POOL for a second
    // connection). doCreate was the one write path that had it the wrong way round.
    String json = serialize(sagaId, state);
    boolean statusIsLiteral = INSERT_FAULTED.equals(sql);
    try (Connection conn = dataSource.getConnection()) {
      int inserted;
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, sagaId.value());
        ps.setString(2, sagaType.value());
        if (statusIsLiteral) {
          // Status (and the other recovery-state columns) are SQL literals in
          // INSERT_FAULTED ('FAULTED', FALSE, TRUE).
          ps.setString(3, json);
        } else {
          // INSERT_NEW and INSERT_GENESIS_PENDING bind identically; each has its
          // genesis_applied recovery-state column as a SQL literal and binds the shield.
          ps.setString(3, status.name());
          ps.setString(4, json);
          ps.setBoolean(5, deadLetterPending);
          boolean entersEpisode = status == SagaStatus.COMPENSATING; // the episode stamp rule
          ps.setBoolean(6, entersEpisode);
          ps.setBoolean(7, entersEpisode);
          ps.setBoolean(8, entersEpisode);
          ps.setString(9, causeValue(episodeCause)); // written only when the row is born stamped
        }
        inserted =
            PostgresTransactions.commitIfManual(
                conn,
                "saga create",
                c -> {
                  boundIdleIfManual(c);
                  return ps.executeUpdate();
                });
      }
      if (inserted == 0) {
        // ON CONFLICT DO NOTHING → a row already exists. Read its type: a DIFFERENT type is a
        // cross-type id collision (loud); the SAME type is the benign concurrent-create dedup.
        String existingType = readSagaType(conn, sagaId);
        if (existingType != null && !existingType.equals(sagaType.value())) {
          throw new SagaTypeCollisionException(sagaId, SagaType.of(existingType), sagaType);
        }
        throw new OptimisticLockException("Saga " + forLog(sagaId) + " already exists");
      }
    } catch (SagaTypeCollisionException | OptimisticLockException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException("Failed to create saga state: " + forLog(sagaId), e);
    }
  }

  private static String readSagaType(Connection conn, SagaId sagaId) throws java.sql.SQLException {
    try (PreparedStatement sel = conn.prepareStatement(SELECT_TYPE)) {
      sel.setString(1, sagaId.value());
      try (ResultSet rs = sel.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException on version mismatch, terminal row, or missing row
   */
  @Override
  public void update(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status, long expectedVersion) {
    casUpdate(
        sagaId, sagaType, state, status, expectedVersion, null, "Failed to update saga state: ");
  }

  /**
   * The shared CAS write ({@link #UPDATE_CAS}) behind {@link #update} and {@link
   * #claimCompensating}: the episode stamp rule is decided by {@code status} inside the statement.
   * {@code episodeCause} is written only when this statement stamps the episode; {@link #update}
   * passes {@code null}.
   */
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
    int updated;
    try {
      updated =
          fenceChangingWrite(
              UPDATE_CAS,
              sagaId,
              sagaType,
              leavesForwardPhase(status),
              "saga update",
              ps -> {
                ps.setString(1, status.name());
                ps.setString(2, json);
                ps.setBoolean(3, entersEpisode);
                ps.setBoolean(4, entersEpisode);
                ps.setBoolean(5, entersEpisode);
                ps.setString(6, causeValue(episodeCause));
                ps.setString(7, sagaId.value());
                ps.setString(8, sagaType.value());
                ps.setLong(9, expectedVersion);
                return ps.executeUpdate();
              });
    } catch (Exception e) {
      throw new EventStoreException(failureMessage + forLog(sagaId), e);
    }
    if (updated == 0) {
      throw staleView(sagaId, expectedVersion);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException on a stale {@code expectedVersion} or a terminal row
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
    if (applied == null) throw new IllegalArgumentException("applied must not be null");
    long eventOffset = applied.eventOffset().value();
    String json = serialize(sagaId, state);
    boolean entersEpisode = status == SagaStatus.COMPENSATING; // the episode stamp rule
    int updated;
    try {
      updated =
          fenceChangingWrite(
              APPLY_EVENT,
              sagaId,
              sagaType,
              leavesForwardPhase(status),
              "saga apply event",
              ps -> {
                ps.setString(1, status.name());
                ps.setString(2, json);
                ps.setBoolean(3, applied.startPath());
                ps.setLong(4, eventOffset);
                ps.setBoolean(5, applied.replayRedrive());
                ps.setLong(6, eventOffset);
                ps.setBoolean(7, entersEpisode);
                ps.setBoolean(8, entersEpisode);
                ps.setBoolean(9, entersEpisode);
                // The applied event is the cause of an episode this write opens.
                ps.setString(10, causeValue(applied.eventId()));
                ps.setString(11, sagaId.value());
                ps.setString(12, sagaType.value());
                ps.setLong(13, expectedVersion);
                return ps.executeUpdate();
              });
    } catch (Exception e) {
      throw new EventStoreException("Failed to apply event to saga state: " + forLog(sagaId), e);
    }
    if (updated == 0) {
      throw staleView(sagaId, expectedVersion);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws OptimisticLockException on a stale {@code expectedVersion} or a terminal row
   */
  @Override
  public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
    requireKey(sagaId, sagaType);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(MARK_FAULTED)) {
      ps.setString(1, sagaId.value());
      ps.setString(2, sagaType.value());
      ps.setLong(3, expectedVersion);
      rowWriteTimeout.applyTo(ps);
      return PostgresTransactions.commitIfManual(
          conn,
          "saga mark faulted",
          c -> {
            boundIdleIfManual(c);
            try (ResultSet rs = ps.executeQuery()) {
              if (!rs.next()) {
                throw staleView(sagaId, expectedVersion);
              }
              return rs.getBoolean("transitioned");
            }
          });
    } catch (OptimisticLockException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException("Failed to mark saga FAULTED: " + forLog(sagaId), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>No version/status predicate, no {@code updated_at} touch; 0 rows (absent row) is the
   * documented no-op.
   */
  @Override
  public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
    requireKey(sagaId, sagaType);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SET_DEAD_LETTER_PENDING)) {
      ps.setBoolean(1, pending);
      ps.setString(2, sagaId.value());
      ps.setString(3, sagaType.value());
      rowWriteTimeout.applyTo(ps);
      // 0 rows = absent row: the documented no-op
      PostgresTransactions.commitIfManual(
          conn,
          "saga dead-letter shield",
          c -> {
            boundIdleIfManual(c);
            return ps.executeUpdate();
          });
    } catch (Exception e) {
      throw new EventStoreException("Failed to set saga dead-letter shield: " + forLog(sagaId), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>The CAS write with {@code status = COMPENSATING} — {@link #UPDATE_CAS}'s {@link
   * #EPISODE_STAMP_SET} stamps {@code episode_version = expectedVersion + 1} (the version the row
   * lands at), {@code episode_claimed_at = NOW()} (the claim instant, anchoring the key-age guards)
   * and {@code episode_cause_event_id = episodeCause} in the same atomic UPDATE, unless the row
   * already carries a stamp, in which case all three stay as they are. Deliberately the SAME
   * statement as {@link #update}: the episode stamp rule is a property of the landed status, not of
   * this method, so an evolved {@code COMPENSATING} is stamped identically.
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

  /**
   * A {@code saga_state} write that can take a saga out of its forward phase: stamp a compensation
   * episode, land a terminal status, or delete the row. It runs only through {@link
   * #fenceChangingWrite}, which takes the saga's exclusive fence lock first; {@code
   * SagaStateWriteRatchetTest} fails the build for a {@code saga_state} write that can do any of
   * the three and is declared otherwise.
   */
  private record FenceChangingWrite(String sql) {}

  /** Binds and executes the prepared statement of a {@link FenceChangingWrite}. */
  @FunctionalInterface
  private interface StatementWork<T> {
    T run(PreparedStatement ps) throws SQLException;
  }

  /**
   * Whether a write landing the row at {@code status} can take the saga out of its forward phase:
   * {@code COMPENSATING} stamps the episode (unless the row already carries a stamp), a terminal
   * status ends the saga. Every other status leaves the fence's verdict as it is.
   */
  private static boolean leavesForwardPhase(SagaStatus status) {
    return status == SagaStatus.COMPENSATING || status.isTerminal();
  }

  /**
   * The one path of every {@link FenceChangingWrite}. When {@code leavesForwardPhase} is true the
   * statement runs in a transaction that first takes the saga's exclusive fence lock ({@link
   * SagaFenceLock#lockExclusive}): it waits for every forward append of the saga in flight (each
   * holds the shared lock from before its read of the row until its commit), and every forward
   * append that arrives later waits for this transaction and then reads the row it wrote. Each
   * forward append of the saga therefore commits wholly before this write or reads its result. When
   * it is false the statement cannot change the fence's verdict and runs alone, as every other
   * write of this store does.
   *
   * <p>The transaction is pinned to READ COMMITTED by its first statement, for the reason {@code
   * PostgresEventStore} pins its append transaction: the compare-and-swap must evaluate {@code
   * version = ?} against the row as it is once the lock is granted, not against a snapshot taken
   * before the wait, which under a REPEATABLE READ default would report a stale view (or a
   * serialization failure) for a row another unfenced write touched meanwhile. The pin is
   * transaction-scoped, so the pooled connection's session keeps its own isolation.
   *
   * <p>The transaction's lock statement also sets its idle bound to the row-write bound, so a
   * writer that goes silent before its commit is ended by the server and its locks released.
   *
   * <p>The transaction holds the fence lock, then the row's lock; it takes no other lock, so it
   * cannot close a cycle with a fenced append (inbox key, fence lock, offset counter). The row's
   * lock may be held by a single-statement write of this store or by a dead-letter-store
   * transaction (its dead-letter rows, then this row, {@code FOR UPDATE} or updated); neither takes
   * a fence lock.
   */
  private <T> T fenceChangingWrite(
      FenceChangingWrite write,
      SagaId sagaId,
      SagaType sagaType,
      boolean leavesForwardPhase,
      String transaction,
      StatementWork<T> work)
      throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (!leavesForwardPhase) {
        try (PreparedStatement ps = conn.prepareStatement(write.sql())) {
          rowWriteTimeout.applyTo(ps);
          return PostgresTransactions.commitIfManual(
              conn,
              transaction,
              c -> {
                boundIdleIfManual(c);
                return work.run(ps);
              });
        }
      }
      boolean autoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);
      boolean committed = false;
      try {
        SagaFenceLock.lockExclusive(conn, sagaType, sagaId, rowWriteTimeout);
        T result;
        try (PreparedStatement ps = conn.prepareStatement(write.sql())) {
          rowWriteTimeout.applyTo(ps);
          result = work.run(ps);
        }
        conn.commit();
        committed = true;
        return result;
      } finally {
        // The single rollback point, BEFORE autoCommit is restored: pgjdbc's setAutoCommit(true)
        // commits an open transaction. A failed rollback aborts the physical connection.
        if (committed) {
          conn.setAutoCommit(autoCommit);
        } else if (PostgresTransactions.rollbackOrAbort(conn, transaction)) {
          restoreAutoCommitQuietly(conn, autoCommit);
        }
      }
    }
  }

  /**
   * The idle bound of a single-statement write on a connection the pool hands out with {@code
   * autoCommit=false}: there the driver opens a transaction implicitly on the statement and {@link
   * PostgresTransactions#commitIfManual} commits it one round trip later, so the row (or a new
   * row's id) is held from the statement to the commit. The transaction's first statement sets
   * {@code SET LOCAL idle_in_transaction_session_timeout} to the row-write bound, so a writer that
   * goes silent in that gap has its session ended by the server and releases the row. Nothing is
   * sent on an {@code autoCommit=true} connection, where the statement commits itself, or when the
   * bound is disabled. Run inside the work of {@code commitIfManual}, so a failure of it is rolled
   * back like a failure of the write.
   */
  private void boundIdleIfManual(Connection conn) throws SQLException {
    if (conn.getAutoCommit() || !rowWriteTimeout.enabled()) {
      return;
    }
    try (Statement st = conn.createStatement()) {
      st.execute(rowWriteTimeout.idleInTransactionSql());
    }
  }

  /** Restores autoCommit after a rollback, without replacing the exception in flight. */
  private static void restoreAutoCommitQuietly(Connection conn, boolean autoCommit) {
    try {
      conn.setAutoCommit(autoCommit);
    } catch (SQLException | RuntimeException restoreFailure) {
      // the pool resets autoCommit on return; the exception in flight is what matters
    }
  }

  /** The bind value of an episode cause: the event id's text, or SQL {@code NULL} for none. */
  private static String causeValue(EventId episodeCause) {
    return episodeCause == null ? null : episodeCause.value();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Filters by {@code saga_type} ({@code WHERE saga_id = ? AND saga_type = ?}), so a row owned
   * by a different saga type is never deserialized or returned — the caller sees "no saga for this
   * type" and its create path surfaces the collision loudly. A {@code null sagaType} is rejected:
   * there is no untyped load.
   *
   * <p>Reads the raw {@code state_json} (+ {@code status}/{@code version}) columns while the pooled
   * connection is open, then closes the connection BEFORE deserializing — <em>if</em> this store
   * was built with a mapper carrying {@link CryptoShreddingModule} (see {@link
   * #createObjectMapper(CryptoEngine)}), {@code objectMapper.readValue} may decrypt
   * {@code @Encrypted} fields via a KMS/Vault round trip, and that must never happen while a pooled
   * connection is held (mirrors {@link PostgresEventStore#load}). With a crypto-blind mapper (one
   * built by {@link #createObjectMapper(CryptoEngine)} with a {@code null} engine, or any mapper
   * without {@link CryptoShreddingModule}), no such module is registered and {@code @Encrypted}
   * fields are read back as plain, still-plaintext strings.
   */
  @Override
  public <S extends SagaState> Optional<LoadedSaga<S>> load(
      SagaId sagaId, SagaType sagaType, Class<S> stateType) {
    requireKey(sagaId, sagaType);
    if (stateType == null) throw new IllegalArgumentException("stateType must not be null");
    String stateJson = null;
    SagaStatus status = null;
    long version = 0;
    Instant updatedAt = null;
    Long episodeVersion = null;
    Instant episodeClaimedAt = null;
    EventId episodeCauseEventId = null;
    boolean genesisApplied = false;
    SagaStatus preFaultStatus = null;
    boolean deadLetterPending = false;
    Long lastAppliedOffset = null;
    Long lastReplayedOffset = null;
    boolean found;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_BY_TYPE)) {
      ps.setString(1, sagaId.value());
      ps.setString(2, sagaType.value());
      try (ResultSet rs = ps.executeQuery()) {
        found = rs.next();
        if (found) {
          stateJson = rs.getString("state_json");
          status = SagaStatus.valueOf(rs.getString("status"));
          version = rs.getLong("version");
          updatedAt = rs.getTimestamp("updated_at").toInstant();
          // Both NULL only for a row that never entered a compensation
          // episode — every write that lands COMPENSATING stamps both (the episode stamp rule, and
          // the saga_state_episode_stamped CHECK), so the runtime reads them with no fallback.
          long ev = rs.getLong("episode_version");
          episodeVersion = rs.wasNull() ? null : ev;
          var claimedTs = rs.getTimestamp("episode_claimed_at");
          episodeClaimedAt = claimedTs == null ? null : claimedTs.toInstant();
          // NULL for a row that never entered an episode and for an episode opened with no event
          // in hand (a timeout claim, an operator's compensation).
          String cause = rs.getString("episode_cause_event_id");
          episodeCauseEventId = cause == null ? null : new EventId(cause);
          genesisApplied = rs.getBoolean("genesis_applied");
          String preFaultStatusStr = rs.getString("pre_fault_status");
          preFaultStatus = preFaultStatusStr == null ? null : SagaStatus.valueOf(preFaultStatusStr);
          deadLetterPending = rs.getBoolean("dead_letter_pending");
          long lastApplied = rs.getLong("last_applied_offset");
          lastAppliedOffset = rs.wasNull() ? null : lastApplied;
          long lastReplayed = rs.getLong("last_replayed_offset");
          lastReplayedOffset = rs.wasNull() ? null : lastReplayed;
        }
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to load saga state: " + forLog(sagaId), e);
    }
    if (!found) {
      return Optional.empty();
    }
    // Deserialize (and decrypt, if any field is @Encrypted) AFTER the connection is released.
    // A mapping/crypto failure here is a STATE-CONVERSION failure, not a storage one —
    // see serialize(). The JDBC read above keeps its EventStoreException.
    try {
      S state = objectMapper.readValue(stateJson, stateType);
      return Optional.of(
          new LoadedSaga<>(
              state,
              status,
              version,
              updatedAt,
              episodeVersion,
              episodeClaimedAt,
              episodeCauseEventId,
              genesisApplied,
              preFaultStatus,
              deadLetterPending,
              lastAppliedOffset,
              lastReplayedOffset));
    } catch (Exception e) {
      throw new SagaStateSerializationException(
          "Failed to deserialize saga state: " + forLog(sagaId), sagaId, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Adds {@code AND saga_type = ?} to the DELETE, so a row owned by a different saga type is
   * never removed by a cross-type {@link SagaId} collision.
   */
  @Override
  public void delete(SagaId sagaId, SagaType sagaType) {
    requireKey(sagaId, sagaType);
    try {
      fenceChangingWrite(
          DELETE_BY_TYPE,
          sagaId,
          sagaType,
          true,
          "saga delete",
          ps -> {
            ps.setString(1, sagaId.value());
            ps.setString(2, sagaType.value());
            return ps.executeUpdate();
          });
    } catch (Exception e) {
      throw new EventStoreException("Failed to delete saga state: " + forLog(sagaId), e);
    }
  }

  @Override
  public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
    requireType(sagaType);
    if (cutoff == null) throw new IllegalArgumentException("cutoff must not be null");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_TIMED_OUT)) {
      java.sql.Timestamp cutoffTs = java.sql.Timestamp.from(cutoff);
      // forward timeouts: absolute from the start (created_at)
      ps.setString(1, sagaType.value());
      ps.setTimestamp(2, cutoffTs);
      ps.setInt(3, limit);
      // compensation re-picks: inactivity (updated_at)
      ps.setString(4, sagaType.value());
      ps.setTimestamp(5, cutoffTs);
      ps.setInt(6, limit);
      // the interleaved batch
      ps.setInt(7, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<SagaId> result = new ArrayList<>();
        while (rs.next()) {
          result.add(SagaId.of(rs.getString("saga_id")));
        }
        return result;
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to find timed-out sagas", e);
    }
  }

  @Override
  public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
    requireType(sagaType);
    requireStatus(status);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_BY_STATUS)) {
      ps.setString(1, sagaType.value());
      ps.setString(2, status.name());
      ps.setInt(3, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<SagaId> result = new ArrayList<>();
        while (rs.next()) {
          result.add(SagaId.of(rs.getString("saga_id")));
        }
        return result;
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to find sagas by status", e);
    }
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
    } catch (Exception e) {
      throw new EventStoreException("Failed to count sagas by status", e);
    }
  }

  @Override
  public List<CompensatingSaga> findCompensating(
      SagaType sagaType, Instant updatedBefore, int limit) {
    requireType(sagaType);
    if (updatedBefore == null) throw new IllegalArgumentException("updatedBefore must not be null");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(SELECT_COMPENSATING)) {
      ps.setString(1, sagaType.value());
      ps.setTimestamp(2, java.sql.Timestamp.from(updatedBefore));
      ps.setInt(3, limit);
      try (ResultSet rs = ps.executeQuery()) {
        List<CompensatingSaga> result = new ArrayList<>();
        while (rs.next()) {
          // The durable claim instant — the sweeper's give-up anchor. Never NULL on a
          // COMPENSATING row here (the episode stamp rule + the CHECK constraint); the record stays
          // nullable so a non-conforming store is refused per saga by the sweeper.
          var claimedTs = rs.getTimestamp("episode_claimed_at");
          result.add(
              new CompensatingSaga(
                  SagaId.of(rs.getString("saga_id")),
                  rs.getTimestamp("updated_at").toInstant(),
                  claimedTs == null ? null : claimedTs.toInstant()));
        }
        return result;
      }
    } catch (Exception e) {
      throw new EventStoreException("Failed to find compensating sagas", e);
    }
  }

  /**
   * The type-scoping precondition every operation runs FIRST — before {@link #serialize} (which may
   * call a KMS) and before borrowing a connection — so a rejected call has no side effect and no
   * crash point. Every argument has such a check (the follow-up, pinned by {@code
   * SagaStoreContract}): left to the JDBC bind or the mapper, a {@code null} one failed inside the
   * storage {@code catch} as an {@link EventStoreException} the saga runtime retries as an
   * infrastructure failure forever (a status, a cutoff), or was stored as a JSON {@code null} (a
   * state).
   */
  private static void requireType(SagaType sagaType) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
  }

  private static void requireStatus(SagaStatus status) {
    if (status == null) throw new IllegalArgumentException("status must not be null");
  }

  /**
   * {@link #requireType} plus the id precondition, for every operation that takes a {@link SagaId}:
   * run FIRST, for the same reason. A {@code null} id otherwise failed at the JDBC bind ({@code
   * sagaId.value()}) inside the storage {@code catch}, and surfaced as an {@link
   * EventStoreException} the saga runtime retries as infrastructure forever.
   */
  private static void requireKey(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) throw new IllegalArgumentException("sagaId must not be null");
    requireType(sagaType);
  }

  /** {@link #requireKey} plus the state and the status, for every write that persists both. */
  private static void requireWrite(
      SagaId sagaId, SagaType sagaType, SagaState state, SagaStatus status) {
    requireKey(sagaId, sagaType);
    if (state == null) throw new IllegalArgumentException("state must not be null");
    requireStatus(status);
  }

  /**
   * The log-safe rendering of a saga id for an exception message. Every caller has passed {@link
   * #requireKey}, so the id is never {@code null} here.
   */
  private static String forLog(SagaId sagaId) {
    return LogSanitizer.sanitizeForLog(sagaId.value());
  }

  private static OptimisticLockException staleView(SagaId sagaId, long expectedVersion) {
    return new OptimisticLockException(
        "Stale view of saga " + forLog(sagaId) + " (expectedVersion=" + expectedVersion + ")");
  }
}

package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
 * the row already carries a stamp, and no statement ever clears or re-stamps one. The schema
 * enforces it a second time: the {@code saga_state_episode_stamped} CHECK constraint in {@code
 * V001__streamrune_baseline.sql} refuses any row that is {@code COMPENSATING}, or {@code FAULTED}
 * out of {@code COMPENSATING}, without both halves.
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
  // episode-identity columns NULL (no compensation episode exists yet). Both
  // CASE binds are the same boolean, status == COMPENSATING.
  private static final String EPISODE_STAMP_VALUES =
      ", CASE WHEN ? THEN 1 END, CASE WHEN ? THEN NOW() END)";

  private static final String INSERT_NEW =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at)"
          + " VALUES (?, ?, ?, ?::jsonb, 1, TRUE, ?"
          + EPISODE_STAMP_VALUES
          + " ON CONFLICT (saga_id) DO NOTHING";

  // The executor's create-first write, inserted BEFORE any command is
  // dispatched. genesis_applied = FALSE until applyEvent(startPath = true) commits the forward
  // step (evolve -> handle -> dispatch loop); SagaRunner's live hold and
  // SagaDeadLetterReplayer's SAGA_ROW_PENDING deferral both read this flag via LoadedSaga.
  // The shield (dead_letter_pending) is bound, not defaulted: a saga may already own dead-letter
  // entries before its row exists (the row-less residual), and the row must be born
  // shielded then. Same on INSERT_NEW above. A genesis whose evolve yielded
  // COMPENSATING is born stamped by the same statement (EPISODE_STAMP_VALUES): no crash point
  // between "row is COMPENSATING" and "episode stamped".
  private static final String INSERT_GENESIS_PENDING =
      "INSERT INTO saga_state (saga_id, saga_type, status, state_json, version, genesis_applied,"
          + " dead_letter_pending, episode_version, episode_claimed_at)"
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
  // lands the row at COMPENSATING (both binds are status == COMPENSATING) and the row carries no
  // stamp yet, stamp both halves in this same statement — episode_version with the version the row
  // lands at, episode_claimed_at with NOW(); otherwise leave both exactly as they are. A stamped
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
          + " THEN NOW() ELSE saga_state.episode_claimed_at END ";

  // pre_fault_status = NULL on every plain CAS write -- a completed write means the
  // row no longer resumes from a fault. claimCompensating is this statement with status =
  // COMPENSATING: the stamp rides in EPISODE_STAMP_SET.
  private static final String UPDATE_CAS =
      "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
          + "version = saga_state.version + 1, updated_at = NOW(), "
          + "pre_fault_status = NULL, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

  // The event CAS -- like UPDATE_CAS but also maintains the three live/replay
  // recovery facts: genesis_applied (OR'd with startPath, never cleared), last_applied_offset
  // (a running maximum -- the live redelivery dedup), and last_replayed_offset (set exactly, not
  // maxed, only when replayRedrive -- the replay drain's feed->discard crash dedup).
  private static final String APPLY_EVENT =
      "UPDATE saga_state SET status = ?, state_json = ?::jsonb, "
          + "version = saga_state.version + 1, updated_at = NOW(), "
          + "pre_fault_status = NULL, "
          + "genesis_applied = saga_state.genesis_applied OR ?, "
          + "last_applied_offset = GREATEST(COALESCE(saga_state.last_applied_offset, -1), ?), "
          + "last_replayed_offset = CASE WHEN ? THEN ? ELSE saga_state.last_replayed_offset END, "
          + EPISODE_STAMP_SET
          + "WHERE saga_id = ? AND saga_type = ? AND version = ?"
          + TERMINAL_FILTER;

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

  // The five recorded recovery facts, read by load into LoadedSaga's 11-arg shape.
  private static final String COLUMNS =
      "state_json, status, version, updated_at, episode_version, episode_claimed_at, "
          + "genesis_applied, pre_fault_status, dead_letter_pending, "
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
  // cross-type SagaId collision cannot delete another type's state.
  private static final String DELETE_BY_TYPE =
      "DELETE FROM saga_state WHERE saga_id = ? AND saga_type = ?";

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

  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

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
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    if (objectMapper == null) throw new IllegalArgumentException("objectMapper is required");
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
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
    doCreate(sagaId, sagaType, state, status, deadLetterPending, INSERT_NEW);
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
      boolean deadLetterPending) {
    doCreate(sagaId, sagaType, state, status, deadLetterPending, INSERT_GENESIS_PENDING);
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
    doCreate(sagaId, sagaType, initialState, SagaStatus.FAULTED, true, INSERT_FAULTED);
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
        }
        inserted =
            PostgresTransactions.commitIfManual(conn, "saga create", c -> ps.executeUpdate());
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
    casUpdate(sagaId, sagaType, state, status, expectedVersion, "Failed to update saga state: ");
  }

  /**
   * The shared CAS write ({@link #UPDATE_CAS}) behind {@link #update} and {@link
   * #claimCompensating}: the episode stamp rule is decided by {@code status} inside the statement.
   */
  private void casUpdate(
      SagaId sagaId,
      SagaType sagaType,
      SagaState state,
      SagaStatus status,
      long expectedVersion,
      String failureMessage) {
    requireWrite(sagaId, sagaType, state, status);
    String json = serialize(sagaId, state);
    boolean entersEpisode = status == SagaStatus.COMPENSATING;
    int updated;
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(UPDATE_CAS)) {
      ps.setString(1, status.name());
      ps.setString(2, json);
      ps.setBoolean(3, entersEpisode);
      ps.setBoolean(4, entersEpisode);
      ps.setString(5, sagaId.value());
      ps.setString(6, sagaType.value());
      ps.setLong(7, expectedVersion);
      updated = PostgresTransactions.commitIfManual(conn, "saga update", c -> ps.executeUpdate());
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
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(APPLY_EVENT)) {
      ps.setString(1, status.name());
      ps.setString(2, json);
      ps.setBoolean(3, applied.startPath());
      ps.setLong(4, eventOffset);
      ps.setBoolean(5, applied.replayRedrive());
      ps.setLong(6, eventOffset);
      ps.setBoolean(7, entersEpisode);
      ps.setBoolean(8, entersEpisode);
      ps.setString(9, sagaId.value());
      ps.setString(10, sagaType.value());
      ps.setLong(11, expectedVersion);
      updated =
          PostgresTransactions.commitIfManual(conn, "saga apply event", c -> ps.executeUpdate());
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
      return PostgresTransactions.commitIfManual(
          conn,
          "saga mark faulted",
          c -> {
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
      // 0 rows = absent row: the documented no-op
      PostgresTransactions.commitIfManual(conn, "saga dead-letter shield", c -> ps.executeUpdate());
    } catch (Exception e) {
      throw new EventStoreException("Failed to set saga dead-letter shield: " + forLog(sagaId), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>The CAS write with {@code status = COMPENSATING} — {@link #UPDATE_CAS}'s {@link
   * #EPISODE_STAMP_SET} stamps {@code episode_version = expectedVersion + 1} (the version the row
   * lands at) and {@code episode_claimed_at = NOW()} (the claim instant, anchoring the key-age
   * guards) in the same atomic UPDATE, unless the row already carries a stamp. Deliberately the
   * SAME statement as {@link #update}: the episode stamp rule is a property of the landed status,
   * not of this method, so an evolved {@code COMPENSATING} is stamped identically.
   */
  @Override
  public void claimCompensating(
      SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
    casUpdate(
        sagaId,
        sagaType,
        state,
        SagaStatus.COMPENSATING,
        expectedVersion,
        "Failed to claim compensating saga: ");
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
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(DELETE_BY_TYPE)) {
      ps.setString(1, sagaId.value());
      ps.setString(2, sagaType.value());
      PostgresTransactions.commitIfManual(conn, "saga delete", c -> ps.executeUpdate());
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

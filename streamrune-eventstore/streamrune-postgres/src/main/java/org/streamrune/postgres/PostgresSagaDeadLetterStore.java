package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * PostgreSQL-backed {@link SagaDeadLetterStore}. Persists poison saga events in the {@code
 * saga_dead_letters} table, created by the framework's baseline Flyway migration ({@code
 * V001__streamrune_baseline.sql}); this class never creates it.
 *
 * <p>{@link #publish} is an idempotent upsert keyed by {@code (saga_id, event_offset)}. A second
 * call with the same pair updates the existing row — refreshing every column except the immutable
 * {@code first_faulted_at}, the immutable {@code first_replay_started_at} anchor and the
 * resolved-target hint {@code target_saga_id}, whose stored values always win on a conflict —
 * rather than inserting a duplicate. When {@code sagaId} is {@code null} (routing failure before a
 * saga was identified), the UNIQUE constraint uses the standard SQL treatment of NULL — multiple
 * null-saga rows at distinct offsets are independent entries; a re-published null-saga event at the
 * same offset inserts a second row — accepted, rare routing failure. Each such duplicate is BORN
 * carrying the set's immutable evidence — the oldest {@code first_faulted_at}, the oldest {@code
 * first_replay_started_at} anchor, and the newest resolved {@code target_saga_id} among
 * type-matching null-saga rows at the offset: readers surface the NEWEST row per entry, so an
 * evidence-bare fresh row would launder a prior replay attempt's anchor out of the replayer's
 * key-age guard (re-opening the pruned-forward-key duplicate execution), grant the set a fresh
 * retention life per re-quarantine, or let the retention sweep prune a TARGET_PENDING-resolved set
 * out from under an operator mid-reconciliation.
 *
 * <p>Failures are wrapped in {@link EventStoreException}.
 */
public final class PostgresSagaDeadLetterStore implements SagaDeadLetterStore {

  private static final Logger log = LoggerFactory.getLogger(PostgresSagaDeadLetterStore.class);

  // Idempotent upsert keyed by (saga_id, event_offset). When saga_id IS NULL, the UNIQUE
  // constraint treats two NULLs as distinct (standard SQL NULL semantics), so two null-saga
  // rows at the same offset produce separate entries — an accepted edge case for rare
  // routing failures.
  // first_faulted_at is set on INSERT and deliberately EXCLUDED from the DO UPDATE
  // set — a re-quarantine refreshes faulted_at (the reads order newest-first by it) but can never
  // reset the immutable first-fault instant that anchors the retention sweep.
  // target_saga_id is likewise EXCLUDED from the DO UPDATE set — the STORED resolved
  // target survives a re-quarantine, so a re-poison can never strip the retention protection an
  // earlier TARGET_PENDING resolution established. setResolvedTarget is its sole writer for
  // existing rows.
  // first_replay_started_at is likewise EXCLUDED from the DO UPDATE set — the STORED
  // first-attempt anchor survives a re-quarantine, exactly like first_faulted_at.
  // establishFirstReplayAnchor's COALESCE is its sole writer for existing rows. There is no
  // in-flight replay marker to exclude:
  // first_replay_started_at is the only replay-related fact the entry keeps.
  // This statement serves NAMED entries only; a NULL saga_id goes through NULL_SAGA_INSERT below,
  // whose carry-forward subqueries give the always-inserting null-saga shape the same
  // immutable-column semantics this DO UPDATE exclusion gives the named shape.
  private static final String COLUMNS =
      "saga_id, saga_type, event_offset, event_type, error_type, error_message, faulted_at, "
          + "first_faulted_at, first_replay_started_at, target_saga_id";

  private static final String UPSERT =
      "INSERT INTO saga_dead_letters ("
          + COLUMNS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT (saga_id, event_offset) DO UPDATE SET "
          + "saga_type = EXCLUDED.saga_type, event_type = EXCLUDED.event_type, "
          + "error_type = EXCLUDED.error_type, error_message = EXCLUDED.error_message, "
          + "faulted_at = EXCLUDED.faulted_at";

  // Dedicated INSERT for NULL-saga entries. The UNIQUE
  // constraint treats NULLs as distinct, so a null-saga publish at an existing offset can never
  // conflict — it always INSERTs a fresh row (the accepted duplicate accumulation documented on
  // the class). The replayer's findEntry reads the NEWEST row of the set, so a fresh row born
  // with first_replay_started_at = NULL and a fresh first_faulted_at would LAUNDER the set's
  // prior-attempt evidence on a re-quarantine: a plain replay more than one inbox-retention
  // window after the first attempt would look like a "first-EVER replay", skip the key-age
  // guard, re-derive the same forward key, MISS the pruned inbox, and re-execute an
  // already-committed forward command (duplicate CapturePayment). The COALESCE
  // subqueries below make every new row inherit the duplicate set's IMMUTABLE evidence from ONE
  // statement snapshot: the OLDEST first_faulted_at, the OLDEST first_replay_started_at, and
  // the NEWEST resolved target_saga_id among type-matching null-saga
  // rows at the offset — a target-bare fresh row would let the retention sweep prune a
  // TARGET_PENDING-resolved set out from under an operator mid-reconciliation. The subqueries
  // match the entry's own saga type: the "set" is all null-saga rows of that type at the offset —
  // exactly what findEntry/discard/setResolvedTarget operate on — so a foreign type's row at the
  // same offset (a different logical entry) never contributes. That snapshot is
  // atomic for the READ, which is NOT race-freedom against concurrent evidence ESTABLISHMENT: an
  // INSERT whose snapshot precedes a concurrent establishFirstReplayAnchor's commit inherits no
  // anchor AND is invisible to that write, so it is born laundered. It needs a live redelivery of
  // the same event racing a replay, and the first_faulted_at half still lands — those rows were
  // committed long before the snapshot. This statement does not close that race, and no pruning
  // bound is claimed for such a row: the replayer stamps the resolved target (setResolvedTarget)
  // before it establishes the anchor, so a born-laundered row whose snapshot follows that stamp
  // inherits target_saga_id, and a RESOLVED null-saga entry is never pruned by DELETE_OLDER_THAN.
  // The MUTABLE columns stay per-row: faulted_at fresh (it makes the new row the set's newest, the
  // row the newest-first reads surface) and error details fresh; target_saga_id verbatim from the
  // entry when carried forward with nothing newer stored (setResolvedTarget remains the sole writer
  // for EXISTING rows; it covers every row of the set, so a new unhinted row is re-unified by the
  // next TARGET_PENDING) — while its carried first_faulted_at keeps it from outliving its shielded
  // siblings.
  //
  // NULL saga_id rows are distinct under the UNIQUE constraint, so a refresh INSERTs a new row;
  // the three stored facts are snapshotted from the existing rows at the same offset/type in the
  // same statement (first_faulted_at, first_replay_started_at, and target_saga_id: the NEWEST
  // resolved target among the set, so a refreshed null-saga entry keeps the target the drain
  // folds on).
  private static final String NULL_SAGA_INSERT =
      "INSERT INTO saga_dead_letters ("
          + COLUMNS
          + ") VALUES (NULL, ?, ?, ?, ?, ?, ?, "
          + "COALESCE((SELECT MIN(s.first_faulted_at) FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ?), ?), "
          + "COALESCE((SELECT MIN(s.first_replay_started_at) FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ?), ?), "
          + "COALESCE((SELECT s.target_saga_id FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ? "
          + "AND s.target_saga_id IS NOT NULL ORDER BY s.faulted_at DESC LIMIT 1), ?))";

  private static final String SELECT_BY_SAGA =
      "SELECT " + COLUMNS + " FROM saga_dead_letters WHERE saga_id = ? ORDER BY faulted_at DESC";

  private static final String SELECT_ALL =
      "SELECT " + COLUMNS + " FROM saga_dead_letters ORDER BY faulted_at DESC LIMIT ?";

  // The TARGETED null-saga read. Same predicate the replayer used to evaluate in Java
  // over findAll(Integer.MAX_VALUE) — saga_id IS NULL, this offset, and the type scoping
  // (the stored type equals the parameter; there is no unscoped form, a null type is rejected
  // before this statement runs) — and the same
  // newest-faulted-first ordering, so the row it resolves is identical. What changes is that
  // PostgreSQL evaluates it and returns ONE row, instead of the JVM materialising every row in the
  // table (twice per replay attempt) to keep one.
  private static final String SELECT_NULL_SAGA_AT_OFFSET =
      "SELECT "
          + COLUMNS
          + " FROM saga_dead_letters WHERE saga_id IS NULL AND event_offset = ? "
          + "AND saga_type = ? ORDER BY faulted_at DESC LIMIT 1";

  // The replayAll fold. Null-saga entries whose REPLAYER-RESOLVED target
  // (target_saga_id, stamped on TARGET_PENDING) is the saga being drained — one row per offset
  // (DISTINCT ON keeps the newest-faulted of each duplicate set, matching
  // SELECT_NULL_SAGA_AT_OFFSET), ascending by offset so the drain can merge them into causal
  // position. Same type scoping as every other null-saga statement. Sequential scan
  // over the null-saga population by design (the offset-indexing follow-up recorded on
  // findNullSagaEntry applies here identically); operator-scale, once per drain.
  private static final String SELECT_NULL_SAGA_BY_RESOLVED_TARGET =
      "SELECT DISTINCT ON (event_offset) "
          + COLUMNS
          + " FROM saga_dead_letters "
          + "WHERE saga_id IS NULL AND target_saga_id = ? "
          + "AND saga_type = ? "
          + "ORDER BY event_offset ASC, faulted_at DESC";

  // Establish-only anchor write. COALESCE keeps a stored anchor
  // (stamp-once). SagaDeadLetterReplayer calls this before the FIRST feed of every entry it
  // drains, named or null-saga alike. With no in-flight marker, the executor's atomic
  // create-first/CAS writes keep the saga row FAULTED for an attempt's whole duration, so the
  // ordinary SHIELDED_OWNER predicate already covers the in-flight window, and this anchor is the
  // entry's ONLY durable record that a replay was ever
  // attempted, and it is what the STALE_REDRIVE_BLOCKED key-age guard measures against. Same IS
  // NOT DISTINCT FROM keying and type scoping as SET_RESOLVED_TARGET.
  private static final String ESTABLISH_FIRST_REPLAY_ANCHOR =
      "UPDATE saga_dead_letters SET first_replay_started_at = "
          + "COALESCE(first_replay_started_at, ?) "
          + "WHERE saga_id IS NOT DISTINCT FROM ? AND event_offset = ? "
          + "AND saga_type = ?";

  // Durable resolved-target write for NULL-saga entries only (a named entry's
  // association IS its saga_id). Type-scoped like ESTABLISH_FIRST_REPLAY_ANCHOR, with no unscoped
  // form: a null type is rejected before this statement runs (a foreign type's row at the same
  // offset is never written). Idempotent re-stamp; never cleared — a recovered target
  // stops matching the retention guard on its own, and discard removes the row.
  private static final String SET_RESOLVED_TARGET =
      "UPDATE saga_dead_letters SET target_saga_id = ? "
          + "WHERE saga_id IS NULL AND event_offset = ? "
          + "AND saga_type = ?";

  // null sagaId matches ONLY null-saga rows (SQL NULL-safe comparison); a non-null sagaId
  // matches only rows with that exact saga_id. Type-scoped — a cross-type SagaId
  // collision must never let one type's replayer delete another type's quarantine record. There
  // is no unscoped form: discard rejects a null type before this statement runs.
  private static final String DISCARD =
      "DELETE FROM saga_dead_letters WHERE saga_id IS NOT DISTINCT FROM ? AND event_offset = ? "
          + "AND saga_type = ?";

  // Batched ctid-based delete, mirroring PostgresCommandInbox.deleteProcessedBefore: repeatedly
  // deletes up to DELETE_BATCH_SIZE strictly-older rows until a chunk deletes nothing (see
  // deleteOlderThan for why not "until a chunk deletes fewer than DELETE_BATCH_SIZE").
  // The cutoff compares first_faulted_at (immutable), not faulted_at (refreshed by
  // every re-quarantine) — a failed replay must not extend an entry's retention life. Served by
  // the baseline's idx_saga_dead_letters_retention index on first_faulted_at.
  // Retention: the saga row's shield (dead_letter_pending) or FAULTED status
  // protects every named entry the saga owns; a named entry whose saga has no row (the row-less
  // residual) and every RESOLVED null-saga entry are never pruned either. Everything else older
  // than the cutoff is garbage. The filters run before LIMIT, so protected rows never occupy
  // batch slots and the batched loop terminates. Type-scoped: a foreign type's row
  // neither protects nor releases this type's entry. Each candidate costs one saga_state PK probe.
  private static final String SHIELDED_OWNER =
      "EXISTS (SELECT 1 FROM saga_state ss WHERE ss.saga_id = sdl.saga_id "
          + "AND (ss.status = 'FAULTED' OR ss.dead_letter_pending) "
          + "AND ss.saga_type = sdl.saga_type)";
  // A null-saga entry that has been RESOLVED to a target (the replayer stamps target_saga_id the
  // moment the router resolves it, before any refusal or feed) is operator-attended by construction
  // and never pruned while it exists; an unresolved null-saga entry is unroutable garbage after the
  // cutoff. (Replaces the earlier marker shield.)
  private static final String RESOLVED_NULL =
      "(sdl.saga_id IS NULL AND sdl.target_saga_id IS NOT NULL)";
  private static final String ROWLESS_NAMED =
      "(sdl.saga_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM saga_state ro "
          + "WHERE ro.saga_id = sdl.saga_id AND ro.saga_type = sdl.saga_type))";

  private static final String DELETE_OLDER_THAN =
      "DELETE FROM saga_dead_letters WHERE ctid IN ("
          + "SELECT sdl.ctid FROM saga_dead_letters sdl "
          + "WHERE sdl.first_faulted_at < ? "
          + "AND NOT "
          + SHIELDED_OWNER
          + " AND NOT "
          + RESOLVED_NULL
          + " AND NOT "
          + ROWLESS_NAMED
          + " ORDER BY sdl.first_faulted_at ASC LIMIT ?)";

  // The exact complement of DELETE_OLDER_THAN's protective clauses — same predicates, same type
  // scoping — with the age filter dropped: counting what the guard PROTECTS is what makes the
  // stranded-saga backlog observable. Costs one saga_state PK probe per dead-letter
  // row; the table is operator-scale and this runs once per retention sweep, not per event.
  private static final String COUNT_FAULTED_BACKLOG =
      "SELECT COUNT(*) FROM saga_dead_letters sdl WHERE "
          + SHIELDED_OWNER
          + " OR "
          + RESOLVED_NULL
          + " OR "
          + ROWLESS_NAMED;

  // The conditional shield clear, as ONE transaction of TWO statements.
  // Statement 1 locks the saga row, so the runner's hold (T: set TRUE -> P: publishShielded, which
  // writes the entry AND the shield in one transaction) queues its flag
  // writes behind this transaction. Statement 2 then runs with a FRESH snapshot taken after the
  // lock was granted, so its NOT EXISTS subqueries see every publish that committed before the
  // lock. A single UPDATE ... WHERE NOT EXISTS is NOT sufficient under READ COMMITTED: its
  // subqueries use the statement snapshot and EvalPlanQual re-checks a concurrently updated row
  // with that SAME snapshot, so a clear whose snapshot predates the hold still writes FALSE behind
  // a completed T->P->R. The two NOT EXISTS predicates are the same type-scoped populations the
  // drain folds (named entries of the type; null-saga entries whose resolved target is the saga) -
  // the exact set the shield exists for.
  private static final String LOCK_SAGA_ROW =
      "SELECT dead_letter_pending FROM saga_state WHERE saga_id = ? AND saga_type = ? FOR UPDATE";

  private static final String CLEAR_SHIELD_IF_DRAINED =
      "UPDATE saga_state SET dead_letter_pending = FALSE "
          + "WHERE saga_id = ? AND saga_type = ? AND dead_letter_pending "
          + "AND NOT EXISTS (SELECT 1 FROM saga_dead_letters n WHERE n.saga_id = saga_state.saga_id "
          + "AND n.saga_type = saga_state.saga_type) "
          + "AND NOT EXISTS (SELECT 1 FROM saga_dead_letters r WHERE r.saga_id IS NULL "
          + "AND r.target_saga_id = saga_state.saga_id "
          + "AND r.saga_type = saga_state.saga_type)";

  // The shield write that publishShielded runs in the SAME transaction as
  // the named upsert. No version bump, no updated_at, no status predicate (mirrors
  // PostgresSagaStore.SET_DEAD_LETTER_PENDING); 0 rows = absent owner row, the documented no-op.
  private static final String ASSERT_SHIELD =
      "UPDATE saga_state SET dead_letter_pending = TRUE WHERE saga_id = ? AND saga_type = ?";

  private static final int DELETE_BATCH_SIZE = 1000;

  private final DataSource dataSource;

  public PostgresSagaDeadLetterStore(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Idempotent for a named entry: a second call with the same {@code (sagaId, eventOffset)} pair
   * updates the existing row rather than inserting a duplicate. A {@code null sagaId} always
   * INSERTs (NULL-distinct unique semantics), carrying the duplicate set's immutable evidence — the
   * oldest {@code first_faulted_at}, the oldest {@code first_replay_started_at}, and the newest
   * resolved {@code target_saga_id} among type-matching null-saga rows at the offset — onto the new
   * row.
   */
  @Override
  public void publish(SagaDeadLetterEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry must not be null");
    if (entry.sagaId() == null) {
      publishNullSaga(entry);
    } else {
      publishNamed(entry);
    }
  }

  private void publishNamed(SagaDeadLetterEntry entry) {
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "saga dead-letter publish",
          c -> {
            upsertNamed(c, entry);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException("Failed to publish saga dead letter entry", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One transaction: the named upsert, then {@code UPDATE saga_state SET dead_letter_pending =
   * TRUE} for the owner row (0 rows when the row is absent — the documented no-op); commit. A
   * failure anywhere — an {@link Error} included — rolls the entry back too. Auto-commit is
   * restored before the connection returns to the pool. Lock order dead-letter row → saga row; see
   * the SPI javadoc for why that never cycles with the conditional clear or retention.
   */
  @Override
  public void publishShielded(SagaDeadLetterEntry entry) {
    if (entry == null) throw new IllegalArgumentException("entry must not be null");
    if (entry.sagaId() == null) throw new IllegalArgumentException("sagaId is required");
    try (var conn = dataSource.getConnection()) {
      inTransaction(
          conn,
          c -> {
            upsertNamed(c, entry);
            try (var ps = c.prepareStatement(ASSERT_SHIELD)) {
              ps.setString(1, entry.sagaId().value());
              ps.setString(2, entry.sagaType().value());
              ps.executeUpdate(); // 0 rows = absent row: the documented no-op
            }
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to publish and shield saga dead letter entry: "
              + LogSanitizer.sanitizeForLog(entry.sagaId().value()),
          e);
    }
  }

  /** One transaction's work, run by {@link #inTransaction}. */
  @FunctionalInterface
  private interface TransactionWork<T> {
    T run(Connection conn) throws SQLException;
  }

  /**
   * Runs {@code work} as one transaction on {@code conn} and commits it (the discipline of {@code
   * PostgresEventStore}). The {@code finally} is the single rollback point for every uncommitted
   * exit — an {@link SQLException}, a {@link RuntimeException} or an {@link Error} — and it runs
   * BEFORE autoCommit is restored, because pgjdbc's {@code setAutoCommit(true)} COMMITS an open
   * transaction. The catch used to take only {@code SQLException | RuntimeException}, so an {@code
   * OutOfMemoryError} between {@code publishShielded}'s upsert and its shield write committed the
   * entry alone: entry present, shield FALSE, the entry-without-shield state the SPI forbids.
   *
   * <p>If the rollback itself fails (a {@code SQLException}, a {@code RuntimeException} or an
   * {@link Error}), the transaction may still be open, and nothing after it may commit it: not an
   * autoCommit restore here, and not the pool's reset when the caller closes the connection.
   * HikariCP rolls a dirty connection back on return, but Agroal, the Quarkus pool, resets a
   * changed autoCommit with {@code setAutoCommit(true)} and no rollback first, and pgjdbc COMMITS
   * the open transaction there: {@code publishShielded}'s entry without its shield. So a failed
   * rollback aborts the physical connection instead ({@link PostgresTransactions#rollbackOrAbort});
   * the server discards the transaction. Neither the rollback's, the restore's nor the abort's
   * {@code SQLException} or {@code RuntimeException} replaces the exception in flight; an {@code
   * Error} from the rollback propagates once the connection is aborted. After a successful commit a
   * failing restore is reported, as before: the writes are durable and idempotent, so a rerun
   * converges.
   */
  private static <T> T inTransaction(Connection conn, TransactionWork<T> work) throws SQLException {
    boolean autoCommit = conn.getAutoCommit();
    conn.setAutoCommit(false);
    boolean committed = false;
    try {
      T result = work.run(conn);
      conn.commit();
      committed = true;
      return result;
    } finally {
      if (committed) {
        conn.setAutoCommit(autoCommit);
      } else if (PostgresTransactions.rollbackOrAbort(conn, "saga dead-letter")) {
        restoreAutoCommitQuietly(conn, autoCommit);
      }
    }
  }

  /** Restores autoCommit after a rollback, without replacing the exception in flight. */
  private static void restoreAutoCommitQuietly(Connection conn, boolean autoCommit) {
    try {
      conn.setAutoCommit(autoCommit);
    } catch (SQLException | RuntimeException restoreFailure) {
      log.debug(
          "Failed to restore autoCommit after a rollback (the pool resets it on return): {}",
          restoreFailure.toString());
    }
  }

  private static void upsertNamed(Connection conn, SagaDeadLetterEntry entry) throws SQLException {
    try (var ps = conn.prepareStatement(UPSERT)) {
      ps.setString(1, entry.sagaId().value());
      ps.setString(2, entry.sagaType().value());
      ps.setLong(3, entry.eventOffset().value());
      ps.setString(4, entry.eventType().name());
      ps.setString(5, entry.errorType());
      ps.setString(6, entry.errorMessage());
      ps.setTimestamp(7, Timestamp.from(entry.faultedAt()));
      // The first-insert candidate for the immutable first_faulted_at. On an upsert
      // conflict the existing stored value wins (excluded from the DO UPDATE set).
      Instant firstFaultedAt =
          entry.firstFaultedAt() == null ? entry.faultedAt() : entry.firstFaultedAt();
      ps.setTimestamp(8, Timestamp.from(firstFaultedAt));
      // The immutable first-replay-attempt anchor, normally null on a quarantine
      // input. First-INSERT only — on a conflict the STORED value wins (excluded from the DO
      // UPDATE set, mirroring first_faulted_at); establishFirstReplayAnchor's COALESCE is its
      // sole writer for existing rows.
      if (entry.firstReplayStartedAt() == null) {
        ps.setNull(9, Types.TIMESTAMP_WITH_TIMEZONE);
      } else {
        ps.setTimestamp(9, Timestamp.from(entry.firstReplayStartedAt()));
      }
      // The resolved-target hint, normally null on a quarantine input. First-INSERT
      // only — on a conflict the STORED value wins (excluded from the DO UPDATE set), so a
      // re-quarantine can never strip the retention protection an earlier TARGET_PENDING
      // resolution established; setResolvedTarget is its sole writer for existing rows.
      ps.setString(10, entry.targetSagaId() == null ? null : entry.targetSagaId().value());
      ps.executeUpdate();
    }
  }

  /**
   * A null-saga publish can never hit the upsert's conflict arm (two NULLs are distinct to the
   * unique constraint), so the immutable-column protection the named path gets from "the STORED
   * value wins on conflict" is provided here by the {@code NULL_SAGA_INSERT} carry-forward
   * subqueries instead: the new row is born with the duplicate set's oldest {@code
   * first_faulted_at} (falling back to the entry's value normalized to {@code faultedAt}), the
   * oldest {@code first_replay_started_at} (falling back to the entry's own value, normally null —
   * a never-attempted set stays unanchored and its first-EVER replay is never refused), and the
   * NEWEST resolved {@code target_saga_id} among the set (falling back to the entry's own value,
   * normally null — so a refreshed null-saga entry keeps the target the drain folds on instead of
   * losing the resolution on every re-quarantine).
   */
  private void publishNullSaga(SagaDeadLetterEntry entry) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(NULL_SAGA_INSERT)) {
      String typeVal = entry.sagaType().value();
      long offsetVal = entry.eventOffset().value();
      ps.setString(1, typeVal);
      ps.setLong(2, offsetVal);
      ps.setString(3, entry.eventType().name());
      ps.setString(4, entry.errorType());
      ps.setString(5, entry.errorMessage());
      ps.setTimestamp(6, Timestamp.from(entry.faultedAt()));
      // first_faulted_at = COALESCE(oldest among the type-matching set, fresh normalized value).
      ps.setLong(7, offsetVal);
      ps.setString(8, typeVal);
      Instant firstFaultedAt =
          entry.firstFaultedAt() == null ? entry.faultedAt() : entry.firstFaultedAt();
      ps.setTimestamp(9, Timestamp.from(firstFaultedAt));
      // first_replay_started_at = COALESCE(oldest anchor among the type-matching set, the entry's
      // own value — normally null).
      ps.setLong(10, offsetVal);
      ps.setString(11, typeVal);
      if (entry.firstReplayStartedAt() == null) {
        ps.setNull(12, Types.TIMESTAMP_WITH_TIMEZONE);
      } else {
        ps.setTimestamp(12, Timestamp.from(entry.firstReplayStartedAt()));
      }
      // target_saga_id = COALESCE(newest resolved target among the type-matching set, the entry's
      // own value — normally null).
      ps.setLong(13, offsetVal);
      ps.setString(14, typeVal);
      ps.setString(15, entry.targetSagaId() == null ? null : entry.targetSagaId().value());
      PostgresTransactions.commitIfManual(
          conn, "saga dead-letter publish", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to publish saga dead letter entry", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Establish-only ({@code COALESCE(first_replay_started_at, ?)} — the stored anchor always
   * wins). Keyed and type-scoped exactly like {@link #setResolvedTarget}; a {@code null sagaType}
   * is rejected before the statement runs.
   */
  @Override
  public void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (eventOffset == null) throw new IllegalArgumentException("eventOffset is required");
    if (anchorIfAbsent == null) throw new IllegalArgumentException("anchorIfAbsent is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(ESTABLISH_FIRST_REPLAY_ANCHOR)) {
      ps.setTimestamp(1, Timestamp.from(anchorIfAbsent));
      if (sagaId == null) {
        ps.setNull(2, Types.VARCHAR);
      } else {
        ps.setString(2, sagaId.value());
      }
      ps.setLong(3, eventOffset.value());
      ps.setString(4, sagaType.value());
      PostgresTransactions.commitIfManual(
          conn, "saga dead-letter replay anchor", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to establish saga dead letter first-replay anchor", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Updates only null-saga rows at the offset; type-scoped like {@link
   * #establishFirstReplayAnchor} (a {@code null sagaType} is rejected before the statement runs).
   */
  @Override
  public void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (eventOffset == null) throw new IllegalArgumentException("eventOffset is required");
    if (targetSagaId == null) throw new IllegalArgumentException("targetSagaId is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SET_RESOLVED_TARGET)) {
      ps.setString(1, targetSagaId.value());
      ps.setLong(2, eventOffset.value());
      ps.setString(3, sagaType.value());
      PostgresTransactions.commitIfManual(
          conn, "saga dead-letter resolved target", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to set saga dead letter resolved target", e);
    }
  }

  @Override
  public List<SagaDeadLetterEntry> findAll(int limit) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_ALL)) {
      ps.setInt(1, limit);
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to find all saga dead letters", e);
    }
  }

  @Override
  public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
    if (sagaId == null) throw new IllegalArgumentException("sagaId is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_BY_SAGA)) {
      ps.setString(1, sagaId.value());
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find saga dead letters for saga: "
              + LogSanitizer.sanitizeForLog(sagaId.value()),
          e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>A single indexed-predicate read returning at most one row, replacing the replayer's former
   * {@code findAll(Integer.MAX_VALUE)} scan over a table whose null-saga population grows one row
   * per re-quarantine.
   *
   * <p><b>No supporting index is shipped with this change, deliberately</b> — recorded here rather
   * than left silent. The only indexes on {@code saga_dead_letters} are the partial one on {@code
   * saga_id} ({@code WHERE saga_id IS NOT NULL}, so it excludes exactly this population), one on
   * {@code faulted_at}, and the retention expression, so this predicate is a sequential scan.
   * Adding {@code (event_offset) WHERE saga_id IS NULL} is a schema change — a new versioned
   * migration, {@code SchemaValidator.EXPECTED_VERSION} and the tests that pin it, and the demo
   * repository's hand-written {@code init-db.sql} drift check, which lives in a DIFFERENT
   * repository — and is tracked as a follow-up. The memory blow-up is what kills the JVM and it is
   * gone either way: the scan runs in the database, streams, and returns one row, where the
   * previous code allocated a {@code SagaDeadLetterEntry} for every row in the table and did it
   * twice per replay attempt.
   *
   * <p>A {@code null sagaType} is rejected before the statement runs: there is no untyped lookup.
   */
  @Override
  public Optional<SagaDeadLetterEntry> findNullSagaEntry(
      SagaType sagaType, GlobalOffset eventOffset) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (eventOffset == null) throw new IllegalArgumentException("eventOffset is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_NULL_SAGA_AT_OFFSET)) {
      ps.setLong(1, eventOffset.value());
      ps.setString(2, sagaType.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(fromRow(rs)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find null-saga dead letter at offset " + eventOffset.value(), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One indexed-predicate read returning the newest row per offset ({@code DISTINCT ON}),
   * ascending by offset — the shape {@code SagaDeadLetterReplayer.replayAll} merges into its drain.
   * A {@code null sagaType} is rejected before the statement runs: there is no untyped lookup.
   */
  @Override
  public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (targetSagaId == null) throw new IllegalArgumentException("targetSagaId is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_NULL_SAGA_BY_RESOLVED_TARGET)) {
      ps.setString(1, targetSagaId.value());
      ps.setString(2, sagaType.value());
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find null-saga dead letters resolved to saga: " + targetSagaId, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code sagaId} nullness is significant: {@code null} is matched using SQL {@code IS NOT
   * DISTINCT FROM}, so it removes only rows whose stored {@code saga_id} is also {@code null} —
   * never a row that has a non-null saga id at the same offset. Type-scoped: only rows whose stored
   * {@code saga_type} equals {@code sagaType} are removed. A {@code null sagaType} is rejected:
   * there is no untyped discard.
   */
  @Override
  public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
    if (sagaType == null) throw new IllegalArgumentException("sagaType must not be null");
    if (eventOffset == null) throw new IllegalArgumentException("eventOffset is required");
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(DISCARD)) {
      if (sagaId == null) {
        ps.setNull(1, Types.VARCHAR);
      } else {
        ps.setString(1, sagaId.value());
      }
      ps.setLong(2, eventOffset.value());
      ps.setString(3, sagaType.value());
      return PostgresTransactions.commitIfManual(
              conn, "saga dead-letter discard", c -> ps.executeUpdate())
          > 0;
    } catch (SQLException e) {
      throw new EventStoreException("Failed to discard saga dead letter entry", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One transaction, two statements: {@code SELECT … FOR UPDATE} on the saga row (serializes
   * against the runner's shield writes; an absent row ends the call with {@code false}), then the
   * conditional {@code UPDATE} with a fresh snapshot. An uncommitted exit, an {@link Error}
   * included, commits nothing. The connection's auto-commit setting is restored before it returns
   * to the pool.
   */
  @Override
  public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) throw new IllegalArgumentException("sagaId is required");
    if (sagaType == null) throw new IllegalArgumentException("sagaType is required");
    try (var conn = dataSource.getConnection()) {
      return inTransaction(
          conn,
          c -> {
            try (var lock = c.prepareStatement(LOCK_SAGA_ROW)) {
              lock.setString(1, sagaId.value());
              lock.setString(2, sagaType.value());
              try (var rs = lock.executeQuery()) {
                if (!rs.next()) {
                  // Absent row (the row-less residual): nothing to clear. The transaction wrote
                  // nothing and locked nothing, so the commit that ends it is a no-op.
                  return false;
                }
              }
            }
            try (var ps = c.prepareStatement(CLEAR_SHIELD_IF_DRAINED)) {
              ps.setString(1, sagaId.value());
              ps.setString(2, sagaType.value());
              return ps.executeUpdate() > 0;
            }
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to clear saga dead-letter shield: " + LogSanitizer.sanitizeForLog(sagaId.value()),
          e);
    }
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    if (cutoff == null) throw new IllegalArgumentException("cutoff is required");
    int total = 0;
    int batch;
    do {
      try (var conn = dataSource.getConnection();
          var ps = conn.prepareStatement(DELETE_OLDER_THAN)) {
        ps.setTimestamp(1, Timestamp.from(cutoff));
        ps.setInt(2, DELETE_BATCH_SIZE);
        batch =
            PostgresTransactions.commitIfManual(
                conn, "saga dead-letter retention", c -> ps.executeUpdate());
        total += batch;
      } catch (SQLException e) {
        throw new EventStoreException("Failed to delete old saga dead letters", e);
      }
      // Loop while ANY row was deleted, not only on a full DELETE_BATCH_SIZE chunk (the same
      // termination as PostgresOutboxStore.deleteDelivered/deleteSkipped). A candidate that a
      // discard or a second instance's sweeper removes inside the DELETE window is skipped by
      // EvalPlanQual, and so is one any concurrent UPDATE rewrites (the outer DELETE matches by
      // ctid, and the rewrite moves it to a new ctid). The replayer's own writes are not that
      // case in production: it stamps a resolved target and shield on a null-saga entry before
      // anchoring it, and a named entry under replay belongs to a shielded saga, so both are
      // already protected. The chunk then deletes fewer than LIMIT although prunable rows remain,
      // and a `batch == DELETE_BATCH_SIZE` termination exited with them left behind; the next
      // chunk re-reads a fresh snapshot, so a row whose rewrite made it protected is filtered out
      // there. `batch > 0` terminates: each non-empty
      // chunk strictly shrinks the prunable aged set, which grows only by a replay re-quarantining
      // an event (inheriting its first fault) or a saga leaving FAULTED — bounded arrivals driven
      // by replays and recoveries, not a stream the sweep can chase indefinitely.
    } while (batch > 0);
    return total;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Counts exactly the set {@link #deleteOlderThan} protects — the same {@code SHIELDED_OWNER} /
   * {@code RESOLVED_NULL} / {@code ROWLESS_NAMED} predicates without the age filter — so the gauge
   * and the retention decision cannot disagree about which entries are protected.
   */
  @Override
  public long countFaultedBacklog() {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(COUNT_FAULTED_BACKLOG);
        var rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    } catch (SQLException e) {
      throw new EventStoreException("Failed to count the FAULTED saga dead-letter backlog", e);
    }
  }

  private List<SagaDeadLetterEntry> readEntries(PreparedStatement ps) throws SQLException {
    var entries = new ArrayList<SagaDeadLetterEntry>();
    try (var rs = ps.executeQuery()) {
      while (rs.next()) {
        entries.add(fromRow(rs));
      }
    }
    return List.copyOf(entries);
  }

  private SagaDeadLetterEntry fromRow(ResultSet rs) throws SQLException {
    String sagaIdVal = rs.getString("saga_id");
    // Nullable immutable first-replay-attempt anchor — NULL means no replay has been
    // attempted yet; the replayer establishes it before the entry's first feed.
    Timestamp firstReplayStarted = rs.getTimestamp("first_replay_started_at");
    // Nullable replayer-resolved target of a null-saga entry.
    String targetSagaIdVal = rs.getString("target_saga_id");
    return new SagaDeadLetterEntry(
        sagaIdVal == null ? null : SagaId.of(sagaIdVal),
        SagaType.of(rs.getString("saga_type")),
        GlobalOffset.of(rs.getLong("event_offset")),
        EventType.of(rs.getString("event_type")),
        rs.getString("error_type"),
        rs.getString("error_message"),
        rs.getTimestamp("faulted_at").toInstant(),
        rs.getTimestamp("first_faulted_at").toInstant(),
        firstReplayStarted == null ? null : firstReplayStarted.toInstant(),
        targetSagaIdVal == null ? null : SagaId.of(targetSagaIdVal));
  }
}

package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SagaType;

/**
 * The SQLite {@link SagaDeadLetterStore}: poison saga events in the {@code saga_dead_letters} table
 * of a {@link SqliteDatabase}, with the semantics of {@code PostgresSagaDeadLetterStore}, and the
 * shield on the {@code saga_state} rows of the same database, the rows {@link SqliteSagaStore}
 * keeps. Pair it with a {@link SqliteSagaStore} on the same database file. The tables belong to the
 * event-store baseline, which {@link SqliteEventStoreFactory} migrates.
 *
 * <p><b>Stored facts win.</b> {@link #publish} upserts a named entry by {@code (saga_id,
 * event_offset)}: a re-quarantine refreshes the error details and {@code faulted_at} and keeps the
 * stored {@code first_faulted_at}, {@code first_replay_started_at} and {@code target_saga_id}. A
 * null-saga entry never conflicts (two {@code NULL}s are distinct to the unique constraint, as on
 * Postgres), so each re-quarantine inserts a row born with the set's evidence: the oldest {@code
 * first_faulted_at}, the oldest {@code first_replay_started_at} and the newest resolved {@code
 * target_saga_id} among null-saga rows of the type at the offset, read by the inserting statement.
 *
 * <p><b>One write lock, so no row locks.</b> Every write runs as {@code BEGIN IMMEDIATE} … {@code
 * COMMIT} (see {@code SqliteTransactions}), holding SQLite's database-wide write lock from before
 * its first statement until it commits, and reading the newest committed rows. So:
 *
 * <ul>
 *   <li>{@link #publishShielded} writes the entry and the owner's shield in one transaction: after
 *       any crash either both exist or neither does;
 *   <li>{@link #publishShieldedIfAbsent} reads the owner's {@code last_applied_offset}, writes the
 *       entry unless it is applied or already held, and asserts the shield, in one transaction: the
 *       row it decides on is the row it writes, and two concurrent holds of one event write it
 *       once;
 *   <li>{@link #clearShieldIfDrained} is one conditional {@code UPDATE}: no entry can be written
 *       between its check and its write. On Postgres it first locks the saga row with {@code FOR
 *       UPDATE} because a READ COMMITTED statement checks with a snapshot older than the lock; a
 *       SQLite write transaction's snapshot is the newest commit, and no writer can commit while it
 *       runs.
 * </ul>
 *
 * A write that reads a row and then writes must keep both in one such transaction.
 *
 * <p><b>The row-write bound.</b> A write waits for the write lock at most the connection's {@code
 * busy_timeout} and then fails with an {@link EventStoreException} naming {@code busy_timeout},
 * having written nothing, as {@code PostgresSagaDeadLetterStore} fails a write past its bound.
 * Reads run in autocommit and never wait for a writer.
 *
 * <p><b>Retention.</b> {@link #deleteOlderThan} deletes in batches of {@value #DELETE_BATCH_SIZE}
 * rows, each its own transaction, until a batch deletes nothing; an entry whose owner row is {@code
 * FAULTED} or shielded, a named entry whose saga has no row, and a resolved null-saga entry are
 * never deleted, and {@link #countFaultedBacklog} counts exactly those.
 *
 * <p><b>Failures.</b> A storage failure — a lost connection, the write lock not granted in time, a
 * constraint — is an {@link EventStoreException}. A connection handed out in manual-commit mode is
 * refused with an {@link IllegalStateException} before anything is written, as every SQLite store
 * refuses it (see {@code SqliteTransactions#write}). A {@code null} argument the interface
 * requires, and a negative {@code limit}, are refused with {@link IllegalArgumentException} before
 * any I/O.
 */
public final class SqliteSagaDeadLetterStore implements SagaDeadLetterStore {

  private static final String COLUMNS =
      "saga_id, saga_type, event_offset, event_type, error_type, error_message, faulted_at, "
          + "first_faulted_at, first_replay_started_at, target_saga_id";

  // The named upsert. first_faulted_at, first_replay_started_at and target_saga_id are not in the
  // DO UPDATE set: on a conflict the stored values win.
  private static final String UPSERT =
      "INSERT INTO saga_dead_letters ("
          + COLUMNS
          + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT (saga_id, event_offset) DO UPDATE SET "
          + "saga_type = excluded.saga_type, event_type = excluded.event_type, "
          + "error_type = excluded.error_type, error_message = excluded.error_message, "
          + "faulted_at = excluded.faulted_at";

  // The hold's upsert: a stored row of the same type is kept as it is (0 rows changed), a stored
  // row of another type is replaced as UPSERT replaces it (the named key has no type).
  private static final String HOLD_UPSERT =
      UPSERT + " WHERE saga_dead_letters.saga_type <> excluded.saga_type";

  // The null-saga insert: born with the set's oldest first_faulted_at and first-replay anchor and
  // its newest resolved target, among null-saga rows of the type at the offset.
  private static final String NULL_SAGA_INSERT =
      "INSERT INTO saga_dead_letters ("
          + COLUMNS
          + ") VALUES (NULL, ?, ?, ?, ?, ?, ?, "
          + "coalesce((SELECT min(s.first_faulted_at) FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ?), ?), "
          + "coalesce((SELECT min(s.first_replay_started_at) FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ?), ?), "
          + "coalesce((SELECT s.target_saga_id FROM saga_dead_letters s "
          + "WHERE s.saga_id IS NULL AND s.event_offset = ? AND s.saga_type = ? "
          + "AND s.target_saga_id IS NOT NULL ORDER BY s.faulted_at DESC LIMIT 1), ?))";

  private static final String SELECT_BY_SAGA =
      "SELECT " + COLUMNS + " FROM saga_dead_letters WHERE saga_id = ? ORDER BY faulted_at DESC";

  private static final String SELECT_ALL =
      "SELECT " + COLUMNS + " FROM saga_dead_letters ORDER BY faulted_at DESC LIMIT ?";

  private static final String SELECT_NULL_SAGA_AT_OFFSET =
      "SELECT "
          + COLUMNS
          + " FROM saga_dead_letters WHERE saga_id IS NULL AND event_offset = ? "
          + "AND saga_type = ? ORDER BY faulted_at DESC LIMIT 1";

  // The newest row of each offset (Postgres: DISTINCT ON), ascending by offset.
  private static final String SELECT_NULL_SAGA_BY_RESOLVED_TARGET =
      "SELECT "
          + COLUMNS
          + " FROM (SELECT "
          + COLUMNS
          + ", row_number() OVER (PARTITION BY event_offset ORDER BY faulted_at DESC) AS newest"
          + " FROM saga_dead_letters WHERE saga_id IS NULL AND target_saga_id = ?"
          + " AND saga_type = ?) WHERE newest = 1 ORDER BY event_offset ASC";

  // Establish-only: a stored anchor wins. "saga_id IS ?" is Postgres' IS NOT DISTINCT FROM: a
  // NULL id matches only null-saga rows.
  private static final String ESTABLISH_FIRST_REPLAY_ANCHOR =
      "UPDATE saga_dead_letters SET first_replay_started_at = "
          + "coalesce(first_replay_started_at, ?) "
          + "WHERE saga_id IS ? AND event_offset = ? AND saga_type = ?";

  private static final String SET_RESOLVED_TARGET =
      "UPDATE saga_dead_letters SET target_saga_id = ? "
          + "WHERE saga_id IS NULL AND event_offset = ? AND saga_type = ?";

  private static final String DISCARD =
      "DELETE FROM saga_dead_letters WHERE saga_id IS ? AND event_offset = ? AND saga_type = ?";

  // The retention guard's three protected populations, type-scoped.
  private static final String SHIELDED_OWNER =
      "EXISTS (SELECT 1 FROM saga_state ss WHERE ss.saga_id = sdl.saga_id "
          + "AND (ss.status = 'FAULTED' OR ss.dead_letter_pending = 1) "
          + "AND ss.saga_type = sdl.saga_type)";
  private static final String RESOLVED_NULL =
      "(sdl.saga_id IS NULL AND sdl.target_saga_id IS NOT NULL)";
  private static final String ROWLESS_NAMED =
      "(sdl.saga_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM saga_state ro "
          + "WHERE ro.saga_id = sdl.saga_id AND ro.saga_type = sdl.saga_type))";

  // One batch, by row id (Postgres: ctid), oldest first fault first; protected rows never occupy
  // a slot, so the loop terminates.
  private static final String DELETE_OLDER_THAN =
      "DELETE FROM saga_dead_letters WHERE rowid IN ("
          + "SELECT sdl.rowid FROM saga_dead_letters sdl "
          + "WHERE sdl.first_faulted_at < ? "
          + "AND NOT "
          + SHIELDED_OWNER
          + " AND NOT "
          + RESOLVED_NULL
          + " AND NOT "
          + ROWLESS_NAMED
          + " ORDER BY sdl.first_faulted_at ASC LIMIT ?)";

  private static final String COUNT_FAULTED_BACKLOG =
      "SELECT count(*) FROM saga_dead_letters sdl WHERE "
          + SHIELDED_OWNER
          + " OR "
          + RESOLVED_NULL
          + " OR "
          + ROWLESS_NAMED;

  // The conditional clear: one statement, under the write lock (see the class Javadoc).
  private static final String CLEAR_SHIELD_IF_DRAINED =
      "UPDATE saga_state SET dead_letter_pending = 0 "
          + "WHERE saga_id = ? AND saga_type = ? AND dead_letter_pending = 1 "
          + "AND NOT EXISTS (SELECT 1 FROM saga_dead_letters n WHERE n.saga_id = saga_state.saga_id "
          + "AND n.saga_type = saga_state.saga_type) "
          + "AND NOT EXISTS (SELECT 1 FROM saga_dead_letters r WHERE r.saga_id IS NULL "
          + "AND r.target_saga_id = saga_state.saga_id "
          + "AND r.saga_type = saga_state.saga_type)";

  // The shield of publishShielded and of a hold: no version bump, no updated_at; 0 rows (absent
  // owner row) is the documented no-op.
  private static final String ASSERT_SHIELD =
      "UPDATE saga_state SET dead_letter_pending = 1 WHERE saga_id = ? AND saga_type = ?";

  // The hold's applied check, read in the hold's transaction before it writes.
  private static final String SELECT_LAST_APPLIED =
      "SELECT last_applied_offset FROM saga_state WHERE saga_id = ? AND saga_type = ?";

  // The entry the hold's upsert kept, read in the hold's transaction.
  private static final String SELECT_NAMED_AT_OFFSET =
      "SELECT " + COLUMNS + " FROM saga_dead_letters WHERE saga_id = ? AND event_offset = ?";

  /** The rows one retention batch deletes at most. */
  static final int DELETE_BATCH_SIZE = 1000;

  private final DataSource dataSource;

  /**
   * Creates the store over {@code database}.
   *
   * @param database the database whose {@code saga_dead_letters} and {@code saga_state} tables the
   *     store reads and writes
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteSagaDeadLetterStore(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
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
  public void publish(SagaDeadLetterEntry entry) {
    if (entry == null) {
      throw new IllegalArgumentException("entry must not be null");
    }
    if (entry.sagaId() == null) {
      write(
          "saga dead-letter publish",
          "Failed to publish saga dead letter entry",
          conn -> {
            insertNullSaga(conn, entry);
            return null;
          });
    } else {
      write(
          "saga dead-letter publish",
          "Failed to publish saga dead letter entry",
          conn -> writeNamed(conn, UPSERT, entry));
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One transaction: the named upsert, then the owner's shield; a failure anywhere writes
   * neither.
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void publishShielded(SagaDeadLetterEntry entry) {
    requireNamed(entry);
    write(
        "saga dead-letter publish",
        "Failed to publish and shield saga dead letter entry: " + forLog(entry.sagaId()),
        conn -> {
          writeNamed(conn, UPSERT, entry);
          assertShield(conn, entry);
          return null;
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>One transaction: the owner's {@code last_applied_offset} is read first (an applied event
   * writes nothing), then the hold's upsert, then the owner's shield, then, when the upsert kept a
   * stored entry, that entry.
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public HoldResult publishShieldedIfAbsent(SagaDeadLetterEntry entry) {
    requireNamed(entry);
    return write(
        "saga dead-letter hold",
        "Failed to record and shield held saga event: " + forLog(entry.sagaId()),
        conn -> hold(conn, entry));
  }

  /** The work of {@link #publishShieldedIfAbsent}'s transaction. */
  private HoldResult hold(Connection conn, SagaDeadLetterEntry entry) throws SQLException {
    Long lastApplied = null;
    try (PreparedStatement ps = conn.prepareStatement(SELECT_LAST_APPLIED)) {
      ps.setString(1, entry.sagaId().value());
      ps.setString(2, entry.sagaType().value());
      try (ResultSet rs = ps.executeQuery()) { // no row = absent owner: applied nothing
        if (rs.next()) {
          long applied = rs.getLong(1);
          lastApplied = rs.wasNull() ? null : applied;
        }
      }
    }
    if (lastApplied != null && lastApplied >= entry.eventOffset().value()) {
      return new HoldResult.AlreadyApplied(lastApplied); // neither the entry nor the shield
    }
    boolean written = writeNamed(conn, HOLD_UPSERT, entry) > 0;
    assertShield(conn, entry);
    if (written) {
      return new HoldResult.Written();
    }
    try (PreparedStatement ps = conn.prepareStatement(SELECT_NAMED_AT_OFFSET)) {
      ps.setString(1, entry.sagaId().value());
      ps.setLong(2, entry.eventOffset().value());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new SQLException(
              "the kept saga dead-letter entry was not found in its transaction");
        }
        return new HoldResult.Kept(fromRow(rs));
      }
    }
  }

  private static void assertShield(Connection conn, SagaDeadLetterEntry entry) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(ASSERT_SHIELD)) {
      ps.setString(1, entry.sagaId().value());
      ps.setString(2, entry.sagaType().value());
      ps.executeUpdate(); // 0 rows = absent owner row: the documented no-op
    }
  }

  /** Binds a named entry to {@code sql} (UPSERT or HOLD_UPSERT) and returns the row count. */
  private static int writeNamed(Connection conn, String sql, SagaDeadLetterEntry entry)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, entry.sagaId().value());
      ps.setString(2, entry.sagaType().value());
      ps.setLong(3, entry.eventOffset().value());
      ps.setString(4, entry.eventType().value());
      ps.setString(5, entry.errorType());
      ps.setString(6, entry.errorMessage());
      ps.setLong(7, SqliteTimestamps.toMicros(entry.faultedAt()));
      // First-insert candidates only: on a conflict the stored values win.
      ps.setLong(8, SqliteTimestamps.toMicros(firstFaultedAt(entry)));
      setInstant(ps, 9, entry.firstReplayStartedAt());
      ps.setString(10, entry.targetSagaId() == null ? null : entry.targetSagaId().value());
      return ps.executeUpdate();
    }
  }

  private static void insertNullSaga(Connection conn, SagaDeadLetterEntry entry)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(NULL_SAGA_INSERT)) {
      String type = entry.sagaType().value();
      long offset = entry.eventOffset().value();
      ps.setString(1, type);
      ps.setLong(2, offset);
      ps.setString(3, entry.eventType().value());
      ps.setString(4, entry.errorType());
      ps.setString(5, entry.errorMessage());
      ps.setLong(6, SqliteTimestamps.toMicros(entry.faultedAt()));
      ps.setLong(7, offset);
      ps.setString(8, type);
      ps.setLong(9, SqliteTimestamps.toMicros(firstFaultedAt(entry)));
      ps.setLong(10, offset);
      ps.setString(11, type);
      setInstant(ps, 12, entry.firstReplayStartedAt());
      ps.setLong(13, offset);
      ps.setString(14, type);
      ps.setString(15, entry.targetSagaId() == null ? null : entry.targetSagaId().value());
      ps.executeUpdate();
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
  public void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
    requireType(sagaType);
    requireOffset(eventOffset);
    if (anchorIfAbsent == null) {
      throw new IllegalArgumentException("anchorIfAbsent is required");
    }
    write(
        "saga dead-letter replay anchor",
        "Failed to establish saga dead letter first-replay anchor",
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(ESTABLISH_FIRST_REPLAY_ANCHOR)) {
            ps.setLong(1, SqliteTimestamps.toMicros(anchorIfAbsent));
            setSagaId(ps, 2, sagaId);
            ps.setLong(3, eventOffset.value());
            ps.setString(4, sagaType.value());
            return ps.executeUpdate();
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
  public void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
    requireType(sagaType);
    requireOffset(eventOffset);
    if (targetSagaId == null) {
      throw new IllegalArgumentException("targetSagaId is required");
    }
    write(
        "saga dead-letter resolved target",
        "Failed to set saga dead letter resolved target",
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(SET_RESOLVED_TARGET)) {
            ps.setString(1, targetSagaId.value());
            ps.setLong(2, eventOffset.value());
            ps.setString(3, sagaType.value());
            return ps.executeUpdate();
          }
        });
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException also if {@code limit} is negative
   */
  @Override
  public List<SagaDeadLetterEntry> findAll(int limit) {
    SqliteSagaStore.requireLimit(limit);
    return read(SELECT_ALL, "Failed to find all saga dead letters", ps -> ps.setInt(1, limit));
  }

  @Override
  public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
    if (sagaId == null) {
      throw new IllegalArgumentException("sagaId is required");
    }
    return read(
        SELECT_BY_SAGA,
        "Failed to find saga dead letters for saga: " + forLog(sagaId),
        ps -> ps.setString(1, sagaId.value()));
  }

  @Override
  public Optional<SagaDeadLetterEntry> findNullSagaEntry(
      SagaType sagaType, GlobalOffset eventOffset) {
    requireType(sagaType);
    requireOffset(eventOffset);
    List<SagaDeadLetterEntry> found =
        read(
            SELECT_NULL_SAGA_AT_OFFSET,
            "Failed to find null-saga dead letter at offset " + eventOffset.value(),
            ps -> {
              ps.setLong(1, eventOffset.value());
              ps.setString(2, sagaType.value());
            });
    return found.stream().findFirst();
  }

  @Override
  public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    requireType(sagaType);
    if (targetSagaId == null) {
      throw new IllegalArgumentException("targetSagaId is required");
    }
    return read(
        SELECT_NULL_SAGA_BY_RESOLVED_TARGET,
        "Failed to find null-saga dead letters resolved to saga: " + forLog(targetSagaId),
        ps -> {
          ps.setString(1, targetSagaId.value());
          ps.setString(2, sagaType.value());
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
  public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
    requireType(sagaType);
    requireOffset(eventOffset);
    int deleted =
        write(
            "saga dead-letter discard",
            "Failed to discard saga dead letter entry",
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(DISCARD)) {
                setSagaId(ps, 1, sagaId);
                ps.setLong(2, eventOffset.value());
                ps.setString(3, sagaType.value());
                return ps.executeUpdate();
              }
            });
    return deleted > 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>One conditional {@code UPDATE} in a write transaction; an absent row clears nothing.
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
    if (sagaId == null) {
      throw new IllegalArgumentException("sagaId is required");
    }
    if (sagaType == null) {
      throw new IllegalArgumentException("sagaType is required");
    }
    int cleared =
        write(
            "saga dead-letter shield clear",
            "Failed to clear saga dead-letter shield: " + forLog(sagaId),
            conn -> {
              try (PreparedStatement ps = conn.prepareStatement(CLEAR_SHIELD_IF_DRAINED)) {
                ps.setString(1, sagaId.value());
                ps.setString(2, sagaType.value());
                return ps.executeUpdate();
              }
            });
    return cleared > 0;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Batches of {@value #DELETE_BATCH_SIZE} rows, each its own write transaction, until a batch
   * deletes nothing.
   *
   * @throws EventStoreException if a batch fails, including when the write lock is not granted
   *     within {@code busy_timeout}; the batches before it stay deleted
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public int deleteOlderThan(Instant cutoff) {
    if (cutoff == null) {
      throw new IllegalArgumentException("cutoff is required");
    }
    long cutoffMicros = SqliteTimestamps.toMicros(cutoff);
    int total = 0;
    int batch;
    do {
      batch =
          write(
              "saga dead-letter retention",
              "Failed to delete old saga dead letters",
              conn -> {
                try (PreparedStatement ps = conn.prepareStatement(DELETE_OLDER_THAN)) {
                  ps.setLong(1, cutoffMicros);
                  ps.setInt(2, DELETE_BATCH_SIZE);
                  return ps.executeUpdate();
                }
              });
      total += batch;
    } while (batch > 0);
    return total;
  }

  @Override
  public long countFaultedBacklog() {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(COUNT_FAULTED_BACKLOG);
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to count the FAULTED saga dead-letter backlog", e);
    }
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private List<SagaDeadLetterEntry> read(String sql, String failureMessage, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      List<SagaDeadLetterEntry> entries = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          entries.add(fromRow(rs));
        }
      }
      return List.copyOf(entries);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
  }

  /** Runs {@code work} as one write transaction, through {@link SqliteTransactions#write}. */
  private <T> T write(String transaction, String failureMessage, SqliteTransactions.Work<T> work) {
    return SqliteTransactions.write(dataSource, transaction, transaction, failureMessage, work);
  }

  private static SagaDeadLetterEntry fromRow(ResultSet rs) throws SQLException {
    String sagaId = rs.getString("saga_id");
    long firstReplay = rs.getLong("first_replay_started_at");
    boolean noReplay = rs.wasNull();
    String targetSagaId = rs.getString("target_saga_id");
    return new SagaDeadLetterEntry(
        sagaId == null ? null : SagaId.of(sagaId),
        SagaType.of(rs.getString("saga_type")),
        GlobalOffset.of(rs.getLong("event_offset")),
        EventType.of(rs.getString("event_type")),
        rs.getString("error_type"),
        rs.getString("error_message"),
        SqliteTimestamps.fromMicros(rs.getLong("faulted_at")),
        SqliteTimestamps.fromMicros(rs.getLong("first_faulted_at")),
        noReplay ? null : SqliteTimestamps.fromMicros(firstReplay),
        targetSagaId == null ? null : SagaId.of(targetSagaId));
  }

  /**
   * The first-insert candidate of {@code first_faulted_at}: the entry's, else its fault instant.
   */
  private static Instant firstFaultedAt(SagaDeadLetterEntry entry) {
    return entry.firstFaultedAt() == null ? entry.faultedAt() : entry.firstFaultedAt();
  }

  private static void setInstant(PreparedStatement ps, int index, Instant instant)
      throws SQLException {
    if (instant == null) {
      ps.setNull(index, Types.INTEGER);
    } else {
      ps.setLong(index, SqliteTimestamps.toMicros(instant));
    }
  }

  private static void setSagaId(PreparedStatement ps, int index, SagaId sagaId)
      throws SQLException {
    if (sagaId == null) {
      ps.setNull(index, Types.VARCHAR);
    } else {
      ps.setString(index, sagaId.value());
    }
  }

  private static void requireNamed(SagaDeadLetterEntry entry) {
    if (entry == null) {
      throw new IllegalArgumentException("entry must not be null");
    }
    if (entry.sagaId() == null) {
      throw new IllegalArgumentException("sagaId is required");
    }
  }

  private static void requireType(SagaType sagaType) {
    if (sagaType == null) {
      throw new IllegalArgumentException("sagaType must not be null");
    }
  }

  private static void requireOffset(GlobalOffset eventOffset) {
    if (eventOffset == null) {
      throw new IllegalArgumentException("eventOffset is required");
    }
  }

  private static String forLog(SagaId sagaId) {
    return LogSanitizer.sanitizeForLog(sagaId.value());
  }
}

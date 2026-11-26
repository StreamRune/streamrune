package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * The SQLite {@link DeadLetterQueue}: failed commands in the {@code dead_letter_queue} table of a
 * {@link SqliteDatabase}, with the semantics of {@code PostgresDeadLetterQueue}. The table belongs
 * to the event-store baseline, which {@link SqliteEventStoreFactory} migrates.
 *
 * <p><b>One entry per command under a key.</b> The baseline's unique partial index {@code
 * idx_dlq_idempotency_key} covers the key, the command type and the stream (through {@code
 * ifnull(..., '')}, since SQLite has no {@code NULLS NOT DISTINCT}), and {@link #publish} inserts
 * with {@code ON CONFLICT} naming exactly that index and {@code DO NOTHING}: a keyed request for a
 * command the table already holds under the key stores nothing and returns normally, also when it
 * carries the stored entry's own {@code commandId}, and also when two publishes race, since every
 * write holds SQLite's database-wide write lock from its {@code BEGIN IMMEDIATE} to its commit. Any
 * other duplicate {@code commandId} still fails on the primary key, with an {@link
 * EventStoreException}, and leaves the stored entry unchanged.
 *
 * <p><b>One clock.</b> {@code published_at} is the application {@link Clock}'s instant, in epoch
 * microseconds, read inside the publish's write transaction once the write lock is granted (on
 * Postgres the database's {@code NOW()} stamps it). Every other timestamp, and every bound of
 * {@link #readRetryable}, {@link #countAwaitingOperator} and {@link #deleteOlderThan}, is the
 * caller's. The two clocks need not agree: when this queue's clock runs behind the clock the
 * retention cutoff is taken from, a publish can stamp a row older than a cutoff already passed to
 * {@link #deleteOlderThan}, and that sweep may delete it too. It deletes only rows older than its
 * cutoff, as promised; on Postgres a cutoff later than the database's {@code NOW()} does the same.
 *
 * <p><b>Durable on return.</b> Every write, single statements included, runs as {@code BEGIN
 * IMMEDIATE} … {@code COMMIT} through the one SQLite write helper, so it is committed when it
 * returns. A connection on which the application left a transaction open is refused rather than
 * joined (its {@code BEGIN IMMEDIATE} fails), and a connection handed out in manual-commit mode is
 * refused with an {@link IllegalStateException} before anything is written, as every SQLite store
 * refuses it. A write waits for the write lock at most the connection's {@code busy_timeout} and
 * then fails with an {@link EventStoreException} naming {@code busy_timeout}, having written
 * nothing. Reads run in autocommit and never wait for a writer.
 *
 * <p><b>No row locks.</b> {@link #readRetryable} is a plain read: on Postgres its {@code FOR UPDATE
 * SKIP LOCKED} locks last only for that one autocommitted statement and claim nothing, and SQLite
 * has no row locks; two runners that read the same entry replay it under its original key, which
 * the command inbox deduplicates, as on Postgres.
 *
 * <p><b>Retention.</b> {@link #deleteOlderThan} deletes in batches of {@value #DELETE_BATCH_SIZE}
 * rows, oldest first, each its own write transaction, until a batch deletes nothing.
 *
 * <p><b>Failures.</b> A storage failure (a lost connection, the write lock not granted in time, a
 * constraint such as a payload that is not JSON) is an {@link EventStoreException}. A {@code null}
 * argument the interface requires, and a non-positive {@code limit}, are refused with {@link
 * IllegalArgumentException} before any I/O.
 */
public final class SqliteDeadLetterQueue implements DeadLetterQueue {

  private static final String COLUMNS =
      "command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "dlq_attempts, last_attempt_at, correlation_id, user_id, trace_id, idempotency_key";

  // The conflict target is the unique partial index idx_dlq_idempotency_key, named exactly: a keyed
  // request for a command already stored under the key inserts nothing, while any other duplicate
  // command_id still fails on the primary key. A target-less ON CONFLICT DO NOTHING would also
  // swallow that duplicate.
  private static final String INSERT =
      "INSERT INTO dead_letter_queue "
          + "(command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "correlation_id, user_id, trace_id, idempotency_key) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
          + "ON CONFLICT (idempotency_key, command_type, ifnull(aggregate_type, ''), "
          + "ifnull(aggregate_id, '')) WHERE idempotency_key IS NOT NULL DO NOTHING";

  private static final String SELECT =
      "SELECT " + COLUMNS + " FROM dead_letter_queue ORDER BY published_at DESC LIMIT ?";

  // command_id is the primary key: an indexed point lookup.
  private static final String SELECT_BY_ID =
      "SELECT " + COLUMNS + " FROM dead_letter_queue WHERE command_id = ?";

  // "idempotency_key = ?" implies the partial index's "idempotency_key IS NOT NULL", so the planner
  // reads idx_dlq_idempotency_key, whose leading column the key is.
  private static final String SELECT_BY_IDEMPOTENCY_KEY =
      "SELECT "
          + COLUMNS
          + " FROM dead_letter_queue WHERE idempotency_key = ? ORDER BY published_at ASC";

  private static final String DELETE = "DELETE FROM dead_letter_queue WHERE command_id = ?";

  private static final String UPDATE_ATTEMPTS =
      "UPDATE dead_letter_queue SET dlq_attempts = ?, last_attempt_at = ? WHERE command_id = ?";

  // A NULL bound makes the keyed comparison NULL, so no keyed entry counts.
  private static final String COUNT_AWAITING_OPERATOR =
      "SELECT count(*) FROM dead_letter_queue WHERE dlq_attempts < ? "
          + "AND ((? AND idempotency_key IS NULL) "
          + "OR (idempotency_key IS NOT NULL AND first_attempt_at < ?))";

  // Least recently active first (see DeadLetterQueue#readRetryable). A plain read: no row lock
  // (Postgres: FOR UPDATE SKIP LOCKED, released when its autocommitted statement ends).
  private static final String SELECT_RETRYABLE =
      "SELECT "
          + COLUMNS
          + " FROM dead_letter_queue WHERE dlq_attempts < ? "
          + "AND (? OR idempotency_key IS NOT NULL) "
          + "AND (idempotency_key IS NULL OR ? IS NULL OR first_attempt_at >= ?) "
          + "ORDER BY coalesce(last_attempt_at, published_at) ASC LIMIT ?";

  private static final String COUNT_PENDING = "SELECT count(*) FROM dead_letter_queue";

  // One batch, by row id (Postgres: ctid), oldest first.
  private static final String DELETE_OLDER_THAN =
      "DELETE FROM dead_letter_queue WHERE rowid IN ("
          + "SELECT rowid FROM dead_letter_queue WHERE published_at < ? "
          + "ORDER BY published_at ASC LIMIT ?)";

  /** The rows one retention batch deletes at most. */
  static final int DELETE_BATCH_SIZE = 1000;

  private final DataSource dataSource;
  private final Clock clock;

  /**
   * Creates the queue over {@code database}, stamping {@code published_at} with {@link
   * Clock#systemUTC()}.
   *
   * @param database the database whose {@code dead_letter_queue} table the queue keeps
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteDeadLetterQueue(SqliteDatabase database) {
    this(database, Clock.systemUTC());
  }

  /**
   * Creates the queue over {@code database}, stamping {@code published_at} with {@code clock}.
   *
   * @param database the database whose {@code dead_letter_queue} table the queue keeps
   * @param clock the clock of {@code published_at}; read inside the publish's write transaction, so
   *     it must return at once
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public SqliteDeadLetterQueue(SqliteDatabase database, Clock clock) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (clock == null) {
      throw new IllegalArgumentException("clock is required");
    }
    this.dataSource = database.dataSource();
    this.clock = clock;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void publish(DeadLetterPublishRequest request) {
    requireArgument(request, "request");
    String failure =
        "Failed to publish to dead letter queue: "
            + LogSanitizer.sanitizeForLog(request.commandId().value());
    write(
        "dead letter publish",
        failure,
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
            ps.setString(1, request.commandId().value());
            ps.setString(2, request.commandType());
            ps.setString(3, request.commandPayload());
            SqliteStreamKeys.bind(ps, 4, request.streamId());
            ps.setString(6, request.errorType());
            ps.setString(7, request.errorMessage());
            ps.setInt(8, request.attempts());
            ps.setLong(9, SqliteTimestamps.toMicros(request.timestamp()));
            ps.setLong(10, SqliteTimestamps.toMicros(clock.instant()));
            ps.setString(
                11, request.correlationId() == null ? null : request.correlationId().value());
            ps.setString(12, request.userId() == null ? null : request.userId().value());
            ps.setString(13, request.traceId() == null ? null : request.traceId().value());
            ps.setString(
                14, request.idempotencyKey() == null ? null : request.idempotencyKey().value());
            return ps.executeUpdate(); // 0 = the keyed rule stored nothing
          }
        });
  }

  @Override
  public List<DeadLetterEntry> read(int limit) {
    requirePositiveLimit(limit);
    return query(SELECT, "Failed to read from dead letter queue", ps -> ps.setInt(1, limit));
  }

  /** Indexed point lookup by {@code command_id}, the table's primary key. */
  @Override
  public Optional<DeadLetterEntry> find(CommandId commandId) {
    requireArgument(commandId, "commandId");
    return query(
            SELECT_BY_ID,
            "Failed to find dead letter queue entry: "
                + LogSanitizer.sanitizeForLog(commandId.value()),
            ps -> ps.setString(1, commandId.value()))
        .stream()
        .findFirst();
  }

  /**
   * The entries stored under {@code key}, oldest first, read through the partial index on {@code
   * idempotency_key}.
   */
  @Override
  public List<DeadLetterEntry> findByIdempotencyKey(IdempotencyKey key) {
    requireArgument(key, "key");
    return query(
        SELECT_BY_IDEMPOTENCY_KEY,
        "Failed to find dead letter queue entries by key",
        ps -> ps.setString(1, key.value()));
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
  public void discard(CommandId commandId) {
    requireArgument(commandId, "commandId");
    write(
        "dead letter discard",
        "Failed to discard from dead letter queue: "
            + LogSanitizer.sanitizeForLog(commandId.value()),
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(DELETE)) {
            ps.setString(1, commandId.value());
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
  public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
    requireArgument(commandId, "commandId");
    requireArgument(lastAttemptAt, "lastAttemptAt");
    long lastAttemptMicros = SqliteTimestamps.toMicros(lastAttemptAt);
    write(
        "dead letter attempts update",
        "Failed to update DLQ attempts: " + LogSanitizer.sanitizeForLog(commandId.value()),
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(UPDATE_ATTEMPTS)) {
            ps.setInt(1, dlqAttempts);
            ps.setLong(2, lastAttemptMicros);
            ps.setString(3, commandId.value());
            return ps.executeUpdate();
          }
        });
  }

  @Override
  public List<DeadLetterEntry> readRetryable(
      int maxRetries, int limit, boolean includeUnkeyed, Instant keyedFirstAttemptAtOrAfter) {
    requirePositiveLimit(limit);
    return query(
        SELECT_RETRYABLE,
        "Failed to read retryable DLQ entries",
        ps -> {
          ps.setInt(1, maxRetries);
          ps.setBoolean(2, includeUnkeyed);
          setInstant(ps, 3, keyedFirstAttemptAtOrAfter);
          setInstant(ps, 4, keyedFirstAttemptAtOrAfter);
          ps.setInt(5, limit);
        });
  }

  @Override
  public long countPending() {
    return count(COUNT_PENDING, "Failed to count dead letter queue entries", ps -> {});
  }

  @Override
  public long countAwaitingOperator(
      int maxRetries, boolean countUnkeyed, Instant keyedFirstAttemptBefore) {
    return count(
        COUNT_AWAITING_OPERATOR,
        "Failed to count dead letter entries awaiting an operator",
        ps -> {
          ps.setInt(1, maxRetries);
          ps.setBoolean(2, countUnkeyed);
          setInstant(ps, 3, keyedFirstAttemptBefore);
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Batches of {@value #DELETE_BATCH_SIZE} rows, oldest first, each its own write transaction,
   * until a batch deletes nothing.
   *
   * @throws EventStoreException if a batch fails, including when the write lock is not granted
   *     within {@code busy_timeout}; the batches before it stay deleted
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public int deleteOlderThan(Instant cutoff) {
    requireArgument(cutoff, "cutoff");
    long cutoffMicros = SqliteTimestamps.toMicros(cutoff);
    int total = 0;
    int batch;
    do {
      batch =
          write(
              "dead letter retention",
              "Failed to delete old dead letter queue entries",
              conn -> {
                try (PreparedStatement ps = conn.prepareStatement(DELETE_OLDER_THAN)) {
                  ps.setLong(1, cutoffMicros);
                  ps.setInt(2, DELETE_BATCH_SIZE);
                  return ps.executeUpdate();
                }
              });
      total += batch;
      // Until a batch deletes nothing, as on Postgres. Each batch deletes rows older than the
      // cutoff; a publish whose clock runs behind the cutoff's can add such rows meanwhile, so the
      // loop ends once deleting outruns those publishes (not because the set can only shrink).
    } while (batch > 0);
    return total;
  }

  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  private List<DeadLetterEntry> query(String sql, String failureMessage, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      List<DeadLetterEntry> entries = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          entries.add(readEntry(rs));
        }
      }
      return entries;
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
  }

  private long count(String sql, String failureMessage, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(failureMessage, e);
    }
  }

  /** Runs {@code work} as one write transaction, through {@link SqliteTransactions#write}. */
  private <T> T write(String transaction, String failureMessage, SqliteTransactions.Work<T> work) {
    return SqliteTransactions.write(dataSource, transaction, transaction, failureMessage, work);
  }

  private static void setInstant(PreparedStatement ps, int index, Instant instant)
      throws SQLException {
    if (instant == null) {
      ps.setNull(index, Types.INTEGER);
    } else {
      ps.setLong(index, SqliteTimestamps.toMicros(instant));
    }
  }

  private static void requireArgument(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " is required");
    }
  }

  private static void requirePositiveLimit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be > 0");
    }
  }

  private static DeadLetterEntry readEntry(ResultSet rs) throws SQLException {
    long lastAttempt = rs.getLong("last_attempt_at");
    boolean neverRetried = rs.wasNull();
    String correlationId = rs.getString("correlation_id");
    String userId = rs.getString("user_id");
    String traceId = rs.getString("trace_id");
    String idempotencyKey = rs.getString("idempotency_key");
    return new DeadLetterEntry(
        CommandId.of(rs.getString("command_id")),
        rs.getString("command_type"),
        rs.getString("payload"),
        // The decode door: a stored stream must read back whatever ingress rules say today.
        SqliteStreamKeys.read(rs),
        rs.getString("error_type"),
        rs.getString("error_message"),
        rs.getInt("attempts"),
        SqliteTimestamps.fromMicros(rs.getLong("first_attempt_at")),
        SqliteTimestamps.fromMicros(rs.getLong("published_at")),
        rs.getInt("dlq_attempts"),
        neverRetried ? null : SqliteTimestamps.fromMicros(lastAttempt),
        correlationId == null ? null : CorrelationId.of(correlationId),
        userId == null ? null : UserId.of(userId),
        traceId == null ? null : TraceId.of(traceId),
        // The decode door (the canonical constructor), never IdempotencyKey.of: a stored key that
        // carries a control character must still read back.
        idempotencyKey == null ? null : new IdempotencyKey(idempotencyKey));
  }
}

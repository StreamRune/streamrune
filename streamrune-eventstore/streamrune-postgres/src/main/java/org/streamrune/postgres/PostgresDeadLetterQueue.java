package org.streamrune.postgres;

import java.sql.*;
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
 * PostgreSQL-backed {@link DeadLetterQueue}. Stores failed commands in a {@code dead_letter_queue}
 * table for later inspection, replay, or manual resolution.
 *
 * <p>Schema — created by the framework's baseline Flyway migration ({@code
 * V001__streamrune_baseline.sql}); this class never creates it:
 *
 * <pre>
 * CREATE TABLE dead_letter_queue (
 *     command_id       VARCHAR(255) PRIMARY KEY,
 *     command_type     VARCHAR(255) NOT NULL,
 *     payload          JSONB       NOT NULL,
 *     aggregate_type   VARCHAR(32),
 *     aggregate_id     VARCHAR(255),
 *     error_type       VARCHAR(255) NOT NULL,
 *     error_message    TEXT,
 *     attempts         INT         NOT NULL,
 *     first_attempt_at TIMESTAMPTZ NOT NULL,
 *     published_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 *     dlq_attempts     INT         NOT NULL DEFAULT 0,
 *     last_attempt_at  TIMESTAMPTZ,
 *     correlation_id   VARCHAR(255),
 *     user_id          VARCHAR(255),
 *     trace_id         VARCHAR(255),
 *     idempotency_key  VARCHAR(512),
 *     CONSTRAINT dead_letter_queue_stream_both_or_neither
 *         CHECK ((aggregate_type IS NULL) = (aggregate_id IS NULL)),
 *     CONSTRAINT dead_letter_queue_aggregate_type_syntax
 *         CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
 * );
 * CREATE INDEX idx_dlq_published_at ON dead_letter_queue (published_at DESC);
 * </pre>
 *
 * <p>The {@code correlation_id}/{@code user_id}/{@code trace_id} columns persist the originating
 * request context so {@link DeadLetterQueue.DeadLetterEntry} can rebind it on replay (events keep
 * their correlation, fail-closed authorization re-evaluates as the original user). All three are
 * nullable: commands that ran with no context bound store null and replay unbound.
 *
 * <p>The {@code idempotency_key} column (nullable) persists the caller-supplied {@code
 * IdempotencyKey} the command originally ran under, so the retry runner replays under the ORIGINAL
 * key rather than a synthetic {@code dlq-replay:<commandId>} key — reconciling a cross-instance
 * duplicate replay AND the client's at-least-once redelivery against the same command inbox row
 * (events produced exactly once). Commands that ran unkeyed store null and are replayed under the
 * synthetic key.
 */
public final class PostgresDeadLetterQueue implements DeadLetterQueue {

  private static final String INSERT =
      "INSERT INTO dead_letter_queue "
          + "(command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "correlation_id, user_id, trace_id, idempotency_key) "
          + "VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?)";

  private static final String SELECT =
      "SELECT command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "dlq_attempts, last_attempt_at, correlation_id, user_id, trace_id, idempotency_key "
          + "FROM dead_letter_queue ORDER BY published_at DESC LIMIT ?";

  // command_id is the table's PRIMARY KEY, so this is an indexed point lookup — unlike the
  // DeadLetterQueue.find(CommandId) default, which scans read(Integer.MAX_VALUE) and would
  // materialize the entire DLQ (every payload included) into heap to find one row.
  private static final String SELECT_BY_ID =
      "SELECT command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "dlq_attempts, last_attempt_at, correlation_id, user_id, trace_id, idempotency_key "
          + "FROM dead_letter_queue WHERE command_id = ?";

  private static final String DELETE = "DELETE FROM dead_letter_queue WHERE command_id = ?";

  private static final String UPDATE_ATTEMPTS =
      "UPDATE dead_letter_queue SET dlq_attempts = ?, last_attempt_at = ? "
          + "WHERE command_id = ?";

  // FOR UPDATE SKIP LOCKED: two retry schedulers polling at the same moment do not pick up
  // the same rows (the second poller skips rows the first one has row-locked).
  //
  // ORDER BY the least-recently-active timestamp — COALESCE(last_attempt_at,
  // published_at) — ascending, NOT published_at. Due-ness (per-attempt exponential backoff) is
  // evaluated by DeadLetterRetryRunner in Java against these DB-stamped timestamps, and the runner
  // over-fetches this page. Ordering by last activity puts the entries most likely to be past their
  // backoff (least recently touched) first, so a recently-retried, still-backing-off entry cannot
  // sit at the head of the page and starve genuinely-due entries behind it. Never-attempted entries
  // (last_attempt_at IS NULL) fall back to published_at, preserving oldest-first among fresh
  // entries.
  private static final String SELECT_RETRYABLE =
      "SELECT command_id, command_type, payload, aggregate_type, aggregate_id, "
          + "error_type, error_message, attempts, first_attempt_at, published_at, "
          + "dlq_attempts, last_attempt_at, correlation_id, user_id, trace_id, idempotency_key "
          + "FROM dead_letter_queue WHERE dlq_attempts < ? "
          + "ORDER BY COALESCE(last_attempt_at, published_at) ASC LIMIT ? "
          + "FOR UPDATE SKIP LOCKED";

  // Batched ctid-based delete, mirroring PostgresSagaDeadLetterStore.deleteOlderThan /
  // PostgresCommandInbox.deleteProcessedBefore: repeatedly deletes up to DELETE_BATCH_SIZE
  // strictly-older rows until a chunk deletes nothing (see deleteOlderThan for why not "until a
  // chunk deletes fewer than DELETE_BATCH_SIZE").
  private static final String DELETE_OLDER_THAN =
      "DELETE FROM dead_letter_queue WHERE ctid IN ("
          + "SELECT ctid FROM dead_letter_queue WHERE published_at < ? "
          + "ORDER BY published_at ASC LIMIT ?)";

  private static final int DELETE_BATCH_SIZE = 1000;

  private final DataSource dataSource;

  /**
   * Creates a queue over {@code dataSource}. The command payload arrives already serialized in
   * {@link DeadLetterPublishRequest#commandPayload()} and is stored as given, so the queue needs no
   * {@code ObjectMapper}.
   *
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   */
  public PostgresDeadLetterQueue(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  @Override
  public void publish(DeadLetterPublishRequest request) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(INSERT)) {
      ps.setString(1, request.commandId().value());
      ps.setString(2, request.commandType());
      ps.setString(3, request.commandPayload());
      PostgresStreamKeys.bind(ps, 4, request.streamId());
      ps.setString(6, request.errorType());
      ps.setString(7, request.errorMessage());
      ps.setInt(8, request.attempts());
      ps.setTimestamp(9, Timestamp.from(request.timestamp()));
      ps.setString(10, request.correlationId() != null ? request.correlationId().value() : null);
      ps.setString(11, request.userId() != null ? request.userId().value() : null);
      ps.setString(12, request.traceId() != null ? request.traceId().value() : null);
      ps.setString(13, request.idempotencyKey() != null ? request.idempotencyKey().value() : null);
      PostgresTransactions.commitIfManual(conn, "dead letter publish", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to publish to dead letter queue: "
              + LogSanitizer.sanitizeForLog(request.commandId().value()),
          e);
    }
  }

  @Override
  public List<DeadLetterEntry> read(int limit) {
    List<DeadLetterEntry> results = new ArrayList<>();
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT)) {
      ps.setInt(1, limit);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          results.add(readEntry(rs));
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read from dead letter queue", e);
    }
    return results;
  }

  /**
   * Indexed point lookup by {@code command_id} (the table's primary key) — overrides the {@link
   * DeadLetterQueue#find(CommandId)} default, which scans {@code read(Integer.MAX_VALUE)} and would
   * otherwise materialize the entire DLQ (every payload included) into heap just to find one row.
   */
  @Override
  public Optional<DeadLetterEntry> find(CommandId commandId) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_BY_ID)) {
      ps.setString(1, commandId.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(readEntry(rs)) : Optional.empty();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find dead letter queue entry: "
              + LogSanitizer.sanitizeForLog(commandId.value()),
          e);
    }
  }

  @Override
  public void discard(CommandId commandId) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(DELETE)) {
      ps.setString(1, commandId.value());
      PostgresTransactions.commitIfManual(conn, "dead letter discard", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to discard from dead letter queue: "
              + LogSanitizer.sanitizeForLog(commandId.value()),
          e);
    }
  }

  @Override
  public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(UPDATE_ATTEMPTS)) {
      ps.setInt(1, dlqAttempts);
      ps.setTimestamp(2, Timestamp.from(lastAttemptAt));
      ps.setString(3, commandId.value());
      PostgresTransactions.commitIfManual(
          conn, "dead letter attempts update", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to update DLQ attempts: " + LogSanitizer.sanitizeForLog(commandId.value()), e);
    }
  }

  @Override
  public List<DeadLetterEntry> readRetryable(int maxRetries, int limit) {
    List<DeadLetterEntry> results = new ArrayList<>();
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(SELECT_RETRYABLE)) {
      ps.setInt(1, maxRetries);
      ps.setInt(2, limit);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          results.add(readEntry(rs));
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to read retryable DLQ entries", e);
    }
    return results;
  }

  @Override
  public long countPending() {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT COUNT(*) FROM dead_letter_queue");
        var rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    } catch (SQLException e) {
      throw new EventStoreException("Failed to count dead letter queue entries", e);
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
                conn, "dead letter retention", c -> ps.executeUpdate());
        total += batch;
      } catch (SQLException e) {
        throw new EventStoreException("Failed to delete old dead letter queue entries", e);
      }
      // Loop while ANY row was deleted, not only on a full DELETE_BATCH_SIZE chunk (the same
      // termination as PostgresOutboxStore.deleteDelivered/deleteSkipped). The outer DELETE matches
      // by ctid, so a candidate the retry runner's updateAttempts rewrites inside the DELETE window
      // has moved to a new ctid and EvalPlanQual skips it, as it skips one a concurrent discard or
      // a second instance's sweeper removed. The chunk then deletes fewer than LIMIT although aged
      // rows remain — the runner works the least-recently-active entries, the very rows this sweep
      // selects first — and a `batch == DELETE_BATCH_SIZE` termination exited with them left
      // behind. `batch > 0` terminates: published_at is stamped NOW() at publish, so no new row is
      // older than the past cutoff, and each non-empty chunk strictly shrinks that finite set.
    } while (batch > 0);
    return total;
  }

  private DeadLetterEntry readEntry(ResultSet rs) throws SQLException {
    Instant firstAttemptAt = rs.getTimestamp("first_attempt_at").toInstant();
    Instant publishedAt = rs.getTimestamp("published_at").toInstant();
    int dlqAttempts = rs.getInt("dlq_attempts");
    Timestamp lastAttemptTs = rs.getTimestamp("last_attempt_at");
    Instant lastAttemptAt = lastAttemptTs != null ? lastAttemptTs.toInstant() : null;
    String correlationIdStr = rs.getString("correlation_id");
    String userIdStr = rs.getString("user_id");
    String traceIdStr = rs.getString("trace_id");
    String idempotencyKeyStr = rs.getString("idempotency_key");

    return new DeadLetterEntry(
        CommandId.of(rs.getString("command_id")),
        rs.getString("command_type"),
        rs.getString("payload"),
        // The decode door — the lenient constructors, never the ingress factories: a stored
        // stream carrying a control character must still read back, or this row would throw
        // inside every read page that reached it.
        PostgresStreamKeys.read(rs),
        rs.getString("error_type"),
        rs.getString("error_message"),
        rs.getInt("attempts"),
        firstAttemptAt,
        publishedAt,
        dlqAttempts,
        lastAttemptAt,
        correlationIdStr != null ? CorrelationId.of(correlationIdStr) : null,
        userIdStr != null ? UserId.of(userIdStr) : null,
        traceIdStr != null ? TraceId.of(traceIdStr) : null,
        // The DECODE door (the canonical constructor), never IdempotencyKey.of — the ingress
        // factory refuses control characters, and a stored key that carries one must read back
        // verbatim instead of throwing inside every page that reaches this row.
        idempotencyKeyStr != null ? new IdempotencyKey(idempotencyKeyStr) : null);
  }
}

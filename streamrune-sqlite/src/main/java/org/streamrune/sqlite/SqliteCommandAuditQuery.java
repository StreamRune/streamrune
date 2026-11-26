package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * The SQLite {@link CommandAuditQuery} over the {@code audit_log} table {@link SqliteAuditStore}
 * writes, with the semantics of {@code PostgresCommandAuditQuery}.
 *
 * <p>{@link #findByCommandId} answers the most recent entry of the command (the log keeps every
 * attempt of a retry); of two entries with an equal timestamp, the one written later. The paged
 * finders page in the database, with {@code LIMIT}/{@code OFFSET}; the page and {@link
 * Page#totalElements()} come from one statement, so from one snapshot of the file, and they agree.
 * The order is {@code occurred_at} ascending, then the row's {@code id}, so entries with an equal
 * timestamp keep one position across pages, and each entry appears once across the pages of a
 * window that receives no entry between the reads; the {@link PageRequest#sort()} component is
 * ignored. The window bounds are inclusive and compare to the microsecond; a window whose {@code
 * from} is after its {@code to} holds nothing.
 *
 * <p>Queries run in autocommit and, under WAL, never wait for a writer.
 *
 * <p><b>Failures.</b> A storage failure, and a stored row the entry types refuse, is an {@link
 * EventStoreException}; a {@code null} argument is refused with {@link NullPointerException}, as
 * the interface says.
 */
public final class SqliteCommandAuditQuery implements CommandAuditQuery {

  private static final String COLUMNS =
      "command_id, command_type, aggregate_type, aggregate_id, user_id, occurred_at, outcome,"
          + " error_message, event_count, correlation_id";

  private static final String FIND_BY_COMMAND =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE command_id = ? ORDER BY occurred_at DESC, id DESC LIMIT 1";

  static final String FIND_BY_USER = pageSql("user_id");

  static final String FIND_BY_OUTCOME = pageSql("outcome");

  static final String FIND_BY_TYPE = pageSql("command_type");

  /**
   * The statement of a paged read: the count of the matching rows and one page of them, joined into
   * one result, so both come from one snapshot. Every row carries the count; a page beyond the last
   * is one row with the count and a {@code NULL} {@code id} ({@code id} is the primary key, so
   * {@code NULL} means "no row"). The page's {@code LIMIT} is 0 unless the count exceeds the
   * offset, so a page beyond the last skips no rows to find none (Postgres gates its page the same
   * way, with a {@code LATERAL} join, which SQLite does not have). The order is {@code
   * occurred_at}, then {@code id}; the outer {@code ORDER BY} restores it after the join.
   * Parameters: the filter value, {@code from}, {@code to} (the count), the same three again (the
   * page), the offset (the gate), {@code LIMIT}, {@code OFFSET}.
   */
  private static String pageSql(String filterColumn) {
    String from =
        " FROM audit_log WHERE " + filterColumn + " = ? AND occurred_at >= ? AND occurred_at <= ?";
    return "WITH total(n) AS MATERIALIZED (SELECT count(*)"
        + from
        + ") SELECT total.n AS total_count, page.* FROM total LEFT JOIN (SELECT id, "
        + COLUMNS
        + from
        + " ORDER BY occurred_at, id"
        + " LIMIT CASE WHEN (SELECT n FROM total) > ? THEN ? ELSE 0 END OFFSET ?) AS page ON 1"
        + " ORDER BY page.occurred_at, page.id";
  }

  private static final String FAILED = "Failed to query command audit log";

  private final DataSource dataSource;

  /**
   * Creates the query over {@code database}.
   *
   * @param database the database whose {@code audit_log} table the query reads
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteCommandAuditQuery(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
  }

  @Override
  public Optional<AuditEntry> findByCommandId(CommandId commandId) {
    Objects.requireNonNull(commandId, "commandId is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_BY_COMMAND)) {
      ps.setString(1, commandId.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(readEntry(rs)) : Optional.empty();
      }
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  @Override
  public Page<AuditEntry> findByUserId(
      UserId userId, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(userId, "userId is required");
    return queryPage(FIND_BY_USER, userId.value(), from, to, pageRequest);
  }

  @Override
  public Page<AuditEntry> findByOutcome(
      AuditOutcome outcome, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(outcome, "outcome is required");
    return queryPage(FIND_BY_OUTCOME, outcome.name(), from, to, pageRequest);
  }

  @Override
  public Page<AuditEntry> findByCommandType(
      String commandType, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(commandType, "commandType is required");
    return queryPage(FIND_BY_TYPE, commandType, from, to, pageRequest);
  }

  private Page<AuditEntry> queryPage(
      String sql, String filter, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    long fromMicros = SqliteTimestamps.toMicros(from);
    long toMicros = SqliteTimestamps.toMicros(to);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, filter);
      ps.setLong(2, fromMicros);
      ps.setLong(3, toMicros);
      ps.setString(4, filter);
      ps.setLong(5, fromMicros);
      ps.setLong(6, toMicros);
      ps.setLong(7, pageRequest.offset());
      ps.setInt(8, pageRequest.size());
      ps.setLong(9, pageRequest.offset());
      long total = 0;
      List<AuditEntry> content = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          total = rs.getLong("total_count");
          rs.getLong("id");
          if (!rs.wasNull()) {
            content.add(readEntry(rs));
          }
        }
      }
      return new Page<>(content, total, pageRequest.page(), pageRequest.size());
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  private static AuditEntry readEntry(ResultSet rs) throws SQLException {
    String commandId = rs.getString("command_id");
    String correlationId = rs.getString("correlation_id");
    String aggregateType = rs.getString("aggregate_type");
    String aggregateId = rs.getString("aggregate_id");
    String userId = rs.getString("user_id");
    return new AuditEntry(
        commandId != null ? CommandId.of(commandId) : null,
        rs.getString("command_type"),
        // Query-origin entries have no aggregate (both NULL); subject entries carry a subject hash
        // and no type. The decode door, the lenient constructors: a stored value carrying a
        // control character still reads back.
        aggregateType != null ? new AggregateType(aggregateType) : null,
        aggregateId != null ? new AggregateId(aggregateId) : null,
        userId != null ? UserId.of(userId) : null,
        SqliteTimestamps.fromMicros(rs.getLong("occurred_at")),
        AuditOutcome.valueOf(rs.getString("outcome")),
        rs.getString("error_message"),
        rs.getInt("event_count"),
        correlationId != null ? CorrelationId.of(correlationId) : null);
  }
}

package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
 * PostgreSQL implementation of {@link CommandAuditQuery} over the {@code audit_log} table written
 * by {@link PostgresAuditStore}. Mirrors {@link PostgresEventAuditQuery} in style.
 *
 * <p>{@link #findByCommandId(CommandId)} returns the most recent matching entry (the audit log is
 * append-only and permits duplicate {@code command_id} values on retry). The paged finders do
 * store-side paging with SQL {@code LIMIT}/{@code OFFSET}; the page and {@link
 * Page#totalElements()} come from one statement, so they agree. The order is {@code occurred_at}
 * ascending, then the row's {@code id}, so entries with an equal timestamp keep one position across
 * pages; the {@link PageRequest#sort()} component is ignored.
 */
public final class PostgresCommandAuditQuery implements CommandAuditQuery {

  private static final String COLUMNS =
      "command_id, command_type, aggregate_type, aggregate_id, user_id, occurred_at, outcome,"
          + " error_message, event_count, correlation_id";

  private static final String FIND_BY_COMMAND =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE command_id = ? ORDER BY occurred_at DESC LIMIT 1";

  private static final String FIND_BY_USER = pageSql("user_id");

  private static final String FIND_BY_OUTCOME = pageSql("outcome");

  private static final String FIND_BY_TYPE = pageSql("command_type");

  /**
   * The statement of a paged read: the count of the matching rows and one page of them, as two
   * subqueries joined into one result, so both come from one snapshot. Every row carries the count;
   * a page beyond the last is one row with the count and a {@code NULL} {@code id} ({@code id} is
   * the primary key, so {@code NULL} means "no row"). The page is gated on the count ({@code
   * LATERAL ... WHERE total.n > offset}), so a page beyond the last reads only the count. The order
   * is {@code occurred_at}, then {@code id}, so rows with an equal timestamp keep one position
   * across pages; the outer {@code ORDER BY} restores it after the join. Parameters: the filter
   * value, {@code from}, {@code to} (the count), the same three again and the offset (the page and
   * its gate), {@code LIMIT}, {@code OFFSET}.
   */
  private static String pageSql(String filterColumn) {
    String where = " WHERE " + filterColumn + " = ? AND occurred_at >= ? AND occurred_at <= ?";
    return "SELECT total.n AS total_count, page.* FROM (SELECT count(*) AS n FROM audit_log"
        + where
        + ") AS total LEFT JOIN LATERAL (SELECT id, "
        + COLUMNS
        + " FROM audit_log"
        + where
        + " AND total.n > ? ORDER BY occurred_at, id LIMIT ? OFFSET ?) AS page ON true"
        + " ORDER BY page.occurred_at, page.id";
  }

  private final DataSource dataSource;

  public PostgresCommandAuditQuery(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    this.dataSource = dataSource;
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
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query command audit log", e);
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
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      Timestamp fromTs = Timestamp.from(from);
      Timestamp toTs = Timestamp.from(to);
      ps.setString(1, filter);
      ps.setTimestamp(2, fromTs);
      ps.setTimestamp(3, toTs);
      ps.setString(4, filter);
      ps.setTimestamp(5, fromTs);
      ps.setTimestamp(6, toTs);
      ps.setLong(7, pageRequest.offset());
      ps.setInt(8, pageRequest.size());
      ps.setLong(9, pageRequest.offset());
      long total = 0;
      List<AuditEntry> content = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          total = rs.getLong("total_count");
          if (rs.getObject("id") != null) {
            content.add(readEntry(rs));
          }
        }
      }
      return new Page<>(content, total, pageRequest.page(), pageRequest.size());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query command audit log", e);
    }
  }

  private AuditEntry readEntry(ResultSet rs) throws SQLException {
    String commandId = rs.getString("command_id");
    String correlationId = rs.getString("correlation_id");
    String aggregateTypeStr = rs.getString("aggregate_type");
    String aggregateIdStr = rs.getString("aggregate_id");
    String userIdStr = rs.getString("user_id");
    return new AuditEntry(
        commandId != null ? CommandId.of(commandId) : null,
        rs.getString("command_type"),
        // Query-origin entries have no aggregate (both NULL); subject entries carry a subject hash
        // and no type. The decode door — the lenient constructors, never the ingress factories —
        // so a stored value carrying a control character still reads back.
        aggregateTypeStr != null ? new AggregateType(aggregateTypeStr) : null,
        aggregateIdStr != null ? new AggregateId(aggregateIdStr) : null,
        userIdStr != null ? UserId.of(userIdStr) : null,
        rs.getTimestamp("occurred_at").toInstant(),
        AuditOutcome.valueOf(rs.getString("outcome")),
        rs.getString("error_message"),
        rs.getInt("event_count"),
        correlationId != null ? CorrelationId.of(correlationId) : null);
  }
}

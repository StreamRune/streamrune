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
 * store-side paging with SQL {@code LIMIT}/{@code OFFSET} and a separate {@code COUNT(*)} for
 * {@link Page#totalElements()}, ordered by {@code occurred_at} ascending; the {@link
 * PageRequest#sort()} component is ignored.
 */
public final class PostgresCommandAuditQuery implements CommandAuditQuery {

  private static final String COLUMNS =
      "command_id, command_type, aggregate_type, aggregate_id, user_id, occurred_at, outcome,"
          + " error_message, event_count, correlation_id";

  private static final String FIND_BY_COMMAND =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE command_id = ? ORDER BY occurred_at DESC LIMIT 1";

  private static final String FIND_BY_USER =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE user_id = ? "
          + "AND occurred_at >= ? AND occurred_at <= ? ORDER BY occurred_at LIMIT ? OFFSET ?";

  private static final String COUNT_BY_USER =
      "SELECT COUNT(*) FROM audit_log WHERE user_id = ? AND occurred_at >= ? AND occurred_at <= ?";

  private static final String FIND_BY_OUTCOME =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE outcome = ? "
          + "AND occurred_at >= ? AND occurred_at <= ? ORDER BY occurred_at LIMIT ? OFFSET ?";

  private static final String COUNT_BY_OUTCOME =
      "SELECT COUNT(*) FROM audit_log WHERE outcome = ? AND occurred_at >= ? AND occurred_at <= ?";

  private static final String FIND_BY_TYPE =
      "SELECT "
          + COLUMNS
          + " FROM audit_log WHERE command_type = ? "
          + "AND occurred_at >= ? AND occurred_at <= ? ORDER BY occurred_at LIMIT ? OFFSET ?";

  private static final String COUNT_BY_TYPE =
      "SELECT COUNT(*) FROM audit_log WHERE command_type = ? "
          + "AND occurred_at >= ? AND occurred_at <= ?";

  private final DataSource dataSource;

  public PostgresCommandAuditQuery(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
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
    return queryPage(FIND_BY_USER, COUNT_BY_USER, userId.value(), from, to, pageRequest);
  }

  @Override
  public Page<AuditEntry> findByOutcome(
      AuditOutcome outcome, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(outcome, "outcome is required");
    return queryPage(FIND_BY_OUTCOME, COUNT_BY_OUTCOME, outcome.name(), from, to, pageRequest);
  }

  @Override
  public Page<AuditEntry> findByCommandType(
      String commandType, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(commandType, "commandType is required");
    return queryPage(FIND_BY_TYPE, COUNT_BY_TYPE, commandType, from, to, pageRequest);
  }

  private Page<AuditEntry> queryPage(
      String selectSql,
      String countSql,
      String filter,
      Instant from,
      Instant to,
      PageRequest pageRequest) {
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    try (Connection conn = dataSource.getConnection()) {
      long total = count(conn, countSql, filter, from, to);
      List<AuditEntry> content = selectPage(conn, selectSql, filter, from, to, pageRequest);
      return new Page<>(content, total, pageRequest.page(), pageRequest.size());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query command audit log", e);
    }
  }

  private long count(Connection conn, String sql, String filter, Instant from, Instant to)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, filter);
      ps.setTimestamp(2, Timestamp.from(from));
      ps.setTimestamp(3, Timestamp.from(to));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  private List<AuditEntry> selectPage(
      Connection conn, String sql, String filter, Instant from, Instant to, PageRequest pageRequest)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, filter);
      ps.setTimestamp(2, Timestamp.from(from));
      ps.setTimestamp(3, Timestamp.from(to));
      ps.setInt(4, pageRequest.size());
      ps.setInt(5, pageRequest.offset());
      return readEntries(ps);
    }
  }

  private List<AuditEntry> readEntries(PreparedStatement ps) throws SQLException {
    List<AuditEntry> entries = new ArrayList<>();
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        entries.add(readEntry(rs));
      }
    }
    return entries;
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

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
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.types.*;

/**
 * PostgreSQL implementation of {@link EventAuditQuery}. Uses WITH RECURSIVE CTE for {@link
 * #traceFullCausationChain(EventId)}.
 */
public final class PostgresEventAuditQuery implements EventAuditQuery {

  private static final String FIND_BY_CORRELATION =
      "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, "
          + "causation_id, user_id, occurred_at "
          + "FROM event_audit_log WHERE correlation_id = ? ORDER BY occurred_at";

  private static final String FIND_BY_COMMAND =
      "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, "
          + "causation_id, user_id, occurred_at "
          + "FROM event_audit_log WHERE command_id = ? ORDER BY occurred_at";

  private static final String FIND_BY_CAUSATION =
      "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, "
          + "causation_id, user_id, occurred_at "
          + "FROM event_audit_log WHERE causation_id = ? ORDER BY occurred_at";

  private static final String FIND_BY_STREAM =
      "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, "
          + "causation_id, user_id, occurred_at "
          + "FROM event_audit_log WHERE aggregate_type = ? AND aggregate_id = ?"
          + " AND occurred_at >= ? AND occurred_at <= ? ORDER BY occurred_at";

  private static final String FIND_BY_USER =
      "SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, "
          + "causation_id, user_id, occurred_at "
          + "FROM event_audit_log WHERE user_id = ? AND occurred_at >= ? AND occurred_at <= ? "
          + "ORDER BY occurred_at";

  // UNION (not UNION ALL) is the cycle guard: a causation cycle (self-reference or A->B->A,
  // possible only through data corruption) re-derives an already-seen row, UNION discards the
  // duplicate, the working set empties, and recursion terminates instead of spinning forever.
  // Legitimate chains are never truncated — recursion is bounded by the distinct rows reachable.
  private static final String TRACE_CHAIN =
      "WITH RECURSIVE chain AS ("
          + "  SELECT event_id, event_type, aggregate_type, aggregate_id, version, command_id,"
          + "         correlation_id, "
          + "         causation_id, user_id, occurred_at "
          + "  FROM event_audit_log WHERE event_id = ? "
          + "  UNION "
          + "  SELECT e.event_id, e.event_type, e.aggregate_type, e.aggregate_id, e.version,"
          + "         e.command_id, "
          + "         e.correlation_id, e.causation_id, e.user_id, e.occurred_at "
          + "  FROM event_audit_log e "
          + "  JOIN chain c ON e.causation_id = c.event_id"
          + ") SELECT * FROM chain ORDER BY occurred_at";

  private final DataSource dataSource;

  public PostgresEventAuditQuery(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  @Override
  public List<EventAuditEntry> findByCorrelationId(CorrelationId correlationId) {
    return queryWithSingleParam(FIND_BY_CORRELATION, correlationId.value());
  }

  @Override
  public List<EventAuditEntry> findByCommandId(CommandId commandId) {
    return queryWithSingleParam(FIND_BY_COMMAND, commandId.value());
  }

  @Override
  public List<EventAuditEntry> findByCausationId(CausationId causationId) {
    return queryWithSingleParam(FIND_BY_CAUSATION, causationId.value());
  }

  @Override
  public List<EventAuditEntry> findByStreamId(StreamId streamId, Instant from, Instant to) {
    Objects.requireNonNull(streamId, "streamId is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(FIND_BY_STREAM)) {
      PostgresStreamKeys.bind(ps, 1, streamId);
      ps.setTimestamp(3, Timestamp.from(from));
      ps.setTimestamp(4, Timestamp.from(to));
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query event audit log", e);
    }
  }

  @Override
  public List<EventAuditEntry> findByUserId(UserId userId, Instant from, Instant to) {
    return queryWithTimeRange(FIND_BY_USER, userId.value(), from, to);
  }

  @Override
  public List<EventAuditEntry> traceFullCausationChain(EventId eventId) {
    return queryWithSingleParam(TRACE_CHAIN, eventId.value());
  }

  private List<EventAuditEntry> queryWithSingleParam(String sql, String param) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, param);
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query event audit log", e);
    }
  }

  private List<EventAuditEntry> queryWithTimeRange(
      String sql, String param, Instant from, Instant to) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, param);
      ps.setTimestamp(2, Timestamp.from(from));
      ps.setTimestamp(3, Timestamp.from(to));
      return readEntries(ps);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query event audit log", e);
    }
  }

  private List<EventAuditEntry> readEntries(PreparedStatement ps) throws SQLException {
    List<EventAuditEntry> entries = new ArrayList<>();
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        entries.add(readEntry(rs));
      }
    } catch (IllegalArgumentException e) {
      // A hand-written row the id types refuse on decode (an empty id passes NOT NULL and the
      // column length): surface it as a store failure, not a raw argument error.
      throw new EventStoreException("Failed to decode an event audit log row", e);
    }
    return entries;
  }

  private EventAuditEntry readEntry(ResultSet rs) throws SQLException {
    String causationId = rs.getString("causation_id");
    String userId = rs.getString("user_id");
    return new EventAuditEntry(
        EventId.of(rs.getString("event_id")),
        new EventType(rs.getString("event_type")),
        PostgresStreamKeys.read(rs),
        new Version(rs.getLong("version")),
        CommandId.of(rs.getString("command_id")),
        CorrelationId.of(rs.getString("correlation_id")),
        causationId != null ? CausationId.of(causationId) : null,
        userId != null ? UserId.of(userId) : null,
        rs.getTimestamp("occurred_at").toInstant());
  }
}

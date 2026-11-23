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
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.types.*;

/**
 * PostgreSQL implementation of {@link EventAuditQuery}. Uses WITH RECURSIVE CTE for {@link
 * #traceFullCausationChain(EventId)}.
 *
 * <p>Every read orders by {@code occurred_at}, then the row's {@code id}, so entries with an equal
 * timestamp (the events of one command are stamped by consecutive clock reads) keep one position.
 * The paged finders do store-side paging with SQL {@code LIMIT}/{@code OFFSET}; the page and {@link
 * Page#totalElements()} come from one statement, so they agree, and a page beyond the last reads
 * only the count. An entry appears on exactly one page of a window that receives no entry between
 * the reads.
 */
public final class PostgresEventAuditQuery implements EventAuditQuery {

  private static final String COLUMNS =
      "event_id, event_type, aggregate_type, aggregate_id, version, command_id, correlation_id,"
          + " causation_id, user_id, occurred_at";

  private static final String BY_CORRELATION = "correlation_id = ?";
  private static final String BY_COMMAND = "command_id = ?";
  private static final String BY_CAUSATION = "causation_id = ?";
  private static final String BY_STREAM =
      "aggregate_type = ? AND aggregate_id = ? AND occurred_at >= ? AND occurred_at <= ?";
  private static final String BY_USER = "user_id = ? AND occurred_at >= ? AND occurred_at <= ?";

  private static final String FIND_BY_CORRELATION = listSql(BY_CORRELATION);
  private static final String FIND_BY_COMMAND = listSql(BY_COMMAND);
  private static final String FIND_BY_CAUSATION = listSql(BY_CAUSATION);
  private static final String FIND_BY_STREAM = listSql(BY_STREAM);
  private static final String FIND_BY_USER = listSql(BY_USER);

  private static final String PAGE_BY_CORRELATION = pageSql(BY_CORRELATION);
  private static final String PAGE_BY_COMMAND = pageSql(BY_COMMAND);
  private static final String PAGE_BY_CAUSATION = pageSql(BY_CAUSATION);
  private static final String PAGE_BY_STREAM = pageSql(BY_STREAM);
  private static final String PAGE_BY_USER = pageSql(BY_USER);

  private static String listSql(String where) {
    return "SELECT "
        + COLUMNS
        + " FROM event_audit_log WHERE "
        + where
        + " ORDER BY occurred_at, id";
  }

  /**
   * The statement of a paged read: the count of the matching rows and one page of them, as two
   * subqueries joined into one result, so both come from one snapshot. Every row carries the count;
   * a page beyond the last is one row with the count and a {@code NULL} {@code id} ({@code id} is
   * the primary key, so {@code NULL} means "no row"). The page is gated on the count ({@code
   * LATERAL ... WHERE total.n > offset}), so a page beyond the last reads only the count. The order
   * is {@code occurred_at}, then {@code id}; the outer {@code ORDER BY} restores it after the join.
   * Parameters: the filter's values (the count), the same again and the offset (the page and its
   * gate), {@code LIMIT}, {@code OFFSET}.
   */
  private static String pageSql(String where) {
    return "SELECT total.n AS total_count, page.* FROM (SELECT count(*) AS n FROM event_audit_log"
        + " WHERE "
        + where
        + ") AS total LEFT JOIN LATERAL (SELECT id, "
        + COLUMNS
        + " FROM event_audit_log WHERE "
        + where
        + " AND total.n > ? ORDER BY occurred_at, id LIMIT ? OFFSET ?) AS page ON true"
        + " ORDER BY page.occurred_at, page.id";
  }

  /** Binds the values of one {@code WHERE} clause from {@code first} on. */
  @FunctionalInterface
  private interface Filter {
    void bind(PreparedStatement ps, int first) throws SQLException;
  }

  // UNION (not UNION ALL) is the cycle guard: a causation cycle (self-reference or A->B->A,
  // possible only through data corruption) re-derives an already-seen row, UNION discards the
  // duplicate, the working set empties, and recursion terminates instead of spinning forever.
  // Legitimate chains are never truncated — recursion is bounded by the distinct rows reachable.
  private static final String TRACE_CHAIN =
      "WITH RECURSIVE chain AS ("
          + "  SELECT id, event_id, event_type, aggregate_type, aggregate_id, version, command_id,"
          + "         correlation_id, "
          + "         causation_id, user_id, occurred_at "
          + "  FROM event_audit_log WHERE event_id = ? "
          + "  UNION "
          + "  SELECT e.id, e.event_id, e.event_type, e.aggregate_type, e.aggregate_id, e.version,"
          + "         e.command_id, "
          + "         e.correlation_id, e.causation_id, e.user_id, e.occurred_at "
          + "  FROM event_audit_log e "
          + "  JOIN chain c ON e.causation_id = c.event_id"
          + ") SELECT * FROM chain ORDER BY occurred_at, id";

  private final DataSource dataSource;

  public PostgresEventAuditQuery(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
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
  public Page<EventAuditEntry> findByCorrelationId(
      CorrelationId correlationId, PageRequest pageRequest) {
    Objects.requireNonNull(correlationId, "correlationId is required");
    return queryPage(
        PAGE_BY_CORRELATION, 1, (ps, i) -> ps.setString(i, correlationId.value()), pageRequest);
  }

  @Override
  public Page<EventAuditEntry> findByCommandId(CommandId commandId, PageRequest pageRequest) {
    Objects.requireNonNull(commandId, "commandId is required");
    return queryPage(
        PAGE_BY_COMMAND, 1, (ps, i) -> ps.setString(i, commandId.value()), pageRequest);
  }

  @Override
  public Page<EventAuditEntry> findByCausationId(CausationId causationId, PageRequest pageRequest) {
    Objects.requireNonNull(causationId, "causationId is required");
    return queryPage(
        PAGE_BY_CAUSATION, 1, (ps, i) -> ps.setString(i, causationId.value()), pageRequest);
  }

  @Override
  public Page<EventAuditEntry> findByStreamId(
      StreamId streamId, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return queryPage(
        PAGE_BY_STREAM,
        4,
        (ps, i) -> {
          PostgresStreamKeys.bind(ps, i, streamId);
          ps.setTimestamp(i + 2, Timestamp.from(from));
          ps.setTimestamp(i + 3, Timestamp.from(to));
        },
        pageRequest);
  }

  @Override
  public Page<EventAuditEntry> findByUserId(
      UserId userId, Instant from, Instant to, PageRequest pageRequest) {
    Objects.requireNonNull(userId, "userId is required");
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return queryPage(
        PAGE_BY_USER,
        3,
        (ps, i) -> {
          ps.setString(i, userId.value());
          ps.setTimestamp(i + 1, Timestamp.from(from));
          ps.setTimestamp(i + 2, Timestamp.from(to));
        },
        pageRequest);
  }

  @Override
  public List<EventAuditEntry> traceFullCausationChain(EventId eventId) {
    return queryWithSingleParam(TRACE_CHAIN, eventId.value());
  }

  private Page<EventAuditEntry> queryPage(
      String sql, int filterParams, Filter filter, PageRequest pageRequest) {
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      filter.bind(ps, 1);
      filter.bind(ps, filterParams + 1);
      ps.setLong(2 * filterParams + 1, pageRequest.offset());
      ps.setInt(2 * filterParams + 2, pageRequest.size());
      ps.setLong(2 * filterParams + 3, pageRequest.offset());
      long total = 0;
      List<EventAuditEntry> content = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          total = rs.getLong("total_count");
          if (rs.getObject("id") != null) {
            content.add(readEntry(rs));
          }
        }
      } catch (IllegalArgumentException e) {
        throw new EventStoreException("Failed to decode an event audit log row", e);
      }
      return new Page<>(content, total, pageRequest.page(), pageRequest.size());
    } catch (SQLException e) {
      throw new EventStoreException("Failed to query event audit log", e);
    }
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

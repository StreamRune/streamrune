package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * The SQLite {@link EventAuditQuery} over the {@code event_audit_log} table {@link
 * SqliteEventAuditStore} and {@link SqliteEventStore} write, with the semantics of {@code
 * PostgresEventAuditQuery}.
 *
 * <p>Every finder answers {@code occurred_at} ascending; entries with equal {@code occurred_at}
 * come in the order they were written (the row id), one order the interface leaves open. The stream
 * and user windows are inclusive and compare to the microsecond; a window whose {@code from} is
 * after its {@code to} holds nothing. The paged forms page in the database, with {@code
 * LIMIT}/{@code OFFSET} and a separate {@code count(*)} for {@link Page#totalElements()}, and
 * ignore the {@link PageRequest#sort()} component (Postgres slices the unpaged result in memory;
 * the pages are the same).
 *
 * <p><b>Causation chain.</b> {@link #traceFullCausationChain} is one {@code WITH RECURSIVE} query
 * that starts at the event and joins, step by step, the rows whose {@code causation_id} is an event
 * found so far, through the partial index on {@code causation_id}. Its rows are combined with
 * {@code UNION}, not {@code UNION ALL}: a cycle in the causation data (a self-reference, or A
 * caused by B caused by A) derives a row already found, which {@code UNION} drops, so the recursion
 * ends with each event once instead of looping.
 *
 * <p>Queries run in autocommit and, under WAL, never wait for a writer.
 *
 * <p><b>Failures.</b> A storage failure, and a stored row the id types refuse, is an {@link
 * EventStoreException}; a {@code null} argument is refused with {@link NullPointerException}, as
 * the interface says.
 */
public final class SqliteEventAuditQuery implements EventAuditQuery {

  private static final String COLUMNS =
      "event_id, event_type, aggregate_type, aggregate_id, version, command_id, correlation_id,"
          + " causation_id, user_id, occurred_at";

  private static final String ORDER = " ORDER BY occurred_at, id";

  private static final String PAGE = ORDER + " LIMIT ? OFFSET ?";

  private static final String BY_CORRELATION = " FROM event_audit_log WHERE correlation_id = ?";

  private static final String BY_COMMAND = " FROM event_audit_log WHERE command_id = ?";

  private static final String BY_CAUSATION = " FROM event_audit_log WHERE causation_id = ?";

  private static final String BY_STREAM =
      " FROM event_audit_log WHERE aggregate_type = ? AND aggregate_id = ?"
          + " AND occurred_at >= ? AND occurred_at <= ?";

  private static final String BY_USER =
      " FROM event_audit_log WHERE user_id = ? AND occurred_at >= ? AND occurred_at <= ?";

  // UNION (not UNION ALL) is the cycle guard: a causation cycle re-derives a row already in the
  // chain, UNION drops it, the queue empties and the recursion ends. A legitimate chain is never
  // cut: the recursion is bounded by the distinct rows reachable from the event.
  static final String TRACE_CHAIN =
      "WITH RECURSIVE chain AS ("
          + "SELECT id, "
          + COLUMNS
          + " FROM event_audit_log WHERE event_id = ?"
          + " UNION "
          + "SELECT e.id, e.event_id, e.event_type, e.aggregate_type, e.aggregate_id, e.version,"
          + " e.command_id, e.correlation_id, e.causation_id, e.user_id, e.occurred_at"
          + " FROM event_audit_log e JOIN chain c ON e.causation_id = c.event_id"
          + ") SELECT "
          + COLUMNS
          + " FROM chain"
          + ORDER;

  private static final String FAILED = "Failed to query event audit log";

  private final DataSource dataSource;

  /**
   * Creates the query over {@code database}.
   *
   * @param database the database whose {@code event_audit_log} table the query reads
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteEventAuditQuery(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
  }

  @Override
  public List<EventAuditEntry> findByCorrelationId(CorrelationId correlationId) {
    Objects.requireNonNull(correlationId, "correlationId is required");
    return list(BY_CORRELATION, ps -> ps.setString(1, correlationId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCorrelationId(
      CorrelationId correlationId, PageRequest pageRequest) {
    Objects.requireNonNull(correlationId, "correlationId is required");
    return page(BY_CORRELATION, 1, ps -> ps.setString(1, correlationId.value()), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByCommandId(CommandId commandId) {
    Objects.requireNonNull(commandId, "commandId is required");
    return list(BY_COMMAND, ps -> ps.setString(1, commandId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCommandId(CommandId commandId, PageRequest pageRequest) {
    Objects.requireNonNull(commandId, "commandId is required");
    return page(BY_COMMAND, 1, ps -> ps.setString(1, commandId.value()), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByCausationId(CausationId causationId) {
    Objects.requireNonNull(causationId, "causationId is required");
    return list(BY_CAUSATION, ps -> ps.setString(1, causationId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCausationId(CausationId causationId, PageRequest pageRequest) {
    Objects.requireNonNull(causationId, "causationId is required");
    return page(BY_CAUSATION, 1, ps -> ps.setString(1, causationId.value()), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByStreamId(StreamId streamId, Instant from, Instant to) {
    return list(BY_STREAM, streamBinder(streamId, from, to));
  }

  @Override
  public Page<EventAuditEntry> findByStreamId(
      StreamId streamId, Instant from, Instant to, PageRequest pageRequest) {
    return page(BY_STREAM, 4, streamBinder(streamId, from, to), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByUserId(UserId userId, Instant from, Instant to) {
    return list(BY_USER, userBinder(userId, from, to));
  }

  @Override
  public Page<EventAuditEntry> findByUserId(
      UserId userId, Instant from, Instant to, PageRequest pageRequest) {
    return page(BY_USER, 3, userBinder(userId, from, to), pageRequest);
  }

  @Override
  public List<EventAuditEntry> traceFullCausationChain(EventId eventId) {
    Objects.requireNonNull(eventId, "eventId is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(TRACE_CHAIN)) {
      ps.setString(1, eventId.value());
      return readEntries(ps);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  /** Binds a finder's filter parameters, from index 1. */
  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }

  /** Checks the arguments now, before any I/O, and binds them as a stream window. */
  private static Binder streamBinder(StreamId streamId, Instant from, Instant to) {
    Objects.requireNonNull(streamId, "streamId is required");
    long fromMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(from, "from is required"));
    long toMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(to, "to is required"));
    return ps -> {
      SqliteStreamKeys.bind(ps, 1, streamId);
      ps.setLong(3, fromMicros);
      ps.setLong(4, toMicros);
    };
  }

  /** Checks the arguments now, before any I/O, and binds them as a user window. */
  private static Binder userBinder(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId is required");
    long fromMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(from, "from is required"));
    long toMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(to, "to is required"));
    return ps -> {
      ps.setString(1, userId.value());
      ps.setLong(2, fromMicros);
      ps.setLong(3, toMicros);
    };
  }

  /** Every entry {@code SELECT COLUMNS + from} finds, in the finders' order. */
  private List<EventAuditEntry> list(String from, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT " + COLUMNS + from + ORDER)) {
      binder.bind(ps);
      return readEntries(ps);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  /**
   * One page of what {@code SELECT COLUMNS + from} finds, and the count of all of it.
   *
   * @param parameters how many parameters {@code binder} binds; the page's follow them
   */
  private Page<EventAuditEntry> page(
      String from, int parameters, Binder binder, PageRequest pageRequest) {
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    try (Connection conn = dataSource.getConnection()) {
      long total;
      try (PreparedStatement ps = conn.prepareStatement("SELECT count(*)" + from)) {
        binder.bind(ps);
        try (ResultSet rs = ps.executeQuery()) {
          total = rs.next() ? rs.getLong(1) : 0L;
        }
      }
      List<EventAuditEntry> content;
      try (PreparedStatement ps = conn.prepareStatement("SELECT " + COLUMNS + from + PAGE)) {
        binder.bind(ps);
        ps.setInt(parameters + 1, pageRequest.size());
        ps.setLong(parameters + 2, pageRequest.offset());
        content = readEntries(ps);
      }
      return new Page<>(content, total, pageRequest.page(), pageRequest.size());
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  private static List<EventAuditEntry> readEntries(PreparedStatement ps) throws SQLException {
    List<EventAuditEntry> entries = new ArrayList<>();
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        entries.add(readEntry(rs));
      }
    }
    return entries;
  }

  private static EventAuditEntry readEntry(ResultSet rs) throws SQLException {
    String causationId = rs.getString("causation_id");
    String userId = rs.getString("user_id");
    return new EventAuditEntry(
        EventId.of(rs.getString("event_id")),
        new EventType(rs.getString("event_type")),
        SqliteStreamKeys.read(rs),
        new Version(rs.getLong("version")),
        CommandId.of(rs.getString("command_id")),
        CorrelationId.of(rs.getString("correlation_id")),
        causationId != null ? CausationId.of(causationId) : null,
        userId != null ? UserId.of(userId) : null,
        SqliteTimestamps.fromMicros(rs.getLong("occurred_at")));
  }
}

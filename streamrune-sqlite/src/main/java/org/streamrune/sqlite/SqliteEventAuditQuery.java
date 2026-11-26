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
 * <p>Every read orders by {@code occurred_at}, then the row's {@code id}, so entries with an equal
 * timestamp (the events of one command are stamped by consecutive clock reads) keep one position.
 * The stream and user windows are inclusive and compare to the microsecond; a window whose {@code
 * from} is after its {@code to} holds nothing. The paged forms page in the database, with {@code
 * LIMIT}/{@code OFFSET}, and ignore the {@link PageRequest#sort()} component; the page and {@link
 * Page#totalElements()} come from one statement, so from one snapshot of the file, and they agree.
 * An entry appears on exactly one page of a window that receives no entry between the reads.
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
    return list(BY_CORRELATION, (ps, first) -> ps.setString(first, correlationId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCorrelationId(
      CorrelationId correlationId, PageRequest pageRequest) {
    Objects.requireNonNull(correlationId, "correlationId is required");
    return page(
        BY_CORRELATION, 1, (ps, first) -> ps.setString(first, correlationId.value()), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByCommandId(CommandId commandId) {
    Objects.requireNonNull(commandId, "commandId is required");
    return list(BY_COMMAND, (ps, first) -> ps.setString(first, commandId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCommandId(CommandId commandId, PageRequest pageRequest) {
    Objects.requireNonNull(commandId, "commandId is required");
    return page(BY_COMMAND, 1, (ps, first) -> ps.setString(first, commandId.value()), pageRequest);
  }

  @Override
  public List<EventAuditEntry> findByCausationId(CausationId causationId) {
    Objects.requireNonNull(causationId, "causationId is required");
    return list(BY_CAUSATION, (ps, first) -> ps.setString(first, causationId.value()));
  }

  @Override
  public Page<EventAuditEntry> findByCausationId(CausationId causationId, PageRequest pageRequest) {
    Objects.requireNonNull(causationId, "causationId is required");
    return page(
        BY_CAUSATION, 1, (ps, first) -> ps.setString(first, causationId.value()), pageRequest);
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

  /** Binds a finder's filter parameters from index {@code first} on. */
  @FunctionalInterface
  private interface Binder {
    void bind(PreparedStatement ps, int first) throws SQLException;
  }

  /** Checks the arguments now, before any I/O, and binds them as a stream window. */
  private static Binder streamBinder(StreamId streamId, Instant from, Instant to) {
    Objects.requireNonNull(streamId, "streamId is required");
    long fromMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(from, "from is required"));
    long toMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(to, "to is required"));
    return (ps, first) -> {
      SqliteStreamKeys.bind(ps, first, streamId);
      ps.setLong(first + 2, fromMicros);
      ps.setLong(first + 3, toMicros);
    };
  }

  /** Checks the arguments now, before any I/O, and binds them as a user window. */
  private static Binder userBinder(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId is required");
    long fromMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(from, "from is required"));
    long toMicros = SqliteTimestamps.toMicros(Objects.requireNonNull(to, "to is required"));
    return (ps, first) -> {
      ps.setString(first, userId.value());
      ps.setLong(first + 1, fromMicros);
      ps.setLong(first + 2, toMicros);
    };
  }

  /** Every entry {@code SELECT COLUMNS + from} finds, in the finders' order. */
  private List<EventAuditEntry> list(String from, Binder binder) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT " + COLUMNS + from + ORDER)) {
      binder.bind(ps, 1);
      return readEntries(ps);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  /**
   * The statement of a paged read of what {@code SELECT COLUMNS + from} finds: the count of the
   * matching rows and one page of them, joined into one result, so both come from one snapshot.
   * Every row carries the count; a page beyond the last is one row with the count and a {@code
   * NULL} {@code id} ({@code id} is the primary key, so {@code NULL} means "no row"). The page's
   * {@code LIMIT} is 0 unless the count exceeds the offset, so a page beyond the last skips no rows
   * to find none (Postgres gates its page the same way, with a {@code LATERAL} join, which SQLite
   * does not have). The outer {@code ORDER BY} restores the order after the join. Parameters: the
   * filter's values (the count), the same again (the page), the offset (the gate), {@code LIMIT},
   * {@code OFFSET}.
   */
  static String pageSql(String from) {
    return "WITH total(n) AS MATERIALIZED (SELECT count(*)"
        + from
        + ") SELECT total.n AS total_count, page.* FROM total LEFT JOIN (SELECT id, "
        + COLUMNS
        + from
        + ORDER
        + " LIMIT CASE WHEN (SELECT n FROM total) > ? THEN ? ELSE 0 END OFFSET ?) AS page ON 1"
        + " ORDER BY page.occurred_at, page.id";
  }

  /**
   * One page of what {@code SELECT COLUMNS + from} finds, and the count of all of it, in one
   * statement ({@link #pageSql}).
   *
   * @param parameters how many parameters {@code binder} binds
   */
  private Page<EventAuditEntry> page(
      String from, int parameters, Binder binder, PageRequest pageRequest) {
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(pageSql(from))) {
      binder.bind(ps, 1);
      binder.bind(ps, parameters + 1);
      ps.setLong(2 * parameters + 1, pageRequest.offset());
      ps.setInt(2 * parameters + 2, pageRequest.size());
      ps.setLong(2 * parameters + 3, pageRequest.offset());
      long total = 0;
      List<EventAuditEntry> content = new ArrayList<>();
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

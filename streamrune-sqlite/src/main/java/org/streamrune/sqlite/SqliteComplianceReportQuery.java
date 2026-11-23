package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.types.UserId;

/**
 * The SQLite {@link ComplianceReportQuery}: aggregates over the {@code audit_log} table {@link
 * SqliteAuditStore} writes, with the semantics and the statements of {@code
 * PostgresComplianceReportQuery}, aggregate {@code FILTER} clauses included (SQLite has them since
 * 3.30; StreamRune requires 3.45).
 *
 * <p>A command is an entry that names an aggregate id, a query one that does not; {@code
 * totalFailures} counts the {@code FAILURE} outcomes of both. The range is inclusive at both ends
 * and compares to the microsecond; a range whose {@code from} is after its {@code to} holds
 * nothing.
 *
 * <p>A report is four reads in autocommit, as on Postgres, which under WAL never wait for a writer:
 * an entry written between them can count in one figure and not another.
 *
 * <p><b>Failures.</b> A storage failure is an {@link EventStoreException} (on Postgres a plain
 * {@link RuntimeException} with the same message); a {@code null} argument is refused with {@link
 * NullPointerException}, as the interface says.
 */
public final class SqliteComplianceReportQuery implements ComplianceReportQuery {

  private static final String COUNTS_SQL =
      "SELECT "
          + "count(*) FILTER (WHERE aggregate_id IS NOT NULL) AS total_commands, "
          + "count(*) FILTER (WHERE aggregate_id IS NULL) AS total_queries, "
          + "count(*) FILTER (WHERE outcome = 'FAILURE') AS total_failures "
          + "FROM audit_log WHERE occurred_at >= ? AND occurred_at <= ?";

  private static final String COUNTS_USER_SQL = COUNTS_SQL + " AND user_id = ?";

  // Per-type counts: commands carry an aggregate_id, queries do not. Four constant statements, so
  // no SQL fragment is ever passed around.
  private static final String TYPE_COUNTS_SELECT =
      "SELECT command_type, count(*) AS cnt FROM audit_log "
          + "WHERE occurred_at >= ? AND occurred_at <= ? AND aggregate_id ";

  private static final String COMMAND_TYPE_COUNTS_SQL =
      TYPE_COUNTS_SELECT + "IS NOT NULL GROUP BY command_type";

  private static final String COMMAND_TYPE_COUNTS_USER_SQL =
      TYPE_COUNTS_SELECT + "IS NOT NULL AND user_id = ? GROUP BY command_type";

  private static final String QUERY_TYPE_COUNTS_SQL =
      TYPE_COUNTS_SELECT + "IS NULL GROUP BY command_type";

  private static final String QUERY_TYPE_COUNTS_USER_SQL =
      TYPE_COUNTS_SELECT + "IS NULL AND user_id = ? GROUP BY command_type";

  private static final String ACTIVITY_SQL =
      "SELECT user_id, count(*) AS cnt FROM audit_log "
          + "WHERE occurred_at >= ? AND occurred_at <= ? AND user_id IS NOT NULL "
          + "GROUP BY user_id";

  private static final String ACTIVITY_USER_SQL =
      "SELECT user_id, count(*) AS cnt FROM audit_log "
          + "WHERE occurred_at >= ? AND occurred_at <= ? AND user_id = ? "
          + "GROUP BY user_id";

  private final DataSource dataSource;

  /**
   * Creates the query over {@code database}.
   *
   * @param database the database whose {@code audit_log} table the query reads
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteComplianceReportQuery(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
  }

  @Override
  public ComplianceReport generate(Instant from, Instant to) {
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return buildReport(from, to, null);
  }

  @Override
  public ComplianceReport generateForUser(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId is required");
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return buildReport(from, to, userId.value());
  }

  private ComplianceReport buildReport(Instant from, Instant to, String userIdFilter) {
    long fromMicros = SqliteTimestamps.toMicros(from);
    long toMicros = SqliteTimestamps.toMicros(to);
    try (Connection conn = dataSource.getConnection()) {
      long[] counts;
      try (PreparedStatement ps =
              conn.prepareStatement(userIdFilter != null ? COUNTS_USER_SQL : COUNTS_SQL);
          ResultSet rs = execute(ps, fromMicros, toMicros, userIdFilter)) {
        rs.next();
        counts =
            new long[] {
              rs.getLong("total_commands"),
              rs.getLong("total_queries"),
              rs.getLong("total_failures")
            };
      }
      Map<String, Long> commandCounts =
          countsBy(
              conn,
              userIdFilter != null ? COMMAND_TYPE_COUNTS_USER_SQL : COMMAND_TYPE_COUNTS_SQL,
              "command_type",
              fromMicros,
              toMicros,
              userIdFilter);
      Map<String, Long> queryCounts =
          countsBy(
              conn,
              userIdFilter != null ? QUERY_TYPE_COUNTS_USER_SQL : QUERY_TYPE_COUNTS_SQL,
              "command_type",
              fromMicros,
              toMicros,
              userIdFilter);
      Map<String, Long> activity =
          countsBy(
              conn,
              userIdFilter != null ? ACTIVITY_USER_SQL : ACTIVITY_SQL,
              "user_id",
              fromMicros,
              toMicros,
              userIdFilter);
      return new ComplianceReport(
          from, to, counts[0], counts[1], counts[2], commandCounts, queryCounts, activity);
    } catch (SQLException | RuntimeException e) {
      throw new EventStoreException("Failed to generate compliance report", e);
    }
  }

  private static ResultSet execute(
      PreparedStatement ps, long fromMicros, long toMicros, String userIdFilter)
      throws SQLException {
    ps.setLong(1, fromMicros);
    ps.setLong(2, toMicros);
    if (userIdFilter != null) {
      ps.setString(3, userIdFilter);
    }
    return ps.executeQuery();
  }

  /** The {@code cnt} of each {@code key} the grouped statement {@code sql} returns. */
  private static Map<String, Long> countsBy(
      Connection conn, String sql, String key, long fromMicros, long toMicros, String userIdFilter)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = execute(ps, fromMicros, toMicros, userIdFilter)) {
      Map<String, Long> result = new HashMap<>();
      while (rs.next()) {
        result.put(rs.getString(key), rs.getLong("cnt"));
      }
      return result;
    }
  }
}

package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.types.UserId;

/**
 * PostgreSQL implementation of {@link ComplianceReportQuery}. Generates compliance reports by
 * running aggregation queries against the {@code audit_log} table.
 */
public final class PostgresComplianceReportQuery implements ComplianceReportQuery {

  private final DataSource dataSource;

  private static final String COUNTS_SQL =
      "SELECT "
          + "COUNT(*) FILTER (WHERE aggregate_id IS NOT NULL) AS total_commands, "
          + "COUNT(*) FILTER (WHERE aggregate_id IS NULL) AS total_queries, "
          + "COUNT(*) FILTER (WHERE outcome = 'FAILURE') AS total_failures "
          + "FROM audit_log WHERE occurred_at >= ? AND occurred_at <= ?";

  private static final String COUNTS_USER_SQL = COUNTS_SQL + " AND user_id = ?";

  // Per-type counts: commands carry an aggregate_id, queries do not. Four constant statements
  // rather than one format string taking the IS [NOT] NULL fragment as a String parameter, so no
  // SQL fragment is ever passed around.
  private static final String TYPE_COUNTS_SELECT =
      "SELECT command_type, COUNT(*) AS cnt FROM audit_log "
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
      "SELECT user_id, COUNT(*) AS cnt FROM audit_log "
          + "WHERE occurred_at >= ? AND occurred_at <= ? AND user_id IS NOT NULL "
          + "GROUP BY user_id";

  private static final String ACTIVITY_USER_SQL =
      "SELECT user_id, COUNT(*) AS cnt FROM audit_log "
          + "WHERE occurred_at >= ? AND occurred_at <= ? AND user_id = ? "
          + "GROUP BY user_id";

  public PostgresComplianceReportQuery(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
  }

  @Override
  public ComplianceReport generate(Instant from, Instant to) {
    return buildReport(from, to, null);
  }

  @Override
  public ComplianceReport generateForUser(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId");
    return buildReport(from, to, userId.value());
  }

  private ComplianceReport buildReport(Instant from, Instant to, String userIdFilter) {
    try (Connection conn = dataSource.getConnection()) {
      long[] counts = queryCounts(conn, from, to, userIdFilter);
      Map<String, Long> commandCounts = queryTypeCounts(conn, from, to, true, userIdFilter);
      Map<String, Long> queryCounts = queryTypeCounts(conn, from, to, false, userIdFilter);
      Map<String, Long> activity = queryActivity(conn, from, to, userIdFilter);

      return new ComplianceReport(
          from, to, counts[0], counts[1], counts[2], commandCounts, queryCounts, activity);
    } catch (SQLException ex) {
      throw new RuntimeException("Failed to generate compliance report", ex);
    }
  }

  private long[] queryCounts(Connection conn, Instant from, Instant to, String userIdFilter)
      throws SQLException {
    String sql = userIdFilter != null ? COUNTS_USER_SQL : COUNTS_SQL;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.from(from));
      ps.setTimestamp(2, Timestamp.from(to));
      if (userIdFilter != null) {
        ps.setString(3, userIdFilter);
      }
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return new long[] {
          rs.getLong("total_commands"), rs.getLong("total_queries"), rs.getLong("total_failures")
        };
      }
    }
  }

  private Map<String, Long> queryTypeCounts(
      Connection conn, Instant from, Instant to, boolean commands, String userIdFilter)
      throws SQLException {
    String sql;
    if (commands) {
      sql = userIdFilter != null ? COMMAND_TYPE_COUNTS_USER_SQL : COMMAND_TYPE_COUNTS_SQL;
    } else {
      sql = userIdFilter != null ? QUERY_TYPE_COUNTS_USER_SQL : QUERY_TYPE_COUNTS_SQL;
    }
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.from(from));
      ps.setTimestamp(2, Timestamp.from(to));
      if (userIdFilter != null) {
        ps.setString(3, userIdFilter);
      }
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, Long> result = new HashMap<>();
        while (rs.next()) {
          result.put(rs.getString("command_type"), rs.getLong("cnt"));
        }
        return result;
      }
    }
  }

  private Map<String, Long> queryActivity(
      Connection conn, Instant from, Instant to, String userIdFilter) throws SQLException {
    String sql = userIdFilter != null ? ACTIVITY_USER_SQL : ACTIVITY_SQL;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setTimestamp(1, Timestamp.from(from));
      ps.setTimestamp(2, Timestamp.from(to));
      if (userIdFilter != null) {
        ps.setString(3, userIdFilter);
      }
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, Long> result = new HashMap<>();
        while (rs.next()) {
          result.put(rs.getString("user_id"), rs.getLong("cnt"));
        }
        return result;
      }
    }
  }
}

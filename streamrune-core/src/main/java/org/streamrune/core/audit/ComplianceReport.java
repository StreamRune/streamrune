package org.streamrune.core.audit;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregated compliance report over a time range from the audit trail.
 *
 * <p>Commands are entries with non-empty {@code aggregateId}. Queries are entries with empty {@code
 * aggregateId} (set by {@code AuditingQueryBus}).
 *
 * @param from start of the reporting period (inclusive)
 * @param to end of the reporting period (inclusive)
 * @param totalCommands count of command audit entries in range
 * @param totalQueries count of query audit entries in range
 * @param totalFailures count of FAILURE outcomes in range
 * @param commandCountsByType breakdown of commands by type
 * @param queryCountsByType breakdown of queries by type
 * @param activityByUser total actions per user (null userIds excluded)
 */
public record ComplianceReport(
    Instant from,
    Instant to,
    long totalCommands,
    long totalQueries,
    long totalFailures,
    Map<String, Long> commandCountsByType,
    Map<String, Long> queryCountsByType,
    Map<String, Long> activityByUser) {

  public ComplianceReport {
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    Objects.requireNonNull(commandCountsByType, "commandCountsByType is required");
    Objects.requireNonNull(queryCountsByType, "queryCountsByType is required");
    Objects.requireNonNull(activityByUser, "activityByUser is required");
    commandCountsByType = Map.copyOf(commandCountsByType);
    queryCountsByType = Map.copyOf(queryCountsByType);
    activityByUser = Map.copyOf(activityByUser);
  }
}

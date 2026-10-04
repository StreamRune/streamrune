package org.streamrune.runtime;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.types.UserId;

/** In-memory implementation of {@link ComplianceReportQuery} for testing. */
public final class InMemoryComplianceReportQuery implements ComplianceReportQuery {

  private final List<AuditEntry> entries;

  public InMemoryComplianceReportQuery(List<AuditEntry> entries) {
    this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
  }

  @Override
  public ComplianceReport generate(Instant from, Instant to) {
    return buildReport(from, to, null);
  }

  @Override
  public ComplianceReport generateForUser(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId");
    return buildReport(from, to, userId);
  }

  private ComplianceReport buildReport(Instant from, Instant to, UserId userIdFilter) {
    List<AuditEntry> filtered =
        entries.stream()
            .filter(e -> !e.occurredAt().isBefore(from) && !e.occurredAt().isAfter(to))
            .filter(e -> userIdFilter == null || userIdFilter.equals(e.userId()))
            .toList();

    long totalCommands = filtered.stream().filter(e -> e.aggregateId() != null).count();
    long totalQueries = filtered.stream().filter(e -> e.aggregateId() == null).count();
    long totalFailures = filtered.stream().filter(e -> e.outcome() == AuditOutcome.FAILURE).count();

    Map<String, Long> commandCounts =
        filtered.stream()
            .filter(e -> e.aggregateId() != null)
            .collect(Collectors.groupingBy(AuditEntry::commandType, Collectors.counting()));

    Map<String, Long> queryCounts =
        filtered.stream()
            .filter(e -> e.aggregateId() == null)
            .collect(Collectors.groupingBy(AuditEntry::commandType, Collectors.counting()));

    Map<String, Long> activityByUser = new HashMap<>();
    for (AuditEntry e : filtered) {
      if (e.userId() != null) {
        activityByUser.merge(e.userId().value(), 1L, Long::sum);
      }
    }

    return new ComplianceReport(
        from,
        to,
        totalCommands,
        totalQueries,
        totalFailures,
        commandCounts,
        queryCounts,
        activityByUser);
  }
}

package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.UserId;

class InMemoryComplianceReportQueryTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private InMemoryComplianceReportQuery query;
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final Instant T1 = Instant.parse("2026-01-01T01:00:00Z");
  private static final Instant T2 = Instant.parse("2026-01-01T02:00:00Z");
  private static final Instant T3 = Instant.parse("2026-01-01T03:00:00Z");

  @BeforeEach
  void setUp() {
    List<AuditEntry> entries =
        List.of(
            new AuditEntry(
                IdGenerator.generateCommandId(),
                "CreateUser",
                TYPE,
                AggregateId.of("agg-1"),
                UserId.of("user-1"),
                T1,
                AuditOutcome.SUCCESS,
                null,
                2,
                null),
            new AuditEntry(
                IdGenerator.generateCommandId(),
                "UpdateUser",
                TYPE,
                AggregateId.of("agg-2"),
                UserId.of("user-1"),
                T1,
                AuditOutcome.SUCCESS,
                null,
                1,
                null),
            new AuditEntry(
                IdGenerator.generateCommandId(),
                "DeleteUser",
                TYPE,
                AggregateId.of("agg-3"),
                UserId.of("user-2"),
                T2,
                AuditOutcome.FAILURE,
                "Not found",
                0,
                null),
            new AuditEntry(
                IdGenerator.generateCommandId(),
                "FindUserById",
                null,
                null,
                UserId.of("user-1"),
                T2,
                AuditOutcome.SUCCESS,
                null,
                0,
                null),
            new AuditEntry(
                IdGenerator.generateCommandId(),
                "ListOrders",
                null,
                null,
                null,
                T2,
                AuditOutcome.SUCCESS,
                null,
                0,
                null));
    query = new InMemoryComplianceReportQuery(entries);
  }

  @Test
  void generateReportForTimeRange() {
    ComplianceReport report = query.generate(T0, T3);

    assertThat(report.from()).isEqualTo(T0);
    assertThat(report.to()).isEqualTo(T3);
    assertThat(report.totalCommands()).isEqualTo(3);
    assertThat(report.totalQueries()).isEqualTo(2);
    assertThat(report.totalFailures()).isEqualTo(1);
    assertThat(report.commandCountsByType()).containsEntry("CreateUser", 1L);
    assertThat(report.commandCountsByType()).containsEntry("UpdateUser", 1L);
    assertThat(report.commandCountsByType()).containsEntry("DeleteUser", 1L);
    assertThat(report.queryCountsByType()).containsEntry("FindUserById", 1L);
    assertThat(report.queryCountsByType()).containsEntry("ListOrders", 1L);
    assertThat(report.activityByUser()).containsEntry("user-1", 3L);
    assertThat(report.activityByUser()).containsEntry("user-2", 1L);
    assertThat(report.activityByUser()).doesNotContainKey(null);
  }

  @Test
  void generateForUserFilters() {
    ComplianceReport report = query.generateForUser(UserId.of("user-2"), T0, T3);

    assertThat(report.totalCommands()).isEqualTo(1);
    assertThat(report.totalQueries()).isZero();
    assertThat(report.totalFailures()).isEqualTo(1);
    assertThat(report.activityByUser()).containsOnlyKeys("user-2");
  }

  @Test
  void timeRangeFiltering() {
    ComplianceReport report = query.generate(T1, T1);

    assertThat(report.totalCommands()).isEqualTo(2);
    assertThat(report.totalQueries()).isZero();
  }

  @Test
  void emptyResultForNonMatchingRange() {
    ComplianceReport report = query.generate(T3, Instant.parse("2026-01-01T04:00:00Z"));

    assertThat(report.totalCommands()).isZero();
    assertThat(report.totalQueries()).isZero();
    assertThat(report.totalFailures()).isZero();
    assertThat(report.commandCountsByType()).isEmpty();
    assertThat(report.queryCountsByType()).isEmpty();
    assertThat(report.activityByUser()).isEmpty();
  }
}

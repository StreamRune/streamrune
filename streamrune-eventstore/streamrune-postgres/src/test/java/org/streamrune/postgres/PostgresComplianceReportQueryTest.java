package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.UserId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresComplianceReportQueryTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_compliance_test");

  private static PGSimpleDataSource dataSource;
  private PostgresComplianceReportQuery query;
  private PostgresAuditStore auditStore;

  private static final Instant T1 = Instant.parse("2026-01-01T01:00:00Z");
  private static final Instant T2 = Instant.parse("2026-01-01T02:00:00Z");

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            // No @Encrypted-bearing types are exercised through this registry in this test; report
            // none explicitly rather than inheriting the throwing default.
            return java.util.List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void setUp() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM audit_log");
    }
    auditStore = new PostgresAuditStore(dataSource);
    query = new PostgresComplianceReportQuery(dataSource);
  }

  @Test
  void generateReportWithMixedEntries() {
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "CreateUser",
            TestStreams.TYPE,
            AggregateId.of("agg-1"),
            UserId.of("user-1"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            2,
            null));
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "FindUserById",
            null,
            null,
            UserId.of("user-1"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "DeleteUser",
            TestStreams.TYPE,
            AggregateId.of("agg-2"),
            UserId.of("user-2"),
            T2,
            AuditOutcome.FAILURE,
            "Not found",
            0,
            null));

    ComplianceReport report =
        query.generate(
            Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T03:00:00Z"));

    assertEquals(2, report.totalCommands());
    assertEquals(1, report.totalQueries());
    assertEquals(1, report.totalFailures());
    assertEquals(1L, report.commandCountsByType().get("CreateUser"));
    assertEquals(1L, report.commandCountsByType().get("DeleteUser"));
    assertEquals(1L, report.queryCountsByType().get("FindUserById"));
    assertEquals(2L, report.activityByUser().get("user-1"));
    assertEquals(1L, report.activityByUser().get("user-2"));
  }

  @Test
  void generateForUserFilters() {
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "CreateUser",
            TestStreams.TYPE,
            AggregateId.of("agg-1"),
            UserId.of("user-1"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "DeleteUser",
            TestStreams.TYPE,
            AggregateId.of("agg-2"),
            UserId.of("user-2"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));

    ComplianceReport report =
        query.generateForUser(
            UserId.of("user-1"),
            Instant.parse("2026-01-01T00:00:00Z"),
            Instant.parse("2026-01-01T03:00:00Z"));

    assertEquals(1, report.totalCommands());
    assertTrue(report.activityByUser().containsKey("user-1"));
    assertFalse(report.activityByUser().containsKey("user-2"));
    assertEquals(1, report.activityByUser().size());
  }

  /**
   * Pins the per-user command/query type split: each of the four type-count statements (commands or
   * queries, all users or one user) is now its own constant, and the per-user pair had no assertion
   * on the type maps before.
   */
  @Test
  void generateForUserSplitsCommandAndQueryTypeCountsForThatUserOnly() {
    for (String user : new String[] {"user-1", "user-2"}) {
      auditStore.save(
          new AuditEntry(
              IdGenerator.generateCommandId(),
              "CreateUser",
              TestStreams.TYPE,
              AggregateId.of("agg-" + user),
              UserId.of(user),
              T1,
              AuditOutcome.SUCCESS,
              null,
              1,
              null));
      auditStore.save(
          new AuditEntry(
              IdGenerator.generateCommandId(),
              "FindUserById",
              null,
              null,
              UserId.of(user),
              T1,
              AuditOutcome.SUCCESS,
              null,
              0,
              null));
    }
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "ListOrders",
            null,
            null,
            UserId.of("user-2"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));

    ComplianceReport report =
        query.generateForUser(
            UserId.of("user-1"),
            Instant.parse("2026-01-01T00:00:00Z"),
            Instant.parse("2026-01-01T03:00:00Z"));

    assertEquals(java.util.Map.of("CreateUser", 1L), report.commandCountsByType());
    assertEquals(java.util.Map.of("FindUserById", 1L), report.queryCountsByType());
    assertEquals(1, report.totalCommands());
    assertEquals(1, report.totalQueries());
  }

  @Test
  void emptyResultForNonMatchingRange() {
    auditStore.save(
        new AuditEntry(
            IdGenerator.generateCommandId(),
            "CreateUser",
            TestStreams.TYPE,
            AggregateId.of("agg-1"),
            UserId.of("user-1"),
            T1,
            AuditOutcome.SUCCESS,
            null,
            1,
            null));

    ComplianceReport report =
        query.generate(
            Instant.parse("2026-02-01T00:00:00Z"), Instant.parse("2026-02-01T01:00:00Z"));

    assertEquals(0, report.totalCommands());
    assertEquals(0, report.totalQueries());
    assertEquals(0, report.totalFailures());
    assertTrue(report.commandCountsByType().isEmpty());
  }
}

package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.UserId;

/**
 * What every {@link ComplianceReportQuery} must honour over the entries an {@link AuditStore}
 * wrote, taken from the interface's and {@link ComplianceReport}'s Javadoc, over one known fixture:
 * commands are the entries that name an aggregate id and queries the entries that do not; {@code
 * totalFailures} counts FAILURE outcomes only (a veto is not a failure) over commands and queries
 * alike; the per-type breakdowns; {@code activityByUser} counts every action of a user and leaves
 * anonymous entries out; the range is inclusive at both ends and echoed in the report; {@code
 * generateForUser} scopes every figure to one user; an empty range or an unknown user yields zeros
 * and empty maps; and null arguments fail with {@link NullPointerException}. Run by {@code
 * InMemoryComplianceReportQueryContractTest} and {@code PostgresAuditContractsTest}.
 */
public abstract class ComplianceReportQueryContract {

  private static final Instant FROM = Instant.parse("2026-01-01T10:00:00.123Z");
  private static final Instant TO = FROM.plus(Duration.ofMinutes(8));
  private static final UserId U1 = UserId.of("u1");
  private static final UserId U2 = UserId.of("u2");

  /** The sink the entries are written through; it shares its storage with {@link #query()}. */
  protected abstract AuditStore writer();

  /** The query under test, over the storage {@link #writer()} writes. */
  protected abstract ComplianceReportQuery query();

  private AuditStore writer;
  private ComplianceReportQuery query;
  private int sequence;

  @BeforeEach
  void fresh() {
    writer = writer();
    query = query();
    sequence = 0;
  }

  // ---------------------------------------------------------------- fixture

  private void command(String type, UserId user, AuditOutcome outcome, Instant at) {
    writer.save(
        new AuditEntry(
            CommandId.of("c-" + sequence++),
            type,
            AggregateType.of("order"),
            AggregateId.of("order-1"),
            user,
            at,
            outcome,
            outcome == AuditOutcome.SUCCESS ? null : "not executed",
            outcome == AuditOutcome.SUCCESS ? 1 : 0,
            null));
  }

  /** A query-origin entry: no command id, no aggregate. */
  private void audited(String type, UserId user, AuditOutcome outcome, Instant at) {
    writer.save(
        new AuditEntry(
            null,
            type,
            null,
            null,
            user,
            at,
            outcome,
            outcome == AuditOutcome.SUCCESS ? null : "not executed",
            0,
            null));
  }

  /** A GDPR entry names a subject as its aggregate id and has no aggregate type. */
  private void subject(UserId user, Instant at) {
    writer.save(
        new AuditEntry(
            CommandId.of("c-" + sequence++),
            "ForgetSubject",
            null,
            AggregateId.of("subject-hash"),
            user,
            at,
            AuditOutcome.SUCCESS,
            null,
            0,
            null));
  }

  /**
   * Nine entries inside [FROM, TO] (the first at FROM, the last at TO) and one a millisecond
   * outside on each side, which no report may count.
   */
  private void writeFixture() {
    command("PlaceOrder", U1, AuditOutcome.SUCCESS, FROM);
    command("PlaceOrder", U1, AuditOutcome.FAILURE, FROM.plus(Duration.ofMinutes(1)));
    command("PlaceOrder", U2, AuditOutcome.SUCCESS, FROM.plus(Duration.ofMinutes(2)));
    command("CancelOrder", U2, AuditOutcome.VETOED, FROM.plus(Duration.ofMinutes(3)));
    command("ShipOrder", null, AuditOutcome.SUCCESS, FROM.plus(Duration.ofMinutes(4)));
    audited("GetOrder", U1, AuditOutcome.SUCCESS, FROM.plus(Duration.ofMinutes(5)));
    audited("GetOrder", U2, AuditOutcome.FAILURE, FROM.plus(Duration.ofMinutes(6)));
    audited("ListOrders", null, AuditOutcome.SUCCESS, FROM.plus(Duration.ofMinutes(7)));
    subject(U2, TO);
    command("PlaceOrder", U1, AuditOutcome.FAILURE, FROM.minusMillis(1));
    audited("GetOrder", U1, AuditOutcome.SUCCESS, TO.plusMillis(1));
  }

  // ---------------------------------------------------------------- generate

  @Test
  void theReportTotalsOverAKnownFixture() {
    writeFixture();

    ComplianceReport report = query.generate(FROM, TO);

    assertEquals(FROM, report.from());
    assertEquals(TO, report.to());
    assertEquals(6, report.totalCommands());
    assertEquals(3, report.totalQueries());
    assertEquals(2, report.totalFailures());
    assertEquals(
        Map.of("PlaceOrder", 3L, "CancelOrder", 1L, "ShipOrder", 1L, "ForgetSubject", 1L),
        report.commandCountsByType());
    assertEquals(Map.of("GetOrder", 2L, "ListOrders", 1L), report.queryCountsByType());
    assertEquals(Map.of("u1", 3L, "u2", 4L), report.activityByUser());
  }

  /** A veto is a rejection, not a failure: only FAILURE outcomes count. */
  @Test
  void aVetoIsNotCountedAsAFailure() {
    command("CancelOrder", U1, AuditOutcome.VETOED, FROM);
    command("CancelOrder", U1, AuditOutcome.FAILURE, FROM.plusSeconds(1));

    ComplianceReport report = query.generate(FROM, TO);

    assertEquals(2, report.totalCommands());
    assertEquals(1, report.totalFailures());
  }

  /** Every attempt of a retried command is an entry: a repeated command id counts each time. */
  @Test
  void aRepeatedCommandIdCountsEveryAttempt() {
    CommandId id = CommandId.of("same");
    for (int attempt = 0; attempt < 2; attempt++) {
      writer.save(
          new AuditEntry(
              id,
              "PlaceOrder",
              AggregateType.of("order"),
              AggregateId.of("order-1"),
              U1,
              FROM.plusSeconds(attempt),
              attempt == 0 ? AuditOutcome.FAILURE : AuditOutcome.SUCCESS,
              attempt == 0 ? "retry" : null,
              attempt == 0 ? 0 : 1,
              null));
    }

    ComplianceReport report = query.generate(FROM, TO);

    assertEquals(2, report.totalCommands());
    assertEquals(1, report.totalFailures());
    assertEquals(Map.of("PlaceOrder", 2L), report.commandCountsByType());
    assertEquals(Map.of("u1", 2L), report.activityByUser());
  }

  @Test
  void aFailedQueryCountsAsAFailure() {
    audited("GetOrder", U1, AuditOutcome.FAILURE, FROM);

    ComplianceReport report = query.generate(FROM, TO);

    assertEquals(0, report.totalCommands());
    assertEquals(1, report.totalQueries());
    assertEquals(1, report.totalFailures());
  }

  @Test
  void theRangeIsInclusiveAtBothEndsAndExcludesMillisecondsOutside() {
    command("PlaceOrder", U1, AuditOutcome.SUCCESS, FROM.minusMillis(1));
    command("PlaceOrder", U1, AuditOutcome.SUCCESS, FROM);
    command("PlaceOrder", U1, AuditOutcome.SUCCESS, TO);
    command("PlaceOrder", U1, AuditOutcome.SUCCESS, TO.plusMillis(1));

    ComplianceReport all = query.generate(FROM, TO);
    ComplianceReport forUser = query.generateForUser(U1, FROM, TO);

    assertEquals(2, all.totalCommands());
    assertEquals(Map.of("PlaceOrder", 2L), all.commandCountsByType());
    assertEquals(Map.of("u1", 2L), all.activityByUser());
    assertEquals(2, forUser.totalCommands());
  }

  @Test
  void anEmptyRangeYieldsZerosAndEmptyMapsAndEchoesTheRange() {
    writeFixture();
    Instant from = TO.plus(Duration.ofDays(1));
    Instant to = from.plus(Duration.ofHours(1));

    ComplianceReport report = query.generate(from, to);

    assertEquals(from, report.from());
    assertEquals(to, report.to());
    assertEquals(0, report.totalCommands());
    assertEquals(0, report.totalQueries());
    assertEquals(0, report.totalFailures());
    assertTrue(report.commandCountsByType().isEmpty());
    assertTrue(report.queryCountsByType().isEmpty());
    assertTrue(report.activityByUser().isEmpty());
  }

  @Test
  void aRangeWhoseFromIsAfterItsToYieldsAnEmptyReport() {
    writeFixture();

    ComplianceReport all = query.generate(TO, FROM);
    ComplianceReport forUser = query.generateForUser(U1, TO, FROM);

    for (ComplianceReport report : java.util.List.of(all, forUser)) {
      assertEquals(0, report.totalCommands());
      assertEquals(0, report.totalQueries());
      assertEquals(0, report.totalFailures());
      assertTrue(report.activityByUser().isEmpty());
    }
  }

  @Test
  void anEmptyTrailYieldsAnEmptyReport() {
    ComplianceReport report = query.generate(FROM, TO);

    assertEquals(0, report.totalCommands());
    assertEquals(0, report.totalQueries());
    assertEquals(0, report.totalFailures());
    assertTrue(report.activityByUser().isEmpty());
  }

  // ---------------------------------------------------------------- generateForUser

  @Test
  void aUserReportScopesEveryFigureToThatUser() {
    writeFixture();

    ComplianceReport u1 = query.generateForUser(U1, FROM, TO);
    ComplianceReport u2 = query.generateForUser(U2, FROM, TO);

    assertEquals(FROM, u1.from());
    assertEquals(TO, u1.to());
    assertEquals(2, u1.totalCommands());
    assertEquals(1, u1.totalQueries());
    assertEquals(1, u1.totalFailures());
    assertEquals(Map.of("PlaceOrder", 2L), u1.commandCountsByType());
    assertEquals(Map.of("GetOrder", 1L), u1.queryCountsByType());
    assertEquals(Map.of("u1", 3L), u1.activityByUser());

    assertEquals(3, u2.totalCommands());
    assertEquals(1, u2.totalQueries());
    assertEquals(1, u2.totalFailures());
    assertEquals(
        Map.of("PlaceOrder", 1L, "CancelOrder", 1L, "ForgetSubject", 1L), u2.commandCountsByType());
    assertEquals(Map.of("GetOrder", 1L), u2.queryCountsByType());
    assertEquals(Map.of("u2", 4L), u2.activityByUser());
  }

  @Test
  void aUserWithNoActivityInTheRangeYieldsAnEmptyReport() {
    writeFixture();

    ComplianceReport report = query.generateForUser(UserId.of("nobody"), FROM, TO);

    assertEquals(0, report.totalCommands());
    assertEquals(0, report.totalQueries());
    assertEquals(0, report.totalFailures());
    assertTrue(report.commandCountsByType().isEmpty());
    assertTrue(report.queryCountsByType().isEmpty());
    assertTrue(report.activityByUser().isEmpty());
  }

  @Test
  void aUserReportDoesNotCountAnonymousEntries() {
    command("ShipOrder", null, AuditOutcome.SUCCESS, FROM);
    audited("ListOrders", null, AuditOutcome.SUCCESS, FROM.plusSeconds(1));

    ComplianceReport report = query.generateForUser(U1, FROM, TO);

    assertEquals(0, report.totalCommands());
    assertEquals(0, report.totalQueries());
  }

  // ---------------------------------------------------------------- null arguments

  @Test
  void nullArgumentsAreRefusedWithANullPointerException() {
    assertThrows(NullPointerException.class, () -> query.generate(null, TO));
    assertThrows(NullPointerException.class, () -> query.generate(FROM, null));
    assertThrows(NullPointerException.class, () -> query.generateForUser(null, FROM, TO));
    assertThrows(NullPointerException.class, () -> query.generateForUser(U1, null, TO));
    assertThrows(NullPointerException.class, () -> query.generateForUser(U1, FROM, null));
  }
}

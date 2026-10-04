package org.streamrune.core.audit;

import java.time.Instant;
import org.streamrune.core.types.UserId;

/** Generates compliance reports by aggregating the audit trail. */
public interface ComplianceReportQuery {

  /**
   * Generates a compliance report covering all activity in the time range.
   *
   * @param from start of range (inclusive)
   * @param to end of range (inclusive)
   * @return aggregated compliance report
   */
  ComplianceReport generate(Instant from, Instant to);

  /**
   * Generates a compliance report scoped to a single user.
   *
   * @param userId the user to report on
   * @param from start of range (inclusive)
   * @param to end of range (inclusive)
   * @return aggregated compliance report for the user
   */
  ComplianceReport generateForUser(UserId userId, Instant from, Instant to);
}

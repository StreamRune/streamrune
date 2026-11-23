package org.streamrune.core.audit;

import java.time.Instant;
import org.streamrune.core.types.UserId;

/** Generates compliance reports by aggregating the audit trail. */
public interface ComplianceReportQuery {

  /**
   * Generates a compliance report covering all activity in the time range.
   *
   * @param from start of range (inclusive); must not be {@code null}
   * @param to end of range (inclusive); must not be {@code null}
   * @return aggregated compliance report; an empty range yields zero totals and empty maps
   * @throws NullPointerException if any argument is {@code null}
   */
  ComplianceReport generate(Instant from, Instant to);

  /**
   * Generates a compliance report scoped to a single user.
   *
   * @param userId the user to report on; must not be {@code null}
   * @param from start of range (inclusive); must not be {@code null}
   * @param to end of range (inclusive); must not be {@code null}
   * @return aggregated compliance report for the user; every figure covers that user's entries only
   * @throws NullPointerException if any argument is {@code null}
   */
  ComplianceReport generateForUser(UserId userId, Instant from, Instant to);
}

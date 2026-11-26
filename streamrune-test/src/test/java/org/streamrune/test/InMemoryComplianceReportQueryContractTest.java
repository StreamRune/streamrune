package org.streamrune.test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.InMemoryComplianceReportQuery;

/**
 * The compliance contract against {@link InMemoryComplianceReportQuery}. That class reads a fixed
 * list of entries, so the writer collects them and each report is built over what was written so
 * far.
 */
class InMemoryComplianceReportQueryContractTest extends ComplianceReportQueryContract {

  private final List<AuditEntry> written = new ArrayList<>();

  @Override
  protected AuditStore writer() {
    return written::add;
  }

  @Override
  protected ComplianceReportQuery query() {
    return new ComplianceReportQuery() {
      @Override
      public ComplianceReport generate(Instant from, Instant to) {
        return new InMemoryComplianceReportQuery(written).generate(from, to);
      }

      @Override
      public ComplianceReport generateForUser(UserId userId, Instant from, Instant to) {
        return new InMemoryComplianceReportQuery(written).generateForUser(userId, from, to);
      }
    };
  }
}

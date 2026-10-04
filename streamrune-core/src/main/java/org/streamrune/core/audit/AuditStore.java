package org.streamrune.core.audit;

/**
 * Write-only sink for command audit entries.
 *
 * <p>Implementations persist {@link AuditEntry} records for observability and compliance. The audit
 * trail is append-only — entries are never updated or deleted through this interface. The read side
 * is {@link CommandAuditQuery} (per-entry forensics) and {@link ComplianceReportQuery} (aggregated
 * counts).
 */
public interface AuditStore {

  /**
   * Persists an audit entry.
   *
   * @param entry the audit entry to save; must not be {@code null}
   * @throws NullPointerException if {@code entry} is {@code null}
   */
  void save(AuditEntry entry);
}

package org.streamrune.runtime.gdpr;

import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.gdpr.GdprAction;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

/**
 * Builds and writes synthetic {@link AuditEntry} records for GDPR operations. Reuses the existing
 * {@code audit_log} table — no schema change. {@code commandId} is a synthetic UUID prefixed with
 * {@code gdpr-}; {@code aggregateId} is repurposed as the subject ID; {@code commandType} is {@code
 * GDPR_<action>} for greppable filtering ({@code WHERE command_type LIKE 'GDPR_%'}).
 *
 * <p><b>The stored subject id is HASHED, never raw.</b> A subject id may itself be personal data
 * (an email or username), so a GDPR erasure that wrote it verbatim into {@code
 * audit_log.aggregate_id} would mint a fresh, unshredded cleartext copy of the identifier — the
 * Article-17 operation re-creating the PII it erases, retained until the audit table's own sweep.
 * The {@code aggregateId} column therefore holds {@link SubjectId#redacted()} (the same SHA-256 hex
 * the crypto engines store at rest), so an operator can still correlate audit rows to the subject's
 * key/tombstone rows without the raw identifier ever landing in the audit trail.
 *
 * <p>Package-private — internal collaborator of {@link ForgetSubjectService} and {@link
 * ExportSubjectDataService}.
 */
final class GdprAuditWriter {

  private static final Logger LOG = LoggerFactory.getLogger(GdprAuditWriter.class);

  private final AuditStore auditStore;

  /**
   * @param auditStore optional; may be {@code null} to disable audit writes
   */
  GdprAuditWriter(AuditStore auditStore) {
    this.auditStore = auditStore;
  }

  /**
   * Writes an audit entry for a GDPR operation. If {@code auditStore} is null, this is a no-op. If
   * the underlying store throws, the exception is propagated to the caller — callers that need
   * swallow-on-failure semantics must wrap accordingly.
   */
  void write(
      GdprAction action,
      AuditOutcome outcome,
      SubjectId subjectId,
      UserId requesterUserId,
      String errorOrSummary) {
    if (auditStore == null) {
      return;
    }
    var entry =
        new AuditEntry(
            CommandId.of("gdpr-" + UUID.randomUUID()),
            "GDPR_" + action.name(),
            // a subject, not an aggregate: no type
            null,
            // Store the SHA-256 hash, never the raw subject id (which may be PII).
            subjectId != null ? AggregateId.of(subjectId.redacted()) : null,
            requesterUserId,
            Instant.now(),
            outcome,
            // Persisted free text — a backend's exception message — sanitized at this sink.
            LogSanitizer.sanitizeFreeText(errorOrSummary),
            0,
            null);
    auditStore.save(entry);
  }

  /** Returns the audit logger for callers that need to log swallowed audit failures. */
  static Logger logger() {
    return LOG;
  }
}

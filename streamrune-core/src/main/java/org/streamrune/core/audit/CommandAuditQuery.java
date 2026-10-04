package org.streamrune.core.audit;

import java.time.Instant;
import java.util.Optional;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.UserId;

/**
 * Read-side interface for querying the command audit trail written through {@link AuditStore}.
 *
 * <p>Mirrors {@link EventAuditQuery} for command executions, answering the forensic and
 * GDPR-accountability questions the audit trail exists for: "show me the failed commands for user X
 * in the last hour" ({@link #findByUserId} / {@link #findByOutcome}), "what command produced this
 * error" ({@link #findByCommandId}). For aggregated counts use {@link ComplianceReportQuery}.
 *
 * <p>All paged methods return results ordered by {@link AuditEntry#occurredAt()} ascending; the
 * {@link PageRequest#sort()} component is ignored. An empty page is returned (never {@code null})
 * when no entries match.
 *
 * <p>The audit trail contains both command and query executions: queries are recorded by {@code
 * AuditingQueryBus} with a {@code null} {@link AuditEntry#commandId()} and an empty {@link
 * AuditEntry#aggregateId()}. Methods on this interface do not discriminate between the two kinds —
 * callers that need only commands can filter on a non-null {@code commandId()} (equivalently, a
 * non-empty {@code aggregateId()}).
 */
public interface CommandAuditQuery {

  /**
   * Returns the audit entry for the given command execution.
   *
   * <p>{@link AuditEntry#commandId()} uniquely identifies one command execution, so at most one
   * entry matches.
   *
   * @param commandId the command execution to look up; must not be {@code null}
   * @return the matching entry, or empty if the command was never audited
   * @throws NullPointerException if {@code commandId} is {@code null}
   */
  Optional<AuditEntry> findByCommandId(CommandId commandId);

  /**
   * Returns one page of the audit entries attributed to the given user within the specified time
   * window.
   *
   * <p>{@link AuditEntry#userId()} is a raw {@code String} where {@code null} marks an anonymous
   * request; implementations match entries whose {@code userId} equals {@code userId.value()}.
   * Anonymous entries are therefore never returned by this method.
   *
   * @param userId the user identity to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  Page<AuditEntry> findByUserId(UserId userId, Instant from, Instant to, PageRequest pageRequest);

  /**
   * Returns one page of the audit entries with the given outcome within the specified time window.
   *
   * @param outcome the outcome to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  Page<AuditEntry> findByOutcome(
      AuditOutcome outcome, Instant from, Instant to, PageRequest pageRequest);

  /**
   * Returns one page of the audit entries for the given command type within the specified time
   * window.
   *
   * @param commandType the simple class name recorded in {@link AuditEntry#commandType()}; must not
   *     be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  Page<AuditEntry> findByCommandType(
      String commandType, Instant from, Instant to, PageRequest pageRequest);
}

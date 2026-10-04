package org.streamrune.core.audit;

import java.time.Instant;
import java.util.Objects;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * Immutable record of a single command (or audited query) execution for audit purposes.
 *
 * <p><strong>Command vs query discriminator.</strong> The audit trail records both command
 * executions (written by {@code AuditCommandInterceptor}) and audited query dispatches (written by
 * {@code AuditingQueryBus}). The two are distinguished by {@link #commandId()}: command-origin
 * entries carry a non-{@code null} {@link CommandId}; query-origin entries leave it {@code null} (a
 * query is not a command and never receives a {@code CommandId}). Query-origin entries carry
 * neither {@link #aggregateType()} nor {@link #aggregateId()}.
 *
 * <p>{@code commandId} is nullable — {@code null} marks a query-origin entry. {@code correlationId}
 * is nullable — it carries the {@code StreamRuneContext} correlation token that ties this entry to
 * the originating business flow, or {@code null} when no request context was bound. {@code userId}
 * is nullable — a {@code null} value indicates an unauthenticated (anonymous) request. {@code
 * errorMessage} is nullable — present for {@link AuditOutcome#FAILURE} entries (the error detail)
 * and {@link AuditOutcome#VETOED} entries (the name of the rejecting interceptor), {@code null} for
 * successes. {@code eventCount} is always 0 for failures, vetoes, and query-origin entries.
 *
 * @param commandId unique ID assigned to the command execution, or {@code null} for query-origin
 *     entries
 * @param commandType simple class name of the command or query
 * @param aggregateType the aggregate type the command's registration declares; {@code null} for
 *     query-origin entries and for GDPR entries (which name a subject, not an aggregate)
 * @param aggregateId the id the command targeted (with {@code aggregateType}, its stream); the
 *     subject hash for GDPR entries; {@code null} for query-origin entries
 * @param userId identity of the caller, or {@code null} for anonymous requests
 * @param occurredAt when the command was first received (from {@code CommandContext.timestamp()})
 * @param outcome whether the execution succeeded or failed
 * @param errorMessage the error detail for a failed execution; {@code null} for success
 * @param eventCount number of domain events produced; 0 for failures and queries
 * @param correlationId the business-flow correlation token from the bound {@code
 *     StreamRuneContext}, or {@code null} when no context was bound
 */
public record AuditEntry(
    CommandId commandId,
    String commandType,
    AggregateType aggregateType,
    AggregateId aggregateId,
    UserId userId,
    Instant occurredAt,
    AuditOutcome outcome,
    String errorMessage,
    int eventCount,
    CorrelationId correlationId) {

  public AuditEntry {
    Objects.requireNonNull(commandType, "commandType is required");
    Objects.requireNonNull(occurredAt, "occurredAt is required");
    Objects.requireNonNull(outcome, "outcome is required");
    if (eventCount < 0) throw new IllegalArgumentException("eventCount must be non-negative");
    if (outcome == AuditOutcome.FAILURE && eventCount != 0) {
      throw new IllegalArgumentException("eventCount must be 0 for FAILURE outcomes");
    }
    if (outcome == AuditOutcome.VETOED && eventCount != 0) {
      throw new IllegalArgumentException("eventCount must be 0 for VETOED outcomes");
    }
  }
}

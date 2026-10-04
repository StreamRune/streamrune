package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.LogSanitizer;

/**
 * PostgreSQL-backed {@link AuditStore}. Appends audit entries to the {@code audit_log} table
 * created by the event-store baseline migration ({@code V001__streamrune_baseline.sql}).
 *
 * <p>The audit log is append-only — duplicate {@code command_id} values are allowed to support
 * retry scenarios where the same command is executed more than once. {@code command_id} is {@code
 * NULL} for query-origin entries (a query is not a command); {@code correlation_id} is {@code NULL}
 * when no request context was bound.
 */
public final class PostgresAuditStore implements AuditStore {

  private static final String INSERT =
      "INSERT INTO audit_log "
          + "(command_id, command_type, aggregate_id, aggregate_type, user_id, occurred_at, outcome,"
          + " error_message, event_count, correlation_id) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private final DataSource dataSource;

  public PostgresAuditStore(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  @Override
  public void save(AuditEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(INSERT)) {
      ps.setString(1, entry.commandId() != null ? entry.commandId().value() : null);
      ps.setString(2, entry.commandType());
      ps.setString(3, entry.aggregateId() != null ? entry.aggregateId().value() : null);
      // NULL for query-origin entries and for subject entries (whose aggregate_id is a hash).
      ps.setString(4, entry.aggregateType() != null ? entry.aggregateType().value() : null);
      ps.setString(5, entry.userId() != null ? entry.userId().value() : null);
      ps.setTimestamp(6, Timestamp.from(entry.occurredAt()));
      ps.setString(7, entry.outcome().name());
      ps.setString(8, entry.errorMessage());
      ps.setInt(9, entry.eventCount());
      ps.setString(10, entry.correlationId() != null ? entry.correlationId().value() : null);
      PostgresTransactions.commitIfManual(conn, "audit log save", c -> ps.executeUpdate());
    } catch (Exception e) {
      throw new EventStoreException(
          "Failed to save audit entry: "
              + LogSanitizer.sanitizeForLog(
                  entry.commandId() != null ? entry.commandId().value() : null),
          e);
    }
  }
}

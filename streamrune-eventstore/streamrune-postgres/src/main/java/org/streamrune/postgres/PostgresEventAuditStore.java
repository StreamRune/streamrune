package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditStore;

/**
 * PostgreSQL-backed {@link EventAuditStore}. Appends event audit entries to the {@code
 * event_audit_log} table created by the event-store baseline migration ({@code
 * V001__streamrune_baseline.sql}).
 *
 * <p>Provides both public methods (acquire own connection) and a package-private variant that
 * accepts an existing {@link Connection} for same-transaction writes with {@link
 * PostgresEventStore}.
 */
public final class PostgresEventAuditStore implements EventAuditStore {

  private static final String INSERT =
      "INSERT INTO event_audit_log "
          + "(event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, causation_id, user_id, occurred_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private final DataSource dataSource;

  public PostgresEventAuditStore(DataSource dataSource) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    this.dataSource = dataSource;
  }

  @Override
  public void save(EventAuditEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    saveAll(List.of(entry));
  }

  @Override
  public void saveAll(List<EventAuditEntry> entries) {
    Objects.requireNonNull(entries, "entries is required");
    if (entries.isEmpty()) return;

    try (Connection conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "event audit save",
          c -> {
            saveAll(entries, c);
            return null;
          });
    } catch (EventStoreException e) {
      throw e;
    } catch (Exception e) {
      throw new EventStoreException("Failed to save event audit entries", e);
    }
  }

  /**
   * Saves entries using the provided connection (same-transaction writes). Package-private for use
   * by {@link PostgresEventStore}.
   */
  void saveAll(List<EventAuditEntry> entries, Connection conn) {
    try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
      for (EventAuditEntry entry : entries) {
        ps.setString(1, entry.eventId().value());
        ps.setString(2, entry.eventType().name());
        PostgresStreamKeys.bind(ps, 3, entry.streamId());
        ps.setLong(5, entry.version().value());
        ps.setString(6, entry.commandId().value());
        ps.setString(7, entry.correlationId().value());
        ps.setString(8, entry.causationId() != null ? entry.causationId().value() : null);
        ps.setString(9, entry.userId() != null ? entry.userId().value() : null);
        ps.setTimestamp(10, Timestamp.from(entry.occurredAt()));
        ps.addBatch();
      }
      ps.executeBatch();
    } catch (Exception e) {
      throw new EventStoreException("Failed to save event audit entries", e);
    }
  }
}

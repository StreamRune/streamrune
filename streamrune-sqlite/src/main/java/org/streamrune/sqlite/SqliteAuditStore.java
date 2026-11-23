package org.streamrune.sqlite;

import java.sql.PreparedStatement;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.LogSanitizer;

/**
 * The SQLite {@link AuditStore}: appends audit entries to the {@code audit_log} table of a {@link
 * SqliteDatabase}, with the semantics of {@code PostgresAuditStore}. The table belongs to the
 * event-store baseline, which {@link SqliteEventStoreFactory} migrates; {@link
 * SqliteCommandAuditQuery} and {@link SqliteComplianceReportQuery} read it.
 *
 * <p>The log is append-only: a repeated {@code command_id} adds an entry, so every attempt of a
 * retried command is kept. {@code command_id} is {@code NULL} for a query-origin entry (a query is
 * not a command), {@code aggregate_type} also for a subject entry, and {@code correlation_id} when
 * no request context was bound. {@code occurred_at} is the entry's own instant, in epoch
 * microseconds.
 *
 * <p><b>Durable on return.</b> A save runs as {@code BEGIN IMMEDIATE} … {@code COMMIT} through the
 * one SQLite write helper, so it is committed when it returns. A connection on which the
 * application left a transaction open is refused rather than joined, and a connection handed out in
 * manual-commit mode is refused with an {@link IllegalStateException} before anything is written. A
 * save waits for the write lock at most the connection's {@code busy_timeout} and then fails with
 * an {@link EventStoreException} naming {@code busy_timeout}, having written nothing.
 *
 * <p><b>Failures.</b> A storage failure is an {@link EventStoreException}; a {@code null} entry is
 * refused with {@link NullPointerException}, as the interface says.
 */
public final class SqliteAuditStore implements AuditStore {

  private static final String INSERT =
      "INSERT INTO audit_log "
          + "(command_id, command_type, aggregate_id, aggregate_type, user_id, occurred_at, outcome,"
          + " error_message, event_count, correlation_id) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private final DataSource dataSource;

  /**
   * Creates the store over {@code database}.
   *
   * @param database the database whose {@code audit_log} table the store appends to
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteAuditStore(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
  }

  /**
   * {@inheritDoc}
   *
   * @throws EventStoreException if the write fails, including when the write lock is not granted
   *     within {@code busy_timeout}
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void save(AuditEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    String shown =
        LogSanitizer.sanitizeForLog(entry.commandId() != null ? entry.commandId().value() : null);
    SqliteTransactions.write(
        dataSource,
        "audit log save",
        "audit log save of " + shown,
        "Failed to save audit entry: " + shown,
        conn -> {
          try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
            ps.setString(1, entry.commandId() != null ? entry.commandId().value() : null);
            ps.setString(2, entry.commandType());
            ps.setString(3, entry.aggregateId() != null ? entry.aggregateId().value() : null);
            // NULL for query-origin entries and for subject entries (whose aggregate_id is a hash).
            ps.setString(4, entry.aggregateType() != null ? entry.aggregateType().value() : null);
            ps.setString(5, entry.userId() != null ? entry.userId().value() : null);
            ps.setLong(6, SqliteTimestamps.toMicros(entry.occurredAt()));
            ps.setString(7, entry.outcome().name());
            ps.setString(8, entry.errorMessage());
            ps.setInt(9, entry.eventCount());
            ps.setString(10, entry.correlationId() != null ? entry.correlationId().value() : null);
            return ps.executeUpdate();
          }
        });
  }
}

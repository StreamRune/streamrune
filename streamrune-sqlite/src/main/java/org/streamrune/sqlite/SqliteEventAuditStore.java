package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.runtime.jdbc.StreamKeys;

/**
 * The SQLite {@link EventAuditStore}: appends event audit entries to the {@code event_audit_log}
 * table of a {@link SqliteDatabase}, with the semantics of {@code PostgresEventAuditStore}. The
 * table belongs to the event-store baseline, which {@link SqliteEventStoreFactory} migrates; {@link
 * SqliteEventAuditQuery} reads it.
 *
 * <p><b>In the append.</b> Given to {@link SqliteEventStoreFactory#eventAuditStore}, the store's
 * rows are written by {@link SqliteEventStore} on its own append connection, inside the append's
 * write transaction, one per event: they commit with the events or not at all. That store must be
 * built over the same database file.
 *
 * <p><b>On its own.</b> {@link #save} and {@link #saveAll} run as one {@code BEGIN IMMEDIATE} …
 * {@code COMMIT} through the one SQLite write helper, so a batch is written whole or not at all and
 * is committed when the call returns. A connection on which the application left a transaction open
 * is refused rather than joined, and a connection handed out in manual-commit mode is refused with
 * an {@link IllegalStateException} before anything is written. A write waits for the write lock at
 * most the connection's {@code busy_timeout} and then fails with an {@link EventStoreException}
 * naming {@code busy_timeout}, having written nothing.
 *
 * <p><b>Failures.</b> An {@code eventId} already stored (the column is unique), also twice in one
 * batch, fails the whole batch with an {@link EventStoreException} and persists none of it, as any
 * other storage failure does. A {@code null} entry, list or element is refused with {@link
 * NullPointerException} before any I/O, as the interface says.
 */
public final class SqliteEventAuditStore implements EventAuditStore {

  private static final String INSERT =
      "INSERT INTO event_audit_log "
          + "(event_id, event_type, aggregate_type, aggregate_id, version, command_id, "
          + "correlation_id, causation_id, user_id, occurred_at) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String FAILED = "Failed to save event audit entries";

  private final DataSource dataSource;
  private final Path file;

  /**
   * Creates the store over {@code database}.
   *
   * @param database the database whose {@code event_audit_log} table the store appends to
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public SqliteEventAuditStore(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    this.dataSource = database.dataSource();
    this.file = database.file();
  }

  /** The database file the store keeps its entries in. */
  Path file() {
    return file;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void save(EventAuditEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    saveAll(List.of(entry));
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalStateException if the borrowed connection is in manual-commit mode (nothing is
   *     written)
   */
  @Override
  public void saveAll(List<EventAuditEntry> entries) {
    Objects.requireNonNull(entries, "entries is required");
    for (EventAuditEntry entry : entries) {
      Objects.requireNonNull(entry, "entries must not contain null");
    }
    if (entries.isEmpty()) {
      return;
    }
    SqliteTransactions.write(
        dataSource,
        "event audit save",
        "event audit save",
        FAILED,
        conn -> {
          insert(conn, entries);
          return null;
        });
  }

  /**
   * Saves {@code entries} on the caller's connection, inside the caller's open write transaction:
   * neither begins, commits nor rolls back. {@link SqliteEventStore} calls it inside its append
   * transaction, so the entries commit with the events or not at all.
   *
   * @param entries the entries to save (empty is a no-op)
   * @param conn the connection of the caller's open transaction
   * @throws EventStoreException if the write fails; the caller rolls its transaction back
   */
  void saveAll(List<EventAuditEntry> entries, Connection conn) {
    if (entries.isEmpty()) {
      return;
    }
    try {
      insert(conn, entries);
    } catch (SQLException e) {
      throw new EventStoreException(FAILED, e);
    }
  }

  private static void insert(Connection conn, List<EventAuditEntry> entries) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
      for (EventAuditEntry entry : entries) {
        ps.setString(1, entry.eventId().value());
        ps.setString(2, entry.eventType().value());
        StreamKeys.bind(ps, 3, entry.streamId());
        ps.setLong(5, entry.version().value());
        ps.setString(6, entry.commandId().value());
        ps.setString(7, entry.correlationId().value());
        ps.setString(8, entry.causationId() != null ? entry.causationId().value() : null);
        ps.setString(9, entry.userId() != null ? entry.userId().value() : null);
        ps.setLong(10, SqliteTimestamps.toMicros(entry.occurredAt()));
        ps.addBatch();
      }
      ps.executeBatch();
    }
  }
}

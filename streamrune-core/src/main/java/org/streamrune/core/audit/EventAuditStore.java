package org.streamrune.core.audit;

import java.util.List;

/**
 * Write-only sink for event audit entries.
 *
 * <p>Implementations persist {@link EventAuditEntry} records for observability and compliance. The
 * audit trail is append-only — entries are never updated or deleted through this interface.
 */
public interface EventAuditStore {

  /**
   * Persists a single event audit entry.
   *
   * <p>Not idempotent: entries are identified by their unique {@link EventAuditEntry#eventId()
   * eventId}, and persisting an entry whose eventId already exists is an error surfaced as a
   * backend-specific exception (e.g. a unique-constraint violation), never as a silent duplicate.
   *
   * @param entry the event audit entry to save; must not be {@code null}
   * @throws NullPointerException if {@code entry} is {@code null}
   */
  void save(EventAuditEntry entry);

  /**
   * Persists a batch of event audit entries in a single operation.
   *
   * <p>The batch is atomic: either every entry is persisted or none is. A failed call never leaves
   * a partial batch behind — partially persisted batches would punch holes into correlation chains
   * that read as missing activity. Implementations that cannot provide batch atomicity must
   * document the deviation and define which entries survive a mid-batch failure.
   *
   * <p>Not idempotent: entries are identified by their unique {@link EventAuditEntry#eventId()
   * eventId}. Retrying a call that failed cleanly is safe — atomicity guarantees the failed attempt
   * persisted nothing — but retrying a call whose outcome is unknown may fail with a
   * duplicate-eventId error rather than persist duplicate rows.
   *
   * <p>An empty list is a no-op.
   *
   * @param entries the list of event audit entries to save; must not be {@code null}
   * @throws NullPointerException if {@code entries} is {@code null}
   */
  void saveAll(List<EventAuditEntry> entries);
}

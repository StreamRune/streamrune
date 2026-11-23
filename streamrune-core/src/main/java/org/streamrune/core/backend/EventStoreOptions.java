package org.streamrune.core.backend;

import java.time.Duration;
import java.util.List;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * What {@link BackendStores#eventStore(EventStoreOptions)} builds an event store with.
 *
 * <p>The event store writes outbox entries, inbox claims and audit rows in the transaction of its
 * own append, so {@code outboxStore}, {@code commandInbox} and {@code eventAuditStore} must be the
 * stores of the same {@link BackendStores}. A backend refuses a store of another backend, or of no
 * backend, with an {@link IllegalArgumentException} when the event store is built, not at the first
 * append.
 *
 * <p>Two options with equal components are equal: the lists compare by content, every other
 * component by its own {@code equals}.
 *
 * @param typeRegistry the event and state types the store reads and writes; not {@code null}
 * @param cryptoEngine the engine that encrypts {@code @Encrypted} fields; {@code null} for none
 * @param upcasters the event upcasters, empty for none; not {@code null} and without {@code null}
 *     elements; copied
 * @param snapshotMigrations the snapshot migrations applied on load, empty for none; not {@code
 *     null} and without {@code null} elements; copied
 * @param outboxStore the outbox the store writes entries to in its append transaction; {@code null}
 *     for none
 * @param outboxEventMapper maps an appended event to its outbox entries; {@code null} for none
 * @param commandInbox the inbox an idempotent append claims its key in; {@code null} for none
 * @param eventAuditStore the audit store the store writes event audit rows to in its append
 *     transaction; {@code null} for none
 * @param statementTimeout the bound on every statement the store runs; {@code null} for the
 *     backend's default, {@link Duration#ZERO} for no bound of the store's own. A backend that
 *     cannot bound statements ignores it
 */
public record EventStoreOptions(
    EventTypeRegistry typeRegistry,
    CryptoEngine cryptoEngine,
    List<EventUpcaster> upcasters,
    List<SnapshotMigration> snapshotMigrations,
    OutboxStore outboxStore,
    OutboxEventMapper outboxEventMapper,
    CommandInbox commandInbox,
    EventAuditStore eventAuditStore,
    Duration statementTimeout) {

  /**
   * Validates the options and copies the lists.
   *
   * @throws IllegalArgumentException if {@code typeRegistry}, {@code upcasters} or {@code
   *     snapshotMigrations} is {@code null}, a list holds a {@code null} element, or {@code
   *     statementTimeout} is negative; the message names the component
   */
  public EventStoreOptions {
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    upcasters = copyOf(upcasters, "upcasters");
    snapshotMigrations = copyOf(snapshotMigrations, "snapshotMigrations");
    if (statementTimeout != null && statementTimeout.isNegative()) {
      throw new IllegalArgumentException(
          "statementTimeout must not be negative, got: " + statementTimeout);
    }
  }

  private static <T> List<T> copyOf(List<T> list, String component) {
    if (list == null) {
      throw new IllegalArgumentException(component + " is required (pass an empty list for none)");
    }
    for (T element : list) {
      if (element == null) {
        throw new IllegalArgumentException(component + " must not contain null");
      }
    }
    return List.copyOf(list);
  }
}

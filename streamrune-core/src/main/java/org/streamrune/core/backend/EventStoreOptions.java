package org.streamrune.core.backend;

import java.time.Duration;
import java.util.List;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * What {@link BackendStores#eventStoreFactory(EventStoreOptions)} configures an event store factory
 * with. The crypto engine is not an option: the event store takes {@link
 * BackendContext#cryptoEngine()}.
 *
 * <p>The event store writes outbox entries, inbox claims and audit rows in the transaction of its
 * own append, so the backend refuses an {@code outboxStore}, {@code commandInbox} or {@code
 * eventAuditStore} that is not this backend's own type, with an {@link IllegalArgumentException}
 * when the factory is configured, not at the first append. A store of the backend's own type is
 * accepted wherever it was built.
 *
 * <p>This record checks only presence and copies the lists. Whether a value is one the backend can
 * honour (for example a statement timeout too short to apply) is the backend's rule, reported as a
 * {@link BackendOptionException} naming the component.
 *
 * <p>Two options with equal components are equal: the lists compare by content, every other
 * component by its own {@code equals}.
 *
 * @param typeRegistry the event and state types the store reads and writes; not {@code null}
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
 *     backend's default, {@link Duration#ZERO} for no bound of the store's own
 */
public record EventStoreOptions(
    EventTypeRegistry typeRegistry,
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
   *     snapshotMigrations} is {@code null}, or a list holds a {@code null} element; the message
   *     names the component
   */
  public EventStoreOptions {
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    upcasters = copyOf(upcasters, "upcasters");
    snapshotMigrations = copyOf(snapshotMigrations, "snapshotMigrations");
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
